//! Dynamic Cutover & Live Hyperliquid Exchange Reconciliation (`aegis-execution::reconcile`).
//!
//! Queries live Hyperliquid Testnet/Mainnet `clearinghouseState` and `openOrders` using the
//! preserved `wallet_address` and decrypted `private_key` from MariaDB `system_config`.
//! Dynamically cancels stale legacy v1 open orders and closes or cleanly adopts any open
//! legacy/dust positions at cutover time, logging the audit trail to `telemetry_logs`.

use crate::http_client::{is_testnet_configured, HyperliquidHttpClient};
use crate::signing::{build_limit_order_wire, infer_tick_size, snap_to_tick, HL_ORACLE_BAND_PCT};
use aegis_core::{
    mask_identifier, normalize_hyperliquid_symbol, now_ms, round_dp, ExecutionMode, OrderSide,
    Result,
};
use aegis_db::{AssetUpsertInput, DatabaseManager, WorkingOrderInput};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;

/// Structured report produced by [`CutoverReconciler::run_cutover_reconciliation`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CutoverReconcileReport {
    pub reconciled_at_ms: i64,
    pub execution_mode: String,
    pub is_testnet: bool,
    pub wallet_address: String,
    pub exchange_reachable: bool,
    pub exchange_equity_usdc: f64,
    pub stale_orders_found: usize,
    pub stale_orders_cancelled: usize,
    pub legacy_positions_found: usize,
    pub legacy_positions_closed: usize,
    pub legacy_positions_adopted: usize,
    pub local_orphans_closed: usize,
    pub details: Vec<String>,
}

/// Dynamic Cutover & Live Exchange State Reconciler.
#[derive(Clone)]
pub struct CutoverReconciler {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
}

impl CutoverReconciler {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient) -> Self {
        Self { db, http }
    }

    /// Queries live Hyperliquid `clearinghouseState` and `openOrders`, cancels stale v1 open orders,
    /// and either closes (`close_legacy_positions == true`) or cleanly adopts any open legacy positions.
    pub fn run_cutover_reconciliation(
        &self,
        close_legacy_positions: bool,
    ) -> Result<CutoverReconcileReport> {
        let ts = now_ms();
        let cfg = self.db.load_config_or_default();
        let mode = cfg.execution_mode();
        let is_testnet = is_testnet_configured(&cfg);
        let wallet = cfg.get_str("wallet_address", "");
        let private_key = cfg.get_str("private_key", "");
        let fallback_eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);

        let mut report = CutoverReconcileReport {
            reconciled_at_ms: ts,
            execution_mode: mode.as_str().to_string(),
            is_testnet,
            wallet_address: mask_identifier(&wallet),
            exchange_reachable: false,
            exchange_equity_usdc: fallback_eq,
            ..Default::default()
        };

        if mode == ExecutionMode::Paper || wallet.trim().is_empty() {
            report.details.push(format!(
                "Cutover reconciliation ran in {} mode (wallet_configured={})",
                mode.as_str(),
                !wallet.trim().is_empty()
            ));
            let _ = self.db.log_telemetry(
                "INFO",
                "execution.reconcile",
                "Cutover reconciliation completed in PAPER / local mode",
                Some(serde_json::to_value(&report).unwrap_or_default()),
            );
            return Ok(report);
        }

        let ch_state = match self.http.fetch_clearinghouse_state(&wallet, &cfg) {
            Ok(val) => {
                report.exchange_reachable = true;
                val
            }
            Err(e) => {
                report
                    .details
                    .push(format!("Clearinghouse state unreachable: {e}"));
                let _ = self.db.log_telemetry(
                    "WARN",
                    "execution.reconcile",
                    &format!("Hyperliquid clearinghouseState unreachable during cutover: {e}"),
                    Some(serde_json::to_value(&report).unwrap_or_default()),
                );
                return Ok(report);
            }
        };

        let parse_f = |v: Option<&Value>| -> f64 {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|val| val.is_finite())
            .unwrap_or(0.0)
        };

        // 1. Resolve live on-chain account equity from perp + spot clearinghouse state
        let perp_eq = parse_f(ch_state.pointer("/marginSummary/accountValue"))
            .max(parse_f(
                ch_state.pointer("/crossMarginSummary/accountValue"),
            ))
            .max(parse_f(ch_state.get("withdrawable")));
        let mut live_eq = perp_eq;
        if live_eq < 1.0 {
            if let Ok(spot_state) = self.http.fetch_spot_clearinghouse_state(&wallet, &cfg) {
                if let Some(balances) = spot_state.get("balances").and_then(Value::as_array) {
                    for b in balances {
                        let coin = b.get("coin").and_then(Value::as_str).unwrap_or("");
                        if coin.eq_ignore_ascii_case("USDC") {
                            live_eq += parse_f(b.get("total"));
                        }
                    }
                }
            }
        }
        report.exchange_equity_usdc = if live_eq >= 10.0 {
            round_dp(live_eq, 2)
        } else {
            fallback_eq
        };

        // 2. Reconcile & cancel stale v1 open orders not present in v2 `positions`
        let known_local_oids: HashSet<u64> = {
            let mut set = HashSet::new();
            if let Ok(ordered) = self.db.get_assets_ordered() {
                for o in ordered {
                    if let Some(oid) = o
                        .exchange_order_id
                        .as_deref()
                        .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        set.insert(oid);
                    }
                }
            }
            if let Ok(held) = self.db.get_assets_held() {
                for h in held {
                    if let Ok(Some(pos)) = self.db.get_position_by_id(h.position_id) {
                        for opt in [pos.exchange_order_id, pos.protective_stop_oid] {
                            if let Some(oid) = opt
                                .as_deref()
                                .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
                                .and_then(|s| s.parse::<u64>().ok())
                            {
                                set.insert(oid);
                            }
                        }
                    }
                }
            }
            set
        };

        if let Ok(open_orders) = self.http.fetch_open_orders(&wallet, &cfg) {
            for ord in &open_orders {
                let coin = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                let oid = ord
                    .get("oid")
                    .and_then(Value::as_u64)
                    .or_else(|| {
                        ord.get("oid")
                            .and_then(Value::as_str)
                            .and_then(|s| s.parse::<u64>().ok())
                    })
                    .unwrap_or(0);
                if oid == 0 || coin.is_empty() || known_local_oids.contains(&oid) {
                    continue;
                }
                report.stale_orders_found += 1;

                if !private_key.is_empty() {
                    if let Ok(Some(asset)) = self.db.get_asset_by_symbol(coin) {
                        if self
                            .http
                            .cancel_order_wire(
                                asset.asset_index,
                                oid,
                                &private_key,
                                Some(&self.db),
                                &cfg,
                            )
                            .is_ok()
                        {
                            report.stale_orders_cancelled += 1;
                            report.details.push(format!(
                                "Cancelled stale open order oid={oid} on {}",
                                asset.symbol
                            ));
                        }
                    }
                }
            }
        }

        // 3. Inspect live Hyperliquid `assetPositions`
        let mut exchange_open_asset_ids: HashSet<i64> = HashSet::new();
        let default_stop_pct = cfg
            .get_f64("protective_stop_default_pct", 0.04)
            .clamp(0.005, 0.25);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);

        if let Some(asset_positions) = ch_state.get("assetPositions").and_then(Value::as_array) {
            for item in asset_positions {
                let pos = item.get("position").unwrap_or(item);
                let raw_coin = pos.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                let szi = parse_f(pos.get("szi"));
                if raw_coin.is_empty() || szi.abs() <= 1e-9 {
                    continue;
                }
                let entry_px = parse_f(pos.get("entryPx")).max(0.0001);
                let Ok((symbol, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                    continue;
                };

                let asset = match self.db.get_asset_by_symbol(&symbol)? {
                    Some(a) => a,
                    None => {
                        let tick = infer_tick_size(entry_px, 2);
                        let aid = self.db.upsert_exchange_asset(&AssetUpsertInput {
                            raw_symbol_or_coin: coin.clone(),
                            asset_index: 0,
                            sz_decimals: 2,
                            tick_size: tick,
                            lot_step_size: 0.01,
                            max_leverage: 20.0,
                            mark_price: entry_px,
                            mid_price: entry_px,
                            oracle_price: entry_px,
                            funding_rate_hourly: 0.0,
                            day_volume_usdc: 100_000.0,
                            open_interest_usdc: 50_000.0,
                            prev_day_price: entry_px,
                            is_core_eligible: true,
                            is_event_eligible: true,
                            is_priority_watched: true,
                        })?;
                        self.db.get_asset_by_id(aid)?.ok_or_else(|| {
                            aegis_core::AegisError::Database(format!("Asset {aid} missing"))
                        })?
                    }
                };

                if let Some(existing_local) = self.db.get_active_position_for_asset(asset.id)? {
                    if existing_local.status == "OPEN" {
                        exchange_open_asset_ids.insert(asset.id);
                        continue;
                    }
                }

                report.legacy_positions_found += 1;
                let side = if szi > 0.0 {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                };
                let qty = szi.abs();
                let ref_px = if asset.mark_price > 0.0 {
                    asset.mark_price
                } else {
                    entry_px
                };

                let mut closed_on_exchange = false;
                if close_legacy_positions && !private_key.is_empty() {
                    let close_is_buy = !side.is_buy();
                    let limit_px = if close_is_buy {
                        ref_px * (1.0 + HL_ORACLE_BAND_PCT)
                    } else {
                        ref_px * (1.0 - HL_ORACLE_BAND_PCT)
                    };
                    let wire = build_limit_order_wire(
                        asset.asset_index,
                        close_is_buy,
                        limit_px,
                        qty,
                        asset.tick_size,
                        asset.lot_step_size,
                        asset.sz_decimals,
                        true,
                        "Ioc",
                        Some(&format!(
                            "cutover-close-{}-{}",
                            coin.to_ascii_lowercase(),
                            ts
                        )),
                    );
                    if let Ok(ack) = self.http.submit_order_wire(
                        wire,
                        qty,
                        ref_px,
                        taker_fee_bps,
                        &private_key,
                        Some(&self.db),
                        &cfg,
                    ) {
                        if ack.accepted && ack.status == "FILLED" {
                            closed_on_exchange = true;
                            report.legacy_positions_closed += 1;
                            report.details.push(format!(
                                "Closed legacy Testnet position {} ({} {:.4} @ ${:.4})",
                                asset.symbol,
                                side.as_str(),
                                qty,
                                ack.avg_fill_price
                            ));
                        }
                    }
                }

                if !closed_on_exchange {
                    // Adopt the position cleanly into MariaDB `positions` so v2 1s Priority Monitor tracks it
                    let tick = asset.tick_size.max(1e-8);
                    let stop_px = if side.is_buy() {
                        snap_to_tick(entry_px * (1.0 - default_stop_pct), tick)
                    } else {
                        snap_to_tick(entry_px * (1.0 + default_stop_pct), tick)
                    };
                    let coid = format!("cutover-adopt-{}-{}", coin.to_ascii_lowercase(), ts);
                    let notional = round_dp(entry_px * qty, 4);
                    let weight = round_dp(
                        (notional / report.exchange_equity_usdc.max(1.0)) * side.sign(),
                        4,
                    );
                    let pid = self.db.create_working_order(&WorkingOrderInput {
                        asset_id: asset.id,
                        client_order_id: coid,
                        side,
                        limit_price: entry_px,
                        requested_quantity: qty,
                        reserved_margin_usdc: notional,
                        portfolio_weight: weight,
                        s1_weight_contrib: weight,
                        s2_weight_contrib: 0.0,
                        s3_weight_contrib: 0.0,
                        protective_stop_px: stop_px,
                        exchange_order_id: Some("hl-cutover-adopted".to_string()),
                        reason: "CUTOVER_ADOPTED_EXCHANGE_POSITION".to_string(),
                    })?;
                    self.db.promote_working_to_open(
                        pid,
                        entry_px,
                        qty,
                        0.0,
                        stop_px,
                        Some("hl-cutover-adopted"),
                        "CUTOVER_ADOPTED_EXCHANGE_POSITION",
                    )?;
                    let _ = self.db.set_asset_priority_watch(asset.id, true);
                    exchange_open_asset_ids.insert(asset.id);
                    report.legacy_positions_adopted += 1;
                    report.details.push(format!(
                        "Adopted live exchange position {} ({} {:.4} @ ${:.4}) into MariaDB",
                        asset.symbol,
                        side.as_str(),
                        qty,
                        entry_px
                    ));
                }
            }
        }

        // 4. Close any local `OPEN` rows in MariaDB that no longer exist on the exchange
        if let Ok(local_held) = self.db.get_assets_held() {
            for h in local_held {
                if !exchange_open_asset_ids.contains(&h.asset_id) {
                    let _ = self.db.close_position(
                        h.position_id,
                        h.mark_price,
                        0.0,
                        "CUTOVER_EXCHANGE_FLAT_RECONCILED",
                    );
                    report.local_orphans_closed += 1;
                    report.details.push(format!(
                        "Closed local orphan position #{} on {} (flat on exchange)",
                        h.position_id, h.symbol
                    ));
                }
            }
        }

        let _ = self.db.log_telemetry(
            "INFO",
            "execution.reconcile",
            &format!(
                "Cutover reconciliation complete: mode={}, equity=${:.2}, stale_orders_cancelled={}/{}, legacy_pos_closed={}, legacy_pos_adopted={}",
                report.execution_mode,
                report.exchange_equity_usdc,
                report.stale_orders_cancelled,
                report.stale_orders_found,
                report.legacy_positions_closed,
                report.legacy_positions_adopted
            ),
            Some(json!(report)),
        );

        Ok(report)
    }
}
