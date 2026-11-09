use aegis_core::{
    round_dp, validate_evm_address, AegisError, OrderExecutionResult, Result, UniverseAsset,
};
use aegis_db::ConfigMap;
use chrono::Utc;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use std::sync::atomic::{AtomicU64, Ordering};

use crate::signing::{
    format_hl_wire_price, format_hl_wire_size, infer_sz_decimals_from_lot_step, sign_l1_action,
    CancelAction, CancelWire, LimitOrderType, OrderAction, OrderTypeWire, OrderWire,
    SubAccountTransferAction, VaultTransferAction,
};
use crate::{
    ExecutionEngine, RestingOrderStatus, SubaccountTransferResult, SubaccountTransferStatus,
    VaultTransferResult, VaultTransferStatus, WalletEquitySyncResult, WalletEquitySyncStatus,
};

static LAST_NONCE_HIGH_WATER: AtomicU64 = AtomicU64::new(0);

/// Parses a Hyperliquid `/exchange` order response into an `OrderExecutionResult` (`NH-7`, `NM-9`, `Q1`).
///
/// Key safety invariants:
/// - Never fabricates `FILLED` with `reference_price` / `requested_units` when `avgPx` or `totalSz`
///   is missing or zero (`NH-7`); instead returns `OPEN_ON_BOOK` with
///   `HL_ACK_MISSING_FILL_DETAILS_PENDING_RECONCILE` so reconciliation resolves the actual fill.
/// - Never classifies an `"ok"` response with an empty or unrecognized `statuses` array as
///   `REJECTED` (`NH-7`); instead returns `OPEN_ON_BOOK` with
///   `HL_OK_UNRECOGNIZED_STATUS_PENDING_RECONCILE` so callers do not retry and duplicate orders.
/// - Incorporates exchange-reported fee or configured maker/taker fee bps (`NM-9`).
#[allow(clippy::too_many_arguments)]
pub fn parse_hyperliquid_order_ack(
    resp: &Value,
    symbol: &str,
    norm_side: &str,
    quantity: f64,
    reference_price: f64,
    prefer_maker: bool,
    fee_bps: f64,
    coid: &str,
) -> OrderExecutionResult {
    let status_str = resp.get("status").and_then(Value::as_str).unwrap_or("err");
    if status_str != "ok" {
        return OrderExecutionResult::rejected_with_type(
            coid,
            symbol,
            norm_side,
            quantity,
            "HL_EXCHANGE_STATUS_ERR",
        );
    }

    let statuses_opt = resp
        .get("response")
        .and_then(|r| r.get("data"))
        .and_then(|d| d.get("statuses"))
        .and_then(Value::as_array);

    let Some(statuses) = statuses_opt else {
        return OrderExecutionResult {
            fill_type: "HL_OK_UNRECOGNIZED_STATUS_PENDING_RECONCILE".to_string(),
            ..OrderExecutionResult::open_on_book(coid, "", symbol, norm_side, quantity)
        };
    };

    let Some(first) = statuses.first() else {
        return OrderExecutionResult {
            fill_type: "HL_OK_UNRECOGNIZED_STATUS_PENDING_RECONCILE".to_string(),
            ..OrderExecutionResult::open_on_book(coid, "", symbol, norm_side, quantity)
        };
    };

    if let Some(err_msg) = first.get("error").and_then(Value::as_str) {
        tracing::warn!(
            symbol = symbol,
            side = norm_side,
            error = err_msg,
            "Hyperliquid order rejected by L1"
        );
        return OrderExecutionResult::rejected_with_type(
            coid,
            symbol,
            norm_side,
            quantity,
            format!("HL_REJECTED:{err_msg}"),
        );
    }

    if let Some(resting) = first.get("resting") {
        let oid = resting
            .get("oid")
            .and_then(Value::as_u64)
            .or_else(|| {
                resting
                    .get("oid")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);
        let ex_oid = if oid > 0 {
            format!("hl-{oid}")
        } else {
            String::new()
        };
        return OrderExecutionResult::open_on_book(coid, &ex_oid, symbol, norm_side, quantity);
    }

    if let Some(filled) = first.get("filled") {
        let oid = filled
            .get("oid")
            .and_then(Value::as_u64)
            .or_else(|| {
                filled
                    .get("oid")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);
        let ex_oid = if oid > 0 {
            format!("hl-{oid}")
        } else {
            String::new()
        };
        let avg_px = filled
            .get("avgPx")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .unwrap_or(0.0);
        let total_sz = filled
            .get("totalSz")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .unwrap_or(0.0);
        let fee_opt = filled.get("fee").and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
        });

        if oid == 0
            || !avg_px.is_finite()
            || avg_px <= 0.0
            || !total_sz.is_finite()
            || total_sz <= 0.0
        {
            return OrderExecutionResult {
                fill_type: "HL_ACK_MISSING_FILL_DETAILS_PENDING_RECONCILE".to_string(),
                ..OrderExecutionResult::open_on_book(coid, &ex_oid, symbol, norm_side, quantity)
            };
        }

        let fees_paid_usdc = fee_opt.unwrap_or_else(|| {
            round_dp(total_sz * avg_px * (fee_bps.max(0.0) / 10_000.0), 4)
        });
        let slip_usd = round_dp((avg_px - reference_price).abs() * total_sz, 4);
        let eff_slip_bps = if reference_price > 0.0 {
            round_dp(
                ((avg_px - reference_price).abs() / reference_price) * 10_000.0,
                2,
            )
        } else {
            0.0
        };

        return OrderExecutionResult::filled(
            coid,
            &ex_oid,
            symbol,
            norm_side,
            quantity,
            round_dp(total_sz, 6),
            round_dp(avg_px, 6),
            slip_usd,
            eff_slip_bps,
            round_dp(fees_paid_usdc, 4),
            prefer_maker,
            "HL_LIVE_FILLED",
            vec![
                "CREATED".to_string(),
                "SUBMITTED".to_string(),
                "FILLED".to_string(),
            ],
        );
    }

    OrderExecutionResult {
        fill_type: "HL_OK_UNRECOGNIZED_STATUS_PENDING_RECONCILE".to_string(),
        ..OrderExecutionResult::open_on_book(coid, "", symbol, norm_side, quantity)
    }
}

impl ExecutionEngine {
    pub(crate) fn verify_live_signer(&self, priv_key: &str, cfg: &ConfigMap) -> Result<String> {
        let wallet = cfg.get_str("wallet_address", "");
        let mut agent = cfg.get_str("hyperliquid_agent_address", "");
        if !wallet.trim().is_empty() {
            if agent.trim().is_empty() {
                if let Ok(derived) = aegis_core::derive_evm_address_from_private_key(priv_key) {
                    if !derived.eq_ignore_ascii_case(&wallet) {
                        agent = derived;
                    }
                }
            }
            aegis_core::verify_signer_matches_wallet(
                priv_key,
                &wallet,
                if agent.trim().is_empty() {
                    None
                } else {
                    Some(agent.trim())
                },
            )
        } else {
            aegis_core::derive_evm_address_from_private_key(priv_key)
        }
    }

    /// Generates a strictly monotonic nonce using SQLite's atomic counter backed by an
    /// in-process `AtomicU64` high-water floor (`R3-N17`).
    pub(crate) fn next_monotonic_nonce(&self) -> u64 {
        let raw = self.db.next_hyperliquid_nonce().unwrap_or_else(|e| {
            tracing::warn!(
                error = %e,
                "DB nonce increment failed; falling back to in-process monotonic clock floor (R3-N17)"
            );
            Utc::now().timestamp_millis() as u64
        });
        let mut prev = LAST_NONCE_HIGH_WATER.load(Ordering::SeqCst);
        loop {
            let next = if raw > prev {
                raw
            } else {
                prev.saturating_add(1)
            };
            match LAST_NONCE_HIGH_WATER.compare_exchange(
                prev,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return next,
                Err(actual) => prev = actual,
            }
        }
    }

    pub(crate) fn post_signed_l1_action_with_nonce_retry<T: serde::Serialize>(
        &self,
        priv_key: &str,
        action: &T,
        cfg: &ConfigMap,
        is_testnet: bool,
    ) -> Result<(Value, Option<Value>)> {
        self.verify_live_signer(priv_key, cfg)?;
        let url = self.exchange_url(cfg, is_testnet);
        for attempt in 0..2 {
            let nonce = self.next_monotonic_nonce();
            let sig = sign_l1_action(priv_key, action, None, nonce, None, !is_testnet)?;
            let payload = json!({
                "action": serde_json::to_value(action).unwrap_or_default(),
                "nonce": nonce,
                "signature": {
                    "r": sig["r"],
                    "s": sig["s"],
                    "v": sig["v"],
                },
                "vaultAddress": Value::Null,
            });
            let resp_opt = self.http_post_json(&url, &payload);
            if attempt == 0 {
                if let Some(ref resp) = resp_opt {
                    let resp_lower = resp.to_string().to_ascii_lowercase();
                    let is_err = resp.get("status").and_then(Value::as_str) != Some("ok")
                        || resp
                            .get("response")
                            .and_then(|r| r.get("type"))
                            .and_then(Value::as_str)
                            == Some("error");
                    if is_err && resp_lower.contains("nonce") {
                        tracing::warn!(
                            nonce = nonce,
                            "Hyperliquid L1 rejected nonce; retrying once with freshly incremented monotonic DB nonce (NH-11)"
                        );
                        continue;
                    }
                }
            }
            return Ok((sig, resp_opt));
        }
        Err(AegisError::Execution(
            "Hyperliquid L1 nonce retry loop exhausted".to_string(),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn submit_hyperliquid_order_live(
        &self,
        asset: &UniverseAsset,
        norm_side: &str,
        quantity: f64,
        limit_price: f64,
        reference_price: f64,
        prefer_maker: bool,
        is_reduce_only: bool,
        coid: &str,
        cfg: &ConfigMap,
        is_testnet: bool,
    ) -> OrderExecutionResult {
        let priv_key = match self.db.get_signing_private_key() {
            Ok(k) if !k.trim().is_empty() => k,
            _ => {
                return OrderExecutionResult::rejected_with_type(
                    coid,
                    &asset.symbol,
                    norm_side,
                    quantity,
                    "MISSING_PRIVATE_KEY",
                );
            }
        };

        if let Err(e) = self.verify_live_signer(&priv_key, cfg) {
            tracing::error!(error = %e, "Live order rejected: signer address mismatch (NH-12)");
            return OrderExecutionResult::rejected_with_type(
                coid,
                &asset.symbol,
                norm_side,
                quantity,
                "SIGNER_MISMATCH",
            );
        }

        let Some(asset_idx) =
            self.fetch_hyperliquid_universe_asset_index(&asset.base_asset, is_testnet)
        else {
            return OrderExecutionResult::rejected_with_type(
                coid,
                &asset.symbol,
                norm_side,
                quantity,
                "UNKNOWN_ASSET_INDEX",
            );
        };

        let sz_decimals = infer_sz_decimals_from_lot_step(asset.lot_step_size);

        let is_buy = norm_side == "LONG";
        let px_str = format_hl_wire_price(limit_price, asset.tick_size, sz_decimals);
        let sz_str = format_hl_wire_size(quantity, asset.lot_step_size, sz_decimals);

        let sz_parsed = sz_str.parse::<f64>().unwrap_or(0.0);
        if sz_parsed <= 0.0 {
            return OrderExecutionResult::rejected_with_type(
                coid,
                &asset.symbol,
                norm_side,
                quantity,
                "ZERO_ROUNDED_SIZE",
            );
        }

        let tif = if prefer_maker { "Alo" } else { "Ioc" };
        let order_wire = OrderWire {
            a: asset_idx,
            b: is_buy,
            p: px_str,
            s: sz_str,
            r: is_reduce_only,
            t: OrderTypeWire {
                limit: Some(LimitOrderType {
                    tif: tif.to_string(),
                }),
                trigger: None,
            },
            c: None,
        };
        let action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![order_wire],
            grouping: "na".to_string(),
        };

        let (_, resp_opt) = match self
            .post_signed_l1_action_with_nonce_retry(&priv_key, &action, cfg, is_testnet)
        {
            Ok(pair) => pair,
            Err(e) => {
                tracing::error!(error = %e, "Failed to sign Hyperliquid order action");
                return OrderExecutionResult::rejected_with_type(
                    coid,
                    &asset.symbol,
                    norm_side,
                    quantity,
                    "SIGNING_ERROR",
                );
            }
        };

        let Some(resp) = resp_opt else {
            return OrderExecutionResult::rejected_with_type(
                coid,
                &asset.symbol,
                norm_side,
                quantity,
                "EXCHANGE_HTTP_ERROR",
            );
        };

        let fee_bps = if prefer_maker {
            cfg.get_f64("maker_fee_bps", 1.5)
        } else {
            cfg.get_f64("taker_fee_bps", 4.5)
        };

        let mut ack_res = parse_hyperliquid_order_ack(
            &resp,
            &asset.symbol,
            norm_side,
            quantity,
            reference_price,
            prefer_maker,
            fee_bps,
            coid,
        );

        if ack_res.fsm_state == "REJECTED" {
            let resp_lower = resp.to_string().to_ascii_lowercase();
            if resp_lower.contains("asset") || resp_lower.contains("invalid") {
                self.asset_index_cache.lock().clear();
            }
            if is_reduce_only
                && !prefer_maker
                && ack_res
                    .fill_type
                    .to_ascii_lowercase()
                    .contains("could not immediately match")
            {
                let info_endpoint = self.info_url(cfg, is_testnet);
                let coin_key = asset.base_asset.trim().to_uppercase();
                let live_mid = self
                    .http_post_json(&info_endpoint, &json!({ "type": "allMids" }))
                    .and_then(|mids| {
                        mids.get(&coin_key)
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .filter(|&p| p.is_finite() && p > 0.0)
                    })
                    .unwrap_or(reference_price);
                let retry_limit_px = if is_buy {
                    live_mid * 1.05
                } else {
                    live_mid * 0.95
                };
                let retry_px_str =
                    format_hl_wire_price(retry_limit_px, asset.tick_size, sz_decimals);
                let retry_sz_str =
                    format_hl_wire_size(quantity, asset.lot_step_size, sz_decimals);
                let retry_action = OrderAction {
                    action_type: "order".to_string(),
                    orders: vec![OrderWire {
                        a: asset_idx,
                        b: is_buy,
                        p: retry_px_str,
                        s: retry_sz_str,
                        r: true,
                        t: OrderTypeWire {
                            limit: Some(LimitOrderType {
                                tif: "Ioc".to_string(),
                            }),
                            trigger: None,
                        },
                        c: None,
                    }],
                    grouping: "na".to_string(),
                };
                if let Ok((_, Some(retry_resp))) = self.post_signed_l1_action_with_nonce_retry(
                    &priv_key,
                    &retry_action,
                    cfg,
                    is_testnet,
                ) {
                    ack_res = parse_hyperliquid_order_ack(
                        &retry_resp,
                        &asset.symbol,
                        norm_side,
                        quantity,
                        live_mid,
                        false,
                        fee_bps,
                        coid,
                    );
                }
            }
        }

        if ack_res.fill_type == "HL_ACK_MISSING_FILL_DETAILS_PENDING_RECONCILE"
            && ack_res.exchange_order_id.starts_with("hl-")
        {
            let status =
                self.check_resting_order_status(&asset.symbol, Some(&ack_res.exchange_order_id));
            if status.state == "FILLED" && status.filled_units > 0.0 && status.entry_price > 0.0 {
                let fees_paid_usdc = if status.fee_usdc > 0.0 {
                    round_dp(status.fee_usdc, 4)
                } else {
                    round_dp(status.filled_units * status.entry_price * (fee_bps / 10_000.0), 4)
                };
                let slip_usd =
                    round_dp((status.entry_price - reference_price).abs() * status.filled_units, 4);
                let eff_slip_bps = if reference_price > 0.0 {
                    round_dp(
                        ((status.entry_price - reference_price).abs() / reference_price) * 10_000.0,
                        2,
                    )
                } else {
                    0.0
                };
                let _ = self.db.log_order_state_transition(
                    coid,
                    "SUBMITTED",
                    "FILLED",
                    Some("HL_LIVE_FILLED_RECONCILED"),
                );
                return OrderExecutionResult::filled(
                    coid,
                    &ack_res.exchange_order_id,
                    &asset.symbol,
                    norm_side,
                    quantity,
                    status.filled_units,
                    status.entry_price,
                    slip_usd,
                    eff_slip_bps,
                    fees_paid_usdc,
                    prefer_maker,
                    "HL_LIVE_FILLED_RECONCILED",
                    vec![
                        "CREATED".to_string(),
                        "SUBMITTED".to_string(),
                        "FILLED".to_string(),
                    ],
                );
            }
        }

        let _ = self.db.log_order_state_transition(
            coid,
            "SUBMITTED",
            &ack_res.fsm_state,
            Some(&ack_res.fill_type),
        );

        ack_res
    }

    /// Queries Hyperliquid `userFills` for a specific `oid` (`NH-6`, `R3-H4`, `R3-H16`).
    /// Returns `(reachable, Option<(filled_sz, avg_px, total_fee)>)`.
    pub(crate) fn query_hyperliquid_fills_for_oid_with_reachability(
        &self,
        wallet_address: &str,
        target_oid: u64,
        is_testnet: bool,
    ) -> (bool, Option<(f64, f64, f64)>) {
        if wallet_address.trim().is_empty() || target_oid == 0 {
            return (false, None);
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_fills_for_oid");
        let url = self.info_url(&cfg, is_testnet);
        let Some(val) = self.http_post_json(
            &url,
            &json!({
                "type": "userFills",
                "user": wallet_address.trim(),
            }),
        ) else {
            return (false, None);
        };
        let Some(arr) = val.as_array() else {
            return (true, None);
        };
        let mut total_sz = 0.0_f64;
        let mut total_notional = 0.0_f64;
        let mut total_fee = 0.0_f64;
        for fill in arr {
            let oid = fill
                .get("oid")
                .and_then(Value::as_u64)
                .or_else(|| {
                    fill.get("oid")
                        .and_then(Value::as_str)
                        .and_then(|s| s.parse::<u64>().ok())
                })
                .unwrap_or(0);
            if oid != target_oid {
                continue;
            }
            let sz = fill
                .get("sz")
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .unwrap_or(0.0);
            let px = fill
                .get("px")
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .unwrap_or(0.0);
            let fee = fill
                .get("fee")
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .unwrap_or(0.0);
            if sz > 0.0 && px > 0.0 {
                total_sz += sz;
                total_notional += sz * px;
                total_fee += fee;
            }
        }
        if total_sz > 0.0 {
            (true, Some((total_sz, total_notional / total_sz, total_fee)))
        } else {
            (true, None)
        }
    }

    /// Queries Hyperliquid `userFills` for a specific `oid` (`NH-6`).
    pub fn fetch_hyperliquid_fills_for_oid(
        &self,
        wallet_address: &str,
        target_oid: u64,
        is_testnet: bool,
    ) -> Option<(f64, f64, f64)> {
        self.query_hyperliquid_fills_for_oid_with_reachability(
            wallet_address,
            target_oid,
            is_testnet,
        )
        .1
    }

    /// Checks whether a resting ALO order (`OPEN_ON_BOOK`) is still resting on `openOrders`,
    /// has filled (`userFills` / `orderStatus` by `oid`), was cancelled/expired (`GONE`), or
    /// could not be verified because all `/info` calls failed (`UNKNOWN`) (`NH-6`, `R3-H4`, `R3-H16`).
    ///
    /// - Never attributes an unrelated pre-existing `clearinghouseState` position `szi` to a
    ///   cancelled/expired resting order (`NH-6`).
    /// - Never falls back to coin-level `ord_coin == coin` matching when `oid` is missing (`R3-H4`).
    /// - Returns `"UNKNOWN"` when all Hyperliquid `/info` lookups fail (`R3-H4`).
    pub fn check_resting_order_status(
        &self,
        _symbol: &str,
        exchange_order_id: Option<&str>,
    ) -> RestingOrderStatus {
        let cfg = self.load_config_logged("check_resting_order_status");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &mode)
            .to_ascii_uppercase();
        if mode == "PAPER" && pond_mode == "PAPER" {
            return RestingOrderStatus {
                state: "FILLED".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
                fee_usdc: 0.0,
            };
        }

        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || mode == "TESTNET"
            || pond_mode == "TESTNET";
        let wallet = cfg.get_str("wallet_address", "");
        let target_oid = exchange_order_id
            .and_then(|s| s.strip_prefix("hl-").or(Some(s)))
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&id| id > 0);

        let Some(oid) = target_oid else {
            return RestingOrderStatus {
                state: "UNKNOWN".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
                fee_usdc: 0.0,
            };
        };

        let mut any_info_reachable = false;

        if let Some(open_orders) = self.fetch_hyperliquid_open_orders(&wallet, is_testnet) {
            any_info_reachable = true;
            let still_open = open_orders.iter().any(|ord| {
                let ord_oid = ord.get("oid").and_then(Value::as_u64).or_else(|| {
                    ord.get("oid")
                        .and_then(Value::as_str)
                        .and_then(|s| s.parse::<u64>().ok())
                });
                ord_oid == Some(oid)
            });
            if still_open {
                return RestingOrderStatus {
                    state: "OPEN_ON_BOOK".to_string(),
                    filled_units: 0.0,
                    entry_price: 0.0,
                    fee_usdc: 0.0,
                };
            }
        }

        let (fills_reachable, fills_opt) =
            self.query_hyperliquid_fills_for_oid_with_reachability(&wallet, oid, is_testnet);
        if fills_reachable {
            any_info_reachable = true;
        }
        if let Some((filled_sz, avg_px, fee)) = fills_opt {
            return RestingOrderStatus {
                state: "FILLED".to_string(),
                filled_units: round_dp(filled_sz, 6),
                entry_price: round_dp(avg_px, 6),
                fee_usdc: round_dp(fee, 4),
            };
        }

        let url = self.info_url(&cfg, is_testnet);
        if let Some(os_val) = self.http_post_json(
            &url,
            &json!({
                "type": "orderStatus",
                "user": wallet.trim(),
                "oid": oid,
            }),
        ) {
            any_info_reachable = true;
            let ord_status = os_val
                .get("order")
                .and_then(|o| o.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if ord_status.eq_ignore_ascii_case("open") {
                return RestingOrderStatus {
                    state: "OPEN_ON_BOOK".to_string(),
                    filled_units: 0.0,
                    entry_price: 0.0,
                    fee_usdc: 0.0,
                };
            }
            if ord_status.eq_ignore_ascii_case("filled") {
                if let Some(order_inner) = os_val.get("order").and_then(|o| o.get("order")) {
                    let orig_sz = order_inner
                        .get("origSz")
                        .or_else(|| order_inner.get("sz"))
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    let limit_px = order_inner
                        .get("limitPx")
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    if orig_sz > 0.0 && limit_px > 0.0 {
                        let maker_fee_bps = cfg.get_f64("maker_fee_bps", 1.5).max(0.0);
                        let est_fee = round_dp(orig_sz * limit_px * (maker_fee_bps / 10_000.0), 4);
                        return RestingOrderStatus {
                            state: "FILLED".to_string(),
                            filled_units: round_dp(orig_sz, 6),
                            entry_price: round_dp(limit_px, 6),
                            fee_usdc: est_fee,
                        };
                    }
                }
            }
        }

        if !any_info_reachable {
            return RestingOrderStatus {
                state: "UNKNOWN".to_string(),
                filled_units: 0.0,
                entry_price: 0.0,
                fee_usdc: 0.0,
            };
        }

        RestingOrderStatus {
            state: "GONE".to_string(),
            filled_units: 0.0,
            entry_price: 0.0,
            fee_usdc: 0.0,
        }
    }

    /// Cancels a resting order on Hyperliquid by `exchange_order_id` (`hl-<oid>`).
    pub fn cancel_resting_order(&self, symbol: &str, exchange_order_id: &str) -> Result<bool> {
        let cfg = self.db.load_config()?;
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &mode)
            .to_ascii_uppercase();
        if mode == "PAPER" && pond_mode == "PAPER" {
            return Ok(true);
        }
        let Some(oid) = exchange_order_id
            .strip_prefix("hl-")
            .or(Some(exchange_order_id))
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&id| id > 0)
        else {
            return Ok(false);
        };
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || mode == "TESTNET"
            || pond_mode == "TESTNET";
        let coin = symbol
            .split('/')
            .next()
            .unwrap_or(symbol)
            .trim()
            .to_uppercase();
        let Some(asset_idx) = self.fetch_hyperliquid_universe_asset_index(&coin, is_testnet) else {
            return Ok(false);
        };
        let priv_key = self.db.get_signing_private_key().unwrap_or_default();
        if priv_key.is_empty() {
            return Ok(false);
        }
        let action = CancelAction {
            action_type: "cancel".to_string(),
            cancels: vec![CancelWire { a: asset_idx, o: oid }],
        };
        let (_, resp_opt) =
            self.post_signed_l1_action_with_nonce_retry(&priv_key, &action, &cfg, is_testnet)?;
        if let Some(resp) = resp_opt {
            if resp.get("status").and_then(Value::as_str) != Some("ok") {
                return Ok(false);
            }
            if let Some(first_status) = resp
                .get("response")
                .and_then(|r| r.get("data"))
                .and_then(|d| d.get("statuses"))
                .and_then(Value::as_array)
                .and_then(|arr| arr.first())
            {
                if first_status.as_str() == Some("success") {
                    return Ok(true);
                }
                if let Some(err_str) = first_status.get("error").and_then(Value::as_str) {
                    let err_lower = err_str.to_ascii_lowercase();
                    return Ok(err_lower.contains("already canceled")
                        || err_lower.contains("already cancelled")
                        || err_lower.contains("never placed")
                        || err_lower.contains("filled")
                        || err_lower.contains("unknown oid"));
                }
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Re-verifies a resting order's status immediately after attempting an L1 cancel (`NH-6`, `R3-H4`, `R3-H16`).
    /// If `openOrders` is momentarily stale (`OPEN_ON_BOOK`) after a confirmed cancel, falls back to `userFills`
    /// with reachability tracking so race-window or partial fills are promoted rather than dropped.
    pub fn verify_post_cancel_resting_order(
        &self,
        symbol: &str,
        oid_str: &str,
        cancel_confirmed: bool,
    ) -> RestingOrderStatus {
        let mut r_status = self.check_resting_order_status(symbol, Some(oid_str));
        if cancel_confirmed && r_status.state == "OPEN_ON_BOOK" {
            if let Some(target_oid) = oid_str
                .strip_prefix("hl-")
                .or(Some(oid_str))
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|&id| id > 0)
            {
                let cfg = self.load_config_logged("verify_post_cancel_resting_order");
                let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
                let pond_mode = cfg
                    .get_str("pond_execution_mode", &mode)
                    .to_ascii_uppercase();
                let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
                    || mode == "TESTNET"
                    || pond_mode == "TESTNET";
                let wallet = cfg.get_str("wallet_address", "").trim().to_string();
                let (fills_reachable, fill_opt) = self
                    .query_hyperliquid_fills_for_oid_with_reachability(
                        &wallet,
                        target_oid,
                        is_testnet,
                    );
                if let Some((filled_sz, avg_px, total_fee)) = fill_opt {
                    r_status = RestingOrderStatus {
                        state: "FILLED".to_string(),
                        filled_units: round_dp(filled_sz, 6),
                        entry_price: round_dp(avg_px, 6),
                        fee_usdc: round_dp(total_fee, 4),
                    };
                } else if fills_reachable {
                    r_status.state = "GONE".to_string();
                } else {
                    r_status.state = "UNKNOWN".to_string();
                }
            }
        }
        r_status
    }

    /// Queries Hyperliquid `meta` endpoint to resolve dynamic coin -> integer asset index with TTL (`NM-10`).
    pub fn fetch_live_hyperliquid_universe_asset_index(
        &self,
        coin: &str,
        is_testnet: bool,
    ) -> Option<u32> {
        let norm_coin = coin
            .split('/')
            .next()
            .unwrap_or(coin)
            .trim()
            .to_uppercase();
        let cfg = self.load_config_logged("fetch_live_hyperliquid_universe_asset_index");
        let ttl_sec = cfg
            .get_i64("asset_index_cache_ttl_secs", cfg.get_i64("hyperliquid_asset_index_cache_ttl_sec", 3600))
            .max(1);
        let now_ms = self.db.now_ms();

        if let Some(&(cached_idx, cached_at_ms)) = self
            .asset_index_cache
            .lock()
            .get(&(is_testnet, norm_coin.clone()))
        {
            if now_ms.saturating_sub(cached_at_ms) <= ttl_sec * 1_000 {
                return Some(cached_idx);
            }
        }

        let url = self.info_url(&cfg, is_testnet);
        let meta = self.http_post_json(&url, &json!({ "type": "meta" }))?;
        let universe = meta.get("universe")?.as_array()?;

        let mut cache = self.asset_index_cache.lock();
        let mut matched: Option<u32> = None;
        for (idx, item) in universe.iter().enumerate() {
            if let Some(name) = item.get("name").and_then(Value::as_str) {
                let upper = name.trim().to_uppercase();
                cache.insert((is_testnet, upper.clone()), (idx as u32, now_ms));
                if upper == norm_coin {
                    matched = Some(idx as u32);
                }
            }
        }
        matched
    }

    pub fn fetch_hyperliquid_universe_asset_index(
        &self,
        coin: &str,
        is_testnet: bool,
    ) -> Option<u32> {
        if let Some(idx) = self.fetch_live_hyperliquid_universe_asset_index(coin, is_testnet) {
            return Some(idx);
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_universe_asset_index");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        if mode == "LIVE" || mode == "TESTNET" {
            return None;
        }
        let coin_upper = coin
            .split('/')
            .next()
            .unwrap_or(coin)
            .trim()
            .to_ascii_uppercase();
        let map_key = if is_testnet {
            "hyperliquid_paper_asset_indices_testnet"
        } else {
            "hyperliquid_paper_asset_indices_mainnet"
        };
        let raw_json = cfg.get_str(map_key, "{}");
        let parsed: Value = serde_json::from_str(&raw_json).ok()?;
        parsed
            .get(&coin_upper)
            .and_then(Value::as_u64)
            .map(|v| v as u32)
    }

    /// Signs a Hyperliquid L1 Exchange action using MsgPack + Keccak-256 + EIP-712 `Agent` phantom struct.
    pub fn sign_hyperliquid_action(
        private_key_hex: &str,
        action: &Value,
        nonce: u64,
        vault_address: Option<&str>,
        is_mainnet: bool,
    ) -> Result<Value> {
        sign_l1_action(
            private_key_hex,
            action,
            vault_address,
            nonce,
            None,
            is_mainnet,
        )
    }

    pub fn fetch_hyperliquid_clearinghouse_state(
        &self,
        wallet_address: &str,
        is_testnet: bool,
    ) -> Option<Value> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_clearinghouse_state");
        let url = self.info_url(&cfg, is_testnet);
        self.http_post_json(
            &url,
            &json!({
                "type": "clearinghouseState",
                "user": wallet_address.trim(),
            }),
        )
    }

    pub fn fetch_hyperliquid_spot_usdc_balance(
        &self,
        wallet_address: &str,
        is_testnet: bool,
    ) -> Option<(f64, f64)> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_spot_usdc_balance");
        let url = self.info_url(&cfg, is_testnet);
        let val = self.http_post_json(
            &url,
            &json!({
                "type": "spotClearinghouseState",
                "user": wallet_address.trim(),
            }),
        )?;
        let balances = val.get("balances")?.as_array()?;
        for b in balances {
            let coin = b.get("coin").and_then(Value::as_str).unwrap_or("");
            if coin.eq_ignore_ascii_case("USDC") {
                let total = b
                    .get("total")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                let hold = b
                    .get("hold")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                return Some((total, (total - hold).max(0.0)));
            }
        }
        Some((0.0, 0.0))
    }

    pub fn fetch_hyperliquid_open_orders(
        &self,
        wallet_address: &str,
        is_testnet: bool,
    ) -> Option<Vec<Value>> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_open_orders");
        let url = self.info_url(&cfg, is_testnet);
        let val = self.http_post_json(
            &url,
            &json!({
                "type": "openOrders",
                "user": wallet_address.trim(),
            }),
        )?;
        val.as_array().cloned()
    }

    pub fn fetch_hyperliquid_user_funding(
        &self,
        wallet_address: &str,
        start_time_ms: i64,
        is_testnet: bool,
    ) -> Option<Vec<Value>> {
        if wallet_address.trim().is_empty() {
            return None;
        }
        let cfg = self.load_config_logged("fetch_hyperliquid_user_funding");
        let url = self.info_url(&cfg, is_testnet);
        let val = self.http_post_json(
            &url,
            &json!({
                "type": "userFunding",
                "user": wallet_address.trim(),
                "startTime": start_time_ms,
            }),
        )?;
        val.as_array().cloned()
    }

    /// Queries Hyperliquid HLP Vault equity, accrued PnL yield, and APR (`vaultDetails` + `userVaultEquities`).
    pub fn fetch_hlp_vault_state(
        &self,
        wallet_address: Option<&str>,
        vault_address: Option<&str>,
        is_testnet: Option<bool>,
    ) -> Value {
        let cfg = self.load_config_logged("fetch_hlp_vault_state");
        let testnet = is_testnet.unwrap_or_else(|| {
            cfg.get_bool("hyperliquid_testnet", false)
                || cfg
                    .get_str("execution_mode", "PAPER")
                    .eq_ignore_ascii_case("TESTNET")
        });
        let wallet = wallet_address
            .map(str::to_string)
            .unwrap_or_else(|| cfg.get_str("wallet_address", ""));
        let vault = vault_address
            .map(str::to_string)
            .unwrap_or_else(|| Self::resolve_hlp_vault_address(&cfg, testnet));
        let url = self.info_url(&cfg, testnet);

        let default_apr = cfg.get_f64("hlp_default_estimated_apr", 0.125);
        let mut user_equity_usdc = 0.0f64;
        let mut accrued_yield_usdc = 0.0f64;
        let mut vault_apr = default_apr;
        let mut lockup_until_ms = 0i64;
        let mut allow_deposits = true;
        let mut live_queried = false;

        if !wallet.is_empty() {
            if let Some(eq_arr) = self
                .http_post_json(
                    &url,
                    &json!({
                        "type": "userVaultEquities",
                        "user": wallet,
                    }),
                )
                .and_then(|v| v.as_array().cloned())
            {
                live_queried = true;
                let mut sum_eq = 0.0f64;
                for item in eq_arr {
                    let eq_val = item
                        .get("equity")
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(0.0);
                    if eq_val.is_finite() && eq_val > 0.0 {
                        sum_eq += eq_val;
                    }
                    let lk = item
                        .get("lockedUntilTimestamp")
                        .and_then(|v| {
                            v.as_i64().or_else(|| {
                                v.as_f64().map(|f| f as i64).or_else(|| {
                                    v.as_str()
                                        .and_then(|s| s.parse::<f64>().ok().map(|f| f as i64))
                                })
                            })
                        })
                        .unwrap_or(0);
                    lockup_until_ms = lockup_until_ms.max(lk);
                }
                user_equity_usdc = sum_eq;
            }
        }

        if let Some(details) = self.http_post_json(
            &url,
            &json!({
                "type": "vaultDetails",
                "vaultAddress": vault,
                "user": if wallet.is_empty() { Value::Null } else { json!(wallet) },
            }),
        ) {
            if details.is_object() {
                live_queried = true;
                vault_apr = details
                    .get("apr")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(default_apr);
                let is_closed = details
                    .get("isClosed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                allow_deposits = details
                    .get("allowDeposits")
                    .and_then(Value::as_bool)
                    .unwrap_or(true)
                    && !is_closed;

                if let Some(follower) = details.get("followerState").and_then(Value::as_object) {
                    if let Some(feq) = follower.get("vaultEquity").and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    }) {
                        if feq.is_finite() && feq >= 0.0 {
                            user_equity_usdc = user_equity_usdc.max(feq);
                        }
                    }
                    let raw_pnl = follower.get("allTimePnl").or_else(|| follower.get("pnl"));
                    if let Some(pnl_val) = raw_pnl.and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    }) {
                        if pnl_val.is_finite() {
                            accrued_yield_usdc = pnl_val;
                        }
                    }
                    if let Some(flk) = follower.get("lockupUntil").and_then(|v| {
                        v.as_i64().or_else(|| {
                            v.as_f64().map(|f| f as i64).or_else(|| {
                                v.as_str()
                                    .and_then(|s| s.parse::<f64>().ok().map(|f| f as i64))
                            })
                        })
                    }) {
                        lockup_until_ms = lockup_until_ms.max(flk);
                    }
                }
            }
        }

        let mut perp_equity_usdc: Option<f64> = None;
        let mut perp_free_usdc: Option<f64> = None;
        if !wallet.is_empty() {
            let mut ch_tot = 0.0f64;
            let mut ch_free = 0.0f64;
            let mut got_ch = false;
            if let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(&wallet, testnet) {
                if let Some(ms) = ch.get("marginSummary") {
                    if let Some(acct_val) = ms.get("accountValue").and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    }) {
                        if acct_val.is_finite() {
                            ch_tot = acct_val;
                            got_ch = true;
                            live_queried = true;
                        }
                    }
                }
                if let Some(wd_val) = ch.get("withdrawable").and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                }) {
                    if wd_val.is_finite() {
                        ch_free = wd_val;
                    }
                }
            }
            if let Some((raw_spot_tot, raw_spot_free)) =
                self.fetch_hyperliquid_spot_usdc_balance(&wallet, testnet)
            {
                let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
                let faucet_offset = if testnet
                    && init_eq > 0.0
                    && init_eq <= 5_000.0
                    && raw_spot_tot >= 10_000.0
                    && (ch_tot + raw_spot_tot + user_equity_usdc) > init_eq * 2.5
                {
                    10_000.0
                } else {
                    0.0
                };
                let spot_tot = (raw_spot_tot - faucet_offset).max(0.0);
                let spot_free = (raw_spot_free - faucet_offset).max(0.0);
                if spot_tot > 0.0 {
                    ch_tot += spot_tot;
                    ch_free += spot_free;
                    got_ch = true;
                    live_queried = true;
                }
            }
            if got_ch {
                perp_equity_usdc = Some(round_dp(ch_tot, 2));
                perp_free_usdc = Some(round_dp(ch_free, 2));
            }
        }

        let rounded_vault_eq = round_dp(user_equity_usdc.max(0.0), 2);
        json!({
            "vault_address": vault,
            "wallet_address": wallet,
            "network": if testnet { "testnet" } else { "mainnet" },
            "user_vault_equity_usdc": rounded_vault_eq,
            "vault_equity_usdc": rounded_vault_eq,
            "accrued_yield_usdc": round_dp(accrued_yield_usdc, 4),
            "estimated_apr": round_dp(vault_apr, 4),
            "apr": round_dp(vault_apr, 4),
            "allow_deposits": allow_deposits,
            "locked_until_timestamp_ms": lockup_until_ms,
            "locked_until_ms": lockup_until_ms,
            "perp_equity_usdc": perp_equity_usdc,
            "perp_free_usdc": perp_free_usdc,
            "live_queried": live_queried,
        })
    }

    /// Executes or simulates an HLP Vault deposit/withdrawal (`vaultTransfer`).
    pub fn execute_hlp_vault_transfer(
        &self,
        amount_usdc: f64,
        is_deposit: bool,
        vault_address: Option<&str>,
    ) -> VaultTransferResult {
        let cfg = self.load_config_logged("execute_hlp_vault_transfer");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";
        let vault = vault_address
            .map(str::to_string)
            .unwrap_or_else(|| Self::resolve_hlp_vault_address(&cfg, is_testnet));
        let usd_micro = ((amount_usdc.max(0.0)) * 1e6).round() as i64;

        if mode == "PAPER" {
            *self.hlp_vault_error_streak.lock() = 0;
            return VaultTransferResult {
                status: VaultTransferStatus::Simulated,
                mode: Some("PAPER".to_string()),
                vault_address: vault,
                is_deposit,
                amount_usdc: round_dp(amount_usdc, 2),
                usd_micro,
                reason: None,
                error_streak: None,
                exchange_response: None,
            };
        }

        let wallet = cfg.get_str("wallet_address", "");
        let priv_key = self.db.get_signing_private_key().unwrap_or_default();
        if wallet.is_empty() || priv_key.is_empty() {
            return VaultTransferResult {
                status: VaultTransferStatus::Skipped,
                mode: None,
                vault_address: vault,
                is_deposit,
                amount_usdc: round_dp(amount_usdc, 2),
                usd_micro,
                reason: Some("MISSING_CREDENTIALS".to_string()),
                error_streak: None,
                exchange_response: None,
            };
        }
        if vault.trim().is_empty() {
            return VaultTransferResult {
                status: VaultTransferStatus::Error,
                mode: None,
                vault_address: String::new(),
                is_deposit,
                amount_usdc: round_dp(amount_usdc, 2),
                usd_micro,
                reason: Some("MISSING_VAULT_ADDRESS".to_string()),
                error_streak: None,
                exchange_response: None,
            };
        }

        let action = VaultTransferAction {
            action_type: "vaultTransfer".to_string(),
            vault_address: vault.clone(),
            is_deposit,
            usd: usd_micro,
        };

        match self.post_signed_l1_action_with_nonce_retry(&priv_key, &action, &cfg, is_testnet) {
            Ok((_, resp_opt)) => {
                if let Some(resp) = resp_opt {
                    let is_ok = resp.get("status").and_then(Value::as_str) == Some("ok")
                        && resp
                            .get("response")
                            .and_then(|r| r.get("type"))
                            .and_then(Value::as_str)
                            != Some("error");
                    if is_ok {
                        *self.hlp_vault_error_streak.lock() = 0;
                        return VaultTransferResult {
                            status: VaultTransferStatus::Submitted,
                            mode: None,
                            vault_address: vault,
                            is_deposit,
                            amount_usdc: round_dp(amount_usdc, 2),
                            usd_micro,
                            reason: None,
                            error_streak: None,
                            exchange_response: Some(resp),
                        };
                    }
                }
                let mut streak = self.hlp_vault_error_streak.lock();
                *streak += 1;
                let cur_streak = *streak;
                drop(streak);
                if cur_streak >= 3 {
                    let _ = self.alerts.notify_critical_event(
                        "HLP_VAULT_TRANSFER_FAILURE_STREAK",
                        &format!("HLP vault transfer failed {cur_streak} consecutive times."),
                        Some(json!({"vault_address": vault, "amount_usdc": amount_usdc})),
                    );
                }
                VaultTransferResult {
                    status: VaultTransferStatus::Error,
                    mode: None,
                    vault_address: vault,
                    is_deposit,
                    amount_usdc: round_dp(amount_usdc, 2),
                    usd_micro,
                    reason: Some("EXCHANGE_POST_FAILED".to_string()),
                    error_streak: Some(cur_streak),
                    exchange_response: None,
                }
            }
            Err(e) => VaultTransferResult {
                status: VaultTransferStatus::Error,
                mode: None,
                vault_address: vault,
                is_deposit,
                amount_usdc: round_dp(amount_usdc, 2),
                usd_micro,
                reason: Some(e.to_string()),
                error_streak: None,
                exchange_response: None,
            },
        }
    }

    /// Queries on-chain Hyperliquid clearinghouse + HLP vault equity and optionally rescales risk caps (`NH-11`).
    pub fn sync_wallet_equity(&self, update_risk_caps: bool) -> WalletEquitySyncResult {
        let cfg = self.load_config_logged("sync_wallet_equity");
        let wallet = cfg.get_str("wallet_address", "");
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false)
            || cfg
                .get_str("execution_mode", "PAPER")
                .eq_ignore_ascii_case("TESTNET");

        if wallet.trim().is_empty() {
            let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
            return WalletEquitySyncResult {
                status: WalletEquitySyncStatus::PaperDefault,
                wallet_address: String::new(),
                network: if is_testnet {
                    "testnet".to_string()
                } else {
                    "mainnet".to_string()
                },
                account_type: "perps".to_string(),
                wallet_balance_usdc: init_eq,
                total_equity_usdc: init_eq,
                perp_account_value_usdc: init_eq,
                withdrawable_usdc: init_eq,
                vault_equity_usdc: 0.0,
            };
        }

        let mut perp_eq = 0.0;
        let mut withdrawable = 0.0;
        let mut live_ok = false;

        if let Some(ch) = self.fetch_hyperliquid_clearinghouse_state(&wallet, is_testnet) {
            if let Some(ms) = ch.get("marginSummary") {
                perp_eq = ms
                    .get("accountValue")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                live_ok = true;
            }
            withdrawable = ch
                .get("withdrawable")
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                })
                .unwrap_or(perp_eq);
        }

        let vault_state = self.fetch_hlp_vault_state(Some(&wallet), None, Some(is_testnet));
        let vault_eq = vault_state
            .get("user_vault_equity_usdc")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);

        if let Some((raw_spot_tot, raw_spot_free)) =
            self.fetch_hyperliquid_spot_usdc_balance(&wallet, is_testnet)
        {
            let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
            let faucet_offset = if is_testnet
                && init_eq > 0.0
                && init_eq <= 5_000.0
                && raw_spot_tot >= 10_000.0
                && (perp_eq + raw_spot_tot + vault_eq) > init_eq * 2.5
            {
                10_000.0
            } else {
                0.0
            };
            let spot_tot = (raw_spot_tot - faucet_offset).max(0.0);
            let spot_free = (raw_spot_free - faucet_offset).max(0.0);
            if spot_tot > 0.0 {
                perp_eq += spot_tot;
                withdrawable += spot_free;
                live_ok = true;
            }
        }
        let total_eq = round_dp(perp_eq + vault_eq, 2);

        if live_ok && total_eq > 0.0 {
            if update_risk_caps {
                if let Err(e) = self.db.scale_risk_caps_for_equity(total_eq) {
                    tracing::error!(error = %e, "Failed to scale risk caps for equity");
                }
                // NOTE (NH-11): Do NOT overwrite `equity_baseline_reset_at` during periodic
                // `sync_wallet_equity(true)` calls — resetting the baseline timestamp on every
                // sync wipes out the daily drawdown window in `get_daily_equity_drawdown_state`.
            }
            if let Ok(mut snap) = self.db.get_latest_portfolio_snapshot() {
                let (_, _, _, total_deployed) = self
                    .db
                    .get_wallet_perps_margin_state()
                    .unwrap_or((total_eq, perp_eq, vault_eq, 0.0));
                snap.timestamp_ms = self.db.now_ms();
                snap.total_equity_usdc = total_eq;
                snap.basis_harvest_notional_usdc = round_dp(vault_eq, 2);
                snap.available_cash_usdc = round_dp((perp_eq - total_deployed).max(0.0), 2);
                if let Err(e) = self.db.save_portfolio_snapshot(&snap) {
                    tracing::error!(error = %e, "Failed to save portfolio snapshot during sync_wallet_equity");
                }
            }
        }

        let eff_total = if total_eq > 0.0 {
            total_eq
        } else {
            cfg.get_f64("initial_equity_usdc", 10000.0)
        };
        WalletEquitySyncResult {
            status: if live_ok {
                WalletEquitySyncStatus::SyncedOnChain
            } else {
                WalletEquitySyncStatus::UnreachableFallback
            },
            wallet_address: wallet,
            network: if is_testnet {
                "testnet".to_string()
            } else {
                "mainnet".to_string()
            },
            account_type: if vault_eq > 0.0 {
                "perps+vault".to_string()
            } else {
                "perps".to_string()
            },
            wallet_balance_usdc: eff_total,
            perp_account_value_usdc: round_dp(perp_eq, 2),
            withdrawable_usdc: round_dp(withdrawable, 2),
            vault_equity_usdc: round_dp(vault_eq, 2),
            total_equity_usdc: eff_total,
        }
    }

    fn submit_single_subaccount_leg(
        &self,
        priv_key: &str,
        sub_addr: &str,
        is_deposit: bool,
        usd_micro: i64,
        cfg: &ConfigMap,
        is_testnet: bool,
    ) -> Result<(String, Value)> {
        let action = SubAccountTransferAction {
            action_type: "subAccountTransfer".to_string(),
            sub_account_user: sub_addr.to_string(),
            is_deposit,
            usd: usd_micro,
        };
        let (sig, resp_opt) = self
            .post_signed_l1_action_with_nonce_retry(priv_key, &action, cfg, is_testnet)
            .map_err(|e| {
                AegisError::Execution(format!(
                    "PRE_SEND_VALIDATION_FAILED: Hyperliquid subAccountTransfer signing/signer validation failed for subAccountUser={sub_addr}: {e}"
                ))
            })?;
        let resp = resp_opt.ok_or_else(|| {
            AegisError::Execution(format!(
                "AMBIGUOUS_POST_SEND: Hyperliquid subAccountTransfer HTTP POST timed out or failed after dispatch for subAccountUser={sub_addr} (isDeposit={is_deposit}) — manual reconciliation required before retry"
            ))
        })?;

        let top_ok = resp.get("status").and_then(Value::as_str) == Some("ok");
        let inner_resp = resp.get("response");
        let inner_err = inner_resp
            .and_then(|r| {
                if r.get("type").and_then(Value::as_str) == Some("error") {
                    r.get("payload")
                        .and_then(Value::as_str)
                        .or(Some("L1 error response"))
                } else {
                    r.get("error").and_then(Value::as_str)
                }
            })
            .or_else(|| resp.get("error").and_then(Value::as_str));

        if !top_ok || inner_err.is_some() {
            return Err(AegisError::Execution(format!(
                "Hyperliquid subAccountTransfer rejected by L1 (subAccountUser={sub_addr}, isDeposit={is_deposit}): {}",
                inner_err.unwrap_or_else(|| resp.get("status").and_then(Value::as_str).unwrap_or("non-ok response"))
            )));
        }

        let extracted_tx_hash = inner_resp
            .and_then(|r| r.get("data"))
            .and_then(|d| d.get("txHash").or_else(|| d.get("hash")))
            .and_then(Value::as_str)
            .or_else(|| {
                inner_resp
                    .and_then(|r| r.get("txHash").or_else(|| r.get("hash")))
                    .and_then(Value::as_str)
            })
            .or_else(|| {
                resp.get("txHash")
                    .or_else(|| resp.get("hash"))
                    .and_then(Value::as_str)
            })
            .filter(|s| !s.trim().is_empty());

        let tx_hash = if let Some(h) = extracted_tx_hash {
            tracing::info!(
                sub_account_user = sub_addr,
                tx_hash = h,
                "Recorded exchange-returned L1 txHash for subAccountTransfer (NM-6)"
            );
            h.trim().to_string()
        } else {
            let conn_id = sig["connection_id"].as_str().unwrap_or("0x0");
            let fallback_ref = format!("action_hash:{conn_id}");
            tracing::info!(
                sub_account_user = sub_addr,
                tx_ref = %fallback_ref,
                "Exchange response omitted txHash; recorded client-side action_hash reference for subAccountTransfer (NM-6)"
            );
            fallback_ref
        };
        Ok((tx_hash, resp))
    }

    /// Executes an inter-book USDC transfer between Book A (PIT) and Book B (POND) (`NB-1`, `NM-6`, `C1`, `R3-B3`).
    pub fn execute_subaccount_transfer(
        &self,
        from_book: &str,
        to_book: &str,
        amount_usdc: f64,
        dry_run: bool,
    ) -> Result<SubaccountTransferResult> {
        self.execute_subaccount_transfer_for_record(None, from_book, to_book, amount_usdc, dry_run)
    }

    /// Executes an inter-book USDC transfer with optional `transfer_id` per-leg persistence (`R3-B3`, `R4-N9`, `R4-H2`).
    ///
    /// - Persists `legs_completed`, `leg1_tx_ref`, and `leg2_tx_ref` after each leg so a retry on a
    ///   2-leg transfer where leg 1 succeeded and leg 2 failed NEVER re-executes leg 1 (`R3-B3`),
    ///   and a transfer where all legs already succeeded returns `CONFIRMED` without re-submitting (`R4-N9`).
    /// - Marks post-send HTTP timeouts with `AMBIGUOUS_POST_SEND:` so callers halt automatic retries (`R3-B3`).
    pub fn execute_subaccount_transfer_for_record(
        &self,
        transfer_id: Option<i64>,
        from_book: &str,
        to_book: &str,
        amount_usdc: f64,
        dry_run: bool,
    ) -> Result<SubaccountTransferResult> {
        let from_norm = from_book.trim().to_ascii_uppercase();
        let to_norm = to_book.trim().to_ascii_uppercase();
        if (from_norm != "PIT" && from_norm != "POND")
            || (to_norm != "PIT" && to_norm != "POND")
            || from_norm == to_norm
        {
            return Err(AegisError::Config(format!(
                "Invalid book transfer direction: {from_book} -> {to_book}"
            )));
        }

        let cfg = self.db.load_config()?;
        let amt = round_dp(amount_usdc.max(0.0), 2);
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_testnet = cfg.get_bool("hyperliquid_testnet", false) || mode == "TESTNET";

        let master_wallet = cfg.get_str("wallet_address", "").trim().to_lowercase();
        let pit_sub = cfg
            .get_str("pit_subaccount_address", "")
            .trim()
            .to_string();
        let pond_sub = cfg
            .get_str("pond_subaccount_address", "")
            .trim()
            .to_string();

        let pit_is_sub = !pit_sub.is_empty()
            && validate_evm_address(&pit_sub, "pit_subaccount_address").is_ok()
            && pit_sub.to_lowercase() != master_wallet;
        let pond_is_sub = !pond_sub.is_empty()
            && validate_evm_address(&pond_sub, "pond_subaccount_address").is_ok()
            && pond_sub.to_lowercase() != master_wallet;

        let is_live_or_testnet = mode == "LIVE" || mode == "TESTNET";
        if !dry_run && is_live_or_testnet {
            if !pit_is_sub && !pond_is_sub {
                return Err(AegisError::Config(
                    "Live subAccountTransfer requires pit_subaccount_address or pond_subaccount_address configured (NB-1)"
                        .to_string(),
                ));
            }
            let priv_key = self.db.get_signing_private_key()?;
            if priv_key.trim().is_empty() {
                return Err(AegisError::Execution(
                    "Missing signing key for live Hyperliquid subAccountTransfer".to_string(),
                ));
            }
            let usd_micro = (amt * 1e6).round() as i64;
            if usd_micro <= 0 {
                return Err(AegisError::Execution(
                    "Transfer amount must be positive for live subAccountTransfer".to_string(),
                ));
            }

            let mut legs: Vec<(String, bool)> = Vec::new();
            if from_norm == "PIT" && to_norm == "POND" {
                if pit_is_sub {
                    legs.push((pit_sub.clone(), false));
                }
                if pond_is_sub && pond_sub.to_lowercase() != pit_sub.to_lowercase() {
                    legs.push((pond_sub.clone(), true));
                }
            } else {
                if pond_is_sub {
                    legs.push((pond_sub.clone(), false));
                }
                if pit_is_sub && pit_sub.to_lowercase() != pond_sub.to_lowercase() {
                    legs.push((pit_sub.clone(), true));
                }
            }

            if legs.is_empty() {
                return Err(AegisError::Config(
                    "Configured pit_subaccount_address and pond_subaccount_address are identical"
                        .to_string(),
                ));
            }

            let (already_completed, saved_leg1, saved_leg2) = match transfer_id {
                Some(tid) if tid > 0 => self
                    .db
                    .get_book_transfer_leg_progress(tid)
                    .unwrap_or((0, None, None)),
                _ => (0, None, None),
            };

            // R4-N9: If all legs for this transfer_id have already completed on a prior attempt,
            // return CONFIRMED immediately without re-executing any leg.
            if already_completed as usize >= legs.len() {
                let mut saved_hashes = Vec::new();
                if let Some(l1) = saved_leg1 {
                    saved_hashes.push(l1);
                }
                if legs.len() > 1 {
                    if let Some(l2) = saved_leg2 {
                        saved_hashes.push(l2);
                    }
                }
                let primary_tx_hash = saved_hashes
                    .last()
                    .cloned()
                    .unwrap_or_else(|| "already-completed".to_string());
                tracing::info!(
                    transfer_id = ?transfer_id,
                    legs_completed = already_completed,
                    tx_hash = %primary_tx_hash,
                    "All subAccountTransfer legs already completed for transfer_id; returning CONFIRMED without re-dispatch (R4-N9)"
                );
                return Ok(SubaccountTransferResult {
                    status: SubaccountTransferStatus::Confirmed,
                    transfer_mode: "HYPERLIQUID_SUBACCOUNT_L1".to_string(),
                    from_book: from_norm,
                    to_book: to_norm,
                    amount_usdc: amt,
                    tx_hash: primary_tx_hash,
                    leg_tx_hashes: saved_hashes,
                    legs_executed: legs.len(),
                    dry_run: false,
                    exchange_response: Some(json!({"status": "already_confirmed_all_legs"})),
                });
            }

            let mut tx_hashes = Vec::new();
            let mut responses = Vec::new();
            for (leg_idx, (sub_addr, is_deposit)) in legs.iter().enumerate() {
                if leg_idx == 0 && already_completed >= 1 {
                    let prev_hash = saved_leg1
                        .clone()
                        .unwrap_or_else(|| "leg1-already-completed".to_string());
                    tracing::info!(
                        transfer_id = ?transfer_id,
                        leg1_tx_ref = %prev_hash,
                        "Skipping already-completed leg 1 of 2-leg subAccountTransfer on resume (R3-B3)"
                    );
                    tx_hashes.push(prev_hash);
                    responses.push(json!({"status": "already_completed_leg1"}));
                    continue;
                }

                match self.submit_single_subaccount_leg(
                    &priv_key,
                    sub_addr,
                    *is_deposit,
                    usd_micro,
                    &cfg,
                    is_testnet,
                ) {
                    Ok((tx_hash, resp)) => {
                        tx_hashes.push(tx_hash.clone());
                        responses.push(resp);
                        if let Some(tid) = transfer_id.filter(|&id| id > 0) {
                            let l1 = tx_hashes.first().map(String::as_str);
                            let l2 = tx_hashes.get(1).map(String::as_str);
                            let _ = self.db.update_book_transfer_leg_progress(
                                tid,
                                (leg_idx + 1) as i64,
                                l1,
                                l2,
                            );
                        }
                    }
                    Err(e) => {
                        if leg_idx == 1 {
                            let leg1_ref = tx_hashes
                                .first()
                                .cloned()
                                .unwrap_or_else(|| "unknown".to_string());
                            return Err(AegisError::Execution(format!(
                                "PARTIAL_LEG1_COMPLETED(leg1_tx_ref={leg1_ref}): Leg 2 failed after Leg 1 succeeded: {e}"
                            )));
                        }
                        return Err(e);
                    }
                }
            }

            let primary_tx_hash = tx_hashes
                .last()
                .cloned()
                .unwrap_or_else(|| "0x0".to_string());
            return Ok(SubaccountTransferResult {
                status: SubaccountTransferStatus::Executed,
                transfer_mode: "HYPERLIQUID_SUBACCOUNT_L1".to_string(),
                from_book: from_norm,
                to_book: to_norm,
                amount_usdc: amt,
                tx_hash: primary_tx_hash,
                leg_tx_hashes: tx_hashes,
                legs_executed: legs.len(),
                dry_run: false,
                exchange_response: Some(if responses.len() == 1 {
                    responses[0].clone()
                } else {
                    json!(responses)
                }),
            });
        }

        let digest_hex = hex::encode(Sha256::digest(
            format!(
                "VIRTUAL_LEDGER:{from_norm}:{to_norm}:{amt}:{}",
                Utc::now().timestamp_nanos_opt().unwrap_or(0)
            )
            .as_bytes(),
        ));
        let tx_hash = if dry_run {
            format!("dryrun-0x{}", &digest_hex[..64])
        } else {
            format!("0x{}", &digest_hex[..64])
        };
        Ok(SubaccountTransferResult {
            status: if dry_run {
                SubaccountTransferStatus::DryRunSimulated
            } else {
                SubaccountTransferStatus::Executed
            },
            transfer_mode: "VIRTUAL_SINGLE_WALLET_LEDGER".to_string(),
            from_book: from_norm,
            to_book: to_norm,
            amount_usdc: amt,
            leg_tx_hashes: vec![tx_hash.clone()],
            tx_hash,
            legs_executed: 1,
            dry_run,
            exchange_response: None,
        })
    }
}
