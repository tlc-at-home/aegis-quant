//! Working orders & unified positions lifecycle repository (`aegis-db::positions_repo`).
//! Enforces the strict Order-to-Open Capital Lifecycle:
//! - `WORKING` orders reserve margin (`reserved_margin_usdc`), `committed_notional_usdc = 0.0`.
//! - `OPEN` positions commit funds (`committed_notional_usdc > 0.0`), `reserved_margin_usdc = 0.0`.

use crate::engine::DatabaseManager;
use crate::orders_repo::truncate_audit_reason;
use crate::portfolio_repo::FundingEventUpsertInput;
use crate::row_decode::{
    decimal_param, row_get_bool, row_get_f64, row_get_i64, row_get_opt_f64, row_get_opt_i64,
    row_get_opt_string, row_get_string, row_get_u32, DEC_20_10_MAX_ABS, DEC_20_8_MAX_ABS,
};
use aegis_core::{
    compute_position_pnl_pct, now_ms, resolve_position_entry_notional_usdc, round_dp, AegisError,
    ClosedPositionView, OrderSide, PositionRecord, Result,
};
use mysql::prelude::*;
use mysql::{params, PooledConn, Transaction, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

static SYNC_COID_SEQ: AtomicU64 = AtomicU64::new(1);

/// Tri-state update directive for exchange trigger order IDs (`R14-M26`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OidUpdate<'a> {
    /// Leave the persisted `protective_stop_oid` / `take_profit_oid` column unchanged.
    Keep,
    /// Overwrite with the confirmed resting exchange order ID.
    Set(&'a str),
    /// Explicitly clear the persisted order ID to `NULL` (only when confirmed absent/cancelled).
    Clear,
}

#[derive(Debug, Clone)]
pub struct WorkingOrderInput {
    pub asset_id: i64,
    pub client_order_id: String,
    pub side: OrderSide,
    pub limit_price: f64,
    pub requested_quantity: f64,
    pub reserved_margin_usdc: f64,
    pub portfolio_weight: f64,
    pub s1_weight_contrib: f64,
    pub s2_weight_contrib: f64,
    pub s3_weight_contrib: f64,
    pub protective_stop_px: f64,
    pub take_profit_px: Option<f64>,
    pub exchange_order_id: Option<String>,
    pub reason: String,
}

/// Parameter bundle for 1:1 exchange `OPEN` position synchronization (`R14-M29`).
#[derive(Debug, Clone)]
pub struct ExchangeOpenPositionSyncInput<'a> {
    pub asset_id: i64,
    pub coin: &'a str,
    pub side: OrderSide,
    pub entry_price: f64,
    pub mark_price: f64,
    pub quantity: f64,
    pub committed_notional_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub portfolio_weight: f64,
    pub protective_stop_px: f64,
    pub take_profit_px: f64,
    pub funding_accrued_usdc: f64,
    pub opened_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ReconciledClosedTradeInput {
    pub asset_id: i64,
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub close_epoch_id: Option<String>,
    pub side: OrderSide,
    pub entry_price: f64,
    pub exit_price: f64,
    pub filled_quantity: f64,
    pub realized_pnl_usdc: f64,
    pub fees_paid_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub created_at_ms: i64,
    pub closed_at_ms: i64,
    pub exit_reason: String,
}

/// Close-epoch aggregated closed trade produced by [`aggregate_close_epoch_trades`].
#[derive(Debug, Clone)]
pub struct MergedClosedTrade {
    pub asset_id: i64,
    pub side: OrderSide,
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub close_epoch_id: Option<String>,
    pub entry_price: f64,
    pub exit_price: f64,
    pub filled_quantity: f64,
    pub realized_pnl_usdc: f64,
    pub fees_paid_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub created_at_ms: i64,
    pub closed_at_ms: i64,
    pub exit_reason: String,
    pub merged_coids: Vec<String>,
    pub merged_eoids: Vec<String>,
}

#[derive(Debug, Clone)]
struct ActiveSyncRow {
    id: i64,
    status: String,
    side: String,
    protective_stop_px: f64,
    entry_r0: f64,
    protective_stop_oid: Option<String>,
    take_profit_px: f64,
    take_profit_oid: Option<String>,
    client_order_id: String,
    fsm_state: String,
    requested_quantity: f64,
}

fn derive_entry_r0_from_stop(entry_price: f64, entry_stop: f64) -> f64 {
    if entry_price.is_finite() && entry_price > 0.0 && entry_stop.is_finite() && entry_stop > 0.0 {
        round_dp((entry_price - entry_stop).abs(), 8)
    } else {
        0.0
    }
}

fn derive_entry_r0_for_sync(side: OrderSide, entry_price: f64, entry_stop: f64) -> f64 {
    let valid_initial_side = match side {
        OrderSide::Long => entry_stop < entry_price,
        OrderSide::Short => entry_stop > entry_price,
    };
    if valid_initial_side {
        derive_entry_r0_from_stop(entry_price, entry_stop)
    } else {
        0.0
    }
}

fn truncate_exit_reason(reason: &str) -> String {
    reason.chars().take(64).collect()
}

fn derive_take_profit_px(side: OrderSide, entry_price: f64, protective_stop_px: f64) -> f64 {
    if !entry_price.is_finite() || entry_price <= 0.0 {
        return 0.0;
    }
    // Default 2.8x/2.2x (~1.27x) R:R when protective stop is known, else 5.5% (`take_profit_default_pct`).
    let tp_dist = if protective_stop_px.is_finite() && protective_stop_px > 0.0 {
        ((entry_price - protective_stop_px).abs() * (2.8 / 2.2)).max(entry_price * 0.012)
    } else {
        entry_price * 0.055
    };
    match side {
        OrderSide::Long => round_dp(entry_price + tp_dist, 8),
        OrderSide::Short => round_dp((entry_price - tp_dist).max(entry_price * 0.05), 8),
    }
}

/// Resolves synced SL/TP targets during 15s exchange reconciliation (`#18` Loophole #1).
/// Never loosens an existing same-side `OPEN` or `WORKING` `protective_stop_px` or `take_profit_px`
/// even when breached against current mark price.
#[allow(clippy::too_many_arguments)]
pub fn resolve_synced_tpsl_targets(
    side: OrderSide,
    entry_price: f64,
    existing_side: Option<&str>,
    existing_stop_px: f64,
    existing_stop_oid: Option<&str>,
    existing_tp_px: f64,
    existing_tp_oid: Option<&str>,
    incoming_stop_px: f64,
    incoming_tp_px: f64,
) -> (f64, Option<String>, f64, Option<String>) {
    let side_matches = existing_side.is_some_and(|s| s.eq_ignore_ascii_case(side.as_str()));

    let eff_stop = if side_matches && existing_stop_px.is_finite() && existing_stop_px > 0.0 {
        existing_stop_px
    } else if incoming_stop_px.is_finite() && incoming_stop_px > 0.0 {
        incoming_stop_px
    } else {
        0.0
    };

    let eff_sl_oid = if side_matches && existing_stop_px.is_finite() && existing_stop_px > 0.0 {
        existing_stop_oid.map(str::to_string)
    } else {
        None
    };

    let eff_tp = if side_matches && existing_tp_px.is_finite() && existing_tp_px > 0.0 {
        existing_tp_px
    } else if incoming_tp_px.is_finite() && incoming_tp_px > 0.0 {
        incoming_tp_px
    } else {
        derive_take_profit_px(side, entry_price, eff_stop)
    };

    let eff_tp_oid = if side_matches && existing_tp_px.is_finite() && existing_tp_px > 0.0 {
        existing_tp_oid.map(str::to_string)
    } else {
        None
    };

    (eff_stop, eff_sl_oid, eff_tp, eff_tp_oid)
}

/// Aggregates multi-IOC closing fill slices belonging to the same `(asset_id, side)` close epoch
/// into a single merged trade while preserving distinct reopen+reclose rounds (`P1-Acct`).
pub fn aggregate_close_epoch_trades(
    trades: &[ReconciledClosedTradeInput],
) -> Vec<MergedClosedTrade> {
    if trades.is_empty() {
        return Vec::new();
    }
    let mut sorted: Vec<&ReconciledClosedTradeInput> = trades.iter().collect();
    sorted.sort_by(|a, b| {
        (
            a.asset_id,
            a.side.as_str(),
            a.closed_at_ms,
            a.created_at_ms,
            a.client_order_id.as_str(),
        )
            .cmp(&(
                b.asset_id,
                b.side.as_str(),
                b.closed_at_ms,
                b.created_at_ms,
                b.client_order_id.as_str(),
            ))
    });

    struct Accumulator {
        asset_id: i64,
        side: OrderSide,
        close_epoch_id: Option<String>,
        qty_sum: f64,
        entry_notional_sum: f64,
        exit_notional_sum: f64,
        last_entry_px: f64,
        last_exit_px: f64,
        rpnl_sum: f64,
        fees_sum: f64,
        funding_sum: f64,
        created_at_ms: i64,
        closed_at_ms: i64,
        primary_coid: String,
        primary_eoid: String,
        exit_reason: String,
        merged_coids: Vec<String>,
        merged_eoids: Vec<String>,
    }

    impl Accumulator {
        fn from_trade(t: &ReconciledClosedTradeInput) -> Self {
            let q = if t.filled_quantity.is_finite() && t.filled_quantity > 0.0 {
                t.filled_quantity
            } else {
                0.0
            };
            let entry = if t.entry_price.is_finite() {
                t.entry_price
            } else {
                0.0
            };
            let exit = if t.exit_price.is_finite() {
                t.exit_price
            } else {
                0.0
            };
            let mut merged_coids = Vec::new();
            let coid_t = t.client_order_id.trim();
            if !coid_t.is_empty() {
                merged_coids.push(coid_t.to_string());
            }
            let mut merged_eoids = Vec::new();
            let eoid_t = t.exchange_order_id.trim();
            if !eoid_t.is_empty() {
                merged_eoids.push(eoid_t.to_string());
            }
            Self {
                asset_id: t.asset_id,
                side: t.side,
                close_epoch_id: t.close_epoch_id.clone(),
                qty_sum: q,
                entry_notional_sum: entry * q,
                exit_notional_sum: exit * q,
                last_entry_px: entry,
                last_exit_px: exit,
                rpnl_sum: t.realized_pnl_usdc,
                fees_sum: t.fees_paid_usdc,
                funding_sum: t.funding_accrued_usdc,
                created_at_ms: t.created_at_ms,
                closed_at_ms: t.closed_at_ms,
                primary_coid: t.client_order_id.clone(),
                primary_eoid: t.exchange_order_id.clone(),
                exit_reason: t.exit_reason.clone(),
                merged_coids,
                merged_eoids,
            }
        }

        fn avg_entry(&self) -> f64 {
            if self.qty_sum > 1e-12 {
                self.entry_notional_sum / self.qty_sum
            } else {
                self.last_entry_px
            }
        }

        fn should_merge(&self, next: &ReconciledClosedTradeInput) -> bool {
            if self.asset_id != next.asset_id || self.side != next.side {
                return false;
            }
            if let (Some(ref ea), Some(ref eb)) = (&self.close_epoch_id, &next.close_epoch_id) {
                return ea == eb;
            }
            if (next.closed_at_ms - self.closed_at_ms).abs() > 30_000 {
                return false;
            }
            let same_open_epoch = next.created_at_ms < self.closed_at_ms;
            let open_time_unavailable = (next.closed_at_ms - next.created_at_ms).abs() <= 1_000;
            let cur_entry = self.avg_entry();
            let entry_matches =
                (cur_entry - next.entry_price).abs() / cur_entry.max(1e-9) <= 0.0005;
            same_open_epoch || (open_time_unavailable && entry_matches)
        }

        fn merge(&mut self, next: &ReconciledClosedTradeInput) {
            let q = if next.filled_quantity.is_finite() && next.filled_quantity > 0.0 {
                next.filled_quantity
            } else {
                0.0
            };
            let entry = if next.entry_price.is_finite() {
                next.entry_price
            } else {
                0.0
            };
            let exit = if next.exit_price.is_finite() {
                next.exit_price
            } else {
                0.0
            };
            self.qty_sum += q;
            self.entry_notional_sum += entry * q;
            self.exit_notional_sum += exit * q;
            if entry > 0.0 {
                self.last_entry_px = entry;
            }
            if exit > 0.0 {
                self.last_exit_px = exit;
            }
            self.rpnl_sum += next.realized_pnl_usdc;
            self.fees_sum += next.fees_paid_usdc;
            self.funding_sum += next.funding_accrued_usdc;
            if next.created_at_ms > 0
                && (self.created_at_ms <= 0 || next.created_at_ms < self.created_at_ms)
            {
                self.created_at_ms = next.created_at_ms;
            }
            if next.closed_at_ms > self.closed_at_ms {
                self.closed_at_ms = next.closed_at_ms;
            }
            self.primary_coid = next.client_order_id.clone();
            if !next.exchange_order_id.trim().is_empty() {
                self.primary_eoid = next.exchange_order_id.clone();
            }
            if !next.exit_reason.trim().is_empty() {
                self.exit_reason = next.exit_reason.clone();
            }
            let coid_t = next.client_order_id.trim();
            if !coid_t.is_empty() && !self.merged_coids.iter().any(|c| c == coid_t) {
                self.merged_coids.push(coid_t.to_string());
            }
            let eoid_t = next.exchange_order_id.trim();
            if !eoid_t.is_empty() && !self.merged_eoids.iter().any(|e| e == eoid_t) {
                self.merged_eoids.push(eoid_t.to_string());
            }
        }

        fn finish(self) -> MergedClosedTrade {
            let entry_price = self.avg_entry();
            let exit_price = if self.qty_sum > 1e-12 {
                self.exit_notional_sum / self.qty_sum
            } else {
                self.last_exit_px
            };
            MergedClosedTrade {
                asset_id: self.asset_id,
                side: self.side,
                client_order_id: self.primary_coid,
                exchange_order_id: self.primary_eoid,
                close_epoch_id: self.close_epoch_id,
                entry_price,
                exit_price,
                filled_quantity: self.qty_sum,
                realized_pnl_usdc: self.rpnl_sum,
                fees_paid_usdc: self.fees_sum,
                funding_accrued_usdc: self.funding_sum,
                created_at_ms: self.created_at_ms,
                closed_at_ms: self.closed_at_ms,
                exit_reason: self.exit_reason,
                merged_coids: self.merged_coids,
                merged_eoids: self.merged_eoids,
            }
        }
    }

    let mut groups: Vec<Accumulator> = Vec::with_capacity(sorted.len());
    for t in sorted {
        if let Some(last) = groups.last_mut() {
            if last.should_merge(t) {
                last.merge(t);
                continue;
            }
        }
        groups.push(Accumulator::from_trade(t));
    }

    groups.into_iter().map(Accumulator::finish).collect()
}

fn oid_update_to_parts(upd: OidUpdate<'_>) -> Result<(i32, Option<String>)> {
    match upd {
        OidUpdate::Keep => Ok((0, None)),
        OidUpdate::Set(oid) => {
            let trimmed = oid.trim();
            if trimmed.is_empty() {
                return Err(AegisError::Validation(
                    "OidUpdate::Set cannot contain an empty OID; use OidUpdate::Clear or OidUpdate::Keep"
                        .to_string(),
                ));
            }
            Ok((1, Some(trimmed.to_string())))
        }
        OidUpdate::Clear => Ok((2, None)),
    }
}

impl DatabaseManager {
    /// Places a new `WORKING` order into `positions`, reserving `reserved_margin_usdc` without spending cash yet,
    /// and writes the initial transition into `order_audit`.
    pub fn create_working_order(&self, input: &WorkingOrderInput) -> Result<i64> {
        let ts = now_ms();
        let take_profit_px = input
            .take_profit_px
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or_else(|| {
                derive_take_profit_px(input.side, input.limit_price, input.protective_stop_px)
            });

        let px_dec = decimal_param(input.limit_price, 8, DEC_20_8_MAX_ABS)?;
        let qty_dec = decimal_param(input.requested_quantity, 8, DEC_20_8_MAX_ABS)?;
        let res_dec = decimal_param(round_dp(input.reserved_margin_usdc, 4), 8, DEC_20_8_MAX_ABS)?;
        let pw_dec = decimal_param(input.portfolio_weight, 8, DEC_20_8_MAX_ABS)?;
        let s1_dec = decimal_param(input.s1_weight_contrib, 8, DEC_20_8_MAX_ABS)?;
        let s2_dec = decimal_param(input.s2_weight_contrib, 8, DEC_20_8_MAX_ABS)?;
        let s3_dec = decimal_param(input.s3_weight_contrib, 8, DEC_20_8_MAX_ABS)?;
        let stop_dec = decimal_param(input.protective_stop_px, 8, DEC_20_8_MAX_ABS)?;
        let entry_r0 = derive_entry_r0_from_stop(input.limit_price, input.protective_stop_px);
        let entry_r0_dec = decimal_param(entry_r0, 8, DEC_20_8_MAX_ABS)?;
        let tp_dec = decimal_param(take_profit_px, 8, DEC_20_8_MAX_ABS)?;

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start working order tx: {e}")))?;

        tx.exec_drop(
            r#"INSERT INTO positions (
                   asset_id, client_order_id, side, status, fsm_state, entry_price, current_price,
                   requested_quantity, filled_quantity, reserved_margin_usdc, committed_notional_usdc,
                   portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                   protective_stop_px, entry_r0, take_profit_px, highest_price_seen, lowest_price_seen, chase_count,
                   exchange_order_id, created_at_ms, updated_at_ms
               ) VALUES (
                   :aid, :coid, :side, 'WORKING', 'WORKING_ALO', :px, :px,
                   :qty, 0.00000000, :res_usdc, 0.00000000,
                   :pw, :s1, :s2, :s3,
                   :stop_px, :entry_r0, :tp_px, :px, :px, 0,
                   :eoid, :ts, :ts
               )"#,
            params! {
                "aid" => input.asset_id,
                "coid" => &input.client_order_id,
                "side" => input.side.as_str(),
                "px" => &px_dec,
                "qty" => &qty_dec,
                "res_usdc" => &res_dec,
                "pw" => &pw_dec,
                "s1" => &s1_dec,
                "s2" => &s2_dec,
                "s3" => &s3_dec,
                "stop_px" => &stop_dec,
                "entry_r0" => &entry_r0_dec,
                "tp_px" => &tp_dec,
                "eoid" => &input.exchange_order_id,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("create_working_order insert failed: {e}")))?;

        let pos_id = tx.last_insert_id().unwrap_or(0) as i64;
        let safe_reason = truncate_audit_reason(&input.reason);

        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, 'NEW', 'WORKING_ALO',
                   :qty, 0.00000000, :px, 0.00000000,
                   :res_usdc, 0.00000000, 0.00000000, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => pos_id,
                "aid" => input.asset_id,
                "coid" => &input.client_order_id,
                "side" => input.side.as_str(),
                "qty" => &qty_dec,
                "px" => &px_dec,
                "res_usdc" => &res_dec,
                "reason" => &safe_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("create_working_order audit failed: {e}")))?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!("create_working_order commit failed: {e}"))
        })?;
        Ok(pos_id)
    }

    /// Transitions a `WORKING` order to `OPEN` once the exchange confirms fill,
    /// releasing `reserved_margin_usdc` (`0.0`) and committing `committed_notional_usdc = fill_price * filled_qty`.
    #[allow(clippy::too_many_arguments)]
    pub fn promote_working_to_open(
        &self,
        position_id: i64,
        fill_price: f64,
        filled_quantity: f64,
        fees_paid_usdc: f64,
        protective_stop_px: f64,
        take_profit_px: Option<f64>,
        exchange_order_id: Option<&str>,
        reason: &str,
    ) -> Result<()> {
        if !fill_price.is_finite() || fill_price <= 0.0 {
            return Err(AegisError::Validation(format!(
                "promote_working_to_open requires positive fill_price, got {fill_price}"
            )));
        }
        if !filled_quantity.is_finite() || filled_quantity <= 0.0 {
            return Err(AegisError::Validation(format!(
                "promote_working_to_open requires positive filled_quantity, got {filled_quantity}"
            )));
        }
        let ts = now_ms();
        let committed_usdc = round_dp(fill_price * filled_quantity, 4);
        let fees_rounded = round_dp(fees_paid_usdc, 4);

        let px_dec = decimal_param(fill_price, 8, DEC_20_8_MAX_ABS)?;
        let qty_dec = decimal_param(filled_quantity, 8, DEC_20_8_MAX_ABS)?;
        let comm_dec = decimal_param(committed_usdc, 8, DEC_20_8_MAX_ABS)?;
        let fees_dec = decimal_param(fees_rounded, 8, DEC_20_8_MAX_ABS)?;

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("promote_working_to_open tx failed: {e}")))?;

        let row: Option<mysql::Row> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price,
                          requested_quantity, protective_stop_px, take_profit_px,
                          portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("promote_working_to_open lock failed: {e}"))
            })?;

        let locked =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let asset_id = row_get_i64(&locked, 0, 0);
        let client_order_id = row_get_string(&locked, 1, "");
        let side_str = row_get_string(&locked, 2, "LONG");
        let status = row_get_string(&locked, 3, "");
        let fsm_state = row_get_string(&locked, 4, "");
        let limit_px = row_get_f64(&locked, 5, 0.0);
        let requested_qty = row_get_f64(&locked, 6, 0.0);
        let existing_stop_px = row_get_f64(&locked, 7, 0.0);
        let existing_tp_px = row_get_f64(&locked, 8, 0.0);
        let old_pw = row_get_f64(&locked, 9, 0.0);
        let old_s1 = row_get_f64(&locked, 10, 0.0);
        let old_s2 = row_get_f64(&locked, 11, 0.0);
        let old_s3 = row_get_f64(&locked, 12, 0.0);

        if !status.eq_ignore_ascii_case("WORKING") {
            return Err(AegisError::Database(format!(
                "Cannot promote position {position_id}: status is '{status}' (expected 'WORKING')"
            )));
        }

        let side = OrderSide::parse(&side_str)?;
        let eff_stop_px = if protective_stop_px.is_finite() && protective_stop_px > 0.0 {
            protective_stop_px
        } else if existing_stop_px.is_finite() && existing_stop_px > 0.0 {
            existing_stop_px
        } else {
            0.0
        };
        let entry_r0 = if eff_stop_px > 0.0 {
            round_dp((fill_price - eff_stop_px).abs().max(0.0), 8)
        } else {
            0.0
        };
        let stop_dec = decimal_param(eff_stop_px, 8, DEC_20_8_MAX_ABS)?;
        let entry_r0_dec = decimal_param(entry_r0, 8, DEC_20_8_MAX_ABS)?;
        let eff_tp = take_profit_px
            .filter(|v| v.is_finite() && *v > 0.0)
            .or_else(|| {
                (existing_tp_px.is_finite() && existing_tp_px > 0.0).then_some(existing_tp_px)
            })
            .unwrap_or_else(|| derive_take_profit_px(side, fill_price, eff_stop_px));
        let tp_dec = decimal_param(eff_tp, 8, DEC_20_8_MAX_ABS)?;
        let req_qty_dec = decimal_param(requested_qty, 8, DEC_20_8_MAX_ABS)?;
        let lim_px_dec = decimal_param(limit_px, 8, DEC_20_8_MAX_ABS)?;
        let (new_pw, new_s1, new_s2, new_s3) =
            if requested_qty > 1e-12 && filled_quantity < requested_qty {
                let fill_ratio = (filled_quantity / requested_qty).clamp(0.0, 1.0);
                (
                    round_dp(old_pw * fill_ratio, 4),
                    round_dp(old_s1 * fill_ratio, 4),
                    round_dp(old_s2 * fill_ratio, 4),
                    round_dp(old_s3 * fill_ratio, 4),
                )
            } else {
                (old_pw, old_s1, old_s2, old_s3)
            };
        let pw_dec = decimal_param(new_pw, 8, DEC_20_8_MAX_ABS)?;
        let s1_dec = decimal_param(new_s1, 8, DEC_20_8_MAX_ABS)?;
        let s2_dec = decimal_param(new_s2, 8, DEC_20_8_MAX_ABS)?;
        let s3_dec = decimal_param(new_s3, 8, DEC_20_8_MAX_ABS)?;

        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'OPEN',
                   fsm_state = 'FILLED_OPEN',
                   entry_price = :px,
                   current_price = :px,
                   filled_quantity = :qty,
                   reserved_margin_usdc = 0.00000000,
                   committed_notional_usdc = :comm_usdc,
                   portfolio_weight = :pw,
                   s1_weight_contrib = :s1,
                   s2_weight_contrib = :s2,
                   s3_weight_contrib = :s3,
                   protective_stop_px = :stop_px,
                   entry_r0 = :entry_r0,
                   take_profit_px = :tp_px,
                   highest_price_seen = :px,
                   lowest_price_seen = :px,
                   fees_paid_usdc = fees_paid_usdc + CAST(:fees AS DECIMAL(20,8)),
                   exchange_order_id = COALESCE(:eoid, exchange_order_id),
                   opened_at_ms = COALESCE(opened_at_ms, :ts),
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'WORKING'"#,
            params! {
                "px" => &px_dec,
                "qty" => &qty_dec,
                "comm_usdc" => &comm_dec,
                "pw" => &pw_dec,
                "s1" => &s1_dec,
                "s2" => &s2_dec,
                "s3" => &s3_dec,
                "stop_px" => &stop_dec,
                "entry_r0" => &entry_r0_dec,
                "tp_px" => &tp_dec,
                "fees" => &fees_dec,
                "eoid" => exchange_order_id,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| AegisError::Database(format!("promote_working_to_open update failed: {e}")))?;

        if tx.affected_rows() == 0 {
            return Err(AegisError::Database(format!(
                "promote_working_to_open affected 0 rows for position {position_id}"
            )));
        }

        let safe_reason = truncate_audit_reason(reason);
        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, :from_st, 'FILLED_OPEN',
                   :req_qty, :fill_qty, :lim_px, :fill_px,
                   0.00000000, :comm_usdc, :fees, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "req_qty" => &req_qty_dec,
                "fill_qty" => &qty_dec,
                "lim_px" => &lim_px_dec,
                "fill_px" => &px_dec,
                "comm_usdc" => &comm_dec,
                "fees" => &fees_dec,
                "reason" => &safe_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("promote_working_to_open audit failed: {e}")))?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!("promote_working_to_open commit failed: {e}"))
        })?;
        self.invalidate_pnl_cache();
        Ok(())
    }

    /// Updates live mark price, high/low watermarks, unrealized PnL, and trailing protective stop on an `OPEN` holding
    /// atomically under `SELECT ... FOR UPDATE` with side-aware monotonic `GREATEST`/`LEAST` stop enforcement (`R14-M18`).
    pub fn update_open_position_mark_and_stop(
        &self,
        position_id: i64,
        mark_price: f64,
        new_protective_stop_px: f64,
    ) -> Result<()> {
        if !mark_price.is_finite() || mark_price <= 0.0 {
            return Err(AegisError::Validation(format!(
                "update_open_position_mark_and_stop requires positive finite mark_price, got {mark_price}"
            )));
        }
        if !new_protective_stop_px.is_finite() || new_protective_stop_px < 0.0 {
            return Err(AegisError::Validation(format!(
                "update_open_position_mark_and_stop requires non-negative finite stop, got {new_protective_stop_px}"
            )));
        }

        let mark_dec = decimal_param(mark_price, 8, DEC_20_8_MAX_ABS)?;
        let stop_dec = decimal_param(new_protective_stop_px, 8, DEC_20_8_MAX_ABS)?;
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("update_open_position_mark_and_stop tx failed: {e}"))
            })?;

        let row: Option<mysql::Row> = tx
            .exec_first(
                r#"SELECT side, status, entry_price, filled_quantity, take_profit_px
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "update_open_position_mark_and_stop lock failed: {e}"
                ))
            })?;

        let locked =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let side_str = row_get_string(&locked, 0, "LONG");
        let status = row_get_string(&locked, 1, "");
        if !status.eq_ignore_ascii_case("OPEN") {
            tx.commit().map_err(|e| {
                AegisError::Database(format!(
                    "update_open_position_mark_and_stop commit failed: {e}"
                ))
            })?;
            return Ok(());
        }
        let entry_price = row_get_f64(&locked, 2, 0.0);
        let filled_quantity = row_get_f64(&locked, 3, 0.0);
        let cur_tp = row_get_f64(&locked, 4, 0.0);

        let side = OrderSide::parse(&side_str)?;
        let qty = filled_quantity.max(0.0);
        let upnl = round_dp((mark_price - entry_price) * qty * side.sign(), 4);
        let eff_tp = if cur_tp > 0.0 {
            cur_tp
        } else {
            derive_take_profit_px(side, entry_price, new_protective_stop_px)
        };

        let upnl_dec = decimal_param(upnl, 8, DEC_20_8_MAX_ABS)?;
        let tp_dec = decimal_param(eff_tp, 8, DEC_20_8_MAX_ABS)?;

        tx.exec_drop(
            r#"UPDATE positions
               SET current_price = :mark,
                   protective_stop_px = CASE
                       WHEN CAST(:stop_px AS DECIMAL(20,8)) <= 0 THEN protective_stop_px
                       WHEN protective_stop_px <= 0 THEN CAST(:stop_px AS DECIMAL(20,8))
                       WHEN side = 'LONG'  THEN GREATEST(protective_stop_px, CAST(:stop_px AS DECIMAL(20,8)))
                       WHEN side = 'SHORT' THEN LEAST(protective_stop_px, CAST(:stop_px AS DECIMAL(20,8)))
                       ELSE protective_stop_px
                   END,
                   take_profit_px = CASE
                       WHEN take_profit_px > 0 THEN take_profit_px
                       WHEN CAST(:tp_px AS DECIMAL(20,8)) > 0 THEN CAST(:tp_px AS DECIMAL(20,8))
                       ELSE take_profit_px
                   END,
                   highest_price_seen = GREATEST(highest_price_seen, CAST(:mark AS DECIMAL(20,8))),
                   lowest_price_seen = CASE
                       WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, CAST(:mark AS DECIMAL(20,8)))
                       ELSE CAST(:mark AS DECIMAL(20,8))
                   END,
                   unrealized_pnl_usdc = :upnl,
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'OPEN'"#,
            params! {
                "mark" => &mark_dec,
                "stop_px" => &stop_dec,
                "tp_px" => &tp_dec,
                "upnl" => &upnl_dec,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("update_open_position_mark_and_stop failed: {e}"))
        })?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!(
                "update_open_position_mark_and_stop commit failed: {e}"
            ))
        })?;
        self.invalidate_pnl_cache();
        Ok(())
    }

    /// Batch updates live mark price, watermarks, unrealized PnL, and monotonic trailing stops
    /// on changed `OPEN` holdings under canonical `ORDER BY id ASC FOR UPDATE` locking (`R14-M18`).
    pub fn update_open_positions_mark_and_stop_batch(
        &self,
        updates: &[(i64, f64, f64)],
    ) -> Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }
        let mut canonical: BTreeMap<i64, (f64, f64)> = BTreeMap::new();
        for &(pid, mark_price, new_stop_px) in updates {
            if !mark_price.is_finite() || mark_price <= 0.0 {
                return Err(AegisError::Validation(format!(
                    "update_open_positions_mark_and_stop_batch requires positive finite mark_price for {pid}, got {mark_price}"
                )));
            }
            if !new_stop_px.is_finite() || new_stop_px < 0.0 {
                return Err(AegisError::Validation(format!(
                    "update_open_positions_mark_and_stop_batch requires non-negative finite stop for {pid}, got {new_stop_px}"
                )));
            }
            canonical.insert(pid, (mark_price, new_stop_px));
        }

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("open position batch tx failed: {e}")))?;

        let rows: Vec<mysql::Row> = tx
            .query(
                r#"SELECT id, side, entry_price, current_price, filled_quantity,
                          protective_stop_px, take_profit_px, highest_price_seen,
                          lowest_price_seen, unrealized_pnl_usdc
                   FROM positions
                   WHERE status = 'OPEN'
                   ORDER BY id ASC
                   FOR UPDATE"#,
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "update_open_positions_mark_and_stop_batch select failed: {e}"
                ))
            })?;

        struct SnapshotRow {
            side: String,
            entry_px: f64,
            cur_px: f64,
            qty: f64,
            cur_stop: f64,
            cur_tp: f64,
            hi_seen: f64,
            lo_seen: f64,
            cur_upnl: f64,
        }

        let by_id: BTreeMap<i64, SnapshotRow> = rows
            .iter()
            .map(|r| {
                (
                    row_get_i64(r, 0, 0),
                    SnapshotRow {
                        side: row_get_string(r, 1, "LONG"),
                        entry_px: row_get_f64(r, 2, 0.0),
                        cur_px: row_get_f64(r, 3, 0.0),
                        qty: row_get_f64(r, 4, 0.0),
                        cur_stop: row_get_f64(r, 5, 0.0),
                        cur_tp: row_get_f64(r, 6, 0.0),
                        hi_seen: row_get_f64(r, 7, 0.0),
                        lo_seen: row_get_f64(r, 8, 0.0),
                        cur_upnl: row_get_f64(r, 9, 0.0),
                    },
                )
            })
            .collect();

        struct BatchUpdate {
            pid: i64,
            mark_dec: String,
            stop_dec: String,
            tp_dec: String,
            upnl_dec: String,
        }

        let mut changed = Vec::new();
        for (pid, (mark_price, new_stop_px)) in canonical {
            let Some(s) = by_id.get(&pid) else {
                continue;
            };
            let side = OrderSide::parse(&s.side)?;
            let eff_qty = s.qty.max(0.0);
            let upnl = round_dp((mark_price - s.entry_px) * eff_qty * side.sign(), 4);
            let high = s.hi_seen.max(mark_price);
            let low = if s.lo_seen > 0.0 {
                s.lo_seen.min(mark_price)
            } else {
                mark_price
            };
            let eff_stop = if new_stop_px <= 0.0 {
                s.cur_stop
            } else if s.cur_stop <= 0.0 {
                new_stop_px
            } else {
                match side {
                    OrderSide::Long => s.cur_stop.max(new_stop_px),
                    OrderSide::Short => s.cur_stop.min(new_stop_px),
                }
            };
            let eff_tp = if s.cur_tp > 0.0 {
                s.cur_tp
            } else {
                derive_take_profit_px(side, s.entry_px, eff_stop)
            };

            let unchanged = (s.cur_px - mark_price).abs() <= 1e-10
                && (s.cur_stop - eff_stop).abs() <= 1e-10
                && (s.cur_tp - eff_tp).abs() <= 1e-10
                && (s.hi_seen - high).abs() <= 1e-10
                && (s.lo_seen - low).abs() <= 1e-10
                && (s.cur_upnl - upnl).abs() <= 1e-10;
            if !unchanged {
                changed.push(BatchUpdate {
                    pid,
                    mark_dec: decimal_param(mark_price, 8, DEC_20_8_MAX_ABS)?,
                    stop_dec: decimal_param(eff_stop, 8, DEC_20_8_MAX_ABS)?,
                    tp_dec: decimal_param(eff_tp, 8, DEC_20_8_MAX_ABS)?,
                    upnl_dec: decimal_param(upnl, 8, DEC_20_8_MAX_ABS)?,
                });
            }
        }

        if changed.is_empty() {
            tx.commit().map_err(|e| {
                AegisError::Database(format!("open position batch empty commit failed: {e}"))
            })?;
            return Ok(0);
        }

        let stmt = tx
            .prep(
                r#"UPDATE positions
                   SET current_price = :mark,
                       protective_stop_px = CASE
                           WHEN CAST(:stop_px AS DECIMAL(20,8)) <= 0 THEN protective_stop_px
                           WHEN protective_stop_px <= 0 THEN CAST(:stop_px AS DECIMAL(20,8))
                           WHEN side = 'LONG'  THEN GREATEST(protective_stop_px, CAST(:stop_px AS DECIMAL(20,8)))
                           WHEN side = 'SHORT' THEN LEAST(protective_stop_px, CAST(:stop_px AS DECIMAL(20,8)))
                           ELSE protective_stop_px
                       END,
                       take_profit_px = CASE
                           WHEN take_profit_px > 0 THEN take_profit_px
                           WHEN CAST(:tp_px AS DECIMAL(20,8)) > 0 THEN CAST(:tp_px AS DECIMAL(20,8))
                           ELSE take_profit_px
                       END,
                       highest_price_seen = GREATEST(highest_price_seen, CAST(:mark AS DECIMAL(20,8))),
                       lowest_price_seen = CASE
                           WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, CAST(:mark AS DECIMAL(20,8)))
                           ELSE CAST(:mark AS DECIMAL(20,8))
                       END,
                       unrealized_pnl_usdc = :upnl,
                       updated_at_ms = :ts
                   WHERE id = :pid AND status = 'OPEN'"#,
            )
            .map_err(|e| AegisError::Database(format!("open position batch prep failed: {e}")))?;

        tx.exec_batch(
            &stmt,
            changed.iter().map(|u| {
                params! {
                    "mark" => &u.mark_dec,
                    "stop_px" => &u.stop_dec,
                    "tp_px" => &u.tp_dec,
                    "upnl" => &u.upnl_dec,
                    "ts" => ts,
                    "pid" => u.pid,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("open position batch exec failed: {e}")))?;
        drop(stmt);

        tx.commit()
            .map_err(|e| AegisError::Database(format!("open position batch commit failed: {e}")))?;
        self.invalidate_pnl_cache();
        Ok(changed.len())
    }

    /// Audited escape hatch to override an `OPEN` position's protective stop when an operator
    /// explicitly resets a stop (`R14-M18`). Rejects empty reasons and already-breached stops.
    pub fn force_reset_open_position_stop(
        &self,
        position_id: i64,
        new_stop_px: f64,
        reason: &str,
    ) -> Result<()> {
        if reason.trim().is_empty() {
            return Err(AegisError::Validation(
                "force_reset_open_position_stop requires a non-empty audit reason".to_string(),
            ));
        }
        if !new_stop_px.is_finite() || new_stop_px <= 0.0 {
            return Err(AegisError::Validation(format!(
                "force_reset_open_position_stop requires positive finite stop, got {new_stop_px}"
            )));
        }
        let stop_dec = decimal_param(new_stop_px, 8, DEC_20_8_MAX_ABS)?;
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("force_reset_open_position_stop tx failed: {e}"))
            })?;

        let row: Option<mysql::Row> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price,
                          current_price, requested_quantity, filled_quantity, committed_notional_usdc
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("force_reset_open_position_stop lock failed: {e}"))
            })?;

        let locked =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let asset_id = row_get_i64(&locked, 0, 0);
        let client_order_id = row_get_string(&locked, 1, "");
        let side_str = row_get_string(&locked, 2, "LONG");
        let status = row_get_string(&locked, 3, "");
        let fsm_state = row_get_string(&locked, 4, "FILLED_OPEN");
        let entry_price = row_get_f64(&locked, 5, 0.0);
        let current_price = row_get_f64(&locked, 6, 0.0);
        let requested_qty = row_get_f64(&locked, 7, 0.0);
        let filled_qty = row_get_f64(&locked, 8, 0.0);
        let committed_usdc = row_get_f64(&locked, 9, 0.0);

        if !status.eq_ignore_ascii_case("OPEN") {
            return Err(AegisError::Database(format!(
                "force_reset_open_position_stop requires status='OPEN', got '{status}'"
            )));
        }
        let side = OrderSide::parse(&side_str)?;
        if current_price > 0.0 {
            let valid = match side {
                OrderSide::Long => new_stop_px < current_price,
                OrderSide::Short => new_stop_px > current_price,
            };
            if !valid {
                return Err(AegisError::Validation(format!(
                    "force_reset_open_position_stop rejected: new_stop_px {new_stop_px} is already breached vs current_price {current_price} for {side:?}"
                )));
            }
        }

        tx.exec_drop(
            r#"UPDATE positions
               SET protective_stop_px = :stop_px,
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'OPEN'"#,
            params! {
                "stop_px" => &stop_dec,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("force_reset_open_position_stop update failed: {e}"))
        })?;

        let safe_reason = truncate_audit_reason(reason);
        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, :from_st, 'STOP_FORCE_RESET',
                   :req_qty, :fill_qty, :stop_px, :entry_px,
                   0.00000000, :comm_usdc, 0.00000000, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "req_qty" => decimal_param(requested_qty, 8, DEC_20_8_MAX_ABS)?,
                "fill_qty" => decimal_param(filled_qty, 8, DEC_20_8_MAX_ABS)?,
                "stop_px" => &stop_dec,
                "entry_px" => decimal_param(entry_price, 8, DEC_20_8_MAX_ABS)?,
                "comm_usdc" => decimal_param(committed_usdc, 8, DEC_20_8_MAX_ABS)?,
                "reason" => &safe_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("force_reset_open_position_stop audit failed: {e}"))
        })?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!("force_reset_open_position_stop commit failed: {e}"))
        })?;
        Ok(())
    }

    /// Persists stop-loss and take-profit trigger prices and exchange order IDs on an active position
    /// using tri-state [`OidUpdate`] (`Keep`/`Set`/`Clear`) and side-aware monotonic stop protection on `OPEN`
    /// positions (`R14-M26`, `#18` Loophole #1).
    pub fn update_position_tpsl_orders(
        &self,
        position_id: i64,
        protective_stop_px: f64,
        protective_stop_oid: OidUpdate<'_>,
        take_profit_px: f64,
        take_profit_oid: OidUpdate<'_>,
    ) -> Result<()> {
        if !protective_stop_px.is_finite() || protective_stop_px < 0.0 {
            return Err(AegisError::Validation(format!(
                "update_position_tpsl_orders requires non-negative finite protective_stop_px, got {protective_stop_px}"
            )));
        }
        if !take_profit_px.is_finite() || take_profit_px < 0.0 {
            return Err(AegisError::Validation(format!(
                "update_position_tpsl_orders requires non-negative finite take_profit_px, got {take_profit_px}"
            )));
        }
        let stop_dec = decimal_param(protective_stop_px, 8, DEC_20_8_MAX_ABS)?;
        let tp_dec = decimal_param(take_profit_px, 8, DEC_20_8_MAX_ABS)?;
        let (sl_mode, sl_oid) = oid_update_to_parts(protective_stop_oid)?;
        let (tp_mode, tp_oid) = oid_update_to_parts(take_profit_oid)?;

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"UPDATE positions
               SET protective_stop_px = CASE
                       WHEN CAST(:stop_px AS DECIMAL(20,8)) <= 0 THEN protective_stop_px
                       WHEN status = 'WORKING' OR protective_stop_px <= 0 THEN CAST(:stop_px AS DECIMAL(20,8))
                       WHEN status = 'OPEN' AND side = 'LONG'  THEN GREATEST(protective_stop_px, CAST(:stop_px AS DECIMAL(20,8)))
                       WHEN status = 'OPEN' AND side = 'SHORT' THEN LEAST(protective_stop_px, CAST(:stop_px AS DECIMAL(20,8)))
                       ELSE protective_stop_px
                   END,
                   protective_stop_oid = CASE :sl_mode
                       WHEN 0 THEN protective_stop_oid
                       WHEN 1 THEN :sl_oid
                       WHEN 2 THEN NULL
                       ELSE protective_stop_oid
                   END,
                   take_profit_px = CASE
                       WHEN CAST(:tp_px AS DECIMAL(20,8)) > 0 THEN CAST(:tp_px AS DECIMAL(20,8))
                       ELSE take_profit_px
                   END,
                   take_profit_oid = CASE :tp_mode
                       WHEN 0 THEN take_profit_oid
                       WHEN 1 THEN :tp_oid
                       WHEN 2 THEN NULL
                       ELSE take_profit_oid
                   END,
                   updated_at_ms = :ts
               WHERE id = :pid AND status IN ('WORKING', 'OPEN')"#,
            params! {
                "stop_px" => &stop_dec,
                "sl_mode" => sl_mode,
                "sl_oid" => sl_oid,
                "tp_px" => &tp_dec,
                "tp_mode" => tp_mode,
                "tp_oid" => tp_oid,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| AegisError::Database(format!("update_position_tpsl_orders failed: {e}")))?;
        Ok(())
    }

    /// Records a partial reduce-only exit on an `OPEN` position atomically under `SELECT ... FOR UPDATE`,
    /// preserving `status = 'OPEN'` and `fsm_state = 'FILLED_OPEN'`, updating remaining quantity and
    /// committed notional, accumulating `realized_pnl_usdc` / `fees_paid_usdc`, and logging `PARTIAL_CLOSE`
    /// in `order_audit` (`R14-M3`).
    pub fn record_partial_position_exit(
        &self,
        position_id: i64,
        closed_qty: f64,
        remaining_qty: f64,
        partial_exit_price: f64,
        partial_fees_usdc: f64,
        reason: &str,
    ) -> Result<f64> {
        if !closed_qty.is_finite() || closed_qty <= 0.0 {
            return Err(AegisError::Validation(format!(
                "record_partial_position_exit requires positive finite closed_qty, got {closed_qty}"
            )));
        }
        if !remaining_qty.is_finite() || remaining_qty <= 0.0 {
            return Err(AegisError::Validation(format!(
                "record_partial_position_exit requires positive finite remaining_qty (use close_position for full exits), got {remaining_qty}"
            )));
        }
        if !partial_exit_price.is_finite() || partial_exit_price <= 0.0 {
            return Err(AegisError::Validation(format!(
                "record_partial_position_exit requires positive finite partial_exit_price, got {partial_exit_price}"
            )));
        }
        if !partial_fees_usdc.is_finite() {
            return Err(AegisError::Validation(format!(
                "record_partial_position_exit requires finite partial_fees_usdc, got {partial_fees_usdc}"
            )));
        }

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("record_partial_position_exit tx failed: {e}"))
            })?;

        let row: Option<mysql::Row> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price,
                          filled_quantity, portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("record_partial_position_exit lock failed: {e}"))
            })?;

        let locked =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let asset_id = row_get_i64(&locked, 0, 0);
        let client_order_id = row_get_string(&locked, 1, "");
        let side_str = row_get_string(&locked, 2, "LONG");
        let status = row_get_string(&locked, 3, "");
        let fsm_state = row_get_string(&locked, 4, "FILLED_OPEN");
        let entry_price = row_get_f64(&locked, 5, 0.0);
        let old_filled_qty = row_get_f64(&locked, 6, 0.0);
        let old_pw = row_get_f64(&locked, 7, 0.0);
        let old_s1 = row_get_f64(&locked, 8, 0.0);
        let old_s2 = row_get_f64(&locked, 9, 0.0);
        let old_s3 = row_get_f64(&locked, 10, 0.0);

        if !status.eq_ignore_ascii_case("OPEN") {
            return Err(AegisError::Database(format!(
                "Cannot record partial exit on position {position_id}: status is '{status}' (expected 'OPEN')"
            )));
        }

        let side = OrderSide::parse(&side_str)?;
        let gross_partial = (partial_exit_price - entry_price) * closed_qty * side.sign();
        let partial_fees_rounded = round_dp(partial_fees_usdc, 4);
        let net_partial_pnl = round_dp(gross_partial - partial_fees_rounded, 4);
        let new_committed = round_dp(entry_price * remaining_qty, 4);
        let scale_ratio = if old_filled_qty > 1e-12 {
            (remaining_qty / old_filled_qty).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let new_pw = round_dp(old_pw * scale_ratio, 4);
        let new_s1 = round_dp(old_s1 * scale_ratio, 4);
        let new_s2 = round_dp(old_s2 * scale_ratio, 4);
        let new_s3 = round_dp(old_s3 * scale_ratio, 4);

        let rem_qty_dec = decimal_param(remaining_qty, 8, DEC_20_8_MAX_ABS)?;
        let closed_qty_dec = decimal_param(closed_qty, 8, DEC_20_8_MAX_ABS)?;
        let exit_px_dec = decimal_param(partial_exit_price, 8, DEC_20_8_MAX_ABS)?;
        let comm_dec = decimal_param(new_committed, 8, DEC_20_8_MAX_ABS)?;
        let new_pw_dec = decimal_param(new_pw, 8, DEC_20_8_MAX_ABS)?;
        let new_s1_dec = decimal_param(new_s1, 8, DEC_20_8_MAX_ABS)?;
        let new_s2_dec = decimal_param(new_s2, 8, DEC_20_8_MAX_ABS)?;
        let new_s3_dec = decimal_param(new_s3, 8, DEC_20_8_MAX_ABS)?;
        let partial_pnl_dec = decimal_param(net_partial_pnl, 8, DEC_20_8_MAX_ABS)?;
        let partial_fees_dec = decimal_param(partial_fees_rounded, 8, DEC_20_8_MAX_ABS)?;

        tx.exec_drop(
            r#"UPDATE positions
               SET filled_quantity = :rem_qty,
                   committed_notional_usdc = :comm_usdc,
                   portfolio_weight = :new_pw,
                   s1_weight_contrib = :new_s1,
                   s2_weight_contrib = :new_s2,
                   s3_weight_contrib = :new_s3,
                   realized_pnl_usdc = realized_pnl_usdc + CAST(:partial_pnl AS DECIMAL(20,8)),
                   fees_paid_usdc = fees_paid_usdc + CAST(:partial_fees AS DECIMAL(20,8)),
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'OPEN'"#,
            params! {
                "rem_qty" => &rem_qty_dec,
                "comm_usdc" => &comm_dec,
                "new_pw" => &new_pw_dec,
                "new_s1" => &new_s1_dec,
                "new_s2" => &new_s2_dec,
                "new_s3" => &new_s3_dec,
                "partial_pnl" => &partial_pnl_dec,
                "partial_fees" => &partial_fees_dec,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("record_partial_position_exit update failed: {e}"))
        })?;

        if tx.affected_rows() == 0 {
            return Err(AegisError::Database(format!(
                "record_partial_position_exit affected 0 rows for position {position_id}"
            )));
        }

        let safe_reason = truncate_audit_reason(reason);
        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, :from_st, 'PARTIAL_CLOSE',
                   :closed_qty, :closed_qty, :exit_px, :exit_px,
                   0.00000000, :comm_usdc, :fees, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "closed_qty" => &closed_qty_dec,
                "exit_px" => &exit_px_dec,
                "comm_usdc" => &comm_dec,
                "fees" => &partial_fees_dec,
                "reason" => &safe_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("record_partial_position_exit audit failed: {e}"))
        })?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!("record_partial_position_exit commit failed: {e}"))
        })?;
        self.invalidate_pnl_cache();
        Ok(net_partial_pnl)
    }

    /// Closes an `OPEN` position, recording realized PnL and releasing `committed_notional_usdc`.
    pub fn close_position(
        &self,
        position_id: i64,
        exit_price: f64,
        fees_paid_usdc: f64,
        exit_reason: &str,
    ) -> Result<f64> {
        if !exit_price.is_finite() || exit_price <= 0.0 {
            return Err(AegisError::Validation(format!(
                "close_position requires positive finite exit_price, got {exit_price}"
            )));
        }
        if !fees_paid_usdc.is_finite() {
            return Err(AegisError::Validation(format!(
                "close_position requires finite fees_paid_usdc, got {fees_paid_usdc}"
            )));
        }
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("close_position tx failed: {e}")))?;

        let row: Option<mysql::Row> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price,
                          requested_quantity, filled_quantity, fees_paid_usdc, funding_accrued_usdc,
                          realized_pnl_usdc
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| AegisError::Database(format!("close_position lock failed: {e}")))?;

        let locked =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let asset_id = row_get_i64(&locked, 0, 0);
        let client_order_id = row_get_string(&locked, 1, "");
        let side_str = row_get_string(&locked, 2, "LONG");
        let status = row_get_string(&locked, 3, "");
        let fsm_state = row_get_string(&locked, 4, "FILLED_OPEN");
        let entry_price = row_get_f64(&locked, 5, 0.0);
        let requested_quantity = row_get_f64(&locked, 6, 0.0);
        let filled_quantity = row_get_f64(&locked, 7, 0.0);
        let existing_fees_paid = row_get_f64(&locked, 8, 0.0);
        let funding_accrued_usdc = row_get_f64(&locked, 9, 0.0);
        let existing_realized_pnl = row_get_f64(&locked, 10, 0.0);

        if !status.eq_ignore_ascii_case("OPEN") {
            return Err(AegisError::Database(format!(
                "Cannot close position {position_id}: status is '{status}' (expected 'OPEN')"
            )));
        }

        let side = OrderSide::parse(&side_str)?;
        let qty = filled_quantity.max(0.0);
        let gross_remaining = (exit_price - entry_price) * qty * side.sign();
        let total_fees = existing_fees_paid + fees_paid_usdc;
        // Note: existing_realized_pnl already includes any prior partial-exit net PnL (net of partial fees),
        // while existing_fees_paid also includes prior partial fees -> subtract only incremental fees_paid_usdc
        // when partial exits have already accrued, or preserve exact formula when existing_realized_pnl == 0.
        let net_realized_pnl = if existing_realized_pnl.abs() > 1e-12 {
            round_dp(
                existing_realized_pnl + gross_remaining - fees_paid_usdc + funding_accrued_usdc,
                4,
            )
        } else {
            round_dp(gross_remaining - total_fees + funding_accrued_usdc, 4)
        };

        let safe_exit_reason = truncate_exit_reason(exit_reason);
        let safe_audit_reason = truncate_audit_reason(exit_reason);

        let exit_px_dec = decimal_param(exit_price, 8, DEC_20_8_MAX_ABS)?;
        let rpnl_dec = decimal_param(net_realized_pnl, 8, DEC_20_8_MAX_ABS)?;
        let total_fees_dec = decimal_param(round_dp(total_fees, 4), 8, DEC_20_8_MAX_ABS)?;
        let req_qty_dec = decimal_param(requested_quantity, 8, DEC_20_8_MAX_ABS)?;
        let fill_qty_dec = decimal_param(qty, 8, DEC_20_8_MAX_ABS)?;
        let inc_fees_dec = decimal_param(round_dp(fees_paid_usdc, 4), 8, DEC_20_8_MAX_ABS)?;

        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'CLOSED',
                   fsm_state = 'CLOSED',
                   current_price = :exit_px,
                   exit_price = :exit_px,
                   reserved_margin_usdc = 0.00000000,
                   committed_notional_usdc = 0.00000000,
                   realized_pnl_usdc = :rpnl,
                   unrealized_pnl_usdc = 0.00000000,
                   fees_paid_usdc = :fees,
                   closed_at_ms = :ts,
                   updated_at_ms = :ts,
                   exit_reason = :reason
               WHERE id = :pid AND status = 'OPEN'"#,
            params! {
                "exit_px" => &exit_px_dec,
                "rpnl" => &rpnl_dec,
                "fees" => &total_fees_dec,
                "ts" => ts,
                "reason" => &safe_exit_reason,
                "pid" => position_id,
            },
        )
        .map_err(|e| AegisError::Database(format!("close_position update failed: {e}")))?;

        if tx.affected_rows() == 0 {
            return Err(AegisError::Database(format!(
                "close_position affected 0 rows for position {position_id}"
            )));
        }

        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, :from_st, 'CLOSED',
                   :req_qty, :fill_qty, :exit_px, :exit_px,
                   0.00000000, 0.00000000, :fees, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "req_qty" => &req_qty_dec,
                "fill_qty" => &fill_qty_dec,
                "exit_px" => &exit_px_dec,
                "fees" => &inc_fees_dec,
                "reason" => &safe_audit_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("close_position audit failed: {e}")))?;

        tx.commit()
            .map_err(|e| AegisError::Database(format!("close_position commit failed: {e}")))?;
        self.invalidate_pnl_cache();
        Ok(net_realized_pnl)
    }

    /// Cancels a `WORKING` order and releases its `reserved_margin_usdc` back to available cash.
    pub fn cancel_working_order(&self, position_id: i64, reason: &str) -> Result<()> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("cancel_working_order tx failed: {e}")))?;

        let row: Option<mysql::Row> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price, requested_quantity
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| AegisError::Database(format!("cancel_working_order lock failed: {e}")))?;

        let locked =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let asset_id = row_get_i64(&locked, 0, 0);
        let client_order_id = row_get_string(&locked, 1, "");
        let side_str = row_get_string(&locked, 2, "LONG");
        let status = row_get_string(&locked, 3, "");
        let fsm_state = row_get_string(&locked, 4, "WORKING_ALO");
        let entry_price = row_get_f64(&locked, 5, 0.0);
        let requested_qty = row_get_f64(&locked, 6, 0.0);

        if !status.eq_ignore_ascii_case("WORKING") {
            return Err(AegisError::Database(format!(
                "Cannot cancel working order {position_id}: status is '{status}' (expected 'WORKING')"
            )));
        }

        let safe_exit_reason = truncate_exit_reason(reason);
        let safe_audit_reason = truncate_audit_reason(reason);
        let req_qty_dec = decimal_param(requested_qty, 8, DEC_20_8_MAX_ABS)?;
        let lim_px_dec = decimal_param(entry_price, 8, DEC_20_8_MAX_ABS)?;

        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'CANCELLED',
                   fsm_state = 'CANCELLED',
                   reserved_margin_usdc = 0.00000000,
                   committed_notional_usdc = 0.00000000,
                   closed_at_ms = :ts,
                   updated_at_ms = :ts,
                   exit_reason = :reason
               WHERE id = :pid AND status = 'WORKING'"#,
            params! { "ts" => ts, "reason" => &safe_exit_reason, "pid" => position_id },
        )
        .map_err(|e| AegisError::Database(format!("cancel_working_order update failed: {e}")))?;

        if tx.affected_rows() == 0 {
            return Err(AegisError::Database(format!(
                "cancel_working_order affected 0 rows for position {position_id}"
            )));
        }

        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, :from_st, 'CANCELLED',
                   :req_qty, 0.00000000, :lim_px, 0.00000000,
                   0.00000000, 0.00000000, 0.00000000, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "req_qty" => &req_qty_dec,
                "lim_px" => &lim_px_dec,
                "reason" => &safe_audit_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("cancel_working_order audit failed: {e}")))?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!("cancel_working_order commit failed: {e}"))
        })?;
        Ok(())
    }

    /// Fetches a `PositionRecord` by `id`.
    pub fn get_position_by_id(&self, position_id: i64) -> Result<Option<PositionRecord>> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .exec_first(
                r#"SELECT id, asset_id, client_order_id, side, status, fsm_state, entry_price,
                          current_price, exit_price, requested_quantity, filled_quantity,
                          reserved_margin_usdc, committed_notional_usdc, portfolio_weight,
                          s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          protective_stop_px, entry_r0, protective_stop_oid, take_profit_px, take_profit_oid,
                          highest_price_seen, lowest_price_seen, chase_count, realized_pnl_usdc,
                          unrealized_pnl_usdc, fees_paid_usdc, funding_accrued_usdc,
                          slippage_paid_usdc, exchange_order_id, created_at_ms, opened_at_ms,
                          closed_at_ms, updated_at_ms, exit_reason
                   FROM positions
                   WHERE id = :pid"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| AegisError::Database(format!("get_position_by_id failed: {e}")))?;
        Ok(row.map(|r| row_to_position_record(&r)))
    }

    /// Returns any active (`WORKING` or `OPEN`) position for `asset_id`.
    pub fn get_active_position_for_asset(&self, asset_id: i64) -> Result<Option<PositionRecord>> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .exec_first(
                r#"SELECT id, asset_id, client_order_id, side, status, fsm_state, entry_price,
                          current_price, exit_price, requested_quantity, filled_quantity,
                          reserved_margin_usdc, committed_notional_usdc, portfolio_weight,
                          s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          protective_stop_px, entry_r0, protective_stop_oid, take_profit_px, take_profit_oid,
                          highest_price_seen, lowest_price_seen, chase_count, realized_pnl_usdc,
                          unrealized_pnl_usdc, fees_paid_usdc, funding_accrued_usdc,
                          slippage_paid_usdc, exchange_order_id, created_at_ms, opened_at_ms,
                          closed_at_ms, updated_at_ms, exit_reason
                   FROM positions
                   WHERE asset_id = :aid AND status IN ('WORKING', 'OPEN')
                   ORDER BY id DESC
                   LIMIT 1"#,
                params! { "aid" => asset_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("get_active_position_for_asset failed: {e}"))
            })?;
        Ok(row.map(|r| row_to_position_record(&r)))
    }

    /// Lists recent `CLOSED` or `CANCELLED` positions.
    pub fn list_closed_positions(&self, limit: usize) -> Result<Vec<PositionRecord>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, asset_id, client_order_id, side, status, fsm_state, entry_price,
                          current_price, exit_price, requested_quantity, filled_quantity,
                          reserved_margin_usdc, committed_notional_usdc, portfolio_weight,
                          s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          protective_stop_px, entry_r0, protective_stop_oid, take_profit_px, take_profit_oid,
                          highest_price_seen, lowest_price_seen, chase_count, realized_pnl_usdc,
                          unrealized_pnl_usdc, fees_paid_usdc, funding_accrued_usdc,
                          slippage_paid_usdc, exchange_order_id, created_at_ms, opened_at_ms,
                          closed_at_ms, updated_at_ms, exit_reason
                   FROM positions
                   WHERE status IN ('CLOSED', 'CANCELLED')
                   ORDER BY updated_at_ms DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| AegisError::Database(format!("list_closed_positions failed: {e}")))?;
        Ok(rows.iter().map(row_to_position_record).collect())
    }

    /// Lists recent `CLOSED` trades joined with `exchange_assets`, enriching each trade with
    /// `entry_notional_usdc` and `realized_pnl_pct`.
    pub fn list_recent_closed_positions_view(
        &self,
        limit: usize,
    ) -> Result<Vec<ClosedPositionView>> {
        let mut conn = self.get_conn()?;
        Self::list_recent_closed_positions_view_with_conn(&mut conn, limit)
    }

    pub(crate) fn list_recent_closed_positions_view_with_conn(
        conn: &mut PooledConn,
        limit: usize,
    ) -> Result<Vec<ClosedPositionView>> {
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT p.id, p.asset_id, a.symbol, a.coin, p.client_order_id, p.side,
                          p.entry_price, COALESCE(p.exit_price, p.current_price),
                          p.filled_quantity, p.committed_notional_usdc,
                          p.realized_pnl_usdc, p.fees_paid_usdc, p.funding_accrued_usdc,
                          p.opened_at_ms, COALESCE(p.closed_at_ms, p.updated_at_ms), p.exit_reason
                   FROM positions p
                   INNER JOIN exchange_assets a ON a.id = p.asset_id
                   WHERE p.status = 'CLOSED'
                     AND COALESCE(p.exit_reason, '') != 'SCALE_IN_MERGED'
                   ORDER BY COALESCE(p.closed_at_ms, p.updated_at_ms) DESC, p.id DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| {
                AegisError::Database(format!("list_recent_closed_positions_view failed: {e}"))
            })?;

        Ok(rows
            .iter()
            .map(|r| {
                let entry_price = row_get_f64(r, 6, 0.0);
                let exit_price = row_get_f64(r, 7, 0.0);
                let filled_quantity = row_get_f64(r, 8, 0.0);
                let committed_notional_usdc = row_get_f64(r, 9, 0.0);
                let realized_pnl_usdc = row_get_f64(r, 10, 0.0);
                let entry_notional_usdc = round_dp(
                    resolve_position_entry_notional_usdc(
                        entry_price,
                        filled_quantity,
                        committed_notional_usdc,
                    ),
                    2,
                );
                let realized_pnl_pct = compute_position_pnl_pct(
                    realized_pnl_usdc,
                    entry_price,
                    filled_quantity,
                    committed_notional_usdc,
                );
                ClosedPositionView {
                    position_id: row_get_i64(r, 0, 0),
                    asset_id: row_get_i64(r, 1, 0),
                    symbol: row_get_string(r, 2, ""),
                    coin: row_get_string(r, 3, ""),
                    client_order_id: row_get_string(r, 4, ""),
                    side: row_get_string(r, 5, "LONG"),
                    entry_price,
                    exit_price,
                    quantity: filled_quantity,
                    filled_quantity,
                    entry_notional_usdc,
                    realized_pnl_usdc,
                    realized_pnl_pct,
                    fees_paid_usdc: row_get_f64(r, 11, 0.0),
                    funding_accrued_usdc: row_get_f64(r, 12, 0.0),
                    opened_at_ms: row_get_opt_i64(r, 13),
                    closed_at_ms: row_get_i64(r, 14, 0),
                    exit_reason: row_get_opt_string(r, 15),
                }
            })
            .collect())
    }

    /// Synchronizes MariaDB `positions` 1:1 with a live Hyperliquid `clearinghouseState` position for
    /// `input.asset_id`, preserving exchange SL/TP OIDs (and never loosening breached `OPEN` stops/TPs),
    /// promoting same-side `WORKING` orders in-place, and storing `funding_accrued_usdc` and `opened_at_ms`.
    ///
    /// Returns `(position_id, was_newly_adopted)`.
    pub fn sync_open_position_from_exchange_v2(
        &self,
        input: &ExchangeOpenPositionSyncInput<'_>,
    ) -> Result<(i64, bool)> {
        if !input.entry_price.is_finite() || input.entry_price <= 0.0 {
            return Err(AegisError::Validation(format!(
                "sync_open_position_from_exchange_v2 requires positive finite entry_price, got {}",
                input.entry_price
            )));
        }
        if !input.mark_price.is_finite() || input.mark_price < 0.0 {
            return Err(AegisError::Validation(format!(
                "sync_open_position_from_exchange_v2 requires non-negative finite mark_price, got {}",
                input.mark_price
            )));
        }
        if !input.quantity.is_finite() || input.quantity < 0.0 {
            return Err(AegisError::Validation(format!(
                "sync_open_position_from_exchange_v2 requires non-negative finite quantity, got {}",
                input.quantity
            )));
        }

        let ts = now_ms();
        let high = input.entry_price.max(input.mark_price);
        let low = input.entry_price.min(input.mark_price).max(0.0);

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!(
                    "sync_open_position_from_exchange_v2 tx failed: {e}"
                ))
            })?;

        let active_rows = lock_active_rows_for_asset(&mut tx, input.asset_id)?;

        let result = if let Some(open_row) = active_rows
            .iter()
            .find(|r| r.status.eq_ignore_ascii_case("OPEN"))
        {
            cancel_duplicate_active_rows(
                &mut tx,
                &active_rows,
                Some(open_row.id),
                "status IN ('OPEN', 'WORKING')",
                "DEDUP_EXCHANGE_RECONCILE",
                ts,
            )?;
            let pid = update_existing_open_row_on_sync(&mut tx, open_row, input, high, low, ts)?;
            (pid, false)
        } else if let Some(working_row) = active_rows.iter().find(|r| {
            r.status.eq_ignore_ascii_case("WORKING")
                && r.side.eq_ignore_ascii_case(input.side.as_str())
        }) {
            cancel_duplicate_active_rows(
                &mut tx,
                &active_rows,
                Some(working_row.id),
                "status = 'WORKING'",
                "DEDUP_EXCHANGE_RECONCILE",
                ts,
            )?;
            let pid =
                promote_same_side_working_row_on_sync(&mut tx, working_row, input, high, low, ts)?;
            (pid, true)
        } else {
            cancel_duplicate_active_rows(
                &mut tx,
                &active_rows,
                None,
                "status = 'WORKING'",
                "REPLACED_BY_EXCHANGE_OPEN",
                ts,
            )?;
            let pid = adopt_new_open_row_on_sync(&mut tx, input, high, low, ts)?;
            (pid, true)
        };

        tx.commit().map_err(|e| {
            AegisError::Database(format!(
                "sync_open_position_from_exchange_v2 commit failed: {e}"
            ))
        })?;
        self.invalidate_pnl_cache();
        Ok(result)
    }

    /// Reconciles historical `CLOSED` positions in MariaDB 1:1 with Hyperliquid `userFills` closed trades
    /// using close-epoch aggregation (`aggregate_close_epoch_trades`), two-pass `FOR UPDATE` matching,
    /// and self-healing of historical split `hl-closed-%` fragments while preserving `order_audit` FKs (`P1-Acct`).
    pub fn reconcile_closed_positions_from_exchange_fills(
        &self,
        trades: &[ReconciledClosedTradeInput],
    ) -> Result<usize> {
        if trades.is_empty() {
            return Ok(0);
        }
        let merged_trades = aggregate_close_epoch_trades(trades);

        let mut all_batch_coids: HashSet<String> = HashSet::new();
        let mut all_batch_eoids: HashSet<String> = HashSet::new();
        for m in &merged_trades {
            for c in &m.merged_coids {
                all_batch_coids.insert(c.clone());
            }
            for e in &m.merged_eoids {
                all_batch_eoids.insert(e.clone());
            }
        }

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!(
                    "reconcile_closed_positions_from_exchange_fills tx failed: {e}"
                ))
            })?;

        let mut matched_ids: HashSet<i64> = HashSet::with_capacity(merged_trades.len());
        let mut resolved_match: Vec<Option<i64>> = vec![None; merged_trades.len()];
        let mut skipped_active: Vec<bool> = vec![false; merged_trades.len()];

        // Pass 1: Exact client_order_id / exchange_order_id matching (`!matched_ids.contains(&pid)` enforced!)
        // + self-healing of split historical `hl-closed-%` fragments.
        for (idx, m) in merged_trades.iter().enumerate() {
            let candidates =
                lock_exact_closed_candidates(&mut tx, &m.merged_coids, &m.merged_eoids)?;
            if candidates.iter().any(|c| {
                !c.status.eq_ignore_ascii_case("CLOSED")
                    && m.merged_coids.iter().any(|mc| mc == &c.client_order_id)
            }) {
                skipped_active[idx] = true;
                continue;
            }

            let mut closed_cands: Vec<ExactClosedCandidate> = candidates
                .into_iter()
                .filter(|c| c.status.eq_ignore_ascii_case("CLOSED") && !matched_ids.contains(&c.id))
                .collect();

            if closed_cands.is_empty() {
                continue;
            }

            closed_cands.sort_by(|a, b| {
                let a_local = !a.client_order_id.starts_with("hl-closed-");
                let b_local = !b.client_order_id.starts_with("hl-closed-");
                (b_local, b.has_audit, b.id).cmp(&(a_local, a.has_audit, a.id))
            });

            let survivor_id = closed_cands[0].id;
            for dup in closed_cands.iter().skip(1) {
                if dup.client_order_id.starts_with("hl-closed-") {
                    tx.exec_drop(
                        "UPDATE order_audit SET position_id = :survivor WHERE position_id = :dup",
                        params! { "survivor" => survivor_id, "dup" => dup.id },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!(
                            "reconcile_closed audit reassign {} -> {} failed: {e}",
                            dup.id, survivor_id
                        ))
                    })?;
                    tx.exec_drop(
                        "DELETE FROM positions WHERE id = :dup AND status = 'CLOSED'",
                        params! { "dup" => dup.id },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!(
                            "reconcile_closed split fragment delete {} failed: {e}",
                            dup.id
                        ))
                    })?;
                }
            }

            matched_ids.insert(survivor_id);
            resolved_match[idx] = Some(survivor_id);
        }

        // Pass 2: 30s close-window fallback for unmatched epochs, skipping rows reserved by another
        // trade's exact coid/eoid in this batch.
        for (idx, m) in merged_trades.iter().enumerate() {
            if skipped_active[idx] || resolved_match[idx].is_some() {
                continue;
            }
            let win_rows: Vec<mysql::Row> = tx
                .exec(
                    r#"SELECT id, client_order_id, exchange_order_id
                       FROM positions
                       WHERE status = 'CLOSED'
                         AND asset_id = :aid
                         AND side = :side
                         AND ABS(COALESCE(closed_at_ms, updated_at_ms) - :clts) <= 30000
                       ORDER BY CASE WHEN client_order_id NOT LIKE 'hl-closed-%' THEN 0 ELSE 1 END,
                                ABS(COALESCE(closed_at_ms, updated_at_ms) - :clts) ASC,
                                id DESC
                       LIMIT 20
                       FOR UPDATE"#,
                    params! {
                        "aid" => m.asset_id,
                        "side" => m.side.as_str(),
                        "clts" => m.closed_at_ms,
                    },
                )
                .map_err(|e| {
                    AegisError::Database(format!(
                        "reconcile_closed window lookup failed for '{}': {e}",
                        m.client_order_id
                    ))
                })?;

            for r in &win_rows {
                let id = row_get_i64(r, 0, 0);
                let coid = row_get_string(r, 1, "");
                let eoid_opt = row_get_opt_string(r, 2);
                if matched_ids.contains(&id)
                    || all_batch_coids.contains(&coid)
                    || eoid_opt
                        .as_deref()
                        .is_some_and(|e| all_batch_eoids.contains(e))
                {
                    continue;
                }
                matched_ids.insert(id);
                resolved_match[idx] = Some(id);
                break;
            }
        }

        let mut processed = 0usize;
        for (idx, m) in merged_trades.iter().enumerate() {
            if skipped_active[idx] {
                continue;
            }
            let entry_px = round_dp(m.entry_price, 8);
            let exit_px = round_dp(m.exit_price, 8);
            let qty = round_dp(m.filled_quantity, 8);
            let high = round_dp(m.entry_price.max(m.exit_price), 8);
            let low = round_dp(m.entry_price.min(m.exit_price).max(0.0), 8);
            let rpnl = round_dp(m.realized_pnl_usdc, 4);
            let fees = round_dp(m.fees_paid_usdc, 4);
            let funding = round_dp(m.funding_accrued_usdc, 4);

            let entry_dec = decimal_param(entry_px.max(1e-8), 8, DEC_20_8_MAX_ABS)?;
            let exit_dec = decimal_param(exit_px.max(1e-8), 8, DEC_20_8_MAX_ABS)?;
            let qty_dec = decimal_param(qty.max(0.0), 8, DEC_20_8_MAX_ABS)?;
            let high_dec = decimal_param(high.max(0.0), 8, DEC_20_8_MAX_ABS)?;
            let low_dec = decimal_param(low.max(0.0), 8, DEC_20_8_MAX_ABS)?;
            let rpnl_dec = decimal_param(rpnl, 8, DEC_20_8_MAX_ABS)?;
            let fees_dec = decimal_param(fees, 8, DEC_20_8_MAX_ABS)?;
            let funding_dec = decimal_param(funding, 8, DEC_20_8_MAX_ABS)?;
            let safe_exit_reason = truncate_exit_reason(&m.exit_reason);

            if let Some(pid) = resolved_match[idx] {
                tx.exec_drop(
                    r#"UPDATE positions
                       SET entry_price = :entry_px,
                           current_price = :exit_px,
                           exit_price = :exit_px,
                           requested_quantity = :qty,
                           filled_quantity = :qty,
                           highest_price_seen = GREATEST(highest_price_seen, CAST(:high AS DECIMAL(20,8))),
                           lowest_price_seen = CASE
                               WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, CAST(:low AS DECIMAL(20,8)))
                               ELSE CAST(:low AS DECIMAL(20,8))
                           END,
                           realized_pnl_usdc = :rpnl,
                           unrealized_pnl_usdc = 0.00000000,
                           fees_paid_usdc = :fees,
                           funding_accrued_usdc = :funding,
                           exchange_order_id = COALESCE(NULLIF(TRIM(exchange_order_id), ''), :eoid),
                           opened_at_ms = CASE
                               WHEN :cts > 0 THEN LEAST(COALESCE(opened_at_ms, :cts), :cts)
                               ELSE opened_at_ms
                           END,
                           closed_at_ms = :clts,
                           updated_at_ms = :clts,
                           exit_reason = COALESCE(NULLIF(TRIM(exit_reason), ''), :reason)
                       WHERE id = :pid AND status = 'CLOSED'"#,
                    params! {
                        "entry_px" => &entry_dec,
                        "exit_px" => &exit_dec,
                        "qty" => &qty_dec,
                        "high" => &high_dec,
                        "low" => &low_dec,
                        "rpnl" => &rpnl_dec,
                        "fees" => &fees_dec,
                        "funding" => &funding_dec,
                        "eoid" => &m.exchange_order_id,
                        "cts" => m.created_at_ms,
                        "clts" => m.closed_at_ms,
                        "reason" => &safe_exit_reason,
                        "pid" => pid,
                    },
                )
                .map_err(|e| {
                    AegisError::Database(format!(
                        "Failed to update matched closed trade #{pid} ('{}'): {e}",
                        m.client_order_id
                    ))
                })?;
            } else {
                tx.exec_drop(
                    r#"INSERT INTO positions (
                           asset_id, client_order_id, side, status, fsm_state, entry_price, current_price,
                           exit_price, requested_quantity, filled_quantity, reserved_margin_usdc,
                           committed_notional_usdc, portfolio_weight, s1_weight_contrib, s2_weight_contrib,
                           s3_weight_contrib, protective_stop_px, highest_price_seen, lowest_price_seen,
                           chase_count, realized_pnl_usdc, unrealized_pnl_usdc, fees_paid_usdc,
                           funding_accrued_usdc, exchange_order_id, created_at_ms, opened_at_ms,
                           closed_at_ms, updated_at_ms, exit_reason
                       ) VALUES (
                           :aid, :coid, :side, 'CLOSED', 'CLOSED', :entry_px, :exit_px,
                           :exit_px, :qty, :qty, 0.00000000,
                           0.00000000, 0.00000000, 0.00000000, 0.00000000,
                           0.00000000, 0.00000000, :high, :low,
                           0, :rpnl, 0.00000000, :fees,
                           :funding, :eoid, :cts, :cts,
                           :clts, :clts, :reason
                       )"#,
                    params! {
                        "aid" => m.asset_id,
                        "coid" => &m.client_order_id,
                        "side" => m.side.as_str(),
                        "entry_px" => &entry_dec,
                        "exit_px" => &exit_dec,
                        "qty" => &qty_dec,
                        "high" => &high_dec,
                        "low" => &low_dec,
                        "rpnl" => &rpnl_dec,
                        "fees" => &fees_dec,
                        "funding" => &funding_dec,
                        "eoid" => &m.exchange_order_id,
                        "cts" => m.created_at_ms,
                        "clts" => m.closed_at_ms,
                        "reason" => &safe_exit_reason,
                    },
                )
                .map_err(|e| {
                    AegisError::Database(format!(
                        "Failed to insert reconciled closed trade '{}': {e}",
                        m.client_order_id
                    ))
                })?;
                let new_id = tx.last_insert_id().unwrap_or(0) as i64;
                if new_id > 0 {
                    matched_ids.insert(new_id);
                }
            }
            processed += 1;
        }

        tx.commit().map_err(|e| {
            AegisError::Database(format!(
                "reconcile_closed_positions_from_exchange_fills commit failed: {e}"
            ))
        })?;
        self.invalidate_pnl_cache();
        Ok(processed)
    }

    /// Tags a newly closed position with its canonical Hyperliquid closing `client_order_id` (`hl-closed-<coin>-<oid>`)
    /// and `exchange_order_id` (`hl-<oid>`) atomically inside a transaction with `SELECT ... FOR UPDATE`.
    pub fn set_position_closed_exchange_ids(
        &self,
        position_id: i64,
        client_order_id: &str,
        exchange_order_id: &str,
    ) -> Result<()> {
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("set_position_closed_exchange_ids tx failed: {e}"))
            })?;

        let status_opt: Option<String> = tx
            .exec_first(
                "SELECT status FROM positions WHERE id = :pid FOR UPDATE",
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("set_position_closed_exchange_ids lock failed: {e}"))
            })?;

        let current_status = status_opt
            .ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        if !matches!(
            current_status.to_ascii_uppercase().as_str(),
            "CLOSED" | "OPEN"
        ) {
            return Err(AegisError::Database(format!(
                "Cannot set closed exchange IDs on position {position_id} with status '{current_status}'"
            )));
        }

        tx.exec_drop(
            "DELETE FROM positions WHERE client_order_id = :coid AND id != :pid AND status IN ('CLOSED', 'CANCELLED')",
            params! { "coid" => client_order_id, "pid" => position_id },
        )
        .map_err(|e| {
            AegisError::Database(format!(
                "set_position_closed_exchange_ids dedup delete failed: {e}"
            ))
        })?;

        tx.exec_drop(
            r#"UPDATE positions
               SET client_order_id = :coid,
                   exchange_order_id = :eoid
               WHERE id = :pid AND status = :expected_status"#,
            params! {
                "coid" => client_order_id,
                "eoid" => exchange_order_id,
                "pid" => position_id,
                "expected_status" => current_status,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("set_position_closed_exchange_ids failed: {e}"))
        })?;

        if tx.affected_rows() == 0 {
            return Err(AegisError::Database(format!(
                "set_position_closed_exchange_ids affected 0 rows for position {position_id}"
            )));
        }

        tx.commit().map_err(|e| {
            AegisError::Database(format!(
                "set_position_closed_exchange_ids commit failed: {e}"
            ))
        })?;
        Ok(())
    }

    /// Returns the millisecond timestamp of the most recent v2 position entry (`client_order_id LIKE 'v2-%'`)
    /// or `last_entry_order_ms` stored in `system_config`, whichever is newer.
    pub fn last_position_entry_timestamp_ms(&self) -> Result<Option<i64>> {
        let db_max: Option<Option<i64>> = {
            let mut conn = self.get_conn()?;
            conn.query_first(
                "SELECT MAX(created_at_ms) FROM positions WHERE client_order_id LIKE 'v2-%'",
            )
            .map_err(|e| {
                AegisError::Database(format!("last_position_entry_timestamp_ms failed: {e}"))
            })?
        };
        let pos_ts = db_max.flatten().unwrap_or(0);
        let cfg_ts = self
            .load_config_or_default()
            .get_i64("last_entry_order_ms", 0);
        let max_ts = pos_ts.max(cfg_ts);
        Ok((max_ts > 0).then_some(max_ts))
    }

    /// Persists the latest new position entry timestamp (`last_entry_order_ms`) in `system_config`.
    pub fn record_position_entry_timestamp_ms(&self, ts_ms: i64) -> Result<()> {
        self.set_config_value(
            "last_entry_order_ms",
            &ts_ms.to_string(),
            Some("Timestamp (ms) of the most recent new position entry order"),
        )
    }

    /// Sums `realized_pnl_usdc` across all rows in `positions`.
    pub fn sum_realized_pnl_usdc(&self) -> Result<f64> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .query_first("SELECT COALESCE(SUM(realized_pnl_usdc), 0) FROM positions")
            .map_err(|e| AegisError::Database(format!("sum_realized_pnl_usdc failed: {e}")))?;
        Ok(row.map(|r| row_get_f64(&r, 0, 0.0)).unwrap_or(0.0))
    }

    /// Returns all active (`WORKING` or `OPEN`) rows for `asset_id` ordered by `id ASC`.
    pub fn list_active_positions_for_asset(&self, asset_id: i64) -> Result<Vec<PositionRecord>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, asset_id, client_order_id, side, status, fsm_state, entry_price,
                          current_price, exit_price, requested_quantity, filled_quantity,
                          reserved_margin_usdc, committed_notional_usdc, portfolio_weight,
                          s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          protective_stop_px, entry_r0, protective_stop_oid, take_profit_px, take_profit_oid,
                          highest_price_seen, lowest_price_seen, chase_count, realized_pnl_usdc,
                          unrealized_pnl_usdc, fees_paid_usdc, funding_accrued_usdc,
                          slippage_paid_usdc, exchange_order_id, created_at_ms, opened_at_ms,
                          closed_at_ms, updated_at_ms, exit_reason
                   FROM positions
                   WHERE asset_id = :aid AND status IN ('WORKING', 'OPEN')
                   ORDER BY id ASC"#,
                params! { "aid" => asset_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("list_active_positions_for_asset failed: {e}"))
            })?;
        Ok(rows.iter().map(row_to_position_record).collect())
    }

    /// Counts non-cancelled v2 entry orders created at or after `since_ms` (for `max_entries_per_bar`).
    pub fn count_entries_since(&self, since_ms: i64) -> Result<usize> {
        let mut conn = self.get_conn()?;
        let cnt: Option<i64> = conn
            .exec_first(
                r#"SELECT COUNT(*)
                   FROM positions
                   WHERE created_at_ms >= :since_ms
                     AND client_order_id LIKE 'v2-%'
                     AND status != 'CANCELLED'"#,
                params! { "since_ms" => since_ms.max(0) },
            )
            .map_err(|e| AegisError::Database(format!("count_entries_since failed: {e}")))?;
        Ok(cnt.unwrap_or(0).max(0) as usize)
    }

    /// Reads the initial `WORKING_ALO` `order_audit.reason` recorded when `position_id` was placed.
    pub fn get_working_order_initial_reason(&self, position_id: i64) -> Result<Option<String>> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .exec_first(
                r#"SELECT reason
                   FROM order_audit
                   WHERE position_id = :pid AND to_state = 'WORKING_ALO'
                   ORDER BY id ASC
                   LIMIT 1"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("get_working_order_initial_reason failed: {e}"))
            })?;
        Ok(row
            .map(|r| row_get_string(&r, 0, ""))
            .filter(|s| !s.trim().is_empty()))
    }

    /// Atomically merges a filled scale-in `WORKING` row (`working_position_id`) into an existing
    /// same-side `OPEN` row (`open_position_id`) under `SELECT ... FOR UPDATE`:
    /// - Recomputes VWAP `entry_price` and cumulative `filled_quantity`, `committed_notional_usdc`,
    ///   `fees_paid_usdc`, and sleeve/portfolio weights on the `OPEN` row.
    /// - Preserves existing positive `entry_r0` and `opened_at_ms` on the `OPEN` row.
    /// - Marks the add-on `WORKING` row `CLOSED` with `exit_reason = 'SCALE_IN_MERGED'`.
    #[allow(clippy::too_many_arguments)]
    pub fn merge_working_scale_in_into_open(
        &self,
        working_position_id: i64,
        open_position_id: i64,
        fill_price: f64,
        filled_quantity: f64,
        fees_paid_usdc: f64,
        exchange_order_id: Option<&str>,
        reason: &str,
    ) -> Result<()> {
        if working_position_id == open_position_id {
            return Err(AegisError::Validation(
                "merge_working_scale_in_into_open requires distinct working and open position IDs"
                    .to_string(),
            ));
        }
        if !fill_price.is_finite() || fill_price <= 0.0 {
            return Err(AegisError::Validation(format!(
                "merge_working_scale_in_into_open requires positive fill_price, got {fill_price}"
            )));
        }
        if !filled_quantity.is_finite() || filled_quantity <= 0.0 {
            return Err(AegisError::Validation(format!(
                "merge_working_scale_in_into_open requires positive filled_quantity, got {filled_quantity}"
            )));
        }
        if !fees_paid_usdc.is_finite() || fees_paid_usdc < 0.0 {
            return Err(AegisError::Validation(format!(
                "merge_working_scale_in_into_open requires non-negative finite fees_paid_usdc, got {fees_paid_usdc}"
            )));
        }

        let ts = now_ms();
        let fees_rounded = round_dp(fees_paid_usdc, 4);
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("merge_working_scale_in_into_open tx failed: {e}"))
            })?;

        let rows: Vec<mysql::Row> = tx
            .exec(
                r#"SELECT id, asset_id, client_order_id, side, status, fsm_state, entry_price,
                          requested_quantity, filled_quantity, committed_notional_usdc,
                          portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          protective_stop_px, entry_r0, highest_price_seen, lowest_price_seen,
                          fees_paid_usdc, opened_at_ms
                   FROM positions
                   WHERE id IN (:wid, :oid)
                   ORDER BY id ASC
                   FOR UPDATE"#,
                params! { "wid" => working_position_id, "oid" => open_position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("merge_working_scale_in_into_open lock failed: {e}"))
            })?;

        let mut working_row: Option<&mysql::Row> = None;
        let mut open_row: Option<&mysql::Row> = None;
        for r in &rows {
            let id = row_get_i64(r, 0, 0);
            if id == working_position_id {
                working_row = Some(r);
            } else if id == open_position_id {
                open_row = Some(r);
            }
        }
        let w = working_row.ok_or_else(|| {
            AegisError::Database(format!("Working position {working_position_id} not found"))
        })?;
        let o = open_row.ok_or_else(|| {
            AegisError::Database(format!("Open position {open_position_id} not found"))
        })?;

        let w_asset_id = row_get_i64(w, 1, 0);
        let w_coid = row_get_string(w, 2, "");
        let w_side_str = row_get_string(w, 3, "LONG");
        let w_status = row_get_string(w, 4, "");
        let w_fsm = row_get_string(w, 5, "WORKING_ALO");
        let w_req_qty = row_get_f64(w, 7, 0.0);
        let w_pw = row_get_f64(w, 10, 0.0);
        let w_s1 = row_get_f64(w, 11, 0.0);
        let w_s2 = row_get_f64(w, 12, 0.0);
        let w_s3 = row_get_f64(w, 13, 0.0);

        let o_asset_id = row_get_i64(o, 1, 0);
        let o_coid = row_get_string(o, 2, "");
        let o_side_str = row_get_string(o, 3, "LONG");
        let o_status = row_get_string(o, 4, "");
        let o_fsm = row_get_string(o, 5, "FILLED_OPEN");
        let o_entry_px = row_get_f64(o, 6, 0.0);
        let o_req_qty = row_get_f64(o, 7, 0.0);
        let o_filled_qty = row_get_f64(o, 8, 0.0);
        let o_pw = row_get_f64(o, 10, 0.0);
        let o_s1 = row_get_f64(o, 11, 0.0);
        let o_s2 = row_get_f64(o, 12, 0.0);
        let o_s3 = row_get_f64(o, 13, 0.0);
        let o_stop_px = row_get_f64(o, 14, 0.0);
        let o_entry_r0 = row_get_f64(o, 15, 0.0);
        let o_fees = row_get_f64(o, 18, 0.0);
        let o_opened_at_ms = row_get_opt_i64(o, 19);

        if !w_status.eq_ignore_ascii_case("WORKING") {
            return Err(AegisError::Database(format!(
                "Scale-in working position {working_position_id} has status '{w_status}' (expected 'WORKING')"
            )));
        }
        if !o_status.eq_ignore_ascii_case("OPEN") {
            return Err(AegisError::Database(format!(
                "Scale-in target open position {open_position_id} has status '{o_status}' (expected 'OPEN')"
            )));
        }
        if w_asset_id != o_asset_id {
            return Err(AegisError::Validation(format!(
                "Scale-in asset mismatch: working={w_asset_id}, open={o_asset_id}"
            )));
        }
        if OrderSide::parse(&w_side_str)? != OrderSide::parse(&o_side_str)? {
            return Err(AegisError::Validation(format!(
                "Scale-in side mismatch: working={w_side_str}, open={o_side_str}"
            )));
        }

        let old_qty = o_filled_qty.max(0.0);
        let add_qty = round_dp(filled_quantity, 8);
        let new_qty = round_dp(old_qty + add_qty, 8);
        let old_notional = o_entry_px * old_qty;
        let add_notional = fill_price * add_qty;
        let total_notional = old_notional + add_notional;
        let vwap_entry = round_dp(total_notional / new_qty.max(1e-12), 8);
        let new_committed = round_dp(total_notional, 4);
        let new_req_qty = round_dp(o_req_qty.max(old_qty) + add_qty, 8);
        let new_fees = round_dp(o_fees + fees_rounded, 4);
        let w_fill_ratio = if w_req_qty > 1e-12 {
            (add_qty / w_req_qty).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let new_pw = round_dp(o_pw + w_pw * w_fill_ratio, 4);
        let new_s1 = round_dp(o_s1 + w_s1 * w_fill_ratio, 4);
        let new_s2 = round_dp(o_s2 + w_s2 * w_fill_ratio, 4);
        let new_s3 = round_dp(o_s3 + w_s3 * w_fill_ratio, 4);
        let preserved_r0 = if o_entry_r0.is_finite() && o_entry_r0 > 0.0 {
            o_entry_r0
        } else {
            derive_entry_r0_from_stop(o_entry_px, o_stop_px)
        };
        let preserved_opened_at = o_opened_at_ms.filter(|&t| t > 0).unwrap_or(ts);

        let vwap_dec = decimal_param(vwap_entry, 8, DEC_20_8_MAX_ABS)?;
        let fill_px_dec = decimal_param(fill_price, 8, DEC_20_8_MAX_ABS)?;
        let new_req_qty_dec = decimal_param(new_req_qty, 8, DEC_20_8_MAX_ABS)?;
        let new_qty_dec = decimal_param(new_qty, 8, DEC_20_8_MAX_ABS)?;
        let add_qty_dec = decimal_param(add_qty, 8, DEC_20_8_MAX_ABS)?;
        let w_req_qty_dec = decimal_param(w_req_qty, 8, DEC_20_8_MAX_ABS)?;
        let new_comm_dec = decimal_param(new_committed, 8, DEC_20_8_MAX_ABS)?;
        let add_comm_dec = decimal_param(round_dp(add_notional, 4), 8, DEC_20_8_MAX_ABS)?;
        let new_fees_dec = decimal_param(new_fees, 8, DEC_20_8_MAX_ABS)?;
        let add_fees_dec = decimal_param(fees_rounded, 8, DEC_20_8_MAX_ABS)?;
        let new_pw_dec = decimal_param(new_pw, 8, DEC_20_8_MAX_ABS)?;
        let new_s1_dec = decimal_param(new_s1, 8, DEC_20_8_MAX_ABS)?;
        let new_s2_dec = decimal_param(new_s2, 8, DEC_20_8_MAX_ABS)?;
        let new_s3_dec = decimal_param(new_s3, 8, DEC_20_8_MAX_ABS)?;
        let r0_dec = decimal_param(preserved_r0, 8, DEC_20_8_MAX_ABS)?;

        tx.exec_drop(
            r#"UPDATE positions
               SET entry_price = :vwap_px,
                   current_price = :fill_px,
                   requested_quantity = :new_req_qty,
                   filled_quantity = :new_qty,
                   committed_notional_usdc = :new_comm,
                   portfolio_weight = :new_pw,
                   s1_weight_contrib = :new_s1,
                   s2_weight_contrib = :new_s2,
                   s3_weight_contrib = :new_s3,
                   entry_r0 = CASE
                       WHEN entry_r0 > 0 THEN entry_r0
                       ELSE CAST(:entry_r0 AS DECIMAL(20,8))
                   END,
                   highest_price_seen = GREATEST(highest_price_seen, CAST(:fill_px AS DECIMAL(20,8))),
                   lowest_price_seen = CASE
                       WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, CAST(:fill_px AS DECIMAL(20,8)))
                       ELSE CAST(:fill_px AS DECIMAL(20,8))
                   END,
                   fees_paid_usdc = :new_fees,
                   opened_at_ms = COALESCE(opened_at_ms, :opened_at),
                   updated_at_ms = :ts
               WHERE id = :oid AND status = 'OPEN'"#,
            params! {
                "vwap_px" => &vwap_dec,
                "fill_px" => &fill_px_dec,
                "new_req_qty" => &new_req_qty_dec,
                "new_qty" => &new_qty_dec,
                "new_comm" => &new_comm_dec,
                "new_pw" => &new_pw_dec,
                "new_s1" => &new_s1_dec,
                "new_s2" => &new_s2_dec,
                "new_s3" => &new_s3_dec,
                "entry_r0" => &r0_dec,
                "new_fees" => &new_fees_dec,
                "opened_at" => preserved_opened_at,
                "ts" => ts,
                "oid" => open_position_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("merge_working_scale_in_into_open update open failed: {e}"))
        })?;
        if tx.affected_rows() == 0 {
            return Err(AegisError::Database(format!(
                "merge_working_scale_in_into_open affected 0 rows on open position {open_position_id}"
            )));
        }

        // Fees and notional are moved onto the parent OPEN row; keep 0.00 fees/realized_pnl on the
        // SCALE_IN_MERGED child row so compute_pnl_summary does not double-count closed fees.
        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'CLOSED',
                   fsm_state = 'CLOSED',
                   current_price = :fill_px,
                   exit_price = :fill_px,
                   filled_quantity = :add_qty,
                   reserved_margin_usdc = 0.00000000,
                   committed_notional_usdc = 0.00000000,
                   realized_pnl_usdc = 0.00000000,
                   unrealized_pnl_usdc = 0.00000000,
                   fees_paid_usdc = 0.00000000,
                   exchange_order_id = COALESCE(:eoid, exchange_order_id),
                   opened_at_ms = COALESCE(opened_at_ms, :ts),
                   closed_at_ms = :ts,
                   updated_at_ms = :ts,
                   exit_reason = 'SCALE_IN_MERGED'
               WHERE id = :wid AND status = 'WORKING'"#,
            params! {
                "fill_px" => &fill_px_dec,
                "add_qty" => &add_qty_dec,
                "eoid" => exchange_order_id,
                "ts" => ts,
                "wid" => working_position_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!(
                "merge_working_scale_in_into_open close working failed: {e}"
            ))
        })?;
        if tx.affected_rows() == 0 {
            return Err(AegisError::Database(format!(
                "merge_working_scale_in_into_open affected 0 rows on working position {working_position_id}"
            )));
        }

        let safe_reason = truncate_audit_reason(&format!(
            "SCALE_IN_MERGED into #{open_position_id}: {reason}"
        ));
        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :wid, :aid, :w_coid, :side, :w_fsm, 'SCALE_IN_MERGED',
                   :w_req_qty, :add_qty, :fill_px, :fill_px,
                   0.00000000, :add_comm, :add_fees, :reason, :ts, :ts
               )"#,
            params! {
                "wid" => working_position_id,
                "aid" => w_asset_id,
                "w_coid" => w_coid,
                "side" => w_side_str,
                "w_fsm" => w_fsm,
                "w_req_qty" => &w_req_qty_dec,
                "add_qty" => &add_qty_dec,
                "fill_px" => &fill_px_dec,
                "add_comm" => &add_comm_dec,
                "add_fees" => &add_fees_dec,
                "reason" => &safe_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!(
                "merge_working_scale_in_into_open working audit failed: {e}"
            ))
        })?;

        let open_audit_reason = truncate_audit_reason(&format!(
            "SCALE_IN_MERGED +{add_qty} from #{working_position_id}"
        ));
        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :oid, :aid, :o_coid, :side, :o_fsm, 'FILLED_OPEN',
                   :new_req_qty, :new_qty, :vwap_px, :fill_px,
                   0.00000000, :new_comm, :add_fees, :reason, :ts, :ts
               )"#,
            params! {
                "oid" => open_position_id,
                "aid" => o_asset_id,
                "o_coid" => o_coid,
                "side" => o_side_str,
                "o_fsm" => o_fsm,
                "new_req_qty" => &new_req_qty_dec,
                "new_qty" => &new_qty_dec,
                "vwap_px" => &vwap_dec,
                "fill_px" => &fill_px_dec,
                "new_comm" => &new_comm_dec,
                "add_fees" => &add_fees_dec,
                "reason" => &open_audit_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!(
                "merge_working_scale_in_into_open open audit failed: {e}"
            ))
        })?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!(
                "merge_working_scale_in_into_open commit failed: {e}"
            ))
        })?;
        self.invalidate_pnl_cache();
        Ok(())
    }

    /// Atomically and idempotently accrues hourly `PAPER`-mode funding events into `funding_events`
    /// (keyed by `dedup_key`) and increments `positions.funding_accrued_usdc` ONLY for newly
    /// inserted events whose `(position_id, hour_ts_ms)` key had not yet been recorded.
    pub fn accrue_paper_funding_events(
        &self,
        events: &[(i64, FundingEventUpsertInput)],
    ) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("accrue_paper_funding_events tx failed: {e}"))
            })?;

        let mut inserted = 0usize;
        for &(position_id, ref ev) in events {
            let safe_usdc = if ev.usdc.is_finite() { ev.usdc } else { 0.0 };
            if safe_usdc.abs() <= 1e-12 {
                continue;
            }
            let coin: String = ev.coin.trim().chars().take(24).collect();
            let dkey: String = ev.dedup_key.chars().take(128).collect();
            let usdc_dec = decimal_param(round_dp(safe_usdc, 4), 8, DEC_20_8_MAX_ABS)?;
            let rem_dec = decimal_param(0.0, 8, DEC_20_8_MAX_ABS)?;
            let szi_dec = decimal_param(
                if ev.position_szi.is_finite() {
                    ev.position_szi
                } else {
                    0.0
                },
                8,
                DEC_20_8_MAX_ABS,
            )?;
            let fr_dec = decimal_param(
                if ev.funding_rate.is_finite() {
                    ev.funding_rate
                } else {
                    0.0
                },
                10,
                DEC_20_10_MAX_ABS,
            )?;

            tx.exec_drop(
                r#"INSERT IGNORE INTO funding_events
                   (asset_id, coin, event_time_ms, usdc, remaining_usdc, position_szi, funding_rate, dedup_key, created_at_ms, updated_at_ms)
                   VALUES (:aid, :coin, :ev_ts, :usdc, :rem_usdc, :szi, :fr, :dkey, :ts, :ts)"#,
                params! {
                    "aid" => ev.asset_id,
                    "coin" => &coin,
                    "ev_ts" => ev.event_time_ms,
                    "usdc" => &usdc_dec,
                    "rem_usdc" => &rem_dec,
                    "szi" => &szi_dec,
                    "fr" => &fr_dec,
                    "dkey" => &dkey,
                    "ts" => ts,
                },
            )
            .map_err(|e| {
                AegisError::Database(format!("accrue_paper_funding_events insert failed: {e}"))
            })?;

            if tx.affected_rows() > 0 {
                tx.exec_drop(
                    r#"UPDATE positions
                       SET funding_accrued_usdc = funding_accrued_usdc + CAST(:delta AS DECIMAL(20,8)),
                           updated_at_ms = :ts
                       WHERE id = :pid AND status = 'OPEN'"#,
                    params! {
                        "delta" => &usdc_dec,
                        "ts" => ts,
                        "pid" => position_id,
                    },
                )
                .map_err(|e| {
                    AegisError::Database(format!(
                        "accrue_paper_funding_events position update failed: {e}"
                    ))
                })?;
                inserted += 1;
            }
        }

        tx.commit().map_err(|e| {
            AegisError::Database(format!("accrue_paper_funding_events commit failed: {e}"))
        })?;
        if inserted > 0 {
            self.invalidate_pnl_cache();
        }
        Ok(inserted)
    }
}

struct ExactClosedCandidate {
    id: i64,
    status: String,
    client_order_id: String,
    has_audit: bool,
}

fn lock_exact_closed_candidates(
    tx: &mut Transaction<'_>,
    coids: &[String],
    eoids: &[String],
) -> Result<Vec<ExactClosedCandidate>> {
    if coids.is_empty() && eoids.is_empty() {
        return Ok(Vec::new());
    }
    let mut clauses = Vec::new();
    let mut bind_params: Vec<(String, Value)> = Vec::new();
    if !coids.is_empty() {
        let mut ph = Vec::with_capacity(coids.len());
        for (i, c) in coids.iter().enumerate() {
            let name = format!("c{i}");
            ph.push(format!(":{name}"));
            bind_params.push((name, Value::from(c.as_str())));
        }
        clauses.push(format!("p.client_order_id IN ({})", ph.join(", ")));
    }
    if !eoids.is_empty() {
        let mut ph = Vec::with_capacity(eoids.len());
        for (i, e) in eoids.iter().enumerate() {
            let name = format!("e{i}");
            ph.push(format!(":{name}"));
            bind_params.push((name, Value::from(e.as_str())));
        }
        clauses.push(format!("p.exchange_order_id IN ({})", ph.join(", ")));
    }
    let sql = format!(
        r#"SELECT p.id, p.status, p.client_order_id,
                  EXISTS (SELECT 1 FROM order_audit oa WHERE oa.position_id = p.id) AS has_audit
           FROM positions p
           WHERE {}
           ORDER BY p.id DESC
           FOR UPDATE"#,
        clauses.join(" OR ")
    );
    let rows: Vec<mysql::Row> = tx
        .exec(sql, mysql::Params::from(bind_params))
        .map_err(|e| AegisError::Database(format!("lock_exact_closed_candidates failed: {e}")))?;

    Ok(rows
        .iter()
        .map(|r| ExactClosedCandidate {
            id: row_get_i64(r, 0, 0),
            status: row_get_string(r, 1, ""),
            client_order_id: row_get_string(r, 2, ""),
            has_audit: row_get_bool(r, 3, false),
        })
        .collect())
}

fn lock_active_rows_for_asset(
    tx: &mut Transaction<'_>,
    asset_id: i64,
) -> Result<Vec<ActiveSyncRow>> {
    let raw_active: Vec<mysql::Row> = tx
        .exec(
            r#"SELECT id, status, side, protective_stop_px, entry_r0, protective_stop_oid,
                      take_profit_px, take_profit_oid, client_order_id, fsm_state,
                      requested_quantity
               FROM positions
               WHERE asset_id = :aid AND status IN ('OPEN', 'WORKING')
               ORDER BY FIELD(status, 'OPEN', 'WORKING'), id DESC
               FOR UPDATE"#,
            params! { "aid" => asset_id },
        )
        .map_err(|e| {
            AegisError::Database(format!(
                "sync_open_position_from_exchange_v2 query failed: {e}"
            ))
        })?;

    Ok(raw_active
        .iter()
        .map(|r| ActiveSyncRow {
            id: row_get_i64(r, 0, 0),
            status: row_get_string(r, 1, ""),
            side: row_get_string(r, 2, "LONG"),
            protective_stop_px: row_get_f64(r, 3, 0.0),
            entry_r0: row_get_f64(r, 4, 0.0),
            protective_stop_oid: row_get_opt_string(r, 5),
            take_profit_px: row_get_f64(r, 6, 0.0),
            take_profit_oid: row_get_opt_string(r, 7),
            client_order_id: row_get_string(r, 8, ""),
            fsm_state: row_get_string(r, 9, ""),
            requested_quantity: row_get_f64(r, 10, 0.0),
        })
        .collect())
}

fn cancel_duplicate_active_rows(
    tx: &mut Transaction<'_>,
    active_rows: &[ActiveSyncRow],
    keep_id: Option<i64>,
    status_predicate: &str,
    exit_reason: &str,
    ts: i64,
) -> Result<()> {
    let sql = format!(
        r#"UPDATE positions
           SET status = 'CANCELLED',
               fsm_state = 'CANCELLED',
               reserved_margin_usdc = 0.00000000,
               committed_notional_usdc = 0.00000000,
               closed_at_ms = :ts,
               updated_at_ms = :ts,
               exit_reason = :reason
           WHERE id = :pid AND {status_predicate}"#
    );
    for other in active_rows {
        if keep_id.is_some_and(|kid| kid == other.id) {
            continue;
        }
        tx.exec_drop(
            &sql,
            params! { "ts" => ts, "reason" => exit_reason, "pid" => other.id },
        )
        .map_err(|e| AegisError::Database(format!("dedup cancel position failed: {e}")))?;
    }
    Ok(())
}

fn update_existing_open_row_on_sync(
    tx: &mut Transaction<'_>,
    open_row: &ActiveSyncRow,
    input: &ExchangeOpenPositionSyncInput<'_>,
    high: f64,
    low: f64,
    ts: i64,
) -> Result<i64> {
    let (eff_stop, eff_sl_oid, eff_tp, eff_tp_oid) = resolve_synced_tpsl_targets(
        input.side,
        input.entry_price,
        Some(&open_row.side),
        open_row.protective_stop_px,
        open_row.protective_stop_oid.as_deref(),
        open_row.take_profit_px,
        open_row.take_profit_oid.as_deref(),
        input.protective_stop_px,
        input.take_profit_px,
    );

    let eff_r0 = if open_row.entry_r0.is_finite() && open_row.entry_r0 > 0.0 {
        open_row.entry_r0
    } else {
        derive_entry_r0_for_sync(input.side, input.entry_price, eff_stop)
    };
    let opened_at_param = input.opened_at_ms.filter(|&t| t > 0);
    tx.exec_drop(
        r#"UPDATE positions
           SET side = :side,
               status = 'OPEN',
               fsm_state = 'FILLED_OPEN',
               entry_price = :entry_px,
               current_price = :mark_px,
               requested_quantity = :qty,
               filled_quantity = :qty,
               reserved_margin_usdc = 0.00000000,
               committed_notional_usdc = :comm_usdc,
               unrealized_pnl_usdc = :upnl,
               funding_accrued_usdc = :funding,
               portfolio_weight = :pw,
               protective_stop_px = :stop_px,
               entry_r0 = CASE
                   WHEN entry_r0 > 0 THEN entry_r0
                   ELSE CAST(:entry_r0 AS DECIMAL(20,8))
               END,
               protective_stop_oid = :sl_oid,
               take_profit_px = :tp_px,
               take_profit_oid = :tp_oid,
               highest_price_seen = GREATEST(highest_price_seen, CAST(:high AS DECIMAL(20,8))),
               lowest_price_seen = CASE
                   WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, CAST(:low AS DECIMAL(20,8)))
                   ELSE CAST(:low AS DECIMAL(20,8))
               END,
               opened_at_ms = COALESCE(:opened_at_opt, opened_at_ms, :ts),
               updated_at_ms = :ts
           WHERE id = :pid AND status = 'OPEN'"#,
        params! {
            "side" => input.side.as_str(),
            "entry_px" => decimal_param(input.entry_price, 8, DEC_20_8_MAX_ABS)?,
            "mark_px" => decimal_param(input.mark_price, 8, DEC_20_8_MAX_ABS)?,
            "qty" => decimal_param(input.quantity, 8, DEC_20_8_MAX_ABS)?,
            "comm_usdc" => decimal_param(round_dp(input.committed_notional_usdc, 4), 8, DEC_20_8_MAX_ABS)?,
            "upnl" => decimal_param(round_dp(input.unrealized_pnl_usdc, 4), 8, DEC_20_8_MAX_ABS)?,
            "funding" => decimal_param(round_dp(input.funding_accrued_usdc, 4), 8, DEC_20_8_MAX_ABS)?,
            "pw" => decimal_param(round_dp(input.portfolio_weight, 4), 8, DEC_20_8_MAX_ABS)?,
            "stop_px" => decimal_param(eff_stop, 8, DEC_20_8_MAX_ABS)?,
            "entry_r0" => decimal_param(eff_r0, 8, DEC_20_8_MAX_ABS)?,
            "sl_oid" => eff_sl_oid,
            "tp_px" => decimal_param(eff_tp, 8, DEC_20_8_MAX_ABS)?,
            "tp_oid" => eff_tp_oid,
            "high" => decimal_param(high, 8, DEC_20_8_MAX_ABS)?,
            "low" => decimal_param(low, 8, DEC_20_8_MAX_ABS)?,
            "opened_at_opt" => opened_at_param,
            "ts" => ts,
            "pid" => open_row.id,
        },
    )
    .map_err(|e| {
        AegisError::Database(format!(
            "sync_open_position_from_exchange_v2 update failed: {e}"
        ))
    })?;
    Ok(open_row.id)
}

fn promote_same_side_working_row_on_sync(
    tx: &mut Transaction<'_>,
    w: &ActiveSyncRow,
    input: &ExchangeOpenPositionSyncInput<'_>,
    high: f64,
    low: f64,
    ts: i64,
) -> Result<i64> {
    let (eff_stop, _, eff_tp, _) = resolve_synced_tpsl_targets(
        input.side,
        input.entry_price,
        Some(&w.side),
        w.protective_stop_px,
        w.protective_stop_oid.as_deref(),
        w.take_profit_px,
        w.take_profit_oid.as_deref(),
        input.protective_stop_px,
        input.take_profit_px,
    );

    let eff_r0 = if w.entry_r0.is_finite() && w.entry_r0 > 0.0 {
        w.entry_r0
    } else {
        derive_entry_r0_for_sync(input.side, input.entry_price, eff_stop)
    };
    let comm_dec = decimal_param(
        round_dp(input.committed_notional_usdc, 4),
        8,
        DEC_20_8_MAX_ABS,
    )?;
    let entry_dec = decimal_param(input.entry_price, 8, DEC_20_8_MAX_ABS)?;
    let qty_dec = decimal_param(input.quantity, 8, DEC_20_8_MAX_ABS)?;
    let opened_at_param = input.opened_at_ms.filter(|&t| t > 0);

    tx.exec_drop(
        r#"UPDATE positions
           SET status = 'OPEN',
               fsm_state = 'FILLED_OPEN',
               entry_price = :entry_px,
               current_price = :mark_px,
               requested_quantity = :qty,
               filled_quantity = :qty,
               reserved_margin_usdc = 0.00000000,
               committed_notional_usdc = :comm_usdc,
               unrealized_pnl_usdc = :upnl,
               funding_accrued_usdc = :funding,
               portfolio_weight = :pw,
               protective_stop_px = :stop_px,
               entry_r0 = CASE
                   WHEN entry_r0 > 0 THEN entry_r0
                   ELSE CAST(:entry_r0 AS DECIMAL(20,8))
               END,
               take_profit_px = :tp_px,
               highest_price_seen = GREATEST(highest_price_seen, CAST(:high AS DECIMAL(20,8))),
               lowest_price_seen = CASE
                   WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, CAST(:low AS DECIMAL(20,8)))
                   ELSE CAST(:low AS DECIMAL(20,8))
               END,
               opened_at_ms = COALESCE(:opened_at_opt, opened_at_ms, :ts),
               updated_at_ms = :ts
           WHERE id = :pid AND status = 'WORKING'"#,
        params! {
            "entry_px" => &entry_dec,
            "mark_px" => decimal_param(input.mark_price, 8, DEC_20_8_MAX_ABS)?,
            "qty" => &qty_dec,
            "comm_usdc" => &comm_dec,
            "upnl" => decimal_param(round_dp(input.unrealized_pnl_usdc, 4), 8, DEC_20_8_MAX_ABS)?,
            "funding" => decimal_param(round_dp(input.funding_accrued_usdc, 4), 8, DEC_20_8_MAX_ABS)?,
            "pw" => decimal_param(round_dp(input.portfolio_weight, 4), 8, DEC_20_8_MAX_ABS)?,
            "stop_px" => decimal_param(eff_stop, 8, DEC_20_8_MAX_ABS)?,
            "entry_r0" => decimal_param(eff_r0, 8, DEC_20_8_MAX_ABS)?,
            "tp_px" => decimal_param(eff_tp, 8, DEC_20_8_MAX_ABS)?,
            "high" => decimal_param(high, 8, DEC_20_8_MAX_ABS)?,
            "low" => decimal_param(low, 8, DEC_20_8_MAX_ABS)?,
            "opened_at_opt" => opened_at_param,
            "ts" => ts,
            "pid" => w.id,
        },
    )
    .map_err(|e| {
        AegisError::Database(format!("promote working on exchange sync failed: {e}"))
    })?;

    let promote_reason = truncate_audit_reason("HL_WORKING_PROMOTED_ON_RECONCILE");
    tx.exec_drop(
        r#"INSERT INTO order_audit (
               position_id, asset_id, client_order_id, side, from_state, to_state,
               requested_qty, filled_qty, limit_price, avg_fill_price,
               reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
           ) VALUES (
               :pid, :aid, :coid, :side, :from_st, 'FILLED_OPEN',
               :req_qty, :qty, :entry_px, :entry_px,
               0.00000000, :comm_usdc, 0.00000000, :reason, :ts, :ts
           )"#,
        params! {
            "pid" => w.id,
            "aid" => input.asset_id,
            "coid" => &w.client_order_id,
            "side" => input.side.as_str(),
            "from_st" => &w.fsm_state,
            "req_qty" => decimal_param(w.requested_quantity, 8, DEC_20_8_MAX_ABS)?,
            "qty" => &qty_dec,
            "entry_px" => &entry_dec,
            "comm_usdc" => &comm_dec,
            "reason" => &promote_reason,
            "ts" => ts,
        },
    )
    .map_err(|e| {
        AegisError::Database(format!(
            "sync_open_position_from_exchange_v2 working promote audit failed: {e}"
        ))
    })?;

    Ok(w.id)
}

fn adopt_new_open_row_on_sync(
    tx: &mut Transaction<'_>,
    input: &ExchangeOpenPositionSyncInput<'_>,
    high: f64,
    low: f64,
    ts: i64,
) -> Result<i64> {
    let (eff_stop, _, eff_tp, _) = resolve_synced_tpsl_targets(
        input.side,
        input.entry_price,
        None,
        0.0,
        None,
        0.0,
        None,
        input.protective_stop_px,
        input.take_profit_px,
    );

    let seq = SYNC_COID_SEQ.fetch_add(1, Ordering::Relaxed);
    let coin_slug: String = input
        .coin
        .trim()
        .to_ascii_lowercase()
        .chars()
        .take(24)
        .collect();
    let coid = format!("hl-sync-{coin_slug}-{ts}-{seq}");
    let eff_opened_at = input.opened_at_ms.filter(|&t| t > 0).unwrap_or(ts);
    let eff_created_at = eff_opened_at.min(ts);

    let eff_r0 = derive_entry_r0_for_sync(input.side, input.entry_price, eff_stop);
    let entry_dec = decimal_param(input.entry_price, 8, DEC_20_8_MAX_ABS)?;
    let qty_dec = decimal_param(input.quantity, 8, DEC_20_8_MAX_ABS)?;
    let comm_dec = decimal_param(
        round_dp(input.committed_notional_usdc, 4),
        8,
        DEC_20_8_MAX_ABS,
    )?;

    tx.exec_drop(
        r#"INSERT INTO positions (
               asset_id, client_order_id, side, status, fsm_state, entry_price, current_price,
               requested_quantity, filled_quantity, reserved_margin_usdc, committed_notional_usdc,
               portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
               protective_stop_px, entry_r0, take_profit_px, highest_price_seen, lowest_price_seen, chase_count,
               unrealized_pnl_usdc, funding_accrued_usdc, exchange_order_id, created_at_ms, opened_at_ms, updated_at_ms
           ) VALUES (
               :aid, :coid, :side, 'OPEN', 'FILLED_OPEN', :entry_px, :mark_px,
               :qty, :qty, 0.00000000, :comm_usdc,
               :pw, :pw, 0.00000000, 0.00000000,
               :stop_px, :entry_r0, :tp_px, :high, :low, 0,
               :upnl, :funding, 'hl-reconciled', :cts, :ots, :ts
           )"#,
        params! {
            "aid" => input.asset_id,
            "coid" => &coid,
            "side" => input.side.as_str(),
            "entry_px" => &entry_dec,
            "mark_px" => decimal_param(input.mark_price, 8, DEC_20_8_MAX_ABS)?,
            "qty" => &qty_dec,
            "comm_usdc" => &comm_dec,
            "pw" => decimal_param(round_dp(input.portfolio_weight, 4), 8, DEC_20_8_MAX_ABS)?,
            "stop_px" => decimal_param(eff_stop, 8, DEC_20_8_MAX_ABS)?,
            "entry_r0" => decimal_param(eff_r0, 8, DEC_20_8_MAX_ABS)?,
            "tp_px" => decimal_param(eff_tp, 8, DEC_20_8_MAX_ABS)?,
            "high" => decimal_param(high, 8, DEC_20_8_MAX_ABS)?,
            "low" => decimal_param(low, 8, DEC_20_8_MAX_ABS)?,
            "upnl" => decimal_param(round_dp(input.unrealized_pnl_usdc, 4), 8, DEC_20_8_MAX_ABS)?,
            "funding" => decimal_param(round_dp(input.funding_accrued_usdc, 4), 8, DEC_20_8_MAX_ABS)?,
            "cts" => eff_created_at,
            "ots" => eff_opened_at,
            "ts" => ts,
        },
    )
    .map_err(|e| {
        AegisError::Database(format!(
            "sync_open_position_from_exchange_v2 insert failed: {e}"
        ))
    })?;

    let new_id = tx.last_insert_id().unwrap_or(0) as i64;
    let sync_reason = truncate_audit_reason("HYPERLIQUID_CLEARINGHOUSE_1_TO_1_SYNC");
    tx.exec_drop(
        r#"INSERT INTO order_audit (
               position_id, asset_id, client_order_id, side, from_state, to_state,
               requested_qty, filled_qty, limit_price, avg_fill_price,
               reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
           ) VALUES (
               :pid, :aid, :coid, :side, 'RECONCILED', 'FILLED_OPEN',
               :qty, :qty, :entry_px, :entry_px,
               0.00000000, :comm_usdc, 0.00000000, :reason, :ts, :ts
           )"#,
        params! {
            "pid" => new_id,
            "aid" => input.asset_id,
            "coid" => &coid,
            "side" => input.side.as_str(),
            "qty" => &qty_dec,
            "entry_px" => &entry_dec,
            "comm_usdc" => &comm_dec,
            "reason" => &sync_reason,
            "ts" => ts,
        },
    )
    .map_err(|e| {
        AegisError::Database(format!(
            "sync_open_position_from_exchange_v2 audit failed: {e}"
        ))
    })?;

    Ok(new_id)
}

fn row_to_position_record(r: &mysql::Row) -> PositionRecord {
    PositionRecord {
        id: row_get_i64(r, 0, 0),
        asset_id: row_get_i64(r, 1, 0),
        client_order_id: row_get_string(r, 2, ""),
        side: row_get_string(r, 3, "LONG"),
        status: row_get_string(r, 4, "WORKING"),
        fsm_state: row_get_string(r, 5, "WORKING_ALO"),
        entry_price: row_get_f64(r, 6, 0.0),
        current_price: row_get_f64(r, 7, 0.0),
        exit_price: row_get_opt_f64(r, 8),
        requested_quantity: row_get_f64(r, 9, 0.0),
        filled_quantity: row_get_f64(r, 10, 0.0),
        reserved_margin_usdc: row_get_f64(r, 11, 0.0),
        committed_notional_usdc: row_get_f64(r, 12, 0.0),
        portfolio_weight: row_get_f64(r, 13, 0.0),
        s1_weight_contrib: row_get_f64(r, 14, 0.0),
        s2_weight_contrib: row_get_f64(r, 15, 0.0),
        s3_weight_contrib: row_get_f64(r, 16, 0.0),
        protective_stop_px: row_get_f64(r, 17, 0.0),
        entry_r0: row_get_f64(r, 18, 0.0),
        protective_stop_oid: row_get_opt_string(r, 19),
        take_profit_px: row_get_f64(r, 20, 0.0),
        take_profit_oid: row_get_opt_string(r, 21),
        highest_price_seen: row_get_f64(r, 22, 0.0),
        lowest_price_seen: row_get_f64(r, 23, 0.0),
        chase_count: row_get_u32(r, 24, 0),
        realized_pnl_usdc: row_get_f64(r, 25, 0.0),
        unrealized_pnl_usdc: row_get_f64(r, 26, 0.0),
        fees_paid_usdc: row_get_f64(r, 27, 0.0),
        funding_accrued_usdc: row_get_f64(r, 28, 0.0),
        slippage_paid_usdc: row_get_f64(r, 29, 0.0),
        exchange_order_id: row_get_opt_string(r, 30),
        created_at_ms: row_get_i64(r, 31, 0),
        opened_at_ms: row_get_opt_i64(r, 32),
        closed_at_ms: row_get_opt_i64(r, 33),
        updated_at_ms: row_get_i64(r, 34, 0),
        exit_reason: row_get_opt_string(r, 35),
    }
}
