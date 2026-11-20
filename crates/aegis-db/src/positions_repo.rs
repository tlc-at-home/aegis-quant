//! Working orders & unified positions lifecycle repository (`aegis-db::positions_repo`).
//! Enforces the strict Order-to-Open Capital Lifecycle:
//! - `WORKING` orders reserve margin (`reserved_margin_usdc`), `committed_notional_usdc = 0.0`.
//! - `OPEN` positions commit funds (`committed_notional_usdc > 0.0`), `reserved_margin_usdc = 0.0`.

use crate::engine::DatabaseManager;
use crate::orders_repo::truncate_audit_reason;
use aegis_core::{
    compute_position_pnl_pct, now_ms, resolve_position_entry_notional_usdc, round_dp, AegisError,
    ClosedPositionView, OrderSide, PositionRecord, Result,
};
use mysql::params;
use mysql::prelude::*;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

static SYNC_COID_SEQ: AtomicU64 = AtomicU64::new(1);

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
    pub exchange_order_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct ReconciledClosedTradeInput {
    pub asset_id: i64,
    pub client_order_id: String,
    pub exchange_order_id: String,
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

type LockedCloseRow = (i64, String, String, String, String, f64, f64, f64, f64, f64);

#[derive(Debug, Clone)]
struct ActiveSyncRow {
    id: i64,
    status: String,
    side: String,
    protective_stop_px: f64,
    protective_stop_oid: Option<String>,
    take_profit_px: f64,
    take_profit_oid: Option<String>,
    client_order_id: String,
    fsm_state: String,
    requested_quantity: f64,
}

fn truncate_exit_reason(reason: &str) -> String {
    reason.chars().take(64).collect()
}

fn derive_take_profit_px(side: OrderSide, entry_price: f64, protective_stop_px: f64) -> f64 {
    if !entry_price.is_finite() || entry_price <= 0.0 {
        return 0.0;
    }
    let tp_dist = if protective_stop_px.is_finite() && protective_stop_px > 0.0 {
        ((entry_price - protective_stop_px).abs() * 2.0).max(entry_price * 0.016)
    } else {
        entry_price * 0.08
    };
    match side {
        OrderSide::Long => round_dp(entry_price + tp_dist, 8),
        OrderSide::Short => round_dp((entry_price - tp_dist).max(entry_price * 0.05), 8),
    }
}

impl DatabaseManager {
    /// Places a new `WORKING` order into `positions`, reserving `reserved_margin_usdc` without spending cash yet,
    /// and writes the initial transition into `order_audit`.
    pub fn create_working_order(&self, input: &WorkingOrderInput) -> Result<i64> {
        let ts = now_ms();
        let take_profit_px =
            derive_take_profit_px(input.side, input.limit_price, input.protective_stop_px);
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start working order tx: {e}")))?;

        tx.exec_drop(
            r#"INSERT INTO positions (
                   asset_id, client_order_id, side, status, fsm_state, entry_price, current_price,
                   requested_quantity, filled_quantity, reserved_margin_usdc, committed_notional_usdc,
                   portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                   protective_stop_px, take_profit_px, highest_price_seen, lowest_price_seen, chase_count,
                   exchange_order_id, created_at_ms, updated_at_ms
               ) VALUES (
                   :aid, :coid, :side, 'WORKING', 'WORKING_ALO', :px, :px,
                   :qty, 0.0, :res_usdc, 0.0,
                   :pw, :s1, :s2, :s3,
                   :stop_px, :tp_px, :px, :px, 0,
                   :eoid, :ts, :ts
               )"#,
            params! {
                "aid" => input.asset_id,
                "coid" => &input.client_order_id,
                "side" => input.side.as_str(),
                "px" => input.limit_price,
                "qty" => input.requested_quantity,
                "res_usdc" => round_dp(input.reserved_margin_usdc, 4),
                "pw" => input.portfolio_weight,
                "s1" => input.s1_weight_contrib,
                "s2" => input.s2_weight_contrib,
                "s3" => input.s3_weight_contrib,
                "stop_px" => input.protective_stop_px,
                "tp_px" => take_profit_px,
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
                   :qty, 0.0, :px, 0.0,
                   :res_usdc, 0.0, 0.0, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => pos_id,
                "aid" => input.asset_id,
                "coid" => &input.client_order_id,
                "side" => input.side.as_str(),
                "qty" => input.requested_quantity,
                "px" => input.limit_price,
                "res_usdc" => round_dp(input.reserved_margin_usdc, 4),
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
        exchange_order_id: Option<&str>,
        reason: &str,
    ) -> Result<()> {
        let ts = now_ms();
        let committed_usdc = round_dp(fill_price * filled_quantity, 4);

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("promote_working_to_open tx failed: {e}")))?;

        let row: Option<(i64, String, String, String, String, f64, f64)> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price, requested_quantity
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| AegisError::Database(format!("promote_working_to_open lock failed: {e}")))?;

        let (asset_id, client_order_id, side_str, status, fsm_state, limit_px, requested_qty) =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;

        if !status.eq_ignore_ascii_case("WORKING") {
            return Err(AegisError::Database(format!(
                "Cannot promote position {position_id}: status is '{status}' (expected 'WORKING')"
            )));
        }

        let side = OrderSide::parse(&side_str);
        let take_profit_px = derive_take_profit_px(side, fill_price, protective_stop_px);

        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'OPEN',
                   fsm_state = 'FILLED_OPEN',
                   entry_price = :px,
                   current_price = :px,
                   filled_quantity = :qty,
                   reserved_margin_usdc = 0.0,
                   committed_notional_usdc = :comm_usdc,
                   protective_stop_px = :stop_px,
                   take_profit_px = :tp_px,
                   highest_price_seen = :px,
                   lowest_price_seen = :px,
                   fees_paid_usdc = fees_paid_usdc + :fees,
                   exchange_order_id = COALESCE(:eoid, exchange_order_id),
                   opened_at_ms = COALESCE(opened_at_ms, :ts),
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'WORKING'"#,
            params! {
                "px" => fill_price,
                "qty" => filled_quantity,
                "comm_usdc" => committed_usdc,
                "stop_px" => protective_stop_px,
                "tp_px" => take_profit_px,
                "fees" => round_dp(fees_paid_usdc, 4),
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
                   0.0, :comm_usdc, :fees, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "req_qty" => requested_qty,
                "fill_qty" => filled_quantity,
                "lim_px" => limit_px,
                "fill_px" => fill_price,
                "comm_usdc" => committed_usdc,
                "fees" => round_dp(fees_paid_usdc, 4),
                "reason" => &safe_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("promote_working_to_open audit failed: {e}")))?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!("promote_working_to_open commit failed: {e}"))
        })?;
        Ok(())
    }

    /// Updates live mark price, high/low watermarks, unrealized PnL, and trailing protective stop on an `OPEN` holding.
    pub fn update_open_position_mark_and_stop(
        &self,
        position_id: i64,
        mark_price: f64,
        new_protective_stop_px: f64,
    ) -> Result<()> {
        let pos = self
            .get_position_by_id(position_id)?
            .ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let side = OrderSide::parse(&pos.side);
        let qty = pos.filled_quantity.max(0.0);
        let raw_upnl = (mark_price - pos.entry_price) * qty * side.sign();
        let upnl = round_dp(raw_upnl, 4);
        let high = pos.highest_price_seen.max(mark_price);
        let low = if pos.lowest_price_seen > 0.0 {
            pos.lowest_price_seen.min(mark_price)
        } else {
            mark_price
        };
        let eff_tp = if pos.take_profit_px > 0.0 {
            pos.take_profit_px
        } else {
            derive_take_profit_px(side, pos.entry_price, new_protective_stop_px)
        };
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"UPDATE positions
               SET current_price = :mark,
                   protective_stop_px = :stop_px,
                   take_profit_px = :tp_px,
                   highest_price_seen = :high,
                   lowest_price_seen = :low,
                   unrealized_pnl_usdc = :upnl,
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'OPEN'"#,
            params! {
                "mark" => mark_price,
                "stop_px" => new_protective_stop_px,
                "tp_px" => eff_tp,
                "high" => high,
                "low" => low,
                "upnl" => upnl,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("update_open_position_mark_and_stop failed: {e}"))
        })?;
        Ok(())
    }

    /// Batch updates live mark price, watermarks, unrealized PnL, and trailing stop on changed `OPEN` holdings
    /// inside a single MariaDB transaction, skipping rows whose tracked values did not change.
    pub fn update_open_positions_mark_and_stop_batch(
        &self,
        updates: &[(i64, f64, f64)],
    ) -> Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }
        type OpenMarkSnapshotRow = (i64, String, f64, f64, f64, f64, f64, f64, f64, f64);

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("open position batch tx failed: {e}")))?;

        let rows: Vec<OpenMarkSnapshotRow> = tx
            .query(
                r#"SELECT id, side, entry_price, current_price, filled_quantity,
                          protective_stop_px, take_profit_px, highest_price_seen,
                          lowest_price_seen, unrealized_pnl_usdc
                   FROM positions
                   WHERE status = 'OPEN'"#,
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "update_open_positions_mark_and_stop_batch select failed: {e}"
                ))
            })?;

        let by_id: std::collections::HashMap<i64, OpenMarkSnapshotRow> =
            rows.into_iter().map(|r| (r.0, r)).collect();

        let mut changed = Vec::new();
        for &(pid, mark_price, new_stop_px) in updates {
            if !mark_price.is_finite() || mark_price <= 0.0 {
                continue;
            }
            let Some((
                _,
                side_str,
                entry_px,
                cur_px,
                qty,
                cur_stop,
                cur_tp,
                hi_seen,
                lo_seen,
                cur_upnl,
            )) = by_id.get(&pid)
            else {
                continue;
            };
            let side = OrderSide::parse(side_str);
            let eff_qty = qty.max(0.0);
            let upnl = round_dp((mark_price - *entry_px) * eff_qty * side.sign(), 4);
            let high = hi_seen.max(mark_price);
            let low = if *lo_seen > 0.0 {
                lo_seen.min(mark_price)
            } else {
                mark_price
            };
            let eff_tp = if *cur_tp > 0.0 {
                *cur_tp
            } else {
                derive_take_profit_px(side, *entry_px, new_stop_px)
            };

            let unchanged = (*cur_px - mark_price).abs() <= 1e-10
                && (*cur_stop - new_stop_px).abs() <= 1e-10
                && (*cur_tp - eff_tp).abs() <= 1e-10
                && (*hi_seen - high).abs() <= 1e-10
                && (*lo_seen - low).abs() <= 1e-10
                && (*cur_upnl - upnl).abs() <= 1e-10;
            if !unchanged {
                changed.push((pid, mark_price, new_stop_px, eff_tp, high, low, upnl));
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
                       protective_stop_px = :stop_px,
                       take_profit_px = :tp_px,
                       highest_price_seen = :high,
                       lowest_price_seen = :low,
                       unrealized_pnl_usdc = :upnl,
                       updated_at_ms = :ts
                   WHERE id = :pid AND status = 'OPEN'"#,
            )
            .map_err(|e| AegisError::Database(format!("open position batch prep failed: {e}")))?;

        tx.exec_batch(
            &stmt,
            changed
                .iter()
                .map(|&(pid, mark, stop_px, tp_px, high, low, upnl)| {
                    params! {
                        "mark" => mark,
                        "stop_px" => stop_px,
                        "tp_px" => tp_px,
                        "high" => high,
                        "low" => low,
                        "upnl" => upnl,
                        "ts" => ts,
                        "pid" => pid,
                    }
                }),
        )
        .map_err(|e| AegisError::Database(format!("open position batch exec failed: {e}")))?;
        drop(stmt);

        tx.commit()
            .map_err(|e| AegisError::Database(format!("open position batch commit failed: {e}")))?;
        Ok(changed.len())
    }

    /// Persists stop-loss and take-profit trigger prices and exchange order IDs on an active position.
    pub fn update_position_tpsl_orders(
        &self,
        position_id: i64,
        protective_stop_px: f64,
        protective_stop_oid: Option<&str>,
        take_profit_px: f64,
        take_profit_oid: Option<&str>,
    ) -> Result<()> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"UPDATE positions
               SET protective_stop_px = CASE WHEN :stop_px > 0 THEN :stop_px ELSE protective_stop_px END,
                   protective_stop_oid = :sl_oid,
                   take_profit_px = CASE WHEN :tp_px > 0 THEN :tp_px ELSE take_profit_px END,
                   take_profit_oid = :tp_oid,
                   updated_at_ms = :ts
               WHERE id = :pid AND status IN ('WORKING', 'OPEN')"#,
            params! {
                "stop_px" => protective_stop_px,
                "sl_oid" => protective_stop_oid,
                "tp_px" => take_profit_px,
                "tp_oid" => take_profit_oid,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| AegisError::Database(format!("update_position_tpsl_orders failed: {e}")))?;
        Ok(())
    }

    /// Closes an `OPEN` position, recording realized PnL and releasing `committed_notional_usdc`.
    pub fn close_position(
        &self,
        position_id: i64,
        exit_price: f64,
        fees_paid_usdc: f64,
        exit_reason: &str,
    ) -> Result<f64> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("close_position tx failed: {e}")))?;

        let row: Option<LockedCloseRow> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price,
                          requested_quantity, filled_quantity, fees_paid_usdc, funding_accrued_usdc
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| AegisError::Database(format!("close_position lock failed: {e}")))?;

        let (
            asset_id,
            client_order_id,
            side_str,
            status,
            fsm_state,
            entry_price,
            requested_quantity,
            filled_quantity,
            existing_fees_paid,
            funding_accrued_usdc,
        ) = row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;

        if !status.eq_ignore_ascii_case("OPEN") {
            return Err(AegisError::Database(format!(
                "Cannot close position {position_id}: status is '{status}' (expected 'OPEN')"
            )));
        }

        let side = OrderSide::parse(&side_str);
        let qty = filled_quantity.max(0.0);
        let gross_pnl = (exit_price - entry_price) * qty * side.sign();
        let total_fees = existing_fees_paid + fees_paid_usdc;
        let net_realized_pnl = round_dp(gross_pnl - total_fees + funding_accrued_usdc, 4);

        let safe_exit_reason = truncate_exit_reason(exit_reason);
        let safe_audit_reason = truncate_audit_reason(exit_reason);

        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'CLOSED',
                   fsm_state = 'CLOSED',
                   current_price = :exit_px,
                   exit_price = :exit_px,
                   reserved_margin_usdc = 0.0,
                   committed_notional_usdc = 0.0,
                   realized_pnl_usdc = :rpnl,
                   unrealized_pnl_usdc = 0.0,
                   fees_paid_usdc = :fees,
                   closed_at_ms = :ts,
                   updated_at_ms = :ts,
                   exit_reason = :reason
               WHERE id = :pid AND status = 'OPEN'"#,
            params! {
                "exit_px" => exit_price,
                "rpnl" => net_realized_pnl,
                "fees" => round_dp(total_fees, 4),
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
                   0.0, 0.0, :fees, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "req_qty" => requested_quantity,
                "fill_qty" => qty,
                "exit_px" => exit_price,
                "fees" => round_dp(fees_paid_usdc, 4),
                "reason" => &safe_audit_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("close_position audit failed: {e}")))?;

        tx.commit()
            .map_err(|e| AegisError::Database(format!("close_position commit failed: {e}")))?;
        Ok(net_realized_pnl)
    }

    /// Cancels a `WORKING` order and releases its `reserved_margin_usdc` back to available cash.
    pub fn cancel_working_order(&self, position_id: i64, reason: &str) -> Result<()> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("cancel_working_order tx failed: {e}")))?;

        let row: Option<(i64, String, String, String, String, f64, f64)> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state, entry_price, requested_quantity
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| AegisError::Database(format!("cancel_working_order lock failed: {e}")))?;

        let (asset_id, client_order_id, side_str, status, fsm_state, entry_price, requested_qty) =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;

        if !status.eq_ignore_ascii_case("WORKING") {
            return Err(AegisError::Database(format!(
                "Cannot cancel working order {position_id}: status is '{status}' (expected 'WORKING')"
            )));
        }

        let safe_exit_reason = truncate_exit_reason(reason);
        let safe_audit_reason = truncate_audit_reason(reason);

        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'CANCELLED',
                   fsm_state = 'CANCELLED',
                   reserved_margin_usdc = 0.0,
                   committed_notional_usdc = 0.0,
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
                   :req_qty, 0.0, :lim_px, 0.0,
                   0.0, 0.0, 0.0, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => client_order_id,
                "side" => side_str,
                "from_st" => fsm_state,
                "req_qty" => requested_qty,
                "lim_px" => entry_price,
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
                          protective_stop_px, protective_stop_oid, take_profit_px, take_profit_oid,
                          highest_price_seen, lowest_price_seen, chase_count, realized_pnl_usdc,
                          unrealized_pnl_usdc, fees_paid_usdc, funding_accrued_usdc,
                          slippage_paid_usdc, exchange_order_id, created_at_ms, opened_at_ms,
                          closed_at_ms, updated_at_ms, exit_reason
                   FROM positions
                   WHERE id = :pid"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| AegisError::Database(format!("get_position_by_id failed: {e}")))?;
        Ok(row.map(row_to_position_record))
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
                          protective_stop_px, protective_stop_oid, take_profit_px, take_profit_oid,
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
        Ok(row.map(row_to_position_record))
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
                          protective_stop_px, protective_stop_oid, take_profit_px, take_profit_oid,
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
        Ok(rows.into_iter().map(row_to_position_record).collect())
    }

    /// Lists recent `CLOSED` trades joined with `exchange_assets`, enriching each trade with
    /// `entry_notional_usdc` and `realized_pnl_pct` (using `entry_price * filled_quantity`
    /// because `CLOSED` rows in MariaDB zero out `committed_notional_usdc`).
    pub fn list_recent_closed_positions_view(
        &self,
        limit: usize,
    ) -> Result<Vec<ClosedPositionView>> {
        let mut conn = self.get_conn()?;
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
                   ORDER BY COALESCE(p.closed_at_ms, p.updated_at_ms) DESC, p.id DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| {
                AegisError::Database(format!("list_recent_closed_positions_view failed: {e}"))
            })?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let entry_price: f64 = r.get(6).unwrap_or_default();
                let exit_price: f64 = r.get(7).unwrap_or_default();
                let filled_quantity: f64 = r.get(8).unwrap_or_default();
                let committed_notional_usdc: f64 = r.get(9).unwrap_or_default();
                let realized_pnl_usdc: f64 = r.get(10).unwrap_or_default();
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
                    position_id: r.get(0).unwrap_or_default(),
                    asset_id: r.get(1).unwrap_or_default(),
                    symbol: r.get(2).unwrap_or_default(),
                    coin: r.get(3).unwrap_or_default(),
                    client_order_id: r.get(4).unwrap_or_default(),
                    side: r.get(5).unwrap_or_else(|| "LONG".to_string()),
                    entry_price,
                    exit_price,
                    quantity: filled_quantity,
                    filled_quantity,
                    entry_notional_usdc,
                    realized_pnl_usdc,
                    realized_pnl_pct,
                    fees_paid_usdc: r.get(11).unwrap_or_default(),
                    funding_accrued_usdc: r.get(12).unwrap_or_default(),
                    opened_at_ms: r.get::<Option<i64>, _>(13).unwrap_or(None),
                    closed_at_ms: r.get(14).unwrap_or_default(),
                    exit_reason: r.get::<Option<String>, _>(15).unwrap_or(None),
                }
            })
            .collect())
    }

    /// Synchronizes MariaDB `positions` 1:1 with a live Hyperliquid `clearinghouseState` position for `asset_id`.
    /// Delegates to `sync_open_position_from_exchange_v2` with zero funding delta and no explicit `opened_at_ms` override.
    #[allow(clippy::too_many_arguments)]
    pub fn sync_open_position_from_exchange(
        &self,
        asset_id: i64,
        coin: &str,
        side: OrderSide,
        entry_price: f64,
        mark_price: f64,
        quantity: f64,
        committed_notional_usdc: f64,
        unrealized_pnl_usdc: f64,
        portfolio_weight: f64,
        protective_stop_px: f64,
        take_profit_px: f64,
    ) -> Result<(i64, bool)> {
        self.sync_open_position_from_exchange_v2(
            asset_id,
            coin,
            side,
            entry_price,
            mark_price,
            quantity,
            committed_notional_usdc,
            unrealized_pnl_usdc,
            portfolio_weight,
            protective_stop_px,
            take_profit_px,
            0.0,
            None,
        )
    }

    /// Synchronizes MariaDB `positions` 1:1 with a live Hyperliquid `clearinghouseState` position for `asset_id`,
    /// preserving exchange SL/TP OIDs, promoting same-side `WORKING` orders in-place, and storing `funding_accrued_usdc`
    /// and true `userFills` `opened_at_ms`.
    ///
    /// Returns `(position_id, was_newly_adopted)`.
    #[allow(clippy::too_many_arguments)]
    pub fn sync_open_position_from_exchange_v2(
        &self,
        asset_id: i64,
        coin: &str,
        side: OrderSide,
        entry_price: f64,
        mark_price: f64,
        quantity: f64,
        committed_notional_usdc: f64,
        unrealized_pnl_usdc: f64,
        portfolio_weight: f64,
        protective_stop_px: f64,
        take_profit_px: f64,
        funding_accrued_usdc: f64,
        opened_at_ms_opt: Option<i64>,
    ) -> Result<(i64, bool)> {
        let ts = now_ms();
        let opened_at_param = opened_at_ms_opt.filter(|&t| t > 0);
        let funding_rounded = round_dp(funding_accrued_usdc, 4);

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!(
                    "sync_open_position_from_exchange_v2 tx failed: {e}"
                ))
            })?;

        let raw_active: Vec<mysql::Row> = tx
            .exec(
                r#"SELECT id, status, side, protective_stop_px, protective_stop_oid,
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

        let active_rows: Vec<ActiveSyncRow> = raw_active
            .into_iter()
            .map(|r| ActiveSyncRow {
                id: r.get(0).unwrap_or_default(),
                status: r.get(1).unwrap_or_default(),
                side: r.get(2).unwrap_or_else(|| "LONG".to_string()),
                protective_stop_px: r.get(3).unwrap_or_default(),
                protective_stop_oid: r.get::<Option<String>, _>(4).unwrap_or(None),
                take_profit_px: r.get(5).unwrap_or_default(),
                take_profit_oid: r.get::<Option<String>, _>(6).unwrap_or(None),
                client_order_id: r.get(7).unwrap_or_default(),
                fsm_state: r.get(8).unwrap_or_default(),
                requested_quantity: r.get(9).unwrap_or_default(),
            })
            .collect();

        let open_row_opt = active_rows
            .iter()
            .find(|r| r.status.eq_ignore_ascii_case("OPEN"))
            .cloned();

        let high = entry_price.max(mark_price);
        let low = entry_price.min(mark_price).max(0.0);

        if let Some(open_row) = open_row_opt {
            for other in &active_rows {
                if other.id != open_row.id {
                    tx.exec_drop(
                        r#"UPDATE positions
                           SET status = 'CANCELLED',
                               fsm_state = 'CANCELLED',
                               reserved_margin_usdc = 0.0,
                               committed_notional_usdc = 0.0,
                               closed_at_ms = :ts,
                               updated_at_ms = :ts,
                               exit_reason = 'DEDUP_EXCHANGE_RECONCILE'
                           WHERE id = :pid AND status IN ('OPEN', 'WORKING')"#,
                        params! { "ts" => ts, "pid" => other.id },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!("dedup cancel position failed: {e}"))
                    })?;
                }
            }

            let side_matches = open_row.side.eq_ignore_ascii_case(side.as_str());

            let stop_is_valid_non_breached = side_matches
                && open_row.protective_stop_px > 0.0
                && mark_price > 0.0
                && match side {
                    OrderSide::Long => open_row.protective_stop_px < mark_price,
                    OrderSide::Short => open_row.protective_stop_px > mark_price,
                };
            let eff_stop = if stop_is_valid_non_breached {
                open_row.protective_stop_px
            } else {
                protective_stop_px
            };
            let eff_sl_oid = if side_matches && eff_stop > 0.0 {
                open_row.protective_stop_oid.clone()
            } else {
                None
            };

            let tp_is_valid_non_breached = side_matches
                && open_row.take_profit_px > 0.0
                && mark_price > 0.0
                && match side {
                    OrderSide::Long => open_row.take_profit_px > mark_price,
                    OrderSide::Short => open_row.take_profit_px < mark_price,
                };
            let eff_tp = if tp_is_valid_non_breached {
                open_row.take_profit_px
            } else if take_profit_px > 0.0 {
                take_profit_px
            } else {
                derive_take_profit_px(side, entry_price, eff_stop)
            };
            let eff_tp_oid = if side_matches && eff_tp > 0.0 {
                open_row.take_profit_oid.clone()
            } else {
                None
            };

            tx.exec_drop(
                r#"UPDATE positions
                   SET side = :side,
                       status = 'OPEN',
                       fsm_state = 'FILLED_OPEN',
                       entry_price = :entry_px,
                       current_price = :mark_px,
                       requested_quantity = :qty,
                       filled_quantity = :qty,
                       reserved_margin_usdc = 0.0,
                       committed_notional_usdc = :comm_usdc,
                       unrealized_pnl_usdc = :upnl,
                       funding_accrued_usdc = :funding,
                       portfolio_weight = :pw,
                       protective_stop_px = :stop_px,
                       protective_stop_oid = :sl_oid,
                       take_profit_px = :tp_px,
                       take_profit_oid = :tp_oid,
                       highest_price_seen = GREATEST(highest_price_seen, :high),
                       lowest_price_seen = CASE WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, :low) ELSE :low END,
                       opened_at_ms = COALESCE(:opened_at_opt, opened_at_ms, :ts),
                       updated_at_ms = :ts
                   WHERE id = :pid AND status = 'OPEN'"#,
                params! {
                    "side" => side.as_str(),
                    "entry_px" => entry_price,
                    "mark_px" => mark_price,
                    "qty" => quantity,
                    "comm_usdc" => round_dp(committed_notional_usdc, 4),
                    "upnl" => round_dp(unrealized_pnl_usdc, 4),
                    "funding" => funding_rounded,
                    "pw" => round_dp(portfolio_weight, 4),
                    "stop_px" => eff_stop,
                    "sl_oid" => eff_sl_oid,
                    "tp_px" => eff_tp,
                    "tp_oid" => eff_tp_oid,
                    "high" => high,
                    "low" => low,
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

            tx.commit().map_err(|e| {
                AegisError::Database(format!(
                    "sync_open_position_from_exchange_v2 commit failed: {e}"
                ))
            })?;
            return Ok((open_row.id, false));
        }

        // If no OPEN row exists, promote an existing same-side WORKING row in-place to preserve
        // client_order_id, sleeve weight attributions, protective_stop_px, and order_audit history.
        let same_side_working = active_rows
            .iter()
            .find(|r| {
                r.status.eq_ignore_ascii_case("WORKING")
                    && r.side.eq_ignore_ascii_case(side.as_str())
            })
            .cloned();

        if let Some(w) = same_side_working {
            for other in &active_rows {
                if other.id != w.id {
                    tx.exec_drop(
                        r#"UPDATE positions
                           SET status = 'CANCELLED',
                               fsm_state = 'CANCELLED',
                               reserved_margin_usdc = 0.0,
                               committed_notional_usdc = 0.0,
                               closed_at_ms = :ts,
                               updated_at_ms = :ts,
                               exit_reason = 'DEDUP_EXCHANGE_RECONCILE'
                           WHERE id = :pid AND status = 'WORKING'"#,
                        params! { "ts" => ts, "pid" => other.id },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!("dedup cancel working failed: {e}"))
                    })?;
                }
            }

            let stop_valid = w.protective_stop_px > 0.0
                && mark_price > 0.0
                && match side {
                    OrderSide::Long => w.protective_stop_px < mark_price,
                    OrderSide::Short => w.protective_stop_px > mark_price,
                };
            let eff_stop = if stop_valid {
                w.protective_stop_px
            } else {
                protective_stop_px
            };

            let tp_valid = w.take_profit_px > 0.0
                && mark_price > 0.0
                && match side {
                    OrderSide::Long => w.take_profit_px > mark_price,
                    OrderSide::Short => w.take_profit_px < mark_price,
                };
            let eff_tp = if tp_valid {
                w.take_profit_px
            } else if take_profit_px > 0.0 {
                take_profit_px
            } else {
                derive_take_profit_px(side, entry_price, eff_stop)
            };

            let comm_usdc = round_dp(committed_notional_usdc, 4);
            tx.exec_drop(
                r#"UPDATE positions
                   SET status = 'OPEN',
                       fsm_state = 'FILLED_OPEN',
                       entry_price = :entry_px,
                       current_price = :mark_px,
                       requested_quantity = :qty,
                       filled_quantity = :qty,
                       reserved_margin_usdc = 0.0,
                       committed_notional_usdc = :comm_usdc,
                       unrealized_pnl_usdc = :upnl,
                       funding_accrued_usdc = :funding,
                       portfolio_weight = :pw,
                       protective_stop_px = :stop_px,
                       take_profit_px = :tp_px,
                       highest_price_seen = GREATEST(highest_price_seen, :high),
                       lowest_price_seen = CASE WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, :low) ELSE :low END,
                       opened_at_ms = COALESCE(:opened_at_opt, opened_at_ms, :ts),
                       updated_at_ms = :ts
                   WHERE id = :pid AND status = 'WORKING'"#,
                params! {
                    "entry_px" => entry_price,
                    "mark_px" => mark_price,
                    "qty" => quantity,
                    "comm_usdc" => comm_usdc,
                    "upnl" => round_dp(unrealized_pnl_usdc, 4),
                    "funding" => funding_rounded,
                    "pw" => round_dp(portfolio_weight, 4),
                    "stop_px" => eff_stop,
                    "tp_px" => eff_tp,
                    "high" => high,
                    "low" => low,
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
                       0.0, :comm_usdc, 0.0, :reason, :ts, :ts
                   )"#,
                params! {
                    "pid" => w.id,
                    "aid" => asset_id,
                    "coid" => &w.client_order_id,
                    "side" => side.as_str(),
                    "from_st" => &w.fsm_state,
                    "req_qty" => w.requested_quantity,
                    "qty" => quantity,
                    "entry_px" => entry_price,
                    "comm_usdc" => comm_usdc,
                    "reason" => &promote_reason,
                    "ts" => ts,
                },
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "sync_open_position_from_exchange_v2 working promote audit failed: {e}"
                ))
            })?;

            tx.commit().map_err(|e| {
                AegisError::Database(format!(
                    "sync_open_position_from_exchange_v2 working promote commit failed: {e}"
                ))
            })?;
            return Ok((w.id, true));
        }

        // Cancel any remaining opposite-side WORKING rows before inserting an adopted OPEN position
        for other in &active_rows {
            tx.exec_drop(
                r#"UPDATE positions
                   SET status = 'CANCELLED',
                       fsm_state = 'CANCELLED',
                       reserved_margin_usdc = 0.0,
                       committed_notional_usdc = 0.0,
                       closed_at_ms = :ts,
                       updated_at_ms = :ts,
                       exit_reason = 'REPLACED_BY_EXCHANGE_OPEN'
                   WHERE id = :pid AND status = 'WORKING'"#,
                params! { "ts" => ts, "pid" => other.id },
            )
            .map_err(|e| AegisError::Database(format!("cancel working before sync failed: {e}")))?;
        }

        let eff_tp = if take_profit_px > 0.0 {
            take_profit_px
        } else {
            derive_take_profit_px(side, entry_price, protective_stop_px)
        };
        let seq = SYNC_COID_SEQ.fetch_add(1, Ordering::Relaxed);
        let coin_slug: String = coin.trim().to_ascii_lowercase().chars().take(24).collect();
        let coid = format!("hl-sync-{coin_slug}-{ts}-{seq}");
        let eff_opened_at = opened_at_param.unwrap_or(ts);
        let eff_created_at = eff_opened_at.min(ts);

        tx.exec_drop(
            r#"INSERT INTO positions (
                   asset_id, client_order_id, side, status, fsm_state, entry_price, current_price,
                   requested_quantity, filled_quantity, reserved_margin_usdc, committed_notional_usdc,
                   portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                   protective_stop_px, take_profit_px, highest_price_seen, lowest_price_seen, chase_count,
                   unrealized_pnl_usdc, funding_accrued_usdc, exchange_order_id, created_at_ms, opened_at_ms, updated_at_ms
               ) VALUES (
                   :aid, :coid, :side, 'OPEN', 'FILLED_OPEN', :entry_px, :mark_px,
                   :qty, :qty, 0.0, :comm_usdc,
                   :pw, :pw, 0.0, 0.0,
                   :stop_px, :tp_px, :high, :low, 0,
                   :upnl, :funding, 'hl-reconciled', :cts, :ots, :ts
               )"#,
            params! {
                "aid" => asset_id,
                "coid" => &coid,
                "side" => side.as_str(),
                "entry_px" => entry_price,
                "mark_px" => mark_price,
                "qty" => quantity,
                "comm_usdc" => round_dp(committed_notional_usdc, 4),
                "pw" => round_dp(portfolio_weight, 4),
                "stop_px" => protective_stop_px,
                "tp_px" => eff_tp,
                "high" => high,
                "low" => low,
                "upnl" => round_dp(unrealized_pnl_usdc, 4),
                "funding" => funding_rounded,
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
                   0.0, :comm_usdc, 0.0, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => new_id,
                "aid" => asset_id,
                "coid" => &coid,
                "side" => side.as_str(),
                "qty" => quantity,
                "entry_px" => entry_price,
                "comm_usdc" => round_dp(committed_notional_usdc, 4),
                "reason" => &sync_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!(
                "sync_open_position_from_exchange_v2 audit failed: {e}"
            ))
        })?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!(
                "sync_open_position_from_exchange_v2 commit failed: {e}"
            ))
        })?;
        Ok((new_id, true))
    }

    /// Reconciles historical `CLOSED` positions in MariaDB 1:1 with Hyperliquid `userFills` closed trades:
    /// - Matches existing local `CLOSED` rows by `client_order_id`, `exchange_order_id`, or
    ///   `(asset_id, side, ABS(COALESCE(closed_at_ms, updated_at_ms) - closed_at_ms) <= 30000)` and updates
    ///   them in-place—preserving real `exit_reason`, `opened_at_ms`, sleeve weights, protective stops,
    ///   `committed_notional_usdc`, and `order_audit` FKs without ever deleting local history.
    /// - Inserts new `hl-closed-*` rows only when no matching local `CLOSED` row exists.
    pub fn reconcile_closed_positions_from_exchange_fills(
        &self,
        trades: &[ReconciledClosedTradeInput],
    ) -> Result<usize> {
        if trades.is_empty() {
            return Ok(0);
        }
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!(
                    "reconcile_closed_positions_from_exchange_fills tx failed: {e}"
                ))
            })?;

        let mut matched_ids: HashSet<i64> = HashSet::with_capacity(trades.len());
        let mut processed = 0usize;

        for t in trades {
            let entry_px = round_dp(t.entry_price, 8);
            let exit_px = round_dp(t.exit_price, 8);
            let qty = round_dp(t.filled_quantity, 8);
            let high = round_dp(t.entry_price.max(t.exit_price), 8);
            let low = round_dp(t.entry_price.min(t.exit_price).max(0.0), 8);
            let rpnl = round_dp(t.realized_pnl_usdc, 4);
            let fees = round_dp(t.fees_paid_usdc, 4);
            let funding = round_dp(t.funding_accrued_usdc, 4);

            // 1. Exact client_order_id check (must never clobber an active OPEN/WORKING position)
            let by_coid: Option<(i64, String)> = tx
                .exec_first(
                    "SELECT id, status FROM positions WHERE client_order_id = :coid LIMIT 1 FOR UPDATE",
                    params! { "coid" => &t.client_order_id },
                )
                .map_err(|e| {
                    AegisError::Database(format!(
                        "reconcile_closed lookup by coid '{}' failed: {e}",
                        t.client_order_id
                    ))
                })?;

            let mut matched_id: Option<i64> = None;
            if let Some((cid_pid, cid_status)) = by_coid {
                if !cid_status.eq_ignore_ascii_case("CLOSED") {
                    continue;
                }
                matched_id = Some(cid_pid);
            }

            // 2. Match by exchange_order_id on CLOSED rows
            if matched_id.is_none() && !t.exchange_order_id.trim().is_empty() {
                let eoid_cands: Vec<i64> = tx
                    .exec(
                        r#"SELECT id FROM positions
                           WHERE status = 'CLOSED' AND exchange_order_id = :eoid
                           ORDER BY id DESC
                           LIMIT 5
                           FOR UPDATE"#,
                        params! { "eoid" => &t.exchange_order_id },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!(
                            "reconcile_closed lookup by eoid '{}' failed: {e}",
                            t.exchange_order_id
                        ))
                    })?;
                matched_id = eoid_cands.into_iter().find(|id| !matched_ids.contains(id));
            }

            // 3. Match by (asset_id, side, <=30s close window) on CLOSED rows
            if matched_id.is_none() {
                let win_cands: Vec<i64> = tx
                    .exec(
                        r#"SELECT id FROM positions
                           WHERE status = 'CLOSED'
                             AND asset_id = :aid
                             AND side = :side
                             AND ABS(COALESCE(closed_at_ms, updated_at_ms) - :clts) <= 30000
                           ORDER BY CASE WHEN client_order_id NOT LIKE 'hl-closed-%' THEN 0 ELSE 1 END,
                                    ABS(COALESCE(closed_at_ms, updated_at_ms) - :clts) ASC,
                                    id DESC
                           LIMIT 5
                           FOR UPDATE"#,
                        params! {
                            "aid" => t.asset_id,
                            "side" => t.side.as_str(),
                            "clts" => t.closed_at_ms,
                        },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!(
                            "reconcile_closed window lookup failed for '{}': {e}",
                            t.client_order_id
                        ))
                    })?;
                matched_id = win_cands.into_iter().find(|id| !matched_ids.contains(id));
            }

            if let Some(pid) = matched_id {
                matched_ids.insert(pid);
                let safe_exit_reason = truncate_exit_reason(&t.exit_reason);
                tx.exec_drop(
                    r#"UPDATE positions
                       SET entry_price = :entry_px,
                           current_price = :exit_px,
                           exit_price = :exit_px,
                           requested_quantity = :qty,
                           filled_quantity = :qty,
                           highest_price_seen = GREATEST(highest_price_seen, :high),
                           lowest_price_seen = CASE WHEN lowest_price_seen > 0 THEN LEAST(lowest_price_seen, :low) ELSE :low END,
                           realized_pnl_usdc = :rpnl,
                           unrealized_pnl_usdc = 0.0,
                           fees_paid_usdc = :fees,
                           funding_accrued_usdc = :funding,
                           exchange_order_id = COALESCE(NULLIF(TRIM(exchange_order_id), ''), :eoid),
                           opened_at_ms = COALESCE(opened_at_ms, :cts),
                           closed_at_ms = :clts,
                           updated_at_ms = :clts,
                           exit_reason = COALESCE(NULLIF(TRIM(exit_reason), ''), :reason)
                       WHERE id = :pid AND status = 'CLOSED'"#,
                    params! {
                        "entry_px" => entry_px,
                        "exit_px" => exit_px,
                        "qty" => qty,
                        "high" => high,
                        "low" => low,
                        "rpnl" => rpnl,
                        "fees" => fees,
                        "funding" => funding,
                        "eoid" => &t.exchange_order_id,
                        "cts" => t.created_at_ms,
                        "clts" => t.closed_at_ms,
                        "reason" => &safe_exit_reason,
                        "pid" => pid,
                    },
                )
                .map_err(|e| {
                    AegisError::Database(format!(
                        "Failed to update matched closed trade #{pid} ('{}'): {e}",
                        t.client_order_id
                    ))
                })?;
            } else {
                let safe_exit_reason = truncate_exit_reason(&t.exit_reason);
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
                           :exit_px, :qty, :qty, 0.0,
                           0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, :high, :low,
                           0, :rpnl, 0.0, :fees,
                           :funding, :eoid, :cts, :cts,
                           :clts, :clts, :reason
                       )"#,
                    params! {
                        "aid" => t.asset_id,
                        "coid" => &t.client_order_id,
                        "side" => t.side.as_str(),
                        "entry_px" => entry_px,
                        "exit_px" => exit_px,
                        "qty" => qty,
                        "high" => high,
                        "low" => low,
                        "rpnl" => rpnl,
                        "fees" => fees,
                        "funding" => funding,
                        "eoid" => &t.exchange_order_id,
                        "cts" => t.created_at_ms,
                        "clts" => t.closed_at_ms,
                        "reason" => &safe_exit_reason,
                    },
                )
                .map_err(|e| {
                    AegisError::Database(format!(
                        "Failed to insert reconciled closed trade '{}': {e}",
                        t.client_order_id
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

        // Remove any older duplicate terminal row holding the same client_order_id before tagging
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
        let mut conn = self.get_conn()?;
        let db_max: Option<Option<i64>> = conn
            .query_first(
                "SELECT MAX(created_at_ms) FROM positions WHERE client_order_id LIKE 'v2-%'",
            )
            .map_err(|e| {
                AegisError::Database(format!("last_position_entry_timestamp_ms failed: {e}"))
            })?;
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
}

fn row_to_position_record(r: mysql::Row) -> PositionRecord {
    PositionRecord {
        id: r.get(0).unwrap_or_default(),
        asset_id: r.get(1).unwrap_or_default(),
        client_order_id: r.get(2).unwrap_or_default(),
        side: r.get(3).unwrap_or_else(|| "LONG".to_string()),
        status: r.get(4).unwrap_or_else(|| "WORKING".to_string()),
        fsm_state: r.get(5).unwrap_or_else(|| "WORKING_ALO".to_string()),
        entry_price: r.get(6).unwrap_or_default(),
        current_price: r.get(7).unwrap_or_default(),
        exit_price: r.get::<Option<f64>, _>(8).unwrap_or(None),
        requested_quantity: r.get(9).unwrap_or_default(),
        filled_quantity: r.get(10).unwrap_or_default(),
        reserved_margin_usdc: r.get(11).unwrap_or_default(),
        committed_notional_usdc: r.get(12).unwrap_or_default(),
        portfolio_weight: r.get(13).unwrap_or_default(),
        s1_weight_contrib: r.get(14).unwrap_or_default(),
        s2_weight_contrib: r.get(15).unwrap_or_default(),
        s3_weight_contrib: r.get(16).unwrap_or_default(),
        protective_stop_px: r.get(17).unwrap_or_default(),
        protective_stop_oid: r.get::<Option<String>, _>(18).unwrap_or(None),
        take_profit_px: r.get(19).unwrap_or_default(),
        take_profit_oid: r.get::<Option<String>, _>(20).unwrap_or(None),
        highest_price_seen: r.get(21).unwrap_or_default(),
        lowest_price_seen: r.get(22).unwrap_or_default(),
        chase_count: r.get(23).unwrap_or_default(),
        realized_pnl_usdc: r.get(24).unwrap_or_default(),
        unrealized_pnl_usdc: r.get(25).unwrap_or_default(),
        fees_paid_usdc: r.get(26).unwrap_or_default(),
        funding_accrued_usdc: r.get(27).unwrap_or_default(),
        slippage_paid_usdc: r.get(28).unwrap_or_default(),
        exchange_order_id: r.get::<Option<String>, _>(29).unwrap_or(None),
        created_at_ms: r.get(30).unwrap_or_default(),
        opened_at_ms: r.get::<Option<i64>, _>(31).unwrap_or(None),
        closed_at_ms: r.get::<Option<i64>, _>(32).unwrap_or(None),
        updated_at_ms: r.get(33).unwrap_or_default(),
        exit_reason: r.get::<Option<String>, _>(34).unwrap_or(None),
    }
}
