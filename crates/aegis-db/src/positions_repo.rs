//! Working orders & unified positions lifecycle repository (`aegis-db::positions_repo`).
//! Enforces the strict Order-to-Open Capital Lifecycle:
//! - `WORKING` orders reserve margin (`reserved_margin_usdc`), `committed_notional_usdc = 0.0`.
//! - `OPEN` positions commit funds (`committed_notional_usdc > 0.0`), `reserved_margin_usdc = 0.0`.

use crate::engine::DatabaseManager;
use aegis_core::{now_ms, round_dp, AegisError, OrderSide, PositionRecord, Result};
use mysql::params;
use mysql::prelude::*;

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

impl DatabaseManager {
    /// Places a new `WORKING` order into `positions`, reserving `reserved_margin_usdc` without spending cash yet,
    /// and writes the initial transition into `order_audit`.
    pub fn create_working_order(&self, input: &WorkingOrderInput) -> Result<i64> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start working order tx: {e}")))?;

        tx.exec_drop(
            r#"INSERT INTO positions (
                   asset_id, client_order_id, side, status, fsm_state, entry_price, current_price,
                   requested_quantity, filled_quantity, reserved_margin_usdc, committed_notional_usdc,
                   portfolio_weight, s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                   protective_stop_px, highest_price_seen, lowest_price_seen, chase_count,
                   exchange_order_id, created_at_ms, updated_at_ms
               ) VALUES (
                   :aid, :coid, :side, 'WORKING', 'WORKING_ALO', :px, :px,
                   :qty, 0.0, :res_usdc, 0.0,
                   :pw, :s1, :s2, :s3,
                   :stop_px, :px, :px, 0,
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
                "eoid" => &input.exchange_order_id,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("create_working_order insert failed: {e}")))?;

        let pos_id = tx.last_insert_id().unwrap_or(0) as i64;

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
                "reason" => &input.reason,
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
        let pos = self
            .get_position_by_id(position_id)?
            .ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let ts = now_ms();
        let committed_usdc = round_dp(fill_price * filled_quantity, 4);

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("promote_working_to_open tx failed: {e}")))?;

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
                   highest_price_seen = :px,
                   lowest_price_seen = :px,
                   fees_paid_usdc = fees_paid_usdc + :fees,
                   exchange_order_id = COALESCE(:eoid, exchange_order_id),
                   opened_at_ms = :ts,
                   updated_at_ms = :ts
               WHERE id = :pid"#,
            params! {
                "px" => fill_price,
                "qty" => filled_quantity,
                "comm_usdc" => committed_usdc,
                "stop_px" => protective_stop_px,
                "fees" => round_dp(fees_paid_usdc, 4),
                "eoid" => exchange_order_id,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| AegisError::Database(format!("promote_working_to_open update failed: {e}")))?;

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
                "aid" => pos.asset_id,
                "coid" => &pos.client_order_id,
                "side" => &pos.side,
                "from_st" => &pos.fsm_state,
                "req_qty" => pos.requested_quantity,
                "fill_qty" => filled_quantity,
                "lim_px" => pos.entry_price,
                "fill_px" => fill_price,
                "comm_usdc" => committed_usdc,
                "fees" => round_dp(fees_paid_usdc, 4),
                "reason" => reason,
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
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"UPDATE positions
               SET current_price = :mark,
                   protective_stop_px = :stop_px,
                   highest_price_seen = :high,
                   lowest_price_seen = :low,
                   unrealized_pnl_usdc = :upnl,
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'OPEN'"#,
            params! {
                "mark" => mark_price,
                "stop_px" => new_protective_stop_px,
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

    /// Closes an `OPEN` position, recording realized PnL and releasing `committed_notional_usdc`.
    pub fn close_position(
        &self,
        position_id: i64,
        exit_price: f64,
        fees_paid_usdc: f64,
        exit_reason: &str,
    ) -> Result<f64> {
        let pos = self
            .get_position_by_id(position_id)?
            .ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let side = OrderSide::parse(&pos.side);
        let qty = pos.filled_quantity.max(0.0);
        let gross_pnl = (exit_price - pos.entry_price) * qty * side.sign();
        let total_fees = pos.fees_paid_usdc + fees_paid_usdc;
        let net_realized_pnl = round_dp(gross_pnl - total_fees + pos.funding_accrued_usdc, 4);
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("close_position tx failed: {e}")))?;

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
               WHERE id = :pid"#,
            params! {
                "exit_px" => exit_price,
                "rpnl" => net_realized_pnl,
                "fees" => round_dp(total_fees, 4),
                "ts" => ts,
                "reason" => exit_reason,
                "pid" => position_id,
            },
        )
        .map_err(|e| AegisError::Database(format!("close_position update failed: {e}")))?;

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
                "aid" => pos.asset_id,
                "coid" => &pos.client_order_id,
                "side" => &pos.side,
                "from_st" => &pos.fsm_state,
                "req_qty" => pos.requested_quantity,
                "fill_qty" => qty,
                "exit_px" => exit_price,
                "fees" => round_dp(fees_paid_usdc, 4),
                "reason" => exit_reason,
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
        let pos = self
            .get_position_by_id(position_id)?
            .ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("cancel_working_order tx failed: {e}")))?;

        tx.exec_drop(
            r#"UPDATE positions
               SET status = 'CANCELLED',
                   fsm_state = 'CANCELLED',
                   reserved_margin_usdc = 0.0,
                   committed_notional_usdc = 0.0,
                   closed_at_ms = :ts,
                   updated_at_ms = :ts,
                   exit_reason = :reason
               WHERE id = :pid"#,
            params! { "ts" => ts, "reason" => reason, "pid" => position_id },
        )
        .map_err(|e| AegisError::Database(format!("cancel_working_order update failed: {e}")))?;

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
                "aid" => pos.asset_id,
                "coid" => &pos.client_order_id,
                "side" => &pos.side,
                "from_st" => &pos.fsm_state,
                "req_qty" => pos.requested_quantity,
                "lim_px" => pos.entry_price,
                "reason" => reason,
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
                          protective_stop_px, protective_stop_oid, highest_price_seen,
                          lowest_price_seen, chase_count, realized_pnl_usdc, unrealized_pnl_usdc,
                          fees_paid_usdc, funding_accrued_usdc, slippage_paid_usdc,
                          exchange_order_id, created_at_ms, opened_at_ms, closed_at_ms,
                          updated_at_ms, exit_reason
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
                          protective_stop_px, protective_stop_oid, highest_price_seen,
                          lowest_price_seen, chase_count, realized_pnl_usdc, unrealized_pnl_usdc,
                          fees_paid_usdc, funding_accrued_usdc, slippage_paid_usdc,
                          exchange_order_id, created_at_ms, opened_at_ms, closed_at_ms,
                          updated_at_ms, exit_reason
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
                          protective_stop_px, protective_stop_oid, highest_price_seen,
                          lowest_price_seen, chase_count, realized_pnl_usdc, unrealized_pnl_usdc,
                          fees_paid_usdc, funding_accrued_usdc, slippage_paid_usdc,
                          exchange_order_id, created_at_ms, opened_at_ms, closed_at_ms,
                          updated_at_ms, exit_reason
                   FROM positions
                   WHERE status IN ('CLOSED', 'CANCELLED')
                   ORDER BY updated_at_ms DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| AegisError::Database(format!("list_closed_positions failed: {e}")))?;
        Ok(rows.into_iter().map(row_to_position_record).collect())
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
        highest_price_seen: r.get(19).unwrap_or_default(),
        lowest_price_seen: r.get(20).unwrap_or_default(),
        chase_count: r.get(21).unwrap_or_default(),
        realized_pnl_usdc: r.get(22).unwrap_or_default(),
        unrealized_pnl_usdc: r.get(23).unwrap_or_default(),
        fees_paid_usdc: r.get(24).unwrap_or_default(),
        funding_accrued_usdc: r.get(25).unwrap_or_default(),
        slippage_paid_usdc: r.get(26).unwrap_or_default(),
        exchange_order_id: r.get::<Option<String>, _>(27).unwrap_or(None),
        created_at_ms: r.get(28).unwrap_or_default(),
        opened_at_ms: r.get::<Option<i64>, _>(29).unwrap_or(None),
        closed_at_ms: r.get::<Option<i64>, _>(30).unwrap_or(None),
        updated_at_ms: r.get(31).unwrap_or_default(),
        exit_reason: r.get::<Option<String>, _>(32).unwrap_or(None),
    }
}
