//! 1-Second Maker Limit Chaser order audit & state transition repository (`aegis-db::orders_repo`).

use crate::engine::DatabaseManager;
use crate::row_decode::{
    decimal_param, row_get_f64, row_get_i64, row_get_opt_f64, row_get_opt_i64, row_get_string,
    row_get_u32, DEC_20_8_MAX_ABS,
};
use aegis_core::{now_ms, AegisError, OrderAuditRecord, Result};
use mysql::params;
use mysql::prelude::*;
use mysql::PooledConn;

/// Clamps an `order_audit.reason` string to at most 250 UTF-8 characters so inserts into
/// `VARCHAR(255)` never trigger `MySqlError` 1406.
pub fn truncate_audit_reason(reason: &str) -> String {
    reason.chars().take(250).collect()
}

impl DatabaseManager {
    /// Updates a `WORKING` order's limit price and chase counter during a 1s top-of-book chase step,
    /// recording an audit entry in `order_audit` with the exact same `new_limit_price` (`R14-L8`).
    pub fn record_working_order_chase(
        &self,
        position_id: i64,
        new_limit_price: f64,
        new_fsm_state: &str,
        exchange_order_id: Option<&str>,
        reason: &str,
    ) -> Result<u32> {
        if !new_limit_price.is_finite() || new_limit_price <= 0.0 {
            return Err(AegisError::Validation(format!(
                "record_working_order_chase requires finite positive new_limit_price, got {new_limit_price}"
            )));
        }
        let px_dec = decimal_param(new_limit_price, 8, DEC_20_8_MAX_ABS)?;

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("record_working_order_chase tx failed: {e}"))
            })?;

        let row: Option<mysql::Row> = tx
            .exec_first(
                r#"SELECT asset_id, client_order_id, side, status, fsm_state,
                          requested_quantity, filled_quantity,
                          reserved_margin_usdc, chase_count
                   FROM positions
                   WHERE id = :pid
                   FOR UPDATE"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("record_working_order_chase lock failed: {e}"))
            })?;

        let locked =
            row.ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let asset_id = row_get_i64(&locked, 0, 0);
        let client_order_id = row_get_string(&locked, 1, "");
        let side = row_get_string(&locked, 2, "LONG");
        let status = row_get_string(&locked, 3, "");
        let from_fsm_state = row_get_string(&locked, 4, "");
        let requested_quantity = row_get_f64(&locked, 5, 0.0);
        let filled_quantity = row_get_f64(&locked, 6, 0.0);
        let reserved_margin_usdc = row_get_f64(&locked, 7, 0.0);
        let chase_count = row_get_u32(&locked, 8, 0);

        if !status.eq_ignore_ascii_case("WORKING") {
            return Err(AegisError::Database(format!(
                "Position {position_id} is '{status}' (expected 'WORKING')"
            )));
        }

        let new_chase_count = chase_count.saturating_add(1);

        tx.exec_drop(
            r#"UPDATE positions
               SET entry_price = :px,
                   current_price = :px,
                   fsm_state = :fsm,
                   chase_count = :cc,
                   exchange_order_id = COALESCE(:eoid, exchange_order_id),
                   updated_at_ms = :ts
               WHERE id = :pid AND status = 'WORKING'"#,
            params! {
                "px" => &px_dec,
                "fsm" => new_fsm_state,
                "cc" => new_chase_count,
                "eoid" => exchange_order_id,
                "ts" => ts,
                "pid" => position_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("record_working_order_chase update failed: {e}"))
        })?;

        if tx.affected_rows() != 1 {
            return Err(AegisError::Database(format!(
                "record_working_order_chase expected 1 affected row for position {position_id}, got {}",
                tx.affected_rows()
            )));
        }

        let req_qty_dec = decimal_param(requested_quantity, 8, DEC_20_8_MAX_ABS)?;
        let fill_qty_dec = decimal_param(filled_quantity, 8, DEC_20_8_MAX_ABS)?;
        let res_usdc_dec = decimal_param(reserved_margin_usdc, 8, DEC_20_8_MAX_ABS)?;
        let safe_reason = truncate_audit_reason(reason);

        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, :from_st, :to_st,
                   :req_qty, :fill_qty, :lim_px, 0.00000000,
                   :res_usdc, 0.00000000, 0.00000000, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => asset_id,
                "coid" => &client_order_id,
                "side" => &side,
                "from_st" => if from_fsm_state.is_empty() { "WORKING_ALO".to_string() } else { from_fsm_state },
                "to_st" => new_fsm_state,
                "req_qty" => &req_qty_dec,
                "fill_qty" => &fill_qty_dec,
                "lim_px" => &px_dec,
                "res_usdc" => &res_usdc_dec,
                "reason" => &safe_reason,
                "ts" => ts,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("record_working_order_chase audit failed: {e}"))
        })?;

        tx.commit().map_err(|e| {
            AegisError::Database(format!("record_working_order_chase commit failed: {e}"))
        })?;
        Ok(new_chase_count)
    }

    /// Returns the initial `WORKING_ALO` limit price recorded for `position_id` in `order_audit`
    /// (`P0-Exec-1` chase-drift ceiling lookup).
    pub fn get_working_order_initial_limit_price(&self, position_id: i64) -> Result<Option<f64>> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .exec_first(
                r#"SELECT limit_price
                   FROM order_audit
                   WHERE position_id = :pid AND limit_price > 0
                   ORDER BY CASE WHEN from_state = 'NEW' AND to_state = 'WORKING_ALO' THEN 0 ELSE 1 END ASC,
                            id ASC
                   LIMIT 1"#,
                params! { "pid" => position_id },
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "get_working_order_initial_limit_price failed: {e}"
                ))
            })?;
        Ok(row.and_then(|r| row_get_opt_f64(&r, 0).filter(|&v| v > 0.0)))
    }

    /// Lists the most recent `limit` 1s Maker Limit Chaser state transitions from `order_audit`.
    pub fn list_recent_order_audits(&self, limit: usize) -> Result<Vec<OrderAuditRecord>> {
        let mut conn = self.get_conn()?;
        Self::list_recent_order_audits_with_conn(&mut conn, limit)
    }

    pub(crate) fn list_recent_order_audits_with_conn(
        conn: &mut PooledConn,
        limit: usize,
    ) -> Result<Vec<OrderAuditRecord>> {
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, position_id, asset_id, client_order_id, side, from_state, to_state,
                          requested_qty, filled_qty, limit_price, avg_fill_price,
                          reserved_usdc, committed_usdc, fees_usdc, reason,
                          created_at_ms, updated_at_ms
                   FROM order_audit
                   ORDER BY created_at_ms DESC, id DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| AegisError::Database(format!("list_recent_order_audits failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| OrderAuditRecord {
                id: row_get_i64(&r, 0, 0),
                position_id: row_get_opt_i64(&r, 1).unwrap_or(0),
                asset_id: row_get_i64(&r, 2, 0),
                client_order_id: row_get_string(&r, 3, ""),
                side: row_get_string(&r, 4, "LONG"),
                from_state: row_get_string(&r, 5, ""),
                to_state: row_get_string(&r, 6, ""),
                requested_qty: row_get_f64(&r, 7, 0.0),
                filled_qty: row_get_f64(&r, 8, 0.0),
                limit_price: row_get_f64(&r, 9, 0.0),
                avg_fill_price: row_get_f64(&r, 10, 0.0),
                reserved_usdc: row_get_f64(&r, 11, 0.0),
                committed_usdc: row_get_f64(&r, 12, 0.0),
                fees_usdc: row_get_f64(&r, 13, 0.0),
                reason: row_get_string(&r, 14, ""),
                created_at_ms: row_get_i64(&r, 15, 0),
                updated_at_ms: row_get_i64(&r, 16, 0),
            })
            .collect())
    }
}
