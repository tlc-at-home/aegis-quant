//! 1-Second Maker Limit Chaser order audit & state transition repository (`aegis-db::orders_repo`).

use crate::engine::DatabaseManager;
use aegis_core::{now_ms, AegisError, OrderAuditRecord, Result};
use mysql::params;
use mysql::prelude::*;

impl DatabaseManager {
    /// Updates a `WORKING` order's limit price and chase counter during a 1s top-of-book chase step,
    /// recording an audit entry in `order_audit`.
    pub fn record_working_order_chase(
        &self,
        position_id: i64,
        new_limit_price: f64,
        new_fsm_state: &str,
        exchange_order_id: Option<&str>,
        reason: &str,
    ) -> Result<u32> {
        let pos = self
            .get_position_by_id(position_id)?
            .ok_or_else(|| AegisError::Database(format!("Position {position_id} not found")))?;
        let new_chase_count = pos.chase_count.saturating_add(1);
        let ts = now_ms();

        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("record_working_order_chase tx failed: {e}"))
            })?;

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
                "px" => new_limit_price,
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

        tx.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, :coid, :side, :from_st, :to_st,
                   :req_qty, :fill_qty, :lim_px, 0.0,
                   :res_usdc, 0.0, 0.0, :reason, :ts, :ts
               )"#,
            params! {
                "pid" => position_id,
                "aid" => pos.asset_id,
                "coid" => &pos.client_order_id,
                "side" => &pos.side,
                "from_st" => &pos.fsm_state,
                "to_st" => new_fsm_state,
                "req_qty" => pos.requested_quantity,
                "fill_qty" => pos.filled_quantity,
                "lim_px" => new_limit_price,
                "res_usdc" => pos.reserved_margin_usdc,
                "reason" => reason,
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

    /// Lists the most recent `limit` 1s Maker Limit Chaser state transitions from `order_audit`.
    pub fn list_recent_order_audits(&self, limit: usize) -> Result<Vec<OrderAuditRecord>> {
        let mut conn = self.get_conn()?;
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
                id: r.get(0).unwrap_or_default(),
                position_id: r.get(1).unwrap_or_default(),
                asset_id: r.get(2).unwrap_or_default(),
                client_order_id: r.get(3).unwrap_or_default(),
                side: r.get(4).unwrap_or_else(|| "LONG".to_string()),
                from_state: r.get(5).unwrap_or_default(),
                to_state: r.get(6).unwrap_or_default(),
                requested_qty: r.get(7).unwrap_or_default(),
                filled_qty: r.get(8).unwrap_or_default(),
                limit_price: r.get(9).unwrap_or_default(),
                avg_fill_price: r.get(10).unwrap_or_default(),
                reserved_usdc: r.get(11).unwrap_or_default(),
                committed_usdc: r.get(12).unwrap_or_default(),
                fees_usdc: r.get(13).unwrap_or_default(),
                reason: r.get(14).unwrap_or_default(),
                created_at_ms: r.get(15).unwrap_or_default(),
                updated_at_ms: r.get(16).unwrap_or_default(),
            })
            .collect())
    }
}
