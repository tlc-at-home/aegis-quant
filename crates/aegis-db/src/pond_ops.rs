//! PP-50 Two-Book Platform (Pit & Pond) CRUD, cell lifecycle, inter-book transfer ledger,
//! and platform snapshot operations.

use crate::DatabaseManager;
use aegis_core::{
    effective_pond_cell_open_heat_usdc, effective_position_open_heat_usdc, round_dp, AegisError,
    BookRow, BookTransfer, BookTransferStatus, PondCell, PondCellState, PondChampion,
    PondScanCycle, PondUniverseRow, Pp50PlatformSnapshot, Result,
};
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Shared PnL, equity, and exposure components for a trading book (`PIT` or `POND`) (`R10-QA`, `R11-QA`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookPnlComponents {
    pub initial_capital_usdc: f64,
    pub closed_realized_pnl_usdc: f64,
    pub partial_realized_pnl_usdc: f64,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub funding_pnl_usdc: f64,
    pub open_fees_usdc: f64,
    pub yield_pnl_usdc: f64,
    pub total_net_pnl_usdc: f64,
    pub transfer_adj_usdc: f64,
    pub book_equity_usdc: f64,
    pub idle_usdc: f64,
    pub deployed_notional_usdc: f64,
    pub resting_notional_usdc: f64,
    pub heat_usdc: f64,
    pub open_positions_count: usize,
    pub resting_positions_count: usize,
}

pub type PitPnlComponents = BookPnlComponents;
pub type PondPnlComponents = BookPnlComponents;

impl DatabaseManager {
    pub fn get_books(&self) -> Result<Vec<BookRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT book_id, book_code, description, equity_allocation_pct, is_active, created_at
                     FROM books
                     ORDER BY book_id ASC;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok(BookRow {
                        book_id: r.get(0)?,
                        book_code: r.get(1)?,
                        description: r.get(2)?,
                        equity_allocation_pct: r.get(3)?,
                        is_active: r.get(4)?,
                        created_at: r.get(5)?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn upsert_pond_universe_batch(&self, rows: &[PondUniverseRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.with_immediate_tx(|conn| {
            {
                let mut stmt = conn
                    .prepare_cached(
                        "INSERT INTO pond_universe (
                            symbol, rank, median_daily_volume_usdc, tick_size, lot_step_size,
                            min_notional_usdc, max_leverage, status, warmup_until,
                            delist_streak, added_at, delisted_at, reference_price, is_synthetic_volume
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                        ON CONFLICT(symbol) DO UPDATE SET
                            rank = excluded.rank,
                            median_daily_volume_usdc = excluded.median_daily_volume_usdc,
                            tick_size = excluded.tick_size,
                            lot_step_size = excluded.lot_step_size,
                            min_notional_usdc = excluded.min_notional_usdc,
                            max_leverage = excluded.max_leverage,
                            status = excluded.status,
                            warmup_until = excluded.warmup_until,
                            delist_streak = excluded.delist_streak,
                            delisted_at = excluded.delisted_at,
                            reference_price = CASE WHEN excluded.reference_price > 0.0 THEN excluded.reference_price ELSE pond_universe.reference_price END,
                            is_synthetic_volume = excluded.is_synthetic_volume;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                for r in rows {
                    let added_at = if r.added_at.trim().is_empty() {
                        aegis_core::now_iso8601_utc()
                    } else {
                        aegis_core::normalize_timestamp_iso8601_utc(&r.added_at)
                    };
                    let ref_px = if r.reference_price.is_finite() && r.reference_price > 0.0 {
                        r.reference_price
                    } else {
                        0.0
                    };
                    stmt.execute(params![
                        r.symbol,
                        r.rank,
                        r.median_daily_volume_usdc,
                        r.tick_size,
                        r.lot_step_size,
                        r.min_notional_usdc,
                        r.max_leverage,
                        r.status,
                        r.warmup_until.as_deref().map(aegis_core::normalize_timestamp_iso8601_utc),
                        r.delist_streak,
                        added_at,
                        r.delisted_at.as_deref().map(aegis_core::normalize_timestamp_iso8601_utc),
                        ref_px,
                        r.is_synthetic_volume,
                    ])
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
            }
            Ok(rows.len())
        })
    }

    pub fn get_pond_universe(&self, active_only: bool) -> Result<Vec<PondUniverseRow>> {
        let sql = if active_only {
            "SELECT symbol, rank, median_daily_volume_usdc, tick_size, lot_step_size,
                    min_notional_usdc, max_leverage, status, warmup_until, delist_streak,
                    added_at, delisted_at, reference_price, is_synthetic_volume
             FROM pond_universe
             WHERE status = 'ACTIVE'
             ORDER BY rank ASC;"
        } else {
            "SELECT symbol, rank, median_daily_volume_usdc, tick_size, lot_step_size,
                    min_notional_usdc, max_leverage, status, warmup_until, delist_streak,
                    added_at, delisted_at, reference_price, is_synthetic_volume
             FROM pond_universe
             ORDER BY rank ASC, symbol ASC;"
        };
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok(PondUniverseRow {
                        symbol: r.get(0)?,
                        rank: r.get(1)?,
                        median_daily_volume_usdc: r.get(2)?,
                        tick_size: r.get(3)?,
                        lot_step_size: r.get(4)?,
                        min_notional_usdc: r.get(5)?,
                        max_leverage: r.get(6)?,
                        status: r.get(7)?,
                        warmup_until: r.get(8)?,
                        delist_streak: r.get(9)?,
                        added_at: r.get(10)?,
                        delisted_at: r.get(11)?,
                        reference_price: r.get::<_, Option<f64>>(12).ok().flatten().unwrap_or(0.0),
                        is_synthetic_volume: r
                            .get::<_, Option<i64>>(13)
                            .ok()
                            .flatten()
                            .unwrap_or(0),
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_active_pond_champion(&self) -> Result<PondChampion> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT champion_id, version, params_json, walk_forward_score, is_active, crowned_at, retired_at
                     FROM pond_champions
                     WHERE is_active = 1 AND retired_at IS NULL
                     ORDER BY champion_id DESC LIMIT 1;",
                    [],
                    |r| {
                        Ok(PondChampion {
                            champion_id: r.get(0)?,
                            version: r.get(1)?,
                            params_json: r.get(2)?,
                            walk_forward_score: r.get(3)?,
                            is_active: r.get(4)?,
                            crowned_at: r.get(5)?,
                            retired_at: r.get(6)?,
                        })
                    },
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if let Some(c) = row {
                return Ok(c);
            }
            let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
            conn.execute(
                "INSERT OR IGNORE INTO pond_champions (version, params_json, walk_forward_score, is_active, crowned_at)
                 VALUES ('NPS-01', '{}', 1.85, 1, ?1);",
                params![now_iso],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            let cid: i64 = conn
                .query_row(
                    "SELECT champion_id FROM pond_champions WHERE version = 'NPS-01';",
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(PondChampion {
                champion_id: cid,
                version: "NPS-01".to_string(),
                params_json: "{}".to_string(),
                walk_forward_score: 1.85,
                is_active: 1,
                crowned_at: Some(now_iso),
                retired_at: None,
            })
        })
    }

    pub fn insert_pond_scan_cycle(&self, cycle: &PondScanCycle) -> Result<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO pond_scan_cycles (
                    started_at, champion_id, coins_scanned, candidates, armed, opened, exits_evaluated
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
                params![
                    cycle.started_at,
                    cycle.champion_id,
                    cycle.coins_scanned,
                    cycle.candidates,
                    cycle.armed,
                    cycle.opened,
                    cycle.exits_evaluated,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        })
    }

    pub fn update_pond_scan_cycle(&self, cycle: &PondScanCycle) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE pond_scan_cycles
                 SET coins_scanned = ?1,
                     candidates = ?2,
                     armed = ?3,
                     opened = ?4,
                     exits_evaluated = ?5
                 WHERE cycle_id = ?6;",
                params![
                    cycle.coins_scanned,
                    cycle.candidates,
                    cycle.armed,
                    cycle.opened,
                    cycle.exits_evaluated,
                    cycle.cycle_id,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_recent_pond_scan_cycles(&self, limit: usize) -> Result<Vec<PondScanCycle>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT cycle_id, started_at, champion_id, coins_scanned, candidates, armed, opened, exits_evaluated
                     FROM pond_scan_cycles
                     ORDER BY cycle_id DESC
                     LIMIT ?1;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![limit as i64], |r| {
                    Ok(PondScanCycle {
                        cycle_id: r.get(0)?,
                        started_at: r.get(1)?,
                        champion_id: r.get(2)?,
                        coins_scanned: r.get(3)?,
                        candidates: r.get(4)?,
                        armed: r.get(5)?,
                        opened: r.get(6)?,
                        exits_evaluated: r.get(7)?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub(crate) fn map_pond_cell_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<PondCell> {
        Ok(PondCell {
            cell_id: r.get("cell_id")?,
            symbol: r.get("symbol")?,
            champion_id: r.get("champion_id")?,
            state: r.get("state")?,
            cycle_opened_id: r.get("cycle_opened_id")?,
            side: r.get("side")?,
            option_analogue: r.get("option_analogue")?,
            entry_price: r.get("entry_price")?,
            quantity: r.get("quantity")?,
            notional_usdc: r.get("notional_usdc")?,
            risk_budget_usdc: r.get("risk_budget_usdc")?,
            conviction_multiplier: r.get("conviction_multiplier")?,
            funding_z_entry: r.get("funding_z_entry")?,
            funding_rate_entry: r.get("funding_rate_entry")?,
            initial_stop: r.get("initial_stop")?,
            chandelier_stop: r.get("chandelier_stop")?,
            highest_price_seen: r.get("highest_price_seen")?,
            lowest_price_seen: r.get("lowest_price_seen")?,
            funding_accrued_usdc: r.get("funding_accrued_usdc")?,
            realized_pnl_usdc: r.get("realized_pnl_usdc")?,
            fees_paid_usdc: r.get("fees_paid_usdc")?,
            exit_price: r.get("exit_price")?,
            exit_reason: r.get("exit_reason")?,
            opened_at: r.get("opened_at")?,
            closed_at: r.get("closed_at")?,
            cooldown_until: r.get("cooldown_until")?,
            updated_at: r.get("updated_at")?,
            resting_order_id: r
                .get::<_, Option<String>>("resting_order_id")
                .ok()
                .flatten(),
        })
    }

    pub fn upsert_pond_cell(&self, cell: &PondCell) -> Result<i64> {
        let parsed_state = PondCellState::parse(&cell.state)?;
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            if cell.cell_id > 0 {
                // R3-H1: Guard open/trailed updates so stale in-memory snapshots cannot resurrect closed/cooldown cells
                let sql = if parsed_state.is_open_or_trailed() {
                    "UPDATE pond_cells
                     SET symbol = ?1,
                         champion_id = ?2,
                         state = ?3,
                         cycle_opened_id = ?4,
                         side = ?5,
                         option_analogue = ?6,
                         entry_price = ?7,
                         quantity = ?8,
                         notional_usdc = ?9,
                         risk_budget_usdc = ?10,
                         conviction_multiplier = ?11,
                         funding_z_entry = ?12,
                         funding_rate_entry = ?13,
                         initial_stop = ?14,
                         chandelier_stop = ?15,
                         highest_price_seen = ?16,
                         lowest_price_seen = ?17,
                         funding_accrued_usdc = ?18,
                         realized_pnl_usdc = ?19,
                         fees_paid_usdc = ?20,
                         exit_price = ?21,
                         exit_reason = ?22,
                         opened_at = ?23,
                         closed_at = ?24,
                         cooldown_until = ?25,
                         updated_at = ?26,
                         resting_order_id = ?27
                     WHERE cell_id = ?28
                       AND state NOT IN ('CLOSED', 'COOLDOWN', 'REJECTED');"
                } else {
                    "UPDATE pond_cells
                     SET symbol = ?1,
                         champion_id = ?2,
                         state = ?3,
                         cycle_opened_id = ?4,
                         side = ?5,
                         option_analogue = ?6,
                         entry_price = ?7,
                         quantity = ?8,
                         notional_usdc = ?9,
                         risk_budget_usdc = ?10,
                         conviction_multiplier = ?11,
                         funding_z_entry = ?12,
                         funding_rate_entry = ?13,
                         initial_stop = ?14,
                         chandelier_stop = ?15,
                         highest_price_seen = ?16,
                         lowest_price_seen = ?17,
                         funding_accrued_usdc = ?18,
                         realized_pnl_usdc = ?19,
                         fees_paid_usdc = ?20,
                         exit_price = ?21,
                         exit_reason = ?22,
                         opened_at = ?23,
                         closed_at = ?24,
                         cooldown_until = ?25,
                         updated_at = ?26,
                         resting_order_id = ?27
                     WHERE cell_id = ?28;"
                };
                conn.execute(
                    sql,
                    params![
                        cell.symbol,
                        cell.champion_id,
                        cell.state,
                        cell.cycle_opened_id,
                        cell.side,
                        cell.option_analogue,
                        cell.entry_price,
                        cell.quantity,
                        cell.notional_usdc,
                        cell.risk_budget_usdc,
                        cell.conviction_multiplier,
                        cell.funding_z_entry,
                        cell.funding_rate_entry,
                        cell.initial_stop,
                        cell.chandelier_stop,
                        cell.highest_price_seen,
                        cell.lowest_price_seen,
                        cell.funding_accrued_usdc,
                        cell.realized_pnl_usdc,
                        cell.fees_paid_usdc,
                        cell.exit_price,
                        cell.exit_reason,
                        cell.opened_at,
                        cell.closed_at,
                        cell.cooldown_until,
                        now_iso,
                        cell.resting_order_id,
                        cell.cell_id,
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(cell.cell_id)
            } else {
                conn.execute(
                    "INSERT INTO pond_cells (
                        symbol, champion_id, state, cycle_opened_id, side, option_analogue,
                        entry_price, quantity, notional_usdc, risk_budget_usdc, conviction_multiplier,
                        funding_z_entry, funding_rate_entry, initial_stop, chandelier_stop,
                        highest_price_seen, lowest_price_seen, funding_accrued_usdc,
                        realized_pnl_usdc, fees_paid_usdc, exit_price, exit_reason,
                        opened_at, closed_at, cooldown_until, updated_at, resting_order_id
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6,
                        ?7, ?8, ?9, ?10, ?11,
                        ?12, ?13, ?14, ?15,
                        ?16, ?17, ?18,
                        ?19, ?20, ?21, ?22,
                        ?23, ?24, ?25, ?26, ?27
                    );",
                    params![
                        cell.symbol,
                        cell.champion_id,
                        cell.state,
                        cell.cycle_opened_id,
                        cell.side,
                        cell.option_analogue,
                        cell.entry_price,
                        cell.quantity,
                        cell.notional_usdc,
                        cell.risk_budget_usdc,
                        cell.conviction_multiplier,
                        cell.funding_z_entry,
                        cell.funding_rate_entry,
                        cell.initial_stop,
                        cell.chandelier_stop,
                        cell.highest_price_seen,
                        cell.lowest_price_seen,
                        cell.funding_accrued_usdc,
                        cell.realized_pnl_usdc,
                        cell.fees_paid_usdc,
                        cell.exit_price,
                        cell.exit_reason,
                        cell.opened_at,
                        cell.closed_at,
                        cell.cooldown_until,
                        now_iso,
                        cell.resting_order_id,
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(conn.last_insert_rowid())
            }
        })
    }

    /// Atomically accrue hourly funding on an open/trailed Pond cell at most once per `hour_bucket` (`R3-B1`, `R3-H1`).
    pub fn accrue_pond_cell_funding_for_hour(
        &self,
        cell_id: i64,
        hour_bucket: i64,
        delta_usdc: f64,
    ) -> Result<bool> {
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE pond_cells
                     SET funding_accrued_usdc = ROUND(funding_accrued_usdc + ?1, 6),
                         last_funding_hour_bucket = ?2,
                         updated_at = ?3
                     WHERE cell_id = ?4
                       AND state IN ('OPEN', 'TRAILED')
                       AND COALESCE(last_funding_hour_bucket, 0) < ?2;",
                    params![round_dp(delta_usdc, 6), hour_bucket, now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    /// Targeted atomic update of open/trailed Pond cell watermarks and Chandelier stop (`R3-H1`, `R4-N10`).
    ///
    /// Note: `state` is strictly restricted to `"OPEN"` or `"TRAILED"`. Callers must use
    /// [`Self::close_open_pond_cell`] to transition a cell to `"CLOSED"` or `"COOLDOWN"`.
    pub fn update_open_pond_cell_watermarks(
        &self,
        cell_id: i64,
        state: &str,
        chandelier_stop: Option<f64>,
        highest_price_seen: Option<f64>,
        lowest_price_seen: Option<f64>,
    ) -> Result<bool> {
        let parsed_state = PondCellState::parse(state)?;
        if !parsed_state.is_open_or_trailed() {
            return Err(AegisError::Validation(format!(
                "update_open_pond_cell_watermarks only accepts 'OPEN' or 'TRAILED' state (received '{state}'); use close_open_pond_cell to close a cell."
            )));
        }
        debug_assert!(parsed_state.is_open_or_trailed());
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE pond_cells
                     SET state = ?1,
                         chandelier_stop = ?2,
                         highest_price_seen = ?3,
                         lowest_price_seen = ?4,
                         updated_at = ?5
                     WHERE cell_id = ?6 AND state IN ('OPEN', 'TRAILED');",
                    params![
                        parsed_state.as_str(),
                        chandelier_stop,
                        highest_price_seen,
                        lowest_price_seen,
                        now_iso,
                        cell_id,
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    /// Targeted atomic update for partial live exit fill on an open/trailed Pond cell (`R3-H1`, `R3-H17`, `R4-N10`).
    ///
    /// Note: `state` is strictly restricted to `"OPEN"` or `"TRAILED"`. Callers must use
    /// [`Self::close_open_pond_cell`] to transition a cell to `"CLOSED"` or `"COOLDOWN"`.
    #[allow(clippy::too_many_arguments)]
    pub fn reduce_open_pond_cell_partial_exit(
        &self,
        cell_id: i64,
        state: &str,
        chandelier_stop: Option<f64>,
        highest_price_seen: Option<f64>,
        lowest_price_seen: Option<f64>,
        remaining_qty: f64,
        remaining_notional: f64,
        realized_pnl_delta: f64,
        fees_delta: f64,
    ) -> Result<bool> {
        let parsed_state = PondCellState::parse(state)?;
        if !parsed_state.is_open_or_trailed() {
            return Err(AegisError::Validation(format!(
                "reduce_open_pond_cell_partial_exit only accepts 'OPEN' or 'TRAILED' state (received '{state}'); use close_open_pond_cell to close a cell."
            )));
        }
        debug_assert!(parsed_state.is_open_or_trailed());
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE pond_cells
                     SET state = ?1,
                         chandelier_stop = ?2,
                         highest_price_seen = ?3,
                         lowest_price_seen = ?4,
                         risk_budget_usdc = CASE
                             WHEN initial_stop > 0.0 AND ABS(entry_price - initial_stop) > 1e-12 THEN ROUND(ABS(entry_price - initial_stop) * ?5, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?5 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         quantity = ?5,
                         notional_usdc = ?6,
                         realized_pnl_usdc = ROUND(realized_pnl_usdc + ?7, 4),
                         fees_paid_usdc = ROUND(fees_paid_usdc + ?8, 4),
                         updated_at = ?9
                     WHERE cell_id = ?10 AND state IN ('OPEN', 'TRAILED');",
                    params![
                        parsed_state.as_str(),
                        chandelier_stop,
                        highest_price_seen,
                        lowest_price_seen,
                        round_dp(remaining_qty.abs(), 8),
                        round_dp(remaining_notional, 2),
                        round_dp(realized_pnl_delta, 4),
                        round_dp(fees_delta, 4),
                        now_iso,
                        cell_id,
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    /// Targeted atomic close of an open/trailed Pond cell (`R3-H1`).
    #[allow(clippy::too_many_arguments)]
    pub fn close_open_pond_cell(
        &self,
        cell_id: i64,
        target_state: &str,
        chandelier_stop: Option<f64>,
        highest_price_seen: Option<f64>,
        lowest_price_seen: Option<f64>,
        exit_price: f64,
        gross_pnl_delta: f64,
        fees_delta: f64,
        exit_reason: Option<&str>,
        closed_at: &str,
        cooldown_until: &str,
    ) -> Result<bool> {
        let _ = PondCellState::parse(target_state)?;
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE pond_cells
                     SET state = ?1,
                         chandelier_stop = ?2,
                         highest_price_seen = ?3,
                         lowest_price_seen = ?4,
                         exit_price = ?5,
                         fees_paid_usdc = ROUND(fees_paid_usdc + ?6, 4),
                         realized_pnl_usdc = ROUND(realized_pnl_usdc + ?7 + funding_accrued_usdc - (fees_paid_usdc + ?6), 4),
                         exit_reason = ?8,
                         closed_at = ?9,
                         cooldown_until = ?10,
                         updated_at = ?11,
                         resting_order_id = NULL,
                         protective_stop_oid = NULL
                     WHERE cell_id = ?12 AND state IN ('OPEN', 'TRAILED');",
                    params![
                        target_state,
                        chandelier_stop,
                        highest_price_seen,
                        lowest_price_seen,
                        exit_price,
                        round_dp(fees_delta, 4),
                        round_dp(gross_pnl_delta, 4),
                        exit_reason,
                        closed_at,
                        cooldown_until,
                        now_iso,
                        cell_id,
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    pub fn get_open_pond_cells(&self) -> Result<Vec<PondCell>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM v_pond_cells_open ORDER BY cell_id ASC;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], Self::map_pond_cell_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_armed_pond_cells(&self) -> Result<Vec<PondCell>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM pond_cells WHERE state = 'ARMED' ORDER BY cell_id ASC;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], Self::map_pond_cell_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_pond_cells(&self, limit: usize) -> Result<Vec<PondCell>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM pond_cells ORDER BY cell_id DESC LIMIT ?1;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![limit as i64], Self::map_pond_cell_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_pond_realized_pnl(&self) -> Result<f64> {
        self.with_conn(|conn| {
            let reset_at = Self::query_equity_baseline_reset_at(conn)?;
            let pnl: f64 = if let Some(ref reset_trimmed) = reset_at {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM pond_cells WHERE state IN ('CLOSED', 'COOLDOWN', 'DORMANT') AND (closed_at IS NULL OR datetime(closed_at) > datetime(?1));",
                    params![reset_trimmed],
                    |r| r.get(0),
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
            } else {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM pond_cells WHERE state IN ('CLOSED', 'COOLDOWN', 'DORMANT');",
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
            };
            Ok(pnl)
        })
    }

    pub fn get_open_pit_symbols(&self) -> Result<HashSet<String>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT DISTINCT symbol FROM trades WHERE status IN ('OPEN', 'PENDING');")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_open_pond_symbols(&self) -> Result<HashSet<String>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT DISTINCT symbol FROM pond_cells WHERE state IN ('OPEN', 'TRAILED', 'ARMED');")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_all_open_wallet_symbols(&self) -> Result<HashSet<String>> {
        let mut syms = self.get_open_pit_symbols().unwrap_or_default();
        if let Ok(pond_syms) = self.get_open_pond_symbols() {
            syms.extend(pond_syms);
        }
        Ok(syms)
    }

    /// Returns `(total_wallet_equity_usdc, perps_trading_equity_usdc, vault_equity_usdc, total_deployed_perps_notional_usdc)`.
    pub fn get_wallet_perps_margin_state(&self) -> Result<(f64, f64, f64, f64)> {
        let cfg = self.load_config_opts(false)?;
        let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0).max(1.0);
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_live_or_testnet = mode == "LIVE" || mode == "TESTNET";

        let (total_eq, vault_eq) = match self.get_latest_portfolio_snapshot() {
            Ok(snap)
                if snap.total_equity_usdc > 0.0
                    && (is_live_or_testnet || snap.basis_harvest_notional_usdc > 0.0) =>
            {
                let tot = snap.total_equity_usdc;
                let v = if is_live_or_testnet {
                    snap.basis_harvest_notional_usdc.clamp(0.0, tot)
                } else {
                    0.0
                };
                (tot, v)
            }
            _ => (init_eq, 0.0),
        };
        let perps_trading_eq = round_dp((total_eq - vault_eq).max(0.0), 2);

        let total_deployed = self.with_conn(|conn| {
            let pit_dep: f64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(ABS(quantity) * current_price), 0.0)
                     FROM trades
                     WHERE status IN ('OPEN', 'PENDING');",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0.0);
            let pond_dep: f64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(COALESCE(notional_usdc, 0.0)), 0.0)
                     FROM pond_cells
                     WHERE state IN ('OPEN', 'TRAILED', 'ARMED');",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0.0);
            Ok(round_dp(pit_dep + pond_dep, 2))
        })?;

        Ok((
            total_eq,
            perps_trading_eq,
            round_dp(vault_eq, 2),
            total_deployed,
        ))
    }

    pub fn reconcile_close_phantom_pond_cell(
        &self,
        cell_id: i64,
        exit_price: f64,
        exit_reason: &str,
    ) -> Result<bool> {
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE pond_cells
                     SET state = 'CLOSED',
                         exit_price = ?1,
                         exit_reason = ?2,
                         closed_at = ?3,
                         updated_at = ?3,
                         resting_order_id = NULL
                     WHERE cell_id = ?4 AND state IN ('OPEN', 'TRAILED', 'ARMED');",
                    params![exit_price, exit_reason, now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    /// Reconcile an open/trailed Pond cell's quantity and entry price against on-chain clearinghouse state (`R3-H3`, `R10-S1`).
    pub fn reconcile_open_pond_cell_size(
        &self,
        cell_id: i64,
        new_qty: f64,
        new_entry_price: Option<f64>,
    ) -> Result<bool> {
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            let rows = if let Some(epx) = new_entry_price.filter(|&p| p > 0.0) {
                let notional = round_dp(new_qty.abs() * epx, 2);
                conn.execute(
                    "UPDATE pond_cells
                     SET quantity = ?1,
                         entry_price = ?2,
                         notional_usdc = ?3,
                         risk_budget_usdc = CASE
                             WHEN initial_stop > 0.0 AND ABS(?2 - initial_stop) > 1e-12 THEN ROUND(ABS(?2 - initial_stop) * ?1, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?1 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         updated_at = ?4
                     WHERE cell_id = ?5 AND state IN ('OPEN', 'TRAILED');",
                    params![new_qty.abs(), epx, notional, now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
            } else {
                conn.execute(
                    "UPDATE pond_cells
                     SET quantity = ?1,
                         notional_usdc = ROUND(?1 * COALESCE(entry_price, 0.0), 2),
                         risk_budget_usdc = CASE
                             WHEN initial_stop > 0.0 AND ABS(entry_price - initial_stop) > 1e-12 THEN ROUND(ABS(entry_price - initial_stop) * ?1, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?1 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         updated_at = ?2
                     WHERE cell_id = ?3 AND state IN ('OPEN', 'TRAILED');",
                    params![new_qty.abs(), now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
            };
            Ok(rows > 0)
        })
    }

    /// Atomically promote an `ARMED` Pond cell to `OPEN` when its resting limit order has filled (`Fix 4`, `R13-QA`).
    /// Preserves the cell's original `initial_stop` and `chandelier_stop` while initializing watermarks
    /// to `fill_price` and rescaling `notional_usdc` and `risk_budget_usdc`.
    pub fn promote_armed_pond_cell_to_open(
        &self,
        cell_id: i64,
        fill_qty: f64,
        fill_price: f64,
        fee_usdc: f64,
    ) -> Result<bool> {
        if fill_qty <= 0.0 || fill_price <= 0.0 || !fill_qty.is_finite() || !fill_price.is_finite()
        {
            return Ok(false);
        }
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let qty = round_dp(fill_qty.abs(), 8);
        let px = round_dp(fill_price, 8);
        let notional = round_dp(qty * px, 2);
        let fee = round_dp(fee_usdc.max(0.0), 4);
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE pond_cells
                     SET state = 'OPEN',
                         entry_price = ?1,
                         highest_price_seen = ?1,
                         lowest_price_seen = ?1,
                         quantity = ?2,
                         notional_usdc = ?3,
                         risk_budget_usdc = CASE
                             WHEN initial_stop > 0.0 AND ABS(?1 - initial_stop) > 1e-12 THEN ROUND(ABS(?1 - initial_stop) * ?2, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?2 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         fees_paid_usdc = ROUND(COALESCE(fees_paid_usdc, 0.0) + ?4, 4),
                         opened_at = ?5,
                         updated_at = ?5,
                         exit_reason = NULL,
                         resting_order_id = NULL
                     WHERE cell_id = ?6 AND state = 'ARMED';",
                    params![px, qty, notional, fee, now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    /// True up an `OPEN` / `TRAILED` Pond cell that was race-promoted via `clearinghouseState`
    /// once the exact maker fill appears in `userFills` (`R13-N1`, `R13-N4(b)`).
    pub fn true_up_promoted_pond_cell_from_user_fills(
        &self,
        cell_id: i64,
        fill_qty: f64,
        fill_price: f64,
        actual_fee_usdc: f64,
        estimated_fee_usdc: f64,
    ) -> Result<bool> {
        if fill_qty <= 0.0 || fill_price <= 0.0 || !fill_qty.is_finite() || !fill_price.is_finite()
        {
            return Ok(false);
        }
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let qty = round_dp(fill_qty.abs(), 8);
        let px = round_dp(fill_price, 8);
        let notional = round_dp(qty * px, 2);
        let fee_delta = round_dp(actual_fee_usdc.max(0.0) - estimated_fee_usdc.max(0.0), 4);
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE pond_cells
                     SET highest_price_seen = CASE
                             WHEN ABS(COALESCE(highest_price_seen, entry_price) - COALESCE(entry_price, ?1)) < 1e-9 THEN ?1
                             ELSE MAX(COALESCE(highest_price_seen, ?1), ?1)
                         END,
                         lowest_price_seen = CASE
                             WHEN ABS(COALESCE(lowest_price_seen, entry_price) - COALESCE(entry_price, ?1)) < 1e-9 THEN ?1
                             WHEN COALESCE(lowest_price_seen, 0.0) > 0.0 THEN MIN(lowest_price_seen, ?1)
                             ELSE ?1
                         END,
                         entry_price = ?1,
                         quantity = ?2,
                         notional_usdc = ?3,
                         risk_budget_usdc = CASE
                             WHEN initial_stop > 0.0 AND ABS(?1 - initial_stop) > 1e-12 THEN ROUND(ABS(?1 - initial_stop) * ?2, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?2 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         fees_paid_usdc = MAX(0.0, ROUND(COALESCE(fees_paid_usdc, 0.0) + ?4, 4)),
                         updated_at = ?5
                     WHERE cell_id = ?6 AND state IN ('OPEN', 'TRAILED');",
                    params![px, qty, notional, fee_delta, now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    pub fn update_pond_cell_funding(&self, cell_id: i64, funding_accrued_usdc: f64) -> Result<()> {
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE pond_cells
                 SET funding_accrued_usdc = ?1,
                     updated_at = ?2
                 WHERE cell_id = ?3 AND state IN ('OPEN', 'TRAILED');",
                params![round_dp(funding_accrued_usdc, 4), now_iso, cell_id],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn insert_book_transfer(&self, transfer: &BookTransfer) -> Result<i64> {
        let is_dry_run = transfer.status.to_ascii_uppercase().starts_with("DRY_RUN")
            || transfer
                .tx_ref
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase()
                .starts_with("dryrun-")
            || transfer
                .tx_ref
                .as_deref()
                .unwrap_or("")
                .to_ascii_uppercase()
                .contains("DRY_RUN");
        let transfer_mode = if is_dry_run { "DRY_RUN" } else { "LIVE" };
        self.insert_book_transfer_with_mode(transfer, transfer_mode)
    }

    pub fn insert_book_transfer_with_mode(
        &self,
        transfer: &BookTransfer,
        transfer_mode: &str,
    ) -> Result<i64> {
        let parsed_status = BookTransferStatus::parse(&transfer.status)?;
        let created_iso = if transfer.created_at.trim().is_empty() {
            aegis_core::now_iso8601_utc()
        } else {
            aegis_core::normalize_timestamp_iso8601_utc(&transfer.created_at)
        };
        let eff_mode = if transfer_mode.trim().is_empty() {
            if parsed_status == BookTransferStatus::DryRun
                || transfer
                    .tx_ref
                    .as_deref()
                    .unwrap_or("")
                    .to_ascii_lowercase()
                    .starts_with("dryrun-")
                || transfer
                    .tx_ref
                    .as_deref()
                    .unwrap_or("")
                    .to_ascii_uppercase()
                    .contains("DRY_RUN")
            {
                "DRY_RUN"
            } else {
                "LIVE"
            }
        } else {
            transfer_mode.trim()
        };
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO book_transfers (
                    settle_date, from_book, to_book, amount_usdc,
                    pre_from_usdc, pre_to_usdc, post_from_usdc, post_to_usdc,
                    drift_bps_before, drift_bps_after, blocked_remainder_usdc,
                    status, attempts, tx_ref, transfer_mode, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16);",
                params![
                    transfer.settle_date,
                    transfer.from_book,
                    transfer.to_book,
                    transfer.amount_usdc,
                    transfer.pre_from_usdc,
                    transfer.pre_to_usdc,
                    transfer.post_from_usdc,
                    transfer.post_to_usdc,
                    transfer.drift_bps_before,
                    transfer.drift_bps_after,
                    transfer.blocked_remainder_usdc,
                    parsed_status.as_str(),
                    transfer.attempts,
                    transfer.tx_ref,
                    eff_mode,
                    created_iso,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_book_transfer_status(
        &self,
        transfer_id: i64,
        status: &str,
        tx_ref: Option<&str>,
        attempts: i64,
        post_from_usdc: Option<f64>,
        post_to_usdc: Option<f64>,
        drift_bps_after: Option<f64>,
        blocked_remainder_usdc: f64,
    ) -> Result<()> {
        let target_status = BookTransferStatus::parse(status)?;
        let is_dry_run = target_status == BookTransferStatus::DryRun
            || tx_ref
                .unwrap_or("")
                .to_ascii_lowercase()
                .starts_with("dryrun-")
            || tx_ref
                .unwrap_or("")
                .to_ascii_uppercase()
                .contains("DRY_RUN");
        let mode_override = if is_dry_run { Some("DRY_RUN") } else { None };
        self.with_conn(|conn| {
            if let Ok(Some(prior_str)) = conn
                .query_row(
                    "SELECT status FROM book_transfers WHERE transfer_id = ?1;",
                    params![transfer_id],
                    |r| r.get::<_, String>(0),
                )
                .optional()
            {
                if let Ok(prior_status) = BookTransferStatus::parse(&prior_str) {
                    if !prior_status.can_transition_to(target_status) {
                        return Err(AegisError::Validation(format!(
                            "Invalid BookTransferStatus transition for transfer_id #{transfer_id}: {prior_status} -> {target_status}"
                        )));
                    }
                }
            }
            conn.execute(
                "UPDATE book_transfers
                 SET status = ?1,
                     attempts = ?2,
                     post_from_usdc = COALESCE(?3, post_from_usdc),
                     post_to_usdc = COALESCE(?4, post_to_usdc),
                     drift_bps_after = COALESCE(?5, drift_bps_after),
                     tx_ref = COALESCE(?6, tx_ref),
                     blocked_remainder_usdc = ?7,
                     transfer_mode = COALESCE(?8, NULLIF(transfer_mode, ''), 'LIVE')
                 WHERE transfer_id = ?9;",
                params![
                    target_status.as_str(),
                    attempts,
                    post_from_usdc,
                    post_to_usdc,
                    drift_bps_after,
                    tx_ref,
                    blocked_remainder_usdc,
                    mode_override,
                    transfer_id,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    /// Updates `transfer_mode` on an existing `book_transfers` row (`R5-M2`).
    pub fn update_book_transfer_mode(&self, transfer_id: i64, transfer_mode: &str) -> Result<()> {
        let trimmed = transfer_mode.trim();
        if trimmed.is_empty() {
            return Ok(());
        }
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE book_transfers SET transfer_mode = ?1 WHERE transfer_id = ?2;",
                params![trimmed, transfer_id],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    /// Reads per-leg progress `(legs_completed, leg1_tx_ref, leg2_tx_ref)` for a two-leg subaccount transfer (`R3-B3`).
    pub fn get_book_transfer_leg_progress(
        &self,
        transfer_id: i64,
    ) -> Result<(i64, Option<String>, Option<String>)> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT COALESCE(legs_completed, 0), leg1_tx_ref, leg2_tx_ref
                     FROM book_transfers
                     WHERE transfer_id = ?1;",
                    params![transfer_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(row.unwrap_or((0, None, None)))
        })
    }

    /// Persists per-leg progress `(legs_completed, leg1_tx_ref, leg2_tx_ref)` for a two-leg subaccount transfer (`R3-B3`).
    pub fn update_book_transfer_leg_progress(
        &self,
        transfer_id: i64,
        legs_completed: i64,
        leg1_tx_ref: Option<&str>,
        leg2_tx_ref: Option<&str>,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE book_transfers
                 SET legs_completed = ?1,
                     leg1_tx_ref = COALESCE(?2, leg1_tx_ref),
                     leg2_tx_ref = COALESCE(?3, leg2_tx_ref)
                 WHERE transfer_id = ?4;",
                params![legs_completed, leg1_tx_ref, leg2_tx_ref, transfer_id],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    /// Startup and pre-settle recovery sweep that inspects stale `PENDING` rows in `book_transfers`
    /// and resolves or quarantines them before allowing a new settle cycle (`R3-B3`, `R3-QE`, `R4-H1`, `R4-QA`).
    ///
    /// - Rows with both legs completed (`legs_completed >= 2` or `leg2_tx_ref` present) transition to `CONFIRMED`.
    /// - Rows with `legs_completed == 1` (or `leg1_tx_ref` present without leg 2) transition to `PARTIAL`
    ///   (requiring manual operator reconciliation because funds are stranded on the master wallet).
    /// - Rows whose `tx_ref` / leg refs indicate `AMBIGUOUS_POST_SEND` transition to `AMBIGUOUS`.
    /// - Rows with 0 legs completed transition to `FAILED` (`RECONCILED_STALE_PENDING`).
    #[allow(clippy::type_complexity)]
    pub fn reconcile_pending_book_transfers(&self) -> Result<usize> {
        let pending_rows: Vec<(
            i64,
            String,
            String,
            f64,
            i64,
            Option<String>,
            Option<String>,
            Option<String>,
        )> = self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT transfer_id, from_book, to_book, amount_usdc,
                            COALESCE(legs_completed, 0), leg1_tx_ref, leg2_tx_ref, tx_ref
                     FROM book_transfers
                     WHERE status = 'PENDING'
                     ORDER BY transfer_id ASC;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                    ))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })?;

        let is_live_or_testnet = self
            .get_raw_config_value("execution_mode")
            .ok()
            .flatten()
            .map(|m| matches!(m.trim().to_ascii_uppercase().as_str(), "LIVE" | "TESTNET"))
            .unwrap_or(false);

        let mut reconciled = 0usize;
        for (tid, from_book, to_book, amount_usdc, legs_completed, leg1_ref, leg2_ref, tx_ref) in
            pending_rows
        {
            let has_leg2 = leg2_ref
                .as_deref()
                .map(|s| !s.trim().is_empty() && !s.to_ascii_uppercase().contains("AMBIGUOUS"))
                .unwrap_or(false);
            let has_confirmed_leg1 = leg1_ref
                .as_deref()
                .map(|s| !s.trim().is_empty() && !s.to_ascii_uppercase().contains("AMBIGUOUS"))
                .unwrap_or(false);
            let is_ambiguous = tx_ref
                .as_deref()
                .map(|s| s.to_ascii_uppercase().contains("AMBIGUOUS"))
                .unwrap_or(false)
                || leg1_ref
                    .as_deref()
                    .map(|s| s.to_ascii_uppercase().contains("AMBIGUOUS"))
                    .unwrap_or(false)
                || leg2_ref
                    .as_deref()
                    .map(|s| s.to_ascii_uppercase().contains("AMBIGUOUS"))
                    .unwrap_or(false);
            let has_confirmed_tx = tx_ref
                .as_deref()
                .map(|s| {
                    let u = s.trim().to_ascii_uppercase();
                    !u.is_empty()
                        && !u.contains("PENDING")
                        && !u.contains("PARTIAL")
                        && !u.contains("AMBIGUOUS")
                        && !u.contains("FAILED")
                        && !u.contains("RECONCILED")
                })
                .unwrap_or(false);

            let (new_status, new_tx_ref) = if legs_completed >= 2 || has_leg2 || has_confirmed_tx {
                (
                    BookTransferStatus::Confirmed,
                    tx_ref
                        .or(leg2_ref)
                        .unwrap_or_else(|| format!("0xreconciled_confirmed_{tid}")),
                )
            } else if legs_completed == 1 || has_confirmed_leg1 {
                let l1 = leg1_ref.unwrap_or_else(|| format!("leg1_{tid}"));
                (
                    BookTransferStatus::Partial,
                    tx_ref.unwrap_or_else(|| format!("PARTIAL_LEG1_ONLY:{l1}")),
                )
            } else if is_ambiguous || is_live_or_testnet {
                (
                    BookTransferStatus::Ambiguous,
                    tx_ref.unwrap_or_else(|| format!("AMBIGUOUS_POST_SEND_{tid}")),
                )
            } else {
                (
                    BookTransferStatus::Failed,
                    tx_ref.unwrap_or_else(|| format!("RECONCILED_STALE_PENDING_{tid}")),
                )
            };

            let reset_post_eq = !new_status.is_ledger_counted();
            self.with_conn(|conn| {
                conn.execute(
                    "UPDATE book_transfers
                     SET status = ?1,
                         tx_ref = ?2,
                         post_from_usdc = CASE WHEN ?3 THEN pre_from_usdc ELSE post_from_usdc END,
                         post_to_usdc = CASE WHEN ?3 THEN pre_to_usdc ELSE post_to_usdc END
                     WHERE transfer_id = ?4 AND status = 'PENDING';",
                    params![new_status.as_str(), new_tx_ref, reset_post_eq, tid],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(())
            })?;

            let log_level = if new_status.needs_manual_reconciliation() {
                "ERROR"
            } else {
                "WARN"
            };
            let _ = self.log_telemetry(
                log_level,
                "aegis-settle.reconcile_pending",
                &format!(
                    "Reconciled stale PENDING book_transfer #{tid} (${amount_usdc:.2} {from_book}->{to_book}, legs={legs_completed}) -> {new_status} ({new_tx_ref})"
                ),
                None,
            );
            reconciled += 1;
        }
        Ok(reconciled)
    }

    /// Query `book_transfers` for any unresolved `PARTIAL` or `AMBIGUOUS` rows, `FAILED` rows with
    /// `legs_completed > 0`, or stale `PENDING` rows exceeding `pending_transfer_stale_ttl_ms` (`R4-H1`, `R4-QB`).
    pub fn get_stranded_or_unresolved_book_transfers(&self) -> Result<Vec<BookTransfer>> {
        let cfg = self.load_config_opts(false).unwrap_or_default();
        let stale_ttl_ms = cfg
            .get_i64("pending_transfer_stale_ttl_ms", 120_000)
            .max(1_000);
        let cutoff_iso = chrono::DateTime::from_timestamp_millis(self.now_ms() - stale_ttl_ms)
            .unwrap_or_else(Utc::now)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT transfer_id, settle_date, from_book, to_book, amount_usdc,
                            pre_from_usdc, pre_to_usdc, post_from_usdc, post_to_usdc,
                            drift_bps_before, drift_bps_after, blocked_remainder_usdc,
                            status, attempts, tx_ref, created_at
                     FROM book_transfers
                     WHERE status IN ('PARTIAL', 'AMBIGUOUS')
                        OR (status = 'FAILED' AND COALESCE(legs_completed, 0) > 0)
                        OR (status = 'PENDING' AND datetime(created_at) <= datetime(?1))
                     ORDER BY transfer_id DESC;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![cutoff_iso], |r| {
                    Ok(BookTransfer {
                        transfer_id: r.get(0)?,
                        settle_date: r.get(1)?,
                        from_book: r.get(2)?,
                        to_book: r.get(3)?,
                        amount_usdc: r.get(4)?,
                        pre_from_usdc: r.get(5)?,
                        pre_to_usdc: r.get(6)?,
                        post_from_usdc: r.get(7)?,
                        post_to_usdc: r.get(8)?,
                        drift_bps_before: r.get(9)?,
                        drift_bps_after: r.get(10)?,
                        blocked_remainder_usdc: r.get(11)?,
                        status: r.get(12)?,
                        attempts: r.get(13)?,
                        tx_ref: r.get(14)?,
                        created_at: r.get(15)?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    /// Rate-limited check for unresolved `PARTIAL`/`AMBIGUOUS` or stale `PENDING` book transfers (`R4-QB`).
    /// Returns the unresolved transfers if at least one exists and `stranded_transfer_alert_cooldown_ms`
    /// has elapsed since the last alert, updating `stranded_transfer_last_alert_ms` in `system_config`.
    pub fn check_and_record_stranded_transfer_alert(&self) -> Result<Vec<BookTransfer>> {
        let stranded = self.get_stranded_or_unresolved_book_transfers()?;
        if stranded.is_empty() {
            return Ok(Vec::new());
        }
        let cfg = self.load_config_opts(false).unwrap_or_default();
        let cooldown_ms = cfg
            .get_i64("stranded_transfer_alert_cooldown_ms", 300_000)
            .max(1_000);
        let last_alert_ms = cfg.get_i64("stranded_transfer_last_alert_ms", 0);
        let now_ms = self.now_ms();
        if last_alert_ms > 0 && (now_ms - last_alert_ms) < cooldown_ms {
            return Ok(Vec::new());
        }
        let _ = self.with_conn(|conn| {
            let _ = conn.execute(
                "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                 VALUES ('stranded_transfer_last_alert_ms', ?1, 'int', 'settling', 'Epoch timestamp (ms) of last stranded book transfer alert', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                 ON CONFLICT(config_key) DO UPDATE SET
                    config_value = excluded.config_value,
                    updated_at = excluded.updated_at;",
                params![now_ms.to_string()],
            );
            Ok(())
        });
        let ids: Vec<i64> = stranded.iter().map(|t| t.transfer_id).collect();
        tracing::warn!(
            unresolved_count = stranded.len(),
            transfer_ids = ?ids,
            "Unresolved PARTIAL/AMBIGUOUS or stale PENDING book_transfers detected; manual reconciliation required"
        );
        let _ = self.log_telemetry(
            "WARNING",
            "aegis-settle.stranded_watcher",
            &format!(
                "Unresolved PARTIAL/AMBIGUOUS or stale PENDING book_transfers detected (count={}, ids={ids:?}); manual reconciliation required",
                stranded.len()
            ),
            None,
        );
        Ok(stranded)
    }

    /// Audits `book_transfers` for any `PARTIAL_IDLE_CAPPED` rows that lack a `DRY_RUN%`
    /// `transfer_mode` or `dryrun-` `tx_ref` prefix (`R5-M2`) so operators can inspect
    /// ledger-counted partial rows before the first live settle.
    pub fn audit_suspect_dry_run_book_transfers(&self) -> Result<Vec<BookTransfer>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT transfer_id, settle_date, from_book, to_book, amount_usdc,
                            pre_from_usdc, pre_to_usdc, post_from_usdc, post_to_usdc,
                            drift_bps_before, drift_bps_after, blocked_remainder_usdc,
                            status, attempts, tx_ref, created_at
                     FROM book_transfers
                     WHERE status = 'PARTIAL_IDLE_CAPPED'
                       AND UPPER(COALESCE(transfer_mode, '')) NOT LIKE 'DRY_RUN%'
                       AND LOWER(COALESCE(tx_ref, '')) NOT LIKE 'dryrun-%'
                     ORDER BY transfer_id DESC;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok(BookTransfer {
                        transfer_id: r.get(0)?,
                        settle_date: r.get(1)?,
                        from_book: r.get(2)?,
                        to_book: r.get(3)?,
                        amount_usdc: r.get(4)?,
                        pre_from_usdc: r.get(5)?,
                        pre_to_usdc: r.get(6)?,
                        post_from_usdc: r.get(7)?,
                        post_to_usdc: r.get(8)?,
                        drift_bps_before: r.get(9)?,
                        drift_bps_after: r.get(10)?,
                        blocked_remainder_usdc: r.get(11)?,
                        status: r.get(12)?,
                        attempts: r.get(13)?,
                        tx_ref: r.get(14)?,
                        created_at: r.get(15)?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    /// Reclassifies historical `PARTIAL_IDLE_CAPPED` rows created before `cutoff_iso` to `DRY_RUN` (`R6-N4`).
    ///
    /// Sets `status = 'DRY_RUN'` and `transfer_mode = 'DRY_RUN_RECLASSIFIED'` for all matching rows,
    /// updates `dry_run_reclassify_before_utc` in `system_config` for operator auditability, and returns
    /// the number of reclassified rows.
    pub fn reclassify_dry_run_transfers_before(&self, cutoff_iso: &str) -> Result<usize> {
        let trimmed = cutoff_iso.trim();
        if trimmed.is_empty() {
            return Err(AegisError::Validation(
                "Cutoff ISO timestamp must not be empty".to_string(),
            ));
        }

        // Validate timestamp format using ISO8601 parser
        if chrono::DateTime::parse_from_rfc3339(trimmed).is_err()
            && chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S").is_err()
            && chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S").is_err()
            && chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").is_err()
        {
            return Err(AegisError::Validation(format!(
                "Invalid ISO-8601 cutoff timestamp: '{trimmed}'. Expected format like 'YYYY-MM-DDTHH:MM:SSZ', 'YYYY-MM-DD HH:MM:SS', or 'YYYY-MM-DD'."
            )));
        }

        let updated_count = self.with_immediate_tx(|conn| {
            let count = conn
                .execute(
                    "UPDATE book_transfers
                     SET status = 'DRY_RUN',
                         transfer_mode = 'DRY_RUN_RECLASSIFIED'
                     WHERE status = 'PARTIAL_IDLE_CAPPED'
                       AND datetime(created_at) <= datetime(?1);",
                    params![trimmed],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            conn.execute(
                "INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
                 VALUES (
                     'dry_run_reclassify_before_utc',
                     ?1,
                     'string',
                     'settling',
                     'Optional ISO-8601 UTC cutoff timestamp before which historical PARTIAL_IDLE_CAPPED rows are reclassified to DRY_RUN',
                     strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 )
                 ON CONFLICT(config_key) DO UPDATE SET
                     config_value = excluded.config_value,
                     updated_at = excluded.updated_at;",
                params![trimmed],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(count)
        })?;
        tracing::info!(
            "Reclassified {updated_count} PARTIAL_IDLE_CAPPED transfer row(s) before {trimmed} to DRY_RUN (R6-N4)"
        );
        Ok(updated_count)
    }

    pub fn get_recent_book_transfers(&self, limit: usize) -> Result<Vec<BookTransfer>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT transfer_id, settle_date, from_book, to_book, amount_usdc,
                            pre_from_usdc, pre_to_usdc, post_from_usdc, post_to_usdc,
                            drift_bps_before, drift_bps_after, blocked_remainder_usdc,
                            status, attempts, tx_ref, created_at
                     FROM book_transfers
                     ORDER BY transfer_id DESC
                     LIMIT ?1;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![limit as i64], |r| {
                    Ok(BookTransfer {
                        transfer_id: r.get(0)?,
                        settle_date: r.get(1)?,
                        from_book: r.get(2)?,
                        to_book: r.get(3)?,
                        amount_usdc: r.get(4)?,
                        pre_from_usdc: r.get(5)?,
                        pre_to_usdc: r.get(6)?,
                        post_from_usdc: r.get(7)?,
                        post_to_usdc: r.get(8)?,
                        drift_bps_before: r.get(9)?,
                        drift_bps_after: r.get(10)?,
                        blocked_remainder_usdc: r.get(11)?,
                        status: r.get(12)?,
                        attempts: r.get(13)?,
                        tx_ref: r.get(14)?,
                        created_at: r.get(15)?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    /// Returns the cumulative net USDC transferred into `book_code` minus USDC transferred out of
    /// `book_code` across all non-dry-run `COMPLETED` / `CONFIRMED` / `PARTIAL_IDLE_CAPPED` rows in `book_transfers` (`C1`, `NL-10`, `R3-H5`, `R4-H2`).
    pub fn get_net_book_transfer_adjustment(&self, book_code: &str) -> Result<f64> {
        let code = book_code.trim().to_ascii_uppercase();
        self.with_conn(|conn| {
            let reset_at = Self::query_equity_baseline_reset_at(conn)?;
            let (inflow, outflow): (f64, f64) = if let Some(ref reset_trimmed) = reset_at {
                let inf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE to_book = ?1
                           AND status IN ('COMPLETED', 'CONFIRMED', 'PARTIAL_IDLE_CAPPED')
                           AND COALESCE(transfer_mode, '') NOT LIKE 'DRY_RUN%'
                           AND COALESCE(tx_ref, '') NOT LIKE '%DRY_RUN%'
                           AND LOWER(COALESCE(tx_ref, '')) NOT LIKE 'dryrun-%'
                           AND (created_at IS NULL OR datetime(created_at) > datetime(?2));",
                        params![code, reset_trimmed],
                        |r| r.get(0),
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let outf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE from_book = ?1
                           AND status IN ('COMPLETED', 'CONFIRMED', 'PARTIAL_IDLE_CAPPED')
                           AND COALESCE(transfer_mode, '') NOT LIKE 'DRY_RUN%'
                           AND COALESCE(tx_ref, '') NOT LIKE '%DRY_RUN%'
                           AND LOWER(COALESCE(tx_ref, '')) NOT LIKE 'dryrun-%'
                           AND (created_at IS NULL OR datetime(created_at) > datetime(?2));",
                        params![code, reset_trimmed],
                        |r| r.get(0),
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                (inf, outf)
            } else {
                let inf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE to_book = ?1
                           AND status IN ('COMPLETED', 'CONFIRMED', 'PARTIAL_IDLE_CAPPED')
                           AND COALESCE(transfer_mode, '') NOT LIKE 'DRY_RUN%'
                           AND COALESCE(tx_ref, '') NOT LIKE '%DRY_RUN%'
                           AND LOWER(COALESCE(tx_ref, '')) NOT LIKE 'dryrun-%';",
                        params![code],
                        |r| r.get(0),
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let outf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE from_book = ?1
                           AND status IN ('COMPLETED', 'CONFIRMED', 'PARTIAL_IDLE_CAPPED')
                           AND COALESCE(transfer_mode, '') NOT LIKE 'DRY_RUN%'
                           AND COALESCE(tx_ref, '') NOT LIKE '%DRY_RUN%'
                           AND LOWER(COALESCE(tx_ref, '')) NOT LIKE 'dryrun-%';",
                        params![code],
                        |r| r.get(0),
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                (inf, outf)
            };
            Ok(round_dp(inflow - outflow, 2))
        })
    }

    /// Single source of truth for Book A (`PIT`) PnL, equity, idle cash, and exposure components (`R10-QA`, `R11-QA`).
    pub fn compute_pit_pnl_components(&self) -> Result<PitPnlComponents> {
        let cfg_map = self.load_config()?;
        let cfg = crate::scoped::PitConfig::from_config_map(&cfg_map);
        let default_base_cap = round_dp(cfg.book_baseline_equity_usdc.max(1.0), 2);
        let initial_capital_usdc = cfg_map
            .get_f64("pit_initial_capital_usdc", default_base_cap)
            .max(1.0);
        let transfer_adj_usdc = self.get_net_book_transfer_adjustment("PIT")?;
        let closed_realized_pnl_usdc = round_dp(self.get_total_realized_pnl()?, 4);
        let open_trades = self.get_open_trades()?;
        let resting_trades = self.get_resting_pit_trades()?;
        let open_positions_count = open_trades.len();
        let resting_positions_count = resting_trades.len();
        let mut open_deployed = 0.0;
        let mut resting_notional = 0.0;
        let mut heat = 0.0;
        let mut raw_upnl = 0.0;
        let mut raw_partial_realized = 0.0;
        let mut raw_funding = 0.0;
        let mut raw_fees = 0.0;
        for t in &open_trades {
            let px = if t.current_price > 0.0 {
                t.current_price
            } else {
                t.entry_price
            };
            open_deployed += t.quantity.abs() * px;
            raw_upnl += t.unrealized_pnl_usdc;
            raw_partial_realized += t.realized_pnl_usdc;
            raw_funding += t.funding_accrued_usdc;
            raw_fees += t.fees_paid_usdc;
            heat += effective_position_open_heat_usdc(
                &t.side,
                t.entry_price,
                t.initial_stop_price,
                (t.chandelier_stop_price > 0.0).then_some(t.chandelier_stop_price),
                t.quantity,
                t.risk_budget_usdc,
                t.funding_accrued_usdc,
            );
        }
        for rt in &resting_trades {
            resting_notional += rt.notional_usdc.max(rt.quantity.abs() * rt.entry_price);
        }
        let latest_snap = self.get_latest_portfolio_snapshot()?;
        let has_baseline_reset = self.get_equity_baseline_reset_at()?.is_some();
        let yield_pnl_usdc = if has_baseline_reset {
            0.0
        } else {
            round_dp(latest_snap.accrued_yield_usdc, 4)
        };
        Ok(self.finalize_book_pnl_components(
            initial_capital_usdc,
            transfer_adj_usdc,
            closed_realized_pnl_usdc,
            raw_partial_realized,
            raw_upnl,
            raw_funding,
            raw_fees,
            yield_pnl_usdc,
            open_deployed,
            resting_notional,
            heat,
            open_positions_count,
            resting_positions_count,
        ))
    }

    /// Single source of truth for Book A (`PIT`) equity and unlocked idle USDC (`R3-H18`, `R3-QC`, `R3-N8`, `R11-QA`).
    pub fn compute_pit_equity_and_idle_usdc(&self) -> Result<(f64, f64)> {
        let comp = self.compute_pit_pnl_components()?;
        Ok((comp.book_equity_usdc, comp.idle_usdc))
    }

    /// Single source of truth for Book B (`POND`) PnL, equity, idle cash, and exposure components (`R10-QA`, `R11-QA`, `R12-N3`).
    pub fn compute_pond_pnl_components(&self) -> Result<PondPnlComponents> {
        self.compute_pond_pnl_components_with_marks()
            .map(|(comp, _)| comp)
    }

    /// Computes Book B (`POND`) PnL components along with the per-symbol 1h mark-price map
    /// so callers (e.g. `build_pond_payload`) can reuse the fetched 1h closes without duplicate SQLite queries (`R12-N3`).
    pub fn compute_pond_pnl_components_with_marks(
        &self,
    ) -> Result<(PondPnlComponents, std::collections::HashMap<String, f64>)> {
        let cfg = crate::scoped::PondConfig::from_config_map(&self.load_config()?);
        let initial_capital_usdc = round_dp(cfg.pond_baseline_equity_usdc.max(1.0), 2);
        let transfer_adj_usdc = self.get_net_book_transfer_adjustment("POND")?;
        let closed_realized_pnl_usdc = round_dp(self.get_pond_realized_pnl()?, 4);
        let open_cells = self.get_open_pond_cells()?;
        let armed_cells = self.get_armed_pond_cells()?;
        let open_positions_count = open_cells.len();
        let resting_positions_count = armed_cells.len();
        let mut open_deployed = 0.0;
        let mut resting_notional = 0.0;
        let mut heat = 0.0;
        let mut raw_upnl = 0.0;
        let mut raw_partial_realized = 0.0;
        let mut raw_funding = 0.0;
        let mut raw_fees = 0.0;
        let mut mark_cache: std::collections::HashMap<String, f64> =
            std::collections::HashMap::with_capacity(open_cells.len());
        for c in &open_cells {
            let entry_px = c.entry_price.unwrap_or(0.0);
            let qty = c.quantity.unwrap_or(0.0);
            let mark_px = if let Some(&cached_px) = mark_cache.get(&c.symbol) {
                cached_px
            } else {
                let fetched = self
                    .get_ohlcv_bars(&c.symbol, "1h", 1, None)
                    .ok()
                    .and_then(|bars| bars.last().map(|b| b.close))
                    .filter(|&p| p > 0.0)
                    .unwrap_or(entry_px);
                if fetched > 0.0 {
                    mark_cache.insert(c.symbol.clone(), fetched);
                }
                fetched
            };
            let price_pnl = if entry_px > 0.0 && qty > 0.0 {
                if c.side.eq_ignore_ascii_case("LONG") {
                    (mark_px - entry_px) * qty
                } else {
                    (entry_px - mark_px) * qty
                }
            } else {
                0.0
            };
            open_deployed += c.notional_usdc.unwrap_or(0.0);
            heat += effective_pond_cell_open_heat_usdc(
                c.risk_budget_usdc.unwrap_or(0.0),
                c.funding_accrued_usdc,
            );
            raw_upnl += round_dp(price_pnl, 4);
            // R3-H17 (NH-8): Include partial-exit realized_pnl_usdc on open/trailed cells
            raw_partial_realized += c.realized_pnl_usdc;
            raw_funding += c.funding_accrued_usdc;
            raw_fees += c.fees_paid_usdc;
        }
        // R11-QA: Resting ARMED cells lock deployed notional but do not contribute open stop heat or PnL.
        for ac in &armed_cells {
            resting_notional += ac.notional_usdc.unwrap_or(0.0);
        }
        let comp = self.finalize_book_pnl_components(
            initial_capital_usdc,
            transfer_adj_usdc,
            closed_realized_pnl_usdc,
            raw_partial_realized,
            raw_upnl,
            raw_funding,
            raw_fees,
            0.0,
            open_deployed,
            resting_notional,
            heat,
            open_positions_count,
            resting_positions_count,
        );
        Ok((comp, mark_cache))
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_book_pnl_components(
        &self,
        initial_capital_usdc: f64,
        transfer_adj_usdc: f64,
        closed_realized_pnl_usdc: f64,
        raw_partial_realized: f64,
        raw_upnl: f64,
        raw_funding: f64,
        raw_fees: f64,
        yield_pnl_usdc: f64,
        open_deployed: f64,
        resting_notional: f64,
        heat: f64,
        open_positions_count: usize,
        resting_positions_count: usize,
    ) -> BookPnlComponents {
        let partial_realized_pnl_usdc = round_dp(raw_partial_realized, 4);
        let realized_pnl_usdc = round_dp(closed_realized_pnl_usdc + partial_realized_pnl_usdc, 4);
        let unrealized_pnl_usdc = round_dp(raw_upnl, 4);
        let funding_pnl_usdc = round_dp(raw_funding, 4);
        let open_fees_usdc = round_dp(raw_fees, 4);
        // R3-N8 / R11-QA: Shared net PnL formula across PIT and POND books
        let total_net_pnl_usdc = round_dp(
            realized_pnl_usdc + unrealized_pnl_usdc + funding_pnl_usdc - open_fees_usdc
                + yield_pnl_usdc,
            4,
        );
        let deployed_notional_usdc = round_dp(open_deployed + resting_notional, 2);
        let resting_notional_usdc = round_dp(resting_notional, 2);
        let heat_usdc = round_dp(heat, 2);
        let book_equity_usdc = round_dp(
            (initial_capital_usdc + transfer_adj_usdc + total_net_pnl_usdc).max(0.0),
            2,
        );
        let mut idle_usdc = round_dp((book_equity_usdc - deployed_notional_usdc).max(0.0), 2);
        if let Ok((_, perps_trading_eq, vault_eq, total_wallet_deployed)) =
            self.get_wallet_perps_margin_state()
        {
            if vault_eq > 0.0 || total_wallet_deployed > deployed_notional_usdc {
                let avail_perps_margin =
                    round_dp((perps_trading_eq - total_wallet_deployed).max(0.0), 2);
                idle_usdc = idle_usdc.min(avail_perps_margin);
            }
        }
        BookPnlComponents {
            initial_capital_usdc,
            closed_realized_pnl_usdc,
            partial_realized_pnl_usdc,
            realized_pnl_usdc,
            unrealized_pnl_usdc,
            funding_pnl_usdc,
            open_fees_usdc,
            yield_pnl_usdc,
            total_net_pnl_usdc,
            transfer_adj_usdc,
            book_equity_usdc,
            idle_usdc,
            deployed_notional_usdc,
            resting_notional_usdc,
            heat_usdc,
            open_positions_count,
            resting_positions_count,
        }
    }

    /// Single source of truth for Book B (`POND`) equity and unlocked idle USDC (`R3-H17`, `R3-QC`, `R11-QA`).
    pub fn compute_pond_equity_and_idle_usdc(&self) -> Result<(f64, f64)> {
        let comp = self.compute_pond_pnl_components()?;
        Ok((comp.book_equity_usdc, comp.idle_usdc))
    }

    pub fn save_pp50_platform_snapshot(&self, snap: &Pp50PlatformSnapshot) -> Result<i64> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO pp50_platform_snapshots (
                    timestamp_ms, platform_equity_usdc, pit_equity_usdc, pond_equity_usdc,
                    split_pct, drift_bps, pit_open_count, pond_open_cells, settling_status
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
                params![
                    snap.timestamp_ms,
                    snap.platform_equity_usdc,
                    snap.pit_equity_usdc,
                    snap.pond_equity_usdc,
                    snap.split_pct,
                    snap.drift_bps,
                    snap.pit_open_count,
                    snap.pond_open_cells,
                    snap.settling_status,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        })
    }

    pub fn get_latest_pp50_platform_snapshot(&self) -> Result<Pp50PlatformSnapshot> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT snapshot_id, timestamp_ms, platform_equity_usdc, pit_equity_usdc,
                            pond_equity_usdc, split_pct, drift_bps, pit_open_count,
                            pond_open_cells, settling_status, created_at
                     FROM v_pp50_book_equity_latest;",
                    [],
                    |r| {
                        Ok(Pp50PlatformSnapshot {
                            snapshot_id: Some(r.get(0)?),
                            timestamp_ms: r.get(1)?,
                            platform_equity_usdc: r.get(2)?,
                            pit_equity_usdc: r.get(3)?,
                            pond_equity_usdc: r.get(4)?,
                            split_pct: r.get(5)?,
                            drift_bps: r.get(6)?,
                            pit_open_count: r.get(7)?,
                            pond_open_cells: r.get(8)?,
                            settling_status: r.get(9)?,
                            created_at: r.get(10).ok(),
                        })
                    },
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if let Some(s) = row {
                return Ok(s);
            }
            let init_eq = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'initial_equity_usdc';",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(10000.0);
            let split_pct = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'settling_split_pct';",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.5);
            let pit_eq = round_dp(init_eq * split_pct, 2);
            let pond_eq = round_dp(init_eq * (1.0 - split_pct), 2);
            Ok(Pp50PlatformSnapshot {
                snapshot_id: None,
                timestamp_ms: self.now_ms(),
                platform_equity_usdc: init_eq,
                pit_equity_usdc: pit_eq,
                pond_equity_usdc: pond_eq,
                split_pct,
                drift_bps: 0.0,
                pit_open_count: 0,
                pond_open_cells: 0,
                settling_status: "NONE".to_string(),
                created_at: None,
            })
        })
    }

    pub fn get_pond_cell_protective_stop_oid(&self, cell_id: i64) -> Result<Option<String>> {
        self.with_conn(|conn| {
            let oid: Option<String> = conn
                .query_row(
                    "SELECT protective_stop_oid FROM pond_cells WHERE cell_id = ?1;",
                    params![cell_id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?
                .flatten();
            Ok(oid.filter(|s| !s.trim().is_empty()))
        })
    }

    pub fn set_pond_cell_protective_stop_oid(
        &self,
        cell_id: i64,
        protective_stop_oid: Option<&str>,
    ) -> Result<()> {
        self.with_conn(|conn| {
            let _ = conn
                .execute(
                    "UPDATE pond_cells SET protective_stop_oid = ?1 WHERE cell_id = ?2;",
                    params![protective_stop_oid, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }
}
