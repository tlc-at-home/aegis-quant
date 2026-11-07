//! PP-50 Two-Book Platform (Pit & Pond) CRUD, cell lifecycle, inter-book transfer ledger,
//! and platform snapshot operations.

use crate::DatabaseManager;
use aegis_core::{
    round_dp, AegisError, BookRow, BookTransfer, PondCell, PondCellState, PondChampion,
    PondScanCycle, PondUniverseRow, Pp50PlatformSnapshot, Result,
};
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use std::collections::HashSet;

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
        self.with_conn(|conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            {
                let mut stmt = tx
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
                    let ref_px = if r.reference_price > 0.0 {
                        r.reference_price
                    } else {
                        10.0
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
            tx.commit()
                .map_err(|e| AegisError::Database(e.to_string()))?;
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
                        reference_price: r.get::<_, Option<f64>>(12).ok().flatten().unwrap_or(10.0),
                        is_synthetic_volume: r.get::<_, Option<i64>>(13).ok().flatten().unwrap_or(0),
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
                     WHERE is_active = 1
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
                .unwrap_or(1);
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
        })
    }

    pub fn upsert_pond_cell(&self, cell: &PondCell) -> Result<i64> {
        let _ = PondCellState::parse(&cell.state)?;
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            if cell.cell_id > 0 {
                conn.execute(
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
                         updated_at = ?26
                     WHERE cell_id = ?27;",
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
                        opened_at, closed_at, cooldown_until, updated_at
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6,
                        ?7, ?8, ?9, ?10, ?11,
                        ?12, ?13, ?14, ?15,
                        ?16, ?17, ?18,
                        ?19, ?20, ?21, ?22,
                        ?23, ?24, ?25, ?26
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
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(conn.last_insert_rowid())
            }
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
            let reset_at: String = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'equity_baseline_reset_at';",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or_default();
            let reset_trimmed = reset_at.trim().trim_matches('"');
            let pnl: f64 = if !reset_trimmed.is_empty() {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM pond_cells WHERE state IN ('CLOSED', 'COOLDOWN', 'DORMANT') AND (closed_at IS NULL OR datetime(closed_at) > datetime(?1));",
                    params![reset_trimmed],
                    |r| r.get(0),
                )
                .unwrap_or(0.0)
            } else {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM pond_cells WHERE state IN ('CLOSED', 'COOLDOWN', 'DORMANT');",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0.0)
            };
            Ok(pnl)
        })
    }

    pub fn get_open_pit_symbols(&self) -> Result<HashSet<String>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT symbol FROM trades
                     WHERE status = 'OPEN'
                       AND NOT (COALESCE(fsm_state, '') = 'OPEN_ON_BOOK' AND COALESCE(filled_units, quantity) <= 0.0);",
                )
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
                     WHERE status = 'OPEN'
                       AND NOT (COALESCE(fsm_state, '') = 'OPEN_ON_BOOK' AND COALESCE(filled_units, quantity) <= 0.0);",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0.0);
            let pond_dep: f64 = conn
                .query_row(
                    "SELECT COALESCE(SUM(COALESCE(notional_usdc, 0.0)), 0.0) FROM pond_cells WHERE state IN ('OPEN', 'TRAILED');",
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
    ) -> Result<()> {
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE pond_cells
                 SET state = 'CLOSED',
                     exit_price = ?1,
                     exit_reason = ?2,
                     closed_at = ?3,
                     updated_at = ?3
                 WHERE cell_id = ?4 AND state IN ('OPEN', 'TRAILED', 'ARMED');",
                params![exit_price, exit_reason, now_iso, cell_id],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn reconcile_open_pond_cell_size(
        &self,
        cell_id: i64,
        new_qty: f64,
        new_entry_price: Option<f64>,
    ) -> Result<()> {
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            if let Some(epx) = new_entry_price.filter(|&p| p > 0.0) {
                let notional = round_dp(new_qty.abs() * epx, 2);
                conn.execute(
                    "UPDATE pond_cells
                     SET quantity = ?1,
                         entry_price = ?2,
                         notional_usdc = ?3,
                         updated_at = ?4
                     WHERE cell_id = ?5;",
                    params![new_qty.abs(), epx, notional, now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            } else {
                conn.execute(
                    "UPDATE pond_cells
                     SET quantity = ?1,
                         notional_usdc = ROUND(?1 * COALESCE(entry_price, 0.0), 2),
                         updated_at = ?2
                     WHERE cell_id = ?3;",
                    params![new_qty.abs(), now_iso, cell_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            Ok(())
        })
    }

    pub fn update_pond_cell_funding(
        &self,
        cell_id: i64,
        funding_accrued_usdc: f64,
    ) -> Result<()> {
        let now_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE pond_cells
                 SET funding_accrued_usdc = ?1,
                     updated_at = ?2
                 WHERE cell_id = ?3;",
                params![round_dp(funding_accrued_usdc, 4), now_iso, cell_id],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn insert_book_transfer(&self, transfer: &BookTransfer) -> Result<i64> {
        let created_iso = if transfer.created_at.trim().is_empty() {
            aegis_core::now_iso8601_utc()
        } else {
            aegis_core::normalize_timestamp_iso8601_utc(&transfer.created_at)
        };
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO book_transfers (
                    settle_date, from_book, to_book, amount_usdc,
                    pre_from_usdc, pre_to_usdc, post_from_usdc, post_to_usdc,
                    drift_bps_before, drift_bps_after, blocked_remainder_usdc,
                    status, attempts, tx_ref, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15);",
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
                    transfer.status,
                    transfer.attempts,
                    transfer.tx_ref,
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
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE book_transfers
                 SET status = ?1,
                     attempts = ?2,
                     post_from_usdc = COALESCE(?3, post_from_usdc),
                     post_to_usdc = COALESCE(?4, post_to_usdc),
                     drift_bps_after = COALESCE(?5, drift_bps_after),
                     tx_ref = COALESCE(?6, tx_ref),
                     blocked_remainder_usdc = ?7
                 WHERE transfer_id = ?8;",
                params![
                    status,
                    attempts,
                    post_from_usdc,
                    post_to_usdc,
                    drift_bps_after,
                    tx_ref,
                    blocked_remainder_usdc,
                    transfer_id,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
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
    /// `book_code` across all `COMPLETED` / `PARTIAL_IDLE_CAPPED` rows in `book_transfers` (`C1`, `NL-10`).
    pub fn get_net_book_transfer_adjustment(&self, book_code: &str) -> Result<f64> {
        let code = book_code.trim().to_ascii_uppercase();
        self.with_conn(|conn| {
            let reset_at: String = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'equity_baseline_reset_at';",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or_default();
            let reset_trimmed = reset_at.trim().trim_matches('"');
            let (inflow, outflow): (f64, f64) = if !reset_trimmed.is_empty() {
                let inf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE to_book = ?1 AND status IN ('COMPLETED', 'PARTIAL_IDLE_CAPPED')
                           AND (created_at IS NULL OR datetime(created_at) > datetime(?2));",
                        params![code, reset_trimmed],
                        |r| r.get(0),
                    )
                    .unwrap_or(0.0);
                let outf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE from_book = ?1 AND status IN ('COMPLETED', 'PARTIAL_IDLE_CAPPED')
                           AND (created_at IS NULL OR datetime(created_at) > datetime(?2));",
                        params![code, reset_trimmed],
                        |r| r.get(0),
                    )
                    .unwrap_or(0.0);
                (inf, outf)
            } else {
                let inf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE to_book = ?1 AND status IN ('COMPLETED', 'PARTIAL_IDLE_CAPPED');",
                        params![code],
                        |r| r.get(0),
                    )
                    .unwrap_or(0.0);
                let outf: f64 = conn
                    .query_row(
                        "SELECT COALESCE(SUM(amount_usdc), 0.0)
                         FROM book_transfers
                         WHERE from_book = ?1 AND status IN ('COMPLETED', 'PARTIAL_IDLE_CAPPED');",
                        params![code],
                        |r| r.get(0),
                    )
                    .unwrap_or(0.0);
                (inf, outf)
            };
            Ok(round_dp(inflow - outflow, 2))
        })
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
}
