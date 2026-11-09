//! Book B (`POND`) Orchestrator Crate (`aegis-pond`).
//!
//! Implements Champion `NPS-01` (Bi-Directional Naked-Option Perp Scanner) across the
//! 80-coin dynamic Hyperliquid USDC perpetual universe (`pond_universe`), 10-state
//! Cell Finite State Machine (`pond_cells`), 6-Tier Exit Priority Engine, and
//! panic-isolated async runtime (`AssertUnwindSafe(...).catch_unwind()`).

pub mod cell;
pub mod scanner;
pub mod universe;

use crate::cell::{
    compute_cell_hourly_funding_delta, evaluate_exit_priority, transition_cell_state,
    update_cell_mark_and_chandelier, CellExitEvalInput,
};
use crate::scanner::{run_nps01_scan, PondAssetScanInput, PondScannerParams};
use crate::universe::{parse_utc_timestamp, PondUniverseManager};
use aegis_core::{
    compute_atr_from_bars, compute_trailing_z_score, now_iso8601_utc, round_dp, BookType,
    OhlcvBar, PondCell, PondCellState, PondScanCycle, PondUniverseRow, Result,
};
use aegis_db::{BookScopedDb, DatabaseManager, PondConfig};
use aegis_execution::{ExecutionEngine, MarketIngestionEngine, RestingOrderStatus};
use aegis_telemetry::TelemetryBus;
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::FutureExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

type CachedPondBars = (Vec<f64>, Vec<OhlcvBar>, Vec<OhlcvBar>);

/// Summary of an hourly Book B (`POND`) S-Cycle scan & cell management run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondCycleSummary {
    pub book_id: String,
    pub champion_id: String,
    pub direction_policy: String,
    pub status: String,
    pub cycle_id: i64,
    pub universe_active_count: usize,
    pub candidates_passed_gates: usize,
    pub entries_opened: usize,
    pub exits_triggered: usize,
    pub open_cells_after: usize,
    pub open_heat_usdc: f64,
    pub deployed_notional_usdc: f64,
    pub gross_leverage: f64,
    pub pond_equity_usdc: f64,
    pub idle_usdc: f64,
}

/// Book B (`POND`) orchestrator.
#[derive(Clone)]
pub struct PondOrchestrator {
    pub scoped_db: BookScopedDb,
    pub ingestion: MarketIngestionEngine,
    pub execution: ExecutionEngine,
    pub universe_mgr: PondUniverseManager,
    pub telemetry: Option<TelemetryBus>,
    pub last_universe_refresh_ms: Arc<Mutex<Option<i64>>>,
    pub exit_attempt_count: Arc<Mutex<HashMap<i64, u32>>>,
    pub cycle_bars_cache: Arc<Mutex<Option<HashMap<String, CachedPondBars>>>>,
}

impl PondOrchestrator {
    pub fn new(db: DatabaseManager, telemetry: Option<TelemetryBus>) -> Self {
        let scoped_db = BookScopedDb::new(db.clone(), BookType::Pond);
        let ingestion = MarketIngestionEngine::new(db.clone());
        let execution = ExecutionEngine::new(db);
        let universe_mgr = PondUniverseManager::new(scoped_db.clone(), ingestion.clone());
        Self {
            scoped_db,
            ingestion,
            execution,
            universe_mgr,
            telemetry,
            last_universe_refresh_ms: Arc::new(Mutex::new(None)),
            exit_attempt_count: Arc::new(Mutex::new(HashMap::new())),
            cycle_bars_cache: Arc::new(Mutex::new(None)),
        }
    }

    /// Loads the Book B (`PondConfig`) namespace view.
    pub fn load_pond_config(&self) -> Result<PondConfig> {
        self.scoped_db.load_pond_config()
    }

    /// Returns true if the platform-wide emergency halt, Pond kill switch, or daily 00:05 UTC settling freeze is active (`NH-10`, `NM-3`).
    pub fn is_entry_frozen(&self) -> Result<(bool, String)> {
        let cfg = self.load_pond_config()?;
        if cfg.platform_pp50_halt {
            return Ok((true, "PLATFORM_PP50_HALT".to_string()));
        }
        if cfg.pond_kill_switch {
            return Ok((true, "POND_KILL_SWITCH".to_string()));
        }
        if cfg.platform_settle_gate_enabled || self.scoped_db.inner().is_entry_frozen() {
            return Ok((true, "SETTLING_IN_PROGRESS".to_string()));
        }
        Ok((false, "ACTIVE".to_string()))
    }

    /// Bootstraps or refreshes the 80-coin dynamic Pond universe (`C7`).
    pub fn initialize_or_refresh_universe(&self) -> Result<Vec<PondUniverseRow>> {
        let rows = self.universe_mgr.refresh_universe(None)?;
        *self.last_universe_refresh_ms.lock() = Some(self.scoped_db.now_ms());
        Ok(rows)
    }

    /// Computes Book B (`POND`) total equity and unlocked idle USDC (`C1`, `C3`, `R3-H17`, `R3-QC`).
    /// Includes net settled inter-book transfers and mark-to-market price PnL minus fees for open cells.
    pub fn compute_pond_equity_and_idle_usdc(&self) -> Result<(f64, f64)> {
        self.scoped_db.compute_pond_equity_and_idle_usdc()
    }

    /// Reconciles resting `ARMED` orders (`B8`, `R3-H4`, `R3-H16`) before evaluating open cells.
    /// - Confirmed fills (`filled_units > 0.0`) transition `ARMED -> OPEN` and book maker/fill fees.
    /// - Confirmed absent resting orders (`GONE` / `CANCELLED_OR_GONE`) transition `ARMED -> REJECTED`.
    /// - Unreachable `/info` (`UNKNOWN`) preserves `ARMED` state.
    /// - Stale resting orders at a new 1H boundary are cancelled on the exchange first and only transition
    ///   to `REJECTED` if cancellation is confirmed.
    pub fn reconcile_armed_cells(&self, is_1h_boundary: bool) -> Result<usize> {
        let armed_cells = self.scoped_db.get_armed_pond_cells()?;
        if armed_cells.is_empty() {
            return Ok(0);
        }
        let cfg = self.load_pond_config()?;
        let maker_fee_bps = self
            .scoped_db
            .inner()
            .load_config()
            .map(|m| m.get_f64("maker_fee_bps", 1.5))
            .unwrap_or(1.5);
        let now = Utc::now();
        let now_str = now_iso8601_utc();
        let mut promoted = 0usize;

        for mut cell in armed_cells {
            let resting_oid = cell
                .resting_order_id
                .clone()
                .or_else(|| {
                    cell.exit_reason
                        .as_deref()
                        .and_then(|r| r.strip_prefix("RESTING_OID:"))
                        .map(|s| s.to_string())
                });
            let mut status: RestingOrderStatus = self
                .execution
                .check_resting_order_status(&cell.symbol, resting_oid.as_deref());
            let mut cancel_confirmed_at_boundary = false;
            if is_1h_boundary
                && !(status.state == "FILLED" && status.filled_units > 0.0)
                && status.state != "GONE"
                && status.state != "CANCELLED_OR_GONE"
            {
                if let Some(oid_str) = resting_oid.as_deref() {
                    cancel_confirmed_at_boundary = self
                        .execution
                        .cancel_resting_order(&cell.symbol, oid_str)
                        .unwrap_or(false);
                    status = self.execution.verify_post_cancel_resting_order(
                        &cell.symbol,
                        oid_str,
                        cancel_confirmed_at_boundary,
                    );
                } else {
                    cancel_confirmed_at_boundary = true;
                }
            }
            match status.state.as_str() {
                "FILLED" if status.filled_units > 0.0 => {
                    let fill_px = if status.entry_price > 0.0 {
                        status.entry_price
                    } else {
                        cell.entry_price.unwrap_or(100.0)
                    };
                    let entry_fee = if status.fee_usdc > 0.0 {
                        status.fee_usdc
                    } else {
                        round_dp(status.filled_units * fill_px * (maker_fee_bps / 10_000.0), 4)
                    };
                    cell.entry_price = Some(fill_px);
                    cell.quantity = Some(status.filled_units);
                    cell.notional_usdc = Some(round_dp(status.filled_units * fill_px, 2));
                    cell.fees_paid_usdc = round_dp(cell.fees_paid_usdc + entry_fee, 4);
                    cell.opened_at = Some(now_str.clone());
                    cell.exit_reason = None;
                    cell.resting_order_id = None;
                    transition_cell_state(&mut cell, PondCellState::Open)?;
                    self.scoped_db.upsert_pond_cell(&cell)?;
                    promoted += 1;
                }
                "GONE" | "CANCELLED_OR_GONE" => {
                    let reason = if is_1h_boundary && cancel_confirmed_at_boundary {
                        "RESTING_ENTRY_UNFILLED_EXPIRED"
                    } else {
                        "RESTING_ENTRY_CANCELLED_OR_EXPIRED"
                    };
                    cell.exit_reason = Some(reason.to_string());
                    cell.resting_order_id = None;
                    cell.closed_at = Some(now_str.clone());
                    cell.cooldown_until = Some(
                        (now + ChronoDuration::hours(cfg.pond_cooldown_hours.max(1)))
                            .format("%Y-%m-%dT%H:%M:%SZ")
                            .to_string(),
                    );
                    transition_cell_state(&mut cell, PondCellState::Rejected)?;
                    self.scoped_db.upsert_pond_cell(&cell)?;
                }
                "UNKNOWN" => {
                    // R3-H4: /info endpoint unreachable; preserve ARMED state and do not blindly mark REJECTED
                    if is_1h_boundary {
                        if resting_oid.is_none() {
                            cell.exit_reason = Some("RESTING_ENTRY_UNFILLED_EXPIRED".to_string());
                            cell.resting_order_id = None;
                            cell.closed_at = Some(now_str.clone());
                            cell.cooldown_until = Some(
                                (now + ChronoDuration::hours(cfg.pond_cooldown_hours.max(1)))
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                            );
                            transition_cell_state(&mut cell, PondCellState::Rejected)?;
                            self.scoped_db.upsert_pond_cell(&cell)?;
                        } else {
                            tracing::warn!(
                                symbol = %cell.symbol,
                                cell_id = cell.cell_id,
                                "Resting ARMED Pond order status is UNKNOWN at 1H boundary; keeping cell ARMED (R3-H4)"
                            );
                        }
                    }
                }
                _ => {
                    if is_1h_boundary {
                        if cancel_confirmed_at_boundary && status.state != "OPEN_ON_BOOK" {
                            cell.exit_reason = Some("RESTING_ENTRY_UNFILLED_EXPIRED".to_string());
                            cell.resting_order_id = None;
                            cell.closed_at = Some(now_str.clone());
                            cell.cooldown_until = Some(
                                (now + ChronoDuration::hours(cfg.pond_cooldown_hours.max(1)))
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string(),
                            );
                            transition_cell_state(&mut cell, PondCellState::Rejected)?;
                            self.scoped_db.upsert_pond_cell(&cell)?;
                        } else {
                            tracing::warn!(
                                symbol = %cell.symbol,
                                cell_id = cell.cell_id,
                                "Cancel of resting ARMED Pond order at 1H boundary was not confirmed; keeping cell ARMED (R3-H4)"
                            );
                        }
                    }
                }
            }
        }
        Ok(promoted)
    }

    /// Evaluates open Pond cells on a 15-second tick (or hourly/4H boundary) and executes exits.
    pub fn evaluate_open_cells(
        &self,
        is_1h_boundary: bool,
        is_4h_bar_close: bool,
    ) -> Result<usize> {
        self.scoped_db.check_table_access("pond_cells")?;
        self.scoped_db.check_table_access("pond_universe")?;

        // Reconcile any resting ARMED entry orders first (B8)
        let _ = self.reconcile_armed_cells(is_1h_boundary)?;

        let cfg = self.load_pond_config()?;
        let is_live_or_testnet =
            cfg.pond_execution_mode == "LIVE" || cfg.pond_execution_mode == "TESTNET";
        let max_staleness_ms = self
            .scoped_db
            .inner()
            .load_config()
            .map(|m| m.get_i64("max_bar_staleness_ms", 28_800_000))
            .unwrap_or(28_800_000);
        let now_ms = self.scoped_db.now_ms();
        let now_hour_bucket = (now_ms / 3_600_000).max(1);
        let wick_mult = cfg.pond_wick_buffer_atr_mult;
        let chand_mult = cfg.pond_chandelier_atr_mult;
        let z_exit = cfg.pond_funding_z_exit;
        let min_fund = cfg.pond_funding_floor_hourly;
        let max_hold = cfg.pond_max_hold_hours as f64;
        let cd_stop = cfg.pond_stop_cooldown_hours;
        let cd_norm = cfg.pond_cooldown_hours;

        let open_cells = self.scoped_db.get_open_pond_cells()?;
        if open_cells.is_empty() {
            return Ok(0);
        }

        let universe = self.scoped_db.get_pond_universe(false)?;
        let now = Utc::now();
        let now_str = now_iso8601_utc();
        let mut exits_triggered = 0usize;

        for mut cell in open_cells {
            let univ_row = universe.iter().find(|u| u.symbol == cell.symbol);
            let rank = univ_row.map(|u| u.rank as usize).unwrap_or(10);
            let univ_status = univ_row
                .map(|u| u.status.clone())
                .unwrap_or_else(|| "ACTIVE".to_string());

            let cached_opt = self
                .cycle_bars_cache
                .lock()
                .as_ref()
                .and_then(|m| m.get(&cell.symbol).cloned());
            let (funding_series, bars_1h, bars_4h) = match cached_opt {
                Some(triple) => triple,
                None => {
                    let loaded = self.ingestion.load_or_fetch_pond_funding_and_bars(
                        &cell.symbol,
                        rank,
                        180,
                        None,
                        true,
                    );
                    if let Some(cache_map) = self.cycle_bars_cache.lock().as_mut() {
                        cache_map.insert(cell.symbol.clone(), loaded.clone());
                    }
                    loaded
                }
            };
            let atr_1h = compute_atr_from_bars(&bars_1h, cfg.pond_atr_period.max(1)).max(0.01);
            let atr_4h = compute_atr_from_bars(&bars_4h, cfg.pond_atr_period.max(1))
                .max(atr_1h * cfg.pond_gate6_atr_4h_floor_mult);
            let last_1h = bars_1h.last();
            let bars_1h_fresh = !is_live_or_testnet
                || last_1h
                    .map(|b| now_ms.saturating_sub(b.timestamp_ms) <= max_staleness_ms)
                    .unwrap_or(false);
            let bars_4h_fresh = !is_live_or_testnet
                || bars_4h
                    .last()
                    .map(|b| now_ms.saturating_sub(b.timestamp_ms) <= max_staleness_ms)
                    .unwrap_or(false);
            if is_live_or_testnet && !bars_1h_fresh {
                tracing::warn!(
                    symbol = %cell.symbol,
                    cell_id = cell.cell_id,
                    "Book B (POND) 1H bar is stale or missing in LIVE/TESTNET mode; skipping hourly funding accrual and non-emergency bar exits"
                );
            }
            let effective_1h_boundary = is_1h_boundary && bars_1h_fresh;
            let effective_4h_close = is_4h_bar_close && bars_4h_fresh;

            let entry_px = cell.entry_price.unwrap_or(100.0);
            let mark_px = last_1h
                .map(|b| b.close)
                .filter(|&p| p > 0.0)
                .unwrap_or(entry_px);
            let high_px = last_1h.map(|b| b.high).unwrap_or(mark_px);
            let low_px = last_1h.map(|b| b.low).unwrap_or(mark_px);
            let bar_4h_close = bars_4h.last().map(|b| b.close).unwrap_or(mark_px);
            let cur_funding = funding_series
                .last()
                .copied()
                .unwrap_or_else(|| cell.funding_rate_entry.unwrap_or(0.0));
            let (cur_z, _, _) = compute_trailing_z_score(
                &funding_series,
                cfg.pond_funding_z_window_hours.max(1),
            );

            // R3-B1 & R3-H1: Atomic hourly funding accrual guarded by last_funding_hour_bucket
            if effective_1h_boundary {
                let delta = compute_cell_hourly_funding_delta(&cell, mark_px, cur_funding);
                if self.scoped_db.accrue_pond_cell_funding_for_hour(
                    cell.cell_id,
                    now_hour_bucket,
                    delta,
                )? {
                    cell.funding_accrued_usdc = round_dp(cell.funding_accrued_usdc + delta, 4);
                }
            }
            let _price_pnl = update_cell_mark_and_chandelier(
                &mut cell,
                mark_px,
                high_px,
                low_px,
                atr_4h,
                chand_mult,
                effective_4h_close,
            );

            let hold_hours = cell
                .opened_at
                .as_deref()
                .and_then(parse_utc_timestamp)
                .map(|dt| (now.naive_utc() - dt).num_minutes() as f64 / 60.0)
                .unwrap_or(1.0)
                .max(0.0);

            let eval_input = CellExitEvalInput {
                mark_price: mark_px,
                intrabar_high: high_px,
                intrabar_low: low_px,
                atr_14_1h: atr_1h,
                bar_4h_close,
                atr_14_4h: atr_4h,
                is_4h_bar_close: effective_4h_close,
                is_1h_boundary: effective_1h_boundary,
                current_funding_hourly: cur_funding,
                current_funding_z: cur_z,
                hold_hours,
                universe_status: univ_status,
                wick_buffer_atr_mult: wick_mult,
                chandelier_atr_mult: chand_mult,
                z_exit_threshold: z_exit,
                min_funding_abs: min_fund,
                max_hold_hours: max_hold,
                cooldown_stop_hours: cd_stop,
                cooldown_normal_hours: cd_norm,
            };

            let decision = evaluate_exit_priority(&cell, &eval_input);
            if decision.should_exit {
                let qty = cell.quantity.unwrap_or(0.0);
                let close_side = if cell.side == "LONG" { "SELL" } else { "BUY" };
                let tick_sz = univ_row.map(|u| u.tick_size).unwrap_or(0.0001);
                let lot_sz = univ_row.map(|u| u.lot_step_size).unwrap_or(0.001);

                // Escalate maker exit to market/IOC on retry or overdue max-hold timeout (NM-7)
                let prev_attempts = self
                    .exit_attempt_count
                    .lock()
                    .get(&cell.cell_id)
                    .copied()
                    .unwrap_or(0);
                let effective_prefer_maker = decision.prefer_maker
                    && prev_attempts == 0
                    && !(decision.tier == Some(5) && hold_hours >= max_hold + 1.0);

                let fill = self.execution.execute_pond_order(
                    &cell.symbol,
                    close_side,
                    qty,
                    mark_px,
                    tick_sz,
                    lot_sz,
                    high_px,
                    low_px,
                    last_1h.map(|b| b.volume).unwrap_or(50_000.0),
                    decision.tier == Some(1) || prev_attempts > 0,
                    effective_prefer_maker,
                    true,
                    None,
                );
                if is_live_or_testnet && fill.fsm_state != "FILLED" {
                    *self
                        .exit_attempt_count
                        .lock()
                        .entry(cell.cell_id)
                        .or_insert(0) += 1;
                    tracing::warn!(
                        "Book B (POND) live exit order for {} (cell #{}, attempt {}) did not fill (state={}); keeping cell OPEN for retry",
                        cell.symbol,
                        cell.cell_id,
                        prev_attempts + 1,
                        fill.fsm_state
                    );
                    self.scoped_db.update_open_pond_cell_watermarks(
                        cell.cell_id,
                        &cell.state,
                        cell.chandelier_stop,
                        cell.highest_price_seen,
                        cell.lowest_price_seen,
                    )?;
                } else if is_live_or_testnet
                    && fill.filled_units > 0.0
                    && fill.filled_units + 1e-8 < qty
                {
                    // Partial live exit fill (NH-8, R3-H1): atomic SQL reduction of cell quantity & notional
                    let exit_px = if fill.avg_fill_price > 0.0 {
                        fill.avg_fill_price
                    } else {
                        mark_px
                    };
                    let partial_gross = if cell.side == "LONG" {
                        (exit_px - entry_px) * fill.filled_units
                    } else {
                        (entry_px - exit_px) * fill.filled_units
                    };
                    let rem_qty = round_dp((qty - fill.filled_units).max(0.0), 6);
                    let rem_notional = round_dp(rem_qty * entry_px, 2);
                    let _ = self.scoped_db.reduce_open_pond_cell_partial_exit(
                        cell.cell_id,
                        &cell.state,
                        cell.chandelier_stop,
                        cell.highest_price_seen,
                        cell.lowest_price_seen,
                        rem_qty,
                        rem_notional,
                        partial_gross,
                        fill.fees_paid_usdc,
                    )?;
                    *self
                        .exit_attempt_count
                        .lock()
                        .entry(cell.cell_id)
                        .or_insert(0) += 1;
                } else {
                    self.exit_attempt_count.lock().remove(&cell.cell_id);
                    let exit_px = if fill.avg_fill_price > 0.0 {
                        fill.avg_fill_price
                    } else {
                        mark_px
                    };
                    let gross_pnl = if cell.side == "LONG" {
                        (exit_px - entry_px) * qty
                    } else {
                        (entry_px - exit_px) * qty
                    };
                    let exit_reason_str = decision
                        .exit_reason
                        .clone()
                        .unwrap_or_else(|| "CELL_EXIT".to_string());
                    let cooldown_str = (now + ChronoDuration::hours(decision.cooldown_hours))
                        .format("%Y-%m-%dT%H:%M:%SZ")
                        .to_string();
                    transition_cell_state(&mut cell, PondCellState::Closed)?;
                    transition_cell_state(&mut cell, PondCellState::Cooldown)?;
                    // R3-H1: Atomic SQL close reading current DB funding_accrued_usdc and fees_paid_usdc
                    if self.scoped_db.close_open_pond_cell(
                        cell.cell_id,
                        &cell.state,
                        cell.chandelier_stop,
                        cell.highest_price_seen,
                        cell.lowest_price_seen,
                        exit_px,
                        gross_pnl,
                        fill.fees_paid_usdc,
                        Some(&exit_reason_str),
                        &now_str,
                        &cooldown_str,
                    )? {
                        exits_triggered += 1;
                    }
                }
            } else {
                // R3-H1: Targeted watermark & Chandelier update without overwriting funding/fees/size
                self.scoped_db.update_open_pond_cell_watermarks(
                    cell.cell_id,
                    &cell.state,
                    cell.chandelier_stop,
                    cell.highest_price_seen,
                    cell.lowest_price_seen,
                )?;
            }
        }

        let _ = self.universe_mgr.finalize_delisted_retirements();
        Ok(exits_triggered)
    }

    /// Executes a scheduled Book B (`POND`) step: hourly S-Cycle scan on UTC hour boundary
    /// (with 4H close flag on 4H boundary), or 15s tick otherwise (`R5-M3`, `R6-QA`).
    pub fn run_scheduled_step(
        &self,
        last_scan_hour_bucket: &mut Option<i64>,
        last_4h_close_bucket: &mut Option<i64>,
    ) -> Result<Option<PondCycleSummary>> {
        let now_ms = self.scoped_db.now_ms();
        let now_hour_bucket = now_ms / 3_600_000;
        let now_4h_bucket = now_ms / 14_400_000;
        let is_hourly_scan = *last_scan_hour_bucket != Some(now_hour_bucket);
        let is_4h_close = *last_4h_close_bucket != Some(now_4h_bucket);

        if is_hourly_scan || is_4h_close {
            match self.run_scan_cycle(is_4h_close) {
                Ok(summary) => {
                    *last_scan_hour_bucket = Some(now_hour_bucket);
                    if is_4h_close {
                        *last_4h_close_bucket = Some(now_4h_bucket);
                    }
                    Ok(Some(summary))
                }
                Err(e) => {
                    tracing::error!("Hourly Pond scan cycle failed: {e}");
                    let _ = self.scoped_db.log_telemetry(
                        "ERROR",
                        "pond.orchestrator",
                        &format!("Hourly Pond scan cycle failed: {e}"),
                        None,
                    );
                    Err(e)
                }
            }
        } else {
            if let Err(e) = self.run_tick_15s() {
                tracing::error!("Pond 15s tick failed: {e}");
                let _ = self.scoped_db.log_telemetry(
                    "ERROR",
                    "pond.orchestrator",
                    &format!("Pond 15s tick failed: {e}"),
                    None,
                );
                return Err(e);
            }
            Ok(None)
        }
    }

    /// Executes the 15-second tick loop step for open Pond cells (Tier 1 Catastrophic Wick Stop).
    pub fn run_tick_15s(&self) -> Result<usize> {
        if let Ok(stranded) = self.scoped_db.check_and_record_stranded_transfer_alert() {
            if !stranded.is_empty() {
                let ids: Vec<i64> = stranded.iter().map(|r| r.transfer_id).collect();
                let _ = self.execution.alerts.notify_critical_event(
                    "STRANDED_BOOK_TRANSFER",
                    &format!(
                        "{} book_transfers row(s) require operator reconciliation or have been PENDING past TTL: {:?}",
                        stranded.len(),
                        ids
                    ),
                    Some(json!({ "transfer_ids": ids })),
                );
            }
        }
        self.evaluate_open_cells(false, false)
    }

    /// Executes a complete hourly Book B (`POND`) S-Cycle scan across the 80-coin dynamic universe.
    pub fn run_scan_cycle(&self, is_4h_bar_close: bool) -> Result<PondCycleSummary> {
        self.scoped_db.check_table_access("pond_universe")?;
        self.scoped_db.check_table_access("pond_cells")?;
        self.scoped_db.check_table_access("pond_scan_cycles")?;
        self.scoped_db.check_table_access("pond_champions")?;
        self.scoped_db.check_table_access("books")?;

        let cfg = self.load_pond_config()?;
        let is_live_or_testnet =
            cfg.pond_execution_mode == "LIVE" || cfg.pond_execution_mode == "TESTNET";
        let (frozen, freeze_reason) = self.is_entry_frozen()?;

        // 1. Ensure 80-coin universe is populated and periodically refreshed (C7)
        let mut universe = self.scoped_db.get_pond_universe(false)?;
        let now_ms = self.scoped_db.now_ms();
        let refresh_interval_ms = cfg.pond_universe_refresh_interval_hours.max(1) * 3_600_000;
        let needs_periodic_refresh = self
            .last_universe_refresh_ms
            .lock()
            .map(|last_ms| now_ms - last_ms >= refresh_interval_ms)
            .unwrap_or(false);
        if universe.is_empty() || needs_periodic_refresh {
            universe = self.initialize_or_refresh_universe()?;
        } else {
            let mut guard = self.last_universe_refresh_ms.lock();
            if guard.is_none() {
                *guard = Some(now_ms);
            }
        }

        // NL-11: Enable per-cycle bar cache so evaluate_open_cells and scan_inputs share fetched bars
        *self.cycle_bars_cache.lock() = Some(HashMap::new());

        // 2. Evaluate open cells at 1H boundary (and 4H boundary if applicable)
        let exits_triggered = match self.evaluate_open_cells(true, is_4h_bar_close) {
            Ok(v) => v,
            Err(e) => {
                *self.cycle_bars_cache.lock() = None;
                return Err(e);
            }
        };

        // 3. Recompute open/armed cells, heat, notional, and cooldown set (including cross-book open wallet symbols)
        let open_cells = self.scoped_db.get_open_pond_cells()?;
        let armed_cells = self.scoped_db.get_armed_pond_cells()?;
        let recent_cells = self.scoped_db.get_pond_cells(200)?;
        let now = Utc::now();
        let now_str = now_iso8601_utc();

        let mut open_or_cooldown_symbols: HashSet<String> = open_cells
            .iter()
            .chain(armed_cells.iter())
            .map(|c| c.symbol.clone())
            .collect();
        if let Ok(wallet_syms) = self.scoped_db.get_wallet_open_symbols() {
            open_or_cooldown_symbols.extend(wallet_syms);
        }
        for c in &recent_cells {
            if c.state == "COOLDOWN" {
                let active_cd = c
                    .cooldown_until
                    .as_deref()
                    .map(|until| {
                        parse_utc_timestamp(until)
                            .map(|dt| dt > now.naive_utc())
                            .unwrap_or_else(|| until > now_str.as_str())
                    })
                    .unwrap_or(false);
                if active_cd {
                    open_or_cooldown_symbols.insert(c.symbol.clone());
                } else {
                    // NM-14: Persist COOLDOWN -> DORMANT transition when cooldown_until has expired
                    let mut expired_cell = c.clone();
                    expired_cell.updated_at = now_str.clone();
                    transition_cell_state(&mut expired_cell, PondCellState::Dormant)?;
                    self.scoped_db.upsert_pond_cell(&expired_cell)?;
                }
            }
        }

        let mut open_heat: f64 = open_cells
            .iter()
            .chain(armed_cells.iter())
            .map(|c| c.risk_budget_usdc.unwrap_or(0.0))
            .sum();
        let mut deployed_notional: f64 = open_cells
            .iter()
            .chain(armed_cells.iter())
            .map(|c| c.notional_usdc.unwrap_or(0.0))
            .sum();
        let (pond_eq, _) = self.compute_pond_equity_and_idle_usdc()?;

        let mut effective_max_gross_leverage = cfg.pond_max_gross_leverage;
        if let Ok((_, perps_trading_eq, vault_eq, total_wallet_deployed)) =
            self.scoped_db.get_wallet_perps_margin_state()
        {
            if vault_eq > 0.0 || total_wallet_deployed > deployed_notional {
                let other_book_deployed = (total_wallet_deployed - deployed_notional).max(0.0);
                let avail_for_pond = (perps_trading_eq - other_book_deployed).max(0.0);
                let perps_cap_lev = avail_for_pond / pond_eq.max(1.0);
                effective_max_gross_leverage = effective_max_gross_leverage.min(perps_cap_lev);
            }
        }

        let active_champ = self.scoped_db.get_active_pond_champion()?;
        let champion_id_num = active_champ.champion_id;
        let champion_version = active_champ.version;
        let direction_policy = cfg.pond_direction_policy;

        let params = PondScannerParams {
            champion_id: champion_version.clone(),
            direction_policy,
            funding_z_window_hours: cfg.pond_funding_z_window_hours,
            z_entry_threshold: cfg.pond_funding_z_entry,
            min_funding_abs: cfg.pond_funding_floor_hourly,
            min_daily_vol_usdc: cfg.pond_min_daily_volume_usdc,
            liquidity_score_ref_usdc: cfg.pond_liquidity_score_ref_usdc,
            trend_veto_ema_fast: cfg.pond_trend_veto_ema_fast,
            trend_veto_ema_slow: cfg.pond_trend_veto_ema_slow,
            trend_veto_overextension_pct: cfg.pond_trend_veto_overextension_pct,
            blowoff_atr_mult: cfg.pond_blowoff_atr_mult,
            blowoff_3h_adverse_atr_mult: cfg.pond_blowoff_3h_adverse_atr_mult,
            trend_veto_4h_atr_mult: cfg.pond_trend_veto_4h_atr_mult,
            initial_stop_atr_mult: cfg.pond_initial_stop_atr_mult,
            chandelier_atr_mult: cfg.pond_chandelier_atr_mult,
            min_cell_risk_usdc: cfg.pond_min_risk_usdc,
            max_cell_risk_usdc: cfg.pond_max_risk_usdc,
            max_cell_notional_usdc: cfg.pond_max_cell_notional_usdc,
            max_open_heat_usdc: cfg.pond_max_heat_usdc,
            max_concurrent_cells: cfg.pond_max_concurrent_cells,
            max_entries_per_cycle: cfg.pond_max_opens_per_cycle,
            max_gross_leverage: effective_max_gross_leverage,
            pond_equity_usdc: pond_eq,
            current_open_cells: open_cells.len() + armed_cells.len(),
            current_open_heat_usdc: open_heat,
            current_deployed_notional_usdc: deployed_notional,
            entry_frozen: frozen,
            atr_period: cfg.pond_atr_period,
            gate6_atr_4h_floor_mult: cfg.pond_gate6_atr_4h_floor_mult,
            min_stop_pct_floor: cfg.pond_min_stop_pct_floor,
            conviction_z_span: cfg.pond_conviction_z_span,
            conviction_scale_min: cfg.pond_conviction_scale_min,
            conviction_scale_max: cfg.pond_conviction_scale_max,
            reject_synthetic_volume: is_live_or_testnet,
        };

        // 4. Build scan inputs across all universe coins (reusing cached bars from evaluate_open_cells, NL-11)
        let live_funding_map = if is_live_or_testnet {
            Some(self.ingestion.fetch_hyperliquid_funding_map())
        } else {
            None
        };
        let cached_map = self.cycle_bars_cache.lock().take().unwrap_or_default();
        let mut scan_inputs = Vec::with_capacity(universe.len());
        for u in &universe {
            let rank = u.rank.max(1) as usize;
            let (funding_series, bars_1h, bars_4h) = match cached_map.get(&u.symbol) {
                Some(triple) => triple.clone(),
                None => self.ingestion.load_or_fetch_pond_funding_and_bars(
                    &u.symbol,
                    rank,
                    180,
                    live_funding_map.as_ref(),
                    true,
                ),
            };
            scan_inputs.push(PondAssetScanInput {
                universe_row: u.clone(),
                funding_series_168h: funding_series,
                bars_1h,
                bars_4h,
                tick_size: u.tick_size,
                lot_step_size: u.lot_step_size,
            });
        }

        let scan_output = run_nps01_scan(&scan_inputs, &params, &open_or_cooldown_symbols);

        // Insert S-Cycle row first so we have `cycle_id` for new cells
        let mut cycle_row = PondScanCycle {
            cycle_id: 0,
            started_at: now_str.clone(),
            champion_id: champion_id_num,
            coins_scanned: scan_output.active_universe_count as i64,
            candidates: scan_output.candidates_passed_gates as i64,
            armed: scan_output.selected_entries.len() as i64,
            opened: 0,
            exits_evaluated: exits_triggered as i64,
        };
        let cycle_id = self.scoped_db.insert_pond_scan_cycle(&cycle_row)?;
        cycle_row.cycle_id = cycle_id;

        let mut entries_opened = 0usize;

        for cand in &scan_output.selected_entries {
            let mut new_cell = PondCell {
                cell_id: 0,
                symbol: cand.symbol.clone(),
                champion_id: champion_id_num,
                state: PondCellState::Dormant.as_str().to_string(),
                cycle_opened_id: Some(cycle_id),
                side: cand.side.as_str().to_string(),
                option_analogue: cand.option_analogue.as_str().to_string(),
                entry_price: None,
                quantity: Some(cand.quantity),
                notional_usdc: Some(cand.notional_usdc),
                risk_budget_usdc: Some(cand.risk_budget_usdc),
                conviction_multiplier: Some(cand.conviction_multiplier),
                funding_z_entry: Some(cand.funding_z_score),
                funding_rate_entry: Some(cand.funding_rate_hourly),
                initial_stop: Some(cand.initial_stop_price),
                chandelier_stop: Some(cand.chandelier_stop_price),
                highest_price_seen: Some(cand.bar_high.max(cand.reference_price)),
                lowest_price_seen: Some(cand.bar_low.min(cand.reference_price)),
                funding_accrued_usdc: 0.0,
                realized_pnl_usdc: 0.0,
                fees_paid_usdc: 0.0,
                exit_price: None,
                exit_reason: None,
                resting_order_id: None,
                opened_at: None,
                closed_at: None,
                cooldown_until: None,
                updated_at: now_str.clone(),
            };

            transition_cell_state(&mut new_cell, PondCellState::Screened)?;
            transition_cell_state(&mut new_cell, PondCellState::Candidate)?;
            transition_cell_state(&mut new_cell, PondCellState::Armed)?;

            let fill = self.execution.execute_pond_order(
                &cand.symbol,
                cand.side.as_str(),
                cand.quantity,
                cand.reference_price,
                cand.tick_size,
                cand.lot_step_size,
                cand.bar_high,
                cand.bar_low,
                cand.bar_volume,
                false,
                true,
                false,
                None,
            );
            if fill.fsm_state == "OPEN_ON_BOOK" || (is_live_or_testnet && fill.filled_units <= 0.0 && fill.fsm_state == "FILLED") {
                // B8 + R4-N11: Resting order is NOT an open position. Keep cell in ARMED state and track resting OID in resting_order_id.
                let resting_oid = if !fill.exchange_order_id.trim().is_empty() {
                    fill.exchange_order_id.as_str()
                } else {
                    fill.client_order_id.as_str()
                };
                new_cell.entry_price = Some(cand.reference_price);
                new_cell.resting_order_id = Some(resting_oid.to_string());
                if let Err(e) = self.scoped_db.upsert_pond_cell(&new_cell) {
                    let _ = self.execution.cancel_resting_order(&cand.symbol, resting_oid);
                    return Err(e);
                }
                open_heat = round_dp(open_heat + cand.risk_budget_usdc, 2);
                deployed_notional = round_dp(deployed_notional + cand.notional_usdc, 2);
                tracing::info!(
                    "Book B (POND) entry order for {} resting on book (oid={}); cell kept ARMED awaiting fill confirmation",
                    cand.symbol,
                    resting_oid
                );
                continue;
            }
            if fill.fsm_state != "FILLED" || fill.filled_units <= 0.0 {
                new_cell.exit_reason = Some(format!("ENTRY_NOT_FILLED:{}", fill.fsm_state));
                new_cell.closed_at = Some(now_str.clone());
                transition_cell_state(&mut new_cell, PondCellState::Rejected)?;
                self.scoped_db.upsert_pond_cell(&new_cell)?;
                tracing::warn!(
                    "Book B (POND) entry order for {} not filled (state={}); skipping cell open",
                    cand.symbol,
                    fill.fsm_state
                );
                continue;
            }
            let fill_px = if fill.avg_fill_price > 0.0 {
                fill.avg_fill_price
            } else {
                cand.reference_price
            };
            let actual_qty = if fill.filled_units > 0.0 {
                fill.filled_units
            } else {
                cand.quantity
            };
            let fill_notional = round_dp(actual_qty * fill_px, 2);
            new_cell.entry_price = Some(fill_px);
            new_cell.quantity = Some(actual_qty);
            new_cell.notional_usdc = Some(fill_notional);
            new_cell.fees_paid_usdc = fill.fees_paid_usdc;
            new_cell.opened_at = Some(now_str.clone());
            transition_cell_state(&mut new_cell, PondCellState::Open)?;

            self.scoped_db.upsert_pond_cell(&new_cell)?;
            entries_opened += 1;
            open_heat = round_dp(open_heat + cand.risk_budget_usdc, 2);
            deployed_notional = round_dp(deployed_notional + fill_notional, 2);
        }

        cycle_row.opened = entries_opened as i64;
        self.scoped_db.update_pond_scan_cycle(&cycle_row)?;

        let open_cells_after = open_cells.len() + entries_opened;
        let (pond_eq_after, idle_after) = self.compute_pond_equity_and_idle_usdc()?;
        let gross_lev = round_dp(deployed_notional / pond_eq_after.max(1.0), 4);

        let summary = PondCycleSummary {
            book_id: "POND".to_string(),
            champion_id: champion_version,
            direction_policy: direction_policy.as_str().to_string(),
            status: freeze_reason,
            cycle_id,
            universe_active_count: scan_output.active_universe_count,
            candidates_passed_gates: scan_output.candidates_passed_gates,
            entries_opened,
            exits_triggered,
            open_cells_after,
            open_heat_usdc: round_dp(open_heat, 2),
            deployed_notional_usdc: round_dp(deployed_notional, 2),
            gross_leverage: gross_lev,
            pond_equity_usdc: pond_eq_after,
            idle_usdc: idle_after,
        };

        if let Some(bus) = &self.telemetry {
            bus.publish_event(
                "INFO",
                "aegis-pond.scan_cycle",
                &format!(
                    "Book B (POND) S-Cycle #{cycle_id}: active={} candidates={} opened={} open_cells={} equity=${:.2}",
                    summary.universe_active_count,
                    summary.candidates_passed_gates,
                    summary.entries_opened,
                    summary.open_cells_after,
                    summary.pond_equity_usdc
                ),
                Some(serde_json::to_value(&summary).unwrap_or_else(|_| json!({}))),
            );
        }

        Ok(summary)
    }
}

/// Spawns the Book B (`POND`) background Tokio task inside a panic-isolated supervision boundary (`Invariant F6`, `R4-M1`, `R6-QA`).
///
/// NOTE: This async background task spawner serves as a Tokio supervision test fixture and standalone async alternative to the
/// synchronous daemon loop `run_scheduled_daemon_step` in `crates/aegis-cli/src/main.rs`. Cadence execution is single-sourced
/// through `PondOrchestrator::run_scheduled_step`, ensuring identical cadence semantics and config reloading across both paths.
pub fn spawn_pond_task(
    orchestrator: PondOrchestrator,
    tick_interval_sec: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Value> {
    tokio::spawn(async move {
        let mut ticks = 0u64;
        let mut last_scan_hour_bucket: Option<i64> = None;
        let mut last_4h_close_bucket: Option<i64> = None;
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            let orch = orchestrator.clone();
            let mut scan_bucket = last_scan_hour_bucket;
            let mut close_bucket = last_4h_close_bucket;

            let step_fut = AssertUnwindSafe(async move {
                tokio::task::spawn_blocking(move || {
                    let res = orch.run_scheduled_step(&mut scan_bucket, &mut close_bucket);
                    (res, scan_bucket, close_bucket)
                })
                .await
            })
            .catch_unwind();

            match step_fut.await {
                Ok(Ok((Ok(_summary_opt), scan_b, close_b))) => {
                    ticks += 1;
                    last_scan_hour_bucket = scan_b;
                    last_4h_close_bucket = close_b;
                }
                Ok(Ok((Err(e), _, _))) => {
                    tracing::error!("Book B (POND) step error: {e}");
                }
                Ok(Err(join_err)) => {
                    tracing::error!("Book B (POND) blocking task join error: {join_err}");
                }
                Err(_panic_payload) => {
                    tracing::error!(
                        "CRITICAL: Book B (POND) task caught a panic via catch_unwind; isolating fault"
                    );
                }
            }

            let sleep_sec = orchestrator
                .load_pond_config()
                .map(|c| c.pond_tick_interval_seconds)
                .unwrap_or_else(|_| tick_interval_sec.max(1))
                .max(1);

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(sleep_sec)) => {}
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
        json!({
            "book_id": "POND",
            "status": "STOPPED",
            "ticks_completed": ticks,
            "stopped_at": Utc::now().to_rfc3339(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cell::accrue_cell_hourly_funding;
    use aegis_core::{OhlcvBar, PondDirectionPolicy};

    fn make_test_bars(symbol: &str, tf: &str, count: usize, price: f64) -> Vec<OhlcvBar> {
        (0..count)
            .map(|i| OhlcvBar {
                symbol: symbol.to_string(),
                timeframe: tf.to_string(),
                timestamp_ms: 1700000000000 + (i as i64) * 3_600_000,
                open: price,
                high: price * 1.005,
                low: price * 0.995,
                close: price,
                volume: 10_000.0,
                funding_rate_hourly: 0.00001,
                is_synthetic: 1,
            })
            .collect()
    }

    #[test]
    fn test_pond_universe_80_coin_bootstrap_and_hysteresis() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PondOrchestrator::new(db.clone(), None);

        let rows = orch.initialize_or_refresh_universe().unwrap();
        assert_eq!(rows.len(), 80);
        assert!(rows.iter().all(|r| r.status == "ACTIVE"));
        assert!(rows.iter().all(|r| r.median_daily_volume_usdc >= 50_000.0));

        // Simulate rank dropping to 98 across 2 consecutive refreshes -> DELISTING/RETIRED
        let mut custom = rows.clone();
        custom[79].rank = 98;
        let refresh1 = orch
            .universe_mgr
            .refresh_universe(Some(custom.clone()))
            .unwrap();
        let last1 = refresh1.iter().find(|r| r.rank == 98).unwrap();
        assert_eq!(last1.delist_streak, 1);
        assert_eq!(last1.status, "ACTIVE");

        let refresh2 = orch.universe_mgr.refresh_universe(Some(custom)).unwrap();
        let last2 = refresh2.iter().find(|r| r.rank == 98).unwrap();
        assert_eq!(last2.delist_streak, 2);
        // No open cell on this symbol -> transitions directly to RETIRED
        assert_eq!(last2.status, "RETIRED");
    }

    #[test]
    fn test_pond_cell_fsm_and_all_6_exit_tiers() {
        let mut cell = PondCell {
            cell_id: 1,
            symbol: "SOL/USDC:USDC".to_string(),
            champion_id: 1,
            state: "DORMANT".to_string(),
            cycle_opened_id: Some(1),
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(100.0),
            quantity: Some(5.0),
            notional_usdc: Some(500.0),
            risk_budget_usdc: Some(50.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.8),
            funding_rate_entry: Some(0.00008),
            initial_stop: Some(105.0),
            chandelier_stop: Some(107.6),
            highest_price_seen: Some(100.5),
            lowest_price_seen: Some(99.5),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: None,
            closed_at: None,
            cooldown_until: None,
            updated_at: String::new(),
        };

        transition_cell_state(&mut cell, PondCellState::Screened).unwrap();
        transition_cell_state(&mut cell, PondCellState::Candidate).unwrap();
        transition_cell_state(&mut cell, PondCellState::Armed).unwrap();
        transition_cell_state(&mut cell, PondCellState::Open).unwrap();
        accrue_cell_hourly_funding(&mut cell, 100.0, 0.00008);
        assert!(cell.funding_accrued_usdc > 0.0);

        // Tier 1: Catastrophic Wick Stop (initial_stop 105.0 + 0.85 * 2.0 = 106.7)
        let inp = CellExitEvalInput {
            intrabar_high: 106.8,
            ..Default::default()
        };
        let d1 = evaluate_exit_priority(&cell, &inp);
        assert_eq!(d1.tier, Some(1));
        assert_eq!(d1.target_state, Some(PondCellState::Closed));
        assert_eq!(d1.cooldown_hours, 56);

        // Tier 2: 4H Bar-Close Chandelier Trail
        let inp2 = CellExitEvalInput {
            is_4h_bar_close: true,
            bar_4h_close: 107.7,
            ..Default::default()
        };
        let d2 = evaluate_exit_priority(&cell, &inp2);
        assert_eq!(d2.tier, Some(2));
        assert_eq!(d2.target_state, Some(PondCellState::Closed));
        assert_eq!(d2.cooldown_hours, 56);

        // Tier 3: Funding Normalisation (|z_t| <= 0.5 at 1H boundary)
        let inp3 = CellExitEvalInput {
            is_1h_boundary: true,
            current_funding_z: 0.35,
            ..Default::default()
        };
        let d3 = evaluate_exit_priority(&cell, &inp3);
        assert_eq!(d3.tier, Some(3));
        assert_eq!(d3.target_state, Some(PondCellState::Closed));
        assert_eq!(d3.cooldown_hours, 24);

        // Tier 4: Adverse Funding Flip (r < -0.000015 for SHORT_CALL at 1H boundary)
        let inp4 = CellExitEvalInput {
            is_1h_boundary: true,
            current_funding_z: 1.8,
            current_funding_hourly: -0.000025,
            ..Default::default()
        };
        let d4 = evaluate_exit_priority(&cell, &inp4);
        assert_eq!(d4.tier, Some(4));
        assert_eq!(d4.target_state, Some(PondCellState::Closed));
        assert_eq!(d4.cooldown_hours, 24);

        // Tier 5: Max Hold Timeout (72h)
        let inp5 = CellExitEvalInput {
            hold_hours: 72.0,
            ..Default::default()
        };
        let d5 = evaluate_exit_priority(&cell, &inp5);
        assert_eq!(d5.tier, Some(5));
        assert_eq!(d5.target_state, Some(PondCellState::Closed));

        // Tier 6: Universe Delisting
        let inp6 = CellExitEvalInput {
            universe_status: "DELISTING".to_string(),
            ..Default::default()
        };
        let d6 = evaluate_exit_priority(&cell, &inp6);
        assert_eq!(d6.tier, Some(6));
        assert_eq!(d6.target_state, Some(PondCellState::Closed));
    }

    #[test]
    fn test_nps01_bidirectional_scanner_and_direction_policy() {
        let mut f_pos = vec![0.000005; 167];
        f_pos.push(0.000095); // positive spike -> SHORT_CALL / SHORT

        let mut f_neg = vec![-0.000005; 167];
        f_neg.push(-0.000095); // negative spike -> SHORT_PUT / LONG

        let asset_short_call = PondAssetScanInput {
            universe_row: PondUniverseRow {
                symbol: "SOL/USDC:USDC".to_string(),
                rank: 3,
                median_daily_volume_usdc: 500_000.0,
                tick_size: 0.01,
                lot_step_size: 0.01,
                min_notional_usdc: 10.0,
                max_leverage: 3.0,
                reference_price: 180.0,
                status: "ACTIVE".to_string(),
                warmup_until: Some("2026-01-08T00:00:00Z".to_string()),
                delist_streak: 0,
                added_at: "2026-01-01T00:00:00Z".to_string(),
                delisted_at: None,
                is_synthetic_volume: 0,
            },
            funding_series_168h: f_pos,
            bars_1h: make_test_bars("SOL/USDC:USDC", "1h", 180, 180.0),
            bars_4h: make_test_bars("SOL/USDC:USDC", "4h", 50, 180.0),
            tick_size: 0.01,
            lot_step_size: 0.01,
        };

        let asset_short_put = PondAssetScanInput {
            universe_row: PondUniverseRow {
                symbol: "SUI/USDC:USDC".to_string(),
                rank: 7,
                median_daily_volume_usdc: 450_000.0,
                tick_size: 0.0001,
                lot_step_size: 0.1,
                min_notional_usdc: 10.0,
                max_leverage: 3.0,
                reference_price: 3.2,
                status: "ACTIVE".to_string(),
                warmup_until: Some("2026-01-08T00:00:00Z".to_string()),
                delist_streak: 0,
                added_at: "2026-01-01T00:00:00Z".to_string(),
                delisted_at: None,
                is_synthetic_volume: 0,
            },
            funding_series_168h: f_neg,
            bars_1h: make_test_bars("SUI/USDC:USDC", "1h", 180, 3.2),
            bars_4h: make_test_bars("SUI/USDC:USDC", "4h", 50, 3.2),
            tick_size: 0.0001,
            lot_step_size: 0.1,
        };

        let inputs = vec![asset_short_call, asset_short_put];
        let mut params = PondScannerParams {
            direction_policy: PondDirectionPolicy::Both,
            ..Default::default()
        };

        // 1. Policy `both` selects both SHORT_CALL and SHORT_PUT
        let out_both = run_nps01_scan(&inputs, &params, &HashSet::new());
        assert_eq!(out_both.selected_entries.len(), 2);

        // 2. Policy `short_only` selects only SHORT_CALL
        params.direction_policy = PondDirectionPolicy::ShortOnly;
        let out_short = run_nps01_scan(&inputs, &params, &HashSet::new());
        assert_eq!(out_short.selected_entries.len(), 1);
        assert_eq!(
            out_short.selected_entries[0].option_analogue.as_str(),
            "SHORT_CALL"
        );

        // 3. Policy `long_only` selects only SHORT_PUT
        params.direction_policy = PondDirectionPolicy::LongOnly;
        let out_long = run_nps01_scan(&inputs, &params, &HashSet::new());
        assert_eq!(out_long.selected_entries.len(), 1);
        assert_eq!(
            out_long.selected_entries[0].option_analogue.as_str(),
            "SHORT_PUT"
        );
    }

    #[test]
    fn test_pond_orchestrator_end_to_end_scan_cycle() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PondOrchestrator::new(db.clone(), None);
        let summary = orch.run_scan_cycle(true).unwrap();
        assert_eq!(summary.book_id, "POND");
        assert_eq!(summary.champion_id, "NPS-01");
        assert_eq!(summary.universe_active_count, 80);
        assert!(summary.entries_opened >= 1 && summary.entries_opened <= 3);
        assert!(summary.open_heat_usdc <= 200.0);
        assert!(summary.gross_leverage <= 1.5);
    }

    #[test]
    fn test_exit_tier4_on_15s_tick_and_tier1_on_trailed_chandelier() {
        let mut cell = PondCell {
            cell_id: 2,
            symbol: "ARB/USDC:USDC".to_string(),
            champion_id: 1,
            state: "OPEN".to_string(),
            cycle_opened_id: Some(1),
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(1.50),
            quantity: Some(100.0),
            notional_usdc: Some(150.0),
            risk_budget_usdc: Some(25.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.4),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(1.75),
            chandelier_stop: Some(1.75),
            highest_price_seen: Some(1.50),
            lowest_price_seen: Some(1.50),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: Some("2026-01-01T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };

        // 1. Tier 4 Adverse Funding Flip MUST trigger even on a 15s tick (`is_1h_boundary = false`)
        let dec_t4 = evaluate_exit_priority(
            &cell,
            &CellExitEvalInput {
                mark_price: 1.48,
                intrabar_high: 1.49,
                intrabar_low: 1.47,
                atr_14_1h: 0.04,
                bar_4h_close: 1.48,
                atr_14_4h: 0.04,
                current_funding_z: -0.5,
                current_funding_hourly: -0.00003,
                hold_hours: 4.0,
                universe_status: "ACTIVE".to_string(),
                is_1h_boundary: false,
                ..Default::default()
            },
        );
        assert!(dec_t4.should_exit);
        assert_eq!(dec_t4.tier, Some(4));
        assert_eq!(
            dec_t4.exit_reason.as_deref(),
            Some("TIER4_ADVERSE_FUNDING_FLIP")
        );

        // 2. Ratchet Chandelier stop into TRAILED state, then test Tier 1 wick stop on 15s tick
        update_cell_mark_and_chandelier(&mut cell, 1.25, 1.28, 1.20, 0.04, 2.8, true);
        assert_eq!(cell.state, "TRAILED");
        let active_stop = cell.chandelier_stop.unwrap();
        assert!((active_stop - 1.312).abs() < 1e-6);

        let dec_t1 = evaluate_exit_priority(
            &cell,
            &CellExitEvalInput {
                mark_price: 1.35,
                intrabar_high: 1.36,
                intrabar_low: 1.24,
                atr_14_1h: 0.04,
                bar_4h_close: 1.25,
                atr_14_4h: 0.04,
                current_funding_z: 1.1,
                current_funding_hourly: 0.0002,
                hold_hours: 5.2,
                universe_status: "ACTIVE".to_string(),
                is_1h_boundary: false,
                ..Default::default()
            },
        );
        assert!(dec_t1.should_exit);
        assert_eq!(dec_t1.tier, Some(1));
        assert_eq!(
            dec_t1.exit_reason.as_deref(),
            Some("TIER1_CATASTROPHIC_WICK_STOP")
        );
    }

    #[test]
    fn test_gate5_168h_warmup_and_gate6_ema64_overextension_veto() {
        let mut f_pos = vec![0.00008; 167];
        for (i, val) in f_pos.iter_mut().enumerate().take(167) {
            *val += ((i % 7) as f64 - 3.0) * 0.00001;
        }
        f_pos.push(0.00045);

        let base_row = PondUniverseRow {
            symbol: "DOGE/USDC:USDC".to_string(),
            rank: 4,
            median_daily_volume_usdc: 800_000.0,
            tick_size: 0.0001,
            lot_step_size: 1.0,
            min_notional_usdc: 10.0,
            max_leverage: 3.0,
            reference_price: 0.25,
            status: "ACTIVE".to_string(),
            warmup_until: Some("2025-01-01T00:00:00Z".to_string()),
            delist_streak: 0,
            added_at: "2025-01-01T00:00:00Z".to_string(),
            delisted_at: None,
            is_synthetic_volume: 0,
        };

        // 1. Only 120 hours of 1H bars (< 168h requirement) -> Vetoed by Gate 5
        let short_history_input = PondAssetScanInput {
            universe_row: base_row.clone(),
            funding_series_168h: f_pos.clone(),
            bars_1h: make_test_bars("DOGE/USDC:USDC", "1h", 120, 0.25),
            bars_4h: make_test_bars("DOGE/USDC:USDC", "4h", 50, 0.25),
            tick_size: 0.0001,
            lot_step_size: 1.0,
        };
        let params = PondScannerParams::default();
        let out_short_hist = run_nps01_scan(&[short_history_input], &params, &HashSet::new());
        assert_eq!(out_short_hist.selected_entries.len(), 0);
        assert!(out_short_hist
            .rejections_by_gate
            .iter()
            .any(|(_, reason)| reason == "GATE5_INSUFFICIENT_168H_WARMUP"));

        // 2. 180 hours of 1H bars, but last 4H bar close is +28% above 4H EMA_64 (> +8% overextension limit)
        let mut bars_4h = make_test_bars("DOGE/USDC:USDC", "4h", 64, 0.25);
        if let Some(last) = bars_4h.last_mut() {
            last.close = 0.32;
            last.high = 0.33;
        }
        let overextended_input = PondAssetScanInput {
            universe_row: base_row,
            funding_series_168h: f_pos,
            bars_1h: make_test_bars("DOGE/USDC:USDC", "1h", 180, 0.25),
            bars_4h,
            tick_size: 0.0001,
            lot_step_size: 1.0,
        };
        let out_overext = run_nps01_scan(&[overextended_input], &params, &HashSet::new());
        assert_eq!(out_overext.selected_entries.len(), 0);
        assert!(out_overext
            .rejections_by_gate
            .iter()
            .any(|(_, reason)| reason == "GATE6_4H_STRUCTURAL_UPTREND_VETO"));
    }

    #[test]
    fn test_iso8601_warmup_timestamp_and_multiweek_delisting_hysteresis_with_open_cell() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PondOrchestrator::new(db, None);

        assert!(parse_utc_timestamp("2099-01-01T00:00:00Z").is_some());
        assert!(parse_utc_timestamp("2020-01-01T00:00:00Z").is_some());
        assert!(parse_utc_timestamp("2026-01-01 00:00:00").is_some());

        let mut rows = orch.initialize_or_refresh_universe().unwrap();
        let target_sym = rows[79].symbol.clone();

        let mut open_cell = PondCell {
            cell_id: 0,
            symbol: target_sym.clone(),
            champion_id: 1,
            state: "OPEN".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(10.0),
            quantity: Some(10.0),
            notional_usdc: Some(100.0),
            risk_budget_usdc: Some(25.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.4),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(12.5),
            chandelier_stop: Some(12.5),
            highest_price_seen: Some(10.0),
            lowest_price_seen: Some(10.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: Some("2026-01-01T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        orch.scoped_db.upsert_pond_cell(&open_cell).unwrap();
        let stored_cells = orch.scoped_db.get_open_pond_cells().unwrap();
        open_cell.cell_id = stored_cells[0].cell_id;

        rows[79].rank = 102;
        let w1 = orch
            .universe_mgr
            .refresh_universe(Some(rows.clone()))
            .unwrap();
        let r_w1 = w1.iter().find(|r| r.symbol == target_sym).unwrap();
        assert_eq!(r_w1.delist_streak, 1);
        assert_eq!(r_w1.status, "ACTIVE");

        let w2 = orch
            .universe_mgr
            .refresh_universe(Some(rows.clone()))
            .unwrap();
        let r_w2 = w2.iter().find(|r| r.symbol == target_sym).unwrap();
        assert_eq!(r_w2.delist_streak, 2);
        assert_eq!(r_w2.status, "DELISTING");

        open_cell.state = "CLOSED".to_string();
        open_cell.exit_reason = Some("TIER6_UNIVERSE_DELISTING_EXIT".to_string());
        orch.scoped_db.upsert_pond_cell(&open_cell).unwrap();

        let w3 = orch.universe_mgr.refresh_universe(Some(rows)).unwrap();
        let r_w3 = w3.iter().find(|r| r.symbol == target_sym).unwrap();
        assert_eq!(r_w3.status, "RETIRED");
        assert!(r_w3.delisted_at.is_some());
    }

    #[test]
    fn test_b8_armed_resting_cell_and_c1_c3_pond_mtm_equity() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PondOrchestrator::new(db.clone(), None);

        let armed_cell = PondCell {
            cell_id: 0,
            symbol: "SOL/USDC:USDC".to_string(),
            champion_id: 1,
            state: "ARMED".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(100.0),
            quantity: Some(5.0),
            notional_usdc: Some(500.0),
            risk_budget_usdc: Some(50.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.4),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(110.0),
            chandelier_stop: Some(110.0),
            highest_price_seen: Some(100.0),
            lowest_price_seen: Some(100.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: Some("HL-RESTING-123".to_string()),
            opened_at: None,
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        orch.scoped_db.upsert_pond_cell(&armed_cell).unwrap();
        assert_eq!(orch.scoped_db.get_open_pond_cells().unwrap().len(), 0);
        assert_eq!(orch.scoped_db.get_armed_pond_cells().unwrap().len(), 1);

        let promoted = orch.reconcile_armed_cells(false).unwrap();
        assert_eq!(promoted, 0);
        assert_eq!(orch.scoped_db.get_open_pond_cells().unwrap().len(), 0);
        assert_eq!(orch.scoped_db.get_armed_pond_cells().unwrap().len(), 1);

        let _ = orch.reconcile_armed_cells(true).unwrap();
        assert_eq!(orch.scoped_db.get_armed_pond_cells().unwrap().len(), 0);
        assert_eq!(orch.scoped_db.get_open_pond_cells().unwrap().len(), 0);
        let all_cells = orch.scoped_db.get_pond_cells(10).unwrap();
        assert_eq!(all_cells[0].state, "REJECTED");
        assert_eq!(all_cells[0].funding_accrued_usdc, 0.0);
        assert!(all_cells[0].resting_order_id.is_none());

        let mut cell = armed_cell;
        cell.state = "OPEN".to_string();
        cell.opened_at = Some("2026-01-01T00:00:00Z".to_string());
        cell.exit_reason = None;
        cell.resting_order_id = None;
        cell.funding_accrued_usdc = 12.0;
        cell.fees_paid_usdc = 2.0;
        orch.scoped_db.upsert_pond_cell(&cell).unwrap();

        db.upsert_ohlcv_batch(&[aegis_core::OhlcvBar {
            symbol: "SOL/USDC:USDC".to_string(),
            timeframe: "1h".to_string(),
            timestamp_ms: 1_767_225_600_000,
            open: 95.0,
            high: 96.0,
            low: 89.0,
            close: 90.0,
            volume: 100_000.0,
            funding_rate_hourly: 0.0005,
            is_synthetic: 0,
        }])
        .unwrap();

        db.insert_book_transfer(&aegis_core::BookTransfer {
            transfer_id: 0,
            settle_date: "2026-01-01".to_string(),
            from_book: "PIT".to_string(),
            to_book: "POND".to_string(),
            amount_usdc: 200.0,
            pre_from_usdc: Some(5200.0),
            pre_to_usdc: Some(4800.0),
            post_from_usdc: Some(5000.0),
            post_to_usdc: Some(5000.0),
            drift_bps_before: Some(200.0),
            drift_bps_after: Some(0.0),
            blocked_remainder_usdc: 0.0,
            status: "COMPLETED".to_string(),
            attempts: 1,
            tx_ref: Some("VIRTUAL_SINGLE_WALLET_LEDGER".to_string()),
            created_at: "2026-01-01T00:05:00Z".to_string(),
        })
        .unwrap();

        let (pond_eq, _idle) = orch.compute_pond_equity_and_idle_usdc().unwrap();
        assert!((pond_eq - 5260.0).abs() < 1e-6);
    }

    #[test]
    fn test_live_synthetic_volume_rejected_in_gate1_nm13() {
        let mut f_pos = vec![0.000005; 167];
        f_pos.push(0.000095);

        let synth_input = PondAssetScanInput {
            universe_row: PondUniverseRow {
                symbol: "SOL/USDC:USDC".to_string(),
                rank: 3,
                median_daily_volume_usdc: 500_000.0,
                tick_size: 0.01,
                lot_step_size: 0.01,
                min_notional_usdc: 10.0,
                max_leverage: 3.0,
                reference_price: 180.0,
                status: "ACTIVE".to_string(),
                warmup_until: Some("2026-01-08T00:00:00Z".to_string()),
                delist_streak: 0,
                added_at: "2026-01-01T00:00:00Z".to_string(),
                delisted_at: None,
                is_synthetic_volume: 1,
            },
            funding_series_168h: f_pos,
            bars_1h: make_test_bars("SOL/USDC:USDC", "1h", 180, 180.0),
            bars_4h: make_test_bars("SOL/USDC:USDC", "4h", 50, 180.0),
            tick_size: 0.01,
            lot_step_size: 0.01,
        };

        let live_params = PondScannerParams {
            reject_synthetic_volume: true,
            ..Default::default()
        };
        let out = run_nps01_scan(&[synth_input], &live_params, &HashSet::new());
        assert_eq!(out.selected_entries.len(), 0);
        assert!(out
            .rejections_by_gate
            .iter()
            .any(|(_, r)| r == "GATE1_SYNTHETIC_VOLUME_REJECTED_IN_LIVE"));
    }

    #[test]
    fn test_nm7_nm8_nm10_nm14_nm15_pond_boundary_staleness_reconcile_fsm_and_risk_scope() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PondOrchestrator::new(db.clone(), None);

        // 1. NM-14: Expired COOLDOWN cell transitions to DORMANT and is persisted in SQLite during run_scan_cycle
        let cd_cell = PondCell {
            cell_id: 0,
            symbol: "SOL/USDC:USDC".to_string(),
            champion_id: 1,
            state: "COOLDOWN".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(150.0),
            quantity: Some(2.0),
            notional_usdc: Some(300.0),
            risk_budget_usdc: Some(30.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.2),
            funding_rate_entry: Some(0.0005),
            initial_stop: Some(165.0),
            chandelier_stop: Some(165.0),
            highest_price_seen: Some(150.0),
            lowest_price_seen: Some(150.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 5.0,
            fees_paid_usdc: 0.2,
            exit_price: Some(147.0),
            exit_reason: Some("FUNDING_DECAY".to_string()),
            resting_order_id: None,
            opened_at: Some("2026-01-01T00:00:00Z".to_string()),
            closed_at: Some("2026-01-01T04:00:00Z".to_string()),
            cooldown_until: Some("2026-01-01T08:00:00Z".to_string()),
            updated_at: "2026-01-01T04:00:00Z".to_string(),
        };
        let cd_id = orch.scoped_db.upsert_pond_cell(&cd_cell).unwrap();
        let _ = orch.run_scan_cycle(false).unwrap();
        let cells_after = orch.scoped_db.get_pond_cells(50).unwrap();
        let updated_cd = cells_after.iter().find(|c| c.cell_id == cd_id).unwrap();
        assert_eq!(updated_cd.state, "DORMANT");

        // 2. NM-10: Exchange reconciliation detects direction flip on an open Pond cell
        let open_cell = PondCell {
            cell_id: 0,
            symbol: "ETH/USDC:USDC".to_string(),
            champion_id: 1,
            state: "OPEN".to_string(),
            cycle_opened_id: None,
            side: "SHORT".to_string(),
            option_analogue: "SHORT_CALL".to_string(),
            entry_price: Some(3000.0),
            quantity: Some(1.0),
            notional_usdc: Some(3000.0),
            risk_budget_usdc: Some(50.0),
            conviction_multiplier: Some(1.0),
            funding_z_entry: Some(2.5),
            funding_rate_entry: Some(0.0004),
            initial_stop: Some(3150.0),
            chandelier_stop: Some(3150.0),
            highest_price_seen: Some(3000.0),
            lowest_price_seen: Some(3000.0),
            funding_accrued_usdc: 0.0,
            realized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            exit_price: None,
            exit_reason: None,
            resting_order_id: None,
            opened_at: Some("2026-01-02T00:00:00Z".to_string()),
            closed_at: None,
            cooldown_until: None,
            updated_at: "2026-01-02T00:00:00Z".to_string(),
        };
        let open_id = orch.scoped_db.upsert_pond_cell(&open_cell).unwrap();
        let mut sim_pos = HashMap::new();
        // Positive szi (+1.5) means LONG on exchange, whereas local cell is SHORT!
        sim_pos.insert(
            "ETH".to_string(),
            json!({"szi": "1.5", "entryPx": "3010.0", "markPx": "3020.0", "unrealizedPnl": "15.0"}),
        );
        let rec = orch
            .execution
            .reconcile_exchange_state(Some(&sim_pos), Some(&[]));
        assert!(rec
            .discrepancies
            .iter()
            .any(|d| d["type"] == "DIRECTION_FLIP_POND_CELL_RECONCILED" && d["cell_id"] == open_id));
    }
}
