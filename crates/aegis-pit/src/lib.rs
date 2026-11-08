//! Book A (`PIT`) Orchestrator Crate (`aegis-pit`).
//!
//! Wraps the locked `v0.2.2` MACE (35%) + Continuous SNR (65%) + 3-State Gaussian HMM
//! signal engine (`aegis-signals`), Chandelier trailing stops (`aegis-risk`), and
//! `MAKER_LIMIT_CHASER` + HLP Basis Harvest execution (`aegis-execution`) inside a
//! strictly scoped Book A database wrapper (`BookScopedDb(BookType::Pit)`) and
//! panic-isolated async runtime (`AssertUnwindSafe(...).catch_unwind()`).

use aegis_core::{BookType, PortfolioSnapshot, Result};
use aegis_db::{BookScopedDb, DatabaseManager, PitConfig};
use aegis_execution::{ExecutionEngine, MarketIngestionEngine};
use aegis_signals::SignalEngine;
use aegis_telemetry::TelemetryBus;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::sync::watch;

/// Summary of a single Book A (`PIT`) evaluation & reconciliation cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PitCycleSummary {
    pub book_id: String,
    pub status: String,
    pub timestamp_ms: i64,
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub deployed_notional_usdc: f64,
    pub portfolio_heat_usdc: f64,
    pub gross_leverage: f64,
    pub hmm_regime: String,
    pub open_long_count: i64,
    pub open_short_count: i64,
    pub evaluations_count: usize,
}

/// Book A (`PIT`) orchestrator.
#[derive(Clone)]
pub struct PitOrchestrator {
    pub scoped_db: BookScopedDb,
    pub ingestion: MarketIngestionEngine,
    pub signals: SignalEngine,
    pub execution: ExecutionEngine,
    pub telemetry: Option<TelemetryBus>,
}

impl PitOrchestrator {
    pub fn new(db: DatabaseManager, telemetry: Option<TelemetryBus>) -> Self {
        let scoped_db = BookScopedDb::new(db.clone(), BookType::Pit);
        let ingestion = MarketIngestionEngine::new(db.clone());
        let signals = SignalEngine::new(db.clone());
        let execution = ExecutionEngine::new(db);
        Self {
            scoped_db,
            ingestion,
            signals,
            execution,
            telemetry,
        }
    }

    /// Loads the Book A (`PitConfig`) namespace view.
    pub fn load_pit_config(&self) -> Result<PitConfig> {
        self.scoped_db.load_pit_config()
    }

    /// Returns true if the platform-wide emergency halt or daily 00:05 UTC settling gate is active (`NH-10`, `NM-3`).
    pub fn is_entry_frozen(&self) -> Result<(bool, String)> {
        let cfg = self.load_pit_config()?;
        if cfg.platform_pp50_halt {
            return Ok((true, "PLATFORM_PP50_HALT".to_string()));
        }
        if cfg.platform_settle_gate_enabled || self.scoped_db.inner().is_entry_frozen() {
            return Ok((true, "SETTLING_IN_PROGRESS".to_string()));
        }
        Ok((false, "ACTIVE".to_string()))
    }

    /// Executes a single Book A (`PIT`) 4H/1H signal & execution reconciliation cycle.
    ///
    /// Even when `frozen` is true (`PLATFORM_PP50_HALT` or `SETTLING_IN_PROGRESS`), protective
    /// Chandelier trailing stops and regime exits continue to execute (`NH-10`), while new
    /// directional entries are suppressed.
    pub fn run_cycle(&self, ingest_first: bool, force_synthetic: bool) -> Result<PitCycleSummary> {
        self.scoped_db.check_table_access("trades")?;
        self.scoped_db.check_table_access("portfolio_snapshots")?;
        self.scoped_db.check_table_access("books")?;

        let cfg = self.load_pit_config()?;
        let (frozen, reason) = self.is_entry_frozen()?;

        if ingest_first {
            let _ = self
                .ingestion
                .ingest_universe(cfg.ingest_lookback_bars.max(60), force_synthetic)?;
        }

        let evals = self.signals.evaluate_universe(true)?;
        let snap: PortfolioSnapshot = self
            .execution
            .reconcile_cycle_with_options(&evals, false, frozen)?;

        let summary = PitCycleSummary {
            book_id: "PIT".to_string(),
            status: reason,
            timestamp_ms: snap.timestamp_ms,
            total_equity_usdc: snap.total_equity_usdc,
            available_cash_usdc: snap.available_cash_usdc,
            deployed_notional_usdc: snap.deployed_notional_usdc,
            portfolio_heat_usdc: snap.portfolio_heat_usdc,
            gross_leverage: snap.gross_leverage,
            hmm_regime: snap.hmm_regime.clone(),
            open_long_count: snap.open_long_count,
            open_short_count: snap.open_short_count,
            evaluations_count: evals.len(),
        };

        if let Some(bus) = &self.telemetry {
            bus.publish_event(
                "INFO",
                "aegis-pit.cycle",
                &format!(
                    "Book A (PIT) cycle complete: regime={} equity=${:.2}",
                    summary.hmm_regime, summary.total_equity_usdc
                ),
                Some(serde_json::to_value(&summary).unwrap_or_else(|_| json!({}))),
            );
        }

        Ok(summary)
    }

    /// Computes Book A (`PIT`) equity and unlocked idle USDC for the 00:05 UTC settling protocol (`C1`, `R3-H18`, `R3-QC`).
    pub fn compute_pit_equity_and_idle_usdc(&self) -> Result<(f64, f64)> {
        self.scoped_db.compute_pit_equity_and_idle_usdc()
    }
}

/// Spawns the Book A (`PIT`) background loop inside a panic-isolated supervision boundary (Invariant F6).
pub fn spawn_pit_task(
    orchestrator: PitOrchestrator,
    interval_sec: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Value> {
    tokio::spawn(async move {
        let mut ticks = 0u64;
        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            let orch = orchestrator.clone();
            let cycle_fut = AssertUnwindSafe(async move {
                tokio::task::spawn_blocking(move || orch.run_cycle(ticks == 0, false)).await
            })
            .catch_unwind();

            match cycle_fut.await {
                Ok(Ok(Ok(_summary))) => {
                    ticks += 1;
                }
                Ok(Ok(Err(e))) => {
                    tracing::error!("Book A (PIT) cycle error: {e}");
                }
                Ok(Err(join_err)) => {
                    tracing::error!("Book A (PIT) blocking task join error: {join_err}");
                }
                Err(_panic_payload) => {
                    tracing::error!(
                        "CRITICAL: Book A (PIT) task caught a panic via catch_unwind; isolating fault"
                    );
                }
            }

            let sleep_sec = if interval_sec == 0 || interval_sec >= 60 {
                orchestrator
                    .load_pit_config()
                    .map(|c| c.pit_tick_interval_seconds)
                    .unwrap_or_else(|_| interval_sec.max(1))
                    .max(1)
            } else {
                interval_sec.max(1)
            };

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
            "book_id": "PIT",
            "status": "STOPPED",
            "ticks_completed": ticks,
            "stopped_at_ms": orchestrator.scoped_db.now_ms(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_core::TradeRecord;

    #[test]
    fn test_pit_orchestrator_scoped_cycle() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PitOrchestrator::new(db.clone(), None);
        let cfg = orch.load_pit_config().unwrap();
        assert!(!cfg.platform_pp50_halt);
        assert_eq!(cfg.pit_tick_interval_seconds, 900);

        let summary = orch.run_cycle(true, true).unwrap();
        assert_eq!(summary.book_id, "PIT");
        assert!(summary.total_equity_usdc > 0.0);
        assert_eq!(summary.evaluations_count, 15);
    }

    #[test]
    fn test_pit_frozen_cycle_still_evaluates_chandelier_stops_nh10() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PitOrchestrator::new(db.clone(), None);

        // Seed synthetic bars first so SOL/USDC:USDC has valid OHLCV bars
        let _ = orch.ingestion.ingest_universe(60, true).unwrap();
        let bars = db
            .get_ohlcv_bars("SOL/USDC:USDC", "4h", 5, None)
            .unwrap();
        let last_bar = bars.last().unwrap();

        // Insert an open LONG trade whose Chandelier stop is above current bar low so it must trigger a stop exit
        let stop_px = last_bar.high + 50.0;
        let entry_px = stop_px + 20.0;
        let trade = TradeRecord {
            trade_id: 0,
            client_order_id: "nh10-stop-test".to_string(),
            symbol: "SOL/USDC:USDC".to_string(),
            base_asset: "SOL".to_string(),
            side: "LONG".to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: entry_px,
            current_price: entry_px,
            exit_price: None,
            quantity: 1.0,
            filled_units: 1.0,
            avg_fill_price: Some(entry_px),
            notional_usdc: entry_px,
            risk_budget_usdc: 20.0,
            conviction_multiplier: 1.0,
            initial_stop_price: stop_px,
            chandelier_stop_price: stop_px,
            current_stop_price: stop_px,
            highest_price_seen: entry_px,
            lowest_price_seen: entry_px,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.1,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("paper-nh10".to_string()),
            opened_at: String::new(),
            closed_at: None,
            exit_reason: None,
        };
        db.insert_trade(&trade).unwrap();
        assert_eq!(db.get_open_trades().unwrap().len(), 1);

        // Freeze entries via both platform_settle_gate_enabled and DB settle lock
        db.set_config_value("platform_settle_gate_enabled", &json!(true))
            .unwrap();
        db.try_acquire_settle_lock("test").unwrap();

        let summary = orch.run_cycle(false, false).unwrap();
        assert_eq!(summary.status, "SETTLING_IN_PROGRESS");
        // The stop-out MUST have executed despite the entry freeze, and no new entries opened
        assert_eq!(db.get_open_trades().unwrap().len(), 0);
    }

    #[test]
    fn test_nh4_nh6_resting_order_confirmation_and_pit_pending_state() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PitOrchestrator::new(db.clone(), None);

        // Insert a resting order in OPEN_ON_BOOK / PENDING_RESTING state (NH-4, NH-6)
        let resting = TradeRecord {
            trade_id: 0,
            client_order_id: "nh4-resting-coid".to_string(),
            symbol: "SOL/USDC:USDC".to_string(),
            base_asset: "SOL".to_string(),
            side: "LONG".to_string(),
            status: "PENDING".to_string(),
            fsm_state: "OPEN_ON_BOOK".to_string(),
            entry_price: 180.0,
            current_price: 180.0,
            exit_price: None,
            quantity: 2.0,
            filled_units: 0.0,
            avg_fill_price: None,
            notional_usdc: 360.0,
            risk_budget_usdc: 20.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 170.0,
            chandelier_stop_price: 170.0,
            current_stop_price: 170.0,
            highest_price_seen: 180.0,
            lowest_price_seen: 180.0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("hl-resting-999".to_string()),
            opened_at: String::new(),
            closed_at: None,
            exit_reason: None,
        };
        db.insert_trade(&resting).unwrap();

        // Unfilled resting order must NOT appear in get_open_trades() (NH-4)
        assert_eq!(db.get_open_trades().unwrap().len(), 0);
        assert_eq!(db.get_resting_pit_trades().unwrap().len(), 1);
        // Resting notional must still reserve idle margin in compute_pit_equity_and_idle_usdc
        let (pit_eq, pit_idle) = orch.compute_pit_equity_and_idle_usdc().unwrap();
        assert!((pit_eq - 5000.0).abs() < 1e-6);
        assert!((pit_idle - 4640.0).abs() < 1e-6);
    }
}
