//! Book A (`PIT`) Orchestrator Crate (`aegis-pit`).
//!
//! Wraps the locked `v0.2.2` MACE (35%) + Continuous SNR (65%) + 3-State Gaussian HMM
//! signal engine (`aegis-signals`), Chandelier trailing stops (`aegis-risk`), and
//! `MAKER_LIMIT_CHASER` + HLP Basis Harvest execution (`aegis-execution`) inside a
//! strictly scoped Book A database wrapper (`BookScopedDb(BookType::Pit)`) and
//! panic-isolated async runtime (`AssertUnwindSafe(...).catch_unwind()`).

use aegis_core::{round_dp, BookType, PortfolioSnapshot, Result};
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

    /// Returns true if the platform-wide emergency halt or daily 00:05 UTC settling gate is active.
    pub fn is_entry_frozen(&self) -> Result<(bool, String)> {
        let cfg = self.load_pit_config()?;
        if cfg.platform_pp50_halt {
            return Ok((true, "PLATFORM_PP50_HALT".to_string()));
        }
        if cfg.platform_settle_gate_enabled {
            return Ok((true, "SETTLING_IN_PROGRESS".to_string()));
        }
        Ok((false, "ACTIVE".to_string()))
    }

    /// Executes a single Book A (`PIT`) 4H/1H signal & execution reconciliation cycle.
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
        let snap: PortfolioSnapshot = if frozen {
            self.scoped_db.get_pit_latest_portfolio_snapshot()?
        } else {
            self.execution.reconcile_cycle(&evals, false)?
        };

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

    /// Computes Book A (`PIT`) equity and unlocked idle USDC for the 00:05 UTC settling protocol (`C1`).
    pub fn compute_pit_equity_and_idle_usdc(&self) -> Result<(f64, f64)> {
        let cfg = self.load_pit_config()?;
        let base_cap = cfg.book_baseline_equity_usdc.max(1.0);
        let net_transfers = self.scoped_db.get_net_book_transfer_adjustment().unwrap_or(0.0);
        let raw_db = self.scoped_db.inner();
        let realized = raw_db.get_total_realized_pnl().unwrap_or(0.0);
        let open_trades = self.scoped_db.get_pit_open_trades().unwrap_or_default();
        let mut deployed = 0.0;
        let mut upnl = 0.0;
        for t in &open_trades {
            deployed += t.quantity.abs() * t.current_price;
            upnl += t.unrealized_pnl_usdc + t.funding_accrued_usdc;
        }
        let latest_snap = self.scoped_db.get_pit_latest_portfolio_snapshot()?;
        let has_baseline_reset = !raw_db
            .load_config()
            .unwrap_or_default()
            .get_str("equity_baseline_reset_at", "")
            .trim()
            .is_empty();
        let yield_usdc = if has_baseline_reset {
            0.0
        } else {
            latest_snap.accrued_yield_usdc
        };
        let equity = round_dp(base_cap + net_transfers + realized + upnl + yield_usdc, 2);
        let mut idle = round_dp((equity - deployed).max(0.0), 2);
        if let Ok((_, perps_trading_eq, vault_eq, total_wallet_deployed)) =
            self.scoped_db.get_wallet_perps_margin_state()
        {
            if vault_eq > 0.0 || total_wallet_deployed > deployed {
                let avail_perps_margin =
                    round_dp((perps_trading_eq - total_wallet_deployed).max(0.0), 2);
                idle = idle.min(avail_perps_margin);
            }
        }
        Ok((equity, idle))
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

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(interval_sec.max(1))) => {}
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

    #[test]
    fn test_pit_orchestrator_scoped_cycle() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = PitOrchestrator::new(db.clone(), None);
        let cfg = orch.load_pit_config().unwrap();
        assert!(!cfg.platform_pp50_halt);

        let summary = orch.run_cycle(true, true).unwrap();
        assert_eq!(summary.book_id, "PIT");
        assert!(summary.total_equity_usdc > 0.0);
        assert_eq!(summary.evaluations_count, 15);
    }
}
