//! Unified CLI & Daemon Orchestrator (`aegis-quant`) for Aegis Quant (`v0.3.0-rc1` PP-50 Two-Book Platform).
//!
//! 1:1 port of `engine/main.py`, `engine/database.py` CLI, `engine/backtest.py` CLI,
//! `engine/alerts.py` CLI, and `scripts/check_mainnet_readiness.py`, extended with
//! Book A (`PIT`), Book B (`POND`), and Daily 00:05 UTC Settling (`SETTLE`).

use aegis_alerts::AlertEngine;
use aegis_backtest::BacktestEngine;
use aegis_core::{
    resolve_project_root, round_dp, validate_evm_address, validate_private_key,
    verify_data_provenance, AegisError, CheckResult, PortfolioSnapshot, Result,
    PLATFORM_VERSION, SENSITIVE_CONFIG_KEYS,
};
use aegis_db::{
    fernet::{
        default_host_fernet_key_b64, generate_fernet_key, is_encrypted_value,
        is_legacy_public_fallback_key, master_key_file_for_db, resolve_fernet_master_key,
    },
    DatabaseManager,
};
use aegis_execution::{ExecutionEngine, MarketIngestionEngine};
use aegis_pit::PitOrchestrator;
use aegis_pond::PondOrchestrator;
use aegis_risk::RiskEngine;
use aegis_settle::{SettleController, SettleOverrideInput};
use aegis_signals::{SignalEngine, WalkForwardCalibrator};
use aegis_telemetry::TelemetryBus;
use aegis_web::serve_web;
use clap::{Parser, Subcommand};
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(
    name = "aegis-quant",
    version = PLATFORM_VERSION,
    about = "Aegis Quant PP-50 Two-Book Crypto Quantitative Trading & Portfolio Management Platform (Hyperliquid L1)"
)]
struct Cli {
    /// Path to SQLite WAL database (`data/aegis.db`; accepts `--db` or `--db-path`)
    #[arg(long = "db", visible_alias = "db-path", global = true)]
    db: Option<PathBuf>,

    /// Which book(s) to run (`pit`, `pond`, or `both`)
    #[arg(long, global = true, default_value = "both")]
    book: String,

    /// Emit machine-readable JSON output
    #[arg(long, global = true)]
    json: bool,

    /// Run a single ingestion + signal + execution cycle and exit
    #[arg(long, global = true)]
    once: bool,

    /// Run walk-forward timeframe calibration (`1H` vs `4H`) before cycle
    #[arg(long, global = true)]
    calibrate: bool,

    /// WebUI bind host (defaults to loopback `127.0.0.1` for security)
    #[arg(long, global = true, default_value = "127.0.0.1")]
    host: String,

    /// Explicitly allow binding the WebUI server to a non-loopback network interface
    #[arg(long, global = true)]
    allow_remote_bind: bool,

    /// WebUI bind port
    #[arg(long, global = true, default_value_t = 8000)]
    port: u16,

    /// Disable embedded Axum WebUI server
    #[arg(long, global = true)]
    no_web: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the AegisOrchestrator daemon & WebUI server (or run `--once`)
    Run,
    /// Book B (The Pond) universe management and hourly S-Cycle scanner
    Pond {
        /// Run an hourly S-Cycle scan across the 80-coin Pond universe
        #[arg(long)]
        scan: bool,
        /// Refresh the 80-coin dynamic Pond universe
        #[arg(long)]
        refresh_universe: bool,
    },
    /// Daily 00:05 UTC Two-Book 50/50 Settling Protocol
    Settle {
        /// Run settling in dry-run simulation mode
        #[arg(long)]
        dry_run: bool,
        /// Optional `<PIT_USDC>:<POND_USDC>` equity override (e.g. `5000:5000`, requires `--dry-run`)
        #[arg(long)]
        initial_split: Option<String>,
        /// Optional unlocked idle USDC override on the rich book (requires `--dry-run`)
        #[arg(long)]
        rich_idle_usdc: Option<f64>,
        /// Day index for the formatted daily report line
        #[arg(long, default_value_t = 1)]
        day_index: u32,
    },
    /// Database initialization, migrations, Fernet AES-128 verification, and key rotation
    Db {
        /// Initialize SQLite WAL schema and seed 31-instrument universe
        #[arg(long)]
        init: bool,
        /// Force drop and recreate schema tables during `--init`
        #[arg(long)]
        force: bool,
        /// Apply pending versioned SQL migrations (`sql/migrations/001..010_*.sql`)
        #[arg(long)]
        migrate: bool,
        /// Verify Fernet AES-128-CBC + HMAC-SHA256 encryption at rest
        #[arg(long)]
        test_encryption: bool,
        /// Atomically rotate the Fernet master key across all encrypted `system_config` keys
        /// (reads prior key from `--old-key-file` or `AEGIS_OLD_MASTER_KEY` env, and new key from `--new-key-file` or `AEGIS_NEW_MASTER_KEY` env)
        #[arg(long)]
        rotate_key: bool,
        /// Path to file containing the prior Fernet master key for `--rotate-key`
        #[arg(long)]
        old_key_file: Option<PathBuf>,
        /// Path to file containing the new Fernet master key for `--rotate-key`
        #[arg(long)]
        new_key_file: Option<PathBuf>,
        /// Generate a fresh 32-byte URL-safe base64 Fernet master key
        #[arg(long)]
        generate_key: bool,
        /// Read-only decrypt and print the existing `web_api_token` from `system_config` to stdout (WARNING: prints bearer secret in plaintext)
        #[arg(long)]
        show_web_token: bool,
        /// Audit `book_transfers` for any ledger-counted `PARTIAL_IDLE_CAPPED` rows lacking a `DRY_RUN` marker (`R5-M2`)
        #[arg(long)]
        audit_dry_run_residue: bool,
        /// Reclassify historical `PARTIAL_IDLE_CAPPED` rows created before `<ISO8601>` to `DRY_RUN` (`R6-N4`)
        #[arg(long)]
        reclassify_dry_run_before: Option<String>,
    },
    /// Run isolated `:memory:` point-in-time historical backtest and print Tear Sheet
    Backtest {
        /// Historical simulation window in days
        #[arg(long, default_value_t = 365)]
        days: usize,
        /// Force synthetic multi-regime data instead of `data/real_4h_backfill.json`
        #[arg(long)]
        synthetic: bool,
    },
    /// Verify real-time Telegram/Discord operational alerting & retry queue
    Alerts {
        /// Run live or loopback webhook verification test
        #[arg(long)]
        test: bool,
    },
    /// Run the 5-Gate Institutional Mainnet Go/No-Go Readiness Suite
    Readiness {
        /// Override target execution mode (`PAPER`, `TESTNET`, `LIVE`)
        #[arg(long)]
        mode: Option<String>,
        /// Skip full 365-day backtest in Gate 5
        #[arg(long)]
        skip_backtest: bool,
    },
}

/// Validates `--initial-split` (`<PIT_USDC>:<POND_USDC>`) for `aegis-quant settle`.
pub fn parse_and_validate_initial_split(raw: &str) -> Result<(f64, f64)> {
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 2 {
        return Err(AegisError::Validation(format!(
            "Invalid --initial-split '{raw}': expected '<PIT_USDC>:<POND_USDC>' (e.g. 5000:5000)"
        )));
    }
    let pit = parts[0]
        .trim()
        .parse::<f64>()
        .map_err(|e| AegisError::Validation(format!("Invalid PIT value in --initial-split '{raw}': {e}")))?;
    let pond = parts[1]
        .trim()
        .parse::<f64>()
        .map_err(|e| AegisError::Validation(format!("Invalid POND value in --initial-split '{raw}': {e}")))?;
    if !pit.is_finite() || !pond.is_finite() || pit <= 0.0 || pond <= 0.0 {
        return Err(AegisError::Validation(format!(
            "Invalid --initial-split '{raw}': PIT and POND values must both be finite positive numbers (> 0.0)"
        )));
    }
    Ok((pit, pond))
}

/// Validates that the WebUI bind address is loopback (`127.0.0.1`, `::1`, `localhost`) unless
/// remote binding has been explicitly opted into via `--allow-remote-bind`, `AEGIS_ALLOW_REMOTE_BIND=1`,
/// or `web_allow_remote_bind = true` in `system_config`.
pub fn validate_bind_host(
    db: &DatabaseManager,
    host: &str,
    allow_remote_bind_flag: bool,
) -> Result<String> {
    let cfg = db.load_config_or_log_default("aegis-cli.bind_host");
    let effective_host = if host.trim().is_empty() {
        cfg.get_str("web_bind_host", "127.0.0.1")
    } else {
        host.trim().to_string()
    };
    let is_loopback = effective_host == "127.0.0.1"
        || effective_host == "::1"
        || effective_host.eq_ignore_ascii_case("localhost");
    if is_loopback {
        return Ok(effective_host);
    }

    let env_allow = std::env::var("AEGIS_ALLOW_REMOTE_BIND")
        .ok()
        .map(|v| {
            let t = v.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false);
    let db_allow = cfg.get_bool("web_allow_remote_bind", false);

    if allow_remote_bind_flag || env_allow || db_allow {
        Ok(effective_host)
    } else {
        Err(AegisError::SecurityViolation(format!(
            "Refusing to bind WebUI to non-loopback address '{effective_host}' without explicit --allow-remote-bind flag, AEGIS_ALLOW_REMOTE_BIND=1, or web_allow_remote_bind=true in system_config."
        )))
    }
}

/// Top-level orchestrator coordinating SQLite WAL, Book A (`PIT`), Book B (`POND`), and WebUI.
pub struct AegisOrchestrator {
    pub db: DatabaseManager,
    pub telemetry: TelemetryBus,
    pub ingestion: MarketIngestionEngine,
    pub signals: SignalEngine,
    pub execution: ExecutionEngine,
    pub calibrator: WalkForwardCalibrator,
    pub pit: PitOrchestrator,
    pub pond: PondOrchestrator,
    pub cycle_count: usize,
    pub last_pit_cycle_ms: Option<i64>,
    pub last_pond_scan_hour_bucket: Option<i64>,
    pub last_pond_4h_bucket: Option<i64>,
    pub last_reconcile_ms: Option<i64>,
    pub last_retention_prune_ms: Option<i64>,
    pub reconcile_error_streak: usize,
}

impl AegisOrchestrator {
    pub fn new(db: DatabaseManager) -> Self {
        let telemetry = TelemetryBus::new(db.clone());
        let ingestion = MarketIngestionEngine::new(db.clone());
        let signals = SignalEngine::new(db.clone());
        let execution = ExecutionEngine::new(db.clone());
        let calibrator = WalkForwardCalibrator::new(db.clone());
        let pit = PitOrchestrator::new(db.clone(), Some(telemetry.clone()));
        let pond = PondOrchestrator::new(db.clone(), Some(telemetry.clone()));
        let now_ms = db.now_ms();
        Self {
            db,
            telemetry,
            ingestion,
            signals,
            execution,
            calibrator,
            pit,
            pond,
            cycle_count: 0,
            last_pit_cycle_ms: None,
            last_pond_scan_hour_bucket: None,
            last_pond_4h_bucket: None,
            last_reconcile_ms: Some(now_ms),
            last_retention_prune_ms: Some(now_ms),
            reconcile_error_streak: 0,
        }
    }

    /// Spawns Book A (`PitOrchestrator`) and Book B (`PondOrchestrator`) background Tokio tasks (`NQ-3`, `R4-M1`, `R6-QA`).
    ///
    /// NOTE: Serves as a Tokio-based supervision test fixture and async alternative to the synchronous
    /// daemon loop `run_scheduled_daemon_step`. Both paths share identical single-sourced execution delegates
    /// (`PitOrchestrator::run_cycle` and `PondOrchestrator::run_scheduled_step`).
    pub fn spawn_async_book_tasks(
        &self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> (tokio::task::JoinHandle<Value>, tokio::task::JoinHandle<Value>) {
        let cfg = self
            .db
            .load_config_or_log_default("aegis-cli.spawn_async_book_tasks");
        let pit_sec = cfg.get_i64("pit_tick_interval_seconds", 900).max(1) as u64;
        let pond_sec = cfg.get_i64("pond_tick_interval_seconds", 15).max(1) as u64;
        let pit_handle =
            aegis_pit::spawn_pit_task(self.pit.clone(), pit_sec, shutdown_rx.clone());
        let pond_handle =
            aegis_pond::spawn_pond_task(self.pond.clone(), pond_sec, shutdown_rx);
        (pit_handle, pond_handle)
    }

    pub fn initialize(&self, run_calibration: bool) -> Result<()> {
        self.db.initialize_schema(false)?;
        if let Err(e) = self.db.prune_retention_data() {
            tracing::warn!("Startup retention pruning warning: {e}");
        }
        let cfg = self.db.load_config()?;
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        if exec_mode == "LIVE"
            || exec_mode == "TESTNET"
            || pond_mode == "LIVE"
            || pond_mode == "TESTNET"
        {
            if let Err(e) = self.db.validate_signer_binding() {
                tracing::warn!(
                    "Startup signer binding check warning in mode {exec_mode}: {e} (WebUI remains available to update credentials)"
                );
            }
        }
        if let Ok(reconciled) = self.db.reconcile_pending_book_transfers() {
            if reconciled > 0 {
                tracing::warn!(
                    "Startup book_transfers recovery sweep reconciled {reconciled} stale PENDING row(s)"
                );
            }
        }
        if let Ok(stranded) = self.db.check_and_record_stranded_transfer_alert() {
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
        let recon = self.execution.reconcile_exchange_state(None, None);
        let recon_status = recon.status.as_str();
        let recon_disc = recon.discrepancies_found;
        tracing::info!(
            "Startup exchange reconciliation: status={recon_status} discrepancies={recon_disc}"
        );
        if let Err(e) = self.pond.initialize_or_refresh_universe() {
            tracing::error!("Startup Pond universe initialization failed: {e}");
        }
        if run_calibration {
            let cal_bars = cfg
                .get_i64("calibration_lookback_bars", 220)
                .clamp(100, 1500) as usize;
            self.ingestion.ingest_universe(cal_bars, false)?;
            let _ = self.calibrator.calibrate_universe(true)?;
            self.signals.clear_caches();
        }
        Ok(())
    }

    /// Evaluates periodic maintenance triggers (exchange reconcile and retention pruning)
    /// based on elapsed wall-clock time rather than cycle-count multiples (`R6-N1`, `R7-S5`).
    pub fn maybe_run_maintenance(&mut self, is_live_mode: bool) {
        let now_ms = self.db.now_ms();

        // R6-N1 / R7-S5: Exchange reconcile runs on every cycle in LIVE/TESTNET mode, or every 5 minutes (300_000 ms) in PAPER mode.
        let reconcile_due = is_live_mode
            || self
                .last_reconcile_ms
                .map(|last| (now_ms - last) >= 300_000)
                .unwrap_or(true);
        if reconcile_due {
            let recon = self.execution.reconcile_exchange_state(None, None);
            // R8-N5: Track `ErrorDbUnavailable` in all modes (including PAPER mode, where
            // `reconcile_exchange_state` now surfaces DB query failures instead of swallowing them),
            // and additionally track `SkippedOffline` when running in `LIVE` or `TESTNET` mode.
            if recon.status == aegis_execution::ReconcileStatus::ErrorDbUnavailable
                || (is_live_mode
                    && recon.status == aegis_execution::ReconcileStatus::SkippedOffline)
            {
                self.reconcile_error_streak += 1;
                let msg = format!(
                    "Periodic exchange reconciliation degraded (streak={}, status={}, reason={})",
                    self.reconcile_error_streak,
                    recon.status,
                    recon.reason.as_deref().unwrap_or("unknown")
                );
                tracing::error!("{msg}");
                let _ = self.db.log_telemetry(
                    "ERROR",
                    "engine.orchestrator",
                    &msg,
                    serde_json::to_value(&recon).ok(),
                );
                if self.reconcile_error_streak == 3 || self.reconcile_error_streak.is_multiple_of(20) {
                    let _ = self.execution.alerts.notify_critical_event(
                        "EXCHANGE_RECONCILE_DEGRADED",
                        &msg,
                        serde_json::to_value(&recon).ok(),
                    );
                }
            } else {
                self.reconcile_error_streak = 0;
            }
            self.last_reconcile_ms = Some(now_ms);
        }

        if let Ok(stranded) = self.db.check_and_record_stranded_transfer_alert() {
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

        // R6-N1: Retention pruning runs every 1 hour (3_600_000 ms) regardless of cadence interval.
        let prune_due = self
            .last_retention_prune_ms
            .map(|last| (now_ms - last) >= 3_600_000)
            .unwrap_or(true);
        if prune_due {
            if let Err(e) = self.db.prune_retention_data() {
                tracing::warn!("Periodic retention pruning warning: {e}");
            }
            self.last_retention_prune_ms = Some(now_ms);
        }
    }

    pub fn run_single_cycle(&mut self, lookback_bars: usize) -> Result<PortfolioSnapshot> {
        let t0 = Instant::now();
        self.cycle_count += 1;

        let cfg = self.db.load_config()?;
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_mode = matches!(exec_mode.as_str(), "LIVE" | "TESTNET")
            || matches!(pond_mode.as_str(), "LIVE" | "TESTNET");

        self.maybe_run_maintenance(is_live_mode);

        let (frozen, _) = self.pit.is_entry_frozen()?;
        let ingest_stats = self.ingestion.ingest_universe(lookback_bars, false)?;
        let evaluations = self.signals.evaluate_universe(true)?;
        self.last_pit_cycle_ms = Some(self.db.now_ms());
        let snapshot =
            self.execution
                .reconcile_cycle_with_options(&evaluations, false, frozen)?;
        self.last_pit_cycle_ms = Some(self.db.now_ms());
        let pond_summary = self.run_pond_scheduled_step().ok().flatten();
        let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        let pit_eq_now = self
            .pit
            .compute_pit_equity_and_idle_usdc()
            .ok()
            .map(|(eq, _)| eq)
            .unwrap_or_else(|| round_dp(cfg.get_f64("initial_equity_usdc", 10000.0) * split_pct, 2));
        let pond_eq_now = pond_summary
            .as_ref()
            .map(|s| s.pond_equity_usdc)
            .or_else(|| {
                self.pond
                    .compute_pond_equity_and_idle_usdc()
                    .ok()
                    .map(|(eq, _)| eq)
            })
            .unwrap_or_else(|| round_dp(cfg.get_f64("initial_equity_usdc", 10000.0) * (1.0 - split_pct), 2));
        let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let mut actions_summary = std::collections::HashMap::<String, usize>::new();
        for ev in &evaluations {
            *actions_summary.entry(ev.signal_action.clone()).or_insert(0) += 1;
        }

        let details = json!({
            "cycle": self.cycle_count,
            "elapsed_ms": round_dp(elapsed_ms, 1),
            "ingest": ingest_stats,
            "actions": actions_summary,
            "pond": pond_summary,
        });
        let msg = format!(
            "Cycle #{} complete in {:.0}ms | Regime={} (B:{:.0}%/Be:{:.0}%/C:{:.0}%) | PitEq=${:.2} | PondEq=${:.2} | WalletEq=${:.2}",
            self.cycle_count,
            elapsed_ms,
            snapshot.hmm_regime,
            snapshot.prob_bull * 100.0,
            snapshot.prob_bear * 100.0,
            snapshot.prob_chop * 100.0,
            pit_eq_now,
            pond_eq_now,
            snapshot.total_equity_usdc,
        );
        self.db
            .log_telemetry("INFO", "engine.orchestrator", &msg, Some(details.clone()))?;
        let _ = self.telemetry.snapshot_tx.send(details);

        println!(
            "\n------------------------------------------------------------------------------"
        );
        println!(
            "AEGIS QUANT v{} | CYCLE #{} | {:.0} ms",
            PLATFORM_VERSION, self.cycle_count, elapsed_ms
        );
        println!(
            "Regime : {:6} (Bull: {:5.1}% | Bear: {:5.1}% | Chop: {:5.1}%)",
            snapshot.hmm_regime,
            snapshot.prob_bull * 100.0,
            snapshot.prob_bear * 100.0,
            snapshot.prob_chop * 100.0
        );
        println!(
            "Equity : ${:.2} USDC | Cash: ${:.2} | Deployed: ${:.2} ({:.2}x)",
            snapshot.total_equity_usdc,
            snapshot.available_cash_usdc,
            snapshot.deployed_notional_usdc,
            snapshot.gross_leverage
        );
        if let Some(ps) = &pond_summary {
            println!(
                "Pond   : ${:.2} USDC | Champion: {} ({}) | Active Universe: {} | Open Cells: {} (Heat ${:.2})",
                ps.pond_equity_usdc,
                ps.champion_id,
                ps.direction_policy,
                ps.universe_active_count,
                ps.open_cells_after,
                ps.open_heat_usdc
            );
        }
        println!(
            "------------------------------------------------------------------------------\n"
        );

        Ok(snapshot)
    }

    /// Executes Book B (`POND`) scheduled step: delegates to `PondOrchestrator::run_scheduled_step`
    /// for single-sourced cadence semantics across synchronous daemon and Tokio spawners (`R5-M3`, `R5-QB`, `R6-QA`).
    pub fn run_pond_scheduled_step(&mut self) -> Result<Option<aegis_pond::PondCycleSummary>> {
        self.pond.run_scheduled_step(
            &mut self.last_pond_scan_hour_bucket,
            &mut self.last_pond_4h_bucket,
        )
    }

    /// Executes one scheduled daemon iteration for `book_mode` (`"pit"`, `"pond"`, or `"both"`)
    /// inside a `std::panic::catch_unwind` fault-isolation boundary (`Invariant F6`) and returns
    /// `(step_result, sleep_seconds)` dynamically resolved from `system_config` (`R5-M3`, `R5-QB`, `R5-N3`, `R7-S5`).
    pub fn run_scheduled_daemon_step(
        &mut self,
        book_mode: &str,
        fallback_lookback_bars: usize,
    ) -> (Result<()>, u64) {
        let cfg = self
            .db
            .load_config_or_log_default("aegis-cli.run_scheduled_daemon_step");
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_mode = matches!(exec_mode.as_str(), "LIVE" | "TESTNET")
            || matches!(pond_mode.as_str(), "LIVE" | "TESTNET");
        let cycle_bars = cfg
            .get_i64("ingest_lookback_bars", fallback_lookback_bars as i64)
            .clamp(60, 1500) as usize;
        let pit_interval_sec = self
            .pit
            .load_pit_config()
            .map(|c| c.pit_tick_interval_seconds)
            .unwrap_or_else(|_| cfg.get_i64("pit_tick_interval_seconds", 900).max(1) as u64)
            .max(1);
        let pond_interval_sec = cfg.get_i64("pond_tick_interval_seconds", 15).max(1) as u64;
        let poll_interval_sec = cfg.get_i64("polling_interval_seconds", 15).max(1) as u64;

        let mode_norm = book_mode.trim().to_ascii_lowercase();
        match mode_norm.as_str() {
            "pit" => {
                let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.maybe_run_maintenance(is_live_mode);
                    self.last_pit_cycle_ms = Some(self.db.now_ms());
                    let r = self.pit.run_cycle(true, false).map(|_| ());
                    if r.is_ok() {
                        self.last_pit_cycle_ms = Some(self.db.now_ms());
                    }
                    r
                }))
                .unwrap_or_else(|_| {
                    tracing::error!(
                        "CRITICAL: Book A (PIT) cycle caught a panic via catch_unwind; isolating fault"
                    );
                    Err(AegisError::Execution(
                        "Book A (PIT) cycle panicked; fault isolated".to_string(),
                    ))
                });
                (res, pit_interval_sec)
            }
            "pond" => {
                let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.maybe_run_maintenance(is_live_mode);
                    self.run_pond_scheduled_step().map(|_| ())
                }))
                .unwrap_or_else(|_| {
                    tracing::error!(
                        "CRITICAL: Book B (POND) step caught a panic via catch_unwind; isolating fault"
                    );
                    Err(AegisError::Execution(
                        "Book B (POND) step panicked; fault isolated".to_string(),
                    ))
                });
                (res, pond_interval_sec)
            }
            _ => {
                let now_ms = self.db.now_ms();
                let pit_due = self
                    .last_pit_cycle_ms
                    .map(|last_ms| (now_ms - last_ms) >= (pit_interval_sec as i64) * 1000)
                    .unwrap_or(true);
                let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if pit_due {
                        self.run_single_cycle(cycle_bars).map(|_| ())
                    } else {
                        self.maybe_run_maintenance(is_live_mode);
                        self.run_pond_scheduled_step().map(|_| ())
                    }
                }))
                .unwrap_or_else(|_| {
                    tracing::error!(
                        "CRITICAL: Combined orchestrator step caught a panic via catch_unwind; isolating fault"
                    );
                    Err(AegisError::Execution(
                        "Combined orchestrator step panicked; fault isolated".to_string(),
                    ))
                });
                let sleep_sec = pond_interval_sec
                    .min(poll_interval_sec)
                    .min(pit_interval_sec)
                    .max(1);
                (res, sleep_sec)
            }
        }
    }
}

/// 5-Gate Institutional Mainnet Go/No-Go Readiness Verifier (`scripts/check_mainnet_readiness.py`).
pub fn run_readiness_suite(
    db: &DatabaseManager,
    mode_override: Option<&str>,
    skip_backtest: bool,
) -> Result<(bool, String, Vec<CheckResult>)> {
    db.initialize_schema(false)?;
    let cfg = db.load_config()?;
    let target_mode = mode_override
        .map(|s| s.trim().to_ascii_uppercase())
        .unwrap_or_else(|| cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase());
    let is_live_or_testnet = target_mode == "LIVE" || target_mode == "TESTNET";

    let mut checks: Vec<CheckResult> = Vec::new();

    // =========================================================================
    // GATE 1: DATABASE & SCHEMA INTEGRITY
    // =========================================================================
    let (jmode, fk) = db.with_conn(|conn| {
        let jm: String = conn
            .query_row("PRAGMA journal_mode;", [], |r| r.get(0))
            .unwrap_or_default();
        let fk_on: i64 = conn
            .query_row("PRAGMA foreign_keys;", [], |r| r.get(0))
            .unwrap_or(0);
        Ok((jm.to_ascii_lowercase(), fk_on))
    })?;
    let schema_ver = db.get_schema_version().unwrap_or(-1);
    let usdc_cnt = db.get_usdc_perp_universe().map(|v| v.len()).unwrap_or(0);
    let synth_cnt = db.get_btc_synth_universe().map(|v| v.len()).unwrap_or(0);
    let pair_cnt = db.get_alt_pairs_universe().map(|v| v.len()).unwrap_or(0);

    checks.push(CheckResult {
        gate: "Gate 1: Database & Schema".to_string(),
        check_id: "DB-01".to_string(),
        name: "SQLite WAL Mode & Foreign Keys".to_string(),
        status: if jmode == "wal" && fk == 1 {
            "PASS".to_string()
        } else {
            "FAIL".to_string()
        },
        observed: format!("journal_mode={jmode}, foreign_keys={fk}"),
        requirement: "journal_mode=wal, foreign_keys=1".to_string(),
        remediation: "Run: aegis-quant db --init".to_string(),
    });

    checks.push(CheckResult {
        gate: "Gate 1: Database & Schema".to_string(),
        check_id: "DB-02".to_string(),
        name: "Schema Migration Version".to_string(),
        status: if schema_ver >= 13 {
            "PASS".to_string()
        } else {
            "FAIL".to_string()
        },
        observed: format!("schema_version={schema_ver}"),
        requirement: "schema_version >= 13 (migrations 001..013 applied)".to_string(),
        remediation: "Run: aegis-quant db --migrate".to_string(),
    });

    checks.push(CheckResult {
        gate: "Gate 1: Database & Schema".to_string(),
        check_id: "DB-03".to_string(),
        name: "31-Instrument Universe Seeded".to_string(),
        status: if usdc_cnt == 16 && synth_cnt == 15 && pair_cnt == 15 {
            "PASS".to_string()
        } else {
            "FAIL".to_string()
        },
        observed: format!("USDC_PERP={usdc_cnt}, BTC_SYNTH={synth_cnt}, ALT_PAIRS={pair_cnt}"),
        requirement: "16 USDC_PERP + 15 BTC_SYNTH (15 Alt Pairs)".to_string(),
        remediation: "Run: aegis-quant db --init --force".to_string(),
    });

    // =========================================================================
    // GATE 2: SECURITY & KEY MANAGEMENT
    // =========================================================================
    let (active_key_b64, _) = resolve_fernet_master_key(None, Some(&db.db_path));
    let default_key_b64 = default_host_fernet_key_b64();
    let using_external_key =
        active_key_b64 != default_key_b64 && !is_legacy_public_fallback_key(&active_key_b64);
    let project_root = resolve_project_root();
    let key_file = master_key_file_for_db(&db.db_path);
    let mut key_perms_ok = true;
    let mut key_source = "FALLBACK_HOST_KEY".to_string();

    if std::env::var("CREDENTIALS_DIRECTORY")
        .ok()
        .filter(|d| PathBuf::from(d).join("aegis_master_key").exists())
        .is_some()
    {
        key_source = "SYSTEMD_LOAD_CREDENTIAL".to_string();
    } else if std::env::var("AEGIS_MASTER_KEY")
        .or_else(|_| std::env::var("FERNET_MASTER_KEY"))
        .ok()
        .filter(|s| aegis_db::fernet::is_usable_env_key(s))
        .is_some()
    {
        key_source = "ENV_AEGIS_MASTER_KEY".to_string();
    } else if key_file.exists() {
        key_source = format!("FILE({})", key_file.display());
        if let Ok(meta) = fs::metadata(&key_file) {
            let mode_bits = meta.permissions().mode() & 0o777;
            key_perms_ok = (mode_bits & 0o077) == 0;
        }
    }

    let sec01_status = if using_external_key && key_perms_ok {
        "PASS"
    } else if !is_live_or_testnet {
        "WARN"
    } else {
        "FAIL"
    };
    checks.push(CheckResult {
        gate: "Gate 2: Security & Key Mgmt".to_string(),
        check_id: "SEC-01".to_string(),
        name: "Fernet Master Key Isolation".to_string(),
        status: sec01_status.to_string(),
        observed: format!(
            "source={key_source}, external={using_external_key}, perms_0600={key_perms_ok}"
        ),
        requirement: format!(
            "External AEGIS_MASTER_KEY or chmod 600 {}",
            key_file.display()
        ),
        remediation: format!(
            "Generate key: aegis-quant db --generate-key > {} && chmod 600 {}",
            key_file.display(),
            key_file.display()
        ),
    });

    let mut plaintext_leaks = Vec::new();
    for &skey in SENSITIVE_CONFIG_KEYS {
        if let Ok(Some(raw_v)) = db.get_raw_config_value(skey) {
            if !raw_v.trim().is_empty() && !is_encrypted_value(&raw_v) {
                plaintext_leaks.push(skey);
            }
        }
    }
    let enc_report = db.test_encryption();
    let sec02_ok = plaintext_leaks.is_empty() && enc_report.is_ok();
    checks.push(CheckResult {
        gate: "Gate 2: Security & Key Mgmt".to_string(),
        check_id: "SEC-02".to_string(),
        name: "Fernet AES-128 Encryption at Rest".to_string(),
        status: if sec02_ok { "PASS" } else { "FAIL" }.to_string(),
        observed: if sec02_ok {
            "All sensitive keys encrypted (ENC:gAAAA...) & round-trip verified".to_string()
        } else {
            format!("Plaintext leaks={plaintext_leaks:?}")
        },
        requirement: "0 plaintext sensitive keys on disk; AES-128-CBC + HMAC-SHA256 verified"
            .to_string(),
        remediation: "Run: aegis-quant db --test-encryption".to_string(),
    });

    let wallet_addr = cfg.get_str("wallet_address", "");
    let agent_addr = cfg.get_str("hyperliquid_agent_address", "");
    let priv_key = db.get_signing_private_key().unwrap_or_default();
    let wallet_ok =
        !wallet_addr.is_empty() && validate_evm_address(&wallet_addr, "wallet_address").is_ok();
    let priv_ok = !priv_key.is_empty() && validate_private_key(&priv_key).is_ok();
    let binding_ok = if wallet_ok && priv_ok && !agent_addr.trim().is_empty() {
        db.validate_signer_binding().is_ok()
    } else {
        true
    };
    let creds_ready = wallet_ok && priv_ok && binding_ok;
    let sec03_status = if creds_ready {
        "PASS"
    } else if !is_live_or_testnet && binding_ok {
        "WARN"
    } else {
        "FAIL"
    };
    checks.push(CheckResult {
        gate: "Gate 2: Security & Key Mgmt".to_string(),
        check_id: "SEC-03".to_string(),
        name: "Trade-Only Agent Wallet & Key".to_string(),
        status: sec03_status.to_string(),
        observed: format!(
            "wallet_valid={wallet_ok}, private_key_valid={priv_ok}, signer_binding_ok={binding_ok} (mode={target_mode})"
        ),
        requirement: "Valid 42-char 0x wallet_address + 64-hex secp256k1 Agent private_key"
            .to_string(),
        remediation:
            "Configure Trade-Only API Wallet in WebUI Settings or PUT /api/v1/credentials"
                .to_string(),
    });

    let backfill_path = project_root.join("data").join("real_4h_backfill.json");
    let sha_path = project_root
        .join("data")
        .join("real_4h_backfill.json.sha256");
    let prov_ok =
        backfill_path.exists() && sha_path.exists() && verify_data_provenance(&backfill_path).unwrap_or(false);
    checks.push(CheckResult {
        gate: "Gate 2: Security & Key Mgmt".to_string(),
        check_id: "SEC-04".to_string(),
        name: "Backfill Dataset SHA-256 Provenance".to_string(),
        status: if prov_ok { "PASS" } else { "FAIL" }.to_string(),
        observed: if prov_ok {
            "SHA-256 verified against real_4h_backfill.json.sha256".to_string()
        } else {
            "Checksum missing or mismatch".to_string()
        },
        requirement: "Exact SHA-256 match on data/real_4h_backfill.json".to_string(),
        remediation: "Re-verify data/real_4h_backfill.json and data/real_4h_backfill.json.sha256"
            .to_string(),
    });

    // =========================================================================
    // GATE 3: RISK GUARDRAILS & CAPITAL SCALING
    // =========================================================================
    let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
    let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
    let heat_usdc = cfg.get_f64("max_portfolio_heat_usdc", 400.0);
    let single_usdc = cfg.get_f64("max_single_asset_notional_usdc", 2500.0);
    let gross_usdc = cfg.get_f64("max_gross_leverage_notional_usdc", 15000.0);
    let pit_book_eq = round_dp(init_eq * split_pct, 2);
    let is_book_scaled = (heat_usdc - pit_book_eq * 0.04).abs() < (heat_usdc - init_eq * 0.04).abs();
    let pit_ref_eq = if is_book_scaled { pit_book_eq } else { init_eq };
    let heat_ratio = heat_usdc / pit_ref_eq.max(1e-9);
    let single_ratio = single_usdc / pit_ref_eq.max(1e-9);
    let gross_ratio = gross_usdc / pit_ref_eq.max(1e-9);
    let pond_book_eq = round_dp(init_eq * (1.0 - split_pct), 2);
    let pond_heat_usdc = cfg.get_f64("pond_max_heat_usdc", 200.0);
    let pond_cell_usdc = cfg.get_f64("pond_max_cell_notional_usdc", 1250.0);
    let pond_caps_ok = if is_book_scaled {
        ((pond_heat_usdc / pond_book_eq.max(1e-9)) - 0.04).abs() <= 0.005
            && ((pond_cell_usdc / pond_book_eq.max(1e-9)) - 0.25).abs() <= 0.01
    } else {
        true
    };
    let caps_ok = init_eq > 0.0
        && (heat_ratio - 0.04).abs() <= 0.005
        && (single_ratio - 0.25).abs() <= 0.01
        && (gross_ratio - 1.50).abs() <= 0.05
        && pond_caps_ok;

    checks.push(CheckResult {
        gate: "Gate 3: Risk Guardrails".to_string(),
        check_id: "RISK-01".to_string(),
        name: "Institutional Capital & Cap Alignment".to_string(),
        status: if caps_ok { "PASS" } else { "FAIL" }.to_string(),
        observed: format!(
            "Wallet=${init_eq:.2} (Book=${pit_ref_eq:.2}) | Heat=${heat_usdc:.2} ({:.1}%) | Single=${single_usdc:.2} ({:.0}%) | Gross=${gross_usdc:.2} ({gross_ratio:.2}x)",
            heat_ratio * 100.0,
            single_ratio * 100.0
        ),
        requirement: "Heat = 4.0% book equity, Single-Asset = 25% book equity, Gross Leverage = 1.50x book equity"
            .to_string(),
        remediation:
            "Update initial_equity_usdc via POST /api/v1/config/batch with scale_risk_caps=true"
                .to_string(),
    });

    let chand_long = cfg.get_f64("chandelier_atr_multiplier_long", 1.2);
    let chand_short = cfg.get_f64("chandelier_atr_multiplier_short", 1.2);
    let wick_buf = cfg.get_f64("chandelier_wick_buffer_atr", 0.35);
    let trail_atr = cfg.get_f64("chandelier_trail_atr_multiplier", 0.45);
    let trail_max_pct = cfg.get_f64("chandelier_trail_max_pct", 0.008);
    let tight_trail_mode = (0.002..=0.05).contains(&trail_max_pct);
    let classic_trail_mode = (0.05..=0.50).contains(&trail_max_pct) && !tight_trail_mode;
    let stops_ok = (0.15..=1.5).contains(&wick_buf)
        && ((tight_trail_mode
            && (0.5..=5.0).contains(&chand_long)
            && (0.5..=5.5).contains(&chand_short)
            && (0.15..=3.0).contains(&trail_atr))
            || (classic_trail_mode
                && (2.0..=5.0).contains(&chand_long)
                && (2.0..=5.5).contains(&chand_short)
                && (0.15..=10.0).contains(&trail_atr)));

    checks.push(CheckResult {
        gate: "Gate 3: Risk Guardrails".to_string(),
        check_id: "RISK-02".to_string(),
        name: "Two-Tier Chandelier Stop Parameters".to_string(),
        status: if stops_ok { "PASS" } else { "FAIL" }.to_string(),
        observed: format!(
            "Live Tight-Trail: Long={chand_long}x ATR, Short={chand_short}x ATR, WickBuffer={wick_buf}x ATR, Trail={trail_atr}x ATR / {:.2}% cap (4H Backtest scales to 3.5x/3.8x ATR)",
            trail_max_pct * 100.0
        ),
        requirement: "Tight-trail (TrailMaxPct in [0.2%, 5.0%]): Long in [0.5, 5.0]x, Short in [0.5, 5.5]x, TrailATR in [0.15, 3.0]x; Classic (TrailMaxPct in (5.0%, 50.0%]): Long in [2.0, 5.0]x, Short in [2.0, 5.5]x; WickBuffer in [0.15, 1.5]x"
            .to_string(),
        remediation: "Restore Chandelier ATR parameters in system_config"
            .to_string(),
    });

    let mem_db = DatabaseManager::memory();
    mem_db.initialize_schema(false)?;
    let risk_engine = RiskEngine::new(mem_db.clone());
    let sample_asset = mem_db.get_usdc_perp_universe()?.into_iter().next().unwrap();
    let res_ok = risk_engine.evaluate_order_risk(
        &sample_asset,
        "LONG",
        100.0,
        2.5,
        10000.0,
        Some(&[]),
        0.8,
        0.0,
    );
    let saturated_trade = aegis_core::TradeRecord {
        trade_id: 1,
        client_order_id: "sat-1".to_string(),
        symbol: "ETH/USDC:USDC".to_string(),
        base_asset: "ETH".to_string(),
        side: "LONG".to_string(),
        status: "OPEN".to_string(),
        fsm_state: "FILLED".to_string(),
        entry_price: 2500.0,
        current_price: 2500.0,
        exit_price: None,
        quantity: 2.0,
        filled_units: 2.0,
        avg_fill_price: Some(2500.0),
        notional_usdc: 5000.0,
        risk_budget_usdc: 400.0,
        conviction_multiplier: 1.0,
        initial_stop_price: 2300.0,
        chandelier_stop_price: 2300.0,
        current_stop_price: 2300.0,
        highest_price_seen: 2500.0,
        lowest_price_seen: 2500.0,
        realized_pnl_usdc: 0.0,
        unrealized_pnl_usdc: 0.0,
        fees_paid_usdc: 0.0,
        funding_accrued_usdc: 0.0,
        slippage_paid_usd: 0.0,
        exchange_order_id: None,
        opened_at: String::new(),
        closed_at: None,
        exit_reason: None,
    };
    let res_heat = risk_engine.evaluate_order_risk(
        &sample_asset,
        "LONG",
        100.0,
        2.5,
        10000.0,
        Some(&[saturated_trade]),
        0.8,
        0.0,
    );
    let guardrail_pass = res_ok.approved
        && res_ok.notional_usdc <= 2500.0
        && !res_heat.approved
        && res_heat.reason.contains("PORTFOLIO_HEAT");

    checks.push(CheckResult {
        gate: "Gate 3: Risk Guardrails".to_string(),
        check_id: "RISK-03".to_string(),
        name: "Live Guardrail Rejection Self-Test".to_string(),
        status: if guardrail_pass { "PASS" } else { "FAIL" }.to_string(),
        observed: format!(
            "NormalOrder={}, SaturatedHeatOrder={} ({})",
            res_ok.approved, res_heat.approved, res_heat.reason
        ),
        requirement: "Approves valid order <= 25% cap; rejects order when heat >= 4.0%"
            .to_string(),
        remediation: "Inspect crates/aegis-risk/src/lib.rs evaluate_order_risk".to_string(),
    });

    // =========================================================================
    // GATE 4: OPERATIONAL TELEMETRY & ALERTING
    // =========================================================================
    let tg_tok = cfg.get_str("telegram_bot_token", "");
    let tg_chat = cfg.get_str("telegram_chat_id", "");
    let disc_url = cfg.get_str("discord_webhook_url", "");
    let live_alerts_configured = (!tg_tok.is_empty() && !tg_chat.is_empty()) || !disc_url.is_empty();

    let mut test_alert_engine = AlertEngine::new(mem_db.clone());
    let alert_res = test_alert_engine.run_test_alert(true)?;
    let loopback_ok = alert_res.status == "DELIVERED";
    let ops01_status = if live_alerts_configured && loopback_ok {
        "PASS"
    } else if loopback_ok && !is_live_or_testnet {
        "WARN"
    } else if !loopback_ok {
        "FAIL"
    } else {
        "WARN"
    };

    checks.push(CheckResult {
        gate: "Gate 4: Operational Readiness".to_string(),
        check_id: "OPS-01".to_string(),
        name: "Real-Time Alert Channel & Retry Queue".to_string(),
        status: ops01_status.to_string(),
        observed: format!(
            "loopback_self_test={loopback_ok}, live_webhook_configured={live_alerts_configured}"
        ),
        requirement: "Loopback self-test DELIVERED + Telegram/Discord configured for LIVE"
            .to_string(),
        remediation: "Configure Telegram Bot Token & Chat ID in WebUI Settings".to_string(),
    });

    let exec_self = ExecutionEngine::new(mem_db.clone());
    let fsm_res = exec_self.execute_maker_limit_chaser(
        &sample_asset,
        "LONG",
        1.0,
        100.0,
        101.0,
        99.0,
        5000.0,
        Some(0.5),
        None,
        Some("readiness-fsm-check"),
        false,
    );
    let audit_rows = mem_db.get_order_state_audit(Some("readiness-fsm-check"), 20)?;
    let fsm_ok = fsm_res.fsm_state == "FILLED" && audit_rows.len() >= 3;

    checks.push(CheckResult {
        gate: "Gate 4: Operational Readiness".to_string(),
        check_id: "OPS-02".to_string(),
        name: "Order FSM & Audit Trail Integrity".to_string(),
        status: if fsm_ok { "PASS" } else { "FAIL" }.to_string(),
        observed: format!(
            "terminal_state={}, audit_transitions={}",
            fsm_res.fsm_state,
            audit_rows.len()
        ),
        requirement: "PENDING -> OPEN_ON_BOOK -> PARTIALLY_FILLED -> FILLED logged in order_state_audit"
            .to_string(),
        remediation: "Inspect crates/aegis-execution/src/lib.rs execute_maker_limit_chaser"
            .to_string(),
    });

    let mut sim_ex = std::collections::HashMap::new();
    sim_ex.insert(
        "SOL".to_string(),
        json!({"szi": 2.5, "entryPx": 180.0, "markPx": 182.0, "unrealizedPnl": 5.0}),
    );
    let recon_res = exec_self.reconcile_exchange_state(Some(&sim_ex), Some(&[]));
    let recon_status = recon_res.status.as_str().to_string();
    let recon_disc = recon_res.discrepancies_found as u64;
    let recon_ok = recon_status == "RECONCILED" && recon_disc >= 1;

    checks.push(CheckResult {
        gate: "Gate 4: Operational Readiness".to_string(),
        check_id: "OPS-03".to_string(),
        name: "Exchange Position Drift Reconciliation".to_string(),
        status: if recon_ok { "PASS" } else { "FAIL" }.to_string(),
        observed: format!(
            "status={recon_status}, discrepancies_reconciled={recon_disc}",
        ),
        requirement: "Automatically detects and reconciles orphan/phantom/size-drift positions"
            .to_string(),
        remediation: "Inspect crates/aegis-execution/src/lib.rs reconcile_exchange_state"
            .to_string(),
    });

    // =========================================================================
    // GATE 5: OUT-OF-SAMPLE QUANTITATIVE ACCEPTANCE (365-DAY BACKTEST)
    // =========================================================================
    if skip_backtest {
        checks.push(CheckResult {
            gate: "Gate 5: Quant Acceptance".to_string(),
            check_id: "QUANT-01".to_string(),
            name: "365-Day Point-in-Time Backtest".to_string(),
            status: "WARN".to_string(),
            observed: "SKIPPED (--skip-backtest)".to_string(),
            requirement: "Sharpe >= 1.20, Max Drawdown <= 20.0%, Profit Factor >= 1.30".to_string(),
            remediation: "Run without --skip-backtest before Mainnet Go-Live".to_string(),
        });
    } else {
        let pre_trades = db.get_recent_trades(1000)?.len();
        let bt = BacktestEngine::new(db.clone(), 365);
        let ts = bt.run_backtest(None, None, true)?;
        let post_trades = db.get_recent_trades(1000)?.len();
        let isolation_ok = pre_trades == post_trades;

        checks.push(CheckResult {
            gate: "Gate 5: Quant Acceptance".to_string(),
            check_id: "QUANT-01".to_string(),
            name: "Out-of-Sample Annualized Sharpe Ratio".to_string(),
            status: if ts.gate_sharpe_pass { "PASS" } else { "FAIL" }.to_string(),
            observed: format!(
                "Sharpe = {:.2} (4H backtest replay uses 3.5x/3.8x ATR stop profile)",
                ts.annualized_sharpe
            ),
            requirement: "Annualized Sharpe >= 1.20".to_string(),
            remediation: "Run walk-forward calibration: aegis-quant run --once --calibrate"
                .to_string(),
        });

        checks.push(CheckResult {
            gate: "Gate 5: Quant Acceptance".to_string(),
            check_id: "QUANT-02".to_string(),
            name: "Out-of-Sample Maximum Drawdown (MDD)".to_string(),
            status: if ts.gate_mdd_pass { "PASS" } else { "FAIL" }.to_string(),
            observed: format!("Max Drawdown = {:.2}%", ts.max_drawdown_pct),
            requirement: "Maximum Drawdown <= 20.0%".to_string(),
            remediation: "Tighten chandelier_atr_multiplier or conviction thresholds".to_string(),
        });

        checks.push(CheckResult {
            gate: "Gate 5: Quant Acceptance".to_string(),
            check_id: "QUANT-03".to_string(),
            name: "Out-of-Sample Profit Factor".to_string(),
            status: if ts.gate_profit_factor_pass {
                "PASS"
            } else {
                "FAIL"
            }
            .to_string(),
            observed: format!(
                "Profit Factor = {:.2} (Trade PF = {:.2}, Return = {:+.2}%)",
                ts.profit_factor, ts.trade_profit_factor, ts.total_return_pct
            ),
            requirement: "Profit Factor >= 1.30".to_string(),
            remediation: "Inspect fee drag and MACE/SNR conviction thresholds".to_string(),
        });

        checks.push(CheckResult {
            gate: "Gate 5: Quant Acceptance".to_string(),
            check_id: "QUANT-04".to_string(),
            name: "In-Memory Backtest DB Isolation".to_string(),
            status: if isolation_ok { "PASS" } else { "FAIL" }.to_string(),
            observed: format!("live_db_trades_before={pre_trades}, after={post_trades}"),
            requirement: "Backtest runs in :memory: without mutating live SQLite WAL tables"
                .to_string(),
            remediation: "Ensure BacktestEngine clones to :memory: SQLite connection".to_string(),
        });
    }

    let fail_count = checks.iter().filter(|c| c.status == "FAIL").count();
    let warn_count = checks.iter().filter(|c| c.status == "WARN").count();
    let is_go = if is_live_or_testnet {
        fail_count == 0
            && checks
                .iter()
                .filter(|c| c.check_id == "SEC-01" || c.check_id == "SEC-03")
                .all(|c| c.status == "PASS")
    } else {
        fail_count == 0
    };
    let verdict = if is_go {
        format!("GO FOR {target_mode}")
    } else {
        format!(
            "NO-GO FOR {target_mode} ({fail_count} FAIL, {warn_count} WARN)"
        )
    };

    Ok((is_go, verdict, checks))
}

fn print_readiness_report(target_mode: &str, verdict: &str, checks: &[CheckResult]) {
    println!("{}", "=".repeat(100));
    println!(
        "AEGIS QUANT v{} — INSTITUTIONAL MAINNET GO/NO-GO READINESS REPORT (Target Mode: {target_mode})",
        PLATFORM_VERSION
    );
    println!("{}", "=".repeat(100));
    println!(
        "{:<8} | {:<6} | {:<36} | {:<42}",
        "ID", "STATUS", "CHECK NAME", "OBSERVED TELEMETRY"
    );
    println!("{}", "-".repeat(100));
    let mut current_gate = "";
    for c in checks {
        if c.gate != current_gate {
            current_gate = &c.gate;
            println!("[{current_gate}]");
        }
        println!(
            "  {:<6} | {:<6} | {:<36} | {}",
            c.check_id, c.status, c.name, c.observed
        );
        if c.status == "FAIL" || c.status == "WARN" {
            println!("         -> Requirement : {}", c.requirement);
            println!("         -> Remediation : {}", c.remediation);
        }
    }
    println!("{}", "=".repeat(100));
    println!("FINAL VERDICT: {verdict}");
    println!("{}", "=".repeat(100));
}

fn main() -> anyhow::Result<()> {
    aegis_telemetry::init_tracing();

    let cli = Cli::parse();
    let json_out = cli.json;
    let book_flag = cli.book.clone();
    let db = match &cli.db {
        Some(p) => DatabaseManager::new(p),
        None => DatabaseManager::default_db(),
    };

    match cli.command {
        Some(Commands::Db {
            init,
            force,
            migrate,
            test_encryption,
            rotate_key,
            old_key_file,
            new_key_file,
            generate_key,
            show_web_token,
            audit_dry_run_residue,
            reclassify_dry_run_before,
        }) => {
            if audit_dry_run_residue {
                let rows = db.audit_suspect_dry_run_book_transfers()?;
                if json_out {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "suspect_partial_idle_capped_count": rows.len(),
                            "rows": rows,
                        }))?
                    );
                } else if rows.is_empty() {
                    println!(
                        "No suspect PARTIAL_IDLE_CAPPED dry-run residue rows found in book_transfers."
                    );
                } else {
                    println!(
                        "Found {} ledger-counted PARTIAL_IDLE_CAPPED row(s) without DRY_RUN marker:",
                        rows.len()
                    );
                    println!("{}", serde_json::to_string_pretty(&rows)?);
                }
                return Ok(());
            }
            if let Some(cutoff) = reclassify_dry_run_before {
                let count = db.reclassify_dry_run_transfers_before(&cutoff)?;
                if json_out {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "reclassified_count": count,
                            "cutoff_utc": cutoff,
                        }))?
                    );
                } else {
                    println!(
                        "Reclassified {} historical PARTIAL_IDLE_CAPPED row(s) before {} to DRY_RUN.",
                        count, cutoff
                    );
                }
                return Ok(());
            }
            if show_web_token {
                let token = db.get_existing_web_api_token()?.ok_or_else(|| {
                    AegisError::Validation(
                        "web_api_token is not initialized in system_config (run `aegis-quant db --init` or start the WebUI first)".to_string(),
                    )
                })?;
                if json_out {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "web_api_token": token
                        }))?
                    );
                } else {
                    println!("{token}");
                }
                return Ok(());
            }
            if generate_key {
                let key = generate_fernet_key();
                if json_out {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "fernet_key": key
                        }))?
                    );
                } else {
                    println!("{key}");
                }
                return Ok(());
            }
            if init {
                db.initialize_schema(force)?;
                let sv = db.get_schema_version()?;
                if json_out {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "action": "init",
                            "db_path": db.db_path.display().to_string(),
                            "schema_version": sv,
                        }))?
                    );
                } else {
                    println!(
                        "Initialized SQLite WAL database at {} (schema_version={})",
                        db.db_path.display(),
                        sv
                    );
                }
            }
            if migrate {
                let applied = db.run_migrations()?;
                let sv = db.get_schema_version()?;
                if json_out {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "action": "migrate",
                            "applied": applied,
                            "schema_version": sv,
                        }))?
                    );
                } else if applied.is_empty() {
                    println!(
                        "Database schema already up to date (schema_version={sv}) — 0 new migrations applied"
                    );
                } else {
                    println!(
                        "Applied {} migration(s): {:?} (schema_version={})",
                        applied.len(),
                        applied,
                        sv
                    );
                }
            }
            if rotate_key {
                let resolved_old = if let Some(path) = old_key_file {
                    Some(
                        fs::read_to_string(&path)?
                            .lines()
                            .next()
                            .unwrap_or("")
                            .trim()
                            .to_string(),
                    )
                } else {
                    std::env::var("AEGIS_OLD_MASTER_KEY")
                        .ok()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                };
                let target_new = if let Some(path) = new_key_file {
                    fs::read_to_string(&path)?
                        .lines()
                        .next()
                        .unwrap_or("")
                        .trim()
                        .to_string()
                } else {
                    std::env::var("AEGIS_NEW_MASTER_KEY")
                        .ok()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(generate_fernet_key)
                };
                let res = db.rotate_master_key_with_old(resolved_old.as_deref(), &target_new)?;
                println!("{}", serde_json::to_string_pretty(&res)?);
            }
            if test_encryption {
                db.initialize_schema(false)?;
                let rep = db.test_encryption()?;
                println!("{}", serde_json::to_string_pretty(&rep)?);
            }
            if !init && !migrate && !rotate_key && !test_encryption {
                db.initialize_schema(false)?;
                let sv = db.get_schema_version()?;
                if json_out {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "db_path": db.db_path.display().to_string(),
                            "schema_version": sv,
                        }))?
                    );
                } else {
                    println!(
                        "Database ready at {} (schema_version={})",
                        db.db_path.display(),
                        sv
                    );
                }
            }
        }
        Some(Commands::Pond {
            scan,
            refresh_universe,
        }) => {
            db.initialize_schema(false)?;
            let bus = TelemetryBus::new(db.clone());
            let pond = PondOrchestrator::new(db, Some(bus));
            if refresh_universe || !scan {
                let rows = pond.initialize_or_refresh_universe()?;
                if json_out && !scan {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "status": "ok",
                            "universe_count": rows.len(),
                            "universe": rows,
                        }))?
                    );
                } else if !json_out {
                    println!("Refreshed Pond universe: {} coins tracked", rows.len());
                }
            }
            if scan {
                let summary = pond.run_scan_cycle(true)?;
                println!("{}", serde_json::to_string_pretty(&summary)?);
            }
        }
        Some(Commands::Settle {
            dry_run,
            initial_split,
            rich_idle_usdc,
            day_index,
        }) => {
            if !dry_run && (initial_split.is_some() || rich_idle_usdc.is_some()) {
                return Err(AegisError::Validation(
                    "Manual equity/idle overrides (--initial-split / --rich-idle-usdc) are only permitted with --dry-run"
                        .to_string(),
                )
                .into());
            }
            if let Some(idle) = rich_idle_usdc {
                if !idle.is_finite() || idle < 0.0 {
                    return Err(AegisError::Validation(format!(
                        "Invalid --rich-idle-usdc '{idle}': must be finite and >= 0.0"
                    ))
                    .into());
                }
            }
            let (pit_override, pond_override) = match initial_split.as_deref() {
                Some(raw) => {
                    let (pit, pond) = parse_and_validate_initial_split(raw)?;
                    (Some(pit), Some(pond))
                }
                None => (None, None),
            };
            db.initialize_schema(false)?;
            let bus = TelemetryBus::new(db.clone());
            let ctrl = SettleController::new(db, Some(bus));
            let res = ctrl.run_settling(SettleOverrideInput {
                pit_equity_usdc: pit_override,
                pond_equity_usdc: pond_override,
                rich_idle_usdc,
                day_index: Some(day_index),
                dry_run,
            })?;
            if !json_out {
                println!("{}", res.report_line);
            }
            println!("{}", serde_json::to_string_pretty(&res)?);
        }
        Some(Commands::Backtest { days, synthetic }) => {
            db.initialize_schema(false)?;
            let bt = BacktestEngine::new(db, days);
            let ts = bt.run_backtest(None, None, !synthetic)?;
            if json_out {
                println!("{}", serde_json::to_string_pretty(&ts)?);
            } else {
                println!("{}", BacktestEngine::format_tear_sheet(&ts));
            }
            if !ts.all_gates_passed {
                std::process::exit(1);
            }
        }
        Some(Commands::Alerts { test }) => {
            db.initialize_schema(false)?;
            if test {
                let mut alerts = AlertEngine::new(db);
                let res = alerts.run_test_alert(true)?;
                println!("{}", serde_json::to_string_pretty(&res)?);
            } else {
                let status = db.get_credentials_status()?;
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
        }
        Some(Commands::Readiness { mode, skip_backtest }) => {
            let (is_go, verdict, checks) =
                run_readiness_suite(&db, mode.as_deref(), skip_backtest)?;
            let target_mode = mode.unwrap_or_else(|| {
                db.load_config()
                    .map(|c| c.get_str("execution_mode", "PAPER"))
                    .unwrap_or_else(|_| "PAPER".to_string())
            });
            if json_out {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "target_mode": target_mode,
                        "is_go": is_go,
                        "verdict": verdict,
                        "checks": checks,
                    }))?
                );
            } else {
                print_readiness_report(&target_mode, &verdict, &checks);
            }
            if !is_go {
                std::process::exit(1);
            }
        }
        Some(Commands::Run) | None => {
            run_orchestrator(
                db,
                cli.once,
                cli.calibrate,
                &cli.host,
                cli.allow_remote_bind,
                cli.port,
                cli.no_web,
                &book_flag,
            )?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_orchestrator(
    db: DatabaseManager,
    once: bool,
    calibrate: bool,
    host: &str,
    allow_remote_bind: bool,
    port: u16,
    no_web: bool,
    book: &str,
) -> anyhow::Result<()> {
    db.initialize_schema(false)?;
    let validated_host = if !once && !no_web {
        validate_bind_host(&db, host, allow_remote_bind)?
    } else {
        host.to_string()
    };

    let running = Arc::new(AtomicBool::new(true));
    if !once {
        let r_sig = running.clone();
        let web_db = db.clone();
        let bind_host = validated_host;

        std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                rt.block_on(async move {
                    if !no_web {
                        let w_db = web_db.clone();
                        let w_host = bind_host.clone();
                        tokio::spawn(async move {
                            tracing::info!(
                                "Starting Aegis Quant Axum WebUI & SSE server on http://{}:{}",
                                w_host,
                                port
                            );
                            if let Err(e) = serve_web(w_db, &w_host, port).await {
                                tracing::error!("Web server error: {e}");
                            }
                        });
                    }
                    if tokio::signal::ctrl_c().await.is_ok() {
                        tracing::info!("Received Ctrl-C, initiating graceful shutdown...");
                        r_sig.store(false, Ordering::SeqCst);
                    }
                });
            }
        });
    }

    let mut orchestrator = AegisOrchestrator::new(db.clone());
    orchestrator.initialize(calibrate)?;
    let cfg = db.load_config()?;
    let lookback_bars = cfg.get_i64("ingest_lookback_bars", 140).clamp(60, 1500) as usize;

    let book_mode = book.trim().to_ascii_lowercase();
    if once {
        match book_mode.as_str() {
            "pit" => {
                let sum = orchestrator.pit.run_cycle(true, false)?;
                println!("{}", serde_json::to_string_pretty(&sum)?);
            }
            "pond" => {
                let sum = orchestrator.pond.run_scan_cycle(true)?;
                println!("{}", serde_json::to_string_pretty(&sum)?);
            }
            _ => {
                let snap = orchestrator.run_single_cycle(lookback_bars)?;
                println!("{}", serde_json::to_string_pretty(&snap)?);
            }
        }
        return Ok(());
    }

    while running.load(Ordering::SeqCst) {
        let (res, sleep_sec) = orchestrator.run_scheduled_daemon_step(&book_mode, lookback_bars);
        if let Err(e) = res {
            tracing::error!("Orchestrator cycle error ({book_mode}): {e}");
        }

        for _ in 0..sleep_sec {
            if !running.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_b1_validate_bind_host_enforces_loopback_unless_opted_in() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // Loopback hosts are allowed by default
        assert_eq!(
            validate_bind_host(&db, "127.0.0.1", false).unwrap(),
            "127.0.0.1"
        );
        assert_eq!(
            validate_bind_host(&db, "localhost", false).unwrap(),
            "localhost"
        );

        // Non-loopback bind is rejected without explicit opt-in
        assert!(validate_bind_host(&db, "0.0.0.0", false).is_err());

        // Non-loopback bind succeeds with explicit --allow-remote-bind flag or DB config
        assert_eq!(
            validate_bind_host(&db, "0.0.0.0", true).unwrap(),
            "0.0.0.0"
        );
        db.set_config_value("web_allow_remote_bind", &json!(true))
            .unwrap();
        assert_eq!(
            validate_bind_host(&db, "0.0.0.0", false).unwrap(),
            "0.0.0.0"
        );
    }

    #[test]
    fn test_s4_readiness_sec01_checks_db_dir_master_key() {
        let tmp_dir = std::env::temp_dir().join(format!(
            "aegis_cli_sec01_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp_dir).unwrap();
        let db_path = tmp_dir.join("aegis_test.db");
        let db = DatabaseManager::new(&db_path);
        db.initialize_schema(true).unwrap();

        let (is_go, _verdict, checks) = run_readiness_suite(&db, Some("PAPER"), true).unwrap();
        assert!(is_go);
        let sec01 = checks.iter().find(|c| c.check_id == "SEC-01").unwrap();
        assert_eq!(sec01.status, "PASS");
        let expected_keyfile = tmp_dir.join(".aegis_master_key");
        assert!(expected_keyfile.exists());
        assert!(sec01.remediation.contains(&expected_keyfile.display().to_string()));

        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn test_r3_h12_h22_h23_cli_flags_and_initial_split_validation() {
        // Global --json flag works before or after subcommand
        let c1 = Cli::try_parse_from(["aegis-quant", "--json", "readiness", "--skip-backtest"])
            .expect("parse global --json before subcommand");
        assert!(c1.json);
        let c2 = Cli::try_parse_from(["aegis-quant", "backtest", "--days", "30", "--json"])
            .expect("parse global --json after subcommand");
        assert!(c2.json);

        // R3-H12: Global run flags (--port, --once, --host, --no-web) work identically before or after `run`
        let run_before = Cli::try_parse_from([
            "aegis-quant",
            "--port",
            "9000",
            "--once",
            "--host",
            "0.0.0.0",
            "--allow-remote-bind",
            "run",
        ])
        .expect("parse run flags before subcommand");
        let run_after = Cli::try_parse_from([
            "aegis-quant",
            "run",
            "--port",
            "9000",
            "--once",
            "--host",
            "0.0.0.0",
            "--allow-remote-bind",
        ])
        .expect("parse run flags after subcommand");
        assert_eq!(run_before.port, 9000);
        assert_eq!(run_after.port, 9000);
        assert!(run_before.once && run_after.once);
        assert_eq!(run_before.host, "0.0.0.0");
        assert_eq!(run_after.host, "0.0.0.0");
        assert!(run_before.allow_remote_bind && run_after.allow_remote_bind);

        // Raw --old-key / --new-key CLI flags are rejected (R3-H23)
        assert!(
            Cli::try_parse_from(["aegis-quant", "db", "--rotate-key", "--old-key", "secret"])
                .is_err(),
            "--old-key CLI flag must not be accepted on command line (R3-H23)"
        );

        // --show-web-token CLI flag is accepted on `db` subcommand
        let db_tok = Cli::try_parse_from(["aegis-quant", "db", "--show-web-token"])
            .expect("parse --show-web-token");
        assert!(matches!(
            db_tok.command,
            Some(Commands::Db {
                show_web_token: true,
                ..
            })
        ));

        // R4-N2: Both --db and --db-path are accepted globally
        let cli_db = Cli::try_parse_from(["aegis-quant", "--db", "data/aegis_quant.db", "db"])
            .expect("parse --db");
        let cli_db_path =
            Cli::try_parse_from(["aegis-quant", "--db-path", "data/aegis_quant.db", "db"])
                .expect("parse --db-path");
        assert_eq!(cli_db.db, cli_db_path.db);

        // R4-N8: get_existing_web_api_token is strictly read-only when empty
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        assert_eq!(
            db.get_existing_web_api_token().unwrap(),
            None,
            "get_existing_web_api_token must not create or mutate an empty web_api_token (R4-N8)"
        );
        let generated = db.get_or_create_web_api_token().unwrap();
        assert_eq!(
            db.get_existing_web_api_token().unwrap(),
            Some(generated)
        );

        // --initial-split validation (R3-H22: both halves must be finite and > 0.0)
        assert_eq!(
            parse_and_validate_initial_split("5000:5000").unwrap(),
            (5000.0, 5000.0)
        );
        assert!(parse_and_validate_initial_split("invalid").is_err());
        assert!(parse_and_validate_initial_split("-100:5000").is_err());
        assert!(parse_and_validate_initial_split("0:5000").is_err());
        assert!(parse_and_validate_initial_split("5000:0").is_err());
        assert!(parse_and_validate_initial_split("0:0").is_err());
    }

    #[tokio::test]
    async fn test_nq3_spawn_async_book_tasks_supervision() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let orch = AegisOrchestrator::new(db.clone());
        let (_tx, rx) = tokio::sync::watch::channel(true);
        let (pit_h, pond_h) = orch.spawn_async_book_tasks(rx);
        let pit_res = pit_h.await.unwrap();
        let pond_res = pond_h.await.unwrap();
        assert_eq!(pit_res["book_id"], "PIT");
        assert_eq!(pit_res["status"], "STOPPED");
        assert_eq!(pond_res["book_id"], "POND");
        assert_eq!(pond_res["status"], "STOPPED");

        // R5-M3, R5-N3, R5-QB: Verify `run_scheduled_daemon_step` honours `pit_tick_interval_seconds`
        // and `pond_tick_interval_seconds` across all three book modes and dynamically reloads config.
        let mut daemon_orch = AegisOrchestrator::new(db.clone());
        let (_r_pit, pit_sleep) = daemon_orch.run_scheduled_daemon_step("pit", 60);
        assert_eq!(pit_sleep, 900);
        let (_r_pond, pond_sleep) = daemon_orch.run_scheduled_daemon_step("pond", 60);
        assert_eq!(pond_sleep, 15);

        // Update pit_tick_interval_seconds to 300 and pond_tick_interval_seconds to 10 in DB
        db.set_config_value("pit_tick_interval_seconds", &json!(300))
            .unwrap();
        db.set_config_value("pond_tick_interval_seconds", &json!(10))
            .unwrap();
        let (_r_pit2, pit_sleep2) = daemon_orch.run_scheduled_daemon_step("pit", 60);
        assert_eq!(pit_sleep2, 300);
        let (_r_pond2, pond_sleep2) = daemon_orch.run_scheduled_daemon_step("pond", 60);
        assert_eq!(pond_sleep2, 10);

        // In `--book both` mode, PIT runs on the first step (`cycle_count == 1`), is skipped on an
        // immediate subsequent 10s tick before `pit_tick_interval_seconds` elapses (`cycle_count == 1`),
        // and runs again once `last_pit_cycle_ms` is older than `pit_tick_interval_seconds` (`cycle_count == 2`).
        daemon_orch.last_pit_cycle_ms = None;
        let (r_both1, both_sleep1) = daemon_orch.run_scheduled_daemon_step("both", 60);
        assert!(r_both1.is_ok());
        assert_eq!(both_sleep1, 10);
        assert_eq!(daemon_orch.cycle_count, 1);

        let (r_both2, _both_sleep2) = daemon_orch.run_scheduled_daemon_step("both", 60);
        assert!(r_both2.is_ok());
        assert_eq!(
            daemon_orch.cycle_count, 1,
            "Immediate tick before pit_tick_interval_seconds must only run Pond 15s step (R5-M3)"
        );

        daemon_orch.last_pit_cycle_ms = Some(db.now_ms() - 301_000);
        let (r_both3, _both_sleep3) = daemon_orch.run_scheduled_daemon_step("both", 60);
        assert!(r_both3.is_ok());
        assert_eq!(
            daemon_orch.cycle_count, 2,
            "Tick after pit_tick_interval_seconds elapsed must run full PIT cycle (R5-M3)"
        );
    }

    #[test]
    fn test_r6_n1_time_based_maintenance_triggers() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let mut orch = AegisOrchestrator::new(db.clone());

        let t0 = orch.last_reconcile_ms.unwrap();
        let p0 = orch.last_retention_prune_ms.unwrap();

        // Immediate check in PAPER mode: no trigger because 5 min / 1 hour not elapsed
        orch.maybe_run_maintenance(false);
        assert_eq!(orch.last_reconcile_ms.unwrap(), t0);
        assert_eq!(orch.last_retention_prune_ms.unwrap(), p0);

        // Advance reconcile clock past 5 minutes (300_000 ms)
        orch.last_reconcile_ms = Some(t0 - 305_000);
        orch.maybe_run_maintenance(false);
        assert!(
            orch.last_reconcile_ms.unwrap() > t0 - 305_000
                && orch.last_reconcile_ms.unwrap() >= t0,
            "Reconcile must trigger after 5 minutes elapsed wall-clock (R6-N1)"
        );
        assert_eq!(orch.last_retention_prune_ms.unwrap(), p0);

        // Advance retention prune clock past 1 hour (3_600_000 ms)
        orch.last_retention_prune_ms = Some(p0 - 3_605_000);
        orch.maybe_run_maintenance(false);
        assert!(
            orch.last_retention_prune_ms.unwrap() > p0 - 3_605_000
                && orch.last_retention_prune_ms.unwrap() >= p0,
            "Retention pruning must trigger after 1 hour elapsed wall-clock (R6-N1)"
        );

        // In LIVE mode, reconcile runs on every cycle and logs ERROR telemetry when SkippedOffline (R7-S5)
        db.set_config_value("execution_mode", &json!("TESTNET"))
            .unwrap();
        orch.last_reconcile_ms = Some(t0 - 1_000);
        orch.maybe_run_maintenance(true);
        assert!(
            orch.last_reconcile_ms.unwrap() > t0 - 1_000
                && orch.last_reconcile_ms.unwrap() >= t0
        );
        let logs = db.get_recent_logs(20).unwrap();
        assert!(
            logs.iter().any(|l| {
                l["level"] == "ERROR"
                    && l["message"]
                        .as_str()
                        .unwrap_or("")
                        .contains("Periodic exchange reconciliation degraded")
            }),
            "Live/Testnet maintenance must log ERROR telemetry when exchange reconcile returns SkippedOffline (R7-S5)"
        );

        // R7-S5: Verify `run_scheduled_daemon_step` in TESTNET mode runs maintenance on every tick
        // across `--book pond` and `--book both` (when !pit_due) instead of hardcoding `false`.
        let now_bucket_ms = db.now_ms();
        orch.last_pond_scan_hour_bucket = Some(now_bucket_ms / 3_600_000);
        orch.last_pond_4h_bucket = Some(now_bucket_ms / 14_400_000);
        orch.last_reconcile_ms = Some(t0 - 1_000);
        let (_res, _) = orch.run_scheduled_daemon_step("pond", 60);
        assert!(
            orch.last_reconcile_ms.unwrap() > t0 - 1_000,
            "--book pond in TESTNET mode must run live maintenance on every tick (R7-S5)"
        );

        let now_bucket_ms2 = db.now_ms();
        orch.last_pond_scan_hour_bucket = Some(now_bucket_ms2 / 3_600_000);
        orch.last_pond_4h_bucket = Some(now_bucket_ms2 / 14_400_000);
        orch.last_reconcile_ms = Some(t0 - 1_000);
        orch.last_pit_cycle_ms = Some(now_bucket_ms2); // !pit_due
        let (_res2, _) = orch.run_scheduled_daemon_step("both", 60);
        assert!(
            orch.last_reconcile_ms.unwrap() > t0 - 1_000,
            "--book both (!pit_due) in TESTNET mode must run live maintenance on every tick (R7-S5)"
        );
        assert_eq!(
            orch.reconcile_error_streak, 3,
            "Three consecutive SkippedOffline reconciles in TESTNET mode must increment reconcile_error_streak to 3 (R7-S5)"
        );

        // Healthy PAPER reconcile resets reconcile_error_streak to 0
        db.set_config_value("execution_mode", &json!("PAPER"))
            .unwrap();
        orch.last_reconcile_ms = Some(t0 - 305_000);
        orch.maybe_run_maintenance(false);
        assert_eq!(orch.reconcile_error_streak, 0);

        // R7-N8: Verify RISK-02 rejects < 2.0x ATR initial stops when tight-trail is disabled (classic mode)
        db.set_config_value("chandelier_atr_multiplier_long", &json!(1.2))
            .unwrap();
        db.set_config_value("chandelier_trail_max_pct", &json!(0.20))
            .unwrap();
        let (_ok, _verdict, checks) = run_readiness_suite(&db, Some("PAPER"), true).unwrap();
        let risk02_bad = checks
            .iter()
            .find(|c| c.check_id == "RISK-02")
            .unwrap();
        assert_eq!(
            risk02_bad.status, "FAIL",
            "RISK-02 must FAIL when chandelier_atr_multiplier_long < 2.0 in classic trail mode (R7-N8)"
        );
    }
}
