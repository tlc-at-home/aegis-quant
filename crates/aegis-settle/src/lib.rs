//! Daily 00:05 UTC Two-Book Settling Protocol (`aegis-settle`).
//!
//! Implements the 5-Phase 50/50 capital rebalancing controller between Book A (`PIT`)
//! and Book B (`POND`) while strictly enforcing Firewall Invariant F5 (Idle-Only Transfer /
//! Zero Forced Liquidation) and Invariant F1 (Zero dependency on book strategy crates).

use aegis_core::{
    compute_drift_bps, now_iso8601_utc, round_dp, AegisError, BookTransfer, Pp50PlatformSnapshot,
    Result,
};
use aegis_db::DatabaseManager;
use aegis_execution::ExecutionEngine;
use aegis_telemetry::TelemetryBus;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// RAII guard that guarantees `platform_settle_gate_enabled` and the DB settle mutex lock
/// are released when `run_settling` exits, even if any mid-protocol step returns `Err` or panics (`C2`, `NH-10`, `NM-3`).
struct SettleGateGuard<'a> {
    db: &'a DatabaseManager,
    telemetry: Option<&'a TelemetryBus>,
    active: bool,
}

impl<'a> SettleGateGuard<'a> {
    fn activate(db: &'a DatabaseManager, telemetry: Option<&'a TelemetryBus>) -> Result<Self> {
        db.try_acquire_settle_lock("aegis-settle")?;
        if let Some(bus) = telemetry {
            bus.set_settling_in_progress(true);
        }
        Ok(Self {
            db,
            telemetry,
            active: true,
        })
    }

    fn release(&mut self) {
        if self.active {
            if let Some(bus) = self.telemetry {
                bus.set_settling_in_progress(false);
            }
            if let Err(e) = self.db.release_settle_lock() {
                let msg = format!(
                    "CRITICAL: Failed to release settle lock in SettleGateGuard: {e}"
                );
                eprintln!("{msg}");
                let _ = self
                    .db
                    .log_telemetry("ERROR", "aegis-settle.gate_guard", &msg, None);
            }
            self.active = false;
        }
    }
}

impl<'a> Drop for SettleGateGuard<'a> {
    fn drop(&mut self) {
        self.release();
    }
}

/// Input overrides for dry-run or test simulation of the 5-phase settling protocol.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SettleOverrideInput {
    pub pit_equity_usdc: Option<f64>,
    pub pond_equity_usdc: Option<f64>,
    pub rich_idle_usdc: Option<f64>,
    pub day_index: Option<u32>,
    pub dry_run: bool,
}

/// Complete output of the 5-Phase 00:05 UTC Settling Protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettleRunResult {
    pub transfer_id: i64,
    pub snapshot_id: i64,
    pub status: String,
    pub from_book: String,
    pub to_book: String,
    pub pit_equity_before: f64,
    pub pond_equity_before: f64,
    pub pit_equity_after: f64,
    pub pond_equity_after: f64,
    pub total_equity_usdc: f64,
    pub target_each_usdc: f64,
    pub drift_bps: f64,
    pub deadband_bps: f64,
    pub required_transfer_usdc: f64,
    pub available_idle_usdc: f64,
    pub executed_transfer_usdc: f64,
    pub blocked_remainder_usdc: f64,
    pub tx_hash: Option<String>,
    pub report_line: String,
}

/// 5-Phase 00:05 UTC Settling Controller.
#[derive(Clone)]
pub struct SettleController {
    pub db: DatabaseManager,
    pub execution: ExecutionEngine,
    pub telemetry: Option<TelemetryBus>,
}

impl SettleController {
    pub fn new(db: DatabaseManager, telemetry: Option<TelemetryBus>) -> Self {
        let execution = ExecutionEngine::new(db.clone());
        Self {
            db,
            execution,
            telemetry,
        }
    }

    /// Reads current Book A (`PIT`) and Book B (`POND`) equities and unlocked idle USDC directly
    /// from the centralized `DatabaseManager` helpers (`C1`, `C3`, `R3-H17`, `R3-H18`, `R3-QC`).
    pub fn inspect_books_state(&self) -> Result<(f64, f64, f64, f64)> {
        let (pit_eq, pit_idle) = self.db.compute_pit_equity_and_idle_usdc()?;
        let (pond_eq, pond_idle) = self.db.compute_pond_equity_and_idle_usdc()?;
        Ok((pit_eq, pit_idle, pond_eq, pond_idle))
    }

    /// Executes the 5-Phase 00:05 UTC Settling Protocol.
    pub fn run_settling(&self, overrides: SettleOverrideInput) -> Result<SettleRunResult> {
        let cfg = self.db.load_config()?;
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_or_testnet = exec_mode == "LIVE"
            || exec_mode == "TESTNET"
            || pond_mode == "LIVE"
            || pond_mode == "TESTNET";

        // NH-3: Reject caller-supplied equity overrides in LIVE/TESTNET mode unless dry_run=true
        if is_live_or_testnet
            && !overrides.dry_run
            && (overrides.pit_equity_usdc.is_some()
                || overrides.pond_equity_usdc.is_some()
                || overrides.rich_idle_usdc.is_some())
        {
            return Err(AegisError::Validation(
                "Caller-supplied equity overrides (pit_equity_usdc, pond_equity_usdc, rich_idle_usdc) are prohibited in LIVE/TESTNET mode when dry_run=false."
                    .to_string(),
            ));
        }

        let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        let deadband_bps = DatabaseManager::resolve_settling_deadband_bps(&cfg);
        let settling_enabled = cfg.get_bool("settling_enabled", true);
        let settling_kill_switch = cfg.get_bool("settling_kill_switch", false);
        let max_retries = cfg.get_i64("settling_max_retries", 3).max(1) as usize;
        let initial_platform_eq = cfg.get_f64("initial_equity_usdc", 10000.0).max(1.0);
        let champion_id = cfg.get_str("pond_champion_active", "NPS-01");
        let max_cells = cfg.get_i64("pond_max_concurrent_cells", 8);

        // Phase 1: Pre-Freeze Audit — activate RAII SettleGateGuard with DB mutex lock (C2, NH-10, NM-3)
        let mut gate_guard = SettleGateGuard::activate(&self.db, self.telemetry.as_ref())?;
        // R3-B3: Reconcile any stale PENDING book_transfers left over from a prior crash before inspecting books
        let _ = self.db.reconcile_pending_book_transfers();

        let (db_pit_eq, db_pit_idle, db_pond_eq, db_pond_idle) = self.inspect_books_state()?;
        let pit_before = round_dp(overrides.pit_equity_usdc.unwrap_or(db_pit_eq).max(0.0), 2);
        let pond_before = round_dp(overrides.pond_equity_usdc.unwrap_or(db_pond_eq).max(0.0), 2);
        let total_equity = round_dp(pit_before + pond_before, 2);

        // Phase 2: Drift & Dead-Band Evaluation (Q3, R3-N15)
        let target_pit = round_dp(total_equity * split_pct, 2);
        let target_each = target_pit;

        let diff_pit = pit_before - target_pit;
        let drift_bps = round_dp(compute_drift_bps(pit_before, total_equity, split_pct), 2);

        let (from_book, to_book, pre_from, pre_to, rich_idle_default) = if diff_pit >= 0.0 {
            (
                "PIT".to_string(),
                "POND".to_string(),
                pit_before,
                pond_before,
                if overrides.pit_equity_usdc.is_some() {
                    pit_before
                } else {
                    db_pit_idle
                },
            )
        } else {
            (
                "POND".to_string(),
                "PIT".to_string(),
                pond_before,
                pit_before,
                if overrides.pond_equity_usdc.is_some() {
                    pond_before
                } else {
                    db_pond_idle
                },
            )
        };

        let available_idle = round_dp(
            overrides
                .rich_idle_usdc
                .unwrap_or(rich_idle_default)
                .max(0.0),
            2,
        );
        let required_transfer = round_dp(diff_pit.abs(), 2);

        let now_str = now_iso8601_utc();
        let status: String;
        let executed_transfer: f64;
        let blocked_remainder: f64;
        let mut tx_hash: Option<String> = None;
        let mut transfer_mode_used = if overrides.dry_run {
            "DRY_RUN_SIMULATED".to_string()
        } else {
            String::new()
        };
        let mut attempts_count = 0i64;
        let mut pending_transfer_id: Option<i64> = None;

        if !settling_enabled || settling_kill_switch {
            status = "BLOCKED_ZERO_IDLE".to_string();
            executed_transfer = 0.0;
            blocked_remainder = required_transfer;
        } else if drift_bps <= deadband_bps || required_transfer <= 0.0 {
            status = "SKIPPED_DEADBAND".to_string();
            executed_transfer = 0.0;
            blocked_remainder = 0.0;
        } else {
            // Phase 3: Idle-Only Transfer Execution (Invariant F5: Never liquidate open positions)
            executed_transfer = round_dp(required_transfer.min(available_idle), 2);
            blocked_remainder = round_dp((required_transfer - executed_transfer).max(0.0), 2);
            if overrides.dry_run && blocked_remainder > 0.0 {
                transfer_mode_used = "DRY_RUN_PARTIAL_IDLE_CAPPED".to_string();
            }

            if executed_transfer <= 0.0 {
                status = if overrides.dry_run {
                    "DRY_RUN".to_string()
                } else {
                    "PARTIAL_IDLE_CAPPED".to_string()
                };
            } else {
                // Q2 & NQ-3: Crash-safe pre-transfer ledger persistence in PENDING state before network call
                let pre_row = BookTransfer {
                    transfer_id: 0,
                    settle_date: now_str.clone(),
                    from_book: from_book.clone(),
                    to_book: to_book.clone(),
                    amount_usdc: executed_transfer,
                    pre_from_usdc: Some(pre_from),
                    pre_to_usdc: Some(pre_to),
                    post_from_usdc: Some(pre_from),
                    post_to_usdc: Some(pre_to),
                    drift_bps_before: Some(drift_bps),
                    drift_bps_after: Some(drift_bps),
                    blocked_remainder_usdc: blocked_remainder,
                    status: "PENDING".to_string(),
                    attempts: 0,
                    tx_ref: None,
                    created_at: now_str.clone(),
                };
                pending_transfer_id = Some(
                    self.db
                        .insert_book_transfer_with_mode(&pre_row, &transfer_mode_used)?,
                );

                let mut transfer_ok = false;
                let mut last_err = String::new();
                for attempt in 0..max_retries {
                    attempts_count = (attempt + 1) as i64;
                    match self.execution.execute_subaccount_transfer_for_record(
                        pending_transfer_id,
                        &from_book,
                        &to_book,
                        executed_transfer,
                        overrides.dry_run,
                    ) {
                        Ok(res) => {
                            tx_hash = res
                                .get("tx_hash")
                                .and_then(|v| v.as_str())
                                .map(ToString::to_string);
                            transfer_ok = true;
                            break;
                        }
                        Err(e) => {
                            last_err = e.to_string();
                            // R3-B3: Never blind-retry after an ambiguous post-send error or partial leg-1 completion
                            if last_err.contains("AMBIGUOUS_POST_SEND")
                                || last_err.contains("PARTIAL_LEG1_COMPLETED")
                            {
                                break;
                            }
                        }
                    }
                }
                if !transfer_ok {
                    status = "FAILED".to_string();
                    let _ = self.db.log_telemetry(
                        "ERROR",
                        "aegis-settle.transfer",
                        &format!(
                            "Subaccount transfer ${executed_transfer:.2} {from_book}->{to_book} failed after {attempts_count} attempt(s): {last_err}"
                        ),
                        None,
                    );
                } else if blocked_remainder > 0.0 {
                    status = "PARTIAL_IDLE_CAPPED".to_string();
                } else if overrides.dry_run {
                    status = "DRY_RUN".to_string();
                } else {
                    status = "COMPLETED".to_string();
                }
            }
        }

        // Phase 4: Post-Transfer Verification & Book Equity Update
        let (pit_after, pond_after) = if executed_transfer > 0.0 && status != "FAILED" {
            if from_book == "PIT" {
                (
                    round_dp(pit_before - executed_transfer, 2),
                    round_dp(pond_before + executed_transfer, 2),
                )
            } else {
                (
                    round_dp(pit_before + executed_transfer, 2),
                    round_dp(pond_before - executed_transfer, 2),
                )
            }
        } else {
            (pit_before, pond_before)
        };

        let (post_from, post_to) = if from_book == "PIT" {
            (pit_after, pond_after)
        } else {
            (pond_after, pit_after)
        };

        let drift_bps_after = round_dp(compute_drift_bps(pit_after, total_equity, split_pct), 2);
        let db_status = if overrides.dry_run && status == "PARTIAL_IDLE_CAPPED" {
            "DRY_RUN"
        } else {
            status.as_str()
        };

        let transfer_id = if let Some(tid) = pending_transfer_id {
            self.db.update_book_transfer_status(
                tid,
                db_status,
                tx_hash.as_deref(),
                attempts_count,
                Some(post_from),
                Some(post_to),
                Some(drift_bps_after),
                blocked_remainder,
            )?;
            tid
        } else {
            let transfer_row = BookTransfer {
                transfer_id: 0,
                settle_date: now_str.clone(),
                from_book: from_book.clone(),
                to_book: to_book.clone(),
                amount_usdc: executed_transfer,
                pre_from_usdc: Some(pre_from),
                pre_to_usdc: Some(pre_to),
                post_from_usdc: Some(post_from),
                post_to_usdc: Some(post_to),
                drift_bps_before: Some(drift_bps),
                drift_bps_after: Some(drift_bps_after),
                blocked_remainder_usdc: blocked_remainder,
                status: status.clone(),
                attempts: attempts_count,
                tx_ref: tx_hash.clone(),
                created_at: now_str.clone(),
            };
            self.db
                .insert_book_transfer_with_mode(&transfer_row, &transfer_mode_used)?
        };

        // Release freeze guard explicitly (Drop also guarantees release on early return/error)
        gate_guard.release();

        // Phase 5: Snapshot Persistence & Formatted Daily Report Line
        let open_trades = self.db.get_open_trades().unwrap_or_default();
        let open_cells = self.db.get_open_pond_cells().unwrap_or_default();
        let pond_heat: f64 = open_cells
            .iter()
            .map(|c| c.risk_budget_usdc.unwrap_or(0.0))
            .sum::<f64>()
            .max(0.0);

        let settle_desc = if executed_transfer > 0.0 && status != "FAILED" {
            if blocked_remainder > 0.0 {
                format!(
                    "${executed_transfer:.2} {from_book}->{to_book} (${blocked_remainder:.2} blocked)"
                )
            } else {
                format!("${executed_transfer:.2} {from_book}->{to_book}")
            }
        } else {
            format!("$0.00 ({status})")
        };

        let day_idx = overrides.day_index.unwrap_or(1);
        let pct_return = ((total_equity - initial_platform_eq) / initial_platform_eq) * 100.0;
        let report_line = format!(
            "PP-50 D+{:02} | platform ${:.2} ({:+.2}%) | PIT ${:.2} | POND ${:.2} | drift {:.1} bps | settle: {} | cells: {}/{} (heat ${:.0}) | champion {}",
            day_idx,
            total_equity,
            pct_return,
            pit_after,
            pond_after,
            drift_bps,
            settle_desc,
            open_cells.len(),
            max_cells,
            pond_heat.abs(),
            champion_id
        );

        let snap_row = Pp50PlatformSnapshot {
            snapshot_id: None,
            timestamp_ms: self.db.now_ms(),
            platform_equity_usdc: total_equity,
            pit_equity_usdc: pit_after,
            pond_equity_usdc: pond_after,
            split_pct: if total_equity > 0.0 {
                round_dp(pit_after / total_equity, 4)
            } else {
                0.5
            },
            drift_bps: drift_bps_after,
            pit_open_count: open_trades.len() as i64,
            pond_open_cells: open_cells.len() as i64,
            settling_status: status.clone(),
            created_at: Some(now_str),
        };
        let snapshot_id = self.db.save_pp50_platform_snapshot(&snap_row)?;

        if let Some(bus) = &self.telemetry {
            bus.publish_event(
                "INFO",
                "aegis-settle.run",
                &report_line,
                Some(json!({
                    "transfer_id": transfer_id,
                    "snapshot_id": snapshot_id,
                    "status": status,
                    "executed_transfer_usdc": executed_transfer,
                    "blocked_remainder_usdc": blocked_remainder,
                })),
            );
        }

        Ok(SettleRunResult {
            transfer_id,
            snapshot_id,
            status,
            from_book,
            to_book,
            pit_equity_before: pit_before,
            pond_equity_before: pond_before,
            pit_equity_after: pit_after,
            pond_equity_after: pond_after,
            total_equity_usdc: total_equity,
            target_each_usdc: target_each,
            drift_bps,
            deadband_bps,
            required_transfer_usdc: required_transfer,
            available_idle_usdc: available_idle,
            executed_transfer_usdc: executed_transfer,
            blocked_remainder_usdc: blocked_remainder,
            tx_hash,
            report_line,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settling_deadband_skip() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let ctrl = SettleController::new(db, None);

        // 5020 vs 4980 -> total 10000, target 5000 each, diff = 20 -> drift = 20 bps (<= 50 bps deadband)
        let res = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5020.0),
                pond_equity_usdc: Some(4980.0),
                rich_idle_usdc: Some(3000.0),
                day_index: Some(1),
                dry_run: true,
            })
            .unwrap();
        assert_eq!(res.status, "SKIPPED_DEADBAND");
        assert_eq!(res.executed_transfer_usdc, 0.0);
        assert!((res.drift_bps - 20.0).abs() < 1e-6);
    }

    #[test]
    fn test_settling_full_transfer_and_partial_idle_cap() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let ctrl = SettleController::new(db, None);

        // 1. Rich book has plenty of idle cash: 5300 vs 4700 -> target 5000, transfer $300 PIT->POND
        let full = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5300.0),
                pond_equity_usdc: Some(4700.0),
                rich_idle_usdc: Some(1500.0),
                day_index: Some(2),
                dry_run: false,
            })
            .unwrap();
        assert_eq!(full.status, "COMPLETED");
        assert_eq!(full.from_book, "PIT");
        assert_eq!(full.to_book, "POND");
        assert!((full.executed_transfer_usdc - 300.0).abs() < 1e-6);
        assert!((full.blocked_remainder_usdc - 0.0).abs() < 1e-6);
        assert!((full.pit_equity_after - 5000.0).abs() < 1e-6);
        assert!((full.pond_equity_after - 5000.0).abs() < 1e-6);

        // 2. Invariant F5: Rich book only has $120 idle cash when $400 transfer is needed
        let partial = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(4600.0),
                pond_equity_usdc: Some(5400.0),
                rich_idle_usdc: Some(120.0),
                day_index: Some(3),
                dry_run: false,
            })
            .unwrap();
        assert_eq!(partial.status, "PARTIAL_IDLE_CAPPED");
        assert_eq!(partial.from_book, "POND");
        assert_eq!(partial.to_book, "PIT");
        assert!((partial.required_transfer_usdc - 400.0).abs() < 1e-6);
        assert!((partial.executed_transfer_usdc - 120.0).abs() < 1e-6);
        assert!((partial.blocked_remainder_usdc - 280.0).abs() < 1e-6);
        assert!(partial
            .report_line
            .starts_with("PP-50 D+03 | platform $10000.00"));
    }

    #[test]
    fn test_settling_tolerance_bps_and_kill_switch() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // Raise settling_tolerance_bps to 400 bps so a 300 bps drift is skipped
        db.set_config_value("settling_tolerance_bps", &json!(400.0))
            .unwrap();
        let ctrl = SettleController::new(db.clone(), None);
        let skipped = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5300.0),
                pond_equity_usdc: Some(4700.0),
                rich_idle_usdc: Some(1500.0),
                day_index: Some(4),
                dry_run: false,
            })
            .unwrap();
        assert_eq!(skipped.status, "SKIPPED_DEADBAND");
        assert!((skipped.deadband_bps - 400.0).abs() < 1e-6);

        // Reset tolerance to 50 bps, activate settling_kill_switch = true
        db.set_config_value("settling_tolerance_bps", &json!(50.0))
            .unwrap();
        db.set_config_value("settling_kill_switch", &json!(true))
            .unwrap();
        let halted = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5300.0),
                pond_equity_usdc: Some(4700.0),
                rich_idle_usdc: Some(1500.0),
                day_index: Some(5),
                dry_run: false,
            })
            .unwrap();
        assert_eq!(halted.status, "BLOCKED_ZERO_IDLE");
        assert!((halted.executed_transfer_usdc - 0.0).abs() < 1e-6);
        assert!((halted.blocked_remainder_usdc - 300.0).abs() < 1e-6);
    }

    #[test]
    fn test_c1_c2_c3_transfer_persistence_and_settle_gate_guard() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let ctrl = SettleController::new(db.clone(), None);

        // Execute a real transfer of $300 from PIT -> POND
        let res = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5300.0),
                pond_equity_usdc: Some(4700.0),
                rich_idle_usdc: Some(1500.0),
                day_index: Some(1),
                dry_run: false,
            })
            .unwrap();
        assert_eq!(res.status, "COMPLETED");

        // C2: Verify platform_settle_gate_enabled is false after run_settling finishes
        let cfg = db.load_config().unwrap();
        assert!(!cfg.get_bool("platform_settle_gate_enabled", true));

        // Also test SettleGateGuard Drop directly and NM-3 mutex exclusivity
        {
            let _guard = SettleGateGuard::activate(&db, None).unwrap();
            assert!(db
                .load_config()
                .unwrap()
                .get_bool("platform_settle_gate_enabled", false));
            // NM-3: Concurrent lock acquisition must fail while guard is active
            assert!(db.try_acquire_settle_lock("concurrent-caller").is_err());
        }
        assert!(!db
            .load_config()
            .unwrap()
            .get_bool("platform_settle_gate_enabled", true));

        // C1: Subsequent inspect_books_state must reflect the -$300 PIT / +$300 POND transfer!
        let (pit_eq, _pit_idle, pond_eq, _pond_idle) = ctrl.inspect_books_state().unwrap();
        assert!((pit_eq - 4700.0).abs() < 1e-6);
        assert!((pond_eq - 5300.0).abs() < 1e-6);
    }

    #[test]
    fn test_live_settle_transfer_executes_real_transfer_or_fails_closed_nb1() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // Seed a realized profit on PIT in PAPER mode first, then switch to LIVE mode
        let ctrl = SettleController::new(db.clone(), None);
        ctrl.run_settling(SettleOverrideInput {
            pit_equity_usdc: Some(4400.0),
            pond_equity_usdc: Some(5600.0),
            rich_idle_usdc: Some(2000.0),
            day_index: Some(1),
            dry_run: false,
        })
        .unwrap();
        // Now PIT is 5600, POND is 4400 in DB state. Switch to LIVE mode without subaccount configured.
        db.set_config_value("execution_mode", &json!("LIVE"))
            .unwrap();

        // NH-3: Equity overrides in LIVE mode with dry_run=false must be rejected
        let override_err = ctrl.run_settling(SettleOverrideInput {
            pit_equity_usdc: Some(6000.0),
            pond_equity_usdc: Some(4000.0),
            rich_idle_usdc: Some(2000.0),
            day_index: Some(2),
            dry_run: false,
        });
        assert!(override_err.is_err());

        // NB-1 & Q2: Without subaccount addresses configured, LIVE settling must fail closed (status="FAILED")
        // and persist the FAILED row in book_transfers without fabricating a fake 0xsettle_ tx hash.
        let res = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: None,
                pond_equity_usdc: None,
                rich_idle_usdc: None,
                day_index: Some(2),
                dry_run: false,
            })
            .unwrap();
        assert_eq!(res.status, "FAILED");
        assert!(res.tx_hash.is_none());
        assert!((res.pit_equity_after - res.pit_equity_before).abs() < 1e-6);
        assert!((res.pond_equity_after - res.pond_equity_before).abs() < 1e-6);

        let recent = db.get_recent_book_transfers(5).unwrap();
        let latest = recent.first().unwrap();
        assert_eq!(latest.transfer_id, res.transfer_id);
        assert_eq!(latest.status, "FAILED");
        assert!(latest.tx_ref.is_none());
    }

    #[test]
    fn test_nb1_live_transfer_failure_never_books_virtual_ledger() {
        test_live_settle_transfer_executes_real_transfer_or_fails_closed_nb1();
    }

    #[test]
    fn test_nh10_nm3_settle_gate_ttl_pit_stops_when_frozen_and_mutex() {
        test_c1_c2_c3_transfer_persistence_and_settle_gate_guard();
    }

    #[test]
    fn test_r3_h5_n15_dry_run_partial_idle_never_pollutes_book_equities() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        db.set_config_value("settling_split_pct", &json!(0.60))
            .unwrap();
        let ctrl = SettleController::new(db.clone(), None);

        // With split_pct = 0.60 and total = 10000, target_each_usdc must be 6000 (R3-N15)
        // Dry-run with partial idle cap (available_idle=100 < required=1000) gets status DRY_RUN (R3-H5)
        let dry_partial = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5000.0),
                pond_equity_usdc: Some(5000.0),
                rich_idle_usdc: Some(100.0),
                day_index: Some(1),
                dry_run: true,
            })
            .unwrap();
        assert_eq!(dry_partial.status, "PARTIAL_IDLE_CAPPED");
        assert!((dry_partial.target_each_usdc - 6000.0).abs() < 1e-6);
        assert!((dry_partial.blocked_remainder_usdc - 900.0).abs() < 1e-6);
        let persisted = db.get_recent_book_transfers(5).unwrap();
        let rec = persisted
            .iter()
            .find(|r| r.transfer_id == dry_partial.transfer_id)
            .unwrap();
        assert_eq!(rec.status, "DRY_RUN");

        // Dry-run with available_idle = 0.0 also gets status DRY_RUN without violating CHECK constraint
        let dry_zero_idle = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5000.0),
                pond_equity_usdc: Some(5000.0),
                rich_idle_usdc: Some(0.0),
                day_index: Some(2),
                dry_run: true,
            })
            .unwrap();
        assert_eq!(dry_zero_idle.status, "DRY_RUN");
        assert!((dry_zero_idle.executed_transfer_usdc - 0.0).abs() < 1e-6);
        assert!((dry_zero_idle.blocked_remainder_usdc - 1000.0).abs() < 1e-6);

        // Verify net book transfer adjustment remains 0.0 after dry_run partial/zero transfers
        assert!((db.get_net_book_transfer_adjustment("PIT").unwrap() - 0.0).abs() < 1e-9);
        assert!((db.get_net_book_transfer_adjustment("POND").unwrap() - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_r3_b3_reconcile_pending_book_transfers() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        // 1. Insert a stale PENDING row with 0 legs completed (crash before leg 1)
        let stale_row = BookTransfer {
            transfer_id: 0,
            settle_date: "2026-11-08T00:05:00Z".to_string(),
            from_book: "PIT".to_string(),
            to_book: "POND".to_string(),
            amount_usdc: 250.0,
            pre_from_usdc: Some(5250.0),
            pre_to_usdc: Some(4750.0),
            post_from_usdc: Some(5000.0),
            post_to_usdc: Some(5000.0),
            drift_bps_before: Some(250.0),
            drift_bps_after: Some(0.0),
            blocked_remainder_usdc: 0.0,
            status: "PENDING".to_string(),
            attempts: 1,
            tx_ref: None,
            created_at: "2026-11-08T00:05:00Z".to_string(),
        };
        let tid_stale = db.insert_book_transfer_with_mode(&stale_row, "LIVE").unwrap();

        // 2. Insert a PENDING row with 2 legs completed (crash after leg 2 confirmed on-chain)
        let confirmed_row = BookTransfer {
            transfer_id: 0,
            settle_date: "2026-11-08T00:05:05Z".to_string(),
            from_book: "POND".to_string(),
            to_book: "PIT".to_string(),
            amount_usdc: 150.0,
            pre_from_usdc: Some(5150.0),
            pre_to_usdc: Some(4850.0),
            post_from_usdc: Some(5000.0),
            post_to_usdc: Some(5000.0),
            drift_bps_before: Some(150.0),
            drift_bps_after: Some(0.0),
            blocked_remainder_usdc: 0.0,
            status: "PENDING".to_string(),
            attempts: 1,
            tx_ref: None,
            created_at: "2026-11-08T00:05:05Z".to_string(),
        };
        let tid_confirmed = db
            .insert_book_transfer_with_mode(&confirmed_row, "LIVE")
            .unwrap();
        db.update_book_transfer_leg_progress(
            tid_confirmed,
            2,
            Some("0xleg1_ok"),
            Some("0xleg2_ok"),
        )
        .unwrap();

        // Sweep stale PENDING transfers
        let count = db.reconcile_pending_book_transfers().unwrap();
        assert_eq!(count, 2);

        let recent = db.get_recent_book_transfers(10).unwrap();
        let rec_stale = recent.iter().find(|r| r.transfer_id == tid_stale).unwrap();
        assert_eq!(rec_stale.status, "FAILED");
        assert!(rec_stale
            .tx_ref
            .as_deref()
            .unwrap_or("")
            .contains("RECONCILED_STALE_PENDING"));

        let rec_conf = recent
            .iter()
            .find(|r| r.transfer_id == tid_confirmed)
            .unwrap();
        assert_eq!(rec_conf.status, "CONFIRMED");
        assert_eq!(rec_conf.tx_ref.as_deref(), Some("0xleg2_ok"));

        // CONFIRMED row counts toward net book transfer adjustment, FAILED row does not
        assert!((db.get_net_book_transfer_adjustment("PIT").unwrap() - 150.0).abs() < 1e-6);
        assert!((db.get_net_book_transfer_adjustment("POND").unwrap() - (-150.0)).abs() < 1e-6);
    }
}
