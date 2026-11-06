//! Daily 00:05 UTC Two-Book Settling Protocol (`aegis-settle`).
//!
//! Implements the 5-Phase 50/50 capital rebalancing controller between Book A (`PIT`)
//! and Book B (`POND`) while strictly enforcing Firewall Invariant F5 (Idle-Only Transfer /
//! Zero Forced Liquidation) and Invariant F1 (Zero dependency on book strategy crates).

use aegis_core::{compute_drift_bps, round_dp, BookTransfer, Pp50PlatformSnapshot, Result};
use aegis_db::DatabaseManager;
use aegis_execution::ExecutionEngine;
use aegis_telemetry::TelemetryBus;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// RAII guard that guarantees `platform_settle_gate_enabled` is reset to `false`
/// when `run_settling` exits, even if any mid-protocol step returns `Err` or panics (`C2`).
struct SettleGateGuard<'a> {
    db: &'a DatabaseManager,
    active: bool,
}

impl<'a> SettleGateGuard<'a> {
    fn activate(db: &'a DatabaseManager) -> Result<Self> {
        db.set_config_value("platform_settle_gate_enabled", &json!(true))?;
        Ok(Self { db, active: true })
    }

    fn release(&mut self) {
        if self.active {
            if let Err(e) = self
                .db
                .set_config_value("platform_settle_gate_enabled", &json!(false))
            {
                let msg = format!(
                    "CRITICAL: Failed to clear platform_settle_gate_enabled in SettleGateGuard: {e}"
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
    /// from the database when no test override is supplied (`C1`, `C3`).
    pub fn inspect_books_state(&self) -> Result<(f64, f64, f64, f64)> {
        let cfg = self.db.load_config()?;
        let initial_eq = cfg.get_f64("initial_equity_usdc", 10000.0).max(1.0);
        let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        let pit_init = round_dp(initial_eq * split_pct, 2);
        let pond_init = round_dp(initial_eq * (1.0 - split_pct), 2);
        let pit_net_transfers = self.db.get_net_book_transfer_adjustment("PIT").unwrap_or(0.0);
        let pond_net_transfers = self.db.get_net_book_transfer_adjustment("POND").unwrap_or(0.0);

        let pit_realized = self.db.get_total_realized_pnl().unwrap_or(0.0);
        let open_trades = self.db.get_open_trades().unwrap_or_default();
        let mut pit_deployed = 0.0;
        let mut pit_upnl = 0.0;
        for t in &open_trades {
            pit_deployed += t.quantity.abs() * t.current_price;
            pit_upnl += t.unrealized_pnl_usdc + t.funding_accrued_usdc;
        }
        let pit_eq = round_dp(pit_init + pit_net_transfers + pit_realized + pit_upnl, 2);
        let mut pit_idle = round_dp((pit_eq - pit_deployed).max(0.0), 2);

        let pond_realized = self.db.get_pond_realized_pnl().unwrap_or(0.0);
        let open_cells = self.db.get_open_pond_cells().unwrap_or_default();
        let mut pond_deployed = 0.0;
        let mut pond_upnl = 0.0;
        for c in &open_cells {
            let entry_px = c.entry_price.unwrap_or(0.0);
            let qty = c.quantity.unwrap_or(0.0);
            let mark_px = self
                .db
                .get_ohlcv_bars(&c.symbol, "1h", 1, None)
                .ok()
                .and_then(|bars| bars.last().map(|b| b.close))
                .filter(|&p| p > 0.0)
                .unwrap_or(entry_px);
            let price_pnl = if entry_px > 0.0 && qty > 0.0 {
                if c.side.eq_ignore_ascii_case("LONG") {
                    (mark_px - entry_px) * qty
                } else {
                    (entry_px - mark_px) * qty
                }
            } else {
                0.0
            };
            pond_deployed += c.notional_usdc.unwrap_or(0.0);
            pond_upnl += price_pnl + c.funding_accrued_usdc - c.fees_paid_usdc;
        }
        let pond_eq = round_dp(pond_init + pond_net_transfers + pond_realized + pond_upnl, 2);
        let mut pond_idle = round_dp((pond_eq - pond_deployed).max(0.0), 2);

        if let Ok((_, perps_trading_eq, vault_eq, total_wallet_deployed)) =
            self.db.get_wallet_perps_margin_state()
        {
            if vault_eq > 0.0 {
                let avail_perps_margin =
                    round_dp((perps_trading_eq - total_wallet_deployed).max(0.0), 2);
                pit_idle = pit_idle.min(avail_perps_margin);
                pond_idle = pond_idle.min(avail_perps_margin);
            }
        }

        Ok((pit_eq, pit_idle, pond_eq, pond_idle))
    }

    /// Executes the 5-Phase 00:05 UTC Settling Protocol.
    pub fn run_settling(&self, overrides: SettleOverrideInput) -> Result<SettleRunResult> {
        let cfg = self.db.load_config()?;
        let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        let deadband_bps = cfg
            .get_f64(
                "settling_tolerance_bps",
                cfg.get_f64("settling_deadband_bps", 50.0),
            )
            .max(0.0);
        let settling_enabled = cfg.get_bool("settling_enabled", true);
        let settling_kill_switch = cfg.get_bool("settling_kill_switch", false);
        let max_retries = cfg.get_i64("settling_max_retries", 3).max(1) as usize;
        let initial_platform_eq = cfg.get_f64("initial_equity_usdc", 10000.0).max(1.0);
        let champion_id = cfg.get_str("pond_champion_active", "NPS-01");
        let max_cells = cfg.get_i64("pond_max_concurrent_cells", 8);

        // Phase 1: Pre-Freeze Audit — activate RAII SettleGateGuard (C2)
        let mut gate_guard = SettleGateGuard::activate(&self.db)?;

        let (db_pit_eq, db_pit_idle, db_pond_eq, db_pond_idle) = self.inspect_books_state()?;
        let pit_before = round_dp(overrides.pit_equity_usdc.unwrap_or(db_pit_eq).max(0.0), 2);
        let pond_before = round_dp(overrides.pond_equity_usdc.unwrap_or(db_pond_eq).max(0.0), 2);
        let total_equity = round_dp(pit_before + pond_before, 2);

        // Phase 2: Drift & Dead-Band Evaluation (Q3)
        let target_pit = round_dp(total_equity * split_pct, 2);
        let target_each = round_dp(total_equity * 0.50, 2);

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

        let now_str = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let status: String;
        let executed_transfer: f64;
        let blocked_remainder: f64;
        let mut tx_hash: Option<String> = None;
        let mut attempts_count = 0i64;

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

            if executed_transfer <= 0.0 {
                status = "PARTIAL_IDLE_CAPPED".to_string();
            } else {
                let mut transfer_ok = false;
                for attempt in 0..max_retries {
                    attempts_count = (attempt + 1) as i64;
                    if let Ok(res) = self.execution.execute_subaccount_transfer(
                        &from_book,
                        &to_book,
                        executed_transfer,
                        overrides.dry_run,
                    ) {
                        tx_hash = res
                            .get("tx_hash")
                            .and_then(|v| v.as_str())
                            .map(ToString::to_string);
                        transfer_ok = true;
                        break;
                    }
                }
                if !transfer_ok {
                    status = "FAILED".to_string();
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

        let settle_desc = if executed_transfer > 0.0 {
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
        let transfer_id = self.db.insert_book_transfer(&transfer_row)?;

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

        // Also test SettleGateGuard Drop directly
        {
            let _guard = SettleGateGuard::activate(&db).unwrap();
            assert!(db
                .load_config()
                .unwrap()
                .get_bool("platform_settle_gate_enabled", false));
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
}
