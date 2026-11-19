//! Unified Aegis Quant v2.0 CLI & Daemon Orchestrator (`aegis-quant`).
//!
//! Wires `aegis-db` (MariaDB 13.0 Rolling + 7 SQL Views), `aegis-execution` (1s Priority Loop + 3s Exchange Scanner),
//! `aegis-web` (Commercial Animated Web Dashboard & Real-Time SSE Server), `aegis-alerts` (Rich HTML Telegram Alerts),
//! `aegis-backtest` (Multi-Sleeve S1/S2/S3 Walk-Forward Simulation), and `aegis-telemetry`.

use aegis_alerts::send_test_alert;
use aegis_backtest::BacktestEngine;
use aegis_core::{AegisError, Result, PLATFORM_VERSION};
use aegis_db::{create_mariadb_backup, generate_fernet_key, verify_latest_backup, DatabaseManager};
use aegis_execution::ExecutionEngine;
use aegis_telemetry::{init_tracing, TelemetryBus};
use aegis_web::{build_dashboard_payload, build_health_payload, serve_web_with_shutdown};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "aegis-quant",
    version = PLATFORM_VERSION,
    about = "Aegis Quant v2.0 Autonomous Web3 DEX Quantitative Trading Platform (Hyperliquid L1 + MariaDB 13.0)"
)]
struct Cli {
    /// Optional keyfile anchor or database hint (`:memory:` for ephemeral test schema)
    #[arg(long = "db", visible_alias = "db-path", global = true)]
    db: Option<PathBuf>,

    /// Emit machine-readable JSON output
    #[arg(long, global = true)]
    json: bool,

    /// Run a single 3s exchange scan + 1s priority tick + multi-sleeve signal cycle and exit
    #[arg(long, global = true)]
    once: bool,

    /// WebUI bind host (defaults to `127.0.0.1`)
    #[arg(long, global = true, default_value = "127.0.0.1")]
    host: String,

    /// Explicitly allow binding the WebUI server to a non-loopback network interface (`0.0.0.0`)
    #[arg(long, global = true)]
    allow_remote_bind: bool,

    /// WebUI bind port (defaults to `8000`)
    #[arg(long, global = true, default_value_t = 8000)]
    port: u16,

    /// Disable embedded Axum WebUI server when running the daemon
    #[arg(long, global = true)]
    no_web: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the unified v2.0 daemon (1s Priority Loop + 3s Exchange Scanner + WebUI on :8000)
    Run,

    /// Initialize MariaDB 13.0 schema (10 InnoDB tables + 7 SQL Views), migrate preserved credentials, or backup
    Db {
        /// Initialize MariaDB 13.0 schema and seed default v2.0 config (`risk_tier = "extreme"`, `1.85x`)
        #[arg(long)]
        init: bool,

        /// Force drop and recreate MariaDB tables and views during `--init`
        #[arg(long)]
        force: bool,

        /// Apply schema & migrate preserved credentials from `data/aegis_quant.db` into MariaDB `system_config`
        #[arg(long)]
        migrate: bool,

        /// Create a logical MariaDB snapshot backup and verify it
        #[arg(long)]
        backup: bool,

        /// Generate a fresh 32-byte URL-safe base64 Fernet master key
        #[arg(long)]
        generate_key: bool,
    },

    /// Run dynamic Hyperliquid Testnet/Mainnet 1:1 exchange reconciliation, seed 150+ perp assets & initial S1/S2/S3 signals
    Cutover {
        /// Close any open legacy/dust positions on Hyperliquid during cutover instead of 1:1 adopting/syncing them
        #[arg(long)]
        close_legacy: bool,
    },

    /// Trigger a live formatted HTML Telegram test alert (`parse_mode = "HTML"`)
    #[command(visible_alias = "alerts")]
    Alert {
        /// Send live formatted HTML test card to Telegram/Discord
        #[arg(long)]
        test: bool,
    },

    /// Run Multi-Sleeve (S1 Carry-Mom, S2 Trend-Breakout, S3 Event-Reversion) walk-forward backtest
    Backtest {
        /// Historical simulation window in days
        #[arg(long, default_value_t = 30)]
        days: usize,
    },

    /// Print live MariaDB 13.0 health, 7 SQL Views summary, risk tier, and portfolio snapshot status
    Status,
}

/// Validates that the WebUI bind address is loopback unless remote bind is permitted.
pub fn validate_bind_host(
    db: &DatabaseManager,
    host: &str,
    allow_remote_bind_flag: bool,
) -> Result<String> {
    let cfg = db.load_config_or_default();
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
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let db_allow = cfg.get_bool("web_allow_remote_bind", false);

    if allow_remote_bind_flag || env_allow || db_allow {
        Ok(effective_host)
    } else {
        Err(AegisError::SecurityViolation(format!(
            "Refusing to bind WebUI to non-loopback address '{effective_host}' without --allow-remote-bind"
        )))
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received SIGINT/Ctrl-C; shutting down Aegis Quant v2.0 daemon...");
            }
            _ = async {
                if let Some(ref mut s) = sigterm {
                    s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                tracing::info!("Received SIGTERM; shutting down Aegis Quant v2.0 daemon...");
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Received Ctrl-C; shutting down Aegis Quant v2.0 daemon...");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();

    if let Some(Commands::Db {
        generate_key: true, ..
    }) = cli.command
    {
        let key = generate_fernet_key();
        if cli.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({ "fernet_key": key }))?
            );
        } else {
            println!("{key}");
        }
        return Ok(());
    }

    let db = DatabaseManager::new(cli.db.as_deref(), None)?;

    match cli.command.unwrap_or(Commands::Run) {
        Commands::Db {
            init,
            force,
            migrate,
            backup,
            generate_key: _,
        } => {
            if init || migrate || (!backup) {
                db.init_schema(force)?;
            }
            let mut backup_report = None;
            if backup {
                let _path = create_mariadb_backup(&db)?;
                backup_report = Some(verify_latest_backup(&db)?);
            }
            let health = build_health_payload(&db)?;
            let out = json!({
                "status": "ok",
                "command": "db",
                "mariadb_version": health.mariadb_version,
                "database": health.database_name,
                "tables_count": health.tables_count,
                "views_healthy_count": health.views_healthy_count,
                "risk_tier": health.risk_tier,
                "target_leverage_multiplier": health.target_leverage_multiplier,
                "backup": backup_report,
            });
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!(
                    "AEGIS QUANT v{} // MariaDB Schema & Vault Initialized ({} | {} tables | {} views | tier={} {:.2}x)",
                    PLATFORM_VERSION,
                    health.mariadb_version,
                    health.tables_count,
                    health.views_healthy_count,
                    health.risk_tier,
                    health.target_leverage_multiplier
                );
            }
        }

        Commands::Cutover { close_legacy } => {
            db.init_schema(false)?;
            let engine = ExecutionEngine::new(db.clone());
            let report = tokio::task::spawn_blocking(move || {
                engine.run_cutover_reconciliation(close_legacy)
            })
            .await??;

            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "AEGIS QUANT v{} // CUTOVER RECONCILIATION COMPLETE",
                    PLATFORM_VERSION
                );
                println!(
                    "Exchange Perps Seeded : {} (Core: {} | Event: {} | Priority: {})",
                    report.market_scan.total_assets_upserted,
                    report.market_scan.core_eligible_count,
                    report.market_scan.event_eligible_count,
                    report.market_scan.priority_watched_count
                );
                println!(
                    "Legacy Orders/Pos     : Cancelled {} stale orders | Closed {} / Adopted {} positions",
                    report.reconcile.stale_orders_cancelled,
                    report.reconcile.legacy_positions_closed,
                    report.reconcile.legacy_positions_adopted
                );
                println!(
                    "Signals & Equity      : {} sleeve signals evaluated | Equity=${:.2} USDC | Alert={}",
                    report.signals_evaluated,
                    report.snapshot.total_equity_usdc,
                    report.alert_status
                );
            }
        }

        Commands::Alert { test: _ } => {
            db.init_schema(false)?;
            let db_clone = db.clone();
            let res = tokio::task::spawn_blocking(move || send_test_alert(&db_clone)).await?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&res)?);
            } else {
                println!(
                    "AEGIS QUANT v{} // HTML ALERT DISPATCH: status={} provider={} telegram_sent={} chat_id={}",
                    PLATFORM_VERSION,
                    res.status,
                    res.provider,
                    res.telegram_sent,
                    res.chat_id
                );
                if let Some(ref err) = res.error_detail {
                    println!("Detail: {err}");
                }
            }
        }

        Commands::Backtest { days } => {
            db.init_schema(false)?;
            let cfg = db.load_config_or_default();
            let bt = BacktestEngine::from_config_map(&cfg, days);
            let report = tokio::task::spawn_blocking(move || bt.run_default_simulation()).await??;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "AEGIS QUANT v{} // MULTI-SLEEVE WALK-FORWARD BACKTEST ({} days | Tier={} {:.2}x)",
                    PLATFORM_VERSION, report.bars_evaluated / 6, report.risk_tier, report.leverage_multiplier
                );
                println!(
                    "Equity  : ${:.2} -> ${:.2} USDC ({:+.2}%)",
                    report.initial_equity_usdc, report.final_equity_usdc, report.total_return_pct
                );
                println!(
                    "Metrics : Sharpe {:.2} | Sortino {:.2} | MaxDD {:.2}% | WinRate {:.1}% | Trades {}",
                    report.sharpe_ratio,
                    report.sortino_ratio,
                    report.max_drawdown_pct,
                    report.win_rate_pct,
                    report.total_trades
                );
                println!(
                    "Sleeves : S1 Carry ${:+.2} | S2 Trend ${:+.2} | S3 Event ${:+.2}",
                    report.s1_attribution_usdc,
                    report.s2_attribution_usdc,
                    report.s3_attribution_usdc
                );
            }
        }

        Commands::Status => {
            db.init_schema(false)?;
            let health = build_health_payload(&db)?;
            let dash = build_dashboard_payload(&db)?;
            let out = json!({
                "health": health,
                "snapshot": dash.snapshot,
                "counts": {
                    "v_assets_held": dash.v_assets_held.len(),
                    "v_assets_ordered": dash.v_assets_ordered.len(),
                    "v_assets_in_cooldown": dash.v_assets_in_cooldown.len(),
                    "v_assets_sleeve_s1": dash.v_assets_sleeve_s1.len(),
                    "v_assets_sleeve_s2": dash.v_assets_sleeve_s2.len(),
                    "v_assets_sleeve_s3": dash.v_assets_sleeve_s3.len(),
                    "v_priority_watchlist": dash.v_priority_watchlist.len(),
                    "exchange_assets": dash.exchange_assets.len(),
                }
            });
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!(
                    "AEGIS QUANT v{} // STATUS: {} ({} | Mode={} | Tier={} {:.2}x)",
                    PLATFORM_VERSION,
                    health.status.to_uppercase(),
                    health.mariadb_version,
                    health.execution_mode,
                    health.risk_tier.to_uppercase(),
                    health.target_leverage_multiplier
                );
                println!(
                    "Equity: ${:.2} | Avail Cash: ${:.2} | Reserved (WORKING): ${:.2} ({}) | Committed (OPEN): ${:.2} ({}L/{}S)",
                    dash.snapshot.total_equity_usdc,
                    dash.snapshot.available_cash_usdc,
                    dash.snapshot.reserved_order_cash_usdc,
                    dash.v_assets_ordered.len(),
                    dash.snapshot.committed_notional_usdc,
                    dash.snapshot.open_long_count,
                    dash.snapshot.open_short_count
                );
                println!(
                    "Views : Held={} | Ordered={} | Cooldown={} | S1={} | S2={} | S3={} | PriorityWatch={} | Perps={}",
                    dash.v_assets_held.len(),
                    dash.v_assets_ordered.len(),
                    dash.v_assets_in_cooldown.len(),
                    dash.v_assets_sleeve_s1.len(),
                    dash.v_assets_sleeve_s2.len(),
                    dash.v_assets_sleeve_s3.len(),
                    dash.v_priority_watchlist.len(),
                    dash.exchange_assets.len()
                );
            }
        }

        Commands::Run => {
            db.init_schema(false)?;
            let telemetry = TelemetryBus::new(db.clone());
            let engine = ExecutionEngine::new(db.clone());

            if cli.once {
                let eng = engine.clone();
                let report =
                    tokio::task::spawn_blocking(move || eng.run_single_cycle(true)).await??;
                telemetry.publish_event(
                    "INFO",
                    "aegis-cli.once",
                    "Completed single v2.0 execution cycle",
                    serde_json::to_value(&report).ok(),
                );
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "AEGIS QUANT v{} // SINGLE CYCLE COMPLETE: Perps={} | Signals={} | Equity=${:.2}",
                        PLATFORM_VERSION,
                        report.market_scan.total_assets_upserted,
                        report.signals_evaluated,
                        report.snapshot.total_equity_usdc
                    );
                }
                return Ok(());
            }

            // Initial startup scan & signal seed if exchange_assets is empty
            let existing_count = db.list_exchange_assets().map(|v| v.len()).unwrap_or(0);
            if existing_count == 0 {
                let eng = engine.clone();
                let _ = tokio::task::spawn_blocking(move || eng.run_cutover_reconciliation(false))
                    .await;
            }

            let bind_host = validate_bind_host(&db, &cli.host, cli.allow_remote_bind)?;
            let runtime_shutdown_tx = engine.spawn_background_runtime();
            let (web_shutdown_tx, web_shutdown_rx) = tokio::sync::watch::channel(false);

            telemetry.publish_event(
                "INFO",
                "aegis-cli.daemon",
                &format!(
                    "Aegis Quant v2.0 daemon started (1s Priority / 3s Scanner | web=http://{}:{})",
                    bind_host, cli.port
                ),
                None,
            );

            if cli.no_web {
                wait_for_shutdown_signal().await;
                let _ = runtime_shutdown_tx.send(true);
                let _ = web_shutdown_tx.send(true);
            } else {
                let web_db = db.clone();
                let host_clone = bind_host.clone();
                let port = cli.port;
                let mut web_task = tokio::spawn(async move {
                    serve_web_with_shutdown(web_db, &host_clone, port, web_shutdown_rx).await
                });
                let mut web_already_exited = false;

                tokio::select! {
                    _ = wait_for_shutdown_signal() => {}
                    res = &mut web_task => {
                        web_already_exited = true;
                        if let Ok(Err(e)) = res {
                            tracing::error!(error = %e, "Web server terminated with error");
                        }
                    }
                }

                let _ = runtime_shutdown_tx.send(true);
                let _ = web_shutdown_tx.send(true);

                if !web_already_exited {
                    match tokio::time::timeout(std::time::Duration::from_secs(5), web_task).await {
                        Ok(Ok(Err(e))) => {
                            tracing::error!(error = %e, "Web server shutdown error");
                        }
                        Err(_) => {
                            tracing::warn!("Timed out waiting for Web server teardown");
                        }
                        _ => {}
                    }
                }
            }

            // Wait for background 1s/3s runtime thread to finish its current tick and drop its receiver
            if tokio::time::timeout(
                std::time::Duration::from_secs(5),
                runtime_shutdown_tx.closed(),
            )
            .await
            .is_err()
            {
                tracing::warn!("Timed out waiting for background runtime thread teardown");
            }

            let _ = telemetry.log_sync(
                "INFO",
                "aegis-cli.daemon",
                "Aegis Quant v2.0 daemon shut down cleanly",
                None,
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_cli_v2_subcommands_and_flags_parsing() {
        let cli_run = Cli::try_parse_from([
            "aegis-quant",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
            "--allow-remote-bind",
        ])
        .unwrap();
        assert_eq!(cli_run.host, "0.0.0.0");
        assert_eq!(cli_run.port, 8000);
        assert!(cli_run.allow_remote_bind);

        let cli_db = Cli::try_parse_from(["aegis-quant", "db", "--init", "--migrate"]).unwrap();
        assert!(matches!(
            cli_db.command,
            Some(Commands::Db {
                init: true,
                migrate: true,
                ..
            })
        ));

        let cli_cutover = Cli::try_parse_from(["aegis-quant", "cutover"]).unwrap();
        assert!(matches!(
            cli_cutover.command,
            Some(Commands::Cutover {
                close_legacy: false
            })
        ));

        let cli_alert = Cli::try_parse_from(["aegis-quant", "alert", "--test"]).unwrap();
        assert!(matches!(
            cli_alert.command,
            Some(Commands::Alert { test: true })
        ));

        let cli_bt = Cli::try_parse_from(["aegis-quant", "backtest", "--days", "45"]).unwrap();
        assert!(matches!(
            cli_bt.command,
            Some(Commands::Backtest { days: 45 })
        ));

        let cli_status = Cli::try_parse_from(["aegis-quant", "status", "--json"]).unwrap();
        assert!(cli_status.json);
        assert!(matches!(cli_status.command, Some(Commands::Status)));
    }

    #[test]
    fn test_validate_bind_host_loopback_and_remote() {
        let db = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
        assert_eq!(
            validate_bind_host(&db, "127.0.0.1", false).unwrap(),
            "127.0.0.1"
        );
        assert!(validate_bind_host(&db, "0.0.0.0", false).is_err());
        assert_eq!(validate_bind_host(&db, "0.0.0.0", true).unwrap(), "0.0.0.0");
    }
}
