//! Unified Aegis Quant v2.0 CLI & Daemon Orchestrator (`aegis-quant`).
//!
//! Wires `aegis-db` (MariaDB 13.0 Rolling + 7 SQL Views), `aegis-execution` (1s Priority Loop + 3s Exchange Scanner),
//! `aegis-web` (Commercial Animated Web Dashboard & Real-Time SSE Server), `aegis-alerts` (Rich HTML Telegram Alerts),
//! `aegis-backtest` (Multi-Sleeve S1/S2/S3 Walk-Forward Simulation), and `aegis-telemetry`.

use aegis_alerts::{send_test_alert, AlertDispatchResult};
use aegis_backtest::dataset::{assemble_db_dataset, slice_dataset_window, HistoricalDataset};
use aegis_backtest::{BacktestEngine, BacktestReport, SensitivityGridReport, WalkForwardReport};
use aegis_core::indicators::timeframe_to_ms;
use aegis_core::{
    now_ms, resolve_project_root, AegisError, AlertQueueRecord, ConfigMap, Result, RiskTier,
    PLATFORM_VERSION,
};
use aegis_db::{
    create_mariadb_backup, generate_fernet_key, master_key_file_for_db, validate_fernet_key,
    verify_latest_backup, write_master_key_file, BackupVerificationReport, DatabaseManager,
    DestructiveResetToken, FernetCipher, RetentionPolicy, RetentionPruneReport,
};
use aegis_execution::{
    fetch_mainnet_raw_history, ExecutionEngine, HyperliquidHttpClient, MainnetHistoryFetchOptions,
};
use aegis_telemetry::{init_tracing, TelemetryBus};
use aegis_web::{build_dashboard_payload, build_health_payload, serve_web_with_shutdown};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum BacktestSource {
    Db,
    Mainnet,
    Synthetic,
}

impl BacktestSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::Db => "db",
            Self::Mainnet => "mainnet",
            Self::Synthetic => "synthetic",
        }
    }
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum BacktestTimeframe {
    #[value(name = "1h")]
    H1,
    #[value(name = "4h")]
    H4,
}

impl BacktestTimeframe {
    fn as_str(self) -> &'static str {
        match self {
            Self::H1 => "1h",
            Self::H4 => "4h",
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "aegis-quant",
    version = PLATFORM_VERSION,
    about = "Aegis Quant v2.1 Autonomous Web3 DEX Quantitative Trading Platform (Hyperliquid L1 + MariaDB 13.0)"
)]
struct Cli {
    /// Path anchor for .aegis_master_key and optional legacy SQLite migration; MariaDB DSN is configured via AEGIS_DB_URL
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

#[derive(Subcommand, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Start the unified v2.0 daemon (1s Priority Loop + 3s Exchange Scanner + WebUI on :8000)
    Run,

    /// Initialize MariaDB 13.0 schema (11 InnoDB tables + 7 SQL Views), migrate preserved credentials, prune retention, or backup
    Db {
        /// Initialize MariaDB 13.0 schema (11 tables + 7 views), seed defaults, and auto-provision `web_api_token` if unset
        #[arg(long)]
        init: bool,

        /// Force drop and recreate MariaDB tables and views during `--init` (requires interactive `DROP <db>` or `--confirm-destructive-reset`)
        #[arg(long)]
        force: bool,

        /// Explicitly authorize non-interactive destructive schema reset (`--init --force`) without a TTY prompt
        #[arg(long)]
        confirm_destructive_reset: bool,

        /// Explicitly migrate preserved credentials from `data/aegis_quant.db` into MariaDB `system_config`
        #[arg(long)]
        migrate: bool,

        /// Create a logical 11-table MariaDB snapshot backup (0600) and verify it
        #[arg(long)]
        backup: bool,

        /// Execute batched retention pruning across historical MariaDB tables
        #[arg(long)]
        prune: bool,

        /// Re-encrypt `system_config` secrets from a legacy SHA-256 passphrase cipher to a strict 32-byte Fernet master key
        #[arg(long)]
        migrate_legacy_key: bool,

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

    /// Inspect recent `alert_queue` deliveries (read-only) or dispatch a live formatted HTML test alert (`--test`)
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

        /// Historical data source (`db` loads MariaDB; `mainnet` fetches Hyperliquid Mainnet public API; `synthetic` uses deterministic generator)
        #[arg(long, value_enum, default_value_t = BacktestSource::Synthetic)]
        source: BacktestSource,

        /// Candle timeframe (`1h` or `4h`) used with `--source db` or `--source mainnet`
        #[arg(long, value_enum, default_value_t = BacktestTimeframe::H4)]
        timeframe: BacktestTimeframe,

        /// Maximum top-volume eligible Mainnet assets to fetch with `--source mainnet` (1-150)
        #[arg(long, default_value_t = 30)]
        max_assets: usize,

        /// Warmup bars preceding the evaluation window (1-500)
        #[arg(long, default_value_t = 72)]
        warmup_bars: usize,

        /// Optional risk-tier override for sensitivity analysis (`conservative`, `standard`, `aggressive`, `extreme`, `custom`)
        #[arg(long)]
        risk_tier: Option<String>,

        /// Optional starting equity override in USDC (>= 100.0)
        #[arg(long)]
        equity: Option<f64>,

        /// Optional override for `profit_gate_signal_exits` (`true` or `false`)
        #[arg(long)]
        profit_gate: Option<bool>,

        /// Optional override for `max_open_positions` (1-100)
        #[arg(long)]
        max_open_positions: Option<usize>,

        /// Optional override for `min_entry_interval_sec` in seconds (0-86400)
        #[arg(long)]
        min_entry_interval_sec: Option<i64>,

        /// Optional override for `stagnation_time_stop_hours` (0.0-168.0; 0 disables stagnation stop)
        #[arg(long)]
        stagnation_hours: Option<f64>,

        /// Optional override for `risk_per_trade_equity_pct` (0.0-0.05; 0 disables risk-budget cap)
        #[arg(long)]
        risk_per_trade_pct: Option<f64>,

        /// Optional path to save the assembled `HistoricalDataset` JSON for deterministic replay
        #[arg(long)]
        save_dataset: Option<PathBuf>,

        /// Optional path to load a saved `HistoricalDataset` JSON directly (bypassing network/DB fetch)
        #[arg(long)]
        load_dataset: Option<PathBuf>,

        /// Optional inclusive evaluation window start timestamp in epoch milliseconds (retains prior bars as warmup)
        #[arg(long)]
        eval_start_ms: Option<i64>,

        /// Optional exclusive evaluation window end timestamp in epoch milliseconds
        #[arg(long)]
        eval_end_ms: Option<i64>,

        /// Run `N` rolling out-of-sample walk-forward folds (`1..=50`) instead of a single pass
        #[arg(long)]
        walk_forward_folds: Option<usize>,

        /// Rolling walk-forward in-sample/warmup window in days (default: 14)
        #[arg(long, default_value_t = 14)]
        train_days: usize,

        /// Rolling walk-forward out-of-sample test window in days (default: 7)
        #[arg(long, default_value_t = 7)]
        test_days: usize,

        /// Run the 3x3 sensitivity matrix (`stagnation_hours in [12, 16, 24]` x `risk_per_trade_pct in [1.5%, 2.0%, 2.5%]`)
        #[arg(long)]
        sensitivity_grid: bool,

        /// Lever N1: enable directional chop/neutral regime budget multipliers (`true` or `false`)
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        regime_budget_mult: Option<bool>,

        /// Lever N2: consecutive bars required to confirm a non-emergency signal exit (`1..=5`)
        #[arg(long)]
        exit_confirm_bars: Option<usize>,

        /// Lever N3: close to flat instead of direct single-step side flips (`true` or `false`)
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        suppress_flips: Option<bool>,

        /// Lever N5: cap portfolio net exposure as a fraction of gross exposure (`0.10..=1.0`)
        #[arg(long)]
        net_exposure_cap: Option<f64>,

        /// Lever N4: route low-urgency entries as resting passive maker-only orders (`true` or `false`)
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        maker_first_entries: Option<bool>,

        /// Lever N6: defer adverse-funding non-flip entries near hourly funding settlement (`true` or `false`)
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        funding_timing: Option<bool>,

        /// Parity cap on maximum new entries allowed per signal bar (`0..=100`; 0 disables cap)
        #[arg(long)]
        max_entries_per_bar: Option<usize>,
    },

    /// Print live MariaDB 13.0 health, 7 SQL Views summary, risk tier, and portfolio snapshot status
    Status,
}

#[derive(Debug, Clone)]
struct DbCommandOptions {
    init: bool,
    force: bool,
    confirm_destructive_reset: bool,
    migrate: bool,
    backup: bool,
    prune: bool,
    migrate_legacy_key: bool,
}

impl DbCommandOptions {
    fn is_read_only_bare(&self) -> bool {
        !self.init && !self.migrate && !self.backup && !self.prune && !self.migrate_legacy_key
    }
}

struct DbCommandOutcome {
    read_only_status: bool,
    credentials_migrated: Option<usize>,
    legacy_secrets_rotated: Option<usize>,
    wrote_new_keyfile: bool,
    web_api_token_provisioned: bool,
    one_time_web_api_token: Option<String>,
    prune_report: Option<RetentionPruneReport>,
    backup_report: Option<BackupVerificationReport>,
    health: aegis_web::HealthPayload,
}

enum AlertCommandOutcome {
    ReadOnlySummary {
        recent: Vec<AlertQueueRecord>,
        status_counts: BTreeMap<String, usize>,
    },
    LiveTest(Box<AlertDispatchResult>),
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

/// Rejects `db --force` without `--init` immediately before opening a database connection (`R14-H6`).
fn validate_db_command_flags(init: bool, force: bool) -> Result<()> {
    if force && !init {
        return Err(AegisError::Validation(
            "Refusing --force without --init: destructive schema reset requires explicit `--init --force`"
                .to_string(),
        ));
    }
    Ok(())
}

/// Authorizes a destructive schema reset (`R14-H6`), requiring either an ephemeral test DB,
/// `--confirm-destructive-reset`, or interactive TTY confirmation typing exact `DROP <db_name>`.
fn authorize_destructive_reset_with_io<R: BufRead>(
    db_name: &str,
    is_ephemeral: bool,
    confirm_destructive_reset: bool,
    stdin_is_tty: bool,
    json_mode: bool,
    reader: &mut R,
) -> Result<DestructiveResetToken> {
    if is_ephemeral {
        return Ok(DestructiveResetToken::ephemeral_test_only());
    }
    if !confirm_destructive_reset {
        if !(stdin_is_tty && !json_mode) {
            return Err(AegisError::SecurityViolation(
                "Non-interactive destructive reset requires --confirm-destructive-reset"
                    .to_string(),
            ));
        }
        let expected = format!("DROP {db_name}");
        eprint!(
            "WARNING: This will permanently DROP all 7 views and 11 tables in MariaDB database '{db_name}'.\n\
             Type '{expected}' to confirm: "
        );
        let _ = std::io::stderr().flush();
        let mut line = String::new();
        reader.read_line(&mut line).map_err(AegisError::Io)?;
        if line.trim() != expected {
            return Err(AegisError::SecurityViolation(format!(
                "Destructive reset aborted: confirmation input did not match '{expected}'"
            )));
        }
    }
    Ok(DestructiveResetToken::confirm_for_db(db_name))
}

fn generate_random_api_token() -> String {
    loop {
        let candidate = generate_fernet_key();
        if !candidate.starts_with("gAAAAAB") {
            return candidate;
        }
    }
}

fn resolve_anchor_path(anchor: Option<&Path>) -> PathBuf {
    anchor
        .map(Path::to_path_buf)
        .unwrap_or_else(|| resolve_project_root().join("data").join("aegis_quant.db"))
}

fn resolve_legacy_passphrase_key_b64<R: BufRead>(
    anchor_path: &Path,
    stdin_is_tty: bool,
    json_mode: bool,
    reader: &mut R,
) -> Result<String> {
    let mut raw_opt: Option<String> = None;
    for env_name in ["AEGIS_LEGACY_MASTER_KEY", "AEGIS_LEGACY_PASSPHRASE"] {
        if let Ok(val) = std::env::var(env_name) {
            let trimmed = val.trim().to_string();
            if !trimmed.is_empty() {
                raw_opt = Some(trimmed);
                break;
            }
        }
    }

    if raw_opt.is_none() {
        let key_file = master_key_file_for_db(anchor_path);
        if key_file.is_file() {
            if let Ok(bytes) = std::fs::read(&key_file) {
                if validate_fernet_key(&bytes).is_err() {
                    let s = String::from_utf8_lossy(&bytes).trim().to_string();
                    if !s.is_empty() {
                        raw_opt = Some(s);
                    }
                }
            }
        }
    }

    if raw_opt.is_none() && stdin_is_tty && !json_mode {
        eprint!("Enter legacy Fernet passphrase or key to migrate: ");
        let _ = std::io::stderr().flush();
        let mut line = String::new();
        reader.read_line(&mut line).map_err(AegisError::Io)?;
        let trimmed = line.trim().to_string();
        if !trimmed.is_empty() {
            raw_opt = Some(trimmed);
        }
    }

    let raw = raw_opt.ok_or_else(|| {
        AegisError::Validation(
            "Missing legacy key source: set AEGIS_LEGACY_MASTER_KEY, store legacy passphrase in .aegis_master_key, or run interactively in a TTY"
                .to_string(),
        )
    })?;

    if let Ok(strict_b64) = validate_fernet_key(raw.as_bytes()) {
        Ok(strict_b64)
    } else {
        let cipher = FernetCipher::from_legacy_sha256_passphrase(&raw)?;
        Ok(cipher.key_b64().to_string())
    }
}

fn connect_and_migrate_legacy_key(
    anchor: Option<&Path>,
    json_mode: bool,
) -> Result<(DatabaseManager, usize, bool)> {
    let anchor_buf = resolve_anchor_path(anchor);
    let stdin = std::io::stdin();
    let is_tty = stdin.is_terminal();
    let mut locked_stdin = stdin.lock();
    let old_key_b64 =
        resolve_legacy_passphrase_key_b64(&anchor_buf, is_tty, json_mode, &mut locked_stdin)?;

    if let Ok(db_strict) = DatabaseManager::connect(anchor, None) {
        if db_strict.has_external_master_key() && db_strict.fernet().key_b64() != old_key_b64 {
            let new_key_b64 = db_strict.fernet().key_b64().to_string();
            let rotated = db_strict.rotate_fernet_master_key(&old_key_b64, &new_key_b64)?;
            return Ok((db_strict, rotated, false));
        }
    }

    let db_old = DatabaseManager::connect(anchor, Some(&old_key_b64))?;
    let new_key_b64 = generate_fernet_key();
    let rotated = db_old.rotate_fernet_master_key(&old_key_b64, &new_key_b64)?;
    let key_file = master_key_file_for_db(&anchor_buf);
    write_master_key_file(&key_file, &new_key_b64)?;
    Ok((db_old, rotated, true))
}

fn execute_db_command_with_io<R: BufRead>(
    db: &DatabaseManager,
    anchor: Option<&Path>,
    opts: &DbCommandOptions,
    stdin_is_tty: bool,
    json_mode: bool,
    reader: &mut R,
) -> Result<DbCommandOutcome> {
    validate_db_command_flags(opts.init, opts.force)?;

    if opts.is_read_only_bare() {
        let health = build_health_payload(db)?;
        return Ok(DbCommandOutcome {
            read_only_status: true,
            credentials_migrated: None,
            legacy_secrets_rotated: None,
            wrote_new_keyfile: false,
            web_api_token_provisioned: false,
            one_time_web_api_token: None,
            prune_report: None,
            backup_report: None,
            health,
        });
    }

    if opts.init {
        if opts.force {
            let token = authorize_destructive_reset_with_io(
                db.db_name(),
                db.is_ephemeral_test_db(),
                opts.confirm_destructive_reset,
                stdin_is_tty,
                json_mode,
                reader,
            )?;
            db.reset_schema_destructive(token)?;
        } else {
            db.init_schema()?;
        }
    }

    let credentials_migrated = if opts.migrate {
        let sqlite_path = resolve_anchor_path(anchor);
        Some(db.migrate_legacy_sqlite_credentials(&sqlite_path)?)
    } else {
        None
    };

    let mut web_api_token_provisioned = false;
    let mut one_time_web_api_token = None;
    if opts.init {
        let cfg = db.load_config()?;
        let existing = cfg.get("web_api_token").map(|s| s.trim()).unwrap_or("");
        if existing.is_empty() {
            let tok = generate_random_api_token();
            db.set_config_value(
                "web_api_token",
                &tok,
                Some("Auto-provisioned 32-byte Fernet-encrypted Web API bearer token"),
            )?;
            web_api_token_provisioned = true;
            one_time_web_api_token = Some(tok);
        }
    }

    let prune_report = if opts.prune {
        let cfg = db.load_config_or_default();
        let policy = RetentionPolicy::from_config(&cfg);
        Some(db.prune_retention(&policy)?)
    } else {
        None
    };

    let backup_report = if opts.backup {
        let _path = create_mariadb_backup(db)?;
        Some(verify_latest_backup(db)?)
    } else {
        None
    };

    let health = build_health_payload(db)?;
    Ok(DbCommandOutcome {
        read_only_status: false,
        credentials_migrated,
        legacy_secrets_rotated: None,
        wrote_new_keyfile: false,
        web_api_token_provisioned,
        one_time_web_api_token,
        prune_report,
        backup_report,
        health,
    })
}

fn execute_alert_command(db: &DatabaseManager, test: bool) -> Result<AlertCommandOutcome> {
    if test {
        db.init_schema()?;
        return Ok(AlertCommandOutcome::LiveTest(Box::new(send_test_alert(db))));
    }
    let recent = db.list_recent_alerts(10)?;
    let mut status_counts: BTreeMap<String, usize> = BTreeMap::new();
    for item in &recent {
        *status_counts.entry(item.status.clone()).or_insert(0) += 1;
    }
    Ok(AlertCommandOutcome::ReadOnlySummary {
        recent,
        status_counts,
    })
}

struct BacktestRunOptions {
    days: usize,
    source: BacktestSource,
    timeframe: BacktestTimeframe,
    max_assets: usize,
    warmup_bars: usize,
    risk_tier: Option<String>,
    equity: Option<f64>,
    profit_gate: Option<bool>,
    max_open_positions: Option<usize>,
    min_entry_interval_sec: Option<i64>,
    stagnation_hours: Option<f64>,
    risk_per_trade_pct: Option<f64>,
    save_dataset: Option<PathBuf>,
    load_dataset: Option<PathBuf>,
    eval_start_ms: Option<i64>,
    eval_end_ms: Option<i64>,
    walk_forward_folds: Option<usize>,
    train_days: usize,
    test_days: usize,
    sensitivity_grid: bool,
    regime_budget_mult: Option<bool>,
    exit_confirm_bars: Option<usize>,
    suppress_flips: Option<bool>,
    net_exposure_cap: Option<f64>,
    maker_first_entries: Option<bool>,
    funding_timing: Option<bool>,
    max_entries_per_bar: Option<usize>,
}

enum BacktestCommandOutcome {
    Single(Box<BacktestReport>),
    WalkForward(WalkForwardReport),
    SensitivityGrid(SensitivityGridReport),
}

fn validate_backtest_run_options(opts: &BacktestRunOptions) -> Result<()> {
    if opts.sensitivity_grid && opts.walk_forward_folds.is_some() {
        return Err(AegisError::Validation(
            "Cannot combine --sensitivity-grid with --walk-forward-folds in one run".to_string(),
        ));
    }
    if let (Some(start), Some(end)) = (opts.eval_start_ms, opts.eval_end_ms) {
        if start >= end {
            return Err(AegisError::Validation(format!(
                "Invalid evaluation window: --eval-start-ms ({start}) must be strictly less than --eval-end-ms ({end})"
            )));
        }
    }
    if let Some(folds) = opts.walk_forward_folds {
        if !(1..=50).contains(&folds) {
            return Err(AegisError::Validation(format!(
                "Invalid --walk-forward-folds '{folds}': must be within 1..=50"
            )));
        }
        if opts.train_days == 0 || opts.test_days == 0 {
            return Err(AegisError::Validation(
                "Invalid walk-forward window: --train-days and --test-days must be >= 1"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

fn apply_backtest_config_overrides(cfg: &mut ConfigMap, opts: &BacktestRunOptions) -> Result<()> {
    validate_backtest_run_options(opts)?;

    if !(1..=500).contains(&opts.warmup_bars) {
        return Err(AegisError::Validation(format!(
            "Invalid --warmup-bars '{}': must be within 1..=500",
            opts.warmup_bars
        )));
    }
    cfg.insert("backtest_warmup_bars", opts.warmup_bars.to_string());
    cfg.insert("signal_timeframe", opts.timeframe.as_str());

    if let Some(ref tier_raw) = opts.risk_tier {
        let normalized = tier_raw.trim().to_ascii_lowercase();
        if !matches!(
            normalized.as_str(),
            "conservative" | "standard" | "aggressive" | "extreme" | "custom"
        ) {
            return Err(AegisError::Validation(format!(
                "Invalid --risk-tier '{tier_raw}' (expected conservative, standard, aggressive, extreme, or custom)"
            )));
        }
        let tier = RiskTier::parse(&normalized);
        let custom_mult =
            (tier == RiskTier::Custom).then(|| cfg.get_f64("target_leverage_multiplier", 1.85));
        let mult = tier.leverage_multiplier(custom_mult);
        cfg.insert("risk_tier", normalized);
        cfg.insert("target_leverage_multiplier", format!("{mult:.2}"));
        if opts.risk_per_trade_pct.is_none() {
            cfg.insert(
                "risk_per_trade_equity_pct",
                format!("{:.4}", tier.default_risk_per_trade_equity_pct(Some(mult))),
            );
        }
        cfg.insert(
            "max_single_asset_weight",
            format!("{:.4}", tier.default_max_single_asset_weight(Some(mult))),
        );
    }
    if let Some(eq) = opts.equity {
        if !eq.is_finite() || eq < 100.0 {
            return Err(AegisError::Validation(format!(
                "Invalid --equity '{eq}': must be finite and >= 100.0 USDC"
            )));
        }
        cfg.insert("initial_paper_equity_usdc", format!("{eq}"));
    }
    if let Some(pg) = opts.profit_gate {
        cfg.insert(
            "profit_gate_signal_exits",
            if pg { "true" } else { "false" },
        );
    }
    if let Some(v) = opts.max_open_positions {
        if !(1..=100).contains(&v) {
            return Err(AegisError::Validation(format!(
                "Invalid --max-open-positions '{v}': must be within 1..=100"
            )));
        }
        cfg.insert("max_open_positions", v.to_string());
    }
    if let Some(v) = opts.min_entry_interval_sec {
        if !(0..=86_400).contains(&v) {
            return Err(AegisError::Validation(format!(
                "Invalid --min-entry-interval-sec '{v}': must be within 0..=86400"
            )));
        }
        cfg.insert("min_entry_interval_sec", v.to_string());
    }
    if let Some(v) = opts.stagnation_hours {
        if !v.is_finite() || !(0.0..=168.0).contains(&v) {
            return Err(AegisError::Validation(format!(
                "Invalid --stagnation-hours '{v}': must be finite and within 0.0..=168.0"
            )));
        }
        cfg.insert("stagnation_time_stop_hours", format!("{v}"));
    }
    if let Some(v) = opts.risk_per_trade_pct {
        if !v.is_finite() || !(0.0..=0.05).contains(&v) {
            return Err(AegisError::Validation(format!(
                "Invalid --risk-per-trade-pct '{v}': must be finite and within 0.0..=0.05"
            )));
        }
        cfg.insert("risk_per_trade_equity_pct", format!("{v}"));
    }
    if let Some(v) = opts.regime_budget_mult {
        cfg.insert(
            "enable_regime_budget_multipliers",
            if v { "true" } else { "false" },
        );
    }
    if let Some(v) = opts.exit_confirm_bars {
        if !(1..=5).contains(&v) {
            return Err(AegisError::Validation(format!(
                "Invalid --exit-confirm-bars '{v}': must be within 1..=5"
            )));
        }
        cfg.insert("signal_exit_confirm_bars", v.to_string());
    }
    if let Some(v) = opts.suppress_flips {
        cfg.insert("suppress_direct_flips", if v { "true" } else { "false" });
    }
    if let Some(v) = opts.net_exposure_cap {
        if !v.is_finite() || !(0.10..=1.0).contains(&v) {
            return Err(AegisError::Validation(format!(
                "Invalid --net-exposure-cap '{v}': must be finite and within 0.10..=1.0"
            )));
        }
        cfg.insert("max_net_exposure_ratio_of_gross", format!("{v}"));
    }
    if let Some(v) = opts.maker_first_entries {
        cfg.insert(
            "maker_first_low_urgency_entries",
            if v { "true" } else { "false" },
        );
    }
    if let Some(v) = opts.funding_timing {
        cfg.insert(
            "funding_entry_timing_enabled",
            if v { "true" } else { "false" },
        );
    }
    if let Some(v) = opts.max_entries_per_bar {
        if v > 100 {
            return Err(AegisError::Validation(format!(
                "Invalid --max-entries-per-bar '{v}': must be within 0..=100"
            )));
        }
        cfg.insert("max_entries_per_bar", v.to_string());
    }
    Ok(())
}

fn save_historical_dataset_json(path: &Path, dataset: &HistoricalDataset) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(AegisError::Io)?;
    }
    let json_text = serde_json::to_string(dataset)
        .map_err(|e| AegisError::Validation(format!("Failed serializing dataset JSON: {e}")))?;
    std::fs::write(path, json_text).map_err(AegisError::Io)
}

fn load_historical_dataset_json(path: &Path) -> Result<HistoricalDataset> {
    let raw = std::fs::read_to_string(path).map_err(AegisError::Io)?;
    serde_json::from_str::<HistoricalDataset>(&raw).map_err(|e| {
        AegisError::Validation(format!(
            "Failed parsing dataset JSON '{}': {e}",
            path.display()
        ))
    })
}

fn resolve_backtest_dataset(
    db_opt: Option<&DatabaseManager>,
    cfg: &ConfigMap,
    bt: &BacktestEngine,
    opts: &BacktestRunOptions,
) -> Result<HistoricalDataset> {
    let raw_dataset = if let Some(ref load_path) = opts.load_dataset {
        load_historical_dataset_json(load_path)?
    } else {
        match opts.source {
            BacktestSource::Synthetic => {
                let (assets, bars_by_asset) =
                    BacktestEngine::generate_canonical_dataset(opts.days, bt.config.warmup_bars);
                HistoricalDataset {
                    source: "synthetic".to_string(),
                    timeframe: "4h".to_string(),
                    eval_start_ms: None,
                    eval_end_ms: None,
                    assets,
                    bars_by_asset,
                }
            }
            BacktestSource::Db => {
                let db = db_opt.ok_or_else(|| {
                    AegisError::Validation(
                        "MariaDB connection required for --source db".to_string(),
                    )
                })?;
                let tf_str = opts.timeframe.as_str();
                let bar_ms = timeframe_to_ms(tf_str).ok_or_else(|| {
                    AegisError::Validation(format!("Unsupported timeframe '{tf_str}'"))
                })?;
                let until_ms = now_ms();
                let warmup_ms = (bt.config.warmup_bars as i64 + 12).saturating_mul(bar_ms);
                let window_ms = (opts.days.max(5) as i64).saturating_mul(86_400_000);
                let since_ms = until_ms.saturating_sub(window_ms.saturating_add(warmup_ms));

                let assets = db.list_exchange_assets()?;
                let bars_by_asset = db.get_ohlcv_bars_window(tf_str, since_ms, until_ms)?;
                let funding_events = db.list_funding_events_window(since_ms, until_ms)?;
                let min_bars = bt.config.warmup_bars.saturating_add(2);
                assemble_db_dataset(assets, bars_by_asset, funding_events, min_bars)?
            }
            BacktestSource::Mainnet => {
                let http = HyperliquidHttpClient::new(12);
                let fetch_opts = MainnetHistoryFetchOptions {
                    days: opts.days,
                    timeframe: opts.timeframe.as_str().to_string(),
                    warmup_bars: opts.warmup_bars,
                    max_assets: opts.max_assets,
                };
                let raw = fetch_mainnet_raw_history(&http, cfg, &fetch_opts)?;
                let mut dataset =
                    assemble_db_dataset(raw.assets, raw.bars_by_asset, raw.funding_events, 24)?;
                dataset.source = "mainnet".to_string();
                dataset.eval_start_ms = Some(raw.eval_start_ms);
                dataset
            }
        }
    };

    if let Some(ref save_path) = opts.save_dataset {
        save_historical_dataset_json(save_path, &raw_dataset)?;
    }

    if opts.eval_start_ms.is_some() || opts.eval_end_ms.is_some() {
        slice_dataset_window(&raw_dataset, opts.eval_start_ms, opts.eval_end_ms)
    } else {
        Ok(raw_dataset)
    }
}

fn run_backtest_command_with_options(
    db_opt: Option<&DatabaseManager>,
    opts: BacktestRunOptions,
) -> Result<BacktestCommandOutcome> {
    let mut cfg = if let Some(db) = db_opt {
        if opts.source == BacktestSource::Db && opts.load_dataset.is_none() {
            db.init_schema()?;
        }
        db.load_config_or_default()
    } else {
        ConfigMap::from_defaults()
    };

    apply_backtest_config_overrides(&mut cfg, &opts)?;

    let bt = BacktestEngine::from_config_map(&cfg, opts.days);
    let dataset = resolve_backtest_dataset(db_opt, &cfg, &bt, &opts)?;

    if opts.sensitivity_grid {
        return Ok(BacktestCommandOutcome::SensitivityGrid(
            bt.run_sensitivity_grid(&dataset)?,
        ));
    }
    if let Some(folds) = opts.walk_forward_folds {
        return Ok(BacktestCommandOutcome::WalkForward(
            bt.run_walk_forward_folds(&dataset, folds, opts.train_days, opts.test_days)?,
        ));
    }
    if opts.load_dataset.is_none()
        && opts.source == BacktestSource::Synthetic
        && opts.eval_start_ms.is_none()
        && opts.eval_end_ms.is_none()
    {
        return Ok(BacktestCommandOutcome::Single(Box::new(
            bt.run_default_simulation()?,
        )));
    }
    Ok(BacktestCommandOutcome::Single(Box::new(
        bt.run_dataset(&dataset)?,
    )))
}

#[cfg_attr(not(test), allow(dead_code))]
fn run_backtest_with_options(
    db_opt: Option<&DatabaseManager>,
    opts: BacktestRunOptions,
) -> Result<BacktestReport> {
    match run_backtest_command_with_options(db_opt, opts)? {
        BacktestCommandOutcome::Single(rep) => Ok(*rep),
        BacktestCommandOutcome::WalkForward(_) | BacktestCommandOutcome::SensitivityGrid(_) => Err(
            AegisError::Validation("Expected single BacktestReport outcome".to_string()),
        ),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn run_backtest_blocking(
    db: &DatabaseManager,
    days: usize,
    source: BacktestSource,
    timeframe: BacktestTimeframe,
) -> Result<BacktestReport> {
    run_backtest_with_options(
        Some(db),
        BacktestRunOptions {
            days,
            source,
            timeframe,
            max_assets: 30,
            warmup_bars: 24,
            risk_tier: None,
            equity: None,
            profit_gate: None,
            max_open_positions: None,
            min_entry_interval_sec: None,
            stagnation_hours: None,
            risk_per_trade_pct: None,
            save_dataset: None,
            load_dataset: None,
            eval_start_ms: None,
            eval_end_ms: None,
            walk_forward_folds: None,
            train_days: 14,
            test_days: 7,
            sensitivity_grid: false,
            regime_budget_mult: None,
            exit_confirm_bars: None,
            suppress_flips: None,
            net_exposure_cap: None,
            maker_first_entries: None,
            funding_timing: None,
            max_entries_per_bar: None,
        },
    )
}

fn print_db_outcome(outcome: &DbCommandOutcome, json_mode: bool) -> anyhow::Result<()> {
    if json_mode {
        let out = json!({
            "status": "ok",
            "command": "db",
            "read_only": outcome.read_only_status,
            "mariadb_version": outcome.health.mariadb_version,
            "database": outcome.health.database_name,
            "tables_count": outcome.health.tables_count,
            "views_healthy_count": outcome.health.views_healthy_count,
            "risk_tier": outcome.health.risk_tier,
            "target_leverage_multiplier": outcome.health.target_leverage_multiplier,
            "credentials_migrated": outcome.credentials_migrated,
            "legacy_secrets_rotated": outcome.legacy_secrets_rotated,
            "wrote_new_keyfile": outcome.wrote_new_keyfile,
            "web_api_token_provisioned": outcome.web_api_token_provisioned,
            "prune_report": outcome.prune_report,
            "backup": outcome.backup_report,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }

    if outcome.read_only_status {
        println!(
            "AEGIS QUANT v{} // MariaDB Status (read-only: {} | db={} | {} tables | {} views | tier={} {:.2}x)",
            PLATFORM_VERSION,
            outcome.health.mariadb_version,
            outcome.health.database_name,
            outcome.health.tables_count,
            outcome.health.views_healthy_count,
            outcome.health.risk_tier,
            outcome.health.target_leverage_multiplier
        );
        println!(
            "No mutations performed. Available modes: --init [--force], --migrate, --backup, --prune, --migrate-legacy-key, --generate-key"
        );
        return Ok(());
    }

    println!(
        "AEGIS QUANT v{} // MariaDB Maintenance Complete ({} | db={} | {} tables | {} views | tier={} {:.2}x)",
        PLATFORM_VERSION,
        outcome.health.mariadb_version,
        outcome.health.database_name,
        outcome.health.tables_count,
        outcome.health.views_healthy_count,
        outcome.health.risk_tier,
        outcome.health.target_leverage_multiplier
    );
    if let Some(migrated) = outcome.credentials_migrated {
        println!("Legacy SQLite credentials migrated: {migrated}");
    }
    if let Some(rotated) = outcome.legacy_secrets_rotated {
        println!(
            "Legacy Fernet secrets rotated to strict 32-byte key: {rotated} (keyfile_updated={})",
            outcome.wrote_new_keyfile
        );
    }
    if let Some(ref rep) = outcome.prune_report {
        println!(
            "Retention pruned : {} rows (snaps={}, tel={}, audit={}, cd={}, alerts={}, bars={}, funding={})",
            rep.total_deleted,
            rep.snapshots_deleted,
            rep.telemetry_deleted,
            rep.order_audit_deleted,
            rep.cooldowns_deleted,
            rep.alert_queue_deleted,
            rep.ohlcv_bars_deleted,
            rep.funding_events_deleted
        );
    }
    if let Some(ref rep) = outcome.backup_report {
        println!(
            "Backup verified  : {} ({} tables)",
            rep.backup_path.display(),
            rep.table_counts.len()
        );
    }
    if outcome.web_api_token_provisioned {
        if let Some(ref tok) = outcome.one_time_web_api_token {
            println!(
                "Auto-provisioned Fernet-encrypted `web_api_token` in MariaDB `system_config`."
            );
            println!(
                "One-time Web API bearer token (save securely; not logged in steady state): {tok}"
            );
        }
    }
    Ok(())
}

fn print_alert_outcome(outcome: &AlertCommandOutcome, json_mode: bool) -> anyhow::Result<()> {
    match outcome {
        AlertCommandOutcome::LiveTest(res) => {
            if json_mode {
                println!("{}", serde_json::to_string_pretty(res)?);
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
        AlertCommandOutcome::ReadOnlySummary {
            recent,
            status_counts,
        } => {
            if json_mode {
                let out = json!({
                    "status": "ok",
                    "mode": "read_only_summary",
                    "live_alert_sent": false,
                    "recent_count": recent.len(),
                    "status_counts": status_counts,
                    "recent_alerts": recent,
                    "hint": "Pass --test to dispatch a live formatted HTML test alert",
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!(
                    "AEGIS QUANT v{} // ALERT QUEUE SUMMARY (read-only | recent={} | counts={:?})",
                    PLATFORM_VERSION,
                    recent.len(),
                    status_counts
                );
                for item in recent {
                    println!(
                        "  #{} [{}] {} {} status={} attempts={}",
                        item.id,
                        item.provider,
                        item.severity,
                        item.event_type,
                        item.status,
                        item.attempts
                    );
                }
                println!("No live alert sent. Pass `--test` to dispatch a live card: `aegis-quant alert --test`");
            }
        }
    }
    Ok(())
}

fn spawn_retention_maintenance_task(
    db: DatabaseManager,
    telemetry: TelemetryBus,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = shutdown_rx.wait_for(|v| *v) => return,
            _ = tokio::time::sleep(Duration::from_secs(120)) => {}
        }

        loop {
            if *shutdown_rx.borrow() {
                break;
            }
            let db_pass = db.clone();
            let res = tokio::task::spawn_blocking(move || {
                let cfg = db_pass.load_config_or_default();
                let interval_sec = cfg
                    .get_i64("retention_prune_interval_sec", 3600)
                    .clamp(300, 86_400) as u64;
                let policy = RetentionPolicy::from_config(&cfg);
                let report = db_pass.prune_retention(&policy)?;
                Ok::<(u64, RetentionPruneReport), AegisError>((interval_sec, report))
            })
            .await;

            let next_interval_sec = match res {
                Ok(Ok((interval_sec, report))) => {
                    if report.total_deleted > 0 {
                        tracing::info!(
                            total_deleted = report.total_deleted,
                            snapshots = report.snapshots_deleted,
                            telemetry = report.telemetry_deleted,
                            order_audit = report.order_audit_deleted,
                            cooldowns = report.cooldowns_deleted,
                            alert_queue = report.alert_queue_deleted,
                            ohlcv_bars = report.ohlcv_bars_deleted,
                            funding_events = report.funding_events_deleted,
                            "Scheduled retention prune pass deleted expired rows"
                        );
                        telemetry.publish_event(
                            "INFO",
                            "aegis-cli.retention",
                            &format!(
                                "Scheduled retention prune deleted {} expired rows",
                                report.total_deleted
                            ),
                            serde_json::to_value(&report).ok(),
                        );
                    }
                    interval_sec
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Scheduled retention prune pass failed");
                    3600
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Scheduled retention prune task panicked/cancelled");
                    3600
                }
            };

            tokio::select! {
                biased;
                _ = shutdown_rx.wait_for(|v| *v) => break,
                _ = tokio::time::sleep(Duration::from_secs(next_interval_sec)) => {}
            }
        }
    })
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received SIGINT/Ctrl-C; shutting down Aegis Quant v2.1 daemon...");
            }
            _ = async {
                if let Some(ref mut s) = sigterm {
                    s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                tracing::info!("Received SIGTERM; shutting down Aegis Quant v2.1 daemon...");
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Received Ctrl-C; shutting down Aegis Quant v2.1 daemon...");
    }
}

async fn run_db_subcommand(cli: &Cli, opts: DbCommandOptions) -> anyhow::Result<()> {
    validate_db_command_flags(opts.init, opts.force)?;

    let anchor_clone = cli.db.clone();
    let json_mode = cli.json;
    let outcome = tokio::task::spawn_blocking(move || -> anyhow::Result<DbCommandOutcome> {
        let (db, legacy_rotated, wrote_keyfile) = if opts.migrate_legacy_key {
            let (db_migrated, cnt, wrote) =
                connect_and_migrate_legacy_key(anchor_clone.as_deref(), json_mode)?;
            (db_migrated, Some(cnt), wrote)
        } else {
            (
                DatabaseManager::connect(anchor_clone.as_deref(), None)?,
                None,
                false,
            )
        };

        let stdin = std::io::stdin();
        let is_tty = stdin.is_terminal();
        let mut locked_stdin = stdin.lock();
        let mut outcome = execute_db_command_with_io(
            &db,
            anchor_clone.as_deref(),
            &opts,
            is_tty,
            json_mode,
            &mut locked_stdin,
        )?;
        outcome.legacy_secrets_rotated = legacy_rotated;
        outcome.wrote_new_keyfile = wrote_keyfile;
        Ok(outcome)
    })
    .await??;

    print_db_outcome(&outcome, cli.json)
}

async fn run_daemon_subcommand(cli: &Cli, db: DatabaseManager) -> anyhow::Result<()> {
    let db_init = db.clone();
    let (telemetry, engine) =
        tokio::task::spawn_blocking(move || -> Result<(TelemetryBus, ExecutionEngine)> {
            db_init.init_schema()?;
            let telemetry = TelemetryBus::new(db_init.clone());
            let engine = ExecutionEngine::new(db_init);
            Ok((telemetry, engine))
        })
        .await??;

    if cli.once {
        let eng = engine.clone();
        let report = tokio::task::spawn_blocking(move || eng.run_single_cycle(true)).await??;
        telemetry.publish_event(
            "INFO",
            "aegis-cli.once",
            "Completed single v2.0 execution cycle",
            serde_json::to_value(&report).ok(),
        );
        let tel_flush = telemetry.clone();
        let _ = tokio::task::spawn_blocking(move || {
            tel_flush.shutdown_and_flush(Duration::from_secs(5))
        })
        .await;

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

    let db_count = db.clone();
    let (existing_count, suspends_startup_cutover) = tokio::task::spawn_blocking(move || {
        let count = db_count
            .list_exchange_assets()
            .map(|v| v.len())
            .unwrap_or(0);
        let suspends = db_count
            .load_config_or_default()
            .trading_run_mode()
            .suspends_background_loops();
        (count, suspends)
    })
    .await
    .unwrap_or((0, false));
    if existing_count == 0 && !suspends_startup_cutover {
        let eng = engine.clone();
        let _ = tokio::task::spawn_blocking(move || eng.run_cutover_reconciliation(false)).await;
    }

    let db_bind = db.clone();
    let host_arg = cli.host.clone();
    let allow_remote = cli.allow_remote_bind;
    let bind_host =
        tokio::task::spawn_blocking(move || validate_bind_host(&db_bind, &host_arg, allow_remote))
            .await??;

    let runtime_shutdown_tx = engine.spawn_background_runtime();
    let (web_shutdown_tx, web_shutdown_rx) = tokio::sync::watch::channel(false);
    let (maint_shutdown_tx, maint_shutdown_rx) = tokio::sync::watch::channel(false);
    let maint_task =
        spawn_retention_maintenance_task(db.clone(), telemetry.clone(), maint_shutdown_rx);

    telemetry.publish_event(
        "INFO",
        "aegis-cli.daemon",
        &format!(
            "Aegis Quant v2.1 daemon started (1s Priority / 3s Scanner | web=http://{}:{})",
            bind_host, cli.port
        ),
        None,
    );

    if cli.no_web {
        wait_for_shutdown_signal().await;
        let _ = runtime_shutdown_tx.send(true);
        let _ = web_shutdown_tx.send(true);
        let _ = maint_shutdown_tx.send(true);
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
        let _ = maint_shutdown_tx.send(true);

        if !web_already_exited {
            match tokio::time::timeout(Duration::from_secs(5), web_task).await {
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

    if tokio::time::timeout(Duration::from_secs(5), maint_task)
        .await
        .is_err()
    {
        tracing::warn!("Timed out waiting for retention maintenance task teardown");
    }

    // Wait for background 1s/3s runtime thread to finish its current tick and drop its receiver
    if tokio::time::timeout(Duration::from_secs(5), runtime_shutdown_tx.closed())
        .await
        .is_err()
    {
        tracing::warn!("Timed out waiting for background runtime thread teardown");
    }

    let tel_end = telemetry.clone();
    let _ = tokio::task::spawn_blocking(move || {
        let _ = tel_end.log_sync(
            "INFO",
            "aegis-cli.daemon",
            "Aegis Quant v2.1 daemon shut down cleanly",
            None,
        );
        tel_end.shutdown_and_flush(Duration::from_secs(5));
    })
    .await;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();

    if let Some(Commands::Db {
        init,
        force,
        generate_key: true,
        ..
    }) = cli.command
    {
        validate_db_command_flags(init, force)?;
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

    if let Some(Commands::Db {
        init,
        force,
        confirm_destructive_reset,
        migrate,
        backup,
        prune,
        migrate_legacy_key,
        generate_key: false,
    }) = cli.command
    {
        return run_db_subcommand(
            &cli,
            DbCommandOptions {
                init,
                force,
                confirm_destructive_reset,
                migrate,
                backup,
                prune,
                migrate_legacy_key,
            },
        )
        .await;
    }

    if let Some(Commands::Backtest {
        days,
        source,
        timeframe,
        max_assets,
        warmup_bars,
        risk_tier,
        equity,
        profit_gate,
        max_open_positions,
        min_entry_interval_sec,
        stagnation_hours,
        risk_per_trade_pct,
        save_dataset,
        load_dataset,
        eval_start_ms,
        eval_end_ms,
        walk_forward_folds,
        train_days,
        test_days,
        sensitivity_grid,
        regime_budget_mult,
        exit_confirm_bars,
        suppress_flips,
        net_exposure_cap,
        maker_first_entries,
        funding_timing,
        max_entries_per_bar,
    }) = cli.command.clone()
    {
        let anchor_clone = cli.db.clone();
        let outcome =
            tokio::task::spawn_blocking(move || -> anyhow::Result<BacktestCommandOutcome> {
                let requires_db = source == BacktestSource::Db && load_dataset.is_none();
                let has_db_url = std::env::var("AEGIS_DB_URL")
                    .ok()
                    .is_some_and(|v| !v.trim().is_empty());
                let db_opt = if requires_db || has_db_url {
                    Some(DatabaseManager::connect(anchor_clone.as_deref(), None)?)
                } else {
                    None
                };
                let outcome = run_backtest_command_with_options(
                    db_opt.as_ref(),
                    BacktestRunOptions {
                        days,
                        source,
                        timeframe,
                        max_assets,
                        warmup_bars,
                        risk_tier,
                        equity,
                        profit_gate,
                        max_open_positions,
                        min_entry_interval_sec,
                        stagnation_hours,
                        risk_per_trade_pct,
                        save_dataset,
                        load_dataset,
                        eval_start_ms,
                        eval_end_ms,
                        walk_forward_folds,
                        train_days,
                        test_days,
                        sensitivity_grid,
                        regime_budget_mult,
                        exit_confirm_bars,
                        suppress_flips,
                        net_exposure_cap,
                        maker_first_entries,
                        funding_timing,
                        max_entries_per_bar,
                    },
                )?;
                Ok(outcome)
            })
            .await??;

        match outcome {
            BacktestCommandOutcome::Single(report) => {
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    println!(
                        "AEGIS QUANT v{} // MULTI-SLEEVE WALK-FORWARD BACKTEST ({} days | tf={} | src={} | Tier={} {:.2}x)",
                        PLATFORM_VERSION,
                        report.days_evaluated,
                        report.timeframe,
                        source.as_str(),
                        report.risk_tier,
                        report.leverage_multiplier
                    );
                    println!(
                        "Equity  : ${:.2} -> ${:.2} USDC ({:+.2}%)",
                        report.initial_equity_usdc,
                        report.final_equity_usdc,
                        report.total_return_pct
                    );
                    println!(
                        "Metrics : Sharpe {:.2} | Sortino {:.2} | MaxDD {:.2}% | WinRate {:.1}% | PF {:.2} | Trades {}",
                        report.sharpe_ratio,
                        report.sortino_ratio,
                        report.max_drawdown_pct,
                        report.win_rate_pct,
                        report.profit_factor,
                        report.total_trades
                    );
                    println!(
                        "Book    : Long {} ({:.1}% WR, ${:+.2}) | Short {} ({:.1}% WR, ${:+.2})",
                        report.long_trades,
                        report.long_win_rate,
                        report.long_pnl_usdc,
                        report.short_trades,
                        report.short_win_rate,
                        report.short_pnl_usdc
                    );
                    println!(
                        "Sleeves : S1 Carry ${:+.2} | S2 Trend ${:+.2} | S3 Event ${:+.2}",
                        report.s1_attribution_usdc,
                        report.s2_attribution_usdc,
                        report.s3_attribution_usdc
                    );
                    println!(
                        "Costs   : Fees ${:.2} | Funding ${:+.2} | Stops {} | Stag {} | Assets {} | Bars {}",
                        report.total_fees_usdc,
                        report.total_funding_usdc,
                        report.stop_out_count,
                        report.stagnation_stop_count,
                        report.assets_evaluated,
                        report.bars_evaluated
                    );
                    if !report.regime_breakdown.is_empty() {
                        println!("Regimes :");
                        for (label, attr) in &report.regime_breakdown {
                            println!(
                                "  {:<20} bars={:<4} trades={:<3} WR={:>5.1}% Long=${:>+8.2} Short=${:>+8.2} Total=${:>+8.2}",
                                label,
                                attr.bars,
                                attr.trades,
                                attr.win_rate_pct,
                                attr.long_pnl_usdc,
                                attr.short_pnl_usdc,
                                attr.total_pnl_usdc
                            );
                        }
                    }
                    if !report.exit_reason_breakdown.is_empty() {
                        println!("Exits   :");
                        for (reason, summary) in &report.exit_reason_breakdown {
                            println!(
                                "  {:<26} count={:<4} wins={:<3} WR={:>5.1}% Net=${:>+8.2}",
                                reason,
                                summary.count,
                                summary.wins,
                                summary.win_rate_pct,
                                summary.net_pnl_usdc
                            );
                        }
                    }
                }
            }
            BacktestCommandOutcome::WalkForward(wf) => {
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&wf)?);
                } else {
                    println!(
                        "AEGIS QUANT v{} // ROLLING WALK-FORWARD ({} folds | train={}d | test={}d | src={})",
                        PLATFORM_VERSION,
                        wf.folds_requested,
                        wf.train_days,
                        wf.test_days,
                        source.as_str()
                    );
                    println!(
                        "OOS Agg : CompRet {:+.2}% | MeanRet {:+.2}% | MeanSharpe {:.2} | WorstDD {:.2}% | WinRate {:.1}% | PF {:.2} | Trades {}",
                        wf.oos_compounded_return_pct,
                        wf.oos_mean_return_pct,
                        wf.oos_mean_sharpe,
                        wf.oos_worst_drawdown_pct,
                        wf.oos_win_rate_pct,
                        wf.oos_profit_factor,
                        wf.oos_total_trades
                    );
                    println!(
                        "OOS Book: Long ${:+.2} | Short ${:+.2}",
                        wf.oos_long_pnl_usdc, wf.oos_short_pnl_usdc
                    );
                    for fold in &wf.folds {
                        println!(
                            "  Fold {:<2}: Train {:>+6.2}% (Sh {:>5.2}) | Test {:>+6.2}% (Sh {:>5.2}, DD {:>5.2}%, PF {:>4.2}, Trades {})",
                            fold.fold_index,
                            fold.train_report.total_return_pct,
                            fold.train_report.sharpe_ratio,
                            fold.test_report.total_return_pct,
                            fold.test_report.sharpe_ratio,
                            fold.test_report.max_drawdown_pct,
                            fold.test_report.profit_factor,
                            fold.test_report.total_trades
                        );
                    }
                }
            }
            BacktestCommandOutcome::SensitivityGrid(grid) => {
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&grid)?);
                } else {
                    println!(
                        "AEGIS QUANT v{} // 3x3 SENSITIVITY GRID (stagnation_hours x risk_per_trade_pct | src={})",
                        PLATFORM_VERSION,
                        source.as_str()
                    );
                    println!(
                        "  {:>7} | {:>7} | {:>8} | {:>6} | {:>7} | {:>6} | {:>5} | {:>6} | {:>5} | {:>9} | {:>9}",
                        "Stag(h)", "Risk(%)", "Ret(%)", "Sharpe", "MaxDD%", "PF", "WR%", "Trades", "Stag#", "Long$", "Short$"
                    );
                    for cell in &grid.cells {
                        println!(
                            "  {:>7.1} | {:>6.2}% | {:>+7.2}% | {:>6.2} | {:>6.2}% | {:>6.2} | {:>4.1}% | {:>6} | {:>5} | {:>+9.2} | {:>+9.2}",
                            cell.stagnation_hours,
                            cell.risk_per_trade_pct * 100.0,
                            cell.total_return_pct,
                            cell.sharpe_ratio,
                            cell.max_drawdown_pct,
                            cell.profit_factor,
                            cell.win_rate_pct,
                            cell.total_trades,
                            cell.stagnation_stop_count,
                            cell.long_pnl_usdc,
                            cell.short_pnl_usdc
                        );
                    }
                }
            }
        }
        return Ok(());
    }

    let anchor_clone = cli.db.clone();
    let db = tokio::task::spawn_blocking(move || {
        DatabaseManager::connect(anchor_clone.as_deref(), None)
    })
    .await??;

    match cli.command.clone().unwrap_or(Commands::Run) {
        Commands::Db { .. } | Commands::Backtest { .. } => Ok(()),

        Commands::Cutover { close_legacy } => {
            let db_clone = db.clone();
            let report = tokio::task::spawn_blocking(move || -> Result<_> {
                db_clone.init_schema()?;
                let engine = ExecutionEngine::new(db_clone);
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
            Ok(())
        }

        Commands::Alert { test } => {
            let db_clone = db.clone();
            let outcome =
                tokio::task::spawn_blocking(move || execute_alert_command(&db_clone, test))
                    .await??;
            print_alert_outcome(&outcome, cli.json)
        }

        Commands::Status => {
            let db_clone = db.clone();
            let (health, dash) = tokio::task::spawn_blocking(move || -> Result<_> {
                let health = build_health_payload(&db_clone)?;
                let dash = build_dashboard_payload(&db_clone)?;
                Ok((health, dash))
            })
            .await??;

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
                    "AEGIS QUANT v{} // STATUS: {} ({} | Mode={} | RunMode={} | Tier={} {:.2}x)",
                    PLATFORM_VERSION,
                    health.status.to_uppercase(),
                    health.mariadb_version,
                    health.execution_mode,
                    health.trading_run_mode,
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
            Ok(())
        }

        Commands::Run => run_daemon_subcommand(&cli, db).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_db::is_encrypted_value;
    use std::io::Cursor;

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

        let cli_db = Cli::try_parse_from([
            "aegis-quant",
            "db",
            "--init",
            "--migrate",
            "--prune",
            "--migrate-legacy-key",
            "--confirm-destructive-reset",
        ])
        .unwrap();
        assert!(matches!(
            cli_db.command,
            Some(Commands::Db {
                init: true,
                migrate: true,
                prune: true,
                migrate_legacy_key: true,
                confirm_destructive_reset: true,
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

        let cli_bt_default =
            Cli::try_parse_from(["aegis-quant", "backtest", "--days", "45"]).unwrap();
        assert!(matches!(
            cli_bt_default.command,
            Some(Commands::Backtest {
                days: 45,
                source: BacktestSource::Synthetic,
                timeframe: BacktestTimeframe::H4,
                max_assets: 30,
                warmup_bars: 72,
                risk_tier: None,
                equity: None,
                profit_gate: None,
                max_open_positions: None,
                min_entry_interval_sec: None,
                stagnation_hours: None,
                risk_per_trade_pct: None,
                save_dataset: None,
                load_dataset: None,
                eval_start_ms: None,
                eval_end_ms: None,
                walk_forward_folds: None,
                train_days: 14,
                test_days: 7,
                sensitivity_grid: false,
                regime_budget_mult: None,
                exit_confirm_bars: None,
                suppress_flips: None,
                net_exposure_cap: None,
                maker_first_entries: None,
                funding_timing: None,
                max_entries_per_bar: None,
            })
        ));

        let cli_bt_db = Cli::try_parse_from([
            "aegis-quant",
            "backtest",
            "--source",
            "db",
            "--timeframe",
            "1h",
        ])
        .unwrap();
        assert!(matches!(
            cli_bt_db.command,
            Some(Commands::Backtest {
                days: 30,
                source: BacktestSource::Db,
                timeframe: BacktestTimeframe::H1,
                ..
            })
        ));

        let cli_bt_mainnet = Cli::try_parse_from([
            "aegis-quant",
            "backtest",
            "--source",
            "mainnet",
            "--days",
            "7",
            "--timeframe",
            "4h",
            "--max-assets",
            "42",
            "--warmup-bars",
            "72",
            "--risk-tier",
            "standard",
            "--equity",
            "25000",
            "--profit-gate",
            "true",
            "--max-open-positions",
            "10",
            "--min-entry-interval-sec",
            "0",
            "--stagnation-hours",
            "12.5",
            "--risk-per-trade-pct",
            "0.015",
            "--save-dataset",
            "/tmp/ds.json",
            "--load-dataset",
            "/tmp/ds_in.json",
            "--eval-start-ms",
            "1700000000000",
            "--eval-end-ms",
            "1700604800000",
            "--walk-forward-folds",
            "3",
            "--train-days",
            "21",
            "--test-days",
            "5",
            "--regime-budget-mult",
            "--exit-confirm-bars",
            "2",
            "--suppress-flips",
            "true",
            "--net-exposure-cap",
            "0.60",
            "--maker-first-entries",
            "true",
            "--funding-timing",
            "--max-entries-per-bar",
            "4",
        ])
        .unwrap();
        match cli_bt_mainnet.command {
            Some(Commands::Backtest {
                days,
                source,
                timeframe,
                max_assets,
                warmup_bars,
                risk_tier,
                equity,
                profit_gate,
                max_open_positions,
                min_entry_interval_sec,
                stagnation_hours,
                risk_per_trade_pct,
                save_dataset,
                load_dataset,
                eval_start_ms,
                eval_end_ms,
                walk_forward_folds,
                train_days,
                test_days,
                sensitivity_grid,
                regime_budget_mult,
                exit_confirm_bars,
                suppress_flips,
                net_exposure_cap,
                maker_first_entries,
                funding_timing,
                max_entries_per_bar,
            }) => {
                assert_eq!(days, 7);
                assert_eq!(source, BacktestSource::Mainnet);
                assert_eq!(timeframe, BacktestTimeframe::H4);
                assert_eq!(max_assets, 42);
                assert_eq!(warmup_bars, 72);
                assert_eq!(risk_tier.as_deref(), Some("standard"));
                assert_eq!(equity, Some(25_000.0));
                assert_eq!(profit_gate, Some(true));
                assert_eq!(max_open_positions, Some(10));
                assert_eq!(min_entry_interval_sec, Some(0));
                assert_eq!(stagnation_hours, Some(12.5));
                assert_eq!(risk_per_trade_pct, Some(0.015));
                assert_eq!(save_dataset, Some(PathBuf::from("/tmp/ds.json")));
                assert_eq!(load_dataset, Some(PathBuf::from("/tmp/ds_in.json")));
                assert_eq!(eval_start_ms, Some(1_700_000_000_000));
                assert_eq!(eval_end_ms, Some(1_700_604_800_000));
                assert_eq!(walk_forward_folds, Some(3));
                assert_eq!(train_days, 21);
                assert_eq!(test_days, 5);
                assert!(!sensitivity_grid);
                assert_eq!(regime_budget_mult, Some(true));
                assert_eq!(exit_confirm_bars, Some(2));
                assert_eq!(suppress_flips, Some(true));
                assert_eq!(net_exposure_cap, Some(0.60));
                assert_eq!(maker_first_entries, Some(true));
                assert_eq!(funding_timing, Some(true));
                assert_eq!(max_entries_per_bar, Some(4));
            }
            _ => panic!("expected Backtest command"),
        }

        let cli_bt_grid = Cli::try_parse_from([
            "aegis-quant",
            "backtest",
            "--sensitivity-grid",
            "--regime-budget-mult",
            "false",
        ])
        .unwrap();
        assert!(matches!(
            cli_bt_grid.command,
            Some(Commands::Backtest {
                sensitivity_grid: true,
                regime_budget_mult: Some(false),
                ..
            })
        ));

        assert!(Cli::try_parse_from([
            "aegis-quant",
            "backtest",
            "--source",
            "db",
            "--timeframe",
            "5m",
        ])
        .is_err());

        let cli_status = Cli::try_parse_from(["aegis-quant", "status", "--json"]).unwrap();
        assert!(cli_status.json);
        assert!(matches!(cli_status.command, Some(Commands::Status)));
    }

    #[test]
    fn test_backtest_cli_overrides_validation_and_config_propagation() {
        let base_opts = || BacktestRunOptions {
            days: 14,
            source: BacktestSource::Synthetic,
            timeframe: BacktestTimeframe::H4,
            max_assets: 42,
            warmup_bars: 72,
            risk_tier: None,
            equity: None,
            profit_gate: None,
            max_open_positions: None,
            min_entry_interval_sec: None,
            stagnation_hours: None,
            risk_per_trade_pct: None,
            save_dataset: None,
            load_dataset: None,
            eval_start_ms: None,
            eval_end_ms: None,
            walk_forward_folds: None,
            train_days: 14,
            test_days: 7,
            sensitivity_grid: false,
            regime_budget_mult: None,
            exit_confirm_bars: None,
            suppress_flips: None,
            net_exposure_cap: None,
            maker_first_entries: None,
            funding_timing: None,
            max_entries_per_bar: None,
        };

        let mut cfg = ConfigMap::from_defaults();
        let mut valid_opts = base_opts();
        valid_opts.warmup_bars = 480;
        valid_opts.max_open_positions = Some(10);
        valid_opts.min_entry_interval_sec = Some(0);
        valid_opts.stagnation_hours = Some(12.5);
        valid_opts.risk_per_trade_pct = Some(0.015);
        valid_opts.regime_budget_mult = Some(true);
        valid_opts.exit_confirm_bars = Some(3);
        valid_opts.suppress_flips = Some(true);
        valid_opts.net_exposure_cap = Some(0.55);
        valid_opts.maker_first_entries = Some(true);
        valid_opts.funding_timing = Some(true);
        valid_opts.max_entries_per_bar = Some(2);
        apply_backtest_config_overrides(&mut cfg, &valid_opts).unwrap();
        assert_eq!(cfg.get_i64("backtest_warmup_bars", 0), 480);
        assert_eq!(cfg.get_usize("max_open_positions", 0), 10);
        assert_eq!(cfg.get_i64("min_entry_interval_sec", -1), 0);
        assert!((cfg.get_f64("stagnation_time_stop_hours", -1.0) - 12.5).abs() < 1e-9);
        assert!((cfg.get_f64("risk_per_trade_equity_pct", -1.0) - 0.015).abs() < 1e-9);
        assert!(cfg.get_bool("enable_regime_budget_multipliers", false));
        assert_eq!(cfg.get_i64("signal_exit_confirm_bars", 0), 3);
        assert!(cfg.get_bool("suppress_direct_flips", false));
        assert!((cfg.get_f64("max_net_exposure_ratio_of_gross", -1.0) - 0.55).abs() < 1e-9);
        assert!(cfg.get_bool("maker_first_low_urgency_entries", false));
        assert!(cfg.get_bool("funding_entry_timing_enabled", false));
        assert_eq!(cfg.get_usize("max_entries_per_bar", 0), 2);

        let bt = BacktestEngine::from_config_map(&cfg, 14);
        assert_eq!(bt.config.warmup_bars, 480);
        assert_eq!(bt.config.max_open_positions, 10);
        assert_eq!(bt.config.min_entry_interval_sec, 0);
        assert!((bt.config.risk_limits.stagnation_time_stop_hours - 12.5).abs() < 1e-9);
        assert!((bt.config.risk_limits.risk_per_trade_equity_pct - 0.015).abs() < 1e-9);
        assert!(bt.config.maker_first_low_urgency_entries);
        assert!(bt.config.funding_entry_timing_enabled);
        assert_eq!(bt.config.max_entries_per_bar, 2);

        // Tier-sync verification: Conservative -> 0.5% risk_per_trade & 0.15 max_single_asset_weight;
        // Standard -> 1.0% & 0.15; Aggressive -> 1.5% & 0.20; Extreme -> 2.0% & 0.25;
        // Explicit --risk-per-trade-pct must NOT be overwritten by --risk-tier sync.
        for (tier_name, exp_risk, exp_max_w) in [
            ("conservative", 0.005, 0.15),
            ("standard", 0.010, 0.15),
            ("aggressive", 0.015, 0.20),
            ("extreme", 0.020, 0.25),
        ] {
            let mut tc = ConfigMap::from_defaults();
            let topts = BacktestRunOptions {
                risk_tier: Some(tier_name.to_string()),
                ..base_opts()
            };
            apply_backtest_config_overrides(&mut tc, &topts).unwrap();
            assert!(
                (tc.get_f64("risk_per_trade_equity_pct", -1.0) - exp_risk).abs() < 1e-9,
                "tier {tier_name} risk sync mismatch"
            );
            assert!(
                (tc.get_f64("max_single_asset_weight", -1.0) - exp_max_w).abs() < 1e-9,
                "tier {tier_name} max_single_asset_weight sync mismatch"
            );
        }
        let mut override_tc = ConfigMap::from_defaults();
        let override_opts = BacktestRunOptions {
            risk_tier: Some("conservative".to_string()),
            risk_per_trade_pct: Some(0.018),
            ..base_opts()
        };
        apply_backtest_config_overrides(&mut override_tc, &override_opts).unwrap();
        assert!((override_tc.get_f64("risk_per_trade_equity_pct", -1.0) - 0.018).abs() < 1e-9);
        assert!((override_tc.get_f64("max_single_asset_weight", -1.0) - 0.15).abs() < 1e-9);

        // Boundary valid checks
        for (opts, exp_warmup, exp_stag) in [
            (
                BacktestRunOptions {
                    warmup_bars: 1,
                    max_open_positions: Some(1),
                    min_entry_interval_sec: Some(0),
                    stagnation_hours: Some(0.0),
                    risk_per_trade_pct: Some(0.0),
                    exit_confirm_bars: Some(1),
                    net_exposure_cap: Some(0.10),
                    max_entries_per_bar: Some(0),
                    ..base_opts()
                },
                1,
                0.0,
            ),
            (
                BacktestRunOptions {
                    warmup_bars: 500,
                    max_open_positions: Some(100),
                    min_entry_interval_sec: Some(86_400),
                    stagnation_hours: Some(168.0),
                    risk_per_trade_pct: Some(0.05),
                    exit_confirm_bars: Some(5),
                    net_exposure_cap: Some(1.0),
                    max_entries_per_bar: Some(100),
                    ..base_opts()
                },
                500,
                168.0,
            ),
        ] {
            let mut c = ConfigMap::from_defaults();
            assert!(apply_backtest_config_overrides(&mut c, &opts).is_ok());
            let bt_bound = BacktestEngine::from_config_map(&c, 14);
            assert_eq!(bt_bound.config.warmup_bars, exp_warmup);
            assert!(
                (bt_bound.config.risk_limits.stagnation_time_stop_hours - exp_stag).abs() < 1e-9
            );
        }

        // Out-of-range / non-finite / incompatible validation errors
        let invalid_cases = [
            BacktestRunOptions {
                warmup_bars: 0,
                ..base_opts()
            },
            BacktestRunOptions {
                warmup_bars: 501,
                ..base_opts()
            },
            BacktestRunOptions {
                max_open_positions: Some(0),
                ..base_opts()
            },
            BacktestRunOptions {
                max_open_positions: Some(101),
                ..base_opts()
            },
            BacktestRunOptions {
                min_entry_interval_sec: Some(-1),
                ..base_opts()
            },
            BacktestRunOptions {
                min_entry_interval_sec: Some(86_401),
                ..base_opts()
            },
            BacktestRunOptions {
                stagnation_hours: Some(-0.1),
                ..base_opts()
            },
            BacktestRunOptions {
                stagnation_hours: Some(168.1),
                ..base_opts()
            },
            BacktestRunOptions {
                stagnation_hours: Some(f64::NAN),
                ..base_opts()
            },
            BacktestRunOptions {
                stagnation_hours: Some(f64::INFINITY),
                ..base_opts()
            },
            BacktestRunOptions {
                risk_per_trade_pct: Some(-0.001),
                ..base_opts()
            },
            BacktestRunOptions {
                risk_per_trade_pct: Some(0.051),
                ..base_opts()
            },
            BacktestRunOptions {
                risk_per_trade_pct: Some(f64::NAN),
                ..base_opts()
            },
            BacktestRunOptions {
                risk_per_trade_pct: Some(f64::NEG_INFINITY),
                ..base_opts()
            },
            BacktestRunOptions {
                exit_confirm_bars: Some(0),
                ..base_opts()
            },
            BacktestRunOptions {
                exit_confirm_bars: Some(6),
                ..base_opts()
            },
            BacktestRunOptions {
                net_exposure_cap: Some(0.09),
                ..base_opts()
            },
            BacktestRunOptions {
                net_exposure_cap: Some(1.01),
                ..base_opts()
            },
            BacktestRunOptions {
                net_exposure_cap: Some(f64::NAN),
                ..base_opts()
            },
            BacktestRunOptions {
                max_entries_per_bar: Some(101),
                ..base_opts()
            },
            BacktestRunOptions {
                eval_start_ms: Some(2_000),
                eval_end_ms: Some(2_000),
                ..base_opts()
            },
            BacktestRunOptions {
                sensitivity_grid: true,
                walk_forward_folds: Some(2),
                ..base_opts()
            },
            BacktestRunOptions {
                walk_forward_folds: Some(0),
                ..base_opts()
            },
        ];
        for opts in invalid_cases {
            let mut c = ConfigMap::from_defaults();
            assert!(matches!(
                apply_backtest_config_overrides(&mut c, &opts),
                Err(AegisError::Validation(_))
            ));
        }

        // End-to-end execution via CLI runner: sensitivity grid + walk-forward folds
        let grid_out = run_backtest_command_with_options(
            None,
            BacktestRunOptions {
                days: 10,
                warmup_bars: 24,
                sensitivity_grid: true,
                ..base_opts()
            },
        )
        .unwrap();
        assert!(matches!(
            grid_out,
            BacktestCommandOutcome::SensitivityGrid(ref g) if g.cells.len() == 9
        ));

        let wf_out = run_backtest_command_with_options(
            None,
            BacktestRunOptions {
                days: 28,
                warmup_bars: 24,
                walk_forward_folds: Some(2),
                train_days: 14,
                test_days: 7,
                ..base_opts()
            },
        )
        .unwrap();
        assert!(matches!(
            wf_out,
            BacktestCommandOutcome::WalkForward(ref w) if w.folds.len() == 2
        ));
    }

    #[test]
    fn test_validate_bind_host_loopback_and_remote() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        assert_eq!(
            validate_bind_host(&db, "127.0.0.1", false).unwrap(),
            "127.0.0.1"
        );
        assert!(validate_bind_host(&db, "0.0.0.0", false).is_err());
        assert_eq!(validate_bind_host(&db, "0.0.0.0", true).unwrap(), "0.0.0.0");
    }

    #[test]
    fn test_r14_h6_db_force_guards_reject_without_init_and_non_tty() {
        // 1. db --force without --init rejected immediately
        let parsed_force_only = Cli::try_parse_from(["aegis-quant", "db", "--force"]).unwrap();
        assert!(matches!(
            parsed_force_only.command,
            Some(Commands::Db {
                init: false,
                force: true,
                ..
            })
        ));
        assert!(matches!(
            validate_db_command_flags(false, true),
            Err(AegisError::Validation(_))
        ));

        // 2. Non-TTY db --init --force on non-ephemeral DB rejected without --confirm-destructive-reset
        let mut empty_in = Cursor::new(b"" as &[u8]);
        assert!(matches!(
            authorize_destructive_reset_with_io(
                "aegis_quant",
                false,
                false,
                false,
                false,
                &mut empty_in,
            ),
            Err(AegisError::SecurityViolation(_))
        ));

        // 3. TTY + --json rejected without --confirm-destructive-reset
        let mut drop_in = Cursor::new(b"DROP aegis_quant\n" as &[u8]);
        assert!(matches!(
            authorize_destructive_reset_with_io(
                "aegis_quant",
                false,
                false,
                true,
                true,
                &mut drop_in,
            ),
            Err(AegisError::SecurityViolation(_))
        ));

        // 4. TTY with mismatched input rejected
        let mut wrong_in = Cursor::new(b"DROP wrong_db\n" as &[u8]);
        assert!(matches!(
            authorize_destructive_reset_with_io(
                "aegis_quant",
                false,
                false,
                true,
                false,
                &mut wrong_in,
            ),
            Err(AegisError::SecurityViolation(_))
        ));

        // 5. Exact TTY confirmation or --confirm-destructive-reset succeeds
        let mut exact_in = Cursor::new(b"DROP aegis_quant\n" as &[u8]);
        assert!(authorize_destructive_reset_with_io(
            "aegis_quant",
            false,
            false,
            true,
            false,
            &mut exact_in,
        )
        .is_ok());
        assert!(authorize_destructive_reset_with_io(
            "aegis_quant",
            false,
            true,
            false,
            false,
            &mut empty_in,
        )
        .is_ok());
    }

    #[test]
    fn test_r14_l19_db_init_provisions_encrypted_web_api_token_and_bare_db_is_read_only() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();

        // Bare db must be strictly read-only and never provision or overwrite web_api_token
        let mut empty_in = Cursor::new(b"" as &[u8]);
        let bare_out = execute_db_command_with_io(
            &db,
            None,
            &DbCommandOptions {
                init: false,
                force: false,
                confirm_destructive_reset: false,
                migrate: false,
                backup: false,
                prune: false,
                migrate_legacy_key: false,
            },
            false,
            false,
            &mut empty_in,
        )
        .unwrap();
        assert!(bare_out.read_only_status);
        assert!(!bare_out.web_api_token_provisioned);
        assert_eq!(db.load_config().unwrap().get_str("web_api_token", ""), "");

        // First db --init provisions random 32-byte Fernet-encrypted web_api_token
        let init_out = execute_db_command_with_io(
            &db,
            None,
            &DbCommandOptions {
                init: true,
                force: false,
                confirm_destructive_reset: false,
                migrate: false,
                backup: false,
                prune: false,
                migrate_legacy_key: false,
            },
            false,
            false,
            &mut empty_in,
        )
        .unwrap();
        assert!(init_out.web_api_token_provisioned);
        let provisioned_tok = init_out.one_time_web_api_token.clone().unwrap();
        assert!(provisioned_tok.len() >= 32);

        let raw_row = db
            .list_system_config_rows()
            .unwrap()
            .into_iter()
            .find(|r| r.config_key == "web_api_token")
            .unwrap();
        assert!(is_encrypted_value(&raw_row.config_value));
        assert_eq!(
            db.load_config().unwrap().get_str("web_api_token", ""),
            provisioned_tok
        );

        // Second db --init preserves existing web_api_token without rotating or printing it
        let second_init = execute_db_command_with_io(
            &db,
            None,
            &DbCommandOptions {
                init: true,
                force: false,
                confirm_destructive_reset: false,
                migrate: false,
                backup: false,
                prune: false,
                migrate_legacy_key: false,
            },
            false,
            false,
            &mut empty_in,
        )
        .unwrap();
        assert!(!second_init.web_api_token_provisioned);
        assert!(second_init.one_time_web_api_token.is_none());
        assert_eq!(
            db.load_config().unwrap().get_str("web_api_token", ""),
            provisioned_tok
        );
    }

    #[test]
    fn test_r14_l19_alert_without_test_is_read_only() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let outcome_empty = execute_alert_command(&db, false).unwrap();
        assert!(matches!(
            outcome_empty,
            AlertCommandOutcome::ReadOnlySummary { ref recent, .. } if recent.is_empty()
        ));
        assert!(db.list_recent_alerts(10).unwrap().is_empty());

        db.insert_alert_queue_record(
            "telegram",
            "SYSTEM_TEST",
            "INFO",
            "Stored notice",
            None,
            "DELIVERED",
        )
        .unwrap();

        let outcome_existing = execute_alert_command(&db, false).unwrap();
        match outcome_existing {
            AlertCommandOutcome::ReadOnlySummary {
                recent,
                status_counts,
            } => {
                assert_eq!(recent.len(), 1);
                assert_eq!(status_counts.get("DELIVERED").copied(), Some(1));
            }
            AlertCommandOutcome::LiveTest(_) => panic!("alert without --test must be read-only"),
        }
        assert_eq!(db.list_recent_alerts(10).unwrap().len(), 1);
    }

    #[test]
    fn test_r14_m15_backtest_source_db_loads_persisted_bars_and_fails_loudly_when_empty() {
        use aegis_db::{AssetUpsertInput, BarUpsertInput, FundingEventUpsertInput};

        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let empty_err =
            run_backtest_blocking(&db, 10, BacktestSource::Db, BacktestTimeframe::H1).unwrap_err();
        assert!(matches!(empty_err, AegisError::Validation(_)));

        let btc_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "BTC".to_string(),
                asset_index: 0,
                sz_decimals: 4,
                tick_size: 1.0,
                lot_step_size: 0.001,
                max_leverage: 40.0,
                mark_price: 92_000.0,
                mid_price: 92_000.0,
                oracle_price: 92_000.0,
                funding_rate_hourly: 0.00001,
                day_volume_usdc: 50_000_000.0,
                open_interest_usdc: 20_000_000.0,
                prev_day_price: 91_000.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: false,
            })
            .unwrap();
        let eth_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "ETH".to_string(),
                asset_index: 1,
                sz_decimals: 3,
                tick_size: 0.1,
                lot_step_size: 0.01,
                max_leverage: 25.0,
                mark_price: 3_200.0,
                mid_price: 3_200.0,
                oracle_price: 3_200.0,
                funding_rate_hourly: 0.00001,
                day_volume_usdc: 35_000_000.0,
                open_interest_usdc: 15_000_000.0,
                prev_day_price: 3_150.0,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: false,
            })
            .unwrap();

        let now = now_ms();
        let step_1h = 3_600_000i64;
        let start = now - 48 * step_1h;
        let mut bars = Vec::new();
        for i in 0..45i64 {
            let ts = start + i * step_1h;
            let btc_px = 90_000.0 + (i as f64) * 120.0;
            let eth_px = 3_000.0 + (i as f64) * 8.0;
            bars.push(BarUpsertInput {
                asset_id: btc_id,
                timeframe: "1h".to_string(),
                timestamp_ms: ts,
                open: btc_px,
                high: btc_px * 1.005,
                low: btc_px * 0.995,
                close: btc_px * 1.002,
                volume: 1_500_000.0,
                funding_rate: 0.0,
            });
            bars.push(BarUpsertInput {
                asset_id: eth_id,
                timeframe: "1h".to_string(),
                timestamp_ms: ts,
                open: eth_px,
                high: eth_px * 1.006,
                low: eth_px * 0.994,
                close: eth_px * 1.003,
                volume: 900_000.0,
                funding_rate: 0.0,
            });
        }
        db.upsert_ohlcv_bars(&bars).unwrap();
        db.upsert_funding_events_batch(&[FundingEventUpsertInput {
            asset_id: btc_id,
            coin: "BTC".to_string(),
            event_time_ms: start + 30 * step_1h + 600_000,
            usdc: -1.5,
            remaining_usdc: 0.0,
            position_szi: 0.1,
            funding_rate: 0.00002,
            dedup_key: "cli_bt_test_funding_1".to_string(),
        }])
        .unwrap();

        let report =
            run_backtest_blocking(&db, 10, BacktestSource::Db, BacktestTimeframe::H1).unwrap();
        assert_eq!(report.timeframe, "1h");
        assert!(report.bars_evaluated >= 15);
        assert!(report.days_evaluated >= 1);
    }

    #[test]
    fn test_r14_l9_resolve_legacy_passphrase_key_from_keyfile_and_rotate() {
        let legacy_pass = "legacy-non-base64-passphrase-for-test";
        let legacy_cipher = FernetCipher::from_legacy_sha256_passphrase(legacy_pass).unwrap();
        let old_b64 = legacy_cipher.key_b64().to_string();

        let tmp_dir = std::env::temp_dir().join(format!("aegis_cli_leg_{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let anchor = tmp_dir.join("aegis_quant.db");
        let keyfile = master_key_file_for_db(&anchor);
        std::fs::write(&keyfile, format!("{legacy_pass}\n")).unwrap();

        let mut empty_in = Cursor::new(b"" as &[u8]);
        let resolved_b64 =
            resolve_legacy_passphrase_key_b64(&anchor, false, false, &mut empty_in).unwrap();
        assert_eq!(resolved_b64, old_b64);

        let db = DatabaseManager::new_isolated_test(Some(&old_b64)).unwrap();
        db.set_config_value("web_api_token", "super-secret-token-xyz", None)
            .unwrap();
        let new_b64 = generate_fernet_key();
        let rotated = db.rotate_fernet_master_key(&old_b64, &new_b64).unwrap();
        assert!(rotated >= 1);
        assert_eq!(
            db.load_config().unwrap().get_str("web_api_token", ""),
            "super-secret-token-xyz"
        );
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
