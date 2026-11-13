use aegis_core::PLATFORM_VERSION;
use aegis_db::DatabaseManager;
use aegis_settle::{SettleController, SettleOverrideInput};
use clap::Parser;
use serde_json::json;

#[derive(Parser, Debug)]
#[command(
    name = "aegis-settle-tool",
    version = PLATFORM_VERSION,
    about = "AEGIS PP-50 Daily 00:05 UTC Two-Book Settling CLI Tool"
)]
struct SettleToolArgs {
    /// Run the 5-phase settling protocol in dry-run simulation mode
    #[arg(long)]
    dry_run: bool,

    /// Optional initial book equity split in `<PIT_USDC>:<POND_USDC>` format (e.g. `5000:5000`)
    #[arg(long)]
    initial_split: Option<String>,

    /// Optional unlocked idle USDC on the rich book
    #[arg(long)]
    rich_idle_usdc: Option<f64>,

    /// Day index for the formatted PP-50 daily report line (default: 1)
    #[arg(long, default_value_t = 1)]
    day_index: u32,

    /// Optional SQLite database path (defaults to data/aegis.db)
    #[arg(long)]
    db: Option<String>,
}

fn parse_split(s: &str) -> aegis_core::Result<(f64, f64)> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(aegis_core::AegisError::Validation(format!(
            "Invalid --initial-split '{s}': expected '<PIT_USDC>:<POND_USDC>' (e.g. 5000:5000)"
        )));
    }
    let pit = parts[0].trim().parse::<f64>().map_err(|e| {
        aegis_core::AegisError::Validation(format!("Invalid PIT equity in --initial-split: {e}"))
    })?;
    let pond = parts[1].trim().parse::<f64>().map_err(|e| {
        aegis_core::AegisError::Validation(format!("Invalid POND equity in --initial-split: {e}"))
    })?;
    if !pit.is_finite() || !pond.is_finite() || pit < 0.0 || pond < 0.0 || (pit + pond) <= 0.0 {
        return Err(aegis_core::AegisError::Validation(format!(
            "Invalid --initial-split '{s}': values must be finite non-negative numbers with positive sum"
        )));
    }
    Ok((pit, pond))
}

fn main() -> aegis_core::Result<()> {
    let args = SettleToolArgs::parse();
    if !args.dry_run && (args.initial_split.is_some() || args.rich_idle_usdc.is_some()) {
        return Err(aegis_core::AegisError::Validation(
            "Manual equity/idle overrides (--initial-split / --rich-idle-usdc) are only permitted with --dry-run".to_string(),
        ));
    }
    if let Some(idle) = args.rich_idle_usdc {
        if !idle.is_finite() || idle < 0.0 {
            return Err(aegis_core::AegisError::Validation(format!(
                "Invalid --rich-idle-usdc '{idle}': must be finite and >= 0.0"
            )));
        }
    }
    let db = match args.db.as_deref() {
        Some(path) => DatabaseManager::new(path),
        None => DatabaseManager::default_db(),
    };
    db.initialize_schema(false)?;

    let (pit_override, pond_override) = match args.initial_split.as_deref() {
        Some(raw) => {
            let (p1, p2) = parse_split(raw)?;
            (Some(p1), Some(p2))
        }
        None => (None, None),
    };

    let ctrl = SettleController::new(db, None);
    let res = ctrl.run_settling(SettleOverrideInput {
        pit_equity_usdc: pit_override,
        pond_equity_usdc: pond_override,
        rich_idle_usdc: args.rich_idle_usdc,
        day_index: Some(args.day_index),
        dry_run: args.dry_run,
    })?;

    println!("{}", res.report_line);
    println!("{}", serde_json::to_string_pretty(&json!(res))?);
    Ok(())
}
