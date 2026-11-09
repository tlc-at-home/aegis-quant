//! Standalone SQLite WAL Schema v10 Migration & Verification CLI (`aegis-db-tool`).
//!
//! Usage:
//!   `cargo run --bin aegis-db-tool -- --apply-migration sql/migrations/010_v030_round5_remediation.sql`

use aegis_core::AegisError;
use aegis_db::DatabaseManager;
use serde_json::json;
use std::path::PathBuf;

#[derive(Debug, Default)]
struct Args {
    db: Option<PathBuf>,
    apply_migration: Option<PathBuf>,
    init: bool,
    force: bool,
    generate_key: bool,
    show_web_token: bool,
    audit_dry_run_residue: bool,
    reclassify_dry_run_before: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--db" | "--db-path" => {
                if let Some(val) = iter.next() {
                    args.db = Some(PathBuf::from(val));
                }
            }
            "--apply-migration" => {
                if let Some(val) = iter.next() {
                    args.apply_migration = Some(PathBuf::from(val));
                }
            }
            "--init" | "--migrate" => args.init = true,
            "--force" => args.force = true,
            "--generate-key" => args.generate_key = true,
            "--show-web-token" => args.show_web_token = true,
            "--audit-dry-run-residue" => args.audit_dry_run_residue = true,
            "--reclassify-dry-run-before" => {
                if let Some(val) = iter.next() {
                    args.reclassify_dry_run_before = Some(val);
                }
            }
            s if s.starts_with("--reclassify-dry-run-before=") => {
                let val = s.trim_start_matches("--reclassify-dry-run-before=");
                args.reclassify_dry_run_before = Some(val.to_string());
            }
            "--verify" => {}
            _ => {}
        }
    }
    args
}

fn main() -> aegis_core::Result<()> {
    let args = parse_args();
    if args.generate_key {
        println!("{}", aegis_db::fernet::generate_fernet_key());
        return Ok(());
    }
    let db = match &args.db {
        Some(p) => DatabaseManager::new(p),
        None => DatabaseManager::default_db(),
    };

    if args.show_web_token {
        match db.get_existing_web_api_token()? {
            Some(token) => {
                println!("{token}");
                return Ok(());
            }
            None => {
                return Err(AegisError::NotFound(
                    "No web_api_token configured in system_config.".to_string(),
                ));
            }
        }
    }

    if args.audit_dry_run_residue {
        let rows = db.audit_suspect_dry_run_book_transfers()?;
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "suspect_partial_idle_capped_count": rows.len(),
                "rows": rows,
            }))?
        );
        return Ok(());
    }

    if let Some(cutoff) = &args.reclassify_dry_run_before {
        let count = db.reclassify_dry_run_transfers_before(cutoff)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "status": "RECLASSIFIED",
                "reclassified_count": count,
                "cutoff_utc": cutoff,
            }))?
        );
        return Ok(());
    }

    if args.init {
        let rep = db.initialize_schema(args.force)?;
        println!("{}", serde_json::to_string_pretty(&rep)?);
        return Ok(());
    }

    if let Some(mig_path) = &args.apply_migration {
        let rep = db.apply_migration(mig_path)?;
        let books = db.get_books().unwrap_or_default();
        let champ = db.get_active_pond_champion().ok();
        let cfg = db.load_config().unwrap_or_default();
        let out = json!({
            "status": "APPLIED",
            "migration": mig_path.display().to_string(),
            "db_path": db.db_path.display().to_string(),
            "schema_version": rep.get("schema_version").and_then(|v| v.as_i64()).unwrap_or(-1),
            "system_config_count": rep.get("system_config_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "pond_config_count": rep.get("pond_config_count").and_then(|v| v.as_i64()).unwrap_or(0),
            "books_count": books.len(),
            "books": books,
            "active_champion": champ.as_ref().map(|c| c.version.as_str()).unwrap_or("NPS-01"),
            "pond_direction_policy": cfg.get_str("pond_direction_policy", "both"),
            "integrity": rep,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }

    let rep = db.verify_integrity()?;
    println!("{}", serde_json::to_string_pretty(&rep)?);
    Ok(())
}

