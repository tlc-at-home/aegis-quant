//! Standalone SQLite WAL Schema v7 Migration & Verification CLI (`aegis-db-tool`).
//!
//! Usage:
//!   `cargo run --bin aegis-db-tool -- --apply-migration sql/migrations/007_v030_round2_remediation.sql`

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
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--db" => {
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
        let token = db.get_or_create_web_api_token()?;
        println!("{token}");
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
            "schema_version": rep.get("schema_version").and_then(|v| v.as_i64()).unwrap_or(7),
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

