//! Standalone SQLite WAL Schema v6 Migration & Verification CLI (`aegis-db-tool`).
//!
//! Usage:
//!   `cargo run --bin aegis-db-tool -- --apply-migration sql/migrations/006_v030_review_remediation.sql`

use aegis_db::DatabaseManager;
use clap::Parser;
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "aegis-db-tool",
    version,
    about = "Aegis Quant SQLite WAL Schema Migration & Integrity Verification Tool"
)]
struct Args {
    /// Path to SQLite WAL database (defaults to data/aegis_quant.db)
    #[arg(long)]
    db: Option<PathBuf>,

    /// Apply a specific SQL migration file (e.g. sql/migrations/005_v030_pp50_two_book.sql)
    #[arg(long = "apply-migration")]
    apply_migration: Option<PathBuf>,

    /// Initialize full schema and all migrations (001..006)
    #[arg(long)]
    init: bool,

    /// Force reset mutable tables and vacuum database when used with --init
    #[arg(long)]
    force: bool,

    /// Generate a fresh 32-byte URL-safe Base64 Fernet master key
    #[arg(long = "generate-key")]
    generate_key: bool,

    /// Verify schema integrity and print JSON report
    #[arg(long)]
    verify: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.generate_key {
        println!("{}", aegis_db::fernet::generate_fernet_key());
        return Ok(());
    }
    let db = match &args.db {
        Some(p) => DatabaseManager::new(p),
        None => DatabaseManager::default_db(),
    };

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
            "schema_version": rep.get("schema_version").and_then(|v| v.as_i64()).unwrap_or(6),
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
