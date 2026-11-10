//! Standalone SQLite WAL Schema v12 Migration & Verification CLI (`aegis-db-tool`).
//!
//! Usage:
//!   `cargo run --bin aegis-db-tool -- --apply-migration sql/migrations/012_v030_round9_remediation.sql`

use aegis_core::AegisError;
use aegis_db::DatabaseManager;
use serde_json::json;
use std::path::PathBuf;

#[derive(Debug, Default, PartialEq, Eq)]
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

fn require_non_empty_flag_value<I>(iter: &mut I, flag: &str) -> aegis_core::Result<String>
where
    I: Iterator<Item = String>,
{
    match iter.next() {
        Some(val) if !val.trim().is_empty() && !val.starts_with("--") => Ok(val),
        Some(next_flag) if next_flag.starts_with("--") => Err(AegisError::Validation(format!(
            "Flag '{flag}' requires a value, but found another flag '{next_flag}'"
        ))),
        _ => Err(AegisError::Validation(format!(
            "Flag '{flag}' requires a non-empty value"
        ))),
    }
}

fn parse_args_from<I, S>(raw_args: I) -> aegis_core::Result<Args>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = Args::default();
    let mut iter = raw_args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--db" | "--db-path" => {
                let val = require_non_empty_flag_value(&mut iter, arg.as_str())?;
                args.db = Some(PathBuf::from(val));
            }
            s if s.starts_with("--db=") => {
                let val = s.trim_start_matches("--db=").trim();
                if val.is_empty() {
                    return Err(AegisError::Validation(
                        "Flag '--db=' requires a non-empty value".to_string(),
                    ));
                }
                args.db = Some(PathBuf::from(val));
            }
            s if s.starts_with("--db-path=") => {
                let val = s.trim_start_matches("--db-path=").trim();
                if val.is_empty() {
                    return Err(AegisError::Validation(
                        "Flag '--db-path=' requires a non-empty value".to_string(),
                    ));
                }
                args.db = Some(PathBuf::from(val));
            }
            "--apply-migration" => {
                let val = require_non_empty_flag_value(&mut iter, "--apply-migration")?;
                args.apply_migration = Some(PathBuf::from(val));
            }
            s if s.starts_with("--apply-migration=") => {
                let val = s.trim_start_matches("--apply-migration=").trim();
                if val.is_empty() {
                    return Err(AegisError::Validation(
                        "Flag '--apply-migration=' requires a non-empty value".to_string(),
                    ));
                }
                args.apply_migration = Some(PathBuf::from(val));
            }
            "--init" | "--migrate" => args.init = true,
            "--force" => args.force = true,
            "--generate-key" => args.generate_key = true,
            "--show-web-token" => args.show_web_token = true,
            "--audit-dry-run-residue" => args.audit_dry_run_residue = true,
            "--reclassify-dry-run-before" => {
                let val = require_non_empty_flag_value(&mut iter, "--reclassify-dry-run-before")?;
                args.reclassify_dry_run_before = Some(val);
            }
            s if s.starts_with("--reclassify-dry-run-before=") => {
                let val = s.trim_start_matches("--reclassify-dry-run-before=").trim();
                if val.is_empty() {
                    return Err(AegisError::Validation(
                        "Flag '--reclassify-dry-run-before=' requires a non-empty value"
                            .to_string(),
                    ));
                }
                args.reclassify_dry_run_before = Some(val.to_string());
            }
            "--verify" => {}
            other => {
                return Err(AegisError::Validation(format!(
                    "Unrecognized aegis-db-tool argument: '{other}'"
                )));
            }
        }
    }
    Ok(args)
}

fn main() -> aegis_core::Result<()> {
    let args = parse_args_from(std::env::args().skip(1))?;
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
            "status": rep.get("status").and_then(|v| v.as_str()).unwrap_or("APPLIED"),
            "already_applied": rep.get("already_applied").and_then(|v| v.as_bool()).unwrap_or(false),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args_rejects_missing_value_for_reclassify_dry_run_before() {
        assert!(parse_args_from(["--reclassify-dry-run-before"]).is_err());
        assert!(parse_args_from(["--reclassify-dry-run-before="]).is_err());
        assert!(parse_args_from(["--reclassify-dry-run-before", "--init"]).is_err());
        assert!(parse_args_from(["--db"]).is_err());
        assert!(parse_args_from(["--apply-migration"]).is_err());
        assert!(parse_args_from(["--unknown-flag"]).is_err());

        let ok = parse_args_from([
            "--db",
            "data/aegis_quant.db",
            "--reclassify-dry-run-before=2026-04-09T09:56:00Z",
        ])
        .unwrap();
        assert_eq!(ok.db, Some(PathBuf::from("data/aegis_quant.db")));
        assert_eq!(
            ok.reclassify_dry_run_before.as_deref(),
            Some("2026-04-09T09:56:00Z")
        );
    }
}
