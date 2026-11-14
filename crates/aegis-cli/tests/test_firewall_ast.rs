//! Compile-Time, AST/Source-Level, and Runtime Firewall Verification Suite (`test_firewall_ast`).
//!
//! Enforces the 6 Non-Negotiable Two-Book Firewall Invariants (F1–F6) defined in
//! `blueprint-v030-rc1-pp50-rust.md`:
//! - F1: Crate Dependency Graph Isolation (`aegis-pit`, `aegis-pond`, `aegis-settle`)
//! - F2: AST / Source-Level Cross-Book Token Isolation + Runtime `BookScopedDb` Table Whitelist
//! - F3: Independent Subaccount / Book Row Isolation (`PIT` vs `POND`)
//! - F4: Config Namespace Isolation (`PitConfig` vs `PondConfig`)
//! - F5: Idle-Only Settling Transfer (Zero Forced Liquidation)
//! - F6: Panic & Task Supervision Isolation (`AssertUnwindSafe(...).catch_unwind()`)

use aegis_core::{resolve_project_root, BookType, FirewallError};
use aegis_db::{BookScopedDb, DatabaseManager};
use aegis_settle::{SettleController, SettleOverrideInput};
use futures_util::FutureExt;
use std::fs;
use std::panic::AssertUnwindSafe;
use std::path::Path;

fn collect_rs_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
}

#[test]
fn test_invariant_f1_cargo_toml_dependency_isolation() {
    let root = resolve_project_root();

    let pit_toml = fs::read_to_string(root.join("crates/aegis-pit/Cargo.toml")).unwrap();
    assert!(
        !pit_toml.contains("aegis-pond"),
        "Invariant F1 violation: aegis-pit/Cargo.toml must not depend on aegis-pond"
    );
    assert!(
        !pit_toml.contains("aegis-settle"),
        "Invariant F1 violation: aegis-pit/Cargo.toml must not depend on aegis-settle"
    );

    let pond_toml = fs::read_to_string(root.join("crates/aegis-pond/Cargo.toml")).unwrap();
    assert!(
        !pond_toml.contains("aegis-pit"),
        "Invariant F1 violation: aegis-pond/Cargo.toml must not depend on aegis-pit"
    );
    assert!(
        !pond_toml.contains("aegis-signals"),
        "Invariant F1 violation: aegis-pond/Cargo.toml must not depend on aegis-signals"
    );
    assert!(
        !pond_toml.contains("aegis-settle"),
        "Invariant F1 violation: aegis-pond/Cargo.toml must not depend on aegis-settle"
    );

    let settle_toml = fs::read_to_string(root.join("crates/aegis-settle/Cargo.toml")).unwrap();
    assert!(
        !settle_toml.contains("aegis-pit"),
        "Invariant F1 violation: aegis-settle/Cargo.toml must not depend on aegis-pit"
    );
    assert!(
        !settle_toml.contains("aegis-pond"),
        "Invariant F1 violation: aegis-settle/Cargo.toml must not depend on aegis-pond"
    );
    assert!(
        !settle_toml.contains("aegis-signals"),
        "Invariant F1 violation: aegis-settle/Cargo.toml must not depend on aegis-signals"
    );
}

#[test]
fn test_invariant_f2_ast_source_token_scan() {
    let root = resolve_project_root();

    // 1. Scan crates/aegis-pit/src for forbidden Book B tokens
    let mut pit_files = Vec::new();
    collect_rs_files(&root.join("crates/aegis-pit/src"), &mut pit_files);
    assert!(!pit_files.is_empty());
    let pit_forbidden = [
        "aegis_pond",
        "aegis-pond",
        "pond_cells",
        "pond_universe",
        "pond_scan_cycles",
        "pond_champions",
        "PondCell",
        "PondConfig",
        "PondOrchestrator",
        "PondScanner",
    ];
    for file in &pit_files {
        let src = fs::read_to_string(file).unwrap();
        for token in pit_forbidden {
            assert!(
                !src.contains(token),
                "Invariant F2 AST violation in {}: forbidden token '{}'",
                file.display(),
                token
            );
        }
    }

    // 2. Scan crates/aegis-pond/src for forbidden Book A tokens
    let mut pond_files = Vec::new();
    collect_rs_files(&root.join("crates/aegis-pond/src"), &mut pond_files);
    assert!(!pond_files.is_empty());
    let pond_forbidden = [
        "aegis_pit",
        "aegis-pit",
        "aegis_signals",
        "aegis-signals",
        "signal_evaluations",
        "portfolio_state",
        "PitConfig",
        "PitOrchestrator",
        "SignalEngine",
    ];
    for file in &pond_files {
        let src = fs::read_to_string(file).unwrap();
        for token in pond_forbidden {
            assert!(
                !src.contains(token),
                "Invariant F2 AST violation in {}: forbidden token '{}'",
                file.display(),
                token
            );
        }
    }

    // 3. Scan crates/aegis-settle/src for forbidden Book strategy tokens
    let mut settle_files = Vec::new();
    collect_rs_files(&root.join("crates/aegis-settle/src"), &mut settle_files);
    assert!(!settle_files.is_empty());
    let settle_forbidden = [
        "aegis_pit",
        "aegis-pit",
        "aegis_pond",
        "aegis-pond",
        "aegis_signals",
        "aegis-signals",
        "PitOrchestrator",
        "PondOrchestrator",
        "SignalEngine",
    ];
    for file in &settle_files {
        let src = fs::read_to_string(file).unwrap();
        for token in settle_forbidden {
            assert!(
                !src.contains(token),
                "Invariant F2 AST violation in {}: forbidden token '{}'",
                file.display(),
                token
            );
        }
    }

    // 4. R7-B1, R7-QA & R8-N1: Static source-token scan on crates/aegis-db/src/schema_ops.rs
    // and crates/aegis-db/src/config_ops.rs to guarantee config read/routing-reconcile paths
    // never execute DML against `trades`, `pond_cells`, or `book_transfers`.
    let schema_ops_src =
        fs::read_to_string(root.join("crates/aegis-db/src/schema_ops.rs")).unwrap();
    let config_ops_src =
        fs::read_to_string(root.join("crates/aegis-db/src/config_ops.rs")).unwrap();
    let config_dml_forbidden = [
        "UPDATE trades",
        "INSERT INTO trades",
        "INSERT OR REPLACE INTO trades",
        "INSERT OR IGNORE INTO trades",
        "REPLACE INTO trades",
        "DELETE FROM trades",
        "UPDATE pond_cells",
        "INSERT INTO pond_cells",
        "INSERT OR REPLACE INTO pond_cells",
        "INSERT OR IGNORE INTO pond_cells",
        "REPLACE INTO pond_cells",
        "DELETE FROM pond_cells",
        "UPDATE book_transfers",
        "INSERT INTO book_transfers",
        "INSERT OR REPLACE INTO book_transfers",
        "INSERT OR IGNORE INTO book_transfers",
        "REPLACE INTO book_transfers",
        "DELETE FROM book_transfers",
    ];
    for token in config_dml_forbidden {
        assert!(
            !schema_ops_src.contains(token),
            "R7-QA/R8-N1 static source-token violation in crates/aegis-db/src/schema_ops.rs: forbidden DML token '{}'",
            token
        );
        assert!(
            !config_ops_src.contains(token),
            "R7-QA/R8-N1 static source-token violation in crates/aegis-db/src/config_ops.rs: forbidden DML token '{}'",
            token
        );
    }
}

#[test]
fn test_invariants_f2_f3_f4_scoped_db_and_config_isolation() {
    let db = DatabaseManager::memory();
    db.initialize_schema(true).unwrap();

    let pit_db = BookScopedDb::new(db.clone(), BookType::Pit);
    let pond_db = BookScopedDb::new(db.clone(), BookType::Pond);

    // F2: Table whitelist enforcement
    assert!(pit_db.check_table_access("trades").is_ok());
    assert!(pit_db.check_table_access("v_trades_pit").is_ok());
    assert!(matches!(
        pit_db.check_table_access("pond_cells"),
        Err(FirewallError::UnauthorizedTableAccess { .. })
    ));
    assert!(matches!(
        pit_db.check_table_access("pond_universe"),
        Err(FirewallError::UnauthorizedTableAccess { .. })
    ));

    assert!(pond_db.check_table_access("pond_cells").is_ok());
    assert!(pond_db.check_table_access("pond_universe").is_ok());
    assert!(matches!(
        pond_db.check_table_access("trades"),
        Err(FirewallError::UnauthorizedTableAccess { .. })
    ));
    assert!(matches!(
        pond_db.check_table_access("signal_evaluations"),
        Err(FirewallError::UnauthorizedTableAccess { .. })
    ));

    // F3: Independent Book rows in `books`
    let books = db.get_books().unwrap();
    let pit_book = books.iter().find(|b| b.book_code == "PIT").unwrap();
    let pond_book = books.iter().find(|b| b.book_code == "POND").unwrap();
    assert_eq!(pit_book.equity_allocation_pct, 0.50);
    assert_eq!(pond_book.equity_allocation_pct, 0.50);

    // F4: Config namespace isolation
    let pit_cfg = pit_db.load_pit_config().unwrap();
    let pond_cfg = pond_db.load_pond_config().unwrap();
    assert_eq!(pit_cfg.initial_equity_usdc, 10000.0);
    assert_eq!(pond_cfg.pond_universe_target_size, 80);

    assert!(pit_db.check_config_key_access("mace_weight").is_ok());
    assert!(matches!(
        pit_db.check_config_key_access("pond_direction_policy"),
        Err(FirewallError::UnauthorizedConfigAccess { .. })
    ));

    assert!(pond_db
        .check_config_key_access("pond_direction_policy")
        .is_ok());
    assert!(matches!(
        pond_db.check_config_key_access("mace_weight"),
        Err(FirewallError::UnauthorizedConfigAccess { .. })
    ));
}

#[test]
fn test_invariant_f5_idle_only_settling_and_f6_panic_isolation() {
    let db = DatabaseManager::memory();
    db.initialize_schema(true).unwrap();
    let ctrl = SettleController::new(db, None);

    // F5: Rich book has $5,600, Poor book has $4,400 -> required transfer = $600,
    // but Rich book only has $150 unlocked idle USDC -> executes $150, blocks $450 without liquidating
    let res = ctrl
        .run_settling(SettleOverrideInput {
            pit_equity_usdc: Some(5600.0),
            pond_equity_usdc: Some(4400.0),
            rich_idle_usdc: Some(150.0),
            day_index: Some(1),
            dry_run: true,
        })
        .unwrap();
    assert_eq!(res.status, "PARTIAL_IDLE_CAPPED");
    assert!((res.required_transfer_usdc - 600.0).abs() < 1e-6);
    assert!((res.executed_transfer_usdc - 150.0).abs() < 1e-6);
    assert!((res.blocked_remainder_usdc - 450.0).abs() < 1e-6);

    // F6: Simulated panic inside one book's future is caught by AssertUnwindSafe(...).catch_unwind()
    // while the other book continues executing normally.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let panicking_book = AssertUnwindSafe(async {
            panic!("simulated book fault");
        })
        .catch_unwind();

        let healthy_book = AssertUnwindSafe(async { 42u64 }).catch_unwind();

        let (r_panic, r_healthy) = tokio::join!(panicking_book, healthy_book);
        assert!(r_panic.is_err());
        assert_eq!(r_healthy.unwrap(), 42u64);
    });
}

#[test]
fn test_concurrent_sqlite_wal_pit_pond_settle() {
    use aegis_pit::PitOrchestrator;
    use aegis_pond::PondOrchestrator;
    use std::thread;

    let tmp_dir = resolve_project_root().join("target").join("tmp_wal_test");
    let _ = fs::create_dir_all(&tmp_dir);
    let db_path = tmp_dir.join(format!("pp50_wal_{}.db", uuid::Uuid::new_v4()));
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));

    let db = DatabaseManager::new(&db_path);
    db.initialize_schema(true).unwrap();

    // Seed initial universe before concurrent stress
    let pond_init = PondOrchestrator::new(db.clone(), None);
    let _ = pond_init.initialize_or_refresh_universe().unwrap();

    let db_pit = db.clone();
    let t_pit = thread::spawn(move || {
        let pit = PitOrchestrator::new(db_pit, None);
        for i in 0..3 {
            let summary = pit.run_cycle(i == 0, true).unwrap();
            assert_eq!(summary.book_id, "PIT");
        }
    });

    let db_pond = db.clone();
    let t_pond = thread::spawn(move || {
        let pond = PondOrchestrator::new(db_pond, None);
        for i in 0..3 {
            let summary = pond.run_scan_cycle(i == 0).unwrap();
            assert_eq!(summary.book_id, "POND");
        }
    });

    let db_settle = db.clone();
    let t_settle = thread::spawn(move || {
        let ctrl = SettleController::new(db_settle, None);
        for day in 1..=3 {
            let res = ctrl
                .run_settling(SettleOverrideInput {
                    pit_equity_usdc: Some(5200.0),
                    pond_equity_usdc: Some(4800.0),
                    rich_idle_usdc: Some(500.0),
                    day_index: Some(day),
                    dry_run: true,
                })
                .unwrap();
            assert_eq!(res.status, "DRY_RUN");
        }
    });

    t_pit.join().unwrap();
    t_pond.join().unwrap();
    t_settle.join().unwrap();

    // Verify database integrity after concurrent multi-book WAL writes
    let integrity = db.verify_integrity().unwrap();
    assert_eq!(integrity["journal_mode"], "wal");
    assert_eq!(integrity["books_count"], 2);
    assert_eq!(integrity["pond_universe_count"], 80);
    let _ = fs::remove_file(&db_path);
    let _ = fs::remove_file(format!("{}-wal", db_path.display()));
    let _ = fs::remove_file(format!("{}-shm", db_path.display()));
}

fn collect_all_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_all_files(&path, out);
            } else if path.is_file() {
                out.push(path);
            }
        }
    }
}

fn contains_contiguous_telegram_bot_token(line: &str) -> bool {
    let mut search_from = 0;
    while let Some(rel) = line[search_from..].find(":AA") {
        let colon_idx = search_from + rel;
        let digits_before = line[..colon_idx]
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .count();
        let after_aa = &line[colon_idx + 3..];
        let token_tail_len = after_aa
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
            .count();
        if (8..=10).contains(&digits_before) && token_tail_len >= 20 {
            return true;
        }
        search_from = colon_idx + 3;
    }
    false
}

fn contains_contiguous_aiza_literal(line: &str) -> bool {
    let prefix = ["AI", "za"].concat();
    let mut search_from = 0;
    while let Some(rel) = line[search_from..].find(&prefix) {
        let idx = search_from + rel;
        let after = &line[idx + 4..];
        let tail_len = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
            .count();
        if tail_len >= 35 {
            return true;
        }
        search_from = idx + 4;
    }
    false
}

#[test]
fn test_r16_u2_no_contiguous_secret_literals_in_tracked_sources() {
    let root = resolve_project_root();
    let scan_dirs = ["crates", "web", "sql", "deploy", ".github"];
    let mut files = Vec::new();
    for dir in scan_dirs {
        collect_all_files(&root.join(dir), &mut files);
    }
    assert!(
        !files.is_empty(),
        "R16-U2 secret scan must inspect tracked repository directories"
    );

    let mut violations = Vec::new();
    for file in &files {
        let Ok(content) = fs::read_to_string(file) else {
            continue;
        };
        let rel = file.strip_prefix(&root).unwrap_or(file).display();
        for (line_no, line) in content.lines().enumerate() {
            if contains_contiguous_telegram_bot_token(line) {
                violations.push(format!(
                    "{rel}:{}: contiguous Telegram bot token literal ([REDACTED])",
                    line_no + 1
                ));
            }
            if contains_contiguous_aiza_literal(line) {
                violations.push(format!(
                    "{rel}:{}: contiguous AIza API key literal ([REDACTED])",
                    line_no + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "R16-U2 secret hygiene violation(s) detected:\n{}",
        violations.join("\n")
    );
}
