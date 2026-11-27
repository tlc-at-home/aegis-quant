//! Comprehensive MariaDB 13.0 Rolling integration tests for `aegis-db` (`tests/db_integration.rs`).

use aegis_core::{now_ms, ytd_start_ms, AegisError, OrderSide, RiskTier};
use aegis_db::{
    aggregate_close_epoch_trades, canonicalize_asset_updates, canonicalize_asset_upserts,
    create_mariadb_backup, order_asset_upserts_by_lock_key, resolve_synced_tpsl_targets,
    verify_latest_backup, AlertQueueInsertInput, AssetUpsertInput, BarUpsertInput, DatabaseManager,
    DestructiveResetToken, ExchangeOpenPositionSyncInput, FernetCipher, FundingEventUpsertInput,
    OidUpdate, ReconciledClosedTradeInput, RetentionPolicy, SignalUpsertInput, SnapshotInsertInput,
    TelemetryInsertInput, WorkingOrderInput, CANONICAL_TABLES,
};
use mysql::params;
use mysql::prelude::*;
use serde_json::json;

#[test]
fn test_mariadb_13_schema_views_and_default_extreme_config() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB should initialize");

    let ver = db.server_version().expect("server_version should succeed");
    assert!(
        ver.contains("MariaDB"),
        "Expected MariaDB server version, got: {ver}"
    );

    let (tables, views) = db
        .list_tables_and_views()
        .expect("list_tables_and_views should succeed");
    assert_eq!(
        tables,
        vec![
            "alert_queue",
            "asset_cooldowns",
            "exchange_assets",
            "funding_events",
            "ohlcv_bars",
            "order_audit",
            "portfolio_snapshots",
            "positions",
            "sleeve_signals",
            "system_config",
            "telemetry_logs",
        ]
    );
    assert_eq!(
        views,
        vec![
            "v_assets_held",
            "v_assets_in_cooldown",
            "v_assets_ordered",
            "v_assets_sleeve_s1",
            "v_assets_sleeve_s2",
            "v_assets_sleeve_s3",
            "v_priority_watchlist",
        ]
    );

    let cfg = db.load_config().expect("load_config should succeed");
    assert_eq!(cfg.risk_tier(), RiskTier::Extreme);
    assert!((cfg.target_leverage_multiplier() - 1.85).abs() < 1e-9);
    assert_eq!(cfg.get_str("alert_provider", ""), "telegram");

    // Verify non-destructive init_schema() creates funding_events cleanly on existing DBs,
    // and reset_schema_destructive drops/recreates all 11 tables cleanly (FK order).
    {
        let mut conn = db.get_conn().unwrap();
        conn.query_drop("DROP TABLE IF EXISTS funding_events")
            .unwrap();
    }
    db.init_schema()
        .expect("init_schema() should recreate missing funding_events non-destructively");
    let (tables_after_migrate, _) = db.list_tables_and_views().unwrap();
    assert_eq!(tables_after_migrate.len(), 11);
    assert!(tables_after_migrate.contains(&"funding_events".to_string()));

    db.reset_schema_destructive(DestructiveResetToken::ephemeral_test_only())
        .expect("reset_schema_destructive should drop and recreate all 11 tables cleanly");
    let (tables_after_recreate, views_after_recreate) = db.list_tables_and_views().unwrap();
    assert_eq!(tables_after_recreate.len(), 11);
    assert_eq!(views_after_recreate.len(), 7);
}

#[test]
fn test_fernet_secret_storage_and_key_rotation() {
    let key1 = aegis_db::generate_fernet_key();
    let db = DatabaseManager::new_isolated_test(Some(&key1)).expect("isolated test DB");

    let raw_pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    db.set_config_value("private_key", raw_pk, Some("Agent key"))
        .expect("set_config_value should encrypt private_key");

    let rows = db.list_system_config_rows().expect("list rows");
    let pk_row = rows
        .iter()
        .find(|r| r.config_key == "private_key")
        .expect("private_key row");
    assert!(
        aegis_db::is_encrypted_value(&pk_row.config_value),
        "Expected Fernet token in system_config"
    );

    let cfg = db.load_config().expect("load_config decrypts");
    assert_eq!(cfg.get_str("private_key", ""), raw_pk);

    let key2 = aegis_db::generate_fernet_key();
    let rotated = db
        .rotate_fernet_master_key(&key1, &key2)
        .expect("rotate_fernet_master_key should succeed");
    assert_eq!(rotated, 1);

    let cfg_after_rotate = db
        .load_config()
        .expect("load_config should use rotated key in memory");
    assert_eq!(cfg_after_rotate.get_str("private_key", ""), raw_pk);

    // Reject invalid raw ciphertext on set_config_value
    assert!(db
        .set_config_value("private_key", "gAAAAABcorrupt_token_123", None)
        .is_err());

    // Isolate per-row decryption errors in load_config
    {
        let mut conn = db.get_conn().unwrap();
        let ts = now_ms();
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES ('broken_secret', 'gAAAAABcorrupt_row', 'secret', 'bad', :ts, :ts)"#,
            params! { "ts" => ts },
        )
        .unwrap();
    }
    let cfg_isolated = db
        .load_config()
        .expect("load_config must isolate single bad Fernet row");
    assert_eq!(cfg_isolated.get_str("private_key", ""), raw_pk);
    assert!(cfg_isolated.get("broken_secret").is_none());
}

#[test]
fn test_seven_views_order_to_open_lifecycle_and_cooldowns() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");

    // 1. Upsert two Hyperliquid assets with canonical symbol normalization
    let sol_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "SOL".to_string(),
            asset_index: 4,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: 184.00,
            mid_price: 184.02,
            oracle_price: 183.98,
            funding_rate_hourly: 0.000045,
            day_volume_usdc: 125_000_000.0,
            open_interest_usdc: 45_000_000.0,
            prev_day_price: 176.50,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .expect("upsert SOL");

    let sol_asset = db
        .get_asset_by_symbol("SOL/USDC:USDC")
        .expect("lookup SOL")
        .expect("SOL exists");
    assert_eq!(sol_asset.symbol, "SOL-USDC");
    assert_eq!(sol_asset.coin, "SOL");
    assert_eq!(sol_asset.id, sol_id);

    // 2. Upsert OHLCV bars
    let bar_count = db
        .upsert_ohlcv_bars(&[
            BarUpsertInput {
                asset_id: sol_id,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_000_000_000,
                open: 180.0,
                high: 185.0,
                low: 179.5,
                close: 184.0,
                volume: 5_000_000.0,
                funding_rate: 0.000045,
            },
            BarUpsertInput {
                asset_id: sol_id,
                timeframe: "4h".to_string(),
                timestamp_ms: 1_700_014_400_000,
                open: 184.0,
                high: 187.0,
                low: 183.0,
                close: 186.2,
                volume: 6_200_000.0,
                funding_rate: 0.000050,
            },
        ])
        .expect("upsert bars");
    assert_eq!(bar_count, 2);
    let bars = db.get_ohlcv_bars(sol_id, "4h", 10).expect("get_ohlcv_bars");
    assert_eq!(bars.len(), 2);
    assert!((bars[1].close - 186.2).abs() < 1e-6);

    // 3. Upsert multi-sleeve signal & verify S1/S2/S3 views
    db.upsert_sleeve_signal(&SignalUpsertInput {
        asset_id: sol_id,
        mark_price: 184.0,
        day_volume_usdc: 125_000_000.0,
        funding_rate_hourly: 0.000045,
        annualized_vol: 0.68,
        market_beta: 1.24,
        s1_mom_score: 1.45,
        s1_carry_score: 0.35,
        s2_trend_alignment: 1,
        s2_vol_surge_ratio: 1.82,
        s3_funding_zscore: -2.15,
        s3_flush_atr_mult: 1.90,
        w_s1_carry_mom: 0.112,
        w_s2_trend_breakout: 0.155,
        w_s3_event_reversion: 0.025,
        raw_combined_weight: 0.1578,
        target_weight: 0.292,
        current_weight: 0.0,
        delta_weight: 0.292,
        event_trigger: Some("LIVE_BREAKOUT_SURGE".to_string()),
        rebalance_action: "BUY".to_string(),
    })
    .expect("upsert signal");

    assert_eq!(db.get_sleeve_s1_view().unwrap().len(), 1);
    assert_eq!(db.get_sleeve_s2_view().unwrap().len(), 1);
    assert_eq!(db.get_sleeve_s3_view().unwrap().len(), 1);

    // 4. Create WORKING order (reserves margin, does NOT commit notional yet)
    let pos_id = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-sol-001".to_string(),
            side: OrderSide::Long,
            limit_price: 183.90,
            requested_quantity: 14.50,
            reserved_margin_usdc: 1_441.22,
            portfolio_weight: 0.267,
            s1_weight_contrib: 0.112,
            s2_weight_contrib: 0.155,
            s3_weight_contrib: 0.0,
            protective_stop_px: 176.80,
            take_profit_px: None,
            exchange_order_id: Some("hl-oid-101".to_string()),
            reason: "Initial ALO maker entry".to_string(),
        })
        .expect("create_working_order");

    let ordered = db.get_assets_ordered().expect("v_assets_ordered");
    assert_eq!(ordered.len(), 1);
    assert_eq!(ordered[0].position_id, pos_id);
    assert!((ordered[0].reserved_margin_usdc - 1_441.22).abs() < 1e-2);
    assert!(db.get_assets_held().expect("v_assets_held").is_empty());

    // Priority watchlist includes this WORKING order
    let prio = db.get_priority_watchlist().expect("v_priority_watchlist");
    assert_eq!(prio.len(), 1);
    assert_eq!(prio[0].position_status.as_deref(), Some("WORKING"));

    // 5. Chase step on WORKING order
    let chase_cnt = db
        .record_working_order_chase(
            pos_id,
            184.00,
            "CHASING_ALO",
            Some("hl-oid-102"),
            "1s top-of-book chase #1",
        )
        .expect("chase");
    assert_eq!(chase_cnt, 1);

    // 6. Promote WORKING -> OPEN (releases reserved_margin_usdc -> commits committed_notional_usdc)
    db.promote_working_to_open(
        pos_id,
        184.25,
        14.50,
        0.40,
        176.80,
        None,
        Some("hl-oid-102"),
        "Exchange fill confirmed",
    )
    .expect("promote_working_to_open");

    assert!(db.get_assets_ordered().unwrap().is_empty());
    let held = db.get_assets_held().expect("v_assets_held after fill");
    assert_eq!(held.len(), 1);
    assert!((held[0].committed_notional_usdc - (184.25 * 14.50)).abs() < 0.05);
    assert!(held[0].stop_distance_pct.is_some());

    // 7. Update live mark price & trailing stop on OPEN holding
    db.update_asset_live_quote(sol_id, 190.00, 190.02)
        .expect("update_asset_live_quote");
    db.update_open_position_mark_and_stop(pos_id, 190.00, 182.40)
        .expect("update_open_position_mark_and_stop");

    let held_updated = db.get_assets_held().unwrap();
    assert!(held_updated[0].unrealized_pnl_usdc > 80.0);

    // 8. Close position & trigger post-stop cooldown
    let realized = db
        .close_position(pos_id, 189.50, 1.20, "TRAILING_STOP_PROFIT")
        .expect("close_position");
    assert!(realized > 70.0);
    assert!(db.get_assets_held().unwrap().is_empty());

    let audits = db.list_recent_order_audits(10).expect("order_audit");
    assert!(audits.len() >= 4);

    // 9. Cooldowns view & expiration
    db.insert_asset_cooldown(sol_id, "STOP_OUT", "Trailing stop hit", 1800)
        .expect("insert_asset_cooldown");
    assert!(db.is_asset_in_cooldown(sol_id).unwrap());
    let cd_view = db.get_assets_in_cooldown().expect("v_assets_in_cooldown");
    assert_eq!(cd_view.len(), 1);
    assert_eq!(cd_view[0].symbol, "SOL-USDC");
    assert!(cd_view[0].remaining_seconds > 0);
}

#[test]
fn test_portfolio_snapshot_telemetry_alerts_and_backup() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");

    let snap_id = db
        .insert_portfolio_snapshot(&SnapshotInsertInput {
            total_equity_usdc: 10_420.50,
            uncapped_equity_usdc: 10_420.50,
            available_cash_usdc: 7_748.87,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 2_671.63,
            net_notional_usdc: 2_671.63,
            gross_leverage_x: 0.256,
            net_leverage_x: 0.256,
            market_beta_exposure: 0.18,
            market_breadth_bull: 0.72,
            market_regime_label: "BULL_EXPANSION".to_string(),
            risk_tier: "extreme".to_string(),
            leverage_multiplier: 1.85,
            s1_gross_weight: 0.112,
            s2_gross_weight: 0.155,
            s3_gross_weight: 0.0,
            working_order_count: 0,
            open_long_count: 1,
            open_short_count: 0,
            eligible_coin_count: 42,
        })
        .expect("insert_portfolio_snapshot");
    assert!(snap_id > 0);

    let latest = db.get_latest_portfolio_snapshot().expect("latest snapshot");
    assert!((latest.total_equity_usdc - 10_420.50).abs() < 1e-4);
    assert_eq!(latest.risk_tier, "extreme");

    db.log_telemetry(
        "INFO",
        "priority_monitor",
        "1s Priority Watchlist cycle completed with token gAAAAABsecret1234567890",
        Some(json!({"watched_count": 1})),
    )
    .expect("log_telemetry");
    let logs = db.list_recent_telemetry(5).expect("list_recent_telemetry");
    assert_eq!(logs.len(), 1);
    assert!(
        !logs[0].message.contains("gAAAAABsecret"),
        "Fernet tokens must be redacted from telemetry logs"
    );

    let alert_id = db
        .insert_alert_queue_record(
            "telegram",
            "ORDER_FILLED_OPEN",
            "INFO",
            "<b>AEGIS v2.0 // ORDER FILLED</b>",
            Some(&json!({"symbol": "SOL-USDC"})),
            "DELIVERED",
        )
        .expect("insert_alert_queue_record");
    assert!(alert_id > 0);
    assert_eq!(db.list_recent_alerts(5).unwrap().len(), 1);

    let backup_path = create_mariadb_backup(&db).expect("create_mariadb_backup");
    assert!(backup_path.is_file());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let file_mode = std::fs::metadata(&backup_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600, "Backup JSON must have 0600 permissions");
        let dir_mode = std::fs::metadata(backup_path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700, "Backup dir must have 0700 permissions");
    }
    let report = verify_latest_backup(&db).expect("verify_latest_backup");
    assert!(report.verified);
    assert_eq!(report.version, "2.0.0");
    assert_eq!(report.table_counts.len(), CANONICAL_TABLES.len());
    assert!(report.table_counts.contains_key("funding_events"));
    let _ = std::fs::remove_file(backup_path);
}

#[test]
fn test_production_local_mariadb_seed_and_credential_migration() {
    let key = aegis_db::generate_fernet_key();
    let db = DatabaseManager::new_isolated_test(Some(&key))
        .expect("isolated test MariaDB should initialize with explicit Fernet key");

    let sqlite_path = std::env::temp_dir().join(format!(
        "aegis_legacy_cred_test_{}_{}.db",
        std::process::id(),
        now_ms()
    ));
    let _ = std::fs::remove_file(&sqlite_path);

    {
        let sqlite_conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        sqlite_conn
            .execute_batch(
                r#"CREATE TABLE system_config (
                       config_key TEXT PRIMARY KEY,
                       config_value TEXT NOT NULL,
                       value_type TEXT NOT NULL,
                       description TEXT
                   );"#,
            )
            .unwrap();
        let rows = [
            (
                "wallet_address",
                "0x1111111111111111111111111111111111111111",
                "string",
                "Primary wallet",
            ),
            (
                "hyperliquid_agent_address",
                "0x2222222222222222222222222222222222222222",
                "string",
                "Agent wallet",
            ),
            (
                "private_key",
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "secret",
                "API signer key",
            ),
            (
                "telegram_bot_token",
                "123456:ABC-DEF-test-token",
                "secret",
                "TG token",
            ),
            ("telegram_chat_id", "-1001234567890", "string", "TG chat ID"),
        ];
        for (k, v, vt, desc) in rows {
            sqlite_conn
                .execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, description) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![k, v, vt, desc],
                )
                .unwrap();
        }
    }

    let migrated = db
        .migrate_legacy_sqlite_credentials(&sqlite_path)
        .expect("migrate_legacy_sqlite_credentials should succeed on isolated fixture");
    let _ = std::fs::remove_file(&sqlite_path);
    assert_eq!(migrated, 5);

    let cfg = db.load_config().expect("load_config on isolated test DB");
    assert_eq!(cfg.risk_tier(), RiskTier::Extreme);
    assert!((cfg.target_leverage_multiplier() - 1.85).abs() < 1e-9);
    assert_eq!(
        cfg.get_str("wallet_address", ""),
        "0x1111111111111111111111111111111111111111"
    );
    assert_eq!(
        cfg.get_str("hyperliquid_agent_address", ""),
        "0x2222222222222222222222222222222222222222"
    );
    assert_eq!(
        cfg.get_str("private_key", ""),
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        cfg.get_str("telegram_bot_token", ""),
        "123456:ABC-DEF-test-token"
    );
    assert_eq!(cfg.get_str("telegram_chat_id", ""), "-1001234567890");
}

#[test]
fn test_reconcile_closed_preserves_audit_and_exit_reason() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "SOL".to_string(),
            asset_index: 4,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: 185.0,
            mid_price: 185.0,
            oracle_price: 185.0,
            funding_rate_hourly: 0.00002,
            day_volume_usdc: 50_000_000.0,
            open_interest_usdc: 20_000_000.0,
            prev_day_price: 180.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .unwrap();

    let pos_id = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "v2-sol-long-audit-preserve".to_string(),
            side: OrderSide::Long,
            limit_price: 180.0,
            requested_quantity: 2.34567891,
            reserved_margin_usdc: 228.0,
            portfolio_weight: 0.15,
            s1_weight_contrib: 0.09,
            s2_weight_contrib: 0.06,
            s3_weight_contrib: 0.0,
            protective_stop_px: 172.50,
            take_profit_px: None,
            exchange_order_id: Some("hl-open-111".to_string()),
            reason: "S1_S2_LONG".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        pos_id,
        180.12345678,
        2.34567891,
        0.08,
        172.50,
        None,
        Some("hl-open-111"),
        "ALO_FILLED",
    )
    .unwrap();
    db.close_position(pos_id, 185.80, 0.25, "PROTECTIVE_STOP_LONG")
        .unwrap();

    let audits_before = db.list_recent_order_audits(20).unwrap();
    let pos_audits_before = audits_before
        .iter()
        .filter(|a| a.position_id == pos_id)
        .count();
    assert!(pos_audits_before >= 3);

    let close_ts = now_ms();
    let old_ts = close_ts - 120_000;
    let count = db
        .reconcile_closed_positions_from_exchange_fills(&[
            ReconciledClosedTradeInput {
                asset_id: sol_id,
                client_order_id: "hl-closed-sol-999".to_string(),
                exchange_order_id: "hl-999".to_string(),
                close_epoch_id: None,
                side: OrderSide::Long,
                entry_price: 180.12345678,
                exit_price: 185.87654321,
                filled_quantity: 2.34567891,
                realized_pnl_usdc: 17.65,
                fees_paid_usdc: 0.35,
                funding_accrued_usdc: 4.50,
                created_at_ms: close_ts - 60_000,
                closed_at_ms: close_ts,
                exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
            },
            ReconciledClosedTradeInput {
                asset_id: sol_id,
                client_order_id: "hl-closed-sol-888".to_string(),
                exchange_order_id: "hl-888".to_string(),
                close_epoch_id: None,
                side: OrderSide::Short,
                entry_price: 190.12345678,
                exit_price: 188.12345678,
                filled_quantity: 1.25,
                realized_pnl_usdc: 2.30,
                fees_paid_usdc: 0.20,
                funding_accrued_usdc: 0.10,
                created_at_ms: old_ts - 30_000,
                closed_at_ms: old_ts,
                exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
            },
        ])
        .unwrap();
    assert_eq!(count, 2);

    let pos_after = db.get_position_by_id(pos_id).unwrap().unwrap();
    assert_eq!(pos_after.client_order_id, "v2-sol-long-audit-preserve");
    assert_eq!(
        pos_after.exit_reason.as_deref(),
        Some("PROTECTIVE_STOP_LONG")
    );
    assert!(pos_after.opened_at_ms.is_some());
    assert!((pos_after.s1_weight_contrib - 0.09).abs() < 1e-6);
    assert!((pos_after.s2_weight_contrib - 0.06).abs() < 1e-6);
    assert!((pos_after.protective_stop_px - 172.50).abs() < 1e-6);
    assert!((pos_after.entry_price - 180.12345678).abs() < 1e-8);
    assert!((pos_after.exit_price.unwrap_or(0.0) - 185.87654321).abs() < 1e-8);
    assert!((pos_after.filled_quantity - 2.34567891).abs() < 1e-8);
    assert!((pos_after.funding_accrued_usdc - 4.50).abs() < 1e-4);
    assert!((pos_after.realized_pnl_usdc - 17.65).abs() < 1e-4);

    let audits_after = db.list_recent_order_audits(20).unwrap();
    let pos_audits_after = audits_after
        .iter()
        .filter(|a| a.position_id == pos_id)
        .count();
    assert_eq!(pos_audits_after, pos_audits_before);
}

#[test]
fn test_sync_open_v2_preserves_stops_oids_working_and_opened_at() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let ids = db
        .upsert_exchange_assets_batch(&[
            AssetUpsertInput {
                raw_symbol_or_coin: "kpepe".to_string(),
                asset_index: 10,
                sz_decimals: 0,
                tick_size: 0.0001,
                lot_step_size: 1.0,
                max_leverage: 10.0,
                mark_price: 0.012,
                mid_price: 0.012,
                oracle_price: 0.012,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 10_000_000.0,
                open_interest_usdc: 4_000_000.0,
                prev_day_price: 0.011,
                is_core_eligible: true,
                is_event_eligible: false,
                is_priority_watched: false,
            },
            AssetUpsertInput {
                raw_symbol_or_coin: "ARB".to_string(),
                asset_index: 11,
                sz_decimals: 1,
                tick_size: 0.0001,
                lot_step_size: 0.1,
                max_leverage: 20.0,
                mark_price: 1.00,
                mid_price: 1.00,
                oracle_price: 1.00,
                funding_rate_hourly: 0.00003,
                day_volume_usdc: 25_000_000.0,
                open_interest_usdc: 8_000_000.0,
                prev_day_price: 0.98,
                is_core_eligible: true,
                is_event_eligible: true,
                is_priority_watched: true,
            },
        ])
        .unwrap();
    assert_eq!(ids.len(), 2);
    let kpepe = db.get_asset_by_symbol("kPEPE").unwrap().unwrap();
    assert_eq!(kpepe.coin, "kPEPE");
    assert_eq!(kpepe.symbol, "kPEPE-USDC");

    let arb_id = ids[1];
    // update_asset_live_mid updates only mid_price, preserving mark_price
    db.update_asset_live_mid(arb_id, 1.05).unwrap();
    let arb = db.get_asset_by_id(arb_id).unwrap().unwrap();
    assert!((arb.mid_price - 1.05).abs() < 1e-6);
    assert!((arb.mark_price - 1.00).abs() < 1e-6);

    // Create WORKING order with multi-sleeve weights and stop, then promote in-place via sync_v2
    let work_id = db
        .create_working_order(&WorkingOrderInput {
            asset_id: arb_id,
            client_order_id: "v2-arb-working-inplace".to_string(),
            side: OrderSide::Long,
            limit_price: 0.995,
            requested_quantity: 100.0,
            reserved_margin_usdc: 54.0,
            portfolio_weight: 0.24,
            s1_weight_contrib: 0.12,
            s2_weight_contrib: 0.08,
            s3_weight_contrib: 0.04,
            protective_stop_px: 0.92,
            take_profit_px: None,
            exchange_order_id: Some("hl-work-1".to_string()),
            reason: "MULTI_SLEEVE".to_string(),
        })
        .unwrap();

    let legacy_open_ts = 1_795_020_000_000i64;
    let (synced_id, promoted) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: arb_id,
            coin: "ARB",
            side: OrderSide::Long,
            entry_price: 0.995,
            mark_price: 1.00,
            quantity: 100.0,
            committed_notional_usdc: 100.0,
            unrealized_pnl_usdc: 0.50,
            portfolio_weight: 0.24,
            protective_stop_px: 0.95,
            take_profit_px: 1.08,
            funding_accrued_usdc: 1.25,
            opened_at_ms: Some(legacy_open_ts),
        })
        .unwrap();
    assert_eq!(synced_id, work_id);
    assert!(promoted);

    let pos_after_promote = db.get_position_by_id(work_id).unwrap().unwrap();
    assert_eq!(pos_after_promote.status, "OPEN");
    assert_eq!(pos_after_promote.client_order_id, "v2-arb-working-inplace");
    assert!((pos_after_promote.s1_weight_contrib - 0.12).abs() < 1e-6);
    assert!((pos_after_promote.s2_weight_contrib - 0.08).abs() < 1e-6);
    assert!((pos_after_promote.s3_weight_contrib - 0.04).abs() < 1e-6);
    assert!((pos_after_promote.protective_stop_px - 0.92).abs() < 1e-6);
    assert!((pos_after_promote.funding_accrued_usdc - 1.25).abs() < 1e-4);
    assert_eq!(pos_after_promote.opened_at_ms, Some(legacy_open_ts));

    // Attach tight SL/TP within 0.1% of mark (strictly non-breached) and OIDs
    db.update_position_tpsl_orders(
        work_id,
        0.999,
        OidUpdate::Set("sl-oid-1"),
        1.001,
        OidUpdate::Set("tp-oid-1"),
    )
    .unwrap();

    let (resync_id, newly_adopted) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: arb_id,
            coin: "ARB",
            side: OrderSide::Long,
            entry_price: 0.995,
            mark_price: 1.00,
            quantity: 100.0,
            committed_notional_usdc: 100.0,
            unrealized_pnl_usdc: 0.50,
            portfolio_weight: 0.24,
            protective_stop_px: 0.95,
            take_profit_px: 1.08,
            funding_accrued_usdc: 2.50,
            opened_at_ms: Some(legacy_open_ts),
        })
        .unwrap();
    assert_eq!(resync_id, work_id);
    assert!(!newly_adopted);

    let pos_resynced = db.get_position_by_id(work_id).unwrap().unwrap();
    assert!((pos_resynced.protective_stop_px - 0.999).abs() < 1e-6);
    assert_eq!(
        pos_resynced.protective_stop_oid.as_deref(),
        Some("sl-oid-1")
    );
    assert!((pos_resynced.take_profit_px - 1.001).abs() < 1e-6);
    assert_eq!(pos_resynced.take_profit_oid.as_deref(), Some("tp-oid-1"));
    assert!((pos_resynced.funding_accrued_usdc - 2.50).abs() < 1e-4);
    assert_eq!(pos_resynced.opened_at_ms, Some(legacy_open_ts));
}

#[test]
fn test_pnl_summary_horizon_baseline_funding_and_cap_cutoff() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();

    let eth_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "ETH".to_string(),
            asset_index: 1,
            sz_decimals: 3,
            tick_size: 0.1,
            lot_step_size: 0.001,
            max_leverage: 25.0,
            mark_price: 115.0,
            mid_price: 115.0,
            oracle_price: 115.0,
            funding_rate_hourly: 0.00001,
            day_volume_usdc: 100_000_000.0,
            open_interest_usdc: 50_000_000.0,
            prev_day_price: 110.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .unwrap();

    let now = now_ms();
    let day_ms = 86_400_000i64;
    let two_days_ago = now - 2 * day_ms;
    let twenty_five_h_ago = now - (25 * 3_600_000);
    let two_hours_ago = now - (2 * 3_600_000);
    let ten_days_ago = now - 10 * day_ms;

    // Insert baseline snapshots at 7d+1h ago ($2,000), 25h ago ($2,500), and old 10d snapshot for pruning
    {
        let mut conn = db.get_conn().unwrap();
        for (eq, ts) in [
            (1800.0, ten_days_ago),
            (2000.0, now - 7 * day_ms - 3_600_000),
            (2500.0, twenty_five_h_ago),
        ] {
            conn.exec_drop(
                r#"INSERT INTO portfolio_snapshots (
                       total_equity_usdc, available_cash_usdc, reserved_order_cash_usdc,
                       committed_notional_usdc, net_notional_usdc, gross_leverage_x, net_leverage_x,
                       market_beta_exposure, market_breadth_bull, market_regime_label, risk_tier,
                       leverage_multiplier, s1_gross_weight, s2_gross_weight, s3_gross_weight,
                       working_order_count, open_long_count, open_short_count, eligible_coin_count,
                       created_at_ms, updated_at_ms
                   ) VALUES (
                       :eq, :eq, 0.0, 200.0, 200.0, 0.1, 0.1, 0.1, 0.5, 'NEUTRAL', 'standard',
                       1.0, 0.1, 0.0, 0.0, 0, 1, 0, 10, :ts, :ts
                   )"#,
                params! { "eq" => eq, "ts" => ts },
            )
            .unwrap();
        }
        // Insert old telemetry log for retention pruning test
        conn.exec_drop(
            r#"INSERT INTO telemetry_logs (level, component, message, details_json, created_at_ms, updated_at_ms)
               VALUES ('INFO', 'test', 'old log', NULL, :ts, :ts)"#,
            params! { "ts" => ten_days_ago },
        )
        .unwrap();
    }

    // Insert 1h bar at 25h ago ($110.0 close) for open position opened 2d ago at $100.0 qty 2.0 (start_upnl = +20.0)
    db.upsert_ohlcv_bars(&[BarUpsertInput {
        asset_id: eth_id,
        timeframe: "1h".to_string(),
        timestamp_ms: twenty_five_h_ago,
        open: 108.0,
        high: 111.0,
        low: 107.0,
        close: 110.0,
        volume: 1_000_000.0,
        funding_rate: 0.0,
    }])
    .unwrap();

    // Open position opened 2d ago with current mark $115 -> total uPnL = +$30.0 (24h delta = +$10.0),
    // funding_accrued_usdc = $3.0, fees_paid_usdc = $1.0
    let (open_id, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth_id,
            coin: "ETH",
            side: OrderSide::Long,
            entry_price: 100.0,
            mark_price: 115.0,
            quantity: 2.0,
            committed_notional_usdc: 230.0,
            unrealized_pnl_usdc: 30.0,
            portfolio_weight: 0.1,
            protective_stop_px: 95.0,
            take_profit_px: 130.0,
            funding_accrued_usdc: 3.0,
            opened_at_ms: Some(two_days_ago),
        })
        .unwrap();
    assert!(open_id > 0);
    {
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            "UPDATE positions SET fees_paid_usdc = 1.0 WHERE id = :pid",
            params! { "pid" => open_id },
        )
        .unwrap();
    }

    // Closed trades: 1 trade 2d ago (+$500 realized, +$10 funding, -$2 fee) and 1 trade 2h ago (+$50 realized, +$5 funding, -$1 fee)
    db.reconcile_closed_positions_from_exchange_fills(&[
        ReconciledClosedTradeInput {
            asset_id: eth_id,
            client_order_id: "hl-closed-eth-2d".to_string(),
            exchange_order_id: "hl-2d".to_string(),
            close_epoch_id: None,
            side: OrderSide::Long,
            entry_price: 100.0,
            exit_price: 120.0,
            filled_quantity: 25.0,
            realized_pnl_usdc: 500.0,
            fees_paid_usdc: 2.0,
            funding_accrued_usdc: 10.0,
            created_at_ms: two_days_ago - 60_000,
            closed_at_ms: two_days_ago,
            exit_reason: "TAKE_PROFIT_LONG".to_string(),
        },
        ReconciledClosedTradeInput {
            asset_id: eth_id,
            client_order_id: "hl-closed-eth-2h".to_string(),
            exchange_order_id: "hl-2h".to_string(),
            close_epoch_id: None,
            side: OrderSide::Long,
            entry_price: 110.0,
            exit_price: 112.0,
            filled_quantity: 23.0,
            realized_pnl_usdc: 50.0,
            fees_paid_usdc: 1.0,
            funding_accrued_usdc: 5.0,
            created_at_ms: two_hours_ago - 60_000,
            closed_at_ms: two_hours_ago,
            exit_reason: "TAKE_PROFIT_LONG".to_string(),
        },
    ])
    .unwrap();

    // Before funding_events exist, time-proration fallback prorates open funding (3.0 * ~1d/2d = ~1.50) for Daily
    let fallback_summary = db.compute_pnl_summary(30.0, 2.0, 2562.0).unwrap();
    assert!((fallback_summary.daily.unrealized_pnl_usdc - 11.5).abs() < 0.05);
    assert!((fallback_summary.daily.fees_funding_usdc - 5.5).abs() < 0.05);
    assert!((fallback_summary.weekly.unrealized_pnl_usdc - 32.0).abs() < 1e-6);

    // Upsert per-event funding ledger: +2.00 at 36h ago (outside 24h) and +1.00 at 2h ago (inside 24h)
    let thirty_six_h_ago = now - (36 * 3_600_000);
    let upserted = db
        .upsert_funding_events_batch(&[
            FundingEventUpsertInput {
                asset_id: eth_id,
                coin: "ETH".to_string(),
                event_time_ms: thirty_six_h_ago,
                usdc: 2.0,
                remaining_usdc: 2.0,
                position_szi: 2.0,
                funding_rate: 0.0001,
                dedup_key: "eth-funding-36h".to_string(),
            },
            FundingEventUpsertInput {
                asset_id: eth_id,
                coin: "ETH".to_string(),
                event_time_ms: two_hours_ago,
                usdc: 1.0,
                remaining_usdc: 1.0,
                position_szi: 2.0,
                funding_rate: 0.00005,
                dedup_key: "eth-funding-2h".to_string(),
            },
        ])
        .unwrap();
    assert_eq!(upserted, 2);
    // Re-upserting with identical dedup_key updates in place without duplicating rows
    db.upsert_funding_events_batch(&[FundingEventUpsertInput {
        asset_id: eth_id,
        coin: "ETH".to_string(),
        event_time_ms: two_hours_ago,
        usdc: 1.0,
        remaining_usdc: 1.0,
        position_szi: 2.0,
        funding_rate: 0.00005,
        dedup_key: "eth-funding-2h".to_string(),
    }])
    .unwrap();
    let evs = db.list_funding_events_for_asset(eth_id).unwrap();
    assert_eq!(evs.len(), 2);
    assert_eq!(evs[0].dedup_key, "eth-funding-36h");
    assert_eq!(evs[1].dedup_key, "eth-funding-2h");

    // Windowed funding_events attribution:
    // - Daily (24h): gets only +1.0 open funding and 0 open entry fee (not +2.0 unwindowed!)
    //   closed_rpnl=50.0, horizon_upnl_delta=10.0, open_funding=1.0 -> uPnL=11.0, net=61.0, fees_funding=4.0+1.0=5.0
    // - Weekly (7d): open position opened inside 7d -> full +3.0 funding - 1.0 fee = +2.0
    //   closed_rpnl=550.0, horizon_upnl_delta=30.0, open_fees_funding=2.0 -> uPnL=32.0, net=582.0, fees_funding=12.0+2.0=14.0
    let summary = db.compute_pnl_summary(30.0, 2.0, 2562.0).unwrap();
    assert!((summary.daily.baseline_equity_usdc - 2500.0).abs() < 1e-6);
    assert!((summary.daily.realized_pnl_usdc - 50.0).abs() < 1e-6);
    assert!((summary.daily.unrealized_pnl_usdc - 11.0).abs() < 1e-6);
    assert!((summary.daily.net_pnl_usdc - 61.0).abs() < 1e-6);
    assert!((summary.daily.fees_funding_usdc - 5.0).abs() < 1e-6);
    assert_eq!(summary.daily.trade_count, 1);
    assert!(!summary.daily.is_cap_clamped);

    assert!((summary.weekly.baseline_equity_usdc - 2000.0).abs() < 1e-6);
    assert!((summary.weekly.realized_pnl_usdc - 550.0).abs() < 1e-6);
    assert!((summary.weekly.unrealized_pnl_usdc - 32.0).abs() < 1e-6);
    assert!((summary.weekly.net_pnl_usdc - 582.0).abs() < 1e-6);
    assert!((summary.weekly.fees_funding_usdc - 14.0).abs() < 1e-6);
    assert_eq!(summary.weekly.trade_count, 2);
    assert!(!summary.weekly.is_cap_clamped);
    assert!(!summary.monthly.is_cap_clamped);
    assert!(!summary.ytd.is_cap_clamped);

    // Setting cap_updated_at_ms to 2 days ago clamps weekly/monthly/ytd (when > Jan 1) but NOT daily
    db.set_config_value(
        "risk_capital_cap_updated_at_ms",
        &two_days_ago.to_string(),
        None,
    )
    .unwrap();
    let cap_2d_summary = db.compute_pnl_summary(30.0, 2.0, 2562.0).unwrap();
    assert!(!cap_2d_summary.daily.is_cap_clamped);
    assert!(cap_2d_summary.weekly.is_cap_clamped);
    assert!(cap_2d_summary.monthly.is_cap_clamped);
    assert_eq!(
        cap_2d_summary.ytd.is_cap_clamped,
        two_days_ago > ytd_start_ms()
    );

    // Updating risk_capital_cap_usdc automatically advances risk_capital_cap_updated_at_ms to now
    db.set_config_value("risk_capital_cap_usdc", "1500.0", None)
        .unwrap();
    let cfg_cap = db.load_config().unwrap();
    assert!(cfg_cap.get_i64("risk_capital_cap_updated_at_ms", 0) >= now);
    let post_cap_summary = db.compute_pnl_summary(30.0, 2.0, 2562.0).unwrap();
    assert_eq!(post_cap_summary.daily.trade_count, 0);
    assert_eq!(post_cap_summary.weekly.trade_count, 0);
    assert!(post_cap_summary.daily.is_cap_clamped);
    assert!(post_cap_summary.weekly.is_cap_clamped);
    assert!(post_cap_summary.monthly.is_cap_clamped);
    assert!(post_cap_summary.ytd.is_cap_clamped);
    assert!((post_cap_summary.daily.fees_funding_usdc - 0.0).abs() < 1e-6);

    // Time-proration fallback when funding_events has 0 rows: post-cap window attributes ~0.0 (not +2.0)
    {
        let mut conn = db.get_conn().unwrap();
        conn.query_drop("DELETE FROM funding_events").unwrap();
    }
    let post_cap_fallback = db.compute_pnl_summary(30.0, 2.0, 2562.0).unwrap();
    assert_eq!(post_cap_fallback.daily.trade_count, 0);
    assert_eq!(post_cap_fallback.weekly.trade_count, 0);
    assert!(post_cap_fallback.daily.fees_funding_usdc.abs() < 0.05);
    assert!(post_cap_fallback.weekly.fees_funding_usdc.abs() < 0.05);
    assert!((post_cap_fallback.daily.unrealized_pnl_usdc - 10.0).abs() < 0.05);

    // Verify dedup_key clamping <= 128 chars in upsert_funding_events_batch
    let long_key = "k".repeat(200);
    db.upsert_funding_events_batch(&[FundingEventUpsertInput {
        asset_id: eth_id,
        coin: "ETH".to_string(),
        event_time_ms: two_hours_ago,
        usdc: 0.25,
        remaining_usdc: 0.25,
        position_szi: 2.0,
        funding_rate: 0.00001,
        dedup_key: long_key,
    }])
    .expect("overlong dedup_key must be clamped safely to <= 128 chars");
    let clamped_evs = db.list_funding_events_for_asset(eth_id).unwrap();
    assert_eq!(clamped_evs.len(), 1);
    assert_eq!(clamped_evs[0].dedup_key.chars().count(), 128);

    // Retention pruning deletes >9d snapshots & logs while keeping recent rows
    let (pruned_snaps, pruned_logs) = db.prune_old_snapshots_and_logs(9).unwrap();
    assert_eq!(pruned_snaps, 1);
    assert_eq!(pruned_logs, 1);

    // Telemetry details_json redacts embedded secrets
    db.log_telemetry(
        "INFO",
        "test",
        "Check JSON sanitization",
        Some(json!({
            "private_key": "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
            "nested": {"token": "gAAAAABembeddedtokenvalue123"}
        })),
    )
    .unwrap();
    let logs = db.list_recent_telemetry(5).unwrap();
    let det_str = logs[0].details_json.as_ref().unwrap().to_string();
    assert!(!det_str.contains("0x4c0883"));
    assert!(!det_str.contains("gAAAAABembedded"));
}

#[test]
fn test_batch_mid_priority_open_positions_and_signals() {
    let db = DatabaseManager::new_isolated_test(None).unwrap();

    let btc_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "BTC".to_string(),
            asset_index: 0,
            sz_decimals: 4,
            tick_size: 1.0,
            lot_step_size: 0.0001,
            max_leverage: 20.0,
            mark_price: 90_000.0,
            mid_price: 90_000.0,
            oracle_price: 90_000.0,
            funding_rate_hourly: 0.0,
            day_volume_usdc: 100_000_000.0,
            open_interest_usdc: 50_000_000.0,
            prev_day_price: 89_000.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();

    // 1. Batch mid & priority watch updates
    assert_eq!(db.update_assets_live_mid_batch(&[]).unwrap(), 0);
    assert_eq!(
        db.update_assets_live_mid_batch(&[(btc_id, 90_125.0), (btc_id, -1.0)])
            .unwrap(),
        1
    );
    assert_eq!(
        db.set_assets_priority_watch_batch(&[(btc_id, true)])
            .unwrap(),
        1
    );
    let btc = db.get_asset_by_id(btc_id).unwrap().unwrap();
    assert!((btc.mark_price - 90_000.0).abs() < 1e-6);
    assert!((btc.mid_price - 90_125.0).abs() < 1e-6);
    assert!(btc.is_priority_watched);

    // 2. Batch OPEN position mark & stop update skips unchanged rows
    let (pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: btc_id,
            coin: "BTC",
            side: OrderSide::Long,
            entry_price: 90_000.0,
            mark_price: 90_500.0,
            quantity: 0.1,
            committed_notional_usdc: 9_050.0,
            unrealized_pnl_usdc: 50.0,
            portfolio_weight: 0.25,
            protective_stop_px: 88_000.0,
            take_profit_px: 95_000.0,
            funding_accrued_usdc: 1.25,
            opened_at_ms: Some(1_700_000_000_000),
        })
        .unwrap();
    // Initial sync leaves highest_price_seen = 90_500 and stop = 88_000 -> first call writes 0 if identical
    assert_eq!(
        db.update_open_positions_mark_and_stop_batch(&[(pid, 90_500.0, 88_000.0)])
            .unwrap(),
        0
    );
    // Changing mark & ratcheted stop writes exactly 1 row, and immediate repeat writes 0 rows
    assert_eq!(
        db.update_open_positions_mark_and_stop_batch(&[(pid, 91_000.0, 88_500.0)])
            .unwrap(),
        1
    );
    assert_eq!(
        db.update_open_positions_mark_and_stop_batch(&[(pid, 91_000.0, 88_500.0)])
            .unwrap(),
        0
    );
    let pos = db.get_position_by_id(pid).unwrap().unwrap();
    assert!((pos.current_price - 91_000.0).abs() < 1e-6);
    assert!((pos.protective_stop_px - 88_500.0).abs() < 1e-6);
    assert!((pos.unrealized_pnl_usdc - 100.0).abs() < 1e-6);

    // 3. Batch signal upsert
    let up = db
        .upsert_sleeve_signals_batch(&[aegis_db::SignalUpsertInput {
            asset_id: btc_id,
            mark_price: 91_000.0,
            day_volume_usdc: 100_000_000.0,
            funding_rate_hourly: 0.0001,
            annualized_vol: 0.55,
            market_beta: 1.0,
            s1_mom_score: 0.5,
            s1_carry_score: 0.2,
            s2_trend_alignment: 1,
            s2_vol_surge_ratio: 1.4,
            s3_funding_zscore: 0.3,
            s3_flush_atr_mult: 0.1,
            w_s1_carry_mom: 0.15,
            w_s2_trend_breakout: 0.10,
            w_s3_event_reversion: 0.0,
            raw_combined_weight: 0.25,
            target_weight: 0.25,
            current_weight: 0.25,
            delta_weight: 0.0,
            event_trigger: None,
            rebalance_action: "HOLD".to_string(),
        }])
        .unwrap();
    assert_eq!(up, 1);
    assert_eq!(db.list_sleeve_signals().unwrap().len(), 1);
}

fn sample_asset_input(raw: &str, asset_index: u32, mark: f64, watched: bool) -> AssetUpsertInput {
    AssetUpsertInput {
        raw_symbol_or_coin: raw.to_string(),
        asset_index,
        sz_decimals: 2,
        tick_size: 0.001,
        lot_step_size: 0.1,
        max_leverage: 20.0,
        mark_price: mark,
        mid_price: mark,
        oracle_price: mark,
        funding_rate_hourly: 0.00001,
        day_volume_usdc: 10_000_000.0,
        open_interest_usdc: 5_000_000.0,
        prev_day_price: mark * 0.98,
        is_core_eligible: true,
        is_event_eligible: false,
        is_priority_watched: watched,
    }
}

#[test]
fn test_exchange_assets_batch_canonical_ordering_and_dedup() {
    let canonical_mids: Vec<(i64, f64)> = canonicalize_asset_updates(
        &[
            (30, 1.25),
            (10, 2.50),
            (20, -1.0),
            (30, 1.75),
            (25, f64::NAN),
            (10, 0.0),
            (40, f64::INFINITY),
        ],
        |mid| mid.is_finite() && *mid > 0.0,
    )
    .into_iter()
    .collect();
    assert_eq!(canonical_mids, vec![(10, 2.50), (30, 1.75)]);

    let canonical_prio: Vec<(i64, bool)> = canonicalize_asset_updates(
        &[(30, true), (10, false), (30, false), (20, true), (10, true)],
        |_| true,
    )
    .into_iter()
    .collect();
    assert_eq!(canonical_prio, vec![(10, true), (20, true), (30, false)]);

    let inputs = vec![
        sample_asset_input("VIRTUAL", 1, 1.0, false),
        sample_asset_input("kpepe", 2, 0.010, false),
        sample_asset_input("ASTER", 3, 2.0, true),
        sample_asset_input("virtual/usdc:usdc", 1, 1.5, true),
        sample_asset_input("kcat", 4, 3.0, false),
        sample_asset_input("kCat", 4, 3.5, true),
        sample_asset_input("KPEPE-USDC", 2, 0.015, true),
    ];
    let batch = canonicalize_asset_upserts(&inputs).unwrap();
    assert_eq!(
        batch
            .input_keys
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>(),
        vec![
            "VIRTUAL-USDC",
            "KPEPE-USDC",
            "ASTER-USDC",
            "VIRTUAL-USDC",
            "KCAT-USDC",
            "KCAT-USDC",
            "KPEPE-USDC",
        ]
    );
    assert_eq!(
        batch.ordered.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["ASTER-USDC", "KCAT-USDC", "KPEPE-USDC", "VIRTUAL-USDC"]
    );
    assert!((batch.ordered["VIRTUAL-USDC"].input.mark_price - 1.5).abs() < 1e-9);
    assert!(batch.ordered["VIRTUAL-USDC"].input.is_priority_watched);
    assert_eq!(batch.ordered["KCAT-USDC"].symbol, "kCAT-USDC");
    assert!((batch.ordered["KCAT-USDC"].input.mark_price - 3.5).abs() < 1e-9);
    assert_eq!(batch.ordered["KPEPE-USDC"].symbol, "kPEPE-USDC");
    assert_eq!(batch.ordered["KPEPE-USDC"].coin, "kPEPE");
    assert!((batch.ordered["KPEPE-USDC"].input.mark_price - 0.015).abs() < 1e-9);

    let existing_ids = std::collections::HashMap::from([
        ("VIRTUAL-USDC".to_string(), 1i64),
        ("ASTER-USDC".to_string(), 2i64),
    ]);
    let lock_ordered: Vec<&str> = order_asset_upserts_by_lock_key(&batch.ordered, &existing_ids)
        .into_iter()
        .map(|entry| entry.symbol.as_str())
        .collect();
    assert_eq!(
        lock_ordered,
        vec!["VIRTUAL-USDC", "ASTER-USDC", "kCAT-USDC", "kPEPE-USDC"]
    );

    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let ids = db.upsert_exchange_assets_batch(&inputs).unwrap();
    assert_eq!(ids.len(), 7);
    let virtual_id = ids[0];
    let kpepe_id = ids[1];
    let aster_id = ids[2];
    let kcat_id = ids[4];
    assert_eq!(ids[3], virtual_id);
    assert_eq!(ids[5], kcat_id);
    assert_eq!(ids[6], kpepe_id);

    let virtual_row = db.get_asset_by_id(virtual_id).unwrap().unwrap();
    assert_eq!(virtual_row.symbol, "VIRTUAL-USDC");
    assert!((virtual_row.mark_price - 1.5).abs() < 1e-6);
    assert!(virtual_row.is_priority_watched);

    let kpepe_row = db.get_asset_by_id(kpepe_id).unwrap().unwrap();
    assert_eq!(kpepe_row.symbol, "kPEPE-USDC");
    assert_eq!(kpepe_row.coin, "kPEPE");
    assert!((kpepe_row.mark_price - 0.015).abs() < 1e-6);
    assert!(kpepe_row.is_priority_watched);

    let kcat_row = db.get_asset_by_id(kcat_id).unwrap().unwrap();
    assert!((kcat_row.mark_price - 3.5).abs() < 1e-6);
    assert!(kcat_row.is_priority_watched);

    // Updating an existing mixed-case row ("kCAT-USDC") via uppercase "KCAT" resolves the same id
    let reup_ids = db
        .upsert_exchange_assets_batch(&[sample_asset_input("KCAT", 4, 4.25, false)])
        .unwrap();
    assert_eq!(reup_ids, vec![kcat_id]);
    let kcat_updated = db.get_asset_by_id(kcat_id).unwrap().unwrap();
    assert!((kcat_updated.mark_price - 4.25).abs() < 1e-6);
    assert!(!kcat_updated.is_priority_watched);

    let mut sorted_ids = [virtual_id, aster_id, kpepe_id];
    sorted_ids.sort_unstable();
    let (min_id, mid_id, max_id) = (sorted_ids[0], sorted_ids[1], sorted_ids[2]);
    let pre_min = db.get_asset_by_id(min_id).unwrap().unwrap();
    let pre_mid = db.get_asset_by_id(mid_id).unwrap().unwrap();

    let mid_count = db
        .update_assets_live_mid_batch(&[
            (max_id, 100.0),
            (min_id, 200.0),
            (mid_id, -5.0),
            (max_id, 105.5),
            (min_id, 205.5),
            (min_id, f64::NAN),
        ])
        .unwrap();
    assert_eq!(mid_count, 2);

    let post_min = db.get_asset_by_id(min_id).unwrap().unwrap();
    let post_mid = db.get_asset_by_id(mid_id).unwrap().unwrap();
    let post_max = db.get_asset_by_id(max_id).unwrap().unwrap();
    assert!((post_min.mid_price - 205.5).abs() < 1e-6);
    assert!((post_min.mark_price - pre_min.mark_price).abs() < 1e-6);
    assert!((post_mid.mid_price - pre_mid.mid_price).abs() < 1e-6);
    assert!((post_max.mid_price - 105.5).abs() < 1e-6);

    let prio_count = db
        .set_assets_priority_watch_batch(&[
            (max_id, false),
            (min_id, true),
            (max_id, true),
            (min_id, false),
        ])
        .unwrap();
    assert_eq!(prio_count, 2);
    assert!(
        !db.get_asset_by_id(min_id)
            .unwrap()
            .unwrap()
            .is_priority_watched
    );
    assert!(
        db.get_asset_by_id(max_id)
            .unwrap()
            .unwrap()
            .is_priority_watched
    );
}

#[test]
fn test_exchange_assets_concurrent_batch_updates_no_deadlock() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");

    // Insert VIRTUAL first and ASTER second so id order opposes alphabetical symbol order.
    let virtual_id = db
        .upsert_exchange_asset(&sample_asset_input("VIRTUAL", 1, 1.20, true))
        .unwrap();
    let aster_id = db
        .upsert_exchange_asset(&sample_asset_input("ASTER", 2, 0.85, true))
        .unwrap();
    let sol_id = db
        .upsert_exchange_asset(&sample_asset_input("SOL", 3, 180.0, true))
        .unwrap();
    let btc_id = db
        .upsert_exchange_asset(&sample_asset_input("BTC", 4, 90_000.0, true))
        .unwrap();
    assert!(virtual_id < aster_id);

    std::thread::scope(|scope| {
        let db_mid = db.clone();
        let h_mid = scope.spawn(move || {
            for i in 0..60 {
                let px = 1.0 + (i as f64) * 0.01;
                let updated = db_mid
                    .update_assets_live_mid_batch(&[
                        (btc_id, 90_000.0 + px),
                        (aster_id, 0.85 + px),
                        (sol_id, 180.0 + px),
                        (virtual_id, 1.20 + px),
                    ])
                    .expect("concurrent update_assets_live_mid_batch must not deadlock");
                assert_eq!(updated, 4);
            }
        });

        let db_prio = db.clone();
        let h_prio = scope.spawn(move || {
            for i in 0..60 {
                let flag = i % 2 == 0;
                let updated = db_prio
                    .set_assets_priority_watch_batch(&[
                        (virtual_id, flag),
                        (sol_id, !flag),
                        (aster_id, flag),
                        (btc_id, !flag),
                    ])
                    .expect("concurrent set_assets_priority_watch_batch must not deadlock");
                assert_eq!(updated, 4);
            }
        });

        h_mid.join().unwrap();
        h_prio.join().unwrap();
    });

    std::thread::scope(|scope| {
        let db_a = db.clone();
        let h_a = scope.spawn(move || {
            for i in 0..30 {
                let mark = 1.0 + (i as f64) * 0.01;
                let batch = vec![
                    sample_asset_input("VIRTUAL", 1, mark, true),
                    sample_asset_input("ASTER", 2, mark, false),
                    sample_asset_input("SOL", 3, 180.0 + mark, true),
                    sample_asset_input("BTC", 4, 90_000.0 + mark, false),
                ];
                let ids = db_a
                    .upsert_exchange_assets_batch(&batch)
                    .expect("concurrent upsert_exchange_assets_batch (fwd) must not deadlock");
                assert_eq!(ids, vec![virtual_id, aster_id, sol_id, btc_id]);
            }
        });

        let db_b = db.clone();
        let h_b = scope.spawn(move || {
            for i in 0..30 {
                let mark = 2.0 + (i as f64) * 0.01;
                let batch = vec![
                    sample_asset_input("BTC", 4, 90_000.0 + mark, true),
                    sample_asset_input("SOL", 3, 180.0 + mark, false),
                    sample_asset_input("ASTER", 2, mark, true),
                    sample_asset_input("VIRTUAL", 1, mark, false),
                ];
                let ids = db_b
                    .upsert_exchange_assets_batch(&batch)
                    .expect("concurrent upsert_exchange_assets_batch (rev) must not deadlock");
                assert_eq!(ids, vec![btc_id, sol_id, aster_id, virtual_id]);
            }
        });

        h_a.join().unwrap();
        h_b.join().unwrap();
    });

    // Concurrent upsert_exchange_assets_batch vs update_assets_live_mid_batch & set_assets_priority_watch_batch
    // where VIRTUAL has lower id than ASTER (opposite to alphabetical symbol order).
    std::thread::scope(|scope| {
        let db_upsert = db.clone();
        let h_upsert = scope.spawn(move || {
            for i in 0..60 {
                let mark = 3.0 + (i as f64) * 0.01;
                let batch = vec![
                    sample_asset_input("ASTER", 2, mark, true),
                    sample_asset_input("BTC", 4, 90_000.0 + mark, true),
                    sample_asset_input("VIRTUAL", 1, mark, true),
                    sample_asset_input("SOL", 3, 180.0 + mark, true),
                ];
                let ids = db_upsert.upsert_exchange_assets_batch(&batch).expect(
                    "concurrent upsert_exchange_assets_batch vs live_mid must not deadlock",
                );
                assert_eq!(ids, vec![aster_id, btc_id, virtual_id, sol_id]);
            }
        });

        let db_mid = db.clone();
        let h_mid = scope.spawn(move || {
            for i in 0..60 {
                let px = 4.0 + (i as f64) * 0.01;
                let updated = db_mid
                    .update_assets_live_mid_batch(&[
                        (btc_id, 90_000.0 + px),
                        (sol_id, 180.0 + px),
                        (aster_id, 0.85 + px),
                        (virtual_id, 1.20 + px),
                    ])
                    .expect("concurrent update_assets_live_mid_batch vs upsert must not deadlock");
                assert_eq!(updated, 4);
            }
        });

        let db_prio = db.clone();
        let h_prio = scope.spawn(move || {
            for i in 0..60 {
                let flag = i % 2 == 0;
                let updated = db_prio
                    .set_assets_priority_watch_batch(&[
                        (sol_id, !flag),
                        (btc_id, flag),
                        (virtual_id, !flag),
                        (aster_id, flag),
                    ])
                    .expect(
                        "concurrent set_assets_priority_watch_batch vs upsert must not deadlock",
                    );
                assert_eq!(updated, 4);
            }
        });

        h_upsert.join().unwrap();
        h_mid.join().unwrap();
        h_prio.join().unwrap();
    });
}

#[test]
fn test_critical_secret_load_config_fails_closed_while_non_critical_stays_isolated() {
    let key = aegis_db::generate_fernet_key();
    let db = DatabaseManager::new_isolated_test(Some(&key)).expect("isolated test DB");
    let ts = now_ms();

    // Non-critical broken_secret remains isolated and returns Ok
    {
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES ('broken_secret', 'gAAAAABcorrupt_non_critical', 'secret', 'bad', :ts, :ts)"#,
            params! { "ts" => ts },
        )
        .unwrap();
    }
    let cfg_ok = db.load_config().expect("non-critical broken row isolated");
    assert!(cfg_ok.get("broken_secret").is_none());

    // Corrupt web_api_token must fail closed
    {
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES ('web_api_token', 'gAAAAABcorrupt_web_token', 'secret', 'bad', :ts, :ts)
               ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)"#,
            params! { "ts" => ts },
        )
        .unwrap();
    }
    let err_web = db
        .load_config()
        .expect_err("corrupt web_api_token must fail closed");
    assert!(
        err_web.to_string().contains("web_api_token"),
        "Unexpected error for web_api_token: {err_web}"
    );

    // Fix web_api_token, corrupt private_key -> must also fail closed
    db.set_config_value("web_api_token", "valid-bearer-token-1234", None)
        .unwrap();
    {
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES ('PRIVATE_KEY', 'ENC:gAAAAABcorrupt_private_key', 'secret', 'bad', :ts, :ts)
               ON DUPLICATE KEY UPDATE config_key = VALUES(config_key), config_value = VALUES(config_value)"#,
            params! { "ts" => ts },
        )
        .unwrap();
    }
    let err_pk = db
        .load_config()
        .expect_err("corrupt private_key must fail closed");
    assert!(
        err_pk.to_string().contains("private_key"),
        "Unexpected error for private_key: {err_pk}"
    );
}

#[test]
fn test_set_config_value_normalizes_case_and_updates_cap_epoch() {
    let key = aegis_db::generate_fernet_key();
    let db = DatabaseManager::new_isolated_test(Some(&key)).expect("isolated test DB");
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();

    // Seed an uppercase legacy row in MariaDB directly to confirm ON DUPLICATE KEY UPDATE self-heals config_key
    {
        let mut conn = db.get_conn().unwrap();
        let ts = now_ms();
        conn.exec_drop(
            r#"INSERT INTO system_config
               (config_key, config_value, value_type, description, created_at_ms, updated_at_ms)
               VALUES ('AUTO_REBALANCE_ENABLED', 'false', 'bool', 'legacy uppercase', :ts, :ts)"#,
            params! { "ts" => ts },
        )
        .unwrap();
    }
    db.set_config_value("  Auto_Rebalance_Enabled  ", "true", None)
        .unwrap();

    let before_cap = now_ms();
    db.set_config_value("RISK_CAPITAL_CAP_USDC", "2750.0", None)
        .unwrap();
    db.set_config_value("TELEGRAM_CHAT_ID", "99887766", None)
        .unwrap();

    let rows = db.list_system_config_rows().unwrap();
    assert!(
        rows.iter()
            .any(|r| r.config_key == "auto_rebalance_enabled" && r.config_value == "true"),
        "Mixed-case row should self-heal to lowercase config_key"
    );
    assert!(
        rows.iter()
            .any(|r| r.config_key == "risk_capital_cap_usdc" && r.config_value == "2750.0"),
        "RISK_CAPITAL_CAP_USDC must normalize to lowercase"
    );
    let chat_row = rows
        .iter()
        .find(|r| r.config_key == "telegram_chat_id")
        .expect("telegram_chat_id row");
    assert_eq!(chat_row.value_type, "string");
    assert_eq!(chat_row.config_value, "99887766");

    let cfg = db.load_config().unwrap();
    assert!((cfg.get_f64("risk_capital_cap_usdc", 0.0) - 2750.0).abs() < 1e-9);
    assert!((cfg.get_f64("RISK_CAPITAL_CAP_USDC", 0.0) - 2750.0).abs() < 1e-9);
    assert!(cfg.get_i64("risk_capital_cap_updated_at_ms", 0) >= before_cap);
    assert!(cfg.get_bool("auto_rebalance_enabled", false));
}

#[test]
fn test_migrate_legacy_sqlite_encrypts_plaintext_and_skips_corrupt_ciphertext() {
    let key = aegis_db::generate_fernet_key();
    let db = DatabaseManager::new_isolated_test(Some(&key)).expect("isolated test DB");
    let cipher = db.fernet().clone();
    let already_enc_token = cipher.encrypt(b"existing-bot-token").unwrap();

    let sqlite_path = std::env::temp_dir().join(format!(
        "aegis_legacy_migration_test_{}_{}.db",
        std::process::id(),
        now_ms()
    ));
    {
        let conn = rusqlite::Connection::open(&sqlite_path).unwrap();
        conn.execute(
            "CREATE TABLE system_config (config_key TEXT PRIMARY KEY, config_value TEXT NOT NULL, value_type TEXT, description TEXT)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO system_config (config_key, config_value, value_type, description) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "PRIVATE_KEY",
                "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
                "secret",
                "plaintext legacy private key"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO system_config (config_key, config_value, value_type, description) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "telegram_bot_token",
                &already_enc_token,
                "secret",
                "valid encrypted bot token"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO system_config (config_key, config_value, value_type, description) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "web_api_token",
                "gAAAAABcorrupt_legacy_ciphertext",
                "secret",
                "corrupt encrypted token"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO system_config (config_key, config_value, value_type, description) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "telegram_chat_id",
                "-10099887766",
                "secret",
                "chat id"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO system_config (config_key, config_value, value_type, description) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                "discord_webhook_url",
                "https://discord.com/api/webhooks/123456/plain_webhook_secret",
                "string",
                "plaintext discord webhook"
            ],
        )
        .unwrap();
    }

    let migrated = db
        .migrate_legacy_sqlite_credentials(&sqlite_path)
        .expect("migration should succeed cleanly");
    let _ = std::fs::remove_file(&sqlite_path);
    assert_eq!(migrated, 4);

    let rows = db.list_system_config_rows().unwrap();
    let pk_row = rows.iter().find(|r| r.config_key == "private_key").unwrap();
    assert!(
        aegis_db::is_encrypted_value(&pk_row.config_value),
        "Plaintext legacy private_key must be Fernet-encrypted during migration"
    );
    assert_eq!(pk_row.value_type, "secret");

    let discord_row = rows
        .iter()
        .find(|r| r.config_key == "discord_webhook_url")
        .unwrap();
    assert!(aegis_db::is_encrypted_value(&discord_row.config_value));
    assert_eq!(discord_row.value_type, "secret");

    let chat_row = rows
        .iter()
        .find(|r| r.config_key == "telegram_chat_id")
        .unwrap();
    assert!(!aegis_db::is_encrypted_value(&chat_row.config_value));
    assert_eq!(chat_row.config_value, "-10099887766");
    assert_eq!(chat_row.value_type, "string");

    let web_row = rows
        .iter()
        .find(|r| r.config_key == "web_api_token")
        .unwrap();
    assert!(
        web_row.config_value.is_empty(),
        "Corrupt legacy ciphertext must not be copied blindly into MariaDB"
    );

    let cfg = db.load_config().unwrap();
    assert_eq!(
        cfg.get_str("private_key", ""),
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
    );
    assert_eq!(cfg.get_str("telegram_bot_token", ""), "existing-bot-token");
    assert_eq!(cfg.get_str("telegram_chat_id", ""), "-10099887766");
    assert_eq!(
        cfg.get_str("discord_webhook_url", ""),
        "https://discord.com/api/webhooks/123456/plain_webhook_secret"
    );
}

#[test]
fn test_get_peak_and_latest_equity_since_windows_and_empty() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    assert_eq!(db.get_peak_and_latest_equity_since(0).unwrap(), None);

    let now = now_ms();
    let t1 = now - 7_200_000;
    let t2 = now - 3_600_000;
    let t3 = now - 600_000;
    let t4 = now - 60_000;

    {
        let mut conn = db.get_conn().unwrap();
        for (eq, ts) in [(2_000.0, t1), (1_500.0, t2), (1_400.0, t3), (1_150.0, t4)] {
            conn.exec_drop(
                r#"INSERT INTO portfolio_snapshots (
                       total_equity_usdc, available_cash_usdc, reserved_order_cash_usdc,
                       committed_notional_usdc, net_notional_usdc, gross_leverage_x, net_leverage_x,
                       market_beta_exposure, market_breadth_bull, market_regime_label, risk_tier,
                       leverage_multiplier, s1_gross_weight, s2_gross_weight, s3_gross_weight,
                       working_order_count, open_long_count, open_short_count, eligible_coin_count,
                       created_at_ms, updated_at_ms
                   ) VALUES (
                       :eq, :eq, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5, 'NEUTRAL', 'extreme',
                       1.85, 0.0, 0.0, 0.0, 0, 0, 0, 0, :ts, :ts
                   )"#,
                params! { "eq" => eq, "ts" => ts },
            )
            .unwrap();
        }
    }

    let (peak_all, latest_all) = db
        .get_peak_and_latest_equity_since(t1 - 1)
        .unwrap()
        .unwrap();
    assert!((peak_all - 2_000.0).abs() < 1e-6);
    assert!((latest_all - 1_150.0).abs() < 1e-6);

    let (peak_recent, latest_recent) = db.get_peak_and_latest_equity_since(t2).unwrap().unwrap();
    assert!((peak_recent - 1_500.0).abs() < 1e-6);
    assert!((latest_recent - 1_150.0).abs() < 1e-6);

    // Future since_ms with no matching positive rows falls back to latest_equity.max(0.0)
    let (peak_future, latest_future) = db
        .get_peak_and_latest_equity_since(now + 10_000)
        .unwrap()
        .unwrap();
    assert!((peak_future - 1_150.0).abs() < 1e-6);
    assert!((latest_future - 1_150.0).abs() < 1e-6);
}

#[test]
fn test_order_audit_reason_clamping_prevents_mysql_1406() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = db
        .upsert_exchange_asset(&sample_asset_input("SOL", 4, 180.0, true))
        .unwrap();

    let long_utf8_reason = "🔥".repeat(280);
    assert!(long_utf8_reason.chars().count() == 280);
    assert!(long_utf8_reason.len() > 1000);

    let pos_id = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-audit-clamp-1".to_string(),
            side: OrderSide::Long,
            limit_price: 180.0,
            requested_quantity: 1.0,
            reserved_margin_usdc: 100.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 170.0,
            take_profit_px: None,
            exchange_order_id: Some("hl-oid-clamp-1".to_string()),
            reason: long_utf8_reason.clone(),
        })
        .expect("create_working_order with >255 char reason must succeed");

    db.record_working_order_chase(
        pos_id,
        180.5,
        "CHASING_ALO",
        Some("hl-oid-clamp-2"),
        &long_utf8_reason,
    )
    .expect("record_working_order_chase with >255 char reason must succeed");

    db.promote_working_to_open(
        pos_id,
        180.5,
        1.0,
        0.1,
        170.0,
        None,
        Some("hl-oid-clamp-2"),
        &long_utf8_reason,
    )
    .expect("promote_working_to_open with >255 char reason must succeed");

    db.close_position(pos_id, 182.0, 0.1, &long_utf8_reason)
        .expect("close_position with >255 char reason must succeed");

    let pos2_id = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-audit-clamp-2".to_string(),
            side: OrderSide::Short,
            limit_price: 182.0,
            requested_quantity: 1.0,
            reserved_margin_usdc: 100.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 190.0,
            take_profit_px: None,
            exchange_order_id: Some("hl-oid-clamp-3".to_string()),
            reason: "short entry".to_string(),
        })
        .unwrap();
    db.cancel_working_order(pos2_id, &long_utf8_reason)
        .expect("cancel_working_order with >255 char reason must succeed");

    let audits = db.list_recent_order_audits(10).unwrap();
    let clamped: Vec<_> = audits
        .iter()
        .filter(|a| a.position_id == pos_id || a.position_id == pos2_id)
        .collect();
    assert!(clamped.len() >= 5);
    assert!(clamped
        .iter()
        .filter(|a| a.reason.starts_with('🔥'))
        .all(|a| a.reason.chars().count() == 250));
}

#[test]
fn test_vault_config_seed_and_position_pnl_percentages() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let cfg = db.load_config().unwrap();
    assert_eq!(
        cfg.get("live_vault_info_json").map(String::as_str),
        Some("")
    );

    let sol_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "SOL".to_string(),
            asset_index: 2,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: 200.0,
            mid_price: 200.0,
            oracle_price: 200.0,
            funding_rate_hourly: 0.00001,
            day_volume_usdc: 50_000_000.0,
            open_interest_usdc: 20_000_000.0,
            prev_day_price: 190.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();

    // Open position: entry=190.0, qty=5.0 (notional=950.0), uPnL=+50.0 -> +5.26%
    let (open_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol_id,
            coin: "SOL",
            side: OrderSide::Long,
            entry_price: 190.0,
            mark_price: 200.0,
            quantity: 5.0,
            committed_notional_usdc: 1000.0,
            unrealized_pnl_usdc: 50.0,
            portfolio_weight: 0.2,
            protective_stop_px: 180.0,
            take_profit_px: 220.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 120_000),
        })
        .unwrap();
    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].position_id, open_pid);
    assert!(held[0].created_at_ms > 0);
    assert!((held[0].unrealized_pnl_pct - 5.26).abs() < 1e-6);

    // Close the position (sets committed_notional_usdc = 0.0 in MariaDB!)
    db.close_position(open_pid, 200.0, 0.5, "TEST_TP").unwrap();
    let raw_closed = db.get_position_by_id(open_pid).unwrap().unwrap();
    assert_eq!(raw_closed.status, "CLOSED");
    assert_eq!(raw_closed.committed_notional_usdc, 0.0);

    // list_recent_closed_positions_view computes realized_pnl_pct from entry_price * filled_quantity
    // gross=50.0, fee=0.5 => realized_pnl=49.5; notional=190*5=950 => 49.5/950*100 = 5.21%
    let closed_views = db.list_recent_closed_positions_view(10).unwrap();
    assert_eq!(closed_views.len(), 1);
    assert_eq!(closed_views[0].position_id, open_pid);
    assert_eq!(closed_views[0].symbol, "SOL-USDC");
    assert_eq!(closed_views[0].coin, "SOL");
    assert!((closed_views[0].entry_notional_usdc - 950.0).abs() < 1e-6);
    assert!((closed_views[0].realized_pnl_usdc - 49.5).abs() < 1e-4);
    assert!((closed_views[0].realized_pnl_pct - 5.21).abs() < 1e-6);
}

#[test]
fn test_pnl_summary_ignores_zero_volume_stale_candles_and_applies_hl_portfolio_cache() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let now = now_ms();
    let day_ms = 86_400_000i64;
    let two_days_ago = now - 2 * day_ms;
    let three_days_ago = now - 3 * day_ms;
    let twenty_five_h_ago = now - 25 * 3_600_000;

    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_usdc", "1000.0", None)
        .unwrap();
    // Place cap update 3 days ago so Daily (24h) is unclamped while Weekly/Monthly/YTD are clamped.
    db.set_config_value(
        "risk_capital_cap_updated_at_ms",
        &three_days_ago.to_string(),
        None,
    )
    .unwrap();

    let met_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "MET".to_string(),
            asset_index: 5,
            sz_decimals: 1,
            tick_size: 0.00001,
            lot_step_size: 1.0,
            max_leverage: 3.0,
            mark_price: 0.18872,
            mid_price: 0.18872,
            oracle_price: 0.18872,
            funding_rate_hourly: 0.0005,
            day_volume_usdc: 25_000.0,
            open_interest_usdc: 100_000.0,
            prev_day_price: 0.23652,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .unwrap();

    // Open LONG 9614 @ 0.23227, current mark 0.18872 -> current_upnl = -418.6897, cumulative funding = +698.08
    let _ = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: met_id,
            coin: "MET",
            side: OrderSide::Long,
            entry_price: 0.23227,
            mark_price: 0.18872,
            quantity: 9614.0,
            committed_notional_usdc: 1814.35,
            unrealized_pnl_usdc: -418.69,
            portfolio_weight: 0.18,
            protective_stop_px: 0.18165,
            take_profit_px: 0.25086,
            funding_accrued_usdc: 698.08,
            opened_at_ms: Some(two_days_ago - day_ms),
        })
        .unwrap();

    // Carry-forward illiquid 1h bar at 25h ago with volume = 0.0 and stale close = 0.23652
    db.upsert_ohlcv_bars(&[BarUpsertInput {
        asset_id: met_id,
        timeframe: "1h".to_string(),
        timestamp_ms: twenty_five_h_ago,
        open: 0.23652,
        high: 0.23652,
        low: 0.23652,
        close: 0.23652,
        volume: 0.0,
        funding_rate: 0.0,
    }])
    .unwrap();

    // Record 24h funding event +268.53 inside last 24h
    db.upsert_funding_events_batch(&[FundingEventUpsertInput {
        asset_id: met_id,
        coin: "MET".to_string(),
        event_time_ms: now - 3_600_000,
        usdc: 268.53,
        remaining_usdc: 268.53,
        position_szi: 9614.0,
        funding_rate: 0.0005,
        dedup_key: "met-24h-funding".to_string(),
    }])
    .unwrap();

    // 1. Local ledger must ignore the volume=0 stale candle (no phantom -$459.55 drop)
    let local_summary = db.compute_pnl_summary(-418.69, 698.08, 679.15).unwrap();
    assert_eq!(local_summary.daily.pnl_source, "LOCAL_LEDGER");
    assert!((local_summary.daily.unrealized_pnl_usdc - 268.53).abs() < 0.05);
    assert!((local_summary.daily.net_pnl_usdc - 268.53).abs() < 0.05);
    assert!(!local_summary.daily.is_cap_clamped);
    assert!(local_summary.weekly.is_cap_clamped);

    // 2. Populate fresh CachedHlPortfolioPnl JSON
    let hl_cache = aegis_core::CachedHlPortfolioPnl {
        synced_at_ms: now - 5_000,
        perp_day: aegis_core::HlPortfolioWindowCache {
            pnl_history: vec![(now - day_ms - 1_800_000, 0.0), (now, 242.70)],
            vlm: 1412.91,
        },
        perp_week: aegis_core::HlPortfolioWindowCache {
            pnl_history: vec![
                (three_days_ago - 3_600_000, -100.0),
                (three_days_ago + 3_600_000, -300.0),
                (now, 362.52),
            ],
            vlm: 64604.1,
        },
        perp_month: aegis_core::HlPortfolioWindowCache {
            pnl_history: vec![
                (three_days_ago - 3_600_000, -100.0),
                (three_days_ago + 3_600_000, -300.0),
                (now, 362.52),
            ],
            vlm: 83291.0,
        },
        perp_all_time: aegis_core::HlPortfolioWindowCache {
            pnl_history: vec![
                (three_days_ago - 3_600_000, -100.0),
                (three_days_ago + 3_600_000, -300.0),
                (now, 362.52),
            ],
            vlm: 83291.0,
        },
        ..Default::default()
    };
    db.set_config_value(
        "live_exchange_portfolio_pnl_json",
        &serde_json::to_string(&hl_cache).unwrap(),
        None,
    )
    .unwrap();

    // Closed trade 2h ago (+12.00 realized) verifies exact 24h interpolation + live unrealized + Prior M2M additive identity
    db.reconcile_closed_positions_from_exchange_fills(&[ReconciledClosedTradeInput {
        asset_id: met_id,
        client_order_id: "hl-closed-met-2h".to_string(),
        exchange_order_id: "hl-met-2h".to_string(),
        close_epoch_id: None,
        side: OrderSide::Long,
        entry_price: 0.20,
        exit_price: 0.21,
        filled_quantity: 120.0,
        realized_pnl_usdc: 12.0,
        fees_paid_usdc: 0.5,
        funding_accrued_usdc: 0.5,
        created_at_ms: now - 7_200_000,
        closed_at_ms: now - 7_200_000,
        exit_reason: "EXCHANGE_CLOSED".to_string(),
    }])
    .unwrap();

    let hl_summary = db.compute_pnl_summary(-418.69, 698.08, 679.15).unwrap();
    assert_eq!(hl_summary.daily.pnl_source, "HL_PORTFOLIO");
    assert!(!hl_summary.daily.is_cap_clamped);
    // perp_day starts 30m before now - 24h, so exact 24h interpolation yields 242.70 * 86_400_000 / 88_200_000 ≈ 237.75
    assert!((hl_summary.daily.net_pnl_usdc - 237.75).abs() < 0.05);
    assert!((hl_summary.daily.realized_pnl_usdc - 12.0).abs() < 1e-6);
    assert!((hl_summary.daily.unrealized_pnl_usdc - 279.39).abs() < 1e-6);
    assert!(
        (hl_summary.daily.realized_pnl_usdc
            + hl_summary.daily.unrealized_pnl_usdc
            + hl_summary.daily.m2m_carry_usdc
            - hl_summary.daily.net_pnl_usdc)
            .abs()
            < 1e-6
    );
    assert!((hl_summary.daily.m2m_carry_usdc - (-53.64)).abs() < 0.05);

    // Weekly clamped at three_days_ago sits halfway between (-100) and (-300) => -200 => delta = 362.52 - (-200) = 562.52
    assert_eq!(hl_summary.weekly.pnl_source, "HL_PORTFOLIO");
    assert!(hl_summary.weekly.is_cap_clamped);
    assert!((hl_summary.weekly.net_pnl_usdc - 562.52).abs() < 1e-4);

    // 3. Stale cache (>10 min old) falls back to LOCAL_LEDGER
    let stale_cache = aegis_core::CachedHlPortfolioPnl {
        synced_at_ms: now - 700_000,
        ..hl_cache.clone()
    };
    db.set_config_value(
        "live_exchange_portfolio_pnl_json",
        &serde_json::to_string(&stale_cache).unwrap(),
        None,
    )
    .unwrap();
    let stale_summary = db.compute_pnl_summary(-418.69, 698.08, 679.15).unwrap();
    assert_eq!(stale_summary.daily.pnl_source, "LOCAL_LEDGER");
    assert!((stale_summary.daily.net_pnl_usdc - 280.53).abs() < 0.05);

    // 4. Cap updated AFTER synced_at_ms must also fall back to LOCAL_LEDGER for all horizons clamped by it
    db.set_config_value(
        "risk_capital_cap_updated_at_ms",
        &(now - 1_000).to_string(),
        None,
    )
    .unwrap();
    let mid_cache = aegis_core::CachedHlPortfolioPnl {
        synced_at_ms: now - 5_000,
        ..hl_cache
    };
    db.set_config_value(
        "live_exchange_portfolio_pnl_json",
        &serde_json::to_string(&mid_cache).unwrap(),
        None,
    )
    .unwrap();
    let post_cap_fallback = db.compute_pnl_summary(-418.69, 698.08, 679.15).unwrap();
    assert_eq!(post_cap_fallback.daily.pnl_source, "LOCAL_LEDGER");
    assert_eq!(post_cap_fallback.weekly.pnl_source, "LOCAL_LEDGER");
}

fn seed_test_asset(db: &DatabaseManager, coin: &str, idx: u32, mark: f64) -> i64 {
    db.upsert_exchange_asset(&AssetUpsertInput {
        raw_symbol_or_coin: coin.to_string(),
        asset_index: idx,
        sz_decimals: 2,
        tick_size: 0.0001,
        lot_step_size: 0.01,
        max_leverage: 20.0,
        mark_price: mark,
        mid_price: mark,
        oracle_price: mark,
        funding_rate_hourly: 0.0000125,
        day_volume_usdc: 25_000_000.0,
        open_interest_usdc: 10_000_000.0,
        prev_day_price: mark * 0.99,
        is_core_eligible: true,
        is_event_eligible: true,
        is_priority_watched: false,
    })
    .expect("seed_test_asset")
}

#[test]
fn test_r14_h6_non_destructive_init_schema_preserves_rows() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let btc_id = seed_test_asset(&db, "BTC", 0, 90_000.0);
    db.set_config_value("custom_user_setting", "keep-me-alive", Some("test setting"))
        .unwrap();
    let pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: btc_id,
            client_order_id: "coid-h6-preserve".to_string(),
            side: OrderSide::Long,
            limit_price: 89_500.0,
            requested_quantity: 0.1,
            reserved_margin_usdc: 500.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 85_000.0,
            take_profit_px: Some(98_000.0),
            exchange_order_id: Some("hl-h6-1".to_string()),
            reason: "preserve across init_schema".to_string(),
        })
        .unwrap();

    // Re-running init_schema() repeatedly must never wipe user config, assets, or positions.
    db.init_schema().expect("first idempotent init_schema");
    db.init_schema().expect("second idempotent init_schema");

    let cfg = db.load_config().unwrap();
    assert_eq!(cfg.get_str("custom_user_setting", ""), "keep-me-alive");
    assert!(db.get_asset_by_id(btc_id).unwrap().is_some());
    assert!(db.get_position_by_id(pid).unwrap().is_some());

    // Explicit destructive reset with ephemeral token recreates empty canonical tables cleanly.
    db.reset_schema_destructive(DestructiveResetToken::ephemeral_test_only())
        .expect("destructive reset allowed on ephemeral DB");
    assert!(db.get_asset_by_id(btc_id).unwrap().is_none());
    assert!(db.get_position_by_id(pid).unwrap().is_none());
}

#[test]
fn test_p1_acct_multi_ioc_close_epoch_aggregation_and_legacy_self_heal() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let avax_id = seed_test_asset(&db, "AVAX", 6, 24.50);
    let base_ts = now_ms() - 120_000;

    // 1. AVAX 2-IOC fills within 850ms close epoch collapse into 1 canonical CLOSED row
    let slice1 = ReconciledClosedTradeInput {
        asset_id: avax_id,
        client_order_id: "hl-closed-avax-ioc1".to_string(),
        exchange_order_id: "eoid-avax-1".to_string(),
        close_epoch_id: Some("epoch-avax-short-1".to_string()),
        side: OrderSide::Short,
        entry_price: 26.00,
        exit_price: 24.00,
        filled_quantity: 5.0,
        realized_pnl_usdc: 10.00,
        fees_paid_usdc: 0.25,
        funding_accrued_usdc: 0.40,
        created_at_ms: base_ts - 60_000,
        closed_at_ms: base_ts,
        exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
    };
    let slice2 = ReconciledClosedTradeInput {
        asset_id: avax_id,
        client_order_id: "hl-closed-avax-ioc2".to_string(),
        exchange_order_id: "eoid-avax-2".to_string(),
        close_epoch_id: Some("epoch-avax-short-1".to_string()),
        side: OrderSide::Short,
        entry_price: 26.00,
        exit_price: 24.30,
        filled_quantity: 10.0,
        realized_pnl_usdc: 17.00,
        fees_paid_usdc: 0.50,
        funding_accrued_usdc: 0.60,
        created_at_ms: base_ts - 59_500,
        closed_at_ms: base_ts + 850,
        exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
    };

    let merged_preview = aggregate_close_epoch_trades(&[slice1.clone(), slice2.clone()]);
    assert_eq!(merged_preview.len(), 1);
    assert!((merged_preview[0].filled_quantity - 15.0).abs() < 1e-6);
    // VWAP exit_price = (5*24.0 + 10*24.3)/15 = 24.20
    assert!((merged_preview[0].exit_price - 24.20).abs() < 1e-6);

    let count1 = db
        .reconcile_closed_positions_from_exchange_fills(&[slice1.clone(), slice2.clone()])
        .unwrap();
    assert_eq!(count1, 1);

    // Re-running idempotently with slices in reverse order preserves exactly 1 row
    let count_idem = db
        .reconcile_closed_positions_from_exchange_fills(&[slice2, slice1])
        .unwrap();
    assert_eq!(count_idem, 1);
    let closed_after_epoch1 = db.list_closed_positions(20).unwrap();
    assert_eq!(closed_after_epoch1.len(), 1);
    assert!((closed_after_epoch1[0].filled_quantity - 15.0).abs() < 1e-6);
    assert!((closed_after_epoch1[0].exit_price.unwrap() - 24.20).abs() < 1e-4);
    assert!((closed_after_epoch1[0].realized_pnl_usdc - 27.0).abs() < 1e-4);
    assert!((closed_after_epoch1[0].fees_paid_usdc - 0.75).abs() < 1e-4);

    // 2. Distinct reopen + reclose round 30s later creates a second CLOSED row without aliasing
    let epoch2_trade = ReconciledClosedTradeInput {
        asset_id: avax_id,
        client_order_id: "hl-closed-avax-reopen".to_string(),
        exchange_order_id: "eoid-avax-3".to_string(),
        close_epoch_id: None,
        side: OrderSide::Short,
        entry_price: 25.00,
        exit_price: 24.00,
        filled_quantity: 4.0,
        realized_pnl_usdc: 4.00,
        fees_paid_usdc: 0.15,
        funding_accrued_usdc: 0.05,
        created_at_ms: base_ts + 35_000,
        closed_at_ms: base_ts + 45_000,
        exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
    };
    assert_eq!(
        db.reconcile_closed_positions_from_exchange_fills(&[epoch2_trade])
            .unwrap(),
        1
    );
    assert_eq!(db.list_closed_positions(20).unwrap().len(), 2);

    // 3. Self-heal historical split `hl-closed-%` rows while repointing order_audit FKs cleanly
    let sol_id = seed_test_asset(&db, "SOL", 5, 180.0);
    let split_ts = now_ms() - 50_000;
    let (id1, id2): (i64, i64) = {
        let mut conn = db.get_conn().unwrap();
        for (coid, qty, pnl, cts) in [
            ("hl-closed-sol-legacy-1", 2.0, 5.0, split_ts),
            ("hl-closed-sol-legacy-2", 3.0, 8.0, split_ts + 1_200),
        ] {
            conn.exec_drop(
                r#"INSERT INTO positions (
                       asset_id, client_order_id, exchange_order_id, side, status, fsm_state,
                       requested_quantity, filled_quantity, entry_price, current_price, exit_price,
                       realized_pnl_usdc, fees_paid_usdc, funding_accrued_usdc, exit_reason,
                       opened_at_ms, closed_at_ms, created_at_ms, updated_at_ms
                   ) VALUES (
                       :aid, :coid, :eoid, 'LONG', 'CLOSED', 'CLOSED',
                       :qty, :qty, 175.0, 180.0, 180.0,
                       :pnl, 0.20, 0.10, 'HL_USER_FILL_RECONCILED',
                       :ots, :cts, :ots, :cts
                   )"#,
                params! {
                    "aid" => sol_id,
                    "coid" => coid,
                    "eoid" => format!("eo-{coid}"),
                    "qty" => qty,
                    "pnl" => pnl,
                    "ots" => cts - 10_000,
                    "cts" => cts,
                },
            )
            .unwrap();
        }
        let id1: i64 = conn
            .exec_first(
                "SELECT id FROM positions WHERE client_order_id = 'hl-closed-sol-legacy-1'",
                (),
            )
            .unwrap()
            .unwrap();
        let id2: i64 = conn
            .exec_first(
                "SELECT id FROM positions WHERE client_order_id = 'hl-closed-sol-legacy-2'",
                (),
            )
            .unwrap()
            .unwrap();

        // Attach order_audit FK records to BOTH split fragments (`id1` and `id2`) so self-healing
        // deterministically keeps one survivor (`id2`, highest id with audit) and repoints `id1`'s audit FK.
        for (pid, coid, qty, ts) in [
            (id1, "hl-closed-sol-legacy-1", 2.0, split_ts),
            (id2, "hl-closed-sol-legacy-2", 3.0, split_ts + 1_200),
        ] {
            conn.exec_drop(
                r#"INSERT INTO order_audit (
                       position_id, asset_id, client_order_id, side, from_state, to_state,
                       requested_qty, filled_qty, limit_price, avg_fill_price,
                       reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
                   ) VALUES (
                       :pid, :aid, :coid, 'LONG', 'FILLED_OPEN', 'CLOSED',
                       :qty, :qty, 180.0, 180.0, 0.0, 0.0, 0.20, 'legacy split audit', :ts, :ts
                   )"#,
                params! { "pid" => pid, "aid" => sol_id, "coid" => coid, "qty" => qty, "ts" => ts },
            )
            .unwrap();
        }
        (id1, id2)
    };

    let heal_inputs = [
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-legacy-1".to_string(),
            exchange_order_id: "eo-hl-closed-sol-legacy-1".to_string(),
            close_epoch_id: Some("sol-heal-epoch".to_string()),
            side: OrderSide::Long,
            entry_price: 175.0,
            exit_price: 179.0,
            filled_quantity: 2.0,
            realized_pnl_usdc: 8.0,
            fees_paid_usdc: 0.20,
            funding_accrued_usdc: 0.10,
            created_at_ms: split_ts - 10_000,
            closed_at_ms: split_ts,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-legacy-2".to_string(),
            exchange_order_id: "eo-hl-closed-sol-legacy-2".to_string(),
            close_epoch_id: Some("sol-heal-epoch".to_string()),
            side: OrderSide::Long,
            entry_price: 175.0,
            exit_price: 181.0,
            filled_quantity: 3.0,
            realized_pnl_usdc: 18.0,
            fees_paid_usdc: 0.30,
            funding_accrued_usdc: 0.15,
            created_at_ms: split_ts - 9_000,
            closed_at_ms: split_ts + 1_200,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
    ];
    assert_eq!(
        db.reconcile_closed_positions_from_exchange_fills(&heal_inputs)
            .unwrap(),
        1
    );

    // Fragment id1 must be merged into survivor id2, with both order_audit rows repointed to id2
    assert!(db.get_position_by_id(id1).unwrap().is_none());
    let healed = db.get_position_by_id(id2).unwrap().unwrap();
    assert!((healed.filled_quantity - 5.0).abs() < 1e-6);
    assert!((healed.realized_pnl_usdc - 26.0).abs() < 1e-4);

    let mut conn = db.get_conn().unwrap();
    let audit_pid_count: i64 = conn
        .exec_first(
            "SELECT COUNT(*) FROM order_audit WHERE position_id = :pid",
            params! { "pid" => id2 },
        )
        .unwrap()
        .unwrap();
    assert_eq!(audit_pid_count, 2);
}

#[test]
fn test_r14_m13_retention_pruning_preserves_anchors_and_active_records() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let btc_id = seed_test_asset(&db, "BTC", 0, 90_000.0);
    let eth_id = seed_test_asset(&db, "ETH", 1, 3_000.0);
    let doge_id = seed_test_asset(&db, "DOGE", 2, 0.25);

    db.set_config_value("retention_snapshots_days", "30", None)
        .unwrap();
    let cfg = db.load_config().unwrap();
    let policy = RetentionPolicy::from_config(&cfg);
    assert_eq!(policy.snapshots_days, 30);

    // BTC has an active OPEN position -> BTC order_audit & BTC ohlcv_bars must be preserved
    let (btc_open_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: btc_id,
            coin: "BTC",
            side: OrderSide::Long,
            entry_price: 88_000.0,
            mark_price: 90_000.0,
            quantity: 0.5,
            committed_notional_usdc: 45_000.0,
            unrealized_pnl_usdc: 1_000.0,
            portfolio_weight: 0.4,
            protective_stop_px: 84_000.0,
            take_profit_px: 98_000.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 210 * 86_400_000),
        })
        .unwrap();

    // ETH has only an old CLOSED position -> its old order_audit can be pruned
    let eth_closed_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: eth_id,
            client_order_id: "coid-eth-old-closed".to_string(),
            side: OrderSide::Long,
            limit_price: 3_000.0,
            requested_quantity: 1.0,
            reserved_margin_usdc: 2_000.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 2_800.0,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "old eth trade".to_string(),
        })
        .unwrap();
    db.cancel_working_order(eth_closed_pid, "old cancelled")
        .unwrap();

    let day_ms = 86_400_000i64;
    // Align day-45 to 02:00 UTC so +3600s stays on the exact same UTC calendar day
    let now = now_ms();
    let day_45_midnight = ((now - 45 * day_ms) / day_ms) * day_ms;
    let day_45_a = day_45_midnight + 2 * 3_600_000;
    let day_45_b = day_45_midnight + 3 * 3_600_000;
    let day_410 = now - 410 * day_ms;
    let day_1 = now - day_ms;
    let day_220 = now - 220 * day_ms;
    let day_200 = now - 200 * day_ms;
    let day_40 = now - 40 * day_ms;
    let day_20 = now - 20 * day_ms;

    {
        let mut conn = db.get_conn().unwrap();
        // Snapshots: 2 on Day-45 (same UTC day -> 1 daily anchor preserved!), 1 on Day-410 (pruned), 1 on Day-1 (kept)
        for (eq, ts) in [
            (10_100.0, day_45_a),
            (10_200.0, day_45_b),
            (9_000.0, day_410),
            (11_000.0, day_1),
        ] {
            conn.exec_drop(
                r#"INSERT INTO portfolio_snapshots (
                       total_equity_usdc, uncapped_equity_usdc, available_cash_usdc, reserved_order_cash_usdc,
                       committed_notional_usdc, net_notional_usdc, gross_leverage_x, net_leverage_x,
                       market_beta_exposure, market_breadth_bull, market_regime_label, risk_tier,
                       leverage_multiplier, s1_gross_weight, s2_gross_weight, s3_gross_weight,
                       working_order_count, open_long_count, open_short_count, eligible_coin_count,
                       created_at_ms, updated_at_ms
                   ) VALUES (
                       :eq, :eq, :eq, 0, 0, 0, 0, 0, 0, 0.5, 'BULL', 'extreme', 1.85,
                       0, 0, 0, 0, 0, 0, 10, :ts, :ts
                   )"#,
                params! { "eq" => eq, "ts" => ts },
            )
            .unwrap();
        }

        // Old & fresh telemetry logs
        conn.exec_drop(
            "INSERT INTO telemetry_logs (level, component, message, created_at_ms, updated_at_ms) VALUES ('INFO', 't', 'old', :ts1, :ts1), ('INFO', 't', 'new', :ts2, :ts2)",
            params! { "ts1" => day_20, "ts2" => day_1 },
        )
        .unwrap();

        // Age order_audit rows to day_40 for both BTC (OPEN) and ETH (CANCELLED)
        conn.exec_drop(
            "UPDATE order_audit SET created_at_ms = :ts WHERE position_id IN (:p1, :p2)",
            params! { "ts" => day_40, "p1" => btc_open_pid, "p2" => eth_closed_pid },
        )
        .unwrap();

        // Cooldowns: 1 inactive old (pruned), 1 active old (preserved)
        conn.exec_drop(
            r#"INSERT INTO asset_cooldowns (asset_id, cooldown_type, reason, triggered_at_ms, expires_at_ms, is_active, created_at_ms, updated_at_ms)
               VALUES (:doge, 'STOP_OUT', 'old_inactive', :t, :e, 0, :t, :t),
                      (:eth,  'STOP_OUT', 'old_active',   :t, :e, 1, :t, :t)"#,
            params! { "doge" => doge_id, "eth" => eth_id, "t" => day_20 - 7_200_000, "e" => day_20 },
        )
        .unwrap();

        // Alert queue: 1 DELIVERED old (pruned), 1 PENDING old (preserved)
        conn.exec_drop(
            r#"INSERT INTO alert_queue (provider, event_type, severity, message, status, attempts, next_retry_ms, created_at_ms, updated_at_ms)
               VALUES ('telegram', 'T1', 'INFO', 'old delivered', 'DELIVERED', 1, 0, :ts, :ts),
                      ('telegram', 'T2', 'INFO', 'old pending',   'PENDING',   0, 0, :ts, :ts)"#,
            params! { "ts" => day_40 },
        )
        .unwrap();
    }

    // OHLCV bars: BTC at 200d (protected by BTC OPEN position at 210d -> kept) & DOGE at 220d (pruned)
    db.upsert_ohlcv_bars(&[
        BarUpsertInput {
            asset_id: btc_id,
            timeframe: "1h".to_string(),
            timestamp_ms: day_200,
            open: 50_000.0,
            high: 51_000.0,
            low: 49_000.0,
            close: 50_500.0,
            volume: 1000.0,
            funding_rate: 0.0,
        },
        BarUpsertInput {
            asset_id: doge_id,
            timeframe: "1h".to_string(),
            timestamp_ms: day_220,
            open: 0.20,
            high: 0.21,
            low: 0.19,
            close: 0.205,
            volume: 1000.0,
            funding_rate: 0.0,
        },
    ])
    .unwrap();

    // Funding events 400d old: 1 settled (remaining=0 -> pruned), 1 unsettled (remaining=5 -> kept)
    db.upsert_funding_events_batch(&[
        FundingEventUpsertInput {
            asset_id: doge_id,
            coin: "DOGE".to_string(),
            event_time_ms: day_410,
            usdc: 5.0,
            remaining_usdc: 0.0,
            position_szi: 1000.0,
            funding_rate: 0.0001,
            dedup_key: "prune-settled-1".to_string(),
        },
        FundingEventUpsertInput {
            asset_id: btc_id,
            coin: "BTC".to_string(),
            event_time_ms: day_410,
            usdc: 12.5,
            remaining_usdc: 12.5,
            position_szi: 0.5,
            funding_rate: 0.0001,
            dedup_key: "prune-unsettled-1".to_string(),
        },
    ])
    .unwrap();

    let report = db.prune_retention(&policy).unwrap();
    // Snapshots deleted = 1 from Day-45 (duplicate same-day) + 1 from Day-410 (>400d) = 2; Day-45 anchor & Day-1 remain
    assert_eq!(report.snapshots_deleted, 2);
    assert_eq!(report.telemetry_deleted, 1);
    assert_eq!(report.order_audit_deleted, 2); // ETH NEW + CANCELLED audit rows; BTC OPEN audit preserved!
    assert_eq!(report.cooldowns_deleted, 1);
    assert_eq!(report.alert_queue_deleted, 1);
    assert_eq!(report.ohlcv_bars_deleted, 1);
    assert_eq!(report.funding_events_deleted, 1);

    let remaining_snaps = db.list_portfolio_snapshots(10).unwrap();
    assert_eq!(remaining_snaps.len(), 2);
    // Returned chronologically ASC: Day-45 UTC start-of-day anchor (10_100.0) first, Day-1 (11_000.0) second
    assert_eq!(remaining_snaps[0].total_equity_usdc, 10_100.0);
    assert_eq!(remaining_snaps[1].total_equity_usdc, 11_000.0);

    assert_eq!(db.get_ohlcv_bars(btc_id, "1h", 10).unwrap().len(), 1);
    assert_eq!(db.get_ohlcv_bars(doge_id, "1h", 10).unwrap().len(), 0);
    assert_eq!(db.list_all_funding_events().unwrap().len(), 1);
}

#[test]
fn test_r14_m18_and_loophole_1_monotonic_stop_preservation_and_force_reset() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let btc_id = seed_test_asset(&db, "BTC", 0, 102.0);
    let eth_id = seed_test_asset(&db, "ETH", 1, 98.0);

    let (btc_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: btc_id,
            coin: "BTC",
            side: OrderSide::Long,
            entry_price: 100.0,
            mark_price: 102.0,
            quantity: 1.0,
            committed_notional_usdc: 102.0,
            unrealized_pnl_usdc: 2.0,
            portfolio_weight: 0.2,
            protective_stop_px: 95.0,
            take_profit_px: 110.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 60_000),
        })
        .unwrap();
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth_id,
            coin: "ETH",
            side: OrderSide::Short,
            entry_price: 100.0,
            mark_price: 98.0,
            quantity: 1.0,
            committed_notional_usdc: 98.0,
            unrealized_pnl_usdc: 2.0,
            portfolio_weight: 0.2,
            protective_stop_px: 105.0,
            take_profit_px: 90.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 60_000),
        })
        .unwrap();

    // 1. Single & batch mark/stop updates NEVER loosen protective_stop_px
    db.update_open_position_mark_and_stop(btc_pid, 101.0, 90.0)
        .unwrap();
    db.update_open_position_mark_and_stop(eth_pid, 97.5, 109.0)
        .unwrap();
    assert!(
        (db.get_position_by_id(btc_pid)
            .unwrap()
            .unwrap()
            .protective_stop_px
            - 95.0)
            .abs()
            < 1e-6
    );
    assert!(
        (db.get_position_by_id(eth_pid)
            .unwrap()
            .unwrap()
            .protective_stop_px
            - 105.0)
            .abs()
            < 1e-6
    );

    db.update_open_positions_mark_and_stop_batch(&[
        (eth_pid, 97.0, 104.0), // tightens SHORT stop (105 -> 104)
        (btc_pid, 103.0, 96.0), // tightens LONG stop (95 -> 96)
    ])
    .unwrap();
    db.update_open_positions_mark_and_stop_batch(&[
        (btc_pid, 102.5, 92.0), // loosen rejected
        (eth_pid, 97.2, 108.0), // loosen rejected
    ])
    .unwrap();
    assert!(
        (db.get_position_by_id(btc_pid)
            .unwrap()
            .unwrap()
            .protective_stop_px
            - 96.0)
            .abs()
            < 1e-6
    );
    assert!(
        (db.get_position_by_id(eth_pid)
            .unwrap()
            .unwrap()
            .protective_stop_px
            - 104.0)
            .abs()
            < 1e-6
    );

    // 2. update_position_tpsl_orders ALSO preserves monotonic OPEN stop against loosening
    db.update_position_tpsl_orders(
        btc_pid,
        91.0,
        OidUpdate::Set("sl-btc-1"),
        110.0,
        OidUpdate::Set("tp-btc-1"),
    )
    .unwrap();
    let btc_after_tpsl = db.get_position_by_id(btc_pid).unwrap().unwrap();
    assert!((btc_after_tpsl.protective_stop_px - 96.0).abs() < 1e-6);
    assert_eq!(
        btc_after_tpsl.protective_stop_oid.as_deref(),
        Some("sl-btc-1")
    );

    // 3. Loophole #1: breached mark below LONG stop + incoming looser stop/TP NEVER loosens OPEN stop or TP!
    let (res_stop, res_sl_oid, res_tp, res_tp_oid) = resolve_synced_tpsl_targets(
        OrderSide::Long,
        100.0,
        Some("LONG"),
        96.0,
        Some("sl-btc-1"),
        110.0,
        Some("tp-btc-1"),
        88.0,
        120.0,
    );
    assert!((res_stop - 96.0).abs() < 1e-6);
    assert!((res_tp - 110.0).abs() < 1e-6);
    assert_eq!(res_sl_oid.as_deref(), Some("sl-btc-1"));
    assert_eq!(res_tp_oid.as_deref(), Some("tp-btc-1"));

    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: btc_id,
        coin: "BTC",
        side: OrderSide::Long,
        entry_price: 100.0,
        mark_price: 94.0, // breached below 96.0!
        quantity: 1.0,
        committed_notional_usdc: 94.0,
        unrealized_pnl_usdc: -6.0,
        portfolio_weight: 0.2,
        protective_stop_px: 88.0, // incoming wider 12% stop
        take_profit_px: 120.0,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(now_ms() - 60_000),
    })
    .unwrap();
    let btc_after_breach_sync = db.get_position_by_id(btc_pid).unwrap().unwrap();
    assert!((btc_after_breach_sync.protective_stop_px - 96.0).abs() < 1e-6);
    assert!((btc_after_breach_sync.take_profit_px - 110.0).abs() < 1e-6);

    // 4. force_reset_open_position_stop rejects empty reason and breached stop, permits valid non-breached reset + audits
    assert!(matches!(
        db.force_reset_open_position_stop(btc_pid, 92.0, "   "),
        Err(AegisError::Validation(_))
    ));
    // current_price is 94.0 on Long; 95.0 is breached!
    assert!(matches!(
        db.force_reset_open_position_stop(btc_pid, 95.0, "would be breached"),
        Err(AegisError::Validation(_))
    ));
    db.force_reset_open_position_stop(btc_pid, 91.5, "Manual wide regime reset")
        .expect("valid force_reset_open_position_stop should succeed");
    assert!(
        (db.get_position_by_id(btc_pid)
            .unwrap()
            .unwrap()
            .protective_stop_px
            - 91.5)
            .abs()
            < 1e-6
    );
    let audits = db.list_recent_order_audits(10).unwrap();
    assert!(audits
        .iter()
        .any(|a| a.position_id == btc_pid && a.to_state == "STOP_FORCE_RESET"));
}

#[test]
fn test_r14_m19_concurrent_fernet_rotation_and_rollback() {
    let key1 = aegis_db::generate_fernet_key();
    let key2 = aegis_db::generate_fernet_key();
    let key3 = aegis_db::generate_fernet_key();
    let key_foreign = aegis_db::generate_fernet_key();
    let db = std::sync::Arc::new(DatabaseManager::new_isolated_test(Some(&key1)).unwrap());

    db.set_config_value("private_key", "0xdeadbeef01", Some("signer"))
        .unwrap();
    db.set_config_value("telegram_bot_token", "tg-secret-initial", Some("tg"))
        .unwrap();

    // Inject a foreign-encrypted secret to prove rollback leaves DB & in-memory cipher untouched on failure
    let foreign_cipher = FernetCipher::from_key_str(&key_foreign).unwrap();
    let alien_token = foreign_cipher.encrypt(b"foreign-secret").unwrap();
    {
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            "UPDATE system_config SET config_value = :v WHERE config_key = 'telegram_bot_token'",
            params! { "v" => alien_token },
        )
        .unwrap();
    }
    assert!(
        db.rotate_fernet_master_key(&key1, &key2).is_err(),
        "Rotation must fail & rollback when any secret fails decryption with old_key"
    );
    // Cleanly restore telegram_bot_token encrypted with active key1 -> private_key MUST still decrypt with key1!
    db.set_config_value("telegram_bot_token", "tg-secret-initial", Some("tg"))
        .unwrap();
    let cfg_after_rollback = db.load_config().unwrap();
    assert_eq!(
        cfg_after_rollback.get_str("private_key", ""),
        "0xdeadbeef01"
    );

    // Wrong old_key must also fail cleanly without rotating in-memory cipher
    assert!(db.rotate_fernet_master_key(&key_foreign, &key2).is_err());

    // Concurrent stress test: readers + writers + two rotations under lock hierarchy (fernet RwLock -> conn -> tx)
    let reader_stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut readers = Vec::new();
    for _ in 0..3 {
        let db_r = std::sync::Arc::clone(&db);
        let stop = std::sync::Arc::clone(&reader_stop);
        readers.push(std::thread::spawn(move || {
            for _ in 0..20 {
                let cfg = db_r
                    .load_config()
                    .expect("concurrent load_config must never fail during rotation");
                assert_eq!(cfg.get_str("private_key", ""), "0xdeadbeef01");
                if stop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
        }));
    }

    let db_w = std::sync::Arc::clone(&db);
    let writer = std::thread::spawn(move || {
        for i in 0..8 {
            db_w.set_config_value("telegram_bot_token", &format!("tg-secret-{i}"), None)
                .expect("concurrent set_config_value must never fail during rotation");
        }
    });

    assert!(db.rotate_fernet_master_key(&key1, &key2).unwrap() >= 2);
    assert!(db.rotate_fernet_master_key(&key2, &key3).unwrap() >= 2);
    reader_stop.store(true, std::sync::atomic::Ordering::Relaxed);

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    // Verify DB rows on disk are decryptable by key3
    let final_cfg = db.load_config().unwrap();
    assert_eq!(final_cfg.get_str("private_key", ""), "0xdeadbeef01");
    let rows = db.list_system_config_rows().unwrap();
    let pk_raw = rows
        .into_iter()
        .find(|r| r.config_key == "private_key")
        .unwrap()
        .config_value;
    let cipher3 = FernetCipher::from_key_str(&key3).unwrap();
    assert_eq!(cipher3.decrypt(&pk_raw).unwrap(), "0xdeadbeef01");
}

#[test]
fn test_r14_m25_explain_zero_filesort_and_pnl_cache_parity() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 180.0);
    let now = now_ms();

    for i in 0..12 {
        let mut conn = db.get_conn().unwrap();
        let ts = now - (12 - i) * 3_600_000;
        conn.exec_drop(
            r#"INSERT INTO portfolio_snapshots (
                   total_equity_usdc, uncapped_equity_usdc, available_cash_usdc, reserved_order_cash_usdc,
                   committed_notional_usdc, net_notional_usdc, gross_leverage_x, net_leverage_x,
                   market_beta_exposure, market_breadth_bull, market_regime_label, risk_tier,
                   leverage_multiplier, s1_gross_weight, s2_gross_weight, s3_gross_weight,
                   working_order_count, open_long_count, open_short_count, eligible_coin_count,
                   created_at_ms, updated_at_ms
               ) VALUES (
                   :eq, :eq, :eq, 0, 0, 0, 0, 0, 0, 0.5, 'BULL', 'extreme', 1.85,
                   0, 0, 0, 0, 0, 0, 10, :ts, :ts
               )"#,
            params! { "eq" => 10_000.0 + (i as f64) * 50.0, "ts" => ts },
        )
        .unwrap();
    }

    let bars: Vec<BarUpsertInput> = (0..12)
        .map(|i| BarUpsertInput {
            asset_id: sol_id,
            timeframe: "1h".to_string(),
            timestamp_ms: now - (12 - i) * 3_600_000,
            open: 175.0,
            high: 185.0,
            low: 170.0,
            close: 180.0 + i as f64,
            volume: 1000.0,
            funding_rate: 0.00001,
        })
        .collect();
    db.upsert_ohlcv_bars(&bars).unwrap();

    // 1. Verify EXPLAIN avoids `Using filesort` on idx_bars_tf_ts and portfolio_snapshots.created_at_ms
    {
        let mut conn = db.get_conn().unwrap();
        let snap_plan: Vec<mysql::Row> = conn
            .exec(
                "EXPLAIN SELECT id, total_equity_usdc, created_at_ms FROM portfolio_snapshots WHERE created_at_ms >= :ts ORDER BY created_at_ms ASC LIMIT 1",
                params! { "ts" => now - 86_400_000 },
            )
            .unwrap();
        for row in snap_plan {
            let extra = row
                .get_opt::<Option<String>, usize>(9)
                .and_then(std::result::Result::ok)
                .flatten()
                .unwrap_or_default();
            assert!(
                !extra.contains("Using filesort"),
                "Unexpected filesort on portfolio_snapshots query: {extra}"
            );
        }

        let bar_plan: Vec<mysql::Row> = conn
            .exec(
                "EXPLAIN SELECT asset_id, timestamp_ms, close FROM ohlcv_bars WHERE timeframe = '1h' AND timestamp_ms >= :s AND timestamp_ms <= :u",
                params! { "s" => now - 86_400_000, "u" => now },
            )
            .unwrap();
        for row in bar_plan {
            let extra = row
                .get_opt::<Option<String>, usize>(9)
                .and_then(std::result::Result::ok)
                .flatten()
                .unwrap_or_default();
            assert!(
                !extra.contains("Using filesort"),
                "Unexpected filesort on ohlcv_bars window query: {extra}"
            );
        }
    }

    // 2. Parity between cached compute_pnl_summary and compute_pnl_summary_uncached + invalidation
    let uncached = db
        .compute_pnl_summary_uncached(25.0, 1.5, 10_600.0)
        .unwrap();
    let cached = db.compute_pnl_summary(25.0, 1.5, 10_600.0).unwrap();
    assert!((uncached.daily.net_pnl_usdc - cached.daily.net_pnl_usdc).abs() < 1e-9);
    assert!((uncached.weekly.net_pnl_usdc - cached.weekly.net_pnl_usdc).abs() < 1e-9);
    assert!((uncached.monthly.net_pnl_usdc - cached.monthly.net_pnl_usdc).abs() < 1e-9);
    assert!((uncached.ytd.net_pnl_usdc - cached.ytd.net_pnl_usdc).abs() < 1e-9);

    // Mutate realized PnL via close_position -> invalidates 5s cache immediately!
    let wid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-m25-invalidate".to_string(),
            side: OrderSide::Long,
            limit_price: 180.0,
            requested_quantity: 2.0,
            reserved_margin_usdc: 200.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 170.0,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "cache invalidation".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(wid, 180.0, 2.0, 0.0, 170.0, None, None, "filled")
        .unwrap();
    db.close_position(wid, 195.0, 0.0, "closed +30").unwrap();

    let after_close = db.compute_pnl_summary(25.0, 1.5, 10_600.0).unwrap();
    assert!(
        (after_close.daily.realized_pnl_usdc - (uncached.daily.realized_pnl_usdc + 30.0)).abs()
            < 1e-6
    );
}

#[test]
fn test_r14_m26_oid_update_tri_state_semantics() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 180.0);
    let pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-m26-oid".to_string(),
            side: OrderSide::Long,
            limit_price: 180.0,
            requested_quantity: 1.0,
            reserved_margin_usdc: 100.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 170.0,
            take_profit_px: Some(200.0),
            exchange_order_id: Some("entry-oid".to_string()),
            reason: "oid test".to_string(),
        })
        .unwrap();

    db.update_position_tpsl_orders(
        pid,
        171.0,
        OidUpdate::Set("sl-oid-initial"),
        201.0,
        OidUpdate::Set("tp-oid-initial"),
    )
    .unwrap();

    // Keep SL while clearing TP
    db.update_position_tpsl_orders(pid, 171.5, OidUpdate::Keep, 202.0, OidUpdate::Clear)
        .unwrap();
    let p1 = db.get_position_by_id(pid).unwrap().unwrap();
    assert_eq!(p1.protective_stop_oid.as_deref(), Some("sl-oid-initial"));
    assert_eq!(p1.take_profit_oid, None);
    assert!((p1.take_profit_px - 202.0).abs() < 1e-6);

    // Update SL while keeping TP None
    db.update_position_tpsl_orders(
        pid,
        172.0,
        OidUpdate::Set("sl-oid-v2"),
        202.0,
        OidUpdate::Keep,
    )
    .unwrap();
    let p2 = db.get_position_by_id(pid).unwrap().unwrap();
    assert_eq!(p2.protective_stop_oid.as_deref(), Some("sl-oid-v2"));
    assert_eq!(p2.take_profit_oid, None);

    // Empty Set("") is rejected cleanly
    assert!(matches!(
        db.update_position_tpsl_orders(pid, 172.0, OidUpdate::Set("   "), 202.0, OidUpdate::Keep),
        Err(AegisError::Validation(_))
    ));
}

#[test]
fn test_r14_m30_decimal_precision_check_constraints_and_legacy_migration() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");

    // 1. DECIMAL(20,10) funding rate, DECIMAL(24,8) >5e12 volume, DECIMAL(20,8) tiny tick_size
    let pe_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "kPEPE".to_string(),
            asset_index: 99,
            sz_decimals: 0,
            tick_size: 0.00000100,
            lot_step_size: 1.0,
            max_leverage: 10.0,
            mark_price: 0.00001234,
            mid_price: 0.00001235,
            oracle_price: 0.00001233,
            funding_rate_hourly: 0.000000125, // 10 decimal places!
            day_volume_usdc: 5_123_456_789_012.34,
            open_interest_usdc: 900_000_000_000.12,
            prev_day_price: 0.00001200,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();
    let pepe = db.get_asset_by_id(pe_id).unwrap().unwrap();
    assert!((pepe.funding_rate_hourly - 0.000000125).abs() < 1e-10);
    assert!((pepe.day_volume_usdc - 5_123_456_789_012.34).abs() < 0.01);
    assert!((pepe.tick_size - 0.000001).abs() < 1e-9);

    db.upsert_funding_events_batch(&[FundingEventUpsertInput {
        asset_id: pe_id,
        coin: "kPEPE".to_string(),
        event_time_ms: now_ms() - 60_000,
        usdc: -0.004215,
        remaining_usdc: -0.004215,
        position_szi: 1_000_000.0,
        funding_rate: -0.000000875,
        dedup_key: "pepe-subbps-1".to_string(),
    }])
    .unwrap();
    let fevs = db.list_funding_events_for_asset(pe_id).unwrap();
    assert_eq!(fevs.len(), 1);
    assert!((fevs[0].funding_rate - (-0.000000875)).abs() < 1e-10);

    // 2. Sleeve signal views (0.000001 replacing 1e-6) query cleanly with DECIMAL schemas
    db.upsert_sleeve_signal(&SignalUpsertInput {
        asset_id: pe_id,
        mark_price: 0.00001234,
        day_volume_usdc: 5_123_456_789_012.34,
        funding_rate_hourly: 0.000000125,
        annualized_vol: 0.65,
        market_beta: 1.15,
        s1_mom_score: 1.2,
        s1_carry_score: 0.4,
        s2_trend_alignment: 2,
        s2_vol_surge_ratio: 1.8,
        s3_funding_zscore: -2.5,
        s3_flush_atr_mult: 1.9,
        w_s1_carry_mom: 0.05,
        w_s2_trend_breakout: 0.04,
        w_s3_event_reversion: -0.02,
        raw_combined_weight: 0.07,
        target_weight: 0.07,
        current_weight: 0.0,
        delta_weight: 0.07,
        event_trigger: Some("FLUSH".to_string()),
        rebalance_action: "BUY".to_string(),
    })
    .unwrap();
    assert_eq!(db.get_sleeve_s1_view().unwrap().len(), 1);
    assert_eq!(db.get_sleeve_s2_view().unwrap().len(), 1);
    assert_eq!(db.get_sleeve_s3_view().unwrap().len(), 1);

    // 3. CHECK constraints: all 10 valid timeframes succeed; invalid timeframe, negative tick, invalid breadth fail; negative equity allowed
    let base_t = (now_ms() / 60_000) * 60_000;
    let all_10_tfs: Vec<BarUpsertInput> = [
        "1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d",
    ]
    .iter()
    .enumerate()
    .map(|(i, tf)| BarUpsertInput {
        asset_id: pe_id,
        timeframe: (*tf).to_string(),
        timestamp_ms: base_t + i as i64 * 60_000,
        open: 1.0,
        high: 1.2,
        low: 0.9,
        close: 1.1,
        volume: 100.0,
        funding_rate: 0.0,
    })
    .collect();
    assert_eq!(db.upsert_ohlcv_bars(&all_10_tfs).unwrap(), 10);

    // Negative total_equity_usdc MUST succeed (insolvency logging)
    db.insert_portfolio_snapshot(&SnapshotInsertInput {
        total_equity_usdc: -125.50,
        uncapped_equity_usdc: -125.50,
        available_cash_usdc: 0.0,
        reserved_order_cash_usdc: 0.0,
        committed_notional_usdc: 0.0,
        net_notional_usdc: 0.0,
        gross_leverage_x: 0.0,
        net_leverage_x: 0.0,
        market_beta_exposure: 0.0,
        market_breadth_bull: 0.5,
        market_regime_label: "INSOLVENT".to_string(),
        risk_tier: "extreme".to_string(),
        leverage_multiplier: 1.85,
        s1_gross_weight: 0.0,
        s2_gross_weight: 0.0,
        s3_gross_weight: 0.0,
        working_order_count: 0,
        open_long_count: 0,
        open_short_count: 0,
        eligible_coin_count: 1,
    })
    .expect("negative total_equity_usdc must be allowed for insolvency logging");

    {
        let mut conn = db.get_conn().unwrap();
        assert!(
            conn.query_drop(format!(
                "INSERT INTO ohlcv_bars (asset_id, timeframe, timestamp_ms, open, high, low, close, volume, funding_rate, created_at_ms, updated_at_ms) VALUES ({pe_id}, '3m', {base_t}, 1, 2, 0.5, 1.5, 10, 0, {base_t}, {base_t})"
            ))
            .is_err(),
            "Invalid timeframe '3m' must violate CHECK chk_bars_timeframe"
        );
        assert!(
            conn.query_drop(format!(
                "INSERT INTO exchange_assets (symbol, coin, asset_index, sz_decimals, tick_size, lot_step_size, max_leverage, mark_price, mid_price, oracle_price, created_at_ms, updated_at_ms) VALUES ('BAD-USDC', 'BAD', 999, 2, -0.01, 0.01, 10, 1, 1, 1, {base_t}, {base_t})"
            ))
            .is_err(),
            "Negative tick_size must violate CHECK chk_assets_positive_specs"
        );
        let breadth_err = conn
            .query_drop(format!(
                "INSERT INTO portfolio_snapshots (total_equity_usdc, uncapped_equity_usdc, available_cash_usdc, reserved_order_cash_usdc, committed_notional_usdc, net_notional_usdc, gross_leverage_x, net_leverage_x, market_beta_exposure, market_breadth_bull, market_regime_label, risk_tier, leverage_multiplier, s1_gross_weight, s2_gross_weight, s3_gross_weight, working_order_count, open_long_count, open_short_count, eligible_coin_count, created_at_ms, updated_at_ms) VALUES (100, 100, 100, 0, 0, 0, 0, 0, 0, 1.25, 'BULL', 'extreme', 1.0, 0, 0, 0, 0, 0, 0, 1, {base_t}, {base_t})"
            ))
            .expect_err("Out-of-range market_breadth_bull=1.25 must violate CHECK chk_snapshots_breadth");
        assert!(
            breadth_err.to_string().contains("chk_snapshots_breadth"),
            "Expected chk_snapshots_breadth violation, got: {breadth_err}"
        );

        // 4. Test legacy DOUBLE -> DECIMAL migration + pre-clamping on init_schema()
        conn.query_drop("ALTER TABLE portfolio_snapshots DROP CONSTRAINT chk_snapshots_breadth")
            .unwrap();
        conn.query_drop("ALTER TABLE portfolio_snapshots MODIFY COLUMN market_breadth_bull DOUBLE NOT NULL DEFAULT 0.5")
            .unwrap();
        let leg_t = base_t + 1_000;
        conn.query_drop(format!(
            "INSERT INTO portfolio_snapshots (total_equity_usdc, uncapped_equity_usdc, available_cash_usdc, reserved_order_cash_usdc, committed_notional_usdc, net_notional_usdc, gross_leverage_x, net_leverage_x, market_beta_exposure, market_breadth_bull, market_regime_label, risk_tier, leverage_multiplier, s1_gross_weight, s2_gross_weight, s3_gross_weight, working_order_count, open_long_count, open_short_count, eligible_coin_count, created_at_ms, updated_at_ms) VALUES (500.0, 500.0, 500.0, 0, 0, 0, 0, 0, 0, 1.75, 'LEGACY_DOUBLE', 'extreme', 1.0, 0, 0, 0, 0, 0, 0, 1, {leg_t}, {leg_t})"
        ))
        .unwrap();
        conn.query_drop("DELETE FROM system_config WHERE config_key = 'schema_migration_version'")
            .unwrap();
    }

    db.init_schema().expect(
        "init_schema must pre-clamp legacy rows, convert DOUBLE->DECIMAL, and restore CHECKs",
    );

    {
        let mut conn = db.get_conn().unwrap();
        let dtype: String = conn
            .query_first(
                r#"SELECT DATA_TYPE FROM information_schema.COLUMNS
                   WHERE TABLE_SCHEMA = DATABASE()
                     AND TABLE_NAME = 'portfolio_snapshots'
                     AND COLUMN_NAME = 'market_breadth_bull'"#,
            )
            .unwrap()
            .unwrap();
        assert_eq!(dtype.to_ascii_lowercase(), "decimal");

        let clamped_breadth: f64 = conn
            .query_first(
                "SELECT CAST(market_breadth_bull AS DOUBLE) FROM portfolio_snapshots WHERE market_regime_label = 'LEGACY_DOUBLE'",
            )
            .unwrap()
            .unwrap();
        assert!((clamped_breadth - 1.0).abs() < 1e-6);

        let ver: String = conn
            .query_first(
                "SELECT config_value FROM system_config WHERE config_key = 'schema_migration_version'",
            )
            .unwrap()
            .unwrap();
        assert_eq!(ver, "2.2.0");
    }
}

#[test]
fn test_r14_m3_partial_exit_and_p1_strat_tp_parity() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 100.0);

    // 1. Explicit take_profit_px preserved on WorkingOrderInput & promote_working_to_open
    let p_explicit = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-tp-explicit".to_string(),
            side: OrderSide::Long,
            limit_price: 100.0,
            requested_quantity: 10.0,
            reserved_margin_usdc: 500.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 90.0,
            take_profit_px: Some(142.50),
            exchange_order_id: None,
            reason: "explicit TP".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(p_explicit, 100.0, 10.0, 0.5, 90.0, None, None, "filled")
        .unwrap();
    let pos_exp = db.get_position_by_id(p_explicit).unwrap().unwrap();
    assert!((pos_exp.take_profit_px - 142.50).abs() < 1e-6);

    // 2. record_partial_position_exit updates remaining_qty, committed_notional, realized_pnl, keeps OPEN, audits PARTIAL_CLOSE
    let net_partial = db
        .record_partial_position_exit(p_explicit, 4.0, 6.0, 110.0, 0.44, "TP1_PARTIAL")
        .unwrap();
    // (110 - 100)*4 - 0.44 = 39.56
    assert!((net_partial - 39.56).abs() < 1e-4);
    let after_partial = db.get_position_by_id(p_explicit).unwrap().unwrap();
    assert_eq!(after_partial.status, "OPEN");
    assert_eq!(after_partial.fsm_state, "FILLED_OPEN");
    assert!((after_partial.filled_quantity - 6.0).abs() < 1e-6);
    assert!((after_partial.committed_notional_usdc - 600.0).abs() < 1e-4);
    assert!((after_partial.realized_pnl_usdc - 39.56).abs() < 1e-4);
    let audits = db.list_recent_order_audits(10).unwrap();
    assert!(audits
        .iter()
        .any(|a| a.position_id == p_explicit && a.to_state == "PARTIAL_CLOSE"));

    db.close_position(p_explicit, 108.0, 0.2, "done").unwrap();

    // 3. Derived TP when None uses 2.8/2.2 R:R ratio instead of legacy hardcoded 2.0
    let p_derived = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-tp-derived".to_string(),
            side: OrderSide::Long,
            limit_price: 100.0,
            requested_quantity: 5.0,
            reserved_margin_usdc: 250.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 90.0,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "derived TP".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(p_derived, 100.0, 5.0, 0.2, 90.0, None, None, "filled")
        .unwrap();
    let pos_drv = db.get_position_by_id(p_derived).unwrap().unwrap();
    let expected_tp = 100.0 + 10.0 * (2.8 / 2.2);
    assert!((pos_drv.take_profit_px - expected_tp).abs() < 1e-5);
}

#[test]
fn test_r14_l8_chase_price_validation_and_initial_limit_price() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 180.0);

    let pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-l8-chase".to_string(),
            side: OrderSide::Long,
            limit_price: 180.0,
            requested_quantity: 1.0,
            reserved_margin_usdc: 100.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 170.0,
            take_profit_px: None,
            exchange_order_id: Some("hl-l8-1".to_string()),
            reason: "initial working".to_string(),
        })
        .unwrap();

    assert_eq!(
        db.get_working_order_initial_limit_price(pid).unwrap(),
        Some(180.0)
    );

    // Zero, negative, and NaN chase prices must fail before mutating state
    assert!(matches!(
        db.record_working_order_chase(pid, 0.0, "CHASING_ALO", None, "bad zero"),
        Err(AegisError::Validation(_))
    ));
    assert!(matches!(
        db.record_working_order_chase(pid, -10.0, "CHASING_ALO", None, "bad neg"),
        Err(AegisError::Validation(_))
    ));
    assert!(matches!(
        db.record_working_order_chase(pid, f64::NAN, "CHASING_ALO", None, "bad nan"),
        Err(AegisError::Validation(_))
    ));

    // Valid chase updates both positions.entry_price & CHASE audit limit_price while keeping initial price intact
    let cnt = db
        .record_working_order_chase(pid, 181.25, "CHASING_ALO", Some("hl-l8-2"), "top of book")
        .unwrap();
    assert_eq!(cnt, 1);
    assert_eq!(
        db.get_working_order_initial_limit_price(pid).unwrap(),
        Some(180.0)
    );
    let pos = db.get_position_by_id(pid).unwrap().unwrap();
    assert!((pos.entry_price - 181.25).abs() < 1e-6);

    let chase_audit = db
        .list_recent_order_audits(10)
        .unwrap()
        .into_iter()
        .find(|a| a.position_id == pid && a.to_state == "CHASING_ALO")
        .expect("CHASING_ALO audit row");
    assert!((chase_audit.limit_price - 181.25).abs() < 1e-6);
}

#[test]
fn test_r14_m15_m17_m22_l14_bars_window_telemetry_batch_dashboard_bundle_and_alerts() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let btc_id = seed_test_asset(&db, "BTC", 0, 90_000.0);
    let sol_id = seed_test_asset(&db, "SOL", 5, 180.0);
    let now = (now_ms() / 3_600_000) * 3_600_000;
    let t1 = now - 3 * 3_600_000;
    let t2 = now - 2 * 3_600_000;
    let t3 = now - 3_600_000;

    // 1. get_ohlcv_bars_window sorted & bounded
    db.upsert_ohlcv_bars(&[
        BarUpsertInput {
            asset_id: btc_id,
            timeframe: "1h".to_string(),
            timestamp_ms: t1 - 3_600_000,
            open: 88_000.0,
            high: 89_000.0,
            low: 87_500.0,
            close: 88_500.0,
            volume: 100.0,
            funding_rate: 0.0,
        },
        BarUpsertInput {
            asset_id: btc_id,
            timeframe: "1h".to_string(),
            timestamp_ms: t3,
            open: 89_500.0,
            high: 90_500.0,
            low: 89_000.0,
            close: 90_000.0,
            volume: 150.0,
            funding_rate: 0.0,
        },
        BarUpsertInput {
            asset_id: btc_id,
            timeframe: "1h".to_string(),
            timestamp_ms: t2,
            open: 88_500.0,
            high: 89_800.0,
            low: 88_200.0,
            close: 89_500.0,
            volume: 120.0,
            funding_rate: 0.0,
        },
        BarUpsertInput {
            asset_id: sol_id,
            timeframe: "1h".to_string(),
            timestamp_ms: t2,
            open: 178.0,
            high: 182.0,
            low: 177.0,
            close: 180.0,
            volume: 500.0,
            funding_rate: 0.0,
        },
    ])
    .unwrap();
    let bars_win = db.get_ohlcv_bars_window("1h", t1, t3).unwrap();
    assert_eq!(bars_win[&btc_id].len(), 2);
    assert_eq!(bars_win[&btc_id][0].timestamp_ms, t2);
    assert_eq!(bars_win[&btc_id][1].timestamp_ms, t3);
    assert_eq!(bars_win[&sol_id].len(), 1);

    // 2. list_funding_events_window
    db.upsert_funding_events_batch(&[FundingEventUpsertInput {
        asset_id: btc_id,
        coin: "BTC".to_string(),
        event_time_ms: t2,
        usdc: 4.25,
        remaining_usdc: 4.25,
        position_szi: 0.5,
        funding_rate: 0.00001,
        dedup_key: "win-fund-1".to_string(),
    }])
    .unwrap();
    let fund_win = db.list_funding_events_window(t1, t3).unwrap();
    assert_eq!(fund_win[&btc_id].len(), 1);
    assert!((fund_win[&btc_id][0].1 - 0.00001).abs() < 1e-9);

    // 3. log_telemetry_batch with secret redaction & get_config_i64_raw
    let inserted_logs = db
        .log_telemetry_batch(&[
            TelemetryInsertInput {
                level: "INFO".to_string(),
                component: "alerts".to_string(),
                message: "Batch flush ok gAAAAABsupersecret123".to_string(),
                details: Some(json!({"batch": 1})),
                created_at_ms: Some(now - 5_000),
            },
            TelemetryInsertInput {
                level: "WARN".to_string(),
                component: "telemetry".to_string(),
                message: "Buffer water mark".to_string(),
                details: None,
                created_at_ms: None,
            },
        ])
        .unwrap();
    assert_eq!(inserted_logs, 2);
    let logs = db.list_recent_telemetry(5).unwrap();
    assert!(!logs
        .iter()
        .any(|l| l.message.contains("gAAAAABsupersecret123")));

    db.set_config_value("test_i64_key", "424242", None).unwrap();
    assert_eq!(
        db.get_config_i64_raw("test_i64_key").unwrap(),
        Some(424_242)
    );

    // 4. uncapped_equity_usdc peak/latest & alert_queue dispatched_at_ms
    db.insert_portfolio_snapshot(&SnapshotInsertInput {
        total_equity_usdc: 500.0,
        uncapped_equity_usdc: 2_500.0,
        available_cash_usdc: 500.0,
        reserved_order_cash_usdc: 0.0,
        committed_notional_usdc: 0.0,
        net_notional_usdc: 0.0,
        gross_leverage_x: 0.0,
        net_leverage_x: 0.0,
        market_beta_exposure: 0.0,
        market_breadth_bull: 0.5,
        market_regime_label: "CAP_ACTIVE".to_string(),
        risk_tier: "extreme".to_string(),
        leverage_multiplier: 1.85,
        s1_gross_weight: 0.0,
        s2_gross_weight: 0.0,
        s3_gross_weight: 0.0,
        working_order_count: 0,
        open_long_count: 0,
        open_short_count: 0,
        eligible_coin_count: 2,
    })
    .unwrap();
    let (unc_peak, unc_latest) = db
        .get_peak_and_latest_uncapped_equity_since(now - 60_000)
        .unwrap()
        .unwrap();
    assert!((unc_peak - 2_500.0).abs() < 1e-4);
    assert!((unc_latest - 2_500.0).abs() < 1e-4);

    let aid = db
        .insert_alert_queue_record_full(&AlertQueueInsertInput {
            provider: "telegram",
            event_type: "TEST_DISPATCH",
            severity: "INFO",
            message: "Alert queued",
            payload_json: Some(&json!({"k": "v"})),
            status: "PENDING",
            attempts: 0,
            next_retry_ms: 0,
            dispatched_at_ms: None,
        })
        .unwrap();
    assert_eq!(db.get_alert_queue_dispatched_at_ms(aid).unwrap(), None);
    let dispatch_ts = now_ms();
    db.mark_alert_queue_dispatched(aid, dispatch_ts).unwrap();
    assert_eq!(
        db.get_alert_queue_dispatched_at_ms(aid).unwrap(),
        Some(dispatch_ts)
    );

    // 5. load_dashboard_read_bundle partial outage isolation (R14-M22)
    let bundle_ok = db.load_dashboard_read_bundle().unwrap();
    assert!(bundle_ok.config.is_ok());
    assert!(bundle_ok.exchange_assets.is_ok());
    assert!(bundle_ok.latest_snapshot.as_ref().unwrap().is_some());

    {
        let mut conn = db.get_conn().unwrap();
        conn.query_drop("DROP VIEW IF EXISTS v_assets_sleeve_s3")
            .unwrap();
    }
    let bundle_partial = db.load_dashboard_read_bundle().unwrap();
    assert!(
        bundle_partial.sleeve_s3.is_err(),
        "Dropped sleeve_s3 view must only fail bundle.sleeve_s3 section"
    );
    assert!(bundle_partial.config.is_ok());
    assert!(bundle_partial.assets_held.is_ok());
    assert!(bundle_partial.latest_snapshot.is_ok());
}

#[test]
fn test_schema_v2_2_0_entry_r0_backfill_immutability_and_sum_realized_pnl() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 100.0);
    let eth_id = seed_test_asset(&db, "ETH", 1, 2000.0);
    let btc_id = seed_test_asset(&db, "BTC", 0, 50000.0);
    let avax_id = seed_test_asset(&db, "AVAX", 3, 25.0);
    let link_id = seed_test_asset(&db, "LINK", 4, 15.0);

    // 0. sum_realized_pnl_usdc returns 0.0 when positions table is empty
    assert!((db.sum_realized_pnl_usdc().unwrap() - 0.0).abs() < 1e-9);

    // 1. create_working_order populates entry_r0, promote_working_to_open re-anchors to fill_price
    let pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "coid-r0-imm".to_string(),
            side: OrderSide::Long,
            limit_price: 100.0,
            requested_quantity: 10.0,
            reserved_margin_usdc: 500.0,
            portfolio_weight: 0.10,
            s1_weight_contrib: 0.10,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 95.0,
            take_profit_px: Some(115.0),
            exchange_order_id: Some("hl-r0-1".to_string()),
            reason: "entry_r0 lifecycle".to_string(),
        })
        .unwrap();
    let working = db.get_position_by_id(pid).unwrap().unwrap();
    assert!((working.entry_r0 - 5.0).abs() < 1e-6);

    db.promote_working_to_open(pid, 102.0, 10.0, 0.5, 96.0, None, None, "filled")
        .unwrap();
    let opened = db.get_position_by_id(pid).unwrap().unwrap();
    assert!((opened.entry_r0 - 6.0).abs() < 1e-6);

    // 2. Ratcheting stops (single + batch), partial exits, and exchange sync NEVER overwrite positive entry_r0
    db.update_open_position_mark_and_stop(pid, 110.0, 103.5)
        .unwrap();
    db.update_open_positions_mark_and_stop_batch(&[(pid, 112.0, 105.0)])
        .unwrap();
    let net_partial = db
        .record_partial_position_exit(pid, 4.0, 6.0, 112.0, 0.4, "TP1")
        .unwrap();
    assert!((net_partial - 39.6).abs() < 1e-4);

    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: sol_id,
        coin: "SOL",
        side: OrderSide::Long,
        entry_price: 102.0,
        mark_price: 113.0,
        quantity: 6.0,
        committed_notional_usdc: 678.0,
        unrealized_pnl_usdc: 66.0,
        portfolio_weight: 0.10,
        protective_stop_px: 108.0,
        take_profit_px: 115.0,
        funding_accrued_usdc: 0.0,
        opened_at_ms: None,
    })
    .unwrap();

    let after_ratchets = db.get_position_by_id(pid).unwrap().unwrap();
    assert!((after_ratchets.entry_r0 - 6.0).abs() < 1e-6);
    assert!((after_ratchets.protective_stop_px - 105.0).abs() < 1e-6);

    let held = db.get_assets_held().unwrap();
    let held_sol = held.iter().find(|h| h.position_id == pid).unwrap();
    assert!((held_sol.entry_r0 - 6.0).abs() < 1e-6);

    // Fresh exchange adoption sets entry_r0 = |entry_price - entry_stop| when on initial loss side
    let (adopted_pid, newly_adopted) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth_id,
            coin: "ETH",
            side: OrderSide::Short,
            entry_price: 2000.0,
            mark_price: 1980.0,
            quantity: 1.0,
            committed_notional_usdc: 1980.0,
            unrealized_pnl_usdc: 20.0,
            portfolio_weight: -0.10,
            protective_stop_px: 2080.0,
            take_profit_px: 1898.2,
            funding_accrued_usdc: 0.0,
            opened_at_ms: None,
        })
        .unwrap();
    assert!(newly_adopted);
    let adopted = db.get_position_by_id(adopted_pid).unwrap().unwrap();
    assert!((adopted.entry_r0 - 80.0).abs() < 1e-6);

    // Close ETH with negative PnL and check sum_realized_pnl_usdc across OPEN partial + CLOSED
    let eth_pnl = db
        .close_position(adopted_pid, 2050.0, 1.0, "SL_HIT")
        .unwrap();
    let total_rpnl = db.sum_realized_pnl_usdc().unwrap();
    assert!((total_rpnl - (net_partial + eth_pnl)).abs() < 1e-4);

    // 3. v2.1.0 -> v2.2.0 schema migration & backfill across all 3 CASE branches
    let ts = now_ms();
    {
        let mut conn = db.get_conn().unwrap();
        conn.query_drop("ALTER TABLE positions DROP CONSTRAINT chk_positions_entry_r0")
            .unwrap();
        conn.query_drop("ALTER TABLE positions DROP COLUMN entry_r0")
            .unwrap();
        conn.query_drop(
            "UPDATE system_config SET config_value = '2.1.0' WHERE config_key = 'schema_migration_version'",
        )
        .unwrap();

        // Insert 5 legacy CLOSED rows representing all backfill branches:
        // a) unratcheted LONG stop: entry=100, stop=93 => 7.0
        // b) unratcheted SHORT stop: entry=100, stop=106 => 6.0
        // c) ratcheted LONG stop (>= entry), TP=114 => |114-100| * 2.2/2.8 = 11.0
        // d) ratcheted SHORT stop (<= entry), TP=86 => |86-100| * 2.2/2.8 = 11.0
        // e) neither stop nor TP, entry=250 => 250 * 0.04 = 10.0
        for (aid, coid, side, entry, stop, tp) in [
            (btc_id, "leg-a-long-stop", "LONG", 100.0, 93.0, 120.0),
            (btc_id, "leg-b-short-stop", "SHORT", 100.0, 106.0, 80.0),
            (avax_id, "leg-c-long-tp", "LONG", 100.0, 102.0, 114.0),
            (avax_id, "leg-d-short-tp", "SHORT", 100.0, 98.0, 86.0),
            (link_id, "leg-e-fallback", "LONG", 250.0, 0.0, 0.0),
        ] {
            conn.exec_drop(
                r#"INSERT INTO positions (
                       asset_id, client_order_id, side, status, fsm_state, entry_price, current_price,
                       exit_price, requested_quantity, filled_quantity, protective_stop_px, take_profit_px,
                       created_at_ms, updated_at_ms
                   ) VALUES (
                       :aid, :coid, :side, 'CLOSED', 'CLOSED', :entry, :entry,
                       :entry, 1.0, 1.0, :stop, :tp, :ts, :ts
                   )"#,
                params! {
                    "aid" => aid,
                    "coid" => coid,
                    "side" => side,
                    "entry" => entry,
                    "stop" => stop,
                    "tp" => tp,
                    "ts" => ts,
                },
            )
            .unwrap();
        }
    }

    db.init_schema()
        .expect("v2.1.0 -> v2.2.0 init_schema migration must succeed before views");

    {
        let mut conn = db.get_conn().unwrap();
        let ver: String = conn
            .query_first(
                "SELECT config_value FROM system_config WHERE config_key = 'schema_migration_version'",
            )
            .unwrap()
            .unwrap();
        assert_eq!(ver, "2.2.0");

        for (coid, expected_r0) in [
            ("leg-a-long-stop", 7.0),
            ("leg-b-short-stop", 6.0),
            ("leg-c-long-tp", 11.0),
            ("leg-d-short-tp", 11.0),
            ("leg-e-fallback", 10.0),
        ] {
            let r0: f64 = conn
                .exec_first(
                    "SELECT CAST(entry_r0 AS DOUBLE) FROM positions WHERE client_order_id = :coid",
                    params! { "coid" => coid },
                )
                .unwrap()
                .unwrap();
            assert!(
                (r0 - expected_r0).abs() < 1e-6,
                "Backfill mismatch for {coid}: got {r0}, expected {expected_r0}"
            );
        }

        let chk_err = conn
            .query_drop(format!(
                "INSERT INTO positions (asset_id, client_order_id, side, status, fsm_state, entry_price, current_price, requested_quantity, entry_r0, created_at_ms, updated_at_ms) VALUES ({btc_id}, 'bad-neg-r0', 'LONG', 'CLOSED', 'CLOSED', 100, 100, 1, -0.01, {ts}, {ts})"
            ))
            .expect_err("Negative entry_r0 must violate chk_positions_entry_r0");
        assert!(
            chk_err.to_string().contains("chk_positions_entry_r0"),
            "Expected chk_positions_entry_r0 violation, got: {chk_err}"
        );
    }
}

#[test]
fn test_merge_working_scale_in_into_open_and_paper_funding_idempotency() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 100.0);
    let eth_id = seed_test_asset(&db, "ETH", 1, 2000.0);

    // 1. Place initial WORKING order and promote to OPEN (entry=100, qty=10, stop=94 => entry_r0=6.0)
    let open_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "v2-sol-long-1".to_string(),
            side: OrderSide::Long,
            limit_price: 100.0,
            requested_quantity: 10.0,
            reserved_margin_usdc: 500.0,
            portfolio_weight: 0.10,
            s1_weight_contrib: 0.08,
            s2_weight_contrib: 0.02,
            s3_weight_contrib: 0.0,
            protective_stop_px: 94.0,
            take_profit_px: Some(115.0),
            exchange_order_id: Some("hl-open-1".to_string()),
            reason: "OPEN_LONG".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        open_pid,
        100.0,
        10.0,
        1.5,
        94.0,
        Some(115.0),
        None,
        "filled",
    )
    .unwrap();
    let initial_open = db.get_position_by_id(open_pid).unwrap().unwrap();
    let initial_opened_at = initial_open.opened_at_ms.unwrap();
    assert!((initial_open.entry_r0 - 6.0).abs() < 1e-6);

    // Simulate stop ratchet so protective_stop_px moves up while entry_r0 stays frozen at 6.0
    db.update_open_position_mark_and_stop(open_pid, 108.0, 101.0)
        .unwrap();

    // Wait >= 2ms so opened_at_ms preservation is distinguishable from merge timestamp
    std::thread::sleep(std::time::Duration::from_millis(5));

    // 2. Place a scale-in WORKING order on SOL (same side)
    let inc_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "v2-sol-long-2".to_string(),
            side: OrderSide::Long,
            limit_price: 112.0,
            requested_quantity: 5.0,
            reserved_margin_usdc: 300.0,
            portfolio_weight: 0.05,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 104.0,
            take_profit_px: Some(124.0),
            exchange_order_id: Some("hl-inc-2".to_string()),
            reason: "INCREASE".to_string(),
        })
        .unwrap();
    let active_rows = db.list_active_positions_for_asset(sol_id).unwrap();
    assert_eq!(active_rows.len(), 2);
    assert_eq!(
        db.get_working_order_initial_reason(inc_pid)
            .unwrap()
            .as_deref(),
        Some("INCREASE")
    );
    assert_eq!(db.count_entries_since(initial_opened_at - 1000).unwrap(), 2);

    // Opposite-side merge must reject cleanly
    let bad_short_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "v2-sol-short-bad".to_string(),
            side: OrderSide::Short,
            limit_price: 112.0,
            requested_quantity: 2.0,
            reserved_margin_usdc: 100.0,
            portfolio_weight: -0.02,
            s1_weight_contrib: -0.02,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 118.0,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "BAD_SIDE".to_string(),
        })
        .unwrap();
    assert!(db
        .merge_working_scale_in_into_open(bad_short_pid, open_pid, 112.0, 2.0, 0.2, None, "bad")
        .is_err());
    db.cancel_working_order(bad_short_pid, "TEST_CLEANUP")
        .unwrap();
    // Cancelled row must NOT be counted in count_entries_since
    assert_eq!(db.count_entries_since(initial_opened_at - 1000).unwrap(), 2);

    // 3. Merge filled scale-in into OPEN: 10 @ 100 + 5 @ 112 => VWAP 104.0, qty 15.0, committed 1560.0, fees 1.5 + 0.8 = 2.3
    db.merge_working_scale_in_into_open(
        inc_pid,
        open_pid,
        112.0,
        5.0,
        0.8,
        Some("hl-inc-filled-2"),
        "INCREASE_FILLED",
    )
    .unwrap();

    let merged_open = db.get_position_by_id(open_pid).unwrap().unwrap();
    assert_eq!(merged_open.status, "OPEN");
    assert!((merged_open.entry_price - 104.0).abs() < 1e-6);
    assert!((merged_open.filled_quantity - 15.0).abs() < 1e-6);
    assert!((merged_open.committed_notional_usdc - 1560.0).abs() < 1e-4);
    assert!((merged_open.fees_paid_usdc - 2.3).abs() < 1e-4);
    assert!((merged_open.portfolio_weight - 0.15).abs() < 1e-4);
    assert!((merged_open.s1_weight_contrib - 0.13).abs() < 1e-4);
    assert!((merged_open.s2_weight_contrib - 0.02).abs() < 1e-4);
    assert!(
        (merged_open.entry_r0 - 6.0).abs() < 1e-6,
        "Positive entry_r0 must be preserved across scale-in merge"
    );
    assert_eq!(
        merged_open.opened_at_ms,
        Some(initial_opened_at),
        "opened_at_ms must be preserved across scale-in merge"
    );

    let merged_child = db.get_position_by_id(inc_pid).unwrap().unwrap();
    assert_eq!(merged_child.status, "CLOSED");
    assert_eq!(merged_child.fsm_state, "CLOSED");
    assert_eq!(merged_child.exit_reason.as_deref(), Some("SCALE_IN_MERGED"));
    assert!((merged_child.fees_paid_usdc - 0.0).abs() < 1e-9);
    assert!((merged_child.realized_pnl_usdc - 0.0).abs() < 1e-9);
    assert!((db.sum_realized_pnl_usdc().unwrap() - 0.0).abs() < 1e-9);

    // 4. Paper-mode hourly funding accrual idempotency
    let hour_ts = 1_700_006_400_000i64;
    let ev1 = aegis_db::FundingEventUpsertInput {
        asset_id: sol_id,
        coin: "SOL".to_string(),
        event_time_ms: hour_ts,
        usdc: -1.56,
        remaining_usdc: 0.0,
        position_szi: 15.0,
        funding_rate: 0.001,
        dedup_key: format!("paper:{open_pid}:{hour_ts}"),
    };
    let ev2 = aegis_db::FundingEventUpsertInput {
        asset_id: eth_id,
        coin: "ETH".to_string(),
        event_time_ms: hour_ts,
        usdc: 0.25,
        remaining_usdc: 0.0,
        position_szi: -1.0,
        funding_rate: 0.000125,
        dedup_key: format!("paper:99999:{hour_ts}"),
    };
    let ins1 = db
        .accrue_paper_funding_events(&[(open_pid, ev1.clone()), (99999, ev2.clone())])
        .unwrap();
    assert_eq!(ins1, 2);
    let after_f1 = db.get_position_by_id(open_pid).unwrap().unwrap();
    assert!((after_f1.funding_accrued_usdc - (-1.56)).abs() < 1e-4);

    // Duplicate call for same (position_id, hour_ts) must insert 0 and NOT double-accrue
    let ins2 = db
        .accrue_paper_funding_events(&[(open_pid, ev1), (99999, ev2)])
        .unwrap();
    assert_eq!(ins2, 0);
    let after_f2 = db.get_position_by_id(open_pid).unwrap().unwrap();
    assert!((after_f2.funding_accrued_usdc - (-1.56)).abs() < 1e-4);
}

#[test]
fn test_wave4_proportional_portfolio_weight_scaling_across_partial_lifecycle() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 100.0);

    // 1. Partial ALO fill promotion (requested 10.0, filled 4.0 => 40% weight scaling)
    let pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "w4-partial-promote".to_string(),
            side: OrderSide::Long,
            limit_price: 100.0,
            requested_quantity: 10.0,
            reserved_margin_usdc: 500.0,
            portfolio_weight: 0.20,
            s1_weight_contrib: 0.12,
            s2_weight_contrib: 0.08,
            s3_weight_contrib: 0.0,
            protective_stop_px: 92.0,
            take_profit_px: Some(120.0),
            exchange_order_id: Some("hl-w4-1".to_string()),
            reason: "OPEN_LONG".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        pid,
        100.0,
        4.0,
        0.2,
        92.0,
        Some(120.0),
        None,
        "partial_fill",
    )
    .unwrap();
    let promoted = db.get_position_by_id(pid).unwrap().unwrap();
    assert!((promoted.portfolio_weight - 0.08).abs() < 1e-6);
    assert!((promoted.s1_weight_contrib - 0.048).abs() < 1e-6);
    assert!((promoted.s2_weight_contrib - 0.032).abs() < 1e-6);

    // 2. Partial scale-in merge (requested 10.0, filled 5.0 => adds 50% of working weights)
    let inc_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "w4-partial-scalein".to_string(),
            side: OrderSide::Long,
            limit_price: 102.0,
            requested_quantity: 10.0,
            reserved_margin_usdc: 500.0,
            portfolio_weight: 0.10,
            s1_weight_contrib: 0.06,
            s2_weight_contrib: 0.04,
            s3_weight_contrib: 0.0,
            protective_stop_px: 92.0,
            take_profit_px: None,
            exchange_order_id: Some("hl-w4-2".to_string()),
            reason: "INCREASE".to_string(),
        })
        .unwrap();
    db.merge_working_scale_in_into_open(
        inc_pid,
        pid,
        102.0,
        5.0,
        0.2,
        Some("hl-w4-2"),
        "INCREASE_PARTIAL",
    )
    .unwrap();
    let merged = db.get_position_by_id(pid).unwrap().unwrap();
    // 0.08 + 0.10*0.5 = 0.13; s1: 0.048 + 0.03 = 0.078; s2: 0.032 + 0.02 = 0.052; qty = 9.0
    assert!((merged.filled_quantity - 9.0).abs() < 1e-6);
    assert!((merged.portfolio_weight - 0.13).abs() < 1e-6);
    assert!((merged.s1_weight_contrib - 0.078).abs() < 1e-6);
    assert!((merged.s2_weight_contrib - 0.052).abs() < 1e-6);

    // 3. Partial position exit (9.0 -> 3.0 remaining => 1/3 scaling)
    db.record_partial_position_exit(pid, 6.0, 3.0, 105.0, 0.3, "REDUCE")
        .unwrap();
    let reduced = db.get_position_by_id(pid).unwrap().unwrap();
    assert!((reduced.filled_quantity - 3.0).abs() < 1e-6);
    assert!((reduced.portfolio_weight - 0.0433).abs() < 1e-4);
    assert!((reduced.s1_weight_contrib - 0.026).abs() < 1e-4);
    assert!((reduced.s2_weight_contrib - 0.0173).abs() < 1e-4);
}

#[test]
fn test_r14_w1_sync_open_preserves_working_scale_in_audits_dedup_and_relinks() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let doge_id = seed_test_asset(&db, "DOGE", 12, 0.2000);
    let now = now_ms();

    // 1. Seed OPEN LONG parent (5,000 DOGE) + same-side WORKING INCREASE scale-in (+1,006 DOGE)
    let open_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: doge_id,
            client_order_id: "v2-doge-long-open-parent".to_string(),
            side: OrderSide::Long,
            limit_price: 0.2000,
            requested_quantity: 5_000.0,
            reserved_margin_usdc: 500.0,
            portfolio_weight: 0.20,
            s1_weight_contrib: 0.20,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.1900,
            take_profit_px: Some(0.2200),
            exchange_order_id: Some("hl-90001".to_string()),
            reason: "OPEN_LONG".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        open_pid,
        0.2000,
        5_000.0,
        0.15,
        0.1900,
        Some(0.2200),
        Some("hl-90001"),
        "FILLED",
    )
    .unwrap();

    let inc_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: doge_id,
            client_order_id: "v2-doge-long-scalein-174773".to_string(),
            side: OrderSide::Long,
            limit_price: 0.1999,
            requested_quantity: 1_006.0,
            reserved_margin_usdc: 100.55,
            portfolio_weight: 0.04,
            s1_weight_contrib: 0.04,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.1900,
            take_profit_px: Some(0.2200),
            exchange_order_id: None,
            reason: "INCREASE".to_string(),
        })
        .unwrap();

    // 2. Repeated 15s sync_open_position_from_exchange_v2 MUST preserve same-side WORKING scale-in
    for _ in 0..3 {
        let (synced_pid, newly_adopted) = db
            .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
                asset_id: doge_id,
                coin: "DOGE",
                side: OrderSide::Long,
                entry_price: 0.2000,
                mark_price: 0.2010,
                quantity: 5_000.0,
                committed_notional_usdc: 1_005.0,
                unrealized_pnl_usdc: 5.0,
                portfolio_weight: 0.20,
                protective_stop_px: 0.1900,
                take_profit_px: 0.2200,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(now - 60_000),
            })
            .unwrap();
        assert_eq!(synced_pid, open_pid);
        assert!(!newly_adopted);
    }

    let inc_row = db.get_position_by_id(inc_pid).unwrap().unwrap();
    assert_eq!(
        inc_row.status, "WORKING",
        "Same-side WORKING scale-in must NEVER be cancelled by DEDUP_EXCHANGE_RECONCILE"
    );
    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 1);
    assert_eq!(ordered[0].position_id, inc_pid);
    assert!((ordered[0].requested_quantity - 1_006.0).abs() < 1e-6);
    assert!((ordered[0].reserved_margin_usdc - 100.55).abs() < 1e-4);

    // 3. Upsert missing exchange_order_id onto WORKING row and verify idempotency
    assert!(db
        .set_working_order_exchange_id(inc_pid, "hl-91001")
        .unwrap());
    assert!(!db
        .set_working_order_exchange_id(inc_pid, "hl-91001")
        .unwrap());
    assert_eq!(
        db.get_position_by_id(inc_pid)
            .unwrap()
            .unwrap()
            .exchange_order_id
            .as_deref(),
        Some("hl-91001")
    );

    // 4. Duplicate OPEN row + opposite-side WORKING row ARE deduplicated WITH explicit order_audit entries
    let dup_open_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: doge_id,
            client_order_id: "v2-doge-long-dup-open".to_string(),
            side: OrderSide::Long,
            limit_price: 0.2000,
            requested_quantity: 1_000.0,
            reserved_margin_usdc: 100.0,
            portfolio_weight: 0.04,
            s1_weight_contrib: 0.04,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.1900,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "DUP_OPEN".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        dup_open_pid,
        0.2000,
        1_000.0,
        0.0,
        0.1900,
        None,
        None,
        "DUP",
    )
    .unwrap();
    let opp_short_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: doge_id,
            client_order_id: "v2-doge-short-stale".to_string(),
            side: OrderSide::Short,
            limit_price: 0.2020,
            requested_quantity: 500.0,
            reserved_margin_usdc: 50.0,
            portfolio_weight: -0.02,
            s1_weight_contrib: -0.02,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.2120,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "OPP_SHORT".to_string(),
        })
        .unwrap();

    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: doge_id,
        coin: "DOGE",
        side: OrderSide::Long,
        entry_price: 0.2000,
        mark_price: 0.2010,
        quantity: 5_000.0,
        committed_notional_usdc: 1_005.0,
        unrealized_pnl_usdc: 5.0,
        portfolio_weight: 0.20,
        protective_stop_px: 0.1900,
        take_profit_px: 0.2200,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(now - 60_000),
    })
    .unwrap();

    assert_eq!(
        db.get_position_by_id(inc_pid).unwrap().unwrap().status,
        "WORKING"
    );
    assert_eq!(
        db.get_position_by_id(dup_open_pid).unwrap().unwrap().status,
        "CANCELLED"
    );
    assert_eq!(
        db.get_position_by_id(opp_short_pid)
            .unwrap()
            .unwrap()
            .status,
        "CANCELLED"
    );
    let audits = db.list_recent_order_audits(30).unwrap();
    assert!(audits.iter().any(|a| {
        a.position_id == dup_open_pid
            && a.to_state == "CANCELLED"
            && a.reason == "DEDUP_EXCHANGE_RECONCILE"
    }));
    assert!(audits.iter().any(|a| {
        a.position_id == opp_short_pid
            && a.to_state == "CANCELLED"
            && a.reason == "REPLACED_BY_EXCHANGE_OPEN"
    }));
    assert!(!audits
        .iter()
        .any(|a| a.position_id == inc_pid && a.to_state == "CANCELLED"));

    // 5. Prematurely cancelled WORKING row is cleanly re-linked back to WORKING with reserved_margin_usdc
    db.cancel_working_order(inc_pid, "SIMULATED_PREMATURE_CANCEL")
        .unwrap();
    assert!(db.get_assets_ordered().unwrap().is_empty());
    let (relinked_id, changed) = db
        .adopt_or_relink_resting_working_order(
            &WorkingOrderInput {
                asset_id: doge_id,
                client_order_id: "hl-working-doge-91001".to_string(),
                side: OrderSide::Long,
                limit_price: 0.1999,
                requested_quantity: 1_006.0,
                reserved_margin_usdc: 100.55,
                portfolio_weight: 0.04,
                s1_weight_contrib: 0.04,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 0.1900,
                take_profit_px: Some(0.2200),
                exchange_order_id: Some("hl-91001".to_string()),
                reason: "HL_RECONCILE_RELINKED_RESTING_ORDER".to_string(),
            },
            Some(inc_pid),
        )
        .unwrap();
    assert_eq!(relinked_id, inc_pid);
    assert!(changed);
    let ordered_relinked = db.get_assets_ordered().unwrap();
    assert_eq!(ordered_relinked.len(), 1);
    assert_eq!(ordered_relinked[0].position_id, inc_pid);
    assert!((ordered_relinked[0].reserved_margin_usdc - 100.55).abs() < 1e-4);
}

#[test]
fn test_r14_w3_partial_reduce_single_booking_scale_in_zero_and_close_parity() {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 5, 100.0);
    let t_open = now_ms() - 3_600_000;

    // 1. Open 2.48 SOL @ $100.00 (fee $0.0868) + merge 1.00 SOL scale-in @ $100.00 (fee $0.0350) => 3.48 SOL @ $100.00, fee $0.1218
    let open_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "v2-sol-long-w3-parent".to_string(),
            side: OrderSide::Long,
            limit_price: 100.0,
            requested_quantity: 2.48,
            reserved_margin_usdc: 124.0,
            portfolio_weight: 0.124,
            s1_weight_contrib: 0.124,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 94.0,
            take_profit_px: Some(115.0),
            exchange_order_id: Some("hl-7001".to_string()),
            reason: "OPEN_LONG".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        open_pid,
        100.0,
        2.48,
        0.0868,
        94.0,
        Some(115.0),
        Some("hl-7001"),
        "FILLED",
    )
    .unwrap();
    {
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            "UPDATE positions SET created_at_ms = :t, opened_at_ms = :t, funding_accrued_usdc = -0.1220 WHERE id = :pid",
            params! { "t" => t_open, "pid" => open_pid },
        )
        .unwrap();
    }

    let scale_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol_id,
            client_order_id: "v2-sol-long-w3-scalein".to_string(),
            side: OrderSide::Long,
            limit_price: 100.0,
            requested_quantity: 1.00,
            reserved_margin_usdc: 50.0,
            portfolio_weight: 0.05,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 94.0,
            take_profit_px: Some(115.0),
            exchange_order_id: Some("hl-7001b".to_string()),
            reason: "INCREASE".to_string(),
        })
        .unwrap();
    db.merge_working_scale_in_into_open(
        scale_pid,
        open_pid,
        100.0,
        1.00,
        0.0350,
        Some("hl-7001b"),
        "INCREASE_FILLED",
    )
    .unwrap();

    // Pollute SCALE_IN_MERGED child row (reproducing #174553) and seed pre-existing phantom hl-closed-sol-7002 (#174737)
    let phantom_id: i64 = {
        let mut conn = db.get_conn().unwrap();
        conn.exec_drop(
            "UPDATE positions SET realized_pnl_usdc = 1.2500, fees_paid_usdc = 0.0900, funding_accrued_usdc = -0.0400 WHERE id = :id",
            params! { "id" => scale_pid },
        )
        .unwrap();
        conn.exec_drop(
            r#"INSERT INTO positions (
                   asset_id, client_order_id, side, status, fsm_state, entry_price, current_price, exit_price,
                   requested_quantity, filled_quantity, protective_stop_px, entry_r0, realized_pnl_usdc,
                   fees_paid_usdc, funding_accrued_usdc, exchange_order_id, created_at_ms, opened_at_ms,
                   closed_at_ms, updated_at_ms, exit_reason
               ) VALUES (
                   :aid, 'hl-closed-sol-7002', 'LONG', 'CLOSED', 'CLOSED', 100.0, 102.5, 102.5,
                   1.04, 1.04, 0.0, 0.0, 0.8332,
                   0.0373, 0.0, 'hl-7002', :t_open, :t_open,
                   :t_close, :t_close, 'HL_USER_FILL_RECONCILED'
               )"#,
            params! { "aid" => sol_id, "t_open" => t_open, "t_close" => t_open + 600_000 },
        )
        .unwrap();
        let pid = conn.last_insert_id() as i64;
        conn.exec_drop(
            r#"INSERT INTO order_audit (
                   position_id, asset_id, client_order_id, side, from_state, to_state,
                   requested_qty, filled_qty, limit_price, avg_fill_price,
                   reserved_usdc, committed_usdc, fees_usdc, reason, created_at_ms, updated_at_ms
               ) VALUES (
                   :pid, :aid, 'hl-closed-sol-7002', 'LONG', 'OPEN', 'CLOSED',
                   1.04, 1.04, 102.5, 102.5, 0, 0, 0.0373, 'LEGACY_SPLIT_AUDIT', :ts, :ts
               )"#,
            params! { "pid" => pid, "aid" => sol_id, "ts" => t_open + 600_000 },
        )
        .unwrap();
        pid
    };

    // 2. Record 3 partial REDUCE exits leaving 1.49 SOL OPEN:
    // -1.04 @ 102.50 (fee 0.0373 => +2.5627), -0.72 @ 103.00 (fee 0.0260 => +2.1340), -0.23 @ 101.80 (fee 0.0082 => +0.4058)
    db.record_partial_position_exit(open_pid, 1.04, 2.44, 102.50, 0.0373, "ACTION_REDUCE")
        .unwrap();
    db.record_partial_position_exit(open_pid, 0.72, 1.72, 103.00, 0.0260, "ACTION_REDUCE")
        .unwrap();
    db.record_partial_position_exit(open_pid, 0.23, 1.49, 101.80, 0.0082, "ACTION_REDUCE")
        .unwrap();

    // 3. Reconcile closed trades while SOL is still OPEN -> skips active open-epoch `hl-closed-*` rows,
    // self-heals/deletes phantom_id (repointing its audit FK to open_pid), and zeroes SCALE_IN_MERGED child!
    let active_trim_inputs = vec![
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-7002".to_string(),
            exchange_order_id: "hl-7002".to_string(),
            close_epoch_id: Some("SOL:1".to_string()),
            side: OrderSide::Long,
            entry_price: 100.0,
            exit_price: 102.50,
            filled_quantity: 1.04,
            realized_pnl_usdc: 2.5254,
            fees_paid_usdc: 0.0746,
            funding_accrued_usdc: -0.05,
            created_at_ms: t_open,
            closed_at_ms: t_open + 600_000,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-7003".to_string(),
            exchange_order_id: "hl-7003".to_string(),
            close_epoch_id: Some("SOL:1".to_string()),
            side: OrderSide::Long,
            entry_price: 100.0,
            exit_price: 103.00,
            filled_quantity: 0.72,
            realized_pnl_usdc: 2.1340,
            fees_paid_usdc: 0.0260,
            funding_accrued_usdc: 0.0,
            created_at_ms: t_open,
            closed_at_ms: t_open + 1_200_000,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
    ];
    for _ in 0..3 {
        let processed = db
            .reconcile_closed_positions_from_exchange_fills(&active_trim_inputs)
            .unwrap();
        assert_eq!(processed, 0);
    }

    assert!(
        db.get_position_by_id(phantom_id).unwrap().is_none(),
        "Pre-existing phantom hl-closed-* row in active open epoch must be deleted"
    );
    let audits = db.list_recent_order_audits(30).unwrap();
    assert!(
        audits
            .iter()
            .any(|a| a.position_id == open_pid && a.reason == "LEGACY_SPLIT_AUDIT"),
        "Audit FK from healed phantom row must be repointed to OPEN survivor"
    );
    let healed_scale_child = db.get_position_by_id(scale_pid).unwrap().unwrap();
    assert_eq!(
        healed_scale_child.exit_reason.as_deref(),
        Some("SCALE_IN_MERGED")
    );
    assert!((healed_scale_child.realized_pnl_usdc - 0.0).abs() < 1e-9);
    assert!((healed_scale_child.fees_paid_usdc - 0.0).abs() < 1e-9);
    assert!((healed_scale_child.funding_accrued_usdc - 0.0).abs() < 1e-9);

    let open_after_trims = db.get_position_by_id(open_pid).unwrap().unwrap();
    let expected_partial_rpnl = (2.60 - 0.0373) + (2.16 - 0.0260) + (0.414 - 0.0082); // 5.1025
    assert!((open_after_trims.realized_pnl_usdc - expected_partial_rpnl).abs() < 1e-4);
    assert!((db.sum_realized_pnl_usdc().unwrap() - expected_partial_rpnl).abs() < 1e-4);
    let pnl_while_open = db
        .compute_pnl_summary_uncached(0.0, -0.1220, 10_000.0)
        .unwrap();
    assert!((pnl_while_open.daily.realized_pnl_usdc - 0.0).abs() < 1e-6);

    // 4. Close final 1.49 SOL @ $104.20 (fee $0.0543) -> full lifecycle net PnL == +$11.0624 to < 1e-4
    let expected_full_net =
        (2.60 + 2.16 + 0.414 + 6.258) - (0.1218 + 0.0373 + 0.0260 + 0.0082 + 0.0543) - 0.1220;
    let final_rpnl = db
        .close_position(open_pid, 104.20, 0.0543, "FINAL_EXIT")
        .unwrap();
    assert!(
        (final_rpnl - expected_full_net).abs() < 1e-4,
        "close_position final_rpnl={final_rpnl} expected={expected_full_net}"
    );

    // 5. Repeated reconcile_closed_positions_from_exchange_fills after flat matches open_pid 1:1 (never SCALE_IN_MERGED!)
    let t_close_final = now_ms() - 45_000; // >30s gap proves opened_at_ms Pass-2 matching works cleanly too!
    let full_epoch_inputs = vec![
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-7002".to_string(),
            exchange_order_id: "hl-7002".to_string(),
            close_epoch_id: Some("SOL:1".to_string()),
            side: OrderSide::Long,
            entry_price: 100.0,
            exit_price: 102.50,
            filled_quantity: 1.04,
            realized_pnl_usdc: 2.60 - 0.0373 - (0.1218 * (1.04 / 3.48)) - 0.05,
            fees_paid_usdc: 0.0373 + (0.1218 * (1.04 / 3.48)),
            funding_accrued_usdc: -0.05,
            created_at_ms: t_open,
            closed_at_ms: t_open + 600_000,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-7003".to_string(),
            exchange_order_id: "hl-7003".to_string(),
            close_epoch_id: Some("SOL:1".to_string()),
            side: OrderSide::Long,
            entry_price: 100.0,
            exit_price: 103.00,
            filled_quantity: 0.72,
            realized_pnl_usdc: 2.16 - 0.0260 - (0.1218 * (0.72 / 3.48)) - 0.04,
            fees_paid_usdc: 0.0260 + (0.1218 * (0.72 / 3.48)),
            funding_accrued_usdc: -0.04,
            created_at_ms: t_open,
            closed_at_ms: t_open + 1_200_000,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-7004".to_string(),
            exchange_order_id: "hl-7004".to_string(),
            close_epoch_id: Some("SOL:1".to_string()),
            side: OrderSide::Long,
            entry_price: 100.0,
            exit_price: 101.80,
            filled_quantity: 0.23,
            realized_pnl_usdc: 0.414 - 0.0082 - (0.1218 * (0.23 / 3.48)) - 0.01,
            fees_paid_usdc: 0.0082 + (0.1218 * (0.23 / 3.48)),
            funding_accrued_usdc: -0.01,
            created_at_ms: t_open,
            closed_at_ms: t_open + 1_800_000,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-7005".to_string(),
            exchange_order_id: "hl-7005".to_string(),
            close_epoch_id: Some("SOL:1".to_string()),
            side: OrderSide::Long,
            entry_price: 100.0,
            exit_price: 104.20,
            filled_quantity: 1.49,
            realized_pnl_usdc: 6.258 - 0.0543 - (0.1218 * (1.49 / 3.48)) - 0.0220,
            fees_paid_usdc: 0.0543 + (0.1218 * (1.49 / 3.48)),
            funding_accrued_usdc: -0.0220,
            created_at_ms: t_open,
            closed_at_ms: t_close_final,
            exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
        },
    ];
    for _ in 0..3 {
        let processed = db
            .reconcile_closed_positions_from_exchange_fills(&full_epoch_inputs)
            .unwrap();
        assert_eq!(processed, 1);
    }

    let non_merged_closed: Vec<_> = db
        .list_closed_positions(20)
        .unwrap()
        .into_iter()
        .filter(|p| p.exit_reason.as_deref() != Some("SCALE_IN_MERGED"))
        .collect();
    assert_eq!(non_merged_closed.len(), 1);
    assert_eq!(non_merged_closed[0].id, open_pid);
    assert!((non_merged_closed[0].realized_pnl_usdc - expected_full_net).abs() < 1e-4);
    assert!((non_merged_closed[0].fees_paid_usdc - 0.2476).abs() < 1e-4);
    assert!((non_merged_closed[0].funding_accrued_usdc - (-0.1220)).abs() < 1e-4);
    assert!((non_merged_closed[0].filled_quantity - 3.48).abs() < 1e-6);

    let scale_child_after = db.get_position_by_id(scale_pid).unwrap().unwrap();
    assert!((scale_child_after.realized_pnl_usdc - 0.0).abs() < 1e-9);
    assert!((scale_child_after.fees_paid_usdc - 0.0).abs() < 1e-9);
}

#[test]
fn test_pnl_summary_flat_account_zero_unrealized_and_unified_vault_exclusion() {
    use aegis_core::{
        CachedHlPortfolioPnl, HlPortfolioWindowCache, VaultInfoPayload, VaultPositionInfo,
    };

    let db = DatabaseManager::new_isolated_test(None).expect("isolated test DB");
    let sol_id = seed_test_asset(&db, "SOL", 1, 70.0);
    let eth_id = seed_test_asset(&db, "ETH", 2, 1700.0);
    let now = now_ms();
    let day_ms = 86_400_000i64;

    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("live_exchange_account_mode", "unifiedAccount", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();

    // Closed trades:
    // 1) Boundary-crossing trade opened 36h ago and closed 2h ago (+10.00): crosses 24h start,
    //    so daily has boundary_crossing_cnt = 1 (flat now, but carried prior-window M2M),
    //    whereas weekly/monthly/ytd are strictly flat-to-flat (boundary_crossing_cnt = 0).
    // 2) Intra-day trade opened 4h ago and closed 1h ago (-2.50).
    db.reconcile_closed_positions_from_exchange_fills(&[
        ReconciledClosedTradeInput {
            asset_id: sol_id,
            client_order_id: "hl-closed-sol-cross24h".to_string(),
            exchange_order_id: "hl-sol-cross".to_string(),
            close_epoch_id: None,
            side: OrderSide::Long,
            entry_price: 68.0,
            exit_price: 70.0,
            filled_quantity: 5.0,
            realized_pnl_usdc: 10.0,
            fees_paid_usdc: 0.10,
            funding_accrued_usdc: 0.20,
            created_at_ms: now - 36 * 3_600_000,
            closed_at_ms: now - 2 * 3_600_000,
            exit_reason: "CLOSE".to_string(),
        },
        ReconciledClosedTradeInput {
            asset_id: eth_id,
            client_order_id: "hl-closed-eth-intraday".to_string(),
            exchange_order_id: "hl-eth-intra".to_string(),
            close_epoch_id: None,
            side: OrderSide::Short,
            entry_price: 1700.0,
            exit_price: 1702.5,
            filled_quantity: 1.0,
            realized_pnl_usdc: -2.50,
            fees_paid_usdc: 0.05,
            funding_accrued_usdc: 0.0,
            created_at_ms: now - 4 * 3_600_000,
            closed_at_ms: now - 3_600_000,
            exit_reason: "CLOSE".to_string(),
        },
    ])
    .unwrap();

    // Verify opened_at_ms guard against synthetic paginated pre-history timestamps:
    // re-reconciling SOL with created_at_ms far earlier (< created_at_ms - 60s) must keep opened_at_ms intact.
    db.reconcile_closed_positions_from_exchange_fills(&[ReconciledClosedTradeInput {
        asset_id: sol_id,
        client_order_id: "hl-closed-sol-cross24h".to_string(),
        exchange_order_id: "hl-sol-cross".to_string(),
        close_epoch_id: None,
        side: OrderSide::Long,
        entry_price: 68.0,
        exit_price: 70.0,
        filled_quantity: 5.0,
        realized_pnl_usdc: 10.0,
        fees_paid_usdc: 0.10,
        funding_accrued_usdc: 0.20,
        created_at_ms: now - 10 * day_ms,
        closed_at_ms: now - 2 * 3_600_000,
        exit_reason: "CLOSE".to_string(),
    }])
    .unwrap();
    let sol_closed = db
        .list_closed_positions(10)
        .unwrap()
        .into_iter()
        .find(|p| p.client_order_id == "hl-closed-sol-cross24h")
        .unwrap();
    assert!(sol_closed.opened_at_ms.unwrap() >= now - 36 * 3_600_000 - 1_000);

    // Vault earned +$0.50 all-time over 4 days => ~+$0.125 in 24h and +$0.50 in 7d/30d/YTD.
    let vault_payload = VaultInfoPayload {
        configured_vault_address: "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0".to_string(),
        total_vault_equity_usdc: 100.50,
        combined_portfolio_equity_usdc: 1979.50,
        vault_count: 1,
        synced_at_ms: now,
        vaults: vec![VaultPositionInfo {
            vault_address: "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0".to_string(),
            name: "HLP".to_string(),
            equity_usdc: 100.50,
            pnl_usdc: 0.50,
            all_time_pnl_usdc: 0.50,
            days_following: 4,
            ..Default::default()
        }],
    };
    db.set_config_value(
        "live_vault_info_json",
        &serde_json::to_string(&vault_payload).unwrap(),
        None,
    )
    .unwrap();

    // Account unified series has +$5.125 over exact 24h (so after subtracting ~$0.125 vault => +$5.00),
    // while perp_* carries an intentional -$2.00 offset to prove unifiedAccount selects account_*.
    let hl_cache = CachedHlPortfolioPnl {
        synced_at_ms: now,
        perp_day: HlPortfolioWindowCache {
            pnl_history: vec![(now - day_ms, 0.0), (now, 3.00)],
            vlm: 1000.0,
        },
        perp_week: HlPortfolioWindowCache {
            pnl_history: vec![(now - 7 * day_ms, 0.0), (now, 5.50)],
            vlm: 1000.0,
        },
        perp_month: HlPortfolioWindowCache {
            pnl_history: vec![(now - 30 * day_ms, 0.0), (now, 5.50)],
            vlm: 1000.0,
        },
        perp_all_time: HlPortfolioWindowCache {
            pnl_history: vec![(now - 3 * day_ms, 0.0), (now, 5.50)],
            vlm: 1000.0,
        },
        account_day: HlPortfolioWindowCache {
            pnl_history: vec![(now - day_ms, 0.0), (now, 5.125)],
            vlm: 1000.0,
        },
        account_week: HlPortfolioWindowCache {
            pnl_history: vec![(now - 7 * day_ms, 0.0), (now, 8.10)],
            vlm: 1000.0,
        },
        account_month: HlPortfolioWindowCache {
            pnl_history: vec![(now - 30 * day_ms, 0.0), (now, 8.10)],
            vlm: 1000.0,
        },
        account_all_time: HlPortfolioWindowCache {
            pnl_history: vec![(now - 3 * day_ms, 0.0), (now, 8.10)],
            vlm: 1000.0,
        },
    };
    db.set_config_value(
        "live_exchange_portfolio_pnl_json",
        &serde_json::to_string(&hl_cache).unwrap(),
        None,
    )
    .unwrap();

    // Even if caller passes stale nonzero uPnL args, DB has 0 OPEN positions => unrealized must be 0.00!
    let summary = db.compute_pnl_summary(0.0, 0.0, 1000.0).unwrap();

    // 1. Daily has boundary_crossing_cnt = 1: account_day (+5.125) - vault 24h (+0.125) = +5.00 net
    //    Realized = +7.50, Unrealized = 0.00, Prior M2M = -2.50 => 7.50 + 0.00 - 2.50 == 5.00.
    assert_eq!(summary.daily.pnl_source, "HL_PORTFOLIO");
    assert!((summary.daily.unrealized_pnl_usdc - 0.0).abs() < 1e-9);
    assert!((summary.daily.realized_pnl_usdc - 7.50).abs() < 1e-6);
    assert!((summary.daily.net_pnl_usdc - 5.00).abs() < 0.02);
    assert!(
        (summary.daily.realized_pnl_usdc
            + summary.daily.unrealized_pnl_usdc
            + summary.daily.m2m_carry_usdc
            - summary.daily.net_pnl_usdc)
            .abs()
            < 1e-6
    );
    assert!((summary.daily.m2m_carry_usdc - (-2.50)).abs() < 0.02);

    // 2. Weekly / Monthly / YTD are strictly flat-to-flat with all opens inside window:
    //    exact zero unrealized and zero Prior M2M despite HL account_week/month noise (+8.10 - 0.50 = +7.60 != +7.50).
    for h in [&summary.weekly, &summary.monthly, &summary.ytd] {
        assert_eq!(h.pnl_source, "HL_PORTFOLIO");
        assert!((h.unrealized_pnl_usdc - 0.0).abs() < 1e-9);
        assert!((h.m2m_carry_usdc - 0.0).abs() < 1e-9);
        assert!((h.realized_pnl_usdc - 7.50).abs() < 1e-6);
        assert!((h.net_pnl_usdc - 7.50).abs() < 1e-6);
    }
}
