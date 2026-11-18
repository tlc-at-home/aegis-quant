//! Comprehensive MariaDB 13.0 Rolling integration tests for `aegis-db` (`tests/db_integration.rs`).

use aegis_core::{OrderSide, RiskTier};
use aegis_db::{
    create_mariadb_backup, verify_latest_backup, AssetUpsertInput, BarUpsertInput, DatabaseManager,
    SignalUpsertInput, SnapshotInsertInput, WorkingOrderInput,
};
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
    let report = verify_latest_backup(&db).expect("verify_latest_backup");
    assert!(report.verified);
    assert_eq!(report.version, "2.0.0");
    let _ = std::fs::remove_file(backup_path);
}

#[test]
fn test_production_local_mariadb_seed_and_credential_migration() {
    let db = DatabaseManager::new(None, None).expect("connect to local aegis_quant MariaDB");
    db.init_schema(false)
        .expect("init_schema and migrate credentials into local aegis_quant");
    let cfg = db.load_config().expect("load_config on local aegis_quant");
    assert_eq!(cfg.risk_tier(), RiskTier::Extreme);
    assert!((cfg.target_leverage_multiplier() - 1.85).abs() < 1e-9);
    assert!(
        !cfg.get_str("wallet_address", "").is_empty(),
        "wallet_address should be migrated from legacy SQLite"
    );
    assert!(
        !cfg.get_str("hyperliquid_agent_address", "").is_empty(),
        "hyperliquid_agent_address should be migrated from legacy SQLite"
    );
    assert!(
        !cfg.get_str("private_key", "").is_empty(),
        "private_key should be migrated and decryptable"
    );
    assert!(
        !cfg.get_str("telegram_bot_token", "").is_empty(),
        "telegram_bot_token should be migrated and decryptable"
    );
    assert!(
        !cfg.get_str("telegram_chat_id", "").is_empty(),
        "telegram_chat_id should be migrated"
    );
}
