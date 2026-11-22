//! Comprehensive MariaDB 13.0 Rolling integration tests for `aegis-db` (`tests/db_integration.rs`).

use aegis_core::{now_ms, ytd_start_ms, OrderSide, RiskTier};
use aegis_db::{
    canonicalize_asset_updates, canonicalize_asset_upserts, create_mariadb_backup,
    order_asset_upserts_by_lock_key, verify_latest_backup, AssetUpsertInput, BarUpsertInput,
    DatabaseManager, FundingEventUpsertInput, ReconciledClosedTradeInput, SignalUpsertInput,
    SnapshotInsertInput, WorkingOrderInput,
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

    // Verify init_schema(false) creates funding_events cleanly on existing DBs,
    // and init_schema(true) drops/recreates all 11 tables cleanly (FK order).
    {
        let mut conn = db.get_conn().unwrap();
        conn.query_drop("DROP TABLE IF EXISTS funding_events")
            .unwrap();
    }
    db.init_schema(false)
        .expect("init_schema(false) should recreate missing funding_events");
    let (tables_after_migrate, _) = db.list_tables_and_views().unwrap();
    assert_eq!(tables_after_migrate.len(), 11);
    assert!(tables_after_migrate.contains(&"funding_events".to_string()));

    db.init_schema(true)
        .expect("init_schema(true) should drop and recreate all 11 tables cleanly");
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
            exchange_order_id: Some("hl-work-1".to_string()),
            reason: "MULTI_SLEEVE".to_string(),
        })
        .unwrap();

    let legacy_open_ts = 1_795_020_000_000i64;
    let (synced_id, promoted) = db
        .sync_open_position_from_exchange_v2(
            arb_id,
            "ARB",
            OrderSide::Long,
            0.995,
            1.00,
            100.0,
            100.0,
            0.50,
            0.24,
            0.95,
            1.08,
            1.25,
            Some(legacy_open_ts),
        )
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
    db.update_position_tpsl_orders(work_id, 0.999, Some("sl-oid-1"), 1.001, Some("tp-oid-1"))
        .unwrap();

    let (resync_id, newly_adopted) = db
        .sync_open_position_from_exchange_v2(
            arb_id,
            "ARB",
            OrderSide::Long,
            0.995,
            1.00,
            100.0,
            100.0,
            0.50,
            0.24,
            0.95,
            1.08,
            2.50,
            Some(legacy_open_ts),
        )
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
        .sync_open_position_from_exchange_v2(
            eth_id,
            "ETH",
            OrderSide::Long,
            100.0,
            115.0,
            2.0,
            230.0,
            30.0,
            0.1,
            95.0,
            130.0,
            3.0,
            Some(two_days_ago),
        )
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
    let db = DatabaseManager::new(Some(std::path::Path::new(":memory:")), None).unwrap();
    db.init_schema(false).unwrap();

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
        .sync_open_position_from_exchange_v2(
            btc_id,
            "BTC",
            OrderSide::Long,
            90_000.0,
            90_500.0,
            0.1,
            9_050.0,
            50.0,
            0.25,
            88_000.0,
            95_000.0,
            1.25,
            Some(1_700_000_000_000),
        )
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
        .sync_open_position_from_exchange_v2(
            sol_id,
            "SOL",
            OrderSide::Long,
            190.0,
            200.0,
            5.0,
            1000.0,
            50.0,
            0.2,
            180.0,
            220.0,
            0.0,
            Some(now_ms() - 120_000),
        )
        .unwrap();
    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].position_id, open_pid);
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
        .sync_open_position_from_exchange_v2(
            met_id,
            "MET",
            OrderSide::Long,
            0.23227,
            0.18872,
            9614.0,
            1814.35,
            -418.69,
            0.18,
            0.18165,
            0.25086,
            698.08,
            Some(two_days_ago - day_ms),
        )
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
    };
    db.set_config_value(
        "live_exchange_portfolio_pnl_json",
        &serde_json::to_string(&hl_cache).unwrap(),
        None,
    )
    .unwrap();

    // Closed trade 2h ago (+12.00 realized) verifies unrealized = net - realized identity
    db.reconcile_closed_positions_from_exchange_fills(&[ReconciledClosedTradeInput {
        asset_id: met_id,
        client_order_id: "hl-closed-met-2h".to_string(),
        exchange_order_id: "hl-met-2h".to_string(),
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
    assert!((hl_summary.daily.net_pnl_usdc - 242.70).abs() < 1e-6);
    assert!((hl_summary.daily.realized_pnl_usdc - 12.0).abs() < 1e-6);
    assert!((hl_summary.daily.unrealized_pnl_usdc - 230.70).abs() < 1e-6);

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
