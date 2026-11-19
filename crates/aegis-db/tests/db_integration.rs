//! Comprehensive MariaDB 13.0 Rolling integration tests for `aegis-db` (`tests/db_integration.rs`).

use aegis_core::{now_ms, OrderSide, RiskTier};
use aegis_db::{
    create_mariadb_backup, verify_latest_backup, AssetUpsertInput, BarUpsertInput, DatabaseManager,
    ReconciledClosedTradeInput, SignalUpsertInput, SnapshotInsertInput, WorkingOrderInput,
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

    // Open position opened 2d ago with current mark $115 -> total uPnL = +$30.0 (24h delta = +$10.0)
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

    // open_fees_funding = +2.0 (3.0 funding - 1.0 open fee)
    let summary = db.compute_pnl_summary(30.0, 2.0, 2562.0).unwrap();
    assert!((summary.daily.baseline_equity_usdc - 2500.0).abs() < 1e-6);
    assert!((summary.daily.realized_pnl_usdc - 50.0).abs() < 1e-6);
    assert!((summary.daily.unrealized_pnl_usdc - 12.0).abs() < 1e-6);
    assert!((summary.daily.net_pnl_usdc - 62.0).abs() < 1e-6);
    assert!((summary.daily.fees_funding_usdc - 6.0).abs() < 1e-6);
    assert_eq!(summary.daily.trade_count, 1);

    assert!((summary.weekly.baseline_equity_usdc - 2000.0).abs() < 1e-6);
    assert!((summary.weekly.realized_pnl_usdc - 550.0).abs() < 1e-6);
    assert!((summary.weekly.unrealized_pnl_usdc - 32.0).abs() < 1e-6);
    assert!((summary.weekly.net_pnl_usdc - 582.0).abs() < 1e-6);
    assert_eq!(summary.weekly.trade_count, 2);

    // Updating risk_capital_cap_usdc automatically advances risk_capital_cap_updated_at_ms
    db.set_config_value("risk_capital_cap_usdc", "1500.0", None)
        .unwrap();
    let cfg_cap = db.load_config().unwrap();
    assert!(cfg_cap.get_i64("risk_capital_cap_updated_at_ms", 0) >= now);
    let post_cap_summary = db.compute_pnl_summary(30.0, 2.0, 2562.0).unwrap();
    assert_eq!(post_cap_summary.daily.trade_count, 0);
    assert_eq!(post_cap_summary.weekly.trade_count, 0);

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
