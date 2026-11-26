//! Round-14 Finding W4 Integration Tests (`aegis-execution`):
//! 1. Like-for-like equity-cliff comparators (`DefaultModeCutover`, `PerpToPerp`, `UnifiedToUnified`).
//! 2. Explicit per-`(db_name, wallet)` REST escalation policy (`60s -> 120s -> 240s -> 300s` backoff).
//! 3. Bounded per-key repeated-`WARN` deduplication (`300_000 ms` summary window + clear reporting).

use aegis_core::OrderSide;
use aegis_db::{AssetUpsertInput, DatabaseManager, ExchangeOpenPositionSyncInput};
use aegis_execution::reconcile::equity::{
    commit_equity_snapshot_locked, evaluate_equity_snapshot_unlocked, extract_open_coins_and_szi,
    fetch_account_mode_unlocked, inspect_cliff_escalation_state, refresh_equity_snapshot_at_ms,
    EquityCliffKind, WarnDedupDecision, ABSTRACTION_SYNC_MIN_INTERVAL_MS,
    CLIFF_ESCALATION_BASE_COOLDOWN_MS, CLIFF_ESCALATION_MAX_BACKOFF_MS,
    UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS,
};
use aegis_execution::{CutoverReconciler, HyperliquidHttpClient};
use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

fn fresh_isolated_db() -> DatabaseManager {
    let db = DatabaseManager::new_isolated_test(None).unwrap();
    db.init_schema().unwrap();
    db
}

fn seed_open_sol_position(db: &DatabaseManager) {
    let asset_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "SOL".to_string(),
            asset_index: 5,
            sz_decimals: 2,
            tick_size: 0.01,
            lot_step_size: 0.01,
            max_leverage: 20.0,
            mark_price: 163.34,
            mid_price: 163.34,
            oracle_price: 163.40,
            funding_rate_hourly: 0.00001,
            day_volume_usdc: 5_000_000.0,
            open_interest_usdc: 2_000_000.0,
            prev_day_price: 160.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .unwrap();

    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id,
        coin: "SOL",
        side: OrderSide::Long,
        quantity: 1.50,
        entry_price: 162.00,
        mark_price: 163.34,
        committed_notional_usdc: 245.01,
        unrealized_pnl_usdc: 2.01,
        protective_stop_px: 155.00,
        take_profit_px: 175.00,
        portfolio_weight: 0.245,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(1_760_000_000_000),
    })
    .unwrap();
}

#[test]
fn test_w4_steady_state_unified_account_no_cliff_normal_throttles_and_zero_warns() {
    let db = fresh_isolated_db();
    seed_open_sol_position(&db);

    let abs_calls = Arc::new(AtomicUsize::new(0));
    let abs_srv = abs_calls.clone();
    let spot_calls = Arc::new(AtomicUsize::new(0));
    let spot_srv = spot_calls.clone();

    let ch_payload = json!({
        "marginSummary": {
            "accountValue": "21.80",
            "totalNtlPos": "245.01",
            "totalMarginUsed": "18.50"
        },
        "withdrawable": "3.30",
        "assetPositions": [{
            "position": {
                "coin": "SOL",
                "szi": "1.50",
                "entryPx": "162.00",
                "positionValue": "245.01",
                "unrealizedPnl": "2.01",
                "marginUsed": "18.50"
            }
        }]
    });
    let ch_for_srv = ch_payload.clone();

    let server = MockHttpServer::start(move |req| {
        let typ = req
            .json_body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("");
        match typ {
            "clearinghouseState" => MockHttpResponse::ok_json(&ch_for_srv),
            "userAbstraction" => {
                abs_srv.fetch_add(1, Ordering::SeqCst);
                MockHttpResponse::ok_json(&json!("unifiedAccount"))
            }
            "spotClearinghouseState" => {
                spot_srv.fetch_add(1, Ordering::SeqCst);
                MockHttpResponse::ok_json(&json!({
                    "balances": [
                        { "coin": "USDC", "token": 0, "total": "1871.00", "hold": "18.50" }
                    ],
                    "tokenToAvailableAfterMaintenance": [[0, "1852.50"]]
                }))
            }
            "frontendOpenOrders" | "openOrders" => MockHttpResponse::ok_json(&json!([
                {
                    "coin": "SOL",
                    "oid": 9001,
                    "side": "A",
                    "sz": "1.50",
                    "limitPx": "154.00",
                    "triggerPx": "155.00",
                    "isTrigger": true,
                    "isPositionTpsl": true,
                    "reduceOnly": true,
                    "orderType": "Stop Limit"
                },
                {
                    "coin": "SOL",
                    "oid": 9002,
                    "side": "A",
                    "sz": "1.50",
                    "limitPx": "175.00",
                    "triggerPx": "175.00",
                    "isTrigger": true,
                    "isPositionTpsl": true,
                    "reduceOnly": true,
                    "orderType": "Take Profit Limit"
                }
            ])),
            "userFillsByTime" | "userFills" => MockHttpResponse::ok_json(&json!([])),
            _ => MockHttpResponse::ok_json(&json!([])),
        }
    });

    let t0 = aegis_core::now_ms();
    let wallet = "0xcc70fb7fa081b3dcfbf495a4b1296a28f356a39f";
    db.set_config_value("execution_mode", "LIVE", None).unwrap();
    db.set_config_value("hyperliquid_mainnet_url", &server.base_url(), None)
        .unwrap();
    db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
        .unwrap();
    db.set_config_value("wallet_address", wallet, None).unwrap();
    db.set_config_value("live_exchange_account_mode", "unifiedAccount", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "1871.00", None)
        .unwrap();
    db.set_config_value("live_exchange_perp_margin_equity_usdc", "21.84", None)
        .unwrap();
    db.set_config_value("live_exchange_spot_usdc", "1871.00", None)
        .unwrap();
    db.set_config_value("live_exchange_spot_synced_at_ms", &t0.to_string(), None)
        .unwrap();
    db.set_config_value("risk_capital_cap_usdc", "1000.00", None)
        .unwrap();

    let http = HyperliquidHttpClient::new(3);
    let cfg_init = db.load_config_or_default();
    // Prime in-memory userAbstraction cache at `t0`
    let _ = fetch_account_mode_unlocked(
        &http,
        db.db_name(),
        &cfg_init,
        t0,
        ABSTRACTION_SYNC_MIN_INTERVAL_MS,
    );
    assert_eq!(abs_calls.load(Ordering::SeqCst), 1);
    assert_eq!(spot_calls.load(Ordering::SeqCst), 0);

    // 1. End-to-end 15s reconciler pass right after `t0` (< 15s spot throttle & < 300s abstraction throttle)
    let reconciler = CutoverReconciler::new(db.clone(), http.clone());
    let rep = reconciler.sync_db_with_hyperliquid().unwrap();
    assert!(rep.exchange_reachable);
    assert!((rep.exchange_equity_usdc - 1871.00).abs() < 1e-6);
    // Neither userAbstraction nor spotClearinghouseState bypassed its cache throttle!
    assert_eq!(abs_calls.load(Ordering::SeqCst), 1);
    assert_eq!(spot_calls.load(Ordering::SeqCst), 0);

    // 2. Simulate 24 consecutive 15s ticks (6 full minutes: t0 + 15s .. t0 + 360s) + sub-15s ticks
    let (open_coins, _) = extract_open_coins_and_szi(&ch_payload);
    assert!(open_coins.contains("SOL"));

    // Sub-15s check at t0 + 5_000 and t0 + 10_000: zero REST calls
    for sub_offset in [5_000_i64, 10_000_i64] {
        let cfg = db.load_config_or_default();
        let eval = evaluate_equity_snapshot_unlocked(
            &http,
            db.db_name(),
            &cfg,
            &ch_payload,
            &open_coins,
            true,
            t0 + sub_offset,
        );
        assert!(!eval.equity_cliff_detected);
        assert_eq!(eval.cliff_kind, None);
        assert!(!eval.forced_refresh_bypass);
        assert_eq!(eval.warn_decision, None);
        assert_eq!(abs_calls.load(Ordering::SeqCst), 1);
        assert_eq!(spot_calls.load(Ordering::SeqCst), 0);
    }

    for step in 1..=24_i64 {
        let ts = t0 + step * UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS;
        let cfg = db.load_config_or_default();
        let eval = evaluate_equity_snapshot_unlocked(
            &http,
            db.db_name(),
            &cfg,
            &ch_payload,
            &open_coins,
            true,
            ts,
        );
        assert!(
            !eval.equity_cliff_detected,
            "steady-state unifiedAccount tick {step} must never detect an equity cliff"
        );
        assert_eq!(eval.cliff_kind, None);
        assert!(!eval.refuse_equity_overwrite);
        assert!(!eval.forced_refresh_bypass);
        assert_eq!(eval.warn_decision, None);
        assert!((eval.effective_wallet_eq - 1871.00).abs() < 1e-6);

        let (eq_saved, _) = commit_equity_snapshot_locked(&db, &cfg, &eval, ts, 10_000.0);
        assert!((eq_saved - 1871.00).abs() < 1e-6);

        // Before 300s (steps 1..19): 0 extra userAbstraction calls; at 300s (step 20): 1 normal refresh
        if ts - t0 < ABSTRACTION_SYNC_MIN_INTERVAL_MS {
            assert_eq!(abs_calls.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(abs_calls.load(Ordering::SeqCst), 2);
        }
        // Exactly 1 normal spot sync per 15s tick
        assert_eq!(spot_calls.load(Ordering::SeqCst), step as usize);
    }

    let warn_logs: Vec<_> = db
        .list_recent_telemetry(100)
        .unwrap()
        .into_iter()
        .filter(|r| r.level == "WARN")
        .collect();
    assert!(
        warn_logs.is_empty(),
        "steady-state unifiedAccount must emit 0 WARN telemetry rows, got: {warn_logs:?}"
    );
}

#[test]
fn test_w4_default_to_unified_cutover_recovers_equity_and_does_not_retrigger() {
    let db = fresh_isolated_db();
    seed_open_sol_position(&db);

    let abs_calls = Arc::new(AtomicUsize::new(0));
    let abs_srv = abs_calls.clone();
    let spot_calls = Arc::new(AtomicUsize::new(0));
    let spot_srv = spot_calls.clone();

    let server = MockHttpServer::start(move |req| {
        let typ = req
            .json_body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("");
        match typ {
            "clearinghouseState" => MockHttpResponse::ok_json(&json!({
                "marginSummary": {
                    "accountValue": "73.02",
                    "totalNtlPos": "245.01",
                    "totalMarginUsed": "60.00"
                },
                "withdrawable": "13.02",
                "assetPositions": [{
                    "position": { "coin": "SOL", "szi": "1.50", "entryPx": "162.00" }
                }]
            })),
            "userAbstraction" => {
                abs_srv.fetch_add(1, Ordering::SeqCst);
                MockHttpResponse::ok_json(&json!("unifiedAccount"))
            }
            "spotClearinghouseState" => {
                spot_srv.fetch_add(1, Ordering::SeqCst);
                MockHttpResponse::ok_json(&json!({
                    "balances": [
                        { "coin": "USDC", "token": 0, "total": "1890.79", "hold": "60.00" }
                    ],
                    "tokenToAvailableAfterMaintenance": [[0, "1830.79"]]
                }))
            }
            _ => MockHttpResponse::ok_json(&json!([])),
        }
    });

    let t0 = 1_770_000_000_000_i64;
    let wallet = "0x3333333333333333333333333333333333333333";
    db.set_config_value("execution_mode", "LIVE", None).unwrap();
    db.set_config_value("hyperliquid_mainnet_url", &server.base_url(), None)
        .unwrap();
    db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
        .unwrap();
    db.set_config_value("wallet_address", wallet, None).unwrap();
    db.set_config_value("live_exchange_account_mode", "default", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "991.87", None)
        .unwrap();
    db.set_config_value("live_exchange_perp_margin_equity_usdc", "991.87", None)
        .unwrap();
    db.set_config_value("live_exchange_spot_synced_at_ms", &t0.to_string(), None)
        .unwrap();

    let http = HyperliquidHttpClient::new(3);
    let cfg0 = db.load_config_or_default();
    // Prime default-mode abstraction cache at t0 so only an escalation bypass can query at t0 + 1_000
    let _ = fetch_account_mode_unlocked(&http, db.db_name(), &cfg0, t0, 60_000);
    assert_eq!(abs_calls.load(Ordering::SeqCst), 1);

    // 1. Cutover occurs at t0 + 1_000 (!was_unified, prev_live_eq = 991.87, perp_eq = 73.02, spot = 1890.79)
    let ch_cutover = json!({
        "marginSummary": { "accountValue": "73.02", "totalMarginUsed": "60.00" },
        "withdrawable": "13.02",
        "assetPositions": [{ "position": { "coin": "SOL", "szi": "1.50", "entryPx": "162.00" } }]
    });
    let (open_coins, _) = extract_open_coins_and_szi(&ch_cutover);

    let eval_cutover = evaluate_equity_snapshot_unlocked(
        &http,
        db.db_name(),
        &cfg0,
        &ch_cutover,
        &open_coins,
        true,
        t0 + 1_000,
    );
    assert!(eval_cutover.equity_cliff_detected);
    assert_eq!(
        eval_cutover.cliff_kind,
        Some(EquityCliffKind::DefaultModeCutover)
    );
    assert!(eval_cutover.forced_refresh_bypass);
    assert!(!eval_cutover.refuse_equity_overwrite);
    assert_eq!(eval_cutover.account_mode, "unifiedAccount");
    assert!((eval_cutover.effective_wallet_eq - 1890.79).abs() < 1e-6);
    assert_eq!(abs_calls.load(Ordering::SeqCst), 2);
    assert_eq!(spot_calls.load(Ordering::SeqCst), 1);

    let (committed_eq, _) =
        commit_equity_snapshot_locked(&db, &cfg0, &eval_cutover, t0 + 1_000, 10_000.0);
    assert!((committed_eq - 1890.79).abs() < 1e-6);

    let cfg_after = db.load_config_or_default();
    assert_eq!(
        cfg_after.get_str("live_exchange_account_mode", ""),
        "unifiedAccount"
    );
    assert!((cfg_after.get_f64("live_exchange_equity_usdc", 0.0) - 1890.79).abs() < 1e-6);
    assert!((cfg_after.get_f64("live_exchange_perp_margin_equity_usdc", 0.0) - 73.02).abs() < 1e-6);

    // 2. Subsequent 15s ticks (+16s, +31s, +46s, +61s, +76s) MUST NOT re-trigger cliff or force userAbstraction!
    for tick in 1..=5_i64 {
        let ts = t0 + 1_000 + tick * 15_000;
        let cfg_tick = db.load_config_or_default();
        let eval_tick = evaluate_equity_snapshot_unlocked(
            &http,
            db.db_name(),
            &cfg_tick,
            &ch_cutover,
            &open_coins,
            true,
            ts,
        );
        assert!(
            !eval_tick.equity_cliff_detected,
            "post-cutover tick {tick} must not re-trigger equity cliff"
        );
        assert_eq!(eval_tick.cliff_kind, None);
        assert!(!eval_tick.forced_refresh_bypass);
        assert_eq!(eval_tick.warn_decision, None);
        assert!((eval_tick.effective_wallet_eq - 1890.79).abs() < 1e-6);
        let (eq_tick, _) = commit_equity_snapshot_locked(&db, &cfg_tick, &eval_tick, ts, 10_000.0);
        assert!((eq_tick - 1890.79).abs() < 1e-6);
        // userAbstraction stays at 2 across all subsequent 15s ticks (300s interval honored!)
        assert_eq!(abs_calls.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn test_w4_like_for_like_genuine_perp_cliff_and_unified_cliff_fire_immediately() {
    let db = fresh_isolated_db();
    seed_open_sol_position(&db);

    let spot_total_usdc = Arc::new(parking_lot::Mutex::new("1871.00".to_string()));
    let spot_srv_val = spot_total_usdc.clone();
    let abs_calls = Arc::new(AtomicUsize::new(0));
    let abs_srv = abs_calls.clone();

    let server = MockHttpServer::start(move |req| {
        let typ = req
            .json_body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("");
        match typ {
            "userAbstraction" => {
                abs_srv.fetch_add(1, Ordering::SeqCst);
                MockHttpResponse::ok_json(&json!("unifiedAccount"))
            }
            "spotClearinghouseState" => {
                let total = spot_srv_val.lock().clone();
                MockHttpResponse::ok_json(&json!({
                    "balances": [
                        { "coin": "USDC", "token": 0, "total": total, "hold": "20.00" }
                    ]
                }))
            }
            _ => MockHttpResponse::ok_json(&json!([])),
        }
    });

    let t0 = 1_780_000_000_000_i64;
    let wallet = "0x4444444444444444444444444444444444444444";
    db.set_config_value("execution_mode", "LIVE", None).unwrap();
    db.set_config_value("hyperliquid_mainnet_url", &server.base_url(), None)
        .unwrap();
    db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
        .unwrap();
    db.set_config_value("wallet_address", wallet, None).unwrap();
    db.set_config_value("live_exchange_account_mode", "unifiedAccount", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "1871.00", None)
        .unwrap();
    db.set_config_value("live_exchange_perp_margin_equity_usdc", "150.00", None)
        .unwrap();
    db.set_config_value("live_exchange_spot_usdc", "1871.00", None)
        .unwrap();
    db.set_config_value("live_exchange_spot_synced_at_ms", &t0.to_string(), None)
        .unwrap();

    let http = HyperliquidHttpClient::new(3);

    // Part A: Genuine perp-to-perp plunge from $150.00 -> $80.00 (>25% drop from prev_perp_eq >= $100)
    let ch_perp_drop = json!({
        "marginSummary": { "accountValue": "80.00", "totalMarginUsed": "20.00" },
        "withdrawable": "60.00",
        "assetPositions": [{ "position": { "coin": "SOL", "szi": "1.50", "entryPx": "162.00" } }]
    });
    let (open_coins, _) = extract_open_coins_and_szi(&ch_perp_drop);
    let cfg_a = db.load_config_or_default();
    let eval_a = evaluate_equity_snapshot_unlocked(
        &http,
        db.db_name(),
        &cfg_a,
        &ch_perp_drop,
        &open_coins,
        true,
        t0 + 5_000,
    );
    assert!(eval_a.equity_cliff_detected);
    assert_eq!(eval_a.cliff_kind, Some(EquityCliffKind::PerpToPerp));
    assert!(eval_a.forced_refresh_bypass);
    assert!(!eval_a.refuse_equity_overwrite);
    assert!((eval_a.effective_wallet_eq - 1871.00).abs() < 1e-6);
    commit_equity_snapshot_locked(&db, &cfg_a, &eval_a, t0 + 5_000, 10_000.0);

    // Next tick (+15s) clears cleanly because prev_perp_eq is now 80.00
    let cfg_a2 = db.load_config_or_default();
    let eval_a2 = evaluate_equity_snapshot_unlocked(
        &http,
        db.db_name(),
        &cfg_a2,
        &ch_perp_drop,
        &open_coins,
        true,
        t0 + 20_000,
    );
    assert!(!eval_a2.equity_cliff_detected);
    assert_eq!(eval_a2.cliff_kind, None);

    // Part B: Genuine unified-to-unified plunge from $1871.00 -> $1200.00 (>25% drop in unified wallet equity)
    *spot_total_usdc.lock() = "1200.00".to_string();
    let abs_before_b = abs_calls.load(Ordering::SeqCst);
    let cfg_b = db.load_config_or_default();
    // Advance past 60s escalation cooldown (t0 + 70_000) so escalation allows immediate mode verification
    let eval_b = evaluate_equity_snapshot_unlocked(
        &http,
        db.db_name(),
        &cfg_b,
        &ch_perp_drop,
        &open_coins,
        true,
        t0 + 70_000,
    );
    assert!(eval_b.equity_cliff_detected);
    assert_eq!(eval_b.cliff_kind, Some(EquityCliffKind::UnifiedToUnified));
    assert!(eval_b.forced_refresh_bypass);
    assert_eq!(abs_calls.load(Ordering::SeqCst), abs_before_b + 1);
    assert!(!eval_b.refuse_equity_overwrite);
    assert!((eval_b.effective_wallet_eq - 1200.00).abs() < 1e-6);
}

#[test]
fn test_w4_persistent_drop_failed_spot_protects_equity_respects_backoff_and_deduplicates_warns() {
    let db = fresh_isolated_db();
    seed_open_sol_position(&db);

    let fail_spot = Arc::new(AtomicBool::new(true));
    let fail_spot_srv = fail_spot.clone();
    let abs_calls = Arc::new(AtomicUsize::new(0));
    let abs_srv = abs_calls.clone();
    let spot_calls = Arc::new(AtomicUsize::new(0));
    let spot_srv = spot_calls.clone();

    let server = MockHttpServer::start(move |req| {
        let typ = req
            .json_body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("");
        match typ {
            "clearinghouseState" => MockHttpResponse::ok_json(&json!({
                "marginSummary": { "accountValue": "73.02", "totalMarginUsed": "60.00" },
                "withdrawable": "13.02",
                "assetPositions": [{
                    "position": { "coin": "SOL", "szi": "1.50", "entryPx": "162.00" }
                }]
            })),
            "userAbstraction" => {
                abs_srv.fetch_add(1, Ordering::SeqCst);
                MockHttpResponse::ok_json(&json!("unifiedAccount"))
            }
            "spotClearinghouseState" => {
                spot_srv.fetch_add(1, Ordering::SeqCst);
                if fail_spot_srv.load(Ordering::SeqCst) {
                    // Malformed / degraded payload missing "balances" -> fails immediately without 5xx sleep
                    MockHttpResponse::ok_json(&json!({ "error": "spot_indexing_lag" }))
                } else {
                    MockHttpResponse::ok_json(&json!({
                        "balances": [
                            { "coin": "USDC", "token": 0, "total": "1890.79", "hold": "60.00" }
                        ]
                    }))
                }
            }
            _ => MockHttpResponse::ok_json(&json!([])),
        }
    });

    let t0 = 1_790_000_000_000_i64;
    let wallet = "0x5555555555555555555555555555555555555555";
    db.set_config_value("execution_mode", "LIVE", None).unwrap();
    db.set_config_value("hyperliquid_mainnet_url", &server.base_url(), None)
        .unwrap();
    db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
        .unwrap();
    db.set_config_value("wallet_address", wallet, None).unwrap();
    db.set_config_value("live_exchange_account_mode", "default", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "1890.79", None)
        .unwrap();
    db.set_config_value("live_exchange_perp_margin_equity_usdc", "1890.79", None)
        .unwrap();
    db.set_config_value("live_exchange_spot_synced_at_ms", &t0.to_string(), None)
        .unwrap();

    let http = HyperliquidHttpClient::new(2);
    let ch_state = json!({
        "marginSummary": { "accountValue": "73.02", "totalMarginUsed": "60.00" },
        "withdrawable": "13.02",
        "assetPositions": [{
            "position": { "coin": "SOL", "szi": "1.50", "entryPx": "162.00" }
        }]
    });
    let (open_coins, _) = extract_open_coins_and_szi(&ch_state);

    // Simulate 28 failed 15s ticks: t = 0, 15s, 30s, ..., 405s
    // Expected forced bypass schedule:
    //   step 0  (t =   0s): attempt #1 -> next allowed at 60s  (+60s = CLIFF_ESCALATION_BASE_COOLDOWN_MS)
    //   step 4  (t =  60s): attempt #2 -> next allowed at 180s (+120s)
    //   step 12 (t = 180s): attempt #3 -> next allowed at 420s (+240s)
    // Expected WARN deduplication schedule:
    //   step 0  (t =   0s): EmitFirst
    //   steps 1..=19 (t = 15s..285s): Suppress { 1..=19 }
    //   step 20 (t = 300s): EmitSummary { suppressed_repeats: 19 }
    //   steps 21..=27 (t = 315s..405s): Suppress { 1..=7 }
    for step in 0..=27_i64 {
        let ts = t0 + step * 15_000;
        let cfg = db.load_config_or_default();
        let eval = evaluate_equity_snapshot_unlocked(
            &http,
            db.db_name(),
            &cfg,
            &ch_state,
            &open_coins,
            true,
            ts,
        );

        assert!(
            eval.equity_cliff_detected,
            "cliff must remain detected at step {step}"
        );
        assert!(
            eval.refuse_equity_overwrite,
            "refuse_equity_overwrite must protect prev_live_eq at step {step}"
        );
        assert!((eval.effective_wallet_eq - 1890.79).abs() < 1e-6);

        let should_bypass = matches!(step, 0 | 4 | 12);
        assert_eq!(
            eval.forced_refresh_bypass,
            should_bypass,
            "unexpected forced_refresh_bypass at step {step} (ts offset {}s)",
            step * 15
        );

        match step {
            0 => assert_eq!(eval.warn_decision, Some(WarnDedupDecision::EmitFirst)),
            1..=19 => assert_eq!(
                eval.warn_decision,
                Some(WarnDedupDecision::Suppress {
                    suppressed_repeats: step as u64
                })
            ),
            20 => assert_eq!(
                eval.warn_decision,
                Some(WarnDedupDecision::EmitSummary {
                    suppressed_repeats: 19
                })
            ),
            21..=27 => assert_eq!(
                eval.warn_decision,
                Some(WarnDedupDecision::Suppress {
                    suppressed_repeats: (step - 20) as u64
                })
            ),
            _ => unreachable!(),
        }

        let (eq_saved, _) = commit_equity_snapshot_locked(&db, &cfg, &eval, ts, 10_000.0);
        assert!((eq_saved - 1890.79).abs() < 1e-6);
        let cfg_post = db.load_config_or_default();
        assert!((cfg_post.get_f64("live_exchange_equity_usdc", 0.0) - 1890.79).abs() < 1e-6);
    }

    // Across 28 ticks (7 minutes), only 3 REST verification attempts were made instead of 28!
    assert_eq!(abs_calls.load(Ordering::SeqCst), 3);
    assert_eq!(spot_calls.load(Ordering::SeqCst), 3);

    let esc_snap = inspect_cliff_escalation_state(db.db_name(), wallet).unwrap();
    assert_eq!(esc_snap.consecutive_triggers, 3);
    assert!(esc_snap.last_verification_failed);
    assert_eq!(esc_snap.next_allowed_bypass_ms, t0 + 420_000);
    assert_eq!(CLIFF_ESCALATION_BASE_COOLDOWN_MS, 60_000);
    assert_eq!(CLIFF_ESCALATION_MAX_BACKOFF_MS, 300_000);

    // Only 2 WARN rows in MariaDB telemetry_logs across 28 ticks: initial + 5m summary!
    let warn_logs: Vec<_> = db
        .list_recent_telemetry(50)
        .unwrap()
        .into_iter()
        .filter(|r| r.level == "WARN" && r.component == "execution.reconcile.equity")
        .collect();
    assert_eq!(warn_logs.len(), 2);
    assert!(warn_logs
        .iter()
        .any(|r| r.message.contains("(suppressed_repeats = 19)")));

    // Step 28 (t = 420s): Spot recovers! Attempt #4 runs, switches to unifiedAccount, and sets 300s max backoff cap
    fail_spot.store(false, Ordering::SeqCst);
    let cfg_recover = db.load_config_or_default();
    let eq_recover = refresh_equity_snapshot_at_ms(&db, &http, &cfg_recover, t0 + 420_000).unwrap();
    assert!((eq_recover - 1890.79).abs() < 1e-6);
    assert_eq!(abs_calls.load(Ordering::SeqCst), 4);
    assert_eq!(spot_calls.load(Ordering::SeqCst), 4);
    let esc_after_4 = inspect_cliff_escalation_state(db.db_name(), wallet).unwrap();
    assert_eq!(esc_after_4.consecutive_triggers, 4);
    assert!(!esc_after_4.last_verification_failed);
    // 4th consecutive trigger hits the 300_000 ms (5m) backoff ceiling (420s + 300s = 720s)
    assert_eq!(
        esc_after_4.next_allowed_bypass_ms,
        t0 + 420_000 + CLIFF_ESCALATION_MAX_BACKOFF_MS
    );

    // Step 29 (t = 435s): Steady-state unifiedAccount tick clears cliff!
    // Verify that the 7 suppressed repeats before Step 28 recovery were persisted in telemetry_logs:
    let clear_logs_7: Vec<_> = db
        .list_recent_telemetry(50)
        .unwrap()
        .into_iter()
        .filter(|r| {
            r.level == "INFO"
                && r.component == "execution.reconcile.equity"
                && r.message.contains("(suppressed_repeats = 7)")
        })
        .collect();
    assert_eq!(clear_logs_7.len(), 1);

    let cfg_steady = db.load_config_or_default();
    assert_eq!(
        cfg_steady.get_str("live_exchange_account_mode", ""),
        "unifiedAccount"
    );
    let eval_clear = evaluate_equity_snapshot_unlocked(
        &http,
        db.db_name(),
        &cfg_steady,
        &ch_state,
        &open_coins,
        true,
        t0 + 435_000,
    );
    assert!(!eval_clear.equity_cliff_detected);
    let esc_cleared = inspect_cliff_escalation_state(db.db_name(), wallet).unwrap();
    assert_eq!(esc_cleared.consecutive_triggers, 0);
    assert!(!esc_cleared.last_verification_failed);
}

#[test]
fn test_w4_cliff_clear_emits_suppressed_repeats_count_when_condition_resolves() {
    let db = fresh_isolated_db();
    seed_open_sol_position(&db);

    let fail_spot = Arc::new(AtomicBool::new(true));
    let fail_spot_srv = fail_spot.clone();

    let server = MockHttpServer::start(move |req| {
        let typ = req
            .json_body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("");
        match typ {
            "userAbstraction" => MockHttpResponse::ok_json(&json!("default")),
            "spotClearinghouseState" => {
                if fail_spot_srv.load(Ordering::SeqCst) {
                    MockHttpResponse::ok_json(&json!({ "error": "unavailable" }))
                } else {
                    MockHttpResponse::ok_json(&json!({ "balances": [] }))
                }
            }
            _ => MockHttpResponse::ok_json(&json!([])),
        }
    });

    let t0 = 1_795_000_000_000_i64;
    let wallet = "0x6666666666666666666666666666666666666666";
    db.set_config_value("execution_mode", "LIVE", None).unwrap();
    db.set_config_value("hyperliquid_mainnet_url", &server.base_url(), None)
        .unwrap();
    db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
        .unwrap();
    db.set_config_value("wallet_address", wallet, None).unwrap();
    db.set_config_value("live_exchange_account_mode", "default", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "1000.00", None)
        .unwrap();
    db.set_config_value("live_exchange_perp_margin_equity_usdc", "1000.00", None)
        .unwrap();

    let http = HyperliquidHttpClient::new(2);
    let ch_drop = json!({
        "marginSummary": { "accountValue": "600.00", "totalMarginUsed": "100.00" },
        "withdrawable": "500.00",
        "assetPositions": [{ "position": { "coin": "SOL", "szi": "1.50", "entryPx": "162.00" } }]
    });
    let (open_coins, _) = extract_open_coins_and_szi(&ch_drop);

    // Tick 0 (EmitFirst) + Ticks 1, 2, 3 (Suppress 1, 2, 3)
    for step in 0..=3_i64 {
        let cfg = db.load_config_or_default();
        let eval = evaluate_equity_snapshot_unlocked(
            &http,
            db.db_name(),
            &cfg,
            &ch_drop,
            &open_coins,
            true,
            t0 + step * 15_000,
        );
        commit_equity_snapshot_locked(&db, &cfg, &eval, t0 + step * 15_000, 10_000.0);
    }

    // Tick 4 (t0 + 60_000): Perp equity recovers back to $1,000.00 -> cliff clears immediately with suppressed_repeats = 3!
    fail_spot.store(false, Ordering::SeqCst);
    let ch_recovered = json!({
        "marginSummary": { "accountValue": "1000.00", "totalMarginUsed": "100.00" },
        "withdrawable": "900.00",
        "assetPositions": [{ "position": { "coin": "SOL", "szi": "1.50", "entryPx": "162.00" } }]
    });
    let cfg_rec = db.load_config_or_default();
    let eval_rec = evaluate_equity_snapshot_unlocked(
        &http,
        db.db_name(),
        &cfg_rec,
        &ch_recovered,
        &open_coins,
        true,
        t0 + 60_000,
    );
    assert!(!eval_rec.equity_cliff_detected);
    assert_eq!(eval_rec.cleared_suppressed_repeats, Some(3));
    commit_equity_snapshot_locked(&db, &cfg_rec, &eval_rec, t0 + 60_000, 10_000.0);

    let info_clear_logs: Vec<_> = db
        .list_recent_telemetry(20)
        .unwrap()
        .into_iter()
        .filter(|r| {
            r.level == "INFO"
                && r.component == "execution.reconcile.equity"
                && r.message.contains("(suppressed_repeats = 3)")
        })
        .collect();
    assert_eq!(info_clear_logs.len(), 1);
}
