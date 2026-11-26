//! Dedicated Round-14 Integration Tests for Findings W1 (HIGH) and W3 (MEDIUM)
//! (`crates/aegis-execution/tests/round14_w1_w3.rs`).
//!
//! - **W1:** `DEDUP_EXCHANGE_RECONCILE` Order-Cancel Races, Keccak256 `cloid` Matching,
//!   `WORKING` `INCREASE` Scale-In Preservation & Merge, and Resting Entry Order Adoption/Re-Link.
//! - **W3:** Single Booking Rule (Zero `CLOSED` `HL_USER_FILL_RECONCILED` Phantom Rows While
//!   Position Epoch Remains `OPEN`), Active Open-Epoch Phantom Self-Healing, `SCALE_IN_MERGED`
//!   Zero-PnL Enforcement, and Full-Lifecycle Close Parity (`< 1e-4`).

use aegis_core::{now_ms, OrderSide};
use aegis_db::{
    AssetUpsertInput, DatabaseManager, ExchangeOpenPositionSyncInput, OidUpdate,
    ReconciledClosedTradeInput, WorkingOrderInput,
};
use aegis_execution::{derive_hl_cloid_hex, EntryUrgency, ExecutionEngine, NewWorkingOrderRequest};
use aegis_test_support::mock_http::MockHttpServer;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn fresh_isolated_db() -> DatabaseManager {
    let db = DatabaseManager::new_isolated_test(None).unwrap();
    db.init_schema().unwrap();
    db
}

fn configure_testnet_mock_db(db: &DatabaseManager, port: u16) {
    let base_url = format!("http://127.0.0.1:{port}");
    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("hyperliquid_testnet_url", &base_url, None)
        .unwrap();
    db.set_config_value(
        "hyperliquid_allowed_hosts",
        "127.0.0.1,api.hyperliquid.xyz,api.hyperliquid-testnet.xyz",
        None,
    )
    .unwrap();
    db.set_config_value(
        "wallet_address",
        "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
        None,
    )
    .unwrap();
    db.set_config_value(
        "private_key",
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        None,
    )
    .unwrap();
}

fn seed_asset(
    db: &DatabaseManager,
    coin: &str,
    asset_index: u32,
    sz_decimals: u32,
    tick_size: f64,
    lot_step_size: f64,
    px: f64,
) -> aegis_core::ExchangeAsset {
    let id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: coin.to_string(),
            asset_index,
            sz_decimals,
            tick_size,
            lot_step_size,
            max_leverage: 10.0,
            mark_price: px,
            mid_price: px,
            oracle_price: px,
            funding_rate_hourly: 0.00005,
            day_volume_usdc: 50_000_000.0,
            open_interest_usdc: 20_000_000.0,
            prev_day_price: px,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .unwrap();
    db.get_asset_by_id(id).unwrap().unwrap()
}

/// Reproduces the exact `#174773 DOGE` live bug chain end-to-end:
/// 1. `OPEN` LONG `5,000 DOGE` exists -> submit same-side `WORKING` `INCREASE` ALO (`+1,006 DOGE`).
/// 2. Run 15s reconciler while order rests on Hyperliquid (both via `OrderChaser::place_working_order`
///    AND via a `WORKING` row with `exchange_order_id = None` so Keccak256 `cloid` matching resolves &
///    upserts `hl-<oid>`) -> `WORKING` rows stay `WORKING`, `0` `DEDUP_EXCHANGE_RECONCILE`, `0` orphan
///    cancels, `get_assets_ordered()` includes `1,006 DOGE` and `reserved_margin_usdc`, and
///    `PriorityMonitor::run_1s_priority_tick()` logs `0` errors.
/// 3. Hyperliquid fills `+1,006 DOGE` (`szi = 6,006`) -> next `PriorityMonitor` tick AND reconciler path
///    merge scale-ins into the `OPEN` parent (`SCALE_IN_MERGED` with zeroed child PnL/fees/funding)
///    and zero `PriorityMonitor` errors!
#[test]
fn test_r14_w1_doge_174773_scale_in_reconcile_preserve_cloid_link_and_fill_merge() {
    let now = now_ms();
    let t_open = now - 300_000;

    #[derive(Clone)]
    struct MockDogeState {
        szi: f64,
        entry_px: f64,
        resting_orders: Vec<Value>,
        fills: Vec<Value>,
        cancelled_oids: Vec<u64>,
        next_oid: u64,
    }

    let state = Arc::new(Mutex::new(MockDogeState {
        szi: 5_000.0,
        entry_px: 0.19600,
        resting_orders: vec![
            json!({
                "coin": "DOGE",
                "oid": 90_010u64,
                "side": "A",
                "limitPx": "0.18350",
                "sz": "5000.0",
                "reduceOnly": true,
                "isTrigger": true,
                "orderType": "Stop Limit",
                "triggerPx": "0.18400",
                "timestamp": (now - 290_000) as u64
            }),
            json!({
                "coin": "DOGE",
                "oid": 90_011u64,
                "side": "A",
                "limitPx": "0.22050",
                "sz": "5000.0",
                "reduceOnly": true,
                "isTrigger": true,
                "orderType": "Take Profit Limit",
                "triggerPx": "0.22000",
                "timestamp": (now - 290_000) as u64
            }),
        ],
        fills: vec![json!({
            "coin": "DOGE",
            "side": "B",
            "px": "0.19600",
            "sz": "5000.0",
            "fee": "0.1470",
            "closedPnl": "0.0",
            "dir": "Open Long",
            "time": t_open,
            "oid": 90_001u64,
            "tid": 1u64,
            "startPosition": "0.0"
        })],
        cancelled_oids: Vec::new(),
        next_oid: 91_000,
    }));

    let state_srv = Arc::clone(&state);
    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            let st = state_srv.lock().clone();
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": {
                            "accountValue": "2500.0",
                            "totalNtlPos": format!("{:.4}", st.szi * 0.20000),
                            "totalMarginUsed": format!("{:.4}", st.szi * 0.10000)
                        },
                        "withdrawable": "1800.0",
                        "assetPositions": [{
                            "position": {
                                "coin": "DOGE",
                                "szi": format!("{:.1}", st.szi),
                                "entryPx": format!("{:.5}", st.entry_px),
                                "positionValue": format!("{:.4}", st.szi * 0.20000),
                                "unrealizedPnl": "0.0",
                                "cumFunding": { "sinceOpen": "0.0" }
                            }
                        }]
                    }),
                ),
                "frontendOpenOrders" | "openOrders" => (200, Value::Array(st.resting_orders)),
                "userFills" | "userFillsByTime" => (200, Value::Array(st.fills)),
                "userFunding" => (200, json!([])),
                "l2Book" => (
                    200,
                    json!({
                        "coin": "DOGE",
                        "levels": [
                            [{ "px": "0.19999", "sz": "50000.0", "n": 4 }],
                            [{ "px": "0.20001", "sz": "50000.0", "n": 4 }]
                        ]
                    }),
                ),
                _ => (200, json!([])),
            };
        }
        if path == "/exchange" {
            let action_type = body
                .pointer("/action/type")
                .and_then(Value::as_str)
                .unwrap_or("");
            let mut st = state_srv.lock();
            if action_type == "cancel" {
                if let Some(arr) = body.pointer("/action/cancels").and_then(Value::as_array) {
                    for c in arr {
                        if let Some(o) = c.get("o").and_then(Value::as_u64) {
                            st.cancelled_oids.push(o);
                            st.resting_orders
                                .retain(|r| r.get("oid").and_then(Value::as_u64) != Some(o));
                        }
                    }
                }
                return (
                    200,
                    json!({
                        "status": "ok",
                        "response": { "type": "cancel", "data": { "statuses": ["success"] } }
                    }),
                );
            }
            if action_type == "order" {
                let mut statuses = Vec::new();
                if let Some(arr) = body.pointer("/action/orders").and_then(Value::as_array) {
                    for ord in arr {
                        st.next_oid += 1;
                        let oid = st.next_oid;
                        let is_buy = ord.get("b").and_then(Value::as_bool).unwrap_or(true);
                        let px = ord.get("p").and_then(Value::as_str).unwrap_or("0.20000");
                        let sz = ord.get("s").and_then(Value::as_str).unwrap_or("0.0");
                        let ro = ord.get("r").and_then(Value::as_bool).unwrap_or(false);
                        let cloid = ord.get("c").and_then(Value::as_str);
                        let is_trig = ord.pointer("/t/trigger").is_some();
                        let mut resting_json = json!({
                            "coin": "DOGE",
                            "oid": oid,
                            "side": if is_buy { "B" } else { "A" },
                            "limitPx": px,
                            "sz": sz,
                            "reduceOnly": ro,
                            "isTrigger": is_trig,
                            "orderType": if is_trig { "Stop Limit" } else { "Limit" },
                            "timestamp": now_ms() as u64
                        });
                        if let Some(c) = cloid {
                            resting_json["cloid"] = Value::String(c.to_string());
                        }
                        if let Some(tpx) =
                            ord.pointer("/t/trigger/triggerPx").and_then(Value::as_str)
                        {
                            resting_json["triggerPx"] = Value::String(tpx.to_string());
                        }
                        st.resting_orders.push(resting_json);
                        statuses.push(json!({ "resting": { "oid": oid } }));
                    }
                }
                return (
                    200,
                    json!({
                        "status": "ok",
                        "response": { "type": "order", "data": { "statuses": statuses } }
                    }),
                );
            }
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    configure_testnet_mock_db(&db, server.port());
    let doge = seed_asset(&db, "DOGE", 12, 0, 0.00001, 1.0, 0.20000);

    // 1. Seed canonical OPEN LONG 5,000 DOGE parent (@ $0.19600 => +204 bps spacing to $0.20000) with SL/TP OIDs
    let open_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: doge.id,
            client_order_id: "v2-doge-long-parent".to_string(),
            side: OrderSide::Long,
            limit_price: 0.19600,
            requested_quantity: 5_000.0,
            reserved_margin_usdc: 490.0,
            portfolio_weight: 0.40,
            s1_weight_contrib: 0.40,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.18400,
            take_profit_px: Some(0.22000),
            exchange_order_id: Some("hl-90001".to_string()),
            reason: "OPEN_LONG".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        open_pid,
        0.19600,
        5_000.0,
        0.1470,
        0.18400,
        Some(0.22000),
        Some("hl-90001"),
        "FILLED",
    )
    .unwrap();
    db.update_position_tpsl_orders(
        open_pid,
        0.18400,
        OidUpdate::Set("hl-90010"),
        0.22000,
        OidUpdate::Set("hl-90011"),
    )
    .unwrap();

    // 2. Submit same-side WORKING INCREASE ALO (+1,006 DOGE) via OrderChaser (assigned oid = 91001)
    let scale1_pid = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: doge.clone(),
            side: OrderSide::Long,
            requested_quantity: 1_006.0,
            reserved_margin_usdc: 100.60,
            portfolio_weight: 0.08,
            s1_weight_contrib: 0.08,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.18400,
            reason: "INCREASE".to_string(),
            bypass_entry_interval: true,
            entry_urgency: EntryUrgency::MakerOnly,
            is_scale_in: true,
        })
        .unwrap();

    // Also create a second WORKING scale-in (`scale2_pid`, +500 DOGE) with `exchange_order_id = None`
    // while its Keccak256 `cloid` rests on Hyperliquid (`oid = 91055`), proving cloid resolution + OID upsert!
    let scale2_coid = "v2-doge-long-cloid-only";
    let scale2_cloid_hex = derive_hl_cloid_hex(scale2_coid);
    let scale2_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: doge.id,
            client_order_id: scale2_coid.to_string(),
            side: OrderSide::Long,
            limit_price: 0.19998,
            requested_quantity: 500.0,
            reserved_margin_usdc: 50.00,
            portfolio_weight: 0.04,
            s1_weight_contrib: 0.04,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.18400,
            take_profit_px: Some(0.22000),
            exchange_order_id: None,
            reason: "INCREASE".to_string(),
        })
        .unwrap();
    state.lock().resting_orders.push(json!({
        "coin": "DOGE",
        "oid": 91_055u64,
        "side": "B",
        "limitPx": "0.19998",
        "sz": "500.0",
        "reduceOnly": false,
        "isTrigger": false,
        "orderType": "Limit",
        "cloid": scale2_cloid_hex,
        "timestamp": now_ms() as u64
    }));

    // 3. Run 15s reconciler TWICE + 1s PriorityMonitor tick while orders rest on Hyperliquid
    for _ in 0..2 {
        let rep = engine.reconciler.sync_db_with_hyperliquid().unwrap();
        assert_eq!(
            rep.stale_orders_cancelled, 0,
            "Resting WORKING scale-in orders must NOT be cancelled on Hyperliquid"
        );
    }
    assert!(
        state.lock().cancelled_oids.is_empty(),
        "Zero cancel wires should be sent for resting scale-ins + tracked SL/TP"
    );

    let scale1_after_reconcile = db.get_position_by_id(scale1_pid).unwrap().unwrap();
    assert_eq!(scale1_after_reconcile.status, "WORKING");
    assert_eq!(
        scale1_after_reconcile.exchange_order_id.as_deref(),
        Some("hl-91001")
    );

    let scale2_after_reconcile = db.get_position_by_id(scale2_pid).unwrap().unwrap();
    assert_eq!(scale2_after_reconcile.status, "WORKING");
    assert_eq!(
        scale2_after_reconcile.exchange_order_id.as_deref(),
        Some("hl-91055"),
        "Reconciler must upsert exchange_order_id resolved via Keccak256 cloid"
    );

    // Exposure/heat check view (`v_assets_ordered`) MUST surface both resting scale-in orders!
    let ordered_live = db.get_assets_ordered().unwrap();
    assert_eq!(ordered_live.len(), 2);
    let total_ordered_qty: f64 = ordered_live.iter().map(|o| o.requested_quantity).sum();
    let total_reserved_usdc: f64 = ordered_live.iter().map(|o| o.reserved_margin_usdc).sum();
    assert!((total_ordered_qty - 1_506.0).abs() < 1e-6);
    assert!((total_reserved_usdc - 150.60).abs() < 1e-2);

    let audits = db.list_recent_order_audits(30).unwrap();
    assert!(
        !audits
            .iter()
            .any(|a| a.reason.contains("DEDUP_EXCHANGE_RECONCILE")),
        "Zero DEDUP_EXCHANGE_RECONCILE cancellations may occur for same-side WORKING scale-ins"
    );

    // First simulated fill: scale2 (+500 DOGE, oid=91055) fills and is merged by the 15s Reconciler!
    std::thread::sleep(Duration::from_millis(3));
    {
        let mut st = state.lock();
        let total_notional = 5_000.0 * 0.19600 + 500.0 * 0.19998;
        st.szi = 5_500.0;
        st.entry_px = total_notional / 5_500.0;
        st.resting_orders
            .retain(|r| r.get("oid").and_then(Value::as_u64) != Some(91_055));
        st.fills.push(json!({
            "coin": "DOGE",
            "side": "B",
            "px": "0.19998",
            "sz": "500.0",
            "fee": "0.0150",
            "closedPnl": "0.0",
            "dir": "Open Long",
            "time": now_ms(),
            "oid": 91_055u64,
            "tid": 2u64,
            "startPosition": "5000.0"
        }));
    }
    std::thread::sleep(Duration::from_millis(3));
    engine.reconciler.run_cutover_reconciliation(false).unwrap();

    let child2 = db.get_position_by_id(scale2_pid).unwrap().unwrap();
    assert_eq!(child2.status, "CLOSED");
    assert_eq!(
        child2.exit_reason.as_deref(),
        Some("SCALE_IN_MERGED"),
        "Reconciler must merge filled WORKING scale-in into OPEN parent instead of marking CANCELLED"
    );
    assert!((child2.realized_pnl_usdc - 0.0).abs() < 1e-9);
    assert!((child2.fees_paid_usdc - 0.0).abs() < 1e-9);
    assert!((child2.funding_accrued_usdc - 0.0).abs() < 1e-9);

    // `scale1_pid` (+1,006 DOGE) is still resting on HL and still WORKING in DB (`v_assets_ordered`)!
    let ordered_after_merge2 = db.get_assets_ordered().unwrap();
    assert_eq!(ordered_after_merge2.len(), 1);
    assert_eq!(ordered_after_merge2[0].position_id, scale1_pid);

    let prio1 = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(prio1.working_order_errors, 0);
    assert_eq!(prio1.open_holding_errors, 0);

    // 4. Simulate Hyperliquid filling `#174773`-equivalent `scale1_pid` (+1,006 DOGE, oid=91001)
    {
        let mut st = state.lock();
        let total_notional = st.szi * st.entry_px + 1_006.0 * 0.19999;
        st.szi = 6_506.0;
        st.entry_px = total_notional / 6_506.0;
        st.resting_orders
            .retain(|r| r.get("oid").and_then(Value::as_u64) != Some(91_001));
        st.fills.push(json!({
            "coin": "DOGE",
            "side": "B",
            "px": "0.19999",
            "sz": "1006.0",
            "fee": "0.0302",
            "closedPnl": "0.0",
            "dir": "Open Long",
            "time": now_ms(),
            "oid": 91_001u64,
            "tid": 3u64,
            "startPosition": "5500.0"
        }));
    }

    // Next 1s PriorityMonitor tick merges `scale1_pid` into OPEN parent with zero errors!
    let prio2 = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(prio2.working_order_errors, 0);
    assert_eq!(prio2.open_holding_errors, 0);
    assert!(db.get_assets_ordered().unwrap().is_empty());

    let child1 = db.get_position_by_id(scale1_pid).unwrap().unwrap();
    assert_eq!(child1.status, "CLOSED");
    assert_eq!(child1.exit_reason.as_deref(), Some("SCALE_IN_MERGED"));
    assert!((child1.realized_pnl_usdc - 0.0).abs() < 1e-9);
    assert!((child1.fees_paid_usdc - 0.0).abs() < 1e-9);
    assert!((child1.funding_accrued_usdc - 0.0).abs() < 1e-9);

    let parent_final = db.get_position_by_id(open_pid).unwrap().unwrap();
    assert_eq!(parent_final.status, "OPEN");
    assert!((parent_final.filled_quantity - 6_506.0).abs() < 1e-6);
}

/// Verifies W1 resting order re-linking, unknown `cloid` entry adoption, and non-cancellation
/// during `openOrders` HTTP outage or inside `ENTRY_PARTIAL_GRACE_MS`.
#[test]
fn test_r14_w1_resting_cloid_relink_adoption_and_open_orders_outage_grace() {
    let fail_open_orders = Arc::new(AtomicBool::new(false));
    let resting_orders = Arc::new(Mutex::new(Vec::<Value>::new()));
    let cancelled_oids = Arc::new(Mutex::new(Vec::<u64>::new()));

    let fail_srv = Arc::clone(&fail_open_orders);
    let orders_srv = Arc::clone(&resting_orders);
    let cancels_srv = Arc::clone(&cancelled_oids);
    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "2000.0" },
                        "withdrawable": "1900.0",
                        "assetPositions": []
                    }),
                ),
                "frontendOpenOrders" | "openOrders" => {
                    if fail_srv.load(Ordering::SeqCst) {
                        (500, json!({ "error": "upstream timeout" }))
                    } else {
                        (200, Value::Array(orders_srv.lock().clone()))
                    }
                }
                "userFills" | "userFillsByTime" | "userFunding" => (200, json!([])),
                _ => (200, json!([])),
            };
        }
        if path == "/exchange" {
            if let Some(arr) = body.pointer("/action/cancels").and_then(Value::as_array) {
                for c in arr {
                    if let Some(o) = c.get("o").and_then(Value::as_u64) {
                        cancels_srv.lock().push(o);
                    }
                }
            }
            return (200, json!({ "status": "ok" }));
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    configure_testnet_mock_db(&db, server.port());
    let doge = seed_asset(&db, "DOGE", 12, 0, 0.00001, 1.0, 0.20000);

    // 1. Seed a prematurely CANCELLED DB row (`v2-doge-relink-1`) whose chased order (`-c1`, oid=92001)
    // AND an unknown live entry order carrying a cloid (oid=92002) both rest on Hyperliquid.
    let relink_coid = "v2-doge-relink-1";
    let relink_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: doge.id,
            client_order_id: relink_coid.to_string(),
            side: OrderSide::Long,
            limit_price: 0.19950,
            requested_quantity: 800.0,
            reserved_margin_usdc: 80.0,
            portfolio_weight: 0.08,
            s1_weight_contrib: 0.08,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.19000,
            take_profit_px: Some(0.22000),
            exchange_order_id: None,
            reason: "INCREASE".to_string(),
        })
        .unwrap();
    db.record_working_order_chase(relink_pid, 0.19960, "CHASING_ALO", None, "chase 1")
        .unwrap();
    db.cancel_working_order(relink_pid, "LEGACY_PREMATURE_DB_CANCEL")
        .unwrap();

    let relink_cloid_hex = derive_hl_cloid_hex(&format!("{relink_coid}-c1"));
    let unknown_cloid_hex = derive_hl_cloid_hex("v2-doge-untracked-entry");
    *resting_orders.lock() = vec![
        json!({
            "coin": "DOGE",
            "oid": 92_001u64,
            "side": "B",
            "limitPx": "0.19960",
            "sz": "800.0",
            "reduceOnly": false,
            "isTrigger": false,
            "orderType": "Limit",
            "cloid": relink_cloid_hex,
            "timestamp": now_ms() as u64
        }),
        json!({
            "coin": "DOGE",
            "oid": 92_002u64,
            "side": "B",
            "limitPx": "0.19800",
            "sz": "500.0",
            "reduceOnly": false,
            "isTrigger": false,
            "orderType": "Limit",
            "cloid": unknown_cloid_hex,
            "timestamp": now_ms() as u64
        }),
    ];

    engine.reconciler.run_cutover_reconciliation(false).unwrap();
    assert!(
        cancelled_oids.lock().is_empty(),
        "Resting cloid entry orders must be protected and adopted/re-linked, never orphan-cancelled"
    );

    let ordered_after_adopt = db.get_assets_ordered().unwrap();
    assert_eq!(ordered_after_adopt.len(), 2);
    let relinked = ordered_after_adopt
        .iter()
        .find(|o| o.position_id == relink_pid)
        .expect("Prematurely CANCELLED row must be re-linked to WORKING");
    assert_eq!(relinked.exchange_order_id.as_deref(), Some("hl-92001"));
    assert!((relinked.requested_quantity - 800.0).abs() < 1e-6);
    assert!(relinked.reserved_margin_usdc > 0.0);

    let adopted = ordered_after_adopt
        .iter()
        .find(|o| o.exchange_order_id.as_deref() == Some("hl-92002"))
        .expect("Unknown resting cloid entry order must be adopted into WORKING");
    assert!((adopted.requested_quantity - 500.0).abs() < 1e-6);
    assert!(adopted.reserved_margin_usdc > 0.0);

    // 2. Simulate `openOrders` HTTP 500 -> MUST NOT locally cancel!
    fail_open_orders.store(true, Ordering::SeqCst);
    engine.reconciler.sync_db_with_hyperliquid().unwrap();
    assert_eq!(
        db.get_assets_ordered().unwrap().len(),
        2,
        "WORKING orders must NEVER be locally cancelled when openOrders fetch failed"
    );

    // 3. Restore `openOrders` returning empty `[]`: fresh WORKING orders (<30s grace) stay WORKING!
    fail_open_orders.store(false, Ordering::SeqCst);
    resting_orders.lock().clear();
    engine.reconciler.sync_db_with_hyperliquid().unwrap();
    assert_eq!(
        db.get_assets_ordered().unwrap().len(),
        2,
        "WORKING orders inside ENTRY_PARTIAL_GRACE_MS must stay WORKING"
    );
}

/// Pins W3 Single Booking Rule & Full-Lifecycle Close Parity on `3.48 SOL`:
/// - Open `3.48 SOL @ $100.00` -> partial `REDUCE` `-1.04`, `-0.72`, `-0.23` while `1.49 SOL` stays `OPEN`.
/// - Run full reconciler multiple times -> `0` `CLOSED` `HL_USER_FILL_RECONCILED` rows exist while `SOL`
///   is `OPEN` (closed ledger PnL == `$0.00`), pre-existing phantom `hl-closed-sol-*` row is self-healed,
///   and `OPEN` row holds exact partial realized PnL, fees, and funding idempotently.
/// - Close final `1.49 SOL` -> run reconciler multiple times -> single `CLOSED` row equals the one-shot
///   full-lifecycle net PnL (`closedPnl - all_fees + all_funding`) to the penny (`< 1e-4`)!
#[test]
fn test_r14_w3_sol_multi_partial_reduce_single_booking_and_full_close_parity() {
    let now = now_ms();
    let t_open = now - 7_200_000;
    let t_f1 = now - 5_400_000;
    let t_c1 = now - 4_800_000;
    let t_c2 = now - 3_600_000;
    let t_f2 = now - 2_400_000;
    let t_c3 = now - 1_800_000;
    let t_c4 = now - 60_000;

    let is_fully_closed = Arc::new(AtomicBool::new(false));
    let closed_srv = Arc::clone(&is_fully_closed);
    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            let closed_now = closed_srv.load(Ordering::SeqCst);
            return match typ {
                "clearinghouseState" => {
                    let asset_positions = if closed_now {
                        json!([])
                    } else {
                        json!([{
                            "position": {
                                "coin": "SOL",
                                "szi": "1.49",
                                "entryPx": "100.00",
                                "positionValue": "153.47",
                                "unrealizedPnl": "4.47",
                                "cumFunding": { "sinceOpen": "0.1220" }
                            }
                        }])
                    };
                    (
                        200,
                        json!({
                            "marginSummary": {
                                "accountValue": "2500.0",
                                "totalNtlPos": if closed_now { "0.0" } else { "153.47" },
                                "totalMarginUsed": if closed_now { "0.0" } else { "76.74" }
                            },
                            "withdrawable": "2400.0",
                            "assetPositions": asset_positions
                        }),
                    )
                }
                "frontendOpenOrders" | "openOrders" => (200, json!([])),
                "userFills" | "userFillsByTime" => {
                    let mut fills = vec![
                        json!({
                            "coin": "SOL", "side": "B", "px": "100.00", "sz": "3.48",
                            "fee": "0.1218", "closedPnl": "0.0", "dir": "Open Long",
                            "time": t_open, "oid": 8801u64, "tid": 1u64, "startPosition": "0.0"
                        }),
                        json!({
                            "coin": "SOL", "side": "A", "px": "102.50", "sz": "1.04",
                            "fee": "0.0373", "closedPnl": "2.60", "dir": "Close Long",
                            "time": t_c1, "oid": 8802u64, "tid": 2u64, "startPosition": "3.48"
                        }),
                        json!({
                            "coin": "SOL", "side": "A", "px": "103.00", "sz": "0.72",
                            "fee": "0.0260", "closedPnl": "2.16", "dir": "Close Long",
                            "time": t_c2, "oid": 8803u64, "tid": 3u64, "startPosition": "2.44"
                        }),
                        json!({
                            "coin": "SOL", "side": "A", "px": "101.80", "sz": "0.23",
                            "fee": "0.0082", "closedPnl": "0.414", "dir": "Close Long",
                            "time": t_c3, "oid": 8804u64, "tid": 4u64, "startPosition": "1.72"
                        }),
                    ];
                    if closed_now {
                        fills.push(json!({
                            "coin": "SOL", "side": "A", "px": "104.20", "sz": "1.49",
                            "fee": "0.0543", "closedPnl": "6.258", "dir": "Close Long",
                            "time": t_c4, "oid": 8805u64, "tid": 5u64, "startPosition": "1.49"
                        }));
                    }
                    (200, Value::Array(fills))
                }
                "userFunding" => (
                    200,
                    json!([
                        {
                            "time": t_f1,
                            "hash": "0xsolf1",
                            "delta": {
                                "type": "funding",
                                "coin": "SOL",
                                "usdc": "-0.1840",
                                "szi": "3.48",
                                "fundingRate": "0.0005"
                            }
                        },
                        {
                            "time": t_f2,
                            "hash": "0xsolf2",
                            "delta": {
                                "type": "funding",
                                "coin": "SOL",
                                "usdc": "0.0620",
                                "szi": "1.72",
                                "fundingRate": "-0.0003"
                            }
                        }
                    ]),
                ),
                _ => (200, json!([])),
            };
        }
        if path == "/exchange" {
            let mut statuses = Vec::new();
            if let Some(arr) = body.pointer("/action/orders").and_then(Value::as_array) {
                for (i, _) in arr.iter().enumerate() {
                    statuses.push(json!({ "resting": { "oid": 99_100u64 + i as u64 } }));
                }
            }
            return (
                200,
                json!({
                    "status": "ok",
                    "response": { "type": "order", "data": { "statuses": statuses } }
                }),
            );
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    configure_testnet_mock_db(&db, server.port());
    let sol = seed_asset(&db, "SOL", 5, 2, 0.01, 0.01, 103.00);

    // Seed a pre-existing phantom `#174737`-equivalent `hl-closed-sol-8802` row BEFORE opening `SOL`
    // so reconciler must self-heal/delete it once `SOL` is OPEN!
    db.reconcile_closed_positions_from_exchange_fills(&[ReconciledClosedTradeInput {
        asset_id: sol.id,
        client_order_id: "hl-closed-sol-8802".to_string(),
        exchange_order_id: "hl-8802".to_string(),
        close_epoch_id: Some("SOL:legacy".to_string()),
        side: OrderSide::Long,
        entry_price: 100.0,
        exit_price: 102.5,
        filled_quantity: 1.04,
        realized_pnl_usdc: 0.8332,
        fees_paid_usdc: 0.0373,
        funding_accrued_usdc: 0.0,
        created_at_ms: t_open,
        closed_at_ms: t_c1,
        exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
    }])
    .unwrap();
    assert_eq!(db.list_closed_positions(10).unwrap().len(), 1);

    // Now seed OPEN 3.48 SOL parent (`opened_at_ms = t_open`) + record the 3 local partial REDUCEs
    let open_pid = db
        .create_working_order(&WorkingOrderInput {
            asset_id: sol.id,
            client_order_id: "v2-sol-long-174729".to_string(),
            side: OrderSide::Long,
            limit_price: 100.00,
            requested_quantity: 3.48,
            reserved_margin_usdc: 174.00,
            portfolio_weight: 0.14,
            s1_weight_contrib: 0.10,
            s2_weight_contrib: 0.04,
            s3_weight_contrib: 0.0,
            protective_stop_px: 94.00,
            take_profit_px: Some(115.00),
            exchange_order_id: Some("hl-8801".to_string()),
            reason: "OPEN_LONG".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        open_pid,
        100.00,
        3.48,
        0.1218,
        94.00,
        Some(115.00),
        Some("hl-8801"),
        "FILLED",
    )
    .unwrap();
    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: sol.id,
        coin: "SOL",
        side: OrderSide::Long,
        entry_price: 100.00,
        mark_price: 100.00,
        quantity: 3.48,
        committed_notional_usdc: 348.00,
        unrealized_pnl_usdc: 0.0,
        portfolio_weight: 0.14,
        protective_stop_px: 94.00,
        take_profit_px: 115.00,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(t_open),
    })
    .unwrap();
    db.record_partial_position_exit(open_pid, 1.04, 2.44, 102.50, 0.0373, "ACTION_REDUCE")
        .unwrap();
    db.record_partial_position_exit(open_pid, 0.72, 1.72, 103.00, 0.0260, "ACTION_REDUCE")
        .unwrap();
    db.record_partial_position_exit(open_pid, 0.23, 1.49, 101.80, 0.0082, "ACTION_REDUCE")
        .unwrap();

    // 1. Run full reconciler 3 times while 1.49 SOL remains OPEN
    engine.aux_sync.sync_startup_before_reconcile();
    for _ in 0..3 {
        engine.reconciler.run_cutover_reconciliation(false).unwrap();
    }

    let closed_while_open = db.list_closed_positions(20).unwrap();
    assert!(
        closed_while_open.is_empty(),
        "0 CLOSED HL_USER_FILL_RECONCILED rows may exist while SOL remains OPEN, got: {closed_while_open:?}"
    );

    let expected_partial_rpnl = (2.60 - 0.0373) + (2.16 - 0.0260) + (0.414 - 0.0082); // +5.1025
    let expected_open_fees = 0.1218 + 0.0373 + 0.0260 + 0.0082; // 0.1933
    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].position_id, open_pid);
    assert!((held[0].quantity - 1.49).abs() < 1e-6);
    assert!((held[0].realized_pnl_usdc - expected_partial_rpnl).abs() < 1e-4);
    assert!((held[0].fees_paid_usdc - expected_open_fees).abs() < 1e-4);
    assert!((held[0].funding_accrued_usdc - (-0.1220)).abs() < 1e-4);
    assert!((db.sum_realized_pnl_usdc().unwrap() - expected_partial_rpnl).abs() < 1e-4);

    let pnl_while_open = db.compute_pnl_summary(4.47, -0.1220, 2500.0).unwrap();
    assert!(
        (pnl_while_open.daily.realized_pnl_usdc - 0.0).abs() < 1e-6,
        "Closed ledger PnL must be $0.00 while SOL remains OPEN"
    );

    // 2. Close final 1.49 SOL @ $104.20 on Hyperliquid and run reconciler 3 times
    is_fully_closed.store(true, Ordering::SeqCst);
    for _ in 0..3 {
        engine.reconciler.run_cutover_reconciliation(false).unwrap();
    }

    assert!(db.get_assets_held().unwrap().is_empty());
    let closed_after_flat = db.list_closed_positions(20).unwrap();
    assert_eq!(
        closed_after_flat.len(),
        1,
        "Exactly ONE canonical CLOSED row must exist for the completed 3.48 SOL lifecycle"
    );

    let one_shot_full_lifecycle_net_pnl = (2.60 + 2.16 + 0.414 + 6.258)
        - (0.1218 + 0.0373 + 0.0260 + 0.0082 + 0.0543)
        + (-0.1840 + 0.0620);
    let final_row = &closed_after_flat[0];
    assert_eq!(final_row.id, open_pid);
    assert!(
        (final_row.realized_pnl_usdc - one_shot_full_lifecycle_net_pnl).abs() < 1e-4,
        "Final CLOSED realized_pnl_usdc={} must equal one-shot full-lifecycle net PnL={} (< 1e-4)",
        final_row.realized_pnl_usdc,
        one_shot_full_lifecycle_net_pnl
    );
    assert!((final_row.fees_paid_usdc - 0.2476).abs() < 1e-4);
    assert!((final_row.funding_accrued_usdc - (-0.1220)).abs() < 1e-4);
    assert!((final_row.filled_quantity - 3.48).abs() < 1e-6);
    assert!((db.sum_realized_pnl_usdc().unwrap() - one_shot_full_lifecycle_net_pnl).abs() < 1e-4);
}

/// Verifies multi-epoch isolation on the same coin (`NEAR`):
/// - Epoch 1 (`10 NEAR` opened and closed to `0.0`) emits its `CLOSED` row cleanly.
/// - Epoch 2 (`20 NEAR` opened, `-5 NEAR` trimmed, `15 NEAR` still `OPEN`) emits `0` `CLOSED` rows
///   and syncs its partial trim realized PnL & fees onto the `OPEN` `NEAR` row.
#[test]
fn test_r14_w3_prior_closed_epoch_preserved_while_reopened_epoch_trims_stay_on_open() {
    let now = now_ms();
    let t_e1_open = now - 6_000_000;
    let t_e1_close = now - 4_500_000;
    let t_e2_open = now - 2_400_000;
    let t_e2_trim = now - 1_200_000;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "2000.0" },
                        "withdrawable": "1800.0",
                        "assetPositions": [{
                            "position": {
                                "coin": "NEAR",
                                "szi": "15.0",
                                "entryPx": "5.00",
                                "positionValue": "78.00",
                                "unrealizedPnl": "3.00",
                                "cumFunding": { "sinceOpen": "0.0" }
                            }
                        }]
                    }),
                ),
                "frontendOpenOrders" | "openOrders" => (200, json!([])),
                "userFills" | "userFillsByTime" => (
                    200,
                    json!([
                        {
                            "coin": "NEAR", "side": "B", "px": "4.80", "sz": "10.0",
                            "fee": "0.0200", "closedPnl": "0.0", "dir": "Open Long",
                            "time": t_e1_open, "oid": 5001u64, "tid": 1u64, "startPosition": "0.0"
                        },
                        {
                            "coin": "NEAR", "side": "A", "px": "5.10", "sz": "10.0",
                            "fee": "0.0200", "closedPnl": "3.00", "dir": "Close Long",
                            "time": t_e1_close, "oid": 5002u64, "tid": 2u64, "startPosition": "10.0"
                        },
                        {
                            "coin": "NEAR", "side": "B", "px": "5.00", "sz": "20.0",
                            "fee": "0.0400", "closedPnl": "0.0", "dir": "Open Long",
                            "time": t_e2_open, "oid": 5003u64, "tid": 3u64, "startPosition": "0.0"
                        },
                        {
                            "coin": "NEAR", "side": "A", "px": "5.20", "sz": "5.0",
                            "fee": "0.0100", "closedPnl": "1.00", "dir": "Close Long",
                            "time": t_e2_trim, "oid": 5004u64, "tid": 4u64, "startPosition": "20.0"
                        }
                    ]),
                ),
                "userFunding" => (200, json!([])),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    configure_testnet_mock_db(&db, server.port());
    let _near = seed_asset(&db, "NEAR", 18, 1, 0.001, 0.1, 5.20);

    engine.aux_sync.sync_startup_before_reconcile();
    for _ in 0..2 {
        engine.reconciler.run_cutover_reconciliation(false).unwrap();
    }

    // Epoch 1 is CLOSED (+3.00 - 0.04 = +2.96), while Epoch 2 is OPEN with partial trim (+1.00 - 0.01 = +0.99)
    let closed = db.list_closed_positions(10).unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].client_order_id, "hl-closed-near-5002");
    assert!((closed[0].realized_pnl_usdc - 2.96).abs() < 1e-4);

    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].symbol, "NEAR-USDC");
    assert!((held[0].quantity - 15.0).abs() < 1e-6);
    assert_eq!(held[0].opened_at_ms, Some(t_e2_open));
    assert!((held[0].realized_pnl_usdc - 0.99).abs() < 1e-4);
    assert!((held[0].fees_paid_usdc - 0.05).abs() < 1e-4);
}
