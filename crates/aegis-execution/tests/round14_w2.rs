//! Round-14 Finding W2 Integration Suite (`crates/aegis-execution/tests/round14_w2.rs`).
//!
//! Verifies:
//! 1. Passive-first `ACTION_REDUCE` & calm `ACTION_CLOSE` maker (`Alo`, `1.5 bps`) ladder,
//!    BBO repricing during `passive_close_rest_sec`, SL/TP trigger size re-sync after partial
//!    reductions, timeout IOC fallback (`4.5 bps`), and immediate IOC reservation for urgent
//!    protective stops, auto-deleverage, and early-loser invalidations.
//! 2. `DOGE #174622` add-then-trim oscillation reproduction: 15m (`scale_in_reduce_cooldown_sec`)
//!    flip cooldown, breakeven-plus hysteresis (`round_trip_cost + reduce_min_net_edge_bps`)
//!    against both average `entry_price` and latest `SCALE_IN` fill price, minimum scale-in
//!    price spacing (`max(2x RT, scale_in_min_atr_mult * atr_pct, scale_in_min_spacing_bps)`),
//!    and duplicate `WORKING` order prevention.
//! 3. Live resting `WORKING` orders (`v_assets_ordered`, both fresh entries and `INCREASE`
//!    scale-ins) counted in gross exposure, single-asset weight, and `max_portfolio_heat_pct`.

use aegis_core::{now_ms, ExchangeAsset, OrderSide, RiskTier};
use aegis_db::{
    AssetUpsertInput, BarUpsertInput, DatabaseManager, ExchangeOpenPositionSyncInput, OidUpdate,
};
use aegis_execution::{
    check_scale_in_spacing_for_open_position, compute_active_exposure_and_heat_with_db,
    compute_min_scale_in_spacing_pct, evaluate_reduce_hysteresis_gate, ChaseStepOutcome,
    EntryUrgency, ExecutionEngine, NewWorkingOrderRequest,
};
use aegis_signals::{
    allocate_multi_sleeve_weights_with_context, AllocatorConfig, CarryMomentumMetrics,
    HeldPositionContext, MarketBreadthSnapshot, RegimeGateState, SignalCycleOutput,
    SleeveSignalInput, ACTION_CLOSE, ACTION_HOLD, ACTION_INCREASE, ACTION_OPEN_LONG, ACTION_REDUCE,
};
use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const TEST_WALLET: &str = "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23";
const TEST_PK: &str = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";

fn fresh_w2_db() -> DatabaseManager {
    let db = DatabaseManager::new_isolated_test(None).expect("isolated test db");
    db.init_schema().expect("init schema");
    db.set_config_value("risk_capital_cap_usdc", "0.0", None)
        .unwrap();
    db
}

fn configure_live_loopback(db: &DatabaseManager, base_url: &str) {
    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("hyperliquid_testnet_url", base_url, None)
        .unwrap();
    db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
        .unwrap();
    db.set_config_value("wallet_address", TEST_WALLET, None)
        .unwrap();
    db.set_config_value("private_key", TEST_PK, None).unwrap();
    db.set_config_value("initial_paper_equity_usdc", "10000.0", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "10000.0", None)
        .unwrap();
    db.set_config_value(
        "live_exchange_equity_synced_at_ms",
        &now_ms().to_string(),
        None,
    )
    .unwrap();
    db.set_config_value("maker_fee_bps", "1.5", None).unwrap();
    db.set_config_value("taker_fee_bps", "4.5", None).unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
fn upsert_perp(
    db: &DatabaseManager,
    coin: &str,
    asset_index: u32,
    sz_decimals: u32,
    tick_size: f64,
    lot_step_size: f64,
    px: f64,
) -> ExchangeAsset {
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
            funding_rate_hourly: 0.00001,
            day_volume_usdc: 25_000_000.0,
            open_interest_usdc: 10_000_000.0,
            prev_day_price: px,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
        })
        .unwrap();
    db.get_asset_by_id(id).unwrap().unwrap()
}

fn make_sig(
    asset: &ExchangeAsset,
    target_w: f64,
    current_w: f64,
    action: &str,
) -> SleeveSignalInput {
    SleeveSignalInput {
        asset_id: asset.id,
        symbol: asset.symbol.clone(),
        coin: asset.coin.clone(),
        mark_price: asset.mark_price,
        day_volume_usdc: asset.day_volume_usdc,
        funding_rate_hourly: asset.funding_rate_hourly,
        annualized_vol: 0.45,
        market_beta: 1.0,
        s1_mom_score: 1.2,
        s1_carry_score: 0.4,
        s2_trend_alignment: 1,
        s2_vol_surge_ratio: 1.5,
        s3_funding_zscore: 0.0,
        s3_flush_atr_mult: 0.0,
        w_s1_carry_mom: target_w,
        w_s2_trend_breakout: 0.0,
        w_s3_event_reversion: 0.0,
        raw_combined_weight: target_w,
        target_weight: target_w,
        current_weight: current_w,
        delta_weight: target_w - current_w,
        event_trigger: None,
        rebalance_action: action.to_string(),
    }
}

fn make_sig_out(signals: Vec<SleeveSignalInput>) -> SignalCycleOutput {
    SignalCycleOutput {
        risk_tier: RiskTier::Aggressive,
        leverage_multiplier: 2.0,
        breadth: MarketBreadthSnapshot {
            eligible_coin_count: signals.len().max(1) as i32,
            market_breadth_bull: 0.65,
            market_beta_exposure: 0.25,
            market_regime_label: "BULL_EXPANSION".to_string(),
        },
        s1_gross_weight: 0.25,
        s2_gross_weight: 0.0,
        s3_gross_weight: 0.0,
        total_gross_weight: 0.25,
        total_net_weight: 0.25,
        signals,
    }
}

#[derive(Debug, Clone)]
struct RecordedWireOrder {
    asset_index: i64,
    is_buy: bool,
    px: String,
    sz: String,
    reduce_only: bool,
    tif: Option<String>,
    is_trigger: bool,
}

fn extract_wire_orders(exchange_json: &Value) -> Vec<RecordedWireOrder> {
    let mut out = Vec::new();
    let Some(action) = exchange_json.get("action") else {
        return out;
    };
    if action.get("type").and_then(Value::as_str) != Some("order") {
        return out;
    }
    let Some(orders) = action.get("orders").and_then(Value::as_array) else {
        return out;
    };
    for o in orders {
        let asset_index = o.get("a").and_then(Value::as_i64).unwrap_or(-1);
        let is_buy = o.get("b").and_then(Value::as_bool).unwrap_or(false);
        let px = o.get("p").and_then(Value::as_str).unwrap_or("").to_string();
        let sz = o.get("s").and_then(Value::as_str).unwrap_or("").to_string();
        let reduce_only = o.get("r").and_then(Value::as_bool).unwrap_or(false);
        let tif = o
            .get("t")
            .and_then(|t| t.get("limit"))
            .and_then(|l| l.get("tif"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let is_trigger = o.get("t").and_then(|t| t.get("trigger")).is_some();
        out.push(RecordedWireOrder {
            asset_index,
            is_buy,
            px,
            sz,
            reduce_only,
            tif,
            is_trigger,
        });
    }
    out
}

#[test]
fn test_w2_passive_first_action_reduce_maker_fill_bbo_reprice_tpsl_sync_and_timeout_ioc_fallback() {
    #[derive(Clone)]
    struct MockExchangeState {
        sol_szi: String,
        sol_bid: String,
        sol_ask: String,
        sol_fills: Vec<Value>,
        eth_szi: String,
        avax_szi: String,
        next_oid: u64,
        submitted: Vec<RecordedWireOrder>,
        cancelled_oids: Vec<u64>,
    }

    let state = Arc::new(Mutex::new(MockExchangeState {
        sol_szi: "10.0".to_string(),
        sol_bid: "101.95".to_string(),
        sol_ask: "102.05".to_string(),
        sol_fills: Vec::new(),
        eth_szi: "1.0".to_string(),
        avax_szi: "20.0".to_string(),
        next_oid: 90_000,
        submitted: Vec::new(),
        cancelled_oids: Vec::new(),
    }));
    let srv_state = Arc::clone(&state);

    let srv = MockHttpServer::start(move |req| {
        let mut st = srv_state.lock();
        if req.path == "/info" {
            match req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("")
            {
                "allMids" => MockHttpResponse::ok_json(&json!({
                    "SOL": "102.0",
                    "ETH": "2030.0",
                    "AVAX": "37.5"
                })),
                "clearinghouseState" => {
                    let mut positions = Vec::new();
                    if st.sol_szi != "0.0" {
                        positions.push(json!({
                            "position": { "coin": "SOL", "szi": st.sol_szi, "entryPx": "100.0", "positionValue": "1020.0" }
                        }));
                    }
                    if st.eth_szi != "0.0" {
                        positions.push(json!({
                            "position": { "coin": "ETH", "szi": st.eth_szi, "entryPx": "2000.0", "positionValue": "4060.0" }
                        }));
                    }
                    if st.avax_szi != "0.0" {
                        positions.push(json!({
                            "position": { "coin": "AVAX", "szi": st.avax_szi, "entryPx": "40.0", "positionValue": "750.0" }
                        }));
                    }
                    MockHttpResponse::ok_json(&json!({
                        "marginSummary": { "accountValue": "10000.0" },
                        "withdrawable": "8000.0",
                        "assetPositions": positions
                    }))
                }
                "l2Book" => {
                    let coin = req
                        .json_body
                        .get("coin")
                        .and_then(Value::as_str)
                        .unwrap_or("SOL");
                    let (bid, ask) = match coin {
                        "ETH" => ("2029.5".to_string(), "2030.5".to_string()),
                        "AVAX" => ("37.45".to_string(), "37.55".to_string()),
                        _ => (st.sol_bid.clone(), st.sol_ask.clone()),
                    };
                    MockHttpResponse::ok_json(&json!({
                        "levels": [
                            [{ "px": bid, "sz": "250.0" }],
                            [{ "px": ask, "sz": "250.0" }]
                        ]
                    }))
                }
                "frontendOpenOrders" | "openOrders" => MockHttpResponse::ok_json(&json!([])),
                "userFillsByTime" | "userFills" => MockHttpResponse::ok_json(&json!(st.sol_fills)),
                _ => MockHttpResponse::ok_json(&json!([])),
            }
        } else {
            if req
                .json_body
                .get("action")
                .and_then(|a| a.get("type"))
                .and_then(Value::as_str)
                == Some("cancel")
            {
                if let Some(cancels) = req
                    .json_body
                    .get("action")
                    .and_then(|a| a.get("cancels"))
                    .and_then(Value::as_array)
                {
                    let mut statuses = Vec::new();
                    for c in cancels {
                        if let Some(oid) = c.get("o").and_then(Value::as_u64) {
                            st.cancelled_oids.push(oid);
                        }
                        statuses.push(json!("success"));
                    }
                    return MockHttpResponse::ok_json(&json!({
                        "status": "ok",
                        "response": { "type": "cancel", "data": { "statuses": statuses } }
                    }));
                }
            }
            let wires = extract_wire_orders(&req.json_body);
            let mut statuses = Vec::new();
            for w in wires {
                st.next_oid += 1;
                let oid = st.next_oid;
                let is_ioc = w.tif.as_deref() == Some("Ioc");
                let px_val = w.px.clone();
                let sz_val = w.sz.clone();
                let asset_idx = w.asset_index;
                st.submitted.push(w);
                if is_ioc {
                    if asset_idx == 4 {
                        st.eth_szi = "0.5".to_string();
                    } else if asset_idx == 5 {
                        st.avax_szi = "0.0".to_string();
                    }
                    statuses.push(json!({
                        "filled": { "totalSz": sz_val, "avgPx": px_val, "oid": oid }
                    }));
                } else {
                    statuses.push(json!({
                        "resting": { "oid": oid }
                    }));
                }
            }
            MockHttpResponse::ok_json(&json!({
                "status": "ok",
                "response": { "type": "order", "data": { "statuses": statuses } }
            }))
        }
    });

    let db = fresh_w2_db();
    configure_live_loopback(&db, &srv.base_url());
    db.set_config_value("passive_close_rest_sec", "8", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "300", None)
        .unwrap();

    let sol = upsert_perp(&db, "SOL", 3, 2, 0.01, 0.01, 102.0);
    let eth = upsert_perp(&db, "ETH", 4, 3, 0.1, 0.001, 2030.0);
    let avax = upsert_perp(&db, "AVAX", 5, 2, 0.01, 0.01, 37.5);

    let engine = ExecutionEngine::new(db.clone());
    let now = now_ms();

    // 1. Seed mature SOL Long: 10.0 @ 100.0 (mark 102.0 = +200 bps >> 14 bps breakeven+edge)
    let (sol_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: "SOL",
            side: OrderSide::Long,
            entry_price: 100.0,
            mark_price: 102.0,
            quantity: 10.0,
            committed_notional_usdc: 1000.0,
            unrealized_pnl_usdc: 20.0,
            portfolio_weight: 0.10,
            protective_stop_px: 95.0,
            take_profit_px: 112.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 1_200_000),
        })
        .unwrap();
    db.update_position_tpsl_orders(
        sol_pid,
        95.0,
        OidUpdate::Set("hl-70001"),
        112.0,
        OidUpdate::Set("hl-70002"),
    )
    .unwrap();

    // Submit non-emergency ACTION_REDUCE (trim 40% = 4.0 SOL -> target remaining 6.0 SOL)
    let sig_reduce_sol = make_sig(&sol, 0.06, 0.10, ACTION_REDUCE);
    let reb_sol = engine
        .execute_signal_rebalance(&make_sig_out(vec![sig_reduce_sol]), 10_000.0)
        .unwrap();
    assert_eq!(reb_sol.positions_closed, 0);

    // Must have posted a strictly passive Alo reduceOnly order (ZERO IOC orders!)
    {
        let st = state.lock();
        let non_trig: Vec<&RecordedWireOrder> =
            st.submitted.iter().filter(|w| !w.is_trigger).collect();
        assert_eq!(non_trig.len(), 1);
        assert_eq!(non_trig[0].asset_index, 3);
        assert!(!non_trig[0].is_buy);
        assert!(non_trig[0].reduce_only);
        assert_eq!(non_trig[0].tif.as_deref(), Some("Alo"));
        assert_eq!(non_trig[0].sz, "4");
    }
    let pending_sol = engine
        .priority_monitor
        .passive_closes
        .get(sol_pid)
        .expect("SOL partial reduce must rest in PassiveCloseTracker");
    assert!(pending_sol.is_partial_reduce);
    assert!((pending_sol.initial_qty - 4.0).abs() < 1e-6);

    // Drift BBO favorably within passive_close_rest_sec -> PriorityMonitor reprices resting Alo order!
    {
        let mut st = state.lock();
        st.sol_bid = "102.10".to_string();
        st.sol_ask = "102.20".to_string();
    }
    let _tick_reprice = engine.priority_monitor.run_1s_priority_tick().unwrap();
    let repriced_sol = engine
        .priority_monitor
        .passive_closes
        .get(sol_pid)
        .expect("still resting after BBO reprice");
    assert_eq!(repriced_sol.reprice_count, 1);
    assert!(repriced_sol.limit_px > pending_sol.limit_px);
    {
        let st = state.lock();
        assert!(
            st.submitted
                .iter()
                .filter(|w| !w.is_trigger && w.asset_index == 3)
                .all(|w| w.tif.as_deref() == Some("Alo")),
            "All passive reduce quotes & reprices must be Alo post-only"
        );
    }

    // Maker fill lands on Hyperliquid (szi 10.0 -> 6.0, maker fee 1.5 bps = $0.0613)
    let fill_ts = now_ms();
    {
        let mut st = state.lock();
        st.sol_szi = "6.0".to_string();
        st.sol_fills = vec![json!({
            "coin": "SOL",
            "time": fill_ts,
            "dir": "Close Long",
            "side": "A",
            "px": "102.19",
            "sz": "4.0",
            "fee": "0.0613",
            "closedPnl": "8.76",
            "oid": repriced_sol.oid.unwrap_or(90_002)
        })];
    }
    let _tick_fill = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert!(
        engine
            .priority_monitor
            .passive_closes
            .get(sol_pid)
            .is_none(),
        "Completed partial maker reduce must clear PassiveCloseTracker"
    );

    let sol_after = db.get_position_by_id(sol_pid).unwrap().unwrap();
    assert_eq!(sol_after.status, "OPEN");
    assert!((sol_after.filled_quantity - 6.0).abs() < 1e-6);
    assert!(
        (sol_after.committed_notional_usdc - 600.0).abs() < 1e-2,
        "committed_notional_usdc must scale 1000 -> 600, got {}",
        sol_after.committed_notional_usdc
    );
    // Maker fee is 1.5 bps ($0.0613), NOT 4.5 bps ($0.1839)!
    assert!(
        (sol_after.fees_paid_usdc - 0.0613).abs() < 1e-3,
        "Expected ~1.5 bps maker fee ($0.0613), got {}",
        sol_after.fees_paid_usdc
    );
    let expected_rpnl = 4.0 * (102.19 - 100.0) - 0.0613;
    assert!(
        (sol_after.realized_pnl_usdc - expected_rpnl).abs() < 1e-2,
        "Expected realized_pnl_usdc ~ {expected_rpnl:.4}, got {}",
        sol_after.realized_pnl_usdc
    );
    // Exchange SL/TP trigger orders must have been re-synced to remaining qty 6!
    {
        let st = state.lock();
        let sol_triggers: Vec<&RecordedWireOrder> = st
            .submitted
            .iter()
            .filter(|w| w.is_trigger && w.asset_index == 3 && w.sz == "6")
            .collect();
        assert!(
            sol_triggers.len() >= 2,
            "SL & TP trigger orders must be re-synced to remaining size 6 after partial reduce"
        );
    }

    // 2. Timeout fallback on ETH: passive Alo rests for passive_close_rest_sec=1s, then escalates to IOC!
    db.set_config_value("passive_close_rest_sec", "1", None)
        .unwrap();
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth.id,
            coin: "ETH",
            side: OrderSide::Long,
            entry_price: 2000.0,
            mark_price: 2030.0,
            quantity: 1.0,
            committed_notional_usdc: 2000.0,
            unrealized_pnl_usdc: 30.0,
            portfolio_weight: 0.20,
            protective_stop_px: 1920.0,
            take_profit_px: 2240.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 1_200_000),
        })
        .unwrap();
    let sig_reduce_eth = make_sig(&eth, 0.10, 0.20, ACTION_REDUCE);
    engine
        .execute_signal_rebalance(&make_sig_out(vec![sig_reduce_eth]), 10_000.0)
        .unwrap();
    assert!(engine
        .priority_monitor
        .passive_closes
        .get(eth_pid)
        .is_some());

    std::thread::sleep(Duration::from_millis(1_150));
    engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert!(engine
        .priority_monitor
        .passive_closes
        .get(eth_pid)
        .is_none());
    let eth_after = db.get_position_by_id(eth_pid).unwrap().unwrap();
    assert_eq!(eth_after.status, "OPEN");
    assert!((eth_after.filled_quantity - 0.5).abs() < 1e-6);
    {
        let st = state.lock();
        assert!(
            st.submitted
                .iter()
                .any(|w| w.asset_index == 4 && w.tif.as_deref() == Some("Ioc")),
            "Timed-out passive reduce must escalate to IOC ladder"
        );
    }

    // 3. Urgent PROTECTIVE_STOP on AVAX (mark 37.5 < stop 38.5 while passive_close_rest_sec=8)
    //    MUST execute ImmediateIoc on Tick 1 without resting passively!
    db.set_config_value("passive_close_rest_sec", "8", None)
        .unwrap();
    let (avax_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: avax.id,
            coin: "AVAX",
            side: OrderSide::Long,
            entry_price: 40.0,
            mark_price: 37.5,
            quantity: 20.0,
            committed_notional_usdc: 800.0,
            unrealized_pnl_usdc: -50.0,
            portfolio_weight: 0.08,
            protective_stop_px: 38.5,
            take_profit_px: 45.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 600_000),
        })
        .unwrap();
    let tick_urgent = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(tick_urgent.stops_triggered_closed, 1);
    assert!(
        engine
            .priority_monitor
            .passive_closes
            .get(avax_pid)
            .is_none(),
        "PROTECTIVE_STOP must never wait in PassiveCloseTracker"
    );
    let avax_closed = db.get_position_by_id(avax_pid).unwrap().unwrap();
    assert_eq!(avax_closed.status, "CLOSED");
    {
        let st = state.lock();
        let avax_orders: Vec<&RecordedWireOrder> = st
            .submitted
            .iter()
            .filter(|w| !w.is_trigger && w.asset_index == 5)
            .collect();
        assert_eq!(avax_orders.len(), 1);
        assert_eq!(avax_orders[0].tif.as_deref(), Some("Ioc"));
    }
}

#[test]
fn test_w2_doge_174622_churn_reproduction_hysteresis_cooldown_and_scale_in_spacing() {
    let db = fresh_w2_db();
    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("initial_paper_equity_usdc", "10000.0", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "10000.0", None)
        .unwrap();
    db.set_config_value("maker_fee_bps", "1.5", None).unwrap();
    db.set_config_value("taker_fee_bps", "4.5", None).unwrap();
    db.set_config_value("scale_in_min_atr_mult", "0.25", None)
        .unwrap();
    db.set_config_value("scale_in_min_spacing_bps", "15.0", None)
        .unwrap();
    db.set_config_value("scale_in_reduce_cooldown_sec", "900", None)
        .unwrap();
    db.set_config_value("reduce_min_net_edge_bps", "8.0", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "300", None)
        .unwrap();
    db.set_config_value("enable_scale_in_out_rebalance", "true", None)
        .unwrap();

    let engine = ExecutionEngine::new(db.clone());
    let t_open = now_ms() - 3_600_000;

    // Seed DOGE-USDC base OPEN Long at 0.055500 + 20 calm 4h bars (~53 bps ATR -> 0.25*ATR = 13.2 bps, floored at 15 bps),
    // then merge a passive scale-in @ 0.056656 (19:43 live fill)
    let doge_base = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.055500);
    let mut doge_bars = Vec::new();
    for i in 0..20 {
        doge_bars.push(BarUpsertInput {
            asset_id: doge_base.id,
            timeframe: "4h".to_string(),
            timestamp_ms: t_open - (20 - i) * 14_400_000,
            open: 0.056600,
            high: 0.056750,
            low: 0.056450,
            close: 0.056600,
            volume: 5_000_000.0,
            funding_rate: 0.0,
        });
    }
    db.upsert_ohlcv_bars(&doge_bars).unwrap();

    let (doge_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: doge_base.id,
            coin: "DOGE",
            side: OrderSide::Long,
            entry_price: 0.055500,
            mark_price: 0.055500,
            quantity: 10_000.0,
            committed_notional_usdc: 555.0,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.0555,
            protective_stop_px: 0.053500,
            take_profit_px: 0.062000,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(t_open),
        })
        .unwrap();

    // Scale-in @ 0.056656 (+208 bps from 0.055500 -> passes 15 bps spacing!)
    let doge_1943 = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.056657);
    let sig_inc_1943 = make_sig(&doge_1943, 0.12, 0.0566, ACTION_INCREASE);
    let reb_1943 = engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_inc_1943]),
            10_000.0,
            Some(now_ms() + 1_000),
        )
        .unwrap();
    assert_eq!(reb_1943.working_orders_placed, 1);
    let child_pid = reb_1943.placed_position_ids[0];
    let child_ord = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == child_pid)
        .unwrap();
    db.update_asset_live_quote(doge_1943.id, 0.056656, 0.056656)
        .unwrap();
    let fill_out = engine.chaser.advance_working_order(&child_ord).unwrap();
    assert!(matches!(fill_out, ChaseStepOutcome::FilledOpen { .. }));

    let t_scale_in = now_ms();
    let merged_pos = db.get_position_by_id(doge_pid).unwrap().unwrap();
    let qty_after_add = merged_pos.filled_quantity;
    assert!(qty_after_add > 10_000.0);

    // 1. At 20:00 (17m = 1020s after scale-in), DOGE is at 0.056641 (underwater vs 0.056656 scale-in
    //    AND below avg entry + 14 bps): BOTH allocator AND engine MUST suppress ACTION_REDUCE!
    let doge_2000 = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.056641);
    let cfg_now = db.load_config().unwrap();
    let alloc_cfg = AllocatorConfig::from_config_map(&cfg_now);
    let mut held_ctx = HashMap::new();
    held_ctx.insert(
        doge_2000.id,
        HeldPositionContext {
            current_weight: 0.10,
            unrealized_pnl_pct: (0.056641 - merged_pos.entry_price) / merged_pos.entry_price,
            entry_r0_pct: 0.036,
        },
    );
    // Even if raw target drops to 0.055, allocator holds 0.10 because +89 bps vs avg entry is checked,
    // AND when unrealized_pnl_pct < +14 bps (e.g. entry @ 0.056656 -> -2.65 bps), allocator outputs ACTION_HOLD!
    let mut held_ctx_doge_band = HashMap::new();
    held_ctx_doge_band.insert(
        doge_2000.id,
        HeldPositionContext {
            current_weight: 0.10,
            unrealized_pnl_pct: (0.056641 - 0.056656) / 0.056656,
            entry_r0_pct: 0.036,
        },
    );
    let mut s1_map = HashMap::new();
    s1_map.insert(
        doge_2000.id,
        CarryMomentumMetrics {
            asset_id: doge_2000.id,
            symbol: doge_2000.symbol.clone(),
            s1_mom_score: 0.35,
            s1_carry_score: 0.10,
            annualized_vol: 0.50,
            market_beta: 1.0,
            w_s1_carry_mom: 0.03,
            passes_quality: true,
        },
    );
    let alloc_decisions = allocate_multi_sleeve_weights_with_context(
        std::slice::from_ref(&doge_2000),
        &s1_map,
        &HashMap::new(),
        &HashMap::new(),
        &held_ctx_doge_band,
        &[],
        &RegimeGateState::default(),
        &alloc_cfg,
    );
    assert_eq!(
        alloc_decisions[0].rebalance_action, ACTION_HOLD,
        "Allocator must suppress non-emergency REDUCE at 0.056641 (-2.65 bps < +14 bps)"
    );

    // Engine gate: 20:00 @ 0.056641 (t_scale_in + 1020s) -> SUPPRESSED (below last_scale_in 0.056656 + 14 bps)!
    let sig_trim_2000 = make_sig(&doge_2000, 0.05, 0.10, ACTION_REDUCE);
    engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_trim_2000]),
            10_000.0,
            Some(t_scale_in + 1_020_000),
        )
        .unwrap();
    assert!(
        (db.get_position_by_id(doge_pid)
            .unwrap()
            .unwrap()
            .filled_quantity
            - qty_after_add)
            .abs()
            < 1e-6,
        "DOGE trim @ 0.056641 must be suppressed"
    );

    // Also test < 15m flip cooldown: even at +40 bps (0.056900), at t_scale_in + 600s (< 900s) -> SUPPRESSED!
    let doge_early_spike = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.056900);
    let sig_trim_cooldown = make_sig(&doge_early_spike, 0.05, 0.10, ACTION_REDUCE);
    engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_trim_cooldown]),
            10_000.0,
            Some(t_scale_in + 600_000),
        )
        .unwrap();
    assert!(
        (db.get_position_by_id(doge_pid)
            .unwrap()
            .unwrap()
            .filled_quantity
            - qty_after_add)
            .abs()
            < 1e-6,
        "DOGE trim within 900s scale_in_reduce_cooldown_sec must be suppressed"
    );

    // Also test 21:35 @ 0.056659 (+0.53 bps vs 0.056656 < 14 bps) -> SUPPRESSED!
    let doge_2135 = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.056659);
    let sig_trim_2135 = make_sig(&doge_2135, 0.05, 0.10, ACTION_REDUCE);
    engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_trim_2135]),
            10_000.0,
            Some(t_scale_in + 2_400_000),
        )
        .unwrap();
    assert!(
        (db.get_position_by_id(doge_pid)
            .unwrap()
            .unwrap()
            .filled_quantity
            - qty_after_add)
            .abs()
            < 1e-6,
        "DOGE trim @ 0.056659 (+0.53 bps < 14 bps) must be suppressed"
    );

    // 2. Test 20:12 scale-in @ 0.056637 (3.35 bps from 0.056656 < max(2x RT 12 bps, 15 bps floor, 0.25 ATR)) -> SUPPRESSED!
    let doge_2012 = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.056637);
    let min_sp = compute_min_scale_in_spacing_pct(&cfg_now, 0.056637, None);
    assert!(
        (min_sp - 0.0015).abs() < 1e-9,
        "Default min spacing floor must be 15 bps (0.0015), got {min_sp}"
    );
    let sig_inc_2012 = make_sig(&doge_2012, 0.18, 0.12, ACTION_INCREASE);
    let reb_2012 = engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_inc_2012]),
            10_000.0,
            Some(t_scale_in + 1_740_000),
        )
        .unwrap();
    assert_eq!(
        reb_2012.working_orders_placed, 0,
        "Second INCREASE @ 0.056637 (3.35 bps from 0.056656) must be suppressed"
    );
    let open_pos_now = db.get_position_by_id(doge_pid).unwrap().unwrap();
    assert!(
        check_scale_in_spacing_for_open_position(
            &db,
            &engine.chaser,
            &open_pos_now,
            0.056637,
            None,
            &cfg_now,
        )
        .is_err(),
        "Direct OrderChaser spacing check must also reject 0.056637"
    );

    // Also test 21:18 scale-in @ 0.056734 (13.77 bps from 0.056656 < 15.0 bps) -> SUPPRESSED!
    let doge_2118 = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.056734);
    let sig_inc_2118 = make_sig(&doge_2118, 0.18, 0.12, ACTION_INCREASE);
    let reb_2118 = engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_inc_2118]),
            10_000.0,
            Some(t_scale_in + 5_700_000),
        )
        .unwrap();
    assert_eq!(
        reb_2118.working_orders_placed, 0,
        "Scale-in @ 0.056734 (13.77 bps < 15 bps) must be suppressed"
    );

    // 3. Duplicate WORKING order prevention: when price moves +180 bps to 0.057680, ONE scale-in
    //    places a WORKING order, and a second INCREASE while WORKING exists is suppressed!
    let doge_breakout = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.057680);
    let sig_inc_ok = make_sig(&doge_breakout, 0.18, 0.12, ACTION_INCREASE);
    let reb_inc_ok = engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_inc_ok.clone()]),
            10_000.0,
            Some(t_scale_in + 6_000_000),
        )
        .unwrap();
    assert_eq!(reb_inc_ok.working_orders_placed, 1);
    let working_id = reb_inc_ok.placed_position_ids[0];

    let reb_dup = engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_inc_ok]),
            10_000.0,
            Some(t_scale_in + 6_010_000),
        )
        .unwrap();
    assert_eq!(
        reb_dup.working_orders_placed, 0,
        "Duplicate INCREASE while WORKING order already exists on asset must be suppressed"
    );
    db.cancel_working_order(working_id, "TEST_CLEANUP").unwrap();

    // 4. Once elapsed >= 900s AND price clears BOTH avg entry AND latest_scale_in_price (0.056656)
    //    by >= 14 bps (e.g. 0.056800 = +25.4 bps), non-emergency ACTION_REDUCE executes cleanly!
    let doge_profit = upsert_perp(&db, "DOGE", 12, 0, 0.000001, 1.0, 0.056800);
    let sig_trim_ok = make_sig(&doge_profit, 0.05, 0.10, ACTION_REDUCE);
    engine
        .execute_signal_rebalance_at(
            &make_sig_out(vec![sig_trim_ok]),
            10_000.0,
            Some(t_scale_in + 6_600_000),
        )
        .unwrap();
    let reduced_pos = db.get_position_by_id(doge_pid).unwrap().unwrap();
    assert!(
        reduced_pos.filled_quantity < qty_after_add,
        "Legitimate profitable REDUCE clearing 900s cooldown and +14 bps above scale-in must execute"
    );

    // Urgent bypass check: evaluate_reduce_hysteresis_gate permits urgent_bypass=true unconditionally
    let anchors = aegis_execution::PositionScaleAnchors {
        latest_scale_in_price: Some(0.056656),
        latest_scale_in_at_ms: Some(t_scale_in),
        latest_scale_or_trim_price: Some(0.056656),
        latest_scale_or_trim_at_ms: Some(t_scale_in),
    };
    assert!(evaluate_reduce_hysteresis_gate(
        OrderSide::Long,
        0.056600,
        0.056641,
        &anchors,
        t_scale_in + 60_000,
        &cfg_now,
        true,
    )
    .is_ok());
}

#[test]
fn test_w2_resting_working_orders_counted_in_exposure_weight_and_portfolio_heat_gates() {
    let db = fresh_w2_db();
    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("initial_paper_equity_usdc", "10000.0", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "10000.0", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("max_gross_leverage_x", "2.00", None)
        .unwrap();
    db.set_config_value("risk_tier", "AGGRESSIVE", None)
        .unwrap(); // 2.0x target mult -> $20,000 max gross
    db.set_config_value("max_single_asset_weight", "0.50", None)
        .unwrap(); // $5,000 max single-asset notional
    db.set_config_value("max_portfolio_heat_pct", "0.05", None)
        .unwrap(); // $500 max portfolio stop-risk budget

    let engine = ExecutionEngine::new(db.clone());
    let now = now_ms();

    let btc = upsert_perp(&db, "BTC", 1, 4, 0.1, 0.0001, 50_000.0);
    let sol = upsert_perp(&db, "SOL", 3, 2, 0.01, 0.01, 100.0);
    let eth = upsert_perp(&db, "ETH", 4, 3, 0.1, 0.001, 2_000.0);

    // Seed 20 4h bars for SOL (~2% ATR -> ~4.4% stop distance) so size_working_order_from_weights computes ~4.4% stop
    let mut sol_bars = Vec::new();
    for i in 0..20 {
        sol_bars.push(BarUpsertInput {
            asset_id: sol.id,
            timeframe: "4h".to_string(),
            timestamp_ms: now - (20 - i) * 14_400_000,
            open: 99.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 10_000.0,
            funding_rate: 0.0,
        });
    }
    db.upsert_ohlcv_bars(&sol_bars).unwrap();

    // 1. BTC OPEN position: 0.06 BTC @ $50,000 = $3,000 notional, stop @ $47,000 (6% = $180 stop risk)
    let (_btc_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: btc.id,
            coin: "BTC",
            side: OrderSide::Long,
            entry_price: 50_000.0,
            mark_price: 50_000.0,
            quantity: 0.06,
            committed_notional_usdc: 3_000.0,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.30,
            protective_stop_px: 47_000.0,
            take_profit_px: 56_000.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 1_800_000),
        })
        .unwrap();

    // 2. Place a resting WORKING INCREASE scale-in on BTC @ $51,000 (+200 bps):
    //    0.035 BTC @ $51,000 = ~$1,785 notional, stop @ $47,000 (~7.84% = ~$140 stop risk)
    let btc_up = upsert_perp(&db, "BTC", 1, 4, 0.1, 0.0001, 51_000.0);
    let btc_scale_wid = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: btc_up,
            side: OrderSide::Long,
            requested_quantity: 0.035,
            reserved_margin_usdc: 892.5,
            portfolio_weight: 0.1785,
            s1_weight_contrib: 0.1785,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 47_000.0,
            reason: "TEST_WORKING_SCALE_IN".to_string(),
            bypass_entry_interval: true,
            entry_urgency: EntryUrgency::MakerOnly,
            is_scale_in: true,
        })
        .unwrap();

    // 3. Place a resting WORKING fresh entry on ETH: 1.5 ETH @ $2,000 = $3,000 notional, stop @ $1,880 (6% = $180 stop risk)
    let eth_wid = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: eth.clone(),
            side: OrderSide::Long,
            requested_quantity: 1.5,
            reserved_margin_usdc: 1_500.0,
            portfolio_weight: 0.30,
            s1_weight_contrib: 0.30,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 1_880.0,
            reason: "TEST_WORKING_FRESH_ENTRY".to_string(),
            bypass_entry_interval: true,
            entry_urgency: EntryUrgency::MakerOnly,
            is_scale_in: false,
        })
        .unwrap();

    // Verify compute_active_exposure_and_heat_with_db counts BOTH the BTC WORKING scale-in AND ETH WORKING order!
    // OPEN risk alone = $180 (1.8% < 5.0%), whereas OPEN + WORKING = $180 + ~$140 + ~$180 = ~$500 (5.0% heat cap full!)
    let held = db.get_assets_held().unwrap();
    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 2);
    let exp =
        compute_active_exposure_and_heat_with_db(Some(&db), &held, &ordered, 2.0, 0.025, None);
    assert!((exp.open_notional_usdc - 3_000.0).abs() < 1e-2);
    assert!(
        exp.working_notional_usdc > 4_700.0,
        "working_notional_usdc must sum both fresh entry + scale-in working orders, got {}",
        exp.working_notional_usdc
    );
    assert!(
        exp.open_stop_risk_usdc < 200.0 && exp.total_stop_risk_usdc >= 495.0,
        "total_stop_risk_usdc must include resting WORKING stop risk: open={}, working={}, total={}",
        exp.open_stop_risk_usdc,
        exp.working_stop_risk_usdc,
        exp.total_stop_risk_usdc
    );

    // 4. Attempt new ACTION_OPEN_LONG on SOL ($2,500 notional, ~$110 stop risk):
    //    Without counting WORKING orders, $180 + $110 = $290 <= $500 would wrongly pass!
    //    With WORKING orders counted, ~$500 + $110 = ~$610 > $500 -> MUST BE BLOCKED!
    let sig_sol = make_sig(&sol, 0.25, 0.0, ACTION_OPEN_LONG);
    let reb_blocked_heat = engine
        .execute_signal_rebalance(&make_sig_out(vec![sig_sol.clone()]), 10_000.0)
        .unwrap();
    assert_eq!(
        reb_blocked_heat.working_orders_placed, 0,
        "Resting WORKING orders (including INCREASE scale-ins) must count against max_portfolio_heat_pct"
    );

    // 5. Cancel the two WORKING orders -> heat drops back to $180 -> SOL entry is now permitted!
    db.cancel_working_order(btc_scale_wid, "FREE_HEAT").unwrap();
    db.cancel_working_order(eth_wid, "FREE_HEAT").unwrap();
    let reb_allowed = engine
        .execute_signal_rebalance(&make_sig_out(vec![sig_sol]), 10_000.0)
        .unwrap();
    assert_eq!(
        reb_allowed.working_orders_placed, 1,
        "Once WORKING orders are cancelled and heat is freed, SOL entry should succeed"
    );
}

#[test]
fn test_w2_calm_signal_close_uses_passive_alo_while_early_loser_uses_immediate_ioc() {
    let submitted: Arc<Mutex<Vec<RecordedWireOrder>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_srv = Arc::clone(&submitted);
    let srv = MockHttpServer::start(move |req| {
        if req.path == "/info" {
            match req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("")
            {
                "clearinghouseState" => MockHttpResponse::ok_json(&json!({
                    "marginSummary": { "accountValue": "10000.0" },
                    "assetPositions": [
                        { "position": { "coin": "SOL", "szi": "5.0", "entryPx": "100.0", "positionValue": "510.0" } },
                        { "position": { "coin": "ETH", "szi": "1.0", "entryPx": "2000.0", "positionValue": "1970.0" } }
                    ]
                })),
                "l2Book" => {
                    let coin = req
                        .json_body
                        .get("coin")
                        .and_then(Value::as_str)
                        .unwrap_or("SOL");
                    let (b, a) = if coin == "ETH" {
                        ("1969.5", "1970.5")
                    } else {
                        ("101.95", "102.05")
                    };
                    MockHttpResponse::ok_json(&json!({
                        "levels": [[{ "px": b, "sz": "100.0" }], [{ "px": a, "sz": "100.0" }]]
                    }))
                }
                _ => MockHttpResponse::ok_json(&json!([])),
            }
        } else {
            let wires = extract_wire_orders(&req.json_body);
            let mut statuses = Vec::new();
            let mut guard = submitted_srv.lock();
            for w in wires {
                let is_ioc = w.tif.as_deref() == Some("Ioc");
                let px = w.px.clone();
                let sz = w.sz.clone();
                guard.push(w);
                if is_ioc {
                    statuses.push(
                        json!({ "filled": { "totalSz": sz, "avgPx": px, "oid": 99_101u64 } }),
                    );
                } else {
                    statuses.push(json!({ "resting": { "oid": 99_102u64 } }));
                }
            }
            MockHttpResponse::ok_json(&json!({
                "status": "ok",
                "response": { "type": "order", "data": { "statuses": statuses } }
            }))
        }
    });

    let db = fresh_w2_db();
    configure_live_loopback(&db, &srv.base_url());
    db.set_config_value("passive_close_rest_sec", "8", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "300", None)
        .unwrap();
    db.set_config_value("early_loser_invalidation_enabled", "true", None)
        .unwrap();

    let sol = upsert_perp(&db, "SOL", 3, 2, 0.01, 0.01, 102.0);
    let eth = upsert_perp(&db, "ETH", 4, 3, 0.1, 0.001, 1970.0);
    let engine = ExecutionEngine::new(db.clone());
    let now = now_ms();

    // 1. Calm profitable SOL (held 10m > 300s, +2.0%): ACTION_CLOSE posts passive Alo maker first!
    let (sol_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: "SOL",
            side: OrderSide::Long,
            entry_price: 100.0,
            mark_price: 102.0,
            quantity: 5.0,
            committed_notional_usdc: 500.0,
            unrealized_pnl_usdc: 10.0,
            portfolio_weight: 0.05,
            protective_stop_px: 95.0,
            take_profit_px: 112.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 600_000),
        })
        .unwrap();

    // 2. Urgent early-loser ETH (held 200s >= 120s, mark 1970 vs entry 2000 = -1.5% <= -0.9%):
    //    ACTION_CLOSE uses ImmediateIoc!
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth.id,
            coin: "ETH",
            side: OrderSide::Long,
            entry_price: 2000.0,
            mark_price: 1970.0,
            quantity: 1.0,
            committed_notional_usdc: 2000.0,
            unrealized_pnl_usdc: -30.0,
            portfolio_weight: 0.20,
            protective_stop_px: 1920.0,
            take_profit_px: 2200.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 200_000),
        })
        .unwrap();

    let sigs = vec![
        make_sig(&sol, 0.0, 0.05, ACTION_CLOSE),
        make_sig(&eth, 0.0, 0.20, ACTION_CLOSE),
    ];
    let reb = engine
        .execute_signal_rebalance(&make_sig_out(sigs), 10_000.0)
        .unwrap();
    assert_eq!(reb.positions_closed, 1);
    assert_eq!(reb.closed_position_ids, vec![eth_pid]);
    assert!(
        engine
            .priority_monitor
            .passive_closes
            .get(sol_pid)
            .is_some(),
        "Calm signal ACTION_CLOSE must rest Alo maker order first"
    );
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "CLOSED",
        "Early-loser invalidation must close immediately via IOC"
    );

    let guard = submitted.lock();
    let sol_w = guard.iter().find(|w| w.asset_index == 3).unwrap();
    let eth_w = guard.iter().find(|w| w.asset_index == 4).unwrap();
    assert_eq!(sol_w.tif.as_deref(), Some("Alo"));
    assert_eq!(eth_w.tif.as_deref(), Some("Ioc"));
}
