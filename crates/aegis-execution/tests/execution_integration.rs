//! End-to-End Integration Tests for Aegis Quant v2.0 Execution Engine (`aegis-execution`).

use aegis_core::{now_ms, ConfigMap, OrderSide, RiskTier};
use aegis_db::{
    AssetUpsertInput, DatabaseManager, ExchangeOpenPositionSyncInput, OidUpdate,
    SnapshotInsertInput,
};
use aegis_execution::{ChaseStepOutcome, ExecutionEngine, NewWorkingOrderRequest};
use aegis_risk::{RiskEngine, RiskLimitsConfig};
use aegis_signals::{
    allocate_multi_sleeve_weights, AllocatorConfig, CarryMomentumMetrics, MarketBreadthSnapshot,
    SignalCycleOutput, SleeveSignalInput, ACTION_CLOSE, ACTION_FLIP, ACTION_OPEN_LONG,
};
use aegis_test_support::mock_http::MockHttpServer;
use aegis_test_support::mock_ws::MockWsServer;
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn fresh_isolated_db() -> DatabaseManager {
    let db = DatabaseManager::new_isolated_test(None).unwrap();
    db.init_schema().unwrap();
    db
}

fn update_test_asset_px(db: &DatabaseManager, asset: &aegis_core::ExchangeAsset, new_px: f64) {
    db.upsert_exchange_asset(&AssetUpsertInput {
        raw_symbol_or_coin: asset.coin.clone(),
        asset_index: asset.asset_index,
        sz_decimals: asset.sz_decimals,
        tick_size: asset.tick_size,
        lot_step_size: asset.lot_step_size,
        max_leverage: asset.max_leverage,
        mark_price: new_px,
        mid_price: new_px,
        oracle_price: new_px,
        funding_rate_hourly: asset.funding_rate_hourly,
        day_volume_usdc: asset.day_volume_usdc,
        open_interest_usdc: asset.open_interest_usdc,
        prev_day_price: asset.prev_day_price,
        is_core_eligible: asset.is_core_eligible,
        is_event_eligible: asset.is_event_eligible,
        is_priority_watched: asset.is_priority_watched,
    })
    .unwrap();
}

#[test]
fn test_3s_market_watcher_and_order_to_open_capital_lifecycle() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();

    // 1. Run 3s exchange scan (discovers/seeds canonical <COIN>-USDC perp assets)
    let scan = engine.watcher.run_3s_exchange_scan().unwrap();
    assert!(scan.total_assets_upserted >= 20);
    assert!(scan.core_eligible_count >= 10);

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    assert_eq!(sol.symbol, "SOL-USDC");
    assert_eq!(sol.coin, "SOL");

    // 2. Place a WORKING order via OrderChaser (reserves margin, does NOT commit notional yet)
    db.update_asset_live_quote(sol.id, 184.25, 184.25).unwrap();
    let sol_fresh = db.get_asset_by_id(sol.id).unwrap().unwrap();
    let pos_id = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: sol_fresh.clone(),
            side: OrderSide::Long,
            requested_quantity: 10.0,
            reserved_margin_usdc: 995.95,
            portfolio_weight: 0.184,
            s1_weight_contrib: 0.104,
            s2_weight_contrib: 0.080,
            s3_weight_contrib: 0.0,
            protective_stop_px: 176.80,
            reason: "LIVE_BREAKOUT_SURGE".to_string(),
            bypass_entry_interval: false,
        })
        .unwrap();
    assert!(pos_id > 0);

    // Verify `v_assets_ordered` has the WORKING order with reserved_margin_usdc > 0 and `v_assets_held` is empty
    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 1);
    assert_eq!(ordered[0].symbol, "SOL-USDC");
    assert!((ordered[0].reserved_margin_usdc - 995.95).abs() < 1e-2);
    assert_eq!(ordered[0].fsm_state, "WORKING_ALO");
    assert!(db.get_assets_held().unwrap().is_empty());

    // Verify `v_priority_watchlist` includes SOL-USDC under 1s surveillance
    let watchlist = db.get_priority_watchlist().unwrap();
    assert!(watchlist
        .iter()
        .any(|w| w.symbol == "SOL-USDC" && w.position_id == Some(pos_id)));

    // 3. Simulate mid-price drifting upward (185.20 -> >4 bps away from limit 184.24) to trigger a 1s ALO chase
    db.update_asset_live_quote(sol.id, 185.20, 185.20).unwrap();
    let ordered_before_chase = db.get_assets_ordered().unwrap();
    let chase_res = engine
        .chaser
        .advance_working_order(&ordered_before_chase[0])
        .unwrap();
    match chase_res {
        ChaseStepOutcome::Chased {
            chase_count,
            fsm_state,
            new_limit_price,
            ..
        } => {
            assert_eq!(chase_count, 1);
            assert_eq!(fsm_state, "CHASING_ALO");
            assert!(new_limit_price > ordered_before_chase[0].limit_price);
        }
        other => panic!("Expected Chased outcome, got {other:?}"),
    }

    // 4. Simulate mid-price touching the chased limit price -> promotes WORKING -> OPEN
    let ordered_after_chase = db.get_assets_ordered().unwrap();
    let chased_limit = ordered_after_chase[0].limit_price;
    db.update_asset_live_quote(sol.id, chased_limit, chased_limit)
        .unwrap();

    let prio_tick = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(prio_tick.orders_promoted_to_open, 1);

    // Verify WORKING orders view is now empty and OPEN holdings view (`v_assets_held`) has the position
    assert!(db.get_assets_ordered().unwrap().is_empty());
    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].position_id, pos_id);
    assert_eq!(held[0].symbol, "SOL-USDC");
    assert!(held[0].committed_notional_usdc > 1800.0);
    assert!(held[0].protective_stop_px > 0.0);
    assert!(held[0].protective_stop_px < held[0].entry_price);
    assert!(held[0].protective_stop_oid.is_some());
    assert!(held[0].take_profit_px > held[0].entry_price);
    assert!(held[0].take_profit_oid.is_some());

    let pos_open = db.get_position_by_id(pos_id).unwrap().unwrap();
    assert_eq!(pos_open.status, "OPEN");
    assert_eq!(pos_open.fsm_state, "FILLED_OPEN");
    assert_eq!(pos_open.reserved_margin_usdc, 0.0);
    assert!(pos_open.committed_notional_usdc > 1800.0);

    // 5. Simulate price rally to 192.00 (below 193.63 TP, >= 0.40R breakeven trigger) -> 1s Priority Monitor ratchets protective stop upward
    let initial_stop = held[0].protective_stop_px;
    update_test_asset_px(&db, &sol_fresh, 192.00);
    let ratchet_tick = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(ratchet_tick.stops_ratcheted, 1);

    let held_ratcheted = db.get_assets_held().unwrap();
    assert!(held_ratcheted[0].protective_stop_px > initial_stop);
    assert!(held_ratcheted[0].unrealized_pnl_usdc > 50.0);

    // 6. Simulate flash drop below protective stop -> 1s Priority Monitor closes position & inserts STOP_OUT cooldown
    let active_stop = held_ratcheted[0].protective_stop_px;
    update_test_asset_px(&db, &sol_fresh, active_stop - 1.50);
    let breach_tick = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(breach_tick.stops_triggered_closed, 1);

    assert!(db.get_assets_held().unwrap().is_empty());
    let cooldowns = db.get_assets_in_cooldown().unwrap();
    assert_eq!(cooldowns.len(), 1);
    assert_eq!(cooldowns[0].symbol, "SOL-USDC");
    assert_eq!(cooldowns[0].cooldown_type, "STOP_OUT");
    assert!(cooldowns[0].remaining_seconds > 0);
}

#[test]
fn test_unified_single_cycle_and_manual_close() {
    let db = fresh_isolated_db();
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    let engine = ExecutionEngine::new(db.clone());

    let cycle = engine.run_single_cycle(true).unwrap();
    assert!(cycle.signals_evaluated >= 15);
    assert_eq!(cycle.snapshot.risk_tier, "extreme");
    assert!((cycle.snapshot.leverage_multiplier - 1.85).abs() < 1e-9);

    // Verify sleeve views are populated
    let s1_view = db.get_sleeve_s1_view().unwrap();
    let s2_view = db.get_sleeve_s2_view().unwrap();
    let s3_view = db.get_sleeve_s3_view().unwrap();
    assert!(!s1_view.is_empty());
    assert!(!s2_view.is_empty());
    assert!(!s3_view.is_empty());

    // If any working orders were placed during the cycle, test manual cancellation + default 3600s cooldown & close_all
    let ordered = db.get_assets_ordered().unwrap();
    if let Some(first_ord) = ordered.first() {
        let close_rep = engine
            .close_or_cancel_position(first_ord.position_id, "MANUAL_TEST_CANCEL", None)
            .unwrap();
        assert_eq!(close_rep.final_status, "CANCELLED");
        assert!(close_rep.released_reserved_usdc > 0.0);
        assert_eq!(close_rep.cooldown_inserted_sec, 3600);
        assert!(db.is_asset_in_cooldown(first_ord.asset_id).unwrap());
    }

    let closed_all = engine
        .close_all_active_positions("CLOSE_ALL_TEST", Some(1800))
        .unwrap();
    for rep in &closed_all {
        assert_eq!(rep.cooldown_inserted_sec, 1800);
    }
    assert!(db.get_assets_ordered().unwrap().is_empty());
    assert!(db.get_assets_held().unwrap().is_empty());
}

#[test]
fn test_take_profit_trigger_exit_and_live_equity_preservation() {
    let db = fresh_isolated_db();
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "1000.00", None)
        .unwrap();

    let engine = ExecutionEngine::new(db.clone());
    engine.watcher.run_3s_exchange_scan().unwrap();

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    db.update_asset_live_quote(sol.id, 180.00, 180.00).unwrap();
    let sol_fresh = db.get_asset_by_id(sol.id).unwrap().unwrap();

    let pos_id = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: sol_fresh,
            side: OrderSide::Long,
            requested_quantity: 5.0,
            reserved_margin_usdc: 450.0,
            portfolio_weight: 0.15,
            s1_weight_contrib: 0.10,
            s2_weight_contrib: 0.05,
            s3_weight_contrib: 0.0,
            protective_stop_px: 174.00,
            reason: "TP_TEST_ENTRY".to_string(),
            bypass_entry_interval: false,
        })
        .unwrap();

    // Promote WORKING -> OPEN via live quote touching the working limit order
    let ordered = db.get_assets_ordered().unwrap();
    let fill_px = ordered[0].limit_price;
    db.update_asset_live_quote(sol.id, fill_px, fill_px)
        .unwrap();
    let open_tick = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(open_tick.orders_promoted_to_open, 1);

    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    let tp_px = held[0].take_profit_px;
    assert!(tp_px > held[0].entry_price);
    assert!(held[0].take_profit_oid.is_some());
    assert!(held[0].protective_stop_oid.is_some());

    // Simulate rally hitting take_profit_px -> Priority Monitor exits at TP without STOP_OUT cooldown
    let sol_now = db.get_asset_by_id(sol.id).unwrap().unwrap();
    update_test_asset_px(&db, &sol_now, tp_px + 0.50);
    let tp_tick = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(tp_tick.stops_triggered_closed, 1);
    assert!(db.get_assets_held().unwrap().is_empty());

    let closed_pos = db.get_position_by_id(pos_id).unwrap().unwrap();
    assert_eq!(closed_pos.status, "CLOSED");
    assert_eq!(closed_pos.exit_reason.as_deref(), Some("TAKE_PROFIT_LONG"));
    assert!(closed_pos.realized_pnl_usdc > 0.0);

    let cooldowns = db.get_assets_in_cooldown().unwrap();
    assert!(cooldowns.iter().all(|c| c.cooldown_type != "STOP_OUT"));

    let cfg = db.load_config().unwrap();
    assert!((cfg.get_f64("live_exchange_equity_usdc", 0.0) - 1000.0).abs() < 1e-6);
}

fn make_signal_output(signals: Vec<SleeveSignalInput>) -> SignalCycleOutput {
    SignalCycleOutput {
        signals,
        breadth: MarketBreadthSnapshot::default(),
        risk_tier: RiskTier::Extreme,
        leverage_multiplier: 1.85,
        s1_gross_weight: 0.4,
        s2_gross_weight: 0.2,
        s3_gross_weight: 0.1,
        total_gross_weight: 0.7,
        total_net_weight: 0.3,
    }
}

fn make_signal_input(
    asset: &aegis_core::ExchangeAsset,
    target_weight: f64,
    current_weight: f64,
    action: &str,
) -> SleeveSignalInput {
    SleeveSignalInput {
        asset_id: asset.id,
        symbol: asset.symbol.clone(),
        coin: asset.coin.clone(),
        mark_price: asset.mark_price,
        day_volume_usdc: asset.day_volume_usdc,
        funding_rate_hourly: asset.funding_rate_hourly,
        annualized_vol: 0.50,
        market_beta: 1.0,
        s1_mom_score: target_weight.signum(),
        s1_carry_score: 0.1,
        s2_trend_alignment: if target_weight >= 0.0 { 1 } else { -1 },
        s2_vol_surge_ratio: 1.2,
        s3_funding_zscore: 0.0,
        s3_flush_atr_mult: 0.0,
        w_s1_carry_mom: target_weight * 0.6,
        w_s2_trend_breakout: target_weight * 0.4,
        w_s3_event_reversion: 0.0,
        raw_combined_weight: target_weight,
        target_weight,
        current_weight,
        delta_weight: target_weight - current_weight,
        event_trigger: None,
        rebalance_action: action.to_string(),
    }
}

fn seed_test_assets(db: &DatabaseManager) {
    let specs = [
        ("BTC", 0, 4, 1.0, 0.0001, 40.0, 95_000.0),
        ("ETH", 1, 3, 0.1, 0.001, 25.0, 3_400.0),
        ("SOL", 2, 2, 0.01, 0.01, 20.0, 184.25),
        ("AVAX", 3, 2, 0.01, 0.01, 20.0, 35.0),
        ("LINK", 4, 1, 0.001, 0.1, 20.0, 18.5),
        ("ARB", 5, 1, 0.0001, 0.1, 10.0, 0.85),
        ("SUI", 6, 1, 0.0001, 0.1, 10.0, 3.42),
    ];
    for (coin, asset_index, sz_decimals, tick_size, lot_step_size, max_leverage, px) in specs {
        db.upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: coin.to_string(),
            asset_index,
            sz_decimals,
            tick_size,
            lot_step_size,
            max_leverage,
            mark_price: px,
            mid_price: px,
            oracle_price: px,
            funding_rate_hourly: 0.0000125,
            day_volume_usdc: 25_000_000.0,
            open_interest_usdc: 10_000_000.0,
            prev_day_price: px * 0.98,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();
    }
}

#[test]
fn test_pre_cap_cutoff_isolation_and_illiquid_backoff_no_phantom_headroom() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    // Switch to TESTNET config (without private_key so DB transitions run synchronously offline)
    let cap_epoch = now_ms() - 1_200_000;
    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("risk_tier", "extreme", None).unwrap();
    db.set_config_value("live_exchange_equity_usdc", "931.72", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_usdc", "1000.0", None)
        .unwrap();
    db.set_config_value(
        "risk_capital_cap_updated_at_ms",
        &cap_epoch.to_string(),
        None,
    )
    .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "300", None)
        .unwrap();

    let cfg = db.load_config_or_default();
    assert!((engine.resolve_effective_base_equity_usdc(&cfg) - 931.72).abs() < 1e-6);

    // Insert 3 pre-cap legacy holdings totaling $3,100 (> 3.3x equity) opened BEFORE cap_epoch
    let pre_ts = cap_epoch - 3_600_000;
    for (sym, notional) in [
        ("ETH-USDC", 1800.0),
        ("AVAX-USDC", 700.0),
        ("LINK-USDC", 600.0),
    ] {
        let a = db.get_asset_by_symbol(sym).unwrap().unwrap();
        let qty = notional / a.mark_price;
        db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: a.id,
            coin: &a.coin,
            side: OrderSide::Short,
            entry_price: a.mark_price,
            mark_price: a.mark_price,
            quantity: qty,
            committed_notional_usdc: notional,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: -(notional / 931.72),
            protective_stop_px: a.mark_price * 1.05,
            take_profit_px: a.mark_price * 0.90,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(pre_ts),
        })
        .unwrap();
    }

    // 1. Pre-cap legacy positions must NOT trigger AUTO_DELEVERAGE_RISK_CAP liquidations
    let deleveraged = engine.enforce_leverage_and_position_caps().unwrap();
    assert!(deleveraged.is_empty());
    assert_eq!(db.get_assets_held().unwrap().len(), 3);

    // 2. R14-H1: Pre-cap legacy holdings MUST count toward gross leverage checks on new entries ($3,100 > $1,723 cap -> blocked!)
    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();
    let arb_sig = make_signal_output(vec![make_signal_input(&arb, 0.20, 0.0, ACTION_OPEN_LONG)]);
    let reb_blocked_by_legacy = engine.execute_signal_rebalance(&arb_sig, 931.72).unwrap();
    assert_eq!(reb_blocked_by_legacy.working_orders_placed, 0);
    assert!(db.get_assets_ordered().unwrap().is_empty());

    // Raise equity/cap to $2,500 ($4,625 gross ceiling) while preserving the original cap_epoch
    db.set_config_value("live_exchange_equity_usdc", "2500.0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_usdc", "2500.0", None)
        .unwrap();
    db.set_config_value(
        "risk_capital_cap_updated_at_ms",
        &cap_epoch.to_string(),
        None,
    )
    .unwrap();
    let reb = engine.execute_signal_rebalance(&arb_sig, 2500.0).unwrap();
    assert_eq!(reb.working_orders_placed, 1);
    assert_eq!(db.get_assets_ordered().unwrap().len(), 1);
    engine
        .close_or_cancel_position(reb.placed_position_ids[0], "TEST_CLEANUP", Some(0))
        .unwrap();

    // 3. Active-epoch illiquid-backoff holding must NOT create phantom leverage headroom ($3,100 + $1,842.50 = $4,942.50 > $4,763.75)
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let (sol_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: &sol.coin,
            side: OrderSide::Long,
            entry_price: sol.mark_price,
            mark_price: sol.mark_price,
            quantity: 10.0,
            committed_notional_usdc: 1842.50,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.74,
            protective_stop_px: sol.mark_price * 0.96,
            take_profit_px: sol.mark_price * 1.08,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 600_000),
        })
        .unwrap();
    engine
        .illiquid_close_backoff_ms
        .lock()
        .insert(sol_pid, now_ms());

    let sui = db.get_asset_by_symbol("SUI-USDC").unwrap().unwrap();
    let sui_sig = make_signal_output(vec![make_signal_input(&sui, 0.20, 0.0, ACTION_OPEN_LONG)]);
    let blocked_reb = engine.execute_signal_rebalance(&sui_sig, 2500.0).unwrap();
    assert_eq!(blocked_reb.working_orders_placed, 0);
    assert!(db.get_assets_ordered().unwrap().is_empty());

    // 4. Young post-cap position protection: older over-cap active holding is protected against collateral liquidation when backed off,
    //    requires 2 confirm ticks once cleared, and closes ONLY post-cap SOL while protecting young SUI and pre-cap legacy rows.
    let (sui_young_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sui.id,
            coin: &sui.coin,
            side: OrderSide::Long,
            entry_price: sui.mark_price,
            mark_price: sui.mark_price,
            quantity: 250.0,
            committed_notional_usdc: 855.0,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.34,
            protective_stop_px: sui.mark_price * 0.96,
            take_profit_px: sui.mark_price * 1.08,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 20_000),
        })
        .unwrap();
    // While older SOL is backed off, neither tick liquidates young SUI
    assert!(engine
        .enforce_leverage_and_position_caps()
        .unwrap()
        .is_empty());
    assert!(engine
        .enforce_leverage_and_position_caps()
        .unwrap()
        .is_empty());
    assert_eq!(
        db.get_position_by_id(sui_young_pid)
            .unwrap()
            .unwrap()
            .status,
        "OPEN"
    );

    // Clear backoff: with fresh confirm window -> set confirm_ticks=1 OR invoke confirm step cleanly
    engine.illiquid_close_backoff_ms.lock().clear();
    db.set_config_value("auto_deleverage_confirm_ticks", "1", None)
        .unwrap();
    let cap_reports = engine.enforce_leverage_and_position_caps().unwrap();
    assert!(cap_reports
        .iter()
        .any(|r| r.position_id == sol_pid && r.final_status == "CLOSED"));
    assert_eq!(
        db.get_position_by_id(sui_young_pid)
            .unwrap()
            .unwrap()
            .status,
        "OPEN",
        "Young post-cap position should be protected when older active candidate exists"
    );
}

#[test]
fn test_symbol_scoped_action_flip_cooldown_bypass() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    seed_test_assets(&db);

    db.set_config_value("min_entry_interval_sec", "150", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "300", None)
        .unwrap();
    db.set_config_value("profit_gate_signal_exits", "false", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();
    db.record_position_entry_timestamp_ms(now_ms() - 10_000)
        .unwrap();

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();

    // Case A: Non-held / unflipped ACTION_FLIP signal does NOT allow ARB-USDC to bypass 150s entry cooldown
    let sig_unflipped = make_signal_output(vec![
        make_signal_input(&sol, -0.20, 0.0, ACTION_FLIP),
        make_signal_input(&arb, 0.28, 0.0, ACTION_OPEN_LONG),
    ]);
    let reb_a = engine
        .execute_signal_rebalance(&sig_unflipped, 10_000.0)
        .unwrap();
    assert_eq!(reb_a.working_orders_placed, 0);

    // Directly verify bypass_entry_interval: false is blocked
    let err = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: arb.clone(),
            side: OrderSide::Long,
            requested_quantity: 100.0,
            reserved_margin_usdc: 50.0,
            portfolio_weight: 0.1,
            s1_weight_contrib: 0.1,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: arb.mark_price * 0.95,
            reason: "TEST_COOLDOWN".to_string(),
            bypass_entry_interval: false,
        })
        .unwrap_err();
    assert!(err.to_string().contains("MIN_ENTRY_INTERVAL_COOLDOWN"));

    // Case B: Confirmed ACTION_FLIP on held SOL-USDC with target_weight=0.0 still does NOT let ARB-USDC bypass cooldown
    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: sol.id,
        coin: &sol.coin,
        side: OrderSide::Long,
        entry_price: sol.mark_price,
        mark_price: sol.mark_price,
        quantity: 5.0,
        committed_notional_usdc: 5.0 * sol.mark_price,
        unrealized_pnl_usdc: 0.0,
        portfolio_weight: 0.10,
        protective_stop_px: sol.mark_price * 0.95,
        take_profit_px: sol.mark_price * 1.10,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(now_ms() - 600_000),
    })
    .unwrap();
    let sig_flip_zero = make_signal_output(vec![
        make_signal_input(&sol, 0.0, 0.10, ACTION_FLIP),
        make_signal_input(&arb, 0.28, 0.0, ACTION_OPEN_LONG),
    ]);
    let reb_zero = engine
        .execute_signal_rebalance(&sig_flip_zero, 10_000.0)
        .unwrap();
    assert_eq!(reb_zero.positions_closed, 1);
    assert_eq!(
        reb_zero.working_orders_placed, 0,
        "Unrelated ARB-USDC must stay blocked by min_entry_interval_sec even when SOL flipped"
    );

    // Case C: Confirmed ACTION_FLIP on held SOL-USDC bypasses cooldown strictly for SOL-USDC
    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: sol.id,
        coin: &sol.coin,
        side: OrderSide::Long,
        entry_price: sol.mark_price,
        mark_price: sol.mark_price,
        quantity: 5.0,
        committed_notional_usdc: 5.0 * sol.mark_price,
        unrealized_pnl_usdc: 0.0,
        portfolio_weight: 0.10,
        protective_stop_px: sol.mark_price * 0.95,
        take_profit_px: sol.mark_price * 1.10,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(now_ms() - 600_000),
    })
    .unwrap();

    let sig_flip = make_signal_output(vec![
        make_signal_input(&sol, -0.20, 0.10, ACTION_FLIP),
        make_signal_input(&arb, 0.28, 0.0, ACTION_OPEN_LONG),
    ]);
    let reb_b = engine
        .execute_signal_rebalance(&sig_flip, 10_000.0)
        .unwrap();
    assert_eq!(reb_b.positions_closed, 1);
    assert_eq!(reb_b.working_orders_placed, 1);
    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 1);
    assert_eq!(ordered[0].symbol, "SOL-USDC");
    assert_eq!(ordered[0].side, "SHORT");
}

#[test]
fn test_live_close_fee_no_double_counting_and_http_error_safety() {
    let fail_ch = Arc::new(Mutex::new(true));
    let fail_ch_srv = fail_ch.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "clearinghouseState" {
                if *fail_ch_srv.lock() {
                    return (500, json!({ "error": "upstream timeout" }));
                }
                return (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "931.72" },
                        "assetPositions": [{
                            "position": {
                                "coin": "ARB",
                                "szi": "646.1",
                                "entryPx": "0.12697",
                                "positionValue": "81.90"
                            }
                        }]
                    }),
                );
            }
            if typ == "l2Book" {
                return (
                    200,
                    json!({
                        "levels": [
                            [{"px": "0.12676", "sz": "1000.0"}],
                            [{"px": "0.12680", "sz": "1000.0"}]
                        ]
                    }),
                );
            }
            return (200, json!([]));
        }
        // /exchange
        let action_type = body
            .pointer("/action/type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if action_type == "order" {
            return (
                200,
                json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": [{
                                "filled": {
                                    "oid": 998877u64,
                                    "totalSz": "646.1",
                                    "avgPx": "0.12676",
                                    "fee": "0.0369"
                                }
                            }]
                        }
                    }
                }),
            );
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    let base_url = server.base_url();
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

    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();
    db.update_asset_live_quote(arb.id, 0.12697, 0.12697)
        .unwrap();
    let pos_id = db
        .create_working_order(&aegis_db::WorkingOrderInput {
            asset_id: arb.id,
            client_order_id: "v2-arb-long-1".to_string(),
            side: OrderSide::Long,
            limit_price: 0.12697,
            requested_quantity: 646.1,
            reserved_margin_usdc: 44.35,
            portfolio_weight: 0.088,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.038,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.12467,
            take_profit_px: None,
            exchange_order_id: Some("hl-111".to_string()),
            reason: "TEST_ENTRY".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        pos_id,
        0.12697,
        646.1,
        0.0123,
        0.12467,
        None,
        Some("hl-111"),
        "TEST_FILL",
    )
    .unwrap();

    // 1. HTTP 500 on clearinghouseState -> must stay OPEN (never mark DB CLOSED blindly)
    let err_rep = engine
        .close_or_cancel_position(pos_id, "MANUAL_CLOSE", Some(300))
        .unwrap();
    assert_eq!(err_rep.final_status, "OPEN");
    assert_eq!(
        db.get_position_by_id(pos_id).unwrap().unwrap().status,
        "OPEN"
    );
    assert!(!db.is_asset_in_cooldown(arb.id).unwrap());

    // 2. Healthy HL fill -> close fee NOT double-counted (0.0123 open + 0.0369 close = 0.0492)
    *fail_ch.lock() = false;
    let ok_rep = engine
        .close_or_cancel_position(pos_id, "AUTO_DELEVERAGE_RISK_CAP", Some(900))
        .unwrap();
    assert_eq!(ok_rep.final_status, "CLOSED");
    assert!((ok_rep.exit_price.unwrap() - 0.12676).abs() < 1e-6);

    let closed_pos = db.get_position_by_id(pos_id).unwrap().unwrap();
    assert_eq!(closed_pos.status, "CLOSED");
    assert_eq!(closed_pos.client_order_id, "hl-closed-arb-998877");
    assert_eq!(closed_pos.exchange_order_id.as_deref(), Some("hl-998877"));
    assert!((closed_pos.fees_paid_usdc - 0.0492).abs() < 1e-4);
}

#[test]
fn test_duplicate_sl_cleanup_cloid_resolution_and_empty_snapshot() {
    let empty_orders = Arc::new(Mutex::new(false));
    let empty_orders_srv = empty_orders.clone();
    let cancelled_oids = Arc::new(Mutex::new(Vec::<u64>::new()));
    let cancelled_oids_srv = cancelled_oids.clone();
    let submitted_orders = Arc::new(Mutex::new(Vec::<Value>::new()));
    let submitted_orders_srv = submitted_orders.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "frontendOpenOrders" || typ == "openOrders" {
                if *empty_orders_srv.lock() {
                    return (200, json!([]));
                }
                return (
                    200,
                    json!([
                        {
                            "coin": "SOL",
                            "oid": 11111u64,
                            "cloid": "0xaaaa",
                            "isTrigger": true,
                            "orderType": "Stop Limit",
                            "limitPx": "160.0",
                            "triggerPx": "174.0",
                            "sz": "5.0"
                        },
                        {
                            "coin": "SOL",
                            "oid": 22222u64,
                            "cloid": "0xbbbb",
                            "isTrigger": true,
                            "orderType": "Stop Limit",
                            "limitPx": "160.0",
                            "triggerPx": "174.0",
                            "sz": "5.0"
                        },
                        {
                            "coin": "SOL",
                            "oid": 33333u64,
                            "cloid": "0xcccc",
                            "isTrigger": true,
                            "orderType": "Take Profit Limit",
                            "limitPx": "202.0",
                            "triggerPx": "196.0",
                            "sz": "5.0"
                        }
                    ]),
                );
            }
            return (200, json!([]));
        }
        let action_type = body
            .pointer("/action/type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if action_type == "cancel" {
            if let Some(arr) = body.pointer("/action/cancels").and_then(Value::as_array) {
                for c in arr {
                    if let Some(o) = c.get("o").and_then(Value::as_u64) {
                        cancelled_oids_srv.lock().push(o);
                    }
                }
            }
            return (200, json!({ "status": "ok" }));
        }
        if action_type == "order" {
            let mut statuses = Vec::new();
            if let Some(arr) = body.pointer("/action/orders").and_then(Value::as_array) {
                for ord in arr {
                    submitted_orders_srv.lock().push(ord.clone());
                    let oid = 44000 + submitted_orders_srv.lock().len() as u64;
                    statuses.push(json!({ "resting": { "oid": oid } }));
                }
            }
            return (
                200,
                json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": statuses
                        }
                    }
                }),
            );
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    let base_url = server.base_url();
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

    let cfg = db.load_config_or_default();
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let (pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: &sol.coin,
            side: OrderSide::Long,
            entry_price: 180.0,
            mark_price: 180.0,
            quantity: 5.0,
            committed_notional_usdc: 900.0,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.2,
            protective_stop_px: 174.0,
            take_profit_px: 196.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms()),
        })
        .unwrap();

    // 1. Duplicate SL cleanup + cloid resolution (`0xaaaa` -> `hl-11111`, duplicate `22222` cancelled)
    let (_s_px, sl_oid, _t_px, tp_oid) = engine
        .chaser
        .ensure_exchange_tpsl_orders(
            pid,
            &sol,
            OrderSide::Long,
            5.0,
            180.0,
            174.0,
            196.0,
            Some("0xaaaa"),
            Some("hl-33333"),
            false,
            false,
            None,
            &cfg,
        )
        .unwrap();
    assert_eq!(sl_oid.as_deref(), Some("hl-11111"));
    assert_eq!(tp_oid.as_deref(), Some("hl-33333"));
    assert_eq!(*cancelled_oids.lock(), vec![22222u64]);
    assert!(submitted_orders.lock().is_empty());

    // 2. Empty frontendOpenOrders snapshot (0 resting orders) -> must NOT assume existing OIDs still rest
    *empty_orders.lock() = true;
    cancelled_oids.lock().clear();
    let (_s2, sl_oid2, _t2, tp_oid2) = engine
        .chaser
        .ensure_exchange_tpsl_orders(
            pid,
            &sol,
            OrderSide::Long,
            5.0,
            180.0,
            174.0,
            196.0,
            Some("hl-11111"),
            Some("hl-33333"),
            false,
            false,
            None,
            &cfg,
        )
        .unwrap();
    assert_eq!(sl_oid2.as_deref(), Some("hl-44001"));
    assert_eq!(tp_oid2.as_deref(), Some("hl-44002"));
    assert_eq!(submitted_orders.lock().len(), 2);
}

#[test]
fn test_reconcile_user_fills_flip_funding_and_opened_at_ms() {
    let open_ts = 1_700_000_000_000i64;
    let funding_ts = 1_700_000_050_000i64;
    let flip_ts = 1_700_000_100_000i64;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "950.50" },
                        "assetPositions": [{
                            "position": {
                                "coin": "ETH",
                                "szi": "2.0",
                                "entryPx": "3000.0",
                                "positionValue": "6100.0",
                                "unrealizedPnl": "100.0",
                                "cumFunding": { "sinceOpen": "1.25" }
                            }
                        }]
                    }),
                ),
                "spotClearinghouseState" => (
                    200,
                    json!({
                        "balances": [{ "coin": "USDC", "total": "5000.0", "hold": "0.0" }]
                    }),
                ),
                "userFills" | "userFillsByTime" => (
                    200,
                    json!([
                        {
                            "coin": "ETH",
                            "side": "B",
                            "px": "3000.0",
                            "sz": "2.0",
                            "fee": "0.90",
                            "closedPnl": "0.0",
                            "dir": "Open Long",
                            "time": open_ts,
                            "oid": 1001u64,
                            "tid": 1u64,
                            "startPosition": "0.0"
                        },
                        {
                            "coin": "SOL",
                            "side": "B",
                            "px": "178.0",
                            "sz": "10.0",
                            "fee": "0.50",
                            "closedPnl": "0.0",
                            "dir": "Open Long",
                            "time": open_ts,
                            "oid": 2001u64,
                            "tid": 2u64,
                            "startPosition": "0.0"
                        },
                        {
                            "coin": "SOL",
                            "side": "A",
                            "px": "180.0",
                            "sz": "15.0",
                            "fee": "0.75",
                            "closedPnl": "20.0",
                            "dir": "Long > Short",
                            "time": flip_ts,
                            "oid": 777u64,
                            "tid": 3u64,
                            "startPosition": "10.0"
                        }
                    ]),
                ),
                "userFunding" => (
                    200,
                    json!([
                        {
                            "time": funding_ts,
                            "hash": "0xabc",
                            "delta": {
                                "type": "funding",
                                "coin": "SOL",
                                "usdc": "2.50",
                                "szi": "10.0",
                                "fundingRate": "0.0001"
                            }
                        }
                    ]),
                ),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    let base_url = server.base_url();
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

    engine.aux_sync.sync_startup_before_reconcile();
    let rep = engine.reconciler.run_cutover_reconciliation(false).unwrap();
    assert!((rep.exchange_equity_usdc - 950.50).abs() < 1e-6);

    let cfg = db.load_config_or_default();
    assert!((cfg.get_f64("live_exchange_equity_usdc", 0.0) - 950.50).abs() < 1e-6);

    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].symbol, "ETH-USDC");
    assert_eq!(held[0].opened_at_ms, Some(open_ts));
    assert!((held[0].funding_accrued_usdc - (-1.25)).abs() < 1e-6);

    let closed = db.list_closed_positions(20).unwrap();
    let sol_closed = closed
        .iter()
        .find(|p| p.client_order_id == "hl-closed-sol-777")
        .expect("reconciled SOL flip trade");
    assert_eq!(sol_closed.side, "LONG");
    assert!((sol_closed.filled_quantity - 10.0).abs() < 1e-6);
    assert!((sol_closed.fees_paid_usdc - 1.0).abs() < 1e-4);
    assert!((sol_closed.funding_accrued_usdc - 2.50).abs() < 1e-4);
    assert!((sol_closed.realized_pnl_usdc - 21.50).abs() < 1e-4);
}

#[test]
fn test_execute_signal_rebalance_enforces_min_position_hold_sec() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    seed_test_assets(&db);

    db.set_config_value("min_position_hold_sec", "300", None)
        .unwrap();
    db.set_config_value("profit_gate_signal_exits", "false", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();
    let avax = db.get_asset_by_symbol("AVAX-USDC").unwrap().unwrap();
    let link = db.get_asset_by_symbol("LINK-USDC").unwrap().unwrap();

    let young_opened_ms = now_ms() - 30_000; // 30s < 300s
    let (sol_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: &sol.coin,
            side: OrderSide::Long,
            entry_price: sol.mark_price,
            mark_price: sol.mark_price,
            quantity: 5.0,
            committed_notional_usdc: 5.0 * sol.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.10,
            protective_stop_px: sol.mark_price * 0.95,
            take_profit_px: sol.mark_price * 1.10,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(young_opened_ms),
        })
        .unwrap();
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth.id,
            coin: &eth.coin,
            side: OrderSide::Long,
            entry_price: eth.mark_price,
            mark_price: eth.mark_price,
            quantity: 0.5,
            committed_notional_usdc: 0.5 * eth.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.15,
            protective_stop_px: eth.mark_price * 0.95,
            take_profit_px: eth.mark_price * 1.10,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(young_opened_ms),
        })
        .unwrap();
    let (arb_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: arb.id,
            coin: &arb.coin,
            side: OrderSide::Long,
            entry_price: arb.mark_price,
            mark_price: arb.mark_price,
            quantity: 100.0,
            committed_notional_usdc: 100.0 * arb.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.05,
            protective_stop_px: arb.mark_price * 0.95,
            take_profit_px: arb.mark_price * 1.10,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(young_opened_ms),
        })
        .unwrap();

    // 1. Both ACTION_CLOSE and ACTION_FLIP must be skipped while held < min_position_hold_sec
    let sig_blocked = make_signal_output(vec![
        make_signal_input(&sol, 0.0, 0.10, ACTION_CLOSE),
        make_signal_input(&eth, -0.20, 0.15, ACTION_FLIP),
    ]);
    let reb_blocked = engine
        .execute_signal_rebalance(&sig_blocked, 10_000.0)
        .unwrap();
    assert_eq!(reb_blocked.positions_closed, 0);
    assert_eq!(reb_blocked.working_orders_placed, 0);
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "OPEN"
    );
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "OPEN"
    );
    assert!(db.get_assets_ordered().unwrap().is_empty());

    // 2. Manual close_or_cancel_position remains unrestricted even inside the hold window
    let manual_rep = engine
        .close_or_cancel_position(arb_pid, "MANUAL_OVERRIDE_TEST", Some(0))
        .unwrap();
    assert_eq!(manual_rep.final_status, "CLOSED");

    // 3. Mature holdings (>= 300s) ARE closed by ACTION_CLOSE and flipped by ACTION_FLIP
    let mature_opened_ms = now_ms() - 360_000;
    let (avax_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: avax.id,
            coin: &avax.coin,
            side: OrderSide::Long,
            entry_price: avax.mark_price,
            mark_price: avax.mark_price,
            quantity: 20.0,
            committed_notional_usdc: 20.0 * avax.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.10,
            protective_stop_px: avax.mark_price * 0.95,
            take_profit_px: avax.mark_price * 1.10,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(mature_opened_ms),
        })
        .unwrap();
    let (link_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: link.id,
            coin: &link.coin,
            side: OrderSide::Long,
            entry_price: link.mark_price,
            mark_price: link.mark_price,
            quantity: 40.0,
            committed_notional_usdc: 40.0 * link.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.10,
            protective_stop_px: link.mark_price * 0.95,
            take_profit_px: link.mark_price * 1.10,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(mature_opened_ms),
        })
        .unwrap();

    let sig_allowed = make_signal_output(vec![
        make_signal_input(&avax, 0.0, 0.10, ACTION_CLOSE),
        make_signal_input(&link, -0.20, 0.10, ACTION_FLIP),
    ]);
    let reb_allowed = engine
        .execute_signal_rebalance(&sig_allowed, 10_000.0)
        .unwrap();
    assert_eq!(reb_allowed.positions_closed, 2);
    assert_eq!(reb_allowed.working_orders_placed, 1);
    assert_eq!(
        db.get_position_by_id(avax_pid).unwrap().unwrap().status,
        "CLOSED"
    );
    assert_eq!(
        db.get_position_by_id(link_pid).unwrap().unwrap().status,
        "CLOSED"
    );
    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 1);
    assert_eq!(ordered[0].symbol, "LINK-USDC");
    assert_eq!(ordered[0].side, "SHORT");

    // Young SOL/ETH remain OPEN throughout
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "OPEN"
    );
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "OPEN"
    );

    // 4. Setting min_position_hold_sec = 0 permits immediate close/flip on young positions
    db.set_config_value("min_position_hold_sec", "0", None)
        .unwrap();
    let reb_zero_hold = engine
        .execute_signal_rebalance(&sig_blocked, 10_000.0)
        .unwrap();
    assert_eq!(reb_zero_hold.positions_closed, 2);
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "CLOSED"
    );
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "CLOSED"
    );
}

#[test]
fn test_order_chaser_partial_fill_grace_chases_remaining_and_finalizes() {
    use std::collections::HashMap;

    #[derive(Clone, Debug)]
    struct WireOrderRecord {
        coin: String,
        tif: Option<String>,
        is_trigger: bool,
        sz: f64,
    }

    let positions_state: Arc<Mutex<HashMap<String, (f64, f64)>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let positions_srv = positions_state.clone();
    let submitted_wires: Arc<Mutex<Vec<WireOrderRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_srv = submitted_wires.clone();
    let cancelled_oids: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let cancelled_srv = cancelled_oids.clone();
    let ioc_should_fill: Arc<Mutex<bool>> = Arc::new(Mutex::new(true));
    let ioc_srv = ioc_should_fill.clone();
    let l2_mids: Arc<Mutex<HashMap<String, f64>>> = Arc::new(Mutex::new(
        [
            ("SOL".to_string(), 184.25),
            ("AVAX".to_string(), 35.0),
            ("LINK".to_string(), 18.5),
            ("ARB".to_string(), 0.85),
        ]
        .into_iter()
        .collect(),
    ));
    let l2_mids_srv = l2_mids.clone();
    let next_oid: Arc<Mutex<u64>> = Arc::new(Mutex::new(50_000));

    let coin_by_idx: HashMap<u64, &'static str> = [
        (0, "BTC"),
        (1, "ETH"),
        (2, "SOL"),
        (3, "AVAX"),
        (4, "LINK"),
        (5, "ARB"),
        (6, "SUI"),
    ]
    .into_iter()
    .collect();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "clearinghouseState" {
                let guard = positions_srv.lock();
                let asset_positions: Vec<Value> = guard
                    .iter()
                    .filter(|(_, (szi, _))| szi.abs() > 1e-9)
                    .map(|(coin, (szi, entry_px))| {
                        json!({
                            "position": {
                                "coin": coin,
                                "szi": format!("{szi}"),
                                "entryPx": format!("{entry_px}"),
                                "positionValue": format!("{}", szi.abs() * entry_px)
                            }
                        })
                    })
                    .collect();
                return (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "10000.0" },
                        "assetPositions": asset_positions
                    }),
                );
            }
            if typ == "l2Book" {
                let coin = body.get("coin").and_then(Value::as_str).unwrap_or("SOL");
                let px = l2_mids_srv.lock().get(coin).copied().unwrap_or(100.0);
                return (
                    200,
                    json!({
                        "levels": [
                            [{"px": format!("{:.6}", px * 0.9999), "sz": "1000.0"}],
                            [{"px": format!("{:.6}", px * 1.0001), "sz": "1000.0"}]
                        ]
                    }),
                );
            }
            return (200, json!([]));
        }

        let action_type = body
            .pointer("/action/type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if action_type == "cancel" {
            if let Some(arr) = body.pointer("/action/cancels").and_then(Value::as_array) {
                for c in arr {
                    if let Some(o) = c.get("o").and_then(Value::as_u64) {
                        cancelled_srv.lock().push(o);
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
            let ords = body
                .pointer("/action/orders")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for ord in ords {
                let a_idx = ord.get("a").and_then(Value::as_u64).unwrap_or(0);
                let coin = coin_by_idx
                    .get(&a_idx)
                    .copied()
                    .unwrap_or("UNK")
                    .to_string();
                let sz_f: f64 = ord
                    .get("s")
                    .and_then(Value::as_str)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let px_str = ord.get("p").and_then(Value::as_str).unwrap_or("0");
                let is_trigger = ord.pointer("/t/trigger").is_some();
                let tif = ord
                    .pointer("/t/limit/tif")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                submitted_srv.lock().push(WireOrderRecord {
                    coin,
                    tif: tif.clone(),
                    is_trigger,
                    sz: sz_f,
                });
                let mut oid_guard = next_oid.lock();
                *oid_guard += 1;
                let oid = *oid_guard;

                if !is_trigger && tif.as_deref() == Some("Ioc") {
                    if *ioc_srv.lock() {
                        statuses.push(json!({
                            "filled": {
                                "oid": oid,
                                "totalSz": format!("{sz_f}"),
                                "avgPx": px_str
                            }
                        }));
                    } else {
                        statuses.push(json!({ "error": "Order could not immediately match" }));
                    }
                } else {
                    statuses.push(json!({ "resting": { "oid": oid } }));
                }
            }

            return (
                200,
                json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": {
                            "statuses": statuses
                        }
                    }
                }),
            );
        }

        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    let base_url = server.base_url();
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
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("max_open_positions", "10", None)
        .unwrap();
    db.set_config_value("max_alo_chase_ticks", "2", None)
        .unwrap();

    // Case A: SOL-USDC 30% partial fill stays WORKING (< max_chases & < 45s), chases rem_qty=7.0,
    // and promotes to OPEN once >= 85% (8.6) with remaining OID cancelled and SL/TP sized to 8.6.
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let sol_pid = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: sol.clone(),
            side: OrderSide::Long,
            requested_quantity: 10.0,
            reserved_margin_usdc: 1842.4,
            portfolio_weight: 0.18,
            s1_weight_contrib: 0.10,
            s2_weight_contrib: 0.08,
            s3_weight_contrib: 0.0,
            protective_stop_px: 176.80,
            reason: "PARTIAL_FILL_TEST_SOL".to_string(),
            bypass_entry_interval: true,
        })
        .unwrap();

    // 30% fill on exchange while resting (< 4 bps drift) -> MUST stay WORKING and NOT cancel OID 50001
    positions_state
        .lock()
        .insert("SOL".to_string(), (3.0, 184.24));
    let ord_sol_1 = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == sol_pid)
        .unwrap();
    assert_eq!(ord_sol_1.exchange_order_id.as_deref(), Some("hl-50001"));
    let step_resting = engine.chaser.advance_working_order(&ord_sol_1).unwrap();
    assert!(matches!(step_resting, ChaseStepOutcome::Resting { .. }));
    assert!(cancelled_oids.lock().is_empty());
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "WORKING"
    );

    // Mid drifts > 4 bps -> cancels 50001 and chases rem_qty=7.0 ONLY
    l2_mids.lock().insert("SOL".to_string(), 185.20);
    db.update_asset_live_quote(sol.id, 185.20, 185.20).unwrap();
    let ord_sol_2 = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == sol_pid)
        .unwrap();
    let step_chased = engine.chaser.advance_working_order(&ord_sol_2).unwrap();
    assert!(matches!(
        step_chased,
        ChaseStepOutcome::Chased { chase_count: 1, .. }
    ));
    assert_eq!(*cancelled_oids.lock(), vec![50_001]);
    let sol_chase_wire = submitted_wires
        .lock()
        .iter()
        .rfind(|w| w.coin == "SOL" && !w.is_trigger)
        .cloned()
        .unwrap();
    assert_eq!(sol_chase_wire.tif.as_deref(), Some("Alo"));
    assert!((sol_chase_wire.sz - 7.0).abs() < 1e-6);

    // Additional fill brings total to 8.6 (86% >= 85%) -> cancels resting 50002 & promotes to OPEN at 8.6
    positions_state
        .lock()
        .insert("SOL".to_string(), (8.6, 184.89));
    let ord_sol_3 = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == sol_pid)
        .unwrap();
    assert_eq!(ord_sol_3.exchange_order_id.as_deref(), Some("hl-50002"));
    let step_open = engine.chaser.advance_working_order(&ord_sol_3).unwrap();
    match step_open {
        ChaseStepOutcome::FilledOpen {
            filled_quantity, ..
        } => assert!((filled_quantity - 8.6).abs() < 1e-6),
        other => panic!("Expected FilledOpen at 86%, got {other:?}"),
    }
    assert!(cancelled_oids.lock().contains(&50_002));
    let sol_pos = db.get_position_by_id(sol_pid).unwrap().unwrap();
    assert_eq!(sol_pos.status, "OPEN");
    assert!((sol_pos.filled_quantity - 8.6).abs() < 1e-6);
    let sol_triggers: Vec<f64> = submitted_wires
        .lock()
        .iter()
        .filter(|w| w.coin == "SOL" && w.is_trigger)
        .map(|w| w.sz)
        .collect();
    assert_eq!(sol_triggers.len(), 2);
    assert!(sol_triggers.iter().all(|&sz| (sz - 8.6).abs() < 1e-6));

    // Case B1: CROSSING_IOC escalation with 30% partial fill -> submits IOC for rem_qty=7.0 ONLY & promotes 10.0
    db.set_config_value("max_alo_chase_drift_bps", "300.0", None)
        .unwrap();
    db.set_config_value("max_alo_oracle_drift_bps", "300.0", None)
        .unwrap();
    let avax = db.get_asset_by_symbol("AVAX-USDC").unwrap().unwrap();
    let avax_pid = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: avax.clone(),
            side: OrderSide::Long,
            requested_quantity: 10.0,
            reserved_margin_usdc: 350.0,
            portfolio_weight: 0.05,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 33.5,
            reason: "IOC_PARTIAL_FILL_TEST".to_string(),
            bypass_entry_interval: true,
        })
        .unwrap();
    positions_state
        .lock()
        .insert("AVAX".to_string(), (3.0, 35.0));
    for (idx, mid_px) in [35.20, 35.40].into_iter().enumerate() {
        l2_mids.lock().insert("AVAX".to_string(), mid_px);
        db.update_asset_live_quote(avax.id, mid_px, mid_px).unwrap();
        let ord = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == avax_pid)
            .unwrap();
        let res = engine.chaser.advance_working_order(&ord).unwrap();
        assert!(matches!(
            res,
            ChaseStepOutcome::Chased { chase_count, .. } if chase_count == (idx as u32 + 1)
        ));
    }
    let ord_avax_ioc = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == avax_pid)
        .unwrap();
    assert_eq!(ord_avax_ioc.chase_count, 2);
    let avax_open = engine.chaser.advance_working_order(&ord_avax_ioc).unwrap();
    match avax_open {
        ChaseStepOutcome::FilledOpen {
            filled_quantity, ..
        } => assert!((filled_quantity - 10.0).abs() < 1e-6),
        other => panic!("Expected FilledOpen after IOC rem_qty fill, got {other:?}"),
    }
    let avax_ioc_wire = submitted_wires
        .lock()
        .iter()
        .find(|w| w.coin == "AVAX" && w.tif.as_deref() == Some("Ioc"))
        .cloned()
        .unwrap();
    assert!((avax_ioc_wire.sz - 7.0).abs() < 1e-6);

    // Case B2: CROSSING_IOC unfilled with 30% existing exchange fill -> STILL promotes 3.0 to OPEN with SL/TP
    *ioc_should_fill.lock() = false;
    let link = db.get_asset_by_symbol("LINK-USDC").unwrap().unwrap();
    let link_pid = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: link.clone(),
            side: OrderSide::Long,
            requested_quantity: 10.0,
            reserved_margin_usdc: 185.0,
            portfolio_weight: 0.05,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 17.5,
            reason: "IOC_UNFILLED_PARTIAL_PROMOTE".to_string(),
            bypass_entry_interval: true,
        })
        .unwrap();
    positions_state
        .lock()
        .insert("LINK".to_string(), (3.0, 18.5));
    for mid_px in [18.70, 18.90] {
        l2_mids.lock().insert("LINK".to_string(), mid_px);
        db.update_asset_live_quote(link.id, mid_px, mid_px).unwrap();
        let ord = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == link_pid)
            .unwrap();
        let _ = engine.chaser.advance_working_order(&ord).unwrap();
    }
    let ord_link_ioc = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == link_pid)
        .unwrap();
    let link_open = engine.chaser.advance_working_order(&ord_link_ioc).unwrap();
    match link_open {
        ChaseStepOutcome::FilledOpen {
            filled_quantity, ..
        } => assert!((filled_quantity - 3.0).abs() < 1e-6),
        other => {
            panic!("Expected FilledOpen preserving 3.0 partial fill on IOC non-fill, got {other:?}")
        }
    }
    let link_triggers: Vec<f64> = submitted_wires
        .lock()
        .iter()
        .filter(|w| w.coin == "LINK" && w.is_trigger)
        .map(|w| w.sz)
        .collect();
    assert_eq!(link_triggers.len(), 2);
    assert!(link_triggers.iter().all(|&sz| (sz - 3.0).abs() < 1e-6));

    // Case C: Grace timeout (>= 45_000 ms) with 30% partial fill cancels resting OID and promotes 30.0 to OPEN
    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();
    let arb_pid = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: arb.clone(),
            side: OrderSide::Long,
            requested_quantity: 100.0,
            reserved_margin_usdc: 85.0,
            portfolio_weight: 0.05,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.80,
            reason: "EXPIRED_PARTIAL_PROMOTE".to_string(),
            bypass_entry_interval: true,
        })
        .unwrap();
    positions_state
        .lock()
        .insert("ARB".to_string(), (30.0, 0.85));
    let mut ord_arb_expired = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == arb_pid)
        .unwrap();
    let arb_oid_num: u64 = ord_arb_expired
        .exchange_order_id
        .as_deref()
        .and_then(|s| s.strip_prefix("hl-"))
        .and_then(|s| s.parse().ok())
        .unwrap();
    ord_arb_expired.created_at_ms = now_ms() - 46_000;
    let arb_open = engine
        .chaser
        .advance_working_order(&ord_arb_expired)
        .unwrap();
    match arb_open {
        ChaseStepOutcome::FilledOpen {
            filled_quantity, ..
        } => assert!((filled_quantity - 30.0).abs() < 1e-6),
        other => panic!("Expected FilledOpen on expired partial fill, got {other:?}"),
    }
    assert!(cancelled_oids.lock().contains(&arb_oid_num));
    let arb_pos = db.get_position_by_id(arb_pid).unwrap().unwrap();
    assert_eq!(arb_pos.status, "OPEN");
    assert!((arb_pos.filled_quantity - 30.0).abs() < 1e-6);
    let arb_triggers: Vec<f64> = submitted_wires
        .lock()
        .iter()
        .filter(|w| w.coin == "ARB" && w.is_trigger)
        .map(|w| w.sz)
        .collect();
    assert_eq!(arb_triggers.len(), 2);
    assert!(arb_triggers.iter().all(|&sz| (sz - 30.0).abs() < 1e-6));
}

fn configure_testnet_mock(db: &DatabaseManager, port: u16) {
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
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("max_open_positions", "10", None)
        .unwrap();
    db.set_config_value("alo_chase_threshold_bps", "4.0", None)
        .unwrap();
}

#[test]
fn test_pump_1tick_no_self_chase_and_unchanged_price_preserves_queue() {
    type SubmittedOrders = Arc<Mutex<Vec<(String, Option<String>, f64)>>>;

    let l2_bbo: Arc<Mutex<(f64, f64)>> = Arc::new(Mutex::new((0.002488, 0.002489)));
    let l2_bbo_srv = l2_bbo.clone();
    let submitted_orders: SubmittedOrders = Arc::new(Mutex::new(Vec::new()));
    let submitted_srv = submitted_orders.clone();
    let cancelled_oids: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let cancelled_srv = cancelled_oids.clone();
    let next_oid: Arc<Mutex<u64>> = Arc::new(Mutex::new(65_000));

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "clearinghouseState" {
                return (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "1000.0" },
                        "assetPositions": []
                    }),
                );
            }
            if typ == "l2Book" {
                let (bid, ask) = *l2_bbo_srv.lock();
                return (
                    200,
                    json!({
                        "levels": [
                            [{"px": format!("{bid:.6}"), "sz": "50000"}],
                            [{"px": format!("{ask:.6}"), "sz": "50000"}]
                        ]
                    }),
                );
            }
            return (200, json!([]));
        }

        let action_type = body
            .pointer("/action/type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if action_type == "cancel" {
            if let Some(arr) = body.pointer("/action/cancels").and_then(Value::as_array) {
                for c in arr {
                    if let Some(o) = c.get("o").and_then(Value::as_u64) {
                        cancelled_srv.lock().push(o);
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
            let ord = body
                .pointer("/action/orders/0")
                .cloned()
                .unwrap_or(Value::Null);
            let px_f: f64 = ord
                .get("p")
                .and_then(Value::as_str)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let tif = ord
                .pointer("/t/limit/tif")
                .and_then(Value::as_str)
                .map(str::to_string);
            submitted_srv.lock().push(("PUMP".to_string(), tif, px_f));
            let mut guard = next_oid.lock();
            *guard += 1;
            let oid = *guard;
            return (
                200,
                json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": { "statuses": [{ "resting": { "oid": oid } }] }
                    }
                }),
            );
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    configure_testnet_mock(&db, server.port());
    db.set_config_value("max_alo_chase_ticks", "5", None)
        .unwrap();

    db.upsert_exchange_asset(&AssetUpsertInput {
        raw_symbol_or_coin: "PUMP".to_string(),
        asset_index: 150,
        sz_decimals: 0,
        tick_size: 1e-6,
        lot_step_size: 1.0,
        max_leverage: 10.0,
        mark_price: 0.002488,
        mid_price: 0.002488,
        oracle_price: 0.002488,
        funding_rate_hourly: 0.0,
        day_volume_usdc: 10_000_000.0,
        open_interest_usdc: 2_000_000.0,
        prev_day_price: 0.002480,
        is_core_eligible: true,
        is_event_eligible: true,
        is_priority_watched: false,
    })
    .unwrap();

    let pump = db.get_asset_by_symbol("PUMP-USDC").unwrap().unwrap();
    let pos_id = engine
        .chaser
        .place_working_order(&NewWorkingOrderRequest {
            asset: pump.clone(),
            side: OrderSide::Short,
            requested_quantity: 31_539.0,
            reserved_margin_usdc: 78.5,
            portfolio_weight: -0.10,
            s1_weight_contrib: -0.10,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.002606,
            reason: "LIVE_BREAKOUT_SURGE".to_string(),
            bypass_entry_interval: true,
        })
        .unwrap();

    // Initial ALO Short placed at 0.002489 (1 tick = 4.02 bps > 4.0 config threshold)
    let ord0 = db
        .get_assets_ordered()
        .unwrap()
        .into_iter()
        .find(|o| o.position_id == pos_id)
        .unwrap();
    assert!((ord0.limit_price - 0.002489).abs() < 1e-9);
    assert_eq!(submitted_orders.lock().len(), 1);

    // 1. Resting 1 tick off mid (4.02 bps < 1.5 * one_tick_bps = 6.03 bps) MUST stay Resting (no cancel/burn)
    let res_1tick = engine.chaser.advance_working_order(&ord0).unwrap();
    assert!(matches!(res_1tick, ChaseStepOutcome::Resting { .. }));
    assert!(cancelled_oids.lock().is_empty());
    assert_eq!(submitted_orders.lock().len(), 1);
    assert_eq!(
        db.get_assets_ordered().unwrap()[0].chase_count,
        0,
        "1-tick PUMP offset must not self-chase"
    );

    // 2. Slight move within 1.5 * one_tick_bps (4.82 bps < 6.03 bps) STILL stays Resting
    db.update_asset_live_quote(pump.id, 0.0024878, 0.0024878)
        .unwrap();
    let ord1 = db.get_assets_ordered().unwrap().remove(0);
    let res_within_15x = engine.chaser.advance_working_order(&ord1).unwrap();
    assert!(matches!(res_within_15x, ChaseStepOutcome::Resting { .. }));
    assert!(cancelled_oids.lock().is_empty());

    // 3. Mid breaches 1.5 * one_tick_bps (0.002487 -> 8.04 bps), while l2Book BBO still clamps
    //    new_limit_price == 0.002489 == ordered.limit_price -> MUST NOT cancel or burn chase_count!
    db.update_asset_live_quote(pump.id, 0.002487, 0.002487)
        .unwrap();
    let ord2 = db.get_assets_ordered().unwrap().remove(0);
    let res_unchanged_px = engine.chaser.advance_working_order(&ord2).unwrap();
    assert!(matches!(res_unchanged_px, ChaseStepOutcome::Resting { .. }));
    assert!(cancelled_oids.lock().is_empty());
    assert_eq!(submitted_orders.lock().len(), 1);
    assert_eq!(db.get_assets_ordered().unwrap()[0].chase_count, 0);

    // 4. Both mid and l2Book move down (0.002486 / 0.002487) -> chases cleanly once to 0.002487
    *l2_bbo.lock() = (0.002486, 0.002487);
    db.update_asset_live_quote(pump.id, 0.002486, 0.002486)
        .unwrap();
    let ord3 = db.get_assets_ordered().unwrap().remove(0);
    let res_chased = engine.chaser.advance_working_order(&ord3).unwrap();
    match res_chased {
        ChaseStepOutcome::Chased {
            new_limit_price,
            chase_count,
            ..
        } => {
            assert_eq!(chase_count, 1);
            assert!((new_limit_price - 0.002487).abs() < 1e-9);
        }
        other => panic!("Expected Chased when BBO moved down, got {other:?}"),
    }
    assert_eq!(*cancelled_oids.lock(), vec![65_001]);
}

#[test]
fn test_l2book_passive_clamp_and_bad_alo_rejection_recovery_to_fill() {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct WarnVisit(bool);
    impl tracing::field::Visit for WarnVisit {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                let s = format!("{value:?}");
                if s.contains("ALO") {
                    self.0 = true;
                }
            }
        }
    }
    struct WarnCapture(Arc<AtomicUsize>);
    impl tracing::Subscriber for WarnCapture {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() <= tracing::Level::WARN
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            if *event.metadata().level() == tracing::Level::WARN {
                let mut v = WarnVisit(false);
                event.record(&mut v);
                if v.0 || event.metadata().target().contains("order_chaser") {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[derive(Clone, Debug)]
    enum AloScript {
        AcceptResting,
        RejectPostOnlyThenAcceptRequote,
        RejectAllAlo,
    }

    let script = Arc::new(Mutex::new(AloScript::AcceptResting));
    let script_srv = script.clone();
    let bbo_by_coin: Arc<Mutex<HashMap<String, (f64, f64)>>> = Arc::new(Mutex::new(
        [
            ("NEAR".to_string(), (2.9680, 2.9684)),
            ("PUMP".to_string(), (0.002488, 0.002489)),
        ]
        .into_iter()
        .collect(),
    ));
    let bbo_srv = bbo_by_coin.clone();
    let submitted_pxs: Arc<Mutex<Vec<(u64, String, f64)>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_srv = submitted_pxs.clone();
    let next_oid: Arc<Mutex<u64>> = Arc::new(Mutex::new(70_000));
    let reject_counter: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let reject_srv = reject_counter.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "clearinghouseState" {
                return (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "1000.0" },
                        "assetPositions": []
                    }),
                );
            }
            if typ == "l2Book" {
                let coin = body.get("coin").and_then(Value::as_str).unwrap_or("PUMP");
                let (bid, ask) = bbo_srv.lock().get(coin).copied().unwrap_or((1.0, 1.001));
                return (
                    200,
                    json!({
                        "levels": [
                            [{"px": format!("{bid:.6}"), "sz": "50000"}],
                            [{"px": format!("{ask:.6}"), "sz": "50000"}]
                        ]
                    }),
                );
            }
            return (200, json!([]));
        }

        let action_type = body
            .pointer("/action/type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if action_type == "cancel" {
            return (
                200,
                json!({
                    "status": "ok",
                    "response": { "type": "cancel", "data": { "statuses": ["success"] } }
                }),
            );
        }
        if action_type == "order" {
            let ord = body
                .pointer("/action/orders/0")
                .cloned()
                .unwrap_or(Value::Null);
            let a_idx = ord.get("a").and_then(Value::as_u64).unwrap_or(0);
            let px_str = ord.get("p").and_then(Value::as_str).unwrap_or("0");
            let px_f: f64 = px_str.parse().unwrap_or(0.0);
            let sz_str = ord.get("s").and_then(Value::as_str).unwrap_or("0");
            let is_trigger = ord.pointer("/t/trigger").is_some();
            let tif = ord
                .pointer("/t/limit/tif")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            submitted_srv.lock().push((a_idx, tif.clone(), px_f));

            let mut oid_guard = next_oid.lock();
            *oid_guard += 1;
            let oid = *oid_guard;

            if !is_trigger && tif == "Ioc" {
                return (
                    200,
                    json!({
                        "status": "ok",
                        "response": {
                            "type": "order",
                            "data": {
                                "statuses": [{
                                    "filled": {
                                        "oid": oid,
                                        "totalSz": sz_str,
                                        "avgPx": "0.002488"
                                    }
                                }]
                            }
                        }
                    }),
                );
            }

            if !is_trigger && tif == "Alo" {
                let mut mode_guard = script_srv.lock();
                match *mode_guard {
                    AloScript::AcceptResting => {}
                    AloScript::RejectPostOnlyThenAcceptRequote => {
                        let mut cnt = reject_srv.lock();
                        *cnt += 1;
                        if *cnt == 1 {
                            bbo_srv
                                .lock()
                                .insert("PUMP".to_string(), (0.002487, 0.002488));
                            return (
                                200,
                                json!({
                                    "status": "ok",
                                    "response": {
                                        "type": "order",
                                        "data": {
                                            "statuses": [{
                                                "error": "Post only order would have immediately matched, bbo was 0.002487@0.002488"
                                            }]
                                        }
                                    }
                                }),
                            );
                        }
                        *mode_guard = AloScript::AcceptResting;
                    }
                    AloScript::RejectAllAlo => {
                        return (
                            200,
                            json!({
                                "status": "ok",
                                "response": {
                                    "type": "order",
                                    "data": { "statuses": [{ "error": "badAloPxRejected" }] }
                                }
                            }),
                        );
                    }
                }
            }

            return (
                200,
                json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": { "statuses": [{ "resting": { "oid": oid } }] }
                    }
                }),
            );
        }
        (200, json!({ "status": "ok" }))
    });

    let warn_count = Arc::new(AtomicUsize::new(0));
    let sub = WarnCapture(warn_count.clone());

    tracing::subscriber::with_default(sub, || {
        let db = fresh_isolated_db();
        let engine = ExecutionEngine::new(db.clone());
        configure_testnet_mock(&db, server.port());
        db.set_config_value("max_alo_chase_ticks", "3", None)
            .unwrap();

        db.upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "NEAR".to_string(),
            asset_index: 74,
            sz_decimals: 1,
            tick_size: 0.0001,
            lot_step_size: 0.1,
            max_leverage: 10.0,
            mark_price: 2.9682,
            mid_price: 2.9682,
            oracle_price: 2.9682,
            funding_rate_hourly: 0.0,
            day_volume_usdc: 20_000_000.0,
            open_interest_usdc: 5_000_000.0,
            prev_day_price: 2.95,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();
        db.upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "PUMP".to_string(),
            asset_index: 150,
            sz_decimals: 0,
            tick_size: 1e-6,
            lot_step_size: 1.0,
            max_leverage: 10.0,
            mark_price: 0.002488,
            mid_price: 0.002488,
            oracle_price: 0.002488,
            funding_rate_hourly: 0.0,
            day_volume_usdc: 10_000_000.0,
            open_interest_usdc: 2_000_000.0,
            prev_day_price: 0.00248,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();

        // Part 1: l2Book BBO clamps chased ALO Long strictly passive against stale allMids spike
        let near = db.get_asset_by_symbol("NEAR-USDC").unwrap().unwrap();
        let near_pid = engine
            .chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: near.clone(),
                side: OrderSide::Long,
                requested_quantity: 20.0,
                reserved_margin_usdc: 59.36,
                portfolio_weight: 0.10,
                s1_weight_contrib: 0.10,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 2.85,
                reason: "BBO_CLAMP_TEST".to_string(),
                bypass_entry_interval: true,
            })
            .unwrap();

        // Stale allMids spikes to 2.9800 while live l2Book ask is only 2.9715 -> must clamp <= 2.9714
        bbo_by_coin
            .lock()
            .insert("NEAR".to_string(), (2.9710, 2.9715));
        db.update_asset_live_quote(near.id, 2.9800, 2.9800).unwrap();
        let near_ord = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == near_pid)
            .unwrap();
        match engine.chaser.advance_working_order(&near_ord).unwrap() {
            ChaseStepOutcome::Chased {
                new_limit_price,
                chase_count,
                ..
            } => {
                assert_eq!(chase_count, 1);
                assert!((new_limit_price - 2.9714).abs() < 1e-6);
            }
            other => panic!("Expected clamped Chased outcome on NEAR, got {other:?}"),
        }
        engine
            .close_or_cancel_position(near_pid, "TEST_CLEANUP", Some(0))
            .unwrap();

        // Part 2: badAloPxRejected on chase triggers warn log + 1-tick passive re-quote instead of killing
        let pump = db.get_asset_by_symbol("PUMP-USDC").unwrap().unwrap();
        let pump_pid_1 = engine
            .chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: pump.clone(),
                side: OrderSide::Short,
                requested_quantity: 31_539.0,
                reserved_margin_usdc: 78.5,
                portfolio_weight: -0.10,
                s1_weight_contrib: -0.10,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 0.002606,
                reason: "REQUOTE_RECOVERY_TEST".to_string(),
                bypass_entry_interval: true,
            })
            .unwrap();

        *script.lock() = AloScript::RejectPostOnlyThenAcceptRequote;
        bbo_by_coin
            .lock()
            .insert("PUMP".to_string(), (0.002486, 0.002487));
        db.update_asset_live_quote(pump.id, 0.002486, 0.002486)
            .unwrap();
        let pump_ord_1 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pump_pid_1)
            .unwrap();
        let warns_before = warn_count.load(Ordering::SeqCst);
        let rq_res = engine.chaser.advance_working_order(&pump_ord_1).unwrap();
        assert!(warn_count.load(Ordering::SeqCst) > warns_before);
        match rq_res {
            ChaseStepOutcome::Chased {
                new_limit_price,
                chase_count,
                ..
            } => {
                assert_eq!(chase_count, 1);
                assert!((new_limit_price - 0.002489).abs() < 1e-9);
            }
            other => panic!("Expected passive re-quote Chased outcome, got {other:?}"),
        }
        let pump_pos_1 = db.get_position_by_id(pump_pid_1).unwrap().unwrap();
        assert_eq!(pump_pos_1.status, "WORKING");
        assert!(pump_pos_1.exchange_order_id.is_some());
        engine
            .close_or_cancel_position(pump_pid_1, "TEST_CLEANUP_2", Some(0))
            .unwrap();

        // Part 3: Even when all ALO chases reject (badAloPxRejected), order stays WORKING at chase_count=1
        //         and escalates cleanly to CROSSING_IOC fill instead of dying at chase_count=0!
        db.set_config_value("max_alo_chase_ticks", "2", None)
            .unwrap();
        *script.lock() = AloScript::AcceptResting;
        bbo_by_coin
            .lock()
            .insert("PUMP".to_string(), (0.002488, 0.002489));
        db.update_asset_live_quote(pump.id, 0.002488, 0.002488)
            .unwrap();

        let pump_pid_2 = engine
            .chaser
            .place_working_order(&NewWorkingOrderRequest {
                asset: pump.clone(),
                side: OrderSide::Short,
                requested_quantity: 31_539.0,
                reserved_margin_usdc: 78.5,
                portfolio_weight: -0.10,
                s1_weight_contrib: -0.10,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 0.002606,
                reason: "IOC_RECOVERY_TEST".to_string(),
                bypass_entry_interval: true,
            })
            .unwrap();

        *script.lock() = AloScript::RejectAllAlo;
        bbo_by_coin
            .lock()
            .insert("PUMP".to_string(), (0.002486, 0.002487));
        db.update_asset_live_quote(pump.id, 0.002486, 0.002486)
            .unwrap();

        // Chase #1 rejects primary + re-quote -> stays WORKING with chase_count = 1
        let ord_c0 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pump_pid_2)
            .unwrap();
        let c1_res = engine.chaser.advance_working_order(&ord_c0).unwrap();
        assert!(matches!(
            c1_res,
            ChaseStepOutcome::Chased { chase_count: 1, .. }
        ));
        assert_eq!(
            db.get_position_by_id(pump_pid_2).unwrap().unwrap().status,
            "WORKING"
        );

        // Chase #2 rejects -> stays WORKING with chase_count = 2 (ready for CROSSING_IOC)
        let ord_c1 = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pump_pid_2)
            .unwrap();
        let c2_res = engine.chaser.advance_working_order(&ord_c1).unwrap();
        assert!(matches!(
            c2_res,
            ChaseStepOutcome::Chased { chase_count: 2, .. }
        ));

        // Tick 3: chase_count == max_chases (2) -> escalates to CROSSING_IOC and fills OPEN
        let ord_ioc = db
            .get_assets_ordered()
            .unwrap()
            .into_iter()
            .find(|o| o.position_id == pump_pid_2)
            .unwrap();
        let fill_res = engine.chaser.advance_working_order(&ord_ioc).unwrap();
        match fill_res {
            ChaseStepOutcome::FilledOpen {
                filled_quantity, ..
            } => assert!((filled_quantity - 31_539.0).abs() < 1e-6),
            other => panic!("Expected FilledOpen via CROSSING_IOC recovery, got {other:?}"),
        }
        assert_eq!(
            db.get_position_by_id(pump_pid_2).unwrap().unwrap().status,
            "OPEN"
        );
    });
}

#[test]
fn test_rebalance_skips_wide_spread_and_thin_l2_books_to_place_liquid_candidate() {
    use std::collections::HashMap;

    let books_by_coin: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));
    let books_srv = books_by_coin.clone();
    let l2_requested_coins: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let l2_req_srv = l2_requested_coins.clone();
    let submitted_asset_indices: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted_srv = submitted_asset_indices.clone();
    let next_oid: Arc<Mutex<u64>> = Arc::new(Mutex::new(81_000));

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "l2Book" {
                let coin = body
                    .get("coin")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                l2_req_srv.lock().push(coin.clone());
                let resp = books_srv
                    .lock()
                    .get(&coin)
                    .cloned()
                    .unwrap_or_else(|| json!({ "levels": [[], []] }));
                return (200, resp);
            }
            if typ == "clearinghouseState" {
                return (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "1000.0" },
                        "assetPositions": []
                    }),
                );
            }
            return (200, json!([]));
        }

        let action_type = body
            .pointer("/action/type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if action_type == "order" {
            let a_idx = body
                .pointer("/action/orders/0/a")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX);
            submitted_srv.lock().push(a_idx);
            let mut oid_guard = next_oid.lock();
            *oid_guard += 1;
            let oid = *oid_guard;
            return (
                200,
                json!({
                    "status": "ok",
                    "response": {
                        "type": "order",
                        "data": { "statuses": [{ "resting": { "oid": oid } }] }
                    }
                }),
            );
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    // Seed kSHIB (wide spread) plus extra assets to test the 6-candidate L2 cap
    for (coin, idx, sz_dec, tick, lot, px) in [
        ("kSHIB", 7u32, 0u32, 1e-6, 1.0, 0.01000),
        ("OP", 8u32, 1u32, 0.0001, 0.1, 1.50),
        ("INJ", 9u32, 1u32, 0.001, 0.1, 20.0),
    ] {
        db.upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: coin.to_string(),
            asset_index: idx,
            sz_decimals: sz_dec,
            tick_size: tick,
            lot_step_size: lot,
            max_leverage: 10.0,
            mark_price: px,
            mid_price: px,
            oracle_price: px,
            funding_rate_hourly: 0.00001,
            day_volume_usdc: 15_000_000.0,
            open_interest_usdc: 5_000_000.0,
            prev_day_price: px * 0.98,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();
    }

    configure_testnet_mock(&db, server.port());
    db.set_config_value("live_exchange_equity_usdc", "1000.0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_usdc", "1000.0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();
    db.set_config_value("max_entry_spread_bps", "20.0", None)
        .unwrap();

    let kshib = db.get_asset_by_symbol("kSHIB-USDC").unwrap().unwrap();
    let avax = db.get_asset_by_symbol("AVAX-USDC").unwrap().unwrap();
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let link = db.get_asset_by_symbol("LINK-USDC").unwrap().unwrap();
    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();
    let sui = db.get_asset_by_symbol("SUI-USDC").unwrap().unwrap();
    let op = db.get_asset_by_symbol("OP-USDC").unwrap().unwrap();
    let inj = db.get_asset_by_symbol("INJ-USDC").unwrap().unwrap();

    // Scenario 1:
    // - #1 kSHIB has wide 54 bps spread (> 20 bps) -> MUST be skipped
    // - #2 AVAX has tight 5 bps spread & deep bids ($700), but thin one-sided ask ($17.50 < $25 within 15 bps;
    //      decoy ask at $35.15 is ~40 bps away so ignored) -> MUST be skipped
    // - #3 SOL has tight ~5 bps spread and ~$550 depth on BOTH sides within 15 bps -> MUST be placed in same cycle!
    {
        let mut books = books_by_coin.lock();
        books.insert(
            "kSHIB".to_string(),
            json!({
                "levels": [
                    [{"px": "0.009973", "sz": "50000"}],
                    [{"px": "0.010027", "sz": "50000"}]
                ]
            }),
        );
        books.insert(
            "AVAX".to_string(),
            json!({
                "levels": [
                    [{"px": "34.99125", "sz": "20.0"}],
                    [
                        {"px": "35.00875", "sz": "0.5"},
                        {"px": "35.15000", "sz": "100.0"}
                    ]
                ]
            }),
        );
        books.insert(
            "SOL".to_string(),
            json!({
                "levels": [
                    [{"px": "184.20", "sz": "2.0"}, {"px": "184.05", "sz": "1.0"}],
                    [{"px": "184.29", "sz": "2.0"}, {"px": "184.45", "sz": "1.0"}]
                ]
            }),
        );
    }

    let sig_s1 = make_signal_output(vec![
        make_signal_input(&kshib, 0.28, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&avax, 0.24, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&sol, 0.20, 0.0, ACTION_OPEN_LONG),
    ]);
    let reb1 = engine.execute_signal_rebalance(&sig_s1, 1000.0).unwrap();
    assert_eq!(reb1.working_orders_placed, 1);

    let ordered1 = db.get_assets_ordered().unwrap();
    assert_eq!(ordered1.len(), 1);
    assert_eq!(ordered1[0].symbol, "SOL-USDC");
    assert_eq!(
        *submitted_asset_indices.lock(),
        vec![sol.asset_index as u64]
    );
    {
        let reqs = l2_requested_coins.lock();
        assert!(reqs.len() >= 3);
        assert_eq!(reqs[0], "kSHIB");
        assert_eq!(reqs[1], "AVAX");
        assert_eq!(reqs[2], "SOL");
    }

    // Cleanup Scenario 1 order
    engine
        .close_or_cancel_position(reb1.placed_position_ids[0], "TEST_CLEANUP", Some(0))
        .unwrap();
    l2_requested_coins.lock().clear();
    submitted_asset_indices.lock().clear();

    // Scenario 2:
    // - #1 LINK has 28 bps spread (rejected under tighter 20 bps cap, would have passed old 45 bps cap)
    // - #2 ARB has an empty L2 book (`Ok(None)`) -> skipped safely
    // - #3 SUI has tight ~5 bps spread and $500+ depth each side -> placed in same cycle
    {
        let mut books = books_by_coin.lock();
        books.insert(
            "LINK".to_string(),
            json!({
                "levels": [
                    [{"px": "18.474", "sz": "30.0"}],
                    [{"px": "18.526", "sz": "30.0"}]
                ]
            }),
        );
        books.insert("ARB".to_string(), json!({ "levels": [[], []] }));
        books.insert(
            "SUI".to_string(),
            json!({
                "levels": [
                    [{"px": "3.4191", "sz": "160.0"}],
                    [{"px": "3.4209", "sz": "160.0"}]
                ]
            }),
        );
    }

    let sig_s2 = make_signal_output(vec![
        make_signal_input(&link, 0.27, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&arb, 0.24, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&sui, 0.20, 0.0, ACTION_OPEN_LONG),
    ]);
    let reb2 = engine.execute_signal_rebalance(&sig_s2, 1000.0).unwrap();
    assert_eq!(reb2.working_orders_placed, 1);
    let ordered2 = db.get_assets_ordered().unwrap();
    assert_eq!(ordered2.len(), 1);
    assert_eq!(ordered2[0].symbol, "SUI-USDC");
    assert_eq!(
        *submitted_asset_indices.lock(),
        vec![sui.asset_index as u64]
    );

    engine
        .close_or_cancel_position(reb2.placed_position_ids[0], "TEST_CLEANUP", Some(0))
        .unwrap();
    l2_requested_coins.lock().clear();
    submitted_asset_indices.lock().clear();

    // Scenario 3: Cap L2 depth fetches per rebalance cycle at 6 so HL /info is never spammed
    {
        let mut books = books_by_coin.lock();
        books.insert(
            "OP".to_string(),
            json!({
                "levels": [
                    [{"px": "1.495", "sz": "400.0"}],
                    [{"px": "1.505", "sz": "400.0"}]
                ]
            }),
        );
        books.insert(
            "INJ".to_string(),
            json!({
                "levels": [
                    [{"px": "19.90", "sz": "30.0"}],
                    [{"px": "20.10", "sz": "30.0"}]
                ]
            }),
        );
        // Make SUI & SOL wide too for ranks #5/#6, then leave BTC/ETH liquid at ranks #7/#8 (past cap=6)
        books.insert(
            "SUI".to_string(),
            json!({
                "levels": [
                    [{"px": "3.4000", "sz": "200.0"}],
                    [{"px": "3.4400", "sz": "200.0"}]
                ]
            }),
        );
        books.insert(
            "BTC".to_string(),
            json!({
                "levels": [
                    [{"px": "94990.0", "sz": "1.0"}],
                    [{"px": "95010.0", "sz": "1.0"}]
                ]
            }),
        );
        books.insert(
            "ETH".to_string(),
            json!({
                "levels": [
                    [{"px": "3399.5", "sz": "1.0"}],
                    [{"px": "3400.5", "sz": "1.0"}]
                ]
            }),
        );
    }
    let btc = db.get_asset_by_symbol("BTC-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();

    let sig_s3 = make_signal_output(vec![
        make_signal_input(&kshib, 0.29, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&avax, 0.28, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&link, 0.27, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&arb, 0.26, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&op, 0.25, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&inj, 0.24, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&btc, 0.23, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&eth, 0.22, 0.0, ACTION_OPEN_LONG),
    ]);
    let reb3 = engine.execute_signal_rebalance(&sig_s3, 1000.0).unwrap();
    assert_eq!(reb3.working_orders_placed, 0);
    assert_eq!(
        l2_requested_coins.lock().len(),
        6,
        "Per-rebalance L2 depth check cap must stop after 6 candidates"
    );
    assert!(submitted_asset_indices.lock().is_empty());
}

#[test]
fn test_single_asset_weight_floor_001_preserved_for_alert_38() {
    let mut cfg = ConfigMap::new();
    cfg.insert("max_single_asset_weight", "0.01");
    cfg.insert("min_rebalance_delta_weight", "0.005");

    let limits = RiskLimitsConfig::from_config_map(&cfg);
    assert!(
        (limits.max_single_asset_weight - 0.01).abs() < 1e-9,
        "RiskLimitsConfig must preserve 0.01 instead of inflating to 0.05"
    );

    let alloc_cfg = AllocatorConfig::from_config_map(&cfg);
    assert!(
        (alloc_cfg.max_single_asset_weight - 0.01).abs() < 1e-9,
        "AllocatorConfig must preserve 0.01 instead of inflating to 0.05"
    );

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    db.set_config_value("initial_paper_equity_usdc", "10000.0", None)
        .unwrap();
    db.set_config_value("max_single_asset_weight", "0.01", None)
        .unwrap();
    db.set_config_value("min_rebalance_delta_weight", "0.005", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();

    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let mut s1 = HashMap::new();
    s1.insert(
        eth.id,
        CarryMomentumMetrics {
            asset_id: eth.id,
            symbol: eth.symbol.clone(),
            annualized_vol: 0.40,
            market_beta: 1.0,
            s1_mom_score: 0.90,
            s1_carry_score: 0.25,
            w_s1_carry_mom: 0.35,
            passes_quality: true,
        },
    );
    let allocated = allocate_multi_sleeve_weights(
        std::slice::from_ref(&eth),
        &s1,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
        &[],
        &alloc_cfg,
    );
    assert_eq!(allocated.len(), 1);
    assert!(
        (allocated[0].target_weight - 0.01).abs() < 1e-6,
        "Allocated target_weight must cap at 0.01, got {}",
        allocated[0].target_weight
    );

    let sig_out = make_signal_output(vec![make_signal_input(&eth, 0.25, 0.0, ACTION_OPEN_LONG)]);
    let reb = engine.execute_signal_rebalance(&sig_out, 10_000.0).unwrap();
    assert_eq!(reb.working_orders_placed, 1);

    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 1);
    let order_notional = ordered[0].limit_price * ordered[0].requested_quantity;
    assert!(
        order_notional <= 100.0 + 1e-2,
        "Order notional ${order_notional:.2} exceeded 1% of $10,000 ($100.00)"
    );
    let pos_record = db
        .get_position_by_id(ordered[0].position_id)
        .unwrap()
        .unwrap();
    assert!(pos_record.portfolio_weight <= 0.0101);
}

#[test]
fn test_max_gross_leverage_below_target_multiplier_enforced_for_alert_37() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    db.set_config_value("risk_tier", "extreme", None).unwrap(); // 1.85x target_leverage_multiplier
    db.set_config_value("max_gross_leverage_x", "0.50", None)
        .unwrap();
    db.set_config_value("max_single_asset_weight", "1.00", None)
        .unwrap();
    db.set_config_value("initial_paper_equity_usdc", "1000.0", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "0", None)
        .unwrap();

    let btc = db.get_asset_by_symbol("BTC-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();

    // Seed $495 committed on BTC (< 0.50 * 1000 = $500, leaving $5 < $12 min order notional)
    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: btc.id,
        coin: &btc.coin,
        side: OrderSide::Long,
        entry_price: btc.mark_price,
        mark_price: btc.mark_price,
        quantity: 495.0 / btc.mark_price,
        committed_notional_usdc: 495.0,
        unrealized_pnl_usdc: 0.0,
        portfolio_weight: 0.495,
        protective_stop_px: btc.mark_price * 0.95,
        take_profit_px: btc.mark_price * 1.10,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(now_ms() - 600_000),
    })
    .unwrap();

    // Rebalance must block ETH entry because effective gross ceiling is $1000 * min(1.85, 0.50) = $500
    let sig_eth = make_signal_output(vec![make_signal_input(&eth, 0.20, 0.0, ACTION_OPEN_LONG)]);
    let reb = engine.execute_signal_rebalance(&sig_eth, 1000.0).unwrap();
    assert_eq!(reb.working_orders_placed, 0);
    assert!(db.get_assets_ordered().unwrap().is_empty());

    // Add $400 SOL position -> $895 total (< $1850 target_mult ceiling, > $500 max_gross_leverage_x ceiling)
    db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
        asset_id: sol.id,
        coin: &sol.coin,
        side: OrderSide::Long,
        entry_price: sol.mark_price,
        mark_price: sol.mark_price,
        quantity: 400.0 / sol.mark_price,
        committed_notional_usdc: 400.0,
        unrealized_pnl_usdc: 0.0,
        portfolio_weight: 0.40,
        protective_stop_px: sol.mark_price * 0.95,
        take_profit_px: sol.mark_price * 1.10,
        funding_accrued_usdc: 0.0,
        opened_at_ms: Some(now_ms() - 600_000),
    })
    .unwrap();

    let tick1_reports = engine.enforce_leverage_and_position_caps().unwrap();
    assert!(
        tick1_reports.is_empty(),
        "tick 1 must confirm OPEN breach before auto-deleveraging"
    );
    let closed_reports = engine.enforce_leverage_and_position_caps().unwrap();
    assert!(
        !closed_reports.is_empty(),
        "enforce_leverage_and_position_caps must deleverage on tick 2 when gross > equity * max_gross_leverage_x"
    );
    let rem_committed: f64 = db
        .get_assets_held()
        .unwrap()
        .iter()
        .map(|h| h.committed_notional_usdc)
        .sum();
    assert!(rem_committed <= 500.0 + 1e-6);
}

#[test]
fn test_risk_capital_cap_10_usd_and_pre_reconcile_no_100_inflation_for_alert_33() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "5000.0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_usdc", "10.0", None)
        .unwrap();

    let cfg = db.load_config().unwrap();
    assert!(
        (engine.resolve_effective_base_equity_usdc(&cfg) - 10.0).abs() < 1e-9,
        "risk_capital_cap_usdc = 10.0 must be enforced when live_eq > 10.0"
    );

    // Small live equity ($50) with cap = $10 must still cap to $10
    db.set_config_value("live_exchange_equity_usdc", "50.0", None)
        .unwrap();
    let cfg_small_live = db.load_config().unwrap();
    assert!((engine.resolve_effective_base_equity_usdc(&cfg_small_live) - 10.0).abs() < 1e-9);

    // Pre-reconcile (live_eq == 0.0, non-ephemeral DB with wallet) must not inflate < $100 cap to $100
    let mut pre_rec_cfg = ConfigMap::new();
    pre_rec_cfg.insert("execution_mode", "TESTNET");
    pre_rec_cfg.insert(
        "wallet_address",
        "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
    );
    pre_rec_cfg.insert("live_exchange_equity_usdc", "0.0");
    pre_rec_cfg.insert("risk_capital_cap_usdc", "25.0");
    assert_eq!(
        ExecutionEngine::resolve_live_base_equity_usdc(&pre_rec_cfg, false),
        Some(25.0)
    );
    pre_rec_cfg.insert("risk_capital_cap_usdc", "10.0");
    assert_eq!(
        ExecutionEngine::resolve_live_base_equity_usdc(&pre_rec_cfg, false),
        Some(10.0)
    );
    pre_rec_cfg.insert("risk_capital_cap_usdc", "0.0");
    assert_eq!(
        ExecutionEngine::resolve_live_base_equity_usdc(&pre_rec_cfg, false),
        None
    );
}

#[test]
fn test_empty_private_key_in_live_testnet_fails_closed_for_alert_31() {
    let exchange_calls = Arc::new(Mutex::new(0usize));
    let exchange_calls_srv = exchange_calls.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "1000.0" },
                        "assetPositions": [{
                            "position": {
                                "coin": "SOL",
                                "szi": "5.0",
                                "entryPx": "180.0",
                                "positionValue": "921.25"
                            }
                        }]
                    }),
                ),
                "l2Book" => (
                    200,
                    json!({
                        "levels": [
                            [{"px": "184.20", "sz": "100.0"}],
                            [{"px": "184.30", "sz": "100.0"}]
                        ]
                    }),
                ),
                "allMids" => (200, json!({ "SOL": "184.25", "ETH": "3400.0" })),
                _ => (200, json!([])),
            };
        }
        *exchange_calls_srv.lock() += 1;
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    let base_url = server.base_url();
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
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();
    // Deliberately leave `private_key` empty!

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();

    // 1. OPEN position must NOT paper-close via close_or_cancel_position or execute_protective_stop_exit,
    //    and ensure_exchange_tpsl_orders must NOT write `paper-sl` / `paper-tp`.
    let (open_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: &sol.coin,
            side: OrderSide::Long,
            entry_price: 180.0,
            mark_price: 184.25,
            quantity: 5.0,
            committed_notional_usdc: 921.25,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.20,
            protective_stop_px: 175.0,
            take_profit_px: 200.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 600_000),
        })
        .unwrap();

    let cfg = db.load_config().unwrap();
    let (_, sl_oid, _, tp_oid) = engine
        .chaser
        .ensure_exchange_tpsl_orders(
            open_pid,
            &sol,
            OrderSide::Long,
            5.0,
            184.25,
            175.0,
            200.0,
            None,
            None,
            false,
            false,
            None,
            &cfg,
        )
        .unwrap();
    assert_eq!(sl_oid, None);
    assert_eq!(tp_oid, None);
    let pos_after_tpsl = db.get_position_by_id(open_pid).unwrap().unwrap();
    assert_eq!(pos_after_tpsl.protective_stop_oid, None);
    assert_eq!(pos_after_tpsl.take_profit_oid, None);

    let close_rep = engine
        .close_or_cancel_position(open_pid, "MANUAL_CLOSE", Some(300))
        .unwrap();
    assert_eq!(close_rep.final_status, "OPEN");
    assert_eq!(
        db.get_position_by_id(open_pid).unwrap().unwrap().status,
        "OPEN"
    );
    assert!(!db.is_asset_in_cooldown(sol.id).unwrap());

    let held_view = db.get_assets_held().unwrap().remove(0);
    let risk = RiskEngine::from_config_map(&cfg);
    let stop_exit = engine
        .priority_monitor
        .execute_protective_stop_exit(&held_view, 174.0, "PROTECTIVE_STOP_BREACH", &risk)
        .unwrap();
    assert_eq!(stop_exit, None);
    assert_eq!(
        db.get_position_by_id(open_pid).unwrap().unwrap().status,
        "OPEN"
    );
    assert!(!db.is_asset_in_cooldown(sol.id).unwrap());

    // 2. WORKING order must NOT paper-cancel or enter paper fill simulation in advance_working_order
    let work_pid = db
        .create_working_order(&aegis_db::WorkingOrderInput {
            asset_id: eth.id,
            client_order_id: "v2-eth-long-empty-pk".to_string(),
            side: OrderSide::Long,
            limit_price: 3_400.0,
            requested_quantity: 0.1,
            reserved_margin_usdc: 185.0,
            portfolio_weight: 0.15,
            s1_weight_contrib: 0.15,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 3_250.0,
            take_profit_px: None,
            exchange_order_id: Some("hl-77701".to_string()),
            reason: "EMPTY_PK_TEST".to_string(),
        })
        .unwrap();
    let cancel_rep = engine
        .close_or_cancel_position(work_pid, "MANUAL_CANCEL", Some(300))
        .unwrap();
    assert_eq!(cancel_rep.final_status, "WORKING");
    assert_eq!(cancel_rep.released_reserved_usdc, 0.0);
    assert_eq!(
        db.get_position_by_id(work_pid).unwrap().unwrap().status,
        "WORKING"
    );

    let ordered_view = db.get_assets_ordered().unwrap().remove(0);
    let adv_res = engine.chaser.advance_working_order(&ordered_view).unwrap();
    assert!(
        matches!(adv_res, ChaseStepOutcome::Resting { .. }),
        "advance_working_order must stay Resting without private_key, got {adv_res:?}"
    );
    assert_eq!(
        db.get_position_by_id(work_pid).unwrap().unwrap().status,
        "WORKING"
    );

    // 3. place_working_order & execute_signal_rebalance must refuse new entries without private_key
    let place_res = engine.chaser.place_working_order(&NewWorkingOrderRequest {
        asset: arb.clone(),
        side: OrderSide::Long,
        requested_quantity: 20.0,
        reserved_margin_usdc: 10.0,
        portfolio_weight: 0.05,
        s1_weight_contrib: 0.05,
        s2_weight_contrib: 0.0,
        s3_weight_contrib: 0.0,
        protective_stop_px: 0.80,
        reason: "EMPTY_PK_BLOCK".to_string(),
        bypass_entry_interval: true,
    });
    assert!(place_res.is_err());

    let sig_out = make_signal_output(vec![make_signal_input(&arb, 0.15, 0.0, ACTION_OPEN_LONG)]);
    let reb = engine.execute_signal_rebalance(&sig_out, 1000.0).unwrap();
    assert_eq!(reb.working_orders_placed, 0);
    assert_eq!(reb.positions_closed, 0);
    assert_eq!(*exchange_calls.lock(), 0);
}

#[test]
fn test_failed_cancel_orders_batch_keeps_working_and_margin_for_alert_23() {
    #[derive(Clone)]
    enum CancelMode {
        PerOrderError(String),
        Http500,
        OpenOrders500,
        OkSuccess,
    }

    let cancel_mode = Arc::new(Mutex::new(CancelMode::PerOrderError(
        "Rate limit exceeded".to_string(),
    )));
    let cancel_mode_srv = cancel_mode.clone();
    let cancelled_oids = Arc::new(Mutex::new(Vec::<u64>::new()));
    let cancelled_srv = cancelled_oids.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        let mode = cancel_mode_srv.lock().clone();
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "frontendOpenOrders" || typ == "openOrders" {
                if matches!(mode, CancelMode::OpenOrders500) {
                    return (500, json!({ "error": "upstream timeout" }));
                }
                return (
                    200,
                    json!([
                        {
                            "coin": "ETH",
                            "oid": 99001u64,
                            "isTrigger": true,
                            "orderType": "Stop Market",
                            "triggerPx": "3200.0",
                            "sz": "0.1"
                        },
                        {
                            "coin": "ETH",
                            "oid": 55002u64,
                            "isTrigger": false,
                            "orderType": "Limit",
                            "sz": "0.1"
                        }
                    ]),
                );
            }
            return (200, json!([]));
        }

        if body.pointer("/action/type").and_then(Value::as_str) == Some("cancel") {
            match mode {
                CancelMode::Http500 => return (500, json!({ "error": "exchange error" })),
                CancelMode::PerOrderError(msg) => {
                    return (
                        200,
                        json!({
                            "status": "ok",
                            "response": {
                                "type": "cancel",
                                "data": { "statuses": [{ "error": msg }] }
                            }
                        }),
                    );
                }
                CancelMode::OkSuccess | CancelMode::OpenOrders500 => {
                    if let Some(arr) = body.pointer("/action/cancels").and_then(Value::as_array) {
                        for c in arr {
                            if let Some(o) = c.get("o").and_then(Value::as_u64) {
                                cancelled_srv.lock().push(o);
                            }
                        }
                    }
                    return (
                        200,
                        json!({
                            "status": "ok",
                            "response": {
                                "type": "cancel",
                                "data": { "statuses": ["success"] }
                            }
                        }),
                    );
                }
            }
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock(&db, server.port());

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();

    // 1. WORKING order with numeric OID: per-order error, "filled" hint, and HTTP 500 must keep WORKING
    let sol_work_id = db
        .create_working_order(&aegis_db::WorkingOrderInput {
            asset_id: sol.id,
            client_order_id: "v2-sol-long-cancel-fail".to_string(),
            side: OrderSide::Long,
            limit_price: 184.0,
            requested_quantity: 2.0,
            reserved_margin_usdc: 200.0,
            portfolio_weight: 0.10,
            s1_weight_contrib: 0.10,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 175.0,
            take_profit_px: None,
            exchange_order_id: Some("hl-55001".to_string()),
            reason: "CANCEL_FAIL_TEST".to_string(),
        })
        .unwrap();

    for mode in [
        CancelMode::PerOrderError("Rate limit exceeded".to_string()),
        CancelMode::PerOrderError(
            "Order was never placed, already canceled, or filled.".to_string(),
        ),
        CancelMode::Http500,
    ] {
        *cancel_mode.lock() = mode;
        let rep = engine
            .close_or_cancel_position(sol_work_id, "MANUAL_CANCEL", Some(300))
            .unwrap();
        assert_eq!(rep.final_status, "WORKING");
        assert_eq!(rep.released_reserved_usdc, 0.0);
        let pos = db.get_position_by_id(sol_work_id).unwrap().unwrap();
        assert_eq!(pos.status, "WORKING");
        assert!((pos.reserved_margin_usdc - 200.0).abs() < 1e-6);
        assert!(!db.is_asset_in_cooldown(sol.id).unwrap());
    }

    // Confirming "already canceled" (without "filled") DOES cancel and release margin
    *cancel_mode.lock() = CancelMode::PerOrderError("Order was already canceled".to_string());
    let ok_rep = engine
        .close_or_cancel_position(sol_work_id, "MANUAL_CANCEL", Some(300))
        .unwrap();
    assert_eq!(ok_rep.final_status, "CANCELLED");
    assert!((ok_rep.released_reserved_usdc - 200.0).abs() < 1e-6);
    assert_eq!(
        db.get_position_by_id(sol_work_id).unwrap().unwrap().status,
        "CANCELLED"
    );

    // 2. WORKING order lacking numeric OID: failed openOrders keeps WORKING; healthy openOrders cancels non-trigger only
    let eth_work_id = db
        .create_working_order(&aegis_db::WorkingOrderInput {
            asset_id: eth.id,
            client_order_id: "v2-eth-long-no-oid".to_string(),
            side: OrderSide::Long,
            limit_price: 3_400.0,
            requested_quantity: 0.1,
            reserved_margin_usdc: 180.0,
            portfolio_weight: 0.10,
            s1_weight_contrib: 0.10,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 3_250.0,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "NO_OID_CANCEL_TEST".to_string(),
        })
        .unwrap();

    *cancel_mode.lock() = CancelMode::OpenOrders500;
    let err_no_oid = engine
        .close_or_cancel_position(eth_work_id, "MANUAL_CANCEL", Some(300))
        .unwrap();
    assert_eq!(err_no_oid.final_status, "WORKING");
    assert_eq!(err_no_oid.released_reserved_usdc, 0.0);

    *cancel_mode.lock() = CancelMode::OkSuccess;
    let ok_no_oid = engine
        .close_or_cancel_position(eth_work_id, "MANUAL_CANCEL", Some(300))
        .unwrap();
    assert_eq!(ok_no_oid.final_status, "CANCELLED");
    assert!((ok_no_oid.released_reserved_usdc - 180.0).abs() < 1e-6);
    assert_eq!(*cancelled_oids.lock(), vec![55002u64]);
}

#[test]
fn test_daily_drawdown_breaker_uses_indexed_peak_and_latest_equity_for_alert_32() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    db.set_config_value("max_daily_drawdown_pct", "0.15", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();

    let base_snap = |eq: f64| SnapshotInsertInput {
        total_equity_usdc: eq,
        uncapped_equity_usdc: eq,
        available_cash_usdc: eq,
        reserved_order_cash_usdc: 0.0,
        committed_notional_usdc: 0.0,
        net_notional_usdc: 0.0,
        gross_leverage_x: 0.0,
        net_leverage_x: 0.0,
        market_beta_exposure: 1.0,
        market_breadth_bull: 0.5,
        market_regime_label: "NEUTRAL".to_string(),
        risk_tier: "extreme".to_string(),
        leverage_multiplier: 1.85,
        s1_gross_weight: 0.0,
        s2_gross_weight: 0.0,
        s3_gross_weight: 0.0,
        working_order_count: 0,
        open_long_count: 0,
        open_short_count: 0,
        eligible_coin_count: 7,
    };

    db.insert_portfolio_snapshot(&base_snap(10_000.0)).unwrap();
    db.insert_portfolio_snapshot(&base_snap(8_000.0)).unwrap();

    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let sig_out = make_signal_output(vec![make_signal_input(&eth, 0.20, 0.0, ACTION_OPEN_LONG)]);
    let reb_tripped = engine.execute_signal_rebalance(&sig_out, 8_000.0).unwrap();
    assert_eq!(reb_tripped.working_orders_placed, 0);
    assert!(db.get_assets_ordered().unwrap().is_empty());
}

#[test]
fn test_reconcile_vault_info_sync_segregation_throttling_and_429_resilience() {
    use aegis_core::VaultInfoPayload;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let vault_eq_calls = Arc::new(AtomicUsize::new(0));
    let vault_details_calls = Arc::new(AtomicUsize::new(0));
    let fail_vault_requests = Arc::new(AtomicBool::new(false));

    let veq_srv = vault_eq_calls.clone();
    let vdet_srv = vault_details_calls.clone();
    let fail_srv = fail_vault_requests.clone();

    let hlp_addr = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";
    let extra_cfg_vault = "0x1111111111111111111111111111111111111111";

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "629.84" },
                        "assetPositions": []
                    }),
                ),
                "userVaultEquities" => {
                    veq_srv.fetch_add(1, Ordering::SeqCst);
                    if fail_srv.load(Ordering::SeqCst) {
                        return (429, json!({ "error": "rate_limited" }));
                    }
                    (
                        200,
                        json!([{
                            "vaultAddress": hlp_addr,
                            "equity": "202.400584",
                            "lockedUntilTimestamp": 1_793_779_590_878i64
                        }]),
                    )
                }
                "vaultDetails" => {
                    vdet_srv.fetch_add(1, Ordering::SeqCst);
                    if fail_srv.load(Ordering::SeqCst) {
                        return (429, json!({ "error": "rate_limited" }));
                    }
                    let addr = body
                        .get("vaultAddress")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if addr.eq_ignore_ascii_case(hlp_addr) {
                        (
                            200,
                            json!({
                                "name": "Hyperliquidity Provider (HLP)",
                                "vaultAddress": hlp_addr,
                                "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
                                "isClosed": false,
                                "apr": 0.0814,
                                "followerState": {
                                    "vaultEquity": "202.400584",
                                    "pnl": "0.92",
                                    "allTimePnl": "1.34",
                                    "daysFollowing": 25,
                                    "lockupUntil": 1_793_779_590_878i64
                                }
                            }),
                        )
                    } else {
                        (
                            200,
                            json!({
                                "name": "Configured Secondary Vault",
                                "vaultAddress": extra_cfg_vault,
                                "leader": "0x2222222222222222222222222222222222222222",
                                "isClosed": false,
                                "apr": 0.05,
                                "followerState": null
                            }),
                        )
                    }
                }
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);

    let base_url = server.base_url();
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
    db.set_config_value("hyperliquid_vault_address", extra_cfg_vault, None)
        .unwrap();

    // 1. First reconciliation syncs perp equity (629.84), followed by auxiliary vault info (202.40) strictly segregated
    let rep1 = engine.reconciler.run_cutover_reconciliation(false).unwrap();
    engine.aux_sync.sync_post_reconcile_views();
    assert!((rep1.exchange_equity_usdc - 629.84).abs() < 1e-6);

    let cfg1 = db.load_config_or_default();
    assert!(
        (cfg1.get_f64("live_exchange_equity_usdc", 0.0) - 629.84).abs() < 1e-6,
        "Vault equity MUST NOT be added into live_exchange_equity_usdc"
    );

    let snap = engine.record_portfolio_snapshot(None, None).unwrap();
    assert!(
        (snap.total_equity_usdc - 629.84).abs() < 1e-6,
        "Vault equity MUST NOT be added into portfolio_snapshots.total_equity_usdc"
    );

    let raw_vault_json = cfg1.get_str("live_vault_info_json", "");
    assert!(!raw_vault_json.is_empty());
    let parsed: VaultInfoPayload = serde_json::from_str(&raw_vault_json).unwrap();
    assert_eq!(parsed.configured_vault_address, extra_cfg_vault);
    assert!((parsed.total_vault_equity_usdc - 202.40).abs() < 1e-6);
    assert!((parsed.combined_portfolio_equity_usdc - 832.24).abs() < 1e-6);
    assert_eq!(parsed.vault_count, 2);
    assert_eq!(parsed.vaults[0].name, "Hyperliquidity Provider (HLP)");
    assert!((parsed.vaults[0].equity_usdc - 202.40).abs() < 1e-6);
    assert!((parsed.vaults[0].pnl_pct - 0.46).abs() < 1e-6);
    assert!((parsed.vaults[0].all_time_pnl_pct - 0.67).abs() < 1e-6);
    assert!((parsed.vaults[0].apr_pct - 8.14).abs() < 1e-6);
    assert_eq!(parsed.vaults[0].days_following, 25);
    assert_eq!(parsed.vaults[1].name, "Configured Secondary Vault");
    assert_eq!(parsed.vaults[1].equity_usdc, 0.0);

    assert_eq!(vault_eq_calls.load(Ordering::SeqCst), 1);
    assert_eq!(vault_details_calls.load(Ordering::SeqCst), 2);

    // 2. Immediate second reconciliation + auxiliary sync (< 60s) MUST NOT hit userVaultEquities or vaultDetails again
    let rep2 = engine.reconciler.run_cutover_reconciliation(false).unwrap();
    engine.aux_sync.sync_post_reconcile_views();
    assert!((rep2.exchange_equity_usdc - 629.84).abs() < 1e-6);
    assert_eq!(
        vault_eq_calls.load(Ordering::SeqCst),
        1,
        "Vault sync must be throttled to at most once every 60s"
    );
    assert_eq!(vault_details_calls.load(Ordering::SeqCst), 2);

    // 3. After >= 60s, if vault fetch hits HTTP 429, last known cached value is preserved without error
    fail_vault_requests.store(true, Ordering::SeqCst);
    let future_ts = parsed.synced_at_ms + aegis_execution::VAULT_SYNC_MIN_INTERVAL_MS + 1_000;
    let res_fail = engine
        .reconciler
        .sync_vault_info_if_due(&cfg1, future_ts, Some(629.84));
    assert!(res_fail.is_none());
    let calls_after_429 = vault_eq_calls.load(Ordering::SeqCst);
    assert!(calls_after_429 >= 2);

    let cfg_after_fail = db.load_config_or_default();
    assert_eq!(
        cfg_after_fail.get_str("live_vault_info_json", ""),
        raw_vault_json,
        "Cached live_vault_info_json must remain intact after 429 vault fetch failure"
    );

    // Immediate retry after 429 failure is still throttled for 60s (no 429 storm)
    let res_throttled =
        engine
            .reconciler
            .sync_vault_info_if_due(&cfg_after_fail, future_ts + 5_000, Some(629.84));
    assert!(res_throttled.is_none());
    assert_eq!(vault_eq_calls.load(Ordering::SeqCst), calls_after_429);
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
}

#[test]
fn test_reconcile_met_partial_close_pro_rata_funding_and_daily_pnl_window() {
    let now = now_ms();
    let open_ts = now - 36 * 3_600_000;
    let pre_24h_fund_ts = now - 30 * 3_600_000;
    let in_24h_fund_ts = now - 4 * 3_600_000;
    let partial_close_ts = now - 3_600_000;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": {
                            "accountValue": "5000.00",
                            "totalMarginUsed": "1200.00"
                        },
                        "withdrawable": "3800.00",
                        "assetPositions": [{
                            "position": {
                                "coin": "MET",
                                "szi": "9634.0",
                                "entryPx": "1.00",
                                "positionValue": "9634.0",
                                "unrealizedPnl": "0.0",
                                "cumFunding": { "sinceOpen": "-665.57" }
                            }
                        }]
                    }),
                ),
                "userFills" | "userFillsByTime" => (
                    200,
                    json!([
                        {
                            "coin": "MET",
                            "side": "B",
                            "px": "1.00",
                            "sz": "9654.0",
                            "fee": "0.0",
                            "closedPnl": "0.0",
                            "dir": "Open Long",
                            "time": open_ts,
                            "oid": 9001u64,
                            "tid": 1u64,
                            "startPosition": "0.0"
                        },
                        {
                            "coin": "MET",
                            "side": "A",
                            "px": "0.9575",
                            "sz": "20.0",
                            "fee": "0.02",
                            "closedPnl": "-0.85",
                            "dir": "Close Long",
                            "time": partial_close_ts,
                            "oid": 9002u64,
                            "tid": 2u64,
                            "startPosition": "9654.0"
                        }
                    ]),
                ),
                "userFunding" => (
                    200,
                    json!([
                        {
                            "time": pre_24h_fund_ts,
                            "hash": "0x0000",
                            "delta": {
                                "type": "funding",
                                "coin": "MET",
                                "usdc": "500.00",
                                "szi": "9654.0",
                                "fundingRate": "0.0005"
                            }
                        },
                        {
                            "time": in_24h_fund_ts,
                            "hash": "0x0000",
                            "delta": {
                                "type": "funding",
                                "coin": "MET",
                                "usdc": "165.57",
                                "szi": "9654.0",
                                "fundingRate": "0.00017"
                            }
                        }
                    ]),
                ),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    let met_id = db
        .upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: "MET".to_string(),
            asset_index: 50,
            sz_decimals: 0,
            tick_size: 0.0001,
            lot_step_size: 1.0,
            max_leverage: 10.0,
            mark_price: 1.0,
            mid_price: 1.0,
            oracle_price: 1.0,
            funding_rate_hourly: 0.0001,
            day_volume_usdc: 10_000_000.0,
            open_interest_usdc: 5_000_000.0,
            prev_day_price: 1.0,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: false,
        })
        .unwrap();
    configure_testnet_mock_db(&db, server.port());

    engine.aux_sync.sync_startup_before_reconcile();
    engine.reconciler.run_cutover_reconciliation(false).unwrap();

    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    let met_open = &held[0];
    assert_eq!(met_open.symbol, "MET-USDC");
    assert_eq!(met_open.opened_at_ms, Some(open_ts));
    assert!((met_open.funding_accrued_usdc - 664.19).abs() < 1e-2);

    let closed = db.list_closed_positions(10).unwrap();
    let met_closed = closed
        .iter()
        .find(|p| p.client_order_id == "hl-closed-met-9002")
        .expect("MET partial close trade");
    assert!((met_closed.funding_accrued_usdc - 1.38).abs() < 1e-2);
    assert!(
        ((met_closed.funding_accrued_usdc + met_open.funding_accrued_usdc) - 665.57).abs() < 1e-2
    );

    let evs = db.list_funding_events_for_asset(met_id).unwrap();
    assert_eq!(evs.len(), 2);
    let rem_sum: f64 = evs.iter().map(|e| e.remaining_usdc).sum();
    assert!((rem_sum - met_open.funding_accrued_usdc).abs() < 1e-2);

    // Daily (24H) reflects ONLY the in-window open funding (165.57 * 9634/9654 ≈ 165.23) + closed (1.38 - 0.02) ≈ 166.59,
    // NEVER leaking the pre-24h 500.00 tick and NEVER double-counting closed 1.38.
    let pnl = db.compute_pnl_summary(0.0, 0.0, 5000.0).unwrap();
    let expected_daily_open_rem = 165.57 * (9634.0 / 9654.0);
    let expected_daily_fees_funding =
        expected_daily_open_rem + met_closed.funding_accrued_usdc - 0.02;
    assert!(
        (pnl.daily.fees_funding_usdc - expected_daily_fees_funding).abs() < 0.05,
        "daily.fees_funding_usdc={} expected={}",
        pnl.daily.fees_funding_usdc,
        expected_daily_fees_funding
    );
    assert!((pnl.weekly.fees_funding_usdc - 665.55).abs() < 0.05);
}

#[test]
fn test_reconcile_multi_partial_close_funding_conservation() {
    let now = now_ms();
    let t_open = now - 10_000_000;
    let t_f1 = now - 8_000_000;
    let t_c1 = now - 6_000_000;
    let t_f2 = now - 4_000_000;
    let t_c2 = now - 2_000_000;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "2000.0" },
                        "assetPositions": [{
                            "position": {
                                "coin": "ETH",
                                "szi": "50.0",
                                "entryPx": "100.0",
                                "positionValue": "5000.0",
                                "unrealizedPnl": "0.0",
                                "cumFunding": { "sinceOpen": "-140.0" }
                            }
                        }]
                    }),
                ),
                "userFills" | "userFillsByTime" => (
                    200,
                    json!([
                        {
                            "coin": "ETH", "side": "B", "px": "100.0", "sz": "100.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Open Long",
                            "time": t_open, "oid": 100u64, "tid": 1u64, "startPosition": "0.0"
                        },
                        {
                            "coin": "ETH", "side": "A", "px": "100.0", "sz": "20.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Close Long",
                            "time": t_c1, "oid": 101u64, "tid": 2u64, "startPosition": "100.0"
                        },
                        {
                            "coin": "ETH", "side": "A", "px": "100.0", "sz": "30.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Close Long",
                            "time": t_c2, "oid": 102u64, "tid": 3u64, "startPosition": "80.0"
                        }
                    ]),
                ),
                "userFunding" => (
                    200,
                    json!([
                        {
                            "time": t_f1, "hash": "0xf1",
                            "delta": { "type": "funding", "coin": "ETH", "usdc": "100.0", "szi": "100.0", "fundingRate": "0.01" }
                        },
                        {
                            "time": t_f2, "hash": "0xf2",
                            "delta": { "type": "funding", "coin": "ETH", "usdc": "40.0", "szi": "80.0", "fundingRate": "0.005" }
                        }
                    ]),
                ),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());

    engine.aux_sync.sync_startup_before_reconcile();
    engine.reconciler.run_cutover_reconciliation(false).unwrap();

    // P1-Acct: partial IOC slices within the same position lifetime share close_epoch_id = "ETH:1"
    // and aggregate cleanly into one closed trade row (c1 20.0 + c2 45.0 = 65.0 funding, 50.0 qty).
    let closed = db.list_closed_positions(10).unwrap();
    let c_merged = closed
        .iter()
        .find(|p| p.client_order_id == "hl-closed-eth-102")
        .expect("merged close_epoch_id closed trade");
    assert!(
        closed
            .iter()
            .all(|p| p.client_order_id != "hl-closed-eth-101"),
        "partial IOC slice hl-closed-eth-101 must aggregate into hl-closed-eth-102 via close_epoch_id"
    );
    assert!((c_merged.filled_quantity - 50.0).abs() < 1e-6);
    assert!((c_merged.funding_accrued_usdc - 65.0).abs() < 1e-4);

    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    let open = &held[0];
    assert!((open.funding_accrued_usdc - 75.0).abs() < 1e-4);
    assert!(((c_merged.funding_accrued_usdc + open.funding_accrued_usdc) - 140.0).abs() < 1e-4);

    let eth_id = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap().id;
    let evs = db.list_funding_events_for_asset(eth_id).unwrap();
    assert_eq!(evs.len(), 2);
    assert!((evs[0].remaining_usdc - 50.0).abs() < 1e-6);
    assert!((evs[1].remaining_usdc - 25.0).abs() < 1e-6);
}

#[test]
fn test_reconcile_prehistory_start_position_partial_close() {
    let now = now_ms();
    let fund_ts = now - 7_200_000;
    let close_ts = now - 1_800_000;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "2000.0" },
                        "assetPositions": [{
                            "position": {
                                "coin": "SOL",
                                "szi": "9634.0",
                                "entryPx": "1.0",
                                "positionValue": "9634.0",
                                "unrealizedPnl": "0.0",
                                "cumFunding": { "sinceOpen": "-96.54" }
                            }
                        }]
                    }),
                ),
                "userFills" | "userFillsByTime" => (
                    200,
                    json!([
                        {
                            "coin": "SOL",
                            "side": "A",
                            "px": "1.0",
                            "sz": "20.0",
                            "fee": "0.02",
                            "closedPnl": "0.0",
                            "dir": "Close Long",
                            "time": close_ts,
                            "oid": 3001u64,
                            "tid": 1u64,
                            "startPosition": "9654.0"
                        }
                    ]),
                ),
                "userFunding" => {
                    let start_time = body.get("startTime").and_then(Value::as_i64).unwrap_or(0);
                    let list = if fund_ts >= start_time {
                        json!([{
                            "time": fund_ts,
                            "hash": "0xpre1",
                            "delta": {
                                "type": "funding",
                                "coin": "SOL",
                                "usdc": "96.54",
                                "szi": "9654.0",
                                "fundingRate": "0.01"
                            }
                        }])
                    } else {
                        json!([])
                    };
                    (200, list)
                }
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());

    engine.aux_sync.sync_startup_before_reconcile();
    engine.reconciler.run_cutover_reconciliation(false).unwrap();

    let closed = db.list_closed_positions(10).unwrap();
    let sol_closed = closed
        .iter()
        .find(|p| p.client_order_id == "hl-closed-sol-3001")
        .expect("SOL pre-history partial close");
    assert!((sol_closed.funding_accrued_usdc - 0.20).abs() < 1e-4);

    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert!((held[0].funding_accrued_usdc - 96.34).abs() < 1e-4);

    // Second reconcile must remain idempotent even when DB stored opened_at_ms >= first_fill.time_ms
    engine.reconciler.run_cutover_reconciliation(false).unwrap();
    let closed_2 = db.list_closed_positions(10).unwrap();
    let sol_closed_2 = closed_2
        .iter()
        .find(|p| p.client_order_id == "hl-closed-sol-3001")
        .expect("SOL pre-history partial close on second reconcile");
    assert!((sol_closed_2.funding_accrued_usdc - 0.20).abs() < 1e-4);
    let held_2 = db.get_assets_held().unwrap();
    assert_eq!(held_2.len(), 1);
    assert!((held_2[0].funding_accrued_usdc - 96.34).abs() < 1e-4);
    let sol_id = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap().id;
    let sol_evs = db.list_funding_events_for_asset(sol_id).unwrap();
    assert_eq!(sol_evs.len(), 1);
    assert!((sol_evs[0].remaining_usdc - 96.34).abs() < 1e-4);
}

#[test]
fn test_reconcile_user_funding_429_does_not_clobber_reconciled_funding() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let fail_funding = Arc::new(AtomicBool::new(false));
    let fail_srv = fail_funding.clone();
    let now = now_ms();
    let open_ts = now - 7_200_000;
    let fund_ts = now - 3_600_000;
    let close_ts = now - 1_800_000;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "2000.0" },
                        "assetPositions": [{
                            "position": {
                                "coin": "ETH",
                                "szi": "80.0",
                                "entryPx": "100.0",
                                "positionValue": "8000.0",
                                "unrealizedPnl": "0.0",
                                "cumFunding": { "sinceOpen": "-100.0" }
                            }
                        }]
                    }),
                ),
                "userFills" | "userFillsByTime" => (
                    200,
                    json!([
                        {
                            "coin": "ETH", "side": "B", "px": "100.0", "sz": "100.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Open Long",
                            "time": open_ts, "oid": 5001u64, "tid": 1u64, "startPosition": "0.0"
                        },
                        {
                            "coin": "ETH", "side": "A", "px": "100.0", "sz": "20.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Close Long",
                            "time": close_ts, "oid": 5002u64, "tid": 2u64, "startPosition": "100.0"
                        }
                    ]),
                ),
                "userFunding" => {
                    if fail_srv.load(Ordering::SeqCst) {
                        return (429, json!({ "error": "rate_limited" }));
                    }
                    (
                        200,
                        json!([{
                            "time": fund_ts, "hash": "0x429ok",
                            "delta": { "type": "funding", "coin": "ETH", "usdc": "100.0", "szi": "100.0", "fundingRate": "0.01" }
                        }]),
                    )
                }
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());

    // 1. Initial aux funding backfill + reconcile succeeds and books +20.0 closed + +80.0 open
    engine.aux_sync.sync_startup_before_reconcile();
    engine.reconciler.run_cutover_reconciliation(false).unwrap();
    let closed1 = db.list_closed_positions(10).unwrap();
    let eth_c1 = closed1
        .iter()
        .find(|p| p.client_order_id == "hl-closed-eth-5002")
        .unwrap();
    assert!((eth_c1.funding_accrued_usdc - 20.0).abs() < 1e-4);
    assert!((db.get_assets_held().unwrap()[0].funding_accrued_usdc - 80.0).abs() < 1e-4);
    let eth_id = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap().id;
    assert_eq!(db.list_funding_events_for_asset(eth_id).unwrap().len(), 1);

    // 2. Subsequent aux sync + reconcile hits HTTP 429 on userFunding -> must NOT zero closed trade or double-count open position
    fail_funding.store(true, Ordering::SeqCst);
    engine.aux_sync.run_once(false);
    engine.reconciler.run_cutover_reconciliation(false).unwrap();

    let closed2 = db.list_closed_positions(10).unwrap();
    let eth_c2 = closed2
        .iter()
        .find(|p| p.client_order_id == "hl-closed-eth-5002")
        .unwrap();
    assert!(
        (eth_c2.funding_accrued_usdc - 20.0).abs() < 1e-4,
        "429 must not clobber closed trade funding to 0.0"
    );
    assert!(
        (db.get_assets_held().unwrap()[0].funding_accrued_usdc - 80.0).abs() < 1e-4,
        "429 must not overwrite open position funding to 100.0"
    );
    let evs_after = db.list_funding_events_for_asset(eth_id).unwrap();
    assert_eq!(evs_after.len(), 1);
    assert!((evs_after[0].remaining_usdc - 80.0).abs() < 1e-6);
}

#[test]
fn test_option_b_snapshot_capping_spot_60s_sync_and_15s_sleeve_preservation() {
    use aegis_core::VaultInfoPayload;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let spot_calls = Arc::new(AtomicUsize::new(0));
    let fail_spot = Arc::new(AtomicBool::new(false));
    let spot_srv = spot_calls.clone();
    let fail_spot_srv = fail_spot.clone();
    let hlp_addr = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": {
                            "accountValue": "10944.72",
                            "totalMarginUsed": "1250.50"
                        },
                        "withdrawable": "8200.0",
                        "assetPositions": [{
                            "position": {
                                "coin": "ETH",
                                "szi": "0.1",
                                "entryPx": "3400.0",
                                "positionValue": "340.0",
                                "unrealizedPnl": "0.0",
                                "cumFunding": { "sinceOpen": "0.0" }
                            }
                        }]
                    }),
                ),
                "frontendOpenOrders" => (
                    200,
                    json!([
                        {
                            "coin": "ETH",
                            "oid": 70001u64,
                            "isTrigger": true,
                            "orderType": "Stop Market",
                            "reduceOnly": true,
                            "triggerPx": "3200.0",
                            "timestamp": 1_795_000_000_000u64
                        }
                    ]),
                ),
                "spotClearinghouseState" => {
                    spot_srv.fetch_add(1, Ordering::SeqCst);
                    if fail_spot_srv.load(Ordering::SeqCst) {
                        return (429, json!({ "error": "rate_limited" }));
                    }
                    (
                        200,
                        json!({
                            "balances": [
                                { "coin": "USDC", "total": "5000.0", "hold": "0.0" }
                            ]
                        }),
                    )
                }
                "userVaultEquities" => (
                    200,
                    json!([{
                        "vaultAddress": hlp_addr,
                        "equity": "202.40",
                        "lockedUntilTimestamp": 0
                    }]),
                ),
                "vaultDetails" => (
                    200,
                    json!({
                        "name": "HLP",
                        "vaultAddress": hlp_addr,
                        "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
                        "isClosed": false,
                        "apr": 0.08,
                        "followerState": {
                            "vaultEquity": "202.40",
                            "pnl": "1.0",
                            "allTimePnl": "2.0",
                            "daysFollowing": 10,
                            "lockupUntil": 0
                        }
                    }),
                ),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());
    db.set_config_value("risk_capital_cap_usdc", "1000.0", None)
        .unwrap();

    // Seed ETH SL OID and sleeve weights so reconcile protects resting trigger order and counts it in live_exchange_open_orders_count
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let eth_pid = db
        .create_working_order(&aegis_db::WorkingOrderInput {
            asset_id: eth.id,
            client_order_id: "test-eth-optb-open".to_string(),
            side: OrderSide::Long,
            limit_price: 3400.0,
            requested_quantity: 0.1,
            reserved_margin_usdc: 170.0,
            portfolio_weight: 0.34,
            s1_weight_contrib: 0.34,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 3200.0,
            take_profit_px: None,
            exchange_order_id: Some("70000".to_string()),
            reason: "OPTION_B_SEED".to_string(),
        })
        .unwrap();
    db.promote_working_to_open(
        eth_pid,
        3400.0,
        0.1,
        0.0,
        3200.0,
        None,
        Some("70000"),
        "TEST_OPEN",
    )
    .unwrap();
    db.update_position_tpsl_orders(
        eth_pid,
        3200.0,
        OidUpdate::Set("70001"),
        3800.0,
        OidUpdate::Clear,
    )
    .unwrap();

    let rep = engine.reconciler.run_cutover_reconciliation(false).unwrap();
    engine.aux_sync.sync_post_reconcile_views();
    assert!((rep.exchange_equity_usdc - 10944.72).abs() < 1e-6);

    let cfg = db.load_config_or_default();
    assert!((cfg.get_f64("live_exchange_equity_usdc", 0.0) - 10944.72).abs() < 1e-6);
    assert!((cfg.get_f64("live_exchange_spot_usdc", 0.0) - 5000.0).abs() < 1e-6);
    assert!((cfg.get_f64("live_exchange_margin_used_usdc", 0.0) - 1250.50).abs() < 1e-6);
    assert!((cfg.get_f64("live_exchange_withdrawable_usdc", 0.0) - 8200.0).abs() < 1e-6);
    assert!(cfg.get_i64("live_exchange_equity_synced_at_ms", 0) > 0);
    let spot_sync_ts = cfg.get_i64("live_exchange_spot_synced_at_ms", 0);
    assert!(spot_sync_ts > 0);
    assert_eq!(cfg.get_i64("live_exchange_open_orders_count", 0), 1);

    // Reconcile weight used Option B cap ($1000 -> 340/1000 = 0.34)
    let held_after = db.get_assets_held().unwrap();
    assert!((held_after[0].portfolio_weight - 0.34).abs() < 1e-4);

    // Combined portfolio equity = perp (10944.72) + spot (5000.0) + vault (202.40) == 16147.12
    let vault_info: VaultInfoPayload =
        serde_json::from_str(&cfg.get_str("live_vault_info_json", "")).unwrap();
    assert!((vault_info.combined_portfolio_equity_usdc - 16147.12).abs() < 1e-6);

    // Immediate second reconcile is throttled on spot (still 1 HTTP call)
    assert_eq!(spot_calls.load(Ordering::SeqCst), 1);
    engine.reconciler.run_cutover_reconciliation(false).unwrap();
    assert_eq!(spot_calls.load(Ordering::SeqCst), 1);

    // After >= 60s, a 429 on spotClearinghouseState preserves cached 5000.0
    fail_spot.store(true, Ordering::SeqCst);
    let future_ts = spot_sync_ts + aegis_execution::VAULT_SYNC_MIN_INTERVAL_MS + 1_000;
    assert!(engine
        .reconciler
        .sync_spot_usdc_if_due(&cfg, future_ts)
        .is_none());
    assert!(spot_calls.load(Ordering::SeqCst) >= 2);
    let cfg_after_spot_429 = db.load_config_or_default();
    assert!((cfg_after_spot_429.get_f64("live_exchange_spot_usdc", 0.0) - 5000.0).abs() < 1e-6);

    // When previous snapshot has all-zero sleeve weights, signal_out == None falls back to held sleeve sums
    let snap_zero_prev = engine.record_portfolio_snapshot(None, None).unwrap();
    assert!((snap_zero_prev.s1_gross_weight - 0.34).abs() < 1e-6);
    assert!((snap_zero_prev.s2_gross_weight - 0.0).abs() < 1e-6);
    assert!((snap_zero_prev.s3_gross_weight - 0.0).abs() < 1e-6);

    // Option B snapshot capping: snapshot.total_equity_usdc == 1000.0 (cap), NOT 10944.72
    let mut sig_out = make_signal_output(vec![]);
    sig_out.s1_gross_weight = 0.50;
    sig_out.s2_gross_weight = 0.30;
    sig_out.s3_gross_weight = 0.20;
    let snap1 = engine
        .record_portfolio_snapshot(None, Some(&sig_out))
        .unwrap();
    assert!((snap1.total_equity_usdc - 1000.0).abs() < 1e-6);
    assert!((snap1.s1_gross_weight - 0.50).abs() < 1e-6);
    assert!((snap1.s2_gross_weight - 0.30).abs() < 1e-6);
    assert!((snap1.s3_gross_weight - 0.20).abs() < 1e-6);

    // 15s snapshot with signal_out == None preserves ALL 3 previous signal sleeve weights
    // instead of mixing held S1 (0.34) with target S2 (0.30) and S3 (0.20)
    let snap_15s = engine.record_portfolio_snapshot(None, None).unwrap();
    assert!((snap_15s.total_equity_usdc - 1000.0).abs() < 1e-6);
    assert!((snap_15s.s1_gross_weight - 0.50).abs() < 1e-6);
    assert!((snap_15s.s2_gross_weight - 0.30).abs() < 1e-6);
    assert!((snap_15s.s3_gross_weight - 0.20).abs() < 1e-6);

    // Also verify withdrawable constraint caps available_cash_usdc when exchange withdrawable < budget_idle
    db.set_config_value("live_exchange_withdrawable_usdc", "150.0", None)
        .unwrap();
    let snap_capped_cash = engine.record_portfolio_snapshot(None, None).unwrap();
    assert!((snap_capped_cash.available_cash_usdc - 150.0).abs() < 1e-6);
}

#[test]
fn test_reconcile_multi_lot_dca_partial_close_funding_conservation_and_pnl() {
    let now = now_ms();
    let t_open_1 = now - 8 * 86_400_000;
    let t_fund_e1 = now - 2 * 86_400_000;
    let t_add_2 = now - 86_400_000;
    let t_close_1 = now - 12 * 3_600_000;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "5000.0" },
                        "assetPositions": [{
                            "position": {
                                "coin": "ETH",
                                "szi": "300.0",
                                "entryPx": "100.0",
                                "positionValue": "30000.0",
                                "unrealizedPnl": "0.0",
                                "cumFunding": { "sinceOpen": "-100.0" }
                            }
                        }]
                    }),
                ),
                "userFills" | "userFillsByTime" => (
                    200,
                    json!([
                        {
                            "coin": "ETH", "side": "B", "px": "100.0", "sz": "100.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Open Long",
                            "time": t_open_1, "oid": 6001u64, "tid": 1u64, "startPosition": "0.0"
                        },
                        {
                            "coin": "ETH", "side": "B", "px": "100.0", "sz": "300.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Open Long",
                            "time": t_add_2, "oid": 6002u64, "tid": 2u64, "startPosition": "100.0"
                        },
                        {
                            "coin": "ETH", "side": "A", "px": "100.0", "sz": "100.0",
                            "fee": "0.0", "closedPnl": "0.0", "dir": "Close Long",
                            "time": t_close_1, "oid": 6003u64, "tid": 3u64, "startPosition": "400.0"
                        }
                    ]),
                ),
                "userFunding" => (
                    200,
                    json!([{
                        "time": t_fund_e1,
                        "hash": "0xdca1",
                        "delta": {
                            "type": "funding",
                            "coin": "ETH",
                            "usdc": "100.0",
                            "szi": "100.0",
                            "fundingRate": "0.01"
                        }
                    }]),
                ),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());

    engine.aux_sync.sync_startup_before_reconcile();
    engine.reconciler.run_cutover_reconciliation(false).unwrap();

    // Lot 1 (100) accrued E1 = +100.0, Lot 2 (300) accrued 0.0.
    // Partial close 100 closes all of Lot 1 -> closed books +100.0, open has 0.0, E1 remaining_usdc == 0.0.
    let closed = db.list_closed_positions(10).unwrap();
    let eth_c = closed
        .iter()
        .find(|p| p.client_order_id == "hl-closed-eth-6003")
        .expect("DCA Lot 1 close trade");
    assert!((eth_c.funding_accrued_usdc - 100.0).abs() < 1e-4);

    let held = db.get_assets_held().unwrap();
    assert_eq!(held.len(), 1);
    assert!((held[0].funding_accrued_usdc - 0.0).abs() < 1e-4);

    let eth_id = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap().id;
    let evs = db.list_funding_events_for_asset(eth_id).unwrap();
    assert_eq!(evs.len(), 1);
    assert!((evs[0].remaining_usdc - 0.0).abs() < 1e-6);

    // Weekly (7D) must report exactly +100.00 (no +75.00 double-count from uniform keep_ratio).
    let pnl = db.compute_pnl_summary(0.0, 0.0, 5000.0).unwrap();
    assert!(
        (pnl.weekly.fees_funding_usdc - 100.0).abs() < 1e-2,
        "weekly.fees_funding_usdc={} expected=100.0",
        pnl.weekly.fees_funding_usdc
    );
    assert!((pnl.daily.fees_funding_usdc - 100.0).abs() < 1e-2);
}

#[test]
fn test_unified_account_equity_no_double_count_and_hl_portfolio_pnl_sync() {
    let hlp_addr = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0";
    let now = now_ms();
    let cap_ms = now - 3 * 86_400_000;
    let d1_start = now - 86_400_000 - 3_600_000;
    let w1_start = now - 7 * 86_400_000;

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "userAbstraction" => (200, json!("unifiedAccount")),
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": {
                            "accountValue": "679.151704",
                            "totalNtlPos": "2267.552108",
                            "totalMarginUsed": "675.765769"
                        },
                        "withdrawable": "0.0",
                        "assetPositions": []
                    }),
                ),
                "spotClearinghouseState" => (
                    200,
                    json!({
                        "balances": [
                            { "coin": "USDC", "token": 0, "total": "10978.372686", "hold": "680.047375", "entryNtl": "0.0" }
                        ],
                        "tokenToAvailableAfterMaintenance": [[0, "10641.560205"]]
                    }),
                ),
                "userVaultEquities" => (
                    200,
                    json!([{ "vaultAddress": hlp_addr, "equity": "202.47", "lockedUntilTimestamp": 0 }]),
                ),
                "vaultDetails" => (
                    200,
                    json!({
                        "name": "HLP",
                        "vaultAddress": hlp_addr,
                        "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
                        "isClosed": false,
                        "apr": 0.0845,
                        "followerState": {
                            "vaultEquity": "202.47",
                            "pnl": "0.99",
                            "allTimePnl": "1.41",
                            "daysFollowing": 26,
                            "lockupUntil": 0
                        }
                    }),
                ),
                "portfolio" => (
                    200,
                    json!([
                        ["perpDay", {
                            "pnlHistory": [[d1_start, "0.0"], [now - 60_000, "242.70"]],
                            "vlm": "1412.91"
                        }],
                        ["perpWeek", {
                            "pnlHistory": [
                                [w1_start, "0.0"],
                                [cap_ms - 3_600_000, "-100.0"],
                                [cap_ms + 3_600_000, "-300.0"],
                                [now - 60_000, "362.52"]
                            ],
                            "vlm": "64604.1"
                        }],
                        ["perpMonth", {
                            "pnlHistory": [
                                [w1_start, "0.0"],
                                [cap_ms - 3_600_000, "-100.0"],
                                [cap_ms + 3_600_000, "-300.0"],
                                [now - 60_000, "362.52"]
                            ],
                            "vlm": "83291.08"
                        }],
                        ["perpAllTime", {
                            "pnlHistory": [
                                [w1_start, "0.0"],
                                [cap_ms - 3_600_000, "-100.0"],
                                [cap_ms + 3_600_000, "-300.0"],
                                [now - 60_000, "362.52"]
                            ],
                            "vlm": "83291.08"
                        }]
                    ]),
                ),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());
    db.set_config_value("risk_capital_cap_usdc", "1000.0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_updated_at_ms", &cap_ms.to_string(), None)
        .unwrap();

    let rep = engine.reconciler.run_cutover_reconciliation(false).unwrap();
    engine.aux_sync.sync_post_reconcile_views();
    assert!((rep.exchange_equity_usdc - 10978.37).abs() < 1e-6);

    let cfg = db.load_config_or_default();
    assert_eq!(
        cfg.get_str("live_exchange_account_mode", ""),
        "unifiedAccount"
    );
    assert!((cfg.get_f64("live_exchange_perp_margin_equity_usdc", 0.0) - 679.15).abs() < 1e-6);
    assert!((cfg.get_f64("live_exchange_equity_usdc", 0.0) - 10978.37).abs() < 1e-6);
    assert!((cfg.get_f64("live_exchange_spot_usdc", 0.0) - 10978.37).abs() < 1e-6);
    // Free spot collateral = total (10978.37) - hold (680.05) = 10298.32
    assert!((cfg.get_f64("live_exchange_withdrawable_usdc", 0.0) - 10298.32).abs() < 0.02);

    // Vault combined equity must NOT double-count $679.15 perp margin on unified accounts: 10978.37 + 202.47 = 11180.84
    let vault_info: aegis_core::VaultInfoPayload =
        serde_json::from_str(&cfg.get_str("live_vault_info_json", "")).unwrap();
    assert!((vault_info.combined_portfolio_equity_usdc - 11180.84).abs() < 1e-6);

    // Option B snapshot uses capped $1,000.00 equity with available_cash == $1,000.00 (no positions)
    let snap = engine.record_portfolio_snapshot(None, None).unwrap();
    assert!((snap.total_equity_usdc - 1000.0).abs() < 1e-6);
    assert!((snap.available_cash_usdc - 1000.0).abs() < 1e-6);

    // Hybrid HL portfolio PnL: daily = 242.70 (HL_PORTFOLIO), weekly clamped = 562.52
    let pnl = db
        .compute_pnl_summary(0.0, 0.0, snap.total_equity_usdc)
        .unwrap();
    assert_eq!(pnl.daily.pnl_source, "HL_PORTFOLIO");
    assert!((pnl.daily.net_pnl_usdc - 242.70).abs() < 1e-6);
    assert!(!pnl.daily.is_cap_clamped);
    assert_eq!(pnl.weekly.pnl_source, "HL_PORTFOLIO");
    assert!(pnl.weekly.is_cap_clamped);
    assert!((pnl.weekly.net_pnl_usdc - 562.52).abs() < 1e-4);
}

#[test]
fn test_execute_signal_rebalance_profit_gates_signal_close_and_flip() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    seed_test_assets(&db);

    // R14-M10: profit_gate_signal_exits defaults to false; opt in explicitly below to test CLOSE gating + FLIP exemption
    assert!(
        !db.load_config_or_default()
            .get_bool("profit_gate_signal_exits", true),
        "profit_gate_signal_exits must default to false (R14-M10)"
    );
    db.set_config_value("profit_gate_signal_exits", "true", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "300", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("risk_capital_cap_updated_at_ms", "0", None)
        .unwrap();

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let arb = db.get_asset_by_symbol("ARB-USDC").unwrap().unwrap();
    let avax = db.get_asset_by_symbol("AVAX-USDC").unwrap().unwrap();
    let link = db.get_asset_by_symbol("LINK-USDC").unwrap().unwrap();

    let mature_opened_ms = now_ms() - 600_000; // 10 min > 300s hold floor

    // 1. SOL LONG underwater (entry 186.0 > mark 184.0) -> blocked from ACTION_CLOSE
    let (sol_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: &sol.coin,
            side: OrderSide::Long,
            entry_price: sol.mark_price + 2.0,
            mark_price: sol.mark_price,
            quantity: 5.0,
            committed_notional_usdc: 5.0 * sol.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.10,
            protective_stop_px: sol.mark_price * 0.90,
            take_profit_px: sol.mark_price * 1.15,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(mature_opened_ms),
        })
        .unwrap();

    // 2. ETH LONG flat (entry == mark) -> ACTION_FLIP unconditionally exempt from profit gate (R14-M10)
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth.id,
            coin: &eth.coin,
            side: OrderSide::Long,
            entry_price: eth.mark_price,
            mark_price: eth.mark_price,
            quantity: 0.5,
            committed_notional_usdc: 0.5 * eth.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.15,
            protective_stop_px: eth.mark_price * 0.90,
            take_profit_px: eth.mark_price * 1.15,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(mature_opened_ms),
        })
        .unwrap();

    // 3. ARB LONG marginally positive (~0.10% < 0.25% + exit fee) -> blocked from ACTION_CLOSE
    let (arb_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: arb.id,
            coin: &arb.coin,
            side: OrderSide::Long,
            entry_price: arb.mark_price * 0.999,
            mark_price: arb.mark_price,
            quantity: 100.0,
            committed_notional_usdc: 100.0 * arb.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.05,
            protective_stop_px: arb.mark_price * 0.90,
            take_profit_px: arb.mark_price * 1.15,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(mature_opened_ms),
        })
        .unwrap();

    // 4. AVAX LONG clearly profitable (+2.5% >> 0.25% + fees) -> ALLOWED to ACTION_CLOSE
    let (avax_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: avax.id,
            coin: &avax.coin,
            side: OrderSide::Long,
            entry_price: avax.mark_price * 0.975,
            mark_price: avax.mark_price,
            quantity: 20.0,
            committed_notional_usdc: 20.0 * avax.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.10,
            protective_stop_px: avax.mark_price * 0.90,
            take_profit_px: avax.mark_price * 1.15,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(mature_opened_ms),
        })
        .unwrap();

    // 5. LINK SHORT clearly profitable (+3.0% >> 0.25% + fees) -> ALLOWED to ACTION_FLIP
    let (link_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: link.id,
            coin: &link.coin,
            side: OrderSide::Short,
            entry_price: link.mark_price * 1.03,
            mark_price: link.mark_price,
            quantity: 40.0,
            committed_notional_usdc: 40.0 * link.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.10,
            protective_stop_px: link.mark_price * 1.15,
            take_profit_px: link.mark_price * 0.85,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(mature_opened_ms),
        })
        .unwrap();

    let sigs = make_signal_output(vec![
        make_signal_input(&sol, 0.0, 0.10, ACTION_CLOSE),
        make_signal_input(&eth, -0.20, 0.15, ACTION_FLIP),
        make_signal_input(&arb, 0.0, 0.05, ACTION_CLOSE),
        make_signal_input(&avax, 0.0, 0.10, ACTION_CLOSE),
        make_signal_input(&link, 0.20, -0.10, ACTION_FLIP),
    ]);
    let reb = engine.execute_signal_rebalance(&sigs, 10_000.0).unwrap();
    assert_eq!(reb.positions_closed, 3);
    // Single-cycle entry cap places 1 working order per cycle; both ETH & LINK flips bypassed profit gate and closed.
    assert_eq!(reb.working_orders_placed, 1);

    assert_eq!(
        db.get_position_by_id(avax_pid).unwrap().unwrap().status,
        "CLOSED"
    );
    assert_eq!(
        db.get_position_by_id(link_pid).unwrap().unwrap().status,
        "CLOSED"
    );
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "CLOSED",
        "ACTION_FLIP must bypass profit_gate_signal_exits"
    );
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "OPEN"
    );
    assert_eq!(
        db.get_position_by_id(arb_pid).unwrap().unwrap().status,
        "OPEN"
    );

    let ordered_after_1 = db.get_assets_ordered().unwrap();
    assert_eq!(ordered_after_1.len(), 1);
    let follow_up_action = if ordered_after_1[0].symbol == "ETH-USDC" {
        make_signal_input(&link, 0.20, 0.0, ACTION_OPEN_LONG)
    } else {
        make_signal_input(&eth, -0.20, 0.0, "OPEN_SHORT")
    };
    let reb_follow = engine
        .execute_signal_rebalance(&make_signal_output(vec![follow_up_action]), 10_000.0)
        .unwrap();
    assert_eq!(reb_follow.working_orders_placed, 1);

    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 2);
    assert!(ordered
        .iter()
        .any(|o| o.symbol == "ETH-USDC" && o.side == "SHORT"));
    assert!(ordered
        .iter()
        .any(|o| o.symbol == "LINK-USDC" && o.side == "LONG"));

    // Manual close_or_cancel_position remains unrestricted on underwater holdings
    let manual_rep = engine
        .close_or_cancel_position(arb_pid, "MANUAL_OVERRIDE_TEST", Some(0))
        .unwrap();
    assert_eq!(manual_rep.final_status, "CLOSED");

    // Setting profit_gate_signal_exits=false restores unrestricted mature ACTION_CLOSE exits
    db.set_config_value("profit_gate_signal_exits", "false", None)
        .unwrap();
    let sigs_unrestricted =
        make_signal_output(vec![make_signal_input(&sol, 0.0, 0.10, ACTION_CLOSE)]);
    let reb_off = engine
        .execute_signal_rebalance(&sigs_unrestricted, 10_000.0)
        .unwrap();
    assert_eq!(reb_off.positions_closed, 1);
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "CLOSED"
    );
}

#[test]
fn test_wave2_concurrent_reconciler_does_not_block_1s_priority_tick_r14_h2() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    let entered_http = Arc::new(AtomicBool::new(false));
    let entered_srv = entered_http.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => {
                    entered_srv.store(true, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(350));
                    (
                        200,
                        json!({
                            "marginSummary": {
                                "accountValue": "2500.0",
                                "totalMarginUsed": "0.0",
                                "withdrawable": "2500.0"
                            },
                            "assetPositions": []
                        }),
                    )
                }
                "userAbstraction" => (200, Value::String("default".to_string())),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());

    let mut prio_tick_elapsed = Duration::from_secs(99);
    let mut lock_free_during_http = false;

    std::thread::scope(|s| {
        let eng_ref = &engine;
        let rec_handle = s.spawn(move || {
            eng_ref
                .reconciler
                .run_cutover_reconciliation(false)
                .expect("reconcile completes cleanly")
        });

        let wait_deadline = Instant::now() + Duration::from_secs(2);
        while !entered_http.load(Ordering::SeqCst) && Instant::now() < wait_deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            entered_http.load(Ordering::SeqCst),
            "reconciler must reach HTTP IO phase"
        );
        std::thread::sleep(Duration::from_millis(40));

        // execution_lock() MUST NOT be held while clearinghouseState HTTP sleeps
        lock_free_during_http = aegis_execution::execution_lock().try_lock().is_some();

        let t0 = Instant::now();
        let _prio_rep = engine
            .priority_monitor
            .run_1s_priority_tick()
            .expect("1s priority tick runs while reconciler HTTP IO sleeps");
        prio_tick_elapsed = t0.elapsed();

        let rep = rec_handle.join().expect("reconciler thread joins");
        assert!((rep.exchange_equity_usdc - 2500.0).abs() < 1e-6);
    });

    assert!(
        lock_free_during_http,
        "execution_lock() must stay released during exchange HTTP IO (R14-H2)"
    );
    assert!(
        prio_tick_elapsed < Duration::from_millis(200),
        "1s priority tick took {:?}; blocked behind 350ms reconciler HTTP!",
        prio_tick_elapsed
    );
}

#[test]
fn test_wave2_ruin_halt_blocks_entries_emits_critical_alert_and_allows_exits_r14_m12() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    seed_test_assets(&db);

    db.set_config_value("initial_paper_equity_usdc", "42.0", None)
        .unwrap();
    db.set_config_value("min_equity_halt_usdc", "50.0", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "0", None)
        .unwrap();
    db.set_config_value("profit_gate_signal_exits", "false", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let (sol_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: &sol.coin,
            side: OrderSide::Long,
            entry_price: sol.mark_price,
            mark_price: sol.mark_price,
            quantity: 0.2,
            committed_notional_usdc: 0.2 * sol.mark_price,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.20,
            protective_stop_px: sol.mark_price * 0.90,
            take_profit_px: sol.mark_price * 1.10,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 600_000),
        })
        .unwrap();

    // Signal both closes SOL and tries to open ETH while equity ($42) < min_equity_halt_usdc ($50)
    let sigs = make_signal_output(vec![
        make_signal_input(&sol, 0.0, 0.20, ACTION_CLOSE),
        make_signal_input(&eth, 0.25, 0.0, ACTION_OPEN_LONG),
    ]);
    let reb = engine.execute_signal_rebalance(&sigs, 42.0).unwrap();

    assert_eq!(
        reb.positions_closed, 1,
        "CLOSE must still be allowed under RUIN_HALT"
    );
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "CLOSED"
    );
    assert_eq!(
        reb.working_orders_placed, 0,
        "RUIN_HALT must block all new entries"
    );
    assert!(db.get_assets_ordered().unwrap().is_empty());

    let alerts = db.list_recent_alerts(20).unwrap();
    assert!(
        alerts
            .iter()
            .any(|a| a.event_type == "RUIN_HALT" && a.severity == "CRITICAL"),
        "RUIN_HALT must enqueue CRITICAL alert; alerts={alerts:?}"
    );
}

#[test]
fn test_wave2_early_loser_invalidation_bypasses_900s_hold_and_protects_winners_p1_strat() {
    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    db.set_config_value("execution_mode", "PAPER", None)
        .unwrap();
    seed_test_assets(&db);

    db.set_config_value("min_position_hold_sec", "900", None)
        .unwrap();
    db.set_config_value("early_loser_invalidation_enabled", "true", None)
        .unwrap();
    db.set_config_value("profit_gate_signal_exits", "false", None)
        .unwrap();

    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let avax = db.get_asset_by_symbol("AVAX-USDC").unwrap().unwrap();
    let now = now_ms();

    // 1. SOL held 200s (120s <= 200s < 900s) and underwater -1.0% (<= -0.3%) -> BYPASSES 900s floor
    let (sol_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: sol.id,
            coin: &sol.coin,
            side: OrderSide::Long,
            entry_price: sol.mark_price * 1.01,
            mark_price: sol.mark_price,
            quantity: 1.0,
            committed_notional_usdc: sol.mark_price,
            unrealized_pnl_usdc: -1.8,
            portfolio_weight: 0.10,
            protective_stop_px: sol.mark_price * 0.90,
            take_profit_px: sol.mark_price * 1.15,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 200_000),
        })
        .unwrap();

    // 2. ETH held 200s (< 900s) and profitable +0.5% -> MUST respect 900s hold floor
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth.id,
            coin: &eth.coin,
            side: OrderSide::Long,
            entry_price: eth.mark_price * 0.995,
            mark_price: eth.mark_price,
            quantity: 0.1,
            committed_notional_usdc: 0.1 * eth.mark_price,
            unrealized_pnl_usdc: 1.7,
            portfolio_weight: 0.10,
            protective_stop_px: eth.mark_price * 0.90,
            take_profit_px: eth.mark_price * 1.15,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 200_000),
        })
        .unwrap();

    // 3. AVAX held 60s (< 120s minimum floor) even though underwater -1.0% -> MUST hold
    let (avax_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: avax.id,
            coin: &avax.coin,
            side: OrderSide::Long,
            entry_price: avax.mark_price * 1.01,
            mark_price: avax.mark_price,
            quantity: 5.0,
            committed_notional_usdc: 5.0 * avax.mark_price,
            unrealized_pnl_usdc: -1.75,
            portfolio_weight: 0.05,
            protective_stop_px: avax.mark_price * 0.90,
            take_profit_px: avax.mark_price * 1.15,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 60_000),
        })
        .unwrap();

    let sigs = make_signal_output(vec![
        make_signal_input(&sol, 0.0, 0.10, ACTION_CLOSE),
        make_signal_input(&eth, 0.0, 0.10, ACTION_CLOSE),
        make_signal_input(&avax, 0.0, 0.05, ACTION_CLOSE),
    ]);
    let reb = engine.execute_signal_rebalance(&sigs, 10_000.0).unwrap();
    assert_eq!(reb.positions_closed, 1);
    assert_eq!(
        db.get_position_by_id(sol_pid).unwrap().unwrap().status,
        "CLOSED"
    );
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "OPEN"
    );
    assert_eq!(
        db.get_position_by_id(avax_pid).unwrap().unwrap().status,
        "OPEN"
    );
}

#[test]
fn test_wave2_2tick_auto_deleverage_recovery_cancels_liquidation_and_fresh_equity_p0_risk() {
    let ch_equity = Arc::new(Mutex::new("100.0".to_string()));
    let ch_eq_srv = ch_equity.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => {
                    let eq = ch_eq_srv.lock().clone();
                    (
                        200,
                        json!({
                            "marginSummary": {
                                "accountValue": eq,
                                "totalMarginUsed": "0.0",
                                "withdrawable": eq
                            },
                            "assetPositions": [{
                                "position": {
                                    "coin": "ETH",
                                    "szi": "0.1",
                                    "entryPx": "3400.0",
                                    "positionValue": "340.0",
                                    "unrealizedPnl": "0.0"
                                }
                            }]
                        }),
                    )
                }
                "userAbstraction" => (200, Value::String("default".to_string())),
                "l2Book" => (
                    200,
                    json!({
                        "levels": [
                            [{"px": "3399.5", "sz": "10.0"}],
                            [{"px": "3400.5", "sz": "10.0"}]
                        ]
                    }),
                ),
                _ => (200, json!([])),
            };
        }
        (
            200,
            json!({
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": { "statuses": [{ "filled": { "oid": 88001u64, "totalSz": "0.1", "avgPx": "3400.0" } }] }
                }
            }),
        )
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());
    db.set_config_value(
        "private_key",
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        None,
    )
    .unwrap();
    db.set_config_value("risk_tier", "extreme", None).unwrap();
    db.set_config_value("max_gross_leverage_x", "1.0", None)
        .unwrap();
    db.set_config_value("min_position_hold_sec", "0", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "100.0", None)
        .unwrap();
    db.set_config_value("live_exchange_equity_synced_at_ms", "0", None)
        .unwrap();

    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth.id,
            coin: &eth.coin,
            side: OrderSide::Long,
            entry_price: 3400.0,
            mark_price: 3400.0,
            quantity: 0.1,
            committed_notional_usdc: 340.0,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.34,
            protective_stop_px: 3100.0,
            take_profit_px: 3700.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 600_000),
        })
        .unwrap();
    let sol_work_pid = db
        .create_working_order(&aegis_db::WorkingOrderInput {
            asset_id: sol.id,
            client_order_id: "test-work-cancel-first".to_string(),
            side: OrderSide::Long,
            limit_price: 184.25,
            requested_quantity: 1.0,
            reserved_margin_usdc: 50.0,
            portfolio_weight: 0.18,
            s1_weight_contrib: 0.18,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 170.0,
            take_profit_px: None,
            exchange_order_id: None,
            reason: "WORK_CANCEL_IMMEDIATE".to_string(),
        })
        .unwrap();

    // Case A: Tick 1 cancels WORKING order immediately, leaves OPEN ETH intact, refreshes equity
    let tick1 = engine.enforce_leverage_and_position_caps().unwrap();
    assert_eq!(tick1.len(), 1);
    assert_eq!(tick1[0].position_id, sol_work_pid);
    assert_eq!(tick1[0].final_status, "CANCELLED");
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "OPEN"
    );

    // Simulate equity recovery (500.0) on exchange before Tick 2 -> breach cleared cleanly
    *ch_equity.lock() = "500.0".to_string();
    db.set_config_value("live_exchange_equity_usdc", "500.0", None)
        .unwrap();
    let tick_recover = engine.enforce_leverage_and_position_caps().unwrap();
    assert!(tick_recover.is_empty());
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "OPEN"
    );

    // Case B: Persistent breach (100.0) -> Tick 1 confirms, Tick 2 verifies fresh equity & liquidates ETH
    *ch_equity.lock() = "100.0".to_string();
    db.set_config_value("live_exchange_equity_usdc", "100.0", None)
        .unwrap();
    let b_tick1 = engine.enforce_leverage_and_position_caps().unwrap();
    assert!(b_tick1.is_empty());
    assert_eq!(
        db.get_position_by_id(eth_pid).unwrap().unwrap().status,
        "OPEN"
    );

    let b_tick2 = engine.enforce_leverage_and_position_caps().unwrap();
    assert!(
        b_tick2
            .iter()
            .any(|r| r.position_id == eth_pid && r.final_status == "CLOSED"),
        "2nd consecutive breach tick with fresh equity must execute AUTO_DELEVERAGE_RISK_CAP"
    );
}

#[test]
fn test_wave2_oracle_deviation_pre_l2_gate_and_cooldown_telemetry_p0_liq_r14_l24() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let l2_sol_calls = Arc::new(AtomicUsize::new(0));
    let l2_sol_srv = l2_sol_calls.clone();

    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "l2Book" {
                let coin = body.get("coin").and_then(Value::as_str).unwrap_or("");
                if coin == "SOL" {
                    l2_sol_srv.fetch_add(1, Ordering::SeqCst);
                }
                if coin == "AVAX" {
                    // Thin book: ask notional = 35 * 1.0 = $35 < $150 min_entry_l2_depth_usdc
                    return (
                        200,
                        json!({
                            "levels": [
                                [{"px": "34.99", "sz": "100.0"}],
                                [{"px": "35.01", "sz": "1.0"}]
                            ]
                        }),
                    );
                }
                if coin == "LINK" {
                    // Deep book: > $150 floor and > 1.5x target notional
                    return (
                        200,
                        json!({
                            "levels": [
                                [{"px": "18.498", "sz": "200.0"}],
                                [{"px": "18.502", "sz": "200.0"}]
                            ]
                        }),
                    );
                }
            }
            return (200, json!({}));
        }
        (
            200,
            json!({
                "status": "ok",
                "response": { "type": "order", "data": { "statuses": [{ "resting": { "oid": 77101u64 } }] } }
            }),
        )
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());
    db.set_config_value(
        "private_key",
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        None,
    )
    .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "1000.0", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();
    db.set_config_value("max_entry_oracle_deviation_pct", "0.015", None)
        .unwrap();
    db.set_config_value("min_entry_l2_depth_mult", "1.5", None)
        .unwrap();
    db.set_config_value("min_entry_l2_depth_usdc", "150.0", None)
        .unwrap();

    // Distort SOL mark vs oracle (+3.1% > 1.5%) while giving SOL highest target_weight
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    db.upsert_exchange_asset(&AssetUpsertInput {
        raw_symbol_or_coin: sol.coin.clone(),
        asset_index: sol.asset_index,
        sz_decimals: sol.sz_decimals,
        tick_size: sol.tick_size,
        lot_step_size: sol.lot_step_size,
        max_leverage: sol.max_leverage,
        mark_price: 190.0,
        mid_price: 190.0,
        oracle_price: 184.25,
        funding_rate_hourly: sol.funding_rate_hourly,
        day_volume_usdc: sol.day_volume_usdc,
        open_interest_usdc: sol.open_interest_usdc,
        prev_day_price: sol.prev_day_price,
        is_core_eligible: true,
        is_event_eligible: true,
        is_priority_watched: false,
    })
    .unwrap();

    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let avax = db.get_asset_by_symbol("AVAX-USDC").unwrap().unwrap();
    let link = db.get_asset_by_symbol("LINK-USDC").unwrap().unwrap();
    let mut eth_cd_sig = make_signal_input(&eth, 0.25, 0.0, aegis_signals::ACTION_COOLDOWN_BLOCKED);
    eth_cd_sig.target_weight = 0.25;
    let sigs = make_signal_output(vec![
        make_signal_input(&sol, 0.30, 0.0, ACTION_OPEN_LONG),
        eth_cd_sig,
        make_signal_input(&avax, 0.20, 0.0, ACTION_OPEN_LONG),
        make_signal_input(&link, 0.15, 0.0, ACTION_OPEN_LONG),
    ]);

    let reb = engine.execute_signal_rebalance(&sigs, 1000.0).unwrap();
    assert_eq!(
        l2_sol_calls.load(Ordering::SeqCst),
        0,
        "pre-L2 oracle deviation gate must reject SOL without spending l2Book REST weight"
    );
    assert_eq!(reb.working_orders_placed, 1);
    let ordered = db.get_assets_ordered().unwrap();
    assert_eq!(ordered.len(), 1);
    assert_eq!(ordered[0].symbol, "LINK-USDC");
    assert_eq!(reb.cooldown_blocked_count, 1);
    assert_eq!(reb.cooldown_blocked_symbols, vec!["ETH-USDC".to_string()]);
}

#[test]
fn test_wave2_reconcile_flat_fill_exit_cooldown_and_metadata_incomplete_guard_r14_m7_p1_acct() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let cancel_reqs: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let cancel_srv = cancel_reqs.clone();
    let profit_close = Arc::new(AtomicBool::new(false));
    let profit_srv = profit_close.clone();

    let close_ts = now_ms() - 30_000;
    let server = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => (
                    200,
                    json!({
                        "marginSummary": {
                            "accountValue": "1000.0",
                            "totalMarginUsed": "0.0",
                            "withdrawable": "1000.0"
                        },
                        "assetPositions": []
                    }),
                ),
                "userAbstraction" => (200, Value::String("default".to_string())),
                // Trigger fallback to openOrders (metadata_complete = false)
                "frontendOpenOrders" => (500, json!({ "error": "frontend timeout" })),
                "openOrders" => (
                    200,
                    json!([{
                        "coin": "AVAX",
                        "oid": 99_901u64,
                        "side": "A",
                        "limitPx": "34.0",
                        "sz": "10.0"
                    }]),
                ),
                "userFills" | "userFillsByTime" => {
                    if profit_srv.load(Ordering::SeqCst) {
                        (
                            200,
                            json!([{
                                "coin": "ETH",
                                "side": "A",
                                "px": "3450.0",
                                "sz": "0.1",
                                "fee": "0.15",
                                "closedPnl": "5.0",
                                "dir": "Close Long",
                                "time": close_ts,
                                "oid": 99_912u64,
                                "tid": 2u64,
                                "startPosition": "0.1"
                            }]),
                        )
                    } else {
                        (
                            200,
                            json!([{
                                "coin": "AVAX",
                                "side": "A",
                                "px": "34.0",
                                "sz": "10.0",
                                "fee": "1.25",
                                "closedPnl": "-25.0",
                                "dir": "Close Long",
                                "time": close_ts,
                                "oid": 99_911u64,
                                "tid": 1u64,
                                "startPosition": "10.0"
                            }]),
                        )
                    }
                }
                _ => (200, json!([])),
            };
        }
        if body.pointer("/action/type").and_then(Value::as_str) == Some("cancel") {
            cancel_srv.lock().push(body.clone());
        }
        (200, json!({ "status": "ok" }))
    });

    let db = fresh_isolated_db();
    let engine = ExecutionEngine::new(db.clone());
    seed_test_assets(&db);
    configure_testnet_mock_db(&db, server.port());
    db.set_config_value(
        "private_key",
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        None,
    )
    .unwrap();
    db.set_config_value("stop_out_cooldown_sec", "600", None)
        .unwrap();

    // Part 1: Loss EXCHANGE_RECONCILED_FLAT uses fill exit price (34.0) + fee (1.25) + inserts STOP_OUT
    let avax = db.get_asset_by_symbol("AVAX-USDC").unwrap().unwrap();
    let (avax_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: avax.id,
            coin: &avax.coin,
            side: OrderSide::Long,
            entry_price: 36.5,
            mark_price: 35.0,
            quantity: 10.0,
            committed_notional_usdc: 350.0,
            unrealized_pnl_usdc: -15.0,
            portfolio_weight: 0.35,
            protective_stop_px: 34.0,
            take_profit_px: 39.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 600_000),
        })
        .unwrap();

    engine.reconciler.run_cutover_reconciliation(false).unwrap();
    // R14-M7: metadata_complete=false must NOT cancel orphan oid 99901
    assert!(
        cancel_reqs.lock().is_empty(),
        "metadata_complete=false fallback must skip orphan cancellation (R14-M7)"
    );
    let avax_closed = db.get_position_by_id(avax_pid).unwrap().unwrap();
    assert_eq!(avax_closed.status, "CLOSED");
    assert_eq!(
        avax_closed.exit_reason.as_deref(),
        Some("EXCHANGE_RECONCILED_FLAT")
    );
    assert!((avax_closed.exit_price.unwrap_or(0.0) - 34.0).abs() < 1e-6);
    assert!((avax_closed.fees_paid_usdc - 1.25).abs() < 1e-4);
    let cds_loss = db.get_assets_in_cooldown().unwrap();
    assert!(
        cds_loss.iter().any(|c| c.symbol == "AVAX-USDC"
            && c.cooldown_type == "STOP_OUT"
            && c.remaining_seconds >= 550),
        "Loss EXCHANGE_RECONCILED_FLAT must insert STOP_OUT cooldown; cds={cds_loss:?}"
    );

    // Part 2: Profitable EXCHANGE_RECONCILED_FLAT inserts CHURN_GUARD (~300s)
    profit_close.store(true, Ordering::SeqCst);
    let eth = db.get_asset_by_symbol("ETH-USDC").unwrap().unwrap();
    let (eth_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: eth.id,
            coin: &eth.coin,
            side: OrderSide::Long,
            entry_price: 3400.0,
            mark_price: 3450.0,
            quantity: 0.1,
            committed_notional_usdc: 345.0,
            unrealized_pnl_usdc: 5.0,
            portfolio_weight: 0.34,
            protective_stop_px: 3250.0,
            take_profit_px: 3600.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 600_000),
        })
        .unwrap();

    engine.reconciler.run_cutover_reconciliation(false).unwrap();
    let eth_closed = db.get_position_by_id(eth_pid).unwrap().unwrap();
    assert_eq!(eth_closed.status, "CLOSED");
    let cds_win = db.get_assets_in_cooldown().unwrap();
    assert!(
        cds_win.iter().any(|c| c.symbol == "ETH-USDC"
            && c.cooldown_type == "CHURN_GUARD"
            && c.remaining_seconds >= 250),
        "Profitable EXCHANGE_RECONCILED_FLAT must insert CHURN_GUARD cooldown; cds={cds_win:?}"
    );
}

#[test]
fn test_wave2_cap_epoch_validation_and_oracle_profit_gate_guards() {
    use aegis_core::HeldAssetView;

    let mut cfg = ConfigMap::new();
    cfg.insert("execution_mode", "TESTNET");
    let now = now_ms();
    cfg.insert(
        "risk_capital_cap_updated_at_ms",
        (now + 120_000).to_string(),
    );
    let fut_warnings = aegis_execution::validate_cap_epoch_on_startup(&cfg, &[], now);
    assert_eq!(fut_warnings.len(), 1);
    assert!(fut_warnings[0].contains("future"));

    cfg.insert("risk_capital_cap_updated_at_ms", (now - 60_000).to_string());
    let legacy_held = HeldAssetView {
        position_id: 1,
        asset_id: 1,
        symbol: "ETH-USDC".to_string(),
        coin: "ETH".to_string(),
        client_order_id: "legacy-1".to_string(),
        side: "LONG".to_string(),
        quantity: 0.5,
        committed_notional_usdc: 1700.0,
        portfolio_weight: 0.5,
        entry_price: 3400.0,
        mark_price: 3400.0,
        protective_stop_px: 3200.0,
        protective_stop_oid: None,
        stop_distance_pct: None,
        take_profit_px: 3700.0,
        take_profit_oid: None,
        tp_distance_pct: None,
        highest_price_seen: 3400.0,
        lowest_price_seen: 3400.0,
        unrealized_pnl_usdc: 0.0,
        unrealized_pnl_pct: 0.0,
        realized_pnl_usdc: 0.0,
        funding_accrued_usdc: 0.0,
        fees_paid_usdc: 0.0,
        s1_weight_contrib: 0.5,
        s2_weight_contrib: 0.0,
        s3_weight_contrib: 0.0,
        latest_target_weight: 0.5,
        latest_delta_weight: 0.0,
        created_at_ms: now - 120_000,
        opened_at_ms: Some(now - 120_000),
        updated_at_ms: now,
    };
    let legacy_warnings = aegis_execution::validate_cap_epoch_on_startup(
        &cfg,
        std::slice::from_ref(&legacy_held),
        now,
    );
    assert_eq!(legacy_warnings.len(), 1);
    assert!(legacy_warnings[0].contains("excluded"));

    // R14-M10: exit fee scales with (mark_price * quantity).abs(), not raw unscaled quantity
    let mut eth_gain = legacy_held;
    eth_gain.entry_price = 3400.0;
    eth_gain.mark_price = 3410.0; // gross +$5.00 vs notional $1700 -> +0.294%; with exit fee $0.767 net +$4.233 < $4.25 (0.25%)
    let (passes_close, _, _) =
        aegis_execution::signal_exit_passes_profit_gate(&eth_gain, 0.0025, 4.5);
    assert!(
        !passes_close,
        "exit fee must scale by mark_price * qty ($0.767), making $5 gross net $4.233 < $4.25"
    );

    // P0-Liq: invalid oracle fails closed; worker cleanly joins on sender drop
    let db = fresh_isolated_db();
    seed_test_assets(&db);
    let mut bad_asset = db.get_asset_by_symbol("BTC-USDC").unwrap().unwrap();
    bad_asset.oracle_price = 0.0;
    assert!(aegis_execution::passes_entry_oracle_deviation_gate(&bad_asset, 0.015).is_err());

    let (tx, handle) =
        aegis_execution::AlertDispatchWorker::spawn(aegis_alerts::AlertEngine::new(db));
    drop(tx);
    handle.join().expect("AlertDispatchWorker joins cleanly");
}

#[test]
fn test_wave3_ws_ssrf_url_validation_and_2conn_full_channel_ingestion() {
    use aegis_execution::{
        derive_hyperliquid_ws_url, resolve_hyperliquid_ws_url, validate_hyperliquid_ws_url,
        HyperliquidWsManager, WsStreamStatus, WsSubscriptionSet, WsTimingConfig,
    };
    use std::time::{Duration, Instant};

    let mut cfg = ConfigMap::new();
    assert!(validate_hyperliquid_ws_url(
        "wss://api.hyperliquid.xyz/ws",
        &cfg
    ));
    assert!(validate_hyperliquid_ws_url(
        "wss://api.hyperliquid-testnet.xyz/ws",
        &cfg
    ));
    assert!(validate_hyperliquid_ws_url("ws://127.0.0.1:19001", &cfg));
    assert!(validate_hyperliquid_ws_url("ws://localhost:19001/ws", &cfg));
    assert!(validate_hyperliquid_ws_url("ws://[::1]:19001/ws", &cfg));
    assert!(!validate_hyperliquid_ws_url(
        "ws://api.hyperliquid.xyz/ws",
        &cfg
    ));
    assert!(!validate_hyperliquid_ws_url(
        "wss://evil.attacker.example/ws",
        &cfg
    ));
    assert!(!validate_hyperliquid_ws_url(
        "http://api.hyperliquid.xyz/ws",
        &cfg
    ));
    cfg.insert("execution_mode", "LIVE");
    cfg.insert("hyperliquid_api_url", "https://api.hyperliquid.xyz/info");
    assert_eq!(
        resolve_hyperliquid_ws_url(&cfg),
        "wss://api.hyperliquid.xyz/ws"
    );
    assert_eq!(
        derive_hyperliquid_ws_url("http://127.0.0.1:19555", &cfg).as_deref(),
        Some("ws://127.0.0.1:19555/ws")
    );

    let ws_srv = MockWsServer::start();
    let db = fresh_isolated_db();
    seed_test_assets(&db);
    db.set_config_value("execution_mode", "TESTNET", None)
        .unwrap();
    db.set_config_value("hyperliquid_ws_url", &ws_srv.ws_url(), None)
        .unwrap();
    db.set_config_value(
        "wallet_address",
        "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23",
        None,
    )
    .unwrap();

    let engine = ExecutionEngine::new(db.clone());
    let loaded_cfg = db.load_config_or_default();
    let (_sub_tx, sub_rx) = tokio::sync::watch::channel(WsSubscriptionSet {
        ctx_and_candle_coins: vec!["SOL".to_string()],
        l2_book_coins: vec!["SOL".to_string()],
    });
    let timing = WsTimingConfig {
        ping_interval: Duration::from_secs(5),
        stale_timeout: Duration::from_secs(15),
        connect_timeout: Duration::from_secs(2),
        backoff_min: Duration::from_millis(25),
        backoff_max: Duration::from_millis(100),
        healthy_reset_duration: Duration::from_secs(5),
        subscribe_pace_interval: Duration::from_millis(5),
        status_persist_interval: Duration::from_millis(80),
    };
    let mut mgr = HyperliquidWsManager::start(
        db.clone(),
        Arc::clone(&engine.ws_cache),
        sub_rx,
        &loaded_cfg,
        timing,
    )
    .unwrap();

    assert!(ws_srv.wait_for_connections(2, Duration::from_secs(3)));
    assert!(ws_srv.wait_for_received(10, Duration::from_secs(3)));

    // Verify all expected subscriptions were sent across Market + User WS
    let subs = ws_srv.received_json();
    for expected_type in [
        "allMids",
        "activeAssetCtx",
        "candle",
        "l2Book",
        "orderUpdates",
        "userFills",
        "userFundings",
        "userEvents",
        "webData3",
    ] {
        assert!(
            subs.iter().any(|v| {
                v.get("method").and_then(Value::as_str) == Some("subscribe")
                    && v.pointer("/subscription/type").and_then(Value::as_str)
                        == Some(expected_type)
            }),
            "missing subscription for channel '{expected_type}'"
        );
    }

    let now = now_ms();
    ws_srv.push_json(&json!({
        "channel": "allMids",
        "data": { "mids": { "SOL": "188.25" } }
    }));
    ws_srv.push_json(&json!({
        "channel": "activeAssetCtx",
        "data": {
            "coin": "SOL",
            "ctx": {
                "markPx": "188.20",
                "oraclePx": "188.10",
                "midPx": "188.25",
                "prevDayPx": "180.00",
                "funding": "0.000025",
                "openInterest": "25000.0",
                "dayNtlVlm": "50000000.0"
            }
        }
    }));
    ws_srv.push_json(&json!({
        "channel": "candle",
        "data": {
            "t": now - 3_600_000,
            "s": "SOL",
            "i": "1h",
            "o": "186.0",
            "h": "189.0",
            "l": "185.5",
            "c": "188.2",
            "v": "1234.5"
        }
    }));
    ws_srv.push_json(&json!({
        "channel": "l2Book",
        "data": {
            "coin": "SOL",
            "levels": [
                [{"px": "188.20", "sz": "15.0"}],
                [{"px": "188.30", "sz": "15.0"}]
            ]
        }
    }));
    ws_srv.push_json(&json!({
        "channel": "webData3",
        "data": {
            "clearinghouseState": {
                "marginSummary": { "accountValue": "2450.0", "totalMarginUsed": "100.0", "withdrawable": "2350.0" },
                "assetPositions": []
            },
            "openOrders": [
                { "coin": "SOL", "oid": 5001u64, "orderType": "Stop Limit", "isTrigger": true, "triggerPx": "175.0" }
            ]
        }
    }));
    ws_srv.push_json(&json!({
        "channel": "orderUpdates",
        "data": [{
            "order": { "coin": "SOL", "side": "B", "limitPx": "188.0", "sz": "1.0", "origSz": "1.0", "oid": 5002u64, "cloid": "0x001122" },
            "status": "open",
            "statusTimestamp": now
        }]
    }));
    ws_srv.push_json(&json!({
        "channel": "userFills",
        "data": {
            "fills": [{ "coin": "SOL", "side": "B", "px": "188.0", "sz": "0.5", "fee": "0.05", "closedPnl": "0.0", "dir": "Open Long", "time": now, "oid": 5003u64, "tid": 77u64 }]
        }
    }));
    ws_srv.push_json(&json!({
        "channel": "userFundings",
        "data": {
            "fundings": [{ "time": now, "coin": "SOL", "usdc": "0.35", "szi": "0.5", "fundingRate": "0.00002", "nSamples": 1 }]
        }
    }));

    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if engine.ws_cache.get_fresh_all_mids().is_some()
            && engine.ws_cache.get_fresh_active_asset_ctx("SOL").is_some()
            && engine.ws_cache.get_fresh_candles("SOL", "1h").is_some()
            && engine.ws_cache.get_fresh_l2_book_depth("SOL").is_some()
            && engine.ws_cache.get_fresh_clearinghouse_state().is_some()
            && engine.ws_cache.get_fresh_open_orders().is_some()
            && engine
                .ws_cache
                .get_order_update_by_cloid("0x001122")
                .is_some()
            && !engine.ws_cache.get_recent_user_fills().0.is_empty()
            && !engine.ws_cache.get_recent_user_fundings().0.is_empty()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(15));
    }

    assert_eq!(
        engine
            .ws_cache
            .get_fresh_all_mids()
            .and_then(|m| m.get("SOL").copied()),
        Some(188.25)
    );
    assert!(engine.ws_cache.get_fresh_active_asset_ctx("SOL").is_some());
    assert!(engine.ws_cache.get_fresh_candles("SOL", "1h").is_some());
    assert!(engine.ws_cache.get_fresh_l2_book_depth("SOL").is_some());
    assert!(engine.ws_cache.get_fresh_clearinghouse_state().is_some());
    assert!(engine
        .ws_cache
        .get_fresh_open_orders()
        .is_some_and(|o| o.metadata_complete && o.orders.len() == 1));
    assert!(engine
        .ws_cache
        .get_order_update_by_cloid("0x001122")
        .is_some());
    assert_eq!(engine.ws_cache.get_recent_user_fills().0.len(), 1);
    assert_eq!(engine.ws_cache.get_recent_user_fundings().0.len(), 1);

    // Ensure execution_lock() is never held by WS network loops
    assert!(aegis_execution::execution_lock().try_lock().is_some());

    // Ensure ws_stream_status_json was persisted
    let status_deadline = Instant::now() + Duration::from_secs(2);
    let mut persisted_connected = false;
    while Instant::now() < status_deadline {
        let raw = db
            .load_config_or_default()
            .get_str("ws_stream_status_json", "");
        if let Ok(st) = serde_json::from_str::<WsStreamStatus>(&raw) {
            if st.market_connected && st.user_connected && st.status == "CONNECTED" {
                persisted_connected = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        persisted_connected,
        "ws_stream_status_json must reflect CONNECTED state"
    );

    mgr.shutdown();
}

#[test]
fn test_wave3_ws_heartbeat_reconnect_paced_resubscribe_gap_fill_and_rest_fallback() {
    use aegis_execution::{HyperliquidWsManager, WsSubscriptionSet, WsTimingConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    let ch_rest_calls = Arc::new(AtomicUsize::new(0));
    let orders_rest_calls = Arc::new(AtomicUsize::new(0));
    let fills_rest_calls = Arc::new(AtomicUsize::new(0));
    let mids_rest_calls = Arc::new(AtomicUsize::new(0));

    let ch_srv = ch_rest_calls.clone();
    let orders_srv = orders_rest_calls.clone();
    let fills_srv = fills_rest_calls.clone();
    let mids_srv = mids_rest_calls.clone();

    let http_srv = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "clearinghouseState" => {
                    ch_srv.fetch_add(1, Ordering::SeqCst);
                    (
                        200,
                        json!({
                            "marginSummary": {
                                "accountValue": "3100.0",
                                "totalMarginUsed": "0.0",
                                "withdrawable": "3100.0"
                            },
                            "assetPositions": []
                        }),
                    )
                }
                "frontendOpenOrders" => {
                    orders_srv.fetch_add(1, Ordering::SeqCst);
                    (200, json!([]))
                }
                "userFillsByTime" | "userFills" => {
                    fills_srv.fetch_add(1, Ordering::SeqCst);
                    (200, json!([]))
                }
                "allMids" => {
                    mids_srv.fetch_add(1, Ordering::SeqCst);
                    (200, json!({ "BTC": "66000.0", "SOL": "185.0" }))
                }
                "userAbstraction" => (200, Value::String("default".to_string())),
                _ => (200, json!([])),
            };
        }
        (200, json!({ "status": "ok" }))
    });

    let ws_srv = MockWsServer::start();
    let db = fresh_isolated_db();
    seed_test_assets(&db);
    let btc = db.get_asset_by_symbol("BTC-USDC").unwrap().unwrap();
    db.set_asset_priority_watch(btc.id, true).unwrap();
    configure_testnet_mock_db(&db, http_srv.port());
    db.set_config_value("hyperliquid_ws_url", &ws_srv.ws_url(), None)
        .unwrap();

    let engine = ExecutionEngine::new(db.clone());
    let cfg = db.load_config_or_default();
    let (_sub_tx, sub_rx) = tokio::sync::watch::channel(WsSubscriptionSet {
        ctx_and_candle_coins: vec!["BTC".to_string(), "SOL".to_string()],
        l2_book_coins: vec!["BTC".to_string(), "SOL".to_string()],
    });
    let timing = WsTimingConfig {
        ping_interval: Duration::from_millis(80),
        stale_timeout: Duration::from_millis(280),
        connect_timeout: Duration::from_secs(2),
        backoff_min: Duration::from_millis(20),
        backoff_max: Duration::from_millis(60),
        healthy_reset_duration: Duration::from_secs(5),
        subscribe_pace_interval: Duration::from_millis(15),
        status_persist_interval: Duration::from_millis(80),
    };
    let mut mgr = HyperliquidWsManager::start(
        db.clone(),
        Arc::clone(&engine.ws_cache),
        sub_rx,
        &cfg,
        timing,
    )
    .unwrap();

    assert!(ws_srv.wait_for_connections(2, Duration::from_secs(3)));
    assert!(ws_srv.wait_for_received(10, Duration::from_secs(3)));

    // Expect periodic ping -> pong
    let ping_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < ping_deadline {
        if ws_srv
            .received_json()
            .iter()
            .any(|v| v.get("method").and_then(Value::as_str) == Some("ping"))
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(15));
    }
    assert!(
        ws_srv
            .received_json()
            .iter()
            .any(|v| v.get("method").and_then(Value::as_str) == Some("ping")),
        "WS manager must send JSON ping heartbeat"
    );

    // 1. Initial connect sets needs_rest_gap_reconcile = true -> first reconcile performs REST gap-fill and clears it
    assert!(engine.ws_cache.needs_rest_gap_reconcile());
    let rep1 = engine.reconciler.sync_db_with_hyperliquid().unwrap();
    assert!(rep1.exchange_reachable);
    assert_eq!(ch_rest_calls.load(Ordering::SeqCst), 1);
    assert_eq!(orders_rest_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fills_rest_calls.load(Ordering::SeqCst), 1);
    assert!(!engine.ws_cache.needs_rest_gap_reconcile());

    // 2. Push fresh webData3 + allMids; steady-state reconcile & PriorityMonitor consume WS cache WITHOUT REST
    ws_srv.push_json(&json!({
        "channel": "allMids",
        "data": { "mids": { "BTC": "66500.0", "SOL": "188.0" } }
    }));
    ws_srv.push_json(&json!({
        "channel": "webData3",
        "data": {
            "clearinghouseState": {
                "marginSummary": { "accountValue": "3250.0", "totalMarginUsed": "0.0", "withdrawable": "3250.0" },
                "assetPositions": []
            },
            "openOrders": []
        }
    }));
    let ch_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < ch_deadline {
        if engine
            .ws_cache
            .get_fresh_clearinghouse_state_if_gap_filled()
            .is_some()
            && engine.ws_cache.get_fresh_all_mids().is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(15));
    }

    let prio_ws = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(prio_ws.watchlist_count, 1);
    assert_eq!(
        mids_rest_calls.load(Ordering::SeqCst),
        0,
        "PriorityMonitor must read fresh allMids from WsStateCache without REST"
    );

    let rep2 = engine.reconciler.sync_db_with_hyperliquid().unwrap();
    assert!((rep2.exchange_equity_usdc - 3250.0).abs() < 1e-6);
    assert_eq!(
        ch_rest_calls.load(Ordering::SeqCst),
        1,
        "steady-state reconcile must reuse fresh User WS clearinghouseState"
    );
    assert_eq!(
        orders_rest_calls.load(Ordering::SeqCst),
        1,
        "steady-state reconcile must reuse fresh User WS openOrders"
    );
    assert_eq!(
        fills_rest_calls.load(Ordering::SeqCst),
        1,
        "steady-state reconcile must reuse User WS userFills ring"
    );

    // 3. Post-order-submit 1_500 ms bypass forces REST read instead of stale WS clearinghouseState
    engine.ws_cache.mark_order_submit();
    assert!(engine.ws_cache.get_fresh_clearinghouse_state().is_none());
    let rep_bypass = engine.reconciler.sync_db_with_hyperliquid().unwrap();
    assert!((rep_bypass.exchange_equity_usdc - 3100.0).abs() < 1e-6);
    assert_eq!(ch_rest_calls.load(Ordering::SeqCst), 2);
    assert_eq!(orders_rest_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fills_rest_calls.load(Ordering::SeqCst), 1);

    // 4. Forced disconnect triggers reconnect, paced resubscription, and re-arms needs_rest_gap_reconcile
    ws_srv.clear_received();
    let conns_before = ws_srv.connection_count();
    ws_srv.disconnect_all();
    assert!(ws_srv.wait_for_connections(conns_before + 2, Duration::from_secs(3)));
    assert!(ws_srv.wait_for_received(10, Duration::from_secs(3)));
    assert!(engine.ws_cache.needs_rest_gap_reconcile());
    engine.reconciler.sync_db_with_hyperliquid().unwrap();
    assert!(!engine.ws_cache.needs_rest_gap_reconcile());
    assert_eq!(ch_rest_calls.load(Ordering::SeqCst), 3);
    assert_eq!(orders_rest_calls.load(Ordering::SeqCst), 3);
    assert_eq!(fills_rest_calls.load(Ordering::SeqCst), 2);

    // 5. Shutdown WS manager -> PriorityMonitor transparently falls back to Phase 1 REST allMids
    mgr.shutdown();
    assert!(!engine.ws_cache.is_market_ws_connected());
    let mids_before = mids_rest_calls.load(Ordering::SeqCst);
    engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert!(
        mids_rest_calls.load(Ordering::SeqCst) > mids_before,
        "PriorityMonitor must transparently fall back to REST allMids when WS disconnects"
    );
}

#[test]
fn test_wave3_ws_market_watcher_and_priority_monitor_zero_redundant_http() {
    use aegis_db::BarUpsertInput;
    use aegis_execution::{HyperliquidWsManager, WsSubscriptionSet, WsTimingConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    let meta_calls = Arc::new(AtomicUsize::new(0));
    let mids_calls = Arc::new(AtomicUsize::new(0));
    let candle_calls = Arc::new(AtomicUsize::new(0));
    let ch_calls = Arc::new(AtomicUsize::new(0));
    let l2_calls = Arc::new(AtomicUsize::new(0));
    let front_orders_calls = Arc::new(AtomicUsize::new(0));

    let meta_srv = meta_calls.clone();
    let mids_srv = mids_calls.clone();
    let candle_srv = candle_calls.clone();
    let ch_srv = ch_calls.clone();
    let l2_srv = l2_calls.clone();
    let front_srv = front_orders_calls.clone();

    let http_srv = MockHttpServer::start_json(move |path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            return match typ {
                "metaAndAssetCtxs" => {
                    meta_srv.fetch_add(1, Ordering::SeqCst);
                    (
                        200,
                        json!([
                            {
                                "universe": [
                                    { "name": "BTC", "szDecimals": 4, "maxLeverage": 50 },
                                    { "name": "SOL", "szDecimals": 2, "maxLeverage": 20 }
                                ]
                            },
                            [
                                {
                                    "markPx": "65000.0",
                                    "midPx": "65000.0",
                                    "oraclePx": "65000.0",
                                    "prevDayPx": "64000.0",
                                    "funding": "0.0000125",
                                    "dayNtlVlm": "500000000.0",
                                    "openInterest": "2000.0"
                                },
                                {
                                    "markPx": "185.0",
                                    "midPx": "185.0",
                                    "oraclePx": "185.0",
                                    "prevDayPx": "180.0",
                                    "funding": "0.0000125",
                                    "dayNtlVlm": "100000000.0",
                                    "openInterest": "100000.0"
                                }
                            ]
                        ]),
                    )
                }
                "allMids" => {
                    mids_srv.fetch_add(1, Ordering::SeqCst);
                    (200, json!({ "BTC": "65000.0", "SOL": "185.0" }))
                }
                "candleSnapshot" => {
                    candle_srv.fetch_add(1, Ordering::SeqCst);
                    (200, json!([]))
                }
                "clearinghouseState" => {
                    ch_srv.fetch_add(1, Ordering::SeqCst);
                    (
                        200,
                        json!({
                            "marginSummary": { "accountValue": "2000.0", "totalMarginUsed": "100.0", "withdrawable": "1900.0" },
                            "assetPositions": []
                        }),
                    )
                }
                "l2Book" => {
                    l2_srv.fetch_add(1, Ordering::SeqCst);
                    (
                        200,
                        json!({
                            "levels": [
                                [{"px": "184.95", "sz": "100.0"}],
                                [{"px": "185.05", "sz": "100.0"}]
                            ]
                        }),
                    )
                }
                "frontendOpenOrders" => {
                    front_srv.fetch_add(1, Ordering::SeqCst);
                    (200, json!([]))
                }
                _ => (200, json!([])),
            };
        }
        (
            200,
            json!({
                "status": "ok",
                "response": { "type": "order", "data": { "statuses": [{ "resting": { "oid": 88001u64 } }] } }
            }),
        )
    });

    let ws_srv = MockWsServer::start();
    let db = fresh_isolated_db();
    configure_testnet_mock_db(&db, http_srv.port());
    db.set_config_value("hyperliquid_ws_url", &ws_srv.ws_url(), None)
        .unwrap();
    db.set_config_value(
        "private_key",
        "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
        None,
    )
    .unwrap();
    db.set_config_value("live_exchange_equity_usdc", "2000.0", None)
        .unwrap();
    db.set_config_value("min_entry_interval_sec", "0", None)
        .unwrap();

    let engine = ExecutionEngine::new(db.clone());
    let cfg = db.load_config_or_default();
    let sub_rx = engine.ws_subscription_sender().subscribe();
    let timing = WsTimingConfig {
        ping_interval: Duration::from_secs(5),
        stale_timeout: Duration::from_secs(15),
        connect_timeout: Duration::from_secs(2),
        backoff_min: Duration::from_millis(25),
        backoff_max: Duration::from_millis(100),
        healthy_reset_duration: Duration::from_secs(5),
        subscribe_pace_interval: Duration::from_millis(5),
        status_persist_interval: Duration::from_secs(5),
    };
    let mut mgr = HyperliquidWsManager::start(
        db.clone(),
        Arc::clone(&engine.ws_cache),
        sub_rx,
        &cfg,
        timing,
    )
    .unwrap();
    assert!(ws_srv.wait_for_connections(2, Duration::from_secs(3)));

    // Initial universe discovery (1 metaAndAssetCtxs call)
    let scan1 = engine.watcher.run_3s_exchange_scan().unwrap();
    assert_eq!(scan1.total_assets_upserted, 2);
    assert_eq!(meta_calls.load(Ordering::SeqCst), 1);

    let btc = db.get_asset_by_symbol("BTC-USDC").unwrap().unwrap();
    let sol = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    db.set_asset_priority_watch(sol.id, true).unwrap();

    // Seed >= 48 historical 1h & 4h bars in DB so steady-state WS candles skip candleSnapshot REST
    let now = now_ms();
    let mut seed_bars = Vec::new();
    for tf in ["1h", "4h"] {
        let tf_ms = if tf == "1h" {
            3_600_000i64
        } else {
            14_400_000i64
        };
        for asset_id in [btc.id, sol.id] {
            for i in (1..=48i64).rev() {
                seed_bars.push(BarUpsertInput {
                    asset_id,
                    timeframe: tf.to_string(),
                    timestamp_ms: now - i * tf_ms,
                    open: 184.0,
                    high: 186.0,
                    low: 183.0,
                    close: 185.0,
                    volume: 1000.0,
                    funding_rate: 0.0,
                });
            }
        }
    }
    db.upsert_ohlcv_bars(&seed_bars).unwrap();

    // Seed an active OPEN holding on BTC with existing SL/TP OIDs
    let (btc_pid, _) = db
        .sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: btc.id,
            coin: &btc.coin,
            side: OrderSide::Long,
            entry_price: 65000.0,
            mark_price: 65000.0,
            quantity: 0.01,
            committed_notional_usdc: 650.0,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.32,
            protective_stop_px: 62000.0,
            take_profit_px: 69000.0,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now - 600_000),
        })
        .unwrap();
    db.update_position_tpsl_orders(
        btc_pid,
        62000.0,
        OidUpdate::Set("hl-9101"),
        69000.0,
        OidUpdate::Set("hl-9102"),
    )
    .unwrap();

    // Reset HTTP call counters, push fresh Market + User WS frames
    meta_calls.store(0, Ordering::SeqCst);
    mids_calls.store(0, Ordering::SeqCst);
    candle_calls.store(0, Ordering::SeqCst);
    ch_calls.store(0, Ordering::SeqCst);
    l2_calls.store(0, Ordering::SeqCst);
    front_orders_calls.store(0, Ordering::SeqCst);

    let _ = engine.ws_subscription_sender().send(WsSubscriptionSet {
        ctx_and_candle_coins: vec!["BTC".to_string(), "SOL".to_string()],
        l2_book_coins: vec!["BTC".to_string(), "SOL".to_string()],
    });

    let push_fresh_ws = || {
        let cur_ts = now_ms();
        ws_srv.push_json(&json!({
            "channel": "allMids",
            "data": { "mids": { "BTC": "66123.5", "SOL": "189.75" } }
        }));
        ws_srv.push_json(&json!({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "BTC",
                "ctx": {
                    "markPx": "66120.0",
                    "oraclePx": "66110.0",
                    "midPx": "66123.5",
                    "prevDayPx": "64000.0",
                    "funding": "0.000015",
                    "openInterest": "2500.0",
                    "dayNtlVlm": "550000000.0"
                }
            }
        }));
        ws_srv.push_json(&json!({
            "channel": "activeAssetCtx",
            "data": {
                "coin": "SOL",
                "ctx": {
                    "markPx": "189.70",
                    "oraclePx": "189.65",
                    "midPx": "189.75",
                    "prevDayPx": "180.0",
                    "funding": "0.00002",
                    "openInterest": "120000.0",
                    "dayNtlVlm": "120000000.0"
                }
            }
        }));
        for (coin, px) in [("BTC", "66120.0"), ("SOL", "189.70")] {
            for tf in ["1h", "4h"] {
                ws_srv.push_json(&json!({
                    "channel": "candle",
                    "data": {
                        "t": cur_ts,
                        "s": coin,
                        "i": tf,
                        "o": px,
                        "h": px,
                        "l": px,
                        "c": px,
                        "v": "500.0"
                    }
                }));
            }
        }
        ws_srv.push_json(&json!({
            "channel": "l2Book",
            "data": {
                "coin": "SOL",
                "levels": [
                    [{"px": "189.70", "sz": "50.0"}],
                    [{"px": "189.80", "sz": "50.0"}]
                ]
            }
        }));
        ws_srv.push_json(&json!({
            "channel": "webData3",
            "data": {
                "clearinghouseState": {
                    "marginSummary": { "accountValue": "2050.0", "totalMarginUsed": "650.0", "withdrawable": "1400.0" },
                    "assetPositions": [{ "position": { "coin": "BTC", "szi": "0.01", "entryPx": "65000.0", "positionValue": "661.2" } }]
                },
                "openOrders": [
                    { "coin": "BTC", "oid": 9101u64, "orderType": "Stop Limit", "isTrigger": true, "triggerPx": "62000.0" },
                    { "coin": "BTC", "oid": 9102u64, "orderType": "Take Profit Limit", "isTrigger": true, "triggerPx": "69000.0" }
                ]
            }
        }));
    };

    push_fresh_ws();
    let ready_deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < ready_deadline {
        if engine.ws_cache.get_fresh_all_mids().is_some()
            && engine.ws_cache.get_fresh_active_asset_ctx("BTC").is_some()
            && engine.ws_cache.get_fresh_candles("SOL", "1h").is_some()
            && engine.ws_cache.get_fresh_l2_book_depth("SOL").is_some()
            && engine.ws_cache.get_fresh_clearinghouse_state().is_some()
            && engine.ws_cache.get_fresh_open_orders().is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(15));
    }

    // Run multiple 3s MarketWatcher scans and 1s PriorityMonitor ticks -> ZERO redundant /info HTTP calls!
    for _ in 0..2 {
        let scan = engine.watcher.run_3s_exchange_scan().unwrap();
        assert!(scan.live_exchange_connected);
        let tick = engine.priority_monitor.run_1s_priority_tick().unwrap();
        assert!(tick.watchlist_count >= 1);
    }

    // Also run rebalance entry L2 gate check on WS-cached SOL without spending REST l2Book weight
    let sol_fresh = db.get_asset_by_symbol("SOL-USDC").unwrap().unwrap();
    assert!((sol_fresh.mid_price - 189.75).abs() < 1e-4);
    assert!((sol_fresh.mark_price - 189.70).abs() < 1e-4);

    let sigs = make_signal_output(vec![make_signal_input(
        &sol_fresh,
        0.25,
        0.0,
        ACTION_OPEN_LONG,
    )]);
    let reb = engine.execute_signal_rebalance(&sigs, 2000.0).unwrap();
    assert_eq!(reb.working_orders_placed, 1);

    assert_eq!(
        meta_calls.load(Ordering::SeqCst),
        0,
        "MarketWatcher must not re-poll metaAndAssetCtxs inside 60s window when Market WS is fresh"
    );
    assert_eq!(
        mids_calls.load(Ordering::SeqCst),
        0,
        "MarketWatcher & PriorityMonitor must read allMids from WsStateCache"
    );
    assert_eq!(
        candle_calls.load(Ordering::SeqCst),
        0,
        "MarketWatcher must consume WS candles without candleSnapshot REST calls when DB has >=48 bars"
    );
    assert_eq!(
        ch_calls.load(Ordering::SeqCst),
        0,
        "PriorityMonitor must read clearinghouseState from WsStateCache"
    );
    assert_eq!(
        l2_calls.load(Ordering::SeqCst),
        0,
        "Rebalance & OrderChaser L2 gate must read l2Book from WsStateCache"
    );
    assert_eq!(
        front_orders_calls.load(Ordering::SeqCst),
        0,
        "PriorityMonitor TP/SL sync must not call frontendOpenOrders when fresh WS openOrders exists"
    );

    mgr.shutdown();
}

#[test]
fn test_wave3_spawn_background_runtime_ws_lifecycle_and_http_only_opt_out() {
    use std::time::{Duration, Instant};

    // 1. HTTP-only ephemeral test DB without explicit loopback WS URL keeps WS disabled
    let http_srv = MockHttpServer::start_json(|path, body| {
        if path == "/info" {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            if typ == "clearinghouseState" {
                return (
                    200,
                    json!({
                        "marginSummary": { "accountValue": "1000.0", "totalMarginUsed": "0.0", "withdrawable": "1000.0" },
                        "assetPositions": []
                    }),
                );
            }
            return (200, json!([]));
        }
        (200, json!({ "status": "ok" }))
    });
    let db_http_only = fresh_isolated_db();
    configure_testnet_mock_db(&db_http_only, http_srv.port());
    let eng_http_only = ExecutionEngine::new(db_http_only);
    let tx_http = eng_http_only.spawn_background_runtime();
    std::thread::sleep(Duration::from_millis(80));
    assert!(!eng_http_only.ws_cache.is_market_ws_connected());
    assert!(!eng_http_only.ws_cache.is_user_ws_connected());
    let _ = tx_http.send(true);

    // 2. Loopback hyperliquid_ws_url spawns Market + User WS via spawn_background_runtime and stops cleanly
    let ws_srv = MockWsServer::start();
    let db_ws = fresh_isolated_db();
    seed_test_assets(&db_ws);
    configure_testnet_mock_db(&db_ws, http_srv.port());
    db_ws
        .set_config_value("hyperliquid_ws_url", &ws_srv.ws_url(), None)
        .unwrap();
    db_ws
        .set_config_value("auto_rebalance_enabled", "false", None)
        .unwrap();

    let eng_ws = ExecutionEngine::new(db_ws);
    let tx_ws = eng_ws.spawn_background_runtime();
    assert!(
        ws_srv.wait_for_connections(2, Duration::from_secs(4)),
        "spawn_background_runtime must establish Market + User WS connections when loopback WS URL is set"
    );
    let conn_deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < conn_deadline {
        if eng_ws.ws_cache.is_market_ws_connected() && eng_ws.ws_cache.is_user_ws_connected() {
            break;
        }
        std::thread::sleep(Duration::from_millis(15));
    }
    assert!(eng_ws.ws_cache.is_market_ws_connected());
    assert!(eng_ws.ws_cache.is_user_ws_connected());

    let _ = tx_ws.send(true);
    let stop_deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < stop_deadline {
        if !eng_ws.ws_cache.is_market_ws_connected() && !eng_ws.ws_cache.is_user_ws_connected() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!eng_ws.ws_cache.is_market_ws_connected());
    assert!(!eng_ws.ws_cache.is_user_ws_connected());
}
