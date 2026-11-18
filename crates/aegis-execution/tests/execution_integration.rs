//! End-to-End Integration Tests for Aegis Quant v2.0 Execution Engine (`aegis-execution`).

use aegis_core::OrderSide;
use aegis_db::DatabaseManager;
use aegis_execution::{ChaseStepOutcome, ExecutionEngine, NewWorkingOrderRequest};
use std::path::Path;

#[test]
fn test_3s_market_watcher_and_order_to_open_capital_lifecycle() {
    let db = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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

    // 5. Simulate price rally to 198.00 -> 1s Priority Monitor ratchets protective stop upward
    let initial_stop = held[0].protective_stop_px;
    db.update_asset_live_quote(sol.id, 198.00, 198.00).unwrap();
    let ratchet_tick = engine.priority_monitor.run_1s_priority_tick().unwrap();
    assert_eq!(ratchet_tick.stops_ratcheted, 1);

    let held_ratcheted = db.get_assets_held().unwrap();
    assert!(held_ratcheted[0].protective_stop_px > initial_stop);
    assert!(held_ratcheted[0].unrealized_pnl_usdc > 100.0);

    // 6. Simulate flash drop below protective stop -> 1s Priority Monitor closes position & inserts STOP_OUT cooldown
    let active_stop = held_ratcheted[0].protective_stop_px;
    db.update_asset_live_quote(sol.id, active_stop - 1.50, active_stop - 1.50)
        .unwrap();
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
    let db = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
    let db = DatabaseManager::new(Some(Path::new(":memory:")), None).unwrap();
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
    db.update_asset_live_quote(sol.id, tp_px + 0.50, tp_px + 0.50)
        .unwrap();
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
