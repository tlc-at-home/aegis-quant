//! Walk-Forward Simulation State, Fill/Stop/Funding Settlement & Live-Gate Parity (`aegis-backtest::settlement`).

use crate::config::BacktestConfig;
use aegis_core::{
    round_dp, CooldownAssetView, ExchangeAsset, OhlcvBar, OrderSide, PortfolioSnapshot,
    PositionRecord, PositionStatus, Result,
};
use aegis_risk::{
    compute_atr_from_bars, round_to_lot, round_to_tick, CapitalLedgerState, RiskEngine,
    StopEvaluationInput,
};
use aegis_signals::{
    SignalCycleOutput, ACTION_CLOSE, ACTION_COOLDOWN_BLOCKED, ACTION_FLIP, ACTION_INCREASE,
    ACTION_OPEN_LONG, ACTION_OPEN_SHORT, ACTION_REDUCE, ACTION_REGIME_BLOCKED,
};
use std::collections::{HashMap, HashSet, VecDeque};

/// Mutable walk-forward simulation state carried across bars.
#[derive(Debug, Clone)]
pub struct SimulationState {
    pub realized_cash_equity: f64,
    pub active_positions: HashMap<i64, PositionRecord>,
    pub pending_increases: HashMap<i64, PositionRecord>,
    pub initial_limit_price_by_pid: HashMap<i64, f64>,
    pub closed_positions: Vec<PositionRecord>,
    pub cooldown_until_ms: HashMap<i64, (i64, String, String)>,
    pub rolling_24h_equity: VecDeque<(i64, f64)>,
    pub equity_curve: Vec<PortfolioSnapshot>,
    pub bar_returns: Vec<f64>,
    pub prev_total_equity: f64,
    pub peak_equity: f64,
    pub max_drawdown_pct: f64,
    pub next_pos_id: i64,
    pub last_entry_ts_ms: Option<i64>,
    pub stop_out_count: usize,
    pub stagnation_stop_count: usize,
    pub cooldown_blocks: usize,
    pub profit_gate_blocks: usize,
    pub drawdown_halt_bars: usize,
    pub ruin_halted: bool,
    pub chase_abort_count: usize,
    pub ioc_escalations: usize,
    pub max_positions_blocks: usize,
    pub entry_interval_blocks: usize,
    pub total_fees_usdc: f64,
    pub total_funding_usdc: f64,
    pub s1_attribution_usdc: f64,
    pub s2_attribution_usdc: f64,
    pub s3_attribution_usdc: f64,
    pub bar_hours_sum: f64,
    pub bar_hours_samples: usize,
    pub first_evaluated_ts_ms: Option<i64>,
    pub last_bar_close_ts_ms: Option<i64>,
}

impl SimulationState {
    pub fn new(initial_equity_usdc: f64) -> Self {
        Self {
            realized_cash_equity: initial_equity_usdc,
            active_positions: HashMap::new(),
            pending_increases: HashMap::new(),
            initial_limit_price_by_pid: HashMap::new(),
            closed_positions: Vec::new(),
            cooldown_until_ms: HashMap::new(),
            rolling_24h_equity: VecDeque::new(),
            equity_curve: Vec::new(),
            bar_returns: Vec::new(),
            prev_total_equity: initial_equity_usdc,
            peak_equity: initial_equity_usdc,
            max_drawdown_pct: 0.0,
            next_pos_id: 1,
            last_entry_ts_ms: None,
            stop_out_count: 0,
            stagnation_stop_count: 0,
            cooldown_blocks: 0,
            profit_gate_blocks: 0,
            drawdown_halt_bars: 0,
            ruin_halted: false,
            chase_abort_count: 0,
            ioc_escalations: 0,
            max_positions_blocks: 0,
            entry_interval_blocks: 0,
            total_fees_usdc: 0.0,
            total_funding_usdc: 0.0,
            s1_attribution_usdc: 0.0,
            s2_attribution_usdc: 0.0,
            s3_attribution_usdc: 0.0,
            bar_hours_sum: 0.0,
            bar_hours_samples: 0,
            first_evaluated_ts_ms: None,
            last_bar_close_ts_ms: None,
        }
    }

    pub fn active_positions_snapshot(&self) -> Vec<PositionRecord> {
        let mut out: Vec<PositionRecord> = self.active_positions.values().cloned().collect();
        out.extend(self.pending_increases.values().cloned());
        out
    }
}

/// Attributes realized or open position net PnL across Sleeves S1/S2/S3 in proportion to aligned contributions.
#[allow(clippy::too_many_arguments)]
pub fn attribute_pnl_to_sleeves(
    net_pnl: f64,
    side: OrderSide,
    w_s1: f64,
    w_s2: f64,
    w_s3: f64,
    s1_attr: &mut f64,
    s2_attr: &mut f64,
    s3_attr: &mut f64,
) {
    let sign = side.sign();
    let s1_signed = w_s1 * sign;
    let s2_signed = w_s2 * sign;
    let s3_signed = w_s3 * sign;
    let abs_sum = w_s1.abs() + w_s2.abs() + w_s3.abs();
    let signed_sum = s1_signed + s2_signed + s3_signed;
    if signed_sum.abs() >= 0.25 * abs_sum && signed_sum.abs() > 1e-6 {
        *s1_attr += net_pnl * (s1_signed / signed_sum);
        *s2_attr += net_pnl * (s2_signed / signed_sum);
        *s3_attr += net_pnl * (s3_signed / signed_sum);
        return;
    }
    let aligned_sum = s1_signed.max(0.0) + s2_signed.max(0.0) + s3_signed.max(0.0);
    if aligned_sum > 1e-6 {
        *s1_attr += net_pnl * (s1_signed.max(0.0) / aligned_sum);
        *s2_attr += net_pnl * (s2_signed.max(0.0) / aligned_sum);
        *s3_attr += net_pnl * (s3_signed.max(0.0) / aligned_sum);
    } else if abs_sum > 1e-6 {
        *s1_attr += net_pnl * (w_s1.abs() / abs_sum);
        *s2_attr += net_pnl * (w_s2.abs() / abs_sum);
        *s3_attr += net_pnl * (w_s3.abs() / abs_sum);
    }
}

/// Settles a full close of an `OPEN` position, stamping `closed_at_ms` at `bar_close_ts_ms` (`R14-L22`).
#[allow(clippy::too_many_arguments)]
pub fn settle_full_close(
    mut pos: PositionRecord,
    raw_px: f64,
    exit_px: f64,
    taker_fee_bps: f64,
    bar_close_ts_ms: i64,
    reason: String,
    state: &mut SimulationState,
) -> Result<PositionRecord> {
    let side = OrderSide::parse(&pos.side)?;
    let gross_pnl = round_dp(
        (exit_px - pos.entry_price) * pos.filled_quantity * side.sign(),
        4,
    );
    let exit_fee = round_dp(
        exit_px * pos.filled_quantity * (taker_fee_bps / 10_000.0),
        4,
    );
    let exit_slip = round_dp((raw_px - exit_px).abs() * pos.filled_quantity, 4);
    state.realized_cash_equity += gross_pnl - exit_fee;
    state.total_fees_usdc += exit_fee;

    let total_pos_net = round_dp(
        gross_pnl - exit_fee - pos.fees_paid_usdc + pos.funding_accrued_usdc,
        4,
    );
    attribute_pnl_to_sleeves(
        total_pos_net,
        side,
        pos.s1_weight_contrib,
        pos.s2_weight_contrib,
        pos.s3_weight_contrib,
        &mut state.s1_attribution_usdc,
        &mut state.s2_attribution_usdc,
        &mut state.s3_attribution_usdc,
    );

    pos.status = "CLOSED".to_string();
    pos.fsm_state = "CLOSED".to_string();
    pos.exit_price = Some(exit_px);
    pos.reserved_margin_usdc = 0.0;
    pos.committed_notional_usdc = 0.0;
    pos.realized_pnl_usdc = total_pos_net;
    pos.unrealized_pnl_usdc = 0.0;
    pos.fees_paid_usdc = round_dp(pos.fees_paid_usdc + exit_fee, 4);
    pos.slippage_paid_usdc = round_dp(pos.slippage_paid_usdc + exit_slip, 4);
    pos.closed_at_ms = Some(bar_close_ts_ms);
    pos.updated_at_ms = bar_close_ts_ms;
    pos.exit_reason = Some(reason);
    Ok(pos)
}

/// Settles a partial reduction (`ACTION_REDUCE`) of an `OPEN` position, recording a synthetic closed child record.
#[allow(clippy::too_many_arguments)]
pub fn settle_partial_reduce(
    pos: &mut PositionRecord,
    asset: &ExchangeAsset,
    trim_qty: f64,
    side: OrderSide,
    slip: f64,
    taker_fee_bps: f64,
    bar_close_ts_ms: i64,
    total_equity_usdc: f64,
    state: &mut SimulationState,
) {
    let tick_floor = asset.tick_size.max(1e-8);
    let frac = (trim_qty / pos.filled_quantity.max(1e-12)).clamp(0.0, 1.0);
    let alloc_entry_fee = round_dp(pos.fees_paid_usdc * frac, 4);
    let alloc_funding = round_dp(pos.funding_accrued_usdc * frac, 4);
    let raw_px = asset.mark_price;
    let exit_px = round_dp((raw_px * (1.0 - side.sign() * slip)).max(tick_floor), 8);
    let gross_pnl = round_dp((exit_px - pos.entry_price) * trim_qty * side.sign(), 4);
    let exit_fee = round_dp(exit_px * trim_qty * (taker_fee_bps / 10_000.0), 4);
    let exit_slip = round_dp((raw_px - exit_px).abs() * trim_qty, 4);
    state.realized_cash_equity += gross_pnl - exit_fee;
    state.total_fees_usdc += exit_fee;

    let partial_net = round_dp(gross_pnl - exit_fee - alloc_entry_fee + alloc_funding, 4);
    attribute_pnl_to_sleeves(
        partial_net,
        side,
        pos.s1_weight_contrib,
        pos.s2_weight_contrib,
        pos.s3_weight_contrib,
        &mut state.s1_attribution_usdc,
        &mut state.s2_attribution_usdc,
        &mut state.s3_attribution_usdc,
    );

    let mut partial_closed = pos.clone();
    partial_closed.id = state.next_pos_id;
    state.next_pos_id += 1;
    partial_closed.client_order_id = format!("{}-trim-{}", pos.client_order_id, partial_closed.id);
    partial_closed.status = "CLOSED".to_string();
    partial_closed.fsm_state = "CLOSED".to_string();
    partial_closed.exit_price = Some(exit_px);
    partial_closed.requested_quantity = trim_qty;
    partial_closed.filled_quantity = trim_qty;
    partial_closed.reserved_margin_usdc = 0.0;
    partial_closed.committed_notional_usdc = 0.0;
    partial_closed.realized_pnl_usdc = partial_net;
    partial_closed.unrealized_pnl_usdc = 0.0;
    partial_closed.fees_paid_usdc = round_dp(alloc_entry_fee + exit_fee, 4);
    partial_closed.funding_accrued_usdc = alloc_funding;
    partial_closed.slippage_paid_usdc = exit_slip;
    partial_closed.closed_at_ms = Some(bar_close_ts_ms);
    partial_closed.updated_at_ms = bar_close_ts_ms;
    partial_closed.exit_reason = Some(ACTION_REDUCE.to_string());
    state.closed_positions.push(partial_closed);

    pos.filled_quantity = round_dp(pos.filled_quantity - trim_qty, 8);
    pos.requested_quantity = pos.filled_quantity;
    pos.committed_notional_usdc = round_dp(pos.entry_price * pos.filled_quantity, 4);
    pos.fees_paid_usdc = round_dp((pos.fees_paid_usdc - alloc_entry_fee).max(0.0), 4);
    pos.funding_accrued_usdc = round_dp(pos.funding_accrued_usdc - alloc_funding, 4);
    if total_equity_usdc > 1e-6 {
        pos.portfolio_weight = round_dp(
            (pos.entry_price * pos.filled_quantity / total_equity_usdc) * side.sign(),
            4,
        );
    }
    pos.unrealized_pnl_usdc = round_dp(
        (asset.mark_price - pos.entry_price) * pos.filled_quantity * side.sign(),
        4,
    );
    pos.updated_at_ms = bar_close_ts_ms;
}

/// Step 1: Promotes resting `WORKING` orders (and `pending_increases`) whose limit price falls inside `[cur_bar.low, cur_bar.high]`.
pub fn fill_working_orders(
    point_assets: &[ExchangeAsset],
    point_bars: &HashMap<i64, &[OhlcvBar]>,
    cur_bars_by_asset: &HashMap<i64, &OhlcvBar>,
    bar_open_ts_ms: i64,
    maker_fee_bps: f64,
    risk_engine: &RiskEngine,
    state: &mut SimulationState,
) -> Result<HashSet<i64>> {
    let active_list = state.active_positions_snapshot();
    let pre_ledger = risk_engine.evaluate_ledger(state.realized_cash_equity, &active_list);
    let mut just_opened_assets = HashSet::new();

    for asset in point_assets {
        let Some(cur_bar) = cur_bars_by_asset.get(&asset.id) else {
            continue;
        };
        if let Some(pos) = state.active_positions.get_mut(&asset.id) {
            if PositionStatus::parse(&pos.status) == PositionStatus::Working
                && pos.entry_price >= cur_bar.low
                && pos.entry_price <= cur_bar.high
            {
                let side = OrderSide::parse(&pos.side)?;
                let bars = point_bars.get(&asset.id).copied().unwrap_or(&[]);
                let atr = compute_atr_from_bars(
                    bars,
                    14,
                    asset.mark_price,
                    risk_engine.limits.protective_stop_default_pct,
                );
                let commit = risk_engine.confirm_order_fill_to_open(
                    side,
                    pos.entry_price,
                    pos.requested_quantity,
                    pos.reserved_margin_usdc,
                    true,
                    asset.tick_size,
                    Some(atr),
                    &pre_ledger,
                );
                let fill_ts = bar_open_ts_ms.max(pos.created_at_ms);
                pos.status = "OPEN".to_string();
                pos.fsm_state = "FILLED_OPEN".to_string();
                pos.filled_quantity = commit.filled_quantity;
                pos.reserved_margin_usdc = 0.0;
                pos.committed_notional_usdc = commit.committed_notional_usdc;
                pos.protective_stop_px = commit.protective_stop_px;
                pos.take_profit_px = commit.take_profit_px;
                pos.fees_paid_usdc += commit.fees_paid_usdc;
                pos.opened_at_ms = Some(fill_ts);
                pos.updated_at_ms = fill_ts;
                state.initial_limit_price_by_pid.remove(&pos.id);
                state.realized_cash_equity -= commit.fees_paid_usdc;
                state.total_fees_usdc += commit.fees_paid_usdc;
                just_opened_assets.insert(asset.id);
            }
        }

        let inc_fills = state
            .pending_increases
            .get(&asset.id)
            .is_some_and(|inc| inc.entry_price >= cur_bar.low && inc.entry_price <= cur_bar.high);
        if inc_fills {
            if let Some(inc) = state.pending_increases.remove(&asset.id) {
                state.initial_limit_price_by_pid.remove(&inc.id);
                if let Some(pos) = state.active_positions.get_mut(&asset.id) {
                    if PositionStatus::parse(&pos.status) == PositionStatus::Open {
                        let old_qty = pos.filled_quantity.max(0.0);
                        let add_qty = inc.requested_quantity.max(0.0);
                        let new_qty = round_dp(old_qty + add_qty, 8);
                        if new_qty > 0.0 {
                            let old_notional = pos.entry_price * old_qty;
                            let add_notional = inc.entry_price * add_qty;
                            let tot_notional = (old_notional + add_notional).max(1e-8);
                            let add_fee = round_dp(add_notional * (maker_fee_bps / 10_000.0), 4);
                            pos.entry_price = round_dp(tot_notional / new_qty, 8);
                            pos.filled_quantity = new_qty;
                            pos.requested_quantity = new_qty;
                            pos.committed_notional_usdc = round_dp(tot_notional, 4);
                            pos.fees_paid_usdc = round_dp(pos.fees_paid_usdc + add_fee, 4);
                            pos.s1_weight_contrib = round_dp(
                                (pos.s1_weight_contrib * old_notional
                                    + inc.s1_weight_contrib * add_notional)
                                    / tot_notional,
                                4,
                            );
                            pos.s2_weight_contrib = round_dp(
                                (pos.s2_weight_contrib * old_notional
                                    + inc.s2_weight_contrib * add_notional)
                                    / tot_notional,
                                4,
                            );
                            pos.s3_weight_contrib = round_dp(
                                (pos.s3_weight_contrib * old_notional
                                    + inc.s3_weight_contrib * add_notional)
                                    / tot_notional,
                                4,
                            );
                            pos.portfolio_weight =
                                round_dp(pos.portfolio_weight + inc.portfolio_weight, 4);
                            pos.updated_at_ms = bar_open_ts_ms.max(inc.created_at_ms);
                            state.realized_cash_equity -= add_fee;
                            state.total_fees_usdc += add_fee;
                        }
                    }
                }
            }
        }
    }

    Ok(just_opened_assets)
}

/// Step 2: Accrues mark-notional funding (`R14-L22`), checks conservative intra-bar SL/TP,
/// evaluates [`RiskEngine::evaluate_1s_position_lifecycle`] (stagnation time-stop + BE + Chandelier ratchet),
/// and stamps stop/TP/stagnation closes at `bar_close_ts_ms`.
#[allow(clippy::too_many_arguments)]
pub fn process_intrabar_stops_and_funding(
    point_assets: &[ExchangeAsset],
    point_bars: &HashMap<i64, &[OhlcvBar]>,
    cur_bars_by_asset: &HashMap<i64, &OhlcvBar>,
    just_opened_assets: &HashSet<i64>,
    bar_close_ts_ms: i64,
    bar_duration_ms: i64,
    bar_hours: f64,
    config: &BacktestConfig,
    risk_engine: &RiskEngine,
    state: &mut SimulationState,
) -> Result<()> {
    let slip = (config.slippage_bps.max(0.0)) / 10_000.0;
    let mut stopped_assets: Vec<(i64, String, f64, f64, String, i64)> = Vec::new();

    for asset in point_assets {
        let Some(pos) = state.active_positions.get_mut(&asset.id) else {
            continue;
        };
        if PositionStatus::parse(&pos.status) != PositionStatus::Open {
            continue;
        }
        let Some(cur_bar) = cur_bars_by_asset.get(&asset.id) else {
            continue;
        };
        let side = OrderSide::parse(&pos.side)?;
        let bars = point_bars.get(&asset.id).copied().unwrap_or(&[]);
        let atr = compute_atr_from_bars(
            bars,
            14,
            asset.mark_price,
            risk_engine.limits.protective_stop_default_pct,
        );

        // R14-L22: Charge/credit funding on live mark notional `(mark * qty).abs()`, not initial entry notional.
        if !just_opened_assets.contains(&asset.id) {
            let mark_notional = (asset.mark_price * pos.filled_quantity).abs();
            let funding_cash = round_dp(
                -side.sign() * asset.funding_rate_hourly * bar_hours * mark_notional,
                4,
            );
            pos.funding_accrued_usdc = round_dp(pos.funding_accrued_usdc + funding_cash, 4);
            state.realized_cash_equity += funding_cash;
            state.total_funding_usdc += funding_cash;
        }

        let tick_floor = asset.tick_size.max(1e-8);
        let intrabar_hit = match side {
            OrderSide::Long => {
                if pos.protective_stop_px > 0.0 && cur_bar.low <= pos.protective_stop_px {
                    let raw_px = if cur_bar.open < pos.protective_stop_px {
                        cur_bar.open
                    } else {
                        pos.protective_stop_px
                    };
                    let fill_px = round_dp((raw_px * (1.0 - slip)).max(tick_floor), 8);
                    Some((
                        raw_px,
                        fill_px,
                        "PROTECTIVE_STOP_LONG".to_string(),
                        risk_engine.limits.stop_out_cooldown_sec * 1000,
                    ))
                } else if pos.take_profit_px > 0.0 && cur_bar.high >= pos.take_profit_px {
                    let raw_px = if cur_bar.open > pos.take_profit_px {
                        cur_bar.open
                    } else {
                        pos.take_profit_px
                    };
                    let fill_px = round_dp((raw_px * (1.0 - slip)).max(tick_floor), 8);
                    Some((raw_px, fill_px, "TAKE_PROFIT_LONG".to_string(), 0))
                } else {
                    None
                }
            }
            OrderSide::Short => {
                if pos.protective_stop_px > 0.0 && cur_bar.high >= pos.protective_stop_px {
                    let raw_px = if cur_bar.open > pos.protective_stop_px {
                        cur_bar.open
                    } else {
                        pos.protective_stop_px
                    };
                    let fill_px = round_dp((raw_px * (1.0 + slip)).max(tick_floor), 8);
                    Some((
                        raw_px,
                        fill_px,
                        "PROTECTIVE_STOP_SHORT".to_string(),
                        risk_engine.limits.stop_out_cooldown_sec * 1000,
                    ))
                } else if pos.take_profit_px > 0.0 && cur_bar.low <= pos.take_profit_px {
                    let raw_px = if cur_bar.open < pos.take_profit_px {
                        cur_bar.open
                    } else {
                        pos.take_profit_px
                    };
                    let fill_px = round_dp((raw_px * (1.0 + slip)).max(tick_floor), 8);
                    Some((raw_px, fill_px, "TAKE_PROFIT_SHORT".to_string(), 0))
                } else {
                    None
                }
            }
        };

        if let Some((raw_px, fill_px, reason, cd_ms)) = intrabar_hit {
            pos.current_price = cur_bar.close;
            pos.highest_price_seen = pos.highest_price_seen.max(cur_bar.high);
            pos.lowest_price_seen = if pos.lowest_price_seen > 0.0 {
                pos.lowest_price_seen.min(cur_bar.low)
            } else {
                cur_bar.low
            };
            pos.updated_at_ms = bar_close_ts_ms;
            stopped_assets.push((
                asset.id,
                asset.symbol.clone(),
                raw_px,
                fill_px,
                reason,
                cd_ms,
            ));
            continue;
        }

        let bar_high = pos.highest_price_seen.max(cur_bar.high);
        let bar_low = if pos.lowest_price_seen > 0.0 {
            pos.lowest_price_seen.min(cur_bar.low)
        } else {
            cur_bar.low
        };
        let held_duration_ms = pos
            .opened_at_ms
            .map(|opened| bar_close_ts_ms.saturating_sub(opened));

        let stop_eval = risk_engine.evaluate_1s_position_lifecycle(&StopEvaluationInput {
            side,
            entry_price: pos.entry_price,
            mark_price: asset.mark_price,
            quantity: pos.filled_quantity,
            current_stop_px: pos.protective_stop_px,
            current_tp_px: pos.take_profit_px,
            highest_price_seen: bar_high,
            lowest_price_seen: bar_low,
            atr_opt: Some(atr),
            tick_size: asset.tick_size,
            check_tp: true,
            held_duration_ms,
        });

        pos.current_price = asset.mark_price;
        pos.highest_price_seen = stop_eval.highest_price_seen;
        pos.lowest_price_seen = stop_eval.lowest_price_seen;
        pos.protective_stop_px = stop_eval.new_protective_stop_px;
        pos.take_profit_px = stop_eval.take_profit_px;
        pos.unrealized_pnl_usdc = stop_eval.unrealized_pnl_usdc;
        pos.updated_at_ms = bar_close_ts_ms;

        if stop_eval.triggered {
            let fill_px = round_dp(
                (asset.mark_price * (1.0 - side.sign() * slip)).max(tick_floor),
                8,
            );
            stopped_assets.push((
                asset.id,
                asset.symbol.clone(),
                asset.mark_price,
                fill_px,
                stop_eval
                    .exit_reason
                    .unwrap_or_else(|| "PROTECTIVE_STOP".to_string()),
                stop_eval.cooldown_duration_ms,
            ));
        }
    }

    for (aid, sym, raw_px, exit_px, reason, eval_cd_ms) in stopped_assets {
        if let Some(inc) = state.pending_increases.remove(&aid) {
            state.initial_limit_price_by_pid.remove(&inc.id);
        }
        if let Some(pos) = state.active_positions.remove(&aid) {
            state.initial_limit_price_by_pid.remove(&pos.id);
            let closed = settle_full_close(
                pos,
                raw_px,
                exit_px,
                config.taker_fee_bps,
                bar_close_ts_ms,
                reason.clone(),
                state,
            )?;
            state.closed_positions.push(closed);
            if reason.starts_with("STAGNATION_TIME_STOP") {
                state.stagnation_stop_count += 1;
                if eval_cd_ms > 0 {
                    state.cooldown_until_ms.insert(
                        aid,
                        (bar_close_ts_ms + eval_cd_ms, sym, "CHURN_GUARD".to_string()),
                    );
                }
            } else if !reason.starts_with("TAKE_PROFIT") {
                state.stop_out_count += 1;
                let cd_ms = config
                    .stop_out_cooldown_override_bars
                    .map(|bars| (bars as i64) * bar_duration_ms)
                    .unwrap_or_else(|| {
                        eval_cd_ms.max(risk_engine.limits.stop_out_cooldown_sec * 1000)
                    });
                if cd_ms > 0 {
                    state
                        .cooldown_until_ms
                        .insert(aid, (bar_close_ts_ms + cd_ms, sym, "STOP_OUT".to_string()));
                }
            }
        }
    }

    Ok(())
}

/// Builds active `CooldownAssetView` entries at `bar_close_ts_ms` (`v_assets_in_cooldown` parity).
pub fn build_active_cooldown_views(
    bar_close_ts_ms: i64,
    state: &mut SimulationState,
) -> Vec<CooldownAssetView> {
    state
        .cooldown_until_ms
        .retain(|_, (exp, _, _)| *exp > bar_close_ts_ms);
    state
        .cooldown_until_ms
        .iter()
        .map(|(&aid, (exp_ms, sym, cd_type))| CooldownAssetView {
            cooldown_id: aid,
            asset_id: aid,
            symbol: sym.clone(),
            coin: sym.split('-').next().unwrap_or(sym).to_string(),
            cooldown_type: cd_type.clone(),
            reason: "Post-exit cooldown".to_string(),
            triggered_at_ms: bar_close_ts_ms,
            expires_at_ms: *exp_ms,
            remaining_seconds: ((*exp_ms - bar_close_ts_ms) / 1000).max(0),
            updated_at_ms: bar_close_ts_ms,
        })
        .collect()
}

/// Step 5: Executes rebalance actions enforcing all live execution gates (`max_open_positions`,
/// `min_position_hold_sec` w/ loser invalidation bypass, `min_entry_interval_sec`,
/// `profit_gate_signal_exits` w/ `FLIP` exemption & `mark*qty` fee, 24h rolling DD breaker,
/// `min_equity_halt_usdc`, and chase drift cap / IOC escalation).
#[allow(clippy::too_many_arguments)]
pub fn execute_rebalance_actions(
    point_assets: &[ExchangeAsset],
    point_bars: &HashMap<i64, &[OhlcvBar]>,
    cycle_out: &SignalCycleOutput,
    mid_ledger: &CapitalLedgerState,
    bar_close_ts_ms: i64,
    config: &BacktestConfig,
    risk_engine: &RiskEngine,
    state: &mut SimulationState,
) -> Result<()> {
    let slip = (config.slippage_bps.max(0.0)) / 10_000.0;
    let lev_mult = config.allocator.effective_leverage_multiplier();
    let min_delta = config.allocator.min_rebalance_delta_weight.max(1e-4);
    let min_hold_ms = config.min_position_hold_sec.max(0) * 1000;
    let early_loser_enabled = config.allocator.hysteresis.early_loser_invalidation_enabled;

    let asset_by_id: HashMap<i64, &ExchangeAsset> =
        point_assets.iter().map(|a| (a.id, a)).collect();

    // Rolling 24h peak-to-trough drawdown check (`max_daily_drawdown_pct`) + Ruin Halt (`min_equity_halt_usdc`)
    let dd_window_start = bar_close_ts_ms - 86_400_000;
    while state
        .rolling_24h_equity
        .front()
        .is_some_and(|&(ts, _)| ts < dd_window_start)
    {
        state.rolling_24h_equity.pop_front();
    }
    let peak_24h = state
        .rolling_24h_equity
        .iter()
        .map(|&(_, eq)| eq)
        .fold(mid_ledger.total_equity_usdc, f64::max);
    let dd_frac = if peak_24h > 0.0 && mid_ledger.total_equity_usdc.is_finite() {
        ((peak_24h - mid_ledger.total_equity_usdc) / peak_24h).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let drawdown_halted = peak_24h > 0.0 && dd_frac >= config.max_daily_drawdown_pct;
    if drawdown_halted {
        state.drawdown_halt_bars += 1;
    }
    let ruin_halted = mid_ledger.is_ruin_halted(risk_engine.limits.min_equity_halt_usdc);
    if ruin_halted {
        state.ruin_halted = true;
    }
    let entries_halted = drawdown_halted || ruin_halted;

    for sig in &cycle_out.signals {
        if sig.rebalance_action == ACTION_COOLDOWN_BLOCKED {
            if sig.raw_combined_weight.abs() * lev_mult >= min_delta {
                state.cooldown_blocks += 1;
            }
            continue;
        }
        if sig.rebalance_action == ACTION_REGIME_BLOCKED {
            continue;
        }

        let Some(&asset) = asset_by_id.get(&sig.asset_id) else {
            continue;
        };
        let tick_floor = asset.tick_size.max(1e-8);

        if sig.rebalance_action != ACTION_INCREASE {
            if let Some(inc) = state.pending_increases.remove(&sig.asset_id) {
                state.initial_limit_price_by_pid.remove(&inc.id);
            }
        }

        let mut is_flipped_entry = false;

        if matches!(sig.rebalance_action.as_str(), ACTION_CLOSE | ACTION_FLIP) {
            let can_exit = if let Some(pos) = state.active_positions.get(&sig.asset_id) {
                if PositionStatus::parse(&pos.status) == PositionStatus::Open {
                    let side = OrderSide::parse(&pos.side)?;
                    let opened_ts = pos.opened_at_ms.unwrap_or(pos.created_at_ms);
                    let held_ms = bar_close_ts_ms.saturating_sub(opened_ts);
                    let upnl_frac = ((asset.mark_price - pos.entry_price)
                        / pos.entry_price.max(1e-12))
                        * side.sign();
                    let bypass_hold =
                        early_loser_enabled && upnl_frac <= -0.003 && held_ms >= 120_000;
                    if min_hold_ms > 0 && held_ms < min_hold_ms && !bypass_hold {
                        false
                    } else if config.profit_gate_signal_exits
                        && sig.rebalance_action == ACTION_CLOSE
                    {
                        let gross_unrealized = (asset.mark_price - pos.entry_price)
                            * pos.filled_quantity
                            * side.sign();
                        let est_exit_fee = (asset.mark_price * pos.filled_quantity).abs()
                            * (config.taker_fee_bps / 10_000.0);
                        let net_exit_pnl = gross_unrealized - pos.fees_paid_usdc - est_exit_fee
                            + pos.funding_accrued_usdc;
                        let entry_notional = (pos.entry_price * pos.filled_quantity).abs();
                        let min_req = entry_notional * config.signal_exit_min_net_profit_pct;
                        if net_exit_pnl < min_req {
                            state.profit_gate_blocks += 1;
                            false
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                } else {
                    true
                }
            } else {
                false
            };

            if can_exit {
                if let Some(pos) = state.active_positions.remove(&sig.asset_id) {
                    state.initial_limit_price_by_pid.remove(&pos.id);
                    if PositionStatus::parse(&pos.status) == PositionStatus::Open {
                        let side = OrderSide::parse(&pos.side)?;
                        let raw_px = asset.mark_price;
                        let exit_px =
                            round_dp((raw_px * (1.0 - side.sign() * slip)).max(tick_floor), 8);
                        let closed = settle_full_close(
                            pos,
                            raw_px,
                            exit_px,
                            config.taker_fee_bps,
                            bar_close_ts_ms,
                            sig.rebalance_action.clone(),
                            state,
                        )?;
                        state.closed_positions.push(closed);
                        if sig.rebalance_action == ACTION_FLIP {
                            is_flipped_entry = true;
                        }
                    }
                }
            }
        } else if sig.rebalance_action == ACTION_REDUCE {
            let existing_status = state
                .active_positions
                .get(&sig.asset_id)
                .map(|p| PositionStatus::parse(&p.status));
            if existing_status == Some(PositionStatus::Working) {
                if let Some(pos) = state.active_positions.remove(&sig.asset_id) {
                    state.initial_limit_price_by_pid.remove(&pos.id);
                }
            } else if existing_status == Some(PositionStatus::Open) {
                if let Some(mut pos) = state.active_positions.remove(&sig.asset_id) {
                    let side = match OrderSide::parse(&pos.side) {
                        Ok(s) => s,
                        Err(e) => {
                            state.active_positions.insert(sig.asset_id, pos);
                            return Err(e);
                        }
                    };
                    let mut full_close = false;
                    let opened_ts = pos.opened_at_ms.unwrap_or(pos.created_at_ms);
                    let held_ms = bar_close_ts_ms.saturating_sub(opened_ts);
                    let upnl_frac = ((asset.mark_price - pos.entry_price)
                        / pos.entry_price.max(1e-12))
                        * side.sign();
                    let bypass_hold =
                        early_loser_enabled && upnl_frac <= -0.003 && held_ms >= 120_000;
                    if !(min_hold_ms > 0 && held_ms < min_hold_ms && !bypass_hold) {
                        let target_qty = if mid_ledger.total_equity_usdc > 1e-6 {
                            round_to_lot(
                                (mid_ledger.total_equity_usdc * sig.target_weight.abs())
                                    / asset.mark_price.max(1e-8),
                                asset.lot_step_size,
                            )
                        } else {
                            0.0
                        };
                        let trim_qty = round_to_lot(
                            (pos.filled_quantity - target_qty).max(0.0),
                            asset.lot_step_size,
                        );
                        if sig.target_weight.abs() < 1e-4
                            || trim_qty >= pos.filled_quantity - asset.lot_step_size * 0.5
                        {
                            full_close = true;
                        } else if trim_qty > 0.0
                            && trim_qty * asset.mark_price
                                >= risk_engine.limits.min_order_notional_usdc
                        {
                            settle_partial_reduce(
                                &mut pos,
                                asset,
                                trim_qty,
                                side,
                                slip,
                                config.taker_fee_bps,
                                bar_close_ts_ms,
                                mid_ledger.total_equity_usdc,
                                state,
                            );
                        }
                    }
                    if full_close {
                        state.initial_limit_price_by_pid.remove(&pos.id);
                        let raw_px = asset.mark_price;
                        let exit_px =
                            round_dp((raw_px * (1.0 - side.sign() * slip)).max(tick_floor), 8);
                        let closed = settle_full_close(
                            pos,
                            raw_px,
                            exit_px,
                            config.taker_fee_bps,
                            bar_close_ts_ms,
                            ACTION_REDUCE.to_string(),
                            state,
                        )?;
                        state.closed_positions.push(closed);
                    } else {
                        state.active_positions.insert(sig.asset_id, pos);
                    }
                }
            }
        } else if sig.rebalance_action == ACTION_INCREASE && !entries_halted {
            let total_active = state.active_positions.len() + state.pending_increases.len();
            let interval_ok = config.min_entry_interval_sec <= 0
                || state.last_entry_ts_ms.is_none_or(|last| {
                    bar_close_ts_ms.saturating_sub(last) >= config.min_entry_interval_sec * 1000
                });
            if total_active >= config.max_open_positions.max(1) {
                state.max_positions_blocks += 1;
            } else if !interval_ok {
                state.entry_interval_blocks += 1;
            } else {
                if let Some(pos) = state.active_positions.get(&sig.asset_id) {
                    if PositionStatus::parse(&pos.status) == PositionStatus::Open
                        && !state.pending_increases.contains_key(&sig.asset_id)
                    {
                        let cur_vec = state.active_positions_snapshot();
                        let cur_ledger =
                            risk_engine.evaluate_ledger(state.realized_cash_equity, &cur_vec);
                        let side = OrderSide::parse(&pos.side)?;
                        let live_current_w = if cur_ledger.total_equity_usdc > 1e-6 {
                            (pos.current_price.max(pos.entry_price) * pos.filled_quantity
                                / cur_ledger.total_equity_usdc)
                                * side.sign()
                        } else {
                            pos.portfolio_weight.abs() * side.sign()
                        };
                        let bars = point_bars.get(&asset.id).copied().unwrap_or(&[]);
                        let atr = compute_atr_from_bars(
                            bars,
                            14,
                            asset.mark_price,
                            risk_engine.limits.protective_stop_default_pct,
                        );
                        let plan = risk_engine.size_working_order_from_weights(
                            asset,
                            sig.target_weight,
                            live_current_w,
                            sig.w_s1_carry_mom,
                            sig.w_s2_trend_breakout,
                            sig.w_s3_event_reversion,
                            &cur_ledger,
                            Some(atr),
                        );
                        if plan.approved && plan.side == side {
                            let pid = state.next_pos_id;
                            state.next_pos_id += 1;
                            state
                                .initial_limit_price_by_pid
                                .insert(pid, plan.limit_price);
                            state.last_entry_ts_ms = Some(bar_close_ts_ms);
                            state.pending_increases.insert(
                                asset.id,
                                build_working_position(
                                    pid,
                                    asset.id,
                                    format!("bt-inc-{pid}"),
                                    &plan,
                                    bar_close_ts_ms,
                                ),
                            );
                        }
                    }
                }
            }
        }

        // Re-anchor unfilled WORKING orders: enforce drift cap (60 bps) and IOC escalation after max_alo_chase_ticks (3)
        let mut abort_chase = false;
        let mut cancel_working = false;
        let mut escalate_ioc = false;
        let mut working_side_opt = None;
        if let Some(pos) = state.active_positions.get_mut(&sig.asset_id) {
            if PositionStatus::parse(&pos.status) == PositionStatus::Working {
                let w_side = OrderSide::parse(&pos.side)?;
                working_side_opt = Some(w_side);
                if sig.target_weight * w_side.sign() <= 0.0 {
                    cancel_working = true;
                } else {
                    let new_px = round_to_tick(asset.mark_price, asset.tick_size, w_side.is_buy());
                    let init_px = state
                        .initial_limit_price_by_pid
                        .get(&pos.id)
                        .copied()
                        .unwrap_or(pos.entry_price);
                    let drift_bps = ((new_px - init_px).abs() / init_px.max(1e-12)) * 10_000.0;
                    if drift_bps > config.max_alo_chase_drift_bps {
                        abort_chase = true;
                    } else if pos.chase_count >= config.max_alo_chase_ticks && !entries_halted {
                        escalate_ioc = true;
                    } else if new_px > 0.0 {
                        pos.entry_price = new_px;
                        pos.current_price = new_px;
                        pos.fsm_state = "CHASING_ALO".to_string();
                        pos.highest_price_seen = new_px;
                        pos.lowest_price_seen = new_px;
                        pos.chase_count += 1;
                        pos.updated_at_ms = bar_close_ts_ms;
                    }
                }
            }
        }

        if cancel_working {
            if let Some(pos) = state.active_positions.remove(&sig.asset_id) {
                state.initial_limit_price_by_pid.remove(&pos.id);
            }
        } else if abort_chase {
            if let Some(pos) = state.active_positions.remove(&sig.asset_id) {
                state.initial_limit_price_by_pid.remove(&pos.id);
                state.chase_abort_count += 1;
                let cd_ms = config.chase_reject_cooldown_sec.max(1) * 1000;
                state.cooldown_until_ms.insert(
                    asset.id,
                    (
                        bar_close_ts_ms + cd_ms,
                        asset.symbol.clone(),
                        "CHURN_GUARD".to_string(),
                    ),
                );
            }
        } else if escalate_ioc {
            if let Some(w_side) = working_side_opt {
                let cur_vec = state.active_positions_snapshot();
                let cur_ledger = risk_engine.evaluate_ledger(state.realized_cash_equity, &cur_vec);
                let bars = point_bars.get(&asset.id).copied().unwrap_or(&[]);
                let atr = compute_atr_from_bars(
                    bars,
                    14,
                    asset.mark_price,
                    risk_engine.limits.protective_stop_default_pct,
                );
                let ioc_slip_frac = config
                    .slippage_bps
                    .max(0.0)
                    .min(config.entry_ioc_max_slippage_bps)
                    / 10_000.0;
                let ioc_fill_px = round_dp(
                    (asset.mark_price * (1.0 + w_side.sign() * ioc_slip_frac)).max(tick_floor),
                    8,
                );
                if let Some(pos) = state.active_positions.get_mut(&sig.asset_id) {
                    let commit = risk_engine.confirm_order_fill_to_open(
                        w_side,
                        ioc_fill_px,
                        pos.requested_quantity,
                        pos.reserved_margin_usdc,
                        false,
                        asset.tick_size,
                        Some(atr),
                        &cur_ledger,
                    );
                    let entry_slip = round_dp(
                        (ioc_fill_px - asset.mark_price).abs() * commit.filled_quantity,
                        4,
                    );
                    pos.status = "OPEN".to_string();
                    pos.fsm_state = "FILLED_OPEN".to_string();
                    pos.entry_price = ioc_fill_px;
                    pos.current_price = asset.mark_price;
                    pos.filled_quantity = commit.filled_quantity;
                    pos.reserved_margin_usdc = 0.0;
                    pos.committed_notional_usdc = commit.committed_notional_usdc;
                    pos.protective_stop_px = commit.protective_stop_px;
                    pos.take_profit_px = commit.take_profit_px;
                    pos.highest_price_seen = ioc_fill_px.max(asset.mark_price);
                    pos.lowest_price_seen = ioc_fill_px.min(asset.mark_price);
                    pos.unrealized_pnl_usdc = round_dp(
                        (asset.mark_price - ioc_fill_px) * commit.filled_quantity * w_side.sign(),
                        4,
                    );
                    pos.fees_paid_usdc += commit.fees_paid_usdc;
                    pos.slippage_paid_usdc += entry_slip;
                    pos.opened_at_ms = Some(bar_close_ts_ms);
                    pos.updated_at_ms = bar_close_ts_ms;
                    state.initial_limit_price_by_pid.remove(&pos.id);
                    state.realized_cash_equity -= commit.fees_paid_usdc;
                    state.total_fees_usdc += commit.fees_paid_usdc;
                    state.ioc_escalations += 1;
                }
            }
        }

        if matches!(
            sig.rebalance_action.as_str(),
            ACTION_OPEN_LONG | ACTION_OPEN_SHORT | ACTION_FLIP
        ) && !state.active_positions.contains_key(&sig.asset_id)
            && !entries_halted
        {
            if state.active_positions.len() + state.pending_increases.len()
                >= config.max_open_positions.max(1)
            {
                state.max_positions_blocks += 1;
                continue;
            }
            if !is_flipped_entry
                && config.min_entry_interval_sec > 0
                && state.last_entry_ts_ms.is_some_and(|last| {
                    bar_close_ts_ms.saturating_sub(last) < config.min_entry_interval_sec * 1000
                })
            {
                state.entry_interval_blocks += 1;
                continue;
            }

            let cur_vec = state.active_positions_snapshot();
            let cur_ledger = risk_engine.evaluate_ledger(state.realized_cash_equity, &cur_vec);
            let bars = point_bars.get(&asset.id).copied().unwrap_or(&[]);
            let atr = compute_atr_from_bars(
                bars,
                14,
                asset.mark_price,
                risk_engine.limits.protective_stop_default_pct,
            );

            let plan = risk_engine.size_working_order_from_weights(
                asset,
                sig.target_weight,
                0.0,
                sig.w_s1_carry_mom,
                sig.w_s2_trend_breakout,
                sig.w_s3_event_reversion,
                &cur_ledger,
                Some(atr),
            );

            if plan.approved {
                let pid = state.next_pos_id;
                state.next_pos_id += 1;
                state
                    .initial_limit_price_by_pid
                    .insert(pid, plan.limit_price);
                state.last_entry_ts_ms = Some(bar_close_ts_ms);
                state.active_positions.insert(
                    asset.id,
                    build_working_position(
                        pid,
                        asset.id,
                        format!("bt-coid-{pid}"),
                        &plan,
                        bar_close_ts_ms,
                    ),
                );
            }
        }
    }

    Ok(())
}

fn build_working_position(
    pid: i64,
    asset_id: i64,
    client_order_id: String,
    plan: &aegis_risk::WorkingOrderSizingPlan,
    ts_ms: i64,
) -> PositionRecord {
    PositionRecord {
        id: pid,
        asset_id,
        client_order_id,
        side: plan.side.as_str().to_string(),
        status: "WORKING".to_string(),
        fsm_state: "WORKING_ALO".to_string(),
        entry_price: plan.limit_price,
        current_price: plan.limit_price,
        exit_price: None,
        requested_quantity: plan.requested_quantity,
        filled_quantity: 0.0,
        reserved_margin_usdc: plan.reserved_margin_usdc,
        committed_notional_usdc: 0.0,
        portfolio_weight: plan.portfolio_weight,
        s1_weight_contrib: plan.s1_weight_contrib,
        s2_weight_contrib: plan.s2_weight_contrib,
        s3_weight_contrib: plan.s3_weight_contrib,
        protective_stop_px: plan.protective_stop_px,
        protective_stop_oid: None,
        take_profit_px: plan.take_profit_px,
        take_profit_oid: None,
        highest_price_seen: plan.limit_price,
        lowest_price_seen: plan.limit_price,
        chase_count: 0,
        realized_pnl_usdc: 0.0,
        unrealized_pnl_usdc: 0.0,
        fees_paid_usdc: 0.0,
        funding_accrued_usdc: 0.0,
        slippage_paid_usdc: 0.0,
        exchange_order_id: Some(format!("hl-bt-{pid}")),
        created_at_ms: ts_ms,
        opened_at_ms: None,
        closed_at_ms: None,
        updated_at_ms: ts_ms,
        exit_reason: None,
    }
}

/// Step 6: Records end-of-bar `PortfolioSnapshot` stamped at `bar_close_ts_ms` (`R14-L22`) and updates drawdown/return series.
pub fn record_bar_snapshot(
    bar_close_ts_ms: i64,
    cycle_out: &SignalCycleOutput,
    risk_engine: &RiskEngine,
    state: &mut SimulationState,
) {
    let end_positions = state.active_positions_snapshot();
    let end_ledger = risk_engine.evaluate_ledger(state.realized_cash_equity, &end_positions);

    let bar_ret = if state.prev_total_equity > 1e-6 {
        (end_ledger.total_equity_usdc - state.prev_total_equity) / state.prev_total_equity
    } else {
        0.0
    };
    state.bar_returns.push(bar_ret);
    state.prev_total_equity = end_ledger.total_equity_usdc;
    state.peak_equity = state.peak_equity.max(end_ledger.total_equity_usdc);
    if state.peak_equity > 0.0 {
        let dd = if end_ledger.total_equity_usdc <= 0.0 {
            100.0
        } else {
            (((state.peak_equity - end_ledger.total_equity_usdc) / state.peak_equity) * 100.0)
                .clamp(0.0, 100.0)
        };
        state.max_drawdown_pct = state.max_drawdown_pct.max(dd);
    }

    state
        .rolling_24h_equity
        .push_back((bar_close_ts_ms, end_ledger.total_equity_usdc));
    state.last_bar_close_ts_ms = Some(bar_close_ts_ms);

    state.equity_curve.push(PortfolioSnapshot {
        id: (state.equity_curve.len() + 1) as i64,
        total_equity_usdc: end_ledger.total_equity_usdc,
        uncapped_equity_usdc: end_ledger.total_equity_usdc,
        available_cash_usdc: end_ledger.available_cash_usdc,
        reserved_order_cash_usdc: end_ledger.reserved_order_cash_usdc,
        committed_notional_usdc: end_ledger.committed_notional_usdc,
        net_notional_usdc: end_ledger.net_notional_usdc,
        gross_leverage_x: end_ledger.gross_leverage_x,
        net_leverage_x: end_ledger.net_leverage_x,
        market_beta_exposure: cycle_out.breadth.market_beta_exposure,
        market_breadth_bull: cycle_out.breadth.market_breadth_bull,
        market_regime_label: cycle_out.breadth.market_regime_label.clone(),
        risk_tier: cycle_out.risk_tier.as_str().to_string(),
        leverage_multiplier: cycle_out.leverage_multiplier,
        s1_gross_weight: cycle_out.s1_gross_weight,
        s2_gross_weight: cycle_out.s2_gross_weight,
        s3_gross_weight: cycle_out.s3_gross_weight,
        working_order_count: end_ledger.working_order_count,
        open_long_count: end_ledger.open_long_count,
        open_short_count: end_ledger.open_short_count,
        eligible_coin_count: cycle_out.breadth.eligible_coin_count,
        created_at_ms: bar_close_ts_ms,
        updated_at_ms: bar_close_ts_ms,
    });
}

/// Attributes remaining end-of-run `OPEN` positions' net unrealized PnL across S1/S2/S3.
pub fn finalize_open_attribution(state: &mut SimulationState) -> Result<()> {
    let mut attributions = Vec::new();
    for pos in state.active_positions.values() {
        if PositionStatus::parse(&pos.status) == PositionStatus::Open {
            let side = OrderSide::parse(&pos.side)?;
            let open_net_pnl = round_dp(
                pos.unrealized_pnl_usdc - pos.fees_paid_usdc + pos.funding_accrued_usdc,
                4,
            );
            attributions.push((
                open_net_pnl,
                side,
                pos.s1_weight_contrib,
                pos.s2_weight_contrib,
                pos.s3_weight_contrib,
            ));
        }
    }
    for (open_net_pnl, side, w1, w2, w3) in attributions {
        attribute_pnl_to_sleeves(
            open_net_pnl,
            side,
            w1,
            w2,
            w3,
            &mut state.s1_attribution_usdc,
            &mut state.s2_attribution_usdc,
            &mut state.s3_attribution_usdc,
        );
    }
    Ok(())
}
