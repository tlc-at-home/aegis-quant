//! Champion `NPS-01` Bi-Directional Naked-Option Perp Scanner (`aegis-pond::scanner`).
//!
//! Evaluates all `ACTIVE` coins in `pond_universe` on the hourly S-Cycle:
//! - Computes 168-hour trailing funding z-score (`z_t`), 1H/4H ATR_14, and 4H EMA_20/EMA_50.
//! - Applies all 7 Entry Gates, `pond_direction_policy` (`both`, `short_only`, `long_only`),
//!   and the single-cell-per-asset invariant.
//! - Ranks qualifying candidates by `premium_score` and sizes up to 3 new cells per cycle.

use aegis_core::{
    compute_atr_from_bars, compute_ema, compute_trailing_z_score, round_dp, round_step, OhlcvBar,
    OptionAnalogue, PondDirectionPolicy, PondSide, PondUniverseRow,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Raw market inputs for a single Pond universe asset during an S-Cycle scan.
#[derive(Debug, Clone)]
pub struct PondAssetScanInput {
    pub universe_row: PondUniverseRow,
    pub funding_series_168h: Vec<f64>,
    pub bars_1h: Vec<OhlcvBar>,
    pub bars_4h: Vec<OhlcvBar>,
    pub tick_size: f64,
    pub lot_step_size: f64,
}

/// Configuration parameters for the `NPS-01` scanner and sizing engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondScannerParams {
    pub champion_id: String,
    pub direction_policy: PondDirectionPolicy,
    pub funding_z_window_hours: usize,
    pub z_entry_threshold: f64,
    pub min_funding_abs: f64,
    pub min_daily_vol_usdc: f64,
    pub liquidity_score_ref_usdc: f64,
    pub trend_veto_ema_fast: usize,
    pub trend_veto_ema_slow: usize,
    pub trend_veto_overextension_pct: f64,
    pub blowoff_atr_mult: f64,
    pub blowoff_3h_adverse_atr_mult: f64,
    pub trend_veto_4h_atr_mult: f64,
    pub initial_stop_atr_mult: f64,
    pub chandelier_atr_mult: f64,
    pub min_cell_risk_usdc: f64,
    pub max_cell_risk_usdc: f64,
    pub max_cell_notional_usdc: f64,
    pub max_open_heat_usdc: f64,
    pub max_concurrent_cells: usize,
    pub max_entries_per_cycle: usize,
    pub max_gross_leverage: f64,
    pub pond_equity_usdc: f64,
    pub current_open_cells: usize,
    pub current_open_heat_usdc: f64,
    pub current_deployed_notional_usdc: f64,
    pub entry_frozen: bool,
    pub atr_period: usize,
    pub gate6_atr_4h_floor_mult: f64,
    pub min_stop_pct_floor: f64,
    pub conviction_z_span: f64,
    pub conviction_scale_min: f64,
    pub conviction_scale_max: f64,
    pub reject_synthetic_volume: bool,
}

impl Default for PondScannerParams {
    fn default() -> Self {
        Self {
            champion_id: "NPS-01".to_string(),
            direction_policy: PondDirectionPolicy::Both,
            funding_z_window_hours: 168,
            z_entry_threshold: 2.0,
            min_funding_abs: 0.000015,
            min_daily_vol_usdc: 50_000.0,
            liquidity_score_ref_usdc: 250_000.0,
            trend_veto_ema_fast: 16,
            trend_veto_ema_slow: 64,
            trend_veto_overextension_pct: 0.08,
            blowoff_atr_mult: 3.0,
            blowoff_3h_adverse_atr_mult: 2.2,
            trend_veto_4h_atr_mult: 2.5,
            initial_stop_atr_mult: 2.5,
            chandelier_atr_mult: 3.8,
            min_cell_risk_usdc: 25.0,
            max_cell_risk_usdc: 75.0,
            max_cell_notional_usdc: 1250.0,
            max_open_heat_usdc: 200.0,
            max_concurrent_cells: 8,
            max_entries_per_cycle: 3,
            max_gross_leverage: 1.5,
            pond_equity_usdc: 5000.0,
            current_open_cells: 0,
            current_open_heat_usdc: 0.0,
            current_deployed_notional_usdc: 0.0,
            entry_frozen: false,
            atr_period: 14,
            gate6_atr_4h_floor_mult: 1.8,
            min_stop_pct_floor: 0.005,
            conviction_z_span: 2.0,
            conviction_scale_min: 0.5,
            conviction_scale_max: 1.5,
            reject_synthetic_volume: false,
        }
    }
}

/// A sized candidate cell produced by the `NPS-01` scanner after passing all 7 gates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondSizedCandidate {
    pub symbol: String,
    pub base_asset: String,
    pub side: PondSide,
    pub option_analogue: OptionAnalogue,
    pub reference_price: f64,
    pub funding_rate_hourly: f64,
    pub funding_z_score: f64,
    pub premium_score: f64,
    pub atr_14_1h: f64,
    pub atr_14_4h: f64,
    pub quantity: f64,
    pub notional_usdc: f64,
    pub risk_budget_usdc: f64,
    pub conviction_multiplier: f64,
    pub initial_stop_price: f64,
    pub chandelier_stop_price: f64,
    pub bar_high: f64,
    pub bar_low: f64,
    pub bar_volume: f64,
    pub tick_size: f64,
    pub lot_step_size: f64,
}

/// Full result of an hourly `NPS-01` S-Cycle scan across the 80-coin Pond universe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondScanOutput {
    pub active_universe_count: usize,
    pub candidates_passed_gates: usize,
    pub selected_entries: Vec<PondSizedCandidate>,
    pub rejections_by_gate: Vec<(String, String)>,
}

/// Structured outcome of evaluating Gates 1–6 on a single asset (`Q7`).
#[derive(Debug, Clone)]
pub struct GateEvaluationOutcome {
    pub side: PondSide,
    pub option_analogue: OptionAnalogue,
    pub funding_rate_hourly: f64,
    pub funding_z_score: f64,
    pub premium_score: f64,
    pub atr_14_1h: f64,
    pub atr_14_4h: f64,
}

/// Evaluates Gates 1–6 on a single asset and returns a raw candidate prior to portfolio sizing.
pub fn evaluate_asset_gates_1_to_6(
    input: &PondAssetScanInput,
    params: &PondScannerParams,
    open_or_cooldown_symbols: &HashSet<String>,
) -> std::result::Result<GateEvaluationOutcome, &'static str> {
    // Gate 1: Universe Eligibility & Liquidity Floor (and live synthetic volume guard NM-13)
    if !input.universe_row.status.eq_ignore_ascii_case("ACTIVE") {
        return Err("GATE1_UNIVERSE_NOT_ACTIVE");
    }
    if params.reject_synthetic_volume && input.universe_row.is_synthetic_volume != 0 {
        return Err("GATE1_SYNTHETIC_VOLUME_REJECTED_IN_LIVE");
    }
    if params.reject_synthetic_volume
        && input
            .bars_1h
            .last()
            .map(|b| b.is_synthetic != 0)
            .unwrap_or(false)
    {
        return Err("GATE1_SYNTHETIC_BARS_REJECTED_IN_LIVE");
    }
    if input.universe_row.median_daily_volume_usdc < params.min_daily_vol_usdc {
        return Err("GATE1_VOLUME_BELOW_FLOOR");
    }

    // Gate 2: Cooldown Clearance & Single-Cell-Per-Asset Invariant
    if open_or_cooldown_symbols.contains(&input.universe_row.symbol) {
        return Err("GATE2_OPEN_CELL_OR_COOLDOWN_ACTIVE");
    }

    // Continuous 168h Data Warmup check
    let req_window = params.funding_z_window_hours.max(1);
    if input.funding_series_168h.len() < req_window || input.bars_1h.len() < req_window {
        return Err("GATE5_INSUFFICIENT_168H_WARMUP");
    }

    let Some(&r_hourly) = input.funding_series_168h.last() else {
        return Err("GATE3_MISSING_FUNDING_DATA");
    };

    // Gate 3: Absolute Funding Floor
    if r_hourly.abs() < params.min_funding_abs {
        return Err("GATE3_FUNDING_BELOW_ABS_FLOOR");
    }

    // Gate 4: Statistical Extreme (z-Score Gate) + Direction Policy
    let (z_t, _, _) = compute_trailing_z_score(&input.funding_series_168h, req_window);
    if z_t.abs() < params.z_entry_threshold {
        return Err("GATE4_Z_SCORE_BELOW_THRESHOLD");
    }

    let (side, analogue) = if r_hourly >= params.min_funding_abs && z_t >= params.z_entry_threshold {
        if !params.direction_policy.allows_short_call() {
            return Err("GATE4_DIRECTION_POLICY_BLOCKS_SHORT_CALL");
        }
        (PondSide::Short, OptionAnalogue::ShortCall)
    } else if r_hourly <= -params.min_funding_abs && z_t <= -params.z_entry_threshold {
        if !params.direction_policy.allows_short_put() {
            return Err("GATE4_DIRECTION_POLICY_BLOCKS_SHORT_PUT");
        }
        (PondSide::Long, OptionAnalogue::ShortPut)
    } else {
        return Err("GATE4_FUNDING_AND_Z_SIGN_MISMATCH");
    };

    // Gate 5: Parabolic Blow-Off / Crash Veto (1H Bar Filter)
    let atr_period = params.atr_period.max(1);
    let atr_1h = compute_atr_from_bars(&input.bars_1h, atr_period);
    let Some(last_1h) = input.bars_1h.last() else {
        return Err("GATE5_MISSING_1H_BARS");
    };
    if atr_1h <= 0.0 || last_1h.close <= 0.0 {
        return Err("GATE5_INVALID_1H_ATR");
    }
    let range_1h = (last_1h.high - last_1h.low).max(0.0);
    if range_1h > params.blowoff_atr_mult * atr_1h {
        return Err("GATE5_1H_BAR_RANGE_BLOWOFF_VETO");
    }
    if input.bars_1h.len() >= 4 {
        let close_3h_ago = input.bars_1h[input.bars_1h.len() - 4].close;
        let adverse_move = match side {
            PondSide::Short => last_1h.close - close_3h_ago,
            PondSide::Long => close_3h_ago - last_1h.close,
        };
        if adverse_move > params.blowoff_3h_adverse_atr_mult * atr_1h {
            return Err("GATE5_3H_PARABOLIC_MOMENTUM_VETO");
        }
    }

    // Gate 6: 4H Structural Momentum & EMA_64 Overextension Veto (Blueprint Section 4.4)
    let atr_4h = compute_atr_from_bars(&input.bars_4h, atr_period)
        .max(atr_1h * params.gate6_atr_4h_floor_mult.max(0.0));
    if !input.bars_4h.is_empty() {
        let closes_4h: Vec<f64> = input.bars_4h.iter().map(|b| b.close).collect();
        let ema_fast_vec = compute_ema(&closes_4h, params.trend_veto_ema_fast.max(1));
        let ema_slow_vec = compute_ema(&closes_4h, params.trend_veto_ema_slow.max(1));
        if let (Some(&close_4h), Some(&ema_fast), Some(&ema_slow)) =
            (closes_4h.last(), ema_fast_vec.last(), ema_slow_vec.last())
        {
            let overext = params.trend_veto_overextension_pct.max(0.0);
            match side {
                PondSide::Short => {
                    // Veto when 4H close > EMA_64 * (1 + 0.08) or extreme structural uptrend
                    if close_4h > ema_slow * (1.0 + overext)
                        || (close_4h > ema_fast
                            && ema_fast > ema_slow
                            && (close_4h - ema_fast) / atr_4h.max(1e-8)
                                > params.trend_veto_4h_atr_mult)
                    {
                        return Err("GATE6_4H_STRUCTURAL_UPTREND_VETO");
                    }
                }
                PondSide::Long => {
                    // Veto when 4H close < EMA_64 * (1 - 0.08) or extreme structural downtrend
                    if close_4h < ema_slow * (1.0 - overext)
                        || (close_4h < ema_fast
                            && ema_fast < ema_slow
                            && (ema_fast - close_4h) / atr_4h.max(1e-8)
                                > params.trend_veto_4h_atr_mult)
                    {
                        return Err("GATE6_4H_STRUCTURAL_DOWNTREND_VETO");
                    }
                }
            }
        }
    }

    let vol_ref = params.liquidity_score_ref_usdc.max(1.0);
    let vol_factor = (input.universe_row.median_daily_volume_usdc / vol_ref)
        .clamp(0.0, 1.0)
        .sqrt();
    let premium_score = round_dp(z_t.abs() * vol_factor, 6);

    Ok(GateEvaluationOutcome {
        side,
        option_analogue: analogue,
        funding_rate_hourly: r_hourly,
        funding_z_score: round_dp(z_t, 4),
        premium_score,
        atr_14_1h: atr_1h,
        atr_14_4h: atr_4h,
    })
}

/// Runs the full `NPS-01` S-Cycle scan across all assets, applying Gates 1–7, ranking by
/// `premium_score`, and sizing up to `max_entries_per_cycle` (3) cells.
pub fn run_nps01_scan(
    inputs: &[PondAssetScanInput],
    params: &PondScannerParams,
    open_or_cooldown_symbols: &HashSet<String>,
) -> PondScanOutput {
    let active_universe_count = inputs
        .iter()
        .filter(|i| i.universe_row.status.eq_ignore_ascii_case("ACTIVE"))
        .count();

    let mut rejections = Vec::new();
    if params.entry_frozen {
        return PondScanOutput {
            active_universe_count,
            candidates_passed_gates: 0,
            selected_entries: Vec::new(),
            rejections_by_gate: vec![("ALL".to_string(), "GATE7_ENTRY_FROZEN".to_string())],
        };
    }

    let mut raw_candidates: Vec<(&PondAssetScanInput, GateEvaluationOutcome)> = Vec::new();
    for item in inputs {
        match evaluate_asset_gates_1_to_6(item, params, open_or_cooldown_symbols) {
            Ok(outcome) => {
                raw_candidates.push((item, outcome));
            }
            Err(reason) => {
                rejections.push((item.universe_row.symbol.clone(), reason.to_string()));
            }
        }
    }

    // Rank descending by Premium Score (with deterministic symbol tie-breaker)
    raw_candidates.sort_by(|a, b| {
        b.1.premium_score
            .partial_cmp(&a.1.premium_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.universe_row.symbol.cmp(&b.0.universe_row.symbol))
    });

    let candidates_passed_gates = raw_candidates.len();
    let mut selected_entries = Vec::new();
    let mut open_cells = params.current_open_cells;
    let mut open_heat = params.current_open_heat_usdc;
    let mut deployed_notional = params.current_deployed_notional_usdc;
    let max_total_notional = params.pond_equity_usdc.max(1.0) * params.max_gross_leverage;

    for (item, eval) in raw_candidates {
        let GateEvaluationOutcome {
            side,
            option_analogue: analogue,
            funding_rate_hourly: r_hourly,
            funding_z_score: z_t,
            premium_score,
            atr_14_1h: atr_1h,
            atr_14_4h: atr_4h,
        } = eval;
        if selected_entries.len() >= params.max_entries_per_cycle {
            break;
        }
        // Gate 7: Book B Portfolio Risk Gate
        if open_cells >= params.max_concurrent_cells {
            rejections.push((
                item.universe_row.symbol.clone(),
                "GATE7_MAX_CONCURRENT_CELLS".to_string(),
            ));
            continue;
        }
        let remaining_heat = params.max_open_heat_usdc - open_heat;
        if remaining_heat < params.min_cell_risk_usdc {
            rejections.push((
                item.universe_row.symbol.clone(),
                "GATE7_MAX_OPEN_HEAT".to_string(),
            ));
            continue;
        }
        let min_notional = item.universe_row.min_notional_usdc.max(10.0);
        let remaining_notional_cap = (max_total_notional - deployed_notional).max(0.0);
        if remaining_notional_cap < min_notional {
            rejections.push((
                item.universe_row.symbol.clone(),
                "GATE7_MAX_GROSS_LEVERAGE".to_string(),
            ));
            continue;
        }

        let Some(last_1h) = item.bars_1h.last() else {
            continue;
        };
        let ref_px = last_1h.close;
        let stop_distance = (params.initial_stop_atr_mult * atr_1h)
            .max(ref_px * params.min_stop_pct_floor.max(0.0001));

        // Conviction-scaled cell risk budget [conviction_scale_min, conviction_scale_max] of baseline risk
        let conviction_scale = ((z_t.abs() - params.z_entry_threshold)
            / params.conviction_z_span.max(1e-6))
        .clamp(0.0, 1.0);
        let conviction_multiplier = round_dp(
            params.conviction_scale_min
                + conviction_scale * (params.conviction_scale_max - params.conviction_scale_min),
            4,
        );
        let desired_risk = params.min_cell_risk_usdc
            + (params.max_cell_risk_usdc - params.min_cell_risk_usdc) * conviction_scale;
        let risk_budget = round_dp(desired_risk.min(remaining_heat), 2);

        let raw_units = risk_budget / stop_distance;
        let max_notional_for_cell = params.max_cell_notional_usdc.min(remaining_notional_cap);
        let capped_units = raw_units.min(max_notional_for_cell / ref_px.max(1e-8));
        let qty = round_step(capped_units, item.lot_step_size.max(1e-6), true);
        if qty <= 0.0 {
            rejections.push((
                item.universe_row.symbol.clone(),
                "GATE7_ZERO_ROUNDED_UNITS".to_string(),
            ));
            continue;
        }

        let notional = round_dp(qty * ref_px, 2);
        if notional < min_notional {
            rejections.push((
                item.universe_row.symbol.clone(),
                "GATE7_BELOW_MIN_NOTIONAL".to_string(),
            ));
            continue;
        }
        let actual_risk = round_dp(qty * stop_distance, 2);
        if actual_risk < params.min_cell_risk_usdc * 0.5 {
            rejections.push((
                item.universe_row.symbol.clone(),
                "GATE7_BELOW_MIN_CELL_RISK".to_string(),
            ));
            continue;
        }
        let chandelier_dist = params.chandelier_atr_mult * atr_4h;

        let (initial_stop, chandelier_stop) = match side {
            PondSide::Long => (
                round_dp((ref_px - stop_distance).max(1e-8), 8),
                round_dp((last_1h.high - chandelier_dist).max(1e-8), 8),
            ),
            PondSide::Short => (
                round_dp(ref_px + stop_distance, 8),
                round_dp(last_1h.low + chandelier_dist, 8),
            ),
        };

        open_cells += 1;
        open_heat += actual_risk;
        deployed_notional += notional;

        let base_asset = item
            .universe_row
            .symbol
            .split('/')
            .next()
            .unwrap_or("COIN")
            .to_string();

        selected_entries.push(PondSizedCandidate {
            symbol: item.universe_row.symbol.clone(),
            base_asset,
            side,
            option_analogue: analogue,
            reference_price: ref_px,
            funding_rate_hourly: r_hourly,
            funding_z_score: z_t,
            premium_score,
            atr_14_1h: atr_1h,
            atr_14_4h: atr_4h,
            quantity: qty,
            notional_usdc: notional,
            risk_budget_usdc: actual_risk,
            conviction_multiplier,
            initial_stop_price: initial_stop,
            chandelier_stop_price: chandelier_stop,
            bar_high: last_1h.high,
            bar_low: last_1h.low,
            bar_volume: last_1h.volume,
            tick_size: item.tick_size,
            lot_step_size: item.lot_step_size,
        });
    }

    PondScanOutput {
        active_universe_count,
        candidates_passed_gates,
        selected_entries,
        rejections_by_gate: rejections,
    }
}
