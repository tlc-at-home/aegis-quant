//! 10-State Pond Cell Finite State Machine & 6-Tier Exit Priority Engine (`aegis-pond::cell`).
//!
//! Enforces strict state transitions (`DORMANT -> SCREENED -> CANDIDATE -> ARMED -> OPEN ->
//! TRAILED -> CLOSED -> COOLDOWN`) and evaluates the 6-Tier Exit Priority Engine in deterministic
//! precedence order (Tier 1 Catastrophic Wick Stop down to Tier 6 Delisting Exit).

use aegis_core::{round_dp, AegisError, PondCell, PondCellState, Price, Quantity, Result, Usdc};
use serde::{Deserialize, Serialize};

/// Input telemetry for evaluating the 6-Tier Exit Priority Engine on an active Pond cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellExitEvalInput {
    pub mark_price: f64,
    pub intrabar_high: f64,
    pub intrabar_low: f64,
    pub atr_14_1h: f64,
    pub bar_4h_close: f64,
    pub atr_14_4h: f64,
    pub is_4h_bar_close: bool,
    pub is_1h_boundary: bool,
    pub current_funding_hourly: f64,
    pub current_funding_z: f64,
    pub hold_hours: f64,
    pub universe_status: String,
    pub wick_buffer_atr_mult: f64,
    pub chandelier_atr_mult: f64,
    pub z_exit_threshold: f64,
    pub min_funding_abs: f64,
    pub max_hold_hours: f64,
    pub cooldown_stop_hours: i64,
    pub cooldown_normal_hours: i64,
}

impl Default for CellExitEvalInput {
    fn default() -> Self {
        Self {
            mark_price: 100.0,
            intrabar_high: 100.0,
            intrabar_low: 100.0,
            atr_14_1h: 2.0,
            bar_4h_close: 100.0,
            atr_14_4h: 4.0,
            is_4h_bar_close: false,
            is_1h_boundary: false,
            current_funding_hourly: 0.00005,
            current_funding_z: 2.2,
            hold_hours: 12.0,
            universe_status: "ACTIVE".to_string(),
            wick_buffer_atr_mult: 0.85,
            chandelier_atr_mult: 3.8,
            z_exit_threshold: 0.5,
            min_funding_abs: 0.000015,
            max_hold_hours: 72.0,
            cooldown_stop_hours: 56,
            cooldown_normal_hours: 24,
        }
    }
}

/// Output decision from the 6-Tier Exit Priority Engine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CellExitDecision {
    pub should_exit: bool,
    pub tier: Option<u8>,
    pub target_state: Option<PondCellState>,
    pub exit_reason: Option<String>,
    pub prefer_maker: bool,
    pub cooldown_hours: i64,
}

/// Validates and applies a state transition on a `PondCell`.
pub fn transition_cell_state(cell: &mut PondCell, next: PondCellState) -> Result<()> {
    let cur = PondCellState::parse(&cell.state)?;
    if !cur.can_transition_to(next) {
        return Err(AegisError::Validation(format!(
            "Illegal PondCell FSM transition: {} -> {}",
            cur.as_str(),
            next.as_str()
        )));
    }
    cell.state = next.as_str().to_string();
    Ok(())
}

/// Updates the cell's watermarks and (on 4H bar close) monotonic Chandelier stop,
/// transitioning `OPEN -> TRAILED` when the Chandelier stop ratchets.
pub fn update_cell_mark_and_chandelier(
    cell: &mut PondCell,
    mark_price: f64,
    high_price: f64,
    low_price: f64,
    atr_14_4h: f64,
    chandelier_mult: f64,
    is_4h_bar_close: bool,
) -> f64 {
    let px = Price::new(mark_price.max(1e-8));
    let cur_high = Price::new(cell.highest_price_seen.unwrap_or(px.0));
    if high_price > cur_high.0 {
        cell.highest_price_seen = Some(high_price);
    }
    let cur_low = Price::new(cell.lowest_price_seen.unwrap_or(0.0));
    if cur_low.0 <= 0.0 || (low_price > 0.0 && low_price < cur_low.0) {
        cell.lowest_price_seen = Some(low_price);
    }

    let entry = Price::new(cell.entry_price.unwrap_or(px.0));
    let qty = Quantity::new(cell.quantity.unwrap_or(0.0).abs());
    let price_pnl = Usdc::new(if cell.side == "LONG" {
        (px.0 - entry.0) * qty.0
    } else {
        (entry.0 - px.0) * qty.0
    });

    // Chandelier trailing stop ratchets monotonically only on 4H bar closes
    if is_4h_bar_close && atr_14_4h > 0.0 {
        let dist = chandelier_mult * atr_14_4h;
        let cur_chand = cell.chandelier_stop.unwrap_or(0.0);
        let init_stop = cell.initial_stop.unwrap_or(0.0);
        if cell.side == "LONG" {
            let high_seen = cell.highest_price_seen.unwrap_or(px.0);
            let raw_cand = round_dp((high_seen - dist).max(1e-8), 8);
            let candidate = if init_stop > 0.0 {
                raw_cand.max(init_stop)
            } else {
                raw_cand
            };
            if candidate > cur_chand {
                cell.chandelier_stop = Some(candidate);
                if cell.state == "OPEN" && (init_stop <= 0.0 || candidate > init_stop + 1e-8) {
                    let _ = transition_cell_state(cell, PondCellState::Trailed);
                }
            }
        } else {
            let low_seen = cell.lowest_price_seen.unwrap_or(px.0);
            let raw_cand = round_dp((low_seen + dist).max(1e-8), 8);
            let candidate = if init_stop > 0.0 {
                raw_cand.min(init_stop)
            } else {
                raw_cand
            };
            if cur_chand <= 0.0 || candidate < cur_chand {
                cell.chandelier_stop = Some(candidate);
                if cell.state == "OPEN" && (init_stop <= 0.0 || candidate < init_stop - 1e-8) {
                    let _ = transition_cell_state(cell, PondCellState::Trailed);
                }
            }
        }
    }

    round_dp(price_pnl.0, 4)
}

/// Computes the 1H funding delta in USDC on an active Pond cell using mark-to-market notional (`R3-B1`, `R3-H1`, `NL-13`, `R3-QA`).
pub fn compute_cell_hourly_funding_delta(
    cell: &PondCell,
    mark_price: f64,
    funding_rate_hourly: f64,
) -> f64 {
    let eff_px = Price::new(if mark_price > 0.0 {
        mark_price
    } else {
        cell.entry_price.unwrap_or(0.0).max(0.0)
    });
    let qty = Quantity::new(cell.quantity.unwrap_or(0.0).abs());
    let notional = Usdc::new(qty.0 * eff_px.0);
    // SHORT receives positive funding; LONG receives negative funding (pays positive funding)
    let delta = Usdc::new(if cell.side == "SHORT" {
        notional.0 * funding_rate_hourly
    } else {
        -notional.0 * funding_rate_hourly
    });
    round_dp(delta.0, 4)
}

/// Accrues 1H funding on an active Pond cell using mark-to-market notional (`NL-13`).
pub fn accrue_cell_hourly_funding(cell: &mut PondCell, mark_price: f64, funding_rate_hourly: f64) {
    let delta = compute_cell_hourly_funding_delta(cell, mark_price, funding_rate_hourly);
    cell.funding_accrued_usdc = round_dp(cell.funding_accrued_usdc + delta, 4);
}

/// Evaluates the 6-Tier Exit Priority Engine in strict order (Tiers 1..6).
pub fn evaluate_exit_priority(cell: &PondCell, input: &CellExitEvalInput) -> CellExitDecision {
    let is_long = cell.side == "LONG";
    let wick_buffer = input.wick_buffer_atr_mult * input.atr_14_1h.max(0.0);
    let init_stop = cell.initial_stop.unwrap_or(0.0);
    let chand_stop = cell.chandelier_stop.unwrap_or(0.0);

    // Tier 1: Catastrophic Wick Stop (15-second tick) — Active stop + 0.85 * ATR_14(1h) wick buffer
    let active_stop = if cell.state == "TRAILED" && chand_stop > 0.0 {
        if is_long {
            init_stop.max(chand_stop)
        } else if init_stop > 0.0 {
            init_stop.min(chand_stop)
        } else {
            chand_stop
        }
    } else {
        init_stop
    };
    if active_stop > 0.0 {
        let tier1_breach = if is_long {
            let trigger = active_stop - wick_buffer;
            input.intrabar_low.min(input.mark_price) <= trigger
        } else {
            let trigger = active_stop + wick_buffer;
            input.intrabar_high.max(input.mark_price) >= trigger
        };
        if tier1_breach {
            return CellExitDecision {
                should_exit: true,
                tier: Some(1),
                target_state: Some(PondCellState::Closed),
                exit_reason: Some("TIER1_CATASTROPHIC_WICK_STOP".to_string()),
                prefer_maker: false,
                cooldown_hours: input.cooldown_stop_hours,
            };
        }
    }

    // Tier 2: 4H Bar-Close Chandelier Trail — evaluated only on 4H bar closes against effective stop
    let tier2_stop = if chand_stop > 0.0 {
        if is_long {
            init_stop.max(chand_stop)
        } else if init_stop > 0.0 {
            init_stop.min(chand_stop)
        } else {
            chand_stop
        }
    } else {
        init_stop
    };
    if input.is_4h_bar_close && tier2_stop > 0.0 {
        let tier2_breach = if is_long {
            input.bar_4h_close <= tier2_stop
        } else {
            input.bar_4h_close >= tier2_stop
        };
        if tier2_breach {
            return CellExitDecision {
                should_exit: true,
                tier: Some(2),
                target_state: Some(PondCellState::Closed),
                exit_reason: Some("TIER2_4H_CHANDELIER_TRAIL".to_string()),
                prefer_maker: true,
                cooldown_hours: input.cooldown_stop_hours,
            };
        }
    }

    // Tier 3: Evaluated at 1H S-cycle funding boundary
    if input.is_1h_boundary {
        // Tier 3: Funding Normalisation / Mean-Reversion Harvest (|z_t| <= 0.5)
        if input.current_funding_z.abs() <= input.z_exit_threshold {
            return CellExitDecision {
                should_exit: true,
                tier: Some(3),
                target_state: Some(PondCellState::Closed),
                exit_reason: Some("TIER3_FUNDING_NORMALISATION".to_string()),
                prefer_maker: true,
                cooldown_hours: input.cooldown_normal_hours,
            };
        }
    }

    // Tier 4: Adverse Funding Flip — evaluated on 15s ticks and 1H boundaries
    let flipped = if is_long {
        // SHORT_PUT (LONG perp) collects negative funding; adverse flip is r > +min_funding_abs
        input.current_funding_hourly > input.min_funding_abs
    } else {
        // SHORT_CALL (SHORT perp) collects positive funding; adverse flip is r < -min_funding_abs
        input.current_funding_hourly < -input.min_funding_abs
    };
    if flipped {
        return CellExitDecision {
            should_exit: true,
            tier: Some(4),
            target_state: Some(PondCellState::Closed),
            exit_reason: Some("TIER4_ADVERSE_FUNDING_FLIP".to_string()),
            prefer_maker: false,
            cooldown_hours: input.cooldown_normal_hours,
        };
    }

    // Tier 5: Maximum Hold Duration Timeout (hold_hours >= 72h)
    if input.hold_hours >= input.max_hold_hours {
        return CellExitDecision {
            should_exit: true,
            tier: Some(5),
            target_state: Some(PondCellState::Closed),
            exit_reason: Some("TIER5_MAX_HOLD_TIMEOUT_72H".to_string()),
            prefer_maker: true,
            cooldown_hours: input.cooldown_normal_hours,
        };
    }

    // Tier 6: Exchange Delisting / Universe Retirement
    if input.universe_status.eq_ignore_ascii_case("DELISTING")
        || input.universe_status.eq_ignore_ascii_case("RETIRED")
    {
        return CellExitDecision {
            should_exit: true,
            tier: Some(6),
            target_state: Some(PondCellState::Closed),
            exit_reason: Some("TIER6_UNIVERSE_DELISTING".to_string()),
            prefer_maker: false,
            cooldown_hours: input.cooldown_normal_hours,
        };
    }

    CellExitDecision {
        should_exit: false,
        tier: None,
        target_state: None,
        exit_reason: None,
        prefer_maker: true,
        cooldown_hours: 0,
    }
}
