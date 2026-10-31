//! Institutional Risk & Position Sizing Engine (`aegis-risk`).
//!
//! 1:1 port of `engine/risk.py`:
//! - Conviction-scaled position sizing (`0.50%` to `1.50%` equity risk per trade)
//! - Two-Tier Chandelier Stop evaluation (Tier 1 Intrabar Wick with `0.85x` ATR buffer,
//!   Tier 2 Confirmed Bar-Close ratchet with `3.0x`/`3.8x` ATR and short profit-lock)
//! - Portfolio heat cap (`4%`), single-asset cap (`25%`), and gross leverage cap (`1.5x`)
//! - Lot-step and tick-size rounding

use aegis_core::{
    round_dp, ChandelierStopEvaluation, OhlcvBar, PositionSizeResult, TradeRecord, UniverseAsset,
};
use aegis_db::{ConfigMap, DatabaseManager};

#[derive(Clone)]
pub struct RiskEngine {
    pub db: DatabaseManager,
}

impl RiskEngine {
    pub fn new(db: DatabaseManager) -> Self {
        Self { db }
    }

    /// Computes Average True Range (ATR) over `period` bars.
    /// Includes a defensive floor (`close * 0.005`) to prevent division-by-zero on low-volatility bars.
    pub fn compute_atr(&self, bars: &[OhlcvBar], period: usize) -> f64 {
        if bars.len() < 2 {
            if let Some(last) = bars.last() {
                let close = last.close;
                let high = last.high;
                let low = last.low;
                return (high - low).max(close * 0.015).max(1e-8);
            }
            return 1.0;
        }

        let mut tr_list = Vec::with_capacity(bars.len() - 1);
        for i in 1..bars.len() {
            let high = bars[i].high;
            let low = bars[i].low;
            let prev_close = bars[i - 1].close;
            let tr = (high - low)
                .max((high - prev_close).abs())
                .max((low - prev_close).abs());
            tr_list.push(tr);
        }

        let window = if tr_list.len() >= period {
            &tr_list[tr_list.len() - period..]
        } else {
            &tr_list[..]
        };

        let atr = if window.is_empty() {
            0.0
        } else {
            window.iter().sum::<f64>() / (window.len() as f64)
        };
        let current_close = bars.last().map(|b| b.close).unwrap_or(1.0);
        atr.max(current_close * 0.005).max(1e-8)
    }

    /// Computes dynamic per-trade risk budget (`0.50%` to `1.50%` of equity)
    /// scaled linearly by `|blended_conviction|` in `[0.0, 1.0]`.
    pub fn compute_conviction_risk_budget(
        &self,
        blended_conviction: f64,
        current_equity_usdc: f64,
        cfg: Option<&ConfigMap>,
    ) -> (f64, f64) {
        let owned_cfg;
        let cfg_ref = match cfg {
            Some(c) => c,
            None => {
                owned_cfg = self.db.load_config().unwrap_or_default();
                &owned_cfg
            }
        };

        let initial_equity = cfg_ref.get_f64("initial_equity_usdc", 10000.0);
        let equity_scale = current_equity_usdc.max(100.0) / initial_equity.max(100.0);

        let baseline_risk = cfg_ref.get_f64(
            "baseline_risk_per_trade_usdc",
            cfg_ref.get_f64("max_risk_per_trade_usdc", 100.0),
        ) * equity_scale;
        let min_risk = cfg_ref.get_f64("min_risk_per_trade_usdc", 50.0) * equity_scale;
        let max_risk = cfg_ref.get_f64(
            "max_risk_per_trade_usdc",
            cfg_ref.get_f64("max_scaled_risk_per_trade_usdc", 150.0),
        ) * equity_scale;
        let scale_min = cfg_ref.get_f64(
            "conviction_scale_min",
            cfg_ref.get_f64("conviction_risk_scale_min", 0.50),
        );
        let scale_max = cfg_ref.get_f64(
            "conviction_scale_max",
            cfg_ref.get_f64("conviction_risk_scale_max", 1.50),
        );

        let mag = blended_conviction.abs().clamp(0.0, 1.0);
        let mult = scale_min + mag * (scale_max - scale_min);
        let risk_budget = (baseline_risk * mult).clamp(min_risk, max_risk);
        (round_dp(risk_budget, 2), round_dp(mult, 4))
    }

    /// Rounds `value` down (or nearest if `round_down == false`) to exchange lot or tick step size.
    pub fn round_step(value: f64, step: f64, round_down: bool) -> f64 {
        if step <= 0.0 {
            return value;
        }
        let ratio = value / step;
        let stepped = if round_down {
            (ratio + 1e-12).floor() * step
        } else {
            ratio.round() * step
        };
        round_dp(stepped, 10)
    }

    /// Evaluates the Two-Tier Chandelier Stop (`v0.2.0-rc2` Calibration 2):
    ///
    /// - **Tier 1 (Intrabar Wick Emergency Stop):**
    ///   - `LONG`: triggers only if `bar_low <= active_stop - 0.85 * eff_atr`
    ///   - `SHORT`: triggers only if `bar_high >= active_stop + 0.85 * eff_atr`
    ///   - Models realistic adverse fill price with overshoot fraction + stop slippage bps.
    /// - **Tier 2 (Confirmed Bar-Close Chandelier Stop & Ratchet):**
    ///   - Updates `highest_price_seen` / `lowest_price_seen` using confirmed `bar_close`
    ///   - Ratchets `chandelier_stop_price` monotonically (`3.0x` ATR for LONG, `3.8x` ATR for SHORT + profit lock)
    ///   - Triggers if `bar_close <= new_stop` (LONG) or `bar_close >= new_stop` (SHORT).
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_two_tier_chandelier_stop(
        &self,
        side: &str,
        entry_price: f64,
        quantity: f64,
        current_stop: f64,
        highest_price: f64,
        lowest_price: f64,
        bar_high: f64,
        bar_low: f64,
        bar_close: f64,
        atr_value: f64,
        tick_size: f64,
        cfg: Option<&ConfigMap>,
    ) -> ChandelierStopEvaluation {
        let owned_cfg;
        let cfg_ref = match cfg {
            Some(c) => c,
            None => {
                owned_cfg = self.db.load_config().unwrap_or_default();
                &owned_cfg
            }
        };

        let mult_long = cfg_ref
            .get_f64(
                "chandelier_atr_multiplier_long",
                cfg_ref.get_f64("chandelier_atr_multiplier", 3.0),
            )
            .max(0.5);
        let mult_short = cfg_ref
            .get_f64(
                "chandelier_atr_multiplier_short",
                (mult_long * 1.25).max(3.8),
            )
            .max(0.5);

        let min_atr_pct = cfg_ref.get_f64("chandelier_min_atr_pct", 0.022);
        let eff_atr = atr_value.max(bar_close * min_atr_pct).max(1e-8);

        let wick_buffer_atr = cfg_ref.get_f64("chandelier_wick_buffer_atr", 0.85);
        let wick_overshoot_frac = cfg_ref.get_f64("wick_overshoot_slippage_fraction", 0.15);
        let stop_slippage_bps = cfg_ref.get_f64("stop_slippage_bps", 8.0);
        let short_break_even_trigger_atr =
            cfg_ref.get_f64("short_break_even_trigger_atr", 1.6);
        let short_break_even_lock_atr = cfg_ref.get_f64("short_break_even_lock_atr", 0.45);
        let qty = quantity.abs();

        if side == "LONG" {
            let init_stop = entry_price - mult_long * eff_atr;
            let active_stop = if current_stop > 0.0 {
                current_stop
            } else {
                init_stop
            };

            // Tier 1: Intrabar Low-Wick Emergency Stop
            let wick_trigger_price = active_stop - wick_buffer_atr * eff_atr;
            if bar_low <= wick_trigger_price {
                let overshoot = (wick_trigger_price - bar_low).max(0.0);
                let raw_fill = (wick_trigger_price - wick_overshoot_frac * overshoot)
                    * (1.0 - stop_slippage_bps / 10000.0);
                let fill_price =
                    Self::round_step(raw_fill.max(bar_low * 0.999), tick_size, true);
                let exit_slippage_usdc = round_dp((wick_trigger_price - fill_price).max(0.0) * qty, 4);
                return ChandelierStopEvaluation {
                    triggered: true,
                    is_wick_stop: true,
                    exit_reason: Some("INTRABAR_LOW_WICK_CHANDELIER_STOP_LONG".to_string()),
                    exit_fill_price: fill_price,
                    exit_slippage_usdc,
                    chandelier_stop_price: Self::round_step(active_stop, tick_size, true),
                    highest_price_seen: highest_price.max(entry_price),
                    lowest_price_seen: if lowest_price > 0.0 {
                        lowest_price.min(bar_low)
                    } else {
                        bar_low
                    },
                    effective_atr: eff_atr,
                };
            }

            // Tier 2: Confirmed Bar-Close Chandelier Ratchet
            let new_highest = highest_price.max(entry_price).max(bar_close);
            let new_lowest = if lowest_price > 0.0 {
                lowest_price.min(bar_low)
            } else {
                bar_low
            };
            let cand_stop = new_highest - mult_long * eff_atr;
            let new_stop = Self::round_step(active_stop.max(cand_stop), tick_size, true);

            if bar_close <= new_stop {
                let raw_fill = bar_close * (1.0 - stop_slippage_bps / 10000.0);
                let fill_price =
                    Self::round_step(raw_fill.max(bar_low * 0.999), tick_size, true);
                let exit_slippage_usdc = round_dp((bar_close - fill_price).max(0.0) * qty, 4);
                return ChandelierStopEvaluation {
                    triggered: true,
                    is_wick_stop: false,
                    exit_reason: Some("CHANDELIER_STOP_LONG".to_string()),
                    exit_fill_price: fill_price,
                    exit_slippage_usdc,
                    chandelier_stop_price: new_stop,
                    highest_price_seen: new_highest,
                    lowest_price_seen: new_lowest,
                    effective_atr: eff_atr,
                };
            }

            ChandelierStopEvaluation {
                triggered: false,
                is_wick_stop: false,
                exit_reason: None,
                exit_fill_price: 0.0,
                exit_slippage_usdc: 0.0,
                chandelier_stop_price: new_stop,
                highest_price_seen: new_highest,
                lowest_price_seen: new_lowest,
                effective_atr: eff_atr,
            }
        } else {
            // SHORT
            let init_stop = entry_price + mult_short * eff_atr;
            let active_stop = if current_stop > 0.0 {
                current_stop
            } else {
                init_stop
            };

            // Tier 1: Intrabar High-Wick Emergency Stop
            let wick_trigger_price = active_stop + wick_buffer_atr * eff_atr;
            if bar_high >= wick_trigger_price {
                let overshoot = (bar_high - wick_trigger_price).max(0.0);
                let raw_fill = (wick_trigger_price + wick_overshoot_frac * overshoot)
                    * (1.0 + stop_slippage_bps / 10000.0);
                let fill_price =
                    Self::round_step(raw_fill.min(bar_high * 1.001), tick_size, false);
                let exit_slippage_usdc = round_dp((fill_price - wick_trigger_price).max(0.0) * qty, 4);
                return ChandelierStopEvaluation {
                    triggered: true,
                    is_wick_stop: true,
                    exit_reason: Some("INTRABAR_HIGH_WICK_CHANDELIER_STOP_SHORT".to_string()),
                    exit_fill_price: fill_price,
                    exit_slippage_usdc,
                    chandelier_stop_price: Self::round_step(active_stop, tick_size, false),
                    highest_price_seen: highest_price.max(bar_high),
                    lowest_price_seen: if lowest_price > 0.0 {
                        lowest_price.min(entry_price)
                    } else {
                        entry_price
                    },
                    effective_atr: eff_atr,
                };
            }

            // Tier 2: Confirmed Bar-Close Chandelier Ratchet + Profit Lock
            let new_lowest = if lowest_price > 0.0 {
                lowest_price.min(entry_price).min(bar_close)
            } else {
                entry_price.min(bar_close)
            };
            let new_highest = highest_price.max(bar_high);
            let mut cand_stop = new_lowest + mult_short * eff_atr;
            if (entry_price - new_lowest) >= short_break_even_trigger_atr * eff_atr {
                cand_stop = cand_stop.min(entry_price - short_break_even_lock_atr * eff_atr);
            }
            let new_stop = Self::round_step(active_stop.min(cand_stop), tick_size, false);

            if bar_close >= new_stop {
                let raw_fill = bar_close * (1.0 + stop_slippage_bps / 10000.0);
                let fill_price =
                    Self::round_step(raw_fill.min(bar_high * 1.001), tick_size, false);
                let exit_slippage_usdc = round_dp((fill_price - bar_close).max(0.0) * qty, 4);
                return ChandelierStopEvaluation {
                    triggered: true,
                    is_wick_stop: false,
                    exit_reason: Some("CHANDELIER_STOP_SHORT".to_string()),
                    exit_fill_price: fill_price,
                    exit_slippage_usdc,
                    chandelier_stop_price: new_stop,
                    highest_price_seen: new_highest,
                    lowest_price_seen: new_lowest,
                    effective_atr: eff_atr,
                };
            }

            ChandelierStopEvaluation {
                triggered: false,
                is_wick_stop: false,
                exit_reason: None,
                exit_fill_price: 0.0,
                exit_slippage_usdc: 0.0,
                chandelier_stop_price: new_stop,
                highest_price_seen: new_highest,
                lowest_price_seen: new_lowest,
                effective_atr: eff_atr,
            }
        }
    }

    /// Evaluates whether a proposed order satisfies all institutional risk guardrails:
    /// 1. Portfolio Heat Cap (`max_portfolio_heat_usdc` = 4% of equity)
    /// 2. Single-Asset Concentration Cap (`max_single_asset_notional_usdc` = 25% of equity)
    /// 3. Max Gross Leverage Cap (`max_gross_leverage_notional_usdc` = 1.5x of equity)
    /// 4. Exchange Minimum Order Notional & Lot Step Rounding
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_order_risk(
        &self,
        asset: &UniverseAsset,
        side: &str,
        entry_price: f64,
        atr_value: f64,
        current_equity_usdc: f64,
        open_trades: Option<&[TradeRecord]>,
        blended_conviction: f64,
        basis_harvest_notional_usdc: f64,
    ) -> PositionSizeResult {
        let cfg = self.db.load_config().unwrap_or_default();
        let initial_equity = cfg.get_f64("initial_equity_usdc", 10000.0);
        let equity_scale = current_equity_usdc.max(100.0) / initial_equity.max(100.0);

        let (max_risk_per_trade, conviction_mult) =
            self.compute_conviction_risk_budget(blended_conviction, current_equity_usdc, Some(&cfg));

        let max_heat = cfg.get_f64("max_portfolio_heat_usdc", 400.0) * equity_scale;
        let max_single_notional =
            cfg.get_f64("max_single_asset_notional_usdc", 2500.0) * equity_scale;
        let max_gross_notional =
            cfg.get_f64("max_gross_leverage_notional_usdc", 15000.0) * equity_scale;

        let mult_long = cfg.get_f64(
            "chandelier_atr_multiplier_long",
            cfg.get_f64("chandelier_atr_multiplier", 3.0),
        );
        let mult_short = cfg.get_f64(
            "chandelier_atr_multiplier_short",
            (mult_long * 1.25).max(3.8),
        );
        let min_atr_pct = cfg.get_f64("chandelier_min_atr_pct", 0.022);
        let chandelier_mult = if side == "LONG" { mult_long } else { mult_short };

        let owned_trades;
        let active_trades: &[TradeRecord] = match open_trades {
            Some(t) => t,
            None => {
                owned_trades = self.db.get_open_trades().unwrap_or_default();
                &owned_trades
            }
        };

        let mut current_heat_usdc = 0.0;
        let mut current_gross_notional_usdc = basis_harvest_notional_usdc.max(0.0);
        let mut existing_asset_notional_usdc = 0.0;

        for t in active_trades {
            let t_size = t.quantity.abs();
            let t_entry = t.entry_price;
            let t_stop = if t.chandelier_stop_price > 0.0 {
                t.chandelier_stop_price
            } else {
                t.initial_stop_price
            };
            let t_notional = t_size * t.current_price.max(t_entry);
            let t_risk = if t.side == "LONG" {
                ((t_entry - t_stop).max(0.0)) * t_size
            } else {
                ((t_stop - t_entry).max(0.0)) * t_size
            };

            current_gross_notional_usdc += t_notional;
            current_heat_usdc += t_risk;
            if t.symbol == asset.symbol || t.base_asset == asset.base_asset {
                existing_asset_notional_usdc += t_notional;
            }
        }

        let remaining_heat = max_heat - current_heat_usdc;
        if remaining_heat <= 1.0 {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "PORTFOLIO_HEAT_EXCEEDED: current_heat={current_heat_usdc:.2} >= max_heat={max_heat:.2}"
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price,
                quantity: 0.0,
                notional_usdc: 0.0,
                risk_budget_usdc: 0.0,
                conviction_multiplier: conviction_mult,
                initial_stop_price: entry_price,
                atr_value,
            };
        }

        let remaining_gross = max_gross_notional - current_gross_notional_usdc;
        if remaining_gross < asset.min_notional_usdc {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "GROSS_LEVERAGE_EXCEEDED: current_gross={current_gross_notional_usdc:.2} >= max_gross={max_gross_notional:.2}"
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price,
                quantity: 0.0,
                notional_usdc: 0.0,
                risk_budget_usdc: 0.0,
                conviction_multiplier: conviction_mult,
                initial_stop_price: entry_price,
                atr_value,
            };
        }

        let remaining_asset_notional = max_single_notional - existing_asset_notional_usdc;
        if remaining_asset_notional < asset.min_notional_usdc {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "ASSET_CAP_EXCEEDED: existing_notional={existing_asset_notional_usdc:.2} >= max_single={max_single_notional:.2}"
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price,
                quantity: 0.0,
                notional_usdc: 0.0,
                risk_budget_usdc: 0.0,
                conviction_multiplier: conviction_mult,
                initial_stop_price: entry_price,
                atr_value,
            };
        }

        let effective_atr = atr_value.max(entry_price * min_atr_pct).max(1e-8);
        let raw_stop_dist = chandelier_mult * effective_atr;
        let stop_price = if side == "LONG" {
            Self::round_step(
                (entry_price - raw_stop_dist).max(asset.tick_size),
                asset.tick_size,
                true,
            )
        } else {
            Self::round_step(entry_price + raw_stop_dist, asset.tick_size, false)
        };

        let stop_distance = (entry_price - stop_price).abs().max(asset.tick_size);

        let allowed_risk_usdc = max_risk_per_trade.min(remaining_heat);
        let size_by_risk = allowed_risk_usdc / stop_distance;

        let allowed_notional_usdc = remaining_asset_notional.min(remaining_gross);
        let size_by_notional = allowed_notional_usdc / entry_price.max(1e-8);

        let raw_size = size_by_risk.min(size_by_notional);
        let rounded_size = Self::round_step(raw_size, asset.lot_step_size, true);
        let final_notional = round_dp(rounded_size * entry_price, 2);
        let final_risk = round_dp(rounded_size * stop_distance, 2);

        if rounded_size <= 0.0 || final_notional < asset.min_notional_usdc {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "BELOW_MIN_NOTIONAL: notional={final_notional:.2} < min_notional_usdc={:.2}",
                    asset.min_notional_usdc
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price,
                quantity: rounded_size,
                notional_usdc: final_notional,
                risk_budget_usdc: final_risk,
                conviction_multiplier: conviction_mult,
                initial_stop_price: stop_price,
                atr_value: effective_atr,
            };
        }

        PositionSizeResult {
            approved: true,
            reason: "APPROVED".to_string(),
            symbol: asset.symbol.clone(),
            side: side.to_string(),
            entry_price,
            quantity: rounded_size,
            notional_usdc: final_notional,
            risk_budget_usdc: final_risk,
            conviction_multiplier: conviction_mult,
            initial_stop_price: stop_price,
            atr_value: effective_atr,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_boundaries_and_chandelier() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let risk = RiskEngine::new(db.clone());
        let assets = db.get_usdc_perp_universe().unwrap();
        let sol = assets.iter().find(|a| a.symbol == "SOL/USDC:USDC").unwrap();

        let sized = risk.evaluate_order_risk(
            sol,
            "LONG",
            185.0,
            4.5,
            10_000.0,
            Some(&[]),
            0.75,
            0.0,
        );
        assert!(sized.approved);
        assert!(sized.notional_usdc <= 2500.0);
        assert!(sized.risk_budget_usdc <= 150.0);

        let stop_eval = risk.evaluate_two_tier_chandelier_stop(
            "LONG",
            185.0,
            sized.quantity,
            sized.initial_stop_price,
            190.0,
            184.0,
            192.0,
            186.0,
            191.0,
            4.5,
            sol.tick_size,
            None,
        );
        assert!(!stop_eval.triggered);
        assert!(stop_eval.chandelier_stop_price >= sized.initial_stop_price);
    }
}
