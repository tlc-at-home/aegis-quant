//! Institutional Risk & Position Sizing Engine (`aegis-risk`).
//!
//! 1:1 port of `engine/risk.py`:
//! - Conviction-scaled position sizing (`0.50%` to `1.50%` equity risk per trade)
//! - Two-Tier Chandelier Stop evaluation (Tier 1 Intrabar Wick with `0.85x` ATR buffer,
//!   Tier 2 Confirmed Bar-Close ratchet with `3.0x`/`3.8x` ATR and short profit-lock)
//! - Portfolio heat cap (`4%`), single-asset cap (`25%`), and gross leverage cap (`1.5x`)
//! - Lot-step and tick-size rounding delegated to `aegis-core`

use aegis_core::{
    compute_atr_from_bars, round_dp, round_step as core_round_step, ChandelierStopEvaluation,
    OhlcvBar, PositionSizeResult, Price, Quantity, TradeRecord, UniverseAsset, Usdc,
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

    fn load_config_logged(&self, _caller: &str) -> ConfigMap {
        self.db.load_config_logged("engine.risk")
    }

    /// Computes Average True Range (ATR) over `period` bars (`Q3`).
    pub fn compute_atr(&self, bars: &[OhlcvBar], period: usize) -> f64 {
        compute_atr_from_bars(bars, period)
    }

    /// Resolves the reference equity denominator for risk cap scaling (`NM-15`).
    /// Uses explicit `risk_caps_reference_scope` (`"BOOK"` vs `"PLATFORM"`) from `system_config`
    /// rather than a fragile `0.04` equality heuristic.
    pub fn resolve_book_reference_equity(cfg_ref: &ConfigMap) -> f64 {
        let initial_equity = cfg_ref.get_f64("initial_equity_usdc", 10000.0).max(50.0);
        let split_pct = cfg_ref.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
        let pit_book_eq = (initial_equity * split_pct).max(50.0);
        let scope = cfg_ref
            .get_str("risk_caps_reference_scope", "BOOK")
            .trim()
            .to_ascii_uppercase();
        if scope == "PLATFORM" {
            initial_equity
        } else {
            pit_book_eq
        }
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
                owned_cfg = self.load_config_logged("compute_conviction_risk_budget");
                &owned_cfg
            }
        };

        let ref_equity = Self::resolve_book_reference_equity(cfg_ref);
        let equity_scale = current_equity_usdc.max(50.0) / ref_equity;

        let baseline_risk = cfg_ref.get_f64(
            "baseline_risk_per_trade_usdc",
            cfg_ref.get_f64("max_risk_per_trade_usdc", 50.0),
        ) * equity_scale;
        let min_risk = cfg_ref.get_f64("min_risk_per_trade_usdc", 25.0) * equity_scale;
        let max_risk = cfg_ref.get_f64(
            "max_risk_per_trade_usdc",
            cfg_ref.get_f64("max_scaled_risk_per_trade_usdc", 75.0),
        ) * equity_scale;
        let scale_min = cfg_ref.get_f64(
            "conviction_scale_min",
            cfg_ref.get_f64(
                "risk_conviction_min_mult",
                cfg_ref.get_f64("conviction_risk_scale_min", 0.50),
            ),
        );
        let scale_max = cfg_ref.get_f64(
            "conviction_scale_max",
            cfg_ref.get_f64(
                "risk_conviction_max_mult",
                cfg_ref.get_f64("conviction_risk_scale_max", 1.50),
            ),
        );

        let mag = blended_conviction.abs().clamp(0.0, 1.0);
        let mult = scale_min + mag * (scale_max - scale_min);
        let risk_budget = Usdc((baseline_risk * mult).clamp(min_risk, max_risk));
        (round_dp(risk_budget.as_f64(), 2), round_dp(mult, 4))
    }

    /// Rounds `value` down (or nearest if `round_down == false`) to exchange lot or tick step size (`Q3`).
    pub fn round_step(value: f64, step: f64, round_down: bool) -> f64 {
        core_round_step(value, step, round_down)
    }

    /// Evaluates the Two-Tier Chandelier Stop (`v0.3.0-rc1` Calibration 2).
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
                owned_cfg = self.load_config_logged("evaluate_two_tier_chandelier_stop");
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

        let min_atr_pct = cfg_ref.get_f64(
            "chandelier_min_atr_pct",
            cfg_ref.get_f64("risk_min_stop_pct_floor", 0.022),
        );
        let eff_atr = atr_value.max(bar_close * min_atr_pct).max(1e-8);

        let wick_buffer_atr = cfg_ref.get_f64(
            "chandelier_wick_buffer_atr",
            cfg_ref.get_f64("chandelier_wick_overshoot_atr", 0.85),
        );
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
                let exit_slippage_usdc =
                    round_dp((wick_trigger_price - fill_price).max(0.0) * qty, 4);
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
                let exit_slippage_usdc =
                    round_dp((fill_price - wick_trigger_price).max(0.0) * qty, 4);
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

    /// Evaluates whether a proposed order satisfies all institutional risk guardrails.
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
        let cfg = self.load_config_logged("evaluate_order_risk");
        let ref_equity = Self::resolve_book_reference_equity(&cfg);
        let equity_usdc = Usdc(current_equity_usdc.max(50.0));
        let equity_scale = equity_usdc.as_f64() / ref_equity;
        let entry_px = Price(entry_price);

        let (max_risk_per_trade_raw, conviction_mult) = self.compute_conviction_risk_budget(
            blended_conviction,
            equity_usdc.as_f64(),
            Some(&cfg),
        );
        let max_risk_per_trade = Usdc(max_risk_per_trade_raw);

        let configured_heat_usdc = cfg.get_f64("max_portfolio_heat_usdc", ref_equity * 0.04);
        let configured_heat_pct = cfg.get_f64("max_portfolio_heat_pct", 0.04).clamp(0.005, 0.50);
        let base_heat_usdc = if (configured_heat_pct - 0.04).abs() > 1e-6
            && (configured_heat_usdc - ref_equity * 0.04).abs() <= 1e-3
        {
            ref_equity * configured_heat_pct
        } else {
            configured_heat_usdc
        };
        let max_heat = Usdc(base_heat_usdc * equity_scale);
        let max_single_notional = Usdc(
            cfg.get_f64("max_single_asset_notional_usdc", ref_equity * 0.25) * equity_scale,
        );
        let max_gross_notional = Usdc(
            cfg.get_f64("max_gross_leverage_notional_usdc", ref_equity * 1.50) * equity_scale,
        );

        let mult_long = cfg.get_f64(
            "chandelier_atr_multiplier_long",
            cfg.get_f64("chandelier_atr_multiplier", 3.0),
        );
        let mult_short = cfg.get_f64(
            "chandelier_atr_multiplier_short",
            (mult_long * 1.25).max(3.8),
        );
        let min_atr_pct = cfg.get_f64(
            "chandelier_min_atr_pct",
            cfg.get_f64("risk_min_stop_pct_floor", 0.022),
        );
        let chandelier_mult = if side == "LONG" { mult_long } else { mult_short };

        let owned_trades;
        let active_trades: &[TradeRecord] = match open_trades {
            Some(t) => t,
            None => {
                owned_trades = match self.db.get_open_trades() {
                    Ok(t) => t,
                    Err(e) => {
                        tracing::error!(component = "engine.risk", error = %e, "Failed to load open trades in evaluate_order_risk");
                        Vec::new()
                    }
                };
                &owned_trades
            }
        };

        let mut current_heat_usdc = Usdc(0.0);
        let mut current_gross_notional_usdc = Usdc(basis_harvest_notional_usdc.max(0.0));
        let mut existing_asset_notional_usdc = Usdc(0.0);

        for t in active_trades {
            // NH-4: Unfilled resting limit orders (`OPEN_ON_BOOK` with zero filled units or `PENDING`)
            // are not yet open positions and must not consume filled portfolio heat or leverage.
            if t.status == "PENDING" || (t.fsm_state == "OPEN_ON_BOOK" && t.filled_units <= 0.0) {
                continue;
            }
            let t_size = Quantity(t.quantity.abs());
            let t_entry = Price(t.entry_price);
            let t_stop = Price(if t.chandelier_stop_price > 0.0 {
                t.chandelier_stop_price
            } else {
                t.initial_stop_price
            });
            let t_mark = Price(t.current_price.max(t_entry.as_f64()));
            let t_notional = t_size.notional_at(t_mark);
            let t_risk = if t.side == "LONG" {
                Usdc(((t_entry.as_f64() - t_stop.as_f64()).max(0.0)) * t_size.as_f64())
            } else {
                Usdc(((t_stop.as_f64() - t_entry.as_f64()).max(0.0)) * t_size.as_f64())
            };

            current_gross_notional_usdc =
                Usdc(current_gross_notional_usdc.as_f64() + t_notional.as_f64());
            current_heat_usdc = Usdc(current_heat_usdc.as_f64() + t_risk.as_f64());
            if t.symbol == asset.symbol || t.base_asset == asset.base_asset {
                existing_asset_notional_usdc =
                    Usdc(existing_asset_notional_usdc.as_f64() + t_notional.as_f64());
            }
        }

        let remaining_heat = max_heat.as_f64() - current_heat_usdc.as_f64();
        if remaining_heat <= 1.0 {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "PORTFOLIO_HEAT_EXCEEDED: current_heat={:.2} >= max_heat={:.2}",
                    current_heat_usdc.as_f64(),
                    max_heat.as_f64()
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price: entry_px.as_f64(),
                quantity: 0.0,
                notional_usdc: 0.0,
                risk_budget_usdc: 0.0,
                conviction_multiplier: conviction_mult,
                initial_stop_price: entry_px.as_f64(),
                atr_value,
            };
        }

        let remaining_gross = max_gross_notional.as_f64() - current_gross_notional_usdc.as_f64();
        if remaining_gross < asset.min_notional_usdc {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "GROSS_LEVERAGE_EXCEEDED: current_gross={:.2} >= max_gross={:.2}",
                    current_gross_notional_usdc.as_f64(),
                    max_gross_notional.as_f64()
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price: entry_px.as_f64(),
                quantity: 0.0,
                notional_usdc: 0.0,
                risk_budget_usdc: 0.0,
                conviction_multiplier: conviction_mult,
                initial_stop_price: entry_px.as_f64(),
                atr_value,
            };
        }

        let remaining_asset_notional =
            max_single_notional.as_f64() - existing_asset_notional_usdc.as_f64();
        if remaining_asset_notional < asset.min_notional_usdc {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "ASSET_CAP_EXCEEDED: existing_notional={:.2} >= max_single={:.2}",
                    existing_asset_notional_usdc.as_f64(),
                    max_single_notional.as_f64()
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price: entry_px.as_f64(),
                quantity: 0.0,
                notional_usdc: 0.0,
                risk_budget_usdc: 0.0,
                conviction_multiplier: conviction_mult,
                initial_stop_price: entry_px.as_f64(),
                atr_value,
            };
        }

        let effective_atr = atr_value.max(entry_px.as_f64() * min_atr_pct).max(1e-8);
        let raw_stop_dist = chandelier_mult * effective_atr;
        let stop_price = Price(if side == "LONG" {
            Self::round_step(
                (entry_px.as_f64() - raw_stop_dist).max(asset.tick_size),
                asset.tick_size,
                true,
            )
        } else {
            Self::round_step(entry_px.as_f64() + raw_stop_dist, asset.tick_size, false)
        });

        let stop_distance = entry_px.abs_diff(stop_price).max(asset.tick_size);

        let allowed_risk_usdc = max_risk_per_trade.as_f64().min(remaining_heat);
        let size_by_risk = allowed_risk_usdc / stop_distance;

        let allowed_notional_usdc = remaining_asset_notional.min(remaining_gross);
        let size_by_notional = allowed_notional_usdc / entry_px.as_f64().max(1e-8);

        let raw_size = size_by_risk.min(size_by_notional);
        let rounded_qty = Quantity(Self::round_step(raw_size, asset.lot_step_size, true));
        let final_notional = round_dp(rounded_qty.notional_at(entry_px).as_f64(), 2);
        let final_risk = round_dp(rounded_qty.as_f64() * stop_distance, 2);

        if rounded_qty.as_f64() <= 0.0 || final_notional < asset.min_notional_usdc {
            return PositionSizeResult {
                approved: false,
                reason: format!(
                    "BELOW_MIN_NOTIONAL: notional={final_notional:.2} < min_notional_usdc={:.2}",
                    asset.min_notional_usdc
                ),
                symbol: asset.symbol.clone(),
                side: side.to_string(),
                entry_price: entry_px.as_f64(),
                quantity: rounded_qty.as_f64(),
                notional_usdc: final_notional,
                risk_budget_usdc: final_risk,
                conviction_multiplier: conviction_mult,
                initial_stop_price: stop_price.as_f64(),
                atr_value: effective_atr,
            };
        }

        PositionSizeResult {
            approved: true,
            reason: "APPROVED".to_string(),
            symbol: asset.symbol.clone(),
            side: side.to_string(),
            entry_price: entry_px.as_f64(),
            quantity: rounded_qty.as_f64(),
            notional_usdc: final_notional,
            risk_budget_usdc: final_risk,
            conviction_multiplier: conviction_mult,
            initial_stop_price: stop_price.as_f64(),
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

    #[test]
    fn test_book_reference_equity_scope_and_custom_heat_pct_nm15_nh4() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let risk = RiskEngine::new(db.clone());
        let assets = db.get_usdc_perp_universe().unwrap();
        let sol = assets.iter().find(|a| a.symbol == "SOL/USDC:USDC").unwrap();

        // 1. Default BOOK scope ($5,000 PIT reference equity) even when max_portfolio_heat_pct is customized to 6%
        db.set_config_value(
            "max_portfolio_heat_pct",
            &aegis_db::cast_config_value("0.06", "float"),
        )
        .unwrap();
        let cfg = db.load_config().unwrap();
        assert_eq!(RiskEngine::resolve_book_reference_equity(&cfg), 5000.0);

        // At $5,000 PIT equity, 1.0x conviction budget must be $50.0 (1.0% of $5,000), not halved to $25.0!
        let (budget_5k, _) = risk.compute_conviction_risk_budget(0.5, 5000.0, Some(&cfg));
        assert!((budget_5k - 50.0).abs() < 1e-6);

        // 2. NH-4: Unfilled OPEN_ON_BOOK resting order must not block new order risk evaluation
        let resting_trade = TradeRecord {
            trade_id: 1,
            client_order_id: "resting-1".to_string(),
            symbol: "SOL/USDC:USDC".to_string(),
            base_asset: "SOL".to_string(),
            side: "LONG".to_string(),
            status: "PENDING".to_string(),
            fsm_state: "OPEN_ON_BOOK".to_string(),
            entry_price: 185.0,
            current_price: 185.0,
            exit_price: None,
            quantity: 100.0,
            filled_units: 0.0,
            avg_fill_price: None,
            notional_usdc: 18500.0,
            risk_budget_usdc: 500.0,
            conviction_multiplier: 1.0,
            initial_stop_price: 170.0,
            chandelier_stop_price: 170.0,
            current_stop_price: 170.0,
            highest_price_seen: 185.0,
            lowest_price_seen: 185.0,
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc: 0.0,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some("oid-1".to_string()),
            opened_at: String::new(),
            closed_at: None,
            exit_reason: None,
        };
        let sized = risk.evaluate_order_risk(
            sol,
            "LONG",
            185.0,
            4.5,
            5_000.0,
            Some(&[resting_trade]),
            0.75,
            0.0,
        );
        assert!(sized.approved, "Unfilled resting order should not consume filled heat/notional");
    }
}
