"""Conviction-scaled position sizing ($50-$150), 4% portfolio heat cap, and 1.5x gross leverage engine."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from engine.database import DatabaseManager


@dataclass
class PositionSizeResult:
    """Validated position sizing and initial Chandelier stop allocation."""

    approved: bool
    reason: str
    symbol: str
    side: str
    entry_price: float
    quantity: float
    notional_usdc: float
    risk_budget_usdc: float
    conviction_multiplier: float
    initial_stop_price: float
    atr_value: float


@dataclass
class ChandelierStopEvaluation:
    """Result of Two-Tier Chandelier Stop evaluation (catastrophic wick vs bar_close ratchet)."""

    triggered: bool
    is_wick_stop: bool
    exit_reason: str | None
    exit_fill_price: float
    exit_slippage_usdc: float
    chandelier_stop_price: float
    highest_price_seen: float
    lowest_price_seen: float
    effective_atr: float


class RiskEngine:
    """
    Enforces conviction-scaled risk sizing ($50 to $150 per trade, 0.5x to 1.5x),
    $400 (4.0%) total portfolio heat ceiling, $2,500 (25%) single-asset notional cap,
    and $15,000 (1.5x) gross leverage ceiling on a $10,000 USDC baseline.
    """

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    @staticmethod
    def compute_atr(df: pd.DataFrame, period: int = 14) -> float:
        """Compute Average True Range (ATR) over `period` bars."""
        if df.empty:
            return 0.0
        highs = df["high"].to_numpy(dtype=np.float64)
        lows = df["low"].to_numpy(dtype=np.float64)
        closes = df["close"].to_numpy(dtype=np.float64)

        if len(closes) < 2:
            return max(float(highs[-1] - lows[-1]), float(closes[-1]) * 0.015)

        prev_closes = np.roll(closes, 1)
        prev_closes[0] = closes[0]
        tr = np.maximum.reduce(
            [
                highs - lows,
                np.abs(highs - prev_closes),
                np.abs(lows - prev_closes),
            ]
        )
        window = tr[-period:] if len(tr) >= period else tr
        atr = float(np.mean(window))
        return max(atr, float(closes[-1]) * 0.005)

    @staticmethod
    def compute_conviction_risk_budget(
        blended_conviction: float,
        current_equity_usdc: float,
        config: dict[str, Any],
    ) -> tuple[float, float]:
        """
        Map |blended_conviction| in [0, 1] to a conviction multiplier in [0.5x, 1.5x]
        and a dollar risk budget in [$50 (0.5%), $150 (1.5%)] scaled to equity.
        """
        base_equity = float(config.get("initial_equity_usdc", 10000.0))
        equity_scale = max(current_equity_usdc, 100.0) / max(base_equity, 100.0)

        baseline_risk = float(config.get("baseline_risk_per_trade_usdc", 100.0)) * equity_scale
        min_risk = float(config.get("min_risk_per_trade_usdc", 50.0)) * equity_scale
        max_risk = float(config.get("max_risk_per_trade_usdc", 150.0)) * equity_scale
        scale_min = float(config.get("conviction_scale_min", 0.5))
        scale_max = float(config.get("conviction_scale_max", 1.5))

        mag = float(np.clip(abs(blended_conviction), 0.0, 1.0))
        multiplier = scale_min + mag * (scale_max - scale_min)
        risk_budget = float(np.clip(baseline_risk * multiplier, min_risk, max_risk))
        return round(multiplier, 4), round(risk_budget, 2)

    @staticmethod
    def _round_step(value: float, step: float) -> float:
        if step <= 0:
            return value
        decimals = max(0, int(round(-math.log10(step)))) if step < 1 else 0
        return round(math.floor(value / step) * step, decimals)

    def evaluate_two_tier_chandelier_stop(
        self,
        *,
        side: str,
        entry_price: float,
        active_stop: float,
        highest_price_seen: float,
        lowest_price_seen: float,
        bar_open: float,
        bar_high: float,
        bar_low: float,
        bar_close: float,
        atr_value: float,
        quantity: float = 1.0,
        config: dict[str, Any] | None = None,
    ) -> ChandelierStopEvaluation:
        """
        Evaluate Two-Tier Chandelier Trailing Stop (Calibration 1):
        - Tier 1 (Catastrophic Wick Liquidation Stop): triggers only if intra-bar low/high
          overshoots `active_stop` by `chandelier_wick_overshoot_atr` (0.85x ATR), applying
          base stop slippage (`stop_slippage_bps`, 8 bps) + `wick_overshoot_slippage_fraction` (15%)
          of the wick overshoot distance.
        - Tier 2 (Confirmed Bar-Close Ratchet & Trailing Stop): ratchets `highest_price_seen` /
          `lowest_price_seen` strictly on confirmed `bar_close` prices (immune to transient intra-bar
          spikes), uses 3.0x ATR for LONG and 3.8x ATR for SHORT, and evaluates standard Chandelier
          trailing stops on `bar_close` (8 bps slippage).
        """
        cfg = config if config is not None else self.db.load_config()
        long_mult = float(cfg.get("chandelier_atr_multiplier_long", 3.0))
        short_mult = float(cfg.get("chandelier_atr_multiplier_short", 3.8))
        wick_overshoot_atr = float(cfg.get("chandelier_wick_overshoot_atr", 0.85))
        stop_slippage_rate = float(cfg.get("stop_slippage_bps", 8.0)) / 10000.0
        wick_overshoot_frac = float(cfg.get("wick_overshoot_slippage_fraction", 0.15))
        min_atr_pct = float(cfg.get("chandelier_min_atr_pct", 0.022))

        eff_atr = max(float(atr_value), float(bar_close) * min_atr_pct)
        qty = float(quantity)
        side_up = str(side).upper()

        if side_up == "LONG":
            wick_stop_trigger = float(active_stop) - wick_overshoot_atr * eff_atr
            if float(bar_low) <= wick_stop_trigger:
                trigger_px = min(wick_stop_trigger, float(bar_open))
                wick_overshoot = max(0.0, trigger_px - float(bar_low)) * wick_overshoot_frac
                base_wick_px = max(float(bar_low), trigger_px - wick_overshoot)
                exit_fill_price = base_wick_px * (1.0 - stop_slippage_rate)
                exit_slip_usdc = max(0.0, (trigger_px - exit_fill_price) * qty)
                return ChandelierStopEvaluation(
                    triggered=True,
                    is_wick_stop=True,
                    exit_reason="INTRABAR_LOW_WICK_CHANDELIER_STOP_LONG",
                    exit_fill_price=exit_fill_price,
                    exit_slippage_usdc=exit_slip_usdc,
                    chandelier_stop_price=float(active_stop),
                    highest_price_seen=float(highest_price_seen),
                    lowest_price_seen=float(lowest_price_seen),
                    effective_atr=eff_atr,
                )

            new_highest = max(float(highest_price_seen), float(bar_close))
            cand_stop = new_highest - long_mult * eff_atr
            new_stop = max(float(active_stop), cand_stop)
            if float(bar_close) <= new_stop:
                exit_fill_price = float(bar_close) * (1.0 - stop_slippage_rate)
                exit_slip_usdc = float(bar_close) * stop_slippage_rate * qty
                return ChandelierStopEvaluation(
                    triggered=True,
                    is_wick_stop=False,
                    exit_reason="CHANDELIER_TRAILING_STOP_LONG",
                    exit_fill_price=exit_fill_price,
                    exit_slippage_usdc=exit_slip_usdc,
                    chandelier_stop_price=new_stop,
                    highest_price_seen=new_highest,
                    lowest_price_seen=float(lowest_price_seen),
                    effective_atr=eff_atr,
                )

            return ChandelierStopEvaluation(
                triggered=False,
                is_wick_stop=False,
                exit_reason=None,
                exit_fill_price=float(bar_close),
                exit_slippage_usdc=0.0,
                chandelier_stop_price=new_stop,
                highest_price_seen=new_highest,
                lowest_price_seen=float(lowest_price_seen),
                effective_atr=eff_atr,
            )

        # SHORT position
        wick_stop_trigger = float(active_stop) + wick_overshoot_atr * eff_atr
        if float(bar_high) >= wick_stop_trigger:
            trigger_px = max(wick_stop_trigger, float(bar_open))
            wick_overshoot = max(0.0, float(bar_high) - trigger_px) * wick_overshoot_frac
            base_wick_px = min(float(bar_high), trigger_px + wick_overshoot)
            exit_fill_price = base_wick_px * (1.0 + stop_slippage_rate)
            exit_slip_usdc = max(0.0, (exit_fill_price - trigger_px) * qty)
            return ChandelierStopEvaluation(
                triggered=True,
                is_wick_stop=True,
                exit_reason="INTRABAR_HIGH_WICK_CHANDELIER_STOP_SHORT",
                exit_fill_price=exit_fill_price,
                exit_slippage_usdc=exit_slip_usdc,
                chandelier_stop_price=float(active_stop),
                highest_price_seen=float(highest_price_seen),
                lowest_price_seen=float(lowest_price_seen),
                effective_atr=eff_atr,
            )

        new_lowest = min(float(lowest_price_seen), float(bar_close))
        cand_stop = new_lowest + short_mult * eff_atr
        if float(entry_price) - new_lowest >= 1.6 * eff_atr:
            cand_stop = min(cand_stop, float(entry_price) - 0.45 * eff_atr)
        new_stop = min(float(active_stop), cand_stop)
        if float(bar_close) >= new_stop:
            exit_fill_price = float(bar_close) * (1.0 + stop_slippage_rate)
            exit_slip_usdc = float(bar_close) * stop_slippage_rate * qty
            return ChandelierStopEvaluation(
                triggered=True,
                is_wick_stop=False,
                exit_reason="INVERTED_CHANDELIER_STOP_SHORT",
                exit_fill_price=exit_fill_price,
                exit_slippage_usdc=exit_slip_usdc,
                chandelier_stop_price=new_stop,
                highest_price_seen=float(highest_price_seen),
                lowest_price_seen=new_lowest,
                effective_atr=eff_atr,
            )

        return ChandelierStopEvaluation(
            triggered=False,
            is_wick_stop=False,
            exit_reason=None,
            exit_fill_price=float(bar_close),
            exit_slippage_usdc=0.0,
            chandelier_stop_price=new_stop,
            highest_price_seen=float(highest_price_seen),
            lowest_price_seen=new_lowest,
            effective_atr=eff_atr,
        )

    def evaluate_order_risk(
        self,
        pair_meta: dict[str, Any],
        side: str,
        blended_conviction: float,
        exec_df: pd.DataFrame,
        open_trades: list[dict[str, Any]],
        current_equity_usdc: float,
        config: dict[str, Any] | None = None,
    ) -> PositionSizeResult:
        """
        Calculate conviction-scaled position size and verify all portfolio caps:
        - Single trade risk: $50 (0.5%) to $150 (1.5%)
        - Total portfolio heat cap: $400 (4.0% of equity)
        - Single-asset notional cap: $2,500 (25% of equity)
        - Gross leverage notional ceiling: $15,000 (1.5x of equity)
        """
        cfg = config or self.db.load_config()
        symbol = str(pair_meta["exec_symbol"])

        if exec_df.empty:
            return PositionSizeResult(
                approved=False,
                reason="Missing OHLCV series",
                symbol=symbol,
                side=side,
                entry_price=0.0,
                quantity=0.0,
                notional_usdc=0.0,
                risk_budget_usdc=0.0,
                conviction_multiplier=0.0,
                initial_stop_price=0.0,
                atr_value=0.0,
            )

        entry_price = float(exec_df["close"].iloc[-1])
        atr_period = int(cfg.get("atr_period", 14))
        atr_val = self.compute_atr(exec_df, period=atr_period)

        # Compute existing portfolio heat and gross notional exposure
        current_heat_usdc = sum(
            float(t.get("risk_budget_usdc", 0.0))
            for t in open_trades
            if t.get("side") in ("LONG", "SHORT")
        )
        current_gross_notional = sum(
            float(t.get("notional_usdc", 0.0))
            for t in open_trades
            if t.get("side") in ("LONG", "SHORT")
        )

        max_heat_usdc = min(
            float(cfg.get("max_portfolio_heat_usdc", 400.0)),
            current_equity_usdc * float(cfg.get("max_portfolio_heat_pct", 0.04)),
        )
        max_single_asset_usdc = min(
            float(cfg.get("max_single_asset_notional_usdc", 2500.0)),
            current_equity_usdc * float(cfg.get("max_single_asset_pct", 0.25)),
        )
        max_gross_notional_usdc = min(
            float(cfg.get("max_gross_leverage_notional_usdc", 15000.0)),
            current_equity_usdc * float(cfg.get("max_gross_leverage", 1.5)),
        )

        remaining_heat = max_heat_usdc - current_heat_usdc
        if remaining_heat < float(cfg.get("min_risk_per_trade_usdc", 50.0)) * 0.5:
            return PositionSizeResult(
                approved=False,
                reason=f"Portfolio heat cap reached (${current_heat_usdc:.2f} / ${max_heat_usdc:.2f})",
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                quantity=0.0,
                notional_usdc=0.0,
                risk_budget_usdc=0.0,
                conviction_multiplier=0.0,
                initial_stop_price=0.0,
                atr_value=atr_val,
            )

        remaining_notional = max_gross_notional_usdc - current_gross_notional
        min_notional = float(pair_meta.get("min_notional_usdc", 10.0))
        if remaining_notional < min_notional:
            return PositionSizeResult(
                approved=False,
                reason=f"Gross leverage ceiling reached (${current_gross_notional:.2f} / ${max_gross_notional_usdc:.2f})",
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                quantity=0.0,
                notional_usdc=0.0,
                risk_budget_usdc=0.0,
                conviction_multiplier=0.0,
                initial_stop_price=0.0,
                atr_value=atr_val,
            )

        multiplier, desired_risk_usdc = self.compute_conviction_risk_budget(
            blended_conviction=blended_conviction,
            current_equity_usdc=current_equity_usdc,
            config=cfg,
        )
        allocated_risk_usdc = min(desired_risk_usdc, remaining_heat)

        min_atr_pct = float(cfg.get("chandelier_min_atr_pct", 0.022))
        eff_atr = max(atr_val, entry_price * min_atr_pct)
        if side == "LONG":
            atr_mult = float(cfg.get("chandelier_atr_multiplier_long", 3.0))
            stop_distance = max(atr_mult * eff_atr, entry_price * 0.01)
            stop_price = max(entry_price - stop_distance, entry_price * 0.10)
            sizing_distance = max(atr_mult * atr_val, entry_price * 0.01)
        else:
            atr_mult = float(cfg.get("chandelier_atr_multiplier_short", 3.8))
            stop_distance = max(atr_mult * eff_atr, entry_price * 0.01)
            stop_price = entry_price + stop_distance
            sizing_mult = float(cfg.get("short_sizing_atr_multiplier", 2.5))
            sizing_distance = max(sizing_mult * atr_val, entry_price * 0.01)

        stop_pct = sizing_distance / max(entry_price, 1e-12)
        raw_notional = allocated_risk_usdc / max(stop_pct, 1e-6)

        capped_notional = min(raw_notional, max_single_asset_usdc, remaining_notional)
        lot_step = float(pair_meta.get("lot_step_size", 0.001))
        tick_size = float(pair_meta.get("tick_size", 0.01))

        quantity = self._round_step(capped_notional / max(entry_price, 1e-12), lot_step)
        final_notional = round(quantity * entry_price, 2)
        effective_risk = round(final_notional * stop_pct, 2)
        rounded_stop = self._round_step(stop_price, tick_size)

        if final_notional < min_notional or quantity <= 0:
            return PositionSizeResult(
                approved=False,
                reason=f"Notional ${final_notional:.2f} below minimum ${min_notional:.2f}",
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                quantity=0.0,
                notional_usdc=0.0,
                risk_budget_usdc=0.0,
                conviction_multiplier=multiplier,
                initial_stop_price=rounded_stop,
                atr_value=atr_val,
            )

        return PositionSizeResult(
            approved=True,
            reason="Approved within conviction, heat, single-asset, and gross leverage caps",
            symbol=symbol,
            side=side,
            entry_price=entry_price,
            quantity=quantity,
            notional_usdc=final_notional,
            risk_budget_usdc=effective_risk,
            conviction_multiplier=multiplier,
            initial_stop_price=rounded_stop,
            atr_value=atr_val,
        )

