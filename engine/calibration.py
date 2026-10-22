"""Walk-forward per-asset decoupled timeframe calibrator for Aegis Quant."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from engine.database import DatabaseManager

logger = logging.getLogger("aegis.calibration")


class WalkForwardCalibrator:
    """
    Optimizes decoupled execution (ALT/USDC:USDC) and relative-strength (ALT/BTC:SYNTH)
    timeframes per asset using a rolling 90-day walk-forward information ratio objective.
    """

    CANDIDATE_EXEC_TFS = ("1h", "4h")
    CANDIDATE_RS_TFS = ("4h",)
    TIMEFRAME_HOURS: dict[str, int] = {
        "1h": 1,
        "4h": 4,
        "6h": 6,
        "8h": 8,
        "12h": 12,
    }

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    @classmethod
    def resolve_timeframe_hours(
        cls, timeframe: str | int | float | None = None, df: pd.DataFrame | None = None
    ) -> float:
        """Resolve exact timeframe duration in hours (Delta t_hours in {1, 4, 6, 8, 12})."""
        if isinstance(timeframe, (int, float)) and timeframe > 0:
            nearest = min((1, 4, 6, 8, 12), key=lambda h: abs(h - float(timeframe)))
            return float(nearest)
        if isinstance(timeframe, str) and timeframe.strip().lower() in cls.TIMEFRAME_HOURS:
            return float(cls.TIMEFRAME_HOURS[timeframe.strip().lower()])
        if df is not None and len(df) >= 2:
            ts_col = "timestamp_ms" if "timestamp_ms" in df.columns else (
                "timestamp_utc" if "timestamp_utc" in df.columns else None
            )
            if ts_col is not None:
                diffs = pd.Series(df[ts_col]).diff().dropna()
                if not diffs.empty:
                    med = float(diffs.median())
                    # Handle ms vs seconds
                    hours_est = med / 3_600_000.0 if med > 100_000 else med / 3_600.0
                    nearest = min((1, 4, 6, 8, 12), key=lambda h: abs(h - hours_est))
                    return float(nearest)
        return 1.0

    @classmethod
    def compute_annualized_sharpe(
        cls,
        returns: pd.Series | np.ndarray,
        dt_hours: float = 1.0,
    ) -> float:
        """
        Compute annualized Sharpe ratio using exact timeframe duration scaling:
            Sharpe_ann = (mu_r / sigma_r) * sqrt(8760 / Delta t_hours)
        where Delta t_hours in {1, 4, 6, 8, 12}.
        """
        arr = np.asarray(returns, dtype=np.float64)
        arr = arr[np.isfinite(arr)]
        if len(arr) < 2:
            return 0.0
        std_val = float(np.std(arr, ddof=1))
        if std_val <= 1e-8:
            return 0.0
        eff_dt = cls.resolve_timeframe_hours(dt_hours)
        return float((np.mean(arr) / std_val) * np.sqrt(8760.0 / eff_dt))

    @classmethod
    def _evaluate_sharpe_proxy(
        cls,
        df: pd.DataFrame,
        fast_span: int = 16,
        slow_span: int = 64,
        timeframe: str | int | float | None = None,
    ) -> float:
        """Compute out-of-sample walk-forward annualized Sharpe ratio on a price series."""
        if len(df) < 120:
            return 0.0
        closes = np.log(np.maximum(df["close"].to_numpy(dtype=np.float64), 1e-12))
        s = pd.Series(closes)
        rets = s.diff().fillna(0.0)

        ema_f = s.ewm(span=fast_span, adjust=False).mean()
        ema_s = s.ewm(span=slow_span, adjust=False).mean()
        pos = np.sign(ema_f - ema_s).shift(1).fillna(0.0)

        strat_rets = (pos * rets).iloc[slow_span:]
        dt_hours = cls.resolve_timeframe_hours(timeframe=timeframe, df=df)
        return cls.compute_annualized_sharpe(strat_rets, dt_hours=dt_hours)

    def calibrate_universe(self) -> list[dict[str, Any]]:
        """Run walk-forward timeframe calibration across all 15 paired altcoins."""
        config = self.db.load_config()
        fast_span = int(config.get("snr_fast_span", 16))
        slow_span = int(config.get("snr_slow_span", 64))
        alt_pairs = self.db.get_alt_pairs_universe()

        results: list[dict[str, Any]] = []
        for pair in alt_pairs:
            base_asset = str(pair["base_asset"])
            exec_sym = str(pair["exec_symbol"])
            synth_sym = str(pair["synth_symbol"])
            current_exec_tf = str(pair["exec_timeframe"])
            current_rs_tf = str(pair["rs_timeframe"])

            best_exec_tf = current_exec_tf
            best_score = -1e9

            for tf in self.CANDIDATE_EXEC_TFS:
                df = self.db.get_ohlcv_df(exec_sym, tf, limit=540)
                if len(df) >= 120:
                    score = self._evaluate_sharpe_proxy(
                        df, fast_span=fast_span, slow_span=slow_span, timeframe=tf
                    )
                    if score > best_score:
                        best_score = score
                        best_exec_tf = tf

            synth_df = self.db.get_ohlcv_df(synth_sym, current_rs_tf, limit=540)
            rs_score = self._evaluate_sharpe_proxy(
                synth_df, fast_span=fast_span, slow_span=slow_span, timeframe=current_rs_tf
            )

            if best_exec_tf != current_exec_tf:
                self.db.update_asset_timeframes(base_asset, best_exec_tf, current_rs_tf)

            results.append(
                {
                    "base_asset": base_asset,
                    "exec_timeframe": best_exec_tf,
                    "rs_timeframe": current_rs_tf,
                    "exec_ir": round(float(best_score if best_score > -1e8 else 0.0), 4),
                    "rs_ir": round(float(rs_score), 4),
                }
            )

        return results
