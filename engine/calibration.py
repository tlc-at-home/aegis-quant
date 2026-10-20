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

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    @staticmethod
    def _evaluate_sharpe_proxy(df: pd.DataFrame, fast_span: int = 16, slow_span: int = 64) -> float:
        """Compute out-of-sample walk-forward information ratio on a price series."""
        if len(df) < 120:
            return 0.0
        closes = np.log(np.maximum(df["close"].to_numpy(dtype=np.float64), 1e-12))
        s = pd.Series(closes)
        rets = s.diff().fillna(0.0)

        ema_f = s.ewm(span=fast_span, adjust=False).mean()
        ema_s = s.ewm(span=slow_span, adjust=False).mean()
        pos = np.sign(ema_f - ema_s).shift(1).fillna(0.0)

        strat_rets = (pos * rets).iloc[slow_span:]
        std_val = float(strat_rets.std())
        if std_val <= 1e-8:
            return 0.0
        return float(strat_rets.mean() / std_val * np.sqrt(len(strat_rets)))

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
                    score = self._evaluate_sharpe_proxy(df, fast_span=fast_span, slow_span=slow_span)
                    if score > best_score:
                        best_score = score
                        best_exec_tf = tf

            synth_df = self.db.get_ohlcv_df(synth_sym, current_rs_tf, limit=540)
            rs_score = self._evaluate_sharpe_proxy(synth_df, fast_span=fast_span, slow_span=slow_span)

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
