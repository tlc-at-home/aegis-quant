"""Unit tests for zero look-ahead MACE, cached warm-start 3-state HMM with hysteresis, and dynamic IC blending."""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from engine.calibration import WalkForwardCalibrator
from engine.database import DatabaseManager
from engine.signals import SignalEngine


def _make_synthetic_ohlcv_df(
    n_bars: int = 240,
    seed: int = 42,
    drift: float = 0.001,
    base_price: float = 100.0,
) -> pd.DataFrame:
    """Create a deterministic synthetic OHLCV DataFrame for unit testing."""
    rng = np.random.default_rng(seed)
    rets = drift + rng.normal(0.0, 0.01, size=n_bars)
    closes = base_price * np.exp(np.cumsum(rets))
    opens = np.roll(closes, 1)
    opens[0] = base_price
    spread = np.abs(rng.normal(0.004, 0.0015, size=n_bars))
    highs = np.maximum(opens, closes) * (1.0 + spread)
    lows = np.minimum(opens, closes) * (1.0 - spread)
    volumes = rng.uniform(1000.0, 5000.0, size=n_bars)
    ts_base = 1750000000000
    timestamps = np.arange(n_bars, dtype=np.int64) * 14_400_000 + ts_base

    return pd.DataFrame(
        {
            "timestamp_ms": timestamps,
            "timestamp_utc": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "funding_rate_hourly": np.full(n_bars, 0.00002, dtype=np.float64),
            "is_synthetic": np.zeros(n_bars, dtype=np.int64),
        }
    )


class TestSignalsHMMMace(unittest.TestCase):
    """Test suite for Rollout Commit Point 1 (v0.2.0-alpha1) Quantitative Core."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_quant_core.db"
        self.db = DatabaseManager(db_path=self.db_path)
        self.db.init_db()
        self.db.apply_migration("sql/migrations/001_v020_config_expansion.sql")
        self.cfg = self.db.load_config()
        self.engine = SignalEngine(self.db)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_mace_no_lookahead_leakage(self) -> None:
        """
        Verify zero look-ahead bias and zero target leakage in compute_mace_nonlinear_score:
        1. Even if _build_mace_features_and_target is computed on a full series [0..N] where
           future bars [eval_cutoff:] are catastrophically mutated (+10,000% spike or -99.9% crash,
           which contaminates target[eval_cutoff - k : eval_cutoff]), slicing [:eval_cutoff]
           produces the exact same MACE score because X_train / y_train strictly purges [-k:].
        2. Mutating both features and targets in [:warmup] and [-k:-1] (and target[-k:]) has
           zero effect on feature standardization (X_center, X_std), target volatility scaling,
           fitted RF/Ridge parameters, or out-of-sample inference at features.iloc[[-1]].
        3. Holds across dynamic mace_forward_horizon_bars values k in {4, 8, 12}.
        """
        full_exec_df = _make_synthetic_ohlcv_df(n_bars=260, seed=101, drift=0.0012, base_price=150.0)
        full_synth_df = _make_synthetic_ohlcv_df(n_bars=260, seed=202, drift=0.0008, base_price=0.0025)

        eval_cutoff = 200
        warmup = 32

        pit_exec_clean = full_exec_df.iloc[:eval_cutoff].copy()
        pit_synth_clean = full_synth_df.iloc[:eval_cutoff].copy()

        score_single_df = self.engine.compute_mace_nonlinear_score(
            pit_exec_clean, self.cfg
        )
        self.assertIsInstance(score_single_df, float)

        orig_builder = SignalEngine._build_mace_features_and_target

        for k in (4, 8, 12):
            cfg_k = dict(self.cfg)
            cfg_k["mace_forward_horizon_bars"] = k

            score_clean = self.engine.compute_mace_nonlinear_score(
                pit_exec_clean, pit_synth_clean, cfg_k
            )
            self.assertIsInstance(score_clean, float)
            self.assertGreaterEqual(score_clean, -1.0)
            self.assertLessEqual(score_clean, 1.0)

            # Case A: Mutate future bars [eval_cutoff:] on the full 260-bar series BEFORE
            # building forward targets (so unpurged target[eval_cutoff-k : eval_cutoff] is
            # directly contaminated by the 100x / 0.001x future price shock), then slice [:eval_cutoff].
            mutated_exec_future = full_exec_df.copy()
            mutated_synth_future = full_synth_df.copy()
            mutated_exec_future.iloc[eval_cutoff:, mutated_exec_future.columns.get_loc("close")] *= 100.0
            mutated_synth_future.iloc[eval_cutoff:, mutated_synth_future.columns.get_loc("close")] *= 0.001

            full_feats_mut, full_targ_mut = orig_builder(
                mutated_exec_future, mutated_synth_future, forward_horizon_bars=k
            )
            # Confirm the unpurged tail [eval_cutoff - k : eval_cutoff] actually changed in full_targ_mut
            clean_feats_full, clean_targ_full = orig_builder(
                full_exec_df, full_synth_df, forward_horizon_bars=k
            )
            self.assertFalse(
                np.allclose(
                    full_targ_mut.iloc[eval_cutoff - k : eval_cutoff].to_numpy(),
                    clean_targ_full.iloc[eval_cutoff - k : eval_cutoff].to_numpy(),
                ),
                msg=f"Expected future mutation at eval_cutoff to alter unpurged target tail for k={k}",
            )

            def _future_leaked_series_builder(
                exec_df: pd.DataFrame,
                synth_df: pd.DataFrame | None = None,
                forward_horizon_bars: int = k,
                _f: pd.DataFrame = full_feats_mut,
                _t: pd.Series = full_targ_mut,
            ) -> tuple[pd.DataFrame, pd.Series]:
                return _f.iloc[:eval_cutoff].copy(), _t.iloc[:eval_cutoff].copy()

            with patch.object(
                SignalEngine,
                "_build_mace_features_and_target",
                side_effect=_future_leaked_series_builder,
            ):
                score_future_mutated = self.engine.compute_mace_nonlinear_score(
                    pit_exec_clean, pit_synth_clean, cfg_k
                )

            self.assertAlmostEqual(
                score_clean,
                score_future_mutated,
                places=12,
                msg=f"Point-in-time MACE score (k={k}) leaked future prices from unpurged tail [eval_cutoff-k:eval_cutoff]",
            )

            # Case B: Directly poison both features and targets in [:warmup] and the embargo window [-k:-1] / [-k:]
            def _contaminated_tail_and_warmup_builder(
                exec_df: pd.DataFrame,
                synth_df: pd.DataFrame | None = None,
                forward_horizon_bars: int = k,
            ) -> tuple[pd.DataFrame, pd.Series]:
                feats, targ = orig_builder(exec_df, synth_df, forward_horizon_bars)
                feats_contaminated = feats.copy()
                targ_contaminated = targ.copy()
                # Poison warmup features & targets [:warmup]
                feats_contaminated.iloc[:warmup, :] = -1e9
                targ_contaminated.iloc[:warmup] = -1e9
                # Poison embargo features [-k:-1] (leaving [-1] intact for current-bar OOS inference)
                if forward_horizon_bars > 1:
                    feats_contaminated.iloc[-forward_horizon_bars:-1, :] = 1e9
                # Poison embargo targets [-k:]
                targ_contaminated.iloc[-forward_horizon_bars:] = 1e9
                return feats_contaminated, targ_contaminated

            with patch.object(
                SignalEngine,
                "_build_mace_features_and_target",
                side_effect=_contaminated_tail_and_warmup_builder,
            ):
                score_with_poisoned_unpurged_tail = self.engine.compute_mace_nonlinear_score(
                    pit_exec_clean, pit_synth_clean, cfg_k
                )

            self.assertAlmostEqual(
                score_clean,
                score_with_poisoned_unpurged_tail,
                places=12,
                msg=f"Purged MACE training slice iloc[warmup:-{k}] leaked features or targets from [-{k}:] or [:warmup]",
            )

    def test_maintenance_bars_nan_zero_inf_sanitization(self) -> None:
        """Verify HMM, PC1, ATR, and MACE gracefully sanitize NaN, 0.0, negative, and +-inf maintenance bars."""
        df_corrupt = _make_synthetic_ohlcv_df(n_bars=240, seed=55, drift=0.001, base_price=50.0)
        df_corrupt.loc[25, ["open", "high", "low", "close", "volume"]] = 0.0
        df_corrupt.loc[60, ["open", "high", "low", "close", "volume"]] = np.nan
        df_corrupt.loc[110, ["open", "high", "low", "close", "volume"]] = np.inf
        df_corrupt.loc[145, ["open", "high", "low", "close", "volume"]] = -5.0

        atr_series = self.engine.calculate_atr(df_corrupt, period=14)
        self.assertTrue(np.all(np.isfinite(atr_series.to_numpy())))

        pc1_series = self.engine.compute_alt_pc1_series(
            df_corrupt, {"SOL": df_corrupt, "ETH": df_corrupt}
        )
        self.assertTrue(np.all(np.isfinite(pc1_series.to_numpy())))

        regime, bull_p, bear_p, chop_p = self.engine.decode_hmm_regime_states(
            df_corrupt, pc1_series, self.cfg
        )
        self.assertIn(regime, ("BULL", "BEAR", "CHOP"))
        self.assertAlmostEqual(bull_p + bear_p + chop_p, 1.0, places=5)
        self.assertTrue(np.all(np.isfinite(self.engine._hmm_cache["vars_diag"])))

        mace_score = self.engine.compute_mace_nonlinear_score(df_corrupt, df_corrupt, self.cfg)
        self.assertTrue(np.isfinite(mace_score))
        self.assertGreaterEqual(mace_score, -1.0)
        self.assertLessEqual(mace_score, 1.0)

    def test_backtest_pit_invariance_to_future_db_bars(self) -> None:
        """
        Verify BacktestEngine._clone_to_memory_db and run_backtest strictly isolate [start_date, end_date]
        and produce identical metrics even when future bars after end_date are inserted or mutated in SQLite.
        """
        from engine.backtest import BacktestEngine

        # Populate 180 4H bars ending on 2026-08-15T20:00:00Z
        end_ms = 1786838400000 - 14_400_000  # 2026-08-15T20:00:00Z
        n_hist = 190
        start_ms = end_ms - (n_hist - 1) * 14_400_000
        replay_start_ms = end_ms - 25 * 14_400_000

        usdc_rows = self.db.get_usdc_perp_universe()
        synth_rows = self.db.get_btc_synth_universe()
        symbols = [str(r["symbol"]) for r in usdc_rows[:7]] + [str(r["symbol"]) for r in synth_rows[:6]]

        batch: list[tuple[object, ...]] = []
        for idx, sym in enumerate(symbols):
            is_synth = 1 if "/BTC" in sym else 0
            df_sym = _make_synthetic_ohlcv_df(
                n_bars=n_hist, seed=300 + idx, drift=0.0015, base_price=100.0 * (idx + 1)
            )
            for bar_i in range(n_hist):
                ts = start_ms + bar_i * 14_400_000
                r = df_sym.iloc[bar_i]
                batch.append(
                    (
                        sym,
                        "4h",
                        int(ts),
                        float(r["open"]),
                        float(r["high"]),
                        float(r["low"]),
                        float(r["close"]),
                        float(r["volume"]),
                        float(r["funding_rate_hourly"]),
                        is_synth,
                    )
                )
        self.db.upsert_ohlcv_batch(batch)

        start_str = pd.to_datetime(replay_start_ms, unit="ms", utc=True).strftime("%Y-%m-%d")
        end_str = pd.to_datetime(end_ms, unit="ms", utc=True).strftime("%Y-%m-%d")

        bt_clean = BacktestEngine(self.db_path, initial_equity=10000.0)
        metrics_clean = bt_clean.run_backtest(start_str, end_str)

        # Now insert extreme future bars after end_ms (+5000% spike and crash)
        future_batch: list[tuple[object, ...]] = []
        for sym in symbols:
            is_synth = 1 if "/BTC" in sym else 0
            for k in range(1, 15):
                future_batch.append(
                    (sym, "4h", int(end_ms + 14_400_000 * k), 999999.0, 999999.0, 0.001, 999999.0, 1e9, 0.05, is_synth)
                )
        self.db.upsert_ohlcv_batch(future_batch)

        bt_with_future = BacktestEngine(self.db_path, initial_equity=10000.0)
        metrics_with_future = bt_with_future.run_backtest(start_str, end_str)

        self.assertAlmostEqual(metrics_clean.final_equity_usdc, metrics_with_future.final_equity_usdc, places=8)
        self.assertAlmostEqual(metrics_clean.annualized_sharpe, metrics_with_future.annualized_sharpe, places=8)
        self.assertAlmostEqual(metrics_clean.max_drawdown_pct, metrics_with_future.max_drawdown_pct, places=8)
        self.assertEqual(metrics_clean.bull_bars_count, metrics_with_future.bull_bars_count)
        self.assertEqual(metrics_clean.bear_bars_count, metrics_with_future.bear_bars_count)
        self.assertEqual(metrics_clean.chop_bars_count, metrics_with_future.chop_bars_count)



    def test_hmm_cached_warm_start_and_hysteresis(self) -> None:
        """
        Verify 4H bar caching in SignalEngine._hmm_cache, >80% CPU drop on 15s intra-bar ticks,
        and 0.55 entry / 0.45 exit hysteresis confirmation gates.
        """
        btc_df = _make_synthetic_ohlcv_df(n_bars=300, seed=77, drift=0.0015, base_price=65000.0)
        alt_pc1 = pd.Series(
            np.log(btc_df["close"] / btc_df["close"].shift(1)).fillna(0.0).to_numpy() * 1.15,
            index=btc_df.index,
        )

        t0 = time.perf_counter()
        regime_1, bull_1, bear_1, chop_1 = self.engine.decode_hmm_regime_states(
            btc_df, alt_pc1, self.cfg
        )
        cold_elapsed = time.perf_counter() - t0

        self.assertIn(regime_1, ("BULL", "BEAR", "CHOP"))
        self.assertAlmostEqual(bull_1 + bear_1 + chop_1, 1.0, places=5)
        self.assertEqual(self.engine._hmm_cache.get("ts"), int(btc_df["timestamp_utc"].iloc[-1]))

        # Simulate 15s intermediate tick on the same 4H bar timestamp: EM must NOT be called
        btc_tick = btc_df.copy()
        btc_tick.iloc[-1, btc_tick.columns.get_loc("close")] *= 1.001

        with patch.object(
            self.engine, "_fit_baum_welch_log", wraps=self.engine._fit_baum_welch_log
        ) as spy_em:
            t1 = time.perf_counter()
            regime_2, bull_2, bear_2, chop_2 = self.engine.decode_hmm_regime_states(
                btc_tick, alt_pc1, self.cfg
            )
            cached_elapsed = time.perf_counter() - t1
            spy_em.assert_not_called()

        self.assertAlmostEqual(bull_2 + bear_2 + chop_2, 1.0, places=5)
        self.assertLess(
            cached_elapsed,
            cold_elapsed * 0.20,
            msg=f"Cached 15s tick ({cached_elapsed:.6f}s) must drop CPU time by >80% vs cold EM ({cold_elapsed:.6f}s)",
        )

        # Verify Hysteresis Confirmation Gates (enter >= 0.55, exit < 0.45)
        latest_ts = int(btc_df["timestamp_utc"].iloc[-1])
        self.engine._hmm_cache["ts"] = latest_ts
        self.engine._hmm_cache["last_obs"] = None

        # When in CHOP, bull_prob=0.52 (< 0.55 enter gate) must remain CHOP
        self.engine._hmm_cache["regime"] = "CHOP"
        self.engine._hmm_cache["posterior"] = np.array([0.18, 0.30, 0.52])
        reg_chop, _, _, _ = self.engine.decode_hmm_regime_states(btc_df, alt_pc1, self.cfg)
        self.assertEqual(reg_chop, "CHOP")

        # When bull_prob=0.56 (>= 0.55 enter gate), transitions to BULL
        self.engine._hmm_cache["posterior"] = np.array([0.14, 0.30, 0.56])
        reg_enter_bull, _, _, _ = self.engine.decode_hmm_regime_states(btc_df, alt_pc1, self.cfg)
        self.assertEqual(reg_enter_bull, "BULL")

        # Once in BULL, bull_prob=0.48 (>= 0.45 exit gate) stays BULL (no flapping)
        self.engine._hmm_cache["posterior"] = np.array([0.20, 0.32, 0.48])
        reg_stay_bull, _, _, _ = self.engine.decode_hmm_regime_states(btc_df, alt_pc1, self.cfg)
        self.assertEqual(reg_stay_bull, "BULL")

        # When bull_prob drops to 0.42 (< 0.45 exit gate) and bear_prob < 0.55, exits to CHOP
        self.engine._hmm_cache["posterior"] = np.array([0.28, 0.30, 0.42])
        reg_exit_bull, _, _, _ = self.engine.decode_hmm_regime_states(btc_df, alt_pc1, self.cfg)
        self.assertEqual(reg_exit_bull, "CHOP")

    def test_dynamic_ic_mace_snr_blending_and_annualized_sharpe(self) -> None:
        """Verify dynamic Spearman IC weight bounds [0.15, 0.60] and timeframe-annualized Sharpe."""
        w_equal = SignalEngine.compute_dynamic_mace_weight(0.10, 0.10, min_ic_floor=0.05)
        self.assertAlmostEqual(w_equal, 0.50, places=6)

        w_high_mace = SignalEngine.compute_dynamic_mace_weight(0.45, 0.01, min_ic_floor=0.05)
        self.assertAlmostEqual(w_high_mace, 0.60, places=6)

        w_low_mace = SignalEngine.compute_dynamic_mace_weight(-0.20, 0.50, min_ic_floor=0.05)
        self.assertAlmostEqual(w_low_mace, 0.15, places=6)

        rets = np.array([0.002, -0.001, 0.003, 0.001, -0.0005, 0.0025], dtype=np.float64)
        s_1h = WalkForwardCalibrator.compute_annualized_sharpe(rets, dt_hours=1.0)
        s_4h = WalkForwardCalibrator.compute_annualized_sharpe(rets, dt_hours=4.0)
        self.assertAlmostEqual(s_1h / s_4h, 2.0, places=6)

    def test_calibration_1_two_tier_chandelier_stops_and_38x_short_buffer(self) -> None:
        """
        Verify Calibration 1:
        - 3.0x ATR LONG and 3.8x ATR SHORT Chandelier multipliers in system_config
        - highest_price_seen / lowest_price_seen ratchet strictly on confirmed bar_close (not intra-bar spikes)
        - Normal intra-bar wicks that do not overshoot active_stop by 0.85x ATR do NOT stop out
        - Catastrophic intra-bar wick overshoots (>= 0.85x ATR beyond stop) trigger wick stop with 8 bps + 15% overshoot slippage
        - Standard bar_close stop triggers with 8 bps slippage
        """
        from engine.risk import RiskEngine

        risk = RiskEngine(self.db)
        self.assertAlmostEqual(float(self.cfg["chandelier_atr_multiplier_long"]), 3.0, places=4)
        self.assertAlmostEqual(float(self.cfg["chandelier_atr_multiplier_short"]), 3.8, places=4)
        self.assertAlmostEqual(float(self.cfg["chandelier_wick_overshoot_atr"]), 0.85, places=4)
        self.assertAlmostEqual(float(self.cfg["stop_slippage_bps"]), 8.0, places=4)
        self.assertAlmostEqual(float(self.cfg["wick_overshoot_slippage_fraction"]), 0.15, places=4)

        # 1. LONG position: transient intra-bar high spike (130.0) must NOT ratchet highest_price_seen when bar_close is 105.0,
        # and a pullback wick to 92.5 (below active_stop=94.0, but above wick_stop_trigger = 94.0 - 0.85*3.0 = 91.45) must NOT trigger stop!
        eval_long_hold = risk.evaluate_two_tier_chandelier_stop(
            side="LONG",
            entry_price=100.0,
            active_stop=91.0,
            highest_price_seen=100.0,
            lowest_price_seen=100.0,
            bar_open=100.0,
            bar_high=130.0,   # transient spike that would pull stop to 121.0 if ratcheted on high
            bar_low=89.0,     # wick_stop_trigger = 91.0 - 0.85 * 3.0 = 88.45 -> 89.0 > 88.45 (survives!)
            bar_close=105.0,
            atr_value=3.0,
            quantity=10.0,
            config=self.cfg,
        )
        self.assertFalse(eval_long_hold.triggered)
        self.assertFalse(eval_long_hold.is_wick_stop)
        self.assertAlmostEqual(eval_long_hold.highest_price_seen, 105.0, places=6)
        self.assertAlmostEqual(eval_long_hold.chandelier_stop_price, 105.0 - 3.0 * 3.0, places=6)

        # 2. LONG catastrophic intra-bar low wick overshoot: bar_low=85.0 <= wick_stop_trigger (96.0 - 0.85*3.0 = 93.45)
        eval_long_wick = risk.evaluate_two_tier_chandelier_stop(
            side="LONG",
            entry_price=100.0,
            active_stop=96.0,
            highest_price_seen=105.0,
            lowest_price_seen=100.0,
            bar_open=104.0,
            bar_high=105.0,
            bar_low=85.0,
            bar_close=98.0,
            atr_value=3.0,
            quantity=10.0,
            config=self.cfg,
        )
        self.assertTrue(eval_long_wick.triggered)
        self.assertTrue(eval_long_wick.is_wick_stop)
        self.assertEqual(eval_long_wick.exit_reason, "INTRABAR_LOW_WICK_CHANDELIER_STOP_LONG")
        expected_trigger = 96.0 - 0.85 * 3.0  # 93.45
        expected_overshoot = (expected_trigger - 85.0) * 0.15
        expected_fill = (expected_trigger - expected_overshoot) * (1.0 - 0.0008)
        self.assertAlmostEqual(eval_long_wick.exit_fill_price, expected_fill, places=6)

        # 3. SHORT position: 3.8x ATR buffer + bar_close ratcheting + profit-lock ratchet
        eval_short_hold = risk.evaluate_two_tier_chandelier_stop(
            side="SHORT",
            entry_price=100.0,
            active_stop=111.4,  # 100 + 3.8 * 3.0
            highest_price_seen=100.0,
            lowest_price_seen=100.0,
            bar_open=100.0,
            bar_high=113.0,     # wick_stop_trigger = 111.4 + 0.85 * 3.0 = 113.95 -> 113.0 < 113.95 (survives!)
            bar_low=80.0,       # transient low wick must NOT ratchet lowest_price_seen
            bar_close=97.0,
            atr_value=3.0,
            quantity=10.0,
            config=self.cfg,
        )
        self.assertFalse(eval_short_hold.triggered)
        self.assertAlmostEqual(eval_short_hold.lowest_price_seen, 97.0, places=6)
        self.assertAlmostEqual(eval_short_hold.chandelier_stop_price, 97.0 + 3.8 * 3.0, places=6)

        # 4. SHORT standard bar_close trailing stop (8 bps slippage)
        eval_short_close_stop = risk.evaluate_two_tier_chandelier_stop(
            side="SHORT",
            entry_price=100.0,
            active_stop=105.0,
            highest_price_seen=100.0,
            lowest_price_seen=94.0,
            bar_open=103.0,
            bar_high=106.5,     # < 105.0 + 0.85 * 3.0 = 107.55 (no wick liquidation)
            bar_low=102.0,
            bar_close=105.5,    # >= active_stop (105.0) -> triggers standard close stop
            atr_value=3.0,
            quantity=10.0,
            config=self.cfg,
        )
        self.assertTrue(eval_short_close_stop.triggered)
        self.assertFalse(eval_short_close_stop.is_wick_stop)
        self.assertEqual(eval_short_close_stop.exit_reason, "INVERTED_CHANDELIER_STOP_SHORT")
        self.assertAlmostEqual(eval_short_close_stop.exit_fill_price, 105.5 * 1.0008, places=6)

        # 5. Boundary wick touch (bar_low == wick_stop_trigger) and gap-open bar (bar_open == bar_low < wick_stop_trigger)
        # must still apply 8 bps base stop slippage (never clamped to 0 bps by bar_low/bar_high)
        eval_long_boundary_wick = risk.evaluate_two_tier_chandelier_stop(
            side="LONG",
            entry_price=100.0,
            active_stop=96.0,
            highest_price_seen=105.0,
            lowest_price_seen=100.0,
            bar_open=100.0,
            bar_high=101.0,
            bar_low=93.45,      # exact wick_stop_trigger (96.0 - 0.85 * 3.0 = 93.45)
            bar_close=95.0,
            atr_value=3.0,
            quantity=10.0,
            config=self.cfg,
        )
        self.assertTrue(eval_long_boundary_wick.triggered)
        self.assertTrue(eval_long_boundary_wick.is_wick_stop)
        self.assertAlmostEqual(eval_long_boundary_wick.exit_fill_price, 93.45 * (1.0 - 0.0008), places=6)
        self.assertGreater(eval_long_boundary_wick.exit_slippage_usdc, 0.0)

        eval_short_gap_open = risk.evaluate_two_tier_chandelier_stop(
            side="SHORT",
            entry_price=100.0,
            active_stop=105.0,
            highest_price_seen=100.0,
            lowest_price_seen=94.0,
            bar_open=112.0,     # gaps above both active_stop (105.0) and wick_stop_trigger (107.55)
            bar_high=112.0,
            bar_low=109.0,
            bar_close=110.0,
            atr_value=3.0,
            quantity=10.0,
            config=self.cfg,
        )
        self.assertTrue(eval_short_gap_open.triggered)
        self.assertTrue(eval_short_gap_open.is_wick_stop)
        self.assertAlmostEqual(eval_short_gap_open.exit_fill_price, 112.0 * (1.0 + 0.0008), places=6)
        self.assertGreater(eval_short_gap_open.exit_slippage_usdc, 0.0)

    def test_calibration_2_symmetric_bear_gate_and_btc_macro_confirmation(self) -> None:
        """
        Verify Calibration 2:
        - Symmetric high-conviction SHORT gate (prob_bear >= 0.58, conv < -0.32, snr_synth < -0.30,
          mace_score < -0.05, rs_96 < -0.03, r_usdc_8 < -0.008)
        - Multi-horizon BTC macro trend confirmation (btc_snr <= -0.45, btc_snr_macro <= -0.20,
          btc_ret_30 <= -0.015, btc_ret_60 <= -0.04, btc_ret_180 <= 0.002)
        - Rejects false SHORT signals during brief pullbacks inside a secular BTC bull expansion
        """
        bear_btc_macro_confirmed = {
            "btc_snr": -0.55,
            "btc_snr_macro": -0.32,
            "btc_ret_30": -0.045,
            "btc_ret_60": -0.085,
            "btc_ret_180": -0.12,
        }
        action_short = SignalEngine.classify_signal_action(
            regime="BEAR",
            prob_bull=0.05,
            prob_bear=0.68,
            rs_rank=15,
            total_assets=15,
            blended_conviction=-0.42,
            snr_synth=-0.38,
            mace_score=-0.12,
            rs_96=-0.055,
            r_usdc_8=-0.018,
            funding_rate_hourly=0.00001,
            btc_macro=bear_btc_macro_confirmed,
            cfg=self.cfg,
        )
        self.assertEqual(action_short, "SHORT")

        # Brief pullback inside a secular BTC bull expansion (btc_ret_180 = +0.18 > 0.002) must NOT short
        secular_bull_pullback_macro = {
            **bear_btc_macro_confirmed,
            "btc_ret_180": 0.18,
        }
        action_filtered_macro = SignalEngine.classify_signal_action(
            regime="BEAR",
            prob_bull=0.05,
            prob_bear=0.68,
            rs_rank=15,
            total_assets=15,
            blended_conviction=-0.42,
            snr_synth=-0.38,
            mace_score=-0.12,
            rs_96=-0.055,
            r_usdc_8=-0.018,
            funding_rate_hourly=0.00001,
            btc_macro=secular_bull_pullback_macro,
            cfg=self.cfg,
        )
        self.assertEqual(action_filtered_macro, "FLAT")

        # Weak cross-sectional conviction (conv = -0.20 >= -0.32) must NOT short
        action_filtered_conv = SignalEngine.classify_signal_action(
            regime="BEAR",
            prob_bull=0.05,
            prob_bear=0.68,
            rs_rank=15,
            total_assets=15,
            blended_conviction=-0.20,
            snr_synth=-0.38,
            mace_score=-0.12,
            rs_96=-0.055,
            r_usdc_8=-0.018,
            funding_rate_hourly=0.00001,
            btc_macro=bear_btc_macro_confirmed,
            cfg=self.cfg,
        )
        self.assertEqual(action_filtered_conv, "FLAT")

    def test_calibration_3_post_stop_cooldown_14_bars(self) -> None:
        """
        Verify Calibration 3:
        - post_stop_cooldown_bars is configured to 14 bars (56 hours on 4H timeframe)
        - ExecutionEngine blocks re-entry on a stopped-out asset for strictly 14 bars / 56h
        - 15-second intra-bar polling (14 polls = 3.5 minutes) does NOT prematurely expire 56h cooldown
        - Daemon restart mid-cooldown reconstructs _cooldown_until_ms from SQLite trades.closed_at
        """
        import asyncio
        from engine.execution import ExecutionEngine
        from engine.ingestion import MarketIngestionEngine
        from engine.risk import RiskEngine
        from engine.signals import RegimeState

        self.assertEqual(int(self.cfg["post_stop_cooldown_bars"]), 14)

        ing = MarketIngestionEngine(self.db)
        for sym in ("BTC/USDC:USDC", "SOL/USDC:USDC"):
            bars = ing._generate_synthetic_usdc_series(sym, "1h", limit=120)
            self.db.upsert_ohlcv_batch(bars)

        base_ts_ms = int(time.time() * 1000)
        bar_4h_ms = 4 * 3_600_000
        self.db.pit_timestamp_ms = base_ts_ms

        exec_engine = ExecutionEngine(self.db, RiskEngine(self.db))
        regime_bull = RegimeState("BULL", 0.85, 0.05, 0.10, 540)
        regime_bear = RegimeState("BEAR", 0.05, 0.85, 0.10, 540)
        sol_eval = [
            {
                "timestamp_ms": base_ts_ms,
                "base_asset": "SOL",
                "exec_symbol": "SOL/USDC:USDC",
                "synth_symbol": "SOL/BTC:SYNTH",
                "hmm_regime": "BULL",
                "prob_bull": 0.85,
                "prob_bear": 0.05,
                "prob_chop": 0.10,
                "snr_score": 0.7,
                "mace_score": 0.6,
                "blended_conviction": 0.665,
                "rs_rank": 1,
                "signal_action": "LONG",
                "funding_rate_hourly": 0.00002,
            }
        ]

        # Step 1: Open SOL LONG at base_ts_ms
        stats_open = asyncio.run(exec_engine.reconcile_cycle(regime_bull, sol_eval))
        self.assertEqual(stats_open["opened_trades"], 1)

        # Step 2: Close SOL LONG at exit_ts_ms -> triggers 14-bar / 56h cooldown
        exit_ts_ms = base_ts_ms + bar_4h_ms
        self.db.pit_timestamp_ms = exit_ts_ms
        stats_close = asyncio.run(exec_engine.reconcile_cycle(regime_bear, []))
        self.assertEqual(stats_close["closed_trades"], 1)
        exit_step = exec_engine._step_counter
        self.assertEqual(exec_engine._cooldown_until_step["SOL/USDC:USDC"], exit_step + 14)
        self.assertEqual(exec_engine._cooldown_until_ms["SOL/USDC:USDC"], exit_ts_ms + 14 * bar_4h_ms)

        # Verify 15-second intra-bar polling (even after 20 polls = 5 minutes) does NOT expire 56h cooldown!
        self.assertTrue(
            exec_engine.is_in_cooldown(
                "SOL/USDC:USDC",
                now_ms=exit_ts_ms + 20 * 15_000,
                step_idx=exit_step + 20,
            )
        )

        # Verify process restart 2 hours into cooldown reconstructs _cooldown_until_ms from SQLite trades.closed_at
        restarted_engine = ExecutionEngine(self.db, RiskEngine(self.db))
        self.assertIn("SOL/USDC:USDC", restarted_engine._cooldown_until_ms)
        self.assertTrue(
            restarted_engine.is_in_cooldown(
                "SOL/USDC:USDC",
                now_ms=exit_ts_ms + 2 * 3_600_000,
                step_idx=1,
            )
        )

        # Steps +1..+13 (13 4H bars / 52h after exit): re-entry must be blocked by cooldown
        for offset in range(1, 14):
            self.db.pit_timestamp_ms = exit_ts_ms + offset * bar_4h_ms
            self.assertTrue(
                exec_engine.is_in_cooldown(
                    "SOL/USDC:USDC",
                    now_ms=self.db.pit_timestamp_ms,
                    step_idx=exit_step + offset,
                ),
                msg=f"Expected SOL/USDC:USDC to remain in cooldown at offset +{offset} bars",
            )
            stats_blocked = asyncio.run(exec_engine.reconcile_cycle(regime_bull, sol_eval))
            self.assertEqual(stats_blocked["opened_trades"], 0)

        # At exit_step + 14 (56 hours / 14 4H bars later): cooldown expires and re-entry succeeds
        self.db.pit_timestamp_ms = exit_ts_ms + 14 * bar_4h_ms
        self.assertFalse(
            exec_engine.is_in_cooldown(
                "SOL/USDC:USDC",
                now_ms=self.db.pit_timestamp_ms,
                step_idx=exit_step + 14,
            )
        )
        self.assertFalse(
            restarted_engine.is_in_cooldown(
                "SOL/USDC:USDC",
                now_ms=self.db.pit_timestamp_ms,
                step_idx=1,
            )
        )
        stats_reopen = asyncio.run(exec_engine.reconcile_cycle(regime_bull, sol_eval))
        self.assertEqual(stats_reopen["opened_trades"], 1)

    def test_hmm_regime_decoding_on_synthetic_bull_bear_chop_series(self) -> None:
        """
        Validate 3-State Diagonal-Covariance Gaussian HMM regime decoding accuracy across
        synthetic Bear, Chop, and Bull market series segments.
        """
        rng = np.random.default_rng(2026)
        rets_bear = -0.009 + rng.normal(0.0, 0.004, size=100)
        rets_chop = 0.000 + rng.normal(0.0, 0.0015, size=100)
        rets_bull = +0.009 + rng.normal(0.0, 0.003, size=100)

        def _build_3phase_df(rets: np.ndarray) -> pd.DataFrame:
            n = len(rets)
            closes = 50000.0 * np.exp(np.cumsum(rets))
            opens = np.roll(closes, 1)
            opens[0] = 50000.0
            highs = np.maximum(opens, closes) * 1.002
            lows = np.minimum(opens, closes) * 0.998
            ts = np.arange(n, dtype=np.int64) * 14_400_000 + 1750000000000
            return pd.DataFrame(
                {
                    "timestamp_ms": ts,
                    "timestamp_utc": ts,
                    "open": opens,
                    "high": highs,
                    "low": lows,
                    "close": closes,
                    "volume": np.full(n, 2500.0, dtype=np.float64),
                    "funding_rate_hourly": np.zeros(n, dtype=np.float64),
                    "is_synthetic": np.zeros(n, dtype=np.int64),
                }
            )

        # 1. Decode ending in BULL phase (Bear -> Chop -> Bull)
        df_end_bull = _build_3phase_df(np.concatenate([rets_bear, rets_chop, rets_bull]))
        pc1_bull = pd.Series(np.concatenate([rets_bear, rets_chop, rets_bull]) * 1.1)
        self.engine._hmm_cache.clear()
        reg_bull, p_bull, p_bear, p_chop = self.engine.decode_hmm_regime_states(
            df_end_bull, pc1_bull, self.cfg
        )
        self.assertEqual(reg_bull, "BULL")
        self.assertGreater(p_bull, 0.55)
        self.assertAlmostEqual(p_bull + p_bear + p_chop, 1.0, places=5)

        # 2. Decode ending in BEAR phase (Chop -> Bull -> Bear)
        df_end_bear = _build_3phase_df(np.concatenate([rets_chop, rets_bull, rets_bear]))
        pc1_bear = pd.Series(np.concatenate([rets_chop, rets_bull, rets_bear]) * 1.1)
        self.engine._hmm_cache.clear()
        reg_bear, p_bull_b, p_bear_b, p_chop_b = self.engine.decode_hmm_regime_states(
            df_end_bear, pc1_bear, self.cfg
        )
        self.assertEqual(reg_bear, "BEAR")
        self.assertGreater(p_bear_b, 0.55)
        self.assertAlmostEqual(p_bull_b + p_bear_b + p_chop_b, 1.0, places=5)

        # 3. Decode ending in CHOP phase (Bear -> Bull -> Chop)
        df_end_chop = _build_3phase_df(np.concatenate([rets_bear, rets_bull, rets_chop]))
        pc1_chop = pd.Series(np.concatenate([rets_bear, rets_bull, rets_chop]) * 1.1)
        self.engine._hmm_cache.clear()
        reg_chop, p_bull_c, p_bear_c, p_chop_c = self.engine.decode_hmm_regime_states(
            df_end_chop, pc1_chop, self.cfg
        )
        self.assertEqual(reg_chop, "CHOP")
        self.assertGreater(p_chop_c, 0.50)
        self.assertAlmostEqual(p_bull_c + p_bear_c + p_chop_c, 1.0, places=5)


def test_mace_no_lookahead_leakage() -> None:
    """Standalone pytest function entry point for test_mace_no_lookahead_leakage."""
    suite = TestSignalsHMMMace("test_mace_no_lookahead_leakage")
    suite.setUp()
    try:
        suite.test_mace_no_lookahead_leakage()
    finally:
        suite.tearDown()


if __name__ == "__main__":
    unittest.main()


