"""
Comprehensive Audit Remediation & Live Parity Test Suite for Aegis Quant (v0.2.0-rc2).

Validates:
1. Migration 004 schema registration and system_config parameter seeding
2. Mandatory Fernet master key enforcement in LIVE/TESTNET execution modes
3. 36-bar MACE model caching & frozen normalization scaling lifecycle (backtest-live parity)
4. Degenerate Spearman Rank IC handling (returns None on < 5 samples or 0.0 std)
5. Backtest SHA-256 data provenance verification, synthetic warmup WARNING logging, and removal of artificial HLP yield floor
6. Asynchronous AlertRetryQueue exponential backoff and permanent failure logging
7. Production clean-up (no unittest.mock.AsyncMock import in engine/execution.py)
8. Two-Tier Chandelier Stop wick overshoot parameter grid sensitivity
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import engine.execution as execution_mod
from engine.alerts import AlertEngine, AlertRetryQueue
from engine.backtest import BacktestEngine, HistoricalBacktester, verify_data_provenance
from engine.database import DatabaseManager
from engine.risk import RiskEngine
from engine.signals import SignalEngine


def _make_ohlcv_df(
    n_bars: int = 240,
    seed: int = 42,
    drift: float = 0.0012,
    base_price: float = 120.0,
    symbol: str = "SOL/USDC:USDC",
) -> pd.DataFrame:
    """Create a deterministic OHLCV DataFrame for MACE and risk testing."""
    rng = np.random.default_rng(seed)
    rets = drift + rng.normal(0.0, 0.011, size=n_bars)
    closes = base_price * np.exp(np.cumsum(rets))
    opens = np.roll(closes, 1)
    opens[0] = base_price
    spread = np.abs(rng.normal(0.004, 0.0015, size=n_bars))
    highs = np.maximum(opens, closes) * (1.0 + spread)
    lows = np.minimum(opens, closes) * (1.0 - spread)
    volumes = rng.uniform(1200.0, 6000.0, size=n_bars)
    ts_base = 1750000000000
    timestamps = np.arange(n_bars, dtype=np.int64) * 14_400_000 + ts_base

    return pd.DataFrame(
        {
            "symbol": [symbol] * n_bars,
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


class TestAuditRemediationRC2(unittest.TestCase):
    """Test suite validating all Rollout Commit Point 4 (v0.2.0-rc2) audit remediations."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp_dir.name)
        self._saved_master_key = os.environ.get("AEGIS_MASTER_KEY")
        self._saved_cred_dir = os.environ.get("CREDENTIALS_DIRECTORY")

    def tearDown(self) -> None:
        if self._saved_master_key is not None:
            os.environ["AEGIS_MASTER_KEY"] = self._saved_master_key
        else:
            os.environ.pop("AEGIS_MASTER_KEY", None)

        if self._saved_cred_dir is not None:
            os.environ["CREDENTIALS_DIRECTORY"] = self._saved_cred_dir
        else:
            os.environ.pop("CREDENTIALS_DIRECTORY", None)

        self.tmp_dir.cleanup()

    def test_migration_004_schema_version_and_config_seeds(self) -> None:
        """Verifies Migration 004 registers schema version 4 and seeds all 5 remediation keys."""
        db_path = self.tmp_path / "test_mig004.db"
        db = DatabaseManager(db_path=db_path)
        db.initialize_database()

        with db.connection() as conn:
            row = conn.execute(
                "SELECT version, description FROM schema_migrations WHERE version = 4;"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(int(row["version"]), 4)
            self.assertIn("Migration 004", str(row["description"]))

        cfg = db.load_config()
        self.assertEqual(int(cfg["mace_refit_interval_bars"]), 36)
        self.assertTrue(bool(cfg["enforce_master_key"]))
        self.assertEqual(int(cfg["alert_max_retries"]), 5)
        self.assertAlmostEqual(float(cfg["alert_backoff_base_sec"]), 2.0, places=4)
        self.assertFalse(bool(cfg["enable_hlp_floor_backtest"]))

    def test_master_key_enforcement(self) -> None:
        """Verifies RuntimeError is raised in LIVE/TESTNET modes without explicit master key."""
        db_path = str(self.tmp_path / "test.db")
        db = DatabaseManager(db_path=db_path)
        db.initialize_database()

        os.environ.pop("AEGIS_MASTER_KEY", None)
        os.environ.pop("CREDENTIALS_DIRECTORY", None)

        # PAPER mode allows fallback key
        paper_key = db.get_master_fernet_key()
        self.assertEqual(len(paper_key), 44)

        # LIVE mode without explicit key raises RuntimeError and logs CRITICAL telemetry
        db.set_config_value("execution_mode", "LIVE")
        with self.assertRaises(RuntimeError) as ctx:
            db.get_master_fernet_key()
        self.assertIn("FATAL SECURITY VIOLATION", str(ctx.exception))

        # Direct encryption in LIVE mode without explicit key also enforces get_master_fernet_key()
        with self.assertRaises(RuntimeError) as ctx_enc:
            db.encrypt_config_value("super-secret-token")
        self.assertIn("FATAL SECURITY VIOLATION", str(ctx_enc.exception))

        # TESTNET mode without explicit key also raises RuntimeError
        db.set_config_value("execution_mode", "TESTNET")
        with self.assertRaises(RuntimeError) as ctx_tn:
            db.get_master_fernet_key()
        self.assertIn("FATAL SECURITY VIOLATION", str(ctx_tn.exception))

        with db.connection() as conn:
            crit_rows = conn.execute(
                "SELECT level, component, message FROM telemetry_logs WHERE level = 'CRITICAL';"
            ).fetchall()
            self.assertGreaterEqual(len(crit_rows), 1)
            self.assertIn("FATAL SECURITY VIOLATION", str(crit_rows[0]["message"]))

        # Providing explicit AEGIS_MASTER_KEY succeeds in LIVE mode
        explicit_key = base64.urlsafe_b64encode(hashlib.sha256(b"prod-explicit-secret").digest()).decode("ascii")
        os.environ["AEGIS_MASTER_KEY"] = explicit_key
        db.set_config_value("execution_mode", "LIVE")
        resolved_key = db.get_master_fernet_key()
        self.assertEqual(resolved_key, explicit_key.encode("utf-8"))

        # Systemd CREDENTIALS_DIRECTORY fallback also succeeds in LIVE mode
        os.environ.pop("AEGIS_MASTER_KEY", None)
        cred_dir = self.tmp_path / "systemd_creds"
        cred_dir.mkdir(parents=True, exist_ok=True)
        (cred_dir / "aegis_master_key").write_text(explicit_key + "\n", encoding="utf-8")
        os.environ["CREDENTIALS_DIRECTORY"] = str(cred_dir)
        self.assertEqual(db.get_master_fernet_key(), explicit_key.encode("utf-8"))

    def test_mace_cache_lifecycle(self) -> None:
        """
        Verifies MACE model is cached on initial evaluation, reused across intermediate
        15s ticks and bars 1..35 with frozen X_center/X_std scaling, and refit strictly
        at bar +36 (achieving exact backtest-live parity).
        """
        db_path = str(self.tmp_path / "test_mace.db")
        db = DatabaseManager(db_path=db_path)
        db.initialize_database()
        cfg = db.load_config()
        engine = SignalEngine(db)

        self.assertEqual(len(engine._mace_cache), 0)

        full_exec = _make_ohlcv_df(n_bars=250, seed=77, drift=0.0014, base_price=140.0, symbol="SOL/USDC:USDC")
        full_synth = _make_ohlcv_df(n_bars=250, seed=88, drift=0.0009, base_price=0.0022, symbol="SOL/BTC:SYNTH")

        base_idx = 180
        df_t0 = full_exec.iloc[:base_idx].copy()
        synth_t0 = full_synth.iloc[:base_idx].copy()

        # Prepare BacktestEngine PIT table on the same series to verify exact numerical parity
        bt = BacktestEngine(source_db_path=db_path, initial_equity=10000.0)
        pair_list = [{"exec_symbol": "SOL/USDC:USDC", "synth_symbol": "SOL/BTC:SYNTH"}]
        ptab = bt._precompute_causal_pair_tables(
            pair_list,
            {"SOL/USDC:USDC": full_exec},
            {"SOL/BTC:SYNTH": full_synth},
            cfg,
        )["SOL/USDC:USDC"]

        # 1. First evaluation at bar t0 populates _mace_cache and matches BacktestEngine
        score_t0 = engine.compute_mace_nonlinear_score(
            df_t0, synth_t0, cfg, symbol="SOL/USDC:USDC"
        )
        _, bt_mace_t0, _ = bt._evaluate_pair_signal_pit(
            exec_sym="SOL/USDC:USDC",
            pair_table=ptab,
            pos_idx=base_idx - 1,
            step_idx=0,
            cfg=cfg,
        )
        self.assertAlmostEqual(score_t0, bt_mace_t0, places=6)
        self.assertIn("SOL/USDC:USDC", engine._mace_cache)
        cache_t0 = engine._mace_cache["SOL/USDC:USDC"]
        rf_t0 = cache_t0["rf"]
        ridge_t0 = cache_t0["ridge"]
        center_t0 = cache_t0["center"].copy()
        std_t0 = cache_t0["std"].copy()
        ts_t0 = int(cache_t0["ts"])
        self.assertEqual(ts_t0, int(df_t0["timestamp_utc"].iloc[-1]))

        # 2. Second evaluation at +15s intermediate tick (same 4H bar timestamp, updated close)
        df_t0_tick = df_t0.copy()
        df_t0_tick.iloc[-1, df_t0_tick.columns.get_loc("close")] *= 1.015
        score_t0_tick = engine.compute_mace_nonlinear_score(
            df_t0_tick, synth_t0, cfg, symbol="SOL/USDC:USDC"
        )
        cache_tick = engine._mace_cache["SOL/USDC:USDC"]
        self.assertIs(cache_tick["rf"], rf_t0)
        self.assertIs(cache_tick["ridge"], ridge_t0)
        self.assertEqual(int(cache_tick["ts"]), ts_t0)
        self.assertTrue(np.array_equal(cache_tick["center"], center_t0))
        self.assertTrue(np.array_equal(cache_tick["std"], std_t0))
        self.assertNotAlmostEqual(score_t0, score_t0_tick, places=6)

        # 3. Evaluations at bar +1, +18, and +35 reuse cached model (bars_since_refit < 36) and match BacktestEngine
        for delta_bars in (1, 18, 35):
            df_step = full_exec.iloc[: base_idx + delta_bars].copy()
            synth_step = full_synth.iloc[: base_idx + delta_bars].copy()
            live_score = engine.compute_mace_nonlinear_score(
                df_step, synth_step, cfg, symbol="SOL/USDC:USDC"
            )
            _, bt_score, _ = bt._evaluate_pair_signal_pit(
                exec_sym="SOL/USDC:USDC",
                pair_table=ptab,
                pos_idx=base_idx + delta_bars - 1,
                step_idx=delta_bars,
                cfg=cfg,
            )
            self.assertAlmostEqual(live_score, bt_score, places=6)
            cache_step = engine._mace_cache["SOL/USDC:USDC"]
            self.assertIs(
                cache_step["rf"],
                rf_t0,
                msg=f"Expected cached RF model to be reused at +{delta_bars} bars (< 36)",
            )
            self.assertEqual(int(cache_step["ts"]), ts_t0)

        # 4. Evaluation at bar +36 triggers model refit (bars_since_refit == 36) and matches BacktestEngine
        df_t36 = full_exec.iloc[: base_idx + 36].copy()
        synth_t36 = full_synth.iloc[: base_idx + 36].copy()
        live_score_36 = engine.compute_mace_nonlinear_score(
            df_t36, synth_t36, cfg, symbol="SOL/USDC:USDC"
        )
        _, bt_score_36, _ = bt._evaluate_pair_signal_pit(
            exec_sym="SOL/USDC:USDC",
            pair_table=ptab,
            pos_idx=base_idx + 36 - 1,
            step_idx=36,
            cfg=cfg,
        )
        self.assertAlmostEqual(live_score_36, bt_score_36, places=6)
        cache_t36 = engine._mace_cache["SOL/USDC:USDC"]
        self.assertIsNot(
            cache_t36["rf"],
            rf_t0,
            msg="Expected MACE RF model to refit at +36 bars",
        )
        self.assertEqual(int(cache_t36["ts"]), int(df_t36["timestamp_utc"].iloc[-1]))

        # 5. Dynamic runtime shortening of mace_refit_interval_bars (e.g., to 12) triggers refit at +48 bars
        rf_t36 = cache_t36["rf"]
        cfg_short = {**cfg, "mace_refit_interval_bars": 12}
        df_t48 = full_exec.iloc[: base_idx + 48].copy()
        synth_t48 = full_synth.iloc[: base_idx + 48].copy()
        _ = engine.compute_mace_nonlinear_score(
            df_t48, synth_t48, cfg_short, symbol="SOL/USDC:USDC"
        )
        self.assertIsNot(engine._mace_cache["SOL/USDC:USDC"]["rf"], rf_t36)
        bt.mem_db.close()

    def test_degenerate_spearman_ic_handling(self) -> None:
        """Verifies _safe_spearman_ic returns None on < 5 samples, 0.0 std, or NaN inputs."""
        # Fewer than 5 samples
        self.assertIsNone(
            SignalEngine._safe_spearman_ic(np.array([0.1, 0.2, 0.3]), np.array([0.1, 0.2, 0.3]))
        )
        # Constant series (std == 0.0)
        self.assertIsNone(
            SignalEngine._safe_spearman_ic(np.ones(25), np.linspace(0.0, 1.0, 25))
        )
        self.assertIsNone(
            SignalEngine._safe_spearman_ic(np.linspace(0.0, 1.0, 25), np.zeros(25))
        )
        # DataFrame + Series degenerate target
        df_feats = pd.DataFrame({"f1": np.linspace(0.0, 1.0, 20), "f2": np.linspace(1.0, 2.0, 20)})
        self.assertIsNone(
            SignalEngine._safe_spearman_ic(df_feats, pd.Series(np.zeros(20)))
        )
        self.assertIsNone(
            SignalEngine._safe_spearman_ic(df_feats, pd.Series(np.full(20, np.nan)))
        )

        # Valid correlated DataFrame + Series returns finite float
        ic_valid = SignalEngine._safe_spearman_ic(df_feats, pd.Series(np.linspace(0.0, 1.0, 20)))
        self.assertIsNotNone(ic_valid)
        assert ic_valid is not None
        self.assertAlmostEqual(ic_valid, 1.0, places=5)

        # Dynamic weight safely falls back when IC is None
        w_fallback = SignalEngine.compute_dynamic_mace_weight(None, None, min_ic_floor=0.05)
        self.assertAlmostEqual(w_fallback, 0.50, places=5)

    def test_backtest_provenance_and_hlp_floor_removal(self) -> None:
        """
        Verifies SHA-256 backfill provenance check, synthetic warmup WARNING logging,
        and removal of the artificial HLP yield floor in BacktestEngine.
        """
        # 1. SHA-256 provenance verification
        sample_json = self.tmp_path / "sample_backfill.json"
        sample_hash = self.tmp_path / "sample_backfill.json.sha256"
        sample_json.write_text('{"coins": {}}', encoding="utf-8")
        digest = hashlib.sha256(sample_json.read_bytes()).hexdigest()
        sample_hash.write_text(f"{digest}  sample_backfill.json\n", encoding="utf-8")

        self.assertTrue(verify_data_provenance(str(sample_json)))

        # Corrupt checksum -> raises ValueError
        sample_hash.write_text("0" * 64 + "  sample_backfill.json\n", encoding="utf-8")
        with self.assertRaises(ValueError) as ctx:
            verify_data_provenance(str(sample_json))
        self.assertIn("BACKFILL DATA CORRUPTION", str(ctx.exception))

        # Missing backfill file -> returns False
        self.assertFalse(verify_data_provenance(str(self.tmp_path / "nonexistent.json")))

        # 2. Verify HLP yield floor removal & synthetic warmup WARNING logging
        bt_db_path = self.tmp_path / "bt_hlp_test.db"
        bt_db = DatabaseManager(bt_db_path)
        bt_db.initialize_database()
        # Set funding_rate_hourly = 0.0 across all universe assets by leaving market_ohlcv empty
        # and overriding basis_min_hourly_funding_rate = 0.0005 (well above 0.00001)
        bt_db.set_config_value("basis_min_hourly_funding_rate", 0.0005)
        bt_db.set_config_value("enable_hlp_floor_backtest", False)

        bt_no_floor = BacktestEngine(source_db_path=bt_db_path, initial_equity=10000.0)
        with self.assertLogs("aegis.backtest", level="WARNING") as cm:
            ts_no_floor = bt_no_floor.run_backtest(start_date="2026-10-01", end_date="2026-10-05")
        self.assertTrue(any("synthetic" in msg.lower() for msg in cm.output))
        self.assertEqual(ts_no_floor.basis_yield_usdc, 0.0)
        bt_no_floor.mem_db.close()

        # When enable_hlp_floor_backtest is explicitly True, the artificial floor accrues yield
        bt_with_floor = BacktestEngine(
            source_db_path=bt_db_path,
            initial_equity=10000.0,
            config_overrides={"enable_hlp_floor_backtest": True},
        )
        ts_with_floor = bt_with_floor.run_backtest(start_date="2026-10-01", end_date="2026-10-05")
        self.assertGreater(ts_with_floor.basis_yield_usdc, 0.0)
        bt_with_floor.mem_db.close()

    def test_alert_retry_queue_exponential_backoff(self) -> None:
        """Verifies AlertRetryQueue retries failed dispatches with exponential backoff and logs permanent failure."""
        db_path = self.tmp_path / "test_alerts.db"
        db = DatabaseManager(db_path=db_path)
        db.initialize_database()
        db.set_config_value("alert_max_retries", 3)
        db.set_config_value("alert_backoff_base_sec", 0.01)

        alert_engine = AlertEngine(db)
        self.assertIsInstance(alert_engine.retry_queue, AlertRetryQueue)

        async def _run_retry_scenario() -> tuple[int, list[float], int]:
            attempts = 0
            sleep_delays: list[float] = []

            async def _mock_dispatch(item: dict[str, object]) -> bool:
                nonlocal attempts
                attempts += 1
                if item.get("title") == "Permanent Fail":
                    return False
                return attempts >= 3

            orig_sleep = asyncio.sleep

            async def _fast_sleep(delay: float) -> None:
                sleep_delays.append(delay)
                await orig_sleep(0.001)

            with (
                patch.object(alert_engine.retry_queue, "_dispatch_http", side_effect=_mock_dispatch),
                patch("engine.alerts.asyncio.sleep", side_effect=_fast_sleep),
            ):
                alert_engine.retry_queue.start()
                # 1. Transient failure recovers on attempt 3
                await alert_engine.retry_queue.enqueue_alert("Test Alert", "Transient failure", "WARNING")
                await asyncio.wait_for(alert_engine.retry_queue.queue.join(), timeout=2.0)

                # 2. Permanent failure exhausts all retries and increments permanent_failure_count
                db.set_config_value("alert_max_retries", 2)
                await alert_engine.retry_queue.enqueue_alert("Permanent Fail", "Always 500", "CRITICAL")
                await asyncio.wait_for(alert_engine.retry_queue.queue.join(), timeout=2.0)
                await alert_engine.retry_queue.stop()

            return attempts, sleep_delays, alert_engine.retry_queue.permanent_failure_count

        with self.assertLogs("aegis.alerts", level="ERROR") as err_logs:
            total_attempts, delays, perm_fails = asyncio.run(_run_retry_scenario())
        self.assertEqual(total_attempts, 6)  # 3 for transient + (1 initial + 2 retries) for permanent
        self.assertEqual(len(delays), 4)
        self.assertAlmostEqual(delays[0], 0.01, places=4)
        self.assertAlmostEqual(delays[1], 0.02, places=4)
        self.assertEqual(perm_fails, 1)
        self.assertTrue(any("PERMANENT ALERT FAILURE" in m for m in err_logs.output))

    def test_execution_no_async_mock_import(self) -> None:
        """Verifies unittest.mock.AsyncMock is not imported in engine/execution.py."""
        source = inspect.getsource(execution_mod)
        self.assertNotIn("from unittest.mock import AsyncMock", source)
        self.assertFalse(hasattr(execution_mod, "AsyncMock"))

    def test_two_tier_wick_stop_grid_sensitivity(self) -> None:
        """
        Executes grid search over wick overshoot parameters (0.5-1.5 ATR, 5-25% fraction)
        verifying bounded slippage scaling and strategy profitability stability.
        """
        db_path = self.tmp_path / "test_wick_grid.db"
        db = DatabaseManager(db_path=db_path)
        db.initialize_database()
        risk = RiskEngine(db)

        atr_mults = (0.5, 1.0, 1.5)
        fractions = (0.05, 0.15, 0.25)

        slippage_grid: dict[tuple[float, float], float] = {}
        for mult in atr_mults:
            for frac in fractions:
                cfg = {
                    "chandelier_atr_multiplier_long": 3.0,
                    "chandelier_min_atr_pct": 0.022,
                    "entry_exit_slippage_bps": 3.0,
                    "wick_stop_base_slippage_bps": 8.0,
                    "wick_overshoot_atr_mult": mult,
                    "wick_overshoot_fraction": frac,
                }
                res = risk.evaluate_two_tier_chandelier_stop(
                    side="LONG",
                    entry_price=100.0,
                    active_stop=94.0,
                    highest_price_seen=105.0,
                    lowest_price_seen=98.0,
                    bar_open=97.0,
                    bar_high=98.0,
                    bar_low=89.0,  # 5.0 overshoot below 94.0 active stop
                    bar_close=95.0,
                    atr_value=2.0,
                    quantity=10.0,
                    config=cfg,
                )
                self.assertTrue(res.triggered)
                self.assertTrue(res.is_wick_stop)
                self.assertEqual(res.exit_reason, "INTRABAR_LOW_WICK_CHANDELIER_STOP_LONG")
                self.assertGreater(res.exit_slippage_usdc, 0.0)
                slippage_grid[(mult, frac)] = res.exit_slippage_usdc

        # Higher overshoot fraction or higher ATR multiplier cap must monotonically increase or maintain slippage
        for mult in atr_mults:
            self.assertLessEqual(slippage_grid[(mult, 0.05)], slippage_grid[(mult, 0.15)])
            self.assertLessEqual(slippage_grid[(mult, 0.15)], slippage_grid[(mult, 0.25)])

        # Verify historical backtest profitability stability across extremes of the parameter grid
        for mult, frac in ((0.5, 0.05), (1.0, 0.15), (1.5, 0.25)):
            bt = HistoricalBacktester(
                initial_equity=10000.0,
                config_overrides={
                    "wick_overshoot_atr_mult": mult,
                    "wick_overshoot_fraction": frac,
                },
            )
            try:
                ts = bt.run(start_date="2026-07-20", end_date="2026-10-20")
                self.assertGreater(ts.sharpe, 1.20)
                self.assertLess(ts.max_drawdown, 0.22)
            finally:
                bt.mem_db.close()

    def test_check_mainnet_readiness_script(self) -> None:
        """Verifies scripts/check_mainnet_readiness.py evaluates all 5 readiness gates."""
        from scripts.check_mainnet_readiness import evaluate_readiness

        test_db_path = self.tmp_path / "readiness_test.db"
        db = DatabaseManager(test_db_path)
        db.initialize_database()

        checks, summary = evaluate_readiness(
            test_db_path,
            min_soak_hours=1.0,
            min_closed_trades=1,
            error_window_hours=24.0,
        )
        self.assertGreaterEqual(len(checks), 11)
        self.assertFalse(bool(summary["mainnet_ready"]))
        check_ids = {c.check_id for c in checks}
        self.assertTrue({"1.1", "1.2", "2.1", "2.2", "3.1", "4.2", "5.1"}.issubset(check_ids))


if __name__ == "__main__":
    unittest.main()
