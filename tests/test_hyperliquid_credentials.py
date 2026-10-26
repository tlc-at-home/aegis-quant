"""Unit and integration tests for Hyperliquid Mainnet/Testnet credentials and execution routing."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from engine.database import (
    PROJECT_ROOT,
    DatabaseManager,
    validate_evm_address,
    validate_private_key,
)
from engine.execution import ExecutionEngine
from engine.ingestion import MarketIngestionEngine
from engine.risk import RiskEngine
from engine.signals import RegimeState
from web.server import create_app

SAMPLE_WALLET = "0x1111111111111111111111111111111111111111"
SAMPLE_VAULT = "0x3333333333333333333333333333333333333333"
SAMPLE_PRIVKEY = "0x2222222222222222222222222222222222222222222222222222222222222222"


class TestHyperliquidCredentials(unittest.TestCase):
    """Verify Platform_Specs.md removal, credential guardrails, CCXT EIP-712 signing, and exit routing."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_aegis_quant.db"
        self.db = DatabaseManager(db_path=self.db_path)
        self.integrity = self.db.init_db()

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_01_platform_specs_removed_and_no_env_config_files(self) -> None:
        import engine.database as db_mod

        specs_path = PROJECT_ROOT / "Platform_Specs.md"
        self.assertFalse(specs_path.exists(), "Platform_Specs.md must be removed from workspace")
        self.assertFalse((PROJECT_ROOT / ".env").exists(), ".env must not exist in workspace")
        self.assertFalse(
            (PROJECT_ROOT / ".env.example").exists(),
            ".env.example must not exist; all config & credentials live in SQLite system_config",
        )
        self.assertFalse(
            (PROJECT_ROOT / "deploy" / "configure_credentials.sh").exists(),
            "External credential shell scripts must not exist; config lives in SQLite system_config",
        )
        self.assertFalse(hasattr(self.db, "sync_env_credentials"))
        self.assertFalse(hasattr(db_mod, "_parse_dotenv_file"))
        self.assertFalse(hasattr(db_mod, "DEFAULT_ENV_PATH"))
        with patch.dict(
            "os.environ",
            {
                "HYPERLIQUID_WALLET_ADDRESS": SAMPLE_WALLET,
                "AEGIS_WALLET_ADDRESS": SAMPLE_WALLET,
                "HYPERLIQUID_PRIVATE_KEY": SAMPLE_PRIVKEY,
                "AEGIS_EXECUTION_MODE": "LIVE",
            },
        ):
            self.db.init_db()
            cfg = self.db.load_config()
            self.assertEqual(cfg["wallet_address"], "")
            self.assertEqual(cfg["private_key"], "")
            self.assertEqual(cfg["execution_mode"], "PAPER")
        gitignore_text = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn("Platform_Specs.md", gitignore_text)

    def test_02_database_integrity_counts(self) -> None:
        self.assertEqual(self.integrity["journal_mode"], "wal")
        self.assertEqual(self.integrity["system_config_count"], 60)
        self.assertEqual(self.integrity["v_universe_usdc_perp_count"], 16)
        self.assertEqual(self.integrity["v_universe_btc_synth_count"], 15)
        self.assertEqual(self.integrity["v_universe_alt_pairs_count"], 15)
        self.assertEqual(self.integrity["universe_assets_total"], 31)

    def test_03_evm_address_and_secp256k1_private_key_validation(self) -> None:
        self.assertEqual(validate_evm_address(""), "")
        self.assertEqual(
            validate_evm_address("0XAbCdEf0123456789AbCdEf0123456789AbCdEf01"),
            "0xAbCdEf0123456789AbCdEf0123456789AbCdEf01",
        )
        with self.assertRaises(ValueError):
            validate_evm_address("0x1234")
        with self.assertRaises(ValueError):
            validate_evm_address("not-an-address")

        self.assertEqual(validate_private_key(""), "")
        self.assertEqual(validate_private_key(SAMPLE_PRIVKEY[2:]), SAMPLE_PRIVKEY)
        # Reject all-zero key
        with self.assertRaises(ValueError):
            validate_private_key("0x" + "0" * 64)
        # Reject key >= secp256k1 curve order n
        with self.assertRaises(ValueError):
            validate_private_key("0x" + "f" * 64)
        with self.assertRaises(ValueError):
            validate_private_key(
                "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141"
            )

    def test_04_credential_guardrails_and_set_config_value_consistency(self) -> None:
        # Cannot set LIVE or TESTNET without wallet_address and private_key
        with self.assertRaises(ValueError):
            self.db.configure_hyperliquid_credentials(execution_mode="LIVE")
        with self.assertRaises(ValueError):
            self.db.set_config_value("execution_mode", "LIVE")
        with self.assertRaises(ValueError):
            self.db.set_config_value("execution_mode", "TESTNET")

        # Cannot combine execution_mode='TESTNET' with hyperliquid_testnet=False
        with self.assertRaises(ValueError):
            self.db.configure_hyperliquid_credentials(
                wallet_address=SAMPLE_WALLET,
                private_key=SAMPLE_PRIVKEY,
                execution_mode="TESTNET",
                hyperliquid_testnet=False,
            )

        # Configure valid Testnet credentials via set_config_value / configure_hyperliquid_credentials
        self.db.set_config_value("wallet_address", SAMPLE_WALLET)
        self.db.set_config_value("private_key", SAMPLE_PRIVKEY)
        self.db.set_config_value("execution_mode", "TESTNET")

        status = self.db.get_credentials_status()
        self.assertEqual(status["execution_mode"], "LIVE")
        self.assertTrue(status["hyperliquid_testnet"])
        self.assertEqual(status["active_routing"], "LIVE_TESTNET")

        # Cannot clear wallet_address or private_key while in LIVE mode
        with self.assertRaises(ValueError):
            self.db.set_config_value("wallet_address", "")
        with self.assertRaises(ValueError):
            self.db.set_config_value("private_key", "")

        # Switching hyperliquid_testnet to False after TESTNET mode must switch to LIVE_MAINNET
        self.db.set_config_value("hyperliquid_testnet", False)
        status_mainnet = self.db.get_credentials_status()
        self.assertFalse(status_mainnet["hyperliquid_testnet"])
        self.assertEqual(status_mainnet["network"], "mainnet")
        self.assertEqual(status_mainnet["api_endpoint"], "https://api.hyperliquid.xyz")
        self.assertEqual(status_mainnet["active_routing"], "LIVE_MAINNET")

    def test_05_sqlite_exclusive_credentials_and_verify_dex_eip712(self) -> None:
        configured = self.db.configure_hyperliquid_credentials(
            wallet_address=SAMPLE_WALLET,
            private_key=SAMPLE_PRIVKEY,
            hyperliquid_testnet=False,
            execution_mode="LIVE",
            hyperliquid_vault_address=SAMPLE_VAULT,
            gemini_api_key="AIzaSyDummyKey1234567890",
        )
        self.assertEqual(configured["active_routing"], "LIVE_MAINNET")
        self.assertEqual(configured["hyperliquid_vault_address"], SAMPLE_VAULT)
        self.assertTrue(configured["gemini_api_key_configured"])

        # Re-initializing SQLite schema/seed must preserve existing system_config credentials
        reinit_integrity = self.db.init_db()
        self.assertEqual(reinit_integrity["system_config_count"], 60)
        persisted = self.db.get_credentials_status()
        self.assertEqual(persisted["wallet_address"], SAMPLE_WALLET)
        self.assertEqual(persisted["active_routing"], "LIVE_MAINNET")

        exec_engine = ExecutionEngine(self.db, RiskEngine(self.db))
        verified_main = asyncio.run(exec_engine.verify_dex_credentials())
        self.assertFalse(verified_main["ccxt_sandbox_mode"])
        self.assertEqual(verified_main["ccxt_api_endpoint"], "https://api.hyperliquid.xyz")
        self.assertTrue(verified_main["ccxt_credentials_bound"])
        self.assertTrue(verified_main["eip712_signing_verified"])

        # Passing an explicit config override to verify_dex_credentials must reflect that override
        override_cfg = self.db.load_config()
        override_cfg["hyperliquid_testnet"] = True
        verified_test = asyncio.run(exec_engine.verify_dex_credentials(override_cfg))
        self.assertTrue(verified_test["hyperliquid_testnet"])
        self.assertTrue(verified_test["ccxt_sandbox_mode"])
        self.assertEqual(
            verified_test["ccxt_api_endpoint"], "https://api.hyperliquid-testnet.xyz"
        )

    def test_06_ingestion_dynamic_exchange_reload(self) -> None:
        ing = MarketIngestionEngine(self.db)

        async def _check() -> None:
            cfg1 = {
                "wallet_address": SAMPLE_WALLET,
                "private_key": SAMPLE_PRIVKEY,
                "hyperliquid_testnet": True,
                "execution_mode": "LIVE",
            }
            ex1 = await ing._get_exchange(cfg1)
            ex1_again = await ing._get_exchange(cfg1)
            self.assertIs(ex1, ex1_again)

            cfg2 = {
                "wallet_address": SAMPLE_WALLET,
                "private_key": SAMPLE_PRIVKEY,
                "hyperliquid_testnet": "false",
                "execution_mode": "LIVE",
            }
            ex2 = await ing._get_exchange(cfg2)
            self.assertIsNot(ex1, ex2)
            await ing.close()

        asyncio.run(_check())

    def test_07_live_order_entry_and_reduce_only_exit_routing(self) -> None:
        self.db.configure_hyperliquid_credentials(
            wallet_address=SAMPLE_WALLET,
            private_key=SAMPLE_PRIVKEY,
            hyperliquid_testnet=True,
            execution_mode="LIVE",
        )
        ing = MarketIngestionEngine(self.db)
        for sym in ("BTC/USDC:USDC", "SOL/USDC:USDC"):
            bars = ing._generate_synthetic_usdc_series(sym, "1h", limit=120)
            self.db.upsert_ohlcv_batch(bars)

        exec_engine = ExecutionEngine(self.db, RiskEngine(self.db))
        regime_bull = RegimeState("BULL", 0.85, 0.05, 0.10, 540)
        evaluations = [
            {
                "timestamp_ms": 1700000000000,
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

        with patch.object(
            exec_engine, "_route_dex_order", new_callable=AsyncMock
        ) as mock_route:
            mock_route.return_value = "hl-testnet-entry123"
            stats_open = asyncio.run(exec_engine.reconcile_cycle(regime_bull, evaluations))
            self.assertEqual(stats_open["opened_trades"], 1)
            mock_route.assert_awaited_once()
            _, open_kwargs = mock_route.call_args
            self.assertEqual(open_kwargs["side"], "LONG")

            # Flip regime to BEAR to trigger exit; verify reduce_only=True closing order is routed
            mock_route.reset_mock()
            mock_route.return_value = "hl-testnet-exit456"
            regime_bear = RegimeState("BEAR", 0.05, 0.85, 0.10, 540)
            stats_close = asyncio.run(exec_engine.reconcile_cycle(regime_bear, []))
            self.assertEqual(stats_close["closed_trades"], 1)
            mock_route.assert_awaited_once()
            _, close_kwargs = mock_route.call_args
            self.assertEqual(close_kwargs["side"], "SHORT")
            self.assertTrue(close_kwargs["reduce_only"])

    def test_08_fastapi_credentials_and_config_endpoints(self) -> None:
        app = create_app(self.db)
        client = TestClient(app)

        res_get = client.get("/api/v1/credentials")
        self.assertEqual(res_get.status_code, 200)
        self.assertEqual(res_get.json()["active_routing"], "PAPER_SIMULATION")

        # Reject invalid wallet via POST /api/v1/credentials
        res_bad = client.post(
            "/api/v1/credentials", json={"wallet_address": "0xinvalid"}
        )
        self.assertEqual(res_bad.status_code, 400)

        # Reject enabling LIVE without credentials via POST /api/v1/config
        res_bad_cfg = client.post(
            "/api/v1/config",
            json={"config_key": "execution_mode", "config_value": "LIVE"},
        )
        self.assertEqual(res_bad_cfg.status_code, 400)

        # Reject type-invalid updates via POST /api/v1/config so system_config cannot be poisoned
        for bad_payload in (
            {"config_key": "initial_equity_usdc", "config_value": "not-a-float"},
            {"config_key": "initial_equity_usdc", "config_value": True},
            {"config_key": "max_concurrent_long_positions", "config_value": "abc"},
            {"config_key": "max_concurrent_long_positions", "config_value": False},
            {"config_key": "basis_harvest_enabled", "config_value": "not-a-bool"},
            {"config_key": "wallet_address", "config_value": 0},
            {"config_key": "private_key", "config_value": False},
        ):
            res_invalid_type = client.post("/api/v1/config", json=bad_payload)
            self.assertEqual(
                res_invalid_type.status_code,
                400,
                f"Expected 400 for invalid payload {bad_payload}",
            )

        # Confirm load_config still succeeds cleanly after rejected updates
        cfg_after = self.db.load_config()
        self.assertEqual(cfg_after["initial_equity_usdc"], 10000.0)
        self.assertEqual(cfg_after["max_concurrent_long_positions"], 2)

        # Valid Testnet credential update via POST /api/v1/credentials
        res_ok = client.post(
            "/api/v1/credentials",
            json={
                "wallet_address": SAMPLE_WALLET,
                "private_key": SAMPLE_PRIVKEY,
                "hyperliquid_testnet": True,
                "execution_mode": "TESTNET",
            },
        )
        self.assertEqual(res_ok.status_code, 200)
        creds = res_ok.json()["credentials"]
        self.assertEqual(creds["active_routing"], "LIVE_TESTNET")
        self.assertTrue(creds["credentials_ready"])

    def test_09_concurrent_sqlite_wal_config_updates_and_polling(self) -> None:
        import threading

        app = create_app(self.db)
        client = TestClient(app)
        ing = MarketIngestionEngine(self.db)
        sample_bars = ing._generate_synthetic_usdc_series("BTC/USDC:USDC", "1h", limit=60)

        errors: list[Exception] = []
        stop_event = threading.Event()

        def _background_engine_poller() -> None:
            while not stop_event.is_set():
                try:
                    cfg = self.db.load_config()
                    status = self.db.get_credentials_status(cfg)
                    self.assertIn(
                        status["active_routing"],
                        ("PAPER_SIMULATION", "LIVE_TESTNET", "LIVE_MAINNET"),
                    )
                    self.db.upsert_ohlcv_batch(sample_bars[:10])
                except (AssertionError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:  # pragma: no cover
                    errors.append(exc)
                    break

        poller_thread = threading.Thread(target=_background_engine_poller, daemon=True)
        poller_thread.start()
        try:
            for i in range(12):
                is_testnet = (i % 2) == 0
                res = client.post(
                    "/api/v1/credentials",
                    json={
                        "wallet_address": SAMPLE_WALLET,
                        "private_key": SAMPLE_PRIVKEY,
                        "hyperliquid_testnet": is_testnet,
                        "execution_mode": "LIVE",
                        "hyperliquid_vault_address": SAMPLE_VAULT,
                        "gemini_api_key": f"AIzaSyConcurrentKey_{i}",
                    },
                )
                self.assertEqual(res.status_code, 200)
                res_cfg = client.post(
                    "/api/v1/config",
                    json={
                        "config_key": "polling_interval_seconds",
                        "config_value": 45 + (i % 15),
                    },
                )
                self.assertEqual(res_cfg.status_code, 200)
        finally:
            stop_event.set()
            poller_thread.join(timeout=5.0)

        self.assertEqual(errors, [])

    def test_10_polling_interval_15s_defaults_and_live_db(self) -> None:
        from engine.main import AegisOrchestrator

        cfg = self.db.load_config()
        self.assertEqual(cfg["polling_interval_seconds"], 15)

        live_db = DatabaseManager()
        if not live_db.db_path.exists():
            live_db.init_db()
        live_cfg = live_db.load_config()
        self.assertEqual(live_cfg["polling_interval_seconds"], 15)

        app = create_app(self.db)
        client = TestClient(app)
        status_res = client.get("/api/v1/status")
        self.assertEqual(status_res.status_code, 200)
        self.assertEqual(status_res.json()["polling_interval_seconds"], 15)

        orchestrator = AegisOrchestrator(db=self.db)
        summary = asyncio.run(orchestrator.run_single_cycle())
        self.assertEqual(summary["cycle"], 1)
        self.assertEqual(summary["ingestion"]["usdc_perp_symbols"], 16)
        self.assertEqual(summary["ingestion"]["btc_synth_symbols"], 15)
        asyncio.run(orchestrator.ingestion.close())

    def test_11_15s_polling_rate_limit_safe_intrabar_and_boundary_updates(self) -> None:
        from engine.ingestion import HYPERLIQUID_INFO_WEIGHT_LIMIT_PER_MIN

        self.db.configure_hyperliquid_credentials(
            wallet_address=SAMPLE_WALLET,
            private_key=SAMPLE_PRIVKEY,
            hyperliquid_testnet=True,
            execution_mode="LIVE",
        )
        ing = MarketIngestionEngine(self.db)
        usdc_symbols = [r["symbol"] for r in self.db.get_usdc_perp_universe()]

        # Align mock clock inside a 4H bar (e.g., 12:10:00 UTC) so 15s ticks are intra-bar
        base_4h_ms = 1700006400000  # divisible by 14,400,000 (4h) and 3,600,000 (1h)
        current_time_sec = (base_4h_ms // 1000) + 600  # +10 minutes into the 1h & 4h bar

        def _make_raw_candles(timeframe: str, end_aligned_ms: int, close_px: float) -> list[list[float]]:
            tf_ms = 3600000 if timeframe == "1h" else 14400000
            start_ms = end_aligned_ms - 599 * tf_ms
            return [
                [
                    float(start_ms + idx * tf_ms),
                    close_px * 0.99,
                    close_px * 1.01,
                    close_px * 0.98,
                    close_px,
                    1000.0,
                ]
                for idx in range(600)
            ]

        mock_ex = AsyncMock()
        lag_new_bar = {"enabled": False}

        async def _fake_fetch_ohlcv(symbol: str, timeframe: str = "1h", limit: int = 600) -> list[list[float]]:
            tf_ms = 3600000 if timeframe == "1h" else 14400000
            aligned_ms = (int(current_time_sec * 1000) // tf_ms) * tf_ms
            if lag_new_bar["enabled"]:
                aligned_ms -= tf_ms
            base_px = 70000.0 if symbol.startswith("BTC/") else 180.0
            return _make_raw_candles(timeframe, aligned_ms, base_px)

        tick_step = {"count": 0}

        async def _fake_fetch_funding_rates() -> dict[str, dict[str, object]]:
            tick_step["count"] += 1
            step = tick_step["count"]
            out: dict[str, dict[str, object]] = {}
            for sym in usdc_symbols:
                base_px = 70000.0 if sym.startswith("BTC/") else 180.0
                live_px = base_px + step * (50.0 if sym.startswith("BTC/") else 2.5)
                out[sym] = {
                    "symbol": sym,
                    "markPrice": live_px,
                    "fundingRate": 0.000025 + step * 0.000001,
                    "info": {
                        "midPx": str(live_px),
                        "markPx": str(live_px),
                        "funding": str(0.000025 + step * 0.000001),
                    },
                }
            return out

        mock_ex.fetch_ohlcv.side_effect = _fake_fetch_ohlcv
        mock_ex.fetch_funding_rates.side_effect = _fake_fetch_funding_rates

        async def _run_minute_simulation() -> None:
            nonlocal current_time_sec
            with patch.object(ing, "_get_exchange", new=AsyncMock(return_value=mock_ex)):
                # Cycle 1 (t=0s, Cold Start): fetches full 600-bar OHLCV for 32 (symbol, tf) pairs + 1 batch snapshot
                with patch("engine.ingestion.time.time", side_effect=lambda: float(current_time_sec)):
                    stats_c1 = await ing.ingest_universe()
                self.assertEqual(stats_c1["ohlcv_fetch_calls"], 32)
                self.assertEqual(stats_c1["batch_snapshot_calls"], 1)
                self.assertEqual(stats_c1["estimated_info_weight"], 660)

                # Cycles 2, 3, 4 (t=15s, 30s, 45s intra-bar ticks): 0 fetch_ohlcv calls, 1 batch snapshot each
                intra_weights = []
                for offset in (15, 30, 45):
                    current_time_sec = (base_4h_ms // 1000) + 600 + offset
                    with patch("engine.ingestion.time.time", side_effect=lambda: float(current_time_sec)):
                        stats_intra = await ing.ingest_universe()
                    self.assertEqual(stats_intra["ohlcv_fetch_calls"], 0)
                    self.assertEqual(stats_intra["batch_snapshot_calls"], 1)
                    self.assertEqual(stats_intra["estimated_info_weight"], 20)
                    self.assertEqual(stats_intra["usdc_bars_upserted"], 32)
                    intra_weights.append(stats_intra["estimated_info_weight"])

                # Cold-start minute total weight (1 cold start + 3 intra-bar 15s ticks) = 720 <= 1,200 limit
                cold_start_minute_weight = stats_c1["estimated_info_weight"] + sum(intra_weights)
                self.assertEqual(cold_start_minute_weight, 720)
                self.assertLessEqual(cold_start_minute_weight, HYPERLIQUID_INFO_WEIGHT_LIMIT_PER_MIN)

                # Steady-state minute total weight (4 intra-bar 15s ticks) = 80 <= 1,200 limit
                steady_state_minute_weight = 4 * intra_weights[-1]
                self.assertEqual(steady_state_minute_weight, 80)
                self.assertLessEqual(steady_state_minute_weight, HYPERLIQUID_INFO_WEIGHT_LIMIT_PER_MIN)

                # Verify intra-bar ticks updated active bar close, high, funding_rate_hourly, and ALT/BTC:SYNTH
                sol_df = self.db.get_ohlcv_df("SOL/USDC:USDC", "1h", limit=5)
                self.assertEqual(len(sol_df), 5)
                latest_sol = sol_df.iloc[-1]
                self.assertAlmostEqual(float(latest_sol["close"]), 180.0 + 4 * 2.5, places=4)
                self.assertAlmostEqual(float(latest_sol["high"]), 180.0 + 4 * 2.5, places=4)
                self.assertAlmostEqual(
                    float(latest_sol["funding_rate_hourly"]), 0.000025 + 4 * 0.000001, places=7
                )
                synth_df = self.db.get_ohlcv_df("SOL/BTC:SYNTH", "1h", limit=5)
                expected_synth_close = (180.0 + 4 * 2.5) / (70000.0 + 4 * 50.0)
                self.assertAlmostEqual(float(synth_df.iloc[-1]["close"]), expected_synth_close, places=8)

                # Advance clock across 1h bar boundary (+1 hour, still inside same 4h bar) with lagging candleSnapshot
                lag_new_bar["enabled"] = True
                current_time_sec = (base_4h_ms // 1000) + 3605
                with patch("engine.ingestion.time.time", side_effect=lambda: float(current_time_sec)):
                    stats_1h_roll = await ing.ingest_universe()
                # Only the 16 1h series fetch full OHLCV; the 16 4h series update intra-bar via batch snapshot
                self.assertEqual(stats_1h_roll["ohlcv_fetch_calls"], 16)
                self.assertEqual(stats_1h_roll["batch_snapshot_calls"], 1)
                self.assertEqual(stats_1h_roll["estimated_info_weight"], 340)

                # Next 15s tick (+15s after lagging 1h rollover) must open the new 1h bar without re-calling fetch_ohlcv
                # or mutating the closed previous 1h bar, and test fetch_tickers fallback when fetch_funding_rates is empty
                mock_ex.fetch_funding_rates.side_effect = None
                mock_ex.fetch_funding_rates.return_value = {}
                mock_ex.fetch_tickers.return_value = {
                    sym: {
                        "symbol": sym,
                        "close": 71000.0 if sym.startswith("BTC/") else 195.0,
                        "info": {"funding": "0.000033"},
                    }
                    for sym in usdc_symbols
                }
                current_time_sec = (base_4h_ms // 1000) + 3620
                with patch("engine.ingestion.time.time", side_effect=lambda: float(current_time_sec)):
                    stats_post_lag = await ing.ingest_universe()
                self.assertEqual(stats_post_lag["ohlcv_fetch_calls"], 0)
                sol_post_df = self.db.get_ohlcv_df("SOL/USDC:USDC", "1h", limit=3)
                self.assertEqual(int(sol_post_df.iloc[-1]["timestamp_ms"]), base_4h_ms + 3600000)
                self.assertAlmostEqual(float(sol_post_df.iloc[-1]["close"]), 195.0, places=4)
                self.assertAlmostEqual(float(sol_post_df.iloc[-2]["close"]), 180.0, places=4)

        asyncio.run(_run_minute_simulation())

    def test_12_bad_symbol_isolation_and_testnet_scaling(self) -> None:
        import ccxt.async_support as ccxt

        self.db.configure_hyperliquid_credentials(
            wallet_address="0xef377c1d5444786236aa34fa72e5879372e0eaec",
            private_key=SAMPLE_PRIVKEY,
            hyperliquid_testnet=True,
            execution_mode="LIVE",
        )
        ing = MarketIngestionEngine(self.db)
        usdc_symbols = [r["symbol"] for r in self.db.get_usdc_perp_universe()]
        base_4h_ms = 1700006400000
        current_time_sec = (base_4h_ms // 1000) + 600

        mock_ex = AsyncMock()
        xrp_calls = {"count": 0}

        async def _fake_fetch_ohlcv(
            symbol: str, timeframe: str = "1h", limit: int = 600
        ) -> list[list[float]]:
            if symbol == "XRP/USDC:USDC":
                xrp_calls["count"] += 1
                raise ccxt.BadSymbol("hyperliquid does not have market symbol XRP/USDC:USDC")
            tf_ms = 3600000 if timeframe == "1h" else 14400000
            aligned_ms = (int(current_time_sec * 1000) // tf_ms) * tf_ms
            start_ms = aligned_ms - 599 * tf_ms
            px = 70000.0 if symbol.startswith("BTC/") else 150.0
            return [
                [float(start_ms + idx * tf_ms), px * 0.99, px * 1.01, px * 0.98, px, 500.0]
                for idx in range(600)
            ]

        mock_ex.fetch_ohlcv.side_effect = _fake_fetch_ohlcv
        mock_ex.fetch_funding_rates.return_value = {
            sym: {
                "symbol": sym,
                "markPrice": 70100.0 if sym.startswith("BTC/") else 152.0,
                "fundingRate": 0.00002,
            }
            for sym in usdc_symbols
            if sym != "XRP/USDC:USDC"
        }

        async def _run_bad_symbol_check() -> None:
            nonlocal current_time_sec
            with patch.object(ing, "_get_exchange", new=AsyncMock(return_value=mock_ex)):
                with patch("engine.ingestion.time.time", side_effect=lambda: float(current_time_sec)):
                    stats_c1 = await ing.ingest_universe()
                # XRP/USDC:USDC fails once on 1h, is added to _unsupported_symbols, and skipped on 4h.
                # All 15 other symbols (30 symbol-tf pairs) still fetch live OHLCV without going offline.
                self.assertEqual(xrp_calls["count"], 1)
                self.assertIn("XRP/USDC:USDC", ing._unsupported_symbols)
                self.assertFalse(ing._cycle_network_offline)
                self.assertEqual(stats_c1["ohlcv_fetch_calls"], 30)

                # Subsequent 15s intra-bar tick does not call fetch_ohlcv for XRP or any other symbol
                current_time_sec += 15
                with patch("engine.ingestion.time.time", side_effect=lambda: float(current_time_sec)):
                    stats_c2 = await ing.ingest_universe()
                self.assertEqual(xrp_calls["count"], 1)
                self.assertEqual(stats_c2["ohlcv_fetch_calls"], 0)
                self.assertEqual(stats_c2["batch_snapshot_calls"], 1)

                # Verify symbols ordered after XRP (e.g., DOGE/USDC:USDC, INJ/USDC:USDC) received live prices
                doge_df = self.db.get_ohlcv_df("DOGE/USDC:USDC", "1h", limit=2)
                self.assertAlmostEqual(float(doge_df.iloc[-1]["close"]), 152.0, places=4)
                self.assertEqual(int(doge_df.iloc[-1]["is_synthetic"]), 0)

                xrp_df = self.db.get_ohlcv_df("XRP/USDC:USDC", "1h", limit=2)
                self.assertEqual(int(xrp_df.iloc[-1]["is_synthetic"]), 1)

                from engine.signals import SignalEngine

                _, evals = SignalEngine(self.db).evaluate_universe()
                xrp_eval = next(e for e in evals if e["exec_symbol"] == "XRP/USDC:USDC")
                self.assertEqual(xrp_eval["rs_rank"], 15)
                self.assertEqual(xrp_eval["signal_action"], "FLAT")

                self.db.set_config_value("initial_equity_usdc", 1000.0)
                snap = self.db.get_latest_portfolio_snapshot()
                self.assertAlmostEqual(float(snap["total_equity_usdc"]), 1000.0, places=2)
                self.assertAlmostEqual(float(snap["available_cash_usdc"]), 1000.0, places=2)

        asyncio.run(_run_bad_symbol_check())

    def test_13_sync_wallet_equity_and_batch_config_webui(self) -> None:
        client = TestClient(create_app(self.db))

        # 1. Without wallet_address configured, sync-wallet returns 400
        res_no_wallet = client.post("/api/v1/config/sync-wallet", json={"scale_risk_caps": True})
        self.assertEqual(res_no_wallet.status_code, 400)

        # 2. Configure wallet_address and mock CCXT fetch_balance returning 998.54 USDC on Testnet
        self.db.configure_hyperliquid_credentials(
            wallet_address=SAMPLE_WALLET,
            private_key=SAMPLE_PRIVKEY,
            hyperliquid_testnet=True,
            execution_mode="TESTNET",
        )
        mock_ex = AsyncMock()
        mock_ex.fetch_balance.return_value = {
            "USDC": {"total": 998.54, "free": 998.54, "used": 0.0},
            "total": {"USDC": 998.54},
            "info": {"marginSummary": {"accountValue": "998.54", "totalMarginUsed": "0.0"}},
        }
        mock_ex.close = AsyncMock()

        with patch.object(
            ExecutionEngine,
            "_build_hyperliquid_client",
            return_value=(mock_ex, True),
        ):
            res_sync = client.post("/api/v1/config/sync-wallet", json={"scale_risk_caps": True})
            self.assertEqual(res_sync.status_code, 200, res_sync.text)
            body = res_sync.json()
            self.assertEqual(body["status"], "synced")
            self.assertEqual(body["network"], "testnet")
            self.assertEqual(body["wallet_balance_usdc"], 998.54)
            self.assertEqual(body["initial_equity_usdc"], 998.54)
            self.assertEqual(body["scaled_risk_caps"]["max_portfolio_heat_usdc"], 39.94)

        p_after = client.get("/api/v1/portfolio").json()
        self.assertEqual(p_after["initial_equity_usdc"], 998.54)
        self.assertEqual(p_after["total_equity_usdc"], 998.54)

        # 3. Verify WebUI HTML includes the Sync from Hyperliquid Wallet button
        html_text = client.get("/").text
        self.assertIn("btn-sync-wallet", html_text)
        self.assertIn("Sync from Hyperliquid Wallet", html_text)


if __name__ == "__main__":
    unittest.main()

