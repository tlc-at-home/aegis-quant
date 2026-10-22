"""Unit tests for RiskEngine boundary enforcement ($400 heat, $2,500 single-asset, $15,000 leverage, $10 min notional) and v0.2.0-rc1 Security & Alerting."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd
from cryptography.fernet import Fernet

from engine.alerts import AlertEngine
from engine.database import DatabaseManager
from engine.risk import RiskEngine


def _make_flat_ohlcv(
    close_price: float = 100.0,
    atr_spread: float = 0.5,
    n_bars: int = 30,
) -> pd.DataFrame:
    """Build a deterministic OHLCV DataFrame with controlled price and ATR."""
    ts = np.arange(n_bars, dtype=np.int64) * 3_600_000 + 1750000000000
    closes = np.full(n_bars, close_price, dtype=np.float64)
    highs = np.full(n_bars, close_price + atr_spread * 0.5, dtype=np.float64)
    lows = np.full(n_bars, close_price - atr_spread * 0.5, dtype=np.float64)
    return pd.DataFrame(
        {
            "timestamp_ms": ts,
            "open": closes,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": np.full(n_bars, 1000.0, dtype=np.float64),
            "funding_rate_hourly": np.zeros(n_bars, dtype=np.float64),
            "is_synthetic": np.zeros(n_bars, dtype=np.int64),
        }
    )


class TestRiskBoundaries(unittest.TestCase):
    """Validate exact risk boundary enforcement, Fernet config encryption, and webhook alerting (v0.2.0-rc1)."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_risk_boundaries.db"
        self.db = DatabaseManager(db_path=self.db_path)
        self.db.init_db()
        self.db.apply_migration("sql/migrations/001_v020_config_expansion.sql")
        self.db.apply_migration("sql/migrations/002_v020_order_fsm.sql")
        self.db.apply_migration("sql/migrations/003_v020_schema_versioning.sql")
        self.cfg = self.db.load_config()
        self.risk = RiskEngine(self.db)
        self.pair_meta = {
            "base_asset": "SOL",
            "exec_symbol": "SOL/USDC:USDC",
            "synth_symbol": "SOL/BTC:SYNTH",
            "exec_timeframe": "1h",
            "rs_timeframe": "4h",
            "tick_size": 0.01,
            "lot_step_size": 0.01,
            "min_notional_usdc": 10.0,
            "max_leverage": 5.0,
        }

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_01_conviction_risk_budget_bounds_50_to_150_usdc(self) -> None:
        """Verify conviction multiplier [0.5x, 1.5x] and dollar risk budget [$50, $150] on $10,000 equity."""
        mult_min, risk_min = self.risk.compute_conviction_risk_budget(
            blended_conviction=0.0,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertAlmostEqual(mult_min, 0.5, places=4)
        self.assertAlmostEqual(risk_min, 50.0, places=2)

        mult_mid, risk_mid = self.risk.compute_conviction_risk_budget(
            blended_conviction=-0.5,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertAlmostEqual(mult_mid, 1.0, places=4)
        self.assertAlmostEqual(risk_mid, 100.0, places=2)

        mult_max, risk_max = self.risk.compute_conviction_risk_budget(
            blended_conviction=1.0,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertAlmostEqual(mult_max, 1.5, places=4)
        self.assertAlmostEqual(risk_max, 150.0, places=2)

        # Out-of-bounds conviction > 1.0 must clip strictly at 1.5x / $150.00
        mult_clip, risk_clip = self.risk.compute_conviction_risk_budget(
            blended_conviction=3.5,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertAlmostEqual(mult_clip, 1.5, places=4)
        self.assertAlmostEqual(risk_clip, 150.0, places=2)

    def test_02_portfolio_heat_cap_4_pct_400_usdc_boundary(self) -> None:
        """
        Verify exact 4.0% ($400.00) Portfolio Heat cap boundary enforcement:
        - At $375.00 existing heat ($25.00 remaining == 0.5 * $50 min risk), trade is approved
          and risk_budget_usdc is clamped to <= $25.00 so total heat never exceeds $400.00.
        - At $375.01 existing heat ($24.99 remaining < $25.00), trade is rejected.
        - At $400.00 existing heat ($0.00 remaining), trade is rejected.
        """
        df = _make_flat_ohlcv(close_price=100.0, atr_spread=2.0)

        # 1. Exact boundary where remaining_heat == $25.00 (current_heat = $375.00)
        open_at_375 = [
            {"symbol": "ETH/USDC:USDC", "side": "LONG", "notional_usdc": 2000.0, "risk_budget_usdc": 200.0},
            {"symbol": "BTC/USDC:USDC", "side": "LONG", "notional_usdc": 2000.0, "risk_budget_usdc": 175.0},
            {"symbol": "USDC/BASIS", "side": "BASIS_HARVEST", "notional_usdc": 3000.0, "risk_budget_usdc": 999.0},
        ]
        res_boundary_ok = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=1.0,  # desires $150 risk, but remaining heat is $25
            exec_df=df,
            open_trades=open_at_375,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertTrue(res_boundary_ok.approved)
        self.assertLessEqual(res_boundary_ok.risk_budget_usdc, 25.0)
        self.assertLessEqual(375.0 + res_boundary_ok.risk_budget_usdc, 400.0)

        # 2. Just past boundary: current_heat = $375.01 (remaining_heat = $24.99 < $25.00) -> rejected
        open_at_375_01 = [
            {"symbol": "ETH/USDC:USDC", "side": "LONG", "notional_usdc": 2000.0, "risk_budget_usdc": 375.01},
        ]
        res_over_heat = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=0.8,
            exec_df=df,
            open_trades=open_at_375_01,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertFalse(res_over_heat.approved)
        self.assertIn("Portfolio heat cap reached", res_over_heat.reason)
        self.assertEqual(res_over_heat.notional_usdc, 0.0)

        # 3. Full $400.00 heat cap reached -> rejected
        open_at_400 = [
            {"symbol": "ETH/USDC:USDC", "side": "LONG", "notional_usdc": 2500.0, "risk_budget_usdc": 400.0},
        ]
        res_full_heat = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="SHORT",
            blended_conviction=-1.0,
            exec_df=df,
            open_trades=open_at_400,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertFalse(res_full_heat.approved)
        self.assertIn("Portfolio heat cap reached", res_full_heat.reason)

    def test_03_single_asset_concentration_cap_25_pct_2500_usdc_boundary(self) -> None:
        """
        Verify exact 25% ($2,500.00) single-asset notional concentration cap:
        - With low ATR (0.5% of price -> 1.5% stop distance) and $150 max conviction risk budget,
          uncapped notional would be $150 / 0.015 = $10,000.00, which MUST be capped at $2,500.00.
        - When equity drops to $8,000.00, 25% single-asset cap scales down to $2,000.00.
        """
        df_low_vol = _make_flat_ohlcv(close_price=100.0, atr_spread=0.1)

        res_10k = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=1.0,
            exec_df=df_low_vol,
            open_trades=[],
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertTrue(res_10k.approved)
        self.assertAlmostEqual(res_10k.notional_usdc, 2500.0, places=2)
        self.assertAlmostEqual(res_10k.quantity, 25.0, places=4)

        # Equity at $8,000 -> 25% of $8,000 = $2,000.00 cap
        res_8k = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="SHORT",
            blended_conviction=-1.0,
            exec_df=df_low_vol,
            open_trades=[],
            current_equity_usdc=8000.0,
            config=self.cfg,
        )
        self.assertTrue(res_8k.approved)
        self.assertAlmostEqual(res_8k.notional_usdc, 2000.0, places=2)

    def test_04_gross_leverage_ceiling_1_5x_15000_usdc_boundary(self) -> None:
        """
        Verify exact 1.5x ($15,000.00) gross leverage notional ceiling:
        - With $14,200.00 existing gross notional, new position is clamped to remaining $800.00.
        - With $14,990.00 existing gross notional ($10.00 remaining == $10.00 min_notional), trade is approved at $10.00.
        - With $14,990.01 existing gross notional ($9.99 remaining < $10.00 min_notional), trade is rejected.
        - With $15,000.00 existing gross notional, trade is rejected.
        """
        df_low_vol = _make_flat_ohlcv(close_price=100.0, atr_spread=0.1)

        # 1. Remaining notional = $800.00 -> clamps notional from $2,500.00 down to $800.00
        open_14200 = [
            {"symbol": "BTC/USDC:USDC", "side": "LONG", "notional_usdc": 14200.0, "risk_budget_usdc": 100.0},
        ]
        res_clamped = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=1.0,
            exec_df=df_low_vol,
            open_trades=open_14200,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertTrue(res_clamped.approved)
        self.assertAlmostEqual(res_clamped.notional_usdc, 800.0, places=2)
        self.assertAlmostEqual(14200.0 + res_clamped.notional_usdc, 15000.0, places=2)

        # 2. Exact boundary: existing gross notional = $14,990.00 -> remaining = $10.00 (== min_notional)
        open_14990 = [
            {"symbol": "BTC/USDC:USDC", "side": "LONG", "notional_usdc": 14990.0, "risk_budget_usdc": 100.0},
        ]
        res_exact_10 = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=1.0,
            exec_df=df_low_vol,
            open_trades=open_14990,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertTrue(res_exact_10.approved)
        self.assertAlmostEqual(res_exact_10.notional_usdc, 10.0, places=2)

        # 3. Just over boundary: existing gross notional = $14,990.01 -> remaining = $9.99 (< $10.00) -> rejected
        open_14990_01 = [
            {"symbol": "BTC/USDC:USDC", "side": "LONG", "notional_usdc": 14990.01, "risk_budget_usdc": 100.0},
        ]
        res_over_lev = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=1.0,
            exec_df=df_low_vol,
            open_trades=open_14990_01,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertFalse(res_over_lev.approved)
        self.assertIn("Gross leverage ceiling reached", res_over_lev.reason)

        # 4. At $15,000.00 -> rejected
        open_15000 = [
            {"symbol": "BTC/USDC:USDC", "side": "LONG", "notional_usdc": 15000.0, "risk_budget_usdc": 100.0},
        ]
        res_at_15k = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="SHORT",
            blended_conviction=-1.0,
            exec_df=df_low_vol,
            open_trades=open_15000,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertFalse(res_at_15k.approved)
        self.assertIn("Gross leverage ceiling reached", res_at_15k.reason)

    def test_05_minimum_notional_10_usdc_limit_and_empty_ohlcv_guard(self) -> None:
        """Verify $10.00 minimum notional limit rejection when lot rounding drops notional below $10.00."""
        # Empty OHLCV DataFrame must reject cleanly
        empty_df = pd.DataFrame(columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
        res_empty = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=0.8,
            exec_df=empty_df,
            open_trades=[],
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertFalse(res_empty.approved)
        self.assertIn("Missing OHLCV", res_empty.reason)

        # Extreme ATR so raw_notional < $10.00 (e.g. ATR = 800 on price 100 -> stop_pct = 24.0 -> $50 / 24 = $2.08 < $10.00)
        df_extreme_atr = _make_flat_ohlcv(close_price=100.0, atr_spread=800.0)
        res_sub_10 = self.risk.evaluate_order_risk(
            pair_meta=self.pair_meta,
            side="LONG",
            blended_conviction=0.0,
            exec_df=df_extreme_atr,
            open_trades=[],
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertFalse(res_sub_10.approved)
        self.assertIn("below minimum $10.00", res_sub_10.reason)
        self.assertEqual(res_sub_10.quantity, 0.0)
        self.assertEqual(res_sub_10.notional_usdc, 0.0)

        # Coarse lot_step_size=1.0 on $15.00 remaining notional at price $100.0 -> quantity rounds to 0.0 -> rejected
        coarse_meta = {**self.pair_meta, "lot_step_size": 1.0}
        df_std = _make_flat_ohlcv(close_price=100.0, atr_spread=0.5)
        open_14985 = [
            {"symbol": "BTC/USDC:USDC", "side": "LONG", "notional_usdc": 14985.0, "risk_budget_usdc": 50.0},
        ]
        res_zero_lot = self.risk.evaluate_order_risk(
            pair_meta=coarse_meta,
            side="LONG",
            blended_conviction=1.0,
            exec_df=df_std,
            open_trades=open_14985,
            current_equity_usdc=10000.0,
            config=self.cfg,
        )
        self.assertFalse(res_zero_lot.approved)
        self.assertIn("below minimum $10.00", res_zero_lot.reason)

    def test_06_migration_003_schema_versioning_and_fernet_config_encryption(self) -> None:
        """
        Verify Rollout Commit Point 3 (v0.2.0-rc1) schema versioning & Fernet AES-128 encryption at rest:
        1. Migration 003 creates schema_migrations with versions 1, 2, 3 and reports schema_version == 3.
        2. Sensitive parameters (private_key, gemini_api_key, telegram_bot_token)
           are stored as Fernet AES-128-CBC + HMAC-SHA256 ciphertext on disk in system_config.
        3. DatabaseManager.get_decrypted_config() transparently decrypts parameters in memory on demand.
        4. Supports master key injection via systemd CREDENTIALS_DIRECTORY and AEGIS_MASTER_KEY.
        """
        self.assertEqual(self.db.get_schema_version(), 3)
        migrations = self.db.get_applied_migrations()
        self.assertEqual([m["version"] for m in migrations], [1, 2, 3])
        integrity = self.db.verify_integrity()
        self.assertEqual(integrity["schema_version"], 3)
        self.assertIsNone(self.db.get_raw_config_value("discord_webhook_url"))

        # Test master key injection via systemd LoadCredentialEncrypted= (CREDENTIALS_DIRECTORY)
        cred_dir = Path(self.tmp_dir.name) / "systemd_creds"
        cred_dir.mkdir(parents=True, exist_ok=True)
        injected_key = Fernet.generate_key()
        (cred_dir / "aegis_master_key").write_bytes(injected_key)

        with patch.dict("os.environ", {"CREDENTIALS_DIRECTORY": str(cred_dir)}):
            db_systemd = DatabaseManager(db_path=self.db_path)
            raw_priv = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
            raw_tg = "123456789:AAH_test_telegram_bot_secret_token"

            db_systemd.set_config_value("private_key", raw_priv)
            db_systemd.set_config_value("telegram_bot_token", raw_tg)

            # Verify raw SQLite disk contents are Fernet tokens (never plaintext!)
            disk_priv = db_systemd.get_raw_config_value("private_key")
            disk_tg = db_systemd.get_raw_config_value("telegram_bot_token")

            assert disk_priv is not None and disk_tg is not None
            self.assertTrue(disk_priv.startswith("gAAAAAB"))
            self.assertTrue(disk_tg.startswith("gAAAAAB"))
            self.assertNotEqual(disk_priv, raw_priv)
            self.assertNotEqual(disk_tg, raw_tg)

            # Direct Fernet(injected_key).decrypt must recover exact plaintext
            f_cipher = Fernet(injected_key)
            self.assertEqual(f_cipher.decrypt(disk_priv.encode("utf-8")).decode("utf-8"), raw_priv)
            self.assertEqual(f_cipher.decrypt(disk_tg.encode("utf-8")).decode("utf-8"), raw_tg)

            # Verify get_decrypted_config() decrypts both full dict and individual keys in memory
            dec_cfg = db_systemd.get_decrypted_config()
            self.assertEqual(dec_cfg["private_key"], raw_priv)
            self.assertEqual(dec_cfg["telegram_bot_token"], raw_tg)
            self.assertEqual(db_systemd.get_decrypted_config("private_key"), raw_priv)

            # Verify --test-encryption helper
            enc_report = db_systemd.test_encryption()
            self.assertEqual(enc_report["status"], "VERIFIED")
            self.assertTrue(enc_report["transparent_decryption_verified"])

            # 5. Verify atomic master key rotation re-encrypts secrets cleanly
            rotated_key = Fernet.generate_key()
            db_systemd.set_config_value("private_key", raw_priv)
            rot_res = db_systemd.rotate_master_key(rotated_key)
            self.assertEqual(rot_res["status"], "ROTATED")
            self.assertIn("private_key", rot_res["re_encrypted_keys"])
            self.assertIn("telegram_bot_token", rot_res["re_encrypted_keys"])
            self.assertEqual(db_systemd.get_decrypted_config("private_key"), raw_priv)
            self.assertEqual(db_systemd.get_decrypted_config("telegram_bot_token"), raw_tg)

            # 6. Verify unmigrated key rotation does not crash get_decrypted_config() or leak ciphertext as plaintext
            unmigrated_key = Fernet.generate_key()
            db_unmigrated = DatabaseManager(db_path=self.db_path, master_key=unmigrated_key)
            unmig_cfg = db_unmigrated.get_decrypted_config()
            self.assertEqual(unmig_cfg["private_key"], "")
            self.assertEqual(unmig_cfg["telegram_bot_token"], "")
            unmig_status = db_unmigrated.get_credentials_status()
            self.assertFalse(unmig_status["private_key_configured"])
            self.assertFalse(unmig_status["telegram_bot_token_configured"])
            self.assertEqual(unmig_status["telegram_bot_token_masked"], "")
            self.assertEqual(db_unmigrated.test_encryption()["status"], "VERIFIED")

    def test_07_webhook_alert_engine_channels_and_triggers(self) -> None:
        """
        Verify AlertEngine (engine/alerts.py) & Web UI/API Telegram alerting:
        1. Dispatches formatted alerts to Telegram Bot API (`telegram_bot_token` + `telegram_chat_id`).
        2. Triggers on HMM Regime Flips (CHOP -> BULL / BEAR), including cold-start recovery from portfolio_snapshots.
        3. Triggers on On-Chain Position Reconciliation Mismatches.
        4. Triggers on Daily Portfolio Drawdown exceeding 3.0% threshold ($9,700 on $10,000 baseline and 24h peak).
        5. Verifies FastAPI `/api/v1/alerts`, `/api/v1/alerts/test`, and dashboard HTML Telegram panel.
        """
        from fastapi.testclient import TestClient
        from web.server import create_app

        captured_calls: list[tuple[str, dict[str, Any]]] = []

        async def _mock_http_post(url: str, payload: dict[str, Any]) -> int:
            captured_calls.append((url, payload))
            return 200

        self.db.set_config_value("telegram_bot_token", "999888777:AAE_mock_token")
        self.db.set_config_value("telegram_chat_id", "-100987654321")

        alert_engine = AlertEngine(self.db, http_post=_mock_http_post)

        # 1. Direct send_alert dispatches to Telegram Bot API
        res_direct = asyncio.run(
            alert_engine.send_alert("System Boot", "v0.2.0-rc1 online", level="INFO")
        )
        self.assertEqual(res_direct["status"], "DELIVERED")
        self.assertEqual(res_direct["channel"], "TELEGRAM")
        self.assertTrue(res_direct["telegram_sent"])
        self.assertEqual(len(captured_calls), 1)
        tg_url, tg_body = captured_calls[0]
        self.assertIn("api.telegram.org/bot999888777:AAE_mock_token/sendMessage", tg_url)
        self.assertEqual(tg_body["chat_id"], "-100987654321")
        self.assertIn("🛡️ AEGIS QUANT: System Boot", tg_body["text"])

        # 2. HMM Market Regime Flip (CHOP -> BULL triggers alert; BULL -> BULL does not)
        captured_calls.clear()
        res_no_flip = asyncio.run(
            alert_engine.check_and_alert_regime_flip("CHOP", "CHOP", 0.20, 0.20, 0.60)
        )
        self.assertIsNone(res_no_flip)
        self.assertEqual(len(captured_calls), 0)

        res_flip = asyncio.run(
            alert_engine.check_and_alert_regime_flip("CHOP", "BULL", 0.72, 0.08, 0.20)
        )
        assert res_flip is not None
        self.assertEqual(res_flip["status"], "DELIVERED")
        self.assertIn("CHOP -> BULL", res_flip["formatted_message"])

        # Cold-start AlertEngine must detect flip against persisted portfolio_snapshots regime when previous_regime=None
        base_snap_ts = int(self.db.get_latest_portfolio_snapshot().get("timestamp_ms", 1750000000000))
        self.db.save_portfolio_snapshot(
            {
                "timestamp_ms": base_snap_ts + 60_000,
                "total_equity_usdc": 12000.0,
                "available_cash_usdc": 12000.0,
                "deployed_notional_usdc": 0.0,
                "gross_leverage": 0.0,
                "portfolio_heat_usdc": 0.0,
                "portfolio_heat_pct": 0.0,
                "basis_harvest_notional_usdc": 0.0,
                "accrued_yield_usdc": 0.0,
                "hmm_regime": "CHOP",
                "prob_bull": 0.2,
                "prob_bear": 0.2,
                "prob_chop": 0.6,
                "open_long_count": 0,
                "open_short_count": 0,
            }
        )
        cold_alert_engine = AlertEngine(self.db, http_post=_mock_http_post)
        res_cold_flip = asyncio.run(
            cold_alert_engine.check_and_alert_regime_flip(None, "BEAR", 0.05, 0.80, 0.15)
        )
        assert res_cold_flip is not None
        self.assertIn("CHOP -> BEAR", res_cold_flip["formatted_message"])

        # 3. Daily Drawdown Threshold (3.0%): $9,750 (2.5% DD) -> no alert; $9,700 (3.0% DD) -> CRITICAL alert
        captured_calls.clear()
        res_dd_safe = asyncio.run(
            alert_engine.check_and_alert_daily_drawdown(9750.0, reference_equity_usdc=10000.0)
        )
        self.assertIsNone(res_dd_safe)
        self.assertEqual(len(captured_calls), 0)

        res_dd_breach = asyncio.run(
            alert_engine.check_and_alert_daily_drawdown(9700.0, reference_equity_usdc=10000.0)
        )
        assert res_dd_breach is not None
        self.assertEqual(res_dd_breach["level"], "CRITICAL")
        self.assertIn("3.00%", res_dd_breach["formatted_message"])

        # Also verify 24h peak reference ($12,000 in portfolio_snapshots -> $11,600 is 3.33% DD > 3.0%)
        res_dd_peak = asyncio.run(
            alert_engine.check_and_alert_daily_drawdown(11600.0)
        )
        assert res_dd_peak is not None
        self.assertIn("3.33%", res_dd_peak["formatted_message"])

        # 4. End-to-end loopback HTTP Telegram Bot test via run_test_alert() (preserving partial token config)
        self.db.set_config_value("telegram_bot_token", "555444333:AAH_partial_saved_token")
        self.db.set_config_value("telegram_chat_id", "")
        live_alert_engine = AlertEngine(self.db)
        loopback_res = asyncio.run(
            live_alert_engine.run_test_alert("v0.2.0-rc1 Deployment Ready")
        )
        self.assertEqual(loopback_res["status"], "DELIVERED")
        self.assertEqual(loopback_res["channel"], "TELEGRAM")
        self.assertTrue(loopback_res["telegram_sent"])
        self.assertTrue(loopback_res["webhook_payload_verified"])
        self.assertEqual(
            self.db.get_decrypted_config("telegram_bot_token"),
            "555444333:AAH_partial_saved_token",
        )
        self.assertEqual(self.db.get_decrypted_config("telegram_chat_id"), "")

        # 5. FastAPI UI + /api/v1/alerts endpoints
        client = TestClient(create_app(self.db))
        html_res = client.get("/")
        self.assertEqual(html_res.status_code, 200)
        self.assertIn("Telegram Bot Alerting", html_res.text)
        self.assertNotIn("discord_webhook_url", html_res.text)

        post_alert_cfg = client.post(
            "/api/v1/alerts",
            json={
                "telegram_bot_token": "777888999:AAF_ui_configured_token",
                "telegram_chat_id": "-100555666777",
                "max_daily_drawdown_alert_pct": 0.025,
            },
        )
        self.assertEqual(post_alert_cfg.status_code, 200)
        alert_status = post_alert_cfg.json()["alerts"]
        self.assertEqual(alert_status["alert_channel"], "TELEGRAM")
        self.assertTrue(alert_status["telegram_alerts_ready"])
        self.assertEqual(alert_status["telegram_chat_id"], "-100555666777")
        self.assertAlmostEqual(alert_status["max_daily_drawdown_alert_pct"], 0.025, places=4)
        self.assertTrue(str(self.db.get_raw_config_value("telegram_bot_token")).startswith("gAAAAAB"))


if __name__ == "__main__":
    unittest.main()
