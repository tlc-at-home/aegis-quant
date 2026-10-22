"""Unit tests for Realistic Paper Fill & Dynamic Volatility-and-Size Slippage Model (v0.2.0-alpha2)."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from engine.database import DatabaseManager
from engine.execution import ExecutionEngine
from engine.risk import RiskEngine


class TestPaperFillModel(unittest.TestCase):
    """Validate Rollout Commit Point 2 (v0.2.0-alpha2) Paper Fill & Dynamic Slippage Model."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_paper_fill.db"
        self.db = DatabaseManager(db_path=self.db_path)
        self.db.init_db()
        self.db.apply_migration("sql/migrations/001_v020_config_expansion.sql")
        self.db.apply_migration("sql/migrations/002_v020_order_fsm.sql")
        self.risk = RiskEngine(self.db)
        self.exec_engine = ExecutionEngine(self.db, self.risk, execution_mode="PAPER")

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_01_dynamic_effective_slippage_formula_scaling(self) -> None:
        """
        Verify exact dynamic effective slippage formula:
        Slippage_eff = default_slippage_bps + impact_coeff * (ATR_14 / Close) * (Notional_USD / 10000)
        """
        # 1. Baseline zero ATR or zero notional -> equals default_slippage_bps (2.5 bps)
        slip_zero = self.exec_engine.calculate_dynamic_slippage_bps(
            atr_14=0.0,
            close_price=100.0,
            notional_usd=2500.0,
            default_slippage_bps=2.5,
            impact_coeff=100.0,
        )
        self.assertAlmostEqual(slip_zero, 2.5, places=8)

        # 2. Standard trade: ATR_14 = 3.0, Close = 150.0 (ATR/Close = 0.02), Notional = $2,500 (0.25x of $10k)
        # Slippage_eff = 2.5 + 100.0 * 0.02 * 0.25 = 3.0 bps
        slip_std = self.exec_engine.calculate_dynamic_slippage_bps(
            atr_14=3.0,
            close_price=150.0,
            notional_usd=2500.0,
            default_slippage_bps=2.5,
            impact_coeff=100.0,
        )
        self.assertAlmostEqual(slip_std, 3.0, places=8)

        # 3. High volatility & double size: ATR_14 = 9.0, Close = 150.0 (ATR/Close = 0.06), Notional = $5,000 (0.50x)
        # Slippage_eff = 2.5 + 100.0 * 0.06 * 0.50 = 5.5 bps
        slip_high = self.exec_engine.calculate_dynamic_slippage_bps(
            atr_14=9.0,
            close_price=150.0,
            notional_usd=5000.0,
            default_slippage_bps=2.5,
            impact_coeff=100.0,
        )
        self.assertAlmostEqual(slip_high, 5.5, places=8)
        self.assertGreater(slip_high, slip_std)

        # 4. Guardrails against invalid/non-positive close price
        slip_guard = self.exec_engine.calculate_dynamic_slippage_bps(
            atr_14=5.0,
            close_price=0.0,
            notional_usd=2000.0,
            default_slippage_bps=2.5,
            impact_coeff=100.0,
        )
        self.assertAlmostEqual(slip_guard, 2.5, places=8)

    def test_02_paper_buy_limit_order_maker_fill_vs_taker_fallback(self) -> None:
        """
        Verify BUY order in PAPER mode:
        - Next bar low <= limit_price credits 1.5 bps maker fill at limit_price with 0 slippage.
        - Next bar low > limit_price credits 4.5 bps taker fill plus dynamic slippage.
        """
        # Case A: Next bar low (99.20) <= limit_price (100.00) -> 1.5 bps Maker Fill
        res_maker = self.exec_engine.simulate_paper_fill(
            symbol="SOL/USDC:USDC",
            side="BUY",
            limit_price=100.0,
            quantity=10.0,
            next_bar_low=99.20,
            next_bar_high=102.50,
            atr_14=2.0,
            close_price=100.0,
            default_slippage_bps=2.5,
            impact_coeff=100.0,
            client_order_id="cli-paper-buy-maker",
        )
        self.assertTrue(res_maker.is_maker)
        self.assertEqual(res_maker.fill_type, "MAKER_LIMIT")
        self.assertEqual(res_maker.fsm_state, "FILLED")
        self.assertAlmostEqual(res_maker.avg_fill_price, 100.0, places=8)
        self.assertAlmostEqual(res_maker.slippage_paid_usd, 0.0, places=8)
        self.assertAlmostEqual(res_maker.effective_slippage_bps, 0.0, places=8)
        # 1.5 bps on $1,000 = $0.15
        self.assertAlmostEqual(res_maker.fees_paid_usdc, 1000.0 * 0.00015, places=6)
        self.assertEqual(res_maker.state_history, ["PENDING", "OPEN_ON_BOOK", "FILLED"])

        # Case B: Exact boundary next bar low == limit_price (100.00) -> 1.5 bps Maker Fill
        res_boundary = self.exec_engine.simulate_paper_fill(
            symbol="SOL/USDC:USDC",
            side="BUY",
            limit_price=100.0,
            quantity=10.0,
            next_bar_low=100.0,
            next_bar_high=103.0,
            atr_14=2.0,
            close_price=100.0,
        )
        self.assertTrue(res_boundary.is_maker)
        self.assertAlmostEqual(res_boundary.avg_fill_price, 100.0, places=8)

        # Case C: Next bar low (100.40) > limit_price (100.00) -> 4.5 bps Taker Fallback + Dynamic Slippage
        # Notional = 20 * 100 = $2,000 (0.20x of $10k), ATR/Close = 4.0 / 100.0 = 0.04
        # Slippage_eff = 2.5 + 100.0 * 0.04 * 0.20 = 3.3 bps (0.00033)
        res_taker = self.exec_engine.simulate_paper_fill(
            symbol="SOL/USDC:USDC",
            side="BUY",
            limit_price=100.0,
            quantity=20.0,
            next_bar_low=100.40,
            next_bar_high=104.00,
            atr_14=4.0,
            close_price=100.0,
            default_slippage_bps=2.5,
            impact_coeff=100.0,
            client_order_id="cli-paper-buy-taker",
        )
        self.assertFalse(res_taker.is_maker)
        self.assertEqual(res_taker.fill_type, "TAKER_FALLBACK")
        self.assertEqual(res_taker.fsm_state, "FILLED")
        self.assertAlmostEqual(res_taker.effective_slippage_bps, 3.3, places=6)
        expected_fill_px = 100.0 * (1.0 + 3.3 / 10000.0)
        self.assertAlmostEqual(res_taker.avg_fill_price, expected_fill_px, places=6)
        self.assertAlmostEqual(
            res_taker.slippage_paid_usd, (expected_fill_px - 100.0) * 20.0, places=6
        )
        self.assertAlmostEqual(
            res_taker.fees_paid_usdc, (20.0 * expected_fill_px) * 0.00045, places=6
        )
        self.assertEqual(
            res_taker.state_history,
            ["PENDING", "OPEN_ON_BOOK", "EXPIRED", "FILLED"],
        )

    def test_03_paper_sell_limit_order_maker_fill_vs_taker_fallback(self) -> None:
        """
        Verify SELL order in PAPER mode:
        - Next bar high >= limit_price credits 1.5 bps maker fill at limit_price with 0 slippage.
        - Next bar high < limit_price credits 4.5 bps taker fill plus dynamic slippage.
        """
        # Case A: Next bar high (201.50) >= limit_price (200.00) -> 1.5 bps Maker Fill
        res_maker = self.exec_engine.simulate_paper_fill(
            symbol="ETH/USDC:USDC",
            side="SELL",
            limit_price=200.0,
            quantity=5.0,
            next_bar_low=196.0,
            next_bar_high=201.50,
            atr_14=5.0,
            close_price=200.0,
            client_order_id="cli-paper-sell-maker",
        )
        self.assertTrue(res_maker.is_maker)
        self.assertEqual(res_maker.fill_type, "MAKER_LIMIT")
        self.assertAlmostEqual(res_maker.avg_fill_price, 200.0, places=8)
        self.assertAlmostEqual(res_maker.slippage_paid_usd, 0.0, places=8)
        self.assertAlmostEqual(res_maker.fees_paid_usdc, 1000.0 * 0.00015, places=6)

        # Case B: Next bar high (199.20) < limit_price (200.00) -> 4.5 bps Taker Fallback + Dynamic Slippage
        # Notional = 10 * 200 = $2,000 (0.20x), ATR/Close = 6.0 / 200.0 = 0.03
        # Slippage_eff = 2.5 + 100.0 * 0.03 * 0.20 = 3.1 bps
        res_taker = self.exec_engine.simulate_paper_fill(
            symbol="ETH/USDC:USDC",
            side="SELL",
            limit_price=200.0,
            quantity=10.0,
            next_bar_low=195.0,
            next_bar_high=199.20,
            atr_14=6.0,
            close_price=200.0,
            default_slippage_bps=2.5,
            impact_coeff=100.0,
            client_order_id="cli-paper-sell-taker",
        )
        self.assertFalse(res_taker.is_maker)
        self.assertEqual(res_taker.fill_type, "TAKER_FALLBACK")
        self.assertAlmostEqual(res_taker.effective_slippage_bps, 3.1, places=6)
        expected_fill_px = 200.0 * (1.0 - 3.1 / 10000.0)
        self.assertAlmostEqual(res_taker.avg_fill_price, expected_fill_px, places=6)
        self.assertAlmostEqual(
            res_taker.slippage_paid_usd, (200.0 - expected_fill_px) * 10.0, places=6
        )
        self.assertAlmostEqual(
            res_taker.fees_paid_usdc, (10.0 * expected_fill_px) * 0.00045, places=6
        )

    def test_04_paper_chaser_fsm_integration_and_audit_trail(self) -> None:
        """Verify execute_maker_limit_chaser in PAPER mode routes through simulate_paper_fill and logs audit rows."""
        res = asyncio.run(
            self.exec_engine.execute_maker_limit_chaser(
                symbol="SOL/USDC:USDC",
                side="LONG",
                quantity=10.0,
                price=150.0,
                client_order_id="cli-paper-chaser-01",
                next_bar={"low": 151.0, "high": 154.0, "close": 150.0},
                atr_14=3.0,
            )
        )
        self.assertEqual(res.fsm_state, "FILLED")
        self.assertFalse(res.is_maker)
        self.assertGreater(res.slippage_paid_usd, 0.0)

        audits = self.db.get_order_state_audit("cli-paper-chaser-01")
        self.assertEqual(
            [(a["from_state"], a["to_state"]) for a in audits],
            [
                ("PENDING", "OPEN_ON_BOOK"),
                ("OPEN_ON_BOOK", "EXPIRED"),
                ("EXPIRED", "FILLED"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
