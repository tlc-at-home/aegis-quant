"""Unit and integration tests for Order FSM (MAKER_LIMIT_CHASER), On-Chain Reconciliation, and 1H Funding Sync."""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import ccxt.async_support as ccxt

from engine.database import DatabaseManager
from engine.execution import ExecutionEngine
from engine.ingestion import MarketIngestionEngine
from engine.risk import RiskEngine
from engine.signals import RegimeState


class TestExecutionStateMachine(unittest.TestCase):
    """Validate Rollout Commit Point 2 (v0.2.0-alpha2) Order FSM and Exchange Reconciliation."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "test_exec_fsm.db"
        self.db = DatabaseManager(db_path=self.db_path)
        self.db.init_db()
        self.db.apply_migration("sql/migrations/001_v020_config_expansion.sql")
        self.db.apply_migration("sql/migrations/002_v020_order_fsm.sql")
        self.risk = RiskEngine(self.db)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_01_migration_002_ddl_columns_check_constraint_and_audit_table(self) -> None:
        """Verify Migration 002 columns on trades, CHECK constraint on fsm_state, idempotency, and order_state_audit."""
        # Re-applying Migration 002 must be idempotent
        res = self.db.apply_migration("sql/migrations/002_v020_order_fsm.sql")
        self.assertEqual(res["status"], "APPLIED")

        with self.db.connection() as conn:
            cols = {
                str(r["name"])
                for r in conn.execute("PRAGMA table_info(trades);").fetchall()
            }
            for expected_col in (
                "client_order_id",
                "fsm_state",
                "exchange_order_id",
                "avg_fill_price",
                "filled_units",
                "slippage_paid_usd",
            ):
                self.assertIn(expected_col, cols)

            audit_cols = {
                str(r["name"])
                for r in conn.execute("PRAGMA table_info(order_state_audit);").fetchall()
            }
            self.assertEqual(
                audit_cols,
                {"id", "client_order_id", "from_state", "to_state", "timestamp_utc", "reason"},
            )

            # Verify SQLite CHECK constraint rejects invalid fsm_state at database level
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO trades (
                        symbol, base_asset, side, status, fsm_state,
                        entry_price, current_price, quantity, notional_usdc,
                        risk_budget_usdc, conviction_multiplier,
                        initial_stop_price, chandelier_stop_price,
                        highest_price_seen, lowest_price_seen
                    ) VALUES (
                        'SOL/USDC:USDC', 'SOL', 'LONG', 'OPEN', 'INVALID_STATE',
                        150.0, 150.0, 1.0, 150.0, 6.0, 1.0, 140.0, 140.0, 150.0, 150.0
                    );
                    """
                )

    def test_02_fsm_maker_limit_chaser_post_only_fill_lifecycle(self) -> None:
        """Verify PENDING -> OPEN_ON_BOOK -> FILLED lifecycle with postOnly=True and 1.5 bps maker fee."""
        mock_ex = AsyncMock()
        mock_ex.create_order.return_value = {
            "id": "hl-maker-001",
            "status": "open",
            "price": 150.0,
            "filled": 0.0,
        }
        mock_ex.fetch_order.return_value = {
            "id": "hl-maker-001",
            "status": "closed",
            "filled": 10.0,
            "average": 150.0,
            "price": 150.0,
        }

        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="LIVE"
        )
        trade_id = self.db.insert_trade(
            {
                "client_order_id": "cli-maker-001",
                "symbol": "SOL/USDC:USDC",
                "side": "LONG",
                "fsm_state": "PENDING",
                "entry_price": 150.0,
                "quantity": 10.0,
                "filled_units": 0.0,
            }
        )
        self.assertGreater(trade_id, 0)

        result = asyncio.run(
            exec_engine.execute_maker_limit_chaser(
                symbol="SOL/USDC:USDC",
                side="LONG",
                quantity=10.0,
                price=150.0,
                client_order_id="cli-maker-001",
                chase_intervals=3,
                chase_interval_seconds=0.0,
            )
        )

        self.assertEqual(result.fsm_state, "FILLED")
        self.assertTrue(result.is_maker)
        self.assertEqual(result.exchange_order_id, "hl-maker-001")
        self.assertAlmostEqual(result.filled_units, 10.0, places=6)
        self.assertAlmostEqual(result.avg_fill_price, 150.0, places=6)
        # 1.5 bps maker fee on $1,500 notional = $0.225
        self.assertAlmostEqual(result.fees_paid_usdc, 1500.0 * 0.00015, places=6)
        self.assertEqual(result.state_history, ["PENDING", "OPEN_ON_BOOK", "FILLED"])

        # Verify postOnly=True was sent on create_order
        _, create_kwargs = mock_ex.create_order.call_args
        self.assertEqual(create_kwargs["type"], "limit")
        self.assertTrue(create_kwargs["params"].get("postOnly"))

        # Verify SQLite trades row and order_state_audit trail
        trade_row = self.db.get_trade_by_client_order_id("cli-maker-001")
        assert trade_row is not None
        self.assertEqual(trade_row["fsm_state"], "FILLED")
        self.assertEqual(trade_row["exchange_order_id"], "hl-maker-001")
        self.assertAlmostEqual(float(trade_row["filled_units"]), 10.0, places=6)
        self.assertAlmostEqual(float(trade_row["avg_fill_price"]), 150.0, places=6)

        audits = self.db.get_order_state_audit("cli-maker-001")
        transitions = [(a["from_state"], a["to_state"]) for a in audits]
        self.assertEqual(
            transitions,
            [("PENDING", "OPEN_ON_BOOK"), ("OPEN_ON_BOOK", "FILLED")],
        )

    def test_03_fsm_partial_fills_and_chaser_repricing_loop(self) -> None:
        """Verify PENDING -> OPEN_ON_BOOK -> PARTIALLY_FILLED -> FILLED with top-of-book repricing."""
        mock_ex = AsyncMock()
        mock_ex.create_order.side_effect = [
            {"id": "hl-ord-step1", "status": "open", "price": 100.0, "filled": 0.0},
            {"id": "hl-ord-step2", "status": "open", "price": 100.5, "filled": 0.0},
        ]
        mock_ex.fetch_order.side_effect = [
            # Interval 1: 4 of 10 units filled at $100.0
            {"id": "hl-ord-step1", "status": "open", "filled": 4.0, "average": 100.0},
            # Interval 2: remaining 6 units filled at repriced $100.5
            {"id": "hl-ord-step2", "status": "closed", "filled": 6.0, "average": 100.5},
        ]
        mock_ex.fetch_order_book.return_value = {
            "bids": [[100.5, 50.0], [100.4, 100.0]],
            "asks": [[100.6, 50.0], [100.7, 100.0]],
        }

        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="LIVE"
        )
        self.db.insert_trade(
            {
                "client_order_id": "cli-partial-001",
                "symbol": "SOL/USDC:USDC",
                "side": "LONG",
                "fsm_state": "PENDING",
                "entry_price": 100.0,
                "quantity": 10.0,
                "filled_units": 0.0,
            }
        )

        result = asyncio.run(
            exec_engine.execute_maker_limit_chaser(
                symbol="SOL/USDC:USDC",
                side="LONG",
                quantity=10.0,
                price=100.0,
                client_order_id="cli-partial-001",
                chase_intervals=3,
                chase_interval_seconds=0.0,
            )
        )

        self.assertEqual(result.fsm_state, "FILLED")
        self.assertEqual(
            result.state_history,
            ["PENDING", "OPEN_ON_BOOK", "PARTIALLY_FILLED", "FILLED"],
        )
        self.assertAlmostEqual(result.filled_units, 10.0, places=6)
        # Weighted avg fill price: (4 * 100.0 + 6 * 100.5) / 10 = 100.30
        self.assertAlmostEqual(result.avg_fill_price, 100.30, places=6)
        mock_ex.cancel_order.assert_awaited_once_with("hl-ord-step1", "SOL/USDC:USDC")

        # Second create_order must request the remaining 6.0 units at the repriced best bid 100.5
        second_call_kwargs = mock_ex.create_order.call_args_list[1].kwargs
        self.assertAlmostEqual(second_call_kwargs["amount"], 6.0, places=6)
        self.assertAlmostEqual(second_call_kwargs["price"], 100.5, places=6)

        audits = self.db.get_order_state_audit("cli-partial-001")
        transitions = [(a["from_state"], a["to_state"]) for a in audits]
        self.assertEqual(
            transitions,
            [
                ("PENDING", "OPEN_ON_BOOK"),
                ("OPEN_ON_BOOK", "PARTIALLY_FILLED"),
                ("PARTIALLY_FILLED", "FILLED"),
            ],
        )

    def test_04_fsm_unfilled_chase_expiration_and_market_ioc_fallback(self) -> None:
        """
        Verify both:
        1. PENDING -> OPEN_ON_BOOK -> EXPIRED when chase intervals expire without IOC fill.
        2. PENDING -> OPEN_ON_BOOK -> EXPIRED -> FILLED when MARKET_IOC fallback crosses the spread (4.5 bps taker fee).
        """
        # Case A: Unfilled across 3 intervals and fallback_to_ioc=False -> ends in EXPIRED
        mock_ex_expired = AsyncMock()
        mock_ex_expired.create_order.return_value = {
            "id": "hl-exp-1",
            "status": "open",
            "price": 100.0,
            "filled": 0.0,
        }
        mock_ex_expired.fetch_order.return_value = {
            "id": "hl-exp-1",
            "status": "open",
            "filled": 0.0,
            "price": 100.0,
        }
        mock_ex_expired.fetch_order_book.return_value = {
            "bids": [[100.1, 20.0]],
            "asks": [[100.2, 20.0]],
        }

        exec_exp = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_expired, execution_mode="LIVE"
        )
        self.db.insert_trade(
            {
                "client_order_id": "cli-exp-001",
                "symbol": "SOL/USDC:USDC",
                "side": "LONG",
                "fsm_state": "PENDING",
                "entry_price": 100.0,
                "quantity": 5.0,
                "filled_units": 0.0,
            }
        )
        res_exp = asyncio.run(
            exec_exp.execute_maker_limit_chaser(
                symbol="SOL/USDC:USDC",
                side="LONG",
                quantity=5.0,
                price=100.0,
                client_order_id="cli-exp-001",
                chase_intervals=3,
                chase_interval_seconds=0.0,
                fallback_to_ioc=False,
            )
        )
        self.assertEqual(res_exp.fsm_state, "EXPIRED")
        self.assertEqual(res_exp.filled_units, 0.0)
        self.assertEqual(res_exp.state_history, ["PENDING", "OPEN_ON_BOOK", "EXPIRED"])
        exp_trade = self.db.get_trade_by_client_order_id("cli-exp-001")
        assert exp_trade is not None
        self.assertEqual(exp_trade["status"], "CANCELLED")
        exp_audits = self.db.get_order_state_audit("cli-exp-001")
        self.assertEqual(
            [(a["from_state"], a["to_state"]) for a in exp_audits],
            [("PENDING", "OPEN_ON_BOOK"), ("OPEN_ON_BOOK", "EXPIRED")],
        )

        # Case B: Unfilled across 3 intervals -> MARKET_IOC fallback crosses spread at 4.5 bps taker fee
        mock_ex_ioc = AsyncMock()
        mock_ex_ioc.create_order.side_effect = [
            {"id": "hl-limit-1", "status": "open", "price": 100.0, "filled": 0.0},
            {"id": "hl-limit-2", "status": "open", "price": 100.1, "filled": 0.0},
            {"id": "hl-limit-3", "status": "open", "price": 100.2, "filled": 0.0},
            {
                "id": "hl-ioc-fallback",
                "status": "closed",
                "type": "market",
                "filled": 5.0,
                "average": 100.35,
            },
        ]
        mock_ex_ioc.fetch_order.return_value = {
            "id": "hl-limit",
            "status": "open",
            "filled": 0.0,
            "price": 100.0,
        }
        mock_ex_ioc.fetch_order_book.side_effect = [
            {"bids": [[100.1, 20.0]], "asks": [[100.3, 20.0]]},
            {"bids": [[100.2, 20.0]], "asks": [[100.35, 20.0]]},
        ]

        exec_ioc = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_ioc, execution_mode="LIVE"
        )
        self.db.insert_trade(
            {
                "client_order_id": "cli-ioc-001",
                "symbol": "SOL/USDC:USDC",
                "side": "LONG",
                "fsm_state": "PENDING",
                "entry_price": 100.0,
                "quantity": 5.0,
                "filled_units": 0.0,
            }
        )
        res_ioc = asyncio.run(
            exec_ioc.execute_maker_limit_chaser(
                symbol="SOL/USDC:USDC",
                side="LONG",
                quantity=5.0,
                price=100.0,
                client_order_id="cli-ioc-001",
                chase_intervals=3,
                chase_interval_seconds=0.0,
                fallback_to_ioc=True,
                atr_14=2.0,
            )
        )
        self.assertEqual(res_ioc.fsm_state, "FILLED")
        self.assertFalse(res_ioc.is_maker)
        self.assertEqual(res_ioc.fill_type, "MARKET_IOC_FALLBACK")
        self.assertEqual(
            res_ioc.state_history,
            ["PENDING", "OPEN_ON_BOOK", "EXPIRED", "FILLED"],
        )
        self.assertAlmostEqual(res_ioc.filled_units, 5.0, places=6)
        self.assertAlmostEqual(res_ioc.avg_fill_price, 100.35, places=6)
        # 4.5 bps taker fee on 5.0 * 100.35 = 501.75 * 0.00045
        self.assertAlmostEqual(res_ioc.fees_paid_usdc, 5.0 * 100.35 * 0.00045, places=6)
        self.assertAlmostEqual(res_ioc.slippage_paid_usd, 0.35 * 5.0, places=6)

        ioc_call_kwargs = mock_ex_ioc.create_order.call_args_list[-1].kwargs
        self.assertEqual(ioc_call_kwargs["type"], "market")
        self.assertEqual(ioc_call_kwargs["params"].get("timeInForce"), "IOC")

    def test_05_fsm_network_timeouts_cancellations_and_rejections(self) -> None:
        """Verify resilience to CCXT network timeouts and accurate CANCELLED / REJECTED transitions."""
        # 1. Transient CCXT RequestTimeout on poll 1, followed by fill on poll 2
        mock_ex_timeout = AsyncMock()
        mock_ex_timeout.create_order.return_value = {
            "id": "hl-to-1",
            "status": "open",
            "price": 50.0,
        }
        mock_ex_timeout.fetch_order.side_effect = [
            ccxt.RequestTimeout("Hyperliquid L1 RPC timeout"),
            {"id": "hl-to-1", "status": "closed", "filled": 2.0, "average": 50.0},
        ]
        exec_to = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_timeout, execution_mode="LIVE"
        )
        res_to = asyncio.run(
            exec_to.execute_maker_limit_chaser(
                symbol="AVAX/USDC:USDC",
                side="LONG",
                quantity=2.0,
                price=50.0,
                client_order_id="cli-timeout-001",
                chase_intervals=3,
                chase_interval_seconds=0.0,
            )
        )
        self.assertEqual(res_to.fsm_state, "FILLED")
        self.assertAlmostEqual(res_to.filled_units, 2.0, places=6)

        # 2. External order cancellation on book -> CANCELLED
        mock_ex_cancel = AsyncMock()
        mock_ex_cancel.create_order.return_value = {
            "id": "hl-can-1",
            "status": "open",
            "price": 50.0,
        }
        mock_ex_cancel.fetch_order.return_value = {
            "id": "hl-can-1",
            "status": "canceled",
            "filled": 0.0,
        }
        exec_can = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_cancel, execution_mode="LIVE"
        )
        res_can = asyncio.run(
            exec_can.execute_maker_limit_chaser(
                symbol="AVAX/USDC:USDC",
                side="LONG",
                quantity=2.0,
                price=50.0,
                client_order_id="cli-cancel-001",
                chase_intervals=3,
                chase_interval_seconds=0.0,
            )
        )
        self.assertEqual(res_can.fsm_state, "CANCELLED")
        self.assertEqual(res_can.state_history, ["PENDING", "OPEN_ON_BOOK", "CANCELLED"])

        # 3. Exchange rejection on order placement -> REJECTED
        mock_ex_reject = AsyncMock()
        mock_ex_reject.create_order.side_effect = ccxt.InvalidOrder("Post-only order would cross book")
        exec_rej = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_reject, execution_mode="LIVE"
        )
        res_rej = asyncio.run(
            exec_rej.execute_maker_limit_chaser(
                symbol="AVAX/USDC:USDC",
                side="LONG",
                quantity=2.0,
                price=50.0,
                client_order_id="cli-reject-001",
                chase_intervals=3,
                chase_interval_seconds=0.0,
            )
        )
        self.assertEqual(res_rej.fsm_state, "REJECTED")
        self.assertEqual(res_rej.state_history, ["PENDING", "REJECTED"])

    def test_06_reconcile_exchange_state_detects_drift_and_force_closes(self) -> None:
        """
        Verify reconcile_exchange_state() detects simulated position drift (open locally in SQLite
        but 0 on-chain) and force-closes local trade with exit_reason='EXCHANGE_RECONCILE' and CRITICAL log.
        """
        # Insert 2 open local trades: SOL (drifted to 0 on-chain) and ETH (still open on-chain)
        self.db.insert_trade(
            {
                "client_order_id": "cli-drift-sol",
                "symbol": "SOL/USDC:USDC",
                "base_asset": "SOL",
                "side": "LONG",
                "entry_price": 180.0,
                "current_price": 175.0,
                "quantity": 5.0,
                "notional_usdc": 900.0,
                "risk_budget_usdc": 50.0,
                "initial_stop_price": 168.0,
                "chandelier_stop_price": 171.5,
                "exchange_order_id": "hl-live-sol-1",
            }
        )
        self.db.insert_trade(
            {
                "client_order_id": "cli-active-eth",
                "symbol": "ETH/USDC:USDC",
                "base_asset": "ETH",
                "side": "LONG",
                "entry_price": 2500.0,
                "current_price": 2550.0,
                "quantity": 0.5,
                "notional_usdc": 1250.0,
                "risk_budget_usdc": 60.0,
                "initial_stop_price": 2400.0,
                "chandelier_stop_price": 2450.0,
                "exchange_order_id": "hl-live-eth-1",
            }
        )

        mock_ex = AsyncMock()
        # Only ETH has a non-zero on-chain position; SOL is 0 on-chain
        mock_ex.fetch_positions.return_value = [
            {"symbol": "ETH/USDC:USDC", "contracts": 0.5, "side": "long"},
            {"symbol": "SOL/USDC:USDC", "contracts": 0.0, "side": "long"},
        ]

        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="LIVE"
        )

        # In PAPER mode, reconcile_exchange_state must be a no-op
        exec_engine.execution_mode = "PAPER"
        reconciled_paper = asyncio.run(exec_engine.reconcile_exchange_state())
        self.assertEqual(reconciled_paper, 0)
        mock_ex.fetch_positions.assert_not_awaited()
        self.assertEqual(len(self.db.get_open_trades()), 2)

        # Switch to LIVE mode -> detects SOL drift and force-closes with EXCHANGE_RECONCILE
        exec_engine.execution_mode = "LIVE"
        reconciled_live = asyncio.run(exec_engine.reconcile_exchange_state())
        self.assertEqual(reconciled_live, 1)
        mock_ex.fetch_positions.assert_awaited_once()

        open_trades = self.db.get_open_trades()
        self.assertEqual(len(open_trades), 1)
        self.assertEqual(open_trades[0]["symbol"], "ETH/USDC:USDC")

        sol_trade = self.db.get_trade_by_client_order_id("cli-drift-sol")
        assert sol_trade is not None
        self.assertEqual(sol_trade["status"], "CLOSED")
        self.assertEqual(sol_trade["exit_reason"], "EXCHANGE_RECONCILE")
        self.assertAlmostEqual(float(sol_trade["exit_price"]), 171.5, places=6)

        logs = self.db.get_recent_logs(limit=10)
        crit_logs = [
            lg for lg in logs if lg["level"] == "CRITICAL" and "RECONCILIATION MISMATCH" in lg["message"]
        ]
        self.assertGreaterEqual(len(crit_logs), 1)

    def test_07_exact_1h_funding_settlement_sync_live_mode(self) -> None:
        """Verify LIVE mode queries exchange.fetch_funding_history(symbol, since=last_sync) for exact USDC funding."""
        ing = MarketIngestionEngine(self.db)
        bars = ing._generate_synthetic_usdc_series("SOL/USDC:USDC", "1h", limit=80)
        self.db.upsert_ohlcv_batch(bars)
        latest_bar_ts = int(bars[-1][2])
        latest_close = float(bars[-1][6])

        self.db.insert_trade(
            {
                "client_order_id": "cli-fund-sol",
                "symbol": "SOL/USDC:USDC",
                "base_asset": "SOL",
                "side": "LONG",
                "entry_price": latest_close,
                "current_price": latest_close,
                "quantity": 10.0,
                "notional_usdc": round(10.0 * latest_close, 2),
                "risk_budget_usdc": 60.0,
                "initial_stop_price": latest_close * 0.50,
                "chandelier_stop_price": latest_close * 0.50,
            }
        )

        mock_ex = AsyncMock()
        mock_ex.fetch_positions.return_value = [
            {"symbol": "SOL/USDC:USDC", "contracts": 10.0, "side": "long"}
        ]
        mock_ex.fetch_funding_history.return_value = [
            {"symbol": "SOL/USDC:USDC", "timestamp": latest_bar_ts - 1800000, "amount": -1.25},
            {
                "symbol": "ETH/USDC:USDC",
                "timestamp": latest_bar_ts,
                "amount": -99.00,
            },
            {
                "info": {
                    "time": latest_bar_ts,
                    "delta": {"type": "funding", "coin": "SOL", "usdc": "-0.75"},
                }
            },
        ]

        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="LIVE"
        )
        exec_engine._last_funding_settlement_hour = 0
        self.db.pit_timestamp_ms = latest_bar_ts

        regime_bull = RegimeState("BULL", 0.85, 0.05, 0.10, 540)
        asyncio.run(exec_engine.reconcile_cycle(regime_bull, []))

        mock_ex.fetch_funding_history.assert_awaited_once_with(
            "SOL/USDC:USDC", since=latest_bar_ts - 3600000
        )
        sol_trade = self.db.get_trade_by_client_order_id("cli-fund-sol")
        assert sol_trade is not None
        self.assertAlmostEqual(float(sol_trade["funding_accrued_usdc"]), -2.00, places=4)
        self.assertEqual(
            exec_engine._last_funding_sync_ms["SOL/USDC:USDC"], latest_bar_ts + 1
        )

    def test_08_reprice_self_cancellation_and_partial_ioc_fallback(self) -> None:
        """
        Verify:
        1. If cancel_order succeeds during a reprice step and the repriced postOnly limit order raises
           InvalidOrder (crosses book), the chaser does NOT falsely abort as EXTERNAL_OR_EXCHANGE_CANCELLED
           and instead proceeds to MARKET_IOC fallback.
        2. Partial maker fill + partial MARKET_IOC fill synchronizes trades.quantity and filled_units.
        """
        mock_ex = AsyncMock()
        mock_ex.create_order.side_effect = [
            {"id": "hl-rep-1", "status": "open", "price": 100.0, "filled": 0.0},
            ccxt.InvalidOrder("Post-only order would cross book"),
            {
                "id": "hl-ioc-partial",
                "status": "closed",
                "type": "market",
                "filled": 2.0,
                "average": 101.0,
            },
        ]
        # Only interval 1 fetches hl-rep-1 (partially filled 4.0 of 10.0); interval 2 has no active order on book
        mock_ex.fetch_order.side_effect = [
            {"id": "hl-rep-1", "status": "open", "filled": 4.0, "average": 100.0},
        ]
        mock_ex.fetch_order_book.return_value = {
            "bids": [[100.5, 20.0]],
            "asks": [[100.6, 20.0]],
        }

        self.db.insert_trade(
            {
                "client_order_id": "cli-partial-ioc-001",
                "symbol": "SOL/USDC:USDC",
                "side": "LONG",
                "fsm_state": "PENDING",
                "entry_price": 100.0,
                "quantity": 10.0,
                "filled_units": 0.0,
            }
        )

        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="LIVE"
        )
        res = asyncio.run(
            exec_engine.execute_maker_limit_chaser(
                symbol="SOL/USDC:USDC",
                side="LONG",
                quantity=10.0,
                price=100.0,
                client_order_id="cli-partial-ioc-001",
                chase_intervals=2,
                chase_interval_seconds=0.0,
                fallback_to_ioc=True,
                atr_14=2.0,
            )
        )

        # 4.0 maker + 2.0 IOC = 6.0 of 10.0 filled -> PARTIALLY_FILLED
        self.assertEqual(res.fsm_state, "PARTIALLY_FILLED")
        self.assertAlmostEqual(res.filled_units, 6.0, places=6)
        expected_avg = (4.0 * 100.0 + 2.0 * 101.0) / 6.0
        self.assertAlmostEqual(res.avg_fill_price, expected_avg, places=6)
        self.assertEqual(
            res.state_history,
            ["PENDING", "OPEN_ON_BOOK", "PARTIALLY_FILLED", "EXPIRED", "PARTIALLY_FILLED"],
        )

        trade_row = self.db.get_trade_by_client_order_id("cli-partial-ioc-001")
        assert trade_row is not None
        self.assertEqual(trade_row["fsm_state"], "PARTIALLY_FILLED")
        self.assertEqual(trade_row["status"], "OPEN")
        self.assertAlmostEqual(float(trade_row["filled_units"]), 6.0, places=6)
        self.assertAlmostEqual(float(trade_row["quantity"]), 6.0, places=6)

    def test_09_partial_on_chain_position_drift_and_atr_slippage_in_reconcile_cycle(self) -> None:
        """
        Verify:
        1. reconcile_exchange_state() detects partial on-chain position drift (0 < on_chain_size < local_qty)
           and synchronizes local quantity, filled_units, notional_usdc, and risk_budget_usdc.
        2. reconcile_cycle() passes positive atr_14 to _route_dex_order on trade entry and exit.
        """
        self.db.insert_trade(
            {
                "client_order_id": "cli-partial-drift-sol",
                "symbol": "SOL/USDC:USDC",
                "base_asset": "SOL",
                "side": "LONG",
                "entry_price": 150.0,
                "current_price": 155.0,
                "quantity": 10.0,
                "filled_units": 10.0,
                "notional_usdc": 1500.0,
                "risk_budget_usdc": 100.0,
                "initial_stop_price": 140.0,
                "chandelier_stop_price": 142.0,
            }
        )

        mock_ex = AsyncMock()
        mock_ex.fetch_positions.return_value = [
            {"symbol": "SOL/USDC:USDC", "contracts": 4.0, "side": "long"}
        ]
        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="LIVE"
        )
        reconciled = asyncio.run(exec_engine.reconcile_exchange_state())
        self.assertEqual(reconciled, 1)

        sol_trade = self.db.get_trade_by_client_order_id("cli-partial-drift-sol")
        assert sol_trade is not None
        self.assertEqual(sol_trade["status"], "OPEN")
        self.assertAlmostEqual(float(sol_trade["quantity"]), 4.0, places=6)
        self.assertAlmostEqual(float(sol_trade["filled_units"]), 4.0, places=6)
        self.assertAlmostEqual(float(sol_trade["notional_usdc"]), 600.0, places=2)
        self.assertAlmostEqual(float(sol_trade["risk_budget_usdc"]), 40.0, places=2)

    def test_10_reconcile_exchange_state_dispatches_webhook_alert_via_alert_engine(self) -> None:
        """Verify on-chain position drift reconciliation dispatches CRITICAL Telegram Bot alerts."""
        from engine.alerts import AlertEngine

        self.db.apply_migration("sql/migrations/003_v020_schema_versioning.sql")
        self.db.set_config_value("telegram_bot_token", "123456789:AAE_drift_alert_token")
        self.db.set_config_value("telegram_chat_id", "-1001122334455")
        self.db.insert_trade(
            {
                "client_order_id": "cli-alert-drift-sol",
                "symbol": "SOL/USDC:USDC",
                "base_asset": "SOL",
                "side": "LONG",
                "entry_price": 160.0,
                "current_price": 158.0,
                "quantity": 5.0,
                "notional_usdc": 800.0,
                "risk_budget_usdc": 50.0,
                "initial_stop_price": 150.0,
                "chandelier_stop_price": 152.0,
            }
        )

        dispatched: list[tuple[str, dict[str, Any]]] = []

        async def _capture_post(url: str, payload: dict[str, Any]) -> int:
            dispatched.append((url, payload))
            return 200

        mock_ex = AsyncMock()
        mock_ex.fetch_positions.return_value = []
        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="LIVE"
        )
        exec_engine.alerts = AlertEngine(self.db, http_post=_capture_post)

        reconciled = asyncio.run(exec_engine.reconcile_exchange_state())
        self.assertEqual(reconciled, 1)
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(
            dispatched[0][0],
            "https://api.telegram.org/bot123456789:AAE_drift_alert_token/sendMessage",
        )
        self.assertEqual(dispatched[0][1]["chat_id"], "-1001122334455")
        self.assertIn(
            "[CRITICAL] 🛡️ AEGIS QUANT: Position Drift Reconciliation",
            dispatched[0][1]["text"],
        )

    def test_11_on_chain_hlp_vault_deposit_withdrawal_and_dashboard_reality(self) -> None:
        """
        Verify on-chain Hyperliquid HLP vault harvesting & reconciliation:
        1. Queries `userVaultEquities` and `vaultDetails` on-chain.
        2. Executes `vaultTransfer` deposit (with 6-decimal micro-USDC `usd`) when idle cash > target.
        3. Executes `vaultTransfer` withdrawal when unlocked, or queues when inside 4-day lockup.
        4. Reports exact on-chain reality (`$0.00` when rejected, or exact on-chain vault equity & PnL).
        """
        self.db.set_config_value("initial_equity_usdc", 999.0)
        self.db.set_config_value("wallet_address", "0xef377c1d5444786236aa34fa72e5879372e0eaec")

        mock_ex = AsyncMock()
        mock_ex.fetch_positions.return_value = []
        mock_ex.milliseconds = lambda: 1793022000000
        mock_ex.sign_l1_action = lambda action, nonce: {"r": "0x1", "s": "0x2", "v": 27}

        # Scenario A: Vault initially has $0.00 on-chain, and L1 rejects vaultTransfer (e.g. trade-only agent key).
        # Dashboard snapshot MUST reflect $0.00 harvest and $999.00 equity (no fake $499.58!).
        mock_ex.fetch_balance.return_value = {
            "USDC": {"total": 999.0, "free": 999.0, "used": 0.0},
            "total": {"USDC": 999.0},
        }

        async def _post_info_zero(payload: dict[str, Any]) -> Any:
            if payload.get("type") == "userVaultEquities":
                return []
            if payload.get("type") == "vaultDetails":
                return {
                    "vaultAddress": "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0",
                    "apr": 0.06,
                    "allowDeposits": True,
                    "isClosed": False,
                    "followerState": None,
                }
            return None

        mock_ex.publicPostInfo.side_effect = _post_info_zero
        mock_ex.privatePostExchange.return_value = {
            "status": "err",
            "response": "Must sign with master wallet",
        }

        exec_engine = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex, execution_mode="TESTNET"
        )
        regime_chop = RegimeState("CHOP", 0.05, 0.05, 0.90, 540)
        res_rejected = asyncio.run(exec_engine.reconcile_cycle(regime_chop, []))

        self.assertEqual(res_rejected["total_equity_usdc"], 999.0)
        self.assertEqual(res_rejected["basis_harvest_notional_usdc"], 0.0)
        snap_rej = self.db.get_latest_portfolio_snapshot()
        self.assertEqual(float(snap_rej["basis_harvest_notional_usdc"]), 0.0)
        self.assertEqual(float(snap_rej["accrued_yield_usdc"]), 0.0)
        self.assertEqual(float(snap_rej["total_equity_usdc"]), 999.0)

        # Verify vaultTransfer deposit payload used 6-decimal micro-USDC ($499.50 -> 499500000)
        mock_ex.privatePostExchange.assert_awaited_once()
        dep_call = mock_ex.privatePostExchange.call_args.args[0]
        self.assertEqual(dep_call["action"]["type"], "vaultTransfer")
        self.assertTrue(dep_call["action"]["isDeposit"])
        self.assertEqual(dep_call["action"]["usd"], 499_500_000)

        # Scenario B: On-chain deposit succeeds -> vault holds $499.50 equity + $1.25 PnL, perp holds $499.50
        exec_engine._last_vault_transfer_attempt_ms = 0
        mock_ex.privatePostExchange.reset_mock()
        mock_ex.privatePostExchange.return_value = {"status": "ok", "response": {"type": "default"}}

        state_step = {"deposited": False}

        async def _post_info_deposit(payload: dict[str, Any]) -> Any:
            if payload.get("type") == "userVaultEquities":
                if state_step["deposited"]:
                    return [
                        {
                            "vaultAddress": "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0",
                            "equity": "500.75",
                            "lockedUntilTimestamp": 1793000000000,
                        }
                    ]
                return []
            if payload.get("type") == "vaultDetails":
                if state_step["deposited"]:
                    return {
                        "vaultAddress": "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0",
                        "apr": 0.08,
                        "allowDeposits": True,
                        "isClosed": False,
                        "followerState": {
                            "vaultEquity": "500.75",
                            "pnl": "1.25",
                            "allTimePnl": "1.25",
                            "lockupUntil": 1793000000000,
                        },
                    }
                return {
                    "vaultAddress": "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0",
                    "apr": 0.08,
                    "allowDeposits": True,
                    "isClosed": False,
                    "followerState": None,
                }
            return None

        async def _accept_deposit(req: dict[str, Any]) -> dict[str, Any]:
            if req["action"]["isDeposit"]:
                state_step["deposited"] = True
                mock_ex.fetch_balance.return_value = {
                    "USDC": {"total": 499.50, "free": 499.50, "used": 0.0},
                    "total": {"USDC": 499.50},
                }
            else:
                state_step["deposited"] = False
                mock_ex.fetch_balance.return_value = {
                    "USDC": {"total": 1000.25, "free": 1000.25, "used": 0.0},
                    "total": {"USDC": 1000.25},
                }
            return {"status": "ok", "response": {"type": "default"}}

        mock_ex.publicPostInfo.side_effect = _post_info_deposit
        mock_ex.privatePostExchange.side_effect = _accept_deposit

        res_dep = asyncio.run(exec_engine.reconcile_cycle(regime_chop, []))
        self.assertEqual(res_dep["basis_harvest_notional_usdc"], 500.75)
        snap_dep = self.db.get_latest_portfolio_snapshot()
        self.assertEqual(float(snap_dep["basis_harvest_notional_usdc"]), 500.75)
        self.assertEqual(float(snap_dep["accrued_yield_usdc"]), 1.25)

        # Scenario C: Disabling basis_harvest_enabled triggers on-chain withdrawal back to Perp account
        self.db.set_config_value("basis_harvest_enabled", False)
        self.db.pit_timestamp_ms = 1793025000000
        res_wd = asyncio.run(exec_engine.reconcile_cycle(regime_chop, []))
        self.assertEqual(res_wd["basis_harvest_notional_usdc"], 0.0)
        wd_call = mock_ex.privatePostExchange.call_args.args[0]
        self.assertEqual(wd_call["action"]["type"], "vaultTransfer")
        self.assertFalse(wd_call["action"]["isDeposit"])
        self.assertEqual(wd_call["action"]["usd"], 500_750_000)

    def test_12_maker_cancel_race_upward_drift_and_orphan_position_reconciliation(self) -> None:
        """
        Verify:
        1. Maker limit cancel race (`cancel_order` raises OrderNotFound because the maker limit filled
           milliseconds before cancel) detects the fill via `fetch_order` and returns MAKER_LIMIT FILLED
           without placing a duplicate MARKET_IOC order.
        2. Initial `postOnly=True` rejection falls back to `MARKET_IOC` when `fallback_to_ioc=True`.
        3. `reconcile_exchange_state()` syncs upward size drift (`on_chain_size > local_qty`) and
           re-opens a prematurely `CLOSED` trade when the on-chain position is still active.
        """
        # 1. Maker cancel race -> no duplicate MARKET_IOC order
        mock_ex_race = AsyncMock()
        mock_ex_race.create_order.return_value = {
            "id": "hl-arb-maker-1",
            "status": "open",
            "price": 0.16482,
            "filled": 0.0,
        }
        mock_ex_race.fetch_order.side_effect = [
            {"id": "hl-arb-maker-1", "status": "open", "filled": 0.0, "price": 0.16482},
            {"id": "hl-arb-maker-1", "status": "closed", "filled": 908.0, "average": 0.16482},
        ]
        mock_ex_race.cancel_order.side_effect = ccxt.OrderNotFound(
            "Order was never placed, already canceled, or filled."
        )
        exec_race = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_race, execution_mode="LIVE"
        )
        res_race = asyncio.run(
            exec_race.execute_maker_limit_chaser(
                symbol="ARB/USDC:USDC",
                side="SHORT",
                quantity=908.0,
                price=0.16482,
                client_order_id="cli-arb-race-1",
                chase_intervals=1,
                chase_interval_seconds=0.0,
                fallback_to_ioc=True,
            )
        )
        self.assertEqual(res_race.fsm_state, "FILLED")
        self.assertTrue(res_race.is_maker)
        self.assertEqual(res_race.fill_type, "MAKER_LIMIT")
        self.assertAlmostEqual(res_race.filled_units, 908.0, places=6)
        self.assertAlmostEqual(res_race.avg_fill_price, 0.16482, places=6)
        self.assertEqual(mock_ex_race.create_order.await_count, 1)

        # 2. Initial postOnly rejection falls back to MARKET_IOC when second call succeeds
        mock_ex_post_only = AsyncMock()
        mock_ex_post_only.create_order.side_effect = [
            ccxt.InvalidOrder("Post only order would have immediately matched."),
            {
                "id": "hl-arb-ioc-2",
                "status": "closed",
                "type": "market",
                "filled": 908.0,
                "average": 0.16216,
            },
        ]
        exec_po = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_post_only, execution_mode="LIVE"
        )
        res_po = asyncio.run(
            exec_po.execute_maker_limit_chaser(
                symbol="ARB/USDC:USDC",
                side="LONG",
                quantity=908.0,
                price=0.16216,
                client_order_id="cli-arb-po-fallback",
                chase_intervals=1,
                chase_interval_seconds=0.0,
                fallback_to_ioc=True,
                reduce_only=True,
            )
        )
        self.assertEqual(res_po.fsm_state, "FILLED")
        self.assertFalse(res_po.is_maker)
        self.assertEqual(res_po.fill_type, "MARKET_IOC_FALLBACK")
        self.assertAlmostEqual(res_po.filled_units, 908.0, places=6)
        self.assertEqual(res_po.state_history, ["PENDING", "EXPIRED", "FILLED"])

        # 3. Upward on-chain size drift (1816.0 > 908.0) and premature CLOSED position reconciliation
        arb_tid = self.db.insert_trade(
            {
                "client_order_id": "aegis-arb-drift",
                "symbol": "ARB/USDC:USDC",
                "base_asset": "ARB",
                "side": "SHORT",
                "status": "OPEN",
                "fsm_state": "FILLED",
                "entry_price": 0.16486,
                "current_price": 0.16404,
                "quantity": 908.0,
                "filled_units": 908.0,
                "notional_usdc": 149.69,
                "risk_budget_usdc": 9.6,
                "initial_stop_price": 0.1809,
                "chandelier_stop_price": 0.1809,
            }
        )
        mock_ex_reconcile = AsyncMock()
        mock_ex_reconcile.fetch_positions.return_value = [
            {
                "symbol": "ARB/USDC:USDC",
                "contracts": 1816.0,
                "side": "short",
                "entryPrice": 0.16484,
                "markPrice": 0.16404,
                "unrealizedPnl": 1.45,
            }
        ]
        exec_rec = ExecutionEngine(
            self.db, self.risk, exchange=mock_ex_reconcile, execution_mode="LIVE"
        )
        rec_up = asyncio.run(exec_rec.reconcile_exchange_state())
        self.assertEqual(rec_up, 1)
        arb_synced = self.db.get_trade_by_client_order_id("aegis-arb-drift")
        assert arb_synced is not None
        self.assertAlmostEqual(float(arb_synced["quantity"]), 1816.0, places=6)
        self.assertAlmostEqual(float(arb_synced["entry_price"]), 0.16484, places=6)

        # Now simulate premature local close while 908.0 ARB remains open on-chain
        self.db.close_trade(
            trade_id=arb_tid,
            exit_price=0.16216,
            exit_reason="INVERTED_CHANDELIER_STOP_SHORT",
        )
        self.assertEqual(len(self.db.get_open_trades()), 0)
        mock_ex_reconcile.fetch_positions.return_value = [
            {
                "symbol": "ARB/USDC:USDC",
                "contracts": 908.0,
                "side": "short",
                "entryPrice": 0.16484,
                "markPrice": 0.16404,
                "unrealizedPnl": 0.7264,
            }
        ]
        rec_reopen = asyncio.run(exec_rec.reconcile_exchange_state())
        self.assertEqual(rec_reopen, 1)
        open_after = self.db.get_open_trades()
        self.assertEqual(len(open_after), 1)
        self.assertEqual(open_after[0]["symbol"], "ARB/USDC:USDC")
        self.assertEqual(open_after[0]["status"], "OPEN")
        self.assertAlmostEqual(float(open_after[0]["quantity"]), 908.0, places=6)
        self.assertAlmostEqual(float(open_after[0]["entry_price"]), 0.16484, places=6)


if __name__ == "__main__":
    unittest.main()


