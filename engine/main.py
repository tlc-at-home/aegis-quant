"""Master asyncio orchestrator for Aegis Quant (v0.1.0-dev)."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path
from typing import Any

import uvicorn

from engine import __platform__, __version__
from engine.calibration import WalkForwardCalibrator
from engine.database import PROJECT_ROOT, DatabaseManager
from engine.execution import ExecutionEngine
from engine.ingestion import MarketIngestionEngine
from engine.risk import RiskEngine
from engine.signals import SignalEngine
from web.server import create_app

LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "engine.log"
MIGRATION_001_PATH = PROJECT_ROOT / "sql" / "migrations" / "001_v020_config_expansion.sql"
MIGRATION_002_PATH = PROJECT_ROOT / "sql" / "migrations" / "002_v020_order_fsm.sql"


def setup_logging() -> logging.Logger:
    """Configure dual file and console operational logging."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ]
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
    return logging.getLogger("aegis.main")


class AegisOrchestrator:
    """Master asyncio orchestrator coordinating ingestion, HMM/MACE signals, risk, and FastAPI."""

    def __init__(self, db: DatabaseManager | None = None) -> None:
        self.logger = setup_logging()
        self.db = db or DatabaseManager()
        self.ingestion = MarketIngestionEngine(self.db)
        self.signals = SignalEngine(self.db)
        self.risk = RiskEngine(self.db)
        self.execution = ExecutionEngine(self.db, self.risk)
        self.calibrator = WalkForwardCalibrator(self.db)
        self._stop_event = asyncio.Event()
        self._cycle_count = 0

    async def run_single_cycle(self) -> dict[str, Any]:
        """Execute one full ingestion, HMM/MACE signal, risk, and execution cycle."""
        self._cycle_count += 1
        ingest_stats = await self.ingestion.ingest_universe()
        regime_state, evaluations = await asyncio.to_thread(self.signals.evaluate_universe)
        exec_stats = await self.execution.reconcile_cycle(regime_state, evaluations)

        if self._cycle_count == 1 or self._cycle_count % 24 == 0:
            await asyncio.to_thread(self.calibrator.calibrate_universe)

        summary = {
            "cycle": self._cycle_count,
            "hmm_regime": regime_state.regime,
            "prob_bull": regime_state.prob_bull,
            "prob_bear": regime_state.prob_bear,
            "prob_chop": regime_state.prob_chop,
            "ingestion": ingest_stats,
            "execution": exec_stats,
        }
        self.db.log_telemetry(
            "INFO",
            "engine.orchestrator",
            f"Cycle #{self._cycle_count} complete | Regime={regime_state.regime} "
            f"(Bull={regime_state.prob_bull:.2f}, Bear={regime_state.prob_bear:.2f}) | "
            f"Equity=${exec_stats['total_equity_usdc']:.2f}",
            summary,
        )
        self.logger.info(
            "Cycle #%d | Regime=%s (Bull=%.2f Bear=%.2f Chop=%.2f) | Equity=$%.2f | Heat=$%.2f",
            self._cycle_count,
            regime_state.regime,
            regime_state.prob_bull,
            regime_state.prob_bear,
            regime_state.prob_chop,
            exec_stats["total_equity_usdc"],
            exec_stats["portfolio_heat_usdc"],
        )
        return summary

    async def _trading_loop(self) -> None:
        """Continuous background quantitative loop."""
        while not self._stop_event.is_set():
            interval = 15
            try:
                await self.run_single_cycle()
                cfg = self.db.load_config()
                interval = max(5, int(cfg.get("polling_interval_seconds", 15)))
            except Exception as exc:
                self.logger.exception("Error in quantitative trading cycle: %s", exc)
                try:
                    self.db.log_telemetry("ERROR", "engine.orchestrator", str(exc))
                except Exception:
                    pass

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=float(interval))
            except asyncio.TimeoutError:
                pass

    async def run_forever(self) -> None:
        """Initialize SQLite WAL, launch FastAPI on :8000, and run the trading engine loop."""
        integrity = self.db.init_db()
        if MIGRATION_001_PATH.exists():
            integrity = self.db.apply_migration(MIGRATION_001_PATH)
        if MIGRATION_002_PATH.exists():
            integrity = self.db.apply_migration(MIGRATION_002_PATH)
        self.logger.info(
            "Starting %s v%s | SQLite journal_mode=%s | config=%d | USDC_PERP=%d | BTC_SYNTH=%d",
            __platform__,
            __version__,
            integrity["journal_mode"],
            integrity["system_config_count"],
            integrity["v_universe_usdc_perp_count"],
            integrity["v_universe_btc_synth_count"],
        )
        self.db.log_telemetry(
            "INFO",
            "engine.startup",
            f"Initialized {__platform__} v{__version__}",
            integrity,
        )

        cfg = self.db.load_config()
        port = int(cfg.get("web_server_port", 8000))
        app = create_app(self.db)

        uvi_config = uvicorn.Config(
            app=app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
            access_log=False,
        )
        server = uvicorn.Server(uvi_config)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_shutdown)
            except NotImplementedError:
                pass

        trading_task = asyncio.create_task(self._trading_loop())
        server_task = asyncio.create_task(server.serve())
        stop_task = asyncio.create_task(self._stop_event.wait())

        await asyncio.wait(
            [trading_task, server_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        self._stop_event.set()
        server.should_exit = True
        trading_task.cancel()
        stop_task.cancel()
        await asyncio.gather(trading_task, server_task, stop_task, return_exceptions=True)
        await self.ingestion.close()

    def request_shutdown(self) -> None:
        """Signal graceful shutdown."""
        self._stop_event.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="Aegis Quant v0.2.0-alpha2 Orchestrator")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run database initialization and a single quantitative cycle then exit",
    )
    parser.add_argument(
        "--show-credentials",
        action="store_true",
        help="Display masked Hyperliquid Web3 DEX credential and routing status then exit",
    )
    parser.add_argument(
        "--configure-only",
        action="store_true",
        help="Apply credential/mode flags directly to SQLite system_config then exit",
    )
    parser.add_argument(
        "--execution-mode",
        type=str,
        choices=["PAPER", "LIVE", "TESTNET", "paper", "live", "testnet"],
        default=None,
        help="Set execution_mode in system_config (PAPER, LIVE, or TESTNET)",
    )
    net_group = parser.add_mutually_exclusive_group()
    net_group.add_argument(
        "--testnet",
        action="store_true",
        help="Route Hyperliquid L1 orders & data to Testnet (https://api.hyperliquid-testnet.xyz)",
    )
    net_group.add_argument(
        "--mainnet",
        action="store_true",
        help="Route Hyperliquid L1 orders & data to Mainnet (https://api.hyperliquid.xyz)",
    )
    parser.add_argument(
        "--wallet-address",
        type=str,
        default=None,
        help="Main Arbitrum Wallet Address (0x...) holding USDC collateral",
    )
    parser.add_argument(
        "--private-key",
        type=str,
        default=None,
        help="Trade-Only API Agent Wallet Private Key (64-hex / 0x...)",
    )
    parser.add_argument(
        "--vault-address",
        type=str,
        default=None,
        help="Optional Hyperliquid Vault or sub-account address (0x...)",
    )
    parser.add_argument(
        "--gemini-api-key",
        type=str,
        default=None,
        help="Optional Google Gemini API key for telemetry diagnostics",
    )
    args = parser.parse_args()

    orchestrator = AegisOrchestrator()
    orchestrator.db.init_db()
    if MIGRATION_001_PATH.exists():
        orchestrator.db.apply_migration(MIGRATION_001_PATH)
    if MIGRATION_002_PATH.exists():
        orchestrator.db.apply_migration(MIGRATION_002_PATH)

    testnet_flag: bool | None = None
    if args.testnet:
        testnet_flag = True
    elif args.mainnet:
        testnet_flag = False

    has_cred_updates = any(
        x is not None
        for x in (
            args.wallet_address,
            args.private_key,
            testnet_flag,
            args.execution_mode,
            args.vault_address,
            args.gemini_api_key,
        )
    )
    if has_cred_updates:
        status = orchestrator.db.configure_hyperliquid_credentials(
            wallet_address=args.wallet_address,
            private_key=args.private_key,
            hyperliquid_testnet=testnet_flag,
            execution_mode=args.execution_mode,
            hyperliquid_vault_address=args.vault_address,
            gemini_api_key=args.gemini_api_key,
            require_credentials_for_live=True,
        )
        orchestrator.db.log_telemetry(
            "INFO",
            "engine.cli",
            f"Updated Hyperliquid credentials via CLI | routing={status['active_routing']}",
            status,
        )

    if args.show_credentials or args.configure_only:
        verification = asyncio.run(orchestrator.execution.verify_dex_credentials())
        print(json.dumps(verification, indent=2))
        return

    if args.once:
        asyncio.run(orchestrator.run_single_cycle())
        asyncio.run(orchestrator.ingestion.close())
    else:
        asyncio.run(orchestrator.run_forever())


if __name__ == "__main__":
    main()
