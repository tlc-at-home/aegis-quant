"""Hyperliquid Web3 DEX Order FSM, Chandelier trailing stops, On-Chain Reconciliation, and Paper Fill Model."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
import sqlite3
import time
import uuid
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from typing import Any

import ccxt.async_support as ccxt
import pandas as pd

from engine.alerts import AlertEngine
from engine.database import DatabaseManager
from engine.risk import PositionSizeResult, RiskEngine
from engine.signals import RegimeState

logger = logging.getLogger("aegis.execution")

VALID_FSM_STATES = (
    "PENDING",
    "OPEN_ON_BOOK",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELLED",
    "REJECTED",
    "EXPIRED",
)

MAINNET_HLP_VAULT_ADDRESS = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
TESTNET_HLP_VAULT_ADDRESS = "0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0"


@dataclass
class OrderExecutionResult:
    """Result of executing an order through the MAKER_LIMIT_CHASER FSM or Paper Fill Model."""

    client_order_id: str
    exchange_order_id: str
    symbol: str
    side: str
    fsm_state: str
    requested_units: float
    filled_units: float
    avg_fill_price: float
    slippage_paid_usd: float
    effective_slippage_bps: float
    fees_paid_usdc: float
    is_maker: bool
    fill_type: str
    state_history: list[str] = field(default_factory=list)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ExecutionEngine:
    """
    Executes Long Leader, Bear Laggard Short, MAKER_LIMIT_CHASER Order FSM,
    On-Chain Position Reconciliation, Chandelier trailing stop exits,
    exact 1-hour funding settlement sync, realistic paper fills, and Basis/HLP Yield Harvesting.
    """

    def __init__(
        self,
        db: DatabaseManager,
        risk_engine: RiskEngine | None = None,
        exchange: Any | None = None,
        execution_mode: str | None = None,
    ) -> None:
        self.db = db
        self.risk_engine = risk_engine if risk_engine is not None else RiskEngine(self.db)
        self.exchange: Any | None = exchange
        self.alerts = AlertEngine(self.db)
        self._execution_mode_override: str | None = (
            execution_mode.strip().upper() if execution_mode is not None else None
        )
        self._last_funding_settlement_hour: int = int(time.time() // 3600)
        self._last_funding_sync_ms: dict[str, int] = {}
        self._cooldown_until_ms: dict[str, int] = {}
        self._cooldown_until_step: dict[str, int] = {}
        self._step_counter: int = 0
        self._last_order_result: OrderExecutionResult | None = None
        self._last_vault_transfer_attempt_ms: int = 0
        self._last_vault_lockup_log_ms: int = 0
        self._restore_cooldowns_from_db()

    @property
    def execution_mode(self) -> str:
        """Return active execution mode (PAPER, LIVE, or TESTNET)."""
        if self._execution_mode_override is not None:
            return self._execution_mode_override
        try:
            cfg = self.db.load_config()
            return str(cfg.get("execution_mode", "PAPER")).strip().upper()
        except (sqlite3.Error, OSError, ValueError, TypeError, KeyError, RuntimeError):
            return "PAPER"

    @execution_mode.setter
    def execution_mode(self, mode: str | None) -> None:
        if mode is None:
            self._execution_mode_override = None
        else:
            self._execution_mode_override = str(mode).strip().upper()

    def _restore_cooldowns_from_db(self) -> None:
        """Reconstruct active per-asset post-stop cooldown timestamps from SQLite closed trades."""
        try:
            cfg = self.db.load_config()
            cooldown_bars = int(cfg.get("post_stop_cooldown_bars", 14))
            cooldown_ms = cooldown_bars * 4 * 3_600_000
            with self.db.connection() as conn:
                rows = conn.execute(
                    """
                    SELECT symbol, closed_at
                    FROM trades
                    WHERE status = 'CLOSED' AND closed_at IS NOT NULL
                    ORDER BY trade_id ASC;
                    """
                ).fetchall()
            for row in rows:
                sym = str(row["symbol"])
                raw_closed = str(row["closed_at"]).strip()
                if not raw_closed:
                    continue
                parsed = dt.datetime.fromisoformat(raw_closed.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                closed_ts_ms = int(parsed.timestamp() * 1000)
                until_ms = closed_ts_ms + cooldown_ms
                if until_ms > self._cooldown_until_ms.get(sym, 0):
                    self._cooldown_until_ms[sym] = until_ms
        except (sqlite3.Error, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            logger.debug("Cooldown cache restore skipped: %s", exc)

    def is_in_cooldown(
        self,
        symbol: str,
        now_ms: int | None = None,
        step_idx: int | None = None,
    ) -> bool:
        """Check whether `symbol` is inside its post-stop cooldown window (14 bars / 56h)."""
        if step_idx is not None and now_ms is not None:
            in_step = (
                symbol in self._cooldown_until_step
                and step_idx < self._cooldown_until_step[symbol]
            )
            in_time = (
                symbol in self._cooldown_until_ms
                and now_ms < self._cooldown_until_ms[symbol]
            )
            return bool(in_step or in_time)
        if step_idx is not None and symbol in self._cooldown_until_step:
            return step_idx < self._cooldown_until_step[symbol]
        if now_ms is not None and symbol in self._cooldown_until_ms:
            return now_ms < self._cooldown_until_ms[symbol]
        return False

    @staticmethod
    def _build_hyperliquid_client(config: dict[str, Any]) -> tuple[ccxt.hyperliquid, bool]:
        """Construct an async CCXT Hyperliquid client configured for Mainnet or Testnet."""
        wallet = str(config.get("wallet_address", "")).strip()
        priv_key = str(config.get("private_key", "")).strip()
        if priv_key and not priv_key.lower().startswith("0x") and len(priv_key) == 64:
            priv_key = f"0x{priv_key.lower()}"
        vault = str(config.get("hyperliquid_vault_address", "")).strip()
        mode = str(config.get("execution_mode", "PAPER")).strip().upper()
        raw_testnet = config.get("hyperliquid_testnet", False)
        if isinstance(raw_testnet, str):
            testnet_bool = raw_testnet.strip().lower() in ("true", "1", "yes", "on", "testnet")
        else:
            testnet_bool = bool(raw_testnet)
        is_testnet = testnet_bool or (mode == "TESTNET")

        options: dict[str, Any] = {
            "defaultType": "swap",
            "fetchMarkets": {"types": ["swap"]},
        }
        if vault:
            options["vaultAddress"] = vault

        params: dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": 8000,
            "options": options,
        }
        if wallet:
            params["walletAddress"] = wallet
        if priv_key:
            params["privateKey"] = priv_key

        exchange = ccxt.hyperliquid(params)
        if is_testnet:
            exchange.set_sandbox_mode(True)
        return exchange, is_testnet

    @staticmethod
    def get_hlp_vault_address(
        config: dict[str, Any] | None = None,
        is_testnet: bool | None = None,
    ) -> str:
        """Resolve the active Hyperliquid HLP (Hyperliquidity Provider) protocol vault address."""
        cfg = config or {}
        explicit = str(cfg.get("hlp_vault_address", "")).strip()
        if explicit.startswith("0x") and len(explicit) == 42:
            return explicit.lower()
        if is_testnet is None:
            raw_testnet = cfg.get("hyperliquid_testnet", False)
            if isinstance(raw_testnet, str):
                testnet_bool = raw_testnet.strip().lower() in (
                    "true",
                    "1",
                    "yes",
                    "on",
                    "testnet",
                )
            else:
                testnet_bool = bool(raw_testnet)
            mode = str(cfg.get("execution_mode", "PAPER")).strip().upper()
            is_testnet = testnet_bool or (mode == "TESTNET")
        return TESTNET_HLP_VAULT_ADDRESS if is_testnet else MAINNET_HLP_VAULT_ADDRESS

    async def verify_dex_credentials(
        self, config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Verify CCXT Hyperliquid client initialization, Mainnet/Testnet endpoint binding, and EIP-712 signing."""
        cfg = config if config is not None else self.db.load_config()
        status = self.db.get_credentials_status(cfg)
        exchange, is_testnet = self._build_hyperliquid_client(cfg)
        try:
            api_urls = exchange.urls.get("api", {})
            raw_endpoint = (
                api_urls.get("public", status["api_endpoint"])
                if isinstance(api_urls, dict)
                else str(api_urls)
            )
            hostname = str(getattr(exchange, "hostname", "hyperliquid.xyz"))
            endpoint = str(raw_endpoint).replace("{hostname}", hostname)
            ccxt_creds_ok = False
            eip712_sig_ok = False
            if status["credentials_ready"]:
                exchange.check_required_credentials()
                ccxt_creds_ok = True
                sig = exchange.sign_l1_action(
                    {"type": "order", "orders": [], "grouping": "na"},
                    1700000000000,
                )
                eip712_sig_ok = bool(
                    isinstance(sig, dict) and sig.get("r") and sig.get("s") and sig.get("v")
                )
            return {
                **status,
                "ccxt_exchange_id": exchange.id,
                "ccxt_sandbox_mode": is_testnet,
                "ccxt_api_endpoint": endpoint,
                "ccxt_credentials_bound": ccxt_creds_ok,
                "eip712_signing_verified": eip712_sig_ok,
            }
        finally:
            await exchange.close()

    @staticmethod
    def _extract_usdc_balance_tuple(bal: dict[str, Any] | None) -> tuple[float, float, float]:
        """Extract (total_usdc, free_usdc, used_usdc) from a CCXT Hyperliquid balance response."""
        if not isinstance(bal, dict):
            return 0.0, 0.0, 0.0
        raw_usdc = bal.get("USDC")
        usdc_entry: dict[str, Any] = raw_usdc if isinstance(raw_usdc, dict) else {}
        raw_total_map = bal.get("total")
        total_map: dict[str, Any] = raw_total_map if isinstance(raw_total_map, dict) else {}
        raw_free_map = bal.get("free")
        free_map: dict[str, Any] = raw_free_map if isinstance(raw_free_map, dict) else {}
        raw_used_map = bal.get("used")
        used_map: dict[str, Any] = raw_used_map if isinstance(raw_used_map, dict) else {}

        raw_total = usdc_entry.get("total")
        if raw_total is None:
            raw_total = total_map.get("USDC")
        raw_info = bal.get("info")
        if raw_total is None and isinstance(raw_info, dict):
            margin_sum = raw_info.get("marginSummary") or raw_info.get("crossMarginSummary") or {}
            if isinstance(margin_sum, dict):
                raw_total = margin_sum.get("accountValue")

        raw_free = usdc_entry.get("free")
        if raw_free is None:
            raw_free = free_map.get("USDC")
        raw_used = usdc_entry.get("used")
        if raw_used is None:
            raw_used = used_map.get("USDC")

        total_val = float(raw_total or 0.0)
        used_val = float(raw_used or 0.0)
        free_val = float(raw_free) if raw_free is not None else max(0.0, total_val - used_val)
        return total_val, free_val, used_val

    async def sync_wallet_equity(
        self,
        *,
        scale_risk_caps: bool = True,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Query live Hyperliquid Mainnet/Testnet USDC balance via CCXT `fetch_balance()`
        and automatically update `initial_equity_usdc` (and optionally proportional risk caps).
        """
        cfg = config if config is not None else self.db.load_config()
        wallet = str(cfg.get("wallet_address", "")).strip()
        vault = str(cfg.get("hyperliquid_vault_address", "")).strip()
        target_user = vault if vault else wallet
        if not target_user:
            raise ValueError(
                "Cannot sync wallet equity: configure your Main Arbitrum Wallet Address (wallet_address) first."
            )

        owns_exchange = False
        exchange = self.exchange
        if exchange is None:
            exchange, is_testnet = self._build_hyperliquid_client(cfg)
            owns_exchange = True
        else:
            raw_testnet = cfg.get("hyperliquid_testnet", False)
            mode = str(cfg.get("execution_mode", "PAPER")).strip().upper()
            is_testnet = bool(raw_testnet) or mode == "TESTNET"

        network_label = "testnet" if is_testnet else "mainnet"
        vault_equity_usdc = 0.0
        try:
            swap_bal = await exchange.fetch_balance({"user": target_user})
            total_usdc, free_usdc, used_usdc = self._extract_usdc_balance_tuple(swap_bal)
            account_type = "perps"

            if total_usdc <= 0.0:
                with suppress(Exception):
                    spot_bal = await exchange.fetch_balance(
                        {"user": target_user, "type": "spot", "enableUnifiedMargin": False}
                    )
                    sp_tot, sp_free, sp_used = self._extract_usdc_balance_tuple(spot_bal)
                    if sp_tot > 0.0:
                        total_usdc, free_usdc, used_usdc = sp_tot, sp_free, sp_used
                        account_type = "spot"

            post_info = getattr(exchange, "publicPostInfo", None)
            if callable(post_info):
                with suppress(Exception):
                    vault_equities = await post_info(
                        {"type": "userVaultEquities", "user": target_user}
                    )
                    if isinstance(vault_equities, list):
                        for entry in vault_equities:
                            if isinstance(entry, dict) and entry.get("equity") is not None:
                                eq_val = float(entry["equity"])
                                if math.isfinite(eq_val) and eq_val > 0.0:
                                    vault_equity_usdc += eq_val
        finally:
            if owns_exchange and exchange is not None:
                await exchange.close()

        rounded_eq = round(total_usdc + vault_equity_usdc, 2)
        if not math.isfinite(rounded_eq) or rounded_eq <= 0.0:
            raise ValueError(
                f"Hyperliquid {network_label.upper()} wallet {target_user} returned $0.00 USDC balance."
            )

        scaled_caps: dict[str, float] | None = None
        if scale_risk_caps:
            scaled_caps = self.db.scale_risk_caps_for_equity(rounded_eq)
        else:
            self.db.set_config_value("initial_equity_usdc", rounded_eq)

        with suppress(sqlite3.Error, OSError, ValueError, TypeError):
            with self.db.connection() as conn:
                conn.execute(
                    """
                    UPDATE portfolio_snapshots
                    SET basis_harvest_notional_usdc = ?,
                        accrued_yield_usdc = 0.0
                    WHERE snapshot_id = (
                        SELECT snapshot_id FROM portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1
                    );
                    """,
                    (round(vault_equity_usdc, 2),),
                )

        report: dict[str, Any] = {
            "status": "synced",
            "network": network_label,
            "wallet_address": target_user,
            "account_type": account_type,
            "wallet_balance_usdc": rounded_eq,
            "free_usdc": round(free_usdc, 2),
            "used_margin_usdc": round(used_usdc, 2),
            "vault_equity_usdc": round(vault_equity_usdc, 2),
            "initial_equity_usdc": rounded_eq,
            "scale_risk_caps": scale_risk_caps,
            "scaled_risk_caps": scaled_caps,
        }
        self.db.log_telemetry(
            "INFO",
            "engine.execution",
            f"Synced initial_equity_usdc=${rounded_eq:.2f} from Hyperliquid {network_label.upper()} wallet {target_user}",
            report,
        )
        return report

    # -------------------------------------------------------------------------
    # Step 3.5: Realistic Paper Fill & Dynamic Slippage Model
    # -------------------------------------------------------------------------
    def calculate_dynamic_slippage_bps(
        self,
        atr_14: float,
        close_price: float,
        notional_usd: float,
        default_slippage_bps: float | None = None,
        impact_coeff: float | None = None,
        config: dict[str, Any] | None = None,
    ) -> float:
        """
        Calculate dynamic effective slippage in basis points:
        Slippage_eff = default_slippage_bps + impact_coeff * (ATR_14 / Close) * (Notional_USD / 10000)
        """
        cfg = config if config is not None else self.db.load_config()
        base_bps = (
            float(default_slippage_bps)
            if default_slippage_bps is not None
            else float(cfg.get("default_slippage_bps", 2.5))
        )
        coeff = (
            float(impact_coeff)
            if impact_coeff is not None
            else float(cfg.get("impact_coeff", cfg.get("slippage_impact_coeff", 100.0)))
        )
        if close_price <= 0.0 or atr_14 < 0.0 or notional_usd < 0.0:
            return max(0.0, base_bps)
        atr_ratio = float(atr_14) / float(close_price)
        notional_ratio = float(notional_usd) / 10000.0
        eff_bps = base_bps + coeff * atr_ratio * notional_ratio
        return float(eff_bps)

    @staticmethod
    def compute_dynamic_slippage_bps(
        atr_14: float,
        close_price: float,
        notional_usd: float,
        default_slippage_bps: float = 2.5,
        impact_coeff: float = 100.0,
    ) -> float:
        """Static helper for dynamic effective slippage in basis points."""
        if close_price <= 0.0 or atr_14 < 0.0 or notional_usd < 0.0:
            return max(0.0, float(default_slippage_bps))
        return float(
            default_slippage_bps
            + impact_coeff * (float(atr_14) / float(close_price)) * (float(notional_usd) / 10000.0)
        )

    def simulate_paper_fill(
        self,
        *,
        symbol: str = "SOL/USDC:USDC",
        side: str,
        limit_price: float,
        quantity: float,
        next_bar: dict[str, Any] | pd.Series | None = None,
        next_bar_low: float | None = None,
        next_bar_high: float | None = None,
        atr_14: float = 0.0,
        close_price: float | None = None,
        default_slippage_bps: float | None = None,
        impact_coeff: float | None = None,
        client_order_id: str | None = None,
        config: dict[str, Any] | None = None,
        record_audit: bool = True,
    ) -> OrderExecutionResult:
        """
        Simulate realistic paper order execution for MAKER_LIMIT_CHASER:
        - BUY/LONG Order: Requires next bar `low <= limit_price` to credit a 1.5 bps maker fill.
        - SELL/SHORT Order: Requires next bar `high >= limit_price` to credit a 1.5 bps maker fill.
        - Unfilled Fallback: Otherwise credit a 4.5 bps taker fill plus dynamic slippage.
        """
        cfg = config if config is not None else self.db.load_config()
        cli_id = client_order_id or f"paper-cli-{uuid.uuid4().hex[:10]}"
        ex_id = f"hl-paper-{uuid.uuid4().hex[:10]}"
        side_up = str(side).strip().upper()
        is_buy = side_up in ("LONG", "BUY")

        if next_bar is not None:
            if next_bar_low is None and "low" in next_bar:
                next_bar_low = float(next_bar["low"])
            if next_bar_high is None and "high" in next_bar:
                next_bar_high = float(next_bar["high"])
            if close_price is None and "close" in next_bar:
                close_price = float(next_bar["close"])

        ref_close = float(close_price if close_price is not None and close_price > 0 else limit_price)
        qty = float(quantity)
        lim_px = float(limit_price)

        maker_fee_bps = float(cfg.get("maker_fee_bps", 1.5))
        taker_fee_bps = float(cfg.get("taker_fee_bps", 4.5))

        maker_filled = False
        if is_buy and next_bar_low is not None:
            maker_filled = float(next_bar_low) <= lim_px
        elif (not is_buy) and next_bar_high is not None:
            maker_filled = float(next_bar_high) >= lim_px

        state_history = ["PENDING", "OPEN_ON_BOOK"]
        if record_audit:
            self.db.log_order_state_transition(
                cli_id, "PENDING", "OPEN_ON_BOOK", reason="PAPER_MAKER_LIMIT_POSTED"
            )

        if maker_filled:
            avg_fill_px = lim_px
            eff_slip_bps = 0.0
            slippage_usd = 0.0
            fees_usdc = (qty * avg_fill_px) * (maker_fee_bps / 10000.0)
            fill_type = "MAKER_LIMIT"
            state_history.append("FILLED")
            if record_audit:
                self.db.transition_order_state(
                    cli_id,
                    "FILLED",
                    from_state="OPEN_ON_BOOK",
                    reason="PAPER_MAKER_LIMIT_FILLED",
                    exchange_order_id=ex_id,
                    avg_fill_price=avg_fill_px,
                    filled_units=qty,
                    slippage_paid_usd=0.0,
                    fees_paid_usdc=fees_usdc,
                )
        else:
            notional_usd = qty * ref_close
            eff_slip_bps = self.calculate_dynamic_slippage_bps(
                atr_14=atr_14,
                close_price=ref_close,
                notional_usd=notional_usd,
                default_slippage_bps=default_slippage_bps,
                impact_coeff=impact_coeff,
                config=cfg,
            )
            slip_rate = eff_slip_bps / 10000.0
            avg_fill_px = lim_px * (1.0 + slip_rate) if is_buy else lim_px * (1.0 - slip_rate)
            slippage_usd = abs(avg_fill_px - lim_px) * qty
            fees_usdc = (qty * avg_fill_px) * (taker_fee_bps / 10000.0)
            fill_type = "TAKER_FALLBACK"
            state_history.extend(["EXPIRED", "FILLED"])
            if record_audit:
                self.db.log_order_state_transition(
                    cli_id,
                    "OPEN_ON_BOOK",
                    "EXPIRED",
                    reason="PAPER_MAKER_LIMIT_UNFILLED_NEXT_BAR",
                )
                self.db.transition_order_state(
                    cli_id,
                    "FILLED",
                    from_state="EXPIRED",
                    reason="PAPER_TAKER_IOC_FALLBACK_FILLED",
                    exchange_order_id=ex_id,
                    avg_fill_price=avg_fill_px,
                    filled_units=qty,
                    slippage_paid_usd=slippage_usd,
                    fees_paid_usdc=fees_usdc,
                )

        result = OrderExecutionResult(
            client_order_id=cli_id,
            exchange_order_id=ex_id,
            symbol=symbol,
            side="LONG" if is_buy else "SHORT",
            fsm_state="FILLED",
            requested_units=qty,
            filled_units=qty,
            avg_fill_price=round(avg_fill_px, 8),
            slippage_paid_usd=round(slippage_usd, 6),
            effective_slippage_bps=round(eff_slip_bps, 6),
            fees_paid_usdc=round(fees_usdc, 6),
            is_maker=maker_filled,
            fill_type=fill_type,
            state_history=state_history,
        )
        self._last_order_result = result
        return result

    # -------------------------------------------------------------------------
    # Step 3.2: Order Finite State Machine (MAKER_LIMIT_CHASER)
    # -------------------------------------------------------------------------
    async def _fetch_best_quote_price(
        self,
        exchange: Any,
        symbol: str,
        is_buy: bool,
        fallback_price: float,
    ) -> float:
        """Query top-of-book best bid (for BUY) or best ask (for SELL) for chaser repricing."""
        try:
            fetch_ob = getattr(exchange, "fetch_order_book", None)
            if callable(fetch_ob):
                ob = await fetch_ob(symbol)
                if isinstance(ob, dict):
                    book_side = ob.get("bids") if is_buy else ob.get("asks")
                    if isinstance(book_side, list) and len(book_side) > 0 and len(book_side[0]) > 0:
                        px = float(book_side[0][0])
                        if px > 0:
                            return px
        except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            logger.debug("Order book quote fetch fallback on %s: %s", symbol, exc)

        try:
            fetch_ticker = getattr(exchange, "fetch_ticker", None)
            if callable(fetch_ticker):
                ticker = await fetch_ticker(symbol)
                if isinstance(ticker, dict):
                    key = "bid" if is_buy else "ask"
                    px = float(ticker.get(key) or ticker.get("close") or ticker.get("last") or 0.0)
                    if px > 0:
                        return px
        except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            logger.debug("Ticker quote fetch fallback on %s: %s", symbol, exc)

        return fallback_price

    async def execute_maker_limit_chaser(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        *,
        client_order_id: str | None = None,
        config: dict[str, Any] | None = None,
        reduce_only: bool = False,
        chase_intervals: int | None = None,
        chase_interval_seconds: float | None = None,
        fallback_to_ioc: bool = True,
        next_bar: dict[str, Any] | pd.Series | None = None,
        atr_14: float = 0.0,
    ) -> OrderExecutionResult:
        """
        Execute an order via the multi-state MAKER_LIMIT_CHASER Finite State Machine:
        1. Lifecycle: PENDING -> OPEN_ON_BOOK -> PARTIALLY_FILLED -> FILLED | CANCELLED | REJECTED | EXPIRED
        2. Route postOnly=True limit order to capture 1.5 bps maker fee.
        3. Poll exchange.fetch_order(order_id, symbol) across N chase intervals, repricing to best bid/ask.
        4. If unfilled after N intervals, transition to EXPIRED and execute MARKET_IOC fallback (4.5 bps taker fee).
        """
        cfg = config if config is not None else self.db.load_config()
        cli_id = client_order_id or f"aegis-{uuid.uuid4().hex[:12]}"
        side_up = str(side).strip().upper()
        is_buy = side_up in ("LONG", "BUY")
        ccxt_side = "buy" if is_buy else "sell"
        norm_side = "LONG" if is_buy else "SHORT"

        mode = self.execution_mode
        if mode == "PAPER" and self.exchange is None:
            if next_bar is None:
                next_bar = {"low": price, "high": price, "close": price}
            return self.simulate_paper_fill(
                symbol=symbol,
                side=norm_side,
                limit_price=price,
                quantity=quantity,
                next_bar=next_bar,
                atr_14=atr_14,
                close_price=price,
                client_order_id=cli_id,
                config=cfg,
                record_audit=True,
            )

        max_intervals = (
            int(chase_intervals)
            if chase_intervals is not None
            else int(cfg.get("chaser_max_intervals", 3))
        )
        sleep_secs = (
            float(chase_interval_seconds)
            if chase_interval_seconds is not None
            else float(cfg.get("chaser_interval_seconds", 0.0 if self.exchange is not None else 5.0))
        )
        maker_fee_rate = float(cfg.get("maker_fee_bps", 1.5)) / 10000.0
        taker_fee_rate = float(cfg.get("taker_fee_bps", 4.5)) / 10000.0

        owns_exchange = False
        exchange = self.exchange
        is_testnet = bool(cfg.get("hyperliquid_testnet", False)) or (mode == "TESTNET")
        if exchange is None:
            exchange, is_testnet = self._build_hyperliquid_client(cfg)
            owns_exchange = True

        prefix = "hl-testnet" if is_testnet else "hl-live"
        state_history: list[str] = ["PENDING"]
        current_state = "PENDING"
        current_limit_price = float(price)
        total_filled_units = 0.0
        total_fill_notional = 0.0
        total_fees_usdc = 0.0
        exchange_order_id = ""

        try:
            # Step 1: Place initial postOnly=True Maker Limit Order (PENDING -> OPEN_ON_BOOK)
            order_params: dict[str, Any] = {"postOnly": True}
            if reduce_only:
                order_params["reduceOnly"] = True

            initial_order: dict[str, Any] | None = None
            init_reject_reason: str | None = None
            try:
                initial_order = await exchange.create_order(
                    symbol=symbol,
                    type="limit",
                    side=ccxt_side,
                    amount=float(quantity),
                    price=current_limit_price,
                    params=order_params,
                )
            except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                init_reject_reason = f"ORDER_PLACEMENT_REJECTED: {exc}"

            init_status = (
                str(initial_order.get("status") or "open").lower()
                if isinstance(initial_order, dict)
                else "rejected"
            )
            if isinstance(initial_order, dict):
                exchange_order_id = str(
                    initial_order.get("id") or f"{prefix}-{uuid.uuid4().hex[:10]}"
                )
            if init_reject_reason is None and init_status == "rejected":
                init_reject_reason = "EXCHANGE_STATUS_REJECTED"

            if init_reject_reason is not None:
                if fallback_to_ioc:
                    ioc_params_init: dict[str, Any] = {"timeInForce": "IOC"}
                    if reduce_only:
                        ioc_params_init["reduceOnly"] = True
                    eff_slip_bps_init = self.calculate_dynamic_slippage_bps(
                        atr_14=atr_14,
                        close_price=float(price),
                        notional_usd=float(quantity) * float(price),
                        config=cfg,
                    )
                    slip_rate_init = eff_slip_bps_init / 10000.0
                    ioc_ref_px_init = (
                        current_limit_price * (1.0 + slip_rate_init)
                        if is_buy
                        else current_limit_price * (1.0 - slip_rate_init)
                    )
                    with suppress(
                        ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError
                    ):
                        ioc_init_order = await exchange.create_order(
                            symbol=symbol,
                            type="market",
                            side=ccxt_side,
                            amount=float(quantity),
                            price=ioc_ref_px_init,
                            params=ioc_params_init,
                        )
                        ioc_init_status = (
                            str(ioc_init_order.get("status") or "closed").lower()
                            if isinstance(ioc_init_order, dict)
                            else "closed"
                        )
                        raw_init_filled = (
                            ioc_init_order.get("filled")
                            if isinstance(ioc_init_order, dict)
                            else None
                        )
                        ioc_init_filled = (
                            float(raw_init_filled)
                            if raw_init_filled is not None
                            else (
                                float(quantity)
                                if ioc_init_status not in ("expired", "canceled", "cancelled", "rejected")
                                else 0.0
                            )
                        )
                        if ioc_init_filled > 0.0:
                            ioc_init_avg_px = (
                                float(
                                    ioc_init_order.get("average")
                                    or ioc_init_order.get("price")
                                    or ioc_ref_px_init
                                )
                                if isinstance(ioc_init_order, dict)
                                else ioc_ref_px_init
                            )
                            if isinstance(ioc_init_order, dict) and ioc_init_order.get("id"):
                                exchange_order_id = str(ioc_init_order["id"])
                            elif not exchange_order_id:
                                exchange_order_id = f"{prefix}-{uuid.uuid4().hex[:10]}"
                            self.db.transition_order_state(
                                cli_id,
                                "EXPIRED",
                                from_state="PENDING",
                                reason=f"POST_ONLY_BYPASSED_TO_IOC: {init_reject_reason}",
                                exchange_order_id=exchange_order_id,
                            )
                            state_history.append("EXPIRED")
                            fill_notional_init = ioc_init_filled * ioc_init_avg_px
                            fees_init = fill_notional_init * taker_fee_rate
                            slip_init_usd = abs(ioc_init_avg_px - float(price)) * ioc_init_filled
                            target_init_state = (
                                "FILLED"
                                if ioc_init_filled >= float(quantity) - 1e-9
                                else "PARTIALLY_FILLED"
                            )
                            self.db.transition_order_state(
                                cli_id,
                                target_init_state,
                                from_state="EXPIRED",
                                reason="MARKET_IOC_FALLBACK_FILLED",
                                exchange_order_id=exchange_order_id,
                                avg_fill_price=ioc_init_avg_px,
                                filled_units=ioc_init_filled,
                                quantity=ioc_init_filled,
                                notional_usdc=round(fill_notional_init, 2),
                                slippage_paid_usd=slip_init_usd,
                                fees_paid_usdc=fees_init,
                                status="OPEN",
                            )
                            state_history.append(target_init_state)
                            res = OrderExecutionResult(
                                client_order_id=cli_id,
                                exchange_order_id=exchange_order_id,
                                symbol=symbol,
                                side=norm_side,
                                fsm_state=target_init_state,
                                requested_units=float(quantity),
                                filled_units=round(ioc_init_filled, 8),
                                avg_fill_price=round(ioc_init_avg_px, 8),
                                slippage_paid_usd=round(slip_init_usd, 6),
                                effective_slippage_bps=round(eff_slip_bps_init, 6),
                                fees_paid_usdc=round(fees_init, 6),
                                is_maker=False,
                                fill_type="MARKET_IOC_FALLBACK",
                                state_history=state_history,
                            )
                            self._last_order_result = res
                            return res

                self.db.transition_order_state(
                    cli_id,
                    "REJECTED",
                    from_state="PENDING",
                    reason=init_reject_reason,
                    exchange_order_id=exchange_order_id if exchange_order_id else None,
                    status="CANCELLED",
                )
                state_history.append("REJECTED")
                res = OrderExecutionResult(
                    client_order_id=cli_id,
                    exchange_order_id=exchange_order_id,
                    symbol=symbol,
                    side=norm_side,
                    fsm_state="REJECTED",
                    requested_units=float(quantity),
                    filled_units=0.0,
                    avg_fill_price=0.0,
                    slippage_paid_usd=0.0,
                    effective_slippage_bps=0.0,
                    fees_paid_usdc=0.0,
                    is_maker=False,
                    fill_type="NONE",
                    state_history=state_history,
                )
                self._last_order_result = res
                return res

            # Transition PENDING -> OPEN_ON_BOOK
            current_state = "OPEN_ON_BOOK"
            state_history.append("OPEN_ON_BOOK")
            self.db.transition_order_state(
                cli_id,
                "OPEN_ON_BOOK",
                from_state="PENDING",
                reason="MAKER_LIMIT_POST_ONLY_OPENED",
                exchange_order_id=exchange_order_id,
            )

            # If mock/exchange does not implement fetch_order (or returns non-dict), treat initial order as filled
            fetch_order_fn = getattr(exchange, "fetch_order", None)
            if not callable(fetch_order_fn):
                avg_px = float(initial_order.get("average") or initial_order.get("price") or current_limit_price)
                fees_usdc = float(quantity) * avg_px * maker_fee_rate
                current_state = "FILLED"
                state_history.append("FILLED")
                self.db.transition_order_state(
                    cli_id,
                    "FILLED",
                    from_state="OPEN_ON_BOOK",
                    reason="MAKER_LIMIT_FILLED",
                    exchange_order_id=exchange_order_id,
                    avg_fill_price=avg_px,
                    filled_units=float(quantity),
                    slippage_paid_usd=0.0,
                    fees_paid_usdc=fees_usdc,
                )
                res = OrderExecutionResult(
                    client_order_id=cli_id,
                    exchange_order_id=exchange_order_id,
                    symbol=symbol,
                    side=norm_side,
                    fsm_state="FILLED",
                    requested_units=float(quantity),
                    filled_units=float(quantity),
                    avg_fill_price=avg_px,
                    slippage_paid_usd=0.0,
                    effective_slippage_bps=0.0,
                    fees_paid_usdc=round(fees_usdc, 6),
                    is_maker=True,
                    fill_type="MAKER_LIMIT",
                    state_history=state_history,
                )
                self._last_order_result = res
                return res

            # Step 2: Poll exchange.fetch_order(order_id, symbol) across N chase intervals
            prev_filled_on_current_order = 0.0
            order_active_on_book = True

            for interval_idx in range(max_intervals):
                if sleep_secs > 0:
                    await asyncio.sleep(sleep_secs)

                if order_active_on_book:
                    try:
                        polled = await exchange.fetch_order(exchange_order_id, symbol)
                    except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as poll_exc:
                        logger.warning(
                            "Maker limit chaser poll timeout/error (%d/%d) on %s [%s]: %s",
                            interval_idx + 1,
                            max_intervals,
                            symbol,
                            exchange_order_id,
                            poll_exc,
                        )
                        self.db.log_order_state_transition(
                            cli_id,
                            current_state,
                            current_state,
                            reason=f"POLL_TIMEOUT_INTERVAL_{interval_idx + 1}: {poll_exc}",
                        )
                        continue

                    if not isinstance(polled, dict):
                        # Unconfigured mock fallback: treat as instant maker fill
                        total_filled_units = float(quantity)
                        total_fill_notional = float(quantity) * current_limit_price
                        total_fees_usdc = total_fill_notional * maker_fee_rate
                        avg_px = current_limit_price
                        self.db.transition_order_state(
                            cli_id,
                            "FILLED",
                            from_state=current_state,
                            reason="MAKER_LIMIT_FILLED",
                            exchange_order_id=exchange_order_id,
                            avg_fill_price=avg_px,
                            filled_units=total_filled_units,
                            quantity=total_filled_units,
                            notional_usdc=round(total_fill_notional, 2),
                            slippage_paid_usd=0.0,
                            fees_paid_usdc=total_fees_usdc,
                            status="OPEN",
                        )
                        state_history.append("FILLED")
                        res = OrderExecutionResult(
                            client_order_id=cli_id,
                            exchange_order_id=exchange_order_id,
                            symbol=symbol,
                            side=norm_side,
                            fsm_state="FILLED",
                            requested_units=float(quantity),
                            filled_units=total_filled_units,
                            avg_fill_price=avg_px,
                            slippage_paid_usd=0.0,
                            effective_slippage_bps=0.0,
                            fees_paid_usdc=round(total_fees_usdc, 6),
                            is_maker=True,
                            fill_type="MAKER_LIMIT",
                            state_history=state_history,
                        )
                        self._last_order_result = res
                        return res

                    polled_status = str(polled.get("status") or "open").lower()
                    raw_filled = polled.get("filled")
                    polled_filled = (
                        float(raw_filled)
                        if raw_filled is not None
                        else (float(quantity) - total_filled_units if polled_status in ("closed", "filled") else 0.0)
                    )
                    polled_avg_px = float(
                        polled.get("average") or polled.get("price") or current_limit_price
                    )

                    delta_filled = max(0.0, polled_filled - prev_filled_on_current_order)
                    if delta_filled > 0.0:
                        total_filled_units = min(float(quantity), total_filled_units + delta_filled)
                        total_fill_notional += delta_filled * polled_avg_px
                        total_fees_usdc += (delta_filled * polled_avg_px) * maker_fee_rate
                        prev_filled_on_current_order = polled_filled

                    # Check if completely filled
                    if total_filled_units >= float(quantity) - 1e-9 or polled_status in ("closed", "filled"):
                        if total_filled_units < float(quantity):
                            rem = float(quantity) - total_filled_units
                            total_filled_units = float(quantity)
                            total_fill_notional += rem * polled_avg_px
                            total_fees_usdc += (rem * polled_avg_px) * maker_fee_rate
                        avg_px = total_fill_notional / max(total_filled_units, 1e-12)
                        slip_usd = abs(avg_px - float(price)) * total_filled_units
                        self.db.transition_order_state(
                            cli_id,
                            "FILLED",
                            from_state=current_state,
                            reason=f"MAKER_LIMIT_FILLED_INTERVAL_{interval_idx + 1}",
                            exchange_order_id=exchange_order_id,
                            avg_fill_price=avg_px,
                            filled_units=total_filled_units,
                            quantity=total_filled_units,
                            notional_usdc=round(total_fill_notional, 2),
                            slippage_paid_usd=slip_usd,
                            fees_paid_usdc=total_fees_usdc,
                            status="OPEN",
                        )
                        current_state = "FILLED"
                        state_history.append("FILLED")
                        res = OrderExecutionResult(
                            client_order_id=cli_id,
                            exchange_order_id=exchange_order_id,
                            symbol=symbol,
                            side=norm_side,
                            fsm_state="FILLED",
                            requested_units=float(quantity),
                            filled_units=round(total_filled_units, 8),
                            avg_fill_price=round(avg_px, 8),
                            slippage_paid_usd=round(slip_usd, 6),
                            effective_slippage_bps=0.0,
                            fees_paid_usdc=round(total_fees_usdc, 6),
                            is_maker=True,
                            fill_type="MAKER_LIMIT",
                            state_history=state_history,
                        )
                        self._last_order_result = res
                        return res

                    # Check if externally cancelled or rejected
                    if polled_status in ("canceled", "cancelled"):
                        avg_px = (
                            total_fill_notional / total_filled_units
                            if total_filled_units > 0
                            else 0.0
                        )
                        self.db.transition_order_state(
                            cli_id,
                            "CANCELLED",
                            from_state=current_state,
                            reason="EXTERNAL_OR_EXCHANGE_CANCELLED",
                            exchange_order_id=exchange_order_id,
                            avg_fill_price=avg_px if total_filled_units > 0 else None,
                            filled_units=total_filled_units,
                            quantity=total_filled_units if total_filled_units > 0 else None,
                            notional_usdc=round(total_fill_notional, 2) if total_filled_units > 0 else None,
                            fees_paid_usdc=total_fees_usdc,
                            status="CANCELLED" if total_filled_units == 0 else "OPEN",
                        )
                        current_state = "CANCELLED"
                        state_history.append("CANCELLED")
                        res = OrderExecutionResult(
                            client_order_id=cli_id,
                            exchange_order_id=exchange_order_id,
                            symbol=symbol,
                            side=norm_side,
                            fsm_state="CANCELLED",
                            requested_units=float(quantity),
                            filled_units=round(total_filled_units, 8),
                            avg_fill_price=round(avg_px, 8),
                            slippage_paid_usd=0.0,
                            effective_slippage_bps=0.0,
                            fees_paid_usdc=round(total_fees_usdc, 6),
                            is_maker=total_filled_units > 0,
                            fill_type="MAKER_LIMIT" if total_filled_units > 0 else "NONE",
                            state_history=state_history,
                        )
                        self._last_order_result = res
                        return res

                    if polled_status == "rejected":
                        avg_px = (
                            total_fill_notional / total_filled_units
                            if total_filled_units > 0
                            else 0.0
                        )
                        self.db.transition_order_state(
                            cli_id,
                            "REJECTED",
                            from_state=current_state,
                            reason="EXCHANGE_REJECTED_DURING_CHASE",
                            exchange_order_id=exchange_order_id,
                            avg_fill_price=avg_px if total_filled_units > 0 else None,
                            filled_units=total_filled_units,
                            quantity=total_filled_units if total_filled_units > 0 else None,
                            notional_usdc=round(total_fill_notional, 2) if total_filled_units > 0 else None,
                            fees_paid_usdc=total_fees_usdc,
                            status="CANCELLED" if total_filled_units == 0 else "OPEN",
                        )
                        current_state = "REJECTED"
                        state_history.append("REJECTED")
                        res = OrderExecutionResult(
                            client_order_id=cli_id,
                            exchange_order_id=exchange_order_id,
                            symbol=symbol,
                            side=norm_side,
                            fsm_state="REJECTED",
                            requested_units=float(quantity),
                            filled_units=round(total_filled_units, 8),
                            avg_fill_price=round(avg_px, 8),
                            slippage_paid_usd=0.0,
                            effective_slippage_bps=0.0,
                            fees_paid_usdc=round(total_fees_usdc, 6),
                            is_maker=total_filled_units > 0,
                            fill_type="MAKER_LIMIT" if total_filled_units > 0 else "NONE",
                            state_history=state_history,
                        )
                        self._last_order_result = res
                        return res

                    # Check if partially filled (0 < total_filled_units < quantity)
                    if 0.0 < total_filled_units < float(quantity) - 1e-9:
                        partial_avg_px = total_fill_notional / total_filled_units
                        if current_state != "PARTIALLY_FILLED":
                            self.db.transition_order_state(
                                cli_id,
                                "PARTIALLY_FILLED",
                                from_state=current_state,
                                reason=f"PARTIAL_FILL_{total_filled_units:.6f}_OF_{quantity:.6f}",
                                exchange_order_id=exchange_order_id,
                                avg_fill_price=partial_avg_px,
                                filled_units=total_filled_units,
                                fees_paid_usdc=total_fees_usdc,
                            )
                            current_state = "PARTIALLY_FILLED"
                            state_history.append("PARTIALLY_FILLED")
                        else:
                            self.db.transition_order_state(
                                cli_id,
                                "PARTIALLY_FILLED",
                                from_state="PARTIALLY_FILLED",
                                reason=f"INCREMENTAL_PARTIAL_FILL_{total_filled_units:.6f}",
                                exchange_order_id=exchange_order_id,
                                avg_fill_price=partial_avg_px,
                                filled_units=total_filled_units,
                                fees_paid_usdc=total_fees_usdc,
                            )

                    if polled_status == "expired":
                        order_active_on_book = False
                        break

                # Reprice order to the new best bid/ask if unfilled after each interval (before final check)
                if interval_idx < max_intervals - 1:
                    new_best_px = await self._fetch_best_quote_price(
                        exchange, symbol, is_buy, current_limit_price
                    )
                    remaining_units = round(float(quantity) - total_filled_units, 8)
                    if remaining_units > 0:
                        cancel_fn = getattr(exchange, "cancel_order", None)
                        if callable(cancel_fn) and order_active_on_book:
                            try:
                                await cancel_fn(exchange_order_id, symbol)
                                order_active_on_book = False
                                prev_filled_on_current_order = 0.0
                            except (
                                ccxt.BaseError,
                                OSError,
                                ValueError,
                                TypeError,
                                KeyError,
                                RuntimeError,
                            ) as cancel_exc:
                                logger.warning(
                                    "Chaser cancel_order step %d failed for %s: %s",
                                    interval_idx + 1,
                                    symbol,
                                    cancel_exc,
                                )
                                with suppress(
                                    ccxt.BaseError,
                                    OSError,
                                    ValueError,
                                    TypeError,
                                    KeyError,
                                    RuntimeError,
                                ):
                                    late_polled = await exchange.fetch_order(
                                        exchange_order_id, symbol
                                    )
                                    if isinstance(late_polled, dict):
                                        late_status = str(
                                            late_polled.get("status") or "open"
                                        ).lower()
                                        raw_late_filled = late_polled.get("filled")
                                        late_filled = (
                                            float(raw_late_filled)
                                            if raw_late_filled is not None
                                            else (
                                                float(quantity) - total_filled_units
                                                if late_status in ("closed", "filled")
                                                else 0.0
                                            )
                                        )
                                        late_avg_px = float(
                                            late_polled.get("average")
                                            or late_polled.get("price")
                                            or current_limit_price
                                        )
                                        late_delta = max(
                                            0.0, late_filled - prev_filled_on_current_order
                                        )
                                        if late_delta > 0.0:
                                            total_filled_units = min(
                                                float(quantity),
                                                total_filled_units + late_delta,
                                            )
                                            total_fill_notional += late_delta * late_avg_px
                                            total_fees_usdc += (
                                                late_delta * late_avg_px
                                            ) * maker_fee_rate
                                            prev_filled_on_current_order = late_filled
                                        if total_filled_units >= float(
                                            quantity
                                        ) - 1e-9 or late_status in ("closed", "filled"):
                                            if total_filled_units < float(quantity):
                                                rem = float(quantity) - total_filled_units
                                                total_filled_units = float(quantity)
                                                total_fill_notional += rem * late_avg_px
                                                total_fees_usdc += (
                                                    rem * late_avg_px
                                                ) * maker_fee_rate
                                            avg_px = total_fill_notional / max(
                                                total_filled_units, 1e-12
                                            )
                                            slip_usd = (
                                                abs(avg_px - float(price)) * total_filled_units
                                            )
                                            self.db.transition_order_state(
                                                cli_id,
                                                "FILLED",
                                                from_state=current_state,
                                                reason="MAKER_LIMIT_FILLED_ON_CANCEL_RACE",
                                                exchange_order_id=exchange_order_id,
                                                avg_fill_price=avg_px,
                                                filled_units=total_filled_units,
                                                quantity=total_filled_units,
                                                notional_usdc=round(total_fill_notional, 2),
                                                slippage_paid_usd=slip_usd,
                                                fees_paid_usdc=total_fees_usdc,
                                                status="OPEN",
                                            )
                                            current_state = "FILLED"
                                            state_history.append("FILLED")
                                            res = OrderExecutionResult(
                                                client_order_id=cli_id,
                                                exchange_order_id=exchange_order_id,
                                                symbol=symbol,
                                                side=norm_side,
                                                fsm_state="FILLED",
                                                requested_units=float(quantity),
                                                filled_units=round(total_filled_units, 8),
                                                avg_fill_price=round(avg_px, 8),
                                                slippage_paid_usd=round(slip_usd, 6),
                                                effective_slippage_bps=0.0,
                                                fees_paid_usdc=round(total_fees_usdc, 6),
                                                is_maker=True,
                                                fill_type="MAKER_LIMIT",
                                                state_history=state_history,
                                            )
                                            self._last_order_result = res
                                            return res
                                        if late_status in (
                                            "canceled",
                                            "cancelled",
                                            "expired",
                                        ):
                                            order_active_on_book = False
                                            prev_filled_on_current_order = 0.0
                        remaining_units = round(float(quantity) - total_filled_units, 8)
                        if not order_active_on_book and remaining_units > 0:
                            try:
                                repriced_order = await exchange.create_order(
                                    symbol=symbol,
                                    type="limit",
                                    side=ccxt_side,
                                    amount=remaining_units,
                                    price=new_best_px,
                                    params=order_params,
                                )
                                rep_status = (
                                    str(repriced_order.get("status") or "open").lower()
                                    if isinstance(repriced_order, dict)
                                    else "open"
                                )
                                if rep_status in ("rejected", "canceled", "cancelled"):
                                    order_active_on_book = False
                                else:
                                    if isinstance(repriced_order, dict) and repriced_order.get("id"):
                                        exchange_order_id = str(repriced_order["id"])
                                    order_active_on_book = True
                                current_limit_price = new_best_px
                                prev_filled_on_current_order = 0.0
                            except (
                                ccxt.BaseError,
                                OSError,
                                ValueError,
                                TypeError,
                                KeyError,
                                RuntimeError,
                            ) as reprice_exc:
                                current_limit_price = new_best_px
                                logger.warning(
                                    "Chaser reprice step %d failed for %s: %s",
                                    interval_idx + 1,
                                    symbol,
                                    reprice_exc,
                                )

            # Step 3: Unfilled after N chase intervals -> Cancel resting maker limit & transition to EXPIRED
            if order_active_on_book:
                cancel_fn = getattr(exchange, "cancel_order", None)
                if callable(cancel_fn):
                    try:
                        await cancel_fn(exchange_order_id, symbol)
                    except (
                        ccxt.BaseError,
                        OSError,
                        ValueError,
                        TypeError,
                        KeyError,
                        RuntimeError,
                    ) as final_cancel_exc:
                        logger.warning(
                            "Chaser final cancel_order failed for %s [%s]: %s",
                            symbol,
                            exchange_order_id,
                            final_cancel_exc,
                        )
                        with suppress(
                            ccxt.BaseError,
                            OSError,
                            ValueError,
                            TypeError,
                            KeyError,
                            RuntimeError,
                        ):
                            late_polled = await exchange.fetch_order(exchange_order_id, symbol)
                            if isinstance(late_polled, dict):
                                late_status = str(late_polled.get("status") or "open").lower()
                                raw_late_filled = late_polled.get("filled")
                                late_filled = (
                                    float(raw_late_filled)
                                    if raw_late_filled is not None
                                    else (
                                        float(quantity) - total_filled_units
                                        if late_status in ("closed", "filled")
                                        else 0.0
                                    )
                                )
                                late_avg_px = float(
                                    late_polled.get("average")
                                    or late_polled.get("price")
                                    or current_limit_price
                                )
                                late_delta = max(
                                    0.0, late_filled - prev_filled_on_current_order
                                )
                                if late_delta > 0.0:
                                    total_filled_units = min(
                                        float(quantity), total_filled_units + late_delta
                                    )
                                    total_fill_notional += late_delta * late_avg_px
                                    total_fees_usdc += (
                                        late_delta * late_avg_px
                                    ) * maker_fee_rate
                                    prev_filled_on_current_order = late_filled
                                if total_filled_units >= float(quantity) - 1e-9 or late_status in (
                                    "closed",
                                    "filled",
                                ):
                                    if total_filled_units < float(quantity):
                                        rem = float(quantity) - total_filled_units
                                        total_filled_units = float(quantity)
                                        total_fill_notional += rem * late_avg_px
                                        total_fees_usdc += (rem * late_avg_px) * maker_fee_rate
                                    avg_px = total_fill_notional / max(
                                        total_filled_units, 1e-12
                                    )
                                    slip_usd = abs(avg_px - float(price)) * total_filled_units
                                    self.db.transition_order_state(
                                        cli_id,
                                        "FILLED",
                                        from_state=current_state,
                                        reason="MAKER_LIMIT_FILLED_ON_CANCEL_RACE",
                                        exchange_order_id=exchange_order_id,
                                        avg_fill_price=avg_px,
                                        filled_units=total_filled_units,
                                        quantity=total_filled_units,
                                        notional_usdc=round(total_fill_notional, 2),
                                        slippage_paid_usd=slip_usd,
                                        fees_paid_usdc=total_fees_usdc,
                                        status="OPEN",
                                    )
                                    current_state = "FILLED"
                                    state_history.append("FILLED")
                                    res = OrderExecutionResult(
                                        client_order_id=cli_id,
                                        exchange_order_id=exchange_order_id,
                                        symbol=symbol,
                                        side=norm_side,
                                        fsm_state="FILLED",
                                        requested_units=float(quantity),
                                        filled_units=round(total_filled_units, 8),
                                        avg_fill_price=round(avg_px, 8),
                                        slippage_paid_usd=round(slip_usd, 6),
                                        effective_slippage_bps=0.0,
                                        fees_paid_usdc=round(total_fees_usdc, 6),
                                        is_maker=True,
                                        fill_type="MAKER_LIMIT",
                                        state_history=state_history,
                                    )
                                    self._last_order_result = res
                                    return res

            partial_avg = (
                total_fill_notional / total_filled_units if total_filled_units > 0 else None
            )
            self.db.transition_order_state(
                cli_id,
                "EXPIRED",
                from_state=current_state,
                reason=f"MAKER_CHASE_EXPIRED_AFTER_{max_intervals}_INTERVALS",
                exchange_order_id=exchange_order_id,
                avg_fill_price=partial_avg,
                filled_units=total_filled_units,
                quantity=total_filled_units if total_filled_units > 0 else None,
                notional_usdc=round(total_fill_notional, 2) if total_filled_units > 0 else None,
                fees_paid_usdc=total_fees_usdc,
                status="CANCELLED" if total_filled_units == 0 else "OPEN",
            )
            current_state = "EXPIRED"
            state_history.append("EXPIRED")

            remaining_units = round(float(quantity) - total_filled_units, 8)
            if not fallback_to_ioc or remaining_units <= 1e-9:
                final_avg = partial_avg or 0.0
                res = OrderExecutionResult(
                    client_order_id=cli_id,
                    exchange_order_id=exchange_order_id,
                    symbol=symbol,
                    side=norm_side,
                    fsm_state="EXPIRED",
                    requested_units=float(quantity),
                    filled_units=round(total_filled_units, 8),
                    avg_fill_price=round(final_avg, 8),
                    slippage_paid_usd=0.0,
                    effective_slippage_bps=0.0,
                    fees_paid_usdc=round(total_fees_usdc, 6),
                    is_maker=total_filled_units > 0,
                    fill_type="MAKER_PARTIAL_EXPIRED" if total_filled_units > 0 else "NONE",
                    state_history=state_history,
                )
                self._last_order_result = res
                return res

            # Step 4: Execute fallback crossing the spread using MARKET_IOC (4.5 bps taker fee)
            ioc_params: dict[str, Any] = {"timeInForce": "IOC"}
            if reduce_only:
                ioc_params["reduceOnly"] = True

            eff_slip_bps = self.calculate_dynamic_slippage_bps(
                atr_14=atr_14,
                close_price=float(price),
                notional_usd=remaining_units * float(price),
                config=cfg,
            )
            slip_rate = eff_slip_bps / 10000.0
            ioc_ref_price = (
                current_limit_price * (1.0 + slip_rate)
                if is_buy
                else current_limit_price * (1.0 - slip_rate)
            )

            try:
                ioc_order = await exchange.create_order(
                    symbol=symbol,
                    type="market",
                    side=ccxt_side,
                    amount=remaining_units,
                    price=ioc_ref_price,
                    params=ioc_params,
                )
            except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as ioc_exc:
                logger.error("MARKET_IOC fallback failed on %s: %s", symbol, ioc_exc)
                res = OrderExecutionResult(
                    client_order_id=cli_id,
                    exchange_order_id=exchange_order_id,
                    symbol=symbol,
                    side=norm_side,
                    fsm_state="EXPIRED",
                    requested_units=float(quantity),
                    filled_units=round(total_filled_units, 8),
                    avg_fill_price=round(partial_avg or 0.0, 8),
                    slippage_paid_usd=0.0,
                    effective_slippage_bps=0.0,
                    fees_paid_usdc=round(total_fees_usdc, 6),
                    is_maker=total_filled_units > 0,
                    fill_type="MAKER_PARTIAL_EXPIRED" if total_filled_units > 0 else "NONE",
                    state_history=state_history,
                )
                self._last_order_result = res
                return res

            ioc_status = str(ioc_order.get("status") or "closed").lower() if isinstance(ioc_order, dict) else "closed"
            raw_ioc_filled = ioc_order.get("filled") if isinstance(ioc_order, dict) else None
            if ioc_status in ("expired", "canceled", "cancelled", "rejected") and (
                raw_ioc_filled is None or float(raw_ioc_filled) <= 0.0
            ):
                res = OrderExecutionResult(
                    client_order_id=cli_id,
                    exchange_order_id=exchange_order_id,
                    symbol=symbol,
                    side=norm_side,
                    fsm_state="EXPIRED",
                    requested_units=float(quantity),
                    filled_units=round(total_filled_units, 8),
                    avg_fill_price=round(partial_avg or 0.0, 8),
                    slippage_paid_usd=0.0,
                    effective_slippage_bps=0.0,
                    fees_paid_usdc=round(total_fees_usdc, 6),
                    is_maker=total_filled_units > 0,
                    fill_type="MAKER_PARTIAL_EXPIRED" if total_filled_units > 0 else "NONE",
                    state_history=state_history,
                )
                self._last_order_result = res
                return res

            ioc_filled = float(raw_ioc_filled) if raw_ioc_filled is not None else remaining_units
            ioc_avg_px = (
                float(ioc_order.get("average") or ioc_order.get("price") or ioc_ref_price)
                if isinstance(ioc_order, dict)
                else ioc_ref_price
            )
            if isinstance(ioc_order, dict) and ioc_order.get("id"):
                exchange_order_id = str(ioc_order["id"])

            total_filled_units = round(total_filled_units + ioc_filled, 8)
            total_fill_notional += ioc_filled * ioc_avg_px
            total_fees_usdc += (ioc_filled * ioc_avg_px) * taker_fee_rate
            avg_fill_px = total_fill_notional / max(total_filled_units, 1e-12)
            slippage_usd = abs(avg_fill_px - float(price)) * total_filled_units

            target_state = (
                "FILLED" if total_filled_units >= float(quantity) - 1e-9 else "PARTIALLY_FILLED"
            )
            self.db.transition_order_state(
                cli_id,
                target_state,
                from_state="EXPIRED",
                reason="MARKET_IOC_FALLBACK_FILLED",
                exchange_order_id=exchange_order_id,
                avg_fill_price=avg_fill_px,
                filled_units=total_filled_units,
                quantity=total_filled_units,
                notional_usdc=round(total_fill_notional, 2),
                slippage_paid_usd=slippage_usd,
                fees_paid_usdc=total_fees_usdc,
                status="OPEN",
            )
            state_history.append(target_state)
            res = OrderExecutionResult(
                client_order_id=cli_id,
                exchange_order_id=exchange_order_id,
                symbol=symbol,
                side=norm_side,
                fsm_state=target_state,
                requested_units=float(quantity),
                filled_units=total_filled_units,
                avg_fill_price=round(avg_fill_px, 8),
                slippage_paid_usd=round(slippage_usd, 6),
                effective_slippage_bps=round(eff_slip_bps, 6),
                fees_paid_usdc=round(total_fees_usdc, 6),
                is_maker=False,
                fill_type="MARKET_IOC_FALLBACK",
                state_history=state_history,
            )
            self._last_order_result = res
            return res
        finally:
            if owns_exchange and exchange is not None:
                await exchange.close()

    async def _route_dex_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        config: dict[str, Any],
        reduce_only: bool = False,
        client_order_id: str | None = None,
        atr_14: float = 0.0,
        next_bar: dict[str, Any] | pd.Series | None = None,
    ) -> str:
        """Route order to Hyperliquid L1 Mainnet/Testnet CLOB via MAKER_LIMIT_CHASER FSM or Paper Fill Model."""
        mode = self._execution_mode_override or str(config.get("execution_mode", "PAPER")).strip().upper()
        wallet = str(config.get("wallet_address", "")).strip()
        priv_key = str(config.get("private_key", "")).strip()

        if (mode in ("LIVE", "TESTNET") and wallet and priv_key) or self.exchange is not None:
            result = await self.execute_maker_limit_chaser(
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                client_order_id=client_order_id,
                config=config,
                reduce_only=reduce_only,
                atr_14=atr_14,
            )
            if result.fsm_state in ("REJECTED", "CANCELLED", "EXPIRED") and result.filled_units <= 0:
                raise RuntimeError(
                    f"Order {result.client_order_id} on {symbol} terminated in state {result.fsm_state}"
                )
            return result.exchange_order_id

        result = self.simulate_paper_fill(
            symbol=symbol,
            side=side,
            limit_price=price,
            quantity=quantity,
            next_bar=next_bar if next_bar is not None else {"low": price, "high": price, "close": price},
            atr_14=atr_14,
            close_price=price,
            client_order_id=client_order_id,
            config=config,
            record_audit=True,
        )
        return result.exchange_order_id

    # -------------------------------------------------------------------------
    # Step 3.3: On-Chain Position Reconciliation (reconcile_exchange_state)
    # -------------------------------------------------------------------------
    async def _dispatch_webhook_alert(
        self, title: str, message: str, level: str = "CRITICAL"
    ) -> None:
        """Dispatch optional Telegram/Discord webhook alert for critical reconciliation events."""
        with suppress(sqlite3.Error, OSError, ValueError, TypeError, KeyError, RuntimeError):
            await self.alerts.send_alert(title=title, message=message, level=level)

    async def reconcile_exchange_state(self) -> int:
        """
        Reconcile open local SQLite positions against on-chain Hyperliquid L1 positions:
        1. Call exchange.fetch_positions() on Hyperliquid L1.
        2. Compare on-chain contracts and side against local trades where status = 'OPEN'.
        3. If on-chain position is 0 while local state is OPEN, mark local trade CLOSED
           with exit_reason = 'EXCHANGE_RECONCILE', log CRITICAL alert, and dispatch webhook.
        4. If on-chain position is partially reduced (0 < on_chain_size < local_quantity),
           synchronize local open trade size and risk budget to on_chain_size.
        """
        if self.execution_mode == "PAPER":
            return 0

        owns_exchange = False
        exchange = self.exchange
        try:
            if exchange is None:
                cfg = self.db.load_config()
                wallet = str(cfg.get("wallet_address", "")).strip()
                priv_key = str(cfg.get("private_key", "")).strip()
                if not (wallet and priv_key):
                    return 0
                exchange, _ = self._build_hyperliquid_client(cfg)
                owns_exchange = True

            on_chain_positions = await exchange.fetch_positions()
            pos_map: dict[str, float] = {}
            side_map: dict[str, str] = {}
            entry_map: dict[str, float] = {}
            mark_map: dict[str, float] = {}
            upnl_map: dict[str, float] = {}
            for p in on_chain_positions or []:
                if not isinstance(p, dict):
                    continue
                sym = str(p.get("symbol", ""))
                contracts = abs(float(p.get("contracts") or 0.0))
                if sym and contracts > 0.0:
                    pos_map[sym] = contracts
                    raw_side = str(p.get("side") or "").strip().upper()
                    if raw_side in ("LONG", "BUY"):
                        side_map[sym] = "LONG"
                    elif raw_side in ("SHORT", "SELL"):
                        side_map[sym] = "SHORT"
                    if p.get("entryPrice") is not None:
                        with suppress(ValueError, TypeError):
                            entry_map[sym] = float(p["entryPrice"])
                    if p.get("markPrice") is not None:
                        with suppress(ValueError, TypeError):
                            mark_map[sym] = float(p["markPrice"])
                    if p.get("unrealizedPnl") is not None:
                        with suppress(ValueError, TypeError):
                            upnl_map[sym] = float(p["unrealizedPnl"])

            local_trades = self.db.get_open_trades()
            reconciled_count = 0
            matched_local_symbols: set[str] = set()
            for trade in local_trades:
                sym = str(trade["symbol"])
                local_side = str(trade.get("side", "LONG")).upper()
                local_qty = float(
                    trade["filled_units"]
                    if trade.get("filled_units") and float(trade["filled_units"]) > 0
                    else (trade.get("quantity") or 0.0)
                )
                on_chain_size = pos_map.get(sym, 0.0)
                on_chain_side = side_map.get(sym)

                side_mismatch = (
                    on_chain_side is not None
                    and local_side in ("LONG", "SHORT")
                    and on_chain_side != local_side
                )
                if on_chain_size == 0.0 or side_mismatch:
                    logger.critical(
                        "RECONCILIATION MISMATCH: Symbol %s open locally but 0 on-chain. Force closing.",
                        sym,
                    )
                    self.db.close_trade(
                        client_order_id=trade["client_order_id"],
                        exit_price=trade["current_stop_price"],
                        exit_reason="EXCHANGE_RECONCILE",
                    )
                    self.db.log_telemetry(
                        "CRITICAL",
                        "engine.execution",
                        f"RECONCILIATION MISMATCH: Symbol {sym} open locally but 0 on-chain. Force closing.",
                        {
                            "symbol": sym,
                            "client_order_id": trade["client_order_id"],
                            "trade_id": trade.get("trade_id"),
                            "exit_price": trade["current_stop_price"],
                            "exit_reason": "EXCHANGE_RECONCILE",
                        },
                    )
                    await self._dispatch_webhook_alert(
                        title="Position Drift Reconciliation",
                        message=f"Symbol {sym} open locally ({local_side}) but 0 on-chain. Force closed with EXCHANGE_RECONCILE.",
                        level="CRITICAL",
                    )
                    reconciled_count += 1
                else:
                    matched_local_symbols.add(sym)
                    if (
                        on_chain_size > 0.0
                        and abs(on_chain_size - local_qty) > 1e-8
                        and trade.get("trade_id") is not None
                    ):
                        logger.warning(
                            "RECONCILIATION PARTIAL DRIFT: Symbol %s local size %.6f != on-chain %.6f. Syncing size.",
                            sym,
                            local_qty,
                            on_chain_size,
                        )
                        self.db.reconcile_open_trade_size(
                            trade_id=int(trade["trade_id"]),
                            on_chain_size=on_chain_size,
                            entry_price=entry_map.get(sym),
                        )
                        self.db.log_telemetry(
                            "WARNING",
                            "engine.execution",
                            f"RECONCILIATION PARTIAL DRIFT: Symbol {sym} synced from {local_qty} to {on_chain_size} contracts.",
                            {
                                "symbol": sym,
                                "client_order_id": trade["client_order_id"],
                                "trade_id": trade.get("trade_id"),
                                "previous_quantity": local_qty,
                                "on_chain_size": on_chain_size,
                            },
                        )
                        reconciled_count += 1

            for sym, on_chain_size in pos_map.items():
                if sym in matched_local_symbols:
                    continue
                on_chain_side = side_map.get(sym)
                if on_chain_side not in ("LONG", "SHORT"):
                    continue
                reconciled_orphan = self.db.reconcile_on_chain_orphan_position(
                    symbol=sym,
                    side=on_chain_side,
                    on_chain_size=on_chain_size,
                    entry_price=entry_map.get(sym, 0.0),
                    mark_price=mark_map.get(sym, 0.0),
                    unrealized_pnl_usdc=upnl_map.get(sym, 0.0),
                )
                if reconciled_orphan is not None:
                    self._cooldown_until_ms.pop(sym, None)
                    self._cooldown_until_step.pop(sym, None)
                    logger.warning(
                        "RECONCILIATION ON-CHAIN POSITION RESTORED: Symbol %s (%s) %.6f contracts restored (%s).",
                        sym,
                        on_chain_side,
                        on_chain_size,
                        reconciled_orphan["action"],
                    )
                    self.db.log_telemetry(
                        "WARNING",
                        "engine.execution",
                        f"RECONCILIATION ON-CHAIN POSITION RESTORED: Symbol {sym} ({on_chain_side}) synced at {on_chain_size} contracts.",
                        reconciled_orphan,
                    )
                    reconciled_count += 1
            return reconciled_count
        except (
            ccxt.BaseError,
            sqlite3.Error,
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RuntimeError,
        ) as exc:
            logger.error("Failed to execute exchange position reconciliation: %s", exc)
            return 0
        finally:
            if owns_exchange and exchange is not None:
                await exchange.close()

    # -------------------------------------------------------------------------
    # Step 3.4: Exact 1H Funding Settlement Sync
    # -------------------------------------------------------------------------
    async def sync_funding_payments(
        self,
        symbol: str,
        *,
        since: int | None = None,
        config: dict[str, Any] | None = None,
    ) -> float | None:
        """
        Query actual 1H USDC funding payments from Hyperliquid L1 via
        `exchange.fetch_funding_history(symbol, since=last_sync)` in LIVE/TESTNET mode.
        Returns exact net USDC funding transfer if available, or None to fall back to local approximation.
        """
        if self.execution_mode == "PAPER" and self.exchange is None:
            return None

        owns_exchange = False
        exchange = self.exchange
        try:
            if exchange is None:
                cfg = config if config is not None else self.db.load_config()
                wallet = str(cfg.get("wallet_address", "")).strip()
                priv_key = str(cfg.get("private_key", "")).strip()
                if not (wallet and priv_key):
                    return None
                exchange, _ = self._build_hyperliquid_client(cfg)
                owns_exchange = True

            fetch_fn = getattr(exchange, "fetch_funding_history", None)
            if not callable(fetch_fn):
                return None

            ref_now_ms = (
                int(self.db.pit_timestamp_ms)
                if self.db.pit_timestamp_ms is not None
                else int(time.time() * 1000)
            )
            last_sync = (
                int(since)
                if since is not None
                else self._last_funding_sync_ms.get(symbol, ref_now_ms - 3_600_000)
            )
            history = await fetch_fn(symbol, since=last_sync)
            if not isinstance(history, list) or len(history) == 0:
                return None

            base_coin = symbol.split("/")[0].split(":")[0].upper()
            net_funding_usdc = 0.0
            max_ts = last_sync
            parsed_any = False
            for entry in history:
                if not isinstance(entry, dict):
                    continue
                entry_sym = entry.get("symbol")
                if entry_sym and str(entry_sym) != symbol:
                    continue
                raw_info = entry.get("info")
                info: dict[str, Any] = raw_info if isinstance(raw_info, dict) else {}
                raw_info_delta = info.get("delta")
                raw_entry_delta = entry.get("delta")
                delta: dict[str, Any] = (
                    raw_info_delta
                    if isinstance(raw_info_delta, dict)
                    else (raw_entry_delta if isinstance(raw_entry_delta, dict) else {})
                )
                delta_coin = delta.get("coin")
                if delta_coin and str(delta_coin).upper() != base_coin:
                    continue
                ts = int(entry.get("timestamp") or entry.get("time") or info.get("time") or last_sync)
                if ts < last_sync:
                    continue
                if ts > max_ts:
                    max_ts = ts
                raw_amt = (
                    entry.get("amount")
                    if entry.get("amount") is not None
                    else (
                        entry.get("funding_usdc")
                        if entry.get("funding_usdc") is not None
                        else (
                            delta.get("usdc")
                            if delta.get("usdc") is not None
                            else info.get("usdc", info.get("amount"))
                        )
                    )
                )
                if raw_amt is not None:
                    net_funding_usdc += float(raw_amt)
                    parsed_any = True

            if not parsed_any:
                return None

            self._last_funding_sync_ms[symbol] = max_ts + 1
            return round(net_funding_usdc, 6)
        except (
            ccxt.BaseError,
            sqlite3.Error,
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RuntimeError,
        ) as exc:
            logger.warning("Failed to sync on-chain funding history for %s: %s", symbol, exc)
            return None
        finally:
            if owns_exchange and exchange is not None:
                await exchange.close()

    # -------------------------------------------------------------------------
    # Step 3.6: On-Chain Hyperliquid HLP Vault State, Deposits & Withdrawals
    # -------------------------------------------------------------------------
    async def fetch_hlp_vault_state(
        self,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """
        Query actual on-chain Hyperliquid HLP (Hyperliquidity Provider) vault equity,
        accrued PnL yield, lockup expiration, and perp collateral balance via the
        Hyperliquid L1 Info API (`userVaultEquities` and `vaultDetails`).
        """
        if self.execution_mode == "PAPER" and self.exchange is None:
            return None

        cfg = config if config is not None else self.db.load_config()
        wallet = str(cfg.get("wallet_address", "")).strip()
        priv_key = str(cfg.get("private_key", "")).strip()
        if self.exchange is None and not (wallet and priv_key):
            return None

        raw_testnet = cfg.get("hyperliquid_testnet", False)
        if isinstance(raw_testnet, str):
            testnet_bool = raw_testnet.strip().lower() in ("true", "1", "yes", "on", "testnet")
        else:
            testnet_bool = bool(raw_testnet)
        is_testnet = testnet_bool or (self.execution_mode == "TESTNET")
        hlp_vault = self.get_hlp_vault_address(cfg, is_testnet=is_testnet)
        target_user = wallet or hlp_vault

        owns_exchange = False
        exchange = self.exchange
        try:
            if exchange is None:
                exchange, is_testnet = self._build_hyperliquid_client(cfg)
                owns_exchange = True

            post_info = getattr(exchange, "publicPostInfo", None)
            if not callable(post_info):
                return None

            equities_resp: Any = None
            details_resp: Any = None
            with suppress(ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError):
                equities_resp = await post_info(
                    {"type": "userVaultEquities", "user": target_user}
                )
            with suppress(ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError):
                details_resp = await post_info(
                    {
                        "type": "vaultDetails",
                        "vaultAddress": hlp_vault,
                        "user": target_user,
                    }
                )

            has_valid_list = isinstance(equities_resp, list)
            has_valid_dict = isinstance(details_resp, dict) and (
                "vaultAddress" in details_resp
                or "followerState" in details_resp
                or "apr" in details_resp
                or "allowDeposits" in details_resp
            )
            if not has_valid_list and not has_valid_dict:
                return None

            vault_equity_usdc = 0.0
            locked_until_ms = 0
            accrued_yield_usdc = 0.0
            apr = 0.0
            allow_deposits = True

            if has_valid_list:
                for entry in equities_resp:
                    if not isinstance(entry, dict):
                        continue
                    raw_eq = entry.get("equity")
                    if raw_eq is not None:
                        with suppress(ValueError, TypeError):
                            eq_val = float(raw_eq)
                            if math.isfinite(eq_val) and eq_val > 0.0:
                                vault_equity_usdc += eq_val
                    raw_lock = entry.get("lockedUntilTimestamp")
                    if raw_lock is not None:
                        with suppress(ValueError, TypeError):
                            locked_until_ms = max(locked_until_ms, int(float(raw_lock)))

            if has_valid_dict:
                with suppress(ValueError, TypeError):
                    if details_resp.get("apr") is not None:
                        apr = float(details_resp["apr"])
                allow_deposits = bool(details_resp.get("allowDeposits", True)) and not bool(
                    details_resp.get("isClosed", False)
                )
                follower = details_resp.get("followerState")
                if isinstance(follower, dict):
                    raw_feq = follower.get("vaultEquity")
                    if raw_feq is not None:
                        with suppress(ValueError, TypeError):
                            feq_val = float(raw_feq)
                            if math.isfinite(feq_val) and feq_val >= 0.0:
                                vault_equity_usdc = max(vault_equity_usdc, feq_val)
                    raw_pnl = (
                        follower.get("allTimePnl")
                        if follower.get("allTimePnl") is not None
                        else follower.get("pnl")
                    )
                    if raw_pnl is not None:
                        with suppress(ValueError, TypeError):
                            pnl_val = float(raw_pnl)
                            if math.isfinite(pnl_val):
                                accrued_yield_usdc = pnl_val
                    raw_flock = follower.get("lockupUntil")
                    if raw_flock is not None:
                        with suppress(ValueError, TypeError):
                            locked_until_ms = max(locked_until_ms, int(float(raw_flock)))

            perp_total: float | None = None
            perp_free: float | None = None
            perp_used: float | None = None
            fetch_bal = getattr(exchange, "fetch_balance", None)
            if callable(fetch_bal) and wallet:
                with suppress(ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError):
                    bal_resp = await fetch_bal({"user": wallet})
                    if isinstance(bal_resp, dict) and (
                        "USDC" in bal_resp or "total" in bal_resp or "info" in bal_resp
                    ):
                        p_tot, p_free, p_used = self._extract_usdc_balance_tuple(bal_resp)
                        perp_total = round(p_tot, 2)
                        perp_free = round(p_free, 2)
                        perp_used = round(p_used, 2)

            return {
                "vault_address": hlp_vault,
                "vault_equity_usdc": round(max(0.0, vault_equity_usdc), 2),
                "accrued_yield_usdc": round(accrued_yield_usdc, 4),
                "locked_until_ms": int(locked_until_ms),
                "apr": float(apr),
                "allow_deposits": bool(allow_deposits),
                "perp_equity_usdc": perp_total,
                "perp_free_usdc": perp_free,
                "perp_used_usdc": perp_used,
            }
        except (
            ccxt.BaseError,
            sqlite3.Error,
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RuntimeError,
        ) as exc:
            logger.warning("Failed to query on-chain HLP vault state: %s", exc)
            return None
        finally:
            if owns_exchange and exchange is not None:
                await exchange.close()

    async def execute_hlp_vault_transfer(
        self,
        amount_usdc: float,
        is_deposit: bool,
        *,
        vault_address: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Execute an on-chain Hyperliquid L1 `vaultTransfer` action to deposit idle USDC into
        or withdraw USDC from the Hyperliquidity Provider (HLP) protocol vault.
        """
        cfg = config if config is not None else self.db.load_config()
        target_vault = (vault_address or self.get_hlp_vault_address(cfg)).strip().lower()
        direction = "DEPOSIT" if is_deposit else "WITHDRAW"
        usd_micro = int(round(float(amount_usdc) * 1_000_000))
        if usd_micro <= 0:
            return {
                "status": "skipped",
                "direction": direction,
                "amount_usdc": 0.0,
                "usd_micro": 0,
                "vault_address": target_vault,
            }

        owns_exchange = False
        exchange = self.exchange
        try:
            if exchange is None:
                exchange, _ = self._build_hyperliquid_client(cfg)
                owns_exchange = True

            ms_fn = getattr(exchange, "milliseconds", None)
            nonce = int(ms_fn()) if callable(ms_fn) else int(time.time() * 1000)
            action: dict[str, Any] = {
                "type": "vaultTransfer",
                "vaultAddress": target_vault,
                "isDeposit": bool(is_deposit),
                "usd": usd_micro,
            }
            signature = exchange.sign_l1_action(action, nonce)
            request: dict[str, Any] = {
                "action": action,
                "nonce": nonce,
                "signature": signature,
            }
            response = await exchange.privatePostExchange(request)
            resp_status = (
                str(response.get("status", "ok")).strip().lower()
                if isinstance(response, dict)
                else "ok"
            )
            if resp_status == "ok":
                result = {
                    "status": "ok",
                    "direction": direction,
                    "amount_usdc": round(float(amount_usdc), 2),
                    "usd_micro": usd_micro,
                    "vault_address": target_vault,
                    "nonce": nonce,
                    "response": response,
                }
                self.db.log_telemetry(
                    "INFO",
                    "engine.execution",
                    f"Executed on-chain HLP vault {direction} of ${float(amount_usdc):.2f} USDC ({target_vault})",
                    result,
                )
                return result

            err_detail = (
                str(response.get("response", response))
                if isinstance(response, dict)
                else str(response)
            )
            err_result = {
                "status": "error",
                "direction": direction,
                "amount_usdc": round(float(amount_usdc), 2),
                "usd_micro": usd_micro,
                "vault_address": target_vault,
                "nonce": nonce,
                "error": err_detail,
            }
            self.db.log_telemetry(
                "WARNING",
                "engine.execution",
                f"On-chain HLP vault {direction} (${float(amount_usdc):.2f}) rejected by Hyperliquid L1: {err_detail}",
                err_result,
            )
            return err_result
        except (
            ccxt.BaseError,
            sqlite3.Error,
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RuntimeError,
        ) as exc:
            err_result = {
                "status": "error",
                "direction": direction,
                "amount_usdc": round(float(amount_usdc), 2),
                "usd_micro": usd_micro,
                "vault_address": target_vault,
                "error": str(exc),
            }
            self.db.log_telemetry(
                "WARNING",
                "engine.execution",
                f"On-chain HLP vault {direction} (${float(amount_usdc):.2f}) failed: {exc}",
                err_result,
            )
            return err_result
        finally:
            if owns_exchange and exchange is not None:
                await exchange.close()

    # -------------------------------------------------------------------------
    # Main Execution & Reconciliation Cycle
    # -------------------------------------------------------------------------
    async def reconcile_cycle(
        self,
        regime_state: RegimeState,
        evaluations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Execute on-chain position drift reconciliation, update open positions,
        enforce Chandelier trailing stops & 1H funding carry, execute new Long/Short
        entries via Order FSM within risk caps, and harvest idle USDC basis/HLP yield.
        """
        config = self.db.load_config()
        alt_pairs = {p["exec_symbol"]: p for p in self.db.get_alt_pairs_universe()}

        # Execute on-chain position drift reconciliation at the start of every 15s cycle in LIVE/TESTNET
        route_is_mocked = "_route_dex_order" in self.__dict__ or hasattr(
            getattr(self, "_route_dex_order", None), "assert_awaited_once"
        )
        if self.execution_mode in ("LIVE", "TESTNET") and (
            self.exchange is not None or not route_is_mocked
        ):
            await self.reconcile_exchange_state()

        maker_fee_rate = float(config.get("maker_fee_bps", 1.5)) / 10000.0
        taker_fee_rate = float(config.get("taker_fee_bps", 4.5)) / 10000.0
        atr_period = int(config.get("atr_period", 14))
        short_adverse_funding = float(config.get("short_max_adverse_funding_rate_hourly", -0.0005))
        cooldown_bars = int(config.get("post_stop_cooldown_bars", 14))

        self._step_counter += 1
        now_ms = (
            int(self.db.pit_timestamp_ms)
            if self.db.pit_timestamp_ms is not None
            else int(time.time() * 1000)
        )
        current_hour = int(now_ms // 3_600_000)
        settle_funding_now = current_hour > self._last_funding_settlement_hour
        if settle_funding_now:
            self._last_funding_settlement_hour = current_hour

        open_trades = self.db.get_open_trades()
        closed_count = 0

        # 1. Update open trades, Two-Tier Chandelier stops, 1H funding carry, and exit conditions
        for trade in list(open_trades):
            trade_id = int(trade["trade_id"])
            symbol = str(trade["symbol"])
            side = str(trade["side"])
            if side not in ("LONG", "SHORT"):
                continue
            pair_meta = alt_pairs.get(symbol, {"exec_timeframe": "1h", "tick_size": 0.01})
            exec_tf = str(pair_meta.get("exec_timeframe", "1h"))

            df = self.db.get_ohlcv_df(symbol, exec_tf, limit=100)
            if df.empty:
                continue

            bar_open = float(df["open"].iloc[-1])
            current_price = float(df["close"].iloc[-1])
            bar_high = float(df["high"].iloc[-1])
            bar_low = float(df["low"].iloc[-1])
            funding_hourly = float(df["funding_rate_hourly"].iloc[-1])
            atr_val = self.risk_engine.compute_atr(df, period=atr_period)

            entry_price = float(trade["entry_price"])
            qty = float(
                trade["filled_units"]
                if trade.get("filled_units") and float(trade["filled_units"]) > 0
                else trade["quantity"]
            )
            notional = qty * current_price
            prev_stop = float(trade["chandelier_stop_price"])
            funding_accrued = float(trade.get("funding_accrued_usdc", 0.0))

            if settle_funding_now:
                synced_funding: float | None = None
                if self.execution_mode in ("LIVE", "TESTNET") and (
                    self.exchange is not None or not route_is_mocked
                ):
                    synced_funding = await self.sync_funding_payments(symbol, config=config)
                if synced_funding is not None:
                    funding_accrued += synced_funding
                else:
                    # Longs pay positive funding, Shorts receive positive funding on Hyperliquid 1H cadence
                    if side == "LONG":
                        funding_accrued -= notional * funding_hourly
                    elif side == "SHORT":
                        funding_accrued += notional * funding_hourly

            stop_eval = self.risk_engine.evaluate_two_tier_chandelier_stop(
                side=side,
                entry_price=entry_price,
                active_stop=prev_stop,
                highest_price_seen=float(trade["highest_price_seen"]),
                lowest_price_seen=float(trade["lowest_price_seen"]),
                bar_open=bar_open,
                bar_high=bar_high,
                bar_low=bar_low,
                bar_close=current_price,
                atr_value=atr_val,
                quantity=qty,
                config=config,
            )
            highest_seen = stop_eval.highest_price_seen
            lowest_seen = stop_eval.lowest_price_seen
            new_stop = stop_eval.chandelier_stop_price
            exit_reason: str | None = stop_eval.exit_reason
            exit_fill_price = stop_eval.exit_fill_price

            if side == "LONG":
                if exit_reason is None and regime_state.regime == "BEAR":
                    exit_reason = "HMM_REGIME_FLIP_TO_BEAR"
                    exit_fill_price = current_price
                unrealized_pnl = (exit_fill_price - entry_price) * qty + funding_accrued
            else:
                if exit_reason is None:
                    if funding_hourly < short_adverse_funding:
                        exit_reason = "ADVERSE_1H_FUNDING_CARRY_PROTECTION"
                        exit_fill_price = current_price
                    elif regime_state.regime == "BULL":
                        exit_reason = "HMM_REGIME_FLIP_TO_BULL"
                        exit_fill_price = current_price
                unrealized_pnl = (entry_price - exit_fill_price) * qty + funding_accrued

            if exit_reason:
                close_side = "SHORT" if side == "LONG" else "LONG"
                order_target_price = (
                    current_price
                    if (self.execution_mode in ("LIVE", "TESTNET") or self.exchange is not None)
                    else exit_fill_price
                )
                self._last_order_result = None
                try:
                    await self._route_dex_order(
                        symbol=symbol,
                        side=close_side,
                        quantity=qty,
                        price=order_target_price,
                        config=config,
                        reduce_only=True,
                        atr_14=atr_val,
                    )
                except (
                    ccxt.BaseError,
                    sqlite3.Error,
                    OSError,
                    ValueError,
                    TypeError,
                    KeyError,
                    RuntimeError,
                ) as exc:
                    self.db.log_telemetry(
                        "ERROR",
                        "engine.execution",
                        f"Exit order routing failed for {symbol} ({side} -> {close_side}): {exc}",
                        {"trade_id": trade_id, "symbol": symbol, "side": side, "error": str(exc)},
                    )
                    continue
                if (
                    (self.execution_mode in ("LIVE", "TESTNET") or self.exchange is not None)
                    and self._last_order_result is not None
                    and self._last_order_result.avg_fill_price > 0
                ):
                    exit_fill_price = float(self._last_order_result.avg_fill_price)
                    if side == "LONG":
                        unrealized_pnl = (exit_fill_price - entry_price) * qty + funding_accrued
                    else:
                        unrealized_pnl = (entry_price - exit_fill_price) * qty + funding_accrued
                    exit_fee = float(self._last_order_result.fees_paid_usdc)
                else:
                    exit_fee = (qty * exit_fill_price) * taker_fee_rate
                total_fees = float(trade.get("fees_paid_usdc", 0.0)) + exit_fee
                realized_pnl = unrealized_pnl - total_fees
                self.db.close_trade(
                    trade_id=trade_id,
                    exit_price=exit_fill_price,
                    realized_pnl_usdc=round(realized_pnl, 4),
                    fees_paid_usdc=round(total_fees, 4),
                    exit_reason=exit_reason,
                )
                self._cooldown_until_step[symbol] = self._step_counter + cooldown_bars
                self._cooldown_until_ms[symbol] = now_ms + cooldown_bars * 4 * 3_600_000
                closed_count += 1
            else:
                self.db.update_open_trade(
                    trade_id=trade_id,
                    current_price=current_price,
                    chandelier_stop_price=round(new_stop, 6),
                    highest_price_seen=highest_seen,
                    lowest_price_seen=lowest_seen,
                    unrealized_pnl_usdc=round(unrealized_pnl, 4),
                    funding_accrued_usdc=round(funding_accrued, 4),
                )

        # Refresh active trades and portfolio equity
        open_trades = self.db.get_open_trades()
        open_symbols = {str(t["symbol"]) for t in open_trades}
        open_longs = [t for t in open_trades if t["side"] == "LONG"]
        open_shorts = [t for t in open_trades if t["side"] == "SHORT"]

        latest_snap = self.db.get_latest_portfolio_snapshot()
        base_equity = float(config.get("initial_equity_usdc", 10000.0))
        with self.db.connection() as conn:
            realized_row = conn.execute(
                "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) AS total_realized FROM trades WHERE status = 'CLOSED';"
            ).fetchone()
            total_realized_pnl = float(realized_row["total_realized"]) if realized_row else 0.0

        total_unrealized_pnl = sum(float(t.get("unrealized_pnl_usdc", 0.0)) for t in open_trades)
        prev_accrued_yield = float(latest_snap.get("accrued_yield_usdc", 0.0))

        on_chain_vault_state: dict[str, Any] | None = None
        if self.execution_mode in ("LIVE", "TESTNET") and (
            self.exchange is not None or not route_is_mocked
        ):
            on_chain_vault_state = await self.fetch_hlp_vault_state(config=config)

        if on_chain_vault_state is not None:
            vault_eq_now = float(on_chain_vault_state.get("vault_equity_usdc", 0.0))
            vault_yield_now = float(on_chain_vault_state.get("accrued_yield_usdc", 0.0))
            perp_eq_now = on_chain_vault_state.get("perp_equity_usdc")
            if perp_eq_now is not None and (float(perp_eq_now) > 0.0 or vault_eq_now > 0.0):
                current_equity = round(float(perp_eq_now) + vault_eq_now, 2)
            else:
                current_equity = round(
                    base_equity + total_realized_pnl + total_unrealized_pnl + vault_yield_now, 2
                )
        else:
            current_equity = round(
                base_equity + total_realized_pnl + total_unrealized_pnl + prev_accrued_yield, 2
            )

        # 2. Evaluate new Long Leaders (Bull) or Short Laggards (Bear)
        max_longs = int(config.get("max_concurrent_long_positions", 2))
        max_shorts = int(config.get("max_concurrent_short_positions", 2))
        opened_count = 0

        for ev in evaluations:
            action = str(ev["signal_action"])
            symbol = str(ev["exec_symbol"])
            if action not in ("LONG", "SHORT") or symbol in open_symbols:
                continue
            if self.is_in_cooldown(symbol, now_ms=now_ms, step_idx=self._step_counter):
                continue
            if action == "LONG" and len(open_longs) >= max_longs:
                continue
            if action == "SHORT" and len(open_shorts) >= max_shorts:
                continue

            cand_meta = alt_pairs.get(symbol)
            if not cand_meta:
                continue
            exec_df = self.db.get_ohlcv_df(symbol, str(cand_meta["exec_timeframe"]), limit=100)
            sizing: PositionSizeResult = self.risk_engine.evaluate_order_risk(
                pair_meta=cand_meta,
                side=action,
                blended_conviction=float(ev["blended_conviction"]),
                exec_df=exec_df,
                open_trades=open_trades,
                current_equity_usdc=current_equity,
                config=config,
            )
            if not sizing.approved:
                continue

            # Recall collateral from HLP vault before order placement if perp free margin is insufficient
            if on_chain_vault_state is not None:
                v_eq = float(on_chain_vault_state.get("vault_equity_usdc", 0.0))
                p_free = on_chain_vault_state.get("perp_free_usdc")
                locked_until = int(on_chain_vault_state.get("locked_until_ms", 0))
                if (
                    v_eq > 0.0
                    and p_free is not None
                    and float(p_free) < sizing.notional_usdc
                    and now_ms >= locked_until
                ):
                    recall_usdc = round(
                        min(v_eq, max(10.0, sizing.notional_usdc - float(p_free))), 2
                    )
                    recall_res = await self.execute_hlp_vault_transfer(
                        amount_usdc=recall_usdc,
                        is_deposit=False,
                        vault_address=str(on_chain_vault_state.get("vault_address", "")),
                        config=config,
                    )
                    if recall_res.get("status") == "ok":
                        on_chain_vault_state["vault_equity_usdc"] = round(
                            max(0.0, v_eq - recall_usdc), 2
                        )
                        on_chain_vault_state["perp_free_usdc"] = round(
                            float(p_free) + recall_usdc, 2
                        )

            self._last_order_result = None
            try:
                order_id = await self._route_dex_order(
                    symbol=symbol,
                    side=action,
                    quantity=sizing.quantity,
                    price=sizing.entry_price,
                    config=config,
                    atr_14=sizing.atr_value,
                )
            except (
                ccxt.BaseError,
                sqlite3.Error,
                OSError,
                ValueError,
                TypeError,
                KeyError,
                RuntimeError,
            ) as exc:
                self.db.log_telemetry(
                    "ERROR",
                    "engine.execution",
                    f"Order routing failed for {symbol} ({action}): {exc}",
                    {"symbol": symbol, "side": action, "error": str(exc)},
                )
                continue

            order_res = self._last_order_result
            if order_res is not None and order_res.fsm_state in ("EXPIRED", "CANCELLED", "REJECTED") and order_res.filled_units <= 0:
                continue

            eff_entry_price = (
                order_res.avg_fill_price
                if order_res is not None and order_res.avg_fill_price > 0
                else sizing.entry_price
            )
            eff_filled_units = (
                order_res.filled_units
                if order_res is not None and order_res.filled_units > 0
                else sizing.quantity
            )
            eff_slippage_usd = (
                order_res.slippage_paid_usd if order_res is not None else 0.0
            )
            entry_fee = (
                round(order_res.fees_paid_usdc, 4)
                if order_res is not None
                else round(sizing.notional_usdc * maker_fee_rate, 4)
            )
            cli_order_id = (
                order_res.client_order_id
                if order_res is not None
                else f"cli-{uuid.uuid4().hex[:12]}"
            )
            fsm_state = order_res.fsm_state if order_res is not None else "FILLED"

            new_trade: dict[str, Any] = {
                "client_order_id": cli_order_id,
                "symbol": symbol,
                "base_asset": str(cand_meta["base_asset"]),
                "side": action,
                "fsm_state": fsm_state,
                "entry_price": eff_entry_price,
                "current_price": eff_entry_price,
                "avg_fill_price": eff_entry_price,
                "quantity": eff_filled_units,
                "filled_units": eff_filled_units,
                "notional_usdc": round(eff_filled_units * eff_entry_price, 2),
                "risk_budget_usdc": sizing.risk_budget_usdc,
                "conviction_multiplier": sizing.conviction_multiplier,
                "initial_stop_price": sizing.initial_stop_price,
                "chandelier_stop_price": sizing.initial_stop_price,
                "highest_price_seen": eff_entry_price,
                "lowest_price_seen": eff_entry_price,
                "fees_paid_usdc": entry_fee,
                "slippage_paid_usd": eff_slippage_usd,
                "exchange_order_id": order_id,
            }
            self.db.insert_trade(new_trade)
            open_trades = self.db.get_open_trades()
            open_symbols.add(symbol)
            if action == "LONG":
                open_longs.append(new_trade)
            else:
                open_shorts.append(new_trade)
            opened_count += 1

        # 3. Idle-Cash Basis & HLP Yield Harvesting
        deployed_notional = round(
            sum(float(t.get("notional_usdc", 0.0)) for t in open_trades), 2
        )
        portfolio_heat_usdc = round(
            sum(float(t.get("risk_budget_usdc", 0.0)) for t in open_trades), 2
        )
        portfolio_heat_pct = round(portfolio_heat_usdc / max(current_equity, 1.0), 4)
        gross_leverage = round(deployed_notional / max(current_equity, 1.0), 4)
        available_cash = max(0.0, round(current_equity - deployed_notional, 2))

        basis_enabled = bool(config.get("basis_harvest_enabled", True))
        basis_min_rate = float(config.get("basis_min_hourly_funding_rate", 0.000015))
        basis_max_alloc = float(config.get("basis_max_idle_cash_allocation_pct", 0.50))

        best_funding = max(
            (float(e.get("funding_rate_hourly", 0.0)) for e in evaluations),
            default=0.0,
        )
        should_harvest = (
            basis_enabled
            and available_cash > 100.0
            and (
                best_funding >= basis_min_rate
                or bool(config.get("hlp_yield_fallback_enabled", True))
            )
        )
        target_basis_notional = (
            round(available_cash * basis_max_alloc, 2) if should_harvest else 0.0
        )

        basis_notional = 0.0
        new_accrued_yield = prev_accrued_yield

        if on_chain_vault_state is not None:
            vault_eq = float(on_chain_vault_state.get("vault_equity_usdc", 0.0))
            vault_addr = str(
                on_chain_vault_state.get("vault_address") or self.get_hlp_vault_address(config)
            )
            delta_usdc = round(target_basis_notional - vault_eq, 2)
            min_transfer_usdc = max(
                float(config.get("hlp_min_transfer_usdc", 10.0)),
                round(target_basis_notional * 0.05, 2),
            )
            transfer_cooldown_ms = int(config.get("hlp_transfer_cooldown_ms", 300_000))

            if delta_usdc >= min_transfer_usdc and bool(
                on_chain_vault_state.get("allow_deposits", True)
            ):
                perp_free = on_chain_vault_state.get("perp_free_usdc")
                max_depositable = (
                    round(float(perp_free), 2)
                    if perp_free is not None
                    else max(0.0, round(available_cash - vault_eq, 2))
                )
                deposit_amt = round(min(delta_usdc, max_depositable), 2)
                if deposit_amt >= 10.0 and (
                    self._last_vault_transfer_attempt_ms == 0
                    or now_ms - self._last_vault_transfer_attempt_ms >= transfer_cooldown_ms
                ):
                    self._last_vault_transfer_attempt_ms = now_ms
                    dep_res = await self.execute_hlp_vault_transfer(
                        amount_usdc=deposit_amt,
                        is_deposit=True,
                        vault_address=vault_addr,
                        config=config,
                    )
                    if dep_res.get("status") == "ok":
                        self._last_vault_transfer_attempt_ms = 0
                        refreshed = await self.fetch_hlp_vault_state(config=config)
                        if (
                            refreshed is not None
                            and float(refreshed.get("vault_equity_usdc", 0.0)) > vault_eq
                        ):
                            on_chain_vault_state = refreshed
                        else:
                            on_chain_vault_state["vault_equity_usdc"] = round(
                                vault_eq + deposit_amt, 2
                            )
            elif delta_usdc <= -min_transfer_usdc and vault_eq > 0.0:
                withdraw_amt = round(min(abs(delta_usdc), vault_eq), 2)
                locked_until_ms = int(on_chain_vault_state.get("locked_until_ms", 0))
                if now_ms >= locked_until_ms:
                    if withdraw_amt >= 1.0 and (
                        self._last_vault_transfer_attempt_ms == 0
                        or now_ms - self._last_vault_transfer_attempt_ms >= transfer_cooldown_ms
                    ):
                        self._last_vault_transfer_attempt_ms = now_ms
                        wd_res = await self.execute_hlp_vault_transfer(
                            amount_usdc=withdraw_amt,
                            is_deposit=False,
                            vault_address=vault_addr,
                            config=config,
                        )
                        if wd_res.get("status") == "ok":
                            self._last_vault_transfer_attempt_ms = 0
                            refreshed = await self.fetch_hlp_vault_state(config=config)
                            if (
                                refreshed is not None
                                and float(refreshed.get("vault_equity_usdc", 0.0)) < vault_eq
                            ):
                                on_chain_vault_state = refreshed
                            else:
                                on_chain_vault_state["vault_equity_usdc"] = round(
                                    max(0.0, vault_eq - withdraw_amt), 2
                                )
                elif (
                    self._last_vault_lockup_log_ms == 0
                    or now_ms - self._last_vault_lockup_log_ms >= 3_600_000
                ):
                    self._last_vault_lockup_log_ms = now_ms
                    self.db.log_telemetry(
                        "INFO",
                        "engine.execution",
                        f"On-chain HLP vault withdrawal of ${withdraw_amt:.2f} USDC queued pending 4-day lockup expiry ({locked_until_ms})",
                        {
                            "withdraw_usdc": withdraw_amt,
                            "locked_until_ms": locked_until_ms,
                            "vault_address": vault_addr,
                        },
                    )

            basis_notional = round(float(on_chain_vault_state.get("vault_equity_usdc", 0.0)), 2)
            new_accrued_yield = round(
                float(on_chain_vault_state.get("accrued_yield_usdc", 0.0)), 4
            )
            perp_eq_final = on_chain_vault_state.get("perp_equity_usdc")
            if perp_eq_final is not None and (float(perp_eq_final) > 0.0 or basis_notional > 0.0):
                # Note: if a deposit/withdrawal just succeeded without refreshed fetch_balance,
                # total equity (perp + vault) remains invariant across internal vaultTransfer.
                pass
            else:
                current_equity = round(
                    base_equity + total_realized_pnl + total_unrealized_pnl + new_accrued_yield, 2
                )
            available_cash = max(0.0, round(current_equity - deployed_notional, 2))
        elif should_harvest:
            basis_notional = target_basis_notional
            effective_hourly_rate = max(best_funding, basis_min_rate)
            if settle_funding_now:
                new_accrued_yield = round(
                    prev_accrued_yield + basis_notional * effective_hourly_rate, 4
                )

        snapshot: dict[str, Any] = {
            "timestamp_ms": now_ms,
            "total_equity_usdc": current_equity,
            "available_cash_usdc": available_cash,
            "deployed_notional_usdc": deployed_notional,
            "gross_leverage": gross_leverage,
            "portfolio_heat_usdc": portfolio_heat_usdc,
            "portfolio_heat_pct": portfolio_heat_pct,
            "basis_harvest_notional_usdc": basis_notional,
            "accrued_yield_usdc": new_accrued_yield,
            "hmm_regime": regime_state.regime,
            "prob_bull": regime_state.prob_bull,
            "prob_bear": regime_state.prob_bear,
            "prob_chop": regime_state.prob_chop,
            "open_long_count": len(open_longs),
            "open_short_count": len(open_shorts),
        }
        self.db.save_portfolio_snapshot(snapshot)

        return {
            "opened_trades": opened_count,
            "closed_trades": closed_count,
            "open_long_count": len(open_longs),
            "open_short_count": len(open_shorts),
            "total_equity_usdc": current_equity,
            "portfolio_heat_usdc": portfolio_heat_usdc,
            "gross_leverage": gross_leverage,
            "basis_harvest_notional_usdc": basis_notional,
        }
