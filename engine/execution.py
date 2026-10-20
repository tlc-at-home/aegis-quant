"""Hyperliquid Web3 DEX order router, Chandelier trailing stops, and Basis/HLP yield harvester."""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any

import ccxt.async_support as ccxt

from engine.database import DatabaseManager
from engine.risk import PositionSizeResult, RiskEngine
from engine.signals import RegimeState

logger = logging.getLogger("aegis.execution")


class ExecutionEngine:
    """
    Executes Long Leader, Bear Laggard Short, Chandelier trailing stop exits,
    1-hour funding settlement carry checks, and Idle-Cash Basis/HLP Yield Harvesting.
    """

    def __init__(self, db: DatabaseManager, risk_engine: RiskEngine) -> None:
        self.db = db
        self.risk_engine = risk_engine
        self._last_funding_settlement_hour: int = int(time.time() // 3600)

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

    async def _route_dex_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        config: dict[str, Any],
        reduce_only: bool = False,
    ) -> str:
        """Route order to Hyperliquid L1 Mainnet/Testnet CLOB if LIVE/TESTNET and credentials present, else paper fill."""
        mode = str(config.get("execution_mode", "PAPER")).strip().upper()
        wallet = str(config.get("wallet_address", "")).strip()
        priv_key = str(config.get("private_key", "")).strip()

        if mode in ("LIVE", "TESTNET") and wallet and priv_key:
            exchange, is_testnet = self._build_hyperliquid_client(config)
            prefix = "hl-testnet" if is_testnet else "hl-live"
            try:
                ccxt_side = "buy" if side == "LONG" else "sell"
                order_params: dict[str, Any] = {"reduceOnly": True} if reduce_only else {}
                order = await exchange.create_order(
                    symbol=symbol,
                    type="limit",
                    side=ccxt_side,
                    amount=quantity,
                    price=price,
                    params=order_params,
                )
                return str(order.get("id", f"{prefix}-{uuid.uuid4().hex[:10]}"))
            except Exception as exc:
                logger.error(
                    "Hyperliquid %s order routing error on %s: %s",
                    "TESTNET" if is_testnet else "LIVE",
                    symbol,
                    exc,
                )
                raise
            finally:
                await exchange.close()

        return f"hl-paper-{uuid.uuid4().hex[:10]}"

    async def reconcile_cycle(
        self,
        regime_state: RegimeState,
        evaluations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Update open positions, enforce Chandelier trailing stops & 1H funding carry,
        execute new Long/Short entries within risk caps, and harvest idle USDC basis/HLP yield.
        """
        config = self.db.load_config()
        alt_pairs = {p["exec_symbol"]: p for p in self.db.get_alt_pairs_universe()}
        eval_by_sym = {e["exec_symbol"]: e for e in evaluations}

        maker_fee_rate = float(config.get("maker_fee_bps", 1.5)) / 10000.0
        taker_fee_rate = float(config.get("taker_fee_bps", 4.5)) / 10000.0
        atr_period = int(config.get("atr_period", 14))
        long_atr_mult = float(config.get("chandelier_atr_multiplier_long", 3.0))
        short_atr_mult = float(config.get("chandelier_atr_multiplier_short", 2.5))
        short_adverse_funding = float(config.get("short_max_adverse_funding_rate_hourly", -0.0005))

        current_hour = int(time.time() // 3600)
        settle_funding_now = current_hour > self._last_funding_settlement_hour
        if settle_funding_now:
            self._last_funding_settlement_hour = current_hour

        open_trades = self.db.get_open_trades()
        closed_count = 0

        # 1. Update open trades, Chandelier stops, 1H funding carry, and exit conditions
        for trade in list(open_trades):
            trade_id = int(trade["trade_id"])
            symbol = str(trade["symbol"])
            side = str(trade["side"])
            pair_meta = alt_pairs.get(symbol, {"exec_timeframe": "1h", "tick_size": 0.01})
            exec_tf = str(pair_meta.get("exec_timeframe", "1h"))

            df = self.db.get_ohlcv_df(symbol, exec_tf, limit=100)
            if df.empty:
                continue

            current_price = float(df["close"].iloc[-1])
            bar_high = float(df["high"].iloc[-1])
            bar_low = float(df["low"].iloc[-1])
            funding_hourly = float(df["funding_rate_hourly"].iloc[-1])
            atr_val = self.risk_engine.compute_atr(df, period=atr_period)

            entry_price = float(trade["entry_price"])
            qty = float(trade["quantity"])
            notional = qty * current_price
            highest_seen = max(float(trade["highest_price_seen"]), bar_high, current_price)
            lowest_seen = min(float(trade["lowest_price_seen"]), bar_low, current_price)
            prev_stop = float(trade["chandelier_stop_price"])
            funding_accrued = float(trade.get("funding_accrued_usdc", 0.0))

            if settle_funding_now:
                # Longs pay positive funding, Shorts receive positive funding on Hyperliquid 1H cadence
                if side == "LONG":
                    funding_accrued -= notional * funding_hourly
                elif side == "SHORT":
                    funding_accrued += notional * funding_hourly

            exit_reason: str | None = None

            if side == "LONG":
                candidate_stop = highest_seen - long_atr_mult * atr_val
                new_stop = max(prev_stop, candidate_stop)
                unrealized_pnl = (current_price - entry_price) * qty + funding_accrued
                if current_price <= new_stop:
                    exit_reason = "CHANDELIER_TRAILING_STOP_LONG"
                elif regime_state.regime == "BEAR":
                    exit_reason = "HMM_REGIME_FLIP_TO_BEAR"
            elif side == "SHORT":
                candidate_stop = lowest_seen + short_atr_mult * atr_val
                new_stop = min(prev_stop, candidate_stop)
                unrealized_pnl = (entry_price - current_price) * qty + funding_accrued
                if current_price >= new_stop:
                    exit_reason = "INVERTED_CHANDELIER_STOP_SHORT"
                elif funding_hourly < short_adverse_funding:
                    exit_reason = "ADVERSE_1H_FUNDING_CARRY_PROTECTION"
                elif regime_state.regime == "BULL":
                    exit_reason = "HMM_REGIME_FLIP_TO_BULL"
            else:
                continue

            if exit_reason:
                close_side = "SHORT" if side == "LONG" else "LONG"
                try:
                    await self._route_dex_order(
                        symbol=symbol,
                        side=close_side,
                        quantity=qty,
                        price=current_price,
                        config=config,
                        reduce_only=True,
                    )
                except Exception as exc:
                    self.db.log_telemetry(
                        "ERROR",
                        "engine.execution",
                        f"Exit order routing failed for {symbol} ({side} -> {close_side}): {exc}",
                        {"trade_id": trade_id, "symbol": symbol, "side": side, "error": str(exc)},
                    )
                    continue
                exit_fee = notional * taker_fee_rate
                total_fees = float(trade.get("fees_paid_usdc", 0.0)) + exit_fee
                realized_pnl = unrealized_pnl - total_fees
                self.db.close_trade(
                    trade_id=trade_id,
                    exit_price=current_price,
                    realized_pnl_usdc=round(realized_pnl, 4),
                    fees_paid_usdc=round(total_fees, 4),
                    exit_reason=exit_reason,
                )
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
            if action == "LONG" and len(open_longs) >= max_longs:
                continue
            if action == "SHORT" and len(open_shorts) >= max_shorts:
                continue

            pair_meta = alt_pairs.get(symbol)
            if not pair_meta:
                continue
            exec_df = self.db.get_ohlcv_df(symbol, str(pair_meta["exec_timeframe"]), limit=100)
            sizing: PositionSizeResult = self.risk_engine.evaluate_order_risk(
                pair_meta=pair_meta,
                side=action,
                blended_conviction=float(ev["blended_conviction"]),
                exec_df=exec_df,
                open_trades=open_trades,
                current_equity_usdc=current_equity,
                config=config,
            )
            if not sizing.approved:
                continue

            try:
                order_id = await self._route_dex_order(
                    symbol=symbol,
                    side=action,
                    quantity=sizing.quantity,
                    price=sizing.entry_price,
                    config=config,
                )
            except Exception as exc:
                self.db.log_telemetry(
                    "ERROR",
                    "engine.execution",
                    f"Order routing failed for {symbol} ({action}): {exc}",
                    {"symbol": symbol, "side": action, "error": str(exc)},
                )
                continue
            entry_fee = round(sizing.notional_usdc * maker_fee_rate, 4)
            new_trade = {
                "symbol": symbol,
                "base_asset": str(pair_meta["base_asset"]),
                "side": action,
                "entry_price": sizing.entry_price,
                "current_price": sizing.entry_price,
                "quantity": sizing.quantity,
                "notional_usdc": sizing.notional_usdc,
                "risk_budget_usdc": sizing.risk_budget_usdc,
                "conviction_multiplier": sizing.conviction_multiplier,
                "initial_stop_price": sizing.initial_stop_price,
                "chandelier_stop_price": sizing.initial_stop_price,
                "highest_price_seen": sizing.entry_price,
                "lowest_price_seen": sizing.entry_price,
                "fees_paid_usdc": entry_fee,
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
        basis_notional = 0.0
        new_accrued_yield = prev_accrued_yield
        if basis_enabled and available_cash > 100.0 and (
            best_funding >= basis_min_rate or bool(config.get("hlp_yield_fallback_enabled", True))
        ):
            basis_notional = round(available_cash * basis_max_alloc, 2)
            effective_hourly_rate = max(best_funding, basis_min_rate)
            if settle_funding_now:
                new_accrued_yield = round(
                    prev_accrued_yield + basis_notional * effective_hourly_rate, 4
                )

        snapshot = {
            "timestamp_ms": int(time.time() * 1000),
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
