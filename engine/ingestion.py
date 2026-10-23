"""Hyperliquid L1 USDC_PERP poller and synthetic ALT/BTC:SYNTH candle generator."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import sqlite3
import time
from contextlib import suppress
from typing import Any

import ccxt.async_support as ccxt
import numpy as np
import pandas as pd

from engine.database import DatabaseManager

logger = logging.getLogger("aegis.ingestion")

BASE_PRICES_USDC: dict[str, float] = {
    "BTC/USDC:USDC": 68500.0,
    "ETH/USDC:USDC": 2650.0,
    "SOL/USDC:USDC": 175.0,
    "HYPE/USDC:USDC": 24.5,
    "XRP/USDC:USDC": 0.56,
    "DOGE/USDC:USDC": 0.145,
    "ADA/USDC:USDC": 0.38,
    "AVAX/USDC:USDC": 28.5,
    "LINK/USDC:USDC": 12.4,
    "SUI/USDC:USDC": 2.15,
    "DOT/USDC:USDC": 4.35,
    "NEAR/USDC:USDC": 4.85,
    "APT/USDC:USDC": 9.80,
    "ARB/USDC:USDC": 0.58,
    "OP/USDC:USDC": 1.65,
    "INJ/USDC:USDC": 21.2,
}

TIMEFRAME_SECONDS: dict[str, int] = {
    "15m": 900,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
}


HYPERLIQUID_INFO_WEIGHT_LIMIT_PER_MIN = 1200
HYPERLIQUID_INFO_CALL_WEIGHT = 20


class MarketIngestionEngine:
    """Polls Hyperliquid L1 USDC_PERP markets and synthesizes ALT/BTC:SYNTH cross-pairs."""

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db
        self._exchange: ccxt.hyperliquid | None = None
        self._exchange_config_sig: tuple[str, str, bool, str] | None = None
        self._cycle_network_offline: bool = False
        self._in_universe_cycle: bool = False
        self._cycle_market_snapshot: dict[str, dict[str, float | None]] | None = None
        self._cycle_paper_snapshot: dict[str, dict[str, float]] = {}
        self._paper_tick_counter: int = 0
        self._cycle_funding_cache: dict[str, float] = {}
        self._last_full_ohlcv_sync_bar: dict[tuple[str, str], int] = {}
        self._cycle_ohlcv_calls: int = 0
        self._cycle_snapshot_calls: int = 0
        self._cycle_single_funding_calls: int = 0

    async def _get_exchange(self, config: dict[str, Any]) -> ccxt.hyperliquid:
        """Initialize or return the async CCXT Hyperliquid client, reloading if credentials change."""
        wallet_address = str(config.get("wallet_address", "")).strip()
        private_key = str(config.get("private_key", "")).strip()
        if private_key and not private_key.lower().startswith("0x") and len(private_key) == 64:
            private_key = f"0x{private_key.lower()}"
        mode = str(config.get("execution_mode", "PAPER")).strip().upper()
        raw_testnet = config.get("hyperliquid_testnet", False)
        if isinstance(raw_testnet, str):
            testnet_bool = raw_testnet.strip().lower() in ("true", "1", "yes", "on", "testnet")
        else:
            testnet_bool = bool(raw_testnet)
        is_testnet = testnet_bool or (mode == "TESTNET")
        vault_address = str(config.get("hyperliquid_vault_address", "")).strip()

        sig = (wallet_address, private_key, is_testnet, vault_address)
        if self._exchange is not None and self._exchange_config_sig == sig:
            return self._exchange

        if self._exchange is not None:
            await self.close()

        options: dict[str, Any] = {
            "defaultType": "swap",
            "fetchMarkets": {"types": ["swap"]},
        }
        if vault_address:
            options["vaultAddress"] = vault_address

        params: dict[str, Any] = {
            "enableRateLimit": True,
            "timeout": 5000,
            "options": options,
        }
        if wallet_address:
            params["walletAddress"] = wallet_address
        if private_key:
            params["privateKey"] = private_key

        exchange = ccxt.hyperliquid(params)
        if is_testnet:
            exchange.set_sandbox_mode(True)
        self._exchange = exchange
        self._exchange_config_sig = sig
        self._last_full_ohlcv_sync_bar.clear()
        self._cycle_market_snapshot = None
        self._cycle_paper_snapshot.clear()
        self._cycle_funding_cache.clear()
        return exchange

    async def close(self) -> None:
        """Close the underlying aiohttp session cleanly."""
        if self._exchange is not None:
            try:
                await self._exchange.close()
            except (ccxt.BaseError, OSError, RuntimeError) as exc:
                logger.debug("Error closing Hyperliquid client: %s", exc)
            finally:
                self._exchange = None
                self._exchange_config_sig = None
                self._cycle_market_snapshot = None
                self._cycle_paper_snapshot.clear()
                self._cycle_funding_cache.clear()
                self._last_full_ohlcv_sync_bar.clear()

    @staticmethod
    def _extract_snapshot_entry(item: dict[str, Any]) -> tuple[float | None, float | None]:
        """Extract (price, funding_rate_hourly) from a CCXT Hyperliquid funding/ticker dict."""
        raw_info = item.get("info")
        info: dict[str, Any] = raw_info if isinstance(raw_info, dict) else {}
        price: float | None = None
        for raw_px in (
            info.get("midPx"),
            info.get("markPx"),
            item.get("markPrice"),
            item.get("close"),
            item.get("last"),
        ):
            if raw_px is not None and not isinstance(raw_px, bool):
                with suppress(ValueError, TypeError):
                    val = float(raw_px)
                    if math.isfinite(val) and val > 0.0:
                        price = val
                        break

        funding: float | None = None
        for raw_fr in (item.get("fundingRate"), info.get("funding")):
            if raw_fr is not None and not isinstance(raw_fr, bool):
                with suppress(ValueError, TypeError):
                    val = float(raw_fr)
                    if math.isfinite(val):
                        funding = val
                        break

        return price, funding

    async def _get_live_market_snapshot(
        self, exchange: ccxt.hyperliquid
    ) -> dict[str, dict[str, float | None]]:
        """
        Fetch a single batch market snapshot (metaAndAssetCtxs / swap tickers, weight 20)
        covering mark/mid prices and 1H funding rates across all perpetuals for the current cycle.
        """
        if self._in_universe_cycle and self._cycle_market_snapshot is not None:
            return self._cycle_market_snapshot

        snapshot: dict[str, dict[str, float | None]] = {}
        last_err: Exception | None = None
        fetched_any = False

        if hasattr(exchange, "fetch_funding_rates"):
            try:
                raw_rates = await exchange.fetch_funding_rates()
                if isinstance(raw_rates, dict):
                    self._cycle_snapshot_calls += 1
                    for sym_key, item in raw_rates.items():
                        if isinstance(item, dict):
                            sym = str(item.get("symbol") or sym_key)
                            px, fr = self._extract_snapshot_entry(item)
                            snapshot[sym] = {"price": px, "funding_rate": fr}
                    if snapshot:
                        fetched_any = True
                elif isinstance(raw_rates, list):
                    self._cycle_snapshot_calls += 1
                    for item in raw_rates:
                        if isinstance(item, dict) and item.get("symbol"):
                            sym = str(item["symbol"])
                            px, fr = self._extract_snapshot_entry(item)
                            snapshot[sym] = {"price": px, "funding_rate": fr}
                    if snapshot:
                        fetched_any = True
            except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                last_err = exc

        has_any_price = any(v.get("price") is not None for v in snapshot.values())
        if not has_any_price and hasattr(exchange, "fetch_tickers"):
            try:
                raw_tickers = await exchange.fetch_tickers(params={"type": "swap"})
                if isinstance(raw_tickers, dict):
                    self._cycle_snapshot_calls += 1
                    for sym_key, item in raw_tickers.items():
                        if isinstance(item, dict):
                            sym = str(item.get("symbol") or sym_key)
                            px, fr = self._extract_snapshot_entry(item)
                            prev = snapshot.get(sym, {})
                            snapshot[sym] = {
                                "price": px if px is not None else prev.get("price"),
                                "funding_rate": fr if fr is not None else prev.get("funding_rate"),
                            }
                    if snapshot:
                        fetched_any = True
            except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
                last_err = exc

        if not fetched_any and last_err is not None:
            raise last_err

        if self._in_universe_cycle:
            self._cycle_market_snapshot = snapshot
        return snapshot

    def _get_paper_intrabar_quote(
        self,
        symbol: str,
        last_row: pd.Series,
        now_ms: int,
        poll_interval_sec: int = 15,
    ) -> tuple[float, float]:
        """
        Compute a deterministic, mean-reverting intra-bar mark price and funding rate for PAPER
        simulation so 15s polling updates active candles, ALT/BTC:SYNTH ratios, and open trade PnL
        consistently across all timeframes within a cycle.
        """
        if self._in_universe_cycle and symbol in self._cycle_paper_snapshot:
            cached = self._cycle_paper_snapshot[symbol]
            return cached["price"], cached["funding_rate"]

        bar_open = float(last_row["open"])
        last_close = float(last_row["close"])
        base_funding = float(last_row["funding_rate_hourly"])

        tick_bucket = int(now_ms // max(1000, poll_interval_sec * 1000))
        seed_str = f"{symbol}:paper_intrabar:{tick_bucket}:{self._paper_tick_counter}"
        seed_val = int(hashlib.sha256(seed_str.encode()).hexdigest()[:8], 16)
        rng = np.random.default_rng(seed_val)

        target_px = max(1e-8, bar_open * 1.0012)
        mean_rev = 0.35 * math.log(target_px / max(last_close, 1e-12))
        step_ret = mean_rev + float(rng.normal(0.00008, 0.00085))
        raw_close = max(1e-8, last_close * math.exp(step_ret))
        live_price = float(np.clip(raw_close, bar_open * 0.994, bar_open * 1.008))

        live_funding = float(
            np.clip(base_funding + float(rng.normal(0.0, 0.0000004)), 0.000016, 0.000035)
        )
        if self._in_universe_cycle:
            self._cycle_paper_snapshot[symbol] = {
                "price": live_price,
                "funding_rate": live_funding,
            }
        return live_price, live_funding

    @staticmethod
    def _generate_synthetic_usdc_series(
        symbol: str, timeframe: str, limit: int = 600
    ) -> list[tuple[str, str, int, float, float, float, float, float, float, int]]:
        """Generate deterministic 600-bar OHLCV series when offline or seeding sandbox."""
        tf_sec = TIMEFRAME_SECONDS.get(timeframe, 3600)
        tf_ms = tf_sec * 1000
        now_aligned_ms = (int(time.time() * 1000) // tf_ms) * tf_ms

        seed_int = int(hashlib.sha256(f"{symbol}:{timeframe}".encode()).hexdigest()[:8], 16)
        rng = np.random.default_rng(seed_int)

        base_price = BASE_PRICES_USDC.get(symbol, 10.0)
        # Create a 3-regime drift pattern (Bear -> Chop -> Bull) over 600 bars
        t_idx = np.arange(limit, dtype=np.float64)
        regime_drift = np.where(
            t_idx < 200,
            -0.0006,
            np.where(t_idx < 360, 0.00005, 0.0009),
        )
        # Add asset-specific relative strength tilt
        asset_tilt = ((seed_int % 21) - 10) * 0.00008
        vol_scale = 0.008 if symbol.startswith("BTC/") else 0.013
        log_rets = regime_drift + asset_tilt + rng.normal(0.0, vol_scale, size=limit)

        closes = base_price * np.exp(np.cumsum(log_rets) - np.sum(log_rets))
        opens = np.roll(closes, 1)
        opens[0] = closes[0] * (1.0 - log_rets[0])

        intrabar_spread = np.abs(rng.normal(0.004, 0.002, size=limit))
        highs = np.maximum(opens, closes) * (1.0 + intrabar_spread)
        lows = np.minimum(opens, closes) * np.maximum(0.0001, 1.0 - intrabar_spread)
        volumes = rng.uniform(500.0, 5000.0, size=limit) * (1000.0 / max(base_price, 0.01))

        # Hourly funding rate centered around +0.0020%/hr in Bull regime
        funding_rate = float(0.000022 + ((seed_int % 11) - 5) * 0.000003)

        rows: list[tuple[str, str, int, float, float, float, float, float, float, int]] = []
        start_ts = now_aligned_ms - (limit - 1) * tf_ms
        for i in range(limit):
            ts = int(start_ts + i * tf_ms)
            rows.append(
                (
                    symbol,
                    timeframe,
                    ts,
                    float(opens[i]),
                    float(highs[i]),
                    float(lows[i]),
                    float(closes[i]),
                    float(volumes[i]),
                    funding_rate,
                    0,
                )
            )
        return rows

    async def fetch_symbol_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 600,
        config: dict[str, Any] | None = None,
    ) -> list[tuple[str, str, int, float, float, float, float, float, float, int]]:
        """
        Fetch OHLCV candles and hourly funding from Hyperliquid or fallback generator.
        To stay safely within Hyperliquid's 1,200 weight/min /info rate limit at 15s polling,
        full 540+ bar fetch_ohlcv (candleSnapshot, weight 20) calls are made only on startup
        or when a new 1h/4h bar boundary is crossed; intra-bar 15s ticks update the active bar
        from a single cycle-wide batch market snapshot (metaAndAssetCtxs, weight 20).
        """
        cfg = config or self.db.load_config()
        existing_df = self.db.get_ohlcv_df(symbol, timeframe, limit=limit)

        tf_ms = TIMEFRAME_SECONDS.get(timeframe, 3600) * 1000
        now_ms = int(time.time() * 1000)
        now_aligned_ms = (now_ms // tf_ms) * tf_ms
        last_ts = int(existing_df.iloc[-1]["timestamp_ms"]) if not existing_df.empty else 0

        # Attempt live public/authenticated Hyperliquid polling if network is reachable
        mode = str(cfg.get("execution_mode", "PAPER")).strip().upper()
        if mode in ("LIVE", "TESTNET") and not self._cycle_network_offline:
            try:
                exchange = await self._get_exchange(cfg)
                already_synced_bar = (
                    self._last_full_ohlcv_sync_bar.get((symbol, timeframe)) == now_aligned_ms
                )
                needs_full_ohlcv = existing_df.empty or not already_synced_bar

                if needs_full_ohlcv:
                    raw_candles = await exchange.fetch_ohlcv(
                        symbol, timeframe=timeframe, limit=limit
                    )
                    self._cycle_ohlcv_calls += 1
                    funding_hourly = 0.000018
                    snapshot: dict[str, dict[str, float | None]] = {}
                    try:
                        snapshot = await self._get_live_market_snapshot(exchange)
                    except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError):
                        snapshot = {}

                    snap_item = snapshot.get(symbol, {})
                    if snap_item.get("funding_rate") is not None:
                        funding_hourly = float(snap_item["funding_rate"])  # type: ignore[arg-type]
                    elif symbol in self._cycle_funding_cache:
                        funding_hourly = self._cycle_funding_cache[symbol]
                    else:
                        with suppress(
                            ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError
                        ):
                            fr_data = await exchange.fetch_funding_rate(symbol)
                            if isinstance(fr_data, dict) and fr_data.get("fundingRate") is not None:
                                funding_hourly = float(fr_data["fundingRate"])
                                self._cycle_single_funding_calls += 1
                                self._cycle_funding_cache[symbol] = funding_hourly

                    if raw_candles and len(raw_candles) >= 50:
                        self._last_full_ohlcv_sync_bar[(symbol, timeframe)] = now_aligned_ms
                        bars: list[
                            tuple[str, str, int, float, float, float, float, float, float, int]
                        ] = [
                            (
                                symbol,
                                timeframe,
                                int(c[0]),
                                float(c[1]),
                                float(c[2]),
                                float(c[3]),
                                float(c[4]),
                                float(c[5] or 0.0),
                                funding_hourly,
                                0,
                            )
                            for c in raw_candles
                        ]
                        if bars[-1][2] < now_aligned_ms:
                            prev_close = float(bars[-1][6])
                            raw_px = snap_item.get("price")
                            live_px = (
                                float(raw_px)
                                if (raw_px is not None and raw_px > 0.0)
                                else prev_close
                            )
                            bars.append(
                                (
                                    symbol,
                                    timeframe,
                                    now_aligned_ms,
                                    prev_close,
                                    max(prev_close, live_px),
                                    min(prev_close, live_px),
                                    live_px,
                                    0.0,
                                    funding_hourly,
                                    0,
                                )
                            )
                        return bars
                else:
                    snapshot = await self._get_live_market_snapshot(exchange)
                    last_row = existing_df.iloc[-1]
                    snap_item = snapshot.get(symbol, {})
                    raw_px = snap_item.get("price")
                    live_price = (
                        float(raw_px)
                        if (raw_px is not None and raw_px > 0.0)
                        else float(last_row["close"])
                    )
                    raw_fr = snap_item.get("funding_rate")
                    live_funding = (
                        float(raw_fr)
                        if raw_fr is not None
                        else float(last_row["funding_rate_hourly"])
                    )
                    if now_aligned_ms > last_ts:
                        last_close = float(last_row["close"])
                        return [
                            (
                                symbol,
                                timeframe,
                                now_aligned_ms,
                                last_close,
                                max(last_close, live_price),
                                min(last_close, live_price),
                                live_price,
                                0.0,
                                live_funding,
                                0,
                            )
                        ]
                    return [
                        (
                            symbol,
                            timeframe,
                            last_ts,
                            float(last_row["open"]),
                            max(float(last_row["high"]), live_price),
                            min(float(last_row["low"]), live_price),
                            live_price,
                            float(last_row["volume"]),
                            live_funding,
                            0,
                        )
                    ]
            except (
                ccxt.BaseError,
                sqlite3.Error,
                OSError,
                ValueError,
                TypeError,
                KeyError,
                RuntimeError,
            ) as exc:
                self._cycle_network_offline = True
                logger.warning(
                    "Hyperliquid live/testnet fetch fallback for %s (%s): %s",
                    symbol,
                    timeframe,
                    exc,
                )

        # In sandbox / paper mode, if we already have >= 540 bars, update active bar intra-bar or step on new interval
        if len(existing_df) >= 540:
            poll_sec = max(5, int(cfg.get("polling_interval_seconds", 15)))
            last_row = existing_df.iloc[-1]
            last_ts = int(last_row["timestamp_ms"])
            last_close = float(last_row["close"])
            if now_aligned_ms <= last_ts:
                live_price, live_funding = self._get_paper_intrabar_quote(
                    symbol=symbol,
                    last_row=last_row,
                    now_ms=now_ms,
                    poll_interval_sec=poll_sec,
                )
                return [
                    (
                        symbol,
                        timeframe,
                        last_ts,
                        float(last_row["open"]),
                        max(float(last_row["high"]), live_price),
                        min(float(last_row["low"]), live_price),
                        live_price,
                        float(last_row["volume"]),
                        live_funding,
                        int(last_row["is_synthetic"]),
                    )
                ]
            if self._in_universe_cycle and symbol in self._cycle_paper_snapshot:
                new_close = self._cycle_paper_snapshot[symbol]["price"]
                funding = self._cycle_paper_snapshot[symbol]["funding_rate"]
                ret = math.log(max(new_close, 1e-12) / max(last_close, 1e-12))
                seed_step = int(hashlib.md5(f"{symbol}:{now_aligned_ms}".encode()).hexdigest()[:8], 16)
                rng = np.random.default_rng(seed_step)
            else:
                seed_step = int(hashlib.md5(f"{symbol}:{now_aligned_ms}".encode()).hexdigest()[:8], 16)
                rng = np.random.default_rng(seed_step)
                ret = float(rng.normal(0.0002, 0.003))
                new_close = max(1e-8, last_close * math.exp(ret))
                funding = float(last_row["funding_rate_hourly"])
                if self._in_universe_cycle:
                    self._cycle_paper_snapshot[symbol] = {
                        "price": new_close,
                        "funding_rate": funding,
                    }
            new_high = max(last_close, new_close) * (1.0 + abs(ret) * 0.6)
            new_low = min(last_close, new_close) * (1.0 - abs(ret) * 0.6)
            vol = float(last_row["volume"]) * float(rng.uniform(0.9, 1.1))
            return [
                (
                    symbol,
                    timeframe,
                    now_aligned_ms,
                    last_close,
                    new_high,
                    new_low,
                    new_close,
                    vol,
                    funding,
                    0,
                )
            ]

        return self._generate_synthetic_usdc_series(symbol, timeframe, limit=limit)

    @staticmethod
    def synthesize_alt_btc_candles(
        alt_df: pd.DataFrame,
        btc_df: pd.DataFrame,
        synth_symbol: str,
        timeframe: str,
    ) -> list[tuple[str, str, int, float, float, float, float, float, float, int]]:
        """
        Synthesize timestamp-aligned ALT/BTC:SYNTH candles from the price ratio:
            P_{Alt/BTC}(t) = P_{Alt/USDC}(t) / P_{BTC/USDC}(t)
        """
        if alt_df.empty or btc_df.empty:
            return []

        merged = pd.merge(
            alt_df,
            btc_df,
            on="timestamp_ms",
            suffixes=("_alt", "_btc"),
            how="inner",
        ).sort_values("timestamp_ms", ascending=True)

        if merged.empty:
            return []

        btc_open = np.maximum(merged["open_btc"].to_numpy(dtype=np.float64), 1e-12)
        btc_close = np.maximum(merged["close_btc"].to_numpy(dtype=np.float64), 1e-12)

        alt_open = merged["open_alt"].to_numpy(dtype=np.float64)
        alt_high = merged["high_alt"].to_numpy(dtype=np.float64)
        alt_low = merged["low_alt"].to_numpy(dtype=np.float64)
        alt_close = merged["close_alt"].to_numpy(dtype=np.float64)

        synth_open = alt_open / btc_open
        synth_close = alt_close / btc_close
        synth_high = np.maximum.reduce(
            [synth_open, synth_close, alt_high / btc_open, alt_high / btc_close]
        )
        synth_low = np.minimum.reduce(
            [synth_open, synth_close, alt_low / btc_open, alt_low / btc_close]
        )
        synth_vol = merged["volume_alt"].to_numpy(dtype=np.float64)
        funding_diff = (
            merged["funding_rate_hourly_alt"].to_numpy(dtype=np.float64)
            - merged["funding_rate_hourly_btc"].to_numpy(dtype=np.float64)
        )
        timestamps = merged["timestamp_ms"].to_numpy(dtype=np.int64)

        rows: list[tuple[str, str, int, float, float, float, float, float, float, int]] = []
        for i in range(len(merged)):
            rows.append(
                (
                    synth_symbol,
                    timeframe,
                    int(timestamps[i]),
                    float(synth_open[i]),
                    float(synth_high[i]),
                    float(synth_low[i]),
                    float(synth_close[i]),
                    float(synth_vol[i]),
                    float(funding_diff[i]),
                    1,
                )
            )
        return rows

    async def ingest_universe(self) -> dict[str, int]:
        """
        Poll all 16 USDC_PERP contracts across required timeframes and synthesize
        all 15 ALT/BTC:SYNTH cross-pair series.
        """
        self._cycle_network_offline = False
        self._in_universe_cycle = True
        self._cycle_market_snapshot = None
        self._cycle_paper_snapshot.clear()
        self._paper_tick_counter += 1
        self._cycle_funding_cache.clear()
        self._cycle_ohlcv_calls = 0
        self._cycle_snapshot_calls = 0
        self._cycle_single_funding_calls = 0
        try:
            config = self.db.load_config()
            limit = int(config.get("ohlcv_fetch_limit", 600))
            benchmark_symbol = str(config.get("benchmark_symbol", "BTC/USDC:USDC"))
            hmm_tf = str(config.get("hmm_timeframe", "4h"))

            usdc_universe = self.db.get_usdc_perp_universe()
            alt_pairs = self.db.get_alt_pairs_universe()

            # Collect distinct timeframes needed across execution, RS, and HMM
            timeframes_by_symbol: dict[str, set[str]] = {
                benchmark_symbol: {"1h", "4h", hmm_tf}
            }
            for row in usdc_universe:
                sym = row["symbol"]
                tfs = timeframes_by_symbol.setdefault(sym, {"1h", "4h"})
                tfs.add(str(row["exec_timeframe"]))
                tfs.add(str(row["rs_timeframe"]))
                tfs.add(hmm_tf)

            usdc_bars_written = 0
            for sym, tfs in timeframes_by_symbol.items():
                for tf in sorted(tfs):
                    bars = await self.fetch_symbol_ohlcv(sym, tf, limit=limit, config=config)
                    usdc_bars_written += self.db.upsert_ohlcv_batch(bars)
                await asyncio.sleep(0.005)

            # Synthesize all 15 ALT/BTC:SYNTH cross-pairs from stored ALT/USDC and BTC/USDC bars
            synth_bars_written = 0
            btc_cache: dict[str, pd.DataFrame] = {}

            for pair in alt_pairs:
                exec_sym = pair["exec_symbol"]
                synth_sym = pair["synth_symbol"]
                pair_tfs = {"1h", "4h", str(pair["exec_timeframe"]), str(pair["rs_timeframe"]), hmm_tf}

                for tf in sorted(pair_tfs):
                    if tf not in btc_cache:
                        btc_cache[tf] = self.db.get_ohlcv_df(benchmark_symbol, tf, limit=limit)
                    alt_df = self.db.get_ohlcv_df(exec_sym, tf, limit=limit)
                    synth_rows = self.synthesize_alt_btc_candles(
                        alt_df=alt_df,
                        btc_df=btc_cache[tf],
                        synth_symbol=synth_sym,
                        timeframe=tf,
                    )
                    synth_bars_written += self.db.upsert_ohlcv_batch(synth_rows)

            total_info_calls = (
                self._cycle_ohlcv_calls
                + self._cycle_snapshot_calls
                + self._cycle_single_funding_calls
            )
            return {
                "usdc_perp_symbols": len(usdc_universe),
                "btc_synth_symbols": len(alt_pairs),
                "usdc_bars_upserted": usdc_bars_written,
                "synth_bars_upserted": synth_bars_written,
                "ohlcv_fetch_calls": self._cycle_ohlcv_calls,
                "batch_snapshot_calls": self._cycle_snapshot_calls,
                "estimated_info_weight": total_info_calls * HYPERLIQUID_INFO_CALL_WEIGHT,
            }
        finally:
            self._in_universe_cycle = False
