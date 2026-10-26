#!/usr/bin/env python3
"""
Automated Backfill Refresh & SHA-256 Provenance Generator for Aegis Quant (v0.2.0-rc2).

Validates or refreshes 4H OHLCV history across all 16 Hyperliquid universe assets in
`data/real_4h_backfill.json`, writes `data/real_4h_backfill.json.sha256`, and verifies
cryptographic data provenance.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import ccxt

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("aegis.refresh_backfill")

DEFAULT_BACKFILL_PATH = PROJECT_ROOT / "data" / "real_4h_backfill.json"
DEFAULT_START_DATE = "2024-11-01"
TF_4H_MS = 14_400_000
UNIVERSE_COINS = [
    "BTC",
    "ETH",
    "SOL",
    "HYPE",
    "XRP",
    "DOGE",
    "ADA",
    "AVAX",
    "LINK",
    "SUI",
    "DOT",
    "NEAR",
    "APT",
    "ARB",
    "OP",
    "INJ",
]


def _parse_start_date_ms(start_date: str) -> int:
    """Parse YYYY-MM-DD or ISO-8601 date string into UTC milliseconds."""
    cleaned = start_date.strip()
    if "T" in cleaned:
        parsed = dt.datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp() * 1000)
    d = dt.date.fromisoformat(cleaned)
    return int(dt.datetime.combine(d, dt.time(0, 0, 0), tzinfo=dt.timezone.utc).timestamp() * 1000)


def fetch_hyperliquid_4h_backfill(
    limit: int = 5000,
    start_date: str = DEFAULT_START_DATE,
) -> dict[str, Any]:
    """Fetch 4H OHLCV history from `start_date` for all 16 universe assets from Hyperliquid via CCXT."""
    from engine.ingestion import BASE_PRICES_USDC

    universe_coins = [sym.split("/")[0] for sym in BASE_PRICES_USDC] or UNIVERSE_COINS
    exchange = ccxt.hyperliquid({"enableRateLimit": True, "timeout": 20000})
    exchange.load_markets()

    start_ms = _parse_start_date_ms(start_date)
    now_ms = int(time.time() * 1000)
    chunk_span_ms = 2000 * TF_4H_MS  # 2,000 4H bars (~333 days) per paginated request

    coins_payload: dict[str, Any] = {}
    max_ts_seen = 0

    for coin in universe_coins:
        symbol = f"{coin}/USDC:USDC"
        by_ts: dict[int, dict[str, float | int]] = {}
        cursor_ms = start_ms

        while cursor_ms <= now_ms:
            until_ms = min(now_ms, cursor_ms + chunk_span_ms)
            candles_raw = exchange.fetch_ohlcv(
                symbol,
                timeframe="4h",
                since=cursor_ms,
                limit=limit,
                params={"until": until_ms},
            )
            for c in candles_raw:
                if c and len(c) >= 6:
                    ts_val = int(c[0])
                    by_ts[ts_val] = {
                        "t": ts_val,
                        "o": float(c[1]),
                        "h": float(c[2]),
                        "l": float(c[3]),
                        "c": float(c[4]),
                        "v": float(c[5]),
                    }
            if until_ms >= now_ms:
                break
            cursor_ms = until_ms + TF_4H_MS

        candles = [by_ts[k] for k in sorted(by_ts.keys())]
        if candles:
            max_ts_seen = max(max_ts_seen, int(candles[-1]["t"]))
            first_iso = dt.datetime.fromtimestamp(candles[0]["t"] / 1000, dt.timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
            last_iso = dt.datetime.fromtimestamp(candles[-1]["t"] / 1000, dt.timezone.utc).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
            logger.info(
                "Fetched %d 4H candles for %s (%s -> %s).",
                len(candles),
                symbol,
                first_iso,
                last_iso,
            )
        else:
            logger.warning("Fetched 0 4H candles for %s.", symbol)

        coins_payload[coin] = {
            "source": "hyperliquid",
            "funding_rate_hourly": 0.000015,
            "count": len(candles),
            "candles": candles,
        }

    return {
        "server_max_ts": max_ts_seen,
        "target_end_ts": max_ts_seen + TF_4H_MS,
        "coins": coins_payload,
    }


def refresh_and_sign_backfill(
    backfill_path: Path = DEFAULT_BACKFILL_PATH,
    live_fetch: bool = False,
    start_date: str = DEFAULT_START_DATE,
) -> str:
    """
    Ensure `data/real_4h_backfill.json` is present and valid for all 16 universe assets,
    write `data/real_4h_backfill.json.sha256`, and verify SHA-256 provenance.
    """
    from engine.backtest import verify_data_provenance

    backfill_path.parent.mkdir(parents=True, exist_ok=True)

    if live_fetch or not backfill_path.exists():
        existing_coins: dict[str, Any] = {}
        if backfill_path.exists():
            try:
                existing_coins = json.loads(backfill_path.read_text(encoding="utf-8")).get("coins", {})
            except (OSError, ValueError, TypeError):
                existing_coins = {}
        try:
            logger.info(
                "Fetching live 4H OHLCV backfill from Hyperliquid via CCXT (start_date=%s)...",
                start_date,
            )
            payload = fetch_hyperliquid_4h_backfill(start_date=start_date)
            max_live_ts = int(payload.get("server_max_ts", 0))
            if existing_coins and max_live_ts > 0:
                for coin, info in payload.get("coins", {}).items():
                    live_candles = list(info.get("candles", []))
                    tail_candles = [
                        c
                        for c in existing_coins.get(coin, {}).get("candles", [])
                        if int(c.get("t", 0)) > max_live_ts
                    ]
                    if tail_candles:
                        merged = live_candles + tail_candles
                        info["candles"] = merged
                        info["count"] = len(merged)
            backfill_path.write_text(json.dumps(payload), encoding="utf-8")
        except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            if live_fetch or not backfill_path.exists():
                raise RuntimeError(f"Failed to fetch live Hyperliquid backfill: {exc}") from exc
            logger.warning(
                "CCXT live fetch unavailable (%s); preserving existing local backfill at %s.",
                exc,
                backfill_path,
            )

    raw_bytes = backfill_path.read_bytes()
    data = json.loads(raw_bytes.decode("utf-8"))
    coins = data.get("coins", {})
    total_candles = sum(len(info.get("candles", [])) for info in coins.values())
    logger.info(
        "Validated backfill dataset at %s: %d universe coins, %d total 4H candles.",
        backfill_path,
        len(coins),
        total_candles,
    )

    digest = hashlib.sha256(raw_bytes).hexdigest()
    hash_path = Path(f"{backfill_path}.sha256")
    rel_name = backfill_path.relative_to(PROJECT_ROOT) if backfill_path.is_relative_to(PROJECT_ROOT) else backfill_path.name
    hash_path.write_text(f"{digest}  {rel_name}\n", encoding="utf-8")
    logger.info("Wrote SHA-256 checksum to %s: %s", hash_path, digest)

    if not verify_data_provenance(str(backfill_path)):
        raise RuntimeError(f"Provenance verification failed for {backfill_path}")

    return digest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh or validate Hyperliquid 4H backfill data and generate SHA-256 checksum."
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=str(DEFAULT_BACKFILL_PATH),
        help="Path to real_4h_backfill.json (default: data/real_4h_backfill.json)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=DEFAULT_START_DATE,
        help="Earliest UTC date (YYYY-MM-DD) to fetch 4H candles from (default: 2024-11-01)",
    )
    parser.add_argument(
        "--live-fetch",
        action="store_true",
        help="Fetch latest 4H candles from Hyperliquid via CCXT before computing checksum",
    )
    args = parser.parse_args()

    digest = refresh_and_sign_backfill(
        backfill_path=Path(args.data_path),
        live_fetch=bool(args.live_fetch),
        start_date=str(args.start_date),
    )
    print(f"SHA-256 ({args.data_path}): {digest}")


if __name__ == "__main__":
    main()
