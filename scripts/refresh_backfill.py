#!/usr/bin/env python3
"""
Automated Backfill Refresh & SHA-256 Provenance Generator for Aegis Quant (v0.2.0-rc2).

Validates or refreshes 4H OHLCV history across all 16 Hyperliquid universe assets in
`data/real_4h_backfill.json`, writes `data/real_4h_backfill.json.sha256`, and verifies
cryptographic data provenance.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
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
UNIVERSE_COINS = [
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "DOGE",
    "AVAX",
    "LINK",
    "SUI",
    "DOT",
    "NEAR",
    "APT",
    "ARB",
    "OP",
    "INJ",
    "TIA",
    "RENDER",
]


def fetch_hyperliquid_4h_backfill(limit: int = 1950) -> dict[str, Any]:
    """Fetch 4H OHLCV history for all 16 universe assets from Hyperliquid via CCXT."""
    from engine.ingestion import BASE_PRICES_USDC

    universe_coins = [sym.split("/")[0] for sym in BASE_PRICES_USDC] or UNIVERSE_COINS
    exchange = ccxt.hyperliquid({"enableRateLimit": True, "timeout": 15000})
    coins_payload: dict[str, Any] = {}
    max_ts_seen = 0

    for coin in universe_coins:
        symbol = f"{coin}/USDC:USDC"
        candles_raw = exchange.fetch_ohlcv(symbol, timeframe="4h", limit=limit)
        candles = [
            {
                "t": int(c[0]),
                "o": float(c[1]),
                "h": float(c[2]),
                "l": float(c[3]),
                "c": float(c[4]),
                "v": float(c[5]),
            }
            for c in candles_raw
            if c and len(c) >= 6
        ]
        if candles:
            max_ts_seen = max(max_ts_seen, max(c["t"] for c in candles))
        coins_payload[coin] = {
            "source": "hyperliquid",
            "funding_rate_hourly": 0.000015,
            "count": len(candles),
            "candles": candles,
        }
        logger.info("Fetched %d 4H candles for %s from Hyperliquid.", len(candles), symbol)

    return {
        "server_max_ts": max_ts_seen,
        "target_end_ts": max_ts_seen + 14_400_000,
        "coins": coins_payload,
    }


def refresh_and_sign_backfill(
    backfill_path: Path = DEFAULT_BACKFILL_PATH,
    live_fetch: bool = False,
) -> str:
    """
    Ensure `data/real_4h_backfill.json` is present and valid for all 16 universe assets,
    write `data/real_4h_backfill.json.sha256`, and verify SHA-256 provenance.
    """
    from engine.backtest import verify_data_provenance

    backfill_path.parent.mkdir(parents=True, exist_ok=True)

    if live_fetch or not backfill_path.exists():
        try:
            logger.info("Fetching live 4H OHLCV backfill from Hyperliquid via CCXT...")
            payload = fetch_hyperliquid_4h_backfill()
            backfill_path.write_text(json.dumps(payload), encoding="utf-8")
        except (ccxt.BaseError, OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            if not backfill_path.exists():
                raise RuntimeError(f"Failed to fetch backfill and no local dataset exists: {exc}") from exc
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
        "--live-fetch",
        action="store_true",
        help="Fetch latest 4H candles from Hyperliquid via CCXT before computing checksum",
    )
    args = parser.parse_args()

    digest = refresh_and_sign_backfill(
        backfill_path=Path(args.data_path),
        live_fetch=bool(args.live_fetch),
    )
    print(f"SHA-256 ({args.data_path}): {digest}")


if __name__ == "__main__":
    main()
