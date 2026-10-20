"""SQLite WAL connection manager and centralized variable loader for Aegis Quant."""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "aegis_quant.db"
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema_and_seed.sql"

_EVM_ADDRESS_RE = re.compile(r"^0[xX][0-9a-fA-F]{40}$")
_HEX_PRIVKEY_RE = re.compile(r"^(?:0[xX])?[0-9a-fA-F]{64}$")
_SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_VALID_EXECUTION_MODES = {"PAPER", "LIVE", "TESTNET"}


def validate_evm_address(value: str, field_name: str = "wallet_address") -> str:
    """Validate an EVM/Arbitrum 0x address (or allow empty string to clear)."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(
            f"Invalid {field_name}: must be a 42-character 0x-prefixed hex EVM address."
        )
    cleaned = value.strip()
    if not cleaned:
        return ""
    if not _EVM_ADDRESS_RE.match(cleaned):
        raise ValueError(
            f"Invalid {field_name}: must be a 42-character 0x-prefixed hex EVM address."
        )
    return f"0x{cleaned[2:]}"


def validate_private_key(value: str) -> str:
    """Validate and normalize a 32-byte hex Trade-Only Agent Wallet private key."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(
            "Invalid private_key: must be a 64-hex-character (32-byte) private key."
        )
    cleaned = value.strip()
    if not cleaned:
        return ""
    if not _HEX_PRIVKEY_RE.match(cleaned):
        raise ValueError(
            "Invalid private_key: must be a 64-hex-character (32-byte) private key."
        )
    raw_hex = cleaned[2:] if cleaned.lower().startswith("0x") else cleaned
    scalar = int(raw_hex, 16)
    if scalar <= 0 or scalar >= _SECP256K1_N:
        raise ValueError(
            "Invalid private_key: scalar must be within secp256k1 range [1, n-1]."
        )
    return f"0x{raw_hex.lower()}"


def _mask_secret(raw_val: str) -> str:
    """Mask sensitive private keys and API keys for safe telemetry output."""
    val = str(raw_val or "").strip()
    if not val:
        return ""
    if len(val) > 10:
        return f"{val[:6]}...{val[-4:]}"
    return "********"


def _cast_config_value(raw_val: str, value_type: str) -> Any:
    """Convert string representation in system_config into native Python type."""
    vtype = (value_type or "string").lower().strip()
    if vtype == "int":
        return int(float(raw_val))
    if vtype == "float":
        return float(raw_val)
    if vtype == "bool":
        return raw_val.strip().lower() in ("true", "1", "yes", "on")
    return raw_val


class DatabaseManager:
    """Thread-safe SQLite Write-Ahead Logging (WAL) database manager."""

    def __init__(
        self,
        db_path: Path | str = DEFAULT_DB_PATH,
        schema_path: Path | str = DEFAULT_SCHEMA_PATH,
    ) -> None:
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Yield a SQLite connection pre-configured for WAL concurrency with cross-VM retry."""
        conn: sqlite3.Connection | None = None
        last_err: Exception | None = None
        for attempt in range(16):
            try:
                conn = sqlite3.connect(str(self.db_path), timeout=30.0)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA busy_timeout = 15000;")
                cur_mode = conn.execute("PRAGMA journal_mode;").fetchone()
                if not cur_mode or str(cur_mode[0]).lower() != "wal":
                    conn.execute("PRAGMA journal_mode = WAL;")
                conn.execute("PRAGMA synchronous = NORMAL;")
                conn.execute("PRAGMA foreign_keys = ON;")
                break
            except sqlite3.OperationalError as exc:
                last_err = exc
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                if attempt < 15:
                    time.sleep(0.25)
                else:
                    raise last_err

        assert conn is not None
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_db(self) -> dict[str, Any]:
        """Execute DDL and seed script into SQLite WAL, returning verification counts."""
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema SQL file not found: {self.schema_path}")

        sql_script = self.schema_path.read_text(encoding="utf-8")
        with self.connection() as conn:
            cur_mode = conn.execute("PRAGMA journal_mode;").fetchone()
            if cur_mode and str(cur_mode[0]).lower() == "wal":
                sql_script = sql_script.replace("PRAGMA journal_mode = WAL;", "")
            conn.executescript(sql_script)

        return self.verify_integrity()

    def verify_integrity(self) -> dict[str, Any]:
        """Verify WAL journal mode, 60 config keys, and 31 universe asset views."""
        with self.connection() as conn:
            journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            config_count = conn.execute(
                "SELECT COUNT(*) FROM system_config;"
            ).fetchone()[0]
            usdc_perp_count = conn.execute(
                "SELECT COUNT(*) FROM v_universe_usdc_perp;"
            ).fetchone()[0]
            btc_synth_count = conn.execute(
                "SELECT COUNT(*) FROM v_universe_btc_synth;"
            ).fetchone()[0]
            alt_pairs_count = conn.execute(
                "SELECT COUNT(*) FROM v_universe_alt_pairs;"
            ).fetchone()[0]
            total_universe = conn.execute(
                "SELECT COUNT(*) FROM universe_assets;"
            ).fetchone()[0]

        return {
            "journal_mode": str(journal_mode).lower(),
            "system_config_count": int(config_count),
            "v_universe_usdc_perp_count": int(usdc_perp_count),
            "v_universe_btc_synth_count": int(btc_synth_count),
            "v_universe_alt_pairs_count": int(alt_pairs_count),
            "universe_assets_total": int(total_universe),
        }

    def load_config(self) -> dict[str, Any]:
        """Load all 60 system_config variables cast to their declared types."""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT config_key, config_value, value_type FROM system_config;"
            ).fetchall()
        return {
            row["config_key"]: _cast_config_value(row["config_value"], row["value_type"])
            for row in rows
        }

    def get_config_rows(self, mask_secrets: bool = True) -> list[dict[str, Any]]:
        """Return full metadata rows for system_config, optionally masking keys."""
        secret_keys = {"private_key", "gemini_api_key"}
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT config_key, config_value, value_type, category, description, updated_at
                FROM system_config
                ORDER BY category ASC, config_key ASC;
                """
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            if mask_secrets and item["config_key"] in secret_keys and item["config_value"]:
                item["config_value"] = _mask_secret(str(item["config_value"]))
            result.append(item)
        return result

    def set_config_value(self, key: str, value: Any) -> bool:
        """Update an existing configuration parameter in system_config with type validation."""
        cred_field_map = {
            "wallet_address": "wallet_address",
            "private_key": "private_key",
            "hyperliquid_vault_address": "hyperliquid_vault_address",
            "gemini_api_key": "gemini_api_key",
            "hyperliquid_testnet": "hyperliquid_testnet",
            "execution_mode": "execution_mode",
        }
        if key in cred_field_map:
            self.configure_hyperliquid_credentials(**{cred_field_map[key]: value})
            return True

        with self.connection() as conn:
            row = conn.execute(
                "SELECT value_type FROM system_config WHERE config_key = ?;",
                (key,),
            ).fetchone()
            if row is None:
                return False

            vtype = str(row["value_type"] or "string").strip().lower()
            if vtype == "bool":
                if isinstance(value, bool):
                    str_val = "true" if value else "false"
                elif isinstance(value, str):
                    low = value.strip().lower()
                    if low in ("true", "1", "yes", "on"):
                        str_val = "true"
                    elif low in ("false", "0", "no", "off"):
                        str_val = "false"
                    else:
                        raise ValueError(f"Invalid boolean value for '{key}': {value!r}")
                else:
                    raise ValueError(f"Invalid boolean value for '{key}': {value!r}")
            elif vtype == "int":
                if isinstance(value, bool) or value is None:
                    raise ValueError(f"Invalid integer value for '{key}': {value!r}")
                try:
                    f_val = float(str(value).strip())
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"Invalid integer value for '{key}': {value!r}") from exc
                if not math.isfinite(f_val) or not f_val.is_integer():
                    raise ValueError(f"Invalid integer value for '{key}': {value!r}")
                int_val = int(f_val)
                if key == "polling_interval_seconds" and int_val < 1:
                    raise ValueError(
                        f"Invalid polling_interval_seconds '{value!r}': must be >= 1 second."
                    )
                str_val = str(int_val)
            elif vtype == "float":
                if isinstance(value, bool) or value is None:
                    raise ValueError(f"Invalid float value for '{key}': {value!r}")
                try:
                    f_val = float(str(value).strip())
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"Invalid float value for '{key}': {value!r}") from exc
                if not math.isfinite(f_val):
                    raise ValueError(f"Invalid float value for '{key}': {value!r}")
                str_val = str(f_val)
            else:
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"Invalid string value for '{key}': {value!r}")
                str_val = value.strip()

            cur = conn.execute(
                """
                UPDATE system_config
                SET config_value = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE config_key = ?;
                """,
                (str_val, key),
            )
            return cur.rowcount > 0

    def get_credentials_status(
        self, config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Return masked Hyperliquid L1 Mainnet/Testnet credential and routing status."""
        cfg = config if config is not None else self.load_config()
        wallet = str(cfg.get("wallet_address", "")).strip()
        priv_key = str(cfg.get("private_key", "")).strip()
        vault = str(cfg.get("hyperliquid_vault_address", "")).strip()
        gemini_key = str(cfg.get("gemini_api_key", "")).strip()
        raw_mode = str(cfg.get("execution_mode", "PAPER")).strip().upper()
        raw_testnet = cfg.get("hyperliquid_testnet", False)
        if isinstance(raw_testnet, str):
            testnet_bool = raw_testnet.strip().lower() in ("true", "1", "yes", "on", "testnet")
        else:
            testnet_bool = bool(raw_testnet)
        is_testnet = testnet_bool or (raw_mode == "TESTNET")

        wallet_valid = bool(wallet and _EVM_ADDRESS_RE.match(wallet))
        try:
            priv_valid = bool(priv_key and validate_private_key(priv_key))
        except ValueError:
            priv_valid = False
        credentials_ready = wallet_valid and priv_valid

        network = "testnet" if is_testnet else "mainnet"
        api_endpoint = (
            "https://api.hyperliquid-testnet.xyz"
            if is_testnet
            else "https://api.hyperliquid.xyz"
        )
        if raw_mode in ("LIVE", "TESTNET") and credentials_ready:
            active_routing = "LIVE_TESTNET" if is_testnet else "LIVE_MAINNET"
        else:
            active_routing = "PAPER_SIMULATION"

        return {
            "exchange_id": str(cfg.get("exchange_id", "hyperliquid")),
            "environment": str(cfg.get("environment", "sandbox")),
            "execution_mode": raw_mode,
            "hyperliquid_testnet": is_testnet,
            "network": network,
            "api_endpoint": api_endpoint,
            "wallet_address": wallet,
            "wallet_configured": wallet_valid,
            "private_key_masked": _mask_secret(priv_key),
            "private_key_configured": priv_valid,
            "hyperliquid_vault_address": vault,
            "gemini_api_key_masked": _mask_secret(gemini_key),
            "gemini_api_key_configured": bool(gemini_key),
            "credentials_ready": credentials_ready,
            "active_routing": active_routing,
        }

    def configure_hyperliquid_credentials(
        self,
        *,
        wallet_address: str | None = None,
        private_key: str | None = None,
        hyperliquid_testnet: bool | str | None = None,
        execution_mode: str | None = None,
        hyperliquid_vault_address: str | None = None,
        gemini_api_key: str | None = None,
        require_credentials_for_live: bool = True,
    ) -> dict[str, Any]:
        """
        Validate and atomically update Hyperliquid Mainnet/Testnet credentials and
        execution routing parameters in SQLite system_config.
        """
        updates: dict[str, str] = {}

        if wallet_address is not None:
            updates["wallet_address"] = validate_evm_address(wallet_address, "wallet_address")

        if private_key is not None:
            updates["private_key"] = validate_private_key(private_key)

        if hyperliquid_vault_address is not None:
            updates["hyperliquid_vault_address"] = validate_evm_address(
                hyperliquid_vault_address, "hyperliquid_vault_address"
            )

        if gemini_api_key is not None:
            if not isinstance(gemini_api_key, str):
                raise ValueError("Invalid gemini_api_key: must be a string.")
            updates["gemini_api_key"] = gemini_api_key.strip()

        if hyperliquid_testnet is not None:
            if isinstance(hyperliquid_testnet, bool):
                updates["hyperliquid_testnet"] = "true" if hyperliquid_testnet else "false"
            elif isinstance(hyperliquid_testnet, str):
                t_low = hyperliquid_testnet.strip().lower()
                if t_low in ("true", "1", "yes", "on", "testnet"):
                    updates["hyperliquid_testnet"] = "true"
                elif t_low in ("false", "0", "no", "off", "mainnet"):
                    updates["hyperliquid_testnet"] = "false"
                else:
                    raise ValueError(
                        f"Invalid hyperliquid_testnet value '{hyperliquid_testnet}': use true or false."
                    )
            else:
                raise ValueError(
                    f"Invalid hyperliquid_testnet value '{hyperliquid_testnet}': use true or false."
                )

        if execution_mode is not None:
            if not isinstance(execution_mode, str):
                raise ValueError(
                    f"Invalid execution_mode '{execution_mode}': must be PAPER, LIVE, or TESTNET."
                )
            mode_up = execution_mode.strip().upper()
            if mode_up not in _VALID_EXECUTION_MODES:
                raise ValueError(
                    f"Invalid execution_mode '{execution_mode}': must be PAPER, LIVE, or TESTNET."
                )
            if mode_up == "TESTNET":
                if updates.get("hyperliquid_testnet") == "false":
                    raise ValueError(
                        "Conflicting configuration: execution_mode='TESTNET' cannot be combined with hyperliquid_testnet=False."
                    )
                updates["execution_mode"] = "LIVE"
                updates["hyperliquid_testnet"] = "true"
            else:
                updates["execution_mode"] = mode_up

        with self.connection() as conn:
            rows = conn.execute(
                "SELECT config_key, config_value, value_type FROM system_config;"
            ).fetchall()
            current_cfg = {
                row["config_key"]: _cast_config_value(row["config_value"], row["value_type"])
                for row in rows
            }

            effective_wallet = updates.get(
                "wallet_address", str(current_cfg.get("wallet_address", "")).strip()
            )
            effective_priv = updates.get(
                "private_key", str(current_cfg.get("private_key", "")).strip()
            )
            effective_mode = updates.get(
                "execution_mode", str(current_cfg.get("execution_mode", "PAPER")).strip().upper()
            )

            if require_credentials_for_live and effective_mode in ("LIVE", "TESTNET"):
                try:
                    valid_w = bool(effective_wallet and validate_evm_address(effective_wallet))
                    valid_p = bool(effective_priv and validate_private_key(effective_priv))
                except ValueError:
                    valid_w = False
                    valid_p = False
                if not (valid_w and valid_p):
                    raise ValueError(
                        "Cannot enable LIVE execution_mode without both a valid wallet_address and private_key configured."
                    )

            for k, v in updates.items():
                conn.execute(
                    """
                    UPDATE system_config
                    SET config_value = ?,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                    WHERE config_key = ?;
                    """,
                    (v, k),
                )
                current_cfg[k] = v

        return self.get_credentials_status(current_cfg)

    def get_usdc_perp_universe(self) -> list[dict[str, Any]]:
        """Return all 16 active Hyperliquid USDC_PERP rows from v_universe_usdc_perp."""
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM v_universe_usdc_perp;").fetchall()
        return [dict(r) for r in rows]

    def get_btc_synth_universe(self) -> list[dict[str, Any]]:
        """Return all 15 active synthetic BTC cross-pairs from v_universe_btc_synth."""
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM v_universe_btc_synth;").fetchall()
        return [dict(r) for r in rows]

    def get_alt_pairs_universe(self) -> list[dict[str, Any]]:
        """Return all 15 paired execution & relative-strength rows from v_universe_alt_pairs."""
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM v_universe_alt_pairs;").fetchall()
        return [dict(r) for r in rows]

    def update_asset_timeframes(
        self, base_asset: str, exec_timeframe: str, rs_timeframe: str
    ) -> None:
        """Update calibrated execution and relative-strength timeframes for an asset."""
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE universe_assets
                SET exec_timeframe = ?,
                    rs_timeframe = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE base_asset = ?;
                """,
                (exec_timeframe, rs_timeframe, base_asset),
            )

    def upsert_ohlcv_batch(self, rows: list[tuple[Any, ...]]) -> int:
        """Upsert OHLCV bars into market_ohlcv."""
        if not rows:
            return 0
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO market_ohlcv (
                    symbol, timeframe, timestamp_ms, open, high, low, close,
                    volume, funding_rate_hourly, is_synthetic
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe, timestamp_ms) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    funding_rate_hourly = excluded.funding_rate_hourly,
                    is_synthetic = excluded.is_synthetic;
                """,
                rows,
            )
        return len(rows)

    def get_ohlcv_df(
        self, symbol: str, timeframe: str, limit: int = 600
    ) -> pd.DataFrame:
        """Fetch historical OHLCV bars for a symbol/timeframe sorted chronologically."""
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT timestamp_ms, open, high, low, close, volume,
                       funding_rate_hourly, is_synthetic
                FROM market_ohlcv
                WHERE symbol = ? AND timeframe = ?
                ORDER BY timestamp_ms DESC
                LIMIT ?;
                """,
                (symbol, timeframe, limit),
            ).fetchall()

        if not rows:
            return pd.DataFrame(
                columns=[
                    "timestamp_ms",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "funding_rate_hourly",
                    "is_synthetic",
                ]
            )
        df = pd.DataFrame([dict(r) for r in rows])
        return df.sort_values("timestamp_ms", ascending=True).reset_index(drop=True)

    def save_signal_evaluations(self, evaluations: list[dict[str, Any]]) -> None:
        """Persist a batch of HMM + MACE + SNR signal evaluations."""
        if not evaluations:
            return
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO signal_evaluations (
                    timestamp_ms, base_asset, exec_symbol, synth_symbol,
                    hmm_regime, prob_bull, prob_bear, prob_chop,
                    snr_score, mace_score, blended_conviction,
                    rs_rank, signal_action, funding_rate_hourly
                ) VALUES (
                    :timestamp_ms, :base_asset, :exec_symbol, :synth_symbol,
                    :hmm_regime, :prob_bull, :prob_bear, :prob_chop,
                    :snr_score, :mace_score, :blended_conviction,
                    :rs_rank, :signal_action, :funding_rate_hourly
                );
                """,
                evaluations,
            )

    def get_latest_signals(self, limit: int = 15) -> list[dict[str, Any]]:
        """Return the most recent batch of signal evaluations ordered by rank."""
        with self.connection() as conn:
            latest_ts_row = conn.execute(
                "SELECT MAX(timestamp_ms) AS max_ts FROM signal_evaluations;"
            ).fetchone()
            if not latest_ts_row or latest_ts_row["max_ts"] is None:
                return []
            rows = conn.execute(
                """
                SELECT *
                FROM signal_evaluations
                WHERE timestamp_ms = ?
                ORDER BY rs_rank ASC
                LIMIT ?;
                """,
                (latest_ts_row["max_ts"], limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_open_trades(self) -> list[dict[str, Any]]:
        """Return all currently open positions."""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM trades WHERE status = 'OPEN' ORDER BY opened_at ASC;"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent open and closed trades."""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY trade_id DESC LIMIT ?;",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def insert_trade(self, trade: dict[str, Any]) -> int:
        """Insert a new trade record and return its trade_id."""
        with self.connection() as conn:
            cur = conn.execute(
                """
                INSERT INTO trades (
                    symbol, base_asset, side, status,
                    entry_price, current_price, quantity, notional_usdc,
                    risk_budget_usdc, conviction_multiplier,
                    initial_stop_price, chandelier_stop_price,
                    highest_price_seen, lowest_price_seen,
                    fees_paid_usdc, exchange_order_id
                ) VALUES (
                    :symbol, :base_asset, :side, 'OPEN',
                    :entry_price, :current_price, :quantity, :notional_usdc,
                    :risk_budget_usdc, :conviction_multiplier,
                    :initial_stop_price, :chandelier_stop_price,
                    :highest_price_seen, :lowest_price_seen,
                    :fees_paid_usdc, :exchange_order_id
                );
                """,
                trade,
            )
            return int(cur.lastrowid or 0)

    def update_open_trade(
        self,
        trade_id: int,
        current_price: float,
        chandelier_stop_price: float,
        highest_price_seen: float,
        lowest_price_seen: float,
        unrealized_pnl_usdc: float,
        funding_accrued_usdc: float,
    ) -> None:
        """Update mark price, trailing Chandelier stop, and unrealized PnL of an open trade."""
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE trades
                SET current_price = ?,
                    chandelier_stop_price = ?,
                    highest_price_seen = ?,
                    lowest_price_seen = ?,
                    unrealized_pnl_usdc = ?,
                    funding_accrued_usdc = ?
                WHERE trade_id = ? AND status = 'OPEN';
                """,
                (
                    current_price,
                    chandelier_stop_price,
                    highest_price_seen,
                    lowest_price_seen,
                    unrealized_pnl_usdc,
                    funding_accrued_usdc,
                    trade_id,
                ),
            )

    def close_trade(
        self,
        trade_id: int,
        exit_price: float,
        realized_pnl_usdc: float,
        fees_paid_usdc: float,
        exit_reason: str,
    ) -> None:
        """Close an open position and record realized PnL."""
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE trades
                SET status = 'CLOSED',
                    current_price = ?,
                    exit_price = ?,
                    realized_pnl_usdc = ?,
                    unrealized_pnl_usdc = 0.0,
                    fees_paid_usdc = ?,
                    closed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                    exit_reason = ?
                WHERE trade_id = ?;
                """,
                (exit_price, exit_price, realized_pnl_usdc, fees_paid_usdc, exit_reason, trade_id),
            )

    def save_portfolio_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Insert a point-in-time portfolio equity and risk telemetry snapshot."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO portfolio_snapshots (
                    timestamp_ms, total_equity_usdc, available_cash_usdc,
                    deployed_notional_usdc, gross_leverage,
                    portfolio_heat_usdc, portfolio_heat_pct,
                    basis_harvest_notional_usdc, accrued_yield_usdc,
                    hmm_regime, prob_bull, prob_bear, prob_chop,
                    open_long_count, open_short_count
                ) VALUES (
                    :timestamp_ms, :total_equity_usdc, :available_cash_usdc,
                    :deployed_notional_usdc, :gross_leverage,
                    :portfolio_heat_usdc, :portfolio_heat_pct,
                    :basis_harvest_notional_usdc, :accrued_yield_usdc,
                    :hmm_regime, :prob_bull, :prob_bear, :prob_chop,
                    :open_long_count, :open_short_count
                );
                """,
                snapshot,
            )

    def get_latest_portfolio_snapshot(self) -> dict[str, Any]:
        """Return the latest portfolio snapshot, or default $10,000 baseline."""
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1;"
            ).fetchone()
        if row:
            return dict(row)
        return {
            "timestamp_ms": int(time.time() * 1000),
            "total_equity_usdc": 10000.0,
            "available_cash_usdc": 10000.0,
            "deployed_notional_usdc": 0.0,
            "gross_leverage": 0.0,
            "portfolio_heat_usdc": 0.0,
            "portfolio_heat_pct": 0.0,
            "basis_harvest_notional_usdc": 0.0,
            "accrued_yield_usdc": 0.0,
            "hmm_regime": "CHOP",
            "prob_bull": 0.3333,
            "prob_bear": 0.3333,
            "prob_chop": 0.3334,
            "open_long_count": 0,
            "open_short_count": 0,
        }

    def log_telemetry(
        self,
        level: str,
        component: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Write an operational event into telemetry_logs."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO telemetry_logs (timestamp_ms, level, component, message, details_json)
                VALUES (?, ?, ?, ?, ?);
                """,
                (
                    int(time.time() * 1000),
                    level.upper(),
                    component,
                    message,
                    json.dumps(details) if details else None,
                ),
            )

    def get_recent_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        """Fetch recent telemetry log entries."""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM telemetry_logs ORDER BY log_id DESC LIMIT ?;",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
