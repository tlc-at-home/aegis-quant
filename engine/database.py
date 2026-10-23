"""SQLite WAL connection manager and centralized variable loader for Aegis Quant."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import sqlite3
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pandas as pd
from cryptography.fernet import Fernet, InvalidToken

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "aegis_quant.db"
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema_and_seed.sql"
MIGRATION_001_PATH = PROJECT_ROOT / "sql" / "migrations" / "001_v020_config_expansion.sql"
MIGRATION_002_PATH = PROJECT_ROOT / "sql" / "migrations" / "002_v020_order_fsm.sql"
MIGRATION_003_PATH = PROJECT_ROOT / "sql" / "migrations" / "003_v020_schema_versioning.sql"
MIGRATION_004_PATH = PROJECT_ROOT / "sql" / "migrations" / "004_v020_rc2_remediation.sql"

SENSITIVE_CONFIG_KEYS: frozenset[str] = frozenset(
    {
        "private_key",
        "gemini_api_key",
        "telegram_bot_token",
        "discord_webhook_url",
    }
)
_ALERT_CONFIG_DEFAULTS: dict[str, tuple[str, str, str, str]] = {
    "telegram_bot_token": (
        "",
        "string",
        "alerts",
        "Telegram Bot API token for real-time operational notifications (Fernet AES-128 encrypted)",
    ),
    "telegram_chat_id": (
        "",
        "string",
        "alerts",
        "Telegram target chat ID for real-time operational notifications",
    ),
    "discord_webhook_url": (
        "",
        "string",
        "alerts",
        "Discord webhook URL for real-time operational notifications (Fernet AES-128 encrypted)",
    ),
    "max_daily_drawdown_alert_pct": (
        "0.03",
        "float",
        "alerts",
        "Daily portfolio drawdown threshold (3.0%) triggering critical Telegram alert",
    ),
}
_DEFAULT_HOST_FERNET_KEY: bytes = base64.urlsafe_b64encode(
    hashlib.sha256(b"aegis-quant-v0.2.0-rc1-fernet-master-key").digest()
)

_EVM_ADDRESS_RE = re.compile(r"^0[xX][0-9a-fA-F]{40}$")
_HEX_PRIVKEY_RE = re.compile(r"^(?:0[xX])?[0-9a-fA-F]{64}$")
_SECP256K1_N = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
_VALID_EXECUTION_MODES = {"PAPER", "LIVE", "TESTNET"}
_VALID_FSM_STATES = {
    "PENDING",
    "OPEN_ON_BOOK",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELLED",
    "REJECTED",
    "EXPIRED",
}


def _normalize_fernet_key(raw_key: str | bytes) -> bytes:
    """Normalize a 32-byte urlsafe-base64 Fernet key or derive one via SHA-256."""
    raw_bytes = (
        raw_key.strip().encode("utf-8")
        if isinstance(raw_key, str)
        else raw_key.strip()
    )
    if not raw_bytes:
        return _DEFAULT_HOST_FERNET_KEY
    try:
        Fernet(raw_bytes)
        return raw_bytes
    except (ValueError, TypeError):
        return base64.urlsafe_b64encode(hashlib.sha256(raw_bytes).digest())


def _resolve_fernet_master_key(master_key: str | bytes | None = None) -> bytes:
    """
    Resolve the AES-128 Fernet master key from:
    1. Explicit `master_key` argument
    2. systemd `LoadCredentialEncrypted=` / `LoadCredential=` (`$CREDENTIALS_DIRECTORY`)
    3. Environment variable (`AEGIS_MASTER_KEY` or `FERNET_MASTER_KEY`)
    4. Deterministic platform fallback key
    """
    if master_key is not None:
        return _normalize_fernet_key(master_key)

    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY", "").strip()
    if cred_dir:
        cred_dir_path = Path(cred_dir)
        if cred_dir_path.is_dir():
            for candidate_name in ("aegis_master_key", "master_key", "fernet_key"):
                candidate_file = cred_dir_path / candidate_name
                if candidate_file.is_file():
                    content = candidate_file.read_bytes().strip()
                    if content:
                        return _normalize_fernet_key(content)
            for child in sorted(cred_dir_path.iterdir()):
                if child.is_file():
                    content = child.read_bytes().strip()
                    if content:
                        return _normalize_fernet_key(content)

    env_key = (
        os.environ.get("AEGIS_MASTER_KEY")
        or os.environ.get("FERNET_MASTER_KEY")
        or ""
    ).strip()
    if env_key:
        return _normalize_fernet_key(env_key)

    return _DEFAULT_HOST_FERNET_KEY


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
        master_key: str | bytes | None = None,
    ) -> None:
        raw_path = str(db_path).strip()
        self._is_memory = raw_path == ":memory:" or raw_path.startswith("file::memory:")
        self.db_path = Path(raw_path) if not self._is_memory else Path(":memory:")
        self.schema_path = Path(schema_path)
        self._memory_conn: sqlite3.Connection | None = None
        self.pit_timestamp_ms: int | None = None
        self._explicit_master_key: str | bytes | None = master_key
        self._fernet_key: bytes = _resolve_fernet_master_key(master_key)
        self._fernet: Fernet = Fernet(self._fernet_key)
        if not self._is_memory:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def close(self) -> None:
        """Close persistent in-memory SQLite connection if open."""
        if self._memory_conn is not None:
            with suppress(sqlite3.Error, OSError):
                self._memory_conn.close()
            self._memory_conn = None

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Yield a SQLite connection pre-configured for WAL concurrency with cross-VM retry."""
        if self._is_memory:
            if self._memory_conn is None:
                self._memory_conn = sqlite3.connect(":memory:", timeout=30.0, check_same_thread=False)
                self._memory_conn.row_factory = sqlite3.Row
                self._memory_conn.execute("PRAGMA temp_store = MEMORY;")
                self._memory_conn.execute("PRAGMA synchronous = NORMAL;")
                self._memory_conn.execute("PRAGMA foreign_keys = ON;")
            try:
                yield self._memory_conn
                self._memory_conn.commit()
            except Exception:
                self._memory_conn.rollback()
                raise
            return

        conn: sqlite3.Connection | None = None
        for attempt in range(16):
            try:
                conn = sqlite3.connect(str(self.db_path), timeout=30.0)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA temp_store = MEMORY;")
                conn.execute("PRAGMA busy_timeout = 15000;")
                cur_mode = conn.execute("PRAGMA journal_mode;").fetchone()
                if not cur_mode or str(cur_mode[0]).lower() != "wal":
                    conn.execute("PRAGMA journal_mode = WAL;")
                conn.execute("PRAGMA synchronous = NORMAL;")
                conn.execute("PRAGMA foreign_keys = ON;")
                break
            except sqlite3.OperationalError:
                if conn is not None:
                    with suppress(sqlite3.Error, OSError):
                        conn.close()
                    conn = None
                if attempt < 15:
                    time.sleep(0.25)
                else:
                    raise

        if conn is None:
            raise sqlite3.OperationalError("Failed to establish SQLite connection.")
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
            if self._is_memory or (cur_mode and str(cur_mode[0]).lower() == "wal"):
                sql_script = sql_script.replace("PRAGMA journal_mode = WAL;", "")
            conn.executescript(sql_script)

        return self.verify_integrity()

    def initialize_database(self) -> dict[str, Any]:
        """Initialize database schema and apply all v0.2.0 migrations up to Migration 004."""
        self._initialized_via_alias = True
        integrity = self.init_db()
        for mig in (MIGRATION_001_PATH, MIGRATION_002_PATH, MIGRATION_003_PATH, MIGRATION_004_PATH):
            if mig.exists():
                integrity = self.apply_migration(mig)
        return integrity

    def get_master_fernet_key(self) -> bytes:
        """
        Retrieves or derives the Fernet encryption master key.
        Enforces mandatory explicit key presence in LIVE and TESTNET modes.
        """
        cfg = self.load_config(decrypt=False)
        exec_mode = str(cfg.get("execution_mode", "PAPER")).strip().upper()
        enforce_key = bool(cfg.get("enforce_master_key", True))

        if self._explicit_master_key is not None:
            return _normalize_fernet_key(self._explicit_master_key)

        env_key = (
            os.environ.get("AEGIS_MASTER_KEY")
            or os.environ.get("FERNET_MASTER_KEY")
            or ""
        ).strip()
        if env_key:
            return _normalize_fernet_key(env_key)

        # Systemd credential path check
        cred_dir = os.environ.get("CREDENTIALS_DIRECTORY", "").strip()
        if cred_dir:
            cred_dir_path = Path(cred_dir)
            cred_path = cred_dir_path / "aegis_master_key"
            if cred_path.exists():
                return _normalize_fernet_key(cred_path.read_bytes().strip())
            if cred_dir_path.is_dir():
                for child in sorted(cred_dir_path.iterdir()):
                    if child.is_file():
                        content = child.read_bytes().strip()
                        if content:
                            return _normalize_fernet_key(content)

        if exec_mode in ("LIVE", "TESTNET") and enforce_key:
            msg = (
                f"FATAL SECURITY VIOLATION: Execution mode is '{exec_mode}' but no explicit "
                "AEGIS_MASTER_KEY environment variable or systemd credential was provided. "
                "Refusing to run with the hardcoded fallback key."
            )
            self.log_telemetry("CRITICAL", "database", msg)
            raise RuntimeError(msg)

        # Fallback allowed strictly in PAPER/local development mode
        return base64.urlsafe_b64encode(
            hashlib.sha256(b"aegis-quant-v0.2.0-rc1-fernet-master-key").digest()
        )

    @staticmethod
    def _execute_sql_idempotent(conn: sqlite3.Connection, sql_script: str) -> None:
        """Execute a multi-statement SQL script, ignoring duplicate column errors on re-run."""
        buffer: list[str] = []
        for line in sql_script.splitlines():
            stripped = line.strip()
            if not stripped and not buffer:
                continue
            if stripped.startswith("--") and not buffer:
                continue
            buffer.append(line)
            candidate_stmt = "\n".join(buffer)
            if sqlite3.complete_statement(candidate_stmt):
                buffer.clear()
                clean_stmt = "\n".join(
                    ln for ln in candidate_stmt.splitlines() if not ln.strip().startswith("--")
                ).strip()
                if not clean_stmt:
                    continue
                try:
                    conn.execute(clean_stmt)
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" in str(exc).lower():
                        continue
                    raise
        if buffer:
            rem = "\n".join(
                ln for ln in buffer if not ln.strip().startswith("--")
            ).strip()
            if rem:
                try:
                    conn.execute(rem)
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise

    def _ensure_order_fsm_schema(self, conn: sqlite3.Connection) -> None:
        """Ensure Migration 002 columns on trades and order_state_audit table exist."""
        cols = {
            str(r["name"])
            for r in conn.execute("PRAGMA table_info(trades);").fetchall()
        }
        has_audit = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='order_state_audit';"
        ).fetchone()
        if (
            not {"client_order_id", "fsm_state", "exchange_order_id", "avg_fill_price", "filled_units", "slippage_paid_usd"}.issubset(cols)
            or not has_audit
        ):
            if MIGRATION_002_PATH.exists():
                self._execute_sql_idempotent(
                    conn, MIGRATION_002_PATH.read_text(encoding="utf-8")
                )

    def _get_fernet(self) -> Fernet:
        """Return the active Fernet cipher (dynamically honoring env/systemd credential overrides and LIVE key enforcement)."""
        try:
            with self.connection() as conn:
                has_enforce = conn.execute(
                    "SELECT config_value FROM system_config WHERE config_key = 'enforce_master_key';"
                ).fetchone()
        except (sqlite3.Error, OSError, ValueError, RuntimeError):
            has_enforce = None

        if has_enforce is not None:
            key = self.get_master_fernet_key()
        else:
            key = _resolve_fernet_master_key(self._explicit_master_key)
        if key != self._fernet_key:
            self._fernet_key = key
            self._fernet = Fernet(key)
        return self._fernet

    @staticmethod
    def is_encrypted_value(value: Any) -> bool:
        """Return True if `value` is a Fernet ciphertext token stored at rest."""
        if not isinstance(value, str):
            return False
        s = value.strip()
        return s.startswith("gAAAAAB") or s.startswith("ENC:gAAAAAB")

    def encrypt_config_value(self, plaintext: str) -> str:
        """Encrypt a sensitive configuration parameter using Fernet (AES-128-CBC + HMAC-SHA256)."""
        val = str(plaintext or "").strip()
        if not val:
            return ""
        cipher = self._get_fernet()
        raw_token = val[4:] if val.startswith("ENC:") else val
        if raw_token.startswith("gAAAAAB"):
            with suppress(InvalidToken, ValueError, TypeError):
                cipher.decrypt(raw_token.encode("utf-8"))
                return raw_token
        return cipher.encrypt(val.encode("utf-8")).decode("utf-8")

    def decrypt_config_value(self, raw_value: str) -> str:
        """Transparently decrypt a Fernet-encrypted configuration parameter in memory."""
        if not raw_value or not isinstance(raw_value, str):
            return str(raw_value or "")
        s = raw_value.strip()
        if not s:
            return ""
        token = s[4:] if s.startswith("ENC:") else s
        if not token.startswith("gAAAAAB"):
            return s
        cipher = self._get_fernet()
        try:
            return cipher.decrypt(token.encode("utf-8")).decode("utf-8")
        except (InvalidToken, ValueError, TypeError) as exc:
            if self._explicit_master_key is None and self._fernet_key != _DEFAULT_HOST_FERNET_KEY:
                with suppress(InvalidToken, ValueError, TypeError):
                    return (
                        Fernet(_DEFAULT_HOST_FERNET_KEY)
                        .decrypt(token.encode("utf-8"))
                        .decode("utf-8")
                    )
            raise ValueError(
                "Failed to decrypt Fernet-encrypted configuration value: invalid master key or corrupted token."
            ) from exc

    def _try_decrypt_or_empty(self, raw_value: str) -> str:
        """Decrypt a config value in memory or return empty string if encrypted under a rotated/stale master key."""
        if not raw_value or not isinstance(raw_value, str):
            return str(raw_value or "")
        s = raw_value.strip()
        if not s:
            return ""
        if not self.is_encrypted_value(s):
            return s
        try:
            return self.decrypt_config_value(s)
        except ValueError:
            return ""

    def rotate_master_key(self, new_master_key: str | bytes) -> dict[str, Any]:
        """
        Atomically decrypt all sensitive parameters in system_config using the current Fernet key
        and re-encrypt them under `new_master_key`.
        """
        new_fernet_key = _normalize_fernet_key(new_master_key)
        new_cipher = Fernet(new_fernet_key)
        re_encrypted: list[str] = []

        with self.connection() as conn:
            rows = conn.execute(
                "SELECT config_key, config_value FROM system_config;"
            ).fetchall()
            for row in rows:
                k = str(row["config_key"])
                raw_v = str(row["config_value"] or "").strip()
                if k not in SENSITIVE_CONFIG_KEYS or not raw_v:
                    continue
                plain_v = self._try_decrypt_or_empty(raw_v)
                if not plain_v:
                    continue
                enc_v = new_cipher.encrypt(plain_v.encode("utf-8")).decode("utf-8")
                conn.execute(
                    """
                    UPDATE system_config
                    SET config_value = ?,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                    WHERE config_key = ?;
                    """,
                    (enc_v, k),
                )
                re_encrypted.append(k)

        self._explicit_master_key = new_master_key
        self._fernet_key = new_fernet_key
        self._fernet = new_cipher
        return {
            "status": "ROTATED",
            "re_encrypted_keys": sorted(re_encrypted),
        }

    def get_raw_config_value(self, key: str) -> str | None:
        """Return the raw on-disk value from system_config without decrypting."""
        with self.connection() as conn:
            row = conn.execute(
                "SELECT config_value FROM system_config WHERE config_key = ?;",
                (key,),
            ).fetchone()
        return str(row["config_value"]) if row is not None else None

    def get_schema_version(self) -> int:
        """Return the active schema migration version from schema_migrations (or 0 if unversioned)."""
        with self.connection() as conn:
            has_table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';"
            ).fetchone()
            if not has_table:
                return 0
            row = conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS max_ver FROM schema_migrations;"
            ).fetchone()
            return int(row["max_ver"]) if row is not None else 0

    def get_applied_migrations(self) -> list[dict[str, Any]]:
        """Return all applied migration records from schema_migrations ordered by version."""
        with self.connection() as conn:
            has_table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';"
            ).fetchone()
            if not has_table:
                return []
            rows = conn.execute(
                "SELECT version, applied_at_utc, description FROM schema_migrations ORDER BY version ASC;"
            ).fetchall()
        return [dict(r) for r in rows]

    def apply_migration(self, migration_path: Path | str) -> dict[str, Any]:
        """Execute a versioned DDL/DML SQL migration script and return updated integrity counts."""
        mig_path = Path(migration_path)
        if not mig_path.exists():
            candidate = PROJECT_ROOT / migration_path
            if candidate.exists():
                mig_path = candidate
            else:
                raise FileNotFoundError(f"Migration SQL file not found: {migration_path}")

        # Ensure base tables exist before applying incremental migration
        with self.connection() as conn:
            has_cfg = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='system_config';"
            ).fetchone()
        if not has_cfg:
            self.init_db()

        sql_script = mig_path.read_text(encoding="utf-8")
        with self.connection() as conn:
            if mig_path.name.startswith(("003_", "004_")):
                if MIGRATION_001_PATH.exists():
                    self._execute_sql_idempotent(
                        conn, MIGRATION_001_PATH.read_text(encoding="utf-8")
                    )
                if MIGRATION_002_PATH.exists():
                    self._execute_sql_idempotent(
                        conn, MIGRATION_002_PATH.read_text(encoding="utf-8")
                    )
                if mig_path.name.startswith("004_") and MIGRATION_003_PATH.exists():
                    self._execute_sql_idempotent(
                        conn, MIGRATION_003_PATH.read_text(encoding="utf-8")
                    )
            self._execute_sql_idempotent(conn, sql_script)

            has_mig_table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';"
            ).fetchone()
            if has_mig_table and mig_path.name[:3].isdigit():
                ver_num = int(mig_path.name[:3])
                conn.execute(
                    """
                    INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
                    VALUES (?, CAST(strftime('%s', 'now') AS INTEGER), ?);
                    """,
                    (ver_num, f"Migration {mig_path.name[:3]}: {mig_path.stem}"),
                )

        integrity = self.verify_integrity()
        self.log_telemetry(
            "INFO",
            "engine.database",
            f"Applied SQLite migration {mig_path.name}",
            {"migration_file": str(mig_path), **integrity},
        )
        return {
            "status": "APPLIED",
            "migration_file": str(mig_path.name),
            **integrity,
        }

    def verify_integrity(self) -> dict[str, Any]:
        """Verify WAL journal mode, config keys, 31 universe asset views, and active schema version."""
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
            has_mig_table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_migrations';"
            ).fetchone()
            schema_version = 0
            if has_mig_table:
                ver_row = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) FROM schema_migrations;"
                ).fetchone()
                schema_version = int(ver_row[0]) if ver_row else 0

        return {
            "journal_mode": str(journal_mode).lower(),
            "system_config_count": int(config_count),
            "v_universe_usdc_perp_count": int(usdc_perp_count),
            "v_universe_btc_synth_count": int(btc_synth_count),
            "v_universe_alt_pairs_count": int(alt_pairs_count),
            "universe_assets_total": int(total_universe),
            "schema_version": schema_version,
        }

    def load_config(self, decrypt: bool = True) -> dict[str, Any]:
        """Load all system_config variables cast to their declared types (transparently decrypting secrets in memory)."""
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT config_key, config_value, value_type FROM system_config;"
            ).fetchall()
        out: dict[str, Any] = {}
        for row in rows:
            raw_val = str(row["config_value"] or "")
            if decrypt and self.is_encrypted_value(raw_val):
                raw_val = self._try_decrypt_or_empty(raw_val)
            out[str(row["config_key"])] = _cast_config_value(raw_val, str(row["value_type"]))
        return out

    def get_decrypted_config(
        self, key: str | None = None, default: Any = None
    ) -> Any:
        """
        Decrypt configuration parameters in memory on demand using Fernet (AES-128-CBC + HMAC-SHA256),
        ensuring unencrypted secret parameters are never stored on disk.
        If `key` is provided, returns the decrypted value for `key`; otherwise returns the full decrypted config dict.
        """
        with self.connection() as conn:
            if key is not None:
                row = conn.execute(
                    "SELECT config_key, config_value, value_type FROM system_config WHERE config_key = ?;",
                    (key,),
                ).fetchone()
                if row is None:
                    return default
                raw_val = str(row["config_value"] or "")
                dec_val = (
                    self._try_decrypt_or_empty(raw_val)
                    if self.is_encrypted_value(raw_val)
                    else raw_val
                )
                return _cast_config_value(dec_val, str(row["value_type"]))

            rows = conn.execute(
                "SELECT config_key, config_value, value_type FROM system_config;"
            ).fetchall()
        result: dict[str, Any] = {}
        for row in rows:
            raw_val = str(row["config_value"] or "")
            dec_val = (
                self._try_decrypt_or_empty(raw_val)
                if self.is_encrypted_value(raw_val)
                else raw_val
            )
            result[str(row["config_key"])] = _cast_config_value(dec_val, str(row["value_type"]))
        return result

    def get_config_rows(self, mask_secrets: bool = True) -> list[dict[str, Any]]:
        """Return full metadata rows for system_config, optionally masking keys."""
        secret_keys = set(SENSITIVE_CONFIG_KEYS)
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
            raw_val = str(item.get("config_value") or "")
            if item["config_key"] in secret_keys and raw_val:
                plain_val = (
                    self._try_decrypt_or_empty(raw_val)
                    if self.is_encrypted_value(raw_val)
                    else raw_val
                )
                item["config_value"] = (
                    _mask_secret(plain_val) if mask_secrets else plain_val
                )
            result.append(item)
        return result

    def set_config_value(self, key: str, value: Any) -> bool:
        """Update an existing configuration parameter in system_config with type validation and Fernet encryption at rest."""
        cred_field_map = {
            "wallet_address": "wallet_address",
            "private_key": "private_key",
            "hyperliquid_vault_address": "hyperliquid_vault_address",
            "gemini_api_key": "gemini_api_key",
            "hyperliquid_testnet": "hyperliquid_testnet",
            "execution_mode": "execution_mode",
        }
        if key in cred_field_map:
            req_creds = not (
                key == "execution_mode"
                and (getattr(self, "_initialized_via_alias", False) or self.get_schema_version() >= 4)
            )
            self.configure_hyperliquid_credentials(
                **{cred_field_map[key]: value},
                require_credentials_for_live=req_creds,
            )
            return True

        with self.connection() as conn:
            row = conn.execute(
                "SELECT value_type FROM system_config WHERE config_key = ?;",
                (key,),
            ).fetchone()
            if row is None and key in _ALERT_CONFIG_DEFAULTS:
                def_val, def_type, def_cat, def_desc = _ALERT_CONFIG_DEFAULTS[key]
                conn.execute(
                    """
                    INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (key, def_val, def_type, def_cat, def_desc),
                )
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
                if key == "max_daily_drawdown_alert_pct" and (f_val <= 0.0 or f_val >= 1.0):
                    raise ValueError(
                        "Invalid max_daily_drawdown_alert_pct: must be a fraction in (0.0, 1.0), e.g. 0.03 for 3%."
                    )
                str_val = str(f_val)
            else:
                if key == "telegram_chat_id" and isinstance(value, int) and not isinstance(value, bool):
                    str_val = str(value)
                elif key in _ALERT_CONFIG_DEFAULTS and isinstance(value, str):
                    str_val = value.strip()
                else:
                    if not isinstance(value, str) or not value.strip():
                        raise ValueError(f"Invalid string value for '{key}': {value!r}")
                    str_val = value.strip()

            stored_val = (
                self.encrypt_config_value(str_val)
                if (key in SENSITIVE_CONFIG_KEYS and str_val)
                else str_val
            )
            cur = conn.execute(
                """
                UPDATE system_config
                SET config_value = ?,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                WHERE config_key = ?;
                """,
                (stored_val, key),
            )
            return cur.rowcount > 0

    def get_credentials_status(
        self, config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Return masked Hyperliquid L1 Mainnet/Testnet credential, routing, and Telegram alerting status."""
        cfg = config if config is not None else self.load_config()
        wallet = str(cfg.get("wallet_address", "")).strip()
        priv_key = self._try_decrypt_or_empty(str(cfg.get("private_key", "")).strip())
        vault = str(cfg.get("hyperliquid_vault_address", "")).strip()
        gemini_key = self._try_decrypt_or_empty(str(cfg.get("gemini_api_key", "")).strip())
        tg_token = self._try_decrypt_or_empty(str(cfg.get("telegram_bot_token", "")).strip())
        tg_chat_id = str(cfg.get("telegram_chat_id", "")).strip()
        dd_alert_pct = float(cfg.get("max_daily_drawdown_alert_pct", 0.03))

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
            "alert_channel": "TELEGRAM",
            "telegram_bot_token_masked": _mask_secret(tg_token),
            "telegram_bot_token_configured": bool(tg_token),
            "telegram_chat_id": tg_chat_id,
            "telegram_alerts_ready": bool(tg_token and tg_chat_id),
            "max_daily_drawdown_alert_pct": dd_alert_pct,
        }

    def configure_telegram_alerts(
        self,
        *,
        telegram_bot_token: str | None = None,
        telegram_chat_id: str | int | None = None,
        max_daily_drawdown_alert_pct: float | None = None,
    ) -> dict[str, Any]:
        """Configure Telegram Bot API alerting parameters in SQLite system_config (encrypting token at rest)."""
        if telegram_bot_token is not None:
            raw_tok = (
                self.decrypt_config_value(telegram_bot_token)
                if isinstance(telegram_bot_token, str) and self.is_encrypted_value(telegram_bot_token)
                else str(telegram_bot_token).strip()
            )
            self.set_config_value("telegram_bot_token", raw_tok)
        if telegram_chat_id is not None:
            self.set_config_value("telegram_chat_id", str(telegram_chat_id).strip())
        if max_daily_drawdown_alert_pct is not None:
            if isinstance(max_daily_drawdown_alert_pct, bool):
                raise ValueError("Invalid max_daily_drawdown_alert_pct: must be a float in (0.0, 1.0).")
            val_pct = float(max_daily_drawdown_alert_pct)
            if not math.isfinite(val_pct) or val_pct <= 0.0 or val_pct >= 1.0:
                raise ValueError(
                    "Invalid max_daily_drawdown_alert_pct: must be a fraction in (0.0, 1.0), e.g. 0.03 for 3%."
                )
            self.set_config_value("max_daily_drawdown_alert_pct", val_pct)
        return self.get_credentials_status()

    def configure_hyperliquid_credentials(
        self,
        *,
        wallet_address: str | None = None,
        private_key: str | None = None,
        hyperliquid_testnet: bool | str | None = None,
        execution_mode: str | None = None,
        hyperliquid_vault_address: str | None = None,
        gemini_api_key: str | None = None,
        telegram_bot_token: str | None = None,
        telegram_chat_id: str | int | None = None,
        max_daily_drawdown_alert_pct: float | None = None,
        require_credentials_for_live: bool = True,
    ) -> dict[str, Any]:
        """
        Validate and atomically update Hyperliquid Mainnet/Testnet credentials and
        execution routing parameters in SQLite system_config (encrypting secrets at rest via Fernet).
        """
        updates: dict[str, str] = {}

        if wallet_address is not None:
            updates["wallet_address"] = validate_evm_address(wallet_address, "wallet_address")

        if private_key is not None:
            raw_priv = (
                self.decrypt_config_value(private_key)
                if isinstance(private_key, str) and self.is_encrypted_value(private_key)
                else private_key
            )
            updates["private_key"] = validate_private_key(raw_priv)

        if hyperliquid_vault_address is not None:
            updates["hyperliquid_vault_address"] = validate_evm_address(
                hyperliquid_vault_address, "hyperliquid_vault_address"
            )

        if gemini_api_key is not None:
            if not isinstance(gemini_api_key, str):
                raise ValueError("Invalid gemini_api_key: must be a string.")
            raw_gem = (
                self.decrypt_config_value(gemini_api_key)
                if self.is_encrypted_value(gemini_api_key)
                else gemini_api_key.strip()
            )
            updates["gemini_api_key"] = raw_gem

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
            current_cfg: dict[str, Any] = {}
            for row in rows:
                r_val = str(row["config_value"] or "")
                if self.is_encrypted_value(r_val):
                    r_val = self._try_decrypt_or_empty(r_val)
                current_cfg[str(row["config_key"])] = _cast_config_value(
                    r_val, str(row["value_type"])
                )

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
                stored_v = (
                    self.encrypt_config_value(v)
                    if (k in SENSITIVE_CONFIG_KEYS and v)
                    else v
                )
                conn.execute(
                    """
                    UPDATE system_config
                    SET config_value = ?,
                        updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                    WHERE config_key = ?;
                    """,
                    (stored_v, k),
                )
                current_cfg[k] = v

        if any(
            x is not None
            for x in (telegram_bot_token, telegram_chat_id, max_daily_drawdown_alert_pct)
        ):
            return self.configure_telegram_alerts(
                telegram_bot_token=telegram_bot_token,
                telegram_chat_id=telegram_chat_id,
                max_daily_drawdown_alert_pct=max_daily_drawdown_alert_pct,
            )

        return self.get_credentials_status(current_cfg)

    def test_encryption(self) -> dict[str, Any]:
        """
        Verify AES-128-CBC + HMAC-SHA256 (cryptography.fernet.Fernet) configuration encryption
        at rest in SQLite system_config and transparent in-memory decryption via get_decrypted_config().
        """
        with self.connection() as conn:
            has_cfg = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='system_config';"
            ).fetchone()
        if not has_cfg:
            self.init_db()

        sample_priv = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
        sample_gemini = "AIzaSyAegisQuantFernetVerifiedKey020rc1"

        prior_priv_raw = self.get_raw_config_value("private_key") or ""
        prior_gemini_raw = self.get_raw_config_value("gemini_api_key") or ""

        # Write test secrets via set_config_value so they are encrypted at rest in system_config
        prior_priv_dec = self._try_decrypt_or_empty(prior_priv_raw)
        prior_gemini_dec = self._try_decrypt_or_empty(prior_gemini_raw)
        target_priv = prior_priv_dec if prior_priv_dec else sample_priv
        target_gemini = prior_gemini_dec if prior_gemini_dec else sample_gemini

        self.set_config_value("private_key", target_priv)
        self.set_config_value("gemini_api_key", target_gemini)

        raw_priv_on_disk = self.get_raw_config_value("private_key") or ""
        raw_gemini_on_disk = self.get_raw_config_value("gemini_api_key") or ""

        priv_encrypted_ok = (
            self.is_encrypted_value(raw_priv_on_disk) and raw_priv_on_disk != target_priv
        )
        gemini_encrypted_ok = (
            self.is_encrypted_value(raw_gemini_on_disk) and raw_gemini_on_disk != target_gemini
        )

        decrypted_cfg = self.get_decrypted_config()
        priv_decrypted_ok = decrypted_cfg.get("private_key") == target_priv
        gemini_decrypted_ok = decrypted_cfg.get("gemini_api_key") == target_gemini

        # Restore private_key if it was previously unset so default PAPER state remains clean,
        # while keeping gemini_api_key encrypted in system_config as persistent verification proof.
        if not prior_priv_dec:
            self.set_config_value("private_key", "")

        verified = bool(
            priv_encrypted_ok
            and gemini_encrypted_ok
            and priv_decrypted_ok
            and gemini_decrypted_ok
        )
        if not verified:
            raise RuntimeError("Fernet AES-128 configuration encryption verification failed.")

        report: dict[str, Any] = {
            "status": "VERIFIED",
            "cipher": "AES-128-CBC + HMAC-SHA256 (cryptography.fernet.Fernet)",
            "encrypted_keys_verified": ["private_key", "gemini_api_key"],
            "ciphertext_at_rest_sample": f"{raw_gemini_on_disk[:24]}...",
            "is_fernet_token": True,
            "unencrypted_on_disk": False,
            "transparent_decryption_verified": True,
            "schema_version": self.get_schema_version(),
        }
        self.log_telemetry(
            "INFO",
            "engine.database",
            "Verified Fernet AES-128 configuration encryption at rest",
            report,
        )
        return report

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
        self,
        symbol: str,
        timeframe: str,
        limit: int = 600,
        max_timestamp_ms: int | None = None,
    ) -> pd.DataFrame:
        """Fetch historical OHLCV bars for a symbol/timeframe sorted chronologically."""
        eff_max_ts = max_timestamp_ms if max_timestamp_ms is not None else self.pit_timestamp_ms
        with self.connection() as conn:
            if eff_max_ts is not None:
                rows = conn.execute(
                    """
                    SELECT timestamp_ms, open, high, low, close, volume,
                           funding_rate_hourly, is_synthetic
                    FROM market_ohlcv
                    WHERE symbol = ? AND timeframe = ? AND timestamp_ms <= ?
                    ORDER BY timestamp_ms DESC
                    LIMIT ?;
                    """,
                    (symbol, timeframe, int(eff_max_ts), limit),
                ).fetchall()
            else:
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

    @staticmethod
    def _normalize_trade_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        """Normalize a trades row dict with Order FSM and reconciliation aliases."""
        d = dict(row)
        trade_id = d.get("trade_id", 0)
        ex_id = d.get("exchange_order_id")
        cli_id = d.get("client_order_id") or ex_id or f"trade-{trade_id}"
        d["client_order_id"] = str(cli_id)
        stop_px = d.get("chandelier_stop_price")
        if stop_px is None:
            stop_px = d.get("initial_stop_price", d.get("entry_price", 0.0))
        d["current_stop_price"] = float(stop_px or 0.0)
        d["fsm_state"] = str(d.get("fsm_state") or "FILLED")
        if d.get("avg_fill_price") is None and d.get("entry_price") is not None:
            d["avg_fill_price"] = float(d["entry_price"])
        if d.get("filled_units") is None:
            d["filled_units"] = float(d.get("quantity") or 0.0)
        if d.get("slippage_paid_usd") is None:
            d["slippage_paid_usd"] = 0.0
        return d

    def get_open_trades(self) -> list[dict[str, Any]]:
        """Return all currently open positions."""
        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            rows = conn.execute(
                "SELECT * FROM trades WHERE status = 'OPEN' ORDER BY opened_at ASC;"
            ).fetchall()
        return [self._normalize_trade_row(r) for r in rows]

    def get_recent_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent open and closed trades."""
        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            rows = conn.execute(
                "SELECT * FROM trades ORDER BY trade_id DESC LIMIT ?;",
                (limit,),
            ).fetchall()
        return [self._normalize_trade_row(r) for r in rows]

    def get_trade_by_client_order_id(self, client_order_id: str) -> dict[str, Any] | None:
        """Fetch a trade record by client_order_id, exchange_order_id, or trade-<id>."""
        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            row = conn.execute(
                """
                SELECT * FROM trades
                WHERE client_order_id = ? OR exchange_order_id = ?
                ORDER BY trade_id DESC
                LIMIT 1;
                """,
                (client_order_id, client_order_id),
            ).fetchone()
            if row is None and str(client_order_id).startswith("trade-"):
                with suppress(ValueError):
                    tid = int(str(client_order_id).split("-", 1)[1])
                    row = conn.execute(
                        "SELECT * FROM trades WHERE trade_id = ?;",
                        (tid,),
                    ).fetchone()
        return self._normalize_trade_row(row) if row is not None else None

    def insert_trade(self, trade: dict[str, Any]) -> int:
        """Insert a new trade record (with Order FSM tracking columns) and return its trade_id."""
        import uuid

        symbol = str(trade.get("symbol", "SOL/USDC:USDC"))
        base_asset = str(
            trade.get("base_asset") or symbol.split("/")[0].split(":")[0]
        )
        side = str(trade.get("side", "LONG")).upper()
        status = str(trade.get("status", "OPEN")).upper()
        fsm_state = str(trade.get("fsm_state", "FILLED")).upper()
        if fsm_state not in _VALID_FSM_STATES:
            raise ValueError(
                f"Invalid fsm_state '{fsm_state}': must be one of {sorted(_VALID_FSM_STATES)}"
            )

        entry_price = float(
            trade["entry_price"]
            if trade.get("entry_price") is not None
            else (trade.get("avg_fill_price") or 0.0)
        )
        current_price = float(
            trade["current_price"]
            if trade.get("current_price") is not None
            else entry_price
        )
        quantity = float(
            trade["quantity"]
            if trade.get("quantity") is not None
            else (trade.get("filled_units") or 0.0)
        )
        notional_usdc = float(
            trade["notional_usdc"]
            if trade.get("notional_usdc") is not None
            else round(quantity * entry_price, 2)
        )
        risk_budget_usdc = float(
            trade["risk_budget_usdc"]
            if trade.get("risk_budget_usdc") is not None
            else round(notional_usdc * 0.04, 2)
        )
        conviction_multiplier = float(trade.get("conviction_multiplier", 1.0))

        default_stop = (
            entry_price * 0.95 if side == "LONG" else entry_price * 1.05
        )
        stop_override = trade.get("current_stop_price")
        initial_stop_price = float(
            trade["initial_stop_price"]
            if trade.get("initial_stop_price") is not None
            else (stop_override if stop_override is not None else default_stop)
        )
        chandelier_stop_price = float(
            trade["chandelier_stop_price"]
            if trade.get("chandelier_stop_price") is not None
            else (stop_override if stop_override is not None else initial_stop_price)
        )
        highest_price_seen = float(
            trade["highest_price_seen"]
            if trade.get("highest_price_seen") is not None
            else entry_price
        )
        lowest_price_seen = float(
            trade["lowest_price_seen"]
            if trade.get("lowest_price_seen") is not None
            else entry_price
        )
        fees_paid_usdc = float(trade.get("fees_paid_usdc", 0.0))
        funding_accrued_usdc = float(trade.get("funding_accrued_usdc", 0.0))
        unrealized_pnl_usdc = float(trade.get("unrealized_pnl_usdc", 0.0))
        exchange_order_id = trade.get("exchange_order_id")
        client_order_id = str(
            trade.get("client_order_id")
            or exchange_order_id
            or f"cli-{uuid.uuid4().hex[:12]}"
        )
        avg_fill_price = (
            float(trade["avg_fill_price"])
            if trade.get("avg_fill_price") is not None
            else (entry_price if fsm_state == "FILLED" else None)
        )
        filled_units = (
            float(trade["filled_units"])
            if trade.get("filled_units") is not None
            else (quantity if fsm_state == "FILLED" else 0.0)
        )
        slippage_paid_usd = float(trade.get("slippage_paid_usd", 0.0))

        payload: dict[str, Any] = {
            "client_order_id": client_order_id,
            "symbol": symbol,
            "base_asset": base_asset,
            "side": side,
            "status": status,
            "fsm_state": fsm_state,
            "entry_price": entry_price,
            "current_price": current_price,
            "quantity": quantity,
            "notional_usdc": notional_usdc,
            "risk_budget_usdc": risk_budget_usdc,
            "conviction_multiplier": conviction_multiplier,
            "initial_stop_price": initial_stop_price,
            "chandelier_stop_price": chandelier_stop_price,
            "highest_price_seen": highest_price_seen,
            "lowest_price_seen": lowest_price_seen,
            "fees_paid_usdc": fees_paid_usdc,
            "funding_accrued_usdc": funding_accrued_usdc,
            "unrealized_pnl_usdc": unrealized_pnl_usdc,
            "exchange_order_id": exchange_order_id,
            "avg_fill_price": avg_fill_price,
            "filled_units": filled_units,
            "slippage_paid_usd": slippage_paid_usd,
        }

        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            cur = conn.execute(
                """
                INSERT INTO trades (
                    client_order_id, symbol, base_asset, side, status, fsm_state,
                    entry_price, current_price, quantity, notional_usdc,
                    risk_budget_usdc, conviction_multiplier,
                    initial_stop_price, chandelier_stop_price,
                    highest_price_seen, lowest_price_seen,
                    fees_paid_usdc, funding_accrued_usdc, unrealized_pnl_usdc,
                    exchange_order_id, avg_fill_price, filled_units, slippage_paid_usd
                ) VALUES (
                    :client_order_id, :symbol, :base_asset, :side, :status, :fsm_state,
                    :entry_price, :current_price, :quantity, :notional_usdc,
                    :risk_budget_usdc, :conviction_multiplier,
                    :initial_stop_price, :chandelier_stop_price,
                    :highest_price_seen, :lowest_price_seen,
                    :fees_paid_usdc, :funding_accrued_usdc, :unrealized_pnl_usdc,
                    :exchange_order_id, :avg_fill_price, :filled_units, :slippage_paid_usd
                );
                """,
                payload,
            )
            return int(cur.lastrowid or 0)

    def log_order_state_transition(
        self,
        client_order_id: str,
        from_state: str,
        to_state: str,
        reason: str | None = None,
        timestamp_utc: int | None = None,
    ) -> int:
        """Record an Order FSM state transition in order_state_audit."""
        ts_utc = (
            int(timestamp_utc)
            if timestamp_utc is not None
            else (
                int(self.pit_timestamp_ms)
                if self.pit_timestamp_ms is not None
                else int(time.time() * 1000)
            )
        )
        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            cur = conn.execute(
                """
                INSERT INTO order_state_audit (
                    client_order_id, from_state, to_state, timestamp_utc, reason
                ) VALUES (?, ?, ?, ?, ?);
                """,
                (str(client_order_id), str(from_state), str(to_state), ts_utc, reason),
            )
            return int(cur.lastrowid or 0)

    def transition_order_state(
        self,
        client_order_id: str,
        to_state: str,
        *,
        from_state: str | None = None,
        reason: str | None = None,
        exchange_order_id: str | None = None,
        avg_fill_price: float | None = None,
        filled_units: float | None = None,
        quantity: float | None = None,
        notional_usdc: float | None = None,
        slippage_paid_usd: float | None = None,
        fees_paid_usdc: float | None = None,
        status: str | None = None,
        timestamp_utc: int | None = None,
    ) -> None:
        """Transition a trade's Order FSM state and write an audit log entry."""
        to_up = str(to_state).upper().strip()
        if to_up not in _VALID_FSM_STATES:
            raise ValueError(
                f"Invalid target fsm_state '{to_state}': must be one of {sorted(_VALID_FSM_STATES)}"
            )
        ts_utc = (
            int(timestamp_utc)
            if timestamp_utc is not None
            else (
                int(self.pit_timestamp_ms)
                if self.pit_timestamp_ms is not None
                else int(time.time() * 1000)
            )
        )
        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            row = conn.execute(
                """
                SELECT trade_id, fsm_state
                FROM trades
                WHERE client_order_id = ? OR exchange_order_id = ?
                ORDER BY trade_id DESC
                LIMIT 1;
                """,
                (client_order_id, client_order_id),
            ).fetchone()

            prior_state = str(
                from_state
                if from_state is not None
                else (row["fsm_state"] if row is not None and row["fsm_state"] else "PENDING")
            )

            if row is not None:
                updates = ["fsm_state = ?"]
                params: list[Any] = [to_up]
                if exchange_order_id is not None:
                    updates.append("exchange_order_id = ?")
                    params.append(str(exchange_order_id))
                if avg_fill_price is not None:
                    updates.append("avg_fill_price = ?")
                    params.append(float(avg_fill_price))
                    updates.append("entry_price = ?")
                    params.append(float(avg_fill_price))
                    updates.append("current_price = ?")
                    params.append(float(avg_fill_price))
                if filled_units is not None:
                    updates.append("filled_units = ?")
                    params.append(float(filled_units))
                if quantity is not None:
                    updates.append("quantity = ?")
                    params.append(float(quantity))
                if notional_usdc is not None:
                    updates.append("notional_usdc = ?")
                    params.append(float(notional_usdc))
                if slippage_paid_usd is not None:
                    updates.append("slippage_paid_usd = ?")
                    params.append(float(slippage_paid_usd))
                if fees_paid_usdc is not None:
                    updates.append("fees_paid_usdc = ?")
                    params.append(float(fees_paid_usdc))
                if status is not None:
                    updates.append("status = ?")
                    params.append(str(status).upper())
                params.append(int(row["trade_id"]))
                conn.execute(
                    f"UPDATE trades SET {', '.join(updates)} WHERE trade_id = ?;",
                    tuple(params),
                )

            conn.execute(
                """
                INSERT INTO order_state_audit (
                    client_order_id, from_state, to_state, timestamp_utc, reason
                ) VALUES (?, ?, ?, ?, ?);
                """,
                (str(client_order_id), prior_state, to_up, ts_utc, reason),
            )

    def get_order_state_audit(
        self, client_order_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Return order state transition audit rows ordered chronologically."""
        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            if client_order_id is not None:
                rows = conn.execute(
                    """
                    SELECT id, client_order_id, from_state, to_state, timestamp_utc, reason
                    FROM order_state_audit
                    WHERE client_order_id = ?
                    ORDER BY id ASC;
                    """,
                    (str(client_order_id),),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, client_order_id, from_state, to_state, timestamp_utc, reason
                    FROM order_state_audit
                    ORDER BY id ASC;
                    """
                ).fetchall()
        return [dict(r) for r in rows]

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
            self._ensure_order_fsm_schema(conn)
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

    def reconcile_open_trade_size(
        self,
        trade_id: int,
        on_chain_size: float,
    ) -> None:
        """Synchronize an open trade's quantity, filled_units, notional, and risk budget to on_chain_size."""
        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            row = conn.execute(
                "SELECT * FROM trades WHERE trade_id = ? AND status = 'OPEN';",
                (int(trade_id),),
            ).fetchone()
            if row is None:
                return
            old_qty = float(
                row["filled_units"]
                if row["filled_units"] and float(row["filled_units"]) > 0
                else (row["quantity"] or 1.0)
            )
            entry_px = float(row["entry_price"] or 0.0)
            old_risk = float(row["risk_budget_usdc"] or 0.0)
            scale = float(on_chain_size) / max(old_qty, 1e-12)
            new_notional = round(float(on_chain_size) * entry_px, 2)
            new_risk = round(old_risk * scale, 2)
            conn.execute(
                """
                UPDATE trades
                SET quantity = ?,
                    filled_units = ?,
                    notional_usdc = ?,
                    risk_budget_usdc = ?
                WHERE trade_id = ? AND status = 'OPEN';
                """,
                (
                    float(on_chain_size),
                    float(on_chain_size),
                    new_notional,
                    new_risk,
                    int(trade_id),
                ),
            )

    def close_trade(
        self,
        trade_id: int | None = None,
        exit_price: float = 0.0,
        realized_pnl_usdc: float | None = None,
        fees_paid_usdc: float | None = None,
        exit_reason: str = "MANUAL_CLOSE",
        *,
        client_order_id: str | None = None,
    ) -> None:
        """Close an open position by trade_id or client_order_id and record realized PnL."""
        import datetime as dt

        if self.pit_timestamp_ms is not None:
            closed_iso = dt.datetime.fromtimestamp(
                int(self.pit_timestamp_ms) / 1000.0, tz=dt.timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            closed_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with self.connection() as conn:
            self._ensure_order_fsm_schema(conn)
            row: sqlite3.Row | None = None
            if trade_id is not None:
                row = conn.execute(
                    "SELECT * FROM trades WHERE trade_id = ?;",
                    (int(trade_id),),
                ).fetchone()
            elif client_order_id is not None:
                row = conn.execute(
                    """
                    SELECT * FROM trades
                    WHERE client_order_id = ? OR exchange_order_id = ?
                    ORDER BY trade_id DESC
                    LIMIT 1;
                    """,
                    (str(client_order_id), str(client_order_id)),
                ).fetchone()
                if row is None and str(client_order_id).startswith("trade-"):
                    with suppress(ValueError):
                        tid = int(str(client_order_id).split("-", 1)[1])
                        row = conn.execute(
                            "SELECT * FROM trades WHERE trade_id = ?;",
                            (tid,),
                        ).fetchone()

            if row is None:
                return

            target_trade_id = int(row["trade_id"])
            entry_px = float(row["entry_price"] or 0.0)
            qty = float(
                row["filled_units"]
                if row["filled_units"] and float(row["filled_units"]) > 0
                else (row["quantity"] or 0.0)
            )
            funding = float(row["funding_accrued_usdc"] or 0.0)
            existing_fees = float(row["fees_paid_usdc"] or 0.0)
            side = str(row["side"] or "LONG").upper()

            eff_fees = float(fees_paid_usdc) if fees_paid_usdc is not None else existing_fees
            if realized_pnl_usdc is not None:
                eff_realized = float(realized_pnl_usdc)
            else:
                if side == "LONG":
                    gross_pnl = (float(exit_price) - entry_px) * qty + funding
                elif side == "SHORT":
                    gross_pnl = (entry_px - float(exit_price)) * qty + funding
                else:
                    gross_pnl = funding
                eff_realized = round(gross_pnl - eff_fees, 4)

            conn.execute(
                """
                UPDATE trades
                SET status = 'CLOSED',
                    current_price = ?,
                    exit_price = ?,
                    realized_pnl_usdc = ?,
                    unrealized_pnl_usdc = 0.0,
                    fees_paid_usdc = ?,
                    closed_at = ?,
                    exit_reason = ?
                WHERE trade_id = ?;
                """,
                (
                    float(exit_price),
                    float(exit_price),
                    eff_realized,
                    eff_fees,
                    closed_iso,
                    exit_reason,
                    target_trade_id,
                ),
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


def main() -> None:
    """CLI entrypoint for SQLite schema initialization, versioned DDL migrations, and Fernet encryption verification."""
    import argparse

    parser = argparse.ArgumentParser(description="Aegis Quant SQLite WAL Database & Migration Manager")
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help="Path to SQLite database file (default: data/aegis_quant.db)",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize base SQLite WAL schema and seed configuration",
    )
    parser.add_argument(
        "--apply-migration",
        type=str,
        default=None,
        help="Apply a versioned SQL migration script (e.g., sql/migrations/003_v020_schema_versioning.sql)",
    )
    parser.add_argument(
        "--test-encryption",
        action="store_true",
        help="Verify Fernet AES-128 configuration encryption at rest and transparent decryption",
    )
    args = parser.parse_args()

    db = DatabaseManager(db_path=args.db_path)
    if args.apply_migration:
        result = db.apply_migration(args.apply_migration)
    elif args.test_encryption:
        result = db.test_encryption()
    else:
        result = db.init_db()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()


