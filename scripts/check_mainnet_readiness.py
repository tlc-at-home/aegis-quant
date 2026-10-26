#!/usr/bin/env python3
"""
Mainnet Go-Live Readiness Gate Checker for Aegis Quant (v0.2.0-rc2).

Evaluates the 5 Go/No-Go Mainnet readiness gates against `data/aegis_quant.db`,
historical backfill SHA-256 provenance, encryption-at-rest posture, risk caps,
and live/testnet soak telemetry, printing a structured PASS/WARN/FAIL checklist.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.backtest import HistoricalBacktester, verify_data_provenance  # noqa: E402
from engine.database import (  # noqa: E402
    DEFAULT_DB_PATH,
    SENSITIVE_CONFIG_KEYS,
    DatabaseManager,
    _DEFAULT_HOST_FERNET_KEY,
    _mask_secret,
    _resolve_fernet_master_key,
    validate_evm_address,
    validate_private_key,
)


@dataclass
class CheckResult:
    """Single readiness check evaluation outcome."""

    gate: str
    check_id: str
    name: str
    status: str  # "PASS", "WARN", or "FAIL"
    observed: str
    requirement: str
    remediation: str = ""


def _parse_iso_utc(ts_str: str | None) -> datetime | None:
    """Parse an ISO-8601 UTC timestamp string (e.g. '2026-10-25T17:45:57Z') into a datetime."""
    if not ts_str or not isinstance(ts_str, str):
        return None
    cleaned = ts_str.strip()
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = f"{cleaned[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _has_explicit_master_key(db_path: Path) -> tuple[bool, str]:
    """Check whether an explicit master key is provided via env, systemd CREDENTIALS_DIRECTORY, or data/.aegis_master_key."""
    cred_dir = os.environ.get("CREDENTIALS_DIRECTORY", "").strip()
    if cred_dir:
        cred_dir_path = Path(cred_dir)
        if cred_dir_path.is_dir():
            for name in ("aegis_master_key", "master_key", "fernet_key"):
                candidate = cred_dir_path / name
                if candidate.is_file() and candidate.read_bytes().strip():
                    return True, f"systemd credential ({candidate})"
            for child in sorted(cred_dir_path.iterdir()):
                if child.is_file() and child.read_bytes().strip():
                    return True, f"systemd credential ({child})"

    env_aegis = os.environ.get("AEGIS_MASTER_KEY", "").strip()
    if env_aegis and not (env_aegis.startswith("<") and env_aegis.endswith(">")):
        return True, "env:AEGIS_MASTER_KEY"
    env_fernet = os.environ.get("FERNET_MASTER_KEY", "").strip()
    if env_fernet and not (env_fernet.startswith("<") and env_fernet.endswith(">")):
        return True, "env:FERNET_MASTER_KEY"

    key_file = db_path.parent / ".aegis_master_key"
    if key_file.is_file() and key_file.read_bytes().strip():
        return True, f"keyfile ({key_file.name})"

    resolved = _resolve_fernet_master_key(None, db_path)
    if resolved != _DEFAULT_HOST_FERNET_KEY:
        return True, "custom master key"
    return False, "hardcoded fallback key (_DEFAULT_HOST_FERNET_KEY)"


def evaluate_readiness(
    db_path: Path,
    *,
    min_soak_hours: float = 120.0,
    min_closed_trades: int = 3,
    error_window_hours: float = 72.0,
    run_backtest: bool = False,
    send_test_alert: bool = False,
) -> tuple[list[CheckResult], dict[str, Any]]:
    """Evaluate all 5 Mainnet Go-Live gates and return (checks, summary_metadata)."""
    checks: list[CheckResult] = []
    if not db_path.exists():
        checks.append(
            CheckResult(
                gate="Gate 1: Plumbing & Soak",
                check_id="1.0",
                name="Database File Exists",
                status="FAIL",
                observed=f"Missing ({db_path})",
                requirement="SQLite WAL database initialized at data/aegis_quant.db",
                remediation="Run `python3 -m engine.database --init` to initialize the database.",
            )
        )
        return checks, {"db_exists": False}

    db = DatabaseManager(db_path=db_path)
    cfg = db.load_config(decrypt=False)
    decrypted_cfg: dict[str, Any] = dict(cfg)
    decryption_errors: list[str] = []

    for k in sorted(SENSITIVE_CONFIG_KEYS):
        raw_v = str(cfg.get(k, "") or "").strip()
        if not raw_v:
            decrypted_cfg[k] = ""
            continue
        try:
            decrypted_cfg[k] = db.decrypt_config_value(raw_v)
        except (ValueError, RuntimeError):
            decrypted_cfg[k] = ""
            decryption_errors.append(k)

    now_utc = datetime.now(timezone.utc)

    # =========================================================================
    # GATE 1: Testnet Soak & Execution Lifecycle Proof
    # =========================================================================
    with db.connection() as conn:
        snap_row = conn.execute(
            """
            SELECT
                COUNT(*) AS cnt,
                MIN(created_at) AS min_created,
                MAX(created_at) AS max_created,
                MIN(total_equity_usdc) AS min_eq,
                MAX(total_equity_usdc) AS max_eq
            FROM portfolio_snapshots;
            """
        ).fetchone()
        snap_count = int(snap_row["cnt"] or 0) if snap_row else 0
        min_created = str(snap_row["min_created"] or "") if snap_row else ""
        max_created = str(snap_row["max_created"] or "") if snap_row else ""

        regime_rows = conn.execute(
            "SELECT hmm_regime, COUNT(*) AS cnt FROM portfolio_snapshots GROUP BY hmm_regime;"
        ).fetchall()
        regime_counts = {str(r["hmm_regime"]): int(r["cnt"]) for r in regime_rows}

        basis_snaps_row = conn.execute(
            "SELECT COUNT(*) AS cnt, MAX(basis_harvest_notional_usdc) AS max_basis FROM portfolio_snapshots WHERE basis_harvest_notional_usdc > 0;"
        ).fetchone()
        basis_snap_count = int(basis_snaps_row["cnt"] or 0) if basis_snaps_row else 0
        max_basis_usdc = float(basis_snaps_row["max_basis"] or 0.0) if basis_snaps_row else 0.0

        trades_rows = conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM trades GROUP BY status;"
        ).fetchall()
        trade_counts = {str(r["status"]): int(r["cnt"]) for r in trades_rows}
        closed_trades = trade_counts.get("CLOSED", 0)
        open_trades = trade_counts.get("OPEN", 0)

        audit_row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_transitions,
                COUNT(DISTINCT client_order_id) AS distinct_orders,
                SUM(CASE WHEN to_state = 'FILLED' THEN 1 ELSE 0 END) AS filled_count
            FROM order_state_audit;
            """
        ).fetchone()
        distinct_orders = int(audit_row["distinct_orders"] or 0) if audit_row else 0
        filled_orders = int(audit_row["filled_count"] or 0) if audit_row else 0

        cutoff_epoch = int(now_utc.timestamp() - error_window_hours * 3600)
        cutoff_iso = datetime.fromtimestamp(cutoff_epoch, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        err_rows = conn.execute(
            """
            SELECT level, component, message, created_at
            FROM telemetry_logs
            WHERE level IN ('ERROR', 'CRITICAL')
              AND created_at >= ?
            ORDER BY log_id DESC;
            """,
            (cutoff_iso,),
        ).fetchall()

    # Check 1.1: Continuous Soak Duration
    dt_start = _parse_iso_utc(min_created)
    dt_end = _parse_iso_utc(max_created)
    soak_hours = (
        max(0.0, (dt_end - dt_start).total_seconds() / 3600.0)
        if (dt_start is not None and dt_end is not None)
        else 0.0
    )
    soak_status = "PASS" if soak_hours >= min_soak_hours else "FAIL"
    checks.append(
        CheckResult(
            gate="Gate 1: Plumbing & Soak",
            check_id="1.1",
            name="Continuous Testnet Soak Duration",
            status=soak_status,
            observed=f"{soak_hours:.1f}h across {snap_count:,} snapshots ({min_created or 'n/a'} -> {max_created or 'n/a'})",
            requirement=f">= {min_soak_hours:.0f}h ({min_soak_hours / 24.0:.1f} days) continuous telemetry soak",
            remediation=(
                ""
                if soak_status == "PASS"
                else f"Continue running Testnet soak for another {max(0.0, min_soak_hours - soak_hours):.1f}h before cutting over to Mainnet."
            ),
        )
    )

    # Check 1.2: Order FSM & Round-Trip Trade Lifecycle Proof
    lifecycle_ok = closed_trades >= min_closed_trades and filled_orders >= min_closed_trades
    checks.append(
        CheckResult(
            gate="Gate 1: Plumbing & Soak",
            check_id="1.2",
            name="Order FSM & Round-Trip Trade Lifecycles",
            status="PASS" if lifecycle_ok else "FAIL",
            observed=(
                f"{closed_trades} closed trades, {open_trades} open trades, "
                f"{filled_orders} FILLED FSM orders ({distinct_orders} total tracked)"
            ),
            requirement=f">= {min_closed_trades} closed round-trip trades & >= {min_closed_trades} FILLED FSM orders",
            remediation=(
                ""
                if lifecycle_ok
                else "Wait for the HMM regime to enter BULL/BEAR on Testnet so `MAKER_LIMIT_CHASER` routes and closes at least "
                f"{min_closed_trades} round-trip trades in `trades` and `order_state_audit`."
            ),
        )
    )

    # Check 1.3: HMM Regime Transition & HLP Basis Coverage
    has_directional_regime = ("BULL" in regime_counts) or ("BEAR" in regime_counts)
    has_chop_regime = "CHOP" in regime_counts
    regime_summary = ", ".join(f"{k}:{v}" for k, v in sorted(regime_counts.items())) or "none"
    regime_status = "PASS" if (has_directional_regime and has_chop_regime) else "WARN"
    checks.append(
        CheckResult(
            gate="Gate 1: Plumbing & Soak",
            check_id="1.3",
            name="Regime Transition & HLP Vault Harvest Coverage",
            status=regime_status,
            observed=f"Regimes=[{regime_summary}], HLP/Basis active in {basis_snap_count} snapshots (max ${max_basis_usdc:,.2f})",
            requirement="Both CHOP (basis harvest) and BULL/BEAR (directional) regimes observed in soak",
            remediation=(
                ""
                if regime_status == "PASS"
                else "Observe at least one live CHOP <-> BULL/BEAR transition to verify HLP vault recall and margin sizing."
            ),
        )
    )

    # Check 1.4: Trailing Error/Critical Telemetry Cleanliness
    exec_orch_errors = [
        r
        for r in err_rows
        if str(r["component"]) in ("engine.execution", "engine.orchestrator", "database")
    ]
    reconcile_errors = [
        r for r in exec_orch_errors if "RECONCILIATION MISMATCH" in str(r["message"])
    ]
    latest_err_msg = (
        f" (latest: [{exec_orch_errors[0]['created_at']}] {exec_orch_errors[0]['component']}: "
        f"{str(exec_orch_errors[0]['message'])[:75]})"
        if exec_orch_errors
        else ""
    )
    err_status = "PASS" if len(exec_orch_errors) == 0 else "FAIL"
    checks.append(
        CheckResult(
            gate="Gate 1: Plumbing & Soak",
            check_id="1.4",
            name=f"Zero Execution/DB Errors (Last {error_window_hours:.0f}h)",
            status=err_status,
            observed=(
                f"{len(exec_orch_errors)} ERROR/CRITICAL logs ({len(reconcile_errors)} drift mismatches)"
                f"{latest_err_msg}"
            ),
            requirement=f"0 ERROR/CRITICAL logs in engine.execution, engine.orchestrator, or database over {error_window_hours:.0f}h",
            remediation=(
                ""
                if err_status == "PASS"
                else "Resolve the root cause of recent ERROR/CRITICAL logs in `telemetry_logs` and verify a clean 72h window."
            ),
        )
    )

    # =========================================================================
    # GATE 2: Security & Key Custody Hardening
    # =========================================================================
    # Check 2.1: Schema Version & DB Integrity (via DatabaseManager.verify_integrity)
    integrity = db.verify_integrity()
    schema_ver = int(integrity.get("schema_version", 0))
    journal_mode = str(integrity.get("journal_mode", "")).lower()
    cfg_count = int(integrity.get("system_config_count", 0))
    univ_count = int(integrity.get("universe_assets_total", 0))

    schema_ok = (
        schema_ver >= 4
        and journal_mode in ("wal", "memory")
        and cfg_count >= 100
        and univ_count >= 31
    )
    checks.append(
        CheckResult(
            gate="Gate 2: Security & Custody",
            check_id="2.1",
            name="Schema Version & SQLite WAL Integrity",
            status="PASS" if schema_ok else "FAIL",
            observed=(
                f"schema_version={schema_ver}, journal_mode={journal_mode}, "
                f"config_keys={cfg_count}, universe_assets={univ_count}"
            ),
            requirement="schema_version >= 4, journal_mode = wal, >= 100 config keys, 31 universe rows",
            remediation=(
                ""
                if schema_ok
                else "Apply migrations 001..004 via `python3 -m engine.database --apply-migration sql/migrations/004_v020_rc2_remediation.sql`."
            ),
        )
    )

    # Check 2.2: Mandatory Fernet Master Key Enforcement
    enforce_key = bool(cfg.get("enforce_master_key", False))
    has_env_key, key_source = _has_explicit_master_key(db_path)
    master_key_ok = enforce_key and has_env_key
    checks.append(
        CheckResult(
            gate="Gate 2: Security & Custody",
            check_id="2.2",
            name="Explicit Master Key & Enforcement (`enforce_master_key`)",
            status="PASS" if master_key_ok else "FAIL",
            observed=f"enforce_master_key={enforce_key}, key_source={key_source}",
            requirement="enforce_master_key=True AND explicit AEGIS_MASTER_KEY or systemd credential present",
            remediation=(
                ""
                if master_key_ok
                else "Export a 32-byte base64 key in `AEGIS_MASTER_KEY` (or systemd `LoadCredentialEncrypted=aegis_master_key`), "
                "rotate secrets via `DatabaseManager.rotate_master_key()`, and set `enforce_master_key = 'true'` in `system_config`."
            ),
        )
    )

    # Check 2.3: Sensitive Secrets Encrypted at Rest
    unencrypted_keys: list[str] = []
    encrypted_keys: list[str] = []
    for k in sorted(SENSITIVE_CONFIG_KEYS):
        raw_val = db.get_raw_config_value(k)
        if not raw_val or not raw_val.strip():
            continue
        if db.is_encrypted_value(raw_val):
            encrypted_keys.append(k)
        else:
            unencrypted_keys.append(k)

    enc_ok = len(unencrypted_keys) == 0 and len(decryption_errors) == 0 and len(encrypted_keys) > 0
    checks.append(
        CheckResult(
            gate="Gate 2: Security & Custody",
            check_id="2.3",
            name="Fernet AES-128 Encryption at Rest",
            status="PASS" if enc_ok else "FAIL",
            observed=(
                f"encrypted={encrypted_keys}, plaintext={unencrypted_keys}, "
                f"decrypt_error_keys={decryption_errors}"
            ),
            requirement="All non-empty sensitive keys stored as `gAAAAAB...` Fernet tokens & decrypt cleanly",
            remediation=(
                ""
                if enc_ok
                else f"Re-save or clear stale encrypted key(s) {decryption_errors + unencrypted_keys} under the active master key."
            ),
        )
    )

    # Check 2.4: Hyperliquid Wallet & Agent Private Key Validity + HLP Vault Nuance
    wallet_addr = str(decrypted_cfg.get("wallet_address", "") or "").strip()
    priv_key = str(decrypted_cfg.get("private_key", "") or "").strip()
    basis_enabled = bool(cfg.get("basis_harvest_enabled", True))
    creds_valid = False
    cred_msg = ""
    try:
        norm_wallet = validate_evm_address(wallet_addr, "wallet_address")
        norm_pk = validate_private_key(priv_key)
        creds_valid = bool(norm_wallet and norm_pk)
        cred_msg = f"wallet={_mask_secret(norm_wallet)}, agent_key={_mask_secret(norm_pk)}"
    except ValueError as exc:
        cred_msg = f"Invalid credentials: {exc}"

    checks.append(
        CheckResult(
            gate="Gate 2: Security & Custody",
            check_id="2.4",
            name="Hyperliquid L1 Wallet & Agent Key Validity",
            status="PASS" if creds_valid else "FAIL",
            observed=f"{cred_msg} (basis_harvest_enabled={basis_enabled})",
            requirement="Valid 42-char 0x wallet_address and 64-hex secp256k1 private_key",
            remediation=(
                ""
                if creds_valid
                else "Configure valid `wallet_address` and Trade-Only Agent `private_key` via Web UI (:8000) or CLI."
            ),
        )
    )

    # =========================================================================
    # GATE 3: Data Provenance & Statistical Backtest Parity
    # =========================================================================
    # Check 3.1: Backfill SHA-256 Provenance
    backfill_path = PROJECT_ROOT / "data" / "real_4h_backfill.json"
    sha_path = PROJECT_ROOT / "data" / "real_4h_backfill.json.sha256"
    try:
        provenance_ok = bool(
            backfill_path.exists()
            and sha_path.exists()
            and verify_data_provenance(str(backfill_path))
        )
        prov_msg = "SHA-256 verified against data/real_4h_backfill.json.sha256"
    except Exception as exc:
        provenance_ok = False
        prov_msg = f"Verification failed: {exc}"

    checks.append(
        CheckResult(
            gate="Gate 3: Provenance & Parity",
            check_id="3.1",
            name="Historical 4H Backfill SHA-256 Provenance",
            status="PASS" if provenance_ok else "FAIL",
            observed=prov_msg,
            requirement="data/real_4h_backfill.json matches .sha256 checksum",
            remediation=(
                ""
                if provenance_ok
                else "Run `python3 scripts/refresh_backfill.py` to refresh and sign the 4H dataset."
            ),
        )
    )

    # Check 3.2: Model Lifecycle Parity & Realistic Funding Config
    refit_bars = int(cfg.get("mace_refit_interval_bars", 0))
    hlp_floor_bt = bool(cfg.get("enable_hlp_floor_backtest", True))
    parity_cfg_ok = refit_bars == 36 and not hlp_floor_bt
    checks.append(
        CheckResult(
            gate="Gate 3: Provenance & Parity",
            check_id="3.2",
            name="MACE 36-Bar Refit & Zero Artificial Backtest Floor",
            status="PASS" if parity_cfg_ok else "FAIL",
            observed=f"mace_refit_interval_bars={refit_bars}, enable_hlp_floor_backtest={hlp_floor_bt}",
            requirement="mace_refit_interval_bars == 36 and enable_hlp_floor_backtest == False",
            remediation=(
                ""
                if parity_cfg_ok
                else "Set `mace_refit_interval_bars=36` and `enable_hlp_floor_backtest=false` in `system_config`."
            ),
        )
    )

    # Check 3.3: Optional Live Acceptance Backtest Replay
    if run_backtest:
        bt = HistoricalBacktester(
            data_path=str(backfill_path),
            initial_equity=10000.0,
        )
        try:
            sheet = bt.run(start_date="2026-07-20", end_date="2026-10-20")
            bt_ok = (
                sheet.sharpe >= 1.50
                and sheet.max_drawdown <= 0.20
                and sheet.profit_factor >= 1.65
            )
            checks.append(
                CheckResult(
                    gate="Gate 3: Provenance & Parity",
                    check_id="3.3",
                    name="90-Day Out-of-Sample Acceptance Backtest",
                    status="PASS" if bt_ok else "FAIL",
                    observed=(
                        f"Sharpe={sheet.sharpe:.2f}, MaxDD={sheet.max_drawdown * 100:.2f}%, "
                        f"PF={sheet.profit_factor:.2f}, NetReturn={sheet.total_return * 100:+.2f}%"
                    ),
                    requirement="Sharpe >= 1.50, MaxDD <= 20.0%, Profit Factor >= 1.65",
                    remediation=(
                        ""
                        if bt_ok
                        else "Inspect signal/risk parameter changes that degraded 90-day acceptance metrics."
                    ),
                )
            )
        finally:
            bt.mem_db.close()

    # =========================================================================
    # GATE 4: Alerting & Risk Circuit Breakers
    # =========================================================================
    # Check 4.1: Webhook Alerting Channel & Exponential Backoff Queue
    tg_token = str(decrypted_cfg.get("telegram_bot_token", "") or "").strip()
    tg_chat = str(cfg.get("telegram_chat_id", "") or "").strip()
    discord_url = str(decrypted_cfg.get("discord_webhook_url", "") or "").strip()
    max_retries = int(cfg.get("alert_max_retries", 0))
    has_tg = bool(tg_token and tg_chat)
    has_discord = bool(discord_url)
    alert_ok = (has_tg or has_discord) and max_retries >= 3

    channels: list[str] = []
    if has_tg:
        channels.append(f"Telegram(chat_id={tg_chat})")
    if has_discord:
        channels.append("Discord(webhook)")

    if send_test_alert and alert_ok:
        from engine.alerts import AlertEngine

        alert_engine = AlertEngine(db)
        dispatched = asyncio.run(
            alert_engine.send_test_alert(
                "Mainnet Readiness Check: Webhook verification ping"
            )
        )
        alert_obs = f"channels={channels or ['none']}, retries={max_retries}, live_ping={'OK' if dispatched else 'FAILED'}"
        alert_ok = alert_ok and bool(dispatched)
    else:
        alert_obs = f"channels={channels or ['none']}, alert_max_retries={max_retries}"

    checks.append(
        CheckResult(
            gate="Gate 4: Alerting & Risk Caps",
            check_id="4.1",
            name="Real-Time Webhook Alerting & Retry Queue",
            status="PASS" if alert_ok else "FAIL",
            observed=alert_obs,
            requirement="Telegram or Discord webhook configured & alert_max_retries >= 3",
            remediation=(
                ""
                if alert_ok
                else "Configure Telegram/Discord credentials in Web UI (:8000) and test with `python3 -m engine.alerts --test-alert 'Ping'`."
            ),
        )
    )

    # Check 4.2: Proportional Risk & Leverage Guardrails
    init_eq = max(1.0, float(cfg.get("initial_equity_usdc", 10000.0)))
    base_risk_usdc = float(cfg.get("baseline_risk_per_trade_usdc", 100.0))
    base_risk_pct = base_risk_usdc / init_eq
    heat_pct = float(cfg.get("max_portfolio_heat_pct", 0.04))
    gross_lev = float(cfg.get("max_gross_leverage", 1.5))
    dd_alert_pct = float(cfg.get("max_daily_drawdown_alert_pct", 0.03))
    single_asset_pct = float(cfg.get("max_single_asset_pct", 0.25))

    risk_bounds_ok = (
        0.002 <= base_risk_pct <= 0.025
        and 0.01 <= heat_pct <= 0.08
        and 0.5 <= gross_lev <= 3.0
        and 0.01 <= dd_alert_pct <= 0.05
        and 0.05 <= single_asset_pct <= 0.35
    )
    checks.append(
        CheckResult(
            gate="Gate 4: Alerting & Risk Caps",
            check_id="4.2",
            name="Portfolio Risk & Leverage Circuit Breakers",
            status="PASS" if risk_bounds_ok else "FAIL",
            observed=(
                f"equity=${init_eq:,.2f}, risk/trade=${base_risk_usdc:.2f} ({base_risk_pct * 100:.2f}%), "
                f"max_heat={heat_pct * 100:.1f}%, max_lev={gross_lev:.2f}x, daily_dd_alert={dd_alert_pct * 100:.1f}%"
            ),
            requirement="risk/trade <= 2.5%, max_heat <= 8.0%, max_lev <= 3.0x, daily_dd_alert <= 5.0%",
            remediation=(
                ""
                if risk_bounds_ok
                else "Run `Sync Wallet Equity` in Web UI with proportional risk scaling enabled or tighten risk caps in `system_config`."
            ),
        )
    )

    # =========================================================================
    # GATE 5: Mainnet Cutover Target & Canary Sizing
    # =========================================================================
    exec_mode = str(cfg.get("execution_mode", "PAPER")).strip().upper()
    raw_testnet = cfg.get("hyperliquid_testnet", True)
    is_testnet = (
        raw_testnet.strip().lower() in ("true", "1", "yes", "on")
        if isinstance(raw_testnet, str)
        else bool(raw_testnet)
    )
    blocking_failures = [c for c in checks if c.status == "FAIL"]

    if not is_testnet and exec_mode == "LIVE":
        cutover_status = "PASS" if not blocking_failures else "FAIL"
        cutover_obs = f"ACTIVE ON MAINNET (execution_mode={exec_mode}, hyperliquid_testnet=False, equity=${init_eq:,.2f})"
    else:
        cutover_status = "PASS" if not blocking_failures else "WARN"
        cutover_obs = (
            f"Currently on {'TESTNET' if is_testnet else exec_mode} "
            f"(execution_mode={exec_mode}, hyperliquid_testnet={is_testnet}, equity=${init_eq:,.2f})"
        )

    checks.append(
        CheckResult(
            gate="Gate 5: Cutover & Canary",
            check_id="5.1",
            name="Mainnet Cutover Readiness & Routing Mode",
            status=cutover_status,
            observed=cutover_obs,
            requirement="All Gates 1..4 PASS before flipping hyperliquid_testnet=false with Stage 1 Canary ($250-$1,000 USDC)",
            remediation=(
                "Ready for Stage 1 Mainnet Canary cutover (`hyperliquid_testnet=false`)."
                if not blocking_failures
                else f"Resolve {len(blocking_failures)} blocking [FAIL] check(s) above before disabling `hyperliquid_testnet`."
            ),
        )
    )

    summary = {
        "timestamp_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_path": str(db_path),
        "execution_mode": exec_mode,
        "hyperliquid_testnet": is_testnet,
        "initial_equity_usdc": init_eq,
        "soak_hours": round(soak_hours, 2),
        "closed_trades": closed_trades,
        "pass_count": sum(1 for c in checks if c.status == "PASS"),
        "warn_count": sum(1 for c in checks if c.status == "WARN"),
        "fail_count": sum(1 for c in checks if c.status == "FAIL"),
        "mainnet_ready": len(blocking_failures) == 0,
    }
    return checks, summary


def _print_checklist(checks: list[CheckResult], summary: dict[str, Any]) -> None:
    """Render a formatted terminal table and remediation plan."""
    badge_map = {
        "PASS": "✅ [PASS]",
        "WARN": "⚠️  [WARN]",
        "FAIL": "❌ [FAIL]",
    }

    print("=" * 100)
    print("  AEGIS QUANT (v0.2.0-rc2) — MAINNET GO-LIVE READINESS CHECKLIST")
    print(
        f"  Database: {summary.get('db_path')} | Evaluated: {summary.get('timestamp_utc')}"
    )
    print(
        f"  Active Mode: execution_mode={summary.get('execution_mode')} | "
        f"hyperliquid_testnet={summary.get('hyperliquid_testnet')} | "
        f"initial_equity=${summary.get('initial_equity_usdc', 0.0):,.2f} USDC"
    )
    print("=" * 100)

    current_gate = ""
    for chk in checks:
        if chk.gate != current_gate:
            current_gate = chk.gate
            print(f"\n--- {current_gate} " + "-" * max(1, 94 - len(current_gate)))
        badge = badge_map.get(chk.status, f"[{chk.status}]")
        print(f"  {badge} {chk.check_id} {chk.name}")
        print(f"             Observed   : {chk.observed}")
        print(f"             Requirement: {chk.requirement}")

    print("\n" + "=" * 100)
    pass_cnt = summary.get("pass_count", 0)
    warn_cnt = summary.get("warn_count", 0)
    fail_cnt = summary.get("fail_count", 0)
    ready = bool(summary.get("mainnet_ready", False))

    if ready:
        print(
            f"  OVERALL VERDICT: ✅ GO FOR MAINNET CANARY ({pass_cnt} PASS, {warn_cnt} WARN, 0 FAIL)"
        )
    else:
        print(
            f"  OVERALL VERDICT: ❌ NO-GO — STAY ON TESTNET ({pass_cnt} PASS, {warn_cnt} WARN, {fail_cnt} FAIL)"
        )
    print("=" * 100)

    action_items = [c for c in checks if c.status in ("FAIL", "WARN") and c.remediation]
    if action_items:
        print("\nRequired Remediation Steps Before Mainnet Cutover:")
        for idx, item in enumerate(action_items, start=1):
            print(f"  {idx}. [{item.check_id} {item.name}] ({item.status})")
            print(f"     -> {item.remediation}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate Aegis Quant Mainnet Go-Live Readiness Gates against data/aegis_quant.db"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help="Path to the SQLite WAL database (default: data/aegis_quant.db)",
    )
    parser.add_argument(
        "--min-soak-hours",
        type=float,
        default=120.0,
        help="Minimum continuous Testnet soak hours required (default: 120.0 = 5 days)",
    )
    parser.add_argument(
        "--min-closed-trades",
        type=int,
        default=3,
        help="Minimum closed round-trip trades required on Testnet (default: 3)",
    )
    parser.add_argument(
        "--error-window-hours",
        type=float,
        default=72.0,
        help="Trailing error-free window in hours required in telemetry_logs (default: 72.0)",
    )
    parser.add_argument(
        "--with-backtest",
        action="store_true",
        help="Also run the 90-day acceptance backtest (2026-07-20 -> 2026-10-20) to verify Sharpe & Drawdown",
    )
    parser.add_argument(
        "--send-test-alert",
        action="store_true",
        help="Dispatch a live Telegram/Discord webhook test alert as part of Gate 4.1",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output readiness report as machine-readable JSON",
    )
    args = parser.parse_args()

    checks, summary = evaluate_readiness(
        Path(args.db_path),
        min_soak_hours=float(args.min_soak_hours),
        min_closed_trades=int(args.min_closed_trades),
        error_window_hours=float(args.error_window_hours),
        run_backtest=bool(args.with_backtest),
        send_test_alert=bool(args.send_test_alert),
    )

    if args.json:
        print(
            json.dumps(
                {
                    "summary": summary,
                    "checks": [asdict(c) for c in checks],
                },
                indent=2,
            )
        )
    else:
        _print_checklist(checks, summary)

    sys.exit(0 if summary.get("mainnet_ready", False) else 1)


if __name__ == "__main__":
    main()
