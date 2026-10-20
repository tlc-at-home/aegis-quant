"""FastAPI REST + Server-Sent Events (SSE) telemetry server for Aegis Quant (Port 8000)."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any, AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

from engine import __platform__, __version__
from engine.database import DatabaseManager

STATIC_DIR = Path(__file__).resolve().parent / "static"
INDEX_HTML_PATH = STATIC_DIR / "index.html"


class ConfigUpdatePayload(BaseModel):
    """Payload for updating a configuration variable in SQLite system_config."""

    config_key: str
    config_value: Any


class CredentialsUpdatePayload(BaseModel):
    """Payload for configuring Hyperliquid Mainnet/Testnet Web3 DEX credentials."""

    wallet_address: str | None = None
    private_key: str | None = None
    hyperliquid_testnet: bool | None = None
    execution_mode: str | None = None
    hyperliquid_vault_address: str | None = None
    gemini_api_key: str | None = None
    require_credentials_for_live: bool = True


def create_app(db: DatabaseManager | None = None) -> FastAPI:
    """Create and configure the FastAPI application instance."""
    db_manager = db or DatabaseManager()
    db_manager.init_db()

    app = FastAPI(
        title="Aegis Quant Telemetry & Control API",
        version=f"v{__version__}",
        description="Autonomous Self-Custodial Web3 DEX Quantitative Crypto Trading Platform",
    )
    app.state.db = db_manager

    @app.get("/", response_class=HTMLResponse)
    async def serve_dashboard() -> HTMLResponse:
        if INDEX_HTML_PATH.exists():
            return HTMLResponse(content=INDEX_HTML_PATH.read_text(encoding="utf-8"))
        return HTMLResponse(content="<h1>Aegis Quant v0.1.0-dev Dashboard</h1>")

    @app.get("/api/v1/status")
    async def get_status() -> dict[str, Any]:
        integrity = db_manager.verify_integrity()
        cfg = db_manager.load_config()
        cred_status = db_manager.get_credentials_status(cfg)
        snap = db_manager.get_latest_portfolio_snapshot()
        return {
            "status": "active",
            "state": "active",
            "platform": __platform__,
            "version": f"v{__version__}",
            "release": __version__,
            "exchange": cfg.get("exchange_id", "hyperliquid"),
            "execution_mode": cfg.get("execution_mode", "PAPER"),
            "hyperliquid_testnet": cred_status["hyperliquid_testnet"],
            "network": cred_status["network"],
            "api_endpoint": cred_status["api_endpoint"],
            "credentials_ready": cred_status["credentials_ready"],
            "active_routing": cred_status["active_routing"],
            "environment": cfg.get("environment", "sandbox"),
            "polling_interval_seconds": int(cfg.get("polling_interval_seconds", 15)),
            "journal_mode": integrity["journal_mode"],
            "system_config_count": integrity["system_config_count"],
            "universe_counts": {
                "v_universe_usdc_perp": integrity["v_universe_usdc_perp_count"],
                "v_universe_btc_synth": integrity["v_universe_btc_synth_count"],
                "v_universe_alt_pairs": integrity["v_universe_alt_pairs_count"],
                "total_assets": integrity["universe_assets_total"],
            },
            "hmm_regime": snap.get("hmm_regime", "CHOP"),
            "initial_equity_usdc": float(cfg.get("initial_equity_usdc", 10000.0)),
            "timestamp_ms": int(time.time() * 1000),
        }

    @app.get("/api/v1/portfolio")
    async def get_portfolio() -> dict[str, Any]:
        cfg = db_manager.load_config()
        snap = db_manager.get_latest_portfolio_snapshot()
        open_trades = db_manager.get_open_trades()
        initial_eq = float(cfg.get("initial_equity_usdc", 10000.0))
        total_eq = float(snap.get("total_equity_usdc", initial_eq))
        return {
            "status": "active",
            "version": f"v{__version__}",
            "base_currency": cfg.get("base_currency", "USDC"),
            "initial_equity_usdc": initial_eq,
            "total_equity_usdc": total_eq,
            "equity_usdc": total_eq,
            "available_cash_usdc": float(snap.get("available_cash_usdc", initial_eq)),
            "deployed_notional_usdc": float(snap.get("deployed_notional_usdc", 0.0)),
            "gross_leverage": float(snap.get("gross_leverage", 0.0)),
            "max_gross_leverage": float(cfg.get("max_gross_leverage", 1.5)),
            "portfolio_heat_usdc": float(snap.get("portfolio_heat_usdc", 0.0)),
            "max_portfolio_heat_usdc": float(cfg.get("max_portfolio_heat_usdc", 400.0)),
            "portfolio_heat_pct": float(snap.get("portfolio_heat_pct", 0.0)),
            "max_portfolio_heat_pct": float(cfg.get("max_portfolio_heat_pct", 0.04)),
            "single_asset_cap_usdc": float(cfg.get("max_single_asset_notional_usdc", 2500.0)),
            "basis_harvest_notional_usdc": float(snap.get("basis_harvest_notional_usdc", 0.0)),
            "accrued_yield_usdc": float(snap.get("accrued_yield_usdc", 0.0)),
            "hmm_regime": snap.get("hmm_regime", "CHOP"),
            "regime_probabilities": {
                "bull": float(snap.get("prob_bull", 0.3333)),
                "bear": float(snap.get("prob_bear", 0.3333)),
                "chop": float(snap.get("prob_chop", 0.3334)),
            },
            "open_long_count": int(snap.get("open_long_count", 0)),
            "open_short_count": int(snap.get("open_short_count", 0)),
            "open_positions": open_trades,
            "polling_interval_seconds": int(cfg.get("polling_interval_seconds", 15)),
            "timestamp_ms": int(snap.get("timestamp_ms", int(time.time() * 1000))),
        }

    @app.get("/api/v1/universe")
    async def get_universe() -> dict[str, Any]:
        usdc_perp = db_manager.get_usdc_perp_universe()
        btc_synth = db_manager.get_btc_synth_universe()
        alt_pairs = db_manager.get_alt_pairs_universe()
        return {
            "usdc_perp_count": len(usdc_perp),
            "btc_synth_count": len(btc_synth),
            "alt_pairs_count": len(alt_pairs),
            "usdc_perp": usdc_perp,
            "btc_synth": btc_synth,
            "alt_pairs": alt_pairs,
        }

    @app.get("/api/v1/signals")
    async def get_signals() -> dict[str, Any]:
        signals = db_manager.get_latest_signals(limit=15)
        snap = db_manager.get_latest_portfolio_snapshot()
        return {
            "hmm_regime": snap.get("hmm_regime", "CHOP"),
            "prob_bull": float(snap.get("prob_bull", 0.3333)),
            "prob_bear": float(snap.get("prob_bear", 0.3333)),
            "prob_chop": float(snap.get("prob_chop", 0.3334)),
            "count": len(signals),
            "signals": signals,
        }

    @app.get("/api/v1/trades")
    async def get_trades() -> dict[str, Any]:
        open_trades = db_manager.get_open_trades()
        recent_trades = db_manager.get_recent_trades(limit=50)
        return {
            "open_count": len(open_trades),
            "open_trades": open_trades,
            "recent_trades": recent_trades,
        }

    @app.get("/api/v1/config")
    async def get_config() -> dict[str, Any]:
        rows = db_manager.get_config_rows(mask_secrets=True)
        return {"count": len(rows), "config": rows}

    @app.post("/api/v1/config")
    async def update_config(payload: ConfigUpdatePayload) -> dict[str, Any]:
        try:
            updated = db_manager.set_config_value(payload.config_key, payload.config_value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not updated:
            raise HTTPException(
                status_code=404, detail=f"Unknown config_key: {payload.config_key}"
            )
        db_manager.log_telemetry(
            "INFO",
            "web.config",
            f"Updated system_config key '{payload.config_key}'",
        )
        return {"status": "updated", "config_key": payload.config_key}

    @app.get("/api/v1/credentials")
    async def get_credentials() -> dict[str, Any]:
        return db_manager.get_credentials_status()

    @app.post("/api/v1/credentials")
    async def update_credentials(payload: CredentialsUpdatePayload) -> dict[str, Any]:
        try:
            status = db_manager.configure_hyperliquid_credentials(
                wallet_address=payload.wallet_address,
                private_key=payload.private_key,
                hyperliquid_testnet=payload.hyperliquid_testnet,
                execution_mode=payload.execution_mode,
                hyperliquid_vault_address=payload.hyperliquid_vault_address,
                gemini_api_key=payload.gemini_api_key,
                require_credentials_for_live=payload.require_credentials_for_live,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        db_manager.log_telemetry(
            "INFO",
            "web.credentials",
            f"Configured Hyperliquid credentials | routing={status['active_routing']} | network={status['network']}",
            {
                "execution_mode": status["execution_mode"],
                "network": status["network"],
                "wallet_address": status["wallet_address"],
                "private_key_masked": status["private_key_masked"],
                "credentials_ready": status["credentials_ready"],
                "active_routing": status["active_routing"],
            },
        )
        return {"status": "updated", "credentials": status}

    @app.get("/api/v1/logs")
    async def get_logs() -> dict[str, Any]:
        logs = db_manager.get_recent_logs(limit=40)
        return {"count": len(logs), "logs": logs}

    @app.get("/api/v1/stream")
    async def telemetry_stream() -> StreamingResponse:
        async def _event_generator() -> AsyncGenerator[str, None]:
            while True:
                cfg = db_manager.load_config()
                snap = db_manager.get_latest_portfolio_snapshot()
                signals = db_manager.get_latest_signals(limit=15)
                open_trades = db_manager.get_open_trades()
                payload = {
                    "timestamp_ms": int(time.time() * 1000),
                    "version": f"v{__version__}",
                    "polling_interval_seconds": int(cfg.get("polling_interval_seconds", 15)),
                    "portfolio": snap,
                    "signals": signals,
                    "open_trades": open_trades,
                }
                yield f"data: {json.dumps(payload)}\n\n"
                await asyncio.sleep(3.0)

        return StreamingResponse(_event_generator(), media_type="text/event-stream")

    return app


_default_app: FastAPI | None = None


def __getattr__(name: str) -> Any:
    global _default_app
    if name == "app":
        if _default_app is None:
            _default_app = create_app()
        return _default_app
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
