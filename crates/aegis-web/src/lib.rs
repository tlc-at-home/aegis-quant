//! Institutional FastAPI-Compatible REST & SSE Server (`aegis-web`).
//!
//! 1:1 port of `web/server.py` extended for PP-50 Two-Book Platform (`v0.3.0-rc1`) built on `axum` and `tokio`:
//! - Serves `web/static/index.html` on `GET /` (4-Page Dashboard: Overall Status, Pit, Pond, Settings)
//! - Exposes all `/api/v1/*` JSON endpoints (including `/api/v1/pp50/*`, `/api/v1/pond/*`, `/api/v1/settle/*`)
//!   and real-time Server-Sent Events (`GET /api/v1/stream`)
//! - Zero-overhead shared `DatabaseManager` state with non-blocking `spawn_blocking` execution

use aegis_alerts::AlertEngine;
use aegis_backtest::BacktestEngine;
use aegis_core::{
    compute_drift_bps, is_immutable_security_config_key, is_sensitive_config_key,
    resolve_project_root, round_dp, AegisError, PLATFORM_NAME, PLATFORM_VERSION,
};
use aegis_db::DatabaseManager;
use aegis_execution::{ExecutionEngine, MarketIngestionEngine};
use aegis_pond::PondOrchestrator;
use aegis_settle::{SettleController, SettleOverrideInput};
use aegis_signals::{SignalEngine, WalkForwardCalibrator};
use axum::{
    extract::{Query, Request, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;
use tower_http::cors::{AllowOrigin, CorsLayer};

pub const ENGINE_RUNTIME_LABEL: &str = "RUST_NATIVE";
pub const ENGINE_RUNTIME_DETAIL: &str = "RUST_NATIVE (Axum / Tokio)";
pub const DEFAULT_CSP_POLICY: &str = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com data:; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'";

#[derive(Clone)]
pub struct WebAppState {
    pub db: DatabaseManager,
    pub static_dir: PathBuf,
    pub session_token: String,
    pub sse_semaphore: Arc<tokio::sync::Semaphore>,
}

fn generate_session_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let mut out = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    if ab.is_empty() || ab.len() != bb.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in ab.iter().zip(bb.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl WebAppState {
    pub fn new(db: DatabaseManager) -> Self {
        let static_dir = resolve_project_root().join("web").join("static");
        Self::with_static_dir(db, static_dir)
    }

    pub fn with_static_dir(db: DatabaseManager, static_dir: PathBuf) -> Self {
        let cfg = db.load_config().unwrap_or_default();
        let max_sse = cfg.get_i64("web_max_sse_clients", 32).clamp(1, 512) as usize;
        let session_token = std::env::var("AEGIS_WEB_TOKEN")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                let tok = cfg.get_str("web_api_token", "");
                let trimmed = tok.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            })
            .unwrap_or_else(|| {
                let generated = generate_session_token();
                let _ = db.set_config_value("web_api_token", &json!(generated));
                generated
            });
        Self {
            db,
            static_dir,
            session_token,
            sse_semaphore: Arc::new(tokio::sync::Semaphore::new(max_sse)),
        }
    }

    pub fn session_token(&self) -> &str {
        &self.session_token
    }
}

#[derive(Debug, Deserialize)]
pub struct ConfigUpdatePayload {
    pub config_key: String,
    pub config_value: Value,
    #[serde(default = "default_true")]
    pub scale_risk_caps: bool,
}

#[derive(Debug, Deserialize)]
pub struct ConfigBatchUpdatePayload {
    pub updates: HashMap<String, Value>,
    #[serde(default = "default_true")]
    pub scale_risk_caps: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Default)]
pub struct CredentialsUpdatePayload {
    pub wallet_address: Option<String>,
    pub private_key: Option<String>,
    pub hyperliquid_testnet: Option<bool>,
    pub execution_mode: Option<String>,
    pub hyperliquid_vault_address: Option<String>,
    pub gemini_api_key: Option<String>,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub max_daily_drawdown_alert_pct: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AlertsUpdatePayload {
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub max_daily_drawdown_alert_pct: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct WalletSyncPayload {
    pub scale_risk_caps: Option<bool>,
    pub update_risk_caps: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
pub struct BacktestRequestPayload {
    #[serde(default = "default_days")]
    pub days: usize,
    #[serde(default = "default_true")]
    pub use_real_history: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct SettleRequestPayload {
    pub dry_run: Option<bool>,
    pub pit_equity_usdc: Option<f64>,
    pub pond_equity_usdc: Option<f64>,
    pub rich_idle_usdc: Option<f64>,
    pub day_index: Option<u32>,
}

fn default_days() -> usize {
    365
}

#[derive(Debug, Serialize)]
struct ApiErrorBody {
    detail: String,
}

fn map_err_response(err: AegisError) -> Response {
    let status = match &err {
        AegisError::Validation(_) => StatusCode::BAD_REQUEST,
        AegisError::NotFound(_) => StatusCode::NOT_FOUND,
        AegisError::SecurityViolation(_) | AegisError::Firewall(_) => StatusCode::FORBIDDEN,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ApiErrorBody {
            detail: err.to_string(),
        }),
    )
        .into_response()
}

async fn run_db_blocking<F>(db: DatabaseManager, f: F) -> Response
where
    F: FnOnce(DatabaseManager) -> aegis_core::Result<Value> + Send + 'static,
{
    match tokio::task::spawn_blocking(move || f(db)).await {
        Ok(Ok(val)) => Json(val).into_response(),
        Ok(Err(e)) => map_err_response(e),
        Err(e) => map_err_response(AegisError::Execution(e.to_string())),
    }
}

/// Builds the enriched Book A (`PIT`) portfolio payload matching `web/server.py::get_portfolio()`
/// plus explicit realized, unrealized, funding, net book transfers, and total PnL fields.
pub fn build_portfolio_payload(db: &DatabaseManager) -> aegis_core::Result<Value> {
    let snap = db.get_latest_portfolio_snapshot()?;
    let cfg = db.load_config()?;
    let open_trades = db.get_open_trades()?;
    let realized_pnl = round_dp(db.get_total_realized_pnl().unwrap_or(0.0), 4);
    let unrealized_pnl: f64 = round_dp(
        open_trades.iter().map(|t| t.unrealized_pnl_usdc).sum(),
        4,
    );
    let funding_pnl: f64 = round_dp(
        open_trades.iter().map(|t| t.funding_accrued_usdc).sum(),
        4,
    );
    let has_baseline_reset = !cfg
        .get_str("equity_baseline_reset_at", "")
        .trim()
        .is_empty();
    let yield_for_pnl = if has_baseline_reset {
        0.0
    } else {
        snap.accrued_yield_usdc
    };
    let total_pnl = round_dp(
        realized_pnl + unrealized_pnl + funding_pnl + yield_for_pnl,
        4,
    );

    let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
    let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0);
    let pit_init = round_dp(init_eq * split_pct, 2);
    let net_pit_transfers = round_dp(db.get_net_book_transfer_adjustment("PIT").unwrap_or(0.0), 2);
    let pit_equity = round_dp((pit_init + total_pnl + net_pit_transfers).max(0.0), 2);
    let (_, perp_trading_eq, vault_eq, total_wallet_deployed) = db
        .get_wallet_perps_margin_state()
        .unwrap_or((init_eq, init_eq, 0.0, 0.0));
    let mut pit_idle = round_dp((pit_equity - snap.deployed_notional_usdc).max(0.0), 2);
    if vault_eq > 0.0 || total_wallet_deployed > snap.deployed_notional_usdc {
        let avail_perps = round_dp((perp_trading_eq - total_wallet_deployed).max(0.0), 2);
        pit_idle = pit_idle.min(avail_perps);
    }

    let mut val = serde_json::to_value(&snap).unwrap_or_else(|_| json!({}));
    if let Some(obj) = val.as_object_mut() {
        obj.insert(
            "initial_equity_usdc".to_string(),
            json!(init_eq),
        );
        obj.insert("pit_initial_capital_usdc".to_string(), json!(pit_init));
        obj.insert("net_transfers_usdc".to_string(), json!(net_pit_transfers));
        obj.insert("pit_equity_usdc".to_string(), json!(pit_equity));
        obj.insert("pit_idle_usdc".to_string(), json!(pit_idle));
        obj.insert(
            "perp_trading_equity_usdc".to_string(),
            json!(perp_trading_eq),
        );
        obj.insert("vault_equity_usdc".to_string(), json!(vault_eq));
        obj.insert("realized_pnl_usdc".to_string(), json!(realized_pnl));
        obj.insert("unrealized_pnl_usdc".to_string(), json!(unrealized_pnl));
        obj.insert("funding_accrued_usdc".to_string(), json!(funding_pnl));
        obj.insert("total_pnl_usdc".to_string(), json!(total_pnl));
        obj.insert(
            "total_pnl_pct".to_string(),
            json!(round_dp((total_pnl / pit_init.max(1.0)) * 100.0, 2)),
        );
        obj.insert(
            "max_gross_leverage".to_string(),
            json!(cfg.get_f64("max_gross_leverage", 1.5)),
        );
        obj.insert(
            "max_gross_leverage_notional_usdc".to_string(),
            json!(cfg.get_f64("max_gross_leverage_notional_usdc", 15000.0)),
        );
        obj.insert(
            "max_portfolio_heat_usdc".to_string(),
            json!(cfg.get_f64("max_portfolio_heat_usdc", 400.0)),
        );
        obj.insert(
            "max_portfolio_heat_pct".to_string(),
            json!(cfg.get_f64("max_portfolio_heat_pct", 0.04)),
        );
        obj.insert(
            "baseline_risk_per_trade_usdc".to_string(),
            json!(cfg.get_f64("baseline_risk_per_trade_usdc", 100.0)),
        );
        obj.insert(
            "min_risk_per_trade_usdc".to_string(),
            json!(cfg.get_f64("min_risk_per_trade_usdc", 50.0)),
        );
        obj.insert(
            "max_risk_per_trade_usdc".to_string(),
            json!(cfg.get_f64("max_risk_per_trade_usdc", 150.0)),
        );
        obj.insert(
            "single_asset_cap_usdc".to_string(),
            json!(cfg.get_f64("max_single_asset_notional_usdc", 2500.0)),
        );
        obj.insert(
            "regime_probabilities".to_string(),
            json!({
                "bull": snap.prob_bull,
                "bear": snap.prob_bear,
                "chop": snap.prob_chop,
            }),
        );
        obj.insert("open_positions".to_string(), json!(open_trades));
    }
    Ok(val)
}

/// Enriches `PondCell` records with live mark price, current funding Z-score, premium score,
/// and unrealized PnL for UI rendering.
fn enrich_pond_cells(
    db: &DatabaseManager,
    cells: &[aegis_core::PondCell],
    rank_map: &HashMap<String, i64>,
) -> (Vec<Value>, f64) {
    let ingestion = MarketIngestionEngine::new(db.clone());
    let mut out = Vec::with_capacity(cells.len());
    let mut open_price_upnl = 0.0;

    for c in cells {
        let rank = rank_map.get(&c.symbol).copied().unwrap_or(1).max(1) as usize;
        let (funding_1h, bars_1h, _bars_4h) =
            ingestion.load_or_fetch_pond_funding_and_bars(&c.symbol, rank, 240, None, false);
        let latest_close = bars_1h
            .last()
            .map(|b| b.close)
            .or(c.entry_price)
            .unwrap_or(0.0);
        let current_price = if c.state == "CLOSED" {
            c.exit_price.unwrap_or(latest_close)
        } else {
            latest_close
        };

        let current_funding_z = if funding_1h.len() >= 24 {
            let tail = &funding_1h[funding_1h.len().saturating_sub(168)..];
            let n = tail.len() as f64;
            let mean = tail.iter().sum::<f64>() / n;
            let var = tail.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
            let std = var.sqrt();
            let last_r = *tail.last().unwrap_or(&0.0);
            if std > 1e-12 {
                round_dp((last_r - mean) / std, 4)
            } else {
                c.funding_z_entry.unwrap_or(0.0)
            }
        } else {
            c.funding_z_entry.unwrap_or(0.0)
        };

        let premium_score = round_dp(
            c.funding_z_entry.unwrap_or(0.0).abs() * c.conviction_multiplier.unwrap_or(1.0),
            4,
        );

        let is_open = c.state == "OPEN" || c.state == "TRAILED";
        let price_pnl = if is_open {
            let entry = c.entry_price.unwrap_or(current_price);
            let qty = c.quantity.unwrap_or(0.0);
            if c.side == "SHORT" {
                (entry - current_price) * qty
            } else {
                (current_price - entry) * qty
            }
        } else {
            0.0
        };
        if is_open {
            open_price_upnl += price_pnl;
        }
        let cell_upnl = round_dp(price_pnl, 4);

        let mut val = serde_json::to_value(c).unwrap_or_else(|_| json!({}));
        if let Some(obj) = val.as_object_mut() {
            obj.insert("current_price".to_string(), json!(round_dp(current_price, 6)));
            obj.insert("current_funding_z".to_string(), json!(current_funding_z));
            obj.insert("premium_score".to_string(), json!(premium_score));
            obj.insert("unrealized_pnl_usdc".to_string(), json!(cell_upnl));
        }
        out.push(val);
    }

    (out, round_dp(open_price_upnl, 4))
}

/// Enriches `SignalEvaluation` rows with both canonical and legacy UI field aliases
/// (`symbol`, `timeframe`, `ensemble_score`, `entry_threshold`, `volatility_tier`).
fn enrich_signals(
    db: &DatabaseManager,
    signals: &[aegis_core::SignalEvaluation],
) -> Vec<Value> {
    let cfg = db.load_config().unwrap_or_default();
    let entry_threshold = cfg.get_f64("conviction_entry_threshold", 0.65);
    let alt_pairs = db.get_alt_pairs_universe().unwrap_or_default();
    let mut meta_by_exec: HashMap<String, (String, String)> = HashMap::new();
    for pair in alt_pairs {
        meta_by_exec.insert(pair.exec_symbol, (pair.exec_timeframe, pair.tier_name));
    }

    signals
        .iter()
        .map(|s| {
            let (tf, tier) = meta_by_exec
                .get(&s.exec_symbol)
                .cloned()
                .unwrap_or_else(|| ("4h".to_string(), "TIER_1_LIQUID".to_string()));
            let mut val = serde_json::to_value(s).unwrap_or_else(|_| json!({}));
            if let Some(obj) = val.as_object_mut() {
                obj.insert("symbol".to_string(), json!(s.exec_symbol));
                obj.insert("timeframe".to_string(), json!(tf));
                obj.insert("ensemble_score".to_string(), json!(s.blended_conviction));
                obj.insert("entry_threshold".to_string(), json!(entry_threshold));
                obj.insert("volatility_tier".to_string(), json!(tier));
            }
            val
        })
        .collect()
}

/// Builds the Book B (`POND`) telemetry payload for the Pond dashboard page and `/api/v1/pond/*`.
pub fn build_pond_payload(db: &DatabaseManager) -> aegis_core::Result<Value> {
    let cfg = db.load_config().unwrap_or_default();
    let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
    let pond_init = round_dp(cfg.get_f64("initial_equity_usdc", 10000.0) * (1.0 - split_pct), 2);

    let mut universe = db.get_pond_universe(false).unwrap_or_default();
    if universe.is_empty() {
        let orch = PondOrchestrator::new(db.clone(), None);
        universe = orch.initialize_or_refresh_universe().unwrap_or_default();
    }

    let rank_map: HashMap<String, i64> = universe
        .iter()
        .map(|u| (u.symbol.clone(), u.rank))
        .collect();

    let open_cells = db.get_open_pond_cells().unwrap_or_default();
    let recent_cells = db.get_pond_cells(40).unwrap_or_default();
    let scan_cycles = db.get_recent_pond_scan_cycles(25).unwrap_or_default();
    let champion = db.get_active_pond_champion().ok();

    let (enriched_open_cells, open_price_upnl) = enrich_pond_cells(db, &open_cells, &rank_map);
    let (enriched_recent_cells, _) = enrich_pond_cells(db, &recent_cells, &rank_map);

    let enriched_universe: Vec<Value> = universe
        .iter()
        .map(|u| {
            let base_asset = u.symbol.split('/').next().unwrap_or(&u.symbol).to_string();
            let mut val = serde_json::to_value(u).unwrap_or_else(|_| json!({}));
            if let Some(obj) = val.as_object_mut() {
                obj.insert("base_asset".to_string(), json!(base_asset));
            }
            val
        })
        .collect();

    let realized_pnl = round_dp(db.get_pond_realized_pnl().unwrap_or(0.0), 4);
    let unrealized_pnl = open_price_upnl;
    let funding_accrued: f64 = round_dp(
        recent_cells.iter().map(|c| c.funding_accrued_usdc).sum(),
        4,
    );
    let open_funding: f64 = open_cells.iter().map(|c| c.funding_accrued_usdc).sum();
    let open_fees_paid: f64 = open_cells.iter().map(|c| c.fees_paid_usdc).sum();
    let total_pnl = round_dp(realized_pnl + unrealized_pnl + open_funding - open_fees_paid, 4);
    let net_pond_transfers = round_dp(db.get_net_book_transfer_adjustment("POND").unwrap_or(0.0), 2);
    let pond_equity = round_dp((pond_init + total_pnl + net_pond_transfers).max(0.0), 2);

    let deployed_notional: f64 = round_dp(
        open_cells
            .iter()
            .map(|c| c.notional_usdc.unwrap_or(0.0))
            .sum(),
        2,
    );
    let open_heat: f64 = round_dp(
        open_cells
            .iter()
            .map(|c| c.risk_budget_usdc.unwrap_or(0.0))
            .sum(),
        2,
    );
    let mut idle_usdc = round_dp((pond_equity - deployed_notional).max(0.0), 2);
    let mut perp_trading_eq = pond_equity;
    let mut vault_eq_out = 0.0;
    if let Ok((_, perps_eq, v_eq, total_wallet_deployed)) = db.get_wallet_perps_margin_state() {
        perp_trading_eq = perps_eq;
        vault_eq_out = v_eq;
        if v_eq > 0.0 || total_wallet_deployed > deployed_notional {
            let avail_perps_margin = round_dp((perps_eq - total_wallet_deployed).max(0.0), 2);
            idle_usdc = idle_usdc.min(avail_perps_margin);
        }
    }
    let gross_leverage = round_dp(deployed_notional / pond_equity.max(1.0), 4);

    let short_call_count = open_cells
        .iter()
        .filter(|c| c.option_analogue == "SHORT_CALL")
        .count();
    let short_put_count = open_cells
        .iter()
        .filter(|c| c.option_analogue == "SHORT_PUT")
        .count();

    let active_count = universe.iter().filter(|u| u.status == "ACTIVE").count();
    let warmup_count = universe.iter().filter(|u| u.status == "WARMUP").count();
    let delisting_count = universe.iter().filter(|u| u.status == "DELISTING").count();

    Ok(json!({
        "book_id": "POND",
        "initial_capital_usdc": pond_init,
        "net_transfers_usdc": net_pond_transfers,
        "pond_equity_usdc": pond_equity,
        "idle_usdc": idle_usdc,
        "perp_trading_equity_usdc": perp_trading_eq,
        "vault_equity_usdc": vault_eq_out,
        "deployed_notional_usdc": deployed_notional,
        "open_heat_usdc": open_heat,
        "max_open_heat_usdc": cfg.get_f64("pond_max_heat_usdc", 200.0),
        "gross_leverage": gross_leverage,
        "max_gross_leverage": cfg.get_f64("pond_max_gross_leverage", 1.5),
        "realized_pnl_usdc": realized_pnl,
        "unrealized_pnl_usdc": unrealized_pnl,
        "funding_accrued_usdc": funding_accrued,
        "total_pnl_usdc": total_pnl,
        "total_pnl_pct": round_dp((total_pnl / pond_init.max(1.0)) * 100.0, 2),
        "open_cells_count": open_cells.len(),
        "max_concurrent_cells": cfg.get_i64("pond_max_concurrent_cells", 8),
        "short_call_count": short_call_count,
        "short_put_count": short_put_count,
        "direction_policy": cfg.get_str("pond_direction_policy", "both"),
        "execution_mode": cfg.get_str("pond_execution_mode", "PAPER"),
        "active_champion_id": cfg.get_str("pond_champion_active", "NPS-01"),
        "champion": champion,
        "universe_total_count": universe.len(),
        "universe_active_count": active_count,
        "universe_warmup_count": warmup_count,
        "universe_delisting_count": delisting_count,
        "open_cells": enriched_open_cells,
        "recent_cells": enriched_recent_cells,
        "universe": enriched_universe,
        "scan_cycles": scan_cycles,
    }))
}

/// Builds the composite PP-50 Two-Book Platform status payload for Page 1 (`Overall Status`)
/// and `/api/v1/pp50/status`.
pub fn build_pp50_payload(db: &DatabaseManager) -> aegis_core::Result<Value> {
    let portfolio = build_portfolio_payload(db)?;
    let pond = build_pond_payload(db)?;
    build_pp50_payload_with_books(db, &portfolio, &pond)
}

pub fn build_pp50_payload_with_books(
    db: &DatabaseManager,
    portfolio: &Value,
    pond: &Value,
) -> aegis_core::Result<Value> {
    let cfg = db.load_config()?;

    let pit_init = portfolio
        .get("pit_initial_capital_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(5000.0);
    let pit_eq = portfolio
        .get("pit_equity_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(5000.0);
    let pit_realized = portfolio
        .get("realized_pnl_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_unrealized = portfolio
        .get("unrealized_pnl_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_funding = portfolio
        .get("funding_accrued_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_yield = portfolio
        .get("accrued_yield_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_total_pnl = portfolio
        .get("total_pnl_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_deployed = portfolio
        .get("deployed_notional_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_heat = portfolio
        .get("portfolio_heat_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_lev = portfolio
        .get("gross_leverage")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pit_regime = portfolio
        .get("hmm_regime")
        .and_then(Value::as_str)
        .unwrap_or("CHOP");
    let pit_open_count = portfolio
        .get("open_long_count")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        + portfolio
            .get("open_short_count")
            .and_then(Value::as_i64)
            .unwrap_or(0);

    let pond_init = pond
        .get("initial_capital_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(5000.0);
    let pond_eq = pond
        .get("pond_equity_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(5000.0);
    let pond_realized = pond
        .get("realized_pnl_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pond_unrealized = pond
        .get("unrealized_pnl_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pond_funding = pond
        .get("funding_accrued_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pond_total_pnl = pond
        .get("total_pnl_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pond_deployed = pond
        .get("deployed_notional_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pond_heat = pond
        .get("open_heat_usdc")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pond_lev = pond
        .get("gross_leverage")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    let pond_open_cells = pond
        .get("open_cells_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);

    let platform_init = round_dp(pit_init + pond_init, 2);
    let total_platform_eq = round_dp(pit_eq + pond_eq, 2);
    let total_platform_pnl = round_dp(pit_total_pnl + pond_total_pnl, 4);
    let total_platform_pnl_pct =
        round_dp((total_platform_pnl / platform_init.max(1.0)) * 100.0, 2);

    let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
    let drift_bps = round_dp(compute_drift_bps(pit_eq, total_platform_eq, split_pct), 2);
    let pit_share_pct = if total_platform_eq > 0.0 {
        round_dp((pit_eq / total_platform_eq) * 100.0, 2)
    } else {
        50.0
    };
    let pond_share_pct = round_dp(100.0 - pit_share_pct, 2);

    let max_cells = cfg.get_i64("pond_max_concurrent_cells", 8);
    let champion_id = cfg.get_str("pond_champion_active", "NPS-01");
    let recent_transfers = db.get_recent_book_transfers(20).unwrap_or_default();
    let latest_snapshot = db
        .get_latest_pp50_platform_snapshot()
        .ok()
        .and_then(|snap| {
            let mut val = serde_json::to_value(&snap).ok()?;
            if let Some(obj) = val.as_object_mut() {
                let snap_ret = ((snap.platform_equity_usdc - platform_init)
                    / platform_init.max(1.0))
                    * 100.0;
                let ts_prefix = snap
                    .created_at
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .map(|s| format!("[{}] ", s))
                    .unwrap_or_default();
                let report_line = format!(
                    "{}PP-50 D+{:02} | platform ${:.2} ({:+.2}%) | PIT ${:.2} | POND ${:.2} | drift {:.1} bps | settle: {} | cells: {}/{} (heat ${:.0}) | champion {}",
                    ts_prefix,
                    snap.snapshot_id.unwrap_or(1),
                    snap.platform_equity_usdc,
                    snap_ret,
                    snap.pit_equity_usdc,
                    snap.pond_equity_usdc,
                    snap.drift_bps,
                    snap.settling_status,
                    snap.pond_open_cells,
                    max_cells,
                    pond_heat.abs(),
                    champion_id
                );
                obj.insert("report_line".to_string(), json!(report_line));
            }
            Some(val)
        });
    let tolerance_bps = cfg.get_f64(
        "settling_tolerance_bps",
        cfg.get_f64("settling_deadband_bps", 50.0),
    );
    let settling_time_utc = cfg.get_str(
        "settling_time_utc",
        &cfg.get_str("settling_utc_hour", "00:05"),
    );

    Ok(json!({
        "platform": PLATFORM_NAME,
        "version": PLATFORM_VERSION,
        "platform_initial_equity_usdc": platform_init,
        "total_platform_equity_usdc": total_platform_eq,
        "total_platform_pnl_usdc": total_platform_pnl,
        "total_platform_pnl_pct": total_platform_pnl_pct,
        "pit_share_pct": pit_share_pct,
        "pond_share_pct": pond_share_pct,
        "target_split_pct": round_dp(split_pct * 100.0, 1),
        "drift_bps": drift_bps,
        "deadband_bps": tolerance_bps,
        "settling_tolerance_bps": tolerance_bps,
        "settling_time_utc": settling_time_utc,
        "settling_enabled": cfg.get_bool("settling_enabled", true),
        "settling_kill_switch": cfg.get_bool("settling_kill_switch", false),
        "settling_in_progress": cfg.get_bool("platform_settle_gate_enabled", false),
        "platform_pp50_halt": cfg.get_bool("platform_pp50_halt", false),
        "pit": {
            "book_id": "PIT",
            "execution_mode": cfg.get_str("execution_mode", "PAPER"),
            "initial_capital_usdc": pit_init,
            "equity_usdc": pit_eq,
            "realized_pnl_usdc": pit_realized,
            "unrealized_pnl_usdc": pit_unrealized,
            "funding_accrued_usdc": pit_funding,
            "accrued_yield_usdc": pit_yield,
            "total_pnl_usdc": pit_total_pnl,
            "total_pnl_pct": round_dp((pit_total_pnl / pit_init.max(1.0)) * 100.0, 2),
            "deployed_notional_usdc": pit_deployed,
            "heat_usdc": pit_heat,
            "gross_leverage": pit_lev,
            "open_positions": pit_open_count,
            "hmm_regime": pit_regime,
        },
        "pond": {
            "book_id": "POND",
            "execution_mode": cfg.get_str("pond_execution_mode", "PAPER"),
            "initial_capital_usdc": pond_init,
            "equity_usdc": pond_eq,
            "realized_pnl_usdc": pond_realized,
            "unrealized_pnl_usdc": pond_unrealized,
            "funding_accrued_usdc": pond_funding,
            "total_pnl_usdc": pond_total_pnl,
            "total_pnl_pct": round_dp((pond_total_pnl / pond_init.max(1.0)) * 100.0, 2),
            "deployed_notional_usdc": pond_deployed,
            "heat_usdc": pond_heat,
            "gross_leverage": pond_lev,
            "open_cells": pond_open_cells,
            "active_champion_id": champion_id,
            "direction_policy": cfg.get_str("pond_direction_policy", "both"),
        },
        "recent_transfers": recent_transfers,
        "latest_snapshot": latest_snapshot,
    }))
}

/// Builds the unified telemetry payload consumed by `GET /api/v1/dashboard` and `GET /api/v1/stream`.
pub fn build_dashboard_payload(db: &DatabaseManager) -> aegis_core::Result<Value> {
    let portfolio = build_portfolio_payload(db)?;
    let pond = build_pond_payload(db)?;
    let pp50 = build_pp50_payload_with_books(db, &portfolio, &pond)?;
    let raw_signals = db.get_latest_signals(15)?;
    let signals = enrich_signals(db, &raw_signals);
    let open_trades = db.get_open_trades()?;
    let recent_trades = db.get_recent_trades(30)?;
    let alt_pairs = db.get_alt_pairs_universe()?;
    let config_meta = db.get_config_rows(true)?;
    let creds = db.get_credentials_status()?;
    let logs = db.get_recent_logs(25)?;
    let cfg = db.load_config()?;
    let schema_version = db.get_schema_version().unwrap_or(6);
    let runtime_detail = cfg.get_str("engine_runtime_detail", ENGINE_RUNTIME_DETAIL);

    Ok(json!({
        "timestamp_ms": db.now_ms(),
        "version": PLATFORM_VERSION,
        "runtime": ENGINE_RUNTIME_LABEL,
        "engine_runtime": runtime_detail,
        "polling_interval_seconds": cfg.get_i64("polling_interval_seconds", 15),
        "pp50": pp50,
        "portfolio": portfolio,
        "pond": pond,
        "signals": signals,
        "open_trades": open_trades,
        "recent_trades": recent_trades,
        "universe": alt_pairs,
        "config": config_meta,
        "credentials": creds,
        "logs": logs,
        "schema_version": schema_version,
    }))
}

const EMBEDDED_INDEX_HTML: &str = include_str!("../../../web/static/index.html");

async fn serve_index(State(state): State<WebAppState>) -> Response {
    let index_path = state.static_dir.join("index.html");
    let raw_html = tokio::task::spawn_blocking(move || fs::read_to_string(&index_path))
        .await
        .ok()
        .and_then(std::result::Result::ok)
        .unwrap_or_else(|| EMBEDDED_INDEX_HTML.to_string());
    let meta_tag = format!(
        "<meta name=\"aegis-session-token\" content=\"{}\" />\n</head>",
        state.session_token
    );
    let html = if raw_html.contains("name=\"aegis-session-token\"") {
        raw_html
    } else {
        raw_html.replacen("</head>", &meta_tag, 1)
    };
    let mut resp = Html(html).into_response();
    let cookie_val = format!(
        "aegis_session={}; HttpOnly; SameSite=Strict; Path=/",
        state.session_token
    );
    if let Ok(hv) = HeaderValue::from_str(&cookie_val) {
        resp.headers_mut().insert(header::SET_COOKIE, hv);
    }
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate"),
    );
    resp.headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    resp
}

async fn api_status(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let integrity = db.verify_integrity()?;
        let cfg = db.load_config()?;
        let snap = db.get_latest_portfolio_snapshot()?;
        let creds = db.get_credentials_status()?;
        let runtime_detail = cfg.get_str("engine_runtime_detail", ENGINE_RUNTIME_DETAIL);

        Ok(json!({
            "status": "ok",
            "state": "ONLINE",
            "platform": cfg.get_str("platform_name", PLATFORM_NAME),
            "version": PLATFORM_VERSION,
            "release": format!("v{PLATFORM_VERSION}"),
            "runtime": ENGINE_RUNTIME_LABEL,
            "engine_runtime": runtime_detail,
            "schema_version": integrity.get("schema_version").and_then(Value::as_i64).unwrap_or(6),
            "exchange": cfg.get_str("exchange_id", "hyperliquid"),
            "execution_mode": creds.get("execution_mode").and_then(Value::as_str).unwrap_or("PAPER"),
            "pond_execution_mode": creds.get("pond_execution_mode").and_then(Value::as_str).unwrap_or("PAPER"),
            "hyperliquid_testnet": creds.get("hyperliquid_testnet").and_then(Value::as_bool).unwrap_or(false),
            "network": creds.get("network").and_then(Value::as_str).unwrap_or("mainnet"),
            "api_endpoint": creds.get("api_endpoint").and_then(Value::as_str).unwrap_or("https://api.hyperliquid.xyz"),
            "credentials_ready": creds.get("credentials_ready").and_then(Value::as_bool).unwrap_or(false),
            "active_routing": creds.get("active_routing").and_then(Value::as_str).unwrap_or("PAPER_SIMULATION"),
            "alert_channel": "TELEGRAM",
            "telegram_alerts_ready": creds.get("telegram_alerts_ready").and_then(Value::as_bool).unwrap_or(false),
            "environment": creds.get("environment").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| cfg.get_str("environment", "sandbox")),
            "polling_interval_seconds": cfg.get_i64("polling_interval_seconds", 15),
            "journal_mode": integrity.get("journal_mode").and_then(Value::as_str).unwrap_or("wal"),
            "system_config_count": integrity.get("system_config_count").and_then(Value::as_i64).unwrap_or(0),
            "universe_counts": {
                "usdc_perp": integrity.get("v_universe_usdc_perp_count").and_then(Value::as_i64).unwrap_or(16),
                "btc_synth": integrity.get("v_universe_btc_synth_count").and_then(Value::as_i64).unwrap_or(15),
                "alt_pairs": integrity.get("v_universe_alt_pairs_count").and_then(Value::as_i64).unwrap_or(15),
                "total": integrity.get("universe_assets_total").and_then(Value::as_i64).unwrap_or(31),
            },
            "hmm_regime": snap.hmm_regime,
            "initial_equity_usdc": cfg.get_f64("initial_equity_usdc", 10000.0),
            "timestamp_ms": db.now_ms(),
        }))
    })
    .await
}

async fn api_pp50_status(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| build_pp50_payload(&db)).await
}

async fn api_pond_universe(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let pond = build_pond_payload(&db)?;
        Ok(json!({
            "universe_total_count": pond.get("universe_total_count").cloned().unwrap_or(json!(0)),
            "universe_active_count": pond.get("universe_active_count").cloned().unwrap_or(json!(0)),
            "universe_warmup_count": pond.get("universe_warmup_count").cloned().unwrap_or(json!(0)),
            "universe_delisting_count": pond.get("universe_delisting_count").cloned().unwrap_or(json!(0)),
            "universe": pond.get("universe").cloned().unwrap_or(json!([])),
        }))
    })
    .await
}

async fn api_pond_cells(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let pond = build_pond_payload(&db)?;
        Ok(json!({
            "open_cells_count": pond.get("open_cells_count").cloned().unwrap_or(json!(0)),
            "short_call_count": pond.get("short_call_count").cloned().unwrap_or(json!(0)),
            "short_put_count": pond.get("short_put_count").cloned().unwrap_or(json!(0)),
            "open_heat_usdc": pond.get("open_heat_usdc").cloned().unwrap_or(json!(0.0)),
            "open_cells": pond.get("open_cells").cloned().unwrap_or(json!([])),
            "recent_cells": pond.get("recent_cells").cloned().unwrap_or(json!([])),
        }))
    })
    .await
}

async fn api_pond_cycles(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let cycles = db.get_recent_pond_scan_cycles(50)?;
        Ok(json!({
            "count": cycles.len(),
            "scan_cycles": cycles,
        }))
    })
    .await
}

async fn api_pond_trigger_scan(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let orch = PondOrchestrator::new(db.clone(), None);
        let summary = orch.run_scan_cycle(true)?;
        let dash = build_dashboard_payload(&db)?;
        Ok(json!({
            "status": "POND_SCAN_COMPLETE",
            "summary": summary,
            "dashboard": dash,
        }))
    })
    .await
}

async fn api_settle_transfers(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let transfers = db.get_recent_book_transfers(50)?;
        Ok(json!({
            "count": transfers.len(),
            "transfers": transfers,
        }))
    })
    .await
}

async fn api_settle_run(
    State(state): State<WebAppState>,
    payload: Option<Json<SettleRequestPayload>>,
) -> Response {
    let req = payload.map(|p| p.0).unwrap_or_default();
    run_db_blocking(state.db, move |db| {
        let ctrl = SettleController::new(db.clone(), None);
        let settle_res = ctrl.run_settling(SettleOverrideInput {
            pit_equity_usdc: req.pit_equity_usdc,
            pond_equity_usdc: req.pond_equity_usdc,
            rich_idle_usdc: req.rich_idle_usdc,
            day_index: req.day_index,
            dry_run: req.dry_run.unwrap_or(true),
        })?;
        let dash = build_dashboard_payload(&db)?;
        Ok(json!({
            "status": settle_res.status,
            "settle": settle_res,
            "dashboard": dash,
        }))
    })
    .await
}

async fn api_portfolio(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| build_portfolio_payload(&db)).await
}

async fn api_signals(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let raw_signals = db.get_latest_signals(15)?;
        let signals = enrich_signals(&db, &raw_signals);
        let snap = db.get_latest_portfolio_snapshot()?;
        Ok(json!({
            "hmm_regime": snap.hmm_regime,
            "prob_bull": snap.prob_bull,
            "prob_bear": snap.prob_bear,
            "prob_chop": snap.prob_chop,
            "count": signals.len(),
            "signals": signals,
        }))
    })
    .await
}

async fn api_universe(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let usdc_perp = db.get_usdc_perp_universe()?;
        let btc_synth = db.get_btc_synth_universe()?;
        let alt_pairs = db.get_alt_pairs_universe()?;
        Ok(json!({
            "usdc_perp_count": usdc_perp.len(),
            "btc_synth_count": btc_synth.len(),
            "alt_pairs_count": alt_pairs.len(),
            "usdc_perp": usdc_perp,
            "btc_synth": btc_synth,
            "alt_pairs": alt_pairs,
        }))
    })
    .await
}

async fn api_trades(
    State(state): State<WebAppState>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    run_db_blocking(state.db, move |db| {
        let open_trades = db.get_open_trades()?;
        let recent_trades = db.get_recent_trades(limit)?;
        Ok(json!({
            "open_count": open_trades.len(),
            "open_trades": open_trades,
            "recent_trades": recent_trades,
        }))
    })
    .await
}

async fn api_logs(
    State(state): State<WebAppState>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 250);
    run_db_blocking(state.db, move |db| {
        let logs = db.get_recent_logs(limit)?;
        Ok(json!({
            "count": logs.len(),
            "logs": logs,
        }))
    })
    .await
}

async fn api_dashboard(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| build_dashboard_payload(&db)).await
}

async fn api_get_config(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let rows = db.get_config_rows(true)?;
        Ok(json!({
            "count": rows.len(),
            "config": rows,
            "items": rows,
        }))
    })
    .await
}

async fn api_update_config(
    State(state): State<WebAppState>,
    Json(payload): Json<ConfigUpdatePayload>,
) -> Response {
    let key = payload.config_key.trim().to_string();
    if key.is_empty() {
        return map_err_response(AegisError::Validation(
            "config_key cannot be empty.".to_string(),
        ));
    }
    if is_immutable_security_config_key(&key) {
        return map_err_response(AegisError::SecurityViolation(format!(
            "Security policy key '{key}' is immutable via the web API."
        )));
    }

    run_db_blocking(state.db, move |db| {
        let mut scaled_caps: Option<HashMap<String, f64>> = None;
        if key == "initial_equity_usdc" && payload.scale_risk_caps {
            let eq = payload
                .config_value
                .as_f64()
                .or_else(|| {
                    payload
                        .config_value
                        .as_str()
                        .and_then(|s| s.trim().parse::<f64>().ok())
                })
                .unwrap_or(0.0);
            scaled_caps = Some(db.scale_risk_caps_for_equity(eq)?);
        } else if !db.set_config_value(&key, &payload.config_value)? {
            return Err(AegisError::NotFound(format!("Unknown config_key: {key}")));
        }

        let redacted_value = if is_sensitive_config_key(&key) {
            json!("[REDACTED]")
        } else {
            payload.config_value.clone()
        };
        let _ = db.log_telemetry(
            "INFO",
            "web.api",
            &format!("Updated system_config[{key}] = {redacted_value}"),
            None,
        );
        let meta = db.get_config_rows(true).unwrap_or_default();
        let creds = db.get_credentials_status().ok();
        Ok(json!({
            "status": "updated",
            "config_key": key,
            "config_value": redacted_value,
            "scaled_caps": scaled_caps,
            "config": meta,
            "credentials": creds,
        }))
    })
    .await
}

async fn api_update_config_batch(
    State(state): State<WebAppState>,
    Json(payload): Json<ConfigBatchUpdatePayload>,
) -> Response {
    if payload.updates.is_empty() {
        return map_err_response(AegisError::Validation(
            "No configuration updates provided.".to_string(),
        ));
    }
    for key in payload.updates.keys() {
        if is_immutable_security_config_key(key.trim()) {
            return map_err_response(AegisError::SecurityViolation(format!(
                "Security policy key '{}' is immutable via the web API.",
                key.trim()
            )));
        }
    }

    run_db_blocking(state.db, move |db| {
        let updated_keys = db.set_config_batch(&payload.updates, payload.scale_risk_caps)?;
        let _ = db.log_telemetry(
            "INFO",
            "web.api",
            &format!(
                "Batch-updated {} system_config keys (scale_risk_caps={})",
                updated_keys.len(),
                payload.scale_risk_caps
            ),
            Some(json!({ "updated_keys": updated_keys })),
        );
        let meta = db.get_config_rows(true).unwrap_or_default();
        let creds = db.get_credentials_status().ok();
        let snap = build_portfolio_payload(&db).ok();
        Ok(json!({
            "status": "updated",
            "count": updated_keys.len(),
            "updated_keys": updated_keys,
            "config": meta,
            "credentials": creds,
            "portfolio": snap,
        }))
    })
    .await
}

async fn api_get_credentials(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| db.get_credentials_status()).await
}

async fn api_update_credentials(
    State(state): State<WebAppState>,
    Json(payload): Json<CredentialsUpdatePayload>,
) -> Response {
    run_db_blocking(state.db, move |db| {
        let mut status = db.configure_hyperliquid_credentials(
            payload.wallet_address.as_deref(),
            payload.private_key.as_deref(),
            payload.hyperliquid_testnet,
            payload.execution_mode.as_deref(),
            payload.hyperliquid_vault_address.as_deref(),
            payload.gemini_api_key.as_deref(),
            true,
        )?;

        if payload.telegram_bot_token.is_some()
            || payload.telegram_chat_id.is_some()
            || payload.max_daily_drawdown_alert_pct.is_some()
        {
            status = db.configure_telegram_alerts(
                payload.telegram_bot_token.as_deref(),
                payload.telegram_chat_id.as_deref(),
                payload.max_daily_drawdown_alert_pct,
            )?;
        }

        let _ = db.log_telemetry(
            "INFO",
            "web.api",
            &format!(
                "Updated Hyperliquid credentials & routing (mode={}, network={}, ready={})",
                status["execution_mode"].as_str().unwrap_or("PAPER"),
                status["network"].as_str().unwrap_or("mainnet"),
                status["credentials_ready"].as_bool().unwrap_or(false),
            ),
            None,
        );

        let cfg_meta = db.get_config_rows(true).unwrap_or_default();
        Ok(json!({
            "status": "updated",
            "credentials": status,
            "config": cfg_meta,
        }))
    })
    .await
}

async fn api_get_alerts(State(state): State<WebAppState>) -> Response {
    run_db_blocking(state.db, |db| {
        let status = db.get_credentials_status()?;
        Ok(json!({
            "alert_channel": "TELEGRAM",
            "telegram_bot_token_masked": status.get("telegram_bot_token_masked").cloned().unwrap_or(json!("")),
            "telegram_bot_token_configured": status.get("telegram_bot_token_configured").cloned().unwrap_or(json!(false)),
            "telegram_chat_id": status.get("telegram_chat_id").cloned().unwrap_or(json!("")),
            "telegram_alerts_ready": status.get("telegram_alerts_ready").cloned().unwrap_or(json!(false)),
            "max_daily_drawdown_alert_pct": status.get("max_daily_drawdown_alert_pct").cloned().unwrap_or(json!(0.03)),
        }))
    })
    .await
}

async fn api_update_alerts(
    State(state): State<WebAppState>,
    Json(payload): Json<AlertsUpdatePayload>,
) -> Response {
    run_db_blocking(state.db, move |db| {
        let mut status = db.configure_telegram_alerts(
            payload.telegram_bot_token.as_deref(),
            payload.telegram_chat_id.as_deref(),
            payload.max_daily_drawdown_alert_pct,
        )?;
        if let Ok(cfg) = db.load_config() {
            let tg_token = cfg.get_str("telegram_bot_token", "").trim().to_string();
            let tg_chat_id = cfg.get_str("telegram_chat_id", "").trim().to_string();
            let bot_id_prefix = tg_token.split(':').next().unwrap_or("").trim().to_string();
            if !tg_token.is_empty()
                && (tg_chat_id.is_empty()
                    || (!bot_id_prefix.is_empty() && tg_chat_id == bot_id_prefix))
            {
                let alerts_engine = AlertEngine::new(db.clone());
                if alerts_engine.discover_telegram_chat_id(&tg_token).is_some() {
                    if let Ok(refreshed) = db.get_credentials_status() {
                        status = refreshed;
                    }
                }
            }
        }
        let _ = db.log_telemetry(
            "INFO",
            "web.api",
            &format!(
                "Updated Telegram alert configuration (ready={}, dd_threshold={:.1}%)",
                status["telegram_alerts_ready"].as_bool().unwrap_or(false),
                status["max_daily_drawdown_alert_pct"]
                    .as_f64()
                    .unwrap_or(0.03)
                    * 100.0
            ),
            None,
        );
        Ok(json!({
            "status": "updated",
            "alerts": status,
        }))
    })
    .await
}

async fn api_sync_wallet_equity(
    State(state): State<WebAppState>,
    payload: Option<Json<WalletSyncPayload>>,
) -> Response {
    let update_caps = payload
        .and_then(|p| p.scale_risk_caps.or(p.update_risk_caps))
        .unwrap_or(true);
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || -> aegis_core::Result<Value> {
        let exec = ExecutionEngine::new(db.clone());
        let sync_res = exec.sync_wallet_equity(update_caps);
        let wallet_bal = sync_res
            .get("wallet_balance_usdc")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let scaled_caps = if update_caps && wallet_bal > 0.0 {
            db.scale_risk_caps_for_equity(wallet_bal).ok()
        } else {
            None
        };
        let cfg = db.get_config_rows(true).unwrap_or_default();
        let snap = build_portfolio_payload(&db).ok();
        let creds = db.get_credentials_status().unwrap_or_default();

        Ok(json!({
            "status": "synced",
            "wallet_balance_usdc": wallet_bal,
            "network": sync_res.get("network").and_then(Value::as_str).unwrap_or("mainnet"),
            "account_type": sync_res.get("account_type").and_then(Value::as_str).unwrap_or("perps"),
            "wallet_address": sync_res.get("wallet_address").and_then(Value::as_str).unwrap_or(""),
            "perp_account_value_usdc": sync_res.get("perp_account_value_usdc").and_then(Value::as_f64).unwrap_or(wallet_bal),
            "vault_equity_usdc": sync_res.get("vault_equity_usdc").and_then(Value::as_f64).unwrap_or(0.0),
            "total_equity_usdc": wallet_bal,
            "scaled_caps": scaled_caps,
            "wallet_sync": sync_res,
            "config": cfg,
            "portfolio": snap,
            "credentials": creds,
        }))
    })
    .await;

    match res {
        Ok(Ok(val)) => Json(val).into_response(),
        Ok(Err(e)) => map_err_response(e),
        Err(e) => map_err_response(AegisError::Execution(e.to_string())),
    }
}

async fn api_test_alert(State(state): State<WebAppState>) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || {
        let mut alerts = AlertEngine::new(db);
        alerts.run_test_alert(true)
    })
    .await;

    match res {
        Ok(Ok(val)) => Json(val).into_response(),
        Ok(Err(e)) => map_err_response(e),
        Err(e) => map_err_response(AegisError::Execution(e.to_string())),
    }
}

async fn api_trigger_cycle(State(state): State<WebAppState>) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || -> aegis_core::Result<Value> {
        let ingestion = MarketIngestionEngine::new(db.clone());
        let signals = SignalEngine::new(db.clone());
        let execution = ExecutionEngine::new(db.clone());
        let pond = PondOrchestrator::new(db.clone(), None);
        let cfg = db.load_config()?;
        let lookback_bars = cfg.get_i64("ingest_lookback_bars", 140).clamp(60, 1500) as usize;

        ingestion.ingest_universe(lookback_bars, false)?;
        let evals = signals.evaluate_universe(true)?;
        execution.reconcile_cycle(&evals, false)?;
        if let Err(e) = pond.run_scan_cycle(true) {
            tracing::error!("Pond scan cycle error during manual cycle trigger: {e}");
        }
        let dash = build_dashboard_payload(&db)?;
        Ok(json!({
            "status": "CYCLE_COMPLETE",
            "dashboard": dash,
        }))
    })
    .await;

    match res {
        Ok(Ok(val)) => Json(val).into_response(),
        Ok(Err(e)) => map_err_response(e),
        Err(e) => map_err_response(AegisError::Execution(e.to_string())),
    }
}

async fn api_trigger_calibration(State(state): State<WebAppState>) -> Response {
    let db = state.db.clone();
    let res = tokio::task::spawn_blocking(move || -> aegis_core::Result<Value> {
        let calibrator = WalkForwardCalibrator::new(db.clone());
        let report = calibrator.calibrate_universe(true)?;
        let universe = db.get_alt_pairs_universe()?;
        Ok(json!({
            "status": "CALIBRATION_COMPLETE",
            "calibration_report": report,
            "universe": universe,
        }))
    })
    .await;

    match res {
        Ok(Ok(val)) => Json(val).into_response(),
        Ok(Err(e)) => map_err_response(e),
        Err(e) => map_err_response(AegisError::Execution(e.to_string())),
    }
}

async fn api_trigger_backtest(
    State(state): State<WebAppState>,
    payload: Option<Json<BacktestRequestPayload>>,
) -> Response {
    let req = payload.map(|p| p.0).unwrap_or(BacktestRequestPayload {
        days: 365,
        use_real_history: true,
    });
    let days = req.days.clamp(30, 730);
    let use_real = req.use_real_history;
    let db = state.db.clone();

    let res = tokio::task::spawn_blocking(move || -> aegis_core::Result<Value> {
        let bt = BacktestEngine::new(db, days);
        let ts = bt.run_backtest(None, None, use_real)?;
        let report = BacktestEngine::format_tear_sheet(&ts);
        Ok(json!({
            "status": "COMPLETED",
            "tear_sheet": ts,
            "formatted_report": report,
        }))
    })
    .await;

    match res {
        Ok(Ok(val)) => Json(val).into_response(),
        Ok(Err(e)) => map_err_response(e),
        Err(e) => map_err_response(AegisError::Execution(e.to_string())),
    }
}

async fn api_sse_stream(State(state): State<WebAppState>) -> Response {
    let Ok(permit) = state.sse_semaphore.clone().try_acquire_owned() else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiErrorBody {
                detail: "Maximum concurrent SSE client connections reached.".to_string(),
            }),
        )
            .into_response();
    };

    let db = state.db.clone();
    let sse_interval_secs = db
        .load_config_opts(false)
        .map(|cfg| cfg.get_i64("web_sse_interval_secs", 3).max(1) as u64)
        .unwrap_or(3);
    let stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(sse_interval_secs))).then(move |_| {
        let _keep_permit = &permit;
        let db_inner = db.clone();
        async move {
            let data = match tokio::task::spawn_blocking(move || build_dashboard_payload(&db_inner)).await {
                Ok(Ok(payload)) => payload.to_string(),
                Ok(Err(e)) => json!({ "error": e.to_string() }).to_string(),
                Err(e) => json!({ "error": e.to_string() }).to_string(),
            };
            Ok::<Event, Infallible>(Event::default().data(data))
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default()).into_response()
}

fn extract_request_tokens(headers: &HeaderMap, query: Option<&str>) -> Vec<String> {
    let mut tokens = Vec::new();
    if let Some(auth) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        let trimmed = auth.trim();
        if let Some(bearer) = trimmed.strip_prefix("Bearer ").or_else(|| trimmed.strip_prefix("bearer ")) {
            let tok = bearer.trim();
            if !tok.is_empty() {
                tokens.push(tok.to_string());
            }
        }
    }
    if let Some(tok) = headers
        .get("x-aegis-token")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        tokens.push(tok.to_string());
    }
    if let Some(cookie_hdr) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) {
        for part in cookie_hdr.split(';') {
            let kv = part.trim();
            if let Some(val) = kv.strip_prefix("aegis_session=") {
                let tok = val.trim();
                if !tok.is_empty() {
                    tokens.push(tok.to_string());
                }
            }
        }
    }
    if let Some(q) = query {
        for pair in q.split('&') {
            if let Some(val) = pair.strip_prefix("token=") {
                let tok = val.trim();
                if !tok.is_empty() {
                    tokens.push(tok.to_string());
                }
            }
        }
    }
    tokens
}

fn is_request_authorized(state: &WebAppState, req: &Request) -> bool {
    let path = req.uri().path();
    if path == "/" || path == "/api/v1/health" || req.method() == Method::OPTIONS {
        return true;
    }

    let cfg = state.db.load_config().unwrap_or_default();
    if !cfg.get_bool("web_auth_enabled", true) {
        return true;
    }

    let candidates = extract_request_tokens(req.headers(), req.uri().query());
    if candidates.is_empty() {
        return false;
    }

    let env_tok = std::env::var("AEGIS_WEB_TOKEN").ok();
    let trimmed_env_tok = env_tok.as_deref().map(str::trim).unwrap_or("");
    let db_tok = cfg.get_str("web_api_token", "");
    let trimmed_db_tok = db_tok.trim();

    for candidate in &candidates {
        if constant_time_eq(candidate, &state.session_token) {
            return true;
        }
        if !trimmed_env_tok.is_empty() && constant_time_eq(candidate, trimmed_env_tok) {
            return true;
        }
        if !trimmed_db_tok.is_empty() && constant_time_eq(candidate, trimmed_db_tok) {
            return true;
        }
    }
    false
}

async fn auth_and_security_headers_middleware(
    State(state): State<WebAppState>,
    req: Request,
    next: Next,
) -> Response {
    let csp = state
        .db
        .load_config()
        .map(|c| c.get_str("web_csp_policy", DEFAULT_CSP_POLICY))
        .unwrap_or_else(|_| DEFAULT_CSP_POLICY.to_string());

    let mut resp = if !is_request_authorized(&state, &req) {
        (
            StatusCode::UNAUTHORIZED,
            Json(ApiErrorBody {
                detail: "Unauthorized: valid Bearer token, X-Aegis-Token header, or session cookie required.".to_string(),
            }),
        )
            .into_response()
    } else {
        next.run(req).await
    };

    if let Ok(hv) = HeaderValue::from_str(&csp) {
        resp.headers_mut()
            .insert(HeaderName::from_static("content-security-policy"), hv);
    }
    resp.headers_mut().insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    resp.headers_mut().insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate"),
    );
    resp.headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    resp
}

fn build_cors_layer(db: &DatabaseManager) -> CorsLayer {
    let raw_origins = db
        .load_config()
        .map(|c| {
            c.get_str(
                "web_cors_allowed_origins",
                "http://127.0.0.1:8080,http://localhost:8080",
            )
        })
        .unwrap_or_else(|_| "http://127.0.0.1:8080,http://localhost:8080".to_string());

    let origins: Vec<HeaderValue> = raw_origins
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| HeaderValue::from_str(s).ok())
        .collect();

    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("x-aegis-token"),
        ])
}

/// Constructs the complete `axum::Router` for Aegis Quant WebUI & REST API.
pub fn create_router(db: DatabaseManager) -> Router {
    let state = WebAppState::new(db);
    create_router_with_state(state)
}

/// Constructs the complete `axum::Router` from an explicit `WebAppState`.
pub fn create_router_with_state(state: WebAppState) -> Router {
    let cors = build_cors_layer(&state.db);

    Router::new()
        .route("/", get(serve_index))
        .route("/api/v1/status", get(api_status))
        .route("/api/v1/health", get(api_status))
        .route("/api/v1/pp50/status", get(api_pp50_status))
        .route("/api/v1/pond/universe", get(api_pond_universe))
        .route("/api/v1/pond/cells", get(api_pond_cells))
        .route("/api/v1/pond/cycles", get(api_pond_cycles))
        .route("/api/v1/pond/scan", post(api_pond_trigger_scan))
        .route("/api/v1/settle/transfers", get(api_settle_transfers))
        .route("/api/v1/settle/run", post(api_settle_run))
        .route("/api/v1/portfolio", get(api_portfolio))
        .route("/api/v1/signals", get(api_signals))
        .route("/api/v1/universe", get(api_universe))
        .route("/api/v1/trades", get(api_trades))
        .route("/api/v1/logs", get(api_logs))
        .route("/api/v1/dashboard", get(api_dashboard))
        .route(
            "/api/v1/config",
            get(api_get_config)
                .post(api_update_config)
                .put(api_update_config),
        )
        .route("/api/v1/config/batch", post(api_update_config_batch))
        .route("/api/v1/config/sync-wallet", post(api_sync_wallet_equity))
        .route("/api/v1/wallet/sync", post(api_sync_wallet_equity))
        .route(
            "/api/v1/credentials",
            get(api_get_credentials)
                .post(api_update_credentials)
                .put(api_update_credentials),
        )
        .route(
            "/api/v1/alerts",
            get(api_get_alerts)
                .post(api_update_alerts)
                .put(api_update_alerts),
        )
        .route("/api/v1/alerts/test", post(api_test_alert))
        .route("/api/v1/engine/cycle", post(api_trigger_cycle))
        .route("/api/v1/engine/calibrate", post(api_trigger_calibration))
        .route("/api/v1/engine/backtest", post(api_trigger_backtest))
        .route("/api/v1/stream", get(api_sse_stream))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_and_security_headers_middleware,
        ))
        .layer(cors)
        .with_state(state)
}

/// Starts the `axum` HTTP + SSE server on `host:port`.
pub async fn serve_web(db: DatabaseManager, host: &str, port: u16) -> anyhow::Result<()> {
    let app = create_router(db);
    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;

    #[test]
    fn test_dashboard_pp50_payload_and_four_page_html() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();

        let ingestion = MarketIngestionEngine::new(db.clone());
        let _ = ingestion.ingest_universe(140, true).unwrap();
        let sig_engine = SignalEngine::new(db.clone());
        let _ = sig_engine.evaluate_universe(true).unwrap();

        let pond = PondOrchestrator::new(db.clone(), None);
        let _ = pond.run_scan_cycle(true).unwrap();

        let ctrl = SettleController::new(db.clone(), None);
        let _ = ctrl
            .run_settling(SettleOverrideInput {
                pit_equity_usdc: Some(5250.0),
                pond_equity_usdc: Some(4750.0),
                rich_idle_usdc: Some(1000.0),
                day_index: Some(1),
                dry_run: true,
            })
            .unwrap();

        let payload = build_dashboard_payload(&db).unwrap();
        assert_eq!(payload["version"], PLATFORM_VERSION);
        assert!(payload.get("portfolio").is_some());
        assert!(payload.get("pond").is_some());
        assert!(payload.get("pp50").is_some());
        assert!(payload["pond"]["universe_active_count"].as_u64().unwrap() >= 80);
        assert!(payload["pp50"]["total_platform_equity_usdc"].as_f64().unwrap() > 0.0);
        assert!((payload["pp50"]["settling_tolerance_bps"].as_f64().unwrap() - 50.0).abs() < 1e-6);
        assert_eq!(payload["pp50"]["settling_time_utc"].as_str().unwrap(), "00:05");
        assert!(payload["pp50"]["settling_enabled"].as_bool().unwrap());
        assert!(!payload["pp50"]["settling_kill_switch"].as_bool().unwrap());
        assert!(
            payload["pp50"]["latest_snapshot"]["report_line"]
                .as_str()
                .unwrap()
                .contains("PP-50 D+")
        );

        // Verify enriched Pit signals contain UI fields
        let sig0 = &payload["signals"][0];
        assert!(sig0["symbol"].as_str().is_some());
        assert!(sig0["timeframe"].as_str().is_some());
        assert!(sig0["ensemble_score"].as_f64().is_some());
        assert!(sig0["entry_threshold"].as_f64().is_some());
        assert!(sig0["volatility_tier"].as_str().is_some());

        // Verify enriched Pond cells contain mark price, current funding Z, premium score, and uPnL
        let cell0 = &payload["pond"]["open_cells"][0];
        assert!(cell0["current_price"].as_f64().unwrap() > 0.0);
        assert!(cell0["current_funding_z"].as_f64().is_some());
        assert!(cell0["premium_score"].as_f64().unwrap() > 0.0);
        assert!(cell0["unrealized_pnl_usdc"].as_f64().is_some());

        // Verify enriched Pond universe contains base_asset and rank
        let u0 = &payload["pond"]["universe"][0];
        assert!(u0["base_asset"].as_str().is_some());
        assert!(u0["rank"].as_i64().is_some());

        let html_path = resolve_project_root().join("web/static/index.html");
        let html = fs::read_to_string(html_path).unwrap();
        assert_eq!(html, EMBEDDED_INDEX_HTML);
        for id in [
            "id=\"page-overall\"",
            "id=\"page-pit\"",
            "id=\"page-pond\"",
            "id=\"page-settings\"",
            "pnl-pos",
            "pnl-neg",
            "pnl-flat",
            "tabular-nums",
            "parseUtcToDate",
            "formatLocalTimestamp",
            "formatUtcTimeOfDayToLocal",
            "escapeHtml",
            "Timestamp (Local)",
            "Opened (Local)",
            "Warmup Ready (168h Local)",
            "Updated (Local)",
        ] {
            assert!(html.contains(id), "index.html missing expected marker: {id}");
        }
        assert!(
            !html.contains("Timestamp (UTC)"),
            "index.html should not contain UTC timestamp column headers"
        );

        // Verify dashboard routing consistency when switching between TESTNET, LIVE, and PAPER
        db.configure_hyperliquid_credentials(
            Some("0xef377c1d5444786236aa34fa72e5879372e0eaec"),
            Some("0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"),
            Some(true),
            Some("TESTNET"),
            None,
            None,
            true,
        )
        .unwrap();

        let dash_tn = build_dashboard_payload(&db).unwrap();
        assert_eq!(dash_tn["credentials"]["execution_mode"], "TESTNET");
        assert_eq!(dash_tn["credentials"]["pond_execution_mode"], "TESTNET");
        assert_eq!(dash_tn["credentials"]["active_routing"], "LIVE_TESTNET");
        assert_eq!(dash_tn["pond"]["execution_mode"], "TESTNET");
        assert_eq!(dash_tn["pp50"]["pond"]["execution_mode"], "TESTNET");
    }

    #[tokio::test]
    async fn test_b1_b2_b3_s1_s3_r1_web_security_and_limits() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let state = WebAppState::new(db.clone());
        let token = state.session_token().to_string();
        let restarted_state = WebAppState::new(db.clone());
        assert_eq!(
            restarted_state.session_token(),
            token,
            "session_token must persist in system_config across service restarts"
        );

        // 1. Public index sets HttpOnly session cookie, no-store Cache-Control, and meta token (B1, S3)
        let resp_index = serve_index(State(state.clone())).await;
        assert_eq!(resp_index.status(), StatusCode::OK);
        let set_cookie = resp_index
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(set_cookie.contains(&format!("aegis_session={token}")));
        assert!(set_cookie.contains("HttpOnly"));
        assert!(set_cookie.contains("SameSite=Strict"));
        let cache_ctrl = resp_index
            .headers()
            .get(header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(cache_ctrl.contains("no-store"));

        // 2. Public routes (GET / and GET /api/v1/health) are authorized without tokens (B1)
        let req_root = Request::builder().uri("/").body(Body::empty()).unwrap();
        assert!(is_request_authorized(&state, &req_root));
        let req_health = Request::builder()
            .uri("/api/v1/health")
            .body(Body::empty())
            .unwrap();
        assert!(is_request_authorized(&state, &req_health));

        // 3. Unauthenticated sensitive/state-changing routes are rejected (B1)
        for (method, uri) in [
            ("GET", "/api/v1/dashboard"),
            ("GET", "/api/v1/logs"),
            ("GET", "/api/v1/credentials"),
            ("POST", "/api/v1/config"),
            ("POST", "/api/v1/credentials"),
            ("POST", "/api/v1/engine/cycle"),
        ] {
            let req = Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            assert!(
                !is_request_authorized(&state, &req),
                "Expected unauthenticated {method} {uri} to be rejected"
            );
        }

        // 4. Requests with valid Bearer token, X-Aegis-Token, or session cookie are authorized (B1)
        let req_bearer = Request::builder()
            .uri("/api/v1/dashboard")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        assert!(is_request_authorized(&state, &req_bearer));

        let req_hdr = Request::builder()
            .uri("/api/v1/logs")
            .header("x-aegis-token", &token)
            .body(Body::empty())
            .unwrap();
        assert!(is_request_authorized(&state, &req_hdr));

        let req_cookie = Request::builder()
            .uri("/api/v1/credentials")
            .header(header::COOKIE, format!("aegis_session={token}"))
            .body(Body::empty())
            .unwrap();
        assert!(is_request_authorized(&state, &req_cookie));

        // 5. Immutable security key (enforce_master_key) cannot be modified via web API (S1)
        let resp_immut = api_update_config(
            State(state.clone()),
            Json(ConfigUpdatePayload {
                config_key: "enforce_master_key".to_string(),
                config_value: json!(false),
                scale_risk_caps: false,
            }),
        )
        .await;
        assert_eq!(resp_immut.status(), StatusCode::FORBIDDEN);

        // 6. Sensitive config update is redacted in telemetry_logs and response (B3)
        let secret_token = "123456789:AAH_SUPER_SECRET_BOT_TOKEN_XYZ";
        let resp_secret = api_update_config(
            State(state.clone()),
            Json(ConfigUpdatePayload {
                config_key: "telegram_bot_token".to_string(),
                config_value: json!(secret_token),
                scale_risk_caps: false,
            }),
        )
        .await;
        assert_eq!(resp_secret.status(), StatusCode::OK);
        let body_bytes = axum::body::to_bytes(resp_secret.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        assert!(
            !body_str.contains(secret_token),
            "API response must not echo plaintext secret"
        );
        assert!(body_str.contains("[REDACTED]"));

        let logs = db.get_recent_logs(25).unwrap();
        for l in &logs {
            let msg = l.get("message").and_then(Value::as_str).unwrap_or("");
            assert!(
                !msg.contains(secret_token),
                "telemetry_logs must never contain plaintext secret value"
            );
        }

        // 7. SSE connection cap returns 429 when semaphore is exhausted (R1)
        let capped_state = WebAppState {
            db: db.clone(),
            static_dir: state.static_dir.clone(),
            session_token: token,
            sse_semaphore: Arc::new(tokio::sync::Semaphore::new(1)),
        };
        let _resp_sse_1 = api_sse_stream(State(capped_state.clone())).await;
        let resp_sse_2 = api_sse_stream(State(capped_state)).await;
        assert_eq!(resp_sse_2.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
