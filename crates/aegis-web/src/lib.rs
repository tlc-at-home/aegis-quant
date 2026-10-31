//! Institutional FastAPI-Compatible REST & SSE Server (`aegis-web`).
//!
//! 1:1 port of `web/server.py` built on `axum` and `tokio`:
//! - Serves `web/static/index.html` on `GET /`
//! - Exposes all `/api/v1/*` JSON endpoints and real-time Server-Sent Events (`GET /api/v1/stream`)
//! - Zero-overhead shared `DatabaseManager` state with non-blocking `spawn_blocking` execution

use aegis_alerts::AlertEngine;
use aegis_backtest::BacktestEngine;
use aegis_core::{resolve_project_root, AegisError, PLATFORM_NAME, PLATFORM_VERSION};
use aegis_db::DatabaseManager;
use aegis_execution::{ExecutionEngine, MarketIngestionEngine};
use aegis_signals::{SignalEngine, WalkForwardCalibrator};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;

pub const ENGINE_RUNTIME_LABEL: &str = "RUST_NATIVE";
pub const ENGINE_RUNTIME_DETAIL: &str = "RUST_NATIVE (rustc 1.99.1 / Axum)";

#[derive(Clone)]
pub struct WebAppState {
    pub db: DatabaseManager,
    pub static_dir: PathBuf,
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
        AegisError::SecurityViolation(_) => StatusCode::FORBIDDEN,
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

/// Builds the enriched portfolio payload matching `web/server.py::get_portfolio()`.
pub fn build_portfolio_payload(db: &DatabaseManager) -> aegis_core::Result<Value> {
    let snap = db.get_latest_portfolio_snapshot()?;
    let cfg = db.load_config()?;
    let open_trades = db.get_open_trades()?;

    let mut val = serde_json::to_value(&snap).unwrap_or_else(|_| json!({}));
    if let Some(obj) = val.as_object_mut() {
        obj.insert(
            "initial_equity_usdc".to_string(),
            json!(cfg.get_f64("initial_equity_usdc", 10000.0)),
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

/// Builds the unified telemetry payload consumed by `GET /api/v1/dashboard` and `GET /api/v1/stream`.
pub fn build_dashboard_payload(db: &DatabaseManager) -> aegis_core::Result<Value> {
    let portfolio = build_portfolio_payload(db)?;
    let signals = db.get_latest_signals(15)?;
    let open_trades = db.get_open_trades()?;
    let recent_trades = db.get_recent_trades(30)?;
    let alt_pairs = db.get_alt_pairs_universe()?;
    let config_meta = db.get_config_rows(true)?;
    let creds = db.get_credentials_status()?;
    let logs = db.get_recent_logs(25)?;
    let cfg = db.load_config().unwrap_or_default();
    let schema_version = db.get_schema_version().unwrap_or(4);

    Ok(json!({
        "timestamp_ms": db.now_ms(),
        "version": PLATFORM_VERSION,
        "runtime": ENGINE_RUNTIME_LABEL,
        "engine_runtime": ENGINE_RUNTIME_DETAIL,
        "polling_interval_seconds": cfg.get_i64("polling_interval_seconds", 15),
        "portfolio": portfolio,
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

async fn serve_index(State(state): State<WebAppState>) -> Response {
    let index_path = state.static_dir.join("index.html");
    match fs::read_to_string(&index_path) {
        Ok(html) => Html(html).into_response(),
        Err(_) => (
            StatusCode::NOT_FOUND,
            Html("<h1>UI Static File Not Found</h1>".to_string()),
        )
            .into_response(),
    }
}

async fn api_status(State(state): State<WebAppState>) -> Response {
    let db = state.db.clone();
    let integrity = match db.verify_integrity() {
        Ok(i) => i,
        Err(e) => return map_err_response(e),
    };
    let cfg = match db.load_config() {
        Ok(c) => c,
        Err(e) => return map_err_response(e),
    };
    let snap = match db.get_latest_portfolio_snapshot() {
        Ok(s) => s,
        Err(e) => return map_err_response(e),
    };
    let creds = match db.get_credentials_status() {
        Ok(c) => c,
        Err(e) => return map_err_response(e),
    };

    Json(json!({
        "status": "ok",
        "state": "ONLINE",
        "platform": cfg.get_str("platform_name", PLATFORM_NAME),
        "version": PLATFORM_VERSION,
        "release": format!("v{PLATFORM_VERSION}"),
        "runtime": ENGINE_RUNTIME_LABEL,
        "engine_runtime": ENGINE_RUNTIME_DETAIL,
        "schema_version": integrity.get("schema_version").and_then(Value::as_i64).unwrap_or(4),
        "exchange": cfg.get_str("exchange_id", "hyperliquid"),
        "execution_mode": creds.get("execution_mode").and_then(Value::as_str).unwrap_or("PAPER"),
        "hyperliquid_testnet": creds.get("hyperliquid_testnet").and_then(Value::as_bool).unwrap_or(false),
        "network": creds.get("network").and_then(Value::as_str).unwrap_or("mainnet"),
        "api_endpoint": creds.get("api_endpoint").and_then(Value::as_str).unwrap_or("https://api.hyperliquid.xyz"),
        "credentials_ready": creds.get("credentials_ready").and_then(Value::as_bool).unwrap_or(false),
        "active_routing": creds.get("active_routing").and_then(Value::as_str).unwrap_or("PAPER_SIMULATION"),
        "alert_channel": "TELEGRAM",
        "telegram_alerts_ready": creds.get("telegram_alerts_ready").and_then(Value::as_bool).unwrap_or(false),
        "environment": cfg.get_str("environment", "sandbox"),
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
    .into_response()
}

async fn api_portfolio(State(state): State<WebAppState>) -> Response {
    match build_portfolio_payload(&state.db) {
        Ok(payload) => Json(payload).into_response(),
        Err(e) => map_err_response(e),
    }
}

async fn api_signals(State(state): State<WebAppState>) -> Response {
    let signals = match state.db.get_latest_signals(15) {
        Ok(s) => s,
        Err(e) => return map_err_response(e),
    };
    let snap = match state.db.get_latest_portfolio_snapshot() {
        Ok(s) => s,
        Err(e) => return map_err_response(e),
    };
    Json(json!({
        "hmm_regime": snap.hmm_regime,
        "prob_bull": snap.prob_bull,
        "prob_bear": snap.prob_bear,
        "prob_chop": snap.prob_chop,
        "count": signals.len(),
        "signals": signals,
    }))
    .into_response()
}

async fn api_universe(State(state): State<WebAppState>) -> Response {
    let usdc_perp = match state.db.get_usdc_perp_universe() {
        Ok(u) => u,
        Err(e) => return map_err_response(e),
    };
    let btc_synth = match state.db.get_btc_synth_universe() {
        Ok(b) => b,
        Err(e) => return map_err_response(e),
    };
    let alt_pairs = match state.db.get_alt_pairs_universe() {
        Ok(a) => a,
        Err(e) => return map_err_response(e),
    };
    Json(json!({
        "usdc_perp_count": usdc_perp.len(),
        "btc_synth_count": btc_synth.len(),
        "alt_pairs_count": alt_pairs.len(),
        "usdc_perp": usdc_perp,
        "btc_synth": btc_synth,
        "alt_pairs": alt_pairs,
    }))
    .into_response()
}

async fn api_trades(
    State(state): State<WebAppState>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let open_trades = match state.db.get_open_trades() {
        Ok(t) => t,
        Err(e) => return map_err_response(e),
    };
    let recent_trades = match state.db.get_recent_trades(limit) {
        Ok(t) => t,
        Err(e) => return map_err_response(e),
    };
    Json(json!({
        "open_count": open_trades.len(),
        "open_trades": open_trades,
        "recent_trades": recent_trades,
    }))
    .into_response()
}

async fn api_logs(
    State(state): State<WebAppState>,
    Query(q): Query<LimitQuery>,
) -> Response {
    let limit = q.limit.unwrap_or(50).clamp(1, 250);
    match state.db.get_recent_logs(limit) {
        Ok(logs) => Json(json!({
            "count": logs.len(),
            "logs": logs,
        }))
        .into_response(),
        Err(e) => map_err_response(e),
    }
}

async fn api_dashboard(State(state): State<WebAppState>) -> Response {
    match build_dashboard_payload(&state.db) {
        Ok(payload) => Json(payload).into_response(),
        Err(e) => map_err_response(e),
    }
}

async fn api_get_config(State(state): State<WebAppState>) -> Response {
    match state.db.get_config_rows(true) {
        Ok(rows) => Json(json!({
            "count": rows.len(),
            "config": rows,
            "items": rows,
        }))
        .into_response(),
        Err(e) => map_err_response(e),
    }
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
        match state.db.scale_risk_caps_for_equity(eq) {
            Ok(caps) => scaled_caps = Some(caps),
            Err(e) => return map_err_response(e),
        }
    } else {
        match state.db.set_config_value(&key, &payload.config_value) {
            Ok(false) => {
                return map_err_response(AegisError::NotFound(format!(
                    "Unknown config_key: {key}"
                )))
            }
            Ok(true) => {}
            Err(e) => return map_err_response(e),
        }
    }

    let _ = state.db.log_telemetry(
        "INFO",
        "web.api",
        &format!("Updated system_config[{key}] = {}", payload.config_value),
        None,
    );
    let meta = state.db.get_config_rows(true).unwrap_or_default();
    Json(json!({
        "status": "updated",
        "config_key": key,
        "config_value": payload.config_value,
        "scaled_caps": scaled_caps,
        "config": meta,
    }))
    .into_response()
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
    match state
        .db
        .set_config_batch(&payload.updates, payload.scale_risk_caps)
    {
        Ok(updated_keys) => {
            let _ = state.db.log_telemetry(
                "INFO",
                "web.api",
                &format!(
                    "Batch-updated {} system_config keys (scale_risk_caps={})",
                    updated_keys.len(),
                    payload.scale_risk_caps
                ),
                Some(json!({ "updated_keys": updated_keys })),
            );
            let meta = state.db.get_config_rows(true).unwrap_or_default();
            let snap = build_portfolio_payload(&state.db).ok();
            Json(json!({
                "status": "updated",
                "count": updated_keys.len(),
                "updated_keys": updated_keys,
                "config": meta,
                "portfolio": snap,
            }))
            .into_response()
        }
        Err(e) => map_err_response(e),
    }
}

async fn api_get_credentials(State(state): State<WebAppState>) -> Response {
    match state.db.get_credentials_status() {
        Ok(creds) => Json(creds).into_response(),
        Err(e) => map_err_response(e),
    }
}

async fn api_update_credentials(
    State(state): State<WebAppState>,
    Json(payload): Json<CredentialsUpdatePayload>,
) -> Response {
    let res = state.db.configure_hyperliquid_credentials(
        payload.wallet_address.as_deref(),
        payload.private_key.as_deref(),
        payload.hyperliquid_testnet,
        payload.execution_mode.as_deref(),
        payload.hyperliquid_vault_address.as_deref(),
        payload.gemini_api_key.as_deref(),
        true,
    );
    let mut status = match res {
        Ok(s) => s,
        Err(e) => return map_err_response(e),
    };

    if payload.telegram_bot_token.is_some()
        || payload.telegram_chat_id.is_some()
        || payload.max_daily_drawdown_alert_pct.is_some()
    {
        match state.db.configure_telegram_alerts(
            payload.telegram_bot_token.as_deref(),
            payload.telegram_chat_id.as_deref(),
            payload.max_daily_drawdown_alert_pct,
        ) {
            Ok(s) => status = s,
            Err(e) => return map_err_response(e),
        }
    }

    let _ = state.db.log_telemetry(
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

    let cfg_meta = state.db.get_config_rows(true).unwrap_or_default();
    Json(json!({
        "status": "updated",
        "credentials": status,
        "config": cfg_meta,
    }))
    .into_response()
}

async fn api_get_alerts(State(state): State<WebAppState>) -> Response {
    match state.db.get_credentials_status() {
        Ok(status) => Json(json!({
            "alert_channel": "TELEGRAM",
            "telegram_bot_token_masked": status.get("telegram_bot_token_masked").cloned().unwrap_or(json!("")),
            "telegram_bot_token_configured": status.get("telegram_bot_token_configured").cloned().unwrap_or(json!(false)),
            "telegram_chat_id": status.get("telegram_chat_id").cloned().unwrap_or(json!("")),
            "telegram_alerts_ready": status.get("telegram_alerts_ready").cloned().unwrap_or(json!(false)),
            "max_daily_drawdown_alert_pct": status.get("max_daily_drawdown_alert_pct").cloned().unwrap_or(json!(0.03)),
        }))
        .into_response(),
        Err(e) => map_err_response(e),
    }
}

async fn api_update_alerts(
    State(state): State<WebAppState>,
    Json(payload): Json<AlertsUpdatePayload>,
) -> Response {
    match state.db.configure_telegram_alerts(
        payload.telegram_bot_token.as_deref(),
        payload.telegram_chat_id.as_deref(),
        payload.max_daily_drawdown_alert_pct,
    ) {
        Ok(status) => {
            let _ = state.db.log_telemetry(
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
            Json(json!({
                "status": "updated",
                "alerts": status,
            }))
            .into_response()
        }
        Err(e) => map_err_response(e),
    }
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

        ingestion.ingest_universe(140, false)?;
        let evals = signals.evaluate_universe(true)?;
        execution.reconcile_cycle(&evals, false)?;
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

async fn api_sse_stream(
    State(state): State<WebAppState>,
) -> Sse<impl tokio_stream::Stream<Item = std::result::Result<Event, Infallible>>> {
    let db = state.db.clone();
    let stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(3))).map(move |_| {
        let data = match build_dashboard_payload(&db) {
            Ok(payload) => payload.to_string(),
            Err(e) => json!({ "error": e.to_string() }).to_string(),
        };
        Ok(Event::default().data(data))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Constructs the complete `axum::Router` for Aegis Quant WebUI & REST API.
pub fn create_router(db: DatabaseManager) -> Router {
    let static_dir = resolve_project_root().join("web").join("static");
    let state = WebAppState { db, static_dir };

    Router::new()
        .route("/", get(serve_index))
        .route("/api/v1/status", get(api_status))
        .route("/api/v1/health", get(api_status))
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
        .layer(CorsLayer::permissive())
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
