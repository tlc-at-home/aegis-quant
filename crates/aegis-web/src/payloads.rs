//! Typed JSON DTOs and dashboard payload builders for `aegis-web`.

use aegis_core::{
    compute_drift_bps, round_dp, BacktestTearSheet, BookTransfer, PondCell, PondScanCycle,
    PondUniverseRow, Pp50PlatformSnapshot, SignalEvaluation, TradeRecord,
};
use aegis_db::DatabaseManager;
use aegis_pit::PitOrchestrator;
use aegis_pond::{PondCycleSummary, PondOrchestrator};
use aegis_settle::SettleRunResult;
use anyhow::{anyhow, Result};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigUpdatePayload {
    pub config_key: String,
    pub config_value: Value,
    #[serde(default)]
    pub scale_risk_caps: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigBatchUpdatePayload {
    pub updates: HashMap<String, Value>,
    #[serde(default)]
    pub scale_risk_caps: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CredentialsUpdatePayload {
    pub execution_mode: Option<String>,
    pub pond_execution_mode: Option<String>,
    pub hyperliquid_testnet: Option<bool>,
    pub wallet_address: Option<String>,
    pub private_key: Option<String>,
    pub hyperliquid_vault_address: Option<String>,
    pub hyperliquid_api_endpoint: Option<String>,
    pub gemini_api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AlertsUpdatePayload {
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub max_daily_drawdown_alert_pct: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct WalletSyncPayload {
    pub wallet_address: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct LimitQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BacktestRequestPayload {
    pub days: Option<usize>,
    pub use_real_history: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SettleRequestPayload {
    pub dry_run: Option<bool>,
    pub pit_equity_usdc: Option<f64>,
    pub pond_equity_usdc: Option<f64>,
    pub pit_idle_usdc: Option<f64>,
    pub pond_idle_usdc: Option<f64>,
    pub rich_idle_usdc: Option<f64>,
    pub day_index: Option<u32>,
    pub confirm_live_settle: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthLoginPayload {
    pub token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthLoginResponse {
    pub status: &'static str,
    pub cookie_secure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiErrorBody {
    pub error: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioDashboardPayload {
    pub initial_equity_usdc: f64,
    pub pit_initial_capital_usdc: f64,
    pub pit_equity_usdc: f64,
    pub pit_idle_usdc: f64,
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub total_pnl_usdc: f64,
    pub total_pnl_pct: f64,
    pub daily_drawdown_pct: f64,
    pub max_daily_drawdown_alert_pct: f64,
    pub deployed_notional_usdc: f64,
    pub gross_leverage: f64,
    pub max_gross_leverage: f64,
    pub portfolio_heat_usdc: f64,
    pub portfolio_heat_pct: f64,
    pub max_portfolio_heat_usdc: f64,
    pub baseline_risk_per_trade_usdc: f64,
    pub min_risk_per_trade_usdc: f64,
    pub max_risk_per_trade_usdc: f64,
    pub single_asset_cap_usdc: f64,
    pub open_positions_count: usize,
    pub max_concurrent_positions: i64,
    pub basis_harvest_notional_usdc: f64,
    pub accrued_yield_usdc: f64,
    pub hmm_regime: String,
    pub prob_bull: f64,
    pub prob_bear: f64,
    pub prob_chop: f64,
    pub circuit_breaker_active: bool,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedPondCell {
    #[serde(flatten)]
    pub cell: PondCell,
    pub current_price: f64,
    pub unrealized_pnl_usdc: f64,
    pub current_funding_z: f64,
    pub premium_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichedSignalRow {
    #[serde(flatten)]
    pub signal: SignalEvaluation,
    pub symbol: String,
    pub timeframe: &'static str,
    pub ensemble_score: f64,
    pub entry_threshold: f64,
    pub volatility_tier: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PondDashboardPayload {
    pub book: String,
    pub active_champion_id: String,
    pub direction_policy: String,
    pub initial_capital_usdc: f64,
    pub pond_equity_usdc: f64,
    pub idle_usdc: f64,
    pub deployed_notional_usdc: f64,
    pub gross_leverage: f64,
    pub max_gross_leverage: f64,
    pub open_cells_count: usize,
    pub max_concurrent_cells: usize,
    pub open_heat_usdc: f64,
    pub max_open_heat_usdc: f64,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub total_pnl_usdc: f64,
    pub total_pnl_pct: f64,
    pub short_call_count: usize,
    pub short_put_count: usize,
    pub universe_active_count: usize,
    pub universe: Vec<PondUniverseRow>,
    pub open_cells: Vec<EnrichedPondCell>,
    pub recent_cells: Vec<EnrichedPondCell>,
    pub scan_cycles: Vec<PondScanCycle>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Pp50BookSummary {
    pub book: &'static str,
    pub label: &'static str,
    pub initial_capital_usdc: f64,
    pub equity_usdc: f64,
    pub idle_usdc: f64,
    pub deployed_notional_usdc: f64,
    pub gross_leverage: f64,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub total_pnl_usdc: f64,
    pub total_pnl_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_positions: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_cells: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrent_cells: Option<usize>,
    pub heat_usdc: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_heat_usdc: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hmm_regime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_champion_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direction_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Pp50DashboardPayload {
    pub platform_initial_equity_usdc: f64,
    pub total_platform_equity_usdc: f64,
    pub total_platform_pnl_usdc: f64,
    pub total_platform_pnl_pct: f64,
    pub pit_share_pct: f64,
    pub pond_share_pct: f64,
    pub drift_bps: f64,
    pub deadband_bps: f64,
    pub settling_time_utc: String,
    pub settling_in_progress: bool,
    pub platform_pp50_halt: bool,
    pub pit: Pp50BookSummary,
    pub pond: Pp50BookSummary,
    pub recent_transfers: Vec<BookTransfer>,
    pub latest_snapshot: Option<Pp50PlatformSnapshot>,
    pub recent_snapshots: Vec<Pp50PlatformSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FullDashboardPayload {
    pub portfolio: PortfolioDashboardPayload,
    pub pond: PondDashboardPayload,
    pub pp50: Pp50DashboardPayload,
    pub signals: Vec<EnrichedSignalRow>,
    pub open_trades: Vec<TradeRecord>,
    pub recent_trades: Vec<TradeRecord>,
    pub config: Vec<Value>,
    pub logs: Vec<Value>,
    pub credentials: Value,
    pub schema_version: i64,
    pub engine_runtime: &'static str,
    pub engine_runtime_detail: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthPayload {
    pub status: &'static str,
    pub service: &'static str,
    pub version: &'static str,
    pub schema_version: i64,
    pub runtime: &'static str,
    pub architecture: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusPayload {
    pub status: &'static str,
    pub service: &'static str,
    pub version: &'static str,
    pub schema_version: i64,
    pub execution_mode: String,
    pub pond_execution_mode: String,
    pub environment: String,
    pub settle_gate_active: bool,
    pub total_platform_equity_usdc: f64,
    pub pit_equity_usdc: f64,
    pub pond_equity_usdc: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PortfolioSummaryPayload {
    pub portfolio: PortfolioDashboardPayload,
    pub open_trades: Vec<TradeRecord>,
    pub credentials: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct SingleConfigUpdateResponse {
    pub status: &'static str,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchConfigUpdateResponse {
    pub status: &'static str,
    pub updated_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CredentialsUpdateResponse {
    pub status: &'static str,
    pub credentials: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct AlertsUpdateResponse {
    pub status: &'static str,
    pub alerts: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletSyncResponse {
    pub status: &'static str,
    pub sync: Value,
    pub credentials: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct CycleTriggerResponse {
    pub status: &'static str,
    pub cycle: Value,
    pub dashboard: FullDashboardPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct PondScanTriggerResponse {
    pub status: &'static str,
    pub scan: PondCycleSummary,
    pub dashboard: FullDashboardPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct SettleTriggerResponse {
    pub status: &'static str,
    pub settle: SettleRunResult,
    pub dashboard: FullDashboardPayload,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestRunResponse {
    pub status: &'static str,
    pub report: BacktestTearSheet,
    pub formatted_report: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CalibrateRunResponse {
    pub status: &'static str,
    pub calibration_report: Value,
}

pub fn map_err_response(err: anyhow::Error) -> Response {
    let msg = err.to_string();
    let status = if msg.starts_with("Validation error:")
        || msg.starts_with("Invalid ")
        || msg.starts_with("Wallet sync failed:")
        || msg.contains("must be")
        || msg.contains("Unknown configuration key")
        || msg.contains("Unknown config_key")
    {
        StatusCode::BAD_REQUEST
    } else if msg.starts_with("Not found:") {
        StatusCode::NOT_FOUND
    } else if msg.starts_with("Forbidden:") || msg.starts_with("Security violation:") {
        StatusCode::FORBIDDEN
    } else if msg.starts_with("Unauthorized") {
        StatusCode::UNAUTHORIZED
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    if status == StatusCode::INTERNAL_SERVER_ERROR {
        tracing::error!(error = %msg, "Unhandled internal error in aegis-web handler");
        let body = ApiErrorBody {
            error: "Internal server error".to_string(),
            detail: "An internal server error occurred; see server logs for details.".to_string(),
        };
        return (status, Json(body)).into_response();
    }

    let body = ApiErrorBody {
        error: msg.clone(),
        detail: msg,
    };
    (status, Json(body)).into_response()
}

pub async fn run_db_blocking<T, F>(db: DatabaseManager, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(DatabaseManager) -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(move || f(db))
        .await
        .map_err(|e| anyhow!("Database worker task join error: {e}"))?
}

pub fn build_portfolio_payload(db: &DatabaseManager) -> Result<PortfolioDashboardPayload> {
    let snap = db.get_latest_portfolio_snapshot()?;
    let cfg = db.load_config_logged("aegis-web.portfolio");
    let max_dd_alert = cfg.get_f64("max_daily_drawdown_alert_pct", 0.03);
    let max_lev = cfg.get_f64("max_gross_leverage", 1.50);
    let max_pos = cfg.get_i64("max_concurrent_positions", 5);

    let init_eq = cfg.get_f64("initial_equity_usdc", 10000.0).max(1.0);
    let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
    let pit_init = cfg.get_f64("pit_initial_capital_usdc", round_dp(init_eq * split_pct, 2));

    let pit_orch = PitOrchestrator::new(db.clone(), None);
    let (pit_eq, pit_idle) = pit_orch
        .compute_pit_equity_and_idle_usdc()
        .unwrap_or((pit_init, pit_init));

    let open_trades = db.get_open_trades().unwrap_or_default();
    let realized_pnl = round_dp(db.get_total_realized_pnl().unwrap_or(0.0), 4);
    let unrealized_pnl = round_dp(open_trades.iter().map(|t| t.unrealized_pnl_usdc).sum(), 4);
    let funding_accrued = round_dp(open_trades.iter().map(|t| t.funding_accrued_usdc).sum(), 4);
    let total_pnl = round_dp(realized_pnl + unrealized_pnl + funding_accrued, 4);
    let total_pnl_pct = if pit_init > 0.0 {
        round_dp((total_pnl / pit_init) * 100.0, 2)
    } else {
        0.0
    };

    let lookback_24h_ms = db.now_ms() - 86_400_000;
    let peak_24h: f64 = db
        .with_conn(|conn| {
            let p: Option<f64> = conn
                .query_row(
                    "SELECT MAX(total_equity_usdc) FROM portfolio_snapshots WHERE timestamp_ms >= ?1;",
                    [lookback_24h_ms],
                    |r| r.get(0),
                )
                .ok()
                .flatten();
            Ok(p.unwrap_or(0.0))
        })
        .unwrap_or(0.0);
    let peak = peak_24h.max(init_eq).max(snap.total_equity_usdc);
    let daily_drawdown_pct = if peak > 0.0 {
        round_dp(((peak - snap.total_equity_usdc).max(0.0)) / peak, 6)
    } else {
        0.0
    };

    let scale = (pit_init / 5000.0).max(0.01);
    let baseline_risk = cfg.get_f64("baseline_risk_per_trade_usdc", round_dp(50.0 * scale, 2));
    let min_risk = cfg.get_f64("min_risk_per_trade_usdc", round_dp(baseline_risk * 0.5, 2));
    let max_risk = cfg.get_f64("max_risk_per_trade_usdc", round_dp(baseline_risk * 1.5, 2));
    let max_heat = cfg.get_f64("max_portfolio_heat_usdc", round_dp(pit_init * 0.04, 2));
    let single_asset_cap = cfg.get_f64(
        "max_single_asset_notional_usdc",
        round_dp(pit_init * 0.25, 2),
    );
    let circuit_breaker_active =
        cfg.get_bool("circuit_breaker_tripped", false) || cfg.get_bool("platform_pp50_halt", false);

    Ok(PortfolioDashboardPayload {
        initial_equity_usdc: init_eq,
        pit_initial_capital_usdc: pit_init,
        pit_equity_usdc: pit_eq,
        pit_idle_usdc: pit_idle,
        total_equity_usdc: snap.total_equity_usdc,
        available_cash_usdc: snap.available_cash_usdc,
        realized_pnl_usdc: realized_pnl,
        unrealized_pnl_usdc: unrealized_pnl,
        funding_accrued_usdc: funding_accrued,
        total_pnl_usdc: total_pnl,
        total_pnl_pct,
        daily_drawdown_pct,
        max_daily_drawdown_alert_pct: max_dd_alert,
        deployed_notional_usdc: snap.deployed_notional_usdc,
        gross_leverage: snap.gross_leverage,
        max_gross_leverage: max_lev,
        portfolio_heat_usdc: snap.portfolio_heat_usdc,
        portfolio_heat_pct: snap.portfolio_heat_pct,
        max_portfolio_heat_usdc: max_heat,
        baseline_risk_per_trade_usdc: baseline_risk,
        min_risk_per_trade_usdc: min_risk,
        max_risk_per_trade_usdc: max_risk,
        single_asset_cap_usdc: single_asset_cap,
        open_positions_count: open_trades.len(),
        max_concurrent_positions: max_pos,
        basis_harvest_notional_usdc: snap.basis_harvest_notional_usdc,
        accrued_yield_usdc: snap.accrued_yield_usdc,
        hmm_regime: snap.hmm_regime,
        prob_bull: snap.prob_bull,
        prob_bear: snap.prob_bear,
        prob_chop: snap.prob_chop,
        circuit_breaker_active,
        timestamp_ms: snap.timestamp_ms,
    })
}

pub fn enrich_pond_cells(db: &DatabaseManager, cells: Vec<PondCell>) -> Vec<EnrichedPondCell> {
    cells
        .into_iter()
        .map(|c| {
            let entry_px = c.entry_price.unwrap_or(0.0);
            let is_open = matches!(c.state.as_str(), "OPEN" | "TRAILED" | "ARMED");
            let current_price = if is_open {
                db.get_ohlcv_bars(&c.symbol, "1h", 1, None)
                    .ok()
                    .and_then(|bars| bars.last().map(|b| b.close))
                    .filter(|&p| p > 0.0)
                    .unwrap_or(entry_px)
            } else {
                c.exit_price.unwrap_or(entry_px)
            };
            let qty = c.quantity.unwrap_or(0.0);
            let unrealized_pnl_usdc =
                if matches!(c.state.as_str(), "OPEN" | "TRAILED") && entry_px > 0.0 && qty > 0.0 {
                    let raw = if c.side.eq_ignore_ascii_case("LONG") {
                        (current_price - entry_px) * qty
                    } else {
                        (entry_px - current_price) * qty
                    };
                    round_dp(raw, 4)
                } else {
                    0.0
                };
            let current_funding_z = c.funding_z_entry.unwrap_or(0.0);
            let premium_score = round_dp(
                c.conviction_multiplier.unwrap_or(1.0) * current_funding_z.abs(),
                2,
            );
            EnrichedPondCell {
                cell: c,
                current_price,
                unrealized_pnl_usdc,
                current_funding_z,
                premium_score,
            }
        })
        .collect()
}

pub fn enrich_signals(
    signals: Vec<SignalEvaluation>,
    entry_threshold: f64,
) -> Vec<EnrichedSignalRow> {
    signals
        .into_iter()
        .map(|s| {
            let symbol = s.exec_symbol.clone();
            let ensemble_score = s.blended_conviction;
            let volatility_tier = format!("RANK_{}", s.rs_rank);
            EnrichedSignalRow {
                signal: s,
                symbol,
                timeframe: "4h",
                ensemble_score,
                entry_threshold,
                volatility_tier,
            }
        })
        .collect()
}

pub fn build_pond_payload(db: &DatabaseManager) -> Result<PondDashboardPayload> {
    let pond_orch = PondOrchestrator::new(db.clone(), None);
    let pond_cfg = pond_orch.load_pond_config()?;
    let (pond_eq, pond_idle) = pond_orch
        .compute_pond_equity_and_idle_usdc()
        .unwrap_or((pond_cfg.pond_baseline_equity_usdc, pond_cfg.pond_baseline_equity_usdc));

    let champion = db.get_active_pond_champion()?;
    let open_cells_raw = db.get_open_pond_cells()?;
    let recent_cells_raw = db.get_pond_cells(50)?;

    let mut universe = db.get_pond_universe(false)?;
    if universe.is_empty() {
        universe = pond_orch.initialize_or_refresh_universe().unwrap_or_default();
    }
    let universe_active_count = universe.iter().filter(|u| u.status == "ACTIVE").count();
    let scan_cycles = db.get_recent_pond_scan_cycles(20)?;

    let short_call_count = open_cells_raw
        .iter()
        .filter(|c| c.side.eq_ignore_ascii_case("SHORT"))
        .count();
    let short_put_count = open_cells_raw
        .iter()
        .filter(|c| c.side.eq_ignore_ascii_case("LONG"))
        .count();

    let open_cells = enrich_pond_cells(db, open_cells_raw);
    let recent_cells = enrich_pond_cells(db, recent_cells_raw);

    let deployed_notional: f64 = round_dp(
        open_cells
            .iter()
            .map(|c| c.cell.notional_usdc.unwrap_or(0.0))
            .sum(),
        2,
    );
    let open_heat: f64 = round_dp(
        open_cells
            .iter()
            .map(|c| c.cell.risk_budget_usdc.unwrap_or(0.0))
            .sum(),
        2,
    );
    let gross_leverage = if pond_eq > 0.0 {
        round_dp(deployed_notional / pond_eq, 4)
    } else {
        0.0
    };

    let realized_pnl = round_dp(db.get_pond_realized_pnl().unwrap_or(0.0), 4);
    let unrealized_pnl: f64 = round_dp(open_cells.iter().map(|c| c.unrealized_pnl_usdc).sum(), 4);
    let funding_accrued: f64 = round_dp(
        open_cells
            .iter()
            .map(|c| c.cell.funding_accrued_usdc)
            .sum(),
        4,
    );

    let init_cap = pond_cfg.pond_baseline_equity_usdc.max(1.0);
    let total_pnl = round_dp(realized_pnl + unrealized_pnl + funding_accrued, 4);
    let total_pnl_pct = if init_cap > 0.0 {
        round_dp((total_pnl / init_cap) * 100.0, 2)
    } else {
        0.0
    };

    Ok(PondDashboardPayload {
        book: "POND".to_string(),
        active_champion_id: champion.version,
        direction_policy: pond_cfg.pond_direction_policy.as_str().to_string(),
        initial_capital_usdc: init_cap,
        pond_equity_usdc: pond_eq,
        idle_usdc: pond_idle,
        deployed_notional_usdc: deployed_notional,
        gross_leverage,
        max_gross_leverage: pond_cfg.pond_max_gross_leverage,
        open_cells_count: open_cells.len(),
        max_concurrent_cells: pond_cfg.pond_max_concurrent_cells,
        open_heat_usdc: open_heat,
        max_open_heat_usdc: pond_cfg.pond_max_heat_usdc,
        realized_pnl_usdc: realized_pnl,
        unrealized_pnl_usdc: unrealized_pnl,
        funding_accrued_usdc: funding_accrued,
        total_pnl_usdc: total_pnl,
        total_pnl_pct,
        short_call_count,
        short_put_count,
        universe_active_count,
        universe,
        open_cells,
        recent_cells,
        scan_cycles,
    })
}

pub fn build_pp50_payload(db: &DatabaseManager) -> Result<Pp50DashboardPayload> {
    let pit_portfolio = build_portfolio_payload(db)?;
    let pond_status = build_pond_payload(db)?;
    build_pp50_payload_with_books(db, &pit_portfolio, &pond_status)
}

pub fn build_pp50_payload_with_books(
    db: &DatabaseManager,
    pit_portfolio: &PortfolioDashboardPayload,
    pond_status: &PondDashboardPayload,
) -> Result<Pp50DashboardPayload> {
    let cfg = db.load_config_logged("aegis-web.pp50");
    let split_pct = cfg.get_f64("settling_split_pct", 0.50).clamp(0.10, 0.90);
    let deadband_bps = DatabaseManager::resolve_settling_deadband_bps(&cfg);
    let settling_time_utc = cfg.get_str("settling_time_utc", "00:05");
    let settling_in_progress = db.is_entry_frozen();
    let platform_pp50_halt = cfg.get_bool("platform_pp50_halt", false);

    let transfers = db.get_recent_book_transfers(20)?;
    let latest_snapshot = db.get_latest_pp50_platform_snapshot().ok();
    let recent_snapshots: Vec<Pp50PlatformSnapshot> =
        latest_snapshot.clone().into_iter().collect();

    let pit_init = pit_portfolio.pit_initial_capital_usdc;
    let pond_init = pond_status.initial_capital_usdc;
    let platform_init = round_dp(pit_init + pond_init, 2);

    let pit_eq = pit_portfolio.pit_equity_usdc;
    let pond_eq = pond_status.pond_equity_usdc;
    let total_eq = round_dp(pit_eq + pond_eq, 2);
    let drift_bps = round_dp(compute_drift_bps(pit_eq, total_eq, split_pct), 2);

    let total_pnl = round_dp(pit_portfolio.total_pnl_usdc + pond_status.total_pnl_usdc, 4);
    let total_pnl_pct = if platform_init > 0.0 {
        round_dp((total_pnl / platform_init) * 100.0, 2)
    } else {
        0.0
    };

    let pit_share_pct = if total_eq > 0.0 {
        round_dp((pit_eq / total_eq) * 100.0, 2)
    } else {
        50.0
    };
    let pond_share_pct = if total_eq > 0.0 {
        round_dp((pond_eq / total_eq) * 100.0, 2)
    } else {
        50.0
    };

    Ok(Pp50DashboardPayload {
        platform_initial_equity_usdc: platform_init,
        total_platform_equity_usdc: total_eq,
        total_platform_pnl_usdc: total_pnl,
        total_platform_pnl_pct: total_pnl_pct,
        pit_share_pct,
        pond_share_pct,
        drift_bps,
        deadband_bps,
        settling_time_utc,
        settling_in_progress,
        platform_pp50_halt,
        pit: Pp50BookSummary {
            book: "PIT",
            label: "Book A — The Pit (Directional MACE+SNR+HMM)",
            initial_capital_usdc: pit_init,
            equity_usdc: pit_eq,
            idle_usdc: pit_portfolio.pit_idle_usdc,
            deployed_notional_usdc: pit_portfolio.deployed_notional_usdc,
            gross_leverage: pit_portfolio.gross_leverage,
            realized_pnl_usdc: pit_portfolio.realized_pnl_usdc,
            unrealized_pnl_usdc: pit_portfolio.unrealized_pnl_usdc,
            funding_accrued_usdc: pit_portfolio.funding_accrued_usdc,
            total_pnl_usdc: pit_portfolio.total_pnl_usdc,
            total_pnl_pct: pit_portfolio.total_pnl_pct,
            open_positions: Some(pit_portfolio.open_positions_count),
            open_cells: None,
            max_concurrent_cells: None,
            heat_usdc: pit_portfolio.portfolio_heat_usdc,
            max_heat_usdc: Some(pit_portfolio.max_portfolio_heat_usdc),
            hmm_regime: Some(pit_portfolio.hmm_regime.clone()),
            active_champion_id: None,
            direction_policy: None,
        },
        pond: Pp50BookSummary {
            book: "POND",
            label: "Book B — The Pond (Champion NPS-01 Naked-Option Perp Scanner)",
            initial_capital_usdc: pond_init,
            equity_usdc: pond_eq,
            idle_usdc: pond_status.idle_usdc,
            deployed_notional_usdc: pond_status.deployed_notional_usdc,
            gross_leverage: pond_status.gross_leverage,
            realized_pnl_usdc: pond_status.realized_pnl_usdc,
            unrealized_pnl_usdc: pond_status.unrealized_pnl_usdc,
            funding_accrued_usdc: pond_status.funding_accrued_usdc,
            total_pnl_usdc: pond_status.total_pnl_usdc,
            total_pnl_pct: pond_status.total_pnl_pct,
            open_positions: None,
            open_cells: Some(pond_status.open_cells_count),
            max_concurrent_cells: Some(pond_status.max_concurrent_cells),
            heat_usdc: pond_status.open_heat_usdc,
            max_heat_usdc: Some(pond_status.max_open_heat_usdc),
            hmm_regime: None,
            active_champion_id: Some(pond_status.active_champion_id.clone()),
            direction_policy: Some(pond_status.direction_policy.clone()),
        },
        recent_transfers: transfers,
        latest_snapshot,
        recent_snapshots,
    })
}

pub fn build_dashboard_payload(db: &DatabaseManager) -> Result<FullDashboardPayload> {
    let portfolio = build_portfolio_payload(db)?;
    let pond = build_pond_payload(db)?;
    let pp50 = build_pp50_payload_with_books(db, &portfolio, &pond)?;
    let cfg = db.load_config_logged("aegis-web.dashboard");
    let entry_thresh = cfg.get_f64("ensemble_entry_threshold", 0.65);

    let signals = enrich_signals(db.get_latest_signals(30).unwrap_or_default(), entry_thresh);
    let open_trades = db.get_open_trades().unwrap_or_default();
    let recent_trades = db.get_recent_trades(25).unwrap_or_default();
    let config = db.get_config_rows(true).unwrap_or_default();
    let logs = db.get_recent_logs(40).unwrap_or_default();
    let credentials = db
        .get_credentials_status()
        .unwrap_or_else(|_| serde_json::json!({}));
    let schema_version = db.get_schema_version().unwrap_or(7);

    Ok(FullDashboardPayload {
        portfolio,
        pond,
        pp50,
        signals,
        open_trades,
        recent_trades,
        config,
        logs,
        credentials,
        schema_version,
        engine_runtime: "RUST_NATIVE",
        engine_runtime_detail: "RUST_NATIVE (Axum / Tokio)",
    })
}
