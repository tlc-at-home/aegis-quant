//! Domain models matching the 11 MariaDB tables and 7 first-class SQL Views (`aegis-core::models`).

use crate::time::round_dp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Typed USDC amount newtype preventing unit confusion in risk sizing and equity accounting.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Usdc(pub f64);

impl Usdc {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub fn new(val: f64) -> Self {
        if val.is_finite() {
            Self(round_dp(val, 4))
        } else {
            Self(0.0)
        }
    }

    #[inline]
    pub const fn as_f64(self) -> f64 {
        self.0
    }
}

impl fmt::Display for Usdc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.2}", self.0)
    }
}

/// Position/Order side (`LONG` or `SHORT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderSide {
    Long,
    Short,
}

impl OrderSide {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_uppercase().as_str() {
            "SHORT" | "SELL" | "A" => Self::Short,
            _ => Self::Long,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
        }
    }

    pub fn is_buy(&self) -> bool {
        matches!(self, Self::Long)
    }

    pub fn sign(&self) -> f64 {
        match self {
            Self::Long => 1.0,
            Self::Short => -1.0,
        }
    }
}

/// Position & Order Capital Lifecycle Status (`WORKING`, `OPEN`, `CLOSED`, `CANCELLED`).
/// - `WORKING`: Order placed on book; margin is reserved in `reserved_margin_usdc`, not yet deducted from cash into committed notional.
/// - `OPEN`: Exchange fill confirmed; capital is spent/committed into `committed_notional_usdc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PositionStatus {
    Working,
    Open,
    Closed,
    Cancelled,
}

impl PositionStatus {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_uppercase().as_str() {
            "WORKING" => Self::Working,
            "OPEN" => Self::Open,
            "CANCELLED" | "CANCELED" => Self::Cancelled,
            _ => Self::Closed,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Working => "WORKING",
            Self::Open => "OPEN",
            Self::Closed => "CLOSED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

/// 1s Maker Limit Chaser FSM state (`WORKING_ALO`, `CHASING_ALO`, `CROSSING_IOC`, `FILLED_OPEN`, `STOP_TRIGGERED`, `CLOSED`, `CANCELLED`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderFsmState {
    WorkingAlo,
    ChasingAlo,
    CrossingIoc,
    FilledOpen,
    StopTriggered,
    Closed,
    Cancelled,
}

impl OrderFsmState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WorkingAlo => "WORKING_ALO",
            Self::ChasingAlo => "CHASING_ALO",
            Self::CrossingIoc => "CROSSING_IOC",
            Self::FilledOpen => "FILLED_OPEN",
            Self::StopTriggered => "STOP_TRIGGERED",
            Self::Closed => "CLOSED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

// ============================================================================
// 11 MARIADB TABLE DOMAIN MODELS
// ============================================================================

/// Table 1: `system_config`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfigRow {
    pub id: i64,
    pub config_key: String,
    pub config_value: String,
    pub value_type: String,
    pub description: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 2: `exchange_assets`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeAsset {
    pub id: i64,
    pub symbol: String, // Canonical: "BTC-USDC", "SOL-USDC"
    pub coin: String,   // Raw coin: "BTC", "SOL"
    pub asset_index: u32,
    pub sz_decimals: u32,
    pub tick_size: f64,
    pub lot_step_size: f64,
    pub max_leverage: f64,
    pub mark_price: f64,
    pub mid_price: f64,
    pub oracle_price: f64,
    pub funding_rate_hourly: f64,
    pub day_volume_usdc: f64,
    pub open_interest_usdc: f64,
    pub prev_day_price: f64,
    pub is_core_eligible: bool,
    pub is_event_eligible: bool,
    pub is_priority_watched: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 3: `asset_cooldowns`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetCooldown {
    pub id: i64,
    pub asset_id: i64,
    pub cooldown_type: String, // "STOP_OUT", "VOL_SHOCK", "CHURN_GUARD", "MANUAL"
    pub reason: String,
    pub triggered_at_ms: i64,
    pub expires_at_ms: i64,
    pub is_active: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 4: `ohlcv_bars`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OhlcvBar {
    pub id: i64,
    pub asset_id: i64,
    pub timeframe: String,
    pub timestamp_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub funding_rate: f64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 5: `sleeve_signals`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleeveSignal {
    pub id: i64,
    pub asset_id: i64,
    pub mark_price: f64,
    pub day_volume_usdc: f64,
    pub funding_rate_hourly: f64,
    pub annualized_vol: f64,
    pub market_beta: f64,
    pub s1_mom_score: f64,
    pub s1_carry_score: f64,
    pub s2_trend_alignment: i32,
    pub s2_vol_surge_ratio: f64,
    pub s3_funding_zscore: f64,
    pub s3_flush_atr_mult: f64,
    pub w_s1_carry_mom: f64,
    pub w_s2_trend_breakout: f64,
    pub w_s3_event_reversion: f64,
    pub raw_combined_weight: f64,
    pub target_weight: f64,
    pub current_weight: f64,
    pub delta_weight: f64,
    pub event_trigger: Option<String>,
    pub rebalance_action: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 6: `positions` (WORKING reserves margin; OPEN commits funds)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRecord {
    pub id: i64,
    pub asset_id: i64,
    pub client_order_id: String,
    pub side: String,
    pub status: String,
    pub fsm_state: String,
    pub entry_price: f64,
    pub current_price: f64,
    pub exit_price: Option<f64>,
    pub requested_quantity: f64,
    pub filled_quantity: f64,
    pub reserved_margin_usdc: f64,
    pub committed_notional_usdc: f64,
    pub portfolio_weight: f64,
    pub s1_weight_contrib: f64,
    pub s2_weight_contrib: f64,
    pub s3_weight_contrib: f64,
    pub protective_stop_px: f64,
    pub protective_stop_oid: Option<String>,
    #[serde(default)]
    pub take_profit_px: f64,
    #[serde(default)]
    pub take_profit_oid: Option<String>,
    pub highest_price_seen: f64,
    pub lowest_price_seen: f64,
    pub chase_count: u32,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub fees_paid_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub slippage_paid_usdc: f64,
    pub exchange_order_id: Option<String>,
    pub created_at_ms: i64,
    pub opened_at_ms: Option<i64>,
    pub closed_at_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub exit_reason: Option<String>,
}

/// Table 7: `order_audit`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAuditRecord {
    pub id: i64,
    pub position_id: i64,
    pub asset_id: i64,
    pub client_order_id: String,
    pub side: String,
    pub from_state: String,
    pub to_state: String,
    pub requested_qty: f64,
    pub filled_qty: f64,
    pub limit_price: f64,
    pub avg_fill_price: f64,
    pub reserved_usdc: f64,
    pub committed_usdc: f64,
    pub fees_usdc: f64,
    pub reason: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 8: `portfolio_snapshots`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub id: i64,
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub reserved_order_cash_usdc: f64,
    pub committed_notional_usdc: f64,
    pub net_notional_usdc: f64,
    pub gross_leverage_x: f64,
    pub net_leverage_x: f64,
    pub market_beta_exposure: f64,
    pub market_breadth_bull: f64,
    pub market_regime_label: String,
    pub risk_tier: String,
    pub leverage_multiplier: f64,
    pub s1_gross_weight: f64,
    pub s2_gross_weight: f64,
    pub s3_gross_weight: f64,
    pub working_order_count: i32,
    pub open_long_count: i32,
    pub open_short_count: i32,
    pub eligible_coin_count: i32,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 9: `telemetry_logs`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryLogRecord {
    pub id: i64,
    pub level: String,
    pub component: String,
    pub message: String,
    pub details_json: Option<Value>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Table 10: `alert_queue`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertQueueRecord {
    pub id: i64,
    pub provider: String,
    pub event_type: String,
    pub severity: String,
    pub message: String,
    pub payload_json: Option<Value>,
    pub status: String,
    pub attempts: i32,
    pub next_retry_ms: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

// ============================================================================
// 7 FIRST-CLASS SQL VIEW DOMAIN MODELS
// ============================================================================

/// View 1: `v_assets_held` (All active OPEN holdings with live mark, stop distance & sleeve attribution)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeldAssetView {
    pub position_id: i64,
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub client_order_id: String,
    pub side: String,
    pub quantity: f64,
    pub committed_notional_usdc: f64,
    pub portfolio_weight: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub protective_stop_px: f64,
    #[serde(default)]
    pub protective_stop_oid: Option<String>,
    pub stop_distance_pct: Option<f64>,
    #[serde(default)]
    pub take_profit_px: f64,
    #[serde(default)]
    pub take_profit_oid: Option<String>,
    #[serde(default)]
    pub tp_distance_pct: Option<f64>,
    pub highest_price_seen: f64,
    pub lowest_price_seen: f64,
    pub unrealized_pnl_usdc: f64,
    #[serde(default)]
    pub unrealized_pnl_pct: f64,
    pub realized_pnl_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub fees_paid_usdc: f64,
    pub s1_weight_contrib: f64,
    pub s2_weight_contrib: f64,
    pub s3_weight_contrib: f64,
    pub latest_target_weight: f64,
    pub latest_delta_weight: f64,
    pub opened_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

/// View 2: `v_assets_ordered` (All WORKING Maker Limit Chaser orders with reserved margin & book distance)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderedAssetView {
    pub position_id: i64,
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub side: String,
    pub fsm_state: String,
    pub requested_quantity: f64,
    pub filled_quantity: f64,
    pub reserved_margin_usdc: f64,
    pub limit_price: f64,
    pub mid_price: f64,
    pub mark_price: f64,
    pub distance_from_mid_bps: f64,
    #[serde(default)]
    pub protective_stop_px: f64,
    #[serde(default)]
    pub take_profit_px: f64,
    pub chase_count: u32,
    pub s1_weight_contrib: f64,
    pub s2_weight_contrib: f64,
    pub s3_weight_contrib: f64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// View 3: `v_assets_in_cooldown` (Assets currently blocked by active post-stop / shock / churn cooldown)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CooldownAssetView {
    pub cooldown_id: i64,
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub cooldown_type: String,
    pub reason: String,
    pub triggered_at_ms: i64,
    pub expires_at_ms: i64,
    pub remaining_seconds: i64,
    pub updated_at_ms: i64,
}

/// View 4: `v_assets_sleeve_s1` (Sleeve 1: Cross-Sectional Carry-Momentum & Market-Beta Hedge)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleeveS1AssetView {
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub mark_price: f64,
    pub day_volume_usdc: f64,
    pub funding_rate_hourly: f64,
    pub funding_apr_pct: f64,
    pub annualized_vol: f64,
    pub market_beta: f64,
    pub s1_mom_score: f64,
    pub s1_carry_score: f64,
    pub w_s1_carry_mom: f64,
    pub target_weight: f64,
    pub current_weight: f64,
    pub updated_at_ms: i64,
}

/// View 5: `v_assets_sleeve_s2` (Sleeve 2: Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleeveS2AssetView {
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub mark_price: f64,
    pub day_volume_usdc: f64,
    pub annualized_vol: f64,
    pub s2_trend_alignment: i32,
    pub s2_vol_surge_ratio: f64,
    pub w_s2_trend_breakout: f64,
    pub event_trigger: Option<String>,
    pub target_weight: f64,
    pub current_weight: f64,
    pub updated_at_ms: i64,
}

/// View 6: `v_assets_sleeve_s3` (Sleeve 3: Live Capitulation Flush & Funding Dislocation Harvester)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SleeveS3AssetView {
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub mark_price: f64,
    pub day_volume_usdc: f64,
    pub funding_rate_hourly: f64,
    pub funding_apr_pct: f64,
    pub s3_funding_zscore: f64,
    pub s3_flush_atr_mult: f64,
    pub w_s3_event_reversion: f64,
    pub event_trigger: Option<String>,
    pub target_weight: f64,
    pub current_weight: f64,
    pub updated_at_ms: i64,
}

/// View 7: `v_priority_watchlist` (All assets under 1-Second High-Frequency Surveillance)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityWatchView {
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub mark_price: f64,
    pub mid_price: f64,
    pub funding_rate_hourly: f64,
    pub position_id: Option<i64>,
    pub position_status: Option<String>,
    pub position_side: Option<String>,
    pub fsm_state: Option<String>,
    pub entry_price: Option<f64>,
    pub requested_quantity: Option<f64>,
    pub filled_quantity: Option<f64>,
    pub reserved_margin_usdc: Option<f64>,
    pub committed_notional_usdc: Option<f64>,
    pub protective_stop_px: Option<f64>,
    #[serde(default)]
    pub protective_stop_oid: Option<String>,
    #[serde(default)]
    pub take_profit_px: Option<f64>,
    #[serde(default)]
    pub take_profit_oid: Option<String>,
    pub unrealized_pnl_usdc: Option<f64>,
    pub target_weight: f64,
    pub event_trigger: Option<String>,
    pub updated_at_ms: i64,
}

/// Readiness / diagnostic check item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResult {
    pub gate: String,
    pub check_id: String,
    pub passed: bool,
    pub detail: String,
}

/// Performance & PnL metrics for a single time horizon (`daily`, `weekly`, `monthly`, `ytd`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnlHorizonMetrics {
    pub horizon_label: String,
    pub window_start_ms: i64,
    pub baseline_equity_usdc: f64,
    pub net_pnl_usdc: f64,
    pub return_pct: f64,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    pub fees_funding_usdc: f64,
    pub trade_count: usize,
    pub win_count: usize,
    #[serde(default)]
    pub is_cap_clamped: bool,
    #[serde(default)]
    pub pnl_source: String,
}

/// Multi-horizon PnL breakdown (`Daily 24h`, `Weekly 7d`, `Monthly 30d`, `YTD`) for the operator dashboard.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnlSummary {
    pub daily: PnlHorizonMetrics,
    pub weekly: PnlHorizonMetrics,
    pub monthly: PnlHorizonMetrics,
    pub ytd: PnlHorizonMetrics,
}

/// Compact time-series window parsed from Hyperliquid `/info {"type":"portfolio"}` (`perpDay`, `perpWeek`, `perpMonth`, `perpAllTime`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct HlPortfolioWindowCache {
    #[serde(default)]
    pub pnl_history: Vec<(i64, f64)>,
    #[serde(default)]
    pub vlm: f64,
}

/// Cached Hyperliquid `portfolio` perp PnL histories persisted by reconciler (`live_exchange_portfolio_pnl_json`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CachedHlPortfolioPnl {
    #[serde(default)]
    pub synced_at_ms: i64,
    #[serde(default)]
    pub perp_day: HlPortfolioWindowCache,
    #[serde(default)]
    pub perp_week: HlPortfolioWindowCache,
    #[serde(default)]
    pub perp_month: HlPortfolioWindowCache,
    #[serde(default)]
    pub perp_all_time: HlPortfolioWindowCache,
}

impl CachedHlPortfolioPnl {
    /// Returns `true` when `synced_at_ms` is valid and no older than `max_age_ms` at `now_ms_val`.
    pub fn is_fresh(&self, now_ms_val: i64, max_age_ms: i64) -> bool {
        self.synced_at_ms > 0
            && now_ms_val >= self.synced_at_ms
            && now_ms_val.saturating_sub(self.synced_at_ms) <= max_age_ms
    }

    /// Linearly interpolates a cumulative PnL series sorted ascending by timestamp `(ts_ms, pnl)`.
    pub fn interpolate_series_pnl(pts: &[(i64, f64)], target_ms: i64) -> Option<f64> {
        let first = *pts.first()?;
        let last = *pts.last()?;
        if target_ms <= first.0 {
            return first.1.is_finite().then_some(first.1);
        }
        if target_ms >= last.0 {
            return last.1.is_finite().then_some(last.1);
        }
        for pair in pts.windows(2) {
            let (t0, p0) = pair[0];
            let (t1, p1) = pair[1];
            if t0 <= target_ms && target_ms <= t1 {
                if !p0.is_finite() || !p1.is_finite() {
                    return None;
                }
                if t1 <= t0 {
                    return Some(p1);
                }
                let frac = (target_ms - t0) as f64 / (t1 - t0) as f64;
                return Some(p0 + (p1 - p0) * frac);
            }
        }
        None
    }

    /// Resolves `(net_pnl_usdc, window_start_ms, is_cap_clamped)` for `"daily"`, `"weekly"`, `"monthly"`, or `"ytd"`.
    pub fn horizon_net_pnl(
        &self,
        horizon: &str,
        raw_start_ms: i64,
        cap_updated_at_ms: i64,
    ) -> Option<(f64, i64, bool)> {
        let is_cap_clamped = cap_updated_at_ms > 0 && cap_updated_at_ms > raw_start_ms;
        if is_cap_clamped {
            if self.synced_at_ms < cap_updated_at_ms {
                return None;
            }
            let candidates = [
                &self.perp_day,
                &self.perp_week,
                &self.perp_month,
                &self.perp_all_time,
            ];
            let series = candidates
                .iter()
                .copied()
                .find(|w| {
                    w.pnl_history
                        .first()
                        .is_some_and(|&(first_ts, _)| first_ts <= cap_updated_at_ms)
                })
                .or_else(|| {
                    (!self.perp_all_time.pnl_history.is_empty()).then_some(&self.perp_all_time)
                })?;
            let (_, last_pnl) = *series.pnl_history.last()?;
            let cap_pnl = Self::interpolate_series_pnl(&series.pnl_history, cap_updated_at_ms)?;
            let net = last_pnl - cap_pnl;
            return net.is_finite().then_some((net, cap_updated_at_ms, true));
        }

        let series = match horizon {
            "daily" => &self.perp_day,
            "weekly" => &self.perp_week,
            "monthly" => &self.perp_month,
            "ytd" => &self.perp_all_time,
            _ => return None,
        };
        let &(first_ts, first_pnl) = series.pnl_history.first()?;
        let &(_, last_pnl) = series.pnl_history.last()?;
        if !first_pnl.is_finite() || !last_pnl.is_finite() {
            return None;
        }
        if horizon == "ytd" && first_ts < raw_start_ms {
            let start_pnl = Self::interpolate_series_pnl(&series.pnl_history, raw_start_ms)?;
            let net = last_pnl - start_pnl;
            return net.is_finite().then_some((net, raw_start_ms, false));
        }
        let net = last_pnl - first_pnl;
        net.is_finite().then_some((net, first_ts, false))
    }
}

/// Explicit equity & capital-sizing basis summary for UI transparency and reconciler sync status.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EquityBasisSummary {
    #[serde(default)]
    pub basis_label: String,
    #[serde(default)]
    pub effective_equity_usdc: f64,
    #[serde(default)]
    pub perp_wallet_equity_usdc: f64,
    #[serde(default)]
    pub risk_capital_cap_usdc: f64,
    #[serde(default)]
    pub is_cap_binding: bool,
    #[serde(default)]
    pub spot_usdc: f64,
    #[serde(default)]
    pub total_wallet_equity_usdc: f64,
    #[serde(default)]
    pub margin_used_usdc: f64,
    #[serde(default)]
    pub withdrawable_usdc: f64,
    #[serde(default)]
    pub synced_at_ms: i64,
    #[serde(default)]
    pub is_stale: bool,
    #[serde(default)]
    pub account_mode: String,
    #[serde(default)]
    pub is_unified_account: bool,
    #[serde(default)]
    pub perp_margin_equity_usdc: f64,
}

/// 24-hour peak-to-trough portfolio drawdown circuit-breaker status.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DrawdownBreakerStatus {
    #[serde(default)]
    pub tripped: bool,
    #[serde(default)]
    pub drawdown_pct: f64,
    #[serde(default)]
    pub max_daily_drawdown_pct: f64,
    #[serde(default)]
    pub peak_24h_equity_usdc: f64,
    #[serde(default)]
    pub latest_equity_usdc: f64,
    #[serde(default)]
    pub window_start_ms: i64,
}

/// Closed trade summary enriched with canonical symbol/coin and computed `realized_pnl_pct`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedPositionView {
    pub position_id: i64,
    pub asset_id: i64,
    pub symbol: String,
    pub coin: String,
    pub client_order_id: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub entry_notional_usdc: f64,
    pub realized_pnl_usdc: f64,
    pub realized_pnl_pct: f64,
    pub fees_paid_usdc: f64,
    pub funding_accrued_usdc: f64,
    pub opened_at_ms: Option<i64>,
    pub closed_at_ms: i64,
    pub exit_reason: Option<String>,
}

/// Individual Hyperliquid vault follower position details.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VaultPositionInfo {
    #[serde(default)]
    pub vault_address: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub leader: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub equity_usdc: f64,
    #[serde(default)]
    pub pnl_usdc: f64,
    #[serde(default)]
    pub all_time_pnl_usdc: f64,
    #[serde(default)]
    pub pnl_pct: f64,
    #[serde(default)]
    pub all_time_pnl_pct: f64,
    #[serde(default)]
    pub apr_pct: f64,
    #[serde(default)]
    pub days_following: u64,
    #[serde(default)]
    pub locked_until_ms: i64,
    #[serde(default)]
    pub is_locked: bool,
    #[serde(default)]
    pub is_closed: bool,
    #[serde(default)]
    pub tvl_usdc: f64,
    #[serde(default)]
    pub follower_count: usize,
    #[serde(default)]
    pub followers_capped: bool,
    #[serde(default)]
    pub leader_fraction_pct: f64,
    #[serde(default)]
    pub leader_commission_pct: f64,
    #[serde(default)]
    pub allow_deposits: bool,
    #[serde(default)]
    pub always_close_on_withdraw: bool,
    #[serde(default)]
    pub max_distributable_usdc: f64,
    #[serde(default)]
    pub max_withdrawable_usdc: f64,
    #[serde(default)]
    pub relationship_type: String,
    #[serde(default)]
    pub child_vault_count: usize,
    #[serde(default)]
    pub all_time_volume_usdc: f64,
}

/// Aggregated Hyperliquid vault portfolio summary (segregated from perp margin).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VaultInfoPayload {
    #[serde(default)]
    pub configured_vault_address: String,
    #[serde(default)]
    pub total_vault_equity_usdc: f64,
    #[serde(default)]
    pub combined_portfolio_equity_usdc: f64,
    #[serde(default)]
    pub vault_count: usize,
    #[serde(default)]
    pub synced_at_ms: i64,
    #[serde(default)]
    pub vaults: Vec<VaultPositionInfo>,
}

/// Resolves position entry notional from `entry_price * quantity`, falling back to
/// `committed_notional_usdc` only when `entry_price * quantity <= 1e-6` and `committed_notional_usdc > 1e-6`.
/// Handles MariaDB `CLOSED` rows where `committed_notional_usdc == 0.0`.
pub fn resolve_position_entry_notional_usdc(
    entry_price: f64,
    quantity: f64,
    committed_notional_usdc: f64,
) -> f64 {
    let entry_notional =
        if entry_price.is_finite() && quantity.is_finite() && entry_price > 0.0 && quantity > 0.0 {
            entry_price * quantity
        } else {
            0.0
        };
    if entry_notional.is_finite() && entry_notional > 1e-6 {
        entry_notional
    } else if committed_notional_usdc.is_finite() && committed_notional_usdc > 1e-6 {
        committed_notional_usdc
    } else {
        0.0
    }
}

/// Computes position PnL percentage (`pnl_usdc / entry_notional * 100.0`, rounded to 2dp).
/// Never produces `NaN` or `Infinity`.
pub fn compute_position_pnl_pct(
    pnl_usdc: f64,
    entry_price: f64,
    quantity: f64,
    committed_notional_usdc: f64,
) -> f64 {
    if !pnl_usdc.is_finite() {
        return 0.0;
    }
    let base_notional =
        resolve_position_entry_notional_usdc(entry_price, quantity, committed_notional_usdc);
    if !base_notional.is_finite() || base_notional <= 1e-6 {
        return 0.0;
    }
    let pct = (pnl_usdc / base_notional) * 100.0;
    if pct.is_finite() {
        round_dp(pct, 2)
    } else {
        0.0
    }
}

/// Computes vault PnL percentage (`pnl_usdc / (equity_usdc - pnl_usdc) * 100.0`, rounded to 2dp)
/// when `(equity_usdc - pnl_usdc) > 1e-6`, else `0.0`.
pub fn compute_vault_pnl_pct(equity_usdc: f64, pnl_usdc: f64) -> f64 {
    if !equity_usdc.is_finite() || !pnl_usdc.is_finite() {
        return 0.0;
    }
    let basis = equity_usdc - pnl_usdc;
    if !basis.is_finite() || basis <= 1e-6 {
        return 0.0;
    }
    let pct = (pnl_usdc / basis) * 100.0;
    if pct.is_finite() {
        round_dp(pct, 2)
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_position_pnl_pct_open_closed_and_edge_cases() {
        // Closed row with committed_notional_usdc == 0.0 must use entry_price * filled_quantity
        let closed_pct = compute_position_pnl_pct(-382.37, 0.23227, 9634.0, 0.0);
        assert!((closed_pct - (-17.09)).abs() < 1e-6);

        // Open row with valid entry_price * quantity
        let open_pct = compute_position_pnl_pct(12.34, 180.0, 2.0, 380.0);
        assert!((open_pct - 3.43).abs() < 1e-6);

        // Fallback to committed_notional_usdc when entry_price * quantity is 0
        let fallback_pct = compute_position_pnl_pct(25.0, 0.0, 0.0, 500.0);
        assert!((fallback_pct - 5.0).abs() < 1e-6);

        // Zero notional / NaN / Inf safely return 0.0
        assert_eq!(compute_position_pnl_pct(10.0, 0.0, 0.0, 0.0), 0.0);
        assert_eq!(compute_position_pnl_pct(f64::NAN, 100.0, 1.0, 100.0), 0.0);
        assert_eq!(
            compute_position_pnl_pct(f64::INFINITY, 100.0, 1.0, 100.0),
            0.0
        );
    }

    #[test]
    fn test_compute_vault_pnl_pct_formulas_and_edge_cases() {
        // Spec example: equity=202.400584, pnl=0.92 -> 0.46%; all_time_pnl=1.34 -> 0.67%
        assert!((compute_vault_pnl_pct(202.400584, 0.92) - 0.46).abs() < 1e-6);
        assert!((compute_vault_pnl_pct(202.400584, 1.34) - 0.67).abs() < 1e-6);

        // Negative PnL: equity=95.0, pnl=-5.0 -> basis=100.0 -> -5.0%
        assert!((compute_vault_pnl_pct(95.0, -5.0) - (-5.0)).abs() < 1e-6);

        // Basis <= 1e-6 (e.g. equity == pnl or 0) -> 0.0
        assert_eq!(compute_vault_pnl_pct(10.0, 10.0), 0.0);
        assert_eq!(compute_vault_pnl_pct(0.0, 0.0), 0.0);
        assert_eq!(compute_vault_pnl_pct(0.0, 5.0), 0.0);
        assert_eq!(compute_vault_pnl_pct(f64::NAN, 1.0), 0.0);
    }

    #[test]
    fn test_cached_hl_portfolio_pnl_interpolation_and_horizons() {
        let day_pts = vec![(1_000_000, 0.0), (1_086_400, 242.70)];
        let week_pts = vec![
            (500_000, 0.0),
            (700_000, -100.0),
            (800_000, -200.0),
            (1_086_400, 362.50),
        ];
        let all_pts = vec![
            (100_000, 0.0),
            (400_000, 50.0),
            (500_000, 100.0),
            (1_086_400, 462.50),
        ];
        let cache = CachedHlPortfolioPnl {
            synced_at_ms: 1_086_500,
            perp_day: HlPortfolioWindowCache {
                pnl_history: day_pts,
                vlm: 1412.91,
            },
            perp_week: HlPortfolioWindowCache {
                pnl_history: week_pts,
                vlm: 64604.1,
            },
            perp_month: HlPortfolioWindowCache::default(),
            perp_all_time: HlPortfolioWindowCache {
                pnl_history: all_pts,
                vlm: 83291.0,
            },
        };

        assert!(cache.is_fresh(1_100_000, 600_000));
        assert!(!cache.is_fresh(1_700_000, 600_000));

        // Exact daily (cap = 750_000 < raw_start 1_000_000 -> not clamped)
        let (daily_net, daily_ts, daily_clamped) =
            cache.horizon_net_pnl("daily", 1_000_000, 750_000).unwrap();
        assert!(!daily_clamped);
        assert_eq!(daily_ts, 1_000_000);
        assert!((daily_net - 242.70).abs() < 1e-6);

        // Clamped weekly: cap = 750_000 sits halfway between (700_000, -100.0) and (800_000, -200.0) => -150.0
        // Delta = 362.50 - (-150.0) = 512.50
        let (wk_net, wk_ts, wk_clamped) =
            cache.horizon_net_pnl("weekly", 400_000, 750_000).unwrap();
        assert!(wk_clamped);
        assert_eq!(wk_ts, 750_000);
        assert!((wk_net - 512.50).abs() < 1e-6);

        // Stale cap check: synced_at_ms < cap_updated_at_ms -> None (forces LOCAL_LEDGER fallback)
        assert!(cache
            .horizon_net_pnl("weekly", 400_000, 1_090_000)
            .is_none());

        // YTD with cap = 0 and first_ts < raw_start (450_000 sits halfway between 50 and 100 => 75; 462.50 - 75 = 387.50)
        let (ytd_net, ytd_ts, ytd_clamped) = cache.horizon_net_pnl("ytd", 450_000, 0).unwrap();
        assert!(!ytd_clamped);
        assert_eq!(ytd_ts, 450_000);
        assert!((ytd_net - 387.50).abs() < 1e-6);

        // Empty monthly window returns None
        assert!(cache.horizon_net_pnl("monthly", 400_000, 0).is_none());
    }
}
