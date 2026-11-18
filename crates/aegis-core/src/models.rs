//! Domain models matching the 10 MariaDB tables and 7 first-class SQL Views (`aegis-core::models`).

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
// 10 MARIADB TABLE DOMAIN MODELS
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
}

/// Multi-horizon PnL breakdown (`Daily 24h`, `Weekly 7d`, `Monthly 30d`, `YTD`) for the operator dashboard.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnlSummary {
    pub daily: PnlHorizonMetrics,
    pub weekly: PnlHorizonMetrics,
    pub monthly: PnlHorizonMetrics,
    pub ytd: PnlHorizonMetrics,
}
