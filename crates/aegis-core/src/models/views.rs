//! First-class SQL View DTOs (`aegis-core::models::views`).

use serde::{Deserialize, Serialize};

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
    #[serde(default)]
    pub created_at_ms: i64,
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
