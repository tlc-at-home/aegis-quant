//! First-class MariaDB SQL Views repository (`aegis-db::views_repo`).
//! Directly queries the 7 relational views:
//! 1. `v_assets_held`
//! 2. `v_assets_ordered`
//! 3. `v_assets_in_cooldown`
//! 4. `v_assets_sleeve_s1`
//! 5. `v_assets_sleeve_s2`
//! 6. `v_assets_sleeve_s3`
//! 7. `v_priority_watchlist`

use crate::engine::DatabaseManager;
use aegis_core::{
    compute_position_pnl_pct, AegisError, CooldownAssetView, HeldAssetView, OrderedAssetView,
    PriorityWatchView, Result, SleeveS1AssetView, SleeveS2AssetView, SleeveS3AssetView,
};
use mysql::prelude::*;

impl DatabaseManager {
    /// Queries `v_assets_held` (All active `OPEN` holdings with live mark, stop distance & sleeve attribution).
    pub fn get_assets_held(&self) -> Result<Vec<HeldAssetView>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT position_id, asset_id, symbol, coin, client_order_id, side, quantity,
                          committed_notional_usdc, portfolio_weight, entry_price, mark_price,
                          protective_stop_px, protective_stop_oid, stop_distance_pct,
                          take_profit_px, take_profit_oid, tp_distance_pct,
                          highest_price_seen, lowest_price_seen, unrealized_pnl_usdc,
                          realized_pnl_usdc, funding_accrued_usdc, fees_paid_usdc,
                          s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          latest_target_weight, latest_delta_weight, opened_at_ms, updated_at_ms
                   FROM v_assets_held
                   ORDER BY committed_notional_usdc DESC, position_id DESC"#,
            )
            .map_err(|e| AegisError::Database(format!("get_assets_held view query failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let quantity: f64 = r.get(6).unwrap_or_default();
                let committed_notional_usdc: f64 = r.get(7).unwrap_or_default();
                let entry_price: f64 = r.get(9).unwrap_or_default();
                let unrealized_pnl_usdc: f64 = r.get(19).unwrap_or_default();
                let unrealized_pnl_pct = compute_position_pnl_pct(
                    unrealized_pnl_usdc,
                    entry_price,
                    quantity,
                    committed_notional_usdc,
                );
                HeldAssetView {
                    position_id: r.get(0).unwrap_or_default(),
                    asset_id: r.get(1).unwrap_or_default(),
                    symbol: r.get(2).unwrap_or_default(),
                    coin: r.get(3).unwrap_or_default(),
                    client_order_id: r.get(4).unwrap_or_default(),
                    side: r.get(5).unwrap_or_else(|| "LONG".to_string()),
                    quantity,
                    committed_notional_usdc,
                    portfolio_weight: r.get(8).unwrap_or_default(),
                    entry_price,
                    mark_price: r.get(10).unwrap_or_default(),
                    protective_stop_px: r.get(11).unwrap_or_default(),
                    protective_stop_oid: r.get::<Option<String>, _>(12).unwrap_or(None),
                    stop_distance_pct: r.get::<Option<f64>, _>(13).unwrap_or(None),
                    take_profit_px: r.get(14).unwrap_or_default(),
                    take_profit_oid: r.get::<Option<String>, _>(15).unwrap_or(None),
                    tp_distance_pct: r.get::<Option<f64>, _>(16).unwrap_or(None),
                    highest_price_seen: r.get(17).unwrap_or_default(),
                    lowest_price_seen: r.get(18).unwrap_or_default(),
                    unrealized_pnl_usdc,
                    unrealized_pnl_pct,
                    realized_pnl_usdc: r.get(20).unwrap_or_default(),
                    funding_accrued_usdc: r.get(21).unwrap_or_default(),
                    fees_paid_usdc: r.get(22).unwrap_or_default(),
                    s1_weight_contrib: r.get(23).unwrap_or_default(),
                    s2_weight_contrib: r.get(24).unwrap_or_default(),
                    s3_weight_contrib: r.get(25).unwrap_or_default(),
                    latest_target_weight: r.get(26).unwrap_or_default(),
                    latest_delta_weight: r.get(27).unwrap_or_default(),
                    created_at_ms: 0,
                    opened_at_ms: r.get::<Option<i64>, _>(28).unwrap_or(None),
                    updated_at_ms: r.get(29).unwrap_or_default(),
                }
            })
            .collect())
    }

    /// Queries `v_assets_ordered` (All `WORKING` Maker Limit Chaser orders with reserved margin & book distance).
    pub fn get_assets_ordered(&self) -> Result<Vec<OrderedAssetView>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT position_id, asset_id, symbol, coin, client_order_id, exchange_order_id,
                          side, fsm_state, requested_quantity, filled_quantity,
                          reserved_margin_usdc, limit_price, mid_price, mark_price,
                          distance_from_mid_bps, chase_count, protective_stop_px, take_profit_px,
                          s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          created_at_ms, updated_at_ms
                   FROM v_assets_ordered
                   ORDER BY created_at_ms DESC, position_id DESC"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("get_assets_ordered view query failed: {e}"))
            })?;

        Ok(rows
            .into_iter()
            .map(|r| OrderedAssetView {
                position_id: r.get(0).unwrap_or_default(),
                asset_id: r.get(1).unwrap_or_default(),
                symbol: r.get(2).unwrap_or_default(),
                coin: r.get(3).unwrap_or_default(),
                client_order_id: r.get(4).unwrap_or_default(),
                exchange_order_id: r.get::<Option<String>, _>(5).unwrap_or(None),
                side: r.get(6).unwrap_or_else(|| "LONG".to_string()),
                fsm_state: r.get(7).unwrap_or_else(|| "WORKING_ALO".to_string()),
                requested_quantity: r.get(8).unwrap_or_default(),
                filled_quantity: r.get(9).unwrap_or_default(),
                reserved_margin_usdc: r.get(10).unwrap_or_default(),
                limit_price: r.get(11).unwrap_or_default(),
                mid_price: r.get(12).unwrap_or_default(),
                mark_price: r.get(13).unwrap_or_default(),
                distance_from_mid_bps: r.get(14).unwrap_or_default(),
                chase_count: r.get(15).unwrap_or_default(),
                protective_stop_px: r.get(16).unwrap_or_default(),
                take_profit_px: r.get(17).unwrap_or_default(),
                s1_weight_contrib: r.get(18).unwrap_or_default(),
                s2_weight_contrib: r.get(19).unwrap_or_default(),
                s3_weight_contrib: r.get(20).unwrap_or_default(),
                created_at_ms: r.get(21).unwrap_or_default(),
                updated_at_ms: r.get(22).unwrap_or_default(),
            })
            .collect())
    }

    /// Queries `v_assets_in_cooldown` (Assets currently blocked by active post-stop / shock / churn cooldown).
    pub fn get_assets_in_cooldown(&self) -> Result<Vec<CooldownAssetView>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT cooldown_id, asset_id, symbol, coin, cooldown_type, reason,
                          triggered_at_ms, expires_at_ms, remaining_seconds, updated_at_ms
                   FROM v_assets_in_cooldown
                   ORDER BY expires_at_ms ASC"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("get_assets_in_cooldown view query failed: {e}"))
            })?;

        Ok(rows
            .into_iter()
            .map(|r| CooldownAssetView {
                cooldown_id: r.get(0).unwrap_or_default(),
                asset_id: r.get(1).unwrap_or_default(),
                symbol: r.get(2).unwrap_or_default(),
                coin: r.get(3).unwrap_or_default(),
                cooldown_type: r.get(4).unwrap_or_default(),
                reason: r.get(5).unwrap_or_default(),
                triggered_at_ms: r.get(6).unwrap_or_default(),
                expires_at_ms: r.get(7).unwrap_or_default(),
                remaining_seconds: r
                    .get::<f64, _>(8)
                    .map(|v| v as i64)
                    .or_else(|| r.get::<i64, _>(8))
                    .unwrap_or_default(),
                updated_at_ms: r.get(9).unwrap_or_default(),
            })
            .collect())
    }

    /// Queries `v_assets_sleeve_s1` (Sleeve 1: Cross-Sectional Carry-Momentum & Market-Beta Hedge).
    pub fn get_sleeve_s1_view(&self) -> Result<Vec<SleeveS1AssetView>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT asset_id, symbol, coin, mark_price, day_volume_usdc,
                          funding_rate_hourly, funding_apr_pct, annualized_vol,
                          market_beta, s1_mom_score, s1_carry_score, w_s1_carry_mom,
                          target_weight, current_weight, updated_at_ms
                   FROM v_assets_sleeve_s1
                   ORDER BY ABS(w_s1_carry_mom) DESC, day_volume_usdc DESC"#,
            )
            .map_err(|e| AegisError::Database(format!("get_sleeve_s1_view query failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| SleeveS1AssetView {
                asset_id: r.get(0).unwrap_or_default(),
                symbol: r.get(1).unwrap_or_default(),
                coin: r.get(2).unwrap_or_default(),
                mark_price: r.get(3).unwrap_or_default(),
                day_volume_usdc: r.get(4).unwrap_or_default(),
                funding_rate_hourly: r.get(5).unwrap_or_default(),
                funding_apr_pct: r.get(6).unwrap_or_default(),
                annualized_vol: r.get(7).unwrap_or_default(),
                market_beta: r.get(8).unwrap_or_default(),
                s1_mom_score: r.get(9).unwrap_or_default(),
                s1_carry_score: r.get(10).unwrap_or_default(),
                w_s1_carry_mom: r.get(11).unwrap_or_default(),
                target_weight: r.get(12).unwrap_or_default(),
                current_weight: r.get(13).unwrap_or_default(),
                updated_at_ms: r.get(14).unwrap_or_default(),
            })
            .collect())
    }

    /// Queries `v_assets_sleeve_s2` (Sleeve 2: Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout).
    pub fn get_sleeve_s2_view(&self) -> Result<Vec<SleeveS2AssetView>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT asset_id, symbol, coin, mark_price, day_volume_usdc,
                          annualized_vol, s2_trend_alignment, s2_vol_surge_ratio,
                          w_s2_trend_breakout, event_trigger, target_weight,
                          current_weight, updated_at_ms
                   FROM v_assets_sleeve_s2
                   ORDER BY ABS(w_s2_trend_breakout) DESC, s2_vol_surge_ratio DESC"#,
            )
            .map_err(|e| AegisError::Database(format!("get_sleeve_s2_view query failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| SleeveS2AssetView {
                asset_id: r.get(0).unwrap_or_default(),
                symbol: r.get(1).unwrap_or_default(),
                coin: r.get(2).unwrap_or_default(),
                mark_price: r.get(3).unwrap_or_default(),
                day_volume_usdc: r.get(4).unwrap_or_default(),
                annualized_vol: r.get(5).unwrap_or_default(),
                s2_trend_alignment: r.get(6).unwrap_or_default(),
                s2_vol_surge_ratio: r.get(7).unwrap_or(1.0),
                w_s2_trend_breakout: r.get(8).unwrap_or_default(),
                event_trigger: r.get::<Option<String>, _>(9).unwrap_or(None),
                target_weight: r.get(10).unwrap_or_default(),
                current_weight: r.get(11).unwrap_or_default(),
                updated_at_ms: r.get(12).unwrap_or_default(),
            })
            .collect())
    }

    /// Queries `v_assets_sleeve_s3` (Sleeve 3: Live Capitulation Flush & Funding Dislocation Harvester).
    pub fn get_sleeve_s3_view(&self) -> Result<Vec<SleeveS3AssetView>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT asset_id, symbol, coin, mark_price, day_volume_usdc,
                          funding_rate_hourly, funding_apr_pct, s3_funding_zscore,
                          s3_flush_atr_mult, w_s3_event_reversion, event_trigger,
                          target_weight, current_weight, updated_at_ms
                   FROM v_assets_sleeve_s3
                   ORDER BY ABS(w_s3_event_reversion) DESC, s3_funding_zscore ASC"#,
            )
            .map_err(|e| AegisError::Database(format!("get_sleeve_s3_view query failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| SleeveS3AssetView {
                asset_id: r.get(0).unwrap_or_default(),
                symbol: r.get(1).unwrap_or_default(),
                coin: r.get(2).unwrap_or_default(),
                mark_price: r.get(3).unwrap_or_default(),
                day_volume_usdc: r.get(4).unwrap_or_default(),
                funding_rate_hourly: r.get(5).unwrap_or_default(),
                funding_apr_pct: r.get(6).unwrap_or_default(),
                s3_funding_zscore: r.get(7).unwrap_or_default(),
                s3_flush_atr_mult: r.get(8).unwrap_or_default(),
                w_s3_event_reversion: r.get(9).unwrap_or_default(),
                event_trigger: r.get::<Option<String>, _>(10).unwrap_or(None),
                target_weight: r.get(11).unwrap_or_default(),
                current_weight: r.get(12).unwrap_or_default(),
                updated_at_ms: r.get(13).unwrap_or_default(),
            })
            .collect())
    }

    /// Queries `v_priority_watchlist` (All assets under 1-Second High-Frequency Surveillance).
    pub fn get_priority_watchlist(&self) -> Result<Vec<PriorityWatchView>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT asset_id, symbol, coin, mark_price, mid_price, funding_rate_hourly,
                          position_id, position_status, position_side, fsm_state,
                          entry_price, requested_quantity, filled_quantity,
                          reserved_margin_usdc, committed_notional_usdc,
                          protective_stop_px, protective_stop_oid,
                          take_profit_px, take_profit_oid, unrealized_pnl_usdc,
                          target_weight, event_trigger, updated_at_ms
                   FROM v_priority_watchlist
                   ORDER BY position_id DESC, ABS(target_weight) DESC, symbol ASC"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("get_priority_watchlist view query failed: {e}"))
            })?;

        Ok(rows
            .into_iter()
            .map(|r| PriorityWatchView {
                asset_id: r.get(0).unwrap_or_default(),
                symbol: r.get(1).unwrap_or_default(),
                coin: r.get(2).unwrap_or_default(),
                mark_price: r.get(3).unwrap_or_default(),
                mid_price: r.get(4).unwrap_or_default(),
                funding_rate_hourly: r.get(5).unwrap_or_default(),
                position_id: r.get::<Option<i64>, _>(6).unwrap_or(None),
                position_status: r.get::<Option<String>, _>(7).unwrap_or(None),
                position_side: r.get::<Option<String>, _>(8).unwrap_or(None),
                fsm_state: r.get::<Option<String>, _>(9).unwrap_or(None),
                entry_price: r.get::<Option<f64>, _>(10).unwrap_or(None),
                requested_quantity: r.get::<Option<f64>, _>(11).unwrap_or(None),
                filled_quantity: r.get::<Option<f64>, _>(12).unwrap_or(None),
                reserved_margin_usdc: r.get::<Option<f64>, _>(13).unwrap_or(None),
                committed_notional_usdc: r.get::<Option<f64>, _>(14).unwrap_or(None),
                protective_stop_px: r.get::<Option<f64>, _>(15).unwrap_or(None),
                protective_stop_oid: r.get::<Option<String>, _>(16).unwrap_or(None),
                take_profit_px: r.get::<Option<f64>, _>(17).unwrap_or(None),
                take_profit_oid: r.get::<Option<String>, _>(18).unwrap_or(None),
                unrealized_pnl_usdc: r.get::<Option<f64>, _>(19).unwrap_or(None),
                target_weight: r.get(20).unwrap_or_default(),
                event_trigger: r.get::<Option<String>, _>(21).unwrap_or(None),
                updated_at_ms: r.get(22).unwrap_or_default(),
            })
            .collect())
    }
}
