//! First-class MariaDB SQL Views repository & single-connection dashboard read bundle (`aegis-db::views_repo`).
//! Directly queries the 7 relational views:
//! 1. `v_assets_held`
//! 2. `v_assets_ordered`
//! 3. `v_assets_in_cooldown`
//! 4. `v_assets_sleeve_s1`
//! 5. `v_assets_sleeve_s2`
//! 6. `v_assets_sleeve_s3`
//! 7. `v_priority_watchlist`

use crate::engine::DatabaseManager;
use crate::row_decode::{
    row_get_f64, row_get_i32, row_get_i64, row_get_opt_f64, row_get_opt_i64, row_get_opt_string,
    row_get_string, row_get_u32,
};
use aegis_core::{
    compute_position_pnl_pct, AegisError, ClosedPositionView, ConfigMap, CooldownAssetView,
    ExchangeAsset, HeldAssetView, OrderAuditRecord, OrderedAssetView, PortfolioSnapshot,
    PriorityWatchView, Result, SleeveS1AssetView, SleeveS2AssetView, SleeveS3AssetView,
    SystemConfigRow, TelemetryLogRecord,
};
use mysql::prelude::*;
use mysql::PooledConn;

/// Per-section read bundle returned over a single pooled `START TRANSACTION READ ONLY` connection
/// for `aegis-web` (`R14-M22`, `R14-M28`).
pub struct DashboardReadBundle {
    pub mariadb_version: Result<String>,
    pub tables_and_views: Result<(Vec<String>, Vec<String>)>,
    pub config: Result<ConfigMap>,
    pub system_config_rows: Result<Vec<SystemConfigRow>>,
    pub assets_held: Result<Vec<HeldAssetView>>,
    pub assets_ordered: Result<Vec<OrderedAssetView>>,
    pub recent_closed_positions: Result<Vec<ClosedPositionView>>,
    pub assets_in_cooldown: Result<Vec<CooldownAssetView>>,
    pub sleeve_s1: Result<Vec<SleeveS1AssetView>>,
    pub sleeve_s2: Result<Vec<SleeveS2AssetView>>,
    pub sleeve_s3: Result<Vec<SleeveS3AssetView>>,
    pub priority_watchlist: Result<Vec<PriorityWatchView>>,
    pub exchange_assets: Result<Vec<ExchangeAsset>>,
    pub order_audit: Result<Vec<OrderAuditRecord>>,
    pub telemetry_logs: Result<Vec<TelemetryLogRecord>>,
    pub latest_snapshot: Result<Option<PortfolioSnapshot>>,
    pub snapshots_history: Result<Vec<PortfolioSnapshot>>,
}

impl DatabaseManager {
    /// Executes all dashboard view and table reads over a single pooled read-only connection
    /// (`START TRANSACTION READ ONLY`), returning independent per-section `Result`s so callers
    /// can fail closed on critical sections or report degraded partial outages cleanly (`R14-M22`).
    pub fn load_dashboard_read_bundle(&self) -> Result<DashboardReadBundle> {
        // Total lock order: fernet RwLock -> DB conn (eliminates TOCTOU vs rotate_fernet_master_key)
        let cipher = self.fernet();
        let mut conn = self.get_conn()?;
        conn.query_drop("START TRANSACTION READ ONLY")
            .map_err(|e| {
                AegisError::Database(format!(
                    "load_dashboard_read_bundle START TRANSACTION READ ONLY failed: {e}"
                ))
            })?;

        let mariadb_version = conn
            .query_first::<Option<String>, _>("SELECT VERSION()")
            .map(|opt| opt.flatten().unwrap_or_else(|| "unknown".to_string()))
            .map_err(|e| AegisError::Database(format!("SELECT VERSION() failed: {e}")));
        let tables_and_views = Self::list_tables_and_views_with_conn(&mut conn);
        let system_config_rows = Self::list_system_config_rows_with_conn(&mut conn);
        let assets_held = Self::get_assets_held_with_conn(&mut conn);
        let assets_ordered = Self::get_assets_ordered_with_conn(&mut conn);
        let recent_closed_positions =
            Self::list_recent_closed_positions_view_with_conn(&mut conn, 100);
        let assets_in_cooldown = Self::get_assets_in_cooldown_with_conn(&mut conn);
        let sleeve_s1 = Self::get_sleeve_s1_view_with_conn(&mut conn);
        let sleeve_s2 = Self::get_sleeve_s2_view_with_conn(&mut conn);
        let sleeve_s3 = Self::get_sleeve_s3_view_with_conn(&mut conn);
        let priority_watchlist = Self::get_priority_watchlist_with_conn(&mut conn);
        let exchange_assets = Self::list_exchange_assets_with_conn(&mut conn);
        let order_audit = Self::list_recent_order_audits_with_conn(&mut conn, 50);
        let telemetry_logs = Self::list_recent_telemetry_with_conn(&mut conn, 50);
        let latest_snapshot = Self::get_latest_portfolio_snapshot_opt_with_conn(&mut conn);
        let snapshots_history = Self::list_portfolio_snapshots_with_conn(&mut conn, 120);

        let _ = conn.query_drop("ROLLBACK");
        drop(conn);

        // Decrypt using pre-acquired `cipher` after returning `conn` to the pool
        let config = match &system_config_rows {
            Ok(rows) => {
                let pairs = rows
                    .iter()
                    .map(|r| (r.config_key.clone(), r.config_value.clone()))
                    .collect();
                Self::decrypt_config_pairs_with_cipher(&cipher, pairs)
            }
            Err(e) => Err(AegisError::Database(e.to_string())),
        };

        Ok(DashboardReadBundle {
            mariadb_version,
            tables_and_views,
            config,
            system_config_rows,
            assets_held,
            assets_ordered,
            recent_closed_positions,
            assets_in_cooldown,
            sleeve_s1,
            sleeve_s2,
            sleeve_s3,
            priority_watchlist,
            exchange_assets,
            order_audit,
            telemetry_logs,
            latest_snapshot,
            snapshots_history,
        })
    }

    /// Queries `v_assets_held` (All active `OPEN` holdings with live mark, stop distance & sleeve attribution).
    pub fn get_assets_held(&self) -> Result<Vec<HeldAssetView>> {
        let mut conn = self.get_conn()?;
        Self::get_assets_held_with_conn(&mut conn)
    }

    pub(crate) fn get_assets_held_with_conn(conn: &mut PooledConn) -> Result<Vec<HeldAssetView>> {
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT position_id, asset_id, symbol, coin, client_order_id, side, quantity,
                          committed_notional_usdc, portfolio_weight, entry_price, mark_price,
                          protective_stop_px, protective_stop_oid, stop_distance_pct,
                          take_profit_px, take_profit_oid, tp_distance_pct,
                          highest_price_seen, lowest_price_seen, unrealized_pnl_usdc,
                          realized_pnl_usdc, funding_accrued_usdc, fees_paid_usdc,
                          s1_weight_contrib, s2_weight_contrib, s3_weight_contrib,
                          latest_target_weight, latest_delta_weight, created_at_ms,
                          opened_at_ms, updated_at_ms
                   FROM v_assets_held
                   ORDER BY committed_notional_usdc DESC, position_id DESC"#,
            )
            .map_err(|e| AegisError::Database(format!("get_assets_held view query failed: {e}")))?;

        Ok(rows
            .iter()
            .map(|r| {
                let quantity = row_get_f64(r, 6, 0.0);
                let committed_notional_usdc = row_get_f64(r, 7, 0.0);
                let entry_price = row_get_f64(r, 9, 0.0);
                let unrealized_pnl_usdc = row_get_f64(r, 19, 0.0);
                let unrealized_pnl_pct = compute_position_pnl_pct(
                    unrealized_pnl_usdc,
                    entry_price,
                    quantity,
                    committed_notional_usdc,
                );
                HeldAssetView {
                    position_id: row_get_i64(r, 0, 0),
                    asset_id: row_get_i64(r, 1, 0),
                    symbol: row_get_string(r, 2, ""),
                    coin: row_get_string(r, 3, ""),
                    client_order_id: row_get_string(r, 4, ""),
                    side: row_get_string(r, 5, "LONG"),
                    quantity,
                    committed_notional_usdc,
                    portfolio_weight: row_get_f64(r, 8, 0.0),
                    entry_price,
                    mark_price: row_get_f64(r, 10, 0.0),
                    protective_stop_px: row_get_f64(r, 11, 0.0),
                    protective_stop_oid: row_get_opt_string(r, 12),
                    stop_distance_pct: row_get_opt_f64(r, 13),
                    take_profit_px: row_get_f64(r, 14, 0.0),
                    take_profit_oid: row_get_opt_string(r, 15),
                    tp_distance_pct: row_get_opt_f64(r, 16),
                    highest_price_seen: row_get_f64(r, 17, 0.0),
                    lowest_price_seen: row_get_f64(r, 18, 0.0),
                    unrealized_pnl_usdc,
                    unrealized_pnl_pct,
                    realized_pnl_usdc: row_get_f64(r, 20, 0.0),
                    funding_accrued_usdc: row_get_f64(r, 21, 0.0),
                    fees_paid_usdc: row_get_f64(r, 22, 0.0),
                    s1_weight_contrib: row_get_f64(r, 23, 0.0),
                    s2_weight_contrib: row_get_f64(r, 24, 0.0),
                    s3_weight_contrib: row_get_f64(r, 25, 0.0),
                    latest_target_weight: row_get_f64(r, 26, 0.0),
                    latest_delta_weight: row_get_f64(r, 27, 0.0),
                    created_at_ms: row_get_i64(r, 28, 0),
                    opened_at_ms: row_get_opt_i64(r, 29),
                    updated_at_ms: row_get_i64(r, 30, 0),
                }
            })
            .collect())
    }

    /// Queries `v_assets_ordered` (All `WORKING` Maker Limit Chaser orders with reserved margin & book distance).
    pub fn get_assets_ordered(&self) -> Result<Vec<OrderedAssetView>> {
        let mut conn = self.get_conn()?;
        Self::get_assets_ordered_with_conn(&mut conn)
    }

    pub(crate) fn get_assets_ordered_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Vec<OrderedAssetView>> {
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
            .iter()
            .map(|r| OrderedAssetView {
                position_id: row_get_i64(r, 0, 0),
                asset_id: row_get_i64(r, 1, 0),
                symbol: row_get_string(r, 2, ""),
                coin: row_get_string(r, 3, ""),
                client_order_id: row_get_string(r, 4, ""),
                exchange_order_id: row_get_opt_string(r, 5),
                side: row_get_string(r, 6, "LONG"),
                fsm_state: row_get_string(r, 7, "WORKING_ALO"),
                requested_quantity: row_get_f64(r, 8, 0.0),
                filled_quantity: row_get_f64(r, 9, 0.0),
                reserved_margin_usdc: row_get_f64(r, 10, 0.0),
                limit_price: row_get_f64(r, 11, 0.0),
                mid_price: row_get_f64(r, 12, 0.0),
                mark_price: row_get_f64(r, 13, 0.0),
                distance_from_mid_bps: row_get_f64(r, 14, 0.0),
                chase_count: row_get_u32(r, 15, 0),
                protective_stop_px: row_get_f64(r, 16, 0.0),
                take_profit_px: row_get_f64(r, 17, 0.0),
                s1_weight_contrib: row_get_f64(r, 18, 0.0),
                s2_weight_contrib: row_get_f64(r, 19, 0.0),
                s3_weight_contrib: row_get_f64(r, 20, 0.0),
                created_at_ms: row_get_i64(r, 21, 0),
                updated_at_ms: row_get_i64(r, 22, 0),
            })
            .collect())
    }

    /// Queries `v_assets_in_cooldown` (Assets currently blocked by active post-stop / shock / churn cooldown).
    pub fn get_assets_in_cooldown(&self) -> Result<Vec<CooldownAssetView>> {
        let mut conn = self.get_conn()?;
        Self::get_assets_in_cooldown_with_conn(&mut conn)
    }

    pub(crate) fn get_assets_in_cooldown_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Vec<CooldownAssetView>> {
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
            .iter()
            .map(|r| CooldownAssetView {
                cooldown_id: row_get_i64(r, 0, 0),
                asset_id: row_get_i64(r, 1, 0),
                symbol: row_get_string(r, 2, ""),
                coin: row_get_string(r, 3, ""),
                cooldown_type: row_get_string(r, 4, ""),
                reason: row_get_string(r, 5, ""),
                triggered_at_ms: row_get_i64(r, 6, 0),
                expires_at_ms: row_get_i64(r, 7, 0),
                remaining_seconds: row_get_i64(r, 8, 0),
                updated_at_ms: row_get_i64(r, 9, 0),
            })
            .collect())
    }

    /// Queries `v_assets_sleeve_s1` (Sleeve 1: Cross-Sectional Carry-Momentum & Market-Beta Hedge).
    pub fn get_sleeve_s1_view(&self) -> Result<Vec<SleeveS1AssetView>> {
        let mut conn = self.get_conn()?;
        Self::get_sleeve_s1_view_with_conn(&mut conn)
    }

    pub(crate) fn get_sleeve_s1_view_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Vec<SleeveS1AssetView>> {
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
            .iter()
            .map(|r| SleeveS1AssetView {
                asset_id: row_get_i64(r, 0, 0),
                symbol: row_get_string(r, 1, ""),
                coin: row_get_string(r, 2, ""),
                mark_price: row_get_f64(r, 3, 0.0),
                day_volume_usdc: row_get_f64(r, 4, 0.0),
                funding_rate_hourly: row_get_f64(r, 5, 0.0),
                funding_apr_pct: row_get_f64(r, 6, 0.0),
                annualized_vol: row_get_f64(r, 7, 0.0),
                market_beta: row_get_f64(r, 8, 0.0),
                s1_mom_score: row_get_f64(r, 9, 0.0),
                s1_carry_score: row_get_f64(r, 10, 0.0),
                w_s1_carry_mom: row_get_f64(r, 11, 0.0),
                target_weight: row_get_f64(r, 12, 0.0),
                current_weight: row_get_f64(r, 13, 0.0),
                updated_at_ms: row_get_i64(r, 14, 0),
            })
            .collect())
    }

    /// Queries `v_assets_sleeve_s2` (Sleeve 2: Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout).
    pub fn get_sleeve_s2_view(&self) -> Result<Vec<SleeveS2AssetView>> {
        let mut conn = self.get_conn()?;
        Self::get_sleeve_s2_view_with_conn(&mut conn)
    }

    pub(crate) fn get_sleeve_s2_view_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Vec<SleeveS2AssetView>> {
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
            .iter()
            .map(|r| SleeveS2AssetView {
                asset_id: row_get_i64(r, 0, 0),
                symbol: row_get_string(r, 1, ""),
                coin: row_get_string(r, 2, ""),
                mark_price: row_get_f64(r, 3, 0.0),
                day_volume_usdc: row_get_f64(r, 4, 0.0),
                annualized_vol: row_get_f64(r, 5, 0.0),
                s2_trend_alignment: row_get_i32(r, 6, 0),
                s2_vol_surge_ratio: row_get_f64(r, 7, 1.0),
                w_s2_trend_breakout: row_get_f64(r, 8, 0.0),
                event_trigger: row_get_opt_string(r, 9),
                target_weight: row_get_f64(r, 10, 0.0),
                current_weight: row_get_f64(r, 11, 0.0),
                updated_at_ms: row_get_i64(r, 12, 0),
            })
            .collect())
    }

    /// Queries `v_assets_sleeve_s3` (Sleeve 3: Live Capitulation Flush & Funding Dislocation Harvester).
    pub fn get_sleeve_s3_view(&self) -> Result<Vec<SleeveS3AssetView>> {
        let mut conn = self.get_conn()?;
        Self::get_sleeve_s3_view_with_conn(&mut conn)
    }

    pub(crate) fn get_sleeve_s3_view_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Vec<SleeveS3AssetView>> {
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
            .iter()
            .map(|r| SleeveS3AssetView {
                asset_id: row_get_i64(r, 0, 0),
                symbol: row_get_string(r, 1, ""),
                coin: row_get_string(r, 2, ""),
                mark_price: row_get_f64(r, 3, 0.0),
                day_volume_usdc: row_get_f64(r, 4, 0.0),
                funding_rate_hourly: row_get_f64(r, 5, 0.0),
                funding_apr_pct: row_get_f64(r, 6, 0.0),
                s3_funding_zscore: row_get_f64(r, 7, 0.0),
                s3_flush_atr_mult: row_get_f64(r, 8, 0.0),
                w_s3_event_reversion: row_get_f64(r, 9, 0.0),
                event_trigger: row_get_opt_string(r, 10),
                target_weight: row_get_f64(r, 11, 0.0),
                current_weight: row_get_f64(r, 12, 0.0),
                updated_at_ms: row_get_i64(r, 13, 0),
            })
            .collect())
    }

    /// Queries `v_priority_watchlist` (All assets under 1-Second High-Frequency Surveillance).
    pub fn get_priority_watchlist(&self) -> Result<Vec<PriorityWatchView>> {
        let mut conn = self.get_conn()?;
        Self::get_priority_watchlist_with_conn(&mut conn)
    }

    pub(crate) fn get_priority_watchlist_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Vec<PriorityWatchView>> {
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
            .iter()
            .map(|r| PriorityWatchView {
                asset_id: row_get_i64(r, 0, 0),
                symbol: row_get_string(r, 1, ""),
                coin: row_get_string(r, 2, ""),
                mark_price: row_get_f64(r, 3, 0.0),
                mid_price: row_get_f64(r, 4, 0.0),
                funding_rate_hourly: row_get_f64(r, 5, 0.0),
                position_id: row_get_opt_i64(r, 6),
                position_status: row_get_opt_string(r, 7),
                position_side: row_get_opt_string(r, 8),
                fsm_state: row_get_opt_string(r, 9),
                entry_price: row_get_opt_f64(r, 10),
                requested_quantity: row_get_opt_f64(r, 11),
                filled_quantity: row_get_opt_f64(r, 12),
                reserved_margin_usdc: row_get_opt_f64(r, 13),
                committed_notional_usdc: row_get_opt_f64(r, 14),
                protective_stop_px: row_get_opt_f64(r, 15),
                protective_stop_oid: row_get_opt_string(r, 16),
                take_profit_px: row_get_opt_f64(r, 17),
                take_profit_oid: row_get_opt_string(r, 18),
                unrealized_pnl_usdc: row_get_opt_f64(r, 19),
                target_weight: row_get_f64(r, 20, 0.0),
                event_trigger: row_get_opt_string(r, 21),
                updated_at_ms: row_get_i64(r, 22, 0),
            })
            .collect())
    }
}
