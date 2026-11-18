//! Portfolio snapshots, telemetry logs, and alert queue repository (`aegis-db::portfolio_repo`).

use crate::engine::DatabaseManager;
use aegis_core::{
    now_ms, round_dp, sanitize_telemetry_message, ytd_start_ms, AegisError, AlertQueueRecord,
    PnlHorizonMetrics, PnlSummary, PortfolioSnapshot, Result, TelemetryLogRecord,
};
use mysql::params;
use mysql::prelude::*;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct SnapshotInsertInput {
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
}

impl DatabaseManager {
    /// Inserts a unified portfolio snapshot and sleeve attribution record.
    pub fn insert_portfolio_snapshot(&self, input: &SnapshotInsertInput) -> Result<i64> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"INSERT INTO portfolio_snapshots (
                   total_equity_usdc, available_cash_usdc, reserved_order_cash_usdc,
                   committed_notional_usdc, net_notional_usdc, gross_leverage_x,
                   net_leverage_x, market_beta_exposure, market_breadth_bull,
                   market_regime_label, risk_tier, leverage_multiplier,
                   s1_gross_weight, s2_gross_weight, s3_gross_weight,
                   working_order_count, open_long_count, open_short_count,
                   eligible_coin_count, created_at_ms, updated_at_ms
               ) VALUES (
                   :eq, :cash, :res,
                   :comm, :net_n, :g_lev,
                   :n_lev, :beta, :breadth,
                   :regime, :tier, :mult,
                   :s1_w, :s2_w, :s3_w,
                   :w_cnt, :l_cnt, :s_cnt,
                   :elig_cnt, :ts, :ts
               )"#,
            params! {
                "eq" => input.total_equity_usdc,
                "cash" => input.available_cash_usdc,
                "res" => input.reserved_order_cash_usdc,
                "comm" => input.committed_notional_usdc,
                "net_n" => input.net_notional_usdc,
                "g_lev" => input.gross_leverage_x,
                "n_lev" => input.net_leverage_x,
                "beta" => input.market_beta_exposure,
                "breadth" => input.market_breadth_bull,
                "regime" => &input.market_regime_label,
                "tier" => &input.risk_tier,
                "mult" => input.leverage_multiplier,
                "s1_w" => input.s1_gross_weight,
                "s2_w" => input.s2_gross_weight,
                "s3_w" => input.s3_gross_weight,
                "w_cnt" => input.working_order_count,
                "l_cnt" => input.open_long_count,
                "s_cnt" => input.open_short_count,
                "elig_cnt" => input.eligible_coin_count,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("insert_portfolio_snapshot failed: {e}")))?;
        Ok(conn.last_insert_id() as i64)
    }

    /// Returns the most recent `PortfolioSnapshot`, or a synthesized baseline snapshot if empty.
    pub fn get_latest_portfolio_snapshot(&self) -> Result<PortfolioSnapshot> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .query_first(
                r#"SELECT id, total_equity_usdc, available_cash_usdc, reserved_order_cash_usdc,
                          committed_notional_usdc, net_notional_usdc, gross_leverage_x,
                          net_leverage_x, market_beta_exposure, market_breadth_bull,
                          market_regime_label, risk_tier, leverage_multiplier,
                          s1_gross_weight, s2_gross_weight, s3_gross_weight,
                          working_order_count, open_long_count, open_short_count,
                          eligible_coin_count, created_at_ms, updated_at_ms
                   FROM portfolio_snapshots
                   ORDER BY created_at_ms DESC, id DESC
                   LIMIT 1"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("get_latest_portfolio_snapshot failed: {e}"))
            })?;

        if let Some(r) = row {
            Ok(row_to_portfolio_snapshot(r))
        } else {
            let cfg = self.load_config_or_default();
            let eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
            let ts = now_ms();
            Ok(PortfolioSnapshot {
                id: 0,
                total_equity_usdc: eq,
                available_cash_usdc: eq,
                reserved_order_cash_usdc: 0.0,
                committed_notional_usdc: 0.0,
                net_notional_usdc: 0.0,
                gross_leverage_x: 0.0,
                net_leverage_x: 0.0,
                market_beta_exposure: 0.0,
                market_breadth_bull: 0.50,
                market_regime_label: "NEUTRAL_INITIALIZING".to_string(),
                risk_tier: cfg.risk_tier().as_str().to_string(),
                leverage_multiplier: cfg.target_leverage_multiplier(),
                s1_gross_weight: 0.0,
                s2_gross_weight: 0.0,
                s3_gross_weight: 0.0,
                working_order_count: 0,
                open_long_count: 0,
                open_short_count: 0,
                eligible_coin_count: 0,
                created_at_ms: ts,
                updated_at_ms: ts,
            })
        }
    }

    /// Lists recent `PortfolioSnapshot` rows chronologically (`created_at_ms ASC`) for equity curve charts.
    pub fn list_portfolio_snapshots(&self, limit: usize) -> Result<Vec<PortfolioSnapshot>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, total_equity_usdc, available_cash_usdc, reserved_order_cash_usdc,
                          committed_notional_usdc, net_notional_usdc, gross_leverage_x,
                          net_leverage_x, market_beta_exposure, market_breadth_bull,
                          market_regime_label, risk_tier, leverage_multiplier,
                          s1_gross_weight, s2_gross_weight, s3_gross_weight,
                          working_order_count, open_long_count, open_short_count,
                          eligible_coin_count, created_at_ms, updated_at_ms
                   FROM portfolio_snapshots
                   ORDER BY created_at_ms DESC, id DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| AegisError::Database(format!("list_portfolio_snapshots failed: {e}")))?;

        let mut list: Vec<PortfolioSnapshot> =
            rows.into_iter().map(row_to_portfolio_snapshot).collect();
        list.reverse();
        Ok(list)
    }

    /// Appends a sanitized operational telemetry log event into `telemetry_logs`.
    pub fn log_telemetry(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
    ) -> Result<()> {
        let ts = now_ms();
        let safe_msg = sanitize_telemetry_message(message);
        let details_str = details.map(|d| d.to_string());
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"INSERT INTO telemetry_logs
               (level, component, message, details_json, created_at_ms, updated_at_ms)
               VALUES (:lvl, :comp, :msg, :det, :ts, :ts)"#,
            params! {
                "lvl" => level.trim().to_ascii_uppercase(),
                "comp" => component.trim(),
                "msg" => safe_msg,
                "det" => details_str,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("log_telemetry failed: {e}")))?;
        Ok(())
    }

    /// Lists the most recent `limit` operational telemetry logs (`created_at_ms DESC`).
    pub fn list_recent_telemetry(&self, limit: usize) -> Result<Vec<TelemetryLogRecord>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, level, component, message, CAST(details_json AS CHAR), created_at_ms, updated_at_ms
                   FROM telemetry_logs
                   ORDER BY created_at_ms DESC, id DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| AegisError::Database(format!("list_recent_telemetry failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let raw_json: Option<String> = r.get::<Option<String>, _>(4).unwrap_or(None);
                let parsed_json = raw_json.and_then(|s| serde_json::from_str::<Value>(&s).ok());
                TelemetryLogRecord {
                    id: r.get(0).unwrap_or_default(),
                    level: r.get(1).unwrap_or_else(|| "INFO".to_string()),
                    component: r.get(2).unwrap_or_default(),
                    message: r.get(3).unwrap_or_default(),
                    details_json: parsed_json,
                    created_at_ms: r.get(5).unwrap_or_default(),
                    updated_at_ms: r.get(6).unwrap_or_default(),
                }
            })
            .collect())
    }

    /// Inserts an alert record into `alert_queue`.
    pub fn insert_alert_queue_record(
        &self,
        provider: &str,
        event_type: &str,
        severity: &str,
        message: &str,
        payload_json: Option<&Value>,
        status: &str,
    ) -> Result<i64> {
        let ts = now_ms();
        let payload_str = payload_json.map(|v| v.to_string());
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"INSERT INTO alert_queue
               (provider, event_type, severity, message, payload_json, status, attempts, next_retry_ms, created_at_ms, updated_at_ms)
               VALUES (:prov, :ev, :sev, :msg, :pay, :st, 1, :ts, :ts, :ts)"#,
            params! {
                "prov" => provider,
                "ev" => event_type,
                "sev" => severity,
                "msg" => message,
                "pay" => payload_str,
                "st" => status,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("insert_alert_queue_record failed: {e}")))?;
        Ok(conn.last_insert_id() as i64)
    }

    /// Lists recent alert records from `alert_queue`.
    pub fn list_recent_alerts(&self, limit: usize) -> Result<Vec<AlertQueueRecord>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, provider, event_type, severity, message, CAST(payload_json AS CHAR),
                          status, attempts, next_retry_ms, created_at_ms, updated_at_ms
                   FROM alert_queue
                   ORDER BY created_at_ms DESC, id DESC
                   LIMIT :lim"#,
                params! { "lim" => limit.max(1) as u64 },
            )
            .map_err(|e| AegisError::Database(format!("list_recent_alerts failed: {e}")))?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let raw_json: Option<String> = r.get::<Option<String>, _>(5).unwrap_or(None);
                let parsed_json = raw_json.and_then(|s| serde_json::from_str::<Value>(&s).ok());
                AlertQueueRecord {
                    id: r.get(0).unwrap_or_default(),
                    provider: r.get(1).unwrap_or_else(|| "telegram".to_string()),
                    event_type: r.get(2).unwrap_or_default(),
                    severity: r.get(3).unwrap_or_else(|| "INFO".to_string()),
                    message: r.get(4).unwrap_or_default(),
                    payload_json: parsed_json,
                    status: r.get(6).unwrap_or_else(|| "DELIVERED".to_string()),
                    attempts: r.get(7).unwrap_or(1),
                    next_retry_ms: r.get(8).unwrap_or_default(),
                    created_at_ms: r.get(9).unwrap_or_default(),
                    updated_at_ms: r.get(10).unwrap_or_default(),
                }
            })
            .collect())
    }

    /// Computes multi-horizon PnL metrics (`Daily 24h`, `Weekly 7d`, `Monthly 30d`, `YTD`)
    /// combining closed positions in each horizon window with active `OPEN` unrealized PnL.
    pub fn compute_pnl_summary(
        &self,
        current_unrealized_pnl: f64,
        open_fees_funding: f64,
        current_total_equity: f64,
    ) -> Result<PnlSummary> {
        let now = now_ms();
        let cfg = self.load_config_or_default();
        let live_eq_cfg = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let anchor_equity = if live_eq_cfg > 0.0 {
            live_eq_cfg
        } else if current_total_equity > 0.0 {
            current_total_equity
        } else {
            cfg.get_f64("initial_paper_equity_usdc", 10_000.0)
        }
        .max(100.0);

        let mut conn = self.get_conn()?;
        let build_horizon = |conn: &mut mysql::PooledConn,
                             label: &str,
                             start_ms: i64|
         -> Result<PnlHorizonMetrics> {
            let row: Option<(f64, f64, i64, i64)> = conn
                .exec_first(
                    r#"SELECT
                           COALESCE(SUM(realized_pnl_usdc), 0.0),
                           COALESCE(SUM(funding_accrued_usdc - fees_paid_usdc), 0.0),
                           COUNT(*),
                           COALESCE(SUM(CASE WHEN realized_pnl_usdc > 0.0 THEN 1 ELSE 0 END), 0)
                       FROM positions
                       WHERE status = 'CLOSED'
                         AND COALESCE(closed_at_ms, updated_at_ms) >= :start_ms"#,
                    params! { "start_ms" => start_ms },
                )
                .map_err(|e| {
                    AegisError::Database(format!("compute_pnl_summary query failed: {e}"))
                })?;

            let (closed_rpnl, closed_fees_funding, trade_cnt, win_cnt) =
                row.unwrap_or((0.0, 0.0, 0, 0));

            let realized_pnl_usdc = round_dp(closed_rpnl, 2);
            let unrealized_pnl_usdc = round_dp(current_unrealized_pnl, 2);
            let net_pnl_usdc = round_dp(realized_pnl_usdc + unrealized_pnl_usdc, 2);
            let fees_funding_usdc = round_dp(closed_fees_funding + open_fees_funding, 2);

            let baseline_equity_usdc = round_dp((anchor_equity - net_pnl_usdc).max(100.0), 2);

            let return_pct = if baseline_equity_usdc > 0.0 {
                round_dp((net_pnl_usdc / baseline_equity_usdc) * 100.0, 2)
            } else {
                0.0
            };

            Ok(PnlHorizonMetrics {
                horizon_label: label.to_string(),
                window_start_ms: start_ms,
                baseline_equity_usdc,
                net_pnl_usdc,
                return_pct,
                realized_pnl_usdc,
                unrealized_pnl_usdc,
                fees_funding_usdc,
                trade_count: trade_cnt.max(0) as usize,
                win_count: win_cnt.max(0) as usize,
            })
        };

        let day_ms = 86_400_000i64;
        let daily = build_horizon(&mut conn, "DAILY (24H)", now - day_ms)?;
        let weekly = build_horizon(&mut conn, "WEEKLY (7D)", now - 7 * day_ms)?;
        let monthly = build_horizon(&mut conn, "MONTHLY (30D)", now - 30 * day_ms)?;
        let ytd = build_horizon(&mut conn, "YTD", ytd_start_ms())?;

        Ok(PnlSummary {
            daily,
            weekly,
            monthly,
            ytd,
        })
    }
}

fn row_to_portfolio_snapshot(r: mysql::Row) -> PortfolioSnapshot {
    PortfolioSnapshot {
        id: r.get(0).unwrap_or_default(),
        total_equity_usdc: r.get(1).unwrap_or_default(),
        available_cash_usdc: r.get(2).unwrap_or_default(),
        reserved_order_cash_usdc: r.get(3).unwrap_or_default(),
        committed_notional_usdc: r.get(4).unwrap_or_default(),
        net_notional_usdc: r.get(5).unwrap_or_default(),
        gross_leverage_x: r.get(6).unwrap_or_default(),
        net_leverage_x: r.get(7).unwrap_or_default(),
        market_beta_exposure: r.get(8).unwrap_or_default(),
        market_breadth_bull: r.get(9).unwrap_or_default(),
        market_regime_label: r.get(10).unwrap_or_else(|| "NEUTRAL".to_string()),
        risk_tier: r.get(11).unwrap_or_else(|| "extreme".to_string()),
        leverage_multiplier: r.get(12).unwrap_or(1.85),
        s1_gross_weight: r.get(13).unwrap_or_default(),
        s2_gross_weight: r.get(14).unwrap_or_default(),
        s3_gross_weight: r.get(15).unwrap_or_default(),
        working_order_count: r.get(16).unwrap_or_default(),
        open_long_count: r.get(17).unwrap_or_default(),
        open_short_count: r.get(18).unwrap_or_default(),
        eligible_coin_count: r.get(19).unwrap_or_default(),
        created_at_ms: r.get(20).unwrap_or_default(),
        updated_at_ms: r.get(21).unwrap_or_default(),
    }
}
