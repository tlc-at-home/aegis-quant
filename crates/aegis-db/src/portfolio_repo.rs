//! Portfolio snapshots, retention pruning, telemetry logs, alert queue, funding ledger,
//! and 5-query cached PnL attribution (`aegis-db::portfolio_repo`).

use crate::engine::DatabaseManager;
use crate::row_decode::{
    decimal_param, row_get_f64, row_get_i32, row_get_i64, row_get_opt_f64, row_get_opt_i64,
    row_get_opt_string, row_get_string, DEC_20_10_MAX_ABS, DEC_20_8_MAX_ABS,
};
use aegis_core::{
    is_sensitive_config_key, mask_identifier, now_ms, round_dp, sanitize_telemetry_message,
    ytd_start_ms, AegisError, AlertQueueRecord, CachedHlPortfolioPnl, ConfigMap, OrderSide,
    PnlHorizonMetrics, PnlSummary, PortfolioSnapshot, Result, TelemetryLogRecord,
};
use mysql::prelude::*;
use mysql::{params, PooledConn, Value};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const PNL_SUMMARY_CACHE_TTL_MS: i64 = 5_000;
const CANONICAL_BAR_TIMEFRAMES: [&str; 10] = [
    "1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d",
];

#[derive(Debug, Clone)]
pub(crate) struct PnlSummaryCacheEntry {
    pub key: (i64, i64, i64),
    pub cached_at_ms: i64,
    pub summary: PnlSummary,
}

fn sanitize_telemetry_string(raw: &str) -> String {
    let mut out = sanitize_telemetry_message(raw);
    for prefix in ["ENC:gAAAAAB", "gAAAAAB"] {
        while let Some(start) = out.find(prefix) {
            let tail = &out[start + prefix.len()..];
            let token_len = tail
                .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_' && c != '=')
                .unwrap_or(tail.len());
            let end = start + prefix.len() + token_len;
            out.replace_range(start..end, "[REDACTED_FERNET_TOKEN]");
        }
    }
    if let Some(scheme_pos) = out.find("mysql://") {
        let after_scheme = scheme_pos + 8;
        if let Some(at_rel) = out[after_scheme..].find('@') {
            let at_pos = after_scheme + at_rel;
            if let Some(colon_rel) = out[after_scheme..at_pos].find(':') {
                let colon_pos = after_scheme + colon_rel;
                out.replace_range(colon_pos + 1..at_pos, "***");
            }
        }
    }
    out
}

fn sanitize_json_value(val: JsonValue) -> JsonValue {
    match val {
        JsonValue::String(s) => JsonValue::String(sanitize_telemetry_string(&s)),
        JsonValue::Array(arr) => {
            JsonValue::Array(arr.into_iter().map(sanitize_json_value).collect())
        }
        JsonValue::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                if is_sensitive_config_key(&k) && !k.eq_ignore_ascii_case("telegram_chat_id") {
                    out.insert(k, JsonValue::String("[REDACTED]".to_string()));
                } else if k.eq_ignore_ascii_case("telegram_chat_id")
                    || k.eq_ignore_ascii_case("wallet_address")
                {
                    let masked = match &v {
                        JsonValue::String(s) if !s.trim().is_empty() => {
                            JsonValue::String(mask_identifier(s))
                        }
                        _ => sanitize_json_value(v),
                    };
                    out.insert(k, masked);
                } else {
                    out.insert(k, sanitize_json_value(v));
                }
            }
            JsonValue::Object(out)
        }
        other => other,
    }
}

fn normalize_telemetry_level(raw: &str) -> &'static str {
    match raw.trim().to_ascii_uppercase().as_str() {
        "TRACE" => "TRACE",
        "DEBUG" => "DEBUG",
        "INFO" => "INFO",
        "WARN" => "WARN",
        "WARNING" => "WARNING",
        "ERROR" => "ERROR",
        "CRITICAL" => "CRITICAL",
        _ => "INFO",
    }
}

struct SanitizedFundingEvent {
    asset_id: i64,
    coin: String,
    event_time_ms: i64,
    usdc: String,
    remaining_usdc: String,
    position_szi: String,
    funding_rate: String,
    dedup_key: String,
}

#[inline]
fn finite_or_zero(val: f64) -> f64 {
    if val.is_finite() {
        val
    } else {
        0.0
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotInsertInput {
    pub total_equity_usdc: f64,
    pub uncapped_equity_usdc: f64,
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

/// Batch telemetry insert entry used by `aegis-telemetry`'s non-blocking OS writer thread (`R14-M17`).
#[derive(Debug, Clone)]
pub struct TelemetryInsertInput {
    pub level: String,
    pub component: String,
    pub message: String,
    pub details: Option<JsonValue>,
    pub created_at_ms: Option<i64>,
}

/// Full parameterized insert payload for `alert_queue` (`R14-L14`).
#[derive(Debug, Clone)]
pub struct AlertQueueInsertInput<'a> {
    pub provider: &'a str,
    pub event_type: &'a str,
    pub severity: &'a str,
    pub message: &'a str,
    pub payload_json: Option<&'a JsonValue>,
    pub status: &'a str,
    pub attempts: i32,
    pub next_retry_ms: i64,
    pub dispatched_at_ms: Option<i64>,
}

/// Input record for batch upserting Hyperliquid funding ledger events into `funding_events`.
#[derive(Debug, Clone)]
pub struct FundingEventUpsertInput {
    pub asset_id: i64,
    pub coin: String,
    pub event_time_ms: i64,
    pub usdc: f64,
    pub remaining_usdc: f64,
    pub position_szi: f64,
    pub funding_rate: f64,
    pub dedup_key: String,
}

/// Persisted Hyperliquid funding ledger row from `funding_events`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingEventRecord {
    pub id: i64,
    pub asset_id: i64,
    pub coin: String,
    pub event_time_ms: i64,
    pub usdc: f64,
    pub remaining_usdc: f64,
    pub position_szi: f64,
    pub funding_rate: f64,
    pub dedup_key: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Config-driven InnoDB retention policy (`R14-M13`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub snapshots_days: i64,
    pub telemetry_days: i64,
    pub order_audit_days: i64,
    pub cooldowns_days: i64,
    pub alert_queue_days: i64,
    pub ohlcv_bars_days: i64,
    pub funding_events_days: i64,
    pub batch_size: usize,
    pub max_batches_per_table: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            snapshots_days: 30,
            telemetry_days: 14,
            order_audit_days: 30,
            cooldowns_days: 14,
            alert_queue_days: 30,
            ohlcv_bars_days: 180,
            funding_events_days: 365,
            batch_size: 1000,
            max_batches_per_table: 20,
        }
    }
}

impl RetentionPolicy {
    pub fn from_config(cfg: &ConfigMap) -> Self {
        Self {
            snapshots_days: cfg.get_i64("retention_snapshots_days", 30).clamp(1, 365),
            telemetry_days: cfg.get_i64("retention_telemetry_days", 14).clamp(1, 365),
            order_audit_days: cfg.get_i64("retention_order_audit_days", 30).clamp(1, 365),
            cooldowns_days: cfg.get_i64("retention_cooldowns_days", 14).clamp(1, 365),
            alert_queue_days: cfg.get_i64("retention_alert_queue_days", 30).clamp(1, 365),
            ohlcv_bars_days: cfg
                .get_i64("retention_ohlcv_bars_days", 180)
                .clamp(30, 1095),
            funding_events_days: cfg
                .get_i64("retention_funding_events_days", 365)
                .clamp(30, 1095),
            batch_size: cfg
                .get_usize("retention_prune_batch_size", 1000)
                .clamp(100, 5000),
            max_batches_per_table: 20,
        }
    }
}

/// Summary report emitted by [`DatabaseManager::prune_retention`] (`R14-M13`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetentionPruneReport {
    pub snapshots_deleted: u64,
    pub telemetry_deleted: u64,
    pub order_audit_deleted: u64,
    pub cooldowns_deleted: u64,
    pub alert_queue_deleted: u64,
    pub ohlcv_bars_deleted: u64,
    pub funding_events_deleted: u64,
    pub total_deleted: u64,
}

impl DatabaseManager {
    /// Inserts a unified portfolio snapshot and sleeve attribution record.
    pub fn insert_portfolio_snapshot(&self, input: &SnapshotInsertInput) -> Result<i64> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"INSERT INTO portfolio_snapshots (
                   total_equity_usdc, uncapped_equity_usdc, available_cash_usdc,
                   reserved_order_cash_usdc, committed_notional_usdc, net_notional_usdc,
                   gross_leverage_x, net_leverage_x, market_beta_exposure, market_breadth_bull,
                   market_regime_label, risk_tier, leverage_multiplier,
                   s1_gross_weight, s2_gross_weight, s3_gross_weight,
                   working_order_count, open_long_count, open_short_count,
                   eligible_coin_count, created_at_ms, updated_at_ms
               ) VALUES (
                   :eq, :ueq, :cash,
                   :res, :comm, :net_n,
                   :g_lev, :n_lev, :beta, :breadth,
                   :regime, :tier, :mult,
                   :s1_w, :s2_w, :s3_w,
                   :w_cnt, :l_cnt, :s_cnt,
                   :elig_cnt, :ts, :ts
               )"#,
            params! {
                "eq" => decimal_param(input.total_equity_usdc, 8, DEC_20_8_MAX_ABS)?,
                "ueq" => decimal_param(input.uncapped_equity_usdc, 8, DEC_20_8_MAX_ABS)?,
                "cash" => decimal_param(input.available_cash_usdc, 8, DEC_20_8_MAX_ABS)?,
                "res" => decimal_param(input.reserved_order_cash_usdc, 8, DEC_20_8_MAX_ABS)?,
                "comm" => decimal_param(input.committed_notional_usdc, 8, DEC_20_8_MAX_ABS)?,
                "net_n" => decimal_param(input.net_notional_usdc, 8, DEC_20_8_MAX_ABS)?,
                "g_lev" => decimal_param(input.gross_leverage_x, 8, DEC_20_8_MAX_ABS)?,
                "n_lev" => decimal_param(input.net_leverage_x, 8, DEC_20_8_MAX_ABS)?,
                "beta" => decimal_param(input.market_beta_exposure, 8, DEC_20_8_MAX_ABS)?,
                "breadth" => decimal_param(input.market_breadth_bull, 8, DEC_20_8_MAX_ABS)?,
                "regime" => &input.market_regime_label,
                "tier" => &input.risk_tier,
                "mult" => decimal_param(input.leverage_multiplier, 8, DEC_20_8_MAX_ABS)?,
                "s1_w" => decimal_param(input.s1_gross_weight, 8, DEC_20_8_MAX_ABS)?,
                "s2_w" => decimal_param(input.s2_gross_weight, 8, DEC_20_8_MAX_ABS)?,
                "s3_w" => decimal_param(input.s3_gross_weight, 8, DEC_20_8_MAX_ABS)?,
                "w_cnt" => input.working_order_count,
                "l_cnt" => input.open_long_count,
                "s_cnt" => input.open_short_count,
                "elig_cnt" => input.eligible_coin_count,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("insert_portfolio_snapshot failed: {e}")))?;
        let id = conn.last_insert_id() as i64;
        self.invalidate_pnl_cache();
        Ok(id)
    }

    pub(crate) fn get_latest_portfolio_snapshot_opt_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Option<PortfolioSnapshot>> {
        let row: Option<mysql::Row> = conn
            .query_first(
                r#"SELECT id, total_equity_usdc, uncapped_equity_usdc, available_cash_usdc,
                          reserved_order_cash_usdc, committed_notional_usdc, net_notional_usdc,
                          gross_leverage_x, net_leverage_x, market_beta_exposure, market_breadth_bull,
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
        Ok(row.map(|r| row_to_portfolio_snapshot(&r)))
    }

    /// Returns the most recent `PortfolioSnapshot`, or a synthesized baseline snapshot if empty.
    pub fn get_latest_portfolio_snapshot(&self) -> Result<PortfolioSnapshot> {
        let row_opt = {
            let mut conn = self.get_conn()?;
            Self::get_latest_portfolio_snapshot_opt_with_conn(&mut conn)?
        };

        if let Some(snap) = row_opt {
            Ok(snap)
        } else {
            let cfg = self.load_config_or_default();
            let eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
            let ts = now_ms();
            Ok(PortfolioSnapshot {
                id: 0,
                total_equity_usdc: eq,
                uncapped_equity_usdc: eq,
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
        Self::list_portfolio_snapshots_with_conn(&mut conn, limit)
    }

    pub(crate) fn list_portfolio_snapshots_with_conn(
        conn: &mut PooledConn,
        limit: usize,
    ) -> Result<Vec<PortfolioSnapshot>> {
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, total_equity_usdc, uncapped_equity_usdc, available_cash_usdc,
                          reserved_order_cash_usdc, committed_notional_usdc, net_notional_usdc,
                          gross_leverage_x, net_leverage_x, market_beta_exposure, market_breadth_bull,
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

        let mut list: Vec<PortfolioSnapshot> = rows.iter().map(row_to_portfolio_snapshot).collect();
        list.reverse();
        Ok(list)
    }

    /// Returns `(peak_equity, latest_equity)` across `portfolio_snapshots` (`total_equity_usdc`)
    /// since `since_ms`, or `None` if `portfolio_snapshots` has no rows.
    pub fn get_peak_and_latest_equity_since(&self, since_ms: i64) -> Result<Option<(f64, f64)>> {
        self.query_peak_and_latest_since_expr("total_equity_usdc", since_ms)
    }

    /// Returns `(peak_uncapped_equity, latest_uncapped_equity)` across `portfolio_snapshots` since
    /// `since_ms` (`P1-Acct`), falling back to `total_equity_usdc` on legacy rows where
    /// `uncapped_equity_usdc = 0`.
    pub fn get_peak_and_latest_uncapped_equity_since(
        &self,
        since_ms: i64,
    ) -> Result<Option<(f64, f64)>> {
        self.query_peak_and_latest_since_expr(
            "COALESCE(NULLIF(uncapped_equity_usdc, 0), total_equity_usdc)",
            since_ms,
        )
    }

    fn query_peak_and_latest_since_expr(
        &self,
        col_expr: &str,
        since_ms: i64,
    ) -> Result<Option<(f64, f64)>> {
        let mut conn = self.get_conn()?;
        let latest_sql = format!(
            "SELECT {col_expr} FROM portfolio_snapshots ORDER BY created_at_ms DESC, id DESC LIMIT 1"
        );
        let latest_row: Option<mysql::Row> = conn.query_first(latest_sql).map_err(|e| {
            AegisError::Database(format!(
                "get_peak_and_latest_equity_since latest query failed: {e}"
            ))
        })?;

        let Some(latest_r) = latest_row else {
            return Ok(None);
        };
        let latest_equity = row_get_f64(&latest_r, 0, 0.0);

        let peak_sql = format!(
            "SELECT MAX({col_expr}) FROM portfolio_snapshots WHERE created_at_ms >= :since_ms AND {col_expr} > 0"
        );
        let peak_row: Option<mysql::Row> = conn
            .exec_first(peak_sql, params! { "since_ms" => since_ms })
            .map_err(|e| {
                AegisError::Database(format!(
                    "get_peak_and_latest_equity_since peak query failed: {e}"
                ))
            })?;

        let peak_equity = peak_row
            .and_then(|r| row_get_opt_f64(&r, 0))
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or_else(|| {
                if latest_equity.is_finite() {
                    latest_equity.max(0.0)
                } else {
                    0.0
                }
            });

        Ok(Some((peak_equity, latest_equity)))
    }

    /// Appends a sanitized operational telemetry log event into `telemetry_logs`.
    pub fn log_telemetry(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<JsonValue>,
    ) -> Result<()> {
        let ts = now_ms();
        let safe_msg = sanitize_telemetry_string(message);
        let details_str = details.map(|d| sanitize_json_value(d).to_string());
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"INSERT INTO telemetry_logs
               (level, component, message, details_json, created_at_ms, updated_at_ms)
               VALUES (:lvl, :comp, :msg, :det, :ts, :ts)"#,
            params! {
                "lvl" => normalize_telemetry_level(level),
                "comp" => component.trim(),
                "msg" => safe_msg,
                "det" => details_str,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("log_telemetry failed: {e}")))?;
        Ok(())
    }

    /// Persists a batch of sanitized operational telemetry events inside one MariaDB transaction (`R14-M17`).
    pub fn log_telemetry_batch(&self, entries: &[TelemetryInsertInput]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }
        let fallback_ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("log_telemetry_batch tx failed: {e}")))?;

        let stmt = tx
            .prep(
                r#"INSERT INTO telemetry_logs
                   (level, component, message, details_json, created_at_ms, updated_at_ms)
                   VALUES (:lvl, :comp, :msg, :det, :ts, :ts)"#,
            )
            .map_err(|e| AegisError::Database(format!("log_telemetry_batch prep failed: {e}")))?;

        let prepared: Vec<(&'static str, String, String, Option<String>, i64)> = entries
            .iter()
            .map(|e| {
                let ts = e.created_at_ms.filter(|&t| t > 0).unwrap_or(fallback_ts);
                (
                    normalize_telemetry_level(&e.level),
                    e.component.trim().chars().take(64).collect(),
                    sanitize_telemetry_string(&e.message),
                    e.details
                        .as_ref()
                        .map(|d| sanitize_json_value(d.clone()).to_string()),
                    ts,
                )
            })
            .collect();

        tx.exec_batch(
            &stmt,
            prepared.iter().map(|(lvl, comp, msg, det, ts)| {
                params! {
                    "lvl" => *lvl,
                    "comp" => comp,
                    "msg" => msg,
                    "det" => det,
                    "ts" => *ts,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("log_telemetry_batch exec failed: {e}")))?;
        drop(stmt);

        tx.commit()
            .map_err(|e| AegisError::Database(format!("log_telemetry_batch commit failed: {e}")))?;
        Ok(entries.len())
    }

    /// Lists the most recent `limit` operational telemetry logs (`created_at_ms DESC`).
    pub fn list_recent_telemetry(&self, limit: usize) -> Result<Vec<TelemetryLogRecord>> {
        let mut conn = self.get_conn()?;
        Self::list_recent_telemetry_with_conn(&mut conn, limit)
    }

    pub(crate) fn list_recent_telemetry_with_conn(
        conn: &mut PooledConn,
        limit: usize,
    ) -> Result<Vec<TelemetryLogRecord>> {
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
            .iter()
            .map(|r| {
                let raw_json = row_get_opt_string(r, 4);
                let parsed_json = raw_json.and_then(|s| serde_json::from_str::<JsonValue>(&s).ok());
                TelemetryLogRecord {
                    id: row_get_i64(r, 0, 0),
                    level: row_get_string(r, 1, "INFO"),
                    component: row_get_string(r, 2, ""),
                    message: row_get_string(r, 3, ""),
                    details_json: parsed_json,
                    created_at_ms: row_get_i64(r, 5, 0),
                    updated_at_ms: row_get_i64(r, 6, 0),
                }
            })
            .collect())
    }

    /// Inserts an alert record into `alert_queue` with explicit retry & dispatch metadata (`R14-L14`).
    pub fn insert_alert_queue_record_full(&self, input: &AlertQueueInsertInput<'_>) -> Result<i64> {
        let ts = now_ms();
        let payload_str = input.payload_json.map(|v| v.to_string());
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            r#"INSERT INTO alert_queue
               (provider, event_type, severity, message, payload_json, status, attempts, next_retry_ms, dispatched_at_ms, created_at_ms, updated_at_ms)
               VALUES (:prov, :ev, :sev, :msg, :pay, :st, :att, :next_retry, :disp_at, :ts, :ts)"#,
            params! {
                "prov" => input.provider,
                "ev" => input.event_type,
                "sev" => input.severity,
                "msg" => input.message,
                "pay" => payload_str,
                "st" => input.status,
                "att" => input.attempts.max(0),
                "next_retry" => input.next_retry_ms,
                "disp_at" => input.dispatched_at_ms,
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("insert_alert_queue_record_full failed: {e}")))?;
        Ok(conn.last_insert_id() as i64)
    }

    /// Inserts an alert record into `alert_queue`.
    pub fn insert_alert_queue_record(
        &self,
        provider: &str,
        event_type: &str,
        severity: &str,
        message: &str,
        payload_json: Option<&JsonValue>,
        status: &str,
    ) -> Result<i64> {
        let ts = now_ms();
        self.insert_alert_queue_record_full(&AlertQueueInsertInput {
            provider,
            event_type,
            severity,
            message,
            payload_json,
            status,
            attempts: 1,
            next_retry_ms: ts,
            dispatched_at_ms: None,
        })
    }

    /// Marks `dispatched_at_ms` on an `alert_queue` row (`R14-L14`).
    pub fn mark_alert_queue_dispatched(&self, alert_id: i64, dispatched_at_ms: i64) -> Result<()> {
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        conn.exec_drop(
            "UPDATE alert_queue SET dispatched_at_ms = :disp, updated_at_ms = :ts WHERE id = :id",
            params! {
                "disp" => dispatched_at_ms,
                "ts" => ts,
                "id" => alert_id,
            },
        )
        .map_err(|e| AegisError::Database(format!("mark_alert_queue_dispatched failed: {e}")))?;
        Ok(())
    }

    /// Reads `dispatched_at_ms` for an `alert_queue` row (`R14-L14`).
    pub fn get_alert_queue_dispatched_at_ms(&self, alert_id: i64) -> Result<Option<i64>> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .exec_first(
                "SELECT dispatched_at_ms FROM alert_queue WHERE id = :id LIMIT 1",
                params! { "id" => alert_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("get_alert_queue_dispatched_at_ms failed: {e}"))
            })?;
        Ok(row.and_then(|r| row_get_opt_i64(&r, 0)))
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
            .iter()
            .map(|r| {
                let raw_json = row_get_opt_string(r, 5);
                let parsed_json = raw_json.and_then(|s| serde_json::from_str::<JsonValue>(&s).ok());
                AlertQueueRecord {
                    id: row_get_i64(r, 0, 0),
                    provider: row_get_string(r, 1, "telegram"),
                    event_type: row_get_string(r, 2, ""),
                    severity: row_get_string(r, 3, "INFO"),
                    message: row_get_string(r, 4, ""),
                    payload_json: parsed_json,
                    status: row_get_string(r, 6, "DELIVERED"),
                    attempts: row_get_i32(r, 7, 1),
                    next_retry_ms: row_get_i64(r, 8, 0),
                    created_at_ms: row_get_i64(r, 9, 0),
                    updated_at_ms: row_get_i64(r, 10, 0),
                }
            })
            .collect())
    }

    /// Runs InnoDB-safe batched retention pruning across all 7 append-only/historical tables,
    /// preserving 1 UTC-day anchor snapshot up to 400d (for Monthly/YTD PnL baselines), active
    /// `order_audit` / `ohlcv_bars`, unsettled `funding_events`, and active cooldowns/alerts (`R14-M13`).
    pub fn prune_retention(&self, policy: &RetentionPolicy) -> Result<RetentionPruneReport> {
        let now = now_ms();
        let batch_size = policy.batch_size.clamp(100, 5000);
        let max_batches = policy.max_batches_per_table.max(1);
        let mut conn = self.get_conn()?;
        let mut report = RetentionPruneReport::default();

        // 1. portfolio_snapshots (preserve earliest anchor per UTC day in [now - 400d, now - snapshots_days))
        let snap_cutoff = now - policy.snapshots_days.max(1) * 86_400_000;
        let anchor_cutoff = now - 400 * 86_400_000;
        report.snapshots_deleted = prune_snapshots_with_anchors(
            &mut conn,
            snap_cutoff,
            anchor_cutoff,
            true,
            batch_size,
            max_batches,
        )?;

        // 2. telemetry_logs
        let telemetry_cutoff = now - policy.telemetry_days.max(1) * 86_400_000;
        report.telemetry_deleted = batched_pk_delete(
            &mut conn,
            "telemetry_logs",
            batch_size,
            max_batches,
            |c, lim| {
                let rows: Vec<mysql::Row> = c
                    .exec(
                        "SELECT id FROM telemetry_logs WHERE created_at_ms < :cutoff ORDER BY created_at_ms ASC, id ASC LIMIT :lim",
                        params! { "cutoff" => telemetry_cutoff, "lim" => lim as u64 },
                    )
                    .map_err(|e| AegisError::Database(format!("prune telemetry_logs select failed: {e}")))?;
                Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
            },
        )?;

        // 3. order_audit (protect active OPEN/WORKING position history)
        let audit_cutoff = now - policy.order_audit_days.max(1) * 86_400_000;
        report.order_audit_deleted = batched_pk_delete(
            &mut conn,
            "order_audit",
            batch_size,
            max_batches,
            |c, lim| {
                let rows: Vec<mysql::Row> = c
                    .exec(
                        r#"SELECT oa.id
                           FROM order_audit oa
                           LEFT JOIN positions p ON p.id = oa.position_id
                           WHERE oa.created_at_ms < :cutoff
                             AND (oa.position_id IS NULL OR p.id IS NULL OR p.status NOT IN ('OPEN', 'WORKING'))
                           ORDER BY oa.created_at_ms ASC, oa.id ASC
                           LIMIT :lim"#,
                        params! { "cutoff" => audit_cutoff, "lim" => lim as u64 },
                    )
                    .map_err(|e| AegisError::Database(format!("prune order_audit select failed: {e}")))?;
                Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
            },
        )?;

        // 4. asset_cooldowns (only inactive expired rows)
        let cd_cutoff = now - policy.cooldowns_days.max(1) * 86_400_000;
        report.cooldowns_deleted = batched_pk_delete(
            &mut conn,
            "asset_cooldowns",
            batch_size,
            max_batches,
            |c, lim| {
                let rows: Vec<mysql::Row> = c
                    .exec(
                        "SELECT id FROM asset_cooldowns WHERE is_active = 0 AND expires_at_ms < :cutoff ORDER BY expires_at_ms ASC, id ASC LIMIT :lim",
                        params! { "cutoff" => cd_cutoff, "lim" => lim as u64 },
                    )
                    .map_err(|e| AegisError::Database(format!("prune asset_cooldowns select failed: {e}")))?;
                Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
            },
        )?;

        // 5. alert_queue (only terminal states)
        let alert_cutoff = now - policy.alert_queue_days.max(1) * 86_400_000;
        report.alert_queue_deleted = batched_pk_delete(
            &mut conn,
            "alert_queue",
            batch_size,
            max_batches,
            |c, lim| {
                let rows: Vec<mysql::Row> = c
                    .exec(
                        "SELECT id FROM alert_queue WHERE status IN ('DELIVERED', 'SKIPPED', 'FAILED') AND created_at_ms < :cutoff ORDER BY created_at_ms ASC, id ASC LIMIT :lim",
                        params! { "cutoff" => alert_cutoff, "lim" => lim as u64 },
                    )
                    .map_err(|e| AegisError::Database(format!("prune alert_queue select failed: {e}")))?;
                Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
            },
        )?;

        // 6. ohlcv_bars (protect bars older than active open positions for start_upnl)
        let raw_bars_cutoff = now - policy.ohlcv_bars_days.max(1) * 86_400_000;
        let min_open_row: Option<mysql::Row> = conn
            .query_first(
                "SELECT MIN(COALESCE(opened_at_ms, created_at_ms)) FROM positions WHERE status IN ('OPEN', 'WORKING')",
            )
            .map_err(|e| AegisError::Database(format!("prune ohlcv_bars open lookup failed: {e}")))?;
        let eff_bars_cutoff = min_open_row
            .and_then(|r| row_get_opt_i64(&r, 0))
            .filter(|&t| t > 0)
            .map_or(raw_bars_cutoff, |t| {
                raw_bars_cutoff.min(t.saturating_sub(86_400_000))
            });
        let mut bars_batches_left = max_batches;
        for tf in CANONICAL_BAR_TIMEFRAMES {
            if bars_batches_left == 0 {
                break;
            }
            let (deleted, used) = batched_pk_delete_counted(
                &mut conn,
                "ohlcv_bars",
                batch_size,
                bars_batches_left,
                |c, lim| {
                    let rows: Vec<mysql::Row> = c
                        .exec(
                            "SELECT id FROM ohlcv_bars WHERE timeframe = :tf AND timestamp_ms < :cutoff ORDER BY timestamp_ms ASC LIMIT :lim",
                            params! { "tf" => tf, "cutoff" => eff_bars_cutoff, "lim" => lim as u64 },
                        )
                        .map_err(|e| AegisError::Database(format!("prune ohlcv_bars select ({tf}) failed: {e}")))?;
                    Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
                },
            )?;
            report.ohlcv_bars_deleted += deleted;
            bars_batches_left = bars_batches_left.saturating_sub(used);
        }

        // 7. funding_events (only settled rows)
        let funding_cutoff = now - policy.funding_events_days.max(1) * 86_400_000;
        report.funding_events_deleted = batched_pk_delete(
            &mut conn,
            "funding_events",
            batch_size,
            max_batches,
            |c, lim| {
                let rows: Vec<mysql::Row> = c
                    .exec(
                        "SELECT id FROM funding_events WHERE ABS(remaining_usdc) <= 0.00000001 AND event_time_ms < :cutoff ORDER BY event_time_ms ASC, id ASC LIMIT :lim",
                        params! { "cutoff" => funding_cutoff, "lim" => lim as u64 },
                    )
                    .map_err(|e| AegisError::Database(format!("prune funding_events select failed: {e}")))?;
                Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
            },
        )?;

        report.total_deleted = report.snapshots_deleted
            + report.telemetry_deleted
            + report.order_audit_deleted
            + report.cooldowns_deleted
            + report.alert_queue_deleted
            + report.ohlcv_bars_deleted
            + report.funding_events_deleted;

        if report.snapshots_deleted > 0 || report.funding_events_deleted > 0 {
            self.invalidate_pnl_cache();
        }
        Ok(report)
    }

    /// Legacy retention helper kept for direct compatibility (`keep_days` on `portfolio_snapshots`
    /// and `telemetry_logs`, delegating to the same InnoDB-safe batched PK delete engine).
    pub fn prune_old_snapshots_and_logs(&self, keep_days: i64) -> Result<(u64, u64)> {
        let now = now_ms();
        let cutoff_ms = now - keep_days.max(1) * 86_400_000;
        let mut conn = self.get_conn()?;

        let snaps_deleted = prune_snapshots_with_anchors(&mut conn, cutoff_ms, 0, false, 1000, 50)?;
        let logs_deleted = batched_pk_delete(&mut conn, "telemetry_logs", 1000, 50, |c, lim| {
            let rows: Vec<mysql::Row> = c
                    .exec(
                        "SELECT id FROM telemetry_logs WHERE created_at_ms < :cutoff ORDER BY created_at_ms ASC, id ASC LIMIT :lim",
                        params! { "cutoff" => cutoff_ms, "lim" => lim as u64 },
                    )
                    .map_err(|e| AegisError::Database(format!("prune_old_snapshots_and_logs logs failed: {e}")))?;
            Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
        })?;

        if snaps_deleted > 0 {
            self.invalidate_pnl_cache();
        }
        Ok((snaps_deleted, logs_deleted))
    }

    /// Batch upserts reconciled Hyperliquid funding events into `funding_events` in a single transaction.
    pub fn upsert_funding_events_batch(&self, events: &[FundingEventUpsertInput]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let mut sanitized = Vec::with_capacity(events.len());
        for ev in events {
            sanitized.push(SanitizedFundingEvent {
                asset_id: ev.asset_id,
                coin: ev.coin.trim().chars().take(24).collect(),
                event_time_ms: ev.event_time_ms,
                usdc: decimal_param(finite_or_zero(ev.usdc), 8, DEC_20_8_MAX_ABS)?,
                remaining_usdc: decimal_param(
                    finite_or_zero(ev.remaining_usdc),
                    8,
                    DEC_20_8_MAX_ABS,
                )?,
                position_szi: decimal_param(finite_or_zero(ev.position_szi), 8, DEC_20_8_MAX_ABS)?,
                funding_rate: decimal_param(
                    finite_or_zero(ev.funding_rate),
                    10,
                    DEC_20_10_MAX_ABS,
                )?,
                dedup_key: ev.dedup_key.chars().take(128).collect(),
            });
        }

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start funding_events tx: {e}")))?;

        let stmt = tx
            .prep(
                r#"INSERT INTO funding_events
                   (asset_id, coin, event_time_ms, usdc, remaining_usdc, position_szi, funding_rate, dedup_key, created_at_ms, updated_at_ms)
                   VALUES (:aid, :coin, :ev_ts, :usdc, :rem_usdc, :szi, :fr, :dkey, :ts, :ts)
                   ON DUPLICATE KEY UPDATE
                   coin = VALUES(coin),
                   usdc = VALUES(usdc),
                   remaining_usdc = VALUES(remaining_usdc),
                   position_szi = VALUES(position_szi),
                   funding_rate = VALUES(funding_rate),
                   updated_at_ms = VALUES(updated_at_ms)"#,
            )
            .map_err(|e| {
                AegisError::Database(format!("Failed to prepare funding_events upsert: {e}"))
            })?;

        tx.exec_batch(
            &stmt,
            sanitized.iter().map(|ev| {
                params! {
                    "aid" => ev.asset_id,
                    "coin" => &ev.coin,
                    "ev_ts" => ev.event_time_ms,
                    "usdc" => &ev.usdc,
                    "rem_usdc" => &ev.remaining_usdc,
                    "szi" => &ev.position_szi,
                    "fr" => &ev.funding_rate,
                    "dkey" => &ev.dedup_key,
                    "ts" => ts,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("Failed to upsert funding_events batch: {e}")))?;
        drop(stmt);

        tx.commit()
            .map_err(|e| AegisError::Database(format!("Failed to commit funding_events: {e}")))?;
        self.invalidate_pnl_cache();
        Ok(events.len())
    }

    /// Lists reconciled `funding_events` records for a specific `asset_id` ordered chronologically.
    pub fn list_funding_events_for_asset(&self, asset_id: i64) -> Result<Vec<FundingEventRecord>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, asset_id, coin, event_time_ms, usdc, remaining_usdc,
                          position_szi, funding_rate, dedup_key, created_at_ms, updated_at_ms
                   FROM funding_events
                   WHERE asset_id = :aid
                   ORDER BY event_time_ms ASC, id ASC"#,
                params! { "aid" => asset_id },
            )
            .map_err(|e| {
                AegisError::Database(format!("list_funding_events_for_asset failed: {e}"))
            })?;

        Ok(rows.iter().map(row_to_funding_event_record).collect())
    }

    /// Lists all `funding_events` across all assets in `[since_ms, until_ms]` grouped by `asset_id`
    /// as `(event_time_ms, funding_rate)` sorted chronologically (`R14-M15`).
    pub fn list_funding_events_window(
        &self,
        since_ms: i64,
        until_ms: i64,
    ) -> Result<HashMap<i64, Vec<(i64, f64)>>> {
        if since_ms > until_ms {
            return Ok(HashMap::new());
        }
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT asset_id, event_time_ms, funding_rate
                   FROM funding_events
                   WHERE event_time_ms >= :since_ms
                     AND event_time_ms <= :until_ms
                   ORDER BY asset_id ASC, event_time_ms ASC, id ASC"#,
                params! { "since_ms" => since_ms, "until_ms" => until_ms },
            )
            .map_err(|e| AegisError::Database(format!("list_funding_events_window failed: {e}")))?;

        let mut grouped: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();
        for r in &rows {
            let aid = row_get_i64(r, 0, 0);
            let ts = row_get_i64(r, 1, 0);
            let fr = row_get_f64(r, 2, 0.0);
            grouped.entry(aid).or_default().push((ts, fr));
        }
        Ok(grouped)
    }

    /// Lists all `funding_events` rows ordered chronologically (used for full 11-table backup export).
    pub fn list_all_funding_events(&self) -> Result<Vec<FundingEventRecord>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT id, asset_id, coin, event_time_ms, usdc, remaining_usdc,
                          position_szi, funding_rate, dedup_key, created_at_ms, updated_at_ms
                   FROM funding_events
                   ORDER BY event_time_ms ASC, id ASC"#,
            )
            .map_err(|e| AegisError::Database(format!("list_all_funding_events failed: {e}")))?;
        Ok(rows.iter().map(row_to_funding_event_record).collect())
    }

    /// Computes multi-horizon PnL metrics (`Daily 24h`, `Weekly 7d`, `Monthly 30d`, `YTD`)
    /// with a 5-second invalidated TTL cache (`R14-M25`).
    pub fn compute_pnl_summary(
        &self,
        current_unrealized_pnl: f64,
        open_fees_funding: f64,
        current_total_equity: f64,
    ) -> Result<PnlSummary> {
        let key = (
            (finite_or_zero(current_unrealized_pnl) * 100.0).round() as i64,
            (finite_or_zero(open_fees_funding) * 100.0).round() as i64,
            (finite_or_zero(current_total_equity) * 100.0).round() as i64,
        );
        let now = now_ms();
        {
            let guard = self.pnl_cache.lock();
            if let Some(ref entry) = *guard {
                if entry.key == key
                    && now.saturating_sub(entry.cached_at_ms) <= PNL_SUMMARY_CACHE_TTL_MS
                {
                    return Ok(entry.summary.clone());
                }
            }
        }

        let summary = self.compute_pnl_summary_uncached(
            current_unrealized_pnl,
            open_fees_funding,
            current_total_equity,
        )?;
        *self.pnl_cache.lock() = Some(PnlSummaryCacheEntry {
            key,
            cached_at_ms: now_ms(),
            summary: summary.clone(),
        });
        Ok(summary)
    }

    /// Uncached multi-horizon PnL calculation executed over at most 5 indexed MariaDB queries
    /// with zero `ORDER BY CASE` filesorts (`R14-M25`).
    pub fn compute_pnl_summary_uncached(
        &self,
        current_unrealized_pnl: f64,
        open_fees_funding: f64,
        current_total_equity: f64,
    ) -> Result<PnlSummary> {
        let now = now_ms();
        let cfg = self.load_config_or_default();
        let is_live_or_testnet = cfg.execution_mode().is_live_or_testnet();
        let cap_updated_at_ms = if is_live_or_testnet {
            cfg.get_i64("risk_capital_cap_updated_at_ms", 0)
                .max(0)
                .min(now)
        } else {
            0
        };

        let live_eq_cfg = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let cap_cfg = cfg.get_f64("risk_capital_cap_usdc", 0.0);
        let fallback_equity = if current_total_equity.is_finite() && current_total_equity > 0.0 {
            current_total_equity
        } else if live_eq_cfg.is_finite() && live_eq_cfg > 0.0 {
            live_eq_cfg
        } else if is_live_or_testnet && cap_cfg.is_finite() && cap_cfg > 0.0 {
            cap_cfg
        } else {
            cfg.get_f64("initial_paper_equity_usdc", 10_000.0)
        }
        .max(1.0);

        let hl_portfolio_opt: Option<CachedHlPortfolioPnl> = if is_live_or_testnet {
            let raw_json = cfg.get_str("live_exchange_portfolio_pnl_json", "");
            (!raw_json.is_empty())
                .then(|| serde_json::from_str::<CachedHlPortfolioPnl>(&raw_json).ok())
                .flatten()
                .filter(|c| c.is_fresh(now, 600_000))
        } else {
            None
        };

        let day_ms = 86_400_000i64;
        let specs = [
            ("daily", "DAILY (24H)", now - day_ms),
            ("weekly", "WEEKLY (7D)", now - 7 * day_ms),
            ("monthly", "MONTHLY (30D)", now - 30 * day_ms),
            ("ytd", "YTD", ytd_start_ms()),
        ];

        let mut is_cap_clamped = [false; 4];
        let mut eff_starts = [0i64; 4];
        let mut hl_matches: [Option<(f64, i64, bool)>; 4] = [None, None, None, None];
        for i in 0..4 {
            let (key, _, raw_start) = specs[i];
            is_cap_clamped[i] = cap_updated_at_ms > 0 && cap_updated_at_ms > raw_start;
            eff_starts[i] = if cap_updated_at_ms > 0 {
                raw_start.max(cap_updated_at_ms)
            } else {
                raw_start
            };
            hl_matches[i] = hl_portfolio_opt
                .as_ref()
                .and_then(|hl| hl.horizon_net_pnl(key, raw_start, cap_updated_at_ms))
                .filter(|(net, _, _)| net.is_finite());
        }
        let min_start = eff_starts.iter().copied().min().unwrap_or(now - day_ms);

        let mut conn = self.get_conn()?;

        // Query 1: Single-pass conditional aggregation over closed positions (uses idx_positions_status_closed)
        let closed_row: Option<mysql::Row> = conn
            .exec_first(
                r#"SELECT
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :d_s THEN realized_pnl_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :d_s THEN funding_accrued_usdc - fees_paid_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :d_s THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :d_s AND realized_pnl_usdc > 0 THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :w_s THEN realized_pnl_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :w_s THEN funding_accrued_usdc - fees_paid_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :w_s THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :w_s AND realized_pnl_usdc > 0 THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :m_s THEN realized_pnl_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :m_s THEN funding_accrued_usdc - fees_paid_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :m_s THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :m_s AND realized_pnl_usdc > 0 THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :y_s THEN realized_pnl_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :y_s THEN funding_accrued_usdc - fees_paid_usdc ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :y_s THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN closed_at_ms >= :y_s AND realized_pnl_usdc > 0 THEN 1 ELSE 0 END), 0)
                   FROM positions
                   WHERE status = 'CLOSED'
                     AND closed_at_ms >= :min_s
                     AND COALESCE(exit_reason, '') != 'SCALE_IN_MERGED'"#,
                params! {
                    "d_s" => eff_starts[0],
                    "w_s" => eff_starts[1],
                    "m_s" => eff_starts[2],
                    "y_s" => eff_starts[3],
                    "min_s" => min_start,
                },
            )
            .map_err(|e| {
                AegisError::Database(format!("compute_pnl_summary closed query failed: {e}"))
            })?;

        let mut closed_rpnl = [0.0_f64; 4];
        let mut closed_ff = [0.0_f64; 4];
        let mut trade_cnt = [0_i64; 4];
        let mut win_cnt = [0_i64; 4];
        if let Some(ref r) = closed_row {
            for i in 0..4 {
                let base = i * 4;
                closed_rpnl[i] = row_get_f64(r, base, 0.0);
                closed_ff[i] = row_get_f64(r, base + 1, 0.0);
                trade_cnt[i] = row_get_i64(r, base + 2, 0);
                win_cnt[i] = row_get_i64(r, base + 3, 0);
            }
        }

        // Query 2: Single UNION ALL for all 4 horizon baseline snapshots + earliest global fallback
        let snap_rows: Vec<mysql::Row> = conn
            .exec(
                r#"(SELECT 0 AS h_idx, total_equity_usdc, committed_notional_usdc, created_at_ms FROM portfolio_snapshots WHERE created_at_ms <= :d_s ORDER BY created_at_ms DESC, id DESC LIMIT 1)
                   UNION ALL
                   (SELECT 1 AS h_idx, total_equity_usdc, committed_notional_usdc, created_at_ms FROM portfolio_snapshots WHERE created_at_ms <= :w_s ORDER BY created_at_ms DESC, id DESC LIMIT 1)
                   UNION ALL
                   (SELECT 2 AS h_idx, total_equity_usdc, committed_notional_usdc, created_at_ms FROM portfolio_snapshots WHERE created_at_ms <= :m_s ORDER BY created_at_ms DESC, id DESC LIMIT 1)
                   UNION ALL
                   (SELECT 3 AS h_idx, total_equity_usdc, committed_notional_usdc, created_at_ms FROM portfolio_snapshots WHERE created_at_ms <= :y_s ORDER BY created_at_ms DESC, id DESC LIMIT 1)
                   UNION ALL
                   (SELECT -1 AS h_idx, total_equity_usdc, committed_notional_usdc, created_at_ms FROM portfolio_snapshots ORDER BY created_at_ms ASC, id ASC LIMIT 1)"#,
                params! {
                    "d_s" => eff_starts[0],
                    "w_s" => eff_starts[1],
                    "m_s" => eff_starts[2],
                    "y_s" => eff_starts[3],
                },
            )
            .map_err(|e| {
                AegisError::Database(format!("compute_pnl_summary snapshot union failed: {e}"))
            })?;

        let mut snaps: [Option<(f64, f64, i64)>; 4] = [None; 4];
        let mut earliest_snap: Option<(f64, f64, i64)> = None;
        for r in &snap_rows {
            let h_idx = row_get_i64(r, 0, -99);
            let tup = (
                row_get_f64(r, 1, 0.0),
                row_get_f64(r, 2, 0.0),
                row_get_i64(r, 3, 0),
            );
            if (0..4).contains(&h_idx) {
                snaps[h_idx as usize] = Some(tup);
            } else if h_idx == -1 {
                earliest_snap = Some(tup);
            }
        }
        for i in 0..4 {
            if snaps[i].is_none() {
                if let Some(e) = earliest_snap {
                    if e.2 >= eff_starts[i] {
                        snaps[i] = Some(e);
                    }
                }
            }
        }

        // Query 3: Open positions
        struct OpenPosPnl {
            asset_id: i64,
            side: OrderSide,
            entry_price: f64,
            qty: f64,
            cur_upnl: f64,
            open_ts: i64,
            funding: f64,
            fees: f64,
        }
        let open_rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT asset_id, side, entry_price, filled_quantity, unrealized_pnl_usdc,
                          COALESCE(opened_at_ms, created_at_ms),
                          funding_accrued_usdc, fees_paid_usdc
                   FROM positions
                   WHERE status = 'OPEN'"#,
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "compute_pnl_summary open positions query failed: {e}"
                ))
            })?;

        let mut open_positions = Vec::with_capacity(open_rows.len());
        for r in &open_rows {
            let side_str = row_get_string(r, 1, "");
            open_positions.push(OpenPosPnl {
                asset_id: row_get_i64(r, 0, 0),
                side: OrderSide::parse(&side_str)?,
                entry_price: row_get_f64(r, 2, 0.0),
                qty: row_get_f64(r, 3, 0.0),
                cur_upnl: row_get_f64(r, 4, 0.0),
                open_ts: row_get_i64(r, 5, 0),
                funding: row_get_f64(r, 6, 0.0),
                fees: row_get_f64(r, 7, 0.0),
            });
        }

        // Collect unique (asset_id, eff_start_ms) pairs needed ONLY for LOCAL_LEDGER horizons
        let mut needed_pairs: Vec<(i64, i64)> = Vec::new();
        let mut seen_pairs: HashSet<(i64, i64)> = HashSet::new();
        let mut needed_funding_assets: Vec<i64> = Vec::new();
        let mut seen_funding_assets: HashSet<i64> = HashSet::new();
        let mut min_needed_open_ts = i64::MAX;

        for i in 0..4 {
            if hl_matches[i].is_some() {
                continue;
            }
            let s_ms = eff_starts[i];
            for pos in &open_positions {
                if pos.open_ts < s_ms {
                    if seen_pairs.insert((pos.asset_id, s_ms)) {
                        needed_pairs.push((pos.asset_id, s_ms));
                    }
                    if seen_funding_assets.insert(pos.asset_id) {
                        needed_funding_assets.push(pos.asset_id);
                    }
                    if pos.open_ts < min_needed_open_ts {
                        min_needed_open_ts = pos.open_ts;
                    }
                }
            }
        }

        // Query 4: Zero-filesort sargable UNION ALL for ohlcv_bars ('1h', plus '4h' fallback only for misses)
        let mut bar_lookup: HashMap<(i64, i64), (f64, f64)> = HashMap::new();
        if !needed_pairs.is_empty() {
            let hit_1h = query_bars_union_for_timeframe(&mut conn, "1h", &needed_pairs)?;
            let mut missing_for_4h = Vec::new();
            for (idx, &pair) in needed_pairs.iter().enumerate() {
                if let Some(&val) = hit_1h.get(&idx) {
                    bar_lookup.insert(pair, val);
                } else {
                    missing_for_4h.push((idx, pair));
                }
            }
            if !missing_for_4h.is_empty() {
                let pairs_4h: Vec<(i64, i64)> =
                    missing_for_4h.iter().map(|&(_, pair)| pair).collect();
                let hit_4h = query_bars_union_for_timeframe(&mut conn, "4h", &pairs_4h)?;
                for (sub_idx, &(_, pair)) in missing_for_4h.iter().enumerate() {
                    if let Some(&val) = hit_4h.get(&sub_idx) {
                        bar_lookup.insert(pair, val);
                    }
                }
            }
        }

        // Query 5: Batched funding_events across all open assets needing horizon attribution
        let mut funding_by_asset: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();
        if !needed_funding_assets.is_empty() && min_needed_open_ts < i64::MAX {
            let mut ph = Vec::with_capacity(needed_funding_assets.len());
            let mut bind_params: Vec<(String, Value)> =
                Vec::with_capacity(needed_funding_assets.len() + 1);
            for (i, &aid) in needed_funding_assets.iter().enumerate() {
                let name = format!("a{i}");
                ph.push(format!(":{name}"));
                bind_params.push((name, Value::from(aid)));
            }
            bind_params.push(("min_ots".to_string(), Value::from(min_needed_open_ts)));
            let sql = format!(
                r#"SELECT asset_id, event_time_ms, remaining_usdc
                   FROM funding_events
                   WHERE asset_id IN ({}) AND event_time_ms >= :min_ots"#,
                ph.join(", ")
            );
            let f_rows: Vec<mysql::Row> = conn
                .exec(sql, mysql::Params::from(bind_params))
                .map_err(|e| {
                    AegisError::Database(format!(
                        "compute_pnl_summary funding_events batch query failed: {e}"
                    ))
                })?;
            for r in &f_rows {
                let aid = row_get_i64(r, 0, 0);
                let ev_ts = row_get_i64(r, 1, 0);
                let rem = row_get_f64(r, 2, 0.0);
                funding_by_asset.entry(aid).or_default().push((ev_ts, rem));
            }
        }

        // Build all 4 horizons in memory
        let mut metrics: Vec<PnlHorizonMetrics> = Vec::with_capacity(4);
        for i in 0..4 {
            let (_, label, _) = specs[i];
            let eff_start_ms = eff_starts[i];
            let (snap_eq_opt, snap_comm_opt, snap_ts_opt) = match snaps[i] {
                Some((eq, comm, st)) => (Some(eq), Some(comm), Some(st)),
                None => (None, None, None),
            };
            let baseline_equity_usdc = round_dp(
                snap_eq_opt
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .unwrap_or(fallback_equity)
                    .max(1.0),
                2,
            );

            let realized_pnl_usdc = round_dp(closed_rpnl[i], 2);
            let mut start_upnl = 0.0_f64;
            let mut horizon_open_fees_funding = 0.0_f64;

            if hl_matches[i].is_none() {
                if open_positions.is_empty() {
                    horizon_open_fees_funding = finite_or_zero(open_fees_funding);
                } else {
                    for pos in &open_positions {
                        if pos.open_ts >= eff_start_ms {
                            horizon_open_fees_funding += pos.funding - pos.fees;
                            continue;
                        }
                        if let Some(&(bar_close, bar_vol)) =
                            bar_lookup.get(&(pos.asset_id, eff_start_ms))
                        {
                            if bar_close.is_finite()
                                && bar_close > 0.0
                                && bar_vol.is_finite()
                                && bar_vol > 0.0
                            {
                                start_upnl +=
                                    (bar_close - pos.entry_price) * pos.qty * pos.side.sign();
                            } else if snap_ts_opt.is_some_and(|st| st <= eff_start_ms)
                                && snap_comm_opt.unwrap_or(0.0) <= 1e-6
                            {
                                // Snapshot at eff_start_ms had no open positions
                            } else {
                                start_upnl += pos.cur_upnl;
                            }
                        } else if snap_ts_opt.is_some_and(|st| st <= eff_start_ms)
                            && snap_comm_opt.unwrap_or(0.0) <= 1e-6
                        {
                            // Snapshot at eff_start_ms had no open positions
                        } else {
                            start_upnl += pos.cur_upnl;
                        }

                        let (ev_count, window_remaining_usdc) = funding_by_asset
                            .get(&pos.asset_id)
                            .map(|evs| {
                                let mut cnt = 0usize;
                                let mut sum = 0.0_f64;
                                for &(ev_ts, rem) in evs {
                                    if ev_ts >= pos.open_ts {
                                        cnt += 1;
                                        if ev_ts >= eff_start_ms {
                                            sum += rem;
                                        }
                                    }
                                }
                                (cnt, sum)
                            })
                            .unwrap_or((0, 0.0));

                        if ev_count > 0 {
                            horizon_open_fees_funding += window_remaining_usdc;
                        } else {
                            let total_span = (now - pos.open_ts).max(1) as f64;
                            let win_span = (now - eff_start_ms).max(0) as f64;
                            horizon_open_fees_funding +=
                                pos.funding * (win_span / total_span).clamp(0.0, 1.0);
                        }
                    }
                }
            }

            let horizon_upnl_delta = current_unrealized_pnl - start_upnl;
            let fees_funding_usdc = round_dp(closed_ff[i] + horizon_open_fees_funding, 2);

            let (
                net_pnl_usdc,
                unrealized_pnl_usdc,
                final_window_start_ms,
                final_is_cap_clamped,
                pnl_source,
            ) = if let Some((hl_net_pnl, hl_win_start, hl_clamped)) = hl_matches[i] {
                let net = round_dp(hl_net_pnl, 2);
                let unrl = round_dp(net - realized_pnl_usdc, 2);
                let win_ms = if hl_clamped {
                    eff_start_ms
                } else if hl_win_start > 0 {
                    hl_win_start
                } else {
                    eff_start_ms
                };
                (net, unrl, win_ms, hl_clamped, "HL_PORTFOLIO".to_string())
            } else {
                let unrl = round_dp(horizon_upnl_delta + horizon_open_fees_funding, 2);
                let net = round_dp(
                    closed_rpnl[i] + horizon_upnl_delta + horizon_open_fees_funding,
                    2,
                );
                (
                    net,
                    unrl,
                    eff_start_ms,
                    is_cap_clamped[i],
                    "LOCAL_LEDGER".to_string(),
                )
            };

            let return_pct = if baseline_equity_usdc > 0.0 {
                round_dp((net_pnl_usdc / baseline_equity_usdc) * 100.0, 2)
            } else {
                0.0
            };

            metrics.push(PnlHorizonMetrics {
                horizon_label: label.to_string(),
                window_start_ms: final_window_start_ms,
                baseline_equity_usdc,
                net_pnl_usdc,
                return_pct,
                realized_pnl_usdc,
                unrealized_pnl_usdc,
                fees_funding_usdc,
                trade_count: trade_cnt[i].max(0) as usize,
                win_count: win_cnt[i].max(0) as usize,
                is_cap_clamped: final_is_cap_clamped,
                pnl_source,
            });
        }

        let mut it = metrics.into_iter();
        let daily = it.next().unwrap_or_default();
        let weekly = it.next().unwrap_or_default();
        let monthly = it.next().unwrap_or_default();
        let ytd = it.next().unwrap_or_default();

        Ok(PnlSummary {
            daily,
            weekly,
            monthly,
            ytd,
        })
    }
}

fn query_bars_union_for_timeframe(
    conn: &mut PooledConn,
    timeframe: &str,
    pairs: &[(i64, i64)],
) -> Result<HashMap<usize, (f64, f64)>> {
    if pairs.is_empty() {
        return Ok(HashMap::new());
    }
    let mut subqueries = Vec::with_capacity(pairs.len());
    let mut bind_params: Vec<(String, Value)> = Vec::with_capacity(pairs.len() * 3 + 1);
    bind_params.push(("tf".to_string(), Value::from(timeframe)));

    for (idx, &(aid, start_ms)) in pairs.iter().enumerate() {
        let k_name = format!("k{idx}");
        let a_name = format!("a{idx}");
        let s_name = format!("s{idx}");
        subqueries.push(format!(
            "(SELECT :{k_name} AS pair_idx, close, volume FROM ohlcv_bars WHERE asset_id = :{a_name} AND timeframe = :tf AND timestamp_ms <= :{s_name} ORDER BY timestamp_ms DESC LIMIT 1)"
        ));
        bind_params.push((k_name, Value::from(idx as i64)));
        bind_params.push((a_name, Value::from(aid)));
        bind_params.push((s_name, Value::from(start_ms)));
    }

    let sql = subqueries.join(" UNION ALL ");
    let rows: Vec<mysql::Row> = conn
        .exec(sql, mysql::Params::from(bind_params))
        .map_err(|e| {
            AegisError::Database(format!(
                "compute_pnl_summary bars union ({timeframe}) failed: {e}"
            ))
        })?;

    let mut out = HashMap::with_capacity(rows.len());
    for r in &rows {
        let idx = row_get_i64(r, 0, -1);
        if idx >= 0 {
            out.insert(
                idx as usize,
                (row_get_f64(r, 1, 0.0), row_get_f64(r, 2, 0.0)),
            );
        }
    }
    Ok(out)
}

fn prune_snapshots_with_anchors(
    conn: &mut PooledConn,
    snap_cutoff: i64,
    anchor_cutoff: i64,
    preserve_daily_anchors: bool,
    batch_size: usize,
    max_batches: usize,
) -> Result<u64> {
    let anchor_ids: Vec<i64> = if preserve_daily_anchors {
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id FROM (
                       SELECT id,
                              ROW_NUMBER() OVER (
                                  PARTITION BY FLOOR(created_at_ms / 86400000)
                                  ORDER BY created_at_ms ASC, id ASC
                              ) AS rn
                       FROM portfolio_snapshots
                       WHERE created_at_ms >= :anchor_cutoff
                         AND created_at_ms < :snap_cutoff
                   ) t
                   WHERE rn = 1"#,
                params! {
                    "anchor_cutoff" => anchor_cutoff,
                    "snap_cutoff" => snap_cutoff,
                },
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "prune_retention anchor snapshot lookup failed: {e}"
                ))
            })?;
        rows.iter().map(|r| row_get_i64(r, 0, 0)).collect()
    } else {
        Vec::new()
    };

    batched_pk_delete(
        conn,
        "portfolio_snapshots",
        batch_size,
        max_batches,
        |c, lim| {
            if anchor_ids.is_empty() {
                let rows: Vec<mysql::Row> = c
                .exec(
                    "SELECT id FROM portfolio_snapshots WHERE created_at_ms < :cutoff ORDER BY created_at_ms ASC, id ASC LIMIT :lim",
                    params! { "cutoff" => snap_cutoff, "lim" => lim as u64 },
                )
                .map_err(|e| {
                    AegisError::Database(format!("prune portfolio_snapshots select failed: {e}"))
                })?;
                Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
            } else {
                let mut ph = Vec::with_capacity(anchor_ids.len());
                let mut bind_params: Vec<(String, Value)> =
                    Vec::with_capacity(anchor_ids.len() + 2);
                bind_params.push(("cutoff".to_string(), Value::from(snap_cutoff)));
                bind_params.push(("lim".to_string(), Value::from(lim as u64)));
                for (i, &aid) in anchor_ids.iter().enumerate() {
                    let name = format!("anc{i}");
                    ph.push(format!(":{name}"));
                    bind_params.push((name, Value::from(aid)));
                }
                let sql = format!(
                "SELECT id FROM portfolio_snapshots WHERE created_at_ms < :cutoff AND id NOT IN ({}) ORDER BY created_at_ms ASC, id ASC LIMIT :lim",
                ph.join(", ")
            );
                let rows: Vec<mysql::Row> =
                    c.exec(sql, mysql::Params::from(bind_params)).map_err(|e| {
                        AegisError::Database(format!(
                            "prune portfolio_snapshots select failed: {e}"
                        ))
                    })?;
                Ok(rows.iter().map(|r| row_get_i64(r, 0, 0)).collect())
            }
        },
    )
}

fn batched_pk_delete<F>(
    conn: &mut PooledConn,
    table: &str,
    batch_size: usize,
    max_batches: usize,
    select_ids: F,
) -> Result<u64>
where
    F: FnMut(&mut PooledConn, usize) -> Result<Vec<i64>>,
{
    let (deleted, _) = batched_pk_delete_counted(conn, table, batch_size, max_batches, select_ids)?;
    Ok(deleted)
}

fn batched_pk_delete_counted<F>(
    conn: &mut PooledConn,
    table: &str,
    batch_size: usize,
    max_batches: usize,
    mut select_ids: F,
) -> Result<(u64, usize)>
where
    F: FnMut(&mut PooledConn, usize) -> Result<Vec<i64>>,
{
    let mut total_deleted = 0u64;
    let mut batches = 0usize;
    let eff_batch = batch_size.max(1);

    while batches < max_batches {
        let ids = select_ids(conn, eff_batch)?;
        if ids.is_empty() {
            break;
        }
        let mut ph = Vec::with_capacity(ids.len());
        let mut bind_params: Vec<(String, Value)> = Vec::with_capacity(ids.len());
        for (i, &id) in ids.iter().enumerate() {
            let name = format!("id{i}");
            ph.push(format!(":{name}"));
            bind_params.push((name, Value::from(id)));
        }
        let del_sql = format!("DELETE FROM `{table}` WHERE id IN ({})", ph.join(", "));
        conn.exec_drop(del_sql, mysql::Params::from(bind_params))
            .map_err(|e| {
                AegisError::Database(format!("batched DELETE on `{table}` failed: {e}"))
            })?;
        total_deleted += conn.affected_rows();
        batches += 1;

        if ids.len() < eff_batch || batches >= max_batches {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    Ok((total_deleted, batches))
}

fn row_to_funding_event_record(r: &mysql::Row) -> FundingEventRecord {
    FundingEventRecord {
        id: row_get_i64(r, 0, 0),
        asset_id: row_get_i64(r, 1, 0),
        coin: row_get_string(r, 2, ""),
        event_time_ms: row_get_i64(r, 3, 0),
        usdc: row_get_f64(r, 4, 0.0),
        remaining_usdc: row_get_f64(r, 5, 0.0),
        position_szi: row_get_f64(r, 6, 0.0),
        funding_rate: row_get_f64(r, 7, 0.0),
        dedup_key: row_get_string(r, 8, ""),
        created_at_ms: row_get_i64(r, 9, 0),
        updated_at_ms: row_get_i64(r, 10, 0),
    }
}

fn row_to_portfolio_snapshot(r: &mysql::Row) -> PortfolioSnapshot {
    PortfolioSnapshot {
        id: row_get_i64(r, 0, 0),
        total_equity_usdc: row_get_f64(r, 1, 0.0),
        uncapped_equity_usdc: row_get_f64(r, 2, 0.0),
        available_cash_usdc: row_get_f64(r, 3, 0.0),
        reserved_order_cash_usdc: row_get_f64(r, 4, 0.0),
        committed_notional_usdc: row_get_f64(r, 5, 0.0),
        net_notional_usdc: row_get_f64(r, 6, 0.0),
        gross_leverage_x: row_get_f64(r, 7, 0.0),
        net_leverage_x: row_get_f64(r, 8, 0.0),
        market_beta_exposure: row_get_f64(r, 9, 0.0),
        market_breadth_bull: row_get_f64(r, 10, 0.0),
        market_regime_label: row_get_string(r, 11, "NEUTRAL"),
        risk_tier: row_get_string(r, 12, "extreme"),
        leverage_multiplier: row_get_f64(r, 13, 1.85),
        s1_gross_weight: row_get_f64(r, 14, 0.0),
        s2_gross_weight: row_get_f64(r, 15, 0.0),
        s3_gross_weight: row_get_f64(r, 16, 0.0),
        working_order_count: row_get_i32(r, 17, 0),
        open_long_count: row_get_i32(r, 18, 0),
        open_short_count: row_get_i32(r, 19, 0),
        eligible_coin_count: row_get_i32(r, 20, 0),
        created_at_ms: row_get_i64(r, 21, 0),
        updated_at_ms: row_get_i64(r, 22, 0),
    }
}
