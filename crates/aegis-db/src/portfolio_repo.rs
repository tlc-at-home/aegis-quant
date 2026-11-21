//! Portfolio snapshots, telemetry logs, and alert queue repository (`aegis-db::portfolio_repo`).

use crate::engine::DatabaseManager;
use aegis_core::{
    is_sensitive_config_key, mask_identifier, now_ms, round_dp, sanitize_telemetry_message,
    ytd_start_ms, AegisError, AlertQueueRecord, OrderSide, PnlHorizonMetrics, PnlSummary,
    PortfolioSnapshot, Result, TelemetryLogRecord,
};
use mysql::params;
use mysql::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

fn sanitize_json_value(val: Value) -> Value {
    match val {
        Value::String(s) => Value::String(sanitize_telemetry_string(&s)),
        Value::Array(arr) => Value::Array(arr.into_iter().map(sanitize_json_value).collect()),
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                if is_sensitive_config_key(&k) && !k.eq_ignore_ascii_case("telegram_chat_id") {
                    out.insert(k, Value::String("[REDACTED]".to_string()));
                } else if k.eq_ignore_ascii_case("telegram_chat_id")
                    || k.eq_ignore_ascii_case("wallet_address")
                {
                    let masked = match &v {
                        Value::String(s) if !s.trim().is_empty() => {
                            Value::String(mask_identifier(s))
                        }
                        _ => sanitize_json_value(v),
                    };
                    out.insert(k, masked);
                } else {
                    out.insert(k, sanitize_json_value(v));
                }
            }
            Value::Object(out)
        }
        other => other,
    }
}

struct SanitizedFundingEvent {
    asset_id: i64,
    coin: String,
    event_time_ms: i64,
    usdc: f64,
    remaining_usdc: f64,
    position_szi: f64,
    funding_rate: f64,
    dedup_key: String,
}

type OpenPnlPositionRow = (i64, String, f64, f64, f64, i64, f64, f64);

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

    /// Returns `(peak_equity, latest_equity)` across `portfolio_snapshots` since `since_ms`,
    /// or `None` if `portfolio_snapshots` has no rows.
    pub fn get_peak_and_latest_equity_since(&self, since_ms: i64) -> Result<Option<(f64, f64)>> {
        let mut conn = self.get_conn()?;
        let latest_opt: Option<f64> = conn
            .query_first(
                "SELECT total_equity_usdc FROM portfolio_snapshots ORDER BY created_at_ms DESC, id DESC LIMIT 1",
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "get_peak_and_latest_equity_since latest query failed: {e}"
                ))
            })?;

        let Some(latest_equity) = latest_opt else {
            return Ok(None);
        };

        let peak_raw: Option<Option<f64>> = conn
            .exec_first(
                "SELECT MAX(total_equity_usdc) FROM portfolio_snapshots WHERE created_at_ms >= :since_ms AND total_equity_usdc > 0.0",
                params! { "since_ms" => since_ms },
            )
            .map_err(|e| {
                AegisError::Database(format!(
                    "get_peak_and_latest_equity_since peak query failed: {e}"
                ))
            })?;

        let peak_equity = peak_raw
            .flatten()
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
        details: Option<Value>,
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

    /// Deletes `portfolio_snapshots` and `telemetry_logs` older than `keep_days` in bounded batches,
    /// returning `(snapshots_deleted, telemetry_logs_deleted)`.
    pub fn prune_old_snapshots_and_logs(&self, keep_days: i64) -> Result<(u64, u64)> {
        let cutoff_ms = now_ms() - keep_days.max(1) * 86_400_000;
        let mut conn = self.get_conn()?;

        let mut snaps_deleted = 0u64;
        loop {
            conn.exec_drop(
                "DELETE FROM portfolio_snapshots WHERE created_at_ms < :cutoff ORDER BY id ASC LIMIT 5000",
                params! { "cutoff" => cutoff_ms },
            )
            .map_err(|e| {
                AegisError::Database(format!("prune_old_snapshots_and_logs snapshots failed: {e}"))
            })?;
            let aff = conn.affected_rows();
            snaps_deleted += aff;
            if aff < 5000 {
                break;
            }
        }

        let mut logs_deleted = 0u64;
        loop {
            conn.exec_drop(
                "DELETE FROM telemetry_logs WHERE created_at_ms < :cutoff ORDER BY id ASC LIMIT 5000",
                params! { "cutoff" => cutoff_ms },
            )
            .map_err(|e| {
                AegisError::Database(format!("prune_old_snapshots_and_logs logs failed: {e}"))
            })?;
            let aff = conn.affected_rows();
            logs_deleted += aff;
            if aff < 5000 {
                break;
            }
        }

        Ok((snaps_deleted, logs_deleted))
    }

    /// Batch upserts reconciled Hyperliquid funding events into `funding_events` in a single transaction.
    pub fn upsert_funding_events_batch(&self, events: &[FundingEventUpsertInput]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
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

        let sanitized: Vec<SanitizedFundingEvent> = events
            .iter()
            .map(|ev| SanitizedFundingEvent {
                asset_id: ev.asset_id,
                coin: ev.coin.trim().chars().take(24).collect(),
                event_time_ms: ev.event_time_ms,
                usdc: finite_or_zero(ev.usdc),
                remaining_usdc: finite_or_zero(ev.remaining_usdc),
                position_szi: finite_or_zero(ev.position_szi),
                funding_rate: finite_or_zero(ev.funding_rate),
                dedup_key: ev.dedup_key.chars().take(128).collect(),
            })
            .collect();

        tx.exec_batch(
            &stmt,
            sanitized.iter().map(|ev| {
                params! {
                    "aid" => ev.asset_id,
                    "coin" => &ev.coin,
                    "ev_ts" => ev.event_time_ms,
                    "usdc" => ev.usdc,
                    "rem_usdc" => ev.remaining_usdc,
                    "szi" => ev.position_szi,
                    "fr" => ev.funding_rate,
                    "dkey" => &ev.dedup_key,
                    "ts" => ts,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("Failed to upsert funding_events batch: {e}")))?;
        drop(stmt);

        tx.commit()
            .map_err(|e| AegisError::Database(format!("Failed to commit funding_events: {e}")))?;
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

        Ok(rows
            .into_iter()
            .map(|r| FundingEventRecord {
                id: r.get(0).unwrap_or_default(),
                asset_id: r.get(1).unwrap_or_default(),
                coin: r.get(2).unwrap_or_default(),
                event_time_ms: r.get(3).unwrap_or_default(),
                usdc: r.get(4).unwrap_or_default(),
                remaining_usdc: r.get(5).unwrap_or_default(),
                position_szi: r.get(6).unwrap_or_default(),
                funding_rate: r.get(7).unwrap_or_default(),
                dedup_key: r.get(8).unwrap_or_default(),
                created_at_ms: r.get(9).unwrap_or_default(),
                updated_at_ms: r.get(10).unwrap_or_default(),
            })
            .collect())
    }

    /// Computes multi-horizon PnL metrics (`Daily 24h`, `Weekly 7d`, `Monthly 30d`, `YTD`)
    /// respecting `risk_capital_cap_updated_at_ms`, anchoring baseline equity to `portfolio_snapshots`
    /// around each horizon start, attributing only the horizon uPnL delta (`current_upnl - start_upnl`),
    /// and windowing open-position funding/fees via `funding_events` (with time-proration fallback).
    pub fn compute_pnl_summary(
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

        let mut conn = self.get_conn()?;
        let open_positions: Vec<OpenPnlPositionRow> = conn
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

        let build_horizon = |conn: &mut mysql::PooledConn,
                             label: &str,
                             raw_start_ms: i64|
         -> Result<PnlHorizonMetrics> {
            let is_cap_clamped = cap_updated_at_ms > 0 && cap_updated_at_ms > raw_start_ms;
            let eff_start_ms = if cap_updated_at_ms > 0 {
                raw_start_ms.max(cap_updated_at_ms)
            } else {
                raw_start_ms
            };

            let closed_row: Option<(f64, f64, i64, i64)> = conn
                .exec_first(
                    r#"SELECT
                           COALESCE(SUM(realized_pnl_usdc), 0.0),
                           COALESCE(SUM(funding_accrued_usdc - fees_paid_usdc), 0.0),
                           COUNT(*),
                           COALESCE(SUM(CASE WHEN realized_pnl_usdc > 0.0 THEN 1 ELSE 0 END), 0)
                       FROM positions
                       WHERE status = 'CLOSED'
                         AND COALESCE(closed_at_ms, updated_at_ms) >= :start_ms"#,
                    params! { "start_ms" => eff_start_ms },
                )
                .map_err(|e| {
                    AegisError::Database(format!("compute_pnl_summary closed query failed: {e}"))
                })?;

            let (closed_rpnl, closed_fees_funding, trade_cnt, win_cnt) =
                closed_row.unwrap_or((0.0, 0.0, 0, 0));

            // Query portfolio_snapshots around eff_start_ms for true baseline_equity_usdc
            let pre_snap: Option<(f64, f64, i64)> = conn
                .exec_first(
                    r#"SELECT total_equity_usdc, committed_notional_usdc, created_at_ms
                       FROM portfolio_snapshots
                       WHERE created_at_ms <= :start_ms
                       ORDER BY created_at_ms DESC, id DESC
                       LIMIT 1"#,
                    params! { "start_ms" => eff_start_ms },
                )
                .map_err(|e| {
                    AegisError::Database(format!("compute_pnl_summary pre_snap query failed: {e}"))
                })?;

            let snap_opt = if pre_snap.is_some() {
                pre_snap
            } else {
                conn.exec_first(
                    r#"SELECT total_equity_usdc, committed_notional_usdc, created_at_ms
                       FROM portfolio_snapshots
                       WHERE created_at_ms >= :start_ms
                       ORDER BY created_at_ms ASC, id ASC
                       LIMIT 1"#,
                    params! { "start_ms" => eff_start_ms },
                )
                .map_err(|e| {
                    AegisError::Database(format!("compute_pnl_summary post_snap query failed: {e}"))
                })?
            };

            let (snap_eq_opt, snap_comm_opt, snap_ts_opt) = match snap_opt {
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

            // Compute unrealized PnL already present at eff_start_ms (`start_upnl`)
            let mut start_upnl = 0.0;
            for (asset_id, side_str, entry_price, qty, cur_pos_upnl, open_ts, _, _) in
                &open_positions
            {
                if *open_ts >= eff_start_ms {
                    // Position opened inside this horizon: start unrealized PnL was 0
                    continue;
                }
                let bar_close_opt: Option<f64> = conn
                    .exec_first(
                        r#"SELECT close
                           FROM ohlcv_bars
                           WHERE asset_id = :aid
                             AND timeframe IN ('1h', '4h')
                             AND timestamp_ms <= :start_ms
                           ORDER BY CASE WHEN timeframe = '1h' THEN 0 ELSE 1 END,
                                    timestamp_ms DESC
                           LIMIT 1"#,
                        params! { "aid" => *asset_id, "start_ms" => eff_start_ms },
                    )
                    .map_err(|e| {
                        AegisError::Database(format!("compute_pnl_summary bar lookup failed: {e}"))
                    })?;

                if let Some(bar_close) = bar_close_opt.filter(|&px| px.is_finite() && px > 0.0) {
                    let side = OrderSide::parse(side_str);
                    start_upnl += (bar_close - *entry_price) * *qty * side.sign();
                } else if snap_ts_opt.is_some_and(|st| st <= eff_start_ms)
                    && snap_comm_opt.unwrap_or(0.0) <= 1e-6
                {
                    // Snapshot at eff_start_ms had no open positions
                } else {
                    start_upnl += *cur_pos_upnl;
                }
            }

            // Compute horizon-windowed open funding & fees
            let mut horizon_open_fees_funding = 0.0_f64;
            if open_positions.is_empty() {
                horizon_open_fees_funding = if open_fees_funding.is_finite() {
                    open_fees_funding
                } else {
                    0.0
                };
            } else {
                for (asset_id, _, _, _, _, open_ts, pos_funding, pos_fees) in &open_positions {
                    if *open_ts >= eff_start_ms {
                        horizon_open_fees_funding += *pos_funding - *pos_fees;
                    } else {
                        // Entry fees occurred prior to horizon start -> 0.0 entry fee in this window.
                        let funding_row: Option<(i64, f64)> = conn
                            .exec_first(
                                r#"SELECT COUNT(*),
                                          COALESCE(SUM(CASE WHEN event_time_ms >= :start_ms THEN remaining_usdc ELSE 0.0 END), 0.0)
                                   FROM funding_events
                                   WHERE asset_id = :aid AND event_time_ms >= :open_ts"#,
                                params! {
                                    "start_ms" => eff_start_ms,
                                    "aid" => *asset_id,
                                    "open_ts" => *open_ts,
                                },
                            )
                            .map_err(|e| {
                                AegisError::Database(format!(
                                    "compute_pnl_summary funding_events query failed: {e}"
                                ))
                            })?;
                        let (ev_count, window_remaining_usdc) = funding_row.unwrap_or((0, 0.0));
                        if ev_count > 0 {
                            horizon_open_fees_funding += window_remaining_usdc;
                        } else {
                            let total_span = (now - *open_ts).max(1) as f64;
                            let win_span = (now - eff_start_ms).max(0) as f64;
                            horizon_open_fees_funding +=
                                *pos_funding * (win_span / total_span).clamp(0.0, 1.0);
                        }
                    }
                }
            }

            let horizon_upnl_delta = current_unrealized_pnl - start_upnl;
            let realized_pnl_usdc = round_dp(closed_rpnl, 2);
            let unrealized_pnl_usdc = round_dp(horizon_upnl_delta + horizon_open_fees_funding, 2);
            let net_pnl_usdc = round_dp(
                closed_rpnl + horizon_upnl_delta + horizon_open_fees_funding,
                2,
            );
            let fees_funding_usdc = round_dp(closed_fees_funding + horizon_open_fees_funding, 2);

            let return_pct = if baseline_equity_usdc > 0.0 {
                round_dp((net_pnl_usdc / baseline_equity_usdc) * 100.0, 2)
            } else {
                0.0
            };

            Ok(PnlHorizonMetrics {
                horizon_label: label.to_string(),
                window_start_ms: eff_start_ms,
                baseline_equity_usdc,
                net_pnl_usdc,
                return_pct,
                realized_pnl_usdc,
                unrealized_pnl_usdc,
                fees_funding_usdc,
                trade_count: trade_cnt.max(0) as usize,
                win_count: win_cnt.max(0) as usize,
                is_cap_clamped,
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
