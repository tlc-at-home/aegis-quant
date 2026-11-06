//! Universe, OHLCV, signal evaluations, trade lifecycle, portfolio snapshot,
//! telemetry logging (with automatic secret redaction), and retention pruning operations.

use crate::DatabaseManager;
use aegis_core::{
    is_sensitive_config_key, round_dp, AegisError, AltPairMetadata, OhlcvBar, OrderFsmState,
    PortfolioSnapshot, Result, SignalEvaluation, TradeRecord, UniverseAsset,
};
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use uuid::Uuid;

/// Scrub any sensitive keys or inline credential literals from a JSON value before logging.
fn sanitize_telemetry_value(val: &mut Value) {
    match val {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if is_sensitive_config_key(k) {
                    *v = Value::String("[REDACTED]".to_string());
                } else {
                    sanitize_telemetry_value(v);
                }
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                sanitize_telemetry_value(item);
            }
        }
        _ => {}
    }
}

/// Scrub inline secret patterns (`private_key=...`, `telegram_bot_token=...`, etc.) from log strings.
pub fn sanitize_telemetry_message(message: &str) -> String {
    let lower = message.to_ascii_lowercase();
    let has_sensitive = [
        "private_key",
        "gemini_api_key",
        "telegram_bot_token",
        "discord_webhook_url",
        "web_api_token",
        "master_key",
    ]
    .iter()
    .any(|k| lower.contains(k));
    if !has_sensitive {
        return message.to_string();
    }

    // Redact `key=value` or `key: value` segments for sensitive keys
    let mut sanitized = message.to_string();
    for key in [
        "private_key",
        "gemini_api_key",
        "telegram_bot_token",
        "discord_webhook_url",
        "web_api_token",
        "aegis_master_key",
        "master_key",
    ] {
        let eq_pat = format!("{key}=");
        if let Some(idx) = sanitized.to_ascii_lowercase().find(&eq_pat) {
            let val_start = idx + eq_pat.len();
            let rest = &sanitized[val_start..];
            let val_end = rest
                .find(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == ')')
                .unwrap_or(rest.len());
            let current_val = &rest[..val_end];
            if current_val != "[REDACTED]" && !current_val.is_empty() {
                sanitized.replace_range(val_start..val_start + val_end, "[REDACTED]");
            }
        }
    }
    sanitized
}

impl DatabaseManager {
    pub fn get_usdc_perp_universe(&self) -> Result<Vec<UniverseAsset>> {
        self.query_universe_view("SELECT * FROM v_universe_usdc_perp;")
    }

    pub fn get_btc_synth_universe(&self) -> Result<Vec<UniverseAsset>> {
        self.query_universe_view("SELECT * FROM v_universe_btc_synth;")
    }

    fn query_universe_view(&self, sql: &str) -> Result<Vec<UniverseAsset>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok(UniverseAsset {
                        asset_id: r.get("asset_id")?,
                        symbol: r.get("symbol")?,
                        base_asset: r.get("base_asset")?,
                        quote_asset: r.get("quote_asset")?,
                        instrument_type: r.get("instrument_type")?,
                        tier: r.get("tier")?,
                        tier_name: r.get("tier_name")?,
                        exec_timeframe: r.get("exec_timeframe")?,
                        rs_timeframe: r.get("rs_timeframe")?,
                        tick_size: r.get("tick_size")?,
                        lot_step_size: r.get("lot_step_size")?,
                        min_notional_usdc: r.get("min_notional_usdc")?,
                        max_leverage: r.get("max_leverage")?,
                        is_active: r.get("is_active")?,
                        updated_at: r.get("updated_at")?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_alt_pairs_universe(&self) -> Result<Vec<AltPairMetadata>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM v_universe_alt_pairs;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], |r| {
                    Ok(AltPairMetadata {
                        base_asset: r.get("base_asset")?,
                        tier: r.get("tier")?,
                        tier_name: r.get("tier_name")?,
                        exec_symbol: r.get("exec_symbol")?,
                        synth_symbol: r.get("synth_symbol")?,
                        exec_timeframe: r.get("exec_timeframe")?,
                        rs_timeframe: r.get("rs_timeframe")?,
                        tick_size: r.get("tick_size")?,
                        lot_step_size: r.get("lot_step_size")?,
                        min_notional_usdc: r.get("min_notional_usdc")?,
                        max_leverage: r.get("max_leverage")?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn update_asset_timeframes(
        &self,
        base_asset: &str,
        exec_timeframe: &str,
        rs_timeframe: &str,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE universe_assets
                 SET exec_timeframe = ?1,
                     rs_timeframe = ?2,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE base_asset = ?3;",
                params![exec_timeframe, rs_timeframe, base_asset],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn upsert_ohlcv_batch(&self, bars: &[OhlcvBar]) -> Result<usize> {
        if bars.is_empty() {
            return Ok(0);
        }
        self.with_conn(|conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            {
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO market_ohlcv (
                            symbol, timeframe, timestamp_ms, open, high, low, close,
                            volume, funding_rate_hourly, is_synthetic
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                        ON CONFLICT(symbol, timeframe, timestamp_ms) DO UPDATE SET
                            open = excluded.open,
                            high = excluded.high,
                            low = excluded.low,
                            close = excluded.close,
                            volume = excluded.volume,
                            funding_rate_hourly = excluded.funding_rate_hourly,
                            is_synthetic = excluded.is_synthetic;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                for b in bars {
                    stmt.execute(params![
                        b.symbol,
                        b.timeframe,
                        b.timestamp_ms,
                        b.open,
                        b.high,
                        b.low,
                        b.close,
                        b.volume,
                        b.funding_rate_hourly,
                        b.is_synthetic,
                    ])
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
            }
            tx.commit()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(bars.len())
        })
    }

    pub fn get_ohlcv_bars(
        &self,
        symbol: &str,
        timeframe: &str,
        limit: usize,
        max_timestamp_ms: Option<i64>,
    ) -> Result<Vec<OhlcvBar>> {
        let eff_max = max_timestamp_ms.or_else(|| self.get_pit_timestamp_ms());
        self.with_conn(|conn| {
            let mut bars: Vec<OhlcvBar> = if let Some(max_ts) = eff_max {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT timestamp_ms, open, high, low, close, volume,
                                funding_rate_hourly, is_synthetic
                         FROM market_ohlcv
                         WHERE symbol = ?1 AND timeframe = ?2 AND timestamp_ms <= ?3
                         ORDER BY timestamp_ms DESC
                         LIMIT ?4;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let rows = stmt
                    .query_map(params![symbol, timeframe, max_ts, limit as i64], |r| {
                        Ok(OhlcvBar {
                            symbol: symbol.to_string(),
                            timeframe: timeframe.to_string(),
                            timestamp_ms: r.get(0)?,
                            open: r.get(1)?,
                            high: r.get(2)?,
                            low: r.get(3)?,
                            close: r.get(4)?,
                            volume: r.get(5)?,
                            funding_rate_hourly: r.get(6)?,
                            is_synthetic: r.get(7)?,
                        })
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                rows.flatten().collect()
            } else {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT timestamp_ms, open, high, low, close, volume,
                                funding_rate_hourly, is_synthetic
                         FROM market_ohlcv
                         WHERE symbol = ?1 AND timeframe = ?2
                         ORDER BY timestamp_ms DESC
                         LIMIT ?3;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let rows = stmt
                    .query_map(params![symbol, timeframe, limit as i64], |r| {
                        Ok(OhlcvBar {
                            symbol: symbol.to_string(),
                            timeframe: timeframe.to_string(),
                            timestamp_ms: r.get(0)?,
                            open: r.get(1)?,
                            high: r.get(2)?,
                            low: r.get(3)?,
                            close: r.get(4)?,
                            volume: r.get(5)?,
                            funding_rate_hourly: r.get(6)?,
                            is_synthetic: r.get(7)?,
                        })
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                rows.flatten().collect()
            };
            bars.reverse();
            Ok(bars)
        })
    }

    pub fn save_signal_evaluations(&self, evaluations: &[SignalEvaluation]) -> Result<()> {
        if evaluations.is_empty() {
            return Ok(());
        }
        self.with_conn(|conn| {
            let tx = conn
                .unchecked_transaction()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            {
                let mut del_stmt = tx
                    .prepare_cached(
                        "DELETE FROM signal_evaluations WHERE timestamp_ms = ?1 AND base_asset = ?2;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO signal_evaluations (
                            timestamp_ms, base_asset, exec_symbol, synth_symbol,
                            hmm_regime, prob_bull, prob_bear, prob_chop,
                            snr_score, mace_score, blended_conviction,
                            rs_rank, signal_action, funding_rate_hourly
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14);",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                for ev in evaluations {
                    del_stmt
                        .execute(params![ev.timestamp_ms, ev.base_asset])
                        .map_err(|e| AegisError::Database(e.to_string()))?;
                    stmt.execute(params![
                        ev.timestamp_ms,
                        ev.base_asset,
                        ev.exec_symbol,
                        ev.synth_symbol,
                        ev.hmm_regime,
                        ev.prob_bull,
                        ev.prob_bear,
                        ev.prob_chop,
                        ev.snr_score,
                        ev.mace_score,
                        ev.blended_conviction,
                        ev.rs_rank,
                        ev.signal_action,
                        ev.funding_rate_hourly,
                    ])
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
            }
            tx.commit()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_latest_signals(&self, limit: usize) -> Result<Vec<SignalEvaluation>> {
        self.with_conn(|conn| {
            let max_ts: Option<i64> = conn
                .query_row(
                    "SELECT MAX(timestamp_ms) FROM signal_evaluations;",
                    [],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?
                .flatten();
            let Some(ts) = max_ts else {
                return Ok(Vec::new());
            };
            let mut stmt = conn
                .prepare(
                    "SELECT eval_id, timestamp_ms, base_asset, exec_symbol, synth_symbol,
                            hmm_regime, prob_bull, prob_bear, prob_chop,
                            snr_score, mace_score, blended_conviction,
                            rs_rank, signal_action, funding_rate_hourly, created_at
                     FROM signal_evaluations
                     WHERE eval_id IN (
                         SELECT MAX(eval_id)
                         FROM signal_evaluations
                         WHERE timestamp_ms = ?1
                         GROUP BY base_asset
                     )
                     ORDER BY rs_rank ASC
                     LIMIT ?2;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![ts, limit as i64], |r| {
                    Ok(SignalEvaluation {
                        eval_id: Some(r.get(0)?),
                        timestamp_ms: r.get(1)?,
                        base_asset: r.get(2)?,
                        exec_symbol: r.get(3)?,
                        synth_symbol: r.get(4)?,
                        hmm_regime: r.get(5)?,
                        prob_bull: r.get(6)?,
                        prob_bear: r.get(7)?,
                        prob_chop: r.get(8)?,
                        snr_score: r.get(9)?,
                        snr_synth: r.get(9)?,
                        rs_96: 0.0,
                        r_usdc_8: 0.0,
                        mace_score: r.get(10)?,
                        blended_conviction: r.get(11)?,
                        rs_rank: r.get(12)?,
                        signal_action: r.get(13)?,
                        funding_rate_hourly: r.get(14)?,
                        created_at: r.get(15)?,
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub(crate) fn map_trade_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<TradeRecord> {
        let trade_id: i64 = r.get("trade_id")?;
        let ex_id: Option<String> = r.get("exchange_order_id").ok().flatten();
        let raw_cli: Option<String> = r.get("client_order_id").ok().flatten();
        let client_order_id = raw_cli
            .or_else(|| ex_id.clone())
            .unwrap_or_else(|| format!("trade-{trade_id}"));
        let entry_price: f64 = r.get("entry_price")?;
        let quantity: f64 = r.get("quantity")?;
        let init_stop: f64 = r.get("initial_stop_price")?;
        let chand_stop: f64 = r.get("chandelier_stop_price").unwrap_or(init_stop);
        let fsm_state: String = r
            .get::<_, Option<String>>("fsm_state")
            .ok()
            .flatten()
            .unwrap_or_else(|| "FILLED".to_string());
        let avg_fill_price: Option<f64> = r
            .get::<_, Option<f64>>("avg_fill_price")
            .ok()
            .flatten()
            .or(Some(entry_price));
        let filled_units: f64 = r
            .get::<_, Option<f64>>("filled_units")
            .ok()
            .flatten()
            .unwrap_or(quantity);
        let slippage_paid_usd: f64 = r
            .get::<_, Option<f64>>("slippage_paid_usd")
            .ok()
            .flatten()
            .unwrap_or(0.0);

        Ok(TradeRecord {
            trade_id,
            client_order_id,
            symbol: r.get("symbol")?,
            base_asset: r.get("base_asset")?,
            side: r.get("side")?,
            status: r.get("status")?,
            fsm_state,
            entry_price,
            current_price: r.get("current_price")?,
            exit_price: r.get("exit_price").ok().flatten(),
            quantity,
            filled_units,
            avg_fill_price,
            notional_usdc: r.get("notional_usdc")?,
            risk_budget_usdc: r.get("risk_budget_usdc")?,
            conviction_multiplier: r.get("conviction_multiplier")?,
            initial_stop_price: init_stop,
            chandelier_stop_price: chand_stop,
            current_stop_price: chand_stop,
            highest_price_seen: r.get("highest_price_seen")?,
            lowest_price_seen: r.get("lowest_price_seen")?,
            realized_pnl_usdc: r.get("realized_pnl_usdc")?,
            unrealized_pnl_usdc: r.get("unrealized_pnl_usdc")?,
            fees_paid_usdc: r.get("fees_paid_usdc")?,
            funding_accrued_usdc: r.get("funding_accrued_usdc")?,
            slippage_paid_usd,
            exchange_order_id: ex_id,
            opened_at: r.get("opened_at")?,
            closed_at: r.get("closed_at").ok().flatten(),
            exit_reason: r.get("exit_reason").ok().flatten(),
        })
    }

    pub fn get_open_trades(&self) -> Result<Vec<TradeRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM trades WHERE status = 'OPEN' ORDER BY opened_at ASC;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], Self::map_trade_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn get_recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare("SELECT * FROM trades ORDER BY trade_id DESC LIMIT ?1;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![limit as i64], Self::map_trade_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<i64> {
        let _ = OrderFsmState::parse(&trade.fsm_state)?;
        let cli_id = if trade.client_order_id.trim().is_empty() {
            format!("cli-{}", &Uuid::new_v4().simple().to_string()[..12])
        } else {
            trade.client_order_id.clone()
        };
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO trades (
                    client_order_id, symbol, base_asset, side, status, fsm_state,
                    entry_price, current_price, quantity, notional_usdc,
                    risk_budget_usdc, conviction_multiplier,
                    initial_stop_price, chandelier_stop_price,
                    highest_price_seen, lowest_price_seen,
                    fees_paid_usdc, funding_accrued_usdc, unrealized_pnl_usdc,
                    exchange_order_id, avg_fill_price, filled_units, slippage_paid_usd
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, ?9, ?10,
                    ?11, ?12,
                    ?13, ?14,
                    ?15, ?16,
                    ?17, ?18, ?19,
                    ?20, ?21, ?22, ?23
                );",
                params![
                    cli_id,
                    trade.symbol,
                    trade.base_asset,
                    trade.side,
                    trade.status,
                    trade.fsm_state,
                    trade.entry_price,
                    trade.current_price,
                    trade.quantity,
                    trade.notional_usdc,
                    trade.risk_budget_usdc,
                    trade.conviction_multiplier,
                    trade.initial_stop_price,
                    trade.chandelier_stop_price,
                    trade.highest_price_seen,
                    trade.lowest_price_seen,
                    trade.fees_paid_usdc,
                    trade.funding_accrued_usdc,
                    trade.unrealized_pnl_usdc,
                    trade.exchange_order_id,
                    trade.avg_fill_price,
                    trade.filled_units,
                    trade.slippage_paid_usd,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        })
    }

    pub fn log_order_state_transition(
        &self,
        client_order_id: &str,
        from_state: &str,
        to_state: &str,
        reason: Option<&str>,
    ) -> Result<i64> {
        let ts = self.now_ms();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO order_state_audit (client_order_id, from_state, to_state, timestamp_utc, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5);",
                params![client_order_id, from_state, to_state, ts, reason],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn transition_order_state(
        &self,
        client_order_id: &str,
        to_state: &str,
        from_state: Option<&str>,
        reason: Option<&str>,
        exchange_order_id: Option<&str>,
        avg_fill_price: Option<f64>,
        filled_units: Option<f64>,
        slippage_paid_usd: Option<f64>,
        fees_paid_usdc: Option<f64>,
        status: Option<&str>,
    ) -> Result<()> {
        let target_fsm = OrderFsmState::parse(to_state)?;
        let ts = self.now_ms();
        self.with_conn(|conn| {
            let existing: Option<(i64, String)> = conn
                .query_row(
                    "SELECT trade_id, COALESCE(fsm_state, 'PENDING')
                     FROM trades
                     WHERE client_order_id = ?1 OR exchange_order_id = ?1
                     ORDER BY trade_id DESC LIMIT 1;",
                    params![client_order_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            let prior_state = from_state
                .map(str::to_string)
                .or_else(|| existing.as_ref().map(|(_, s)| s.clone()))
                .unwrap_or_else(|| "PENDING".to_string());

            if let Some((trade_id, _)) = existing {
                conn.execute(
                    "UPDATE trades
                     SET fsm_state = ?1,
                         exchange_order_id = COALESCE(?2, exchange_order_id),
                         avg_fill_price = COALESCE(?3, avg_fill_price),
                         entry_price = COALESCE(?3, entry_price),
                         current_price = COALESCE(?3, current_price),
                         filled_units = COALESCE(?4, filled_units),
                         slippage_paid_usd = COALESCE(?5, slippage_paid_usd),
                         fees_paid_usdc = COALESCE(?6, fees_paid_usdc),
                         status = COALESCE(?7, status)
                     WHERE trade_id = ?8;",
                    params![
                        target_fsm.as_str(),
                        exchange_order_id,
                        avg_fill_price,
                        filled_units,
                        slippage_paid_usd,
                        fees_paid_usdc,
                        status,
                        trade_id,
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            }

            conn.execute(
                "INSERT INTO order_state_audit (client_order_id, from_state, to_state, timestamp_utc, reason)
                 VALUES (?1, ?2, ?3, ?4, ?5);",
                params![client_order_id, prior_state, target_fsm.as_str(), ts, reason],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_open_trade(
        &self,
        trade_id: i64,
        current_price: f64,
        chandelier_stop_price: f64,
        highest_price_seen: f64,
        lowest_price_seen: f64,
        unrealized_pnl_usdc: f64,
        funding_accrued_usdc: f64,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE trades
                 SET current_price = ?1,
                     chandelier_stop_price = ?2,
                     highest_price_seen = ?3,
                     lowest_price_seen = ?4,
                     unrealized_pnl_usdc = ?5,
                     funding_accrued_usdc = ?6
                 WHERE trade_id = ?7 AND status = 'OPEN';",
                params![
                    current_price,
                    chandelier_stop_price,
                    highest_price_seen,
                    lowest_price_seen,
                    unrealized_pnl_usdc,
                    funding_accrued_usdc,
                    trade_id,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn close_trade(
        &self,
        trade_id: i64,
        exit_price: f64,
        realized_pnl_usdc: f64,
        fees_paid_usdc: f64,
        exit_reason: &str,
    ) -> Result<()> {
        let closed_iso = chrono::DateTime::from_timestamp_millis(self.now_ms())
            .unwrap_or_else(Utc::now)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        self.with_conn(|conn| {
            conn.execute(
                "UPDATE trades
                 SET status = 'CLOSED',
                     current_price = ?1,
                     exit_price = ?1,
                     realized_pnl_usdc = ?2,
                     unrealized_pnl_usdc = 0.0,
                     fees_paid_usdc = ?3,
                     closed_at = ?4,
                     exit_reason = ?5
                 WHERE trade_id = ?6;",
                params![
                    exit_price,
                    realized_pnl_usdc,
                    fees_paid_usdc,
                    closed_iso,
                    exit_reason,
                    trade_id
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_total_realized_pnl(&self) -> Result<f64> {
        self.with_conn(|conn| {
            let reset_at: String = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'equity_baseline_reset_at';",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or_default();
            let reset_trimmed = reset_at.trim().trim_matches('"');
            let pnl: f64 = if !reset_trimmed.is_empty() {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM trades WHERE status = 'CLOSED' AND (closed_at IS NULL OR closed_at > ?1);",
                    params![reset_trimmed],
                    |r| r.get(0),
                )
                .unwrap_or(0.0)
            } else {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM trades WHERE status = 'CLOSED';",
                    [],
                    |r| r.get(0),
                )
                .unwrap_or(0.0)
            };
            Ok(pnl)
        })
    }

    pub fn save_portfolio_snapshot(&self, snap: &PortfolioSnapshot) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO portfolio_snapshots (
                    timestamp_ms, total_equity_usdc, available_cash_usdc,
                    deployed_notional_usdc, gross_leverage,
                    portfolio_heat_usdc, portfolio_heat_pct,
                    basis_harvest_notional_usdc, accrued_yield_usdc,
                    hmm_regime, prob_bull, prob_bear, prob_chop,
                    open_long_count, open_short_count
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15);",
                params![
                    snap.timestamp_ms,
                    snap.total_equity_usdc,
                    snap.available_cash_usdc,
                    snap.deployed_notional_usdc,
                    snap.gross_leverage,
                    snap.portfolio_heat_usdc,
                    snap.portfolio_heat_pct,
                    snap.basis_harvest_notional_usdc,
                    snap.accrued_yield_usdc,
                    snap.hmm_regime,
                    snap.prob_bull,
                    snap.prob_bear,
                    snap.prob_chop,
                    snap.open_long_count,
                    snap.open_short_count,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn get_latest_portfolio_snapshot(&self) -> Result<PortfolioSnapshot> {
        self.with_conn(|conn| {
            let row = conn
                .query_row(
                    "SELECT snapshot_id, timestamp_ms, total_equity_usdc, available_cash_usdc,
                            deployed_notional_usdc, gross_leverage, portfolio_heat_usdc,
                            portfolio_heat_pct, basis_harvest_notional_usdc, accrued_yield_usdc,
                            hmm_regime, prob_bull, prob_bear, prob_chop,
                            open_long_count, open_short_count, created_at
                     FROM portfolio_snapshots
                     ORDER BY snapshot_id DESC LIMIT 1;",
                    [],
                    |r| {
                        Ok(PortfolioSnapshot {
                            snapshot_id: Some(r.get(0)?),
                            timestamp_ms: r.get(1)?,
                            total_equity_usdc: r.get(2)?,
                            available_cash_usdc: r.get(3)?,
                            deployed_notional_usdc: r.get(4)?,
                            gross_leverage: r.get(5)?,
                            portfolio_heat_usdc: r.get(6)?,
                            portfolio_heat_pct: r.get(7)?,
                            basis_harvest_notional_usdc: r.get(8)?,
                            accrued_yield_usdc: r.get(9)?,
                            hmm_regime: r.get(10)?,
                            prob_bull: r.get(11)?,
                            prob_bear: r.get(12)?,
                            prob_chop: r.get(13)?,
                            open_long_count: r.get(14)?,
                            open_short_count: r.get(15)?,
                            created_at: r.get(16).ok(),
                        })
                    },
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;

            if let Some(s) = row {
                return Ok(s);
            }
            let init_eq: f64 = conn
                .query_row(
                    "SELECT config_value FROM system_config WHERE config_key = 'initial_equity_usdc';",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(10000.0);

            Ok(PortfolioSnapshot {
                snapshot_id: None,
                timestamp_ms: self.now_ms(),
                total_equity_usdc: init_eq,
                available_cash_usdc: init_eq,
                deployed_notional_usdc: 0.0,
                gross_leverage: 0.0,
                portfolio_heat_usdc: 0.0,
                portfolio_heat_pct: 0.0,
                basis_harvest_notional_usdc: 0.0,
                accrued_yield_usdc: 0.0,
                hmm_regime: "CHOP".to_string(),
                prob_bull: 0.3333,
                prob_bear: 0.3333,
                prob_chop: 0.3334,
                open_long_count: 0,
                open_short_count: 0,
                created_at: None,
            })
        })
    }

    pub fn log_telemetry(
        &self,
        level: &str,
        component: &str,
        message: &str,
        details: Option<Value>,
    ) -> Result<()> {
        let safe_message = sanitize_telemetry_message(message);
        let details_str = details.map(|mut v| {
            sanitize_telemetry_value(&mut v);
            v.to_string()
        });
        let ts = Utc::now().timestamp_millis();
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO telemetry_logs (timestamp_ms, level, component, message, details_json)
                 VALUES (?1, ?2, ?3, ?4, ?5);",
                params![
                    ts,
                    level.to_ascii_uppercase(),
                    component,
                    safe_message,
                    details_str
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    /// Scrub any historical telemetry log rows that may contain plaintext sensitive config values (`B3`).
    pub fn redact_sensitive_telemetry_logs(&self) -> Result<usize> {
        self.with_conn(|conn| {
            let updated = conn
                .execute(
                    "UPDATE telemetry_logs
                     SET message = 'Updated system_config [REDACTED SENSITIVE KEYS]',
                         details_json = '{\"redacted\":true}'
                     WHERE message LIKE '%private_key=%'
                        OR message LIKE '%gemini_api_key=%'
                        OR message LIKE '%telegram_bot_token=%'
                        OR message LIKE '%discord_webhook_url=%'
                        OR message LIKE '%web_api_token=%'
                        OR details_json LIKE '%\"private_key\"%'
                        OR details_json LIKE '%\"gemini_api_key\"%'
                        OR details_json LIKE '%\"telegram_bot_token\"%'
                        OR details_json LIKE '%\"discord_webhook_url\"%'
                        OR details_json LIKE '%\"web_api_token\"%';",
                    [],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(updated)
        })
    }

    /// Prune unbounded `telemetry_logs` and synthetic/Pond `market_ohlcv` rows according to
    /// DB-configured retention settings (`R6`).
    pub fn prune_retention_data(&self) -> Result<Value> {
        let cfg = self.load_config_opts(false).unwrap_or_default();
        let ohlcv_retention_days = cfg.get_i64("pond_ohlcv_retention_days", 90).max(7);
        let telemetry_retention_days = cfg.get_i64("telemetry_retention_days", 30).max(1);
        let telemetry_max_rows = cfg.get_i64("telemetry_max_rows", 100_000).max(1_000);

        let now_ms = self.now_ms();
        let ohlcv_cutoff_ms = now_ms - ohlcv_retention_days * 86_400_000;
        let telemetry_cutoff_ms = now_ms - telemetry_retention_days * 86_400_000;

        self.with_conn(|conn| {
            // Preserve the core 7-asset Pit universe bars for full backtest history; prune old non-Pit Pond/synthetic bars
            let ohlcv_deleted = conn
                .execute(
                    "DELETE FROM market_ohlcv
                     WHERE timestamp_ms < ?1
                       AND symbol NOT IN (SELECT symbol FROM universe_assets WHERE is_active = 1);",
                    params![ohlcv_cutoff_ms],
                )
                .unwrap_or(0);

            let telemetry_age_deleted = conn
                .execute(
                    "DELETE FROM telemetry_logs WHERE timestamp_ms < ?1;",
                    params![telemetry_cutoff_ms],
                )
                .unwrap_or(0);

            let telemetry_cap_deleted = conn
                .execute(
                    "DELETE FROM telemetry_logs
                     WHERE log_id NOT IN (
                         SELECT log_id FROM telemetry_logs
                         ORDER BY log_id DESC
                         LIMIT ?1
                     );",
                    params![telemetry_max_rows],
                )
                .unwrap_or(0);

            Ok(json!({
                "ohlcv_deleted": ohlcv_deleted,
                "telemetry_age_deleted": telemetry_age_deleted,
                "telemetry_cap_deleted": telemetry_cap_deleted,
            }))
        })
    }

    pub fn get_recent_logs(&self, limit: usize) -> Result<Vec<Value>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT log_id, timestamp_ms, level, component, message, details_json, created_at
                     FROM telemetry_logs
                     ORDER BY log_id DESC
                     LIMIT ?1;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map(params![limit as i64], |r| {
                    let raw_msg: String = r.get(4)?;
                    Ok(json!({
                        "log_id": r.get::<_, i64>(0)?,
                        "timestamp_ms": r.get::<_, i64>(1)?,
                        "level": r.get::<_, String>(2)?,
                        "component": r.get::<_, String>(3)?,
                        "message": sanitize_telemetry_message(&raw_msg),
                        "details_json": r.get::<_, Option<String>>(5)?,
                        "created_at": r.get::<_, String>(6)?,
                    }))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    pub fn reconcile_open_trade_size(
        &self,
        trade_id: i64,
        exchange_size: f64,
        exchange_entry_price: Option<f64>,
    ) -> Result<()> {
        let size = exchange_size.abs();
        self.with_conn(|conn| {
            if let Some(px) = exchange_entry_price.filter(|&p| p > 0.0) {
                let notional = size * px;
                conn.execute(
                    "UPDATE trades
                     SET quantity = ?1,
                         filled_units = ?1,
                         entry_price = ?2,
                         avg_fill_price = ?2,
                         notional_usdc = ?3,
                         fsm_state = 'FILLED'
                     WHERE trade_id = ?4 AND status = 'OPEN';",
                    params![size, px, notional, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            } else {
                conn.execute(
                    "UPDATE trades
                     SET quantity = ?1,
                         filled_units = ?1,
                         notional_usdc = ?1 * entry_price,
                         fsm_state = 'FILLED'
                     WHERE trade_id = ?2 AND status = 'OPEN';",
                    params![size, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            }
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reconcile_on_chain_orphan_position(
        &self,
        symbol: &str,
        base_asset: &str,
        side: &str,
        quantity: f64,
        entry_price: f64,
        mark_price: f64,
        unrealized_pnl_usdc: f64,
    ) -> Result<i64> {
        let cfg = self.load_config_opts(false).unwrap_or_default();
        let stop_pct = cfg
            .get_f64("orphan_reconcile_stop_pct", 0.09)
            .clamp(0.005, 0.50);
        let size = quantity.abs();
        let px = if entry_price > 0.0 {
            entry_price
        } else {
            mark_price
        };
        let notional = size * px;
        let stop_mult = if side == "LONG" {
            1.0 - stop_pct
        } else {
            1.0 + stop_pct
        };
        let default_stop = round_dp(px * stop_mult, 6);
        let cli_id = format!("reconciled-{}", &Uuid::new_v4().simple().to_string()[..12]);
        let ex_id = format!("onchain-reconciled-{base_asset}-{}", self.now_ms());

        let rec = TradeRecord {
            trade_id: 0,
            client_order_id: cli_id.clone(),
            symbol: symbol.to_string(),
            base_asset: base_asset.to_string(),
            side: side.to_string(),
            status: "OPEN".to_string(),
            fsm_state: "FILLED".to_string(),
            entry_price: px,
            current_price: if mark_price > 0.0 { mark_price } else { px },
            exit_price: None,
            quantity: size,
            filled_units: size,
            avg_fill_price: Some(px),
            notional_usdc: notional,
            risk_budget_usdc: round_dp((px - default_stop).abs() * size, 2),
            conviction_multiplier: 1.0,
            initial_stop_price: default_stop,
            chandelier_stop_price: default_stop,
            current_stop_price: default_stop,
            highest_price_seen: px.max(mark_price),
            lowest_price_seen: if mark_price > 0.0 {
                px.min(mark_price)
            } else {
                px
            },
            realized_pnl_usdc: 0.0,
            unrealized_pnl_usdc,
            fees_paid_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            slippage_paid_usd: 0.0,
            exchange_order_id: Some(ex_id),
            opened_at: String::new(),
            closed_at: None,
            exit_reason: None,
        };
        let tid = self.insert_trade(&rec)?;
        let _ = self.log_order_state_transition(
            &cli_id,
            "PENDING",
            "FILLED",
            Some("ON_CHAIN_ORPHAN_RECONCILED"),
        );
        Ok(tid)
    }

    pub fn get_order_state_audit(
        &self,
        client_order_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Value>> {
        self.with_conn(|conn| {
            if let Some(cid) = client_order_id {
                let mut stmt = conn
                    .prepare(
                        "SELECT id, client_order_id, from_state, to_state, timestamp_utc, reason
                         FROM order_state_audit
                         WHERE client_order_id = ?1
                         ORDER BY id ASC
                         LIMIT ?2;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let iter = stmt
                    .query_map(params![cid, limit as i64], |r| {
                        Ok(json!({
                            "id": r.get::<_, i64>(0)?,
                            "client_order_id": r.get::<_, String>(1)?,
                            "from_state": r.get::<_, String>(2)?,
                            "to_state": r.get::<_, String>(3)?,
                            "timestamp_utc": r.get::<_, i64>(4)?,
                            "reason": r.get::<_, Option<String>>(5)?,
                        }))
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(iter.flatten().collect())
            } else {
                let mut stmt = conn
                    .prepare(
                        "SELECT id, client_order_id, from_state, to_state, timestamp_utc, reason
                         FROM order_state_audit
                         ORDER BY id DESC
                         LIMIT ?1;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let iter = stmt
                    .query_map(params![limit as i64], |r| {
                        Ok(json!({
                            "id": r.get::<_, i64>(0)?,
                            "client_order_id": r.get::<_, String>(1)?,
                            "from_state": r.get::<_, String>(2)?,
                            "to_state": r.get::<_, String>(3)?,
                            "timestamp_utc": r.get::<_, i64>(4)?,
                            "reason": r.get::<_, Option<String>>(5)?,
                        }))
                    })
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(iter.flatten().collect())
            }
        })
    }

    /// Creates an isolated `:memory:` SQLite database and copies `system_config` and `universe_assets`
    /// from `self` so backtests never mutate the live/paper WAL database.
    pub fn clone_to_memory(&self) -> Result<DatabaseManager> {
        let active_key = self.get_master_fernet_key().ok();
        let mem_db = Self::with_paths(":memory:", &self.schema_path, active_key);
        mem_db.initialize_schema(false)?;

        let cfg_rows: Vec<(String, String, String, String, String)> = self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT config_key, config_value, value_type, category, description FROM system_config;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows.flatten().collect())
        })?;

        mem_db.with_conn(|conn| {
            for (k, v, vt, cat, desc) in cfg_rows {
                let _ = conn.execute(
                    "INSERT INTO system_config (config_key, config_value, value_type, category, description)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(config_key) DO UPDATE SET
                        config_value = excluded.config_value,
                        value_type = excluded.value_type,
                        category = excluded.category,
                        description = excluded.description;",
                    params![k, v, vt, cat, desc],
                );
            }
            Ok(())
        })?;

        Ok(mem_db)
    }
}
