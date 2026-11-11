//! Universe, OHLCV, signal evaluations, trade lifecycle, portfolio snapshot,
//! telemetry logging (with automatic secret redaction), and retention pruning operations.

use crate::DatabaseManager;
use aegis_core::{
    round_dp, AegisError, AltPairMetadata, OhlcvBar, OrderFsmState, PortfolioSnapshot, Result,
    SignalEvaluation, TradeRecord, UniverseAsset,
};
pub use aegis_core::{
    redact_embedded_url_secrets, sanitize_telemetry_details, sanitize_telemetry_message,
    sanitize_telemetry_value,
};
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use uuid::Uuid;

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
                        reference_price: r
                            .get::<_, Option<f64>>("reference_price")
                            .ok()
                            .flatten()
                            .unwrap_or(0.0),
                    })
                })
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    /// Resolve the reference price for `symbol` from `universe_assets`, `pond_universe`,
    /// latest `market_ohlcv` close, or `system_config` (`HP`, `R3-H15`).
    /// Propagates database errors via `Result` and returns `Ok(None)` when no positive
    /// reference price is configured or present in the database (never fabricates a hardcoded price).
    pub fn resolve_reference_price(&self, symbol: &str) -> Result<Option<f64>> {
        let sym = symbol.trim();
        if sym.is_empty() {
            return Ok(None);
        }
        self.with_conn(|conn| {
            let ua_px: Option<f64> = conn
                .query_row(
                    "SELECT reference_price FROM universe_assets WHERE symbol = ?1 LIMIT 1;",
                    params![sym],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            if let Some(px) = ua_px.filter(|&p| p.is_finite() && p > 0.0) {
                return Ok(Some(px));
            }
            let pond_px: Option<f64> = conn
                .query_row(
                    "SELECT reference_price FROM pond_universe WHERE symbol = ?1 LIMIT 1;",
                    params![sym],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            if let Some(px) = pond_px.filter(|&p| p.is_finite() && p > 0.0) {
                return Ok(Some(px));
            }
            let ohlcv_px: Option<f64> = conn
                .query_row(
                    "SELECT close FROM market_ohlcv WHERE symbol = ?1 ORDER BY timestamp_ms DESC LIMIT 1;",
                    params![sym],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(ohlcv_px.filter(|&p| p.is_finite() && p > 0.0))
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
        aegis_core::parse_timeframe_seconds(exec_timeframe)?;
        aegis_core::parse_timeframe_seconds(rs_timeframe)?;
        let norm_exec = exec_timeframe.trim().to_ascii_lowercase();
        let norm_rs = rs_timeframe.trim().to_ascii_lowercase();
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE universe_assets
                 SET exec_timeframe = ?1,
                     rs_timeframe = ?2,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE base_asset = ?3;",
                params![norm_exec, norm_rs, base_asset],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
        })
    }

    pub fn upsert_ohlcv_batch(&self, bars: &[OhlcvBar]) -> Result<usize> {
        if bars.is_empty() {
            return Ok(0);
        }
        self.with_immediate_tx(|conn| {
            {
                let mut stmt = conn
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
        self.with_immediate_tx(|conn| {
            {
                let mut del_stmt = conn
                    .prepare_cached(
                        "DELETE FROM signal_evaluations WHERE timestamp_ms = ?1 AND base_asset = ?2;",
                    )
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                let mut stmt = conn
                    .prepare_cached(
                        "INSERT INTO signal_evaluations (
                            timestamp_ms, base_asset, exec_symbol, synth_symbol,
                            hmm_regime, prob_bull, prob_bear, prob_chop,
                            snr_score, snr_synth, rs_96, r_usdc_8,
                            mace_score, blended_conviction,
                            rs_rank, signal_action, funding_rate_hourly
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17);",
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
                        ev.snr_synth,
                        ev.rs_96,
                        ev.r_usdc_8,
                        ev.mace_score,
                        ev.blended_conviction,
                        ev.rs_rank,
                        ev.signal_action,
                        ev.funding_rate_hourly,
                    ])
                    .map_err(|e| AegisError::Database(e.to_string()))?;
                }
            }
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
                            snr_score, snr_synth, rs_96, r_usdc_8,
                            mace_score, blended_conviction,
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
                    let snr_score: f64 = r.get(9)?;
                    let snr_synth: f64 = r.get::<_, Option<f64>>(10).ok().flatten().unwrap_or(snr_score);
                    let rs_96: f64 = r.get::<_, Option<f64>>(11).ok().flatten().unwrap_or(0.0);
                    let r_usdc_8: f64 = r.get::<_, Option<f64>>(12).ok().flatten().unwrap_or(0.0);
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
                        snr_score,
                        snr_synth,
                        rs_96,
                        r_usdc_8,
                        mace_score: r.get(13)?,
                        blended_conviction: r.get(14)?,
                        rs_rank: r.get(15)?,
                        signal_action: r.get(16)?,
                        funding_rate_hourly: r.get(17)?,
                        created_at: r.get(18)?,
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

    /// Returns all active filled open PIT positions, excluding unfilled resting `OPEN_ON_BOOK` orders (`NH-4`).
    pub fn get_open_trades(&self) -> Result<Vec<TradeRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT * FROM trades
                     WHERE status = 'OPEN'
                       AND NOT (COALESCE(fsm_state, '') = 'OPEN_ON_BOOK' AND COALESCE(filled_units, quantity) <= 0.0)
                     ORDER BY opened_at ASC;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], Self::map_trade_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    /// Returns unfilled resting `OPEN_ON_BOOK` PIT orders awaiting exchange fill/cancel reconciliation (`NH-4`).
    pub fn get_resting_pit_trades(&self) -> Result<Vec<TradeRecord>> {
        self.with_conn(|conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT * FROM trades
                     WHERE status = 'PENDING'
                        OR (status = 'OPEN' AND COALESCE(fsm_state, '') = 'OPEN_ON_BOOK' AND COALESCE(filled_units, 0.0) <= 0.0)
                     ORDER BY opened_at ASC;",
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let iter = stmt
                .query_map([], Self::map_trade_row)
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(iter.flatten().collect())
        })
    }

    /// Promote a resting `OPEN_ON_BOOK` order to an active `OPEN` / `FILLED` position once filled on exchange (`NH-4`, `R3-H2`).
    pub fn promote_resting_trade_to_open(
        &self,
        trade_id: i64,
        filled_units: f64,
        avg_fill_price: f64,
        fees_paid_usdc: f64,
        slippage_paid_usd: f64,
    ) -> Result<bool> {
        if filled_units <= 0.0
            || avg_fill_price <= 0.0
            || !filled_units.is_finite()
            || !avg_fill_price.is_finite()
        {
            return Ok(false);
        }
        let notional = round_dp(filled_units * avg_fill_price, 2);
        let ts = self.now_ms();
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE trades
                     SET status = 'OPEN',
                         fsm_state = 'FILLED',
                         quantity = ?1,
                         filled_units = ?1,
                         entry_price = ?2,
                         current_price = ?2,
                         avg_fill_price = ?2,
                         notional_usdc = ?3,
                         risk_budget_usdc = CASE
                             WHEN initial_stop_price > 0.0 AND ABS(?2 - initial_stop_price) > 1e-12 THEN ROUND(ABS(?2 - initial_stop_price) * ?1, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?1 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         highest_price_seen = MAX(highest_price_seen, ?2),
                         lowest_price_seen = CASE WHEN lowest_price_seen > 0.0 THEN MIN(lowest_price_seen, ?2) ELSE ?2 END,
                         fees_paid_usdc = ?4,
                         slippage_paid_usd = ?5
                     WHERE trade_id = ?6
                       AND status IN ('PENDING', 'OPEN')
                       AND COALESCE(fsm_state, '') IN ('OPEN_ON_BOOK', 'PENDING');",
                    params![
                        filled_units,
                        avg_fill_price,
                        notional,
                        fees_paid_usdc,
                        slippage_paid_usd,
                        trade_id
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            if rows > 0 {
                if let Ok(Some(cli_id)) = conn
                    .query_row(
                        "SELECT COALESCE(client_order_id, exchange_order_id, 'trade-' || trade_id)
                         FROM trades WHERE trade_id = ?1;",
                        params![trade_id],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()
                {
                    let _ = conn.execute(
                        "INSERT INTO order_state_audit (client_order_id, from_state, to_state, timestamp_utc, reason)
                         VALUES (?1, 'OPEN_ON_BOOK', 'FILLED', ?2, 'RESTING_ORDER_FILLED');",
                        params![cli_id, ts],
                    );
                }
            }
            Ok(rows > 0)
        })
    }

    /// True up an `OPEN` PIT trade that was race-promoted via `clearinghouseState`
    /// once the exact maker fill appears in `userFills` (`R13-N1`, `R13-N4(b)`).
    pub fn true_up_promoted_trade_from_user_fills(
        &self,
        trade_id: i64,
        filled_units: f64,
        avg_fill_price: f64,
        actual_fee_usdc: f64,
        estimated_fee_usdc: f64,
    ) -> Result<bool> {
        if filled_units <= 0.0
            || avg_fill_price <= 0.0
            || !filled_units.is_finite()
            || !avg_fill_price.is_finite()
        {
            return Ok(false);
        }
        let qty = round_dp(filled_units.abs(), 8);
        let px = round_dp(avg_fill_price, 8);
        let notional = round_dp(qty * px, 2);
        let fee_delta = round_dp(actual_fee_usdc.max(0.0) - estimated_fee_usdc.max(0.0), 4);
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE trades
                     SET quantity = ?1,
                         filled_units = ?1,
                         highest_price_seen = CASE
                             WHEN ABS(COALESCE(highest_price_seen, entry_price) - COALESCE(entry_price, ?2)) < 1e-9 THEN ?2
                             ELSE MAX(highest_price_seen, ?2)
                         END,
                         lowest_price_seen = CASE
                             WHEN ABS(COALESCE(lowest_price_seen, entry_price) - COALESCE(entry_price, ?2)) < 1e-9 THEN ?2
                             WHEN lowest_price_seen > 0.0 THEN MIN(lowest_price_seen, ?2)
                             ELSE ?2
                         END,
                         entry_price = ?2,
                         avg_fill_price = ?2,
                         notional_usdc = ?3,
                         risk_budget_usdc = CASE
                             WHEN initial_stop_price > 0.0 AND ABS(?2 - initial_stop_price) > 1e-12 THEN ROUND(ABS(?2 - initial_stop_price) * ?1, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?1 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         unrealized_pnl_usdc = CASE
                             WHEN current_price > 0.0 AND ?2 > 0.0 THEN
                                 ROUND((CASE WHEN UPPER(side) = 'LONG' THEN (current_price - ?2) ELSE (?2 - current_price) END) * ?1, 4)
                             ELSE unrealized_pnl_usdc
                         END,
                         fees_paid_usdc = MAX(0.0, ROUND(COALESCE(fees_paid_usdc, 0.0) + ?4, 4))
                     WHERE trade_id = ?5 AND status = 'OPEN';",
                    params![qty, px, notional, fee_delta, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(rows > 0)
        })
    }

    /// Cancel an unfilled resting `OPEN_ON_BOOK` or `PENDING` trade row (`NH-4`, `R7-S3`, `R9-N1`).
    pub fn cancel_resting_trade(&self, trade_id: i64, reason: &str) -> Result<bool> {
        let now_ms = self.now_ms();
        let closed_iso = aegis_core::format_ms_iso8601_utc(now_ms);
        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE trades
                     SET status = 'CANCELLED',
                         fsm_state = 'CANCELLED',
                         closed_at = ?1,
                         exit_reason = ?2
                     WHERE trade_id = ?3
                       AND status IN ('PENDING', 'OPEN')
                       AND COALESCE(fsm_state, '') IN ('OPEN_ON_BOOK', 'PENDING');",
                    params![closed_iso, reason, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            if rows > 0 {
                if let Ok(Some(cli_id)) = conn
                    .query_row(
                        "SELECT COALESCE(client_order_id, exchange_order_id, 'trade-' || trade_id)
                         FROM trades WHERE trade_id = ?1;",
                        params![trade_id],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()
                {
                    let _ = conn.execute(
                        "INSERT INTO order_state_audit (client_order_id, from_state, to_state, timestamp_utc, reason)
                         VALUES (?1, 'OPEN_ON_BOOK', 'CANCELLED', ?2, ?3);",
                        params![cli_id, now_ms, reason],
                    );
                }
            }
            Ok(rows > 0)
        })
    }

    /// Reduce an open trade's quantity, notional, and risk budget on a partial live exit fill (`NH-8`).
    #[allow(clippy::too_many_arguments)]
    pub fn reduce_open_trade_partial_exit(
        &self,
        trade_id: i64,
        remaining_qty: f64,
        remaining_notional: f64,
        realized_pnl_delta: f64,
        fees_delta: f64,
        slippage_delta: f64,
        mark_price: f64,
    ) -> Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE trades
                 SET quantity = ?1,
                     filled_units = ?1,
                     notional_usdc = ?2,
                     risk_budget_usdc = CASE
                         WHEN initial_stop_price > 0.0 AND ABS(entry_price - initial_stop_price) > 1e-12 THEN ROUND(ABS(entry_price - initial_stop_price) * ?1, 4)
                         WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?1 / quantity), 4)
                         ELSE risk_budget_usdc
                     END,
                     realized_pnl_usdc = ROUND(realized_pnl_usdc + ?3, 4),
                     fees_paid_usdc = ROUND(fees_paid_usdc + ?4, 4),
                     slippage_paid_usd = ROUND(COALESCE(slippage_paid_usd, 0.0) + ?5, 4),
                     current_price = CASE WHEN ?6 > 0.0 THEN ?6 ELSE current_price END,
                     unrealized_pnl_usdc = CASE
                         WHEN (CASE WHEN ?6 > 0.0 THEN ?6 ELSE current_price END) > 0.0 AND entry_price > 0.0 THEN
                             ROUND(
                                 (CASE WHEN UPPER(side) = 'LONG'
                                     THEN ((CASE WHEN ?6 > 0.0 THEN ?6 ELSE current_price END) - entry_price)
                                     ELSE (entry_price - (CASE WHEN ?6 > 0.0 THEN ?6 ELSE current_price END))
                                 END) * ?1,
                                 4
                             )
                         ELSE unrealized_pnl_usdc
                     END
                 WHERE trade_id = ?7 AND status = 'OPEN';",
                params![
                    round_dp(remaining_qty.abs(), 8),
                    round_dp(remaining_notional, 2),
                    round_dp(realized_pnl_delta, 4),
                    round_dp(fees_delta, 4),
                    round_dp(slippage_delta, 4),
                    mark_price,
                    trade_id,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(())
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

    /// Repairs half-completed `trades_v11_rebuild` crash windows and upgrades legacy `trades` tables
    /// whose `status` CHECK constraint lacks `'PENDING'` (`CHECK (status IN ('OPEN', 'CLOSED', 'CANCELLED'))`),
    /// preserving all rows, columns, child foreign-key rows, indexes, and dependent views (`R9-N7`, `R9-QA`).
    pub(crate) fn ensure_trades_pending_status_check(conn: &rusqlite::Connection) -> Result<()> {
        let has_main: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trades';",
                [],
                |_| Ok(true),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))?
            .unwrap_or(false);
        let has_rebuild: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trades_v11_rebuild';",
                [],
                |_| Ok(true),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))?
            .unwrap_or(false);

        if has_rebuild && !has_main {
            conn.execute_batch(
                "ALTER TABLE trades_v11_rebuild RENAME TO trades;
                 CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status, opened_at DESC);
                 CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_client_order_id_unique
                     ON trades(client_order_id)
                     WHERE client_order_id IS NOT NULL AND client_order_id != '';
                 CREATE VIEW IF NOT EXISTS v_trades_pit AS SELECT * FROM trades;",
            )
            .map_err(|e| {
                AegisError::Database(format!("Failed to recover trades_v11_rebuild: {e}"))
            })?;
            tracing::info!("Recovered interrupted trades_v11_rebuild table rename (R9-QA)");
            let _ = conn.execute(
                "INSERT INTO telemetry_logs (timestamp_ms, level, component, message, details_json)
                 VALUES (?1, 'INFO', 'db.schema_self_heal', ?2, ?3);",
                params![
                    Utc::now().timestamp_millis(),
                    "Recovered interrupted trades_v11_rebuild table rename",
                    json!({"recovered_from": "trades_v11_rebuild"}).to_string()
                ],
            );
            return Ok(());
        } else if has_rebuild && has_main {
            conn.execute_batch("DROP TABLE IF EXISTS trades_v11_rebuild;")
                .map_err(|e| {
                    AegisError::Database(format!("Failed to drop stale trades_v11_rebuild: {e}"))
                })?;
        }

        if !has_main {
            return Ok(());
        }

        let trades_sql: String = conn
            .query_row(
                "SELECT COALESCE(sql, '') FROM sqlite_master WHERE type='table' AND name='trades';",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))?
            .unwrap_or_default();

        let sql_upper = trades_sql.to_ascii_uppercase();
        if !sql_upper.contains("STATUS IN ('OPEN', 'CLOSED', 'CANCELLED')") {
            return Ok(());
        }

        let mut view_stmt = conn
            .prepare(
                "SELECT name, sql FROM sqlite_master
                 WHERE type = 'view' AND sql IS NOT NULL AND LOWER(sql) LIKE '%trades%';",
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
        let dependent_views: Vec<(String, String)> = view_stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .map_err(|e| AegisError::Database(e.to_string()))?
            .flatten()
            .collect();
        drop(view_stmt);

        let target_cols = [
            "trade_id",
            "symbol",
            "base_asset",
            "side",
            "status",
            "entry_price",
            "current_price",
            "exit_price",
            "quantity",
            "notional_usdc",
            "risk_budget_usdc",
            "conviction_multiplier",
            "initial_stop_price",
            "chandelier_stop_price",
            "highest_price_seen",
            "lowest_price_seen",
            "realized_pnl_usdc",
            "unrealized_pnl_usdc",
            "fees_paid_usdc",
            "funding_accrued_usdc",
            "exchange_order_id",
            "opened_at",
            "closed_at",
            "exit_reason",
            "client_order_id",
            "fsm_state",
            "avg_fill_price",
            "filled_units",
            "slippage_paid_usd",
            "last_funding_hour_bucket",
        ];

        let mut stmt = conn
            .prepare("PRAGMA table_info(trades);")
            .map_err(|e| AegisError::Database(e.to_string()))?;
        let existing_cols: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(1))
            .map_err(|e| AegisError::Database(e.to_string()))?
            .flatten()
            .filter(|c| target_cols.contains(&c.as_str()))
            .collect();
        drop(stmt);

        let col_list = existing_cols.join(", ");
        let fk_was_on = conn
            .query_row("PRAGMA foreign_keys;", [], |r| r.get::<_, i64>(0))
            .unwrap_or(1)
            != 0;
        let began_tx = conn.is_autocommit();
        if began_tx {
            if fk_was_on {
                let _ = conn.execute_batch("PRAGMA foreign_keys = OFF;");
            }
            if let Err(e) = conn.execute_batch("BEGIN IMMEDIATE;") {
                if fk_was_on {
                    let _ = conn.execute_batch("PRAGMA foreign_keys = ON;");
                }
                return Err(AegisError::Database(format!(
                    "Failed to begin trades rebuild tx: {e}"
                )));
            }
        }

        let mut drop_views_sql = String::from("DROP VIEW IF EXISTS v_trades_pit;\n");
        for (v_name, _) in &dependent_views {
            let escaped = v_name.replace('"', "\"\"");
            drop_views_sql.push_str(&format!("DROP VIEW IF EXISTS \"{escaped}\";\n"));
        }

        let mut script = format!(
            "{drop_views_sql}
             DROP TABLE IF EXISTS trades_v11_rebuild;
             CREATE TABLE trades_v11_rebuild (
                 trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                 symbol TEXT NOT NULL,
                 base_asset TEXT NOT NULL,
                 side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT', 'BASIS_HARVEST')),
                 status TEXT NOT NULL CHECK (status IN ('PENDING', 'OPEN', 'CLOSED', 'CANCELLED')) DEFAULT 'OPEN',
                 entry_price REAL NOT NULL,
                 current_price REAL NOT NULL,
                 exit_price REAL,
                 quantity REAL NOT NULL,
                 notional_usdc REAL NOT NULL,
                 risk_budget_usdc REAL NOT NULL,
                 conviction_multiplier REAL NOT NULL DEFAULT 1.0,
                 initial_stop_price REAL NOT NULL,
                 chandelier_stop_price REAL NOT NULL,
                 highest_price_seen REAL NOT NULL,
                 lowest_price_seen REAL NOT NULL,
                 realized_pnl_usdc REAL NOT NULL DEFAULT 0.0,
                 unrealized_pnl_usdc REAL NOT NULL DEFAULT 0.0,
                 fees_paid_usdc REAL NOT NULL DEFAULT 0.0,
                 funding_accrued_usdc REAL NOT NULL DEFAULT 0.0,
                 exchange_order_id TEXT,
                 opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
                 closed_at TEXT,
                 exit_reason TEXT,
                 client_order_id TEXT,
                 fsm_state TEXT DEFAULT 'PENDING' CHECK(fsm_state IN ('PENDING', 'OPEN_ON_BOOK', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED')),
                 avg_fill_price REAL,
                 filled_units REAL DEFAULT 0.0,
                 slippage_paid_usd REAL DEFAULT 0.0,
                 last_funding_hour_bucket INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO trades_v11_rebuild ({col_list}) SELECT {col_list} FROM trades;
             DROP TABLE trades;
             ALTER TABLE trades_v11_rebuild RENAME TO trades;
             CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status, opened_at DESC);
             CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_client_order_id_unique
                 ON trades(client_order_id)
                 WHERE client_order_id IS NOT NULL AND client_order_id != '';"
        );
        for (_, v_sql) in &dependent_views {
            let trimmed = v_sql.trim().trim_end_matches(';');
            if !trimmed.is_empty() {
                script.push_str(&format!("\n{trimmed};"));
            }
        }

        match conn.execute_batch(&script) {
            Ok(()) => {
                if began_tx {
                    let commit_res = conn.execute_batch("COMMIT;");
                    if fk_was_on {
                        let _ = conn.execute_batch("PRAGMA foreign_keys = ON;");
                    }
                    commit_res.map_err(|e| {
                        let _ = conn.execute_batch("ROLLBACK;");
                        AegisError::Database(format!("Failed to commit trades rebuild tx: {e}"))
                    })?;
                }
                tracing::info!(
                    preserved_columns = existing_cols.len(),
                    dependent_views = dependent_views.len(),
                    "Rebuilt legacy trades table to include PENDING in status CHECK constraint (R9-QA)"
                );
                let _ = conn.execute(
                    "INSERT INTO telemetry_logs (timestamp_ms, level, component, message, details_json)
                     VALUES (?1, 'INFO', 'db.schema_self_heal', ?2, ?3);",
                    params![
                        Utc::now().timestamp_millis(),
                        "Rebuilt legacy trades table to include PENDING in status CHECK constraint",
                        json!({
                            "preserved_columns": existing_cols.len(),
                            "dependent_views": dependent_views.len(),
                        })
                        .to_string()
                    ],
                );
                Ok(())
            }
            Err(e) => {
                if began_tx {
                    let _ = conn.execute_batch("ROLLBACK;");
                    if fk_was_on {
                        let _ = conn.execute_batch("PRAGMA foreign_keys = ON;");
                    }
                }
                Err(AegisError::Database(format!(
                    "Failed to rebuild legacy trades table for PENDING status: {e}"
                )))
            }
        }
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<i64> {
        let _ = OrderFsmState::parse(&trade.fsm_state)?;
        let cli_id = if trade.client_order_id.trim().is_empty() {
            format!("cli-{}", &Uuid::new_v4().simple().to_string()[..12])
        } else {
            trade.client_order_id.trim().to_string()
        };
        let opened_iso = if trade.opened_at.trim().is_empty() {
            aegis_core::format_ms_iso8601_utc(self.now_ms())
        } else {
            aegis_core::normalize_timestamp_iso8601_utc(&trade.opened_at)
        };
        let try_insert = |conn: &rusqlite::Connection| -> Result<i64> {
            if let Some(existing_id) = conn
                .query_row(
                    "SELECT trade_id FROM trades WHERE client_order_id = ?1 LIMIT 1;",
                    params![cli_id],
                    |r| r.get::<_, i64>(0),
                )
                .optional()
                .map_err(|e| AegisError::Database(e.to_string()))?
            {
                return Ok(existing_id);
            }

            conn.execute(
                "INSERT INTO trades (
                    client_order_id, symbol, base_asset, side, status, fsm_state,
                    entry_price, current_price, quantity, notional_usdc,
                    risk_budget_usdc, conviction_multiplier,
                    initial_stop_price, chandelier_stop_price,
                    highest_price_seen, lowest_price_seen,
                    fees_paid_usdc, funding_accrued_usdc, unrealized_pnl_usdc,
                    exchange_order_id, avg_fill_price, filled_units, slippage_paid_usd,
                    opened_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, ?9, ?10,
                    ?11, ?12,
                    ?13, ?14,
                    ?15, ?16,
                    ?17, ?18, ?19,
                    ?20, ?21, ?22, ?23,
                    ?24
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
                    trade.quantity.abs(),
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
                    trade.filled_units.abs(),
                    trade.slippage_paid_usd,
                    opened_iso,
                ],
            )
            .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(conn.last_insert_rowid())
        };

        match self.with_immediate_tx(try_insert) {
            Ok(row_id) => Ok(row_id),
            Err(e)
                if e.to_string()
                    .to_ascii_uppercase()
                    .contains("STATUS IN ('OPEN', 'CLOSED', 'CANCELLED')")
                    || (trade.status.eq_ignore_ascii_case("PENDING")
                        && e.to_string()
                            .to_ascii_uppercase()
                            .contains("CHECK CONSTRAINT FAILED")) =>
            {
                if let Err(heal_err) = self.with_conn(Self::ensure_trades_pending_status_check) {
                    let msg = format!(
                        "Critical: legacy trades table self-heal failed while inserting {} ({}): {heal_err}",
                        trade.symbol, trade.status
                    );
                    tracing::error!("{msg}");
                    let _ = self.log_telemetry(
                        "CRITICAL",
                        "db.schema_self_heal",
                        &msg,
                        Some(json!({
                            "symbol": trade.symbol,
                            "status": trade.status,
                            "fsm_state": trade.fsm_state,
                            "exchange_order_id": trade.exchange_order_id,
                            "error": heal_err.to_string(),
                        })),
                    );
                    return Err(heal_err);
                }
                match self.with_immediate_tx(try_insert) {
                    Ok(row_id) => Ok(row_id),
                    Err(retry_err) => {
                        let msg = format!(
                            "Critical: insert_trade failed after trades schema self-heal for {} ({}): {retry_err}",
                            trade.symbol, trade.status
                        );
                        tracing::error!("{msg}");
                        let _ = self.log_telemetry(
                            "CRITICAL",
                            "db.schema_self_heal",
                            &msg,
                            Some(json!({
                                "symbol": trade.symbol,
                                "status": trade.status,
                                "fsm_state": trade.fsm_state,
                                "exchange_order_id": trade.exchange_order_id,
                                "error": retry_err.to_string(),
                            })),
                        );
                        Err(retry_err)
                    }
                }
            }
            Err(e) => {
                let msg = format!(
                    "Critical: insert_trade failed for {} (status={} fsm_state={} oid={:?}): {e}",
                    trade.symbol, trade.status, trade.fsm_state, trade.exchange_order_id
                );
                tracing::error!("{msg}");
                let _ = self.log_telemetry(
                    "CRITICAL",
                    "db.trade_ops",
                    &msg,
                    Some(json!({
                        "symbol": trade.symbol,
                        "status": trade.status,
                        "fsm_state": trade.fsm_state,
                        "exchange_order_id": trade.exchange_order_id,
                        "error": e.to_string(),
                    })),
                );
                Err(e)
            }
        }
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
            conn.execute_batch("BEGIN IMMEDIATE;")
                .map_err(|e| AegisError::Database(e.to_string()))?;
            let result = (|| -> Result<()> {
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

                let last_audit_state: Option<String> = conn
                    .query_row(
                        "SELECT to_state FROM order_state_audit
                         WHERE client_order_id = ?1
                         ORDER BY id DESC LIMIT 1;",
                        params![client_order_id],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| AegisError::Database(e.to_string()))?;

                let prior_state = from_state
                    .map(str::to_string)
                    .or_else(|| existing.as_ref().map(|(_, s)| s.clone()))
                    .or(last_audit_state)
                    .unwrap_or_else(|| "PENDING".to_string());

                let prior_fsm = OrderFsmState::parse(&prior_state)?;
                if !prior_fsm.can_transition_to(target_fsm) {
                    return Err(AegisError::Validation(format!(
                        "Invalid OrderFsmState transition for '{client_order_id}': {prior_fsm} -> {target_fsm}"
                    )));
                }

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
                    params![client_order_id, prior_fsm.as_str(), target_fsm.as_str(), ts, reason],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
                Ok(())
            })();

            match result {
                Ok(()) => {
                    conn.execute_batch("COMMIT;")
                        .map_err(|e| AegisError::Database(e.to_string()))?;
                    Ok(())
                }
                Err(e) => {
                    let _ = conn.execute_batch("ROLLBACK;");
                    Err(e)
                }
            }
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

    /// Atomically accrue hourly funding on an open PIT trade at most once per UTC hour bucket (`R4-N4`).
    /// Returns `Ok(true)` if the funding delta was accrued, or `Ok(false)` if `hour_bucket` was already accrued.
    pub fn accrue_trade_funding_for_hour(
        &self,
        trade_id: i64,
        hour_bucket: i64,
        mark_price: f64,
        unrealized_pnl_usdc: f64,
        delta_usdc: f64,
    ) -> Result<bool> {
        if !delta_usdc.is_finite() {
            return Ok(false);
        }
        self.with_conn(|conn| {
            let updated = conn
                .execute(
                    "UPDATE trades
                     SET current_price = ?1,
                         unrealized_pnl_usdc = ?2,
                         funding_accrued_usdc = ROUND(COALESCE(funding_accrued_usdc, 0.0) + ?3, 4),
                         last_funding_hour_bucket = ?4
                     WHERE trade_id = ?5
                       AND status = 'OPEN'
                       AND COALESCE(last_funding_hour_bucket, 0) < ?4;",
                    params![
                        mark_price,
                        unrealized_pnl_usdc,
                        delta_usdc,
                        hour_bucket,
                        trade_id
                    ],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(updated > 0)
        })
    }

    /// Atomically transitions an `OPEN` trade to `CLOSED` (`R7-S3`).
    /// Returns `Ok(true)` if the row was open and transitioned to `CLOSED`, or `Ok(false)` if already closed/absent.
    pub fn close_trade(
        &self,
        trade_id: i64,
        exit_price: f64,
        realized_pnl_usdc: f64,
        fees_paid_usdc: f64,
        exit_reason: &str,
    ) -> Result<bool> {
        let closed_iso = chrono::DateTime::from_timestamp_millis(self.now_ms())
            .unwrap_or_else(Utc::now)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        self.with_conn(|conn| {
            let rows = conn
                .execute(
                    "UPDATE trades
                     SET status = 'CLOSED',
                         current_price = ?1,
                         exit_price = ?1,
                         realized_pnl_usdc = ?2,
                         unrealized_pnl_usdc = 0.0,
                         fees_paid_usdc = ?3,
                         closed_at = ?4,
                         exit_reason = ?5
                     WHERE trade_id = ?6
                       AND status = 'OPEN';",
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
            Ok(rows > 0)
        })
    }

    pub(crate) fn query_equity_baseline_reset_at(
        conn: &rusqlite::Connection,
    ) -> Result<Option<String>> {
        let raw: Option<String> = conn
            .query_row(
                "SELECT config_value FROM system_config WHERE config_key = 'equity_baseline_reset_at';",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| AegisError::Database(e.to_string()))?;
        Ok(raw
            .map(|s| s.trim().trim_matches('"').to_string())
            .filter(|s| !s.is_empty()))
    }

    pub fn get_equity_baseline_reset_at(&self) -> Result<Option<String>> {
        self.with_conn(Self::query_equity_baseline_reset_at)
    }

    pub fn get_total_realized_pnl(&self) -> Result<f64> {
        self.with_conn(|conn| {
            let reset_at = Self::query_equity_baseline_reset_at(conn)?;
            let pnl: f64 = if let Some(ref reset_trimmed) = reset_at {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM trades WHERE status = 'CLOSED' AND (closed_at IS NULL OR datetime(closed_at) > datetime(?1));",
                    params![reset_trimmed],
                    |r| r.get(0),
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
            } else {
                conn.query_row(
                    "SELECT COALESCE(SUM(realized_pnl_usdc), 0.0) FROM trades WHERE status = 'CLOSED';",
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
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

    /// Scrub any historical telemetry log rows that may contain plaintext sensitive config values (`B3`, `R3-B5`).
    pub fn redact_sensitive_telemetry_logs(&self) -> Result<usize> {
        self.with_conn(|conn| {
            let updated = conn
                .execute(
                    "UPDATE telemetry_logs
                     SET message = 'Updated system_config [REDACTED SENSITIVE KEYS]',
                         details_json = '{\"redacted\":true}'
                     WHERE (LOWER(message) LIKE '%private_key%=%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%private_key%:%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%gemini_api_key%=%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%gemini_api_key%:%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%bot_token%=%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%bot_token%:%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%webhook_url%=%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%webhook_url%:%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%web_api_token%=%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%web_api_token%:%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%auth_token%=%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%auth_token%:%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%api.telegram.org/bot%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(message) LIKE '%/api/webhooks/%' AND message NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%\"private_key\"%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%\"hyperliquid_private_key\"%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%\"gemini_api_key\"%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%\"telegram_bot_token\"%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%\"discord_webhook_url\"%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%\"web_api_token\"%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%api.telegram.org/bot%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%')
                        OR (LOWER(COALESCE(details_json, '')) LIKE '%/api/webhooks/%' AND COALESCE(details_json, '') NOT LIKE '%[REDACTED]%');",
                    [],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;
            Ok(updated)
        })
    }

    /// Prune unbounded `telemetry_logs` and synthetic/Pond `market_ohlcv` rows according to
    /// DB-configured retention settings (`R6`, `NL-10`).
    pub fn prune_retention_data(&self) -> Result<Value> {
        let cfg = self.load_config_opts(false)?;
        let ohlcv_retention_days = cfg.get_i64("pond_ohlcv_retention_days", 90).max(7);
        let telemetry_retention_days = cfg.get_i64("telemetry_retention_days", 30).max(1);
        let telemetry_max_rows = cfg.get_i64("telemetry_max_rows", 100_000).max(1_000);

        let now_ms = self.now_ms();
        let ohlcv_cutoff_ms = now_ms - ohlcv_retention_days * 86_400_000;
        let telemetry_cutoff_ms = now_ms - telemetry_retention_days * 86_400_000;

        self.with_immediate_tx(|conn| {
            // Preserve the core 7-asset Pit universe bars for full backtest history; prune old non-Pit Pond/synthetic bars
            let ohlcv_deleted = conn
                .execute(
                    "DELETE FROM market_ohlcv
                     WHERE timestamp_ms < ?1
                       AND symbol NOT IN (SELECT symbol FROM universe_assets WHERE is_active = 1);",
                    params![ohlcv_cutoff_ms],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;

            let telemetry_age_deleted = conn
                .execute(
                    "DELETE FROM telemetry_logs WHERE timestamp_ms < ?1;",
                    params![telemetry_cutoff_ms],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?;

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
                .map_err(|e| AegisError::Database(e.to_string()))?;

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
    ) -> Result<bool> {
        let size = exchange_size.abs();
        self.with_conn(|conn| {
            let rows = if let Some(px) = exchange_entry_price.filter(|&p| p > 0.0) {
                let notional = round_dp(size * px, 2);
                conn.execute(
                    "UPDATE trades
                     SET quantity = ?1,
                         filled_units = ?1,
                         entry_price = ?2,
                         avg_fill_price = ?2,
                         notional_usdc = ?3,
                         risk_budget_usdc = CASE
                             WHEN initial_stop_price > 0.0 AND ABS(?2 - initial_stop_price) > 1e-12 THEN ROUND(ABS(?2 - initial_stop_price) * ?1, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?1 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         unrealized_pnl_usdc = CASE
                             WHEN current_price > 0.0 AND ?2 > 0.0 THEN
                                 ROUND((CASE WHEN UPPER(side) = 'LONG' THEN (current_price - ?2) ELSE (?2 - current_price) END) * ?1, 4)
                             ELSE 0.0
                         END,
                         fsm_state = 'FILLED'
                     WHERE trade_id = ?4 AND status = 'OPEN';",
                    params![size, px, notional, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
            } else {
                conn.execute(
                    "UPDATE trades
                     SET quantity = ?1,
                         filled_units = ?1,
                         notional_usdc = ROUND(?1 * COALESCE(entry_price, 0.0), 2),
                         risk_budget_usdc = CASE
                             WHEN initial_stop_price > 0.0 AND ABS(entry_price - initial_stop_price) > 1e-12 THEN ROUND(ABS(entry_price - initial_stop_price) * ?1, 4)
                             WHEN quantity > 0.0 THEN ROUND(risk_budget_usdc * (?1 / quantity), 4)
                             ELSE risk_budget_usdc
                         END,
                         unrealized_pnl_usdc = CASE
                             WHEN current_price > 0.0 AND entry_price > 0.0 THEN
                                 ROUND((CASE WHEN UPPER(side) = 'LONG' THEN (current_price - entry_price) ELSE (entry_price - current_price) END) * ?1, 4)
                             ELSE unrealized_pnl_usdc
                         END,
                         fsm_state = 'FILLED'
                     WHERE trade_id = ?2 AND status = 'OPEN';",
                    params![size, trade_id],
                )
                .map_err(|e| AegisError::Database(e.to_string()))?
            };
            Ok(rows > 0)
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
            .get_f64("orphan_reconcile_stop_pct", 0.015)
            .clamp(0.002, 0.50);
        let trail_pct = cfg
            .get_f64("chandelier_trail_max_pct", 0.008)
            .clamp(0.002, stop_pct);
        let size = quantity.abs();
        let px = if entry_price > 0.0 {
            entry_price
        } else {
            mark_price
        };
        let ref_mark = if mark_price > 0.0 { mark_price } else { px };
        let notional = size * px;
        let default_stop = if side == "LONG" {
            round_dp((px * (1.0 - stop_pct)).min(ref_mark * (1.0 - trail_pct)), 6)
        } else {
            round_dp((px * (1.0 + stop_pct)).max(ref_mark * (1.0 + trail_pct)), 6)
        };
        let trail_stop = if side == "LONG" {
            round_dp(ref_mark * (1.0 - trail_pct), 6)
        } else {
            round_dp(ref_mark * (1.0 + trail_pct), 6)
        };
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
            current_price: ref_mark,
            exit_price: None,
            quantity: size,
            filled_units: size,
            avg_fill_price: Some(px),
            notional_usdc: notional,
            risk_budget_usdc: round_dp((px - default_stop).abs() * size, 2),
            conviction_multiplier: 1.0,
            initial_stop_price: default_stop,
            chandelier_stop_price: trail_stop,
            current_stop_price: trail_stop,
            highest_price_seen: px.max(ref_mark),
            lowest_price_seen: px.min(ref_mark),
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
