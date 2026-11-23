//! Historical & live OHLCV candle series repository (`aegis-db::bars_repo`).

use crate::engine::DatabaseManager;
use crate::row_decode::{
    decimal_param, row_get_f64, row_get_i64, row_get_string, DEC_20_10_MAX_ABS, DEC_20_8_MAX_ABS,
    DEC_24_8_MAX_ABS,
};
use aegis_core::{now_ms, AegisError, OhlcvBar, Result};
use mysql::params;
use mysql::prelude::*;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct BarUpsertInput {
    pub asset_id: i64,
    pub timeframe: String,
    pub timestamp_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub funding_rate: f64,
}

struct PreparedBarRow<'a> {
    asset_id: i64,
    timeframe: &'a str,
    timestamp_ms: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    funding_rate: String,
}

fn row_to_ohlcv_bar(r: &mysql::Row) -> OhlcvBar {
    OhlcvBar {
        id: row_get_i64(r, 0, 0),
        asset_id: row_get_i64(r, 1, 0),
        timeframe: row_get_string(r, 2, ""),
        timestamp_ms: row_get_i64(r, 3, 0),
        open: row_get_f64(r, 4, 0.0),
        high: row_get_f64(r, 5, 0.0),
        low: row_get_f64(r, 6, 0.0),
        close: row_get_f64(r, 7, 0.0),
        volume: row_get_f64(r, 8, 0.0),
        funding_rate: row_get_f64(r, 9, 0.0),
        created_at_ms: row_get_i64(r, 10, 0),
        updated_at_ms: row_get_i64(r, 11, 0),
    }
}

impl DatabaseManager {
    /// Batch upserts OHLCV candles for any supported timeframe.
    pub fn upsert_ohlcv_bars(&self, bars: &[BarUpsertInput]) -> Result<usize> {
        if bars.is_empty() {
            return Ok(0);
        }
        let mut prepared = Vec::with_capacity(bars.len());
        for b in bars {
            prepared.push(PreparedBarRow {
                asset_id: b.asset_id,
                timeframe: b.timeframe.trim(),
                timestamp_ms: b.timestamp_ms,
                open: decimal_param(b.open, 8, DEC_20_8_MAX_ABS)?,
                high: decimal_param(b.high, 8, DEC_20_8_MAX_ABS)?,
                low: decimal_param(b.low, 8, DEC_20_8_MAX_ABS)?,
                close: decimal_param(b.close, 8, DEC_20_8_MAX_ABS)?,
                volume: decimal_param(b.volume, 8, DEC_24_8_MAX_ABS)?,
                funding_rate: decimal_param(b.funding_rate, 10, DEC_20_10_MAX_ABS)?,
            });
        }

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start bars tx: {e}")))?;

        let stmt = tx
            .prep(
                r#"INSERT INTO ohlcv_bars
                   (asset_id, timeframe, timestamp_ms, open, high, low, close, volume, funding_rate, created_at_ms, updated_at_ms)
                   VALUES (:aid, :tf, :ts_ms, :o, :h, :l, :c, :v, :fr, :now, :now)
                   ON DUPLICATE KEY UPDATE
                   open = VALUES(open),
                   high = VALUES(high),
                   low = VALUES(low),
                   close = VALUES(close),
                   volume = VALUES(volume),
                   funding_rate = VALUES(funding_rate),
                   updated_at_ms = VALUES(updated_at_ms)"#,
            )
            .map_err(|e| AegisError::Database(format!("Failed to prepare OHLCV bar upsert: {e}")))?;

        tx.exec_batch(
            &stmt,
            prepared.iter().map(|b| {
                params! {
                    "aid" => b.asset_id,
                    "tf" => b.timeframe,
                    "ts_ms" => b.timestamp_ms,
                    "o" => &b.open,
                    "h" => &b.high,
                    "l" => &b.low,
                    "c" => &b.close,
                    "v" => &b.volume,
                    "fr" => &b.funding_rate,
                    "now" => ts,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("Failed to upsert OHLCV bars batch: {e}")))?;
        drop(stmt);

        tx.commit()
            .map_err(|e| AegisError::Database(format!("Failed to commit OHLCV bars: {e}")))?;
        Ok(bars.len())
    }

    /// Fetches the most recent `limit` OHLCV bars for `(asset_id, timeframe)` ordered chronologically (`timestamp_ms ASC`).
    pub fn get_ohlcv_bars(
        &self,
        asset_id: i64,
        timeframe: &str,
        limit: usize,
    ) -> Result<Vec<OhlcvBar>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, asset_id, timeframe, timestamp_ms, open, high, low, close, volume, funding_rate, created_at_ms, updated_at_ms
                   FROM ohlcv_bars
                   WHERE asset_id = :aid AND timeframe = :tf
                   ORDER BY timestamp_ms DESC
                   LIMIT :lim"#,
                params! {
                    "aid" => asset_id,
                    "tf" => timeframe,
                    "lim" => limit.max(1) as u64,
                },
            )
            .map_err(|e| AegisError::Database(format!("get_ohlcv_bars failed: {e}")))?;

        let mut bars: Vec<OhlcvBar> = rows.iter().map(row_to_ohlcv_bar).collect();
        bars.reverse();
        Ok(bars)
    }

    /// Fetches all OHLCV bars across all assets for `timeframe` in `[since_ms, until_ms]`
    /// grouped by `asset_id` and sorted chronologically (`timestamp_ms ASC`) using `idx_bars_tf_ts` (`R14-M15`).
    pub fn get_ohlcv_bars_window(
        &self,
        timeframe: &str,
        since_ms: i64,
        until_ms: i64,
    ) -> Result<HashMap<i64, Vec<OhlcvBar>>> {
        if since_ms > until_ms {
            return Ok(HashMap::new());
        }
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .exec(
                r#"SELECT id, asset_id, timeframe, timestamp_ms, open, high, low, close, volume, funding_rate, created_at_ms, updated_at_ms
                   FROM ohlcv_bars
                   WHERE timeframe = :tf
                     AND timestamp_ms >= :since_ms
                     AND timestamp_ms <= :until_ms
                   ORDER BY asset_id ASC, timestamp_ms ASC"#,
                params! {
                    "tf" => timeframe.trim(),
                    "since_ms" => since_ms,
                    "until_ms" => until_ms,
                },
            )
            .map_err(|e| AegisError::Database(format!("get_ohlcv_bars_window failed: {e}")))?;

        let mut grouped: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
        for r in &rows {
            let bar = row_to_ohlcv_bar(r);
            grouped.entry(bar.asset_id).or_default().push(bar);
        }
        Ok(grouped)
    }
}
