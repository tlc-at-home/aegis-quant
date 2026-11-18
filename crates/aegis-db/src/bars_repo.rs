//! Historical & live OHLCV candle series repository (`aegis-db::bars_repo`).

use crate::engine::DatabaseManager;
use aegis_core::{now_ms, AegisError, OhlcvBar, Result};
use mysql::params;
use mysql::prelude::*;

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

impl DatabaseManager {
    /// Batch upserts 1h or 4h OHLCV candles for an asset.
    pub fn upsert_ohlcv_bars(&self, bars: &[BarUpsertInput]) -> Result<usize> {
        if bars.is_empty() {
            return Ok(0);
        }
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start bars tx: {e}")))?;

        for b in bars {
            tx.exec_drop(
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
                params! {
                    "aid" => b.asset_id,
                    "tf" => &b.timeframe,
                    "ts_ms" => b.timestamp_ms,
                    "o" => b.open,
                    "h" => b.high,
                    "l" => b.low,
                    "c" => b.close,
                    "v" => b.volume,
                    "fr" => b.funding_rate,
                    "now" => ts,
                },
            )
            .map_err(|e| AegisError::Database(format!("Failed to upsert OHLCV bar: {e}")))?;
        }
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

        let mut bars: Vec<OhlcvBar> = rows
            .into_iter()
            .map(|r| OhlcvBar {
                id: r.get(0).unwrap_or_default(),
                asset_id: r.get(1).unwrap_or_default(),
                timeframe: r.get(2).unwrap_or_default(),
                timestamp_ms: r.get(3).unwrap_or_default(),
                open: r.get(4).unwrap_or_default(),
                high: r.get(5).unwrap_or_default(),
                low: r.get(6).unwrap_or_default(),
                close: r.get(7).unwrap_or_default(),
                volume: r.get(8).unwrap_or_default(),
                funding_rate: r.get(9).unwrap_or_default(),
                created_at_ms: r.get(10).unwrap_or_default(),
                updated_at_ms: r.get(11).unwrap_or_default(),
            })
            .collect();
        bars.reverse();
        Ok(bars)
    }
}
