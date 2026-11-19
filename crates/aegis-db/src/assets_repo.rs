//! Exchange assets discovery & live pricing repository (`aegis-db::assets_repo`).

use crate::engine::DatabaseManager;
use aegis_core::{normalize_hyperliquid_symbol, now_ms, AegisError, ExchangeAsset, Result};
use mysql::params;
use mysql::prelude::*;

#[derive(Debug, Clone)]
pub struct AssetUpsertInput {
    pub raw_symbol_or_coin: String,
    pub asset_index: u32,
    pub sz_decimals: u32,
    pub tick_size: f64,
    pub lot_step_size: f64,
    pub max_leverage: f64,
    pub mark_price: f64,
    pub mid_price: f64,
    pub oracle_price: f64,
    pub funding_rate_hourly: f64,
    pub day_volume_usdc: f64,
    pub open_interest_usdc: f64,
    pub prev_day_price: f64,
    pub is_core_eligible: bool,
    pub is_event_eligible: bool,
    pub is_priority_watched: bool,
}

impl DatabaseManager {
    /// Upserts a Hyperliquid perpetual asset with canonical `"<COIN>-USDC"` symbol normalization
    /// and returns its MariaDB `exchange_assets.id` primary key.
    pub fn upsert_exchange_asset(&self, input: &AssetUpsertInput) -> Result<i64> {
        let (symbol, coin) = normalize_hyperliquid_symbol(&input.raw_symbol_or_coin)?;
        let ts = now_ms();
        let mut conn = self.get_conn()?;

        conn.exec_drop(
            r#"INSERT INTO exchange_assets (
                   symbol, coin, asset_index, sz_decimals, tick_size, lot_step_size, max_leverage,
                   mark_price, mid_price, oracle_price, funding_rate_hourly, day_volume_usdc,
                   open_interest_usdc, prev_day_price, is_core_eligible, is_event_eligible,
                   is_priority_watched, created_at_ms, updated_at_ms
               ) VALUES (
                   :symbol, :coin, :idx, :sz_dec, :tick, :lot, :max_lev,
                   :mark, :mid, :oracle, :funding, :vol,
                   :oi, :prev_px, :core, :event,
                   :prio, :ts, :ts
               )
               ON DUPLICATE KEY UPDATE
                   asset_index = VALUES(asset_index),
                   sz_decimals = VALUES(sz_decimals),
                   tick_size = VALUES(tick_size),
                   lot_step_size = VALUES(lot_step_size),
                   max_leverage = VALUES(max_leverage),
                   mark_price = VALUES(mark_price),
                   mid_price = VALUES(mid_price),
                   oracle_price = VALUES(oracle_price),
                   funding_rate_hourly = VALUES(funding_rate_hourly),
                   day_volume_usdc = VALUES(day_volume_usdc),
                   open_interest_usdc = VALUES(open_interest_usdc),
                   prev_day_price = VALUES(prev_day_price),
                   is_core_eligible = VALUES(is_core_eligible),
                   is_event_eligible = VALUES(is_event_eligible),
                   is_priority_watched = VALUES(is_priority_watched),
                   updated_at_ms = VALUES(updated_at_ms)"#,
            params! {
                "symbol" => &symbol,
                "coin" => &coin,
                "idx" => input.asset_index,
                "sz_dec" => input.sz_decimals,
                "tick" => input.tick_size,
                "lot" => input.lot_step_size,
                "max_lev" => input.max_leverage,
                "mark" => input.mark_price,
                "mid" => input.mid_price,
                "oracle" => input.oracle_price,
                "funding" => input.funding_rate_hourly,
                "vol" => input.day_volume_usdc,
                "oi" => input.open_interest_usdc,
                "prev_px" => input.prev_day_price,
                "core" => i32::from(input.is_core_eligible),
                "event" => i32::from(input.is_event_eligible),
                "prio" => i32::from(input.is_priority_watched),
                "ts" => ts,
            },
        )
        .map_err(|e| AegisError::Database(format!("Failed to upsert asset '{symbol}': {e}")))?;

        let id: Option<i64> = conn
            .exec_first(
                "SELECT id FROM exchange_assets WHERE symbol = :symbol",
                params! { "symbol" => &symbol },
            )
            .map_err(|e| AegisError::Database(format!("Failed to lookup asset '{symbol}': {e}")))?;

        id.ok_or_else(|| AegisError::Database(format!("Asset '{symbol}' missing after upsert")))
    }

    /// Batch upserts multiple Hyperliquid perpetual assets inside a single MariaDB transaction,
    /// returning their `exchange_assets.id` primary keys in input order.
    pub fn upsert_exchange_assets_batch(&self, inputs: &[AssetUpsertInput]) -> Result<Vec<i64>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let mut normalized = Vec::with_capacity(inputs.len());
        for inp in inputs {
            let (symbol, coin) = normalize_hyperliquid_symbol(&inp.raw_symbol_or_coin)?;
            normalized.push((symbol, coin, inp));
        }

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start assets batch tx: {e}")))?;

        let stmt = tx
            .prep(
                r#"INSERT INTO exchange_assets (
                       symbol, coin, asset_index, sz_decimals, tick_size, lot_step_size, max_leverage,
                       mark_price, mid_price, oracle_price, funding_rate_hourly, day_volume_usdc,
                       open_interest_usdc, prev_day_price, is_core_eligible, is_event_eligible,
                       is_priority_watched, created_at_ms, updated_at_ms
                   ) VALUES (
                       :symbol, :coin, :idx, :sz_dec, :tick, :lot, :max_lev,
                       :mark, :mid, :oracle, :funding, :vol,
                       :oi, :prev_px, :core, :event,
                       :prio, :ts, :ts
                   )
                   ON DUPLICATE KEY UPDATE
                       asset_index = VALUES(asset_index),
                       sz_decimals = VALUES(sz_decimals),
                       tick_size = VALUES(tick_size),
                       lot_step_size = VALUES(lot_step_size),
                       max_leverage = VALUES(max_leverage),
                       mark_price = VALUES(mark_price),
                       mid_price = VALUES(mid_price),
                       oracle_price = VALUES(oracle_price),
                       funding_rate_hourly = VALUES(funding_rate_hourly),
                       day_volume_usdc = VALUES(day_volume_usdc),
                       open_interest_usdc = VALUES(open_interest_usdc),
                       prev_day_price = VALUES(prev_day_price),
                       is_core_eligible = VALUES(is_core_eligible),
                       is_event_eligible = VALUES(is_event_eligible),
                       is_priority_watched = VALUES(is_priority_watched),
                       updated_at_ms = VALUES(updated_at_ms)"#,
            )
            .map_err(|e| AegisError::Database(format!("Failed preparing batch asset upsert: {e}")))?;

        tx.exec_batch(
            &stmt,
            normalized.iter().map(|(symbol, coin, input)| {
                params! {
                    "symbol" => symbol,
                    "coin" => coin,
                    "idx" => input.asset_index,
                    "sz_dec" => input.sz_decimals,
                    "tick" => input.tick_size,
                    "lot" => input.lot_step_size,
                    "max_lev" => input.max_leverage,
                    "mark" => input.mark_price,
                    "mid" => input.mid_price,
                    "oracle" => input.oracle_price,
                    "funding" => input.funding_rate_hourly,
                    "vol" => input.day_volume_usdc,
                    "oi" => input.open_interest_usdc,
                    "prev_px" => input.prev_day_price,
                    "core" => i32::from(input.is_core_eligible),
                    "event" => i32::from(input.is_event_eligible),
                    "prio" => i32::from(input.is_priority_watched),
                    "ts" => ts,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("Batch asset upsert failed: {e}")))?;
        drop(stmt);

        let all_rows: Vec<(String, i64)> = tx
            .query("SELECT symbol, id FROM exchange_assets")
            .map_err(|e| AegisError::Database(format!("Batch asset id lookup failed: {e}")))?;
        let id_map: std::collections::HashMap<String, i64> = all_rows.into_iter().collect();

        tx.commit()
            .map_err(|e| AegisError::Database(format!("Batch asset commit failed: {e}")))?;

        let mut ids = Vec::with_capacity(normalized.len());
        for (symbol, ..) in normalized {
            let id = id_map.get(&symbol).copied().ok_or_else(|| {
                AegisError::Database(format!("Asset '{symbol}' missing after batch upsert"))
            })?;
            ids.push(id);
        }
        Ok(ids)
    }

    /// Updates ONLY `mid_price` and `updated_at_ms` from `allMids`, preserving oracle-anchored `mark_price`.
    pub fn update_asset_live_mid(&self, asset_id: i64, mid_price: f64) -> Result<()> {
        if !mid_price.is_finite() || mid_price <= 0.0 {
            return Ok(());
        }
        let mut conn = self.get_conn()?;
        let ts = now_ms();
        conn.exec_drop(
            r#"UPDATE exchange_assets
               SET mid_price = :mid, updated_at_ms = :ts
               WHERE id = :id"#,
            params! {
                "mid" => mid_price,
                "ts" => ts,
                "id" => asset_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("Failed to update live mid for {asset_id}: {e}"))
        })?;
        Ok(())
    }

    /// Updates live mark/mid price on an existing asset during the 1s Priority Watchlist loop.
    pub fn update_asset_live_quote(
        &self,
        asset_id: i64,
        mark_price: f64,
        mid_price: f64,
    ) -> Result<()> {
        let mut conn = self.get_conn()?;
        let ts = now_ms();
        conn.exec_drop(
            r#"UPDATE exchange_assets
               SET mark_price = :mark, mid_price = :mid, updated_at_ms = :ts
               WHERE id = :id"#,
            params! {
                "mark" => mark_price,
                "mid" => mid_price,
                "ts" => ts,
                "id" => asset_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("Failed to update live quote for {asset_id}: {e}"))
        })?;
        Ok(())
    }

    /// Sets or clears the `is_priority_watched` flag for an asset.
    pub fn set_asset_priority_watch(&self, asset_id: i64, watched: bool) -> Result<()> {
        let mut conn = self.get_conn()?;
        let ts = now_ms();
        conn.exec_drop(
            "UPDATE exchange_assets SET is_priority_watched = :w, updated_at_ms = :ts WHERE id = :id",
            params! { "w" => i32::from(watched), "ts" => ts, "id" => asset_id },
        )
        .map_err(|e| AegisError::Database(format!("Failed to set priority watch: {e}")))?;
        Ok(())
    }

    /// Looks up an `ExchangeAsset` by symbol (`"BTC-USDC"`, `"BTC"`, or `"BTC/USDC:USDC"`).
    pub fn get_asset_by_symbol(&self, raw_symbol: &str) -> Result<Option<ExchangeAsset>> {
        let (symbol, _) = normalize_hyperliquid_symbol(raw_symbol)?;
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .exec_first(
                r#"SELECT id, symbol, coin, asset_index, sz_decimals, tick_size, lot_step_size,
                          max_leverage, mark_price, mid_price, oracle_price, funding_rate_hourly,
                          day_volume_usdc, open_interest_usdc, prev_day_price, is_core_eligible,
                          is_event_eligible, is_priority_watched, created_at_ms, updated_at_ms
                   FROM exchange_assets
                   WHERE symbol = :symbol"#,
                params! { "symbol" => &symbol },
            )
            .map_err(|e| AegisError::Database(format!("get_asset_by_symbol failed: {e}")))?;

        Ok(row.map(row_to_exchange_asset))
    }

    /// Looks up an `ExchangeAsset` by primary key `id`.
    pub fn get_asset_by_id(&self, asset_id: i64) -> Result<Option<ExchangeAsset>> {
        let mut conn = self.get_conn()?;
        let row: Option<mysql::Row> = conn
            .exec_first(
                r#"SELECT id, symbol, coin, asset_index, sz_decimals, tick_size, lot_step_size,
                          max_leverage, mark_price, mid_price, oracle_price, funding_rate_hourly,
                          day_volume_usdc, open_interest_usdc, prev_day_price, is_core_eligible,
                          is_event_eligible, is_priority_watched, created_at_ms, updated_at_ms
                   FROM exchange_assets
                   WHERE id = :id"#,
                params! { "id" => asset_id },
            )
            .map_err(|e| AegisError::Database(format!("get_asset_by_id failed: {e}")))?;

        Ok(row.map(row_to_exchange_asset))
    }

    /// Lists all discovered Hyperliquid perpetual assets ordered by 24h USDC volume descending.
    pub fn list_exchange_assets(&self) -> Result<Vec<ExchangeAsset>> {
        let mut conn = self.get_conn()?;
        let rows: Vec<mysql::Row> = conn
            .query(
                r#"SELECT id, symbol, coin, asset_index, sz_decimals, tick_size, lot_step_size,
                          max_leverage, mark_price, mid_price, oracle_price, funding_rate_hourly,
                          day_volume_usdc, open_interest_usdc, prev_day_price, is_core_eligible,
                          is_event_eligible, is_priority_watched, created_at_ms, updated_at_ms
                   FROM exchange_assets
                   ORDER BY day_volume_usdc DESC, symbol ASC"#,
            )
            .map_err(|e| AegisError::Database(format!("list_exchange_assets failed: {e}")))?;

        Ok(rows.into_iter().map(row_to_exchange_asset).collect())
    }
}

fn row_to_exchange_asset(r: mysql::Row) -> ExchangeAsset {
    ExchangeAsset {
        id: r.get(0).unwrap_or_default(),
        symbol: r.get(1).unwrap_or_default(),
        coin: r.get(2).unwrap_or_default(),
        asset_index: r.get(3).unwrap_or_default(),
        sz_decimals: r.get(4).unwrap_or_default(),
        tick_size: r.get(5).unwrap_or(0.01),
        lot_step_size: r.get(6).unwrap_or(0.01),
        max_leverage: r.get(7).unwrap_or(20.0),
        mark_price: r.get(8).unwrap_or_default(),
        mid_price: r.get(9).unwrap_or_default(),
        oracle_price: r.get(10).unwrap_or_default(),
        funding_rate_hourly: r.get(11).unwrap_or_default(),
        day_volume_usdc: r.get(12).unwrap_or_default(),
        open_interest_usdc: r.get(13).unwrap_or_default(),
        prev_day_price: r.get(14).unwrap_or_default(),
        is_core_eligible: r.get::<i32, _>(15).unwrap_or(0) != 0,
        is_event_eligible: r.get::<i32, _>(16).unwrap_or(0) != 0,
        is_priority_watched: r.get::<i32, _>(17).unwrap_or(0) != 0,
        created_at_ms: r.get(18).unwrap_or_default(),
        updated_at_ms: r.get(19).unwrap_or_default(),
    }
}
