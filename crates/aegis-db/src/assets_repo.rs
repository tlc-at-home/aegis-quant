//! Exchange assets discovery & live pricing repository (`aegis-db::assets_repo`).

use crate::engine::DatabaseManager;
use crate::row_decode::{
    decimal_param, row_get_bool, row_get_f64, row_get_i64, row_get_string, row_get_u32,
    DEC_20_10_MAX_ABS, DEC_20_8_MAX_ABS, DEC_24_8_MAX_ABS,
};
use aegis_core::{normalize_hyperliquid_symbol, now_ms, AegisError, ExchangeAsset, Result};
use mysql::params;
use mysql::prelude::*;
use mysql::PooledConn;
use std::collections::{BTreeMap, HashMap};

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

/// Normalized upsert entry kept in canonical lock order.
#[derive(Debug, Clone)]
pub struct CanonicalAssetUpsert<'a> {
    pub symbol: String,
    pub coin: String,
    pub input: &'a AssetUpsertInput,
}

/// Canonicalized batch of asset upserts with caller-order lookup keys and
/// deduplicated (last-wins) entries ordered by uppercase normalized `symbol`.
#[derive(Debug)]
pub struct CanonicalAssetUpsertBatch<'a> {
    pub input_keys: Vec<(String, String)>,
    pub ordered: BTreeMap<String, CanonicalAssetUpsert<'a>>,
}

struct PreparedAssetDecimals {
    tick_size: String,
    lot_step_size: String,
    max_leverage: String,
    mark_price: String,
    mid_price: String,
    oracle_price: String,
    funding_rate_hourly: String,
    day_volume_usdc: String,
    open_interest_usdc: String,
    prev_day_price: String,
}

fn prepare_asset_decimals(input: &AssetUpsertInput) -> Result<PreparedAssetDecimals> {
    Ok(PreparedAssetDecimals {
        tick_size: decimal_param(input.tick_size, 8, DEC_20_8_MAX_ABS)?,
        lot_step_size: decimal_param(input.lot_step_size, 8, DEC_20_8_MAX_ABS)?,
        max_leverage: decimal_param(input.max_leverage, 8, DEC_20_8_MAX_ABS)?,
        mark_price: decimal_param(input.mark_price, 8, DEC_20_8_MAX_ABS)?,
        mid_price: decimal_param(input.mid_price, 8, DEC_20_8_MAX_ABS)?,
        oracle_price: decimal_param(input.oracle_price, 8, DEC_20_8_MAX_ABS)?,
        funding_rate_hourly: decimal_param(input.funding_rate_hourly, 10, DEC_20_10_MAX_ABS)?,
        day_volume_usdc: decimal_param(input.day_volume_usdc, 8, DEC_24_8_MAX_ABS)?,
        open_interest_usdc: decimal_param(input.open_interest_usdc, 8, DEC_24_8_MAX_ABS)?,
        prev_day_price: decimal_param(input.prev_day_price, 8, DEC_20_8_MAX_ABS)?,
    })
}

/// Filters, sorts deterministically by `asset_id` ascending, and deduplicates
/// repeated `asset_id`s (last-wins) prior to starting a batch update transaction.
pub fn canonicalize_asset_updates<T: Copy>(
    updates: &[(i64, T)],
    mut keep: impl FnMut(&T) -> bool,
) -> BTreeMap<i64, T> {
    let mut ordered = BTreeMap::new();
    for &(asset_id, value) in updates {
        if keep(&value) {
            ordered.insert(asset_id, value);
        }
    }
    ordered
}

/// Normalizes and canonicalizes batch upsert inputs by uppercase normalized `symbol`
/// (deduplicating last-wins) while preserving caller input order for ID lookup.
pub fn canonicalize_asset_upserts(
    inputs: &[AssetUpsertInput],
) -> Result<CanonicalAssetUpsertBatch<'_>> {
    let mut input_keys = Vec::with_capacity(inputs.len());
    let mut ordered = BTreeMap::new();
    for inp in inputs {
        let (symbol, coin) = normalize_hyperliquid_symbol(&inp.raw_symbol_or_coin)?;
        let sort_key = symbol.to_ascii_uppercase();
        input_keys.push((sort_key.clone(), symbol.clone()));
        ordered.insert(
            sort_key,
            CanonicalAssetUpsert {
                symbol,
                coin,
                input: inp,
            },
        );
    }
    Ok(CanonicalAssetUpsertBatch {
        input_keys,
        ordered,
    })
}

/// Orders deduplicated upserts deterministically by `(existing_id.unwrap_or(i64::MAX), sort_key)`
/// ascending so existing rows acquire InnoDB locks in `exchange_assets.id` order (matching
/// `update_assets_live_mid_batch` and `set_assets_priority_watch_batch`) while brand-new listings
/// lock at the tail in uppercase normalized `symbol` order.
pub fn order_asset_upserts_by_lock_key<'b, 'a>(
    ordered: &'b BTreeMap<String, CanonicalAssetUpsert<'a>>,
    existing_ids: &HashMap<String, i64>,
) -> Vec<&'b CanonicalAssetUpsert<'a>> {
    let mut items: Vec<((i64, &str), &'b CanonicalAssetUpsert<'a>)> = ordered
        .iter()
        .map(|(sort_key, entry)| {
            let existing_id = existing_ids.get(sort_key).copied().unwrap_or(i64::MAX);
            ((existing_id, sort_key.as_str()), entry)
        })
        .collect();
    items.sort_by_key(|(a_key, _)| *a_key);
    items.into_iter().map(|(_, entry)| entry).collect()
}

const ASSET_UPSERT_SQL: &str = r#"INSERT INTO exchange_assets (
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
       updated_at_ms = VALUES(updated_at_ms)"#;

impl DatabaseManager {
    /// Upserts a Hyperliquid perpetual asset with canonical `"<COIN>-USDC"` symbol normalization
    /// and returns its MariaDB `exchange_assets.id` primary key.
    pub fn upsert_exchange_asset(&self, input: &AssetUpsertInput) -> Result<i64> {
        let (symbol, coin) = normalize_hyperliquid_symbol(&input.raw_symbol_or_coin)?;
        let dec = prepare_asset_decimals(input)?;
        let ts = now_ms();
        let mut conn = self.get_conn()?;

        conn.exec_drop(
            ASSET_UPSERT_SQL,
            params! {
                "symbol" => &symbol,
                "coin" => &coin,
                "idx" => input.asset_index,
                "sz_dec" => input.sz_decimals,
                "tick" => &dec.tick_size,
                "lot" => &dec.lot_step_size,
                "max_lev" => &dec.max_leverage,
                "mark" => &dec.mark_price,
                "mid" => &dec.mid_price,
                "oracle" => &dec.oracle_price,
                "funding" => &dec.funding_rate_hourly,
                "vol" => &dec.day_volume_usdc,
                "oi" => &dec.open_interest_usdc,
                "prev_px" => &dec.prev_day_price,
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
        let CanonicalAssetUpsertBatch {
            input_keys,
            ordered,
        } = canonicalize_asset_upserts(inputs)?;

        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let existing_rows: Vec<(String, i64)> = conn
            .query("SELECT UPPER(symbol), id FROM exchange_assets")
            .map_err(|e| {
                AegisError::Database(format!("Batch asset existing id lookup failed: {e}"))
            })?;
        let existing_ids: HashMap<String, i64> = existing_rows
            .into_iter()
            .map(|(symbol, id)| (symbol.to_ascii_uppercase(), id))
            .collect();
        let lock_ordered = order_asset_upserts_by_lock_key(&ordered, &existing_ids);

        let mut prepared = Vec::with_capacity(lock_ordered.len());
        for entry in lock_ordered {
            prepared.push((entry, prepare_asset_decimals(entry.input)?));
        }

        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start assets batch tx: {e}")))?;

        let stmt = tx.prep(ASSET_UPSERT_SQL).map_err(|e| {
            AegisError::Database(format!("Failed preparing batch asset upsert: {e}"))
        })?;

        tx.exec_batch(
            &stmt,
            prepared.iter().map(|(entry, dec)| {
                let input = entry.input;
                params! {
                    "symbol" => &entry.symbol,
                    "coin" => &entry.coin,
                    "idx" => input.asset_index,
                    "sz_dec" => input.sz_decimals,
                    "tick" => &dec.tick_size,
                    "lot" => &dec.lot_step_size,
                    "max_lev" => &dec.max_leverage,
                    "mark" => &dec.mark_price,
                    "mid" => &dec.mid_price,
                    "oracle" => &dec.oracle_price,
                    "funding" => &dec.funding_rate_hourly,
                    "vol" => &dec.day_volume_usdc,
                    "oi" => &dec.open_interest_usdc,
                    "prev_px" => &dec.prev_day_price,
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
        let id_map: HashMap<String, i64> = all_rows
            .into_iter()
            .map(|(symbol, id)| (symbol.to_ascii_uppercase(), id))
            .collect();

        tx.commit()
            .map_err(|e| AegisError::Database(format!("Batch asset commit failed: {e}")))?;

        let mut ids = Vec::with_capacity(input_keys.len());
        for (sort_key, symbol) in input_keys {
            let id = id_map.get(&sort_key).copied().ok_or_else(|| {
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
        let mid_dec = decimal_param(mid_price, 8, DEC_20_8_MAX_ABS)?;
        let mut conn = self.get_conn()?;
        let ts = now_ms();
        conn.exec_drop(
            r#"UPDATE exchange_assets
               SET mid_price = :mid, updated_at_ms = :ts
               WHERE id = :id"#,
            params! {
                "mid" => &mid_dec,
                "ts" => ts,
                "id" => asset_id,
            },
        )
        .map_err(|e| {
            AegisError::Database(format!("Failed to update live mid for {asset_id}: {e}"))
        })?;
        Ok(())
    }

    /// Batch updates ONLY `mid_price` and `updated_at_ms` inside a single transaction.
    /// Updates are sorted deterministically by `asset_id` ascending (deduplicating repeated
    /// `asset_id`s last-wins) before starting the transaction to prevent InnoDB deadlocks.
    pub fn update_assets_live_mid_batch(&self, updates: &[(i64, f64)]) -> Result<usize> {
        let ordered = canonicalize_asset_updates(updates, |mid| mid.is_finite() && *mid > 0.0);
        if ordered.is_empty() {
            return Ok(0);
        }
        let mut prepared = Vec::with_capacity(ordered.len());
        for (&asset_id, &mid) in &ordered {
            prepared.push((asset_id, decimal_param(mid, 8, DEC_20_8_MAX_ABS)?));
        }
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| AegisError::Database(format!("Failed to start mid batch tx: {e}")))?;
        let stmt = tx
            .prep(
                r#"UPDATE exchange_assets
                   SET mid_price = :mid, updated_at_ms = :ts
                   WHERE id = :id"#,
            )
            .map_err(|e| AegisError::Database(format!("Failed preparing mid batch update: {e}")))?;
        tx.exec_batch(
            &stmt,
            prepared.iter().map(|(asset_id, mid_dec)| {
                params! {
                    "mid" => mid_dec,
                    "ts" => ts,
                    "id" => *asset_id,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("Batch live mid update failed: {e}")))?;
        drop(stmt);
        tx.commit()
            .map_err(|e| AegisError::Database(format!("Batch live mid commit failed: {e}")))?;
        Ok(prepared.len())
    }

    /// Updates live mark/mid price on an existing asset during the 1s Priority Watchlist loop.
    pub fn update_asset_live_quote(
        &self,
        asset_id: i64,
        mark_price: f64,
        mid_price: f64,
    ) -> Result<()> {
        let mark_dec = decimal_param(mark_price, 8, DEC_20_8_MAX_ABS)?;
        let mid_dec = decimal_param(mid_price, 8, DEC_20_8_MAX_ABS)?;
        let mut conn = self.get_conn()?;
        let ts = now_ms();
        conn.exec_drop(
            r#"UPDATE exchange_assets
               SET mark_price = :mark, mid_price = :mid, updated_at_ms = :ts
               WHERE id = :id"#,
            params! {
                "mark" => &mark_dec,
                "mid" => &mid_dec,
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

    /// Batch updates `is_priority_watched` inside a single transaction.
    /// Updates are sorted deterministically by `asset_id` ascending (deduplicating repeated
    /// `asset_id`s last-wins) before starting the transaction to prevent InnoDB deadlocks.
    pub fn set_assets_priority_watch_batch(&self, updates: &[(i64, bool)]) -> Result<usize> {
        let ordered = canonicalize_asset_updates(updates, |_| true);
        if ordered.is_empty() {
            return Ok(0);
        }
        let ts = now_ms();
        let mut conn = self.get_conn()?;
        let mut tx = conn
            .start_transaction(mysql::TxOpts::default())
            .map_err(|e| {
                AegisError::Database(format!("Failed to start priority watch batch tx: {e}"))
            })?;
        let stmt = tx
            .prep("UPDATE exchange_assets SET is_priority_watched = :w, updated_at_ms = :ts WHERE id = :id")
            .map_err(|e| AegisError::Database(format!("Failed preparing priority watch batch: {e}")))?;
        tx.exec_batch(
            &stmt,
            ordered.iter().map(|(&asset_id, &watched)| {
                params! {
                    "w" => i32::from(watched),
                    "ts" => ts,
                    "id" => asset_id,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("Batch priority watch update failed: {e}")))?;
        drop(stmt);
        tx.commit().map_err(|e| {
            AegisError::Database(format!("Batch priority watch commit failed: {e}"))
        })?;
        Ok(ordered.len())
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

        Ok(row.map(|r| row_to_exchange_asset(&r)))
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

        Ok(row.map(|r| row_to_exchange_asset(&r)))
    }

    /// Lists all discovered Hyperliquid perpetual assets ordered by 24h USDC volume descending.
    pub fn list_exchange_assets(&self) -> Result<Vec<ExchangeAsset>> {
        let mut conn = self.get_conn()?;
        Self::list_exchange_assets_with_conn(&mut conn)
    }

    pub(crate) fn list_exchange_assets_with_conn(
        conn: &mut PooledConn,
    ) -> Result<Vec<ExchangeAsset>> {
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

        Ok(rows.iter().map(row_to_exchange_asset).collect())
    }
}

fn row_to_exchange_asset(r: &mysql::Row) -> ExchangeAsset {
    ExchangeAsset {
        id: row_get_i64(r, 0, 0),
        symbol: row_get_string(r, 1, ""),
        coin: row_get_string(r, 2, ""),
        asset_index: row_get_u32(r, 3, 0),
        sz_decimals: row_get_u32(r, 4, 0),
        tick_size: row_get_f64(r, 5, 0.01),
        lot_step_size: row_get_f64(r, 6, 0.01),
        max_leverage: row_get_f64(r, 7, 20.0),
        mark_price: row_get_f64(r, 8, 0.0),
        mid_price: row_get_f64(r, 9, 0.0),
        oracle_price: row_get_f64(r, 10, 0.0),
        funding_rate_hourly: row_get_f64(r, 11, 0.0),
        day_volume_usdc: row_get_f64(r, 12, 0.0),
        open_interest_usdc: row_get_f64(r, 13, 0.0),
        prev_day_price: row_get_f64(r, 14, 0.0),
        is_core_eligible: row_get_bool(r, 15, false),
        is_event_eligible: row_get_bool(r, 16, false),
        is_priority_watched: row_get_bool(r, 17, false),
        created_at_ms: row_get_i64(r, 18, 0),
        updated_at_ms: row_get_i64(r, 19, 0),
    }
}
