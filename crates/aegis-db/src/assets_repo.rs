//! Exchange assets discovery & live pricing repository (`aegis-db::assets_repo`).

use crate::engine::DatabaseManager;
use aegis_core::{normalize_hyperliquid_symbol, now_ms, AegisError, ExchangeAsset, Result};
use mysql::params;
use mysql::prelude::*;
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
    /// Inputs are deduplicated (last-wins) by uppercase normalized `symbol`, then ordered
    /// deterministically by `(existing_id.unwrap_or(i64::MAX), uppercase_symbol)` before starting
    /// the transaction so existing rows lock in `id` order (matching live-mid/priority batches)
    /// and brand-new listings lock at the tail in canonical symbol order.
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
            lock_ordered.into_iter().map(|entry| {
                let input = entry.input;
                params! {
                    "symbol" => &entry.symbol,
                    "coin" => &entry.coin,
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

    /// Batch updates ONLY `mid_price` and `updated_at_ms` inside a single transaction.
    /// Updates are sorted deterministically by `asset_id` ascending (deduplicating repeated
    /// `asset_id`s last-wins) before starting the transaction to prevent InnoDB deadlocks.
    pub fn update_assets_live_mid_batch(&self, updates: &[(i64, f64)]) -> Result<usize> {
        let ordered = canonicalize_asset_updates(updates, |mid| mid.is_finite() && *mid > 0.0);
        if ordered.is_empty() {
            return Ok(0);
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
            ordered.iter().map(|(&asset_id, &mid)| {
                params! {
                    "mid" => mid,
                    "ts" => ts,
                    "id" => asset_id,
                }
            }),
        )
        .map_err(|e| AegisError::Database(format!("Batch live mid update failed: {e}")))?;
        drop(stmt);
        tx.commit()
            .map_err(|e| AegisError::Database(format!("Batch live mid commit failed: {e}")))?;
        Ok(ordered.len())
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
