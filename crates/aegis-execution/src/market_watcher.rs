//! 3-Second Full-Exchange Scanner (`aegis-execution::market_watcher`).
//!
//! Polls Hyperliquid `metaAndAssetCtxs` across all 150+ perpetuals (falling back
//! to `allMids` only when `midPx` is absent), normalizes all symbols to canonical
//! `"<COIN>-USDC"`, updates `exchange_assets`, expires finished cooldowns in
//! `asset_cooldowns`, and incrementally refreshes 1h/4h OHLCV bars (`R14-H4`, `P0-Liq`).

use crate::http_client::{is_testnet_configured, HyperliquidHttpClient};
use crate::signing::infer_tick_size;
use aegis_core::{
    normalize_hyperliquid_symbol, now_ms, parse_hl_f64, parse_hl_f64_opt, round_dp, ConfigMap,
    ExchangeAsset, Result,
};
use aegis_db::{AssetUpsertInput, BarUpsertInput, DatabaseManager};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const CANDLE_1H_TTL_MS: i64 = 300_000;
const CANDLE_4H_TTL_MS: i64 = 900_000;
const CANDLE_POST_BOUNDARY_TRIGGER_MS: i64 = 15_000;
const CANDLE_INCREMENTAL_DB_MIN_BARS: usize = 48;
const DEFAULT_CANDLE_REFRESH_MAX_ASSETS: usize = 30;
const MAX_CANDLE_ASSETS_PER_3S_TICK: usize = 2;

fn is_timeframe_sync_due(last_sync_ms: Option<i64>, now: i64, tf_ms: i64, ttl_ms: i64) -> bool {
    let Some(last) = last_sync_ms else {
        return true;
    };
    if now.saturating_sub(last) >= ttl_ms {
        return true;
    }
    let boundary_trigger_ms = (now / tf_ms) * tf_ms + CANDLE_POST_BOUNDARY_TRIGGER_MS;
    now >= boundary_trigger_ms && last < boundary_trigger_ms
}

/// Summary of a single 3-second full-exchange scanner tick.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MarketScanReport {
    pub scanned_at_ms: i64,
    pub total_assets_upserted: usize,
    pub core_eligible_count: usize,
    pub event_eligible_count: usize,
    pub priority_watched_count: usize,
    pub expired_cooldowns: u64,
    pub bars_refreshed_assets: usize,
    pub live_exchange_connected: bool,
}

/// 3-Second Full-Exchange Market Watcher.
#[derive(Clone)]
pub struct MarketWatcher {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
    last_bar_sync_ms: Arc<Mutex<HashMap<(i64, String), i64>>>,
}

impl MarketWatcher {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient) -> Self {
        Self {
            db,
            http,
            last_bar_sync_ms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Executes a single 3-second full-exchange scan:
    /// 1. Polls Hyperliquid `metaAndAssetCtxs` + `allMids` (or uses fallback seed if offline & DB empty).
    /// 2. Normalizes symbols to `"<COIN>-USDC"` and applies 24h volume + open-interest liquidity floors to assign `is_core_eligible` / `is_event_eligible`.
    /// 3. Upserts all assets into MariaDB `exchange_assets`.
    /// 4. Expires finished cooldowns in `asset_cooldowns`.
    /// 5. Incrementally refreshes 1h and 4h OHLCV bars for core/event/priority and active held/ordered assets.
    pub fn run_3s_exchange_scan(&self) -> Result<MarketScanReport> {
        let cfg = self.db.load_config_or_default();
        let ts = now_ms();

        let expired_cooldowns = self.db.expire_finished_cooldowns().unwrap_or(0);

        let (mut raw_inputs, live_connected) = match self.fetch_live_asset_inputs(&cfg) {
            Some(list) if !list.is_empty() => (list, true),
            _ => {
                let existing = self.db.list_exchange_assets().unwrap_or_default();
                if existing.is_empty() {
                    (Self::build_offline_fallback_assets(), false)
                } else {
                    (Vec::new(), false)
                }
            }
        };

        let mut active_asset_ids: HashSet<i64> = HashSet::new();
        let mut active_symbols: HashSet<String> = HashSet::new();
        for h in self.db.get_assets_held().unwrap_or_default() {
            active_asset_ids.insert(h.asset_id);
            active_symbols.insert(h.symbol);
        }
        for o in self.db.get_assets_ordered().unwrap_or_default() {
            active_asset_ids.insert(o.asset_id);
            active_symbols.insert(o.symbol);
        }

        // Preserve 1s priority surveillance on any currently held or working-order symbol even if
        // its live 24h volume/OI dips below Core/Event eligibility floors.
        if !active_symbols.is_empty() {
            for input in &mut raw_inputs {
                if let Ok((sym, _)) = normalize_hyperliquid_symbol(&input.raw_symbol_or_coin) {
                    if active_symbols.contains(&sym) {
                        input.is_priority_watched = true;
                    }
                }
            }
        }

        let mut total_upserted = 0usize;
        let mut core_count = 0usize;
        let mut event_count = 0usize;
        let mut prio_count = 0usize;
        let mut upserted_prio_symbols: HashSet<String> = HashSet::new();

        if self.db.upsert_exchange_assets_batch(&raw_inputs).is_ok() {
            for input in &raw_inputs {
                total_upserted += 1;
                if input.is_core_eligible {
                    core_count += 1;
                }
                if input.is_event_eligible {
                    event_count += 1;
                }
                if input.is_priority_watched {
                    prio_count += 1;
                    if let Ok((sym, _)) = normalize_hyperliquid_symbol(&input.raw_symbol_or_coin) {
                        upserted_prio_symbols.insert(sym);
                    }
                }
            }
        } else {
            for input in &raw_inputs {
                if self.db.upsert_exchange_asset(input).is_ok() {
                    total_upserted += 1;
                    if input.is_core_eligible {
                        core_count += 1;
                    }
                    if input.is_event_eligible {
                        event_count += 1;
                    }
                    if input.is_priority_watched {
                        prio_count += 1;
                        if let Ok((sym, _)) =
                            normalize_hyperliquid_symbol(&input.raw_symbol_or_coin)
                        {
                            upserted_prio_symbols.insert(sym);
                        }
                    }
                }
            }
        }

        // Clear stale `is_priority_watched` flags on assets that are no longer held, ordered, or extreme-dislocated
        if let Ok(existing) = self.db.list_exchange_assets() {
            let mut prio_clears: Vec<(i64, bool)> = Vec::new();
            for a in &existing {
                if !a.is_priority_watched || active_asset_ids.contains(&a.id) {
                    continue;
                }
                if live_connected {
                    if !upserted_prio_symbols.contains(&a.symbol) {
                        prio_clears.push((a.id, false));
                    }
                } else if raw_inputs.is_empty() {
                    let day_chg = if a.prev_day_price > 0.0 {
                        ((a.mark_price - a.prev_day_price) / a.prev_day_price).abs()
                    } else {
                        0.0
                    };
                    if a.funding_rate_hourly.abs() < 0.0004 && day_chg < 0.08 {
                        prio_clears.push((a.id, false));
                    }
                }
            }
            let _ = self.db.set_assets_priority_watch_batch(&prio_clears);
        }

        if total_upserted == 0 {
            if let Ok(existing) = self.db.list_exchange_assets() {
                total_upserted = existing.len();
                core_count = existing.iter().filter(|a| a.is_core_eligible).count();
                event_count = existing.iter().filter(|a| a.is_event_eligible).count();
                prio_count = existing.iter().filter(|a| a.is_priority_watched).count();
            }
        }

        let bars_refreshed = self
            .refresh_bars_for_eligible_assets(
                MAX_CANDLE_ASSETS_PER_3S_TICK,
                72,
                live_connected,
                &cfg,
            )
            .unwrap_or(0);

        Ok(MarketScanReport {
            scanned_at_ms: ts,
            total_assets_upserted: total_upserted,
            core_eligible_count: core_count,
            event_eligible_count: event_count,
            priority_watched_count: prio_count,
            expired_cooldowns,
            bars_refreshed_assets: bars_refreshed,
            live_exchange_connected: live_connected,
        })
    }

    /// Parses live `metaAndAssetCtxs` from Hyperliquid into normalized [`AssetUpsertInput`] records.
    ///
    /// Uses `midPx` embedded in `metaAndAssetCtxs`, falling back to `allMids` only when `midPx`
    /// is absent across the universe (`R14-H4`).
    pub fn fetch_live_asset_inputs(&self, cfg: &ConfigMap) -> Option<Vec<AssetUpsertInput>> {
        if !crate::engine::is_live_exchange_active(&self.db, cfg) {
            return None;
        }
        let meta_ctxs = self.http.fetch_meta_and_asset_ctxs(cfg).ok()?;
        let has_ctx_mids = meta_ctxs
            .as_array()
            .and_then(|arr| arr.get(1))
            .and_then(Value::as_array)
            .is_some_and(|ctxs| {
                ctxs.iter()
                    .any(|c| parse_hl_f64_opt(c.get("midPx")).is_some_and(|p| p > 0.0))
            });
        let mids = if has_ctx_mids {
            HashMap::new()
        } else {
            self.http.fetch_all_mids(cfg).unwrap_or_default()
        };
        Self::parse_live_asset_inputs(&meta_ctxs, &mids, cfg)
    }

    /// Pure parser and 24h volume + open-interest liquidity gate for Hyperliquid `metaAndAssetCtxs` + `allMids`.
    pub fn parse_live_asset_inputs(
        meta_ctxs: &Value,
        mids: &HashMap<String, f64>,
        cfg: &ConfigMap,
    ) -> Option<Vec<AssetUpsertInput>> {
        let arr = meta_ctxs.as_array()?;
        if arr.len() < 2 {
            return None;
        }
        let universe = arr[0].get("universe")?.as_array()?;
        let ctxs = arr[1].as_array()?;

        struct RawParsedAsset {
            coin: String,
            asset_index: u32,
            sz_decimals: u32,
            max_leverage: f64,
            mark_price: f64,
            mid_price: f64,
            oracle_price: f64,
            funding_rate_hourly: f64,
            day_volume_usdc: f64,
            open_interest_usdc: f64,
            prev_day_price: f64,
        }

        let mut parsed = Vec::with_capacity(universe.len());
        for (idx, (u, c)) in universe.iter().zip(ctxs.iter()).enumerate() {
            if u.get("isDelisted")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                continue;
            }
            let raw_name = u.get("name").and_then(Value::as_str).unwrap_or("").trim();
            if raw_name.is_empty() {
                continue;
            }
            let Ok((_, coin)) = normalize_hyperliquid_symbol(raw_name) else {
                continue;
            };
            let sz_decimals = u
                .get("szDecimals")
                .and_then(Value::as_u64)
                .unwrap_or(2)
                .min(6) as u32;
            let max_leverage = u
                .get("maxLeverage")
                .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                .unwrap_or(20.0)
                .max(1.0);

            let mark_px = parse_hl_f64(c.get("markPx"));
            let ctx_mid_px = parse_hl_f64(c.get("midPx"));
            let live_mid = mids
                .get(raw_name)
                .copied()
                .or_else(|| mids.get(&coin).copied())
                .filter(|v| *v > 0.0)
                .unwrap_or(if ctx_mid_px > 0.0 {
                    ctx_mid_px
                } else {
                    mark_px
                });
            let eff_mark = if mark_px > 0.0 { mark_px } else { live_mid };
            if eff_mark <= 0.0 {
                continue;
            }
            let oracle_px = {
                let o = parse_hl_f64(c.get("oraclePx"));
                if o > 0.0 {
                    o
                } else {
                    eff_mark
                }
            };
            let prev_day_px = {
                let p = parse_hl_f64(c.get("prevDayPx"));
                if p > 0.0 {
                    p
                } else {
                    eff_mark
                }
            };
            let funding_hourly = parse_hl_f64(c.get("funding"));
            let day_vol_usdc = parse_hl_f64(c.get("dayNtlVlm")).max(0.0);
            let oi_coins = parse_hl_f64(c.get("openInterest")).max(0.0);
            let oi_usdc = round_dp(oi_coins * eff_mark, 2);

            parsed.push(RawParsedAsset {
                coin,
                asset_index: idx as u32,
                sz_decimals,
                max_leverage,
                mark_price: eff_mark,
                mid_price: if live_mid > 0.0 { live_mid } else { eff_mark },
                oracle_price: oracle_px,
                funding_rate_hourly: funding_hourly,
                day_volume_usdc: round_dp(day_vol_usdc, 2),
                open_interest_usdc: oi_usdc,
                prev_day_price: prev_day_px,
            });
        }

        if parsed.is_empty() {
            return None;
        }

        let is_testnet = is_testnet_configured(cfg);
        let (core_min_vol, event_min_vol, core_min_oi, event_min_oi) = if is_testnet {
            (
                cfg.get_f64("testnet_core_min_day_volume_usdc", 50_000.0)
                    .max(0.0),
                cfg.get_f64("testnet_event_min_day_volume_usdc", 50_000.0)
                    .max(0.0),
                cfg.get_f64("testnet_core_min_open_interest_usdc", 50_000.0)
                    .max(0.0),
                cfg.get_f64("testnet_event_min_open_interest_usdc", 50_000.0)
                    .max(0.0),
            )
        } else {
            (
                cfg.get_f64("core_min_day_volume_usdc", 2_000_000.0)
                    .max(0.0),
                cfg.get_f64("event_min_day_volume_usdc", 500_000.0).max(0.0),
                cfg.get_f64("core_min_open_interest_usdc", 250_000.0)
                    .max(0.0),
                cfg.get_f64("event_min_open_interest_usdc", 100_000.0)
                    .max(0.0),
            )
        };

        let mut inputs = Vec::with_capacity(parsed.len());
        for p in parsed {
            let lot_step_size = 10_f64.powi(-(p.sz_decimals as i32)).max(1e-6);
            let tick_size = infer_tick_size(p.mark_price, p.sz_decimals);
            let passes_core_liquidity =
                p.day_volume_usdc >= core_min_vol && p.open_interest_usdc >= core_min_oi;
            let passes_event_liquidity =
                p.day_volume_usdc >= event_min_vol && p.open_interest_usdc >= event_min_oi;
            let is_core = passes_core_liquidity;
            let is_event = is_core || passes_event_liquidity;

            // Flag extreme funding dislocation or >8% intraday move on liquid assets for 1s priority surveillance
            let day_change_pct = if p.prev_day_price > 0.0 {
                ((p.mark_price - p.prev_day_price) / p.prev_day_price).abs()
            } else {
                0.0
            };
            let is_prio =
                is_event && (p.funding_rate_hourly.abs() >= 0.0004 || day_change_pct >= 0.08);

            inputs.push(AssetUpsertInput {
                raw_symbol_or_coin: p.coin,
                asset_index: p.asset_index,
                sz_decimals: p.sz_decimals,
                tick_size,
                lot_step_size,
                max_leverage: p.max_leverage,
                mark_price: p.mark_price,
                mid_price: p.mid_price,
                oracle_price: p.oracle_price,
                funding_rate_hourly: p.funding_rate_hourly,
                day_volume_usdc: p.day_volume_usdc,
                open_interest_usdc: p.open_interest_usdc,
                prev_day_price: p.prev_day_price,
                is_core_eligible: is_core,
                is_event_eligible: is_event,
                is_priority_watched: is_prio,
            });
        }

        Some(inputs)
    }

    /// Refreshes 1h and 4h OHLCV bars for up to `max_assets_per_tick` active, priority-watched,
    /// and top `candle_refresh_max_assets` eligible assets using split TTLs (`1h` = 300s, `4h` = 900s)
    /// plus 15s post-boundary triggers and incremental lookback (`R14-H4`).
    pub fn refresh_bars_for_eligible_assets(
        &self,
        max_assets_per_tick: usize,
        lookback_bars: usize,
        live_connected: bool,
        cfg: &ConfigMap,
    ) -> Result<usize> {
        self.refresh_bars_for_eligible_assets_at(
            now_ms(),
            max_assets_per_tick,
            lookback_bars,
            live_connected,
            cfg,
        )
    }

    /// Timestamp-parameterized variant of [`Self::refresh_bars_for_eligible_assets`] enabling
    /// deterministic boundary and split-TTL verification.
    pub fn refresh_bars_for_eligible_assets_at(
        &self,
        now: i64,
        max_assets_per_tick: usize,
        lookback_bars: usize,
        live_connected: bool,
        cfg: &ConfigMap,
    ) -> Result<usize> {
        if max_assets_per_tick == 0 {
            return Ok(0);
        }
        let assets = self.db.list_exchange_assets()?;
        if assets.is_empty() {
            return Ok(0);
        }

        let active_asset_ids: HashSet<i64> = self
            .db
            .get_assets_held()
            .unwrap_or_default()
            .into_iter()
            .map(|h| h.asset_id)
            .chain(
                self.db
                    .get_assets_ordered()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|o| o.asset_id),
            )
            .collect();

        let candle_refresh_max_assets = cfg
            .get_usize(
                "candle_refresh_max_assets",
                DEFAULT_CANDLE_REFRESH_MAX_ASSETS,
            )
            .clamp(5, 150);

        let mut priority_or_active: Vec<ExchangeAsset> = Vec::new();
        let mut volume_eligible: Vec<ExchangeAsset> = Vec::new();
        for asset in assets {
            if active_asset_ids.contains(&asset.id) || asset.is_priority_watched {
                priority_or_active.push(asset);
            } else if asset.is_core_eligible || asset.is_event_eligible {
                volume_eligible.push(asset);
            }
        }
        volume_eligible.sort_by(|a, b| b.day_volume_usdc.total_cmp(&a.day_volume_usdc));
        volume_eligible.truncate(candle_refresh_max_assets);

        let mut refresh_set = priority_or_active;
        refresh_set.extend(volume_eligible);

        let mut due_candidates: Vec<(ExchangeAsset, bool, bool, i64)> = Vec::new();
        {
            let guard = self.last_bar_sync_ms.lock();
            for asset in refresh_set {
                let last_1h = guard.get(&(asset.id, "1h".to_string())).copied();
                let last_4h = guard.get(&(asset.id, "4h".to_string())).copied();
                let needs_1h = is_timeframe_sync_due(last_1h, now, 3_600_000, CANDLE_1H_TTL_MS);
                let needs_4h = is_timeframe_sync_due(last_4h, now, 14_400_000, CANDLE_4H_TTL_MS);
                if !needs_1h && !needs_4h {
                    continue;
                }
                let earliest_last = match (needs_1h, needs_4h) {
                    (true, true) => last_1h.unwrap_or(0).min(last_4h.unwrap_or(0)),
                    (true, false) => last_1h.unwrap_or(0),
                    (false, true) => last_4h.unwrap_or(0),
                    (false, false) => i64::MAX,
                };
                due_candidates.push((asset, needs_1h, needs_4h, earliest_last));
            }
        }

        due_candidates.sort_by(|(a, _, _, a_ts), (b, _, _, b_ts)| {
            (!active_asset_ids.contains(&a.id))
                .cmp(&!active_asset_ids.contains(&b.id))
                .then_with(|| (!a.is_priority_watched).cmp(&!b.is_priority_watched))
                .then_with(|| a_ts.cmp(b_ts))
                .then_with(|| b.day_volume_usdc.total_cmp(&a.day_volume_usdc))
        });

        let mut refreshed_assets = 0usize;
        for (asset, needs_1h, needs_4h, _) in due_candidates.into_iter().take(max_assets_per_tick) {
            for (tf, due) in [("1h", needs_1h), ("4h", needs_4h)] {
                if !due {
                    continue;
                }
                let bars = self.fetch_or_synthesize_bars(
                    &asset,
                    tf,
                    lookback_bars.max(CANDLE_INCREMENTAL_DB_MIN_BARS),
                    live_connected,
                    cfg,
                );
                if !bars.is_empty() {
                    let _ = self.db.upsert_ohlcv_bars(&bars);
                }
                self.last_bar_sync_ms
                    .lock()
                    .insert((asset.id, tf.to_string()), now);
            }
            refreshed_assets += 1;
        }

        Ok(refreshed_assets)
    }

    pub fn fetch_or_synthesize_bars(
        &self,
        asset: &ExchangeAsset,
        timeframe: &str,
        lookback_bars: usize,
        live_connected: bool,
        cfg: &ConfigMap,
    ) -> Vec<BarUpsertInput> {
        let tf_ms = match timeframe {
            "1h" => 3_600_000i64,
            "4h" => 14_400_000i64,
            _ => 3_600_000i64,
        };
        let now = now_ms();
        let full_start_ms = now - (lookback_bars as i64 + 6) * tf_ms;
        let start_ms = if live_connected {
            let existing_bars = self
                .db
                .get_ohlcv_bars(asset.id, timeframe, CANDLE_INCREMENTAL_DB_MIN_BARS)
                .unwrap_or_default();
            if existing_bars.len() >= CANDLE_INCREMENTAL_DB_MIN_BARS {
                existing_bars
                    .last()
                    .map(|b| (b.timestamp_ms - 2 * tf_ms).max(0))
                    .unwrap_or(full_start_ms)
            } else {
                full_start_ms
            }
        } else {
            full_start_ms
        };

        let is_live_mode = cfg.execution_mode().is_live_or_testnet()
            && (!self.db.is_ephemeral_test_db() || crate::http_client::endpoints_are_loopback(cfg));

        if live_connected {
            if let Ok(raw_list) =
                self.http
                    .fetch_candle_snapshot(&asset.coin, timeframe, start_ms, now, cfg)
            {
                let mut live_bars = Vec::with_capacity(raw_list.len());
                for item in &raw_list {
                    let t = item.get("t").and_then(Value::as_i64).unwrap_or(0);
                    let o = parse_hl_f64(item.get("o"));
                    let h = parse_hl_f64(item.get("h"));
                    let l = parse_hl_f64(item.get("l"));
                    let c = parse_hl_f64(item.get("c"));
                    let v = parse_hl_f64(item.get("v"));
                    let f = parse_hl_f64(item.get("f"));
                    if t > 0 && c > 0.0 {
                        live_bars.push(BarUpsertInput {
                            asset_id: asset.id,
                            timeframe: timeframe.to_string(),
                            timestamp_ms: t,
                            open: if o > 0.0 { o } else { c },
                            high: if h > 0.0 { h } else { c },
                            low: if l > 0.0 { l } else { c },
                            close: c,
                            volume: v.max(0.0),
                            funding_rate: f,
                        });
                    }
                }
                if is_live_mode {
                    return live_bars;
                }
                if live_bars.len() >= 24 {
                    return live_bars;
                }
            }
        }

        if is_live_mode {
            tracing::debug!(
                symbol = %asset.symbol,
                timeframe = timeframe,
                "Skipping synthetic bar generation in LIVE/TESTNET mode on transient candle snapshot failure"
            );
            return Vec::new();
        }

        Self::generate_anchored_bars(
            asset.id,
            &asset.symbol,
            timeframe,
            lookback_bars,
            asset.mark_price,
            asset.prev_day_price,
            asset.funding_rate_hourly,
            asset.day_volume_usdc,
        )
    }

    /// Generates deterministic historical OHLCV bars anchored to the asset's current `mark_price`
    /// and `prev_day_price` when Testnet `candleSnapshot` history is sparse or in offline test mode.
    #[allow(clippy::too_many_arguments)]
    pub fn generate_anchored_bars(
        asset_id: i64,
        symbol: &str,
        timeframe: &str,
        num_bars: usize,
        current_price: f64,
        prev_day_price: f64,
        funding_rate_hourly: f64,
        day_volume_usdc: f64,
    ) -> Vec<BarUpsertInput> {
        let count = num_bars.max(24);
        let tf_ms = match timeframe {
            "4h" => 14_400_000i64,
            _ => 3_600_000i64,
        };
        let now_aligned = (now_ms() / tf_ms) * tf_ms;
        let start_ms = now_aligned - ((count.saturating_sub(1)) as i64) * tf_ms;

        let target_px = current_price.max(0.0001);
        let prev_px = if prev_day_price > 0.0 {
            prev_day_price
        } else {
            target_px * 0.985
        };
        let day_ret = (target_px / prev_px).ln().clamp(-0.35, 0.35);
        let per_bar_drift = day_ret / 24.0;

        let mut hasher = Sha256::new();
        hasher.update(format!("{symbol}:{timeframe}").as_bytes());
        let digest = hasher.finalize();
        let mut rng = u64::from_le_bytes(digest[0..8].try_into().unwrap_or([1; 8]));
        let mut next_unit = || -> f64 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (((rng >> 11) as f64) / ((1u64 << 53) as f64)) - 0.5
        };

        let mut prices = vec![target_px; count];
        for i in (0..count.saturating_sub(1)).rev() {
            let noise = next_unit() * 0.012;
            let step = (per_bar_drift + noise).clamp(-0.06, 0.06);
            prices[i] = (prices[i + 1] / (1.0 + step)).max(target_px * 0.25);
        }

        let base_bar_vol = (day_volume_usdc.max(50_000.0) / 24.0) / target_px;
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let ts = start_ms + (i as i64) * tf_ms;
            let close = prices[i];
            let open = if i > 0 { prices[i - 1] } else { close * 0.998 };
            let spread = close * (0.004 + next_unit().abs() * 0.008);
            let high = open.max(close) + spread;
            let low = (open.min(close) - spread).max(close * 0.5);
            let vol_mult = 0.75 + next_unit().abs() * 1.1;
            out.push(BarUpsertInput {
                asset_id,
                timeframe: timeframe.to_string(),
                timestamp_ms: ts,
                open: round_dp(open, 6),
                high: round_dp(high, 6),
                low: round_dp(low, 6),
                close: round_dp(close, 6),
                volume: round_dp(base_bar_vol * vol_mult, 4),
                funding_rate: funding_rate_hourly,
            });
        }
        out
    }

    /// Deterministic fallback perpetual universe used when running offline unit tests without network access.
    pub fn build_offline_fallback_assets() -> Vec<AssetUpsertInput> {
        let seeds = [
            (
                "BTC",
                0u32,
                5u32,
                40.0,
                92450.0,
                90800.0,
                0.000014,
                1_450_000_000.0,
                620_000_000.0,
            ),
            (
                "ETH",
                1,
                4,
                40.0,
                3180.0,
                3110.0,
                0.000012,
                820_000_000.0,
                340_000_000.0,
            ),
            (
                "SOL",
                2,
                2,
                20.0,
                184.25,
                177.40,
                0.000028,
                540_000_000.0,
                210_000_000.0,
            ),
            (
                "HYPE",
                3,
                2,
                20.0,
                28.40,
                26.90,
                0.000045,
                310_000_000.0,
                125_000_000.0,
            ),
            (
                "SUI",
                4,
                1,
                20.0,
                3.42,
                3.25,
                0.000022,
                180_000_000.0,
                74_000_000.0,
            ),
            (
                "AVAX",
                5,
                2,
                20.0,
                34.80,
                33.90,
                0.000011,
                120_000_000.0,
                52_000_000.0,
            ),
            (
                "LINK",
                6,
                1,
                20.0,
                18.65,
                18.10,
                0.000015,
                95_000_000.0,
                44_000_000.0,
            ),
            (
                "DOGE",
                7,
                0,
                20.0,
                0.365,
                0.382,
                -0.000018,
                260_000_000.0,
                88_000_000.0,
            ),
            (
                "XRP",
                8,
                0,
                20.0,
                2.45,
                2.38,
                0.000019,
                290_000_000.0,
                110_000_000.0,
            ),
            (
                "AAVE",
                9,
                2,
                20.0,
                285.0,
                274.0,
                0.000021,
                78_000_000.0,
                36_000_000.0,
            ),
            (
                "TAO",
                10,
                3,
                20.0,
                520.0,
                498.0,
                0.000032,
                68_000_000.0,
                29_000_000.0,
            ),
            (
                "INJ",
                11,
                1,
                20.0,
                24.50,
                25.10,
                -0.000014,
                52_000_000.0,
                22_000_000.0,
            ),
            (
                "TIA",
                12,
                1,
                20.0,
                5.40,
                5.65,
                -0.000025,
                48_000_000.0,
                19_000_000.0,
            ),
            (
                "ARB",
                13,
                1,
                20.0,
                0.92,
                0.95,
                -0.000012,
                62_000_000.0,
                27_000_000.0,
            ),
            (
                "OP",
                14,
                1,
                20.0,
                1.78,
                1.82,
                -0.000010,
                55_000_000.0,
                23_000_000.0,
            ),
            (
                "NEAR",
                15,
                1,
                20.0,
                5.85,
                5.70,
                0.000016,
                64_000_000.0,
                26_000_000.0,
            ),
            (
                "PENDLE",
                16,
                0,
                20.0,
                5.12,
                4.88,
                0.000029,
                42_000_000.0,
                18_000_000.0,
            ),
            (
                "RENDER",
                17,
                1,
                20.0,
                7.85,
                7.50,
                0.000024,
                46_000_000.0,
                20_000_000.0,
            ),
            (
                "JUP",
                18,
                0,
                20.0,
                1.12,
                1.08,
                0.000018,
                39_000_000.0,
                17_000_000.0,
            ),
            (
                "WIF",
                19,
                0,
                20.0,
                2.35,
                2.52,
                -0.000035,
                85_000_000.0,
                31_000_000.0,
            ),
        ];

        seeds
            .into_iter()
            .map(
                |(coin, idx, sz_dec, max_lev, mark, prev, funding, vol, oi)| {
                    let lot_step = 10_f64.powi(-(sz_dec as i32)).max(1e-6);
                    let tick = infer_tick_size(mark, sz_dec);
                    AssetUpsertInput {
                        raw_symbol_or_coin: coin.to_string(),
                        asset_index: idx,
                        sz_decimals: sz_dec,
                        tick_size: tick,
                        lot_step_size: lot_step,
                        max_leverage: max_lev,
                        mark_price: mark,
                        mid_price: mark,
                        oracle_price: mark,
                        funding_rate_hourly: funding,
                        day_volume_usdc: vol,
                        open_interest_usdc: oi,
                        prev_day_price: prev,
                        is_core_eligible: true,
                        is_event_eligible: true,
                        is_priority_watched: idx < 4,
                    }
                },
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_core::OrderSide;
    use aegis_db::ExchangeOpenPositionSyncInput;
    use aegis_test_support::mock_http::MockHttpServer;
    use serde_json::json;

    fn build_meta_and_ctxs(rows: &[(&str, f64, f64, f64, f64, f64)]) -> Value {
        let mut universe = Vec::with_capacity(rows.len());
        let mut ctxs = Vec::with_capacity(rows.len());
        for (name, mark_px, prev_day_px, funding, day_vol_usdc, oi_usdc) in rows {
            let oi_coins = if *mark_px > 0.0 {
                oi_usdc / mark_px
            } else {
                0.0
            };
            universe.push(json!({
                "name": name,
                "szDecimals": 2,
                "maxLeverage": 20,
            }));
            ctxs.push(json!({
                "markPx": format!("{mark_px}"),
                "midPx": format!("{mark_px}"),
                "oraclePx": format!("{mark_px}"),
                "prevDayPx": format!("{prev_day_px}"),
                "funding": format!("{funding}"),
                "dayNtlVlm": format!("{day_vol_usdc}"),
                "openInterest": format!("{oi_coins}"),
            }));
        }
        json!([{ "universe": universe }, ctxs])
    }

    #[test]
    fn test_testnet_liquidity_and_oi_eligibility_gating() {
        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("core_min_day_volume_usdc", "2000000.0");
        cfg.insert("event_min_day_volume_usdc", "500000.0");

        let payload = build_meta_and_ctxs(&[
            // Dead & thin Testnet coins must NEVER pass default $50k vol / $50k OI floors
            ("WIF", 2.24, 2.00, 0.0005, 79.0, 100_000.0),
            ("kSHIB", 0.02, 0.02, 0.00001, 1_750.0, 120_000.0),
            ("THINOLD", 1.0, 1.0, 0.00001, 5_000.0, 10_000.0),
            ("MIDVOL", 10.0, 10.0, 0.00001, 25_000.0, 60_000.0),
            // Active Testnet coins meeting default >= $50k vol & >= $50k OI
            ("BTC", 90_000.0, 89_000.0, 0.00001, 250_000.0, 1_000_000.0),
            ("INJ", 27.5, 25.0, 0.00002, 50_000.0, 50_000.0),
            // High volume but OI below Testnet $50k floor -> rejected
            ("LOWOI", 10.0, 10.0, 0.00001, 80_000.0, 45_000.0),
        ]);

        let parsed = MarketWatcher::parse_live_asset_inputs(&payload, &HashMap::new(), &cfg)
            .expect("parsed testnet inputs");
        let by_coin: HashMap<&str, &AssetUpsertInput> = parsed
            .iter()
            .map(|a| (a.raw_symbol_or_coin.as_str(), a))
            .collect();

        assert!(!by_coin["WIF"].is_core_eligible);
        assert!(!by_coin["WIF"].is_event_eligible);
        assert!(!by_coin["WIF"].is_priority_watched);

        assert!(!by_coin["kSHIB"].is_core_eligible);
        assert!(!by_coin["kSHIB"].is_event_eligible);

        assert!(!by_coin["THINOLD"].is_core_eligible);
        assert!(!by_coin["THINOLD"].is_event_eligible);

        assert!(!by_coin["MIDVOL"].is_core_eligible);
        assert!(!by_coin["MIDVOL"].is_event_eligible);

        assert!(by_coin["BTC"].is_core_eligible);
        assert!(by_coin["BTC"].is_event_eligible);

        assert!(by_coin["INJ"].is_core_eligible);
        assert!(by_coin["INJ"].is_event_eligible);
        assert!(by_coin["INJ"].is_priority_watched);

        assert!(!by_coin["LOWOI"].is_core_eligible);
        assert!(!by_coin["LOWOI"].is_event_eligible);

        // Explicit testnet_* keys must control Testnet thresholds without silent .min(5_000) clamping
        cfg.insert("testnet_core_min_day_volume_usdc", "100000.0");
        cfg.insert("testnet_event_min_day_volume_usdc", "60000.0");
        cfg.insert("testnet_core_min_open_interest_usdc", "100000.0");
        cfg.insert("testnet_event_min_open_interest_usdc", "60000.0");
        let custom_payload = build_meta_and_ctxs(&[
            ("COREOK", 10.0, 10.0, 0.0, 120_000.0, 120_000.0),
            ("EVOK", 10.0, 10.0, 0.0, 75_000.0, 75_000.0),
            ("BELOW", 10.0, 10.0, 0.0, 50_000.0, 50_000.0),
        ]);
        let custom = MarketWatcher::parse_live_asset_inputs(&custom_payload, &HashMap::new(), &cfg)
            .expect("parsed custom testnet inputs");
        let custom_map: HashMap<&str, &AssetUpsertInput> = custom
            .iter()
            .map(|a| (a.raw_symbol_or_coin.as_str(), a))
            .collect();
        assert!(custom_map["COREOK"].is_core_eligible);
        assert!(!custom_map["EVOK"].is_core_eligible);
        assert!(custom_map["EVOK"].is_event_eligible);
        assert!(!custom_map["BELOW"].is_core_eligible);
        assert!(!custom_map["BELOW"].is_event_eligible);
    }

    #[test]
    fn test_mainnet_liquidity_and_oi_eligibility_gating() {
        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "LIVE");
        cfg.insert("core_min_day_volume_usdc", "2000000.0");
        cfg.insert("event_min_day_volume_usdc", "500000.0");

        let payload = build_meta_and_ctxs(&[
            (
                "BTC",
                90_000.0,
                89_000.0,
                0.00001,
                500_000_000.0,
                200_000_000.0,
            ),
            ("ARB", 1.0, 1.0, 0.00001, 2_000_000.0, 250_000.0),
            ("INJ", 25.0, 25.0, 0.00001, 1_200_000.0, 150_000.0),
            // High volume, OI below Mainnet $100k Event floor -> rejected
            ("LOWOIMAIN", 5.0, 5.0, 0.00001, 5_000_000.0, 99_000.0),
            // Core volume, OI in [$100k, $250k) -> Event-only
            ("MIDMAIN", 5.0, 5.0, 0.00001, 3_000_000.0, 150_000.0),
            // Testnet-grade volume/OI must NOT qualify on Mainnet even in a tiny universe
            ("THINMAIN", 2.0, 2.0, 0.00001, 50_000.0, 50_000.0),
        ]);

        let parsed = MarketWatcher::parse_live_asset_inputs(&payload, &HashMap::new(), &cfg)
            .expect("parsed mainnet inputs");
        let by_coin: HashMap<&str, &AssetUpsertInput> = parsed
            .iter()
            .map(|a| (a.raw_symbol_or_coin.as_str(), a))
            .collect();

        assert!(by_coin["BTC"].is_core_eligible);
        assert!(by_coin["BTC"].is_event_eligible);

        assert!(by_coin["ARB"].is_core_eligible);
        assert!(by_coin["ARB"].is_event_eligible);

        assert!(!by_coin["INJ"].is_core_eligible);
        assert!(by_coin["INJ"].is_event_eligible);

        assert!(!by_coin["LOWOIMAIN"].is_core_eligible);
        assert!(!by_coin["LOWOIMAIN"].is_event_eligible);

        assert!(!by_coin["MIDMAIN"].is_core_eligible);
        assert!(by_coin["MIDMAIN"].is_event_eligible);

        assert!(!by_coin["THINMAIN"].is_core_eligible);
        assert!(!by_coin["THINMAIN"].is_event_eligible);
    }

    #[test]
    fn test_refresh_bars_preserves_active_held_ineligible_position() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();
        db.set_config_value("execution_mode", "PAPER", None)
            .unwrap();
        let watcher = MarketWatcher::new(db.clone(), HyperliquidHttpClient::new(5));

        // Insert two assets that have fallen below Core/Event eligibility and are not priority-watched
        let met_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "MET".to_string(),
                asset_index: 42,
                sz_decimals: 1,
                tick_size: 0.001,
                lot_step_size: 0.1,
                max_leverage: 10.0,
                mark_price: 1.25,
                mid_price: 1.25,
                oracle_price: 1.25,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 800.0,
                open_interest_usdc: 2_000.0,
                prev_day_price: 1.24,
                is_core_eligible: false,
                is_event_eligible: false,
                is_priority_watched: false,
            })
            .unwrap();
        let wif_id = db
            .upsert_exchange_asset(&AssetUpsertInput {
                raw_symbol_or_coin: "WIF".to_string(),
                asset_index: 43,
                sz_decimals: 0,
                tick_size: 0.001,
                lot_step_size: 1.0,
                max_leverage: 10.0,
                mark_price: 2.0,
                mid_price: 2.0,
                oracle_price: 2.0,
                funding_rate_hourly: 0.0,
                day_volume_usdc: 79.0,
                open_interest_usdc: 100_000.0,
                prev_day_price: 2.0,
                is_core_eligible: false,
                is_event_eligible: false,
                is_priority_watched: false,
            })
            .unwrap();

        // Hold an open position on MET so protective stops/closes still require fresh ATR bars
        db.sync_open_position_from_exchange_v2(&ExchangeOpenPositionSyncInput {
            asset_id: met_id,
            coin: "MET",
            side: OrderSide::Long,
            entry_price: 1.20,
            mark_price: 1.25,
            quantity: 50.0,
            committed_notional_usdc: 62.5,
            unrealized_pnl_usdc: 0.0,
            portfolio_weight: 0.06,
            protective_stop_px: 1.15,
            take_profit_px: 1.35,
            funding_accrued_usdc: 0.0,
            opened_at_ms: Some(now_ms() - 600_000),
        })
        .unwrap();

        let cfg = db.load_config_or_default();
        let refreshed = watcher
            .refresh_bars_for_eligible_assets(4, 48, false, &cfg)
            .unwrap();
        assert_eq!(refreshed, 1);
        assert!(db.get_ohlcv_bars(met_id, "1h", 72).unwrap().len() >= 24);
        assert!(db.get_ohlcv_bars(met_id, "4h", 72).unwrap().len() >= 24);
        assert!(db.get_ohlcv_bars(wif_id, "1h", 72).unwrap().is_empty());
    }

    #[test]
    fn test_fetch_live_asset_inputs_skips_all_mids_when_mid_px_present_and_falls_back_when_absent()
    {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();

        // Case 1: metaAndAssetCtxs includes midPx -> allMids MUST NOT be called
        let server_with_mids = MockHttpServer::start_json(|_, body| {
            let req_type = body.get("type").and_then(Value::as_str).unwrap_or("");
            if req_type == "metaAndAssetCtxs" {
                (
                    200,
                    build_meta_and_ctxs(&[(
                        "BTC",
                        90_000.0,
                        89_000.0,
                        0.0,
                        500_000_000.0,
                        200_000_000.0,
                    )]),
                )
            } else {
                (200, json!({ "BTC": "90001.0" }))
            }
        });
        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "LIVE");
        cfg.insert("hyperliquid_mainnet_url", server_with_mids.base_url());
        cfg.insert("hyperliquid_info_url", server_with_mids.url("/info"));
        cfg.insert(
            "hyperliquid_exchange_url",
            server_with_mids.url("/exchange"),
        );

        let watcher = MarketWatcher::new(db.clone(), HyperliquidHttpClient::new(2));
        let parsed = watcher.fetch_live_asset_inputs(&cfg).expect("parsed");
        assert_eq!(parsed.len(), 1);
        let types_called: Vec<String> = server_with_mids
            .requests()
            .into_iter()
            .filter_map(|r| {
                r.json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        assert_eq!(types_called, vec!["metaAndAssetCtxs"]);

        // Case 2: metaAndAssetCtxs omits midPx -> falls back to allMids
        let server_no_mids = MockHttpServer::start_json(|_, body| {
            let req_type = body.get("type").and_then(Value::as_str).unwrap_or("");
            if req_type == "metaAndAssetCtxs" {
                (
                    200,
                    json!([
                        { "universe": [{ "name": "BTC", "szDecimals": 5, "maxLeverage": 40 }] },
                        [{
                            "markPx": "90000.0",
                            "oraclePx": "90000.0",
                            "prevDayPx": "89000.0",
                            "funding": "0.0",
                            "dayNtlVlm": "500000000.0",
                            "openInterest": "2000.0"
                        }]
                    ]),
                )
            } else {
                (200, json!({ "BTC": "90050.0" }))
            }
        });
        cfg.insert("hyperliquid_mainnet_url", server_no_mids.base_url());
        cfg.insert("hyperliquid_info_url", server_no_mids.url("/info"));
        cfg.insert("hyperliquid_exchange_url", server_no_mids.url("/exchange"));
        let parsed_fb = watcher
            .fetch_live_asset_inputs(&cfg)
            .expect("parsed fallback");
        assert!((parsed_fb[0].mid_price - 90_050.0).abs() < 1e-6);
        let fb_types: Vec<String> = server_no_mids
            .requests()
            .into_iter()
            .filter_map(|r| {
                r.json_body
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        assert_eq!(fb_types, vec!["metaAndAssetCtxs", "allMids"]);
    }

    #[test]
    fn test_split_candle_ttls_boundaries_incremental_lookback_and_refresh_cap() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();

        // Seed 7 eligible assets (plus 1 active & 1 priority-watched) with candle_refresh_max_assets=5
        // Verify active + priority + top-5 by volume are in refresh set, while lowest-volume 2 are skipped.
        let mut asset_ids = Vec::new();
        for i in 0..8 {
            let vol = (i as f64 + 1.0) * 1_000_000.0;
            let id = db
                .upsert_exchange_asset(&AssetUpsertInput {
                    raw_symbol_or_coin: format!("C{i}"),
                    asset_index: i as u32,
                    sz_decimals: 2,
                    tick_size: 0.01,
                    lot_step_size: 0.01,
                    max_leverage: 20.0,
                    mark_price: 100.0,
                    mid_price: 100.0,
                    oracle_price: 100.0,
                    funding_rate_hourly: 0.0,
                    day_volume_usdc: vol,
                    open_interest_usdc: 500_000.0,
                    prev_day_price: 99.0,
                    is_core_eligible: true,
                    is_event_eligible: true,
                    is_priority_watched: i == 0,
                })
                .unwrap();
            asset_ids.push(id);
        }
        // Seed 48 existing 1h bars for C7 so incremental lookback triggers (`latest - 2 * 3_600_000`)
        let latest_1h_ts = 1_700_000_000_000i64 - (1_700_000_000_000i64 % 3_600_000);
        let seed_bars: Vec<BarUpsertInput> = (0..48)
            .map(|idx| BarUpsertInput {
                asset_id: asset_ids[7],
                timeframe: "1h".to_string(),
                timestamp_ms: latest_1h_ts - ((47 - idx) as i64) * 3_600_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 1000.0,
                funding_rate: 0.0,
            })
            .collect();
        db.upsert_ohlcv_bars(&seed_bars).unwrap();

        let server = MockHttpServer::start_json(|_, body| {
            let interval = body
                .pointer("/req/interval")
                .and_then(Value::as_str)
                .unwrap_or("1h");
            let tf_ms = if interval == "4h" {
                14_400_000
            } else {
                3_600_000
            };
            let t0 = now_ms() - tf_ms;
            (
                200,
                json!([
                    { "t": t0, "o": "100", "h": "101", "l": "99", "c": "100.5", "v": "500" }
                ]),
            )
        });

        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "LIVE");
        cfg.insert("hyperliquid_info_url", server.url("/info"));
        cfg.insert("candle_refresh_max_assets", "5");

        let watcher = MarketWatcher::new(db, HyperliquidHttpClient::new(2));

        // Base time aligned to 4h boundary + 20s
        let base_4h = 1_700_006_400_000i64; // divisible by 14_400_000
        assert_eq!(base_4h % 14_400_000, 0);
        let t0 = base_4h + 20_000;

        // Tick 1: max_assets_per_tick=2 refreshes C0 (priority) + C7 (highest volume)
        let n1 = watcher
            .refresh_bars_for_eligible_assets_at(t0, 2, 48, true, &cfg)
            .unwrap();
        assert_eq!(n1, 2);

        let reqs_after_t0 = server.requests();
        let c7_1h_req = reqs_after_t0
            .iter()
            .find(|r| {
                r.json_body.pointer("/req/coin").and_then(Value::as_str) == Some("C7")
                    && r.json_body.pointer("/req/interval").and_then(Value::as_str) == Some("1h")
            })
            .expect("C7 1h candle request");
        assert_eq!(
            c7_1h_req
                .json_body
                .pointer("/req/startTime")
                .and_then(Value::as_i64),
            Some(latest_1h_ts - 2 * 3_600_000)
        );

        server.clear_requests();
        // Refresh remaining 4 assets in the cap (C6, C5, C4, C3) -> C1 & C2 excluded (6 total: C0 priority + top 5 C7..C3)
        let n_rest = watcher
            .refresh_bars_for_eligible_assets_at(t0 + 1_000, 10, 48, true, &cfg)
            .unwrap();
        assert_eq!(n_rest, 4);

        server.clear_requests();
        // At t0 + 100s (< 300s 1h TTL, < next hour boundary): zero due
        assert_eq!(
            watcher
                .refresh_bars_for_eligible_assets_at(t0 + 100_000, 10, 48, true, &cfg)
                .unwrap(),
            0
        );

        // At t0 + 305s (>= 300s 1h TTL, < 900s 4h TTL): ONLY 1h candles refresh (6 requests, 0 4h requests)
        assert_eq!(
            watcher
                .refresh_bars_for_eligible_assets_at(t0 + 305_000, 10, 48, true, &cfg)
                .unwrap(),
            6
        );
        let intervals_305: Vec<String> = server
            .requests()
            .into_iter()
            .filter_map(|r| {
                r.json_body
                    .pointer("/req/interval")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        assert_eq!(intervals_305.len(), 6);
        assert!(intervals_305.iter().all(|iv| iv == "1h"));

        server.clear_requests();
        // At base_4h + 3_600_000 + 20_000 (1h boundary + 20s, last 4h synced at base_4h + 20s -> > 900s AND 1h boundary crossed):
        // Verify post-hour boundary trigger refreshes 1h even if last 1h was 100s ago!
        let pre_boundary = base_4h + 3_500_000;
        let _ = watcher
            .refresh_bars_for_eligible_assets_at(pre_boundary, 10, 48, true, &cfg)
            .unwrap();
        server.clear_requests();
        let post_1h_boundary = base_4h + 3_600_000 + 20_000;
        assert_eq!(
            watcher
                .refresh_bars_for_eligible_assets_at(post_1h_boundary, 10, 48, true, &cfg)
                .unwrap(),
            6
        );
    }
}
