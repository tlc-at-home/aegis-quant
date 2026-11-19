//! 3-Second Full-Exchange Scanner (`aegis-execution::market_watcher`).
//!
//! Polls Hyperliquid `metaAndAssetCtxs` + `allMids` across all 150+ perpetuals,
//! normalizes all symbols to canonical `"<COIN>-USDC"`, updates `exchange_assets`,
//! expires finished cooldowns in `asset_cooldowns`, and refreshes 1h/4h OHLCV bars.

use crate::http_client::{is_testnet_configured, HyperliquidHttpClient};
use crate::signing::infer_tick_size;
use aegis_core::{
    normalize_hyperliquid_symbol, now_ms, round_dp, ConfigMap, ExchangeAsset, Result,
};
use aegis_db::{AssetUpsertInput, BarUpsertInput, DatabaseManager};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Canonical majors always treated as Core/Event eligible on Testnet when volume data is sparse.
const CANONICAL_CORE_MAJORS: &[&str] = &[
    "BTC", "ETH", "SOL", "HYPE", "SUI", "AVAX", "LINK", "DOGE", "XRP", "ADA", "BNB", "ARB", "OP",
    "INJ", "TIA", "NEAR", "AAVE", "TAO", "RENDER", "JUP", "WIF", "PENDLE", "LTC", "DOT", "ATOM",
];

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
    /// 2. Normalizes symbols to `"<COIN>-USDC"` and ranks by 24h volume to assign `is_core_eligible` / `is_event_eligible`.
    /// 3. Upserts all assets into MariaDB `exchange_assets`.
    /// 4. Expires finished cooldowns in `asset_cooldowns`.
    /// 5. Incrementally refreshes 1h and 4h OHLCV bars for core/priority assets.
    pub fn run_3s_exchange_scan(&self) -> Result<MarketScanReport> {
        let cfg = self.db.load_config_or_default();
        let ts = now_ms();

        let expired_cooldowns = self.db.expire_finished_cooldowns().unwrap_or(0);

        let (raw_inputs, live_connected) = match self.fetch_live_asset_inputs(&cfg) {
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

        let mut total_upserted = 0usize;
        let mut core_count = 0usize;
        let mut event_count = 0usize;
        let mut prio_count = 0usize;
        let mut upserted_prio_symbols: HashSet<String> = HashSet::new();

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
                    if let Ok((sym, _)) = normalize_hyperliquid_symbol(&input.raw_symbol_or_coin) {
                        upserted_prio_symbols.insert(sym);
                    }
                }
            }
        }

        // Clear stale `is_priority_watched` flags on assets that are no longer held, ordered, or extreme-dislocated
        if let Ok(existing) = self.db.list_exchange_assets() {
            for a in &existing {
                if !a.is_priority_watched || active_asset_ids.contains(&a.id) {
                    continue;
                }
                if live_connected {
                    if !upserted_prio_symbols.contains(&a.symbol) {
                        let _ = self.db.set_asset_priority_watch(a.id, false);
                    }
                } else if raw_inputs.is_empty() {
                    let day_chg = if a.prev_day_price > 0.0 {
                        ((a.mark_price - a.prev_day_price) / a.prev_day_price).abs()
                    } else {
                        0.0
                    };
                    if a.funding_rate_hourly.abs() < 0.0004 && day_chg < 0.08 {
                        let _ = self.db.set_asset_priority_watch(a.id, false);
                    }
                }
            }
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
            .refresh_bars_for_eligible_assets(6, 72, live_connected, &cfg)
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

    /// Parses live `metaAndAssetCtxs` + `allMids` from Hyperliquid into normalized [`AssetUpsertInput`] records.
    pub fn fetch_live_asset_inputs(&self, cfg: &ConfigMap) -> Option<Vec<AssetUpsertInput>> {
        let meta_ctxs = self.http.fetch_meta_and_asset_ctxs(cfg).ok()?;
        let arr = meta_ctxs.as_array()?;
        if arr.len() < 2 {
            return None;
        }
        let universe = arr[0].get("universe")?.as_array()?;
        let ctxs = arr[1].as_array()?;
        let mids = self.http.fetch_all_mids(cfg).unwrap_or_default();

        let parse_f64 = |v: Option<&Value>| -> f64 {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|val| val.is_finite())
            .unwrap_or(0.0)
        };

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

            let mark_px = parse_f64(c.get("markPx"));
            let ctx_mid_px = parse_f64(c.get("midPx"));
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
                let o = parse_f64(c.get("oraclePx"));
                if o > 0.0 {
                    o
                } else {
                    eff_mark
                }
            };
            let prev_day_px = {
                let p = parse_f64(c.get("prevDayPx"));
                if p > 0.0 {
                    p
                } else {
                    eff_mark
                }
            };
            let funding_hourly = parse_f64(c.get("funding"));
            let day_vol_usdc = parse_f64(c.get("dayNtlVlm")).max(0.0);
            let oi_coins = parse_f64(c.get("openInterest")).max(0.0);
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

        // Rank by 24h USDC volume descending to determine top-volume eligibility tiers
        let mut vols: Vec<(String, f64)> = parsed
            .iter()
            .map(|p| (p.coin.clone(), p.day_volume_usdc))
            .collect();
        vols.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let top_core_coins: HashSet<String> =
            vols.iter().take(30).map(|(c, _)| c.clone()).collect();
        let top_event_coins: HashSet<String> =
            vols.iter().take(65).map(|(c, _)| c.clone()).collect();

        let is_testnet = is_testnet_configured(cfg);
        let core_min_vol = if is_testnet {
            cfg.get_f64("core_min_day_volume_usdc", 2_000_000.0)
                .min(1_000.0)
        } else {
            cfg.get_f64("core_min_day_volume_usdc", 2_000_000.0)
        };
        let event_min_vol = if is_testnet {
            cfg.get_f64("event_min_day_volume_usdc", 500_000.0)
                .min(250.0)
        } else {
            cfg.get_f64("event_min_day_volume_usdc", 500_000.0)
        };

        let mut inputs = Vec::with_capacity(parsed.len());
        for p in parsed {
            let lot_step_size = 10_f64.powi(-(p.sz_decimals as i32)).max(1e-6);
            let tick_size = infer_tick_size(p.mark_price, p.sz_decimals);
            let is_major = CANONICAL_CORE_MAJORS.contains(&p.coin.as_str());
            let is_core = p.day_volume_usdc >= core_min_vol
                || top_core_coins.contains(&p.coin)
                || (is_testnet && is_major);
            let is_event =
                is_core || p.day_volume_usdc >= event_min_vol || top_event_coins.contains(&p.coin);

            // Flag extreme funding dislocation or >8% intraday move for 1s priority surveillance
            let day_change_pct = if p.prev_day_price > 0.0 {
                ((p.mark_price - p.prev_day_price) / p.prev_day_price).abs()
            } else {
                0.0
            };
            let is_prio = p.funding_rate_hourly.abs() >= 0.0004 || day_change_pct >= 0.08;

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

    /// Refreshes 1h and 4h OHLCV bars for up to `max_assets_per_tick` core/event/priority eligible assets.
    /// When `seed_all_eligible` is needed at cutover, pass a large `max_assets_per_tick` (e.g. `40`).
    pub fn refresh_bars_for_eligible_assets(
        &self,
        max_assets_per_tick: usize,
        lookback_bars: usize,
        live_connected: bool,
        cfg: &ConfigMap,
    ) -> Result<usize> {
        let assets = self.db.list_exchange_assets()?;
        if assets.is_empty() {
            return Ok(0);
        }
        let now = now_ms();
        let refresh_ttl_ms = 60_000i64; // 60s bar refresh cadence per asset

        let mut candidates: Vec<ExchangeAsset> = assets
            .into_iter()
            .filter(|a| a.is_core_eligible || a.is_priority_watched || a.is_event_eligible)
            .collect();
        {
            let guard = self.last_bar_sync_ms.lock();
            candidates.sort_by_key(|a| guard.get(&(a.id, "1h".to_string())).copied().unwrap_or(0));
        }

        let mut refreshed_assets = 0usize;
        for asset in candidates {
            if refreshed_assets >= max_assets_per_tick {
                break;
            }
            let needs_1h = {
                let guard = self.last_bar_sync_ms.lock();
                guard
                    .get(&(asset.id, "1h".to_string()))
                    .map(|&last| now - last >= refresh_ttl_ms)
                    .unwrap_or(true)
            };
            if !needs_1h {
                continue;
            }

            for tf in ["1h", "4h"] {
                let bars = self.fetch_or_synthesize_bars(
                    &asset,
                    tf,
                    lookback_bars.max(48),
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
        let start_ms = now - (lookback_bars as i64 + 6) * tf_ms;

        let is_live_mode = cfg.execution_mode().is_live_or_testnet()
            && (!self.db.is_ephemeral_test_db() || crate::http_client::endpoints_are_loopback(cfg));

        if live_connected {
            if let Ok(raw_list) =
                self.http
                    .fetch_candle_snapshot(&asset.coin, timeframe, start_ms, now, cfg)
            {
                let parse_f = |v: Option<&Value>| -> f64 {
                    v.and_then(|x| {
                        x.as_f64()
                            .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .filter(|val| val.is_finite())
                    .unwrap_or(0.0)
                };
                let mut live_bars = Vec::with_capacity(raw_list.len());
                for item in &raw_list {
                    let t = item.get("t").and_then(Value::as_i64).unwrap_or(0);
                    let o = parse_f(item.get("o"));
                    let h = parse_f(item.get("h"));
                    let l = parse_f(item.get("l"));
                    let c = parse_f(item.get("c"));
                    let v = parse_f(item.get("v"));
                    let f = parse_f(item.get("f"));
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
