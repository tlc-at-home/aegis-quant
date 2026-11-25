//! Hyperliquid Mainnet Public Historical Candle & Funding Fetcher (`aegis-execution::mainnet_history`).
//!
//! Fetches public `metaAndAssetCtxs`, `candleSnapshot`, and paginated `fundingHistory`
//! directly from Hyperliquid Mainnet (`is_testnet = false`) under the host-keyed
//! [`RestWeightGovernor`] using blocking backfill pacing (`RequestOptions::historical_backfill()`).

use crate::http_client::{HyperliquidHttpClient, RequestOptions};
use crate::market_watcher::MarketWatcher;
use aegis_core::indicators::timeframe_to_ms;
use aegis_core::{
    normalize_hyperliquid_symbol, now_ms, parse_hl_f64, parse_hl_f64_opt, parse_hl_i64_opt,
    AegisError, ConfigMap, ExchangeAsset, OhlcvBar, Result,
};
use aegis_db::AssetUpsertInput;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Options controlling Hyperliquid Mainnet historical dataset fetching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MainnetHistoryFetchOptions {
    pub days: usize,
    pub timeframe: String,
    pub warmup_bars: usize,
    pub max_assets: usize,
}

impl Default for MainnetHistoryFetchOptions {
    fn default() -> Self {
        Self {
            days: 7,
            timeframe: "4h".to_string(),
            warmup_bars: 72,
            max_assets: 30,
        }
    }
}

/// Raw Mainnet historical universe, completed candles, and hourly funding events prior to causal assembly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MainnetRawHistory {
    pub fetched_at_ms: i64,
    pub until_ms: i64,
    pub eval_start_ms: i64,
    pub fetch_since_ms: i64,
    pub timeframe: String,
    pub total_exchange_perps: usize,
    pub total_core_eligible: usize,
    pub total_event_eligible: usize,
    pub assets: Vec<ExchangeAsset>,
    pub bars_by_asset: HashMap<i64, Vec<OhlcvBar>>,
    pub funding_events: HashMap<i64, Vec<(i64, f64)>>,
}

/// Fetches Hyperliquid Mainnet public market metadata, completed OHLCV candles, and funding history.
pub fn fetch_mainnet_raw_history(
    http: &HyperliquidHttpClient,
    cfg: &ConfigMap,
    opts: &MainnetHistoryFetchOptions,
) -> Result<MainnetRawHistory> {
    fetch_mainnet_raw_history_at(http, cfg, opts, now_ms())
}

/// Timestamp-parameterized variant of [`fetch_mainnet_raw_history`] for deterministic testing.
pub fn fetch_mainnet_raw_history_at(
    http: &HyperliquidHttpClient,
    cfg: &ConfigMap,
    opts: &MainnetHistoryFetchOptions,
    now: i64,
) -> Result<MainnetRawHistory> {
    let tf = opts.timeframe.trim();
    if !matches!(tf, "1h" | "4h") {
        return Err(AegisError::Validation(format!(
            "Unsupported Mainnet backtest timeframe '{tf}' (expected '1h' or '4h')"
        )));
    }
    let bar_ms = timeframe_to_ms(tf)
        .ok_or_else(|| AegisError::Validation(format!("Unsupported timeframe '{tf}'")))?;

    let until_ms = (now / bar_ms) * bar_ms;
    let eval_days = opts.days.max(5);
    let eval_start_ms = until_ms.saturating_sub((eval_days as i64) * 86_400_000);
    let warmup = opts.warmup_bars.clamp(24, 500);
    let fetch_since_ms = eval_start_ms.saturating_sub((warmup as i64) * bar_ms);

    let mut mainnet_cfg = cfg.clone();
    mainnet_cfg.insert("execution_mode", "LIVE");

    let backfill_opts = RequestOptions::historical_backfill();
    let meta_ctxs =
        http.fetch_meta_and_asset_ctxs_for_network(&mainnet_cfg, false, backfill_opts)?;
    let parsed = MarketWatcher::parse_live_asset_inputs(&meta_ctxs, &HashMap::new(), &mainnet_cfg)
        .ok_or_else(|| {
            AegisError::Execution(
                "Failed to parse Hyperliquid Mainnet metaAndAssetCtxs universe".to_string(),
            )
        })?;

    let total_exchange_perps = parsed.len();
    let total_core_eligible = parsed.iter().filter(|a| a.is_core_eligible).count();
    let total_event_eligible = parsed.iter().filter(|a| a.is_event_eligible).count();

    let mut eligible: Vec<AssetUpsertInput> = parsed
        .into_iter()
        .filter(|a| a.is_core_eligible || a.is_event_eligible)
        .collect();
    eligible.sort_by(|a, b| b.day_volume_usdc.total_cmp(&a.day_volume_usdc));

    let max_assets = opts.max_assets.clamp(1, 150);
    let btc_pos = eligible.iter().position(|a| {
        normalize_hyperliquid_symbol(&a.raw_symbol_or_coin)
            .map(|(_, c)| c.eq_ignore_ascii_case("BTC"))
            .unwrap_or(false)
    });
    if eligible.len() > max_assets {
        if let Some(pos) = btc_pos {
            if pos >= max_assets {
                let btc_item = eligible.remove(pos);
                eligible.truncate(max_assets.saturating_sub(1));
                eligible.push(btc_item);
            } else {
                eligible.truncate(max_assets);
            }
        } else {
            eligible.truncate(max_assets);
        }
    }

    if eligible.is_empty() {
        return Err(AegisError::Validation(
            "Zero eligible Hyperliquid Mainnet assets found under Mainnet liquidity thresholds"
                .to_string(),
        ));
    }

    let mut assets: Vec<ExchangeAsset> = Vec::with_capacity(eligible.len());
    let mut bars_by_asset: HashMap<i64, Vec<OhlcvBar>> = HashMap::new();
    let mut funding_events: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();

    for (idx, input) in eligible.into_iter().enumerate() {
        let asset_id = (idx + 1) as i64;
        let Ok((symbol, coin)) = normalize_hyperliquid_symbol(&input.raw_symbol_or_coin) else {
            continue;
        };
        let asset = ExchangeAsset {
            id: asset_id,
            symbol,
            coin: coin.clone(),
            asset_index: input.asset_index,
            sz_decimals: input.sz_decimals,
            tick_size: input.tick_size,
            lot_step_size: input.lot_step_size,
            max_leverage: input.max_leverage,
            mark_price: input.mark_price,
            mid_price: input.mid_price,
            oracle_price: input.oracle_price,
            funding_rate_hourly: input.funding_rate_hourly,
            day_volume_usdc: input.day_volume_usdc,
            open_interest_usdc: input.open_interest_usdc,
            prev_day_price: input.prev_day_price,
            is_core_eligible: input.is_core_eligible,
            is_event_eligible: input.is_event_eligible,
            is_priority_watched: input.is_priority_watched,
            created_at_ms: until_ms,
            updated_at_ms: until_ms,
        };

        let raw_candles = match http.fetch_candle_snapshot_for_network(
            &coin,
            tf,
            fetch_since_ms,
            until_ms.saturating_sub(1),
            &mainnet_cfg,
            false,
            backfill_opts,
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    coin = %coin,
                    timeframe = tf,
                    error = %e,
                    "Skipping Mainnet asset after candleSnapshot fetch error"
                );
                continue;
            }
        };

        let mut parsed_bars: Vec<OhlcvBar> = Vec::with_capacity(raw_candles.len());
        for item in &raw_candles {
            let t = parse_hl_i64_opt(item.get("t")).unwrap_or(0);
            let o = parse_hl_f64(item.get("o"));
            let h = parse_hl_f64(item.get("h"));
            let l = parse_hl_f64(item.get("l"));
            let c = parse_hl_f64(item.get("c"));
            let v = parse_hl_f64(item.get("v"));
            if t >= fetch_since_ms
                && t.saturating_add(bar_ms) <= until_ms
                && c.is_finite()
                && c > 0.0
            {
                let open = if o.is_finite() && o > 0.0 { o } else { c };
                let high = if h.is_finite() && h > 0.0 {
                    h.max(open).max(c)
                } else {
                    open.max(c)
                };
                let low = if l.is_finite() && l > 0.0 {
                    l.min(open).min(c)
                } else {
                    open.min(c)
                };
                parsed_bars.push(OhlcvBar {
                    id: 0,
                    asset_id,
                    timeframe: tf.to_string(),
                    timestamp_ms: t,
                    open,
                    high,
                    low,
                    close: c,
                    volume: if v.is_finite() { v.max(0.0) } else { 0.0 },
                    funding_rate: 0.0,
                    created_at_ms: t,
                    updated_at_ms: t,
                });
            }
        }
        parsed_bars.sort_by_key(|b| b.timestamp_ms);
        parsed_bars.dedup_by_key(|b| b.timestamp_ms);
        for (b_idx, bar) in parsed_bars.iter_mut().enumerate() {
            bar.id = (b_idx + 1) as i64;
        }
        if parsed_bars.is_empty() {
            tracing::warn!(
                coin = %coin,
                timeframe = tf,
                "Skipping Mainnet asset with zero completed candles in requested window"
            );
            continue;
        }

        let mut coin_funding: Vec<(i64, f64)> = Vec::new();
        match http.fetch_funding_history_for_network(
            &coin,
            fetch_since_ms,
            Some(until_ms),
            &mainnet_cfg,
            false,
            backfill_opts,
        ) {
            Ok(raw_funding) => {
                for ev in &raw_funding {
                    if let (Some(t), Some(fr)) = (
                        parse_hl_i64_opt(ev.get("time")),
                        parse_hl_f64_opt(ev.get("fundingRate")),
                    ) {
                        if t >= fetch_since_ms && t < until_ms && fr.is_finite() {
                            coin_funding.push((t, fr));
                        }
                    }
                }
                coin_funding.sort_by_key(|&(t, _)| t);
            }
            Err(e) => {
                tracing::warn!(
                    coin = %coin,
                    error = %e,
                    "Failed fetching fundingHistory for Mainnet asset; continuing with zero funding"
                );
            }
        }

        assets.push(asset);
        bars_by_asset.insert(asset_id, parsed_bars);
        if !coin_funding.is_empty() {
            funding_events.insert(asset_id, coin_funding);
        }
    }

    if bars_by_asset.is_empty() {
        return Err(AegisError::Execution(format!(
            "Failed to fetch any completed Mainnet candles for timeframe '{tf}'"
        )));
    }

    Ok(MainnetRawHistory {
        fetched_at_ms: now,
        until_ms,
        eval_start_ms,
        fetch_since_ms,
        timeframe: tf.to_string(),
        total_exchange_perps,
        total_core_eligible,
        total_event_eligible,
        assets,
        bars_by_asset,
        funding_events,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_test_support::mock_http::MockHttpServer;
    use serde_json::{json, Value};

    #[test]
    fn test_fetch_mainnet_raw_history_applies_mainnet_thresholds_and_excludes_forming_candle() {
        let bar_ms = 14_400_000i64;
        let now_ts = 1_750_000_000_000i64 + 1_800_000; // 30m into forming candle
        let until_ms = (now_ts / bar_ms) * bar_ms;
        let fetch_since_ms = until_ms - (7 * 86_400_000 + 24 * bar_ms);

        let srv = MockHttpServer::start_json(move |_path, body| {
            let typ = body.get("type").and_then(Value::as_str).unwrap_or("");
            match typ {
                "metaAndAssetCtxs" => (
                    200,
                    json!([
                        {
                            "universe": [
                                { "name": "BTC", "szDecimals": 5, "maxLeverage": 40 },
                                { "name": "TINY", "szDecimals": 2, "maxLeverage": 10 }
                            ]
                        },
                        [
                            {
                                "markPx": "60000.0",
                                "midPx": "60000.0",
                                "oraclePx": "60000.0",
                                "prevDayPx": "59500.0",
                                "funding": "0.0000125",
                                "dayNtlVlm": "50000000.0",
                                "openInterest": "1000.0"
                            },
                            {
                                "markPx": "1.0",
                                "midPx": "1.0",
                                "oraclePx": "1.0",
                                "prevDayPx": "1.0",
                                "funding": "0.00001",
                                "dayNtlVlm": "100000.0",
                                "openInterest": "60000.0"
                            }
                        ]
                    ]),
                ),
                "candleSnapshot" => {
                    let mut candles = Vec::new();
                    let total_completed = (until_ms - fetch_since_ms) / bar_ms;
                    for i in 0..total_completed {
                        let t = fetch_since_ms + i * bar_ms;
                        candles.push(json!({
                            "t": t,
                            "T": t + bar_ms - 1,
                            "s": "BTC",
                            "i": "4h",
                            "o": "60000.0",
                            "h": "60200.0",
                            "l": "59800.0",
                            "c": "60100.0",
                            "v": "500.0"
                        }));
                    }
                    // Include a still-forming candle at t == until_ms which MUST be filtered out
                    candles.push(json!({
                        "t": until_ms,
                        "T": until_ms + bar_ms - 1,
                        "s": "BTC",
                        "i": "4h",
                        "o": "60100.0",
                        "h": "69999.0",
                        "l": "59999.0",
                        "c": "69999.0",
                        "v": "10.0"
                    }));
                    (200, Value::Array(candles))
                }
                "fundingHistory" => (
                    200,
                    json!([
                        {
                            "coin": "BTC",
                            "fundingRate": "0.000025",
                            "time": fetch_since_ms + 3_600_000
                        },
                        {
                            "coin": "BTC",
                            "fundingRate": "0.000999",
                            "time": until_ms
                        }
                    ]),
                ),
                _ => (404, json!({})),
            }
        });

        let mut cfg = ConfigMap::from_defaults();
        // Even with TESTNET configured, mainnet history fetch enforces Mainnet $2M/$500k thresholds
        cfg.insert("execution_mode", "TESTNET");
        cfg.insert("hyperliquid_mainnet_url", srv.base_url());
        cfg.insert("hyperliquid_allowed_hosts", "127.0.0.1");

        let client = HyperliquidHttpClient::new(5);
        let opts = MainnetHistoryFetchOptions {
            days: 7,
            timeframe: "4h".to_string(),
            warmup_bars: 24,
            max_assets: 10,
        };
        let raw = fetch_mainnet_raw_history_at(&client, &cfg, &opts, now_ts)
            .expect("mainnet raw history succeeds");

        assert_eq!(raw.total_exchange_perps, 2);
        assert_eq!(raw.total_core_eligible, 1);
        assert_eq!(raw.total_event_eligible, 1);
        // TINY ($100k vol) passes Testnet ($50k) but MUST be excluded by Mainnet thresholds ($500k)
        assert_eq!(raw.assets.len(), 1);
        assert_eq!(raw.assets[0].coin, "BTC");
        assert_eq!(raw.until_ms, until_ms);

        let btc_bars = raw.bars_by_asset.get(&raw.assets[0].id).unwrap();
        assert_eq!(btc_bars.len(), 7 * 6 + 24);
        assert!(btc_bars.iter().all(|b| b.timestamp_ms < until_ms));
        assert!((btc_bars.last().unwrap().close - 60100.0).abs() < 1e-6);

        let btc_funding = raw.funding_events.get(&raw.assets[0].id).unwrap();
        assert_eq!(btc_funding.len(), 1);
        assert!((btc_funding[0].1 - 0.000025).abs() < 1e-9);

        // Verify warmup_bars > 240 (up to 500) is honored and upper-clamped at 500
        let bar_ms_1h = 3_600_000i64;
        let until_1h = (now_ts / bar_ms_1h) * bar_ms_1h;
        let eval_start_1h = until_1h - 5 * 86_400_000;
        let clamped_opts = MainnetHistoryFetchOptions {
            days: 5,
            timeframe: "1h".to_string(),
            warmup_bars: 600,
            max_assets: 10,
        };
        let raw_1h = fetch_mainnet_raw_history_at(&client, &cfg, &clamped_opts, now_ts)
            .expect("mainnet raw history with 600 warmup_bars clamps to 500");
        assert_eq!(raw_1h.eval_start_ms, eval_start_1h);
        assert_eq!(raw_1h.fetch_since_ms, eval_start_1h - 500 * bar_ms_1h);
    }
}
