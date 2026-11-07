//! Market Data Ingestion & `ALT/BTC:SYNTH` Synthesis Engine (`aegis-execution::ingestion`).

use crate::http_client::{http_post_json, resolve_info_url, shared_blocking_client};
use aegis_core::{
    now_iso8601_utc, round_dp, timeframe_seconds, OhlcvBar, PondUniverseRow, Result,
};
use aegis_db::{ConfigMap, DatabaseManager};
use chrono::Utc;
use reqwest::blocking::Client;
use serde_json::{json, Value};
use sha2::{Digest as Sha2Digest, Sha256};
use std::collections::{HashMap, HashSet};

/// Market Data Ingestion & `ALT/BTC:SYNTH` Synthesis Engine (`engine/ingestion.py`).
#[derive(Clone)]
pub struct MarketIngestionEngine {
    pub db: DatabaseManager,
    pub timeout_sec: u64,
    http_client: Client,
    client_timeout_sec: u64,
}

impl MarketIngestionEngine {
    pub fn new(db: DatabaseManager) -> Self {
        let timeout_sec = db
            .load_config()
            .ok()
            .map(|c| {
                c.get_i64(
                    "ingestion_http_timeout_sec",
                    c.get_i64("http_timeout_secs", 8),
                )
                .clamp(1, 60) as u64
            })
            .unwrap_or(8);
        let http_client = shared_blocking_client(timeout_sec);
        Self {
            db,
            timeout_sec,
            http_client,
            client_timeout_sec: timeout_sec,
        }
    }

    fn load_config_logged(&self, caller: &str) -> ConfigMap {
        self.db.load_config_logged(caller)
    }

    fn info_url(&self) -> String {
        let cfg = self.load_config_logged("engine.ingestion.info_url");
        resolve_info_url(&cfg)
    }

    fn http_post_json(&self, url: &str, payload: &Value) -> Option<Value> {
        let cfg = self.load_config_logged("engine.ingestion.http_post_json");
        http_post_json(
            &self.http_client,
            self.timeout_sec,
            self.client_timeout_sec,
            url,
            payload,
            &cfg,
        )
    }

    pub fn fetch_hyperliquid_funding_map(&self) -> HashMap<String, f64> {
        let mut out = HashMap::new();
        let Some(resp) =
            self.http_post_json(&self.info_url(), &json!({"type": "metaAndAssetCtxs"}))
        else {
            return out;
        };
        let Some(arr) = resp.as_array() else {
            return out;
        };
        if arr.len() < 2 {
            return out;
        }
        let universe = arr[0]
            .get("universe")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let ctxs = arr[1].as_array().cloned().unwrap_or_default();
        for (u, c) in universe.iter().zip(ctxs.iter()) {
            if let Some(name) = u.get("name").and_then(Value::as_str) {
                let f = c
                    .get("funding")
                    .and_then(|v| {
                        v.as_f64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                    })
                    .unwrap_or(0.0);
                out.insert(name.to_uppercase(), f);
            }
        }
        out
    }

    pub fn fetch_hyperliquid_candles_result(
        &self,
        coin: &str,
        interval: &str,
        lookback_bars: usize,
        funding_hourly: f64,
    ) -> Result<Vec<OhlcvBar>> {
        let tf_sec = timeframe_seconds(interval);
        let now_ms = Utc::now().timestamp_millis();
        let start_ms = now_ms - ((lookback_bars as i64 + 2) * tf_sec * 1000);
        let payload = json!({
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": start_ms,
                "endTime": now_ms,
            }
        });
        let raw = self.http_post_json(&self.info_url(), &payload).ok_or_else(|| {
            aegis_core::AegisError::Execution(format!(
                "Hyperliquid candleSnapshot HTTP request failed for {coin} ({interval})"
            ))
        })?;
        let list = raw.as_array().ok_or_else(|| {
            aegis_core::AegisError::Execution(format!(
                "Hyperliquid candleSnapshot response is not an array for {coin} ({interval})"
            ))
        })?;
        if list.is_empty() {
            return Ok(Vec::new());
        }

        let symbol = format!("{coin}/USDC:USDC");
        let mut bars = Vec::with_capacity(list.len());
        let parse_f = |v: Option<&Value>| -> f64 {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .unwrap_or(0.0)
        };

        for item in list {
            let ts = item.get("t").and_then(Value::as_i64).unwrap_or(0);
            if ts <= 0 {
                continue;
            }
            bars.push(OhlcvBar {
                symbol: symbol.clone(),
                timeframe: interval.to_string(),
                timestamp_ms: ts,
                open: parse_f(item.get("o")),
                high: parse_f(item.get("h")),
                low: parse_f(item.get("l")),
                close: parse_f(item.get("c")),
                volume: parse_f(item.get("v")),
                funding_rate_hourly: funding_hourly,
                is_synthetic: 0,
            });
        }
        bars.sort_by_key(|b| b.timestamp_ms);
        if bars.len() > lookback_bars {
            bars = bars[bars.len() - lookback_bars..].to_vec();
        }
        Ok(bars)
    }

    pub fn fetch_hyperliquid_candles(
        &self,
        coin: &str,
        interval: &str,
        lookback_bars: usize,
        funding_hourly: f64,
    ) -> Option<Vec<OhlcvBar>> {
        match self.fetch_hyperliquid_candles_result(coin, interval, lookback_bars, funding_hourly) {
            Ok(bars) if !bars.is_empty() => Some(bars),
            _ => None,
        }
    }

    /// Generates realistic multi-regime OHLCV bars (`BEAR`, `CHOP`, `BULL`) for offline / sandbox testing.
    #[allow(clippy::too_many_arguments)]
    pub fn generate_synthetic_bars(
        &self,
        symbol: &str,
        timeframe: &str,
        num_bars: usize,
        base_price: f64,
        btc_bars: Option<&[OhlcvBar]>,
        seed_offset: u64,
        end_ts_ms: Option<i64>,
    ) -> Vec<OhlcvBar> {
        let mut digest = Sha256::new();
        digest.update(format!("{symbol}:{timeframe}:{seed_offset}").as_bytes());
        let hash_bytes = digest.finalize();
        let mut rng_state = u64::from_le_bytes(hash_bytes[0..8].try_into().unwrap_or([1; 8]));
        let mut next_norm = || -> f64 {
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u1 = ((rng_state >> 11) as f64 / ((1u64 << 53) as f64)).clamp(1e-9, 1.0 - 1e-9);
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u2 = ((rng_state >> 11) as f64 / ((1u64 << 53) as f64)).clamp(1e-9, 1.0 - 1e-9);
            (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
        };

        let tf_sec = timeframe_seconds(timeframe);
        let step_ms = tf_sec * 1000;
        let now_aligned_ms = match end_ts_ms {
            Some(ts) => (ts / step_ms) * step_ms,
            None => (Utc::now().timestamp_millis() / step_ms) * step_ms,
        };
        let start_ms = now_aligned_ms - ((num_bars.saturating_sub(1)) as i64) * step_ms;

        let seg1 = num_bars / 3;
        let seg2 = (2 * num_bars) / 3;
        let tf_scale = ((tf_sec as f64) / 3600.0).sqrt();
        let is_btc = symbol.starts_with("BTC/");
        let mut alpha_tilt = 0.0;
        if !is_btc {
            let leader_rank = (seed_offset % 15) as f64;
            alpha_tilt = (7.0 - leader_rank) * 0.00028 * tf_scale;
        }

        let mut price = base_price * 0.88;
        let mut bars = Vec::with_capacity(num_bars);

        for i in 0..num_bars {
            let ts = start_ms + (i as i64) * step_ms;
            let (drift, vol, base_funding) = if i < seg1 {
                (-0.0014 * tf_scale, 0.011 * tf_scale, -0.00008)
            } else if i < seg2 {
                (0.0001 * tf_scale, 0.006 * tf_scale, 0.00026)
            } else {
                (0.0022 * tf_scale + alpha_tilt, 0.009 * tf_scale, 0.00014)
            };

            let shock = if let Some(b_bars) = btc_bars {
                if i > 0 && i < b_bars.len() {
                    let btc_ret =
                        (b_bars[i].close.max(1e-8) / b_bars[i - 1].close.max(1e-8)).ln();
                    let beta = 1.05 + ((seed_offset % 5) as f64) * 0.08;
                    beta * btc_ret + next_norm() * (vol * 0.55) + alpha_tilt
                } else {
                    drift + next_norm() * vol
                }
            } else {
                drift + next_norm() * vol
            };

            let open_px = price;
            let close_px = (open_px * shock.exp()).max(base_price * 0.05);
            let wick_up = next_norm().abs() * vol * 0.65;
            let wick_dn = next_norm().abs() * vol * 0.65;
            let high_px = open_px.max(close_px) * (1.0 + wick_up);
            let low_px = (open_px.min(close_px) * (1.0 - wick_dn)).max(1e-8);
            let volume =
                (150_000.0 / base_price.max(0.01)) * (1.0 + (shock / vol.max(1e-6)).abs());
            let funding = base_funding + next_norm() * 0.00003;

            bars.push(OhlcvBar {
                symbol: symbol.to_string(),
                timeframe: timeframe.to_string(),
                timestamp_ms: ts,
                open: round_dp(open_px, 8),
                high: round_dp(high_px, 8),
                low: round_dp(low_px, 8),
                close: round_dp(close_px, 8),
                volume: round_dp(volume, 4),
                funding_rate_hourly: round_dp(funding, 6),
                is_synthetic: 1,
            });
            price = close_px;
        }
        bars
    }

    /// Synthesizes `ALT/BTC:SYNTH` ratio OHLCV candles from timestamp-aligned `ALT/USDC` and `BTC/USDC` bars.
    pub fn synthesize_alt_btc_candles(
        alt_usdc_bars: &[OhlcvBar],
        btc_usdc_bars: &[OhlcvBar],
        synth_symbol: &str,
        timeframe: &str,
    ) -> Vec<OhlcvBar> {
        if alt_usdc_bars.is_empty() || btc_usdc_bars.is_empty() {
            return Vec::new();
        }
        let btc_by_ts: HashMap<i64, &OhlcvBar> = btc_usdc_bars
            .iter()
            .filter(|b| b.close > 0.0)
            .map(|b| (b.timestamp_ms, b))
            .collect();

        let mut synth = Vec::with_capacity(alt_usdc_bars.len());
        for a in alt_usdc_bars {
            let Some(b) = btc_by_ts.get(&a.timestamp_ms) else {
                continue;
            };
            if b.open <= 0.0 || b.close <= 0.0 {
                continue;
            }
            let r_open = a.open / b.open;
            let r_close = a.close / b.close;
            let r_high = r_open
                .max(r_close)
                .max(a.high / b.open.min(b.close).max(1e-8));
            let r_low = r_open
                .min(r_close)
                .min(a.low / b.open.max(b.close).max(1e-8));
            let rel_funding = a.funding_rate_hourly - b.funding_rate_hourly;

            synth.push(OhlcvBar {
                symbol: synth_symbol.to_string(),
                timeframe: timeframe.to_string(),
                timestamp_ms: a.timestamp_ms,
                open: round_dp(r_open, 10),
                high: round_dp(r_high, 10),
                low: round_dp(r_low, 10),
                close: round_dp(r_close, 10),
                volume: round_dp(a.volume * a.close, 2),
                funding_rate_hourly: round_dp(rel_funding, 6),
                is_synthetic: 1,
            });
        }
        synth
    }

    /// Ingests all active `USDC_PERP` assets and synthesizes all 15 `ALT/BTC:SYNTH` relative-strength pairs.
    pub fn ingest_universe(
        &self,
        lookback_bars: usize,
        force_synthetic: bool,
    ) -> Result<HashMap<String, usize>> {
        let usdc_assets = self.db.get_usdc_perp_universe()?;
        let alt_pairs = self.db.get_alt_pairs_universe()?;

        let mut timeframes: HashSet<String> = HashSet::from(["4h".to_string(), "1h".to_string()]);
        for a in &usdc_assets {
            timeframes.insert(a.exec_timeframe.clone());
            timeframes.insert(a.rs_timeframe.clone());
        }

        let cfg = self.load_config_logged("engine.ingestion.ingest_universe");
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_live_or_testnet =
            mode == "LIVE" || mode == "TESTNET" || cfg.get_bool("hyperliquid_testnet", false);

        let funding_map = if force_synthetic {
            HashMap::new()
        } else {
            self.fetch_hyperliquid_funding_map()
        };

        let mut btc_bars_by_tf: HashMap<String, Vec<OhlcvBar>> = HashMap::new();
        let mut total_usdc_bars = 0usize;
        let mut total_synth_bars = 0usize;
        let mut live_api_used = false;
        let mut api_unreachable = false;

        let max_consecutive_failures =
            cfg.get_i64("max_consecutive_api_failures", 5).max(1) as usize;
        let mut consecutive_failures = 0usize;

        let mut sorted_tfs: Vec<String> = timeframes.into_iter().collect();
        sorted_tfs.sort();

        let btc_ref_px = self.db.resolve_reference_price("BTC/USDC:USDC");

        for tf in &sorted_tfs {
            let mut bars_opt = None;
            if !force_synthetic && !api_unreachable {
                let f_rate = funding_map.get("BTC").copied().unwrap_or(0.0001);
                match self.fetch_hyperliquid_candles_result("BTC", tf, lookback_bars, f_rate) {
                    Ok(b) => {
                        consecutive_failures = 0;
                        if !b.is_empty() {
                            live_api_used = true;
                            bars_opt = Some(b);
                        }
                    }
                    Err(_) => {
                        consecutive_failures += 1;
                        if funding_map.is_empty()
                            || consecutive_failures >= max_consecutive_failures
                        {
                            api_unreachable = true;
                        }
                    }
                }
            }
            let bars = match bars_opt {
                Some(b) => {
                    total_usdc_bars += self.db.upsert_ohlcv_batch(&b)?;
                    b
                }
                None => {
                    let existing = if !force_synthetic {
                        self.db
                            .get_ohlcv_bars("BTC/USDC:USDC", tf, lookback_bars, None)
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };
                    if !force_synthetic && (is_live_or_testnet || !existing.is_empty()) {
                        tracing::warn!(
                            "Transient candleSnapshot failure for BTC/USDC:USDC ({}); preserving {} existing bars",
                            tf,
                            existing.len()
                        );
                        existing
                    } else {
                        let synth = self.generate_synthetic_bars(
                            "BTC/USDC:USDC",
                            tf,
                            lookback_bars,
                            btc_ref_px,
                            None,
                            1,
                            None,
                        );
                        total_usdc_bars += self.db.upsert_ohlcv_batch(&synth)?;
                        synth
                    }
                }
            };
            btc_bars_by_tf.insert(tf.clone(), bars);
        }

        for (idx, asset) in usdc_assets.iter().enumerate() {
            if asset.base_asset == "BTC" {
                continue;
            }
            let mut req_tfs = vec![asset.exec_timeframe.clone()];
            if asset.rs_timeframe != asset.exec_timeframe {
                req_tfs.push(asset.rs_timeframe.clone());
            }
            let f_rate = funding_map
                .get(&asset.base_asset)
                .copied()
                .unwrap_or(0.00012);
            let ref_px = if asset.reference_price > 0.0 {
                asset.reference_price
            } else {
                self.db.resolve_reference_price(&asset.symbol)
            };

            for tf in req_tfs {
                let mut bars_opt = None;
                if !force_synthetic && !api_unreachable {
                    match self.fetch_hyperliquid_candles_result(
                        &asset.base_asset,
                        &tf,
                        lookback_bars,
                        f_rate,
                    ) {
                        Ok(b) => {
                            consecutive_failures = 0;
                            if !b.is_empty() {
                                live_api_used = true;
                                bars_opt = Some(b);
                            }
                        }
                        Err(_) => {
                            consecutive_failures += 1;
                            if consecutive_failures >= max_consecutive_failures {
                                tracing::warn!(
                                    consecutive_failures = consecutive_failures,
                                    "Hyperliquid candleSnapshot reached max_consecutive_api_failures in altcoin loop; tripping api_unreachable circuit breaker (NM-12)"
                                );
                                api_unreachable = true;
                            }
                        }
                    }
                }
                match bars_opt {
                    Some(bars) => {
                        total_usdc_bars += self.db.upsert_ohlcv_batch(&bars)?;
                    }
                    None => {
                        let existing = if !force_synthetic {
                            self.db
                                .get_ohlcv_bars(&asset.symbol, &tf, lookback_bars, None)
                                .unwrap_or_default()
                        } else {
                            Vec::new()
                        };
                        if !force_synthetic && (is_live_or_testnet || !existing.is_empty()) {
                            tracing::warn!(
                                "Transient candleSnapshot failure for {} ({}); preserving {} existing bars",
                                asset.symbol,
                                tf,
                                existing.len()
                            );
                        } else {
                            let bars = self.generate_synthetic_bars(
                                &asset.symbol,
                                &tf,
                                lookback_bars,
                                ref_px,
                                btc_bars_by_tf.get(&tf).map(Vec::as_slice),
                                (idx + 2) as u64,
                                None,
                            );
                            total_usdc_bars += self.db.upsert_ohlcv_batch(&bars)?;
                        }
                    }
                }
            }
        }

        for pair in &alt_pairs {
            let tf = &pair.rs_timeframe;
            let alt_bars = self
                .db
                .get_ohlcv_bars(&pair.exec_symbol, tf, lookback_bars, None)?;
            let btc_bars = match btc_bars_by_tf.get(tf) {
                Some(b) => b.clone(),
                None => self.db.get_ohlcv_bars("BTC/USDC:USDC", tf, lookback_bars, None)?,
            };
            let synth_bars =
                Self::synthesize_alt_btc_candles(&alt_bars, &btc_bars, &pair.synth_symbol, tf);
            total_synth_bars += self.db.upsert_ohlcv_batch(&synth_bars)?;
        }

        let source_label = if live_api_used {
            "HYPERLIQUID_LIVE_API"
        } else if !force_synthetic && (is_live_or_testnet || total_usdc_bars == 0) {
            "CACHED_LIVE_BARS_OFFLINE"
        } else {
            "DETERMINISTIC_SYNTHETIC_FALLBACK"
        };
        if let Err(e) = self.db.log_telemetry(
            "INFO",
            "engine.ingestion",
            &format!(
                "Ingested {total_usdc_bars} USDC perp bars and synthesized {total_synth_bars} ALT/BTC:SYNTH bars ({source_label})"
            ),
            Some(json!({
                "usdc_bars": total_usdc_bars,
                "synth_bars": total_synth_bars,
                "source": source_label,
            })),
        ) {
            tracing::warn!(error = %e, "Failed to log ingestion telemetry");
        }

        Ok(HashMap::from([
            ("usdc_bars_upserted".to_string(), total_usdc_bars),
            ("synth_bars_upserted".to_string(), total_synth_bars),
        ]))
    }

    /// Discovers top-N Hyperliquid USDC perpetual candidates ranked by 7-day median daily volume,
    /// falling back to a deterministic 85-coin Hyperliquid universe when offline (`NH-9`, `NM-13`, `HP`).
    pub fn discover_pond_universe_candidates(
        &self,
        target_size: usize,
        min_daily_vol_usdc: f64,
    ) -> Vec<PondUniverseRow> {
        let now_str = now_iso8601_utc();
        let warmup_ready = (Utc::now() - chrono::Duration::hours(200))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        let cfg = self.load_config_logged("discover_pond_universe_candidates");
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let has_custom_info_url = !cfg.get_str("hyperliquid_info_url", "").trim().is_empty()
            || !cfg
                .get_str("hyperliquid_testnet_info_url", "")
                .trim()
                .is_empty();
        let should_query_live = exec_mode == "LIVE"
            || exec_mode == "TESTNET"
            || pond_mode == "LIVE"
            || pond_mode == "TESTNET"
            || has_custom_info_url;

        // Try live Hyperliquid metaAndAssetCtxs when in LIVE/TESTNET mode or custom info endpoint is set
        if should_query_live {
            if let Some(resp) =
                self.http_post_json(&self.info_url(), &json!({"type": "metaAndAssetCtxs"}))
            {
                if let Some(arr) = resp.as_array() {
                    if arr.len() >= 2 {
                    let universe = arr[0]
                        .get("universe")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let ctxs = arr[1].as_array().cloned().unwrap_or_default();
                    let mut live_items: Vec<(String, f64, u32, f64, f64)> = Vec::new();
                    for (u, c) in universe.iter().zip(ctxs.iter()) {
                        let is_delisted = u
                            .get("isDelisted")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        if is_delisted {
                            continue;
                        }
                        if let Some(name) = u.get("name").and_then(Value::as_str) {
                            let day_vlm = c
                                .get("dayNtlVlm")
                                .and_then(|v| {
                                    v.as_f64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                                })
                                .unwrap_or(0.0);
                            if day_vlm >= min_daily_vol_usdc {
                                let sz_decimals = u
                                    .get("szDecimals")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(3)
                                    .min(6) as u32;
                                let max_lev = u
                                    .get("maxLeverage")
                                    .and_then(|v| {
                                        v.as_f64().or_else(|| {
                                            v.as_str().and_then(|s| s.parse::<f64>().ok())
                                        })
                                    })
                                    .unwrap_or(3.0)
                                    .clamp(1.0, 50.0);
                                let mark_px = c
                                    .get("markPx")
                                    .or_else(|| c.get("midPx"))
                                    .and_then(|v| {
                                        v.as_f64().or_else(|| {
                                            v.as_str().and_then(|s| s.parse::<f64>().ok())
                                        })
                                    })
                                    .unwrap_or(0.0);
                                live_items.push((
                                    name.to_uppercase(),
                                    day_vlm,
                                    sz_decimals,
                                    max_lev,
                                    mark_px,
                                ));
                            }
                        }
                    }
                    live_items
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                    if live_items.len() >= target_size.min(40) {
                        return live_items
                            .into_iter()
                            .take(target_size)
                            .enumerate()
                            .map(|(idx, (coin, vol, sz_dec, max_lev, mark_px))| {
                                let symbol = format!("{coin}/USDC:USDC");
                                let ref_px = if mark_px > 0.0 {
                                    mark_px
                                } else {
                                    self.db.resolve_reference_price(&symbol)
                                };
                                let lot_step = round_dp(10f64.powi(-(sz_dec as i32)), 8).max(1e-6);
                                let max_dec = (6 - sz_dec as i32).max(0);
                                let mag = if ref_px > 0.0 {
                                    ref_px.log10().floor() as i32
                                } else {
                                    0
                                };
                                let dec = (4 - mag).max(0).min(max_dec);
                                let tick_size = round_dp(10f64.powi(-dec), 8).max(1e-6);
                                PondUniverseRow {
                                    symbol,
                                    rank: (idx + 1) as i64,
                                    median_daily_volume_usdc: round_dp(vol, 2),
                                    tick_size,
                                    lot_step_size: lot_step,
                                    min_notional_usdc: 10.0,
                                    max_leverage: max_lev,
                                    status: "ACTIVE".to_string(),
                                    warmup_until: Some(warmup_ready.clone()),
                                    delist_streak: 0,
                                    added_at: now_str.clone(),
                                    delisted_at: None,
                                    is_synthetic_volume: 0,
                                    reference_price: ref_px,
                                }
                            })
                            .collect();
                    }
                }
            }
        }
        }

        // Deterministic 85-coin Hyperliquid universe fallback (sorted by realistic daily volume >= $50k)
        const POND_COINS: [&str; 85] = [
            "BTC", "ETH", "SOL", "XRP", "DOGE", "HYPE", "SUI", "AVAX", "LINK", "ADA",
            "BNB", "DOT", "NEAR", "ARB", "OP", "LTC", "APT", "TIA", "INJ", "UNI",
            "AAVE", "ATOM", "FIL", "RENDER", "TAO", "WIF", "PEPE", "BONK", "FET", "SEI",
            "JUP", "ONDO", "PENDLE", "ENA", "WLD", "STX", "IMX", "KAS", "RUNE", "AR",
            "PYTH", "STRK", "ORDI", "GALA", "SAND", "MANA", "AXS", "CRV", "LDO", "MKR",
            "SNX", "COMP", "DYDX", "GMX", "BLUR", "ENS", "APE", "CHZ", "ALGO", "VET",
            "ICP", "EGLD", "FLOW", "THETA", "NEO", "KAVA", "MINA", "ROSE", "ZIL", "IOTA",
            "JASMY", "FLOKI", "MEME", "BOME", "POPCAT", "BRETT", "MEW", "NOT", "TON", "TRUMP",
            "VIRTUAL", "AI16Z", "FARTCOIN", "PENGU", "ZRO",
        ];

        POND_COINS
            .iter()
            .take(target_size.min(POND_COINS.len()))
            .enumerate()
            .map(|(idx, coin)| {
                let rank = (idx + 1) as i64;
                let symbol = format!("{coin}/USDC:USDC");
                let vol = (850_000_000.0 * (-0.112 * (idx as f64)).exp())
                    .max(min_daily_vol_usdc + 25_000.0);
                let ref_px = self.db.resolve_reference_price(&symbol);
                PondUniverseRow {
                    symbol,
                    rank,
                    median_daily_volume_usdc: round_dp(vol, 2),
                    tick_size: 0.0001,
                    lot_step_size: 0.001,
                    min_notional_usdc: 10.0,
                    max_leverage: 3.0,
                    status: "ACTIVE".to_string(),
                    warmup_until: Some(warmup_ready.clone()),
                    delist_streak: 0,
                    added_at: now_str.clone(),
                    delisted_at: None,
                    is_synthetic_volume: 1,
                    reference_price: ref_px,
                }
            })
            .collect()
    }

    /// Generates a deterministic 168-hour+ 1H funding & OHLCV series + 4H bars for a Pond asset.
    pub fn generate_pond_funding_and_bars(
        &self,
        symbol: &str,
        rank: usize,
        hours: usize,
    ) -> (Vec<f64>, Vec<OhlcvBar>, Vec<OhlcvBar>) {
        let cfg = self.load_config_logged("engine.ingestion.generate_pond_funding_and_bars");
        let fallback_base = cfg.get_f64("pond_fallback_base_price_usdc", 10.0);
        let fallback_slope = cfg.get_f64("pond_fallback_rank_slope_usdc", 0.75);
        let resolved_px = self.db.resolve_reference_price(symbol);
        let base_px = if (resolved_px - fallback_base).abs() > 1e-6
            || symbol.starts_with("APT")
            || symbol.starts_with("UNI")
            || symbol.starts_with("ATOM")
        {
            resolved_px
        } else {
            fallback_base + (rank as f64) * fallback_slope
        };
        let bars_1h = self.generate_synthetic_bars(
            symbol,
            "1h",
            hours.max(180),
            base_px,
            None,
            (rank as u64) + 100,
            None,
        );
        let bars_4h = self.generate_synthetic_bars(
            symbol,
            "4h",
            (hours / 4).max(45),
            base_px,
            None,
            (rank as u64) + 500,
            None,
        );

        let mut funding_series: Vec<f64> = bars_1h
            .iter()
            .enumerate()
            .map(|(i, b)| {
                let wave = ((i as f64 + rank as f64) * 0.17).sin() * 0.000018;
                round_dp(0.000004 + wave + b.funding_rate_hourly * 0.05, 8)
            })
            .collect();

        if let Some(last) = funding_series.last_mut() {
            if rank % 11 == 3 {
                *last = 0.000085 + (rank as f64) * 0.000001;
            } else if rank % 13 == 5 {
                *last = -0.000082 - (rank as f64) * 0.000001;
            }
        }

        (funding_series, bars_1h, bars_4h)
    }

    /// Queries Hyperliquid `/info` (`fundingHistory`) for up to `hours` of historical 1H funding rates with timestamps (`NL-8`).
    pub fn fetch_hyperliquid_funding_history_timestamped(
        &self,
        coin: &str,
        hours: usize,
    ) -> Option<Vec<(i64, f64)>> {
        let now_ms = Utc::now().timestamp_millis();
        let start_ms = now_ms - ((hours as i64 + 4) * 3_600_000);
        let payload = json!({
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
            "endTime": now_ms,
        });
        let raw = self.http_post_json(&self.info_url(), &payload)?;
        let list = raw.as_array()?;
        if list.is_empty() {
            tracing::warn!(coin = coin, "Hyperliquid fundingHistory returned empty array (NL-8)");
            return None;
        }
        let mut out = Vec::with_capacity(list.len());
        for item in list {
            if let Some(f) = item.get("fundingRate").and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            }) {
                if f.is_finite() {
                    let ts_ms = item
                        .get("time")
                        .and_then(|v| {
                            v.as_i64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                        })
                        .unwrap_or(0);
                    out.push((ts_ms, round_dp(f, 8)));
                }
            }
        }
        if out.is_empty() {
            tracing::warn!(coin = coin, "Hyperliquid fundingHistory contained no finite fundingRate values (NL-8)");
            return None;
        }
        if out.len() < 24 {
            tracing::warn!(
                coin = coin,
                samples = out.len(),
                "Hyperliquid fundingHistory returned fewer than 24 hourly samples (NL-8)"
            );
        }
        if out.len() > hours {
            out = out[out.len() - hours..].to_vec();
        }
        Some(out)
    }

    /// Queries Hyperliquid `/info` (`fundingHistory`) for up to `hours` of historical 1H funding rates.
    pub fn fetch_hyperliquid_funding_history(&self, coin: &str, hours: usize) -> Option<Vec<f64>> {
        self.fetch_hyperliquid_funding_history_timestamped(coin, hours)
            .map(|v| v.into_iter().map(|(_, r)| r).collect())
    }

    /// Loads or fetches 1H funding history + 1H/4H OHLCV bars for a Pond asset.
    pub fn load_or_fetch_pond_funding_and_bars(
        &self,
        symbol: &str,
        rank: usize,
        hours: usize,
        live_funding_map: Option<&HashMap<String, f64>>,
        allow_network_fetch: bool,
    ) -> (Vec<f64>, Vec<OhlcvBar>, Vec<OhlcvBar>) {
        let cfg = self.load_config_logged("engine.ingestion.load_or_fetch_pond_funding_and_bars");
        let exec_mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let pond_mode = cfg
            .get_str("pond_execution_mode", &exec_mode)
            .to_ascii_uppercase();
        let is_live_or_testnet = pond_mode == "LIVE"
            || pond_mode == "TESTNET"
            || exec_mode == "LIVE"
            || exec_mode == "TESTNET";

        if !is_live_or_testnet {
            return self.generate_pond_funding_and_bars(symbol, rank, hours);
        }

        let coin = symbol
            .split('/')
            .next()
            .unwrap_or(symbol)
            .trim()
            .to_uppercase();
        let live_f = live_funding_map.and_then(|m| m.get(&coin).copied());
        let min_fund_floor = cfg.get_f64("pond_funding_floor_hourly", 0.000015);
        let max_staleness_ms = cfg.get_i64("max_bar_staleness_ms", 28_800_000).max(3_600_000);
        let now_ms = Utc::now().timestamp_millis();

        let mut bars_1h: Vec<OhlcvBar> = self
            .db
            .get_ohlcv_bars(symbol, "1h", hours.max(180), None)
            .unwrap_or_default()
            .into_iter()
            .filter(|b| b.is_synthetic == 0)
            .collect();
        let mut bars_4h: Vec<OhlcvBar> = self
            .db
            .get_ohlcv_bars(symbol, "4h", (hours / 4).max(45), None)
            .unwrap_or_default()
            .into_iter()
            .filter(|b| b.is_synthetic == 0)
            .collect();

        let passes_floor_or_single = match live_f {
            Some(r) => r.abs() >= min_fund_floor,
            None => live_funding_map.is_none(),
        };

        if allow_network_fetch && passes_floor_or_single {
            let f_rate = live_f.unwrap_or_else(|| {
                bars_1h
                    .last()
                    .map(|b| b.funding_rate_hourly)
                    .unwrap_or(0.0)
            });

            let need_1h = bars_1h.len() < hours
                || bars_1h
                    .last()
                    .map(|b| now_ms - b.timestamp_ms > 3_600_000)
                    .unwrap_or(true);
            if need_1h {
                if let Some(fetched_1h) =
                    self.fetch_hyperliquid_candles(&coin, "1h", hours.max(180), f_rate)
                {
                    bars_1h = fetched_1h;
                }
            }

            let need_4h = bars_4h.len() < (hours / 4).max(45)
                || bars_4h
                    .last()
                    .map(|b| now_ms - b.timestamp_ms > 14_400_000)
                    .unwrap_or(true);
            if need_4h {
                if let Some(fetched_4h) =
                    self.fetch_hyperliquid_candles(&coin, "4h", (hours / 4).max(45), f_rate)
                {
                    if let Err(e) = self.db.upsert_ohlcv_batch(&fetched_4h) {
                        tracing::error!(symbol = symbol, error = %e, "Failed to upsert 4h pond bars");
                    }
                    bars_4h = fetched_4h;
                }
            }

            let has_funding_variance = if bars_1h.len() >= 24 {
                let first_f = bars_1h[0].funding_rate_hourly;
                bars_1h
                    .iter()
                    .any(|b| (b.funding_rate_hourly - first_f).abs() > 1e-10)
            } else {
                false
            };

            if need_1h || !has_funding_variance {
                if let Some(f_hist_ts) =
                    self.fetch_hyperliquid_funding_history_timestamped(&coin, hours.max(180))
                {
                    let mut f_hist: Vec<f64> = f_hist_ts.iter().map(|(_, r)| *r).collect();
                    if let Some(lf) = live_f {
                        if let Some(last) = f_hist.last_mut() {
                            *last = lf;
                        }
                    }
                    if !bars_1h.is_empty() && !f_hist_ts.is_empty() {
                        let mut bucket_map: HashMap<i64, f64> = HashMap::new();
                        for &(ts_ms, rate) in &f_hist_ts {
                            if ts_ms > 0 {
                                bucket_map.insert(ts_ms / 3_600_000, rate);
                            }
                        }
                        if !bucket_map.is_empty() {
                            for bar in &mut bars_1h {
                                let bar_bucket = bar.timestamp_ms / 3_600_000;
                                if let Some(&rate) = bucket_map.get(&bar_bucket) {
                                    bar.funding_rate_hourly = rate;
                                }
                            }
                            if let Some(lf) = live_f {
                                if let Some(last_bar) = bars_1h.last_mut() {
                                    last_bar.funding_rate_hourly = lf;
                                }
                            }
                        } else {
                            let b_len = bars_1h.len();
                            let f_len = f_hist.len();
                            let overlap = b_len.min(f_len);
                            for i in 0..overlap {
                                bars_1h[b_len - overlap + i].funding_rate_hourly =
                                    f_hist[f_len - overlap + i];
                            }
                        }
                        if let Err(e) = self.db.upsert_ohlcv_batch(&bars_1h) {
                            tracing::error!(symbol = symbol, error = %e, "Failed to upsert 1h pond bars");
                        }
                    }
                    return (f_hist, bars_1h, bars_4h);
                } else {
                    tracing::warn!(
                        symbol = symbol,
                        coin = %coin,
                        "Live fundingHistory unavailable for Pond asset; falling back to bar funding series (NL-8)"
                    );
                    if need_1h && !bars_1h.is_empty() {
                        if let Err(e) = self.db.upsert_ohlcv_batch(&bars_1h) {
                            tracing::error!(symbol = symbol, error = %e, "Failed to upsert 1h pond bars");
                        }
                    }
                }
            }
        }

        // Enforce live bar staleness guard (NM-8)
        if let Some(last_1h) = bars_1h.last() {
            if now_ms - last_1h.timestamp_ms > max_staleness_ms {
                tracing::warn!(
                    symbol = symbol,
                    age_ms = now_ms - last_1h.timestamp_ms,
                    max_staleness_ms = max_staleness_ms,
                    "Discarding stale 1h Pond bars in live/testnet mode (NM-8)"
                );
                bars_1h.clear();
            }
        }
        if let Some(last_4h) = bars_4h.last() {
            if now_ms - last_4h.timestamp_ms > max_staleness_ms {
                tracing::warn!(
                    symbol = symbol,
                    age_ms = now_ms - last_4h.timestamp_ms,
                    max_staleness_ms = max_staleness_ms,
                    "Discarding stale 4h Pond bars in live/testnet mode (NM-8)"
                );
                bars_4h.clear();
            }
        }

        let mut funding_series: Vec<f64> =
            bars_1h.iter().map(|b| b.funding_rate_hourly).collect();
        if let Some(lf) = live_f {
            if funding_series.is_empty() {
                funding_series.push(lf);
            } else if let Some(last) = funding_series.last_mut() {
                *last = lf;
            }
        }
        (funding_series, bars_1h, bars_4h)
    }
}
