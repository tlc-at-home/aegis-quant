//! Point-in-Time Historical Backtesting & Tear-Sheet Engine (`aegis-backtest`).
//!
//! 1:1 port of `engine/backtest.py`:
//! - Isolated `:memory:` SQLite database cloning (`clone_to_memory`) so backtests never overwrite
//!   live or paper WAL state (`trades`, `portfolio_snapshots`, `signal_evaluations`, `market_ohlcv`)
//! - SHA-256 dataset provenance verification (`data/real_4h_backfill.json`) (`B5`)
//! - Strictly causal cross-sectional feature precomputation via `rayon` (`compute_continuous_snr_series`,
//!   `build_mace_features_and_target`, `fit_predict_mace_series_from_xy`, `compute_rs_lookbacks`) (`B6`, `C6`)
//! - Strict point-in-time bar replay (`pit_timestamp_ms`) with Two-Tier Chandelier Stops,
//!   post-stop cooldowns, dynamic slippage, and DB-configured institutional acceptance gates

use aegis_core::{
    resolve_project_root, round_dp, verify_data_provenance, AegisError, AltPairMetadata,
    BacktestTearSheet, OhlcvBar, PortfolioSnapshot, RegimeState, Result, SignalEvaluation,
    TradeRecord, UniverseAsset,
};
use aegis_db::DatabaseManager;
use aegis_execution::{ExecutionEngine, MarketIngestionEngine};
use aegis_risk::RiskEngine;
use aegis_signals::SignalEngine;
use chrono::{TimeZone, Utc};
use rayon::prelude::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

pub struct BacktestEngine {
    pub db: DatabaseManager,
    pub days: usize,
}

#[derive(Clone)]
struct PrecomputedPairSeries {
    pub pair: AltPairMetadata,
    pub asset: UniverseAsset,
    pub usdc_bars: Vec<OhlcvBar>,
    pub snr_series: Vec<f64>,
    pub snr_synth_series: Vec<f64>,
    pub mace_series: Vec<f64>,
    pub ic_series: Vec<f64>,
    pub rs_composite_series: Vec<f64>,
    pub rs_96_series: Vec<f64>,
    pub r_usdc_8_series: Vec<f64>,
}

impl BacktestEngine {
    pub fn new(db: DatabaseManager, days: usize) -> Self {
        let min_days = db
            .load_config()
            .ok()
            .map(|c| c.get_usize("backtest_min_days", 30).max(5))
            .unwrap_or(30);
        Self {
            db,
            days: days.max(min_days),
        }
    }

    pub fn backfill_path() -> PathBuf {
        resolve_project_root()
            .join("data")
            .join("real_4h_backfill.json")
    }

    /// Parses historical 4H backfill JSON supporting both the nested `{"coins": {"BTC": {"funding_rate_hourly": ..., "candles": [...]}}}`
    /// production schema (`B5`) and flat `{"candles": {"BTC": [...]}}` / `{"BTC": [...]}` schemas.
    pub fn parse_backfill_json(raw_json: &str) -> Result<HashMap<String, Vec<OhlcvBar>>> {
        Self::parse_backfill_json_with_default_funding(raw_json, 0.0)
    }

    /// Parses historical 4H backfill JSON using `fallback_funding_hourly` from `system_config` (`R3-N16`).
    pub fn parse_backfill_json_with_default_funding(
        raw_json: &str,
        fallback_funding_hourly: f64,
    ) -> Result<HashMap<String, Vec<OhlcvBar>>> {
        let root_val: Value = serde_json::from_str(raw_json).map_err(|e| {
            AegisError::Validation(format!("Invalid backfill JSON syntax: {e}"))
        })?;
        let Value::Object(root) = root_val else {
            return Err(AegisError::Validation(
                "Backfill JSON root must be an object".to_string(),
            ));
        };

        let mut loaded_real: HashMap<String, Vec<OhlcvBar>> = HashMap::new();

        let parse_candle_array =
            |norm_sym: &str, arr: &[Value], default_funding: f64| -> Vec<OhlcvBar> {
                let mut bars = Vec::with_capacity(arr.len());
                for row in arr {
                    let ts = row
                        .get("timestamp_ms")
                        .or_else(|| row.get("t"))
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    if ts <= 0 {
                        continue;
                    }
                    let parse_f = |k1: &str, k2: &str, def: f64| -> f64 {
                        row.get(k1)
                            .or_else(|| row.get(k2))
                            .and_then(|v| {
                                v.as_f64()
                                    .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                            })
                            .unwrap_or(def)
                    };
                    bars.push(OhlcvBar {
                        symbol: norm_sym.to_string(),
                        timeframe: "4h".to_string(),
                        timestamp_ms: ts,
                        open: parse_f("open", "o", 0.0),
                        high: parse_f("high", "h", 0.0),
                        low: parse_f("low", "l", 0.0),
                        close: parse_f("close", "c", 0.0),
                        volume: parse_f("volume", "v", 1000.0),
                        funding_rate_hourly: parse_f(
                            "funding_rate_hourly",
                            "f",
                            default_funding,
                        ),
                        is_synthetic: 0,
                    });
                }
                bars.sort_by_key(|b| b.timestamp_ms);
                bars
            };

        // 1. Primary schema: root["coins"][coin] -> {"funding_rate_hourly": f64, "candles": [...]}
        if let Some(coins_obj) = root.get("coins").and_then(Value::as_object) {
            for (sym_key, coin_entry) in coins_obj {
                let norm_sym = if sym_key.contains("/USDC:USDC") {
                    sym_key.clone()
                } else {
                    format!("{}/USDC:USDC", sym_key.trim().to_uppercase())
                };
                if let Some(coin_obj) = coin_entry.as_object() {
                    let default_funding = coin_obj
                        .get("funding_rate_hourly")
                        .and_then(|v| {
                            v.as_f64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                        })
                        .unwrap_or(fallback_funding_hourly);
                    if let Some(arr) = coin_obj.get("candles").and_then(Value::as_array) {
                        let bars = parse_candle_array(&norm_sym, arr, default_funding);
                        if !bars.is_empty() {
                            loaded_real.insert(norm_sym, bars);
                        }
                    }
                } else if let Some(arr) = coin_entry.as_array() {
                    let bars = parse_candle_array(&norm_sym, arr, fallback_funding_hourly);
                    if !bars.is_empty() {
                        loaded_real.insert(norm_sym, bars);
                    }
                }
            }
        }

        // 2. Fallback flat schema: root["candles"][coin] -> [...] or root[coin] -> [...]
        if loaded_real.is_empty() {
            let candles_obj = root
                .get("candles")
                .and_then(Value::as_object)
                .unwrap_or(&root);
            for (sym_key, entry_val) in candles_obj {
                let norm_sym = if sym_key.contains("/USDC:USDC") {
                    sym_key.clone()
                } else {
                    format!("{}/USDC:USDC", sym_key.trim().to_uppercase())
                };
                if let Some(arr) = entry_val.as_array() {
                    let bars = parse_candle_array(&norm_sym, arr, fallback_funding_hourly);
                    if !bars.is_empty() {
                        loaded_real.insert(norm_sym, bars);
                    }
                } else if let Some(arr) = entry_val
                    .as_object()
                    .and_then(|o| o.get("candles"))
                    .and_then(Value::as_array)
                {
                    let def_f = entry_val
                        .get("funding_rate_hourly")
                        .and_then(Value::as_f64)
                        .unwrap_or(fallback_funding_hourly);
                    let bars = parse_candle_array(&norm_sym, arr, def_f);
                    if !bars.is_empty() {
                        loaded_real.insert(norm_sym, bars);
                    }
                }
            }
        }

        Ok(loaded_real)
    }

    /// Generates causal warmup bars prior to `first_real_bar` when historical data is shorter than `warmup_count + replay_bars`.
    fn generate_warmup_bars(
        symbol: &str,
        timeframe: &str,
        first_bar: &OhlcvBar,
        warmup_count: usize,
        seed_offset: u64,
    ) -> Vec<OhlcvBar> {
        let mut digest = Sha256::new();
        digest.update(format!("WARMUP:{symbol}:{seed_offset}").as_bytes());
        let h = digest.finalize();
        let mut rng_state = u64::from_le_bytes(h[0..8].try_into().unwrap_or([7; 8]));
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

        let step_ms = 4 * 3_600_000i64;
        let anchor_px = first_bar.open.max(1e-8);
        let mut rev_closes = Vec::with_capacity(warmup_count);
        let mut cur = anchor_px;
        for _ in 0..warmup_count {
            let shock = next_norm() * 0.008;
            cur = (cur / shock.exp()).max(anchor_px * 0.15);
            rev_closes.push(cur);
        }
        rev_closes.reverse();

        let mut out = Vec::with_capacity(warmup_count);
        for i in 0..warmup_count {
            let ts = first_bar.timestamp_ms - ((warmup_count - i) as i64) * step_ms;
            let close_px = rev_closes[i];
            let open_px = if i == 0 {
                close_px * 0.998
            } else {
                rev_closes[i - 1]
            };
            let high_px = open_px.max(close_px) * 1.006;
            let low_px = open_px.min(close_px) * 0.994;
            out.push(OhlcvBar {
                symbol: symbol.to_string(),
                timeframe: timeframe.to_string(),
                timestamp_ms: ts,
                open: round_dp(open_px, 8),
                high: round_dp(high_px, 8),
                low: round_dp(low_px, 8),
                close: round_dp(close_px, 8),
                volume: first_bar.volume.max(1000.0),
                funding_rate_hourly: first_bar.funding_rate_hourly,
                is_synthetic: 1,
            });
        }
        out
    }

    /// Loads `data/real_4h_backfill.json` (verifying SHA-256 provenance) or generates deterministic bars (`B5`).
    /// When `use_real_history` is `true`, hard-fails with `AegisError::Validation` if the dataset yields zero real bars.
    #[allow(clippy::type_complexity)]
    fn load_historical_dataset(
        &self,
        bt_db: &DatabaseManager,
        replay_bars: usize,
        warmup_bars: usize,
        use_real_history: bool,
    ) -> Result<(
        Vec<OhlcvBar>,
        HashMap<String, Vec<OhlcvBar>>,
        HashMap<String, Vec<OhlcvBar>>,
    )> {
        let cfg = bt_db.load_config()?;
        let default_funding_hourly = cfg.get_f64("default_funding_rate_hourly", 0.0);
        let ingestion = MarketIngestionEngine::new(bt_db.clone());
        let usdc_assets = bt_db.get_usdc_perp_universe()?;
        let alt_pairs = bt_db.get_alt_pairs_universe()?;

        let backfill_file = Self::backfill_path();
        let mut loaded_real: HashMap<String, Vec<OhlcvBar>> = HashMap::new();

        if use_real_history {
            let verified = verify_data_provenance(&backfill_file)?;
            if !verified {
                return Err(AegisError::Validation(format!(
                    "Dataset provenance verification failed for {}",
                    backfill_file.display()
                )));
            }
            let raw_json = fs::read_to_string(&backfill_file)?;
            loaded_real = Self::parse_backfill_json_with_default_funding(
                &raw_json,
                default_funding_hourly,
            )?;
            if loaded_real.is_empty() || !loaded_real.contains_key("BTC/USDC:USDC") {
                return Err(AegisError::Validation(format!(
                    "Real historical dataset {} yielded zero usable BTC/USDC:USDC bars (refusing synthetic fallback when use_real_history=true)",
                    backfill_file.display()
                )));
            }
        }

        let total_needed = warmup_bars + replay_bars;
        let btc_sym = "BTC/USDC:USDC";

        let btc_bars = if let Some(real_btc) = loaded_real.get(btc_sym) {
            if real_btc.len() >= total_needed {
                real_btc[real_btc.len() - total_needed..].to_vec()
            } else {
                let missing = total_needed - real_btc.len();
                let mut warm =
                    Self::generate_warmup_bars(btc_sym, "4h", &real_btc[0], missing, 1);
                warm.extend_from_slice(real_btc);
                warm
            }
        } else if use_real_history {
            return Err(AegisError::Validation(
                "Missing real historical candles for BTC/USDC:USDC".to_string(),
            ));
        } else {
            let btc_ref_px = bt_db.resolve_reference_price(btc_sym)?.ok_or_else(|| {
                AegisError::Validation("Missing BTC reference price in universe_assets".to_string())
            })?;
            ingestion.generate_synthetic_bars(
                btc_sym,
                "4h",
                total_needed,
                btc_ref_px,
                None,
                1,
                None,
            )
        };
        bt_db.upsert_ohlcv_batch(&btc_bars)?;

        let mut usdc_map: HashMap<String, Vec<OhlcvBar>> = HashMap::new();
        let mut synth_map: HashMap<String, Vec<OhlcvBar>> = HashMap::new();

        for (idx, asset) in usdc_assets.iter().enumerate() {
            if asset.base_asset == "BTC" {
                continue;
            }
            let bars = if let Some(real_alt) = loaded_real.get(&asset.symbol) {
                if real_alt.len() >= total_needed {
                    real_alt[real_alt.len() - total_needed..].to_vec()
                } else {
                    let missing = total_needed - real_alt.len();
                    let mut warm = Self::generate_warmup_bars(
                        &asset.symbol,
                        "4h",
                        &real_alt[0],
                        missing,
                        (idx + 2) as u64,
                    );
                    warm.extend_from_slice(real_alt);
                    warm
                }
            } else if use_real_history {
                return Err(AegisError::Validation(format!(
                    "Missing real historical candles for {} in verified backfill dataset",
                    asset.symbol
                )));
            } else {
                let resolved_ref = bt_db
                    .resolve_reference_price(&asset.symbol)?
                    .unwrap_or(asset.reference_price);
                ingestion.generate_synthetic_bars(
                    &asset.symbol,
                    "4h",
                    total_needed,
                    asset.reference_price.max(resolved_ref),
                    Some(&btc_bars),
                    (idx + 2) as u64,
                    btc_bars.last().map(|b| b.timestamp_ms),
                )
            };
            bt_db.upsert_ohlcv_batch(&bars)?;
            usdc_map.insert(asset.symbol.clone(), bars);
        }

        for pair in &alt_pairs {
            if let Some(alt_bars) = usdc_map.get(&pair.exec_symbol) {
                let s_bars = MarketIngestionEngine::synthesize_alt_btc_candles(
                    alt_bars,
                    &btc_bars,
                    &pair.synth_symbol,
                    "4h",
                );
                bt_db.upsert_ohlcv_batch(&s_bars)?;
                synth_map.insert(pair.synth_symbol.clone(), s_bars);
            }
        }

        Ok((btc_bars, usdc_map, synth_map))
    }

    /// Runs the full point-in-time historical simulation in an isolated `:memory:` SQLite database.
    pub fn run_backtest(
        &self,
        total_bars: Option<usize>,
        warmup_bars: Option<usize>,
        use_real_history: bool,
    ) -> Result<BacktestTearSheet> {
        let bt_db = self.db.clone_to_memory()?;
        let cfg = bt_db.load_config()?;

        let default_warmup = cfg.get_usize("backtest_warmup_bars", 110).max(60);
        let replay_bars = total_bars.unwrap_or(self.days * 6).max(60);
        let warmup = warmup_bars.unwrap_or(default_warmup).max(60);

        let (btc_bars, usdc_map, synth_map) =
            self.load_historical_dataset(&bt_db, replay_bars, warmup, use_real_history)?;

        let (tear_sheet, _, _) =
            Self::execute_pit_replay(&bt_db, &btc_bars, &usdc_map, &synth_map, warmup)?;
        Ok(tear_sheet)
    }

    /// Executes strict point-in-time bar replay over pre-loaded bars (`B6`, `C4`, `C6`).
    /// Returns `(BacktestTearSheet, per_bar_signal_evaluations, per_bar_open_trades)` for causality verification.
    #[allow(clippy::type_complexity)]
    pub fn execute_pit_replay(
        bt_db: &DatabaseManager,
        btc_bars: &[OhlcvBar],
        usdc_map: &HashMap<String, Vec<OhlcvBar>>,
        synth_map: &HashMap<String, Vec<OhlcvBar>>,
        warmup: usize,
    ) -> Result<(
        BacktestTearSheet,
        Vec<Vec<SignalEvaluation>>,
        Vec<Vec<TradeRecord>>,
    )> {
        let cfg = bt_db.load_config()?;
        let scfg = aegis_signals::SignalConfig::from_config_map(&cfg);
        let n_total = btc_bars.len();
        let eval_start = warmup.min(n_total.saturating_sub(10));

        let signals = SignalEngine::new(bt_db.clone());
        let risk = RiskEngine::new(bt_db.clone());
        let exec = ExecutionEngine::new(bt_db.clone());

        let hmm_window = SignalEngine::resolve_hmm_window(&cfg);
        let snr_fast = cfg.get_usize("snr_ema_fast_period", cfg.get_usize("snr_fast_span", 16));
        let snr_slow = cfg.get_usize("snr_ema_slow_period", cfg.get_usize("snr_slow_span", 64));
        let rs_fast = cfg.get_usize("rs_lookback_fast_bars", 24);
        let rs_mid = cfg.get_usize("rs_lookback_mid_bars", 72);
        let rs_slow = cfg.get_usize("rs_lookback_slow_bars", 96);
        let rs_u8 = cfg.get_usize(
            "rs_usdc_momentum_bars",
            cfg.get_usize("rs_usdc_short_lookback_bars", 8),
        );
        let atr_period = cfg
            .get_usize("atr_period", cfg.get_usize("atr_default_period", 14))
            .max(1);
        let base_mace_w = cfg.get_f64(
            "mace_conviction_weight",
            cfg.get_f64("mace_blend_weight", 0.35),
        );
        let long_thresh = cfg.get_f64(
            "conviction_long_threshold",
            cfg.get_f64("bull_conviction_gate", 0.32),
        );
        let short_thresh = cfg.get_f64(
            "conviction_short_threshold",
            cfg.get_f64("bear_conviction_gate", -0.32),
        );
        let chop_thresh = cfg.get_f64("hmm_chop_prob_threshold", 0.55);
        let basis_harvest_enabled = cfg.get_bool("basis_harvest_enabled", true);
        let basis_floor = cfg.get_f64(
            "basis_harvest_min_funding_hourly",
            cfg.get_f64("basis_min_hourly_funding_rate", 0.000015),
        );
        let basis_alloc_pct = cfg.get_f64("basis_max_idle_cash_allocation_pct", 0.50);
        let bear_flip_prob = cfg.get_f64("execution_regime_flip_bear_prob", 0.62);
        let bull_flip_prob = cfg.get_f64("execution_regime_flip_bull_prob", 0.60);
        let chop_conv_max = cfg.get_f64("execution_high_chop_conviction_max", 0.08);
        let chop_prob_offset = cfg.get_f64("execution_high_chop_prob_offset", 0.20);
        let chop_prob_cap = cfg.get_f64("execution_high_chop_prob_cap", 0.82);
        let rs_breakdown_long_snr = cfg.get_f64("execution_rs_breakdown_long_snr", -0.10);
        let rs_reversal_short_snr = cfg.get_f64("execution_rs_reversal_short_snr", 0.10);
        let cooldown_bars = cfg
            .get_usize(
                "execution_stop_cooldown_bars",
                cfg.get_usize("post_stop_cooldown_bars", 14),
            )
            .max(1);
        let max_longs = cfg.get_usize("max_concurrent_long_positions", 2);
        let max_shorts = cfg.get_usize("max_concurrent_short_positions", 2);
        let initial_equity = cfg.get_f64("initial_equity_usdc", 10000.0);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);

        let btc_ts: Vec<i64> = btc_bars.iter().map(|b| b.timestamp_ms).collect();
        let alt_pc1_series =
            SignalEngine::compute_alt_pc1_series(usdc_map, &btc_ts, hmm_window);
        let regime_states: Vec<RegimeState> =
            signals.decode_hmm_regime_states(btc_bars, hmm_window, Some(&alt_pc1_series), None);

        let usdc_assets = bt_db.get_usdc_perp_universe()?;
        let asset_by_sym: HashMap<String, UniverseAsset> = usdc_assets
            .into_iter()
            .map(|a| (a.symbol.clone(), a))
            .collect();
        let alt_pairs = bt_db.get_alt_pairs_universe()?;

        // Parallel feature precomputation across all 15 altcoin pairs using Rayon
        let precomputed: Vec<PrecomputedPairSeries> = alt_pairs
            .into_par_iter()
            .filter_map(|pair| {
                let asset = asset_by_sym.get(&pair.exec_symbol)?.clone();
                let u_bars = usdc_map.get(&pair.exec_symbol)?.clone();
                let s_bars = synth_map.get(&pair.synth_symbol)?.clone();
                if u_bars.len() < n_total || s_bars.len() < n_total {
                    return None;
                }
                let (snr_series, snr_synth_series) =
                    SignalEngine::compute_continuous_snr_series_with_config(
                        &s_bars, &u_bars, snr_fast, snr_slow, &scfg,
                    );
                let (mace_series, ic_series) = if let Some((x_raw, y_target, k)) =
                    SignalEngine::build_mace_features_and_target_with_config(&s_bars, &u_bars, &scfg)
                {
                    let local_sig = SignalEngine::new(bt_db.clone());
                    local_sig.fit_predict_mace_series_from_xy(&x_raw, &y_target, k, None)
                } else {
                    (vec![0.0; n_total], vec![0.0; n_total])
                };

                let s_closes: Vec<f64> = s_bars.iter().map(|b| b.close.max(1e-12)).collect();
                let u_closes: Vec<f64> = u_bars.iter().map(|b| b.close.max(1e-12)).collect();

                let mut rs_composite_series = vec![0.0; n_total];
                let mut rs_96_series = vec![0.0; n_total];
                let mut r_usdc_8_series = vec![0.0; n_total];

                for t in 0..n_total {
                    // C6: Require full lookbacks via compute_rs_lookbacks instead of saturating_sub clamping
                    let (rs_24, rs_72, rs_96, r_u8) = aegis_signals::compute_rs_lookbacks(
                        &s_closes[..=t],
                        &u_closes[..=t],
                        rs_fast,
                        rs_mid,
                        rs_slow,
                        rs_u8,
                    );
                    let snr_s = snr_synth_series.get(t).copied().unwrap_or(0.0);
                    rs_composite_series[t] = scfg.rs_composite_snr_weight * snr_s
                        + scfg.rs_composite_fast_weight * (rs_24 * scfg.rs_composite_fast_scale).tanh()
                        + scfg.rs_composite_mid_weight * (rs_72 * scfg.rs_composite_mid_scale).tanh();
                    rs_96_series[t] = rs_96;
                    r_usdc_8_series[t] = r_u8;
                }

                Some(PrecomputedPairSeries {
                    pair,
                    asset,
                    usdc_bars: u_bars,
                    snr_series,
                    snr_synth_series,
                    mace_series,
                    ic_series,
                    rs_composite_series,
                    rs_96_series,
                    r_usdc_8_series,
                })
            })
            .collect();

        let mut equity_curve = Vec::with_capacity(n_total - eval_start);
        let mut bar_returns = Vec::with_capacity(n_total - eval_start);
        let mut per_bar_evals = Vec::with_capacity(n_total - eval_start);
        let mut per_bar_open_trades = Vec::with_capacity(n_total - eval_start);
        let mut prev_equity = initial_equity;
        let mut accrued_yield_usdc = 0.0;
        let mut total_slippage_usdc = 0.0;
        let mut wick_stop_outs = 0usize;
        let mut bull_pnl = 0.0;
        let mut bear_pnl = 0.0;
        let mut chop_pnl = 0.0;
        let mut bull_bars = 0usize;
        let mut bear_bars = 0usize;
        let mut chop_bars = 0usize;
        let mut cooldown_until_bar: HashMap<String, usize> = HashMap::new();

        for t in eval_start..n_total {
            let now_ms = btc_bars[t].timestamp_ms;
            bt_db.set_pit_timestamp_ms(Some(now_ms));

            let regime = regime_states.get(t).cloned().unwrap_or_default();
            match regime.regime.as_str() {
                "BULL" => bull_bars += 1,
                "BEAR" => bear_bars += 1,
                _ => chop_bars += 1,
            }

            let btc_macro =
                SignalEngine::compute_btc_macro_confirmation_with_config(&btc_bars[..=t], &scfg);

            let mut order_indices: Vec<usize> = (0..precomputed.len()).collect();
            order_indices.sort_by(|&i, &j| {
                precomputed[j].rs_composite_series[t]
                    .partial_cmp(&precomputed[i].rs_composite_series[t])
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let total_assets = order_indices.len();
            let mut evals_t = Vec::with_capacity(total_assets);
            let mut eval_by_sym: HashMap<String, SignalEvaluation> = HashMap::new();

            for (rank_zero, &p_idx) in order_indices.iter().enumerate() {
                let p = &precomputed[p_idx];
                let rank = rank_zero + 1;
                let snr = p.snr_series[t];
                let snr_s = p.snr_synth_series[t];
                let mace = p.mace_series[t];
                let ic = p.ic_series[t];
                let dyn_w =
                    SignalEngine::compute_dynamic_mace_weight_with_config(ic, base_mace_w, &scfg);
                let blended = round_dp(((1.0 - dyn_w) * snr + dyn_w * mace).clamp(-1.0, 1.0), 4);
                let funding = p.usdc_bars[t].funding_rate_hourly;

                let action = SignalEngine::classify_signal_action_with_config(
                    &regime.regime,
                    regime.prob_bull,
                    regime.prob_bear,
                    regime.prob_chop,
                    blended,
                    snr,
                    snr_s,
                    mace,
                    p.rs_96_series[t],
                    p.r_usdc_8_series[t],
                    rank,
                    total_assets,
                    funding,
                    long_thresh,
                    short_thresh,
                    chop_thresh,
                    basis_floor,
                    btc_macro,
                    &scfg,
                );

                let ev = SignalEvaluation {
                    eval_id: None,
                    timestamp_ms: now_ms,
                    base_asset: p.pair.base_asset.clone(),
                    exec_symbol: p.pair.exec_symbol.clone(),
                    synth_symbol: p.pair.synth_symbol.clone(),
                    hmm_regime: regime.regime.clone(),
                    prob_bull: regime.prob_bull,
                    prob_bear: regime.prob_bear,
                    prob_chop: regime.prob_chop,
                    snr_score: snr,
                    snr_synth: snr_s,
                    rs_96: p.rs_96_series[t],
                    r_usdc_8: p.r_usdc_8_series[t],
                    mace_score: mace,
                    blended_conviction: blended,
                    rs_rank: rank as i64,
                    signal_action: action,
                    funding_rate_hourly: funding,
                    created_at: None,
                };
                eval_by_sym.insert(ev.exec_symbol.clone(), ev.clone());
                evals_t.push(ev);
            }

            // 1. Update & evaluate stops on open trades
            let open_trades = bt_db.get_open_trades()?;
            for trade in open_trades {
                let Some(p) = precomputed.iter().find(|x| x.pair.exec_symbol == trade.symbol)
                else {
                    continue;
                };
                let bar = &p.usdc_bars[t];
                let atr_slice_start = t.saturating_sub(atr_period + 1);
                let atr = risk.compute_atr(&p.usdc_bars[atr_slice_start..=t], atr_period);

                let stop_eval = risk.evaluate_two_tier_chandelier_stop(
                    &trade.side,
                    trade.entry_price,
                    trade.quantity,
                    trade.chandelier_stop_price,
                    trade.highest_price_seen,
                    trade.lowest_price_seen,
                    bar.high,
                    bar.low,
                    bar.close,
                    atr,
                    p.asset.tick_size,
                    Some(&cfg),
                );

                let ev_opt = eval_by_sym.get(&trade.symbol);
                let mut exit_reason = stop_eval.exit_reason.clone();
                let mut exit_px = if stop_eval.triggered {
                    stop_eval.exit_fill_price
                } else {
                    bar.close
                };

                if exit_reason.is_none() {
                    if let Some(ev) = ev_opt {
                        if trade.side == "LONG"
                            && ev.hmm_regime == "BEAR"
                            && ev.prob_bear >= bear_flip_prob
                        {
                            exit_reason = Some("REGIME_FLIP_BEAR".to_string());
                            exit_px = bar.close;
                        } else if trade.side == "LONG" && ev.snr_synth < rs_breakdown_long_snr {
                            exit_reason = Some("RS_SYNTH_BREAKDOWN_LONG".to_string());
                            exit_px = bar.close;
                        } else if trade.side == "SHORT"
                            && ev.hmm_regime == "BULL"
                            && ev.prob_bull >= bull_flip_prob
                        {
                            exit_reason = Some("REGIME_FLIP_BULL".to_string());
                            exit_px = bar.close;
                        } else if trade.side == "SHORT" && ev.snr_synth > rs_reversal_short_snr {
                            exit_reason = Some("RS_SYNTH_REVERSAL_SHORT".to_string());
                            exit_px = bar.close;
                        } else if ev.prob_chop >= (chop_thresh + chop_prob_offset).min(chop_prob_cap)
                            && ev.blended_conviction.abs() < chop_conv_max
                        {
                            exit_reason = Some("HIGH_CHOP_DELEVERAGE".to_string());
                            exit_px = bar.close;
                        }
                    }
                }

                let funding_4h = if trade.side == "LONG" {
                    -(trade.quantity * bar.close) * bar.funding_rate_hourly * 4.0
                } else {
                    (trade.quantity * bar.close) * bar.funding_rate_hourly * 4.0
                };
                let new_funding = round_dp(trade.funding_accrued_usdc + funding_4h, 4);

                if let Some(reason) = exit_reason {
                    if stop_eval.is_wick_stop {
                        wick_stop_outs += 1;
                    }
                    total_slippage_usdc += stop_eval.exit_slippage_usdc;
                    let exit_fee = (trade.quantity * exit_px) * (taker_fee_bps / 10000.0);
                    let total_fees = round_dp(trade.fees_paid_usdc + exit_fee, 4);
                    let gross_pnl = if trade.side == "LONG" {
                        (exit_px - trade.entry_price) * trade.quantity
                    } else {
                        (trade.entry_price - exit_px) * trade.quantity
                    };
                    let net_pnl = round_dp(gross_pnl - total_fees + new_funding, 4);
                    bt_db.close_trade(trade.trade_id, exit_px, net_pnl, total_fees, &reason)?;
                    if stop_eval.triggered {
                        cooldown_until_bar.insert(trade.symbol.clone(), t + cooldown_bars);
                    }
                } else {
                    let upnl = if trade.side == "LONG" {
                        (bar.close - trade.entry_price) * trade.quantity
                    } else {
                        (trade.entry_price - bar.close) * trade.quantity
                    };
                    bt_db.update_open_trade(
                        trade.trade_id,
                        bar.close,
                        stop_eval.chandelier_stop_price,
                        stop_eval.highest_price_seen,
                        stop_eval.lowest_price_seen,
                        round_dp(upnl, 4),
                        new_funding,
                    )?;
                }
            }

            // 2. Evaluate new directional entries
            let mut active_trades = bt_db.get_open_trades()?;
            let realized_pnl = bt_db.get_total_realized_pnl()?;
            let cur_upnl: f64 = active_trades
                .iter()
                .map(|tr| tr.unrealized_pnl_usdc + tr.funding_accrued_usdc)
                .sum();
            let cur_equity =
                (initial_equity + realized_pnl + cur_upnl + accrued_yield_usdc).max(100.0);

            for (ev_idx, ev) in evals_t.iter().enumerate() {
                if ev.signal_action != "LONG" && ev.signal_action != "SHORT" {
                    continue;
                }
                if active_trades.iter().any(|tr| tr.symbol == ev.exec_symbol) {
                    continue;
                }
                let cur_longs = active_trades.iter().filter(|tr| tr.side == "LONG").count();
                let cur_shorts = active_trades.iter().filter(|tr| tr.side == "SHORT").count();
                if ev.signal_action == "LONG" && cur_longs >= max_longs {
                    continue;
                }
                if ev.signal_action == "SHORT" && cur_shorts >= max_shorts {
                    continue;
                }
                if let Some(&cd_bar) = cooldown_until_bar.get(&ev.exec_symbol) {
                    if t < cd_bar {
                        continue;
                    }
                }
                let Some(p) = precomputed.iter().find(|x| x.pair.exec_symbol == ev.exec_symbol)
                else {
                    continue;
                };
                let bar = &p.usdc_bars[t];
                let atr_slice_start = t.saturating_sub(atr_period + 1);
                let atr = risk.compute_atr(&p.usdc_bars[atr_slice_start..=t], atr_period);

                let sizing = risk.evaluate_order_risk(
                    &p.asset,
                    &ev.signal_action,
                    bar.close,
                    atr,
                    cur_equity,
                    Some(&active_trades),
                    ev.blended_conviction,
                    0.0,
                );
                if !sizing.approved {
                    continue;
                }

                let det_cid = format!("bt-{}-{}-{}", p.asset.base_asset, t, ev_idx);
                let fill = exec.simulate_paper_fill(
                    &p.asset.symbol,
                    &ev.signal_action,
                    sizing.quantity,
                    bar.close,
                    p.asset.tick_size,
                    p.asset.lot_step_size,
                    bar.high,
                    bar.low,
                    bar.volume,
                    false,
                    true,
                    None,
                    Some(&det_cid),
                );
                if fill.fsm_state != "FILLED" || fill.filled_units <= 0.0 {
                    continue;
                }
                total_slippage_usdc += fill.slippage_paid_usd;

                let rec = TradeRecord {
                    trade_id: 0,
                    client_order_id: fill.client_order_id,
                    symbol: p.asset.symbol.clone(),
                    base_asset: p.asset.base_asset.clone(),
                    side: ev.signal_action.clone(),
                    status: "OPEN".to_string(),
                    fsm_state: "FILLED".to_string(),
                    entry_price: fill.avg_fill_price,
                    current_price: fill.avg_fill_price,
                    exit_price: None,
                    quantity: fill.filled_units,
                    filled_units: fill.filled_units,
                    avg_fill_price: Some(fill.avg_fill_price),
                    notional_usdc: round_dp(fill.filled_units * fill.avg_fill_price, 2),
                    risk_budget_usdc: sizing.risk_budget_usdc,
                    conviction_multiplier: sizing.conviction_multiplier,
                    initial_stop_price: sizing.initial_stop_price,
                    chandelier_stop_price: sizing.initial_stop_price,
                    current_stop_price: sizing.initial_stop_price,
                    highest_price_seen: fill.avg_fill_price,
                    lowest_price_seen: fill.avg_fill_price,
                    realized_pnl_usdc: 0.0,
                    unrealized_pnl_usdc: 0.0,
                    fees_paid_usdc: fill.fees_paid_usdc,
                    funding_accrued_usdc: 0.0,
                    slippage_paid_usd: fill.slippage_paid_usd,
                    exchange_order_id: Some(fill.exchange_order_id),
                    opened_at: String::new(),
                    closed_at: None,
                    exit_reason: None,
                };
                let tid = bt_db.insert_trade(&rec)?;
                let mut inserted = rec;
                inserted.trade_id = tid;
                active_trades.push(inserted);
            }

            // 3. Basis Harvest & Snapshot accounting (R3-H6, R3-N8)
            let deployed_notional: f64 = active_trades
                .iter()
                .map(|tr| tr.quantity * tr.current_price)
                .sum();
            let total_upnl: f64 = active_trades
                .iter()
                .map(|tr| tr.unrealized_pnl_usdc + tr.funding_accrued_usdc)
                .sum();
            let realized_after = bt_db.get_total_realized_pnl()?;
            let avail_cash = (initial_equity + realized_after + total_upnl + accrued_yield_usdc
                - deployed_notional)
                .max(0.0);

            let mut basis_notional_t = 0.0;
            if basis_harvest_enabled {
                let basis_rates: Vec<f64> = evals_t
                    .iter()
                    .filter(|e| e.signal_action == "BASIS_HARVEST")
                    .map(|e| e.funding_rate_hourly)
                    .collect();
                if (regime.regime == "CHOP" || regime.prob_chop >= chop_thresh)
                    && !basis_rates.is_empty()
                {
                    let (notional, yield_delta) = aegis_core::compute_basis_harvest_accrual(
                        avail_cash,
                        basis_alloc_pct,
                        basis_floor,
                        &basis_rates,
                        true,
                    );
                    basis_notional_t = notional;
                    accrued_yield_usdc = round_dp(accrued_yield_usdc + yield_delta, 4);
                }
            }

            let bar_equity =
                (initial_equity + realized_after + total_upnl + accrued_yield_usdc).max(100.0);
            let bar_pnl_delta = bar_equity - prev_equity;
            match regime.regime.as_str() {
                "BULL" => bull_pnl += bar_pnl_delta,
                "BEAR" => bear_pnl += bar_pnl_delta,
                _ => chop_pnl += bar_pnl_delta,
            }
            let bar_ret = (bar_equity - prev_equity) / prev_equity.max(1.0);
            bar_returns.push(bar_ret);
            equity_curve.push(bar_equity);
            per_bar_evals.push(evals_t);
            per_bar_open_trades.push(active_trades.clone());
            prev_equity = bar_equity;

            if t == n_total - 1 {
                let snap = PortfolioSnapshot {
                    snapshot_id: None,
                    timestamp_ms: now_ms,
                    total_equity_usdc: round_dp(bar_equity, 2),
                    available_cash_usdc: round_dp((bar_equity - deployed_notional).max(0.0), 2),
                    deployed_notional_usdc: round_dp(deployed_notional, 2),
                    gross_leverage: round_dp(deployed_notional / bar_equity.max(1.0), 4),
                    portfolio_heat_usdc: 0.0,
                    portfolio_heat_pct: 0.0,
                    basis_harvest_notional_usdc: round_dp(basis_notional_t, 2),
                    accrued_yield_usdc: round_dp(accrued_yield_usdc, 2),
                    hmm_regime: regime.regime,
                    prob_bull: regime.prob_bull,
                    prob_bear: regime.prob_bear,
                    prob_chop: regime.prob_chop,
                    open_long_count: active_trades.iter().filter(|x| x.side == "LONG").count()
                        as i64,
                    open_short_count: active_trades.iter().filter(|x| x.side == "SHORT").count()
                        as i64,
                    created_at: None,
                };
                let _ = bt_db.save_portfolio_snapshot(&snap);
            }
        }

        // Force-close remaining open trades at final mark price with exit slippage and taker fee (R3-N7)
        let base_slip_bps = cfg.get_f64("base_slippage_bps", 2.0).max(0.0);
        let remaining_open = bt_db.get_open_trades()?;
        let mut final_exit_friction_usdc = 0.0;
        for tr in remaining_open {
            let slip_mult = if tr.side == "LONG" {
                1.0 - base_slip_bps / 10_000.0
            } else {
                1.0 + base_slip_bps / 10_000.0
            };
            let exit_px = round_dp(tr.current_price * slip_mult, 8);
            let exit_slip_usdc = (tr.current_price - exit_px).abs() * tr.quantity;
            let exit_fee_usdc = (tr.quantity * exit_px) * (taker_fee_bps / 10_000.0);
            let total_fees = round_dp(tr.fees_paid_usdc + exit_fee_usdc, 4);
            total_slippage_usdc += exit_slip_usdc;
            final_exit_friction_usdc += exit_slip_usdc + exit_fee_usdc;
            let gross_pnl = if tr.side == "LONG" {
                (exit_px - tr.entry_price) * tr.quantity
            } else {
                (tr.entry_price - exit_px) * tr.quantity
            };
            let final_pnl = round_dp(gross_pnl - total_fees + tr.funding_accrued_usdc, 4);
            bt_db.close_trade(
                tr.trade_id,
                exit_px,
                final_pnl,
                total_fees,
                "END_OF_BACKTEST_MARK",
            )?;
        }
        if final_exit_friction_usdc > 0.0 {
            if let Some(last_eq) = equity_curve.last_mut() {
                *last_eq = (*last_eq - final_exit_friction_usdc).max(100.0);
            }
        }

        let all_trades = bt_db.get_recent_trades(10_000)?;
        let closed_trades: Vec<&TradeRecord> = all_trades
            .iter()
            .filter(|tr| tr.status == "CLOSED")
            .collect();

        let total_trades = closed_trades.len();
        let winning_trades = closed_trades
            .iter()
            .filter(|tr| tr.realized_pnl_usdc > 0.0)
            .count();
        let losing_trades = closed_trades
            .iter()
            .filter(|tr| tr.realized_pnl_usdc <= 0.0)
            .count();
        let gross_profit: f64 = closed_trades
            .iter()
            .filter(|tr| tr.realized_pnl_usdc > 0.0)
            .map(|tr| tr.realized_pnl_usdc)
            .sum();
        let gross_loss: f64 = closed_trades
            .iter()
            .filter(|tr| tr.realized_pnl_usdc < 0.0)
            .map(|tr| tr.realized_pnl_usdc.abs())
            .sum();
        let total_fees: f64 = closed_trades.iter().map(|tr| tr.fees_paid_usdc).sum();
        let funding_net: f64 = closed_trades.iter().map(|tr| tr.funding_accrued_usdc).sum();

        let final_equity = equity_curve.last().copied().unwrap_or(initial_equity);
        let total_return_pct = ((final_equity - initial_equity) / initial_equity) * 100.0;
        let bars_replayed = equity_curve.len();
        let duration_days = (bars_replayed as f64) / 6.0;
        let years = (duration_days / 365.0).max(1.0 / 365.0);
        let annualized_return_pct =
            ((final_equity / initial_equity).max(0.01).powf(1.0 / years) - 1.0) * 100.0;

        let n_r = bar_returns.len().max(1) as f64;
        let mean_r = bar_returns.iter().sum::<f64>() / n_r;
        let var_r = bar_returns
            .iter()
            .map(|r| (r - mean_r).powi(2))
            .sum::<f64>()
            / (bar_returns.len().saturating_sub(1).max(1) as f64);
        let std_r = var_r.sqrt();
        let ann_factor = (2190.0_f64).sqrt();
        let annualized_sharpe = if std_r > 1e-12 {
            (mean_r / std_r) * ann_factor
        } else {
            0.0
        };

        let downside_var = bar_returns
            .iter()
            .filter(|&&r| r < 0.0)
            .map(|r| r * r)
            .sum::<f64>()
            / n_r;
        let downside_std = downside_var.sqrt();
        let sortino_ratio = if downside_std > 1e-12 {
            (mean_r / downside_std) * ann_factor
        } else {
            annualized_sharpe
        };

        let mut peak = initial_equity;
        let mut max_dd = 0.0;
        for &eq in &equity_curve {
            if eq > peak {
                peak = eq;
            }
            let dd = (peak - eq) / peak.max(1.0);
            if dd > max_dd {
                max_dd = dd;
            }
        }
        let max_drawdown_pct = max_dd * 100.0;
        let calmar_ratio = if max_drawdown_pct > 0.01 {
            annualized_return_pct / max_drawdown_pct
        } else {
            annualized_return_pct
        };

        // R3-H7: Compute trade_profit_factor strictly from closed trade PnLs (without accrued_yield_usdc or .max(bar_pf))
        let trade_pf = if gross_loss > 1e-6 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            9.99
        } else {
            1.0
        };
        let profit_factor = trade_pf;
        let win_rate_pct = if total_trades > 0 {
            (winning_trades as f64 / total_trades as f64) * 100.0
        } else {
            0.0
        };
        let fee_drag_pct = if (gross_profit + accrued_yield_usdc) > 1.0 {
            (total_fees / (gross_profit + accrued_yield_usdc)) * 100.0
        } else {
            0.0
        };

        let start_ts = btc_bars[eval_start].timestamp_ms;
        let end_ts = btc_bars[n_total - 1].timestamp_ms;
        let start_date = Utc
            .timestamp_millis_opt(start_ts)
            .single()
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "2024-01-01".to_string());
        let end_date = Utc
            .timestamp_millis_opt(end_ts)
            .single()
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "2025-01-01".to_string());

        let min_sharpe = cfg.get_f64("backtest_gate_min_sharpe", 1.20);
        let max_mdd = cfg.get_f64("backtest_gate_max_mdd_pct", 20.0);
        let min_pf = cfg.get_f64("backtest_gate_min_profit_factor", 1.30);

        let gate_sharpe_pass = annualized_sharpe >= min_sharpe;
        let gate_mdd_pass = max_drawdown_pct <= max_mdd;
        let gate_profit_factor_pass = trade_pf >= min_pf;
        let all_gates_passed = gate_sharpe_pass && gate_mdd_pass && gate_profit_factor_pass;

        Ok((
            BacktestTearSheet {
                start_date,
                end_date,
                duration_days: round_dp(duration_days, 1),
                bars_replayed,
                initial_equity_usdc: round_dp(initial_equity, 2),
                final_equity_usdc: round_dp(final_equity, 2),
                total_return_pct: round_dp(total_return_pct, 2),
                annualized_return_pct: round_dp(annualized_return_pct, 2),
                annualized_sharpe: round_dp(annualized_sharpe, 2),
                sortino_ratio: round_dp(sortino_ratio, 2),
                calmar_ratio: round_dp(calmar_ratio, 2),
                max_drawdown_pct: round_dp(max_drawdown_pct, 2),
                profit_factor: round_dp(profit_factor, 2),
                trade_profit_factor: round_dp(trade_pf, 2),
                win_rate_pct: round_dp(win_rate_pct, 2),
                total_trades,
                winning_trades,
                losing_trades,
                gross_profit_usdc: round_dp(gross_profit, 2),
                gross_loss_usdc: round_dp(gross_loss, 2),
                total_fees_usdc: round_dp(total_fees, 2),
                total_slippage_usdc: round_dp(total_slippage_usdc, 2),
                wick_stop_outs,
                fee_drag_pct: round_dp(fee_drag_pct, 2),
                funding_net_usdc: round_dp(funding_net, 2),
                basis_yield_usdc: round_dp(accrued_yield_usdc, 2),
                bull_regime_pnl_usdc: round_dp(bull_pnl, 2),
                bear_regime_pnl_usdc: round_dp(bear_pnl, 2),
                chop_regime_pnl_usdc: round_dp(chop_pnl, 2),
                bull_bars_count: bull_bars,
                bear_bars_count: bear_bars,
                chop_bars_count: chop_bars,
                gate_sharpe_pass,
                gate_mdd_pass,
                gate_profit_factor_pass,
                all_gates_passed,
            },
            per_bar_evals,
            per_bar_open_trades,
        ))
    }

    /// Formats the institutional ASCII Tear Sheet (`engine/backtest.py::format_tear_sheet`).
    pub fn format_tear_sheet(ts: &BacktestTearSheet) -> String {
        let s_gate = if ts.gate_sharpe_pass { "PASS" } else { "FAIL" };
        let m_gate = if ts.gate_mdd_pass { "PASS" } else { "FAIL" };
        let p_gate = if ts.gate_profit_factor_pass {
            "PASS"
        } else {
            "FAIL"
        };
        let overall = if ts.all_gates_passed {
            "ALL INSTITUTIONAL ACCEPTANCE GATES PASSED"
        } else {
            "ONE OR MORE ACCEPTANCE GATES FAILED"
        };

        format!(
            "==============================================================================\n\
             AEGIS QUANT v0.3.0-rc1 — POINT-IN-TIME HISTORICAL BACKTEST TEAR SHEET\n\
             ==============================================================================\n\
             Window         : {} -> {} ({:.1} days | {} 4H bars)\n\
             Initial Equity : ${:.2} USDC\n\
             Final Equity   : ${:.2} USDC ({:+.2}% Total | {:+.2}% Annualized)\n\
             ------------------------------------------------------------------------------\n\
             RISK-ADJUSTED PERFORMANCE & ACCEPTANCE GATES\n\
             ------------------------------------------------------------------------------\n\
             Annualized Sharpe Ratio : {:6.2}   [Gate >= 1.20 : {}]\n\
             Maximum Drawdown (MDD)  : {:5.2}%   [Gate <= 20.0%: {}]\n\
             Profit Factor           : {:6.2}   [Gate >= 1.30 : {}] (Trade PF: {:.2})\n\
             Sortino Ratio           : {:6.2}   | Calmar Ratio : {:.2}\n\
             ------------------------------------------------------------------------------\n\
             EXECUTION, FRICTION & TWO-TIER CHANDELIER TELEMETRY\n\
             ------------------------------------------------------------------------------\n\
             Total Closed Trades     : {} ({} Wins / {} Losses | Win Rate: {:.1}%)\n\
             Gross Profit / Loss     : +${:.2} / -${:.2}\n\
             Exchange Fees + Slippage: ${:.2} Fees + ${:.2} Slippage (Fee Drag: {:.2}%)\n\
             Intrabar Wick Stop-Outs : {}   | Funding Net: ${:+.2} | Basis Yield: +${:.2}\n\
             ------------------------------------------------------------------------------\n\
             3-STATE GAUSSIAN HMM REGIME ATTRIBUTION\n\
             ------------------------------------------------------------------------------\n\
             BULL Regime ({:4} bars) : ${:+.2} USDC\n\
             BEAR Regime ({:4} bars) : ${:+.2} USDC\n\
             CHOP Regime ({:4} bars) : ${:+.2} USDC\n\
             ==============================================================================\n\
             VERDICT: {}\n\
             ==============================================================================",
            ts.start_date,
            ts.end_date,
            ts.duration_days,
            ts.bars_replayed,
            ts.initial_equity_usdc,
            ts.final_equity_usdc,
            ts.total_return_pct,
            ts.annualized_return_pct,
            ts.annualized_sharpe,
            s_gate,
            ts.max_drawdown_pct,
            m_gate,
            ts.profit_factor,
            p_gate,
            ts.trade_profit_factor,
            ts.sortino_ratio,
            ts.calmar_ratio,
            ts.total_trades,
            ts.winning_trades,
            ts.losing_trades,
            ts.win_rate_pct,
            ts.gross_profit_usdc,
            ts.gross_loss_usdc,
            ts.total_fees_usdc,
            ts.total_slippage_usdc,
            ts.fee_drag_pct,
            ts.wick_stop_outs,
            ts.funding_net_usdc,
            ts.basis_yield_usdc,
            ts.bull_bars_count,
            ts.bull_regime_pnl_usdc,
            ts.bear_bars_count,
            ts.bear_regime_pnl_usdc,
            ts.chop_bars_count,
            ts.chop_regime_pnl_usdc,
            overall,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_real_4h_backfill_schema_and_file() {
        // 1. Parse nested production schema directly
        let sample = json!({
            "server_max_ts": 1792771200000i64,
            "target_end_ts": 1792785600000i64,
            "coins": {
                "BTC": {
                    "source": "hyperliquid",
                    "funding_rate_hourly": 0.000015,
                    "count": 2,
                    "candles": [
                        {"t": 1730419200000i64, "o": 70364.0, "h": 70522.0, "l": 68928.0, "c": 69417.0, "v": 2457.92721},
                        {"t": 1730433600000i64, "o": 69417.0, "h": 69730.0, "l": 69111.0, "c": 69304.0, "v": 1076.2431}
                    ]
                }
            }
        });
        let parsed = BacktestEngine::parse_backfill_json(&sample.to_string()).unwrap();
        let btc = parsed.get("BTC/USDC:USDC").expect("BTC/USDC:USDC parsed");
        assert_eq!(btc.len(), 2);
        assert!((btc[0].open - 70364.0).abs() < 1e-6);
        assert!((btc[0].funding_rate_hourly - 0.000015).abs() < 1e-10);
        assert_eq!(btc[0].is_synthetic, 0);

        // 2. Verify empty dataset is rejected when use_real_history is requested
        let empty = BacktestEngine::parse_backfill_json(r#"{"coins": {}}"#).unwrap();
        assert!(empty.is_empty());

        // 3. Parse actual shipped data/real_4h_backfill.json
        let path = BacktestEngine::backfill_path();
        assert!(verify_data_provenance(&path).unwrap());
        let raw = fs::read_to_string(&path).unwrap();
        let full_map = BacktestEngine::parse_backfill_json(&raw).unwrap();
        assert!(full_map.len() >= 16);
        let btc_real = full_map.get("BTC/USDC:USDC").unwrap();
        assert!(btc_real.len() >= 4000);
        assert!(btc_real.iter().all(|b| b.is_synthetic == 0));
    }

    #[test]
    fn test_zero_lookahead_causality_under_shuffled_future_bars() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let bt = BacktestEngine::new(db.clone(), 30);

        let bt_db_a = db.clone_to_memory().unwrap();
        let (btc_a, usdc_a, synth_a) = bt
            .load_historical_dataset(&bt_db_a, 80, 70, true)
            .unwrap();
        assert!(btc_a.iter().all(|b| b.is_synthetic == 0));

        let n_total = btc_a.len();
        let warmup = 70usize;
        let t_pivot = warmup + 40; // Bar index 110 out of 150
        assert!(t_pivot + 10 < n_total);

        let (_, evals_a, trades_a) =
            BacktestEngine::execute_pit_replay(&bt_db_a, &btc_a, &usdc_a, &synth_a, warmup)
                .unwrap();

        // Construct dataset B identical on [0..=t_pivot] and wildly mutated/shuffled on [t_pivot + 1..n_total]
        let bt_db_b = db.clone_to_memory().unwrap();
        let mut btc_b = btc_a.clone();
        let tail_len = n_total - (t_pivot + 1);
        for offset in 0..tail_len {
            let src_idx = n_total - 1 - offset;
            let dst_idx = t_pivot + 1 + offset;
            let ts_keep = btc_a[dst_idx].timestamp_ms;
            let mut mutated = btc_a[src_idx].clone();
            mutated.timestamp_ms = ts_keep;
            mutated.close *= 1.35;
            mutated.high *= 1.45;
            mutated.low *= 0.65;
            btc_b[dst_idx] = mutated;
        }
        bt_db_b.upsert_ohlcv_batch(&btc_b).unwrap();

        let mut usdc_b = usdc_a.clone();
        for bars in usdc_b.values_mut() {
            let orig = bars.clone();
            for offset in 0..tail_len {
                let src_idx = n_total - 1 - offset;
                let dst_idx = t_pivot + 1 + offset;
                let ts_keep = orig[dst_idx].timestamp_ms;
                let mut mutated = orig[src_idx].clone();
                mutated.timestamp_ms = ts_keep;
                mutated.close *= 0.55;
                mutated.high *= 1.50;
                mutated.low *= 0.45;
                bars[dst_idx] = mutated;
            }
            bt_db_b.upsert_ohlcv_batch(bars).unwrap();
        }

        let mut synth_b = HashMap::new();
        for pair in bt_db_b.get_alt_pairs_universe().unwrap() {
            let alt_bars = usdc_b.get(&pair.exec_symbol).unwrap();
            let s_bars = MarketIngestionEngine::synthesize_alt_btc_candles(
                alt_bars,
                &btc_b,
                &pair.synth_symbol,
                "4h",
            );
            bt_db_b.upsert_ohlcv_batch(&s_bars).unwrap();
            synth_b.insert(pair.synth_symbol.clone(), s_bars);
        }

        let (_, evals_b, trades_b) =
            BacktestEngine::execute_pit_replay(&bt_db_b, &btc_b, &usdc_b, &synth_b, warmup)
                .unwrap();

        let check_steps = t_pivot - warmup + 1;
        for step in 0..check_steps {
            let ev_a = &evals_a[step];
            let ev_b = &evals_b[step];
            assert_eq!(ev_a.len(), ev_b.len());
            for (a, b) in ev_a.iter().zip(ev_b.iter()) {
                assert_eq!(a.exec_symbol, b.exec_symbol, "rank mismatch at step {step}");
                assert_eq!(
                    a.hmm_regime, b.hmm_regime,
                    "regime lookahead leak at step {step}"
                );
                assert!(
                    (a.prob_bull - b.prob_bull).abs() < 1e-9,
                    "prob_bull lookahead leak at step {step}"
                );
                assert!(
                    (a.mace_score - b.mace_score).abs() < 1e-9,
                    "mace_score lookahead leak at step {step}"
                );
                assert!(
                    (a.blended_conviction - b.blended_conviction).abs() < 1e-9,
                    "blended_conviction lookahead leak at step {step}"
                );
                assert_eq!(
                    a.signal_action, b.signal_action,
                    "signal_action lookahead leak at step {step}"
                );
            }

            let tr_a = &trades_a[step];
            let tr_b = &trades_b[step];
            assert_eq!(
                tr_a.len(),
                tr_b.len(),
                "open trade count mismatch at step {step}"
            );
            for (ta, tb) in tr_a.iter().zip(tr_b.iter()) {
                assert_eq!(ta.symbol, tb.symbol);
                assert_eq!(ta.side, tb.side);
                assert!((ta.quantity - tb.quantity).abs() < 1e-9);
                assert!((ta.entry_price - tb.entry_price).abs() < 1e-9);
                assert!((ta.chandelier_stop_price - tb.chandelier_stop_price).abs() < 1e-9);
            }
        }
    }

    #[test]
    fn test_tear_sheet_gate_math_and_formatting() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        let bt = BacktestEngine::new(db, 30);
        let ts = bt.run_backtest(Some(90), Some(70), true).unwrap();
        assert_eq!(ts.bars_replayed, 90);
        assert_eq!(
            ts.bull_bars_count + ts.bear_bars_count + ts.chop_bars_count,
            90
        );
        assert_eq!(
            ts.all_gates_passed,
            ts.gate_sharpe_pass && ts.gate_mdd_pass && ts.gate_profit_factor_pass
        );
        let formatted = BacktestEngine::format_tear_sheet(&ts);
        assert!(formatted.contains("POINT-IN-TIME HISTORICAL BACKTEST TEAR SHEET"));
        assert!(formatted.contains("VERDICT:"));
    }
}
