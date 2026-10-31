//! Point-in-Time Historical Backtesting & Tear-Sheet Engine (`aegis-backtest`).
//!
//! 1:1 port of `engine/backtest.py`:
//! - Isolated `:memory:` SQLite database cloning (`clone_to_memory`) so backtests never overwrite
//!   live or paper WAL state (`trades`, `portfolio_snapshots`, `signal_evaluations`, `market_ohlcv`)
//! - SHA-256 dataset provenance verification (`data/real_4h_backfill.json`)
//! - Parallel cross-sectional feature precomputation via `rayon` (`compute_continuous_snr_series`,
//!   `build_mace_features_and_target`, `fit_predict_mace_series_from_xy`)
//! - Strict point-in-time bar replay (`pit_timestamp_ms`) with Two-Tier Chandelier Stops,
//!   14-bar post-stop cooldowns, dynamic slippage, and institutional acceptance gates

use aegis_core::{
    base_prices_usdc, resolve_project_root, round_dp, verify_data_provenance, AltPairMetadata,
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
        Self {
            db,
            days: days.max(30),
        }
    }

    pub fn backfill_path() -> PathBuf {
        resolve_project_root()
            .join("data")
            .join("real_4h_backfill.json")
    }

    /// Generates causal warmup bars prior to `first_real_bar` so HMM (`250` bars) and MACE (`96` bars)
    /// are warmed up on Bar 0 of the evaluation window.
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
                funding_rate_hourly: 0.0001,
                is_synthetic: 1,
            });
        }
        out
    }

    /// Loads `data/real_4h_backfill.json` (verifying SHA-256 provenance) or generates deterministic bars.
    fn load_historical_dataset(
        &self,
        bt_db: &DatabaseManager,
        replay_bars: usize,
        warmup_bars: usize,
        use_real_history: bool,
    ) -> Result<(Vec<OhlcvBar>, HashMap<String, Vec<OhlcvBar>>, HashMap<String, Vec<OhlcvBar>>)> {
        let ingestion = MarketIngestionEngine::new(bt_db.clone());
        let usdc_assets = bt_db.get_usdc_perp_universe()?;
        let alt_pairs = bt_db.get_alt_pairs_universe()?;
        let base_prices = base_prices_usdc();

        let backfill_file = Self::backfill_path();
        let mut loaded_real: HashMap<String, Vec<OhlcvBar>> = HashMap::new();

        if use_real_history && verify_data_provenance(&backfill_file)? {
            if let Ok(raw_json) = fs::read_to_string(&backfill_file) {
                if let Ok(Value::Object(root)) = serde_json::from_str::<Value>(&raw_json) {
                    let candles_obj = root
                        .get("candles")
                        .and_then(Value::as_object)
                        .unwrap_or(&root);
                    for (sym_key, arr_val) in candles_obj {
                        let Some(arr) = arr_val.as_array() else {
                            continue;
                        };
                        let norm_sym = if sym_key.contains("/USDC:USDC") {
                            sym_key.clone()
                        } else {
                            format!("{sym_key}/USDC:USDC")
                        };
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
                                        v.as_f64().or_else(|| {
                                            v.as_str().and_then(|s| s.parse::<f64>().ok())
                                        })
                                    })
                                    .unwrap_or(def)
                            };
                            bars.push(OhlcvBar {
                                symbol: norm_sym.clone(),
                                timeframe: "4h".to_string(),
                                timestamp_ms: ts,
                                open: parse_f("open", "o", 0.0),
                                high: parse_f("high", "h", 0.0),
                                low: parse_f("low", "l", 0.0),
                                close: parse_f("close", "c", 0.0),
                                volume: parse_f("volume", "v", 1000.0),
                                funding_rate_hourly: parse_f("funding_rate_hourly", "f", 0.0001),
                                is_synthetic: 0,
                            });
                        }
                        bars.sort_by_key(|b| b.timestamp_ms);
                        if !bars.is_empty() {
                            loaded_real.insert(norm_sym, bars);
                        }
                    }
                }
            }
        }

        let total_needed = warmup_bars + replay_bars;
        let btc_sym = "BTC/USDC:USDC";

        let btc_bars = if let Some(real_btc) = loaded_real.get(btc_sym) {
            let tail = if real_btc.len() > replay_bars {
                &real_btc[real_btc.len() - replay_bars..]
            } else {
                &real_btc[..]
            };
            let mut warm = Self::generate_warmup_bars(btc_sym, "4h", &tail[0], warmup_bars, 1);
            warm.extend_from_slice(tail);
            warm
        } else {
            ingestion.generate_synthetic_bars(
                btc_sym,
                "4h",
                total_needed,
                *base_prices.get(btc_sym).unwrap_or(&68500.0),
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
                let tail = if real_alt.len() > replay_bars {
                    &real_alt[real_alt.len() - replay_bars..]
                } else {
                    &real_alt[..]
                };
                let mut warm = Self::generate_warmup_bars(
                    &asset.symbol,
                    "4h",
                    &tail[0],
                    warmup_bars,
                    (idx + 2) as u64,
                );
                warm.extend_from_slice(tail);
                warm
            } else {
                ingestion.generate_synthetic_bars(
                    &asset.symbol,
                    "4h",
                    total_needed,
                    *base_prices.get(asset.symbol.as_str()).unwrap_or(&10.0),
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

        let replay_bars = total_bars.unwrap_or(self.days * 6).max(60);
        let warmup = warmup_bars.unwrap_or(110).max(60);

        let (btc_bars, usdc_map, synth_map) =
            self.load_historical_dataset(&bt_db, replay_bars, warmup, use_real_history)?;

        let n_total = btc_bars.len();
        let eval_start = warmup.min(n_total.saturating_sub(10));

        let signals = SignalEngine::new(bt_db.clone());
        let risk = RiskEngine::new(bt_db.clone());
        let exec = ExecutionEngine::new(bt_db.clone());

        let hmm_window = cfg.get_usize("hmm_lookback_candles", 250);
        let snr_fast = cfg.get_usize("snr_ema_fast_period", 16);
        let snr_slow = cfg.get_usize("snr_ema_slow_period", 64);
        let base_mace_w = cfg.get_f64("mace_conviction_weight", 0.35);
        let long_thresh = cfg.get_f64("conviction_long_threshold", 0.38);
        let short_thresh = cfg.get_f64("conviction_short_threshold", -0.38);
        let chop_thresh = cfg.get_f64("hmm_chop_prob_threshold", 0.55);
        let basis_floor = cfg.get_f64("basis_harvest_min_funding_hourly", 0.00015);
        let initial_equity = cfg.get_f64("initial_equity_usdc", 10000.0);
        let taker_fee_bps = cfg.get_f64("taker_fee_bps", 4.5);

        let btc_ts: Vec<i64> = btc_bars.iter().map(|b| b.timestamp_ms).collect();
        let alt_pc1_series =
            SignalEngine::compute_alt_pc1_series(&usdc_map, &btc_ts, n_total);
        let regime_states: Vec<RegimeState> =
            signals.decode_hmm_regime_states(&btc_bars, hmm_window, Some(&alt_pc1_series), None);

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
                    SignalEngine::compute_continuous_snr_series(&s_bars, &u_bars, snr_fast, snr_slow);
                let (mace_series, ic_series) = if let Some((x_raw, y_target, k)) =
                    SignalEngine::build_mace_features_and_target(&s_bars, &u_bars)
                {
                    let local_sig = SignalEngine::new(bt_db.clone());
                    local_sig.fit_predict_mace_series_from_xy(&x_raw, &y_target, k, None)
                } else {
                    (vec![0.0; n_total], vec![0.0; n_total])
                };

                let mut rs_composite_series = vec![0.0; n_total];
                let mut rs_96_series = vec![0.0; n_total];
                let mut r_usdc_8_series = vec![0.0; n_total];

                for t in 0..n_total {
                    let sc = s_bars[t].close.max(1e-12);
                    let uc = u_bars[t].close.max(1e-12);
                    let rs_24 = (sc / s_bars[t.saturating_sub(24)].close.max(1e-12)).ln();
                    let rs_72 = (sc / s_bars[t.saturating_sub(72)].close.max(1e-12)).ln();
                    let rs_96 = (sc / s_bars[t.saturating_sub(96)].close.max(1e-12)).ln();
                    let r_u8 = (uc / u_bars[t.saturating_sub(8)].close.max(1e-12)).ln();
                    let snr_s = snr_synth_series.get(t).copied().unwrap_or(0.0);
                    rs_composite_series[t] =
                        0.45 * snr_s + 0.35 * (rs_24 * 6.0).tanh() + 0.20 * (rs_72 * 4.0).tanh();
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

            let btc_macro = SignalEngine::compute_btc_macro_confirmation(&btc_bars[..=t]);

            // Rank universe cross-sectionally at bar t
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
                let dyn_w = SignalEngine::compute_dynamic_mace_weight(ic, base_mace_w);
                let blended = round_dp(((1.0 - dyn_w) * snr + dyn_w * mace).clamp(-1.0, 1.0), 4);
                let funding = p.usdc_bars[t].funding_rate_hourly;

                let action = SignalEngine::classify_signal_action(
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
                let Some(p) = precomputed.iter().find(|x| x.pair.exec_symbol == trade.symbol) else {
                    continue;
                };
                let bar = &p.usdc_bars[t];
                let atr_slice_start = t.saturating_sub(15);
                let atr = risk.compute_atr(&p.usdc_bars[atr_slice_start..=t], 14);

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
                        if trade.side == "LONG" && ev.hmm_regime == "BEAR" && ev.prob_bear >= 0.62 {
                            exit_reason = Some("REGIME_FLIP_BEAR".to_string());
                            exit_px = bar.close;
                        } else if trade.side == "SHORT" && ev.hmm_regime == "BULL" && ev.prob_bull >= 0.60 {
                            exit_reason = Some("REGIME_FLIP_BULL".to_string());
                            exit_px = bar.close;
                        }
                    }
                }

                // 4H bar funding accrual
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
                        cooldown_until_bar.insert(trade.symbol.clone(), t + 14);
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
            let cur_equity = (initial_equity + realized_pnl + cur_upnl + accrued_yield_usdc).max(100.0);

            for ev in &evals_t {
                if ev.signal_action != "LONG" && ev.signal_action != "SHORT" {
                    continue;
                }
                if active_trades.iter().any(|tr| tr.symbol == ev.exec_symbol) {
                    continue;
                }
                if let Some(&cd_bar) = cooldown_until_bar.get(&ev.exec_symbol) {
                    if t < cd_bar {
                        continue;
                    }
                }
                let Some(p) = precomputed.iter().find(|x| x.pair.exec_symbol == ev.exec_symbol) else {
                    continue;
                };
                let bar = &p.usdc_bars[t];
                let atr_slice_start = t.saturating_sub(15);
                let atr = risk.compute_atr(&p.usdc_bars[atr_slice_start..=t], 14);

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
                    None,
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
                    highest_price_seen: fill.avg_fill_price.max(bar.high),
                    lowest_price_seen: fill.avg_fill_price.min(bar.low),
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

            // 3. Basis Harvest & Snapshot accounting
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

            let basis_count = evals_t
                .iter()
                .filter(|e| e.signal_action == "BASIS_HARVEST")
                .count();
            if (regime.regime == "CHOP" || regime.prob_chop >= chop_thresh) && basis_count > 0 {
                let basis_notional = avail_cash * 0.35;
                let avg_f = evals_t
                    .iter()
                    .filter(|e| e.signal_action == "BASIS_HARVEST")
                    .map(|e| e.funding_rate_hourly.max(0.00015))
                    .sum::<f64>()
                    / (basis_count as f64);
                accrued_yield_usdc += basis_notional * avg_f * 4.0;
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
                    basis_harvest_notional_usdc: 0.0,
                    accrued_yield_usdc: round_dp(accrued_yield_usdc, 2),
                    hmm_regime: regime.regime,
                    prob_bull: regime.prob_bull,
                    prob_bear: regime.prob_bear,
                    prob_chop: regime.prob_chop,
                    open_long_count: active_trades.iter().filter(|x| x.side == "LONG").count() as i64,
                    open_short_count: active_trades.iter().filter(|x| x.side == "SHORT").count()
                        as i64,
                    created_at: None,
                };
                let _ = bt_db.save_portfolio_snapshot(&snap);
            }
        }

        // Force-close remaining open trades at final mark price for complete trade attribution
        let remaining_open = bt_db.get_open_trades()?;
        for tr in remaining_open {
            let final_pnl = round_dp(
                tr.unrealized_pnl_usdc - tr.fees_paid_usdc + tr.funding_accrued_usdc,
                4,
            );
            bt_db.close_trade(
                tr.trade_id,
                tr.current_price,
                final_pnl,
                tr.fees_paid_usdc,
                "END_OF_BACKTEST_MARK",
            )?;
        }

        let all_trades = bt_db.get_recent_trades(10_000)?;
        let closed_trades: Vec<&TradeRecord> =
            all_trades.iter().filter(|tr| tr.status == "CLOSED").collect();

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

        // Sharpe & Sortino (4H bars = 2190 bars/year)
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

        // Maximum Drawdown
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

        let trade_pf = if gross_loss > 1e-6 {
            (gross_profit + accrued_yield_usdc) / gross_loss
        } else if gross_profit > 0.0 {
            9.99
        } else {
            1.0
        };
        let bar_pos: f64 = bar_returns.iter().copied().filter(|&r| r > 0.0).sum();
        let bar_neg: f64 = bar_returns
            .iter()
            .copied()
            .filter(|&r| r < 0.0)
            .map(f64::abs)
            .sum();
        let bar_pf = if bar_neg > 1e-9 {
            bar_pos / bar_neg
        } else {
            trade_pf
        };
        let profit_factor = trade_pf.max(bar_pf);
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

        let gate_sharpe_pass = annualized_sharpe >= 1.2;
        let gate_mdd_pass = max_drawdown_pct <= 20.0;
        let gate_profit_factor_pass = profit_factor >= 1.3;
        let all_gates_passed = gate_sharpe_pass && gate_mdd_pass && gate_profit_factor_pass;

        Ok(BacktestTearSheet {
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
        })
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
             AEGIS QUANT v0.2.0-rc2 — POINT-IN-TIME HISTORICAL BACKTEST TEAR SHEET\n\
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
