//! Multi-Layer Quantitative Signal & Regime Engine (`aegis-signals`).
//!
//! 1:1 port of `engine/signals.py` and `engine/calibration.py`:
//! - **Layer 1 (`hmm`):** 3-State Diagonal Gaussian HMM (`BULL`, `BEAR`, `CHOP`) with MAP directional priors,
//!   sticky transition priors, causal rolling-window decoding (`states.len() == bars.len()`),
//!   4H bar-close `Arc<[RegimeState]>` cache + 15s single-step forward filtering (`alpha_{T-1} -> alpha_T`),
//!   causal cross-sectional `alt_pc1_ret` via rolling covariance power-iteration, and hysteresis transition gates.
//! - **Layer 2:** Continuous Signal-to-Noise Ratio (`SNR` in `[-1, +1]`) blending `60%` `ALT/BTC:SYNTH`
//!   and `40%` `ALT/USDC:USDC` causal EMA trend/volatility ratio (`16`/`64` spans).
//! - **Layer 3 (`mace`):** Out-of-Sample Purged MACE (`6` base features causally standardized -> `18` nonlinear features,
//!   purged `k=4` bars, timestamp-keyed refit every `36` bars with L2 Ridge + Random Forest Regressor ensemble)
//!   and causal Spearman Rank IC.
//! - **Layer 4:** Calibration 2 Cross-Sectional Ranking, BTC Macro Confirmation, and Walk-Forward Calibrator.

#![allow(clippy::needless_range_loop, clippy::manual_memcpy)]

pub mod hmm;
pub mod mace;

pub use hmm::*;
pub use mace::*;

use aegis_core::{round_dp, AegisError, OhlcvBar, Result, SignalEvaluation};
use aegis_db::{ConfigMap, DatabaseManager};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

/// Database-backed quantitative signal engine configuration (`system_config`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    pub hmm_refit_step_bars: usize,
    pub hmm_min_fit_obs_bars: usize,
    pub hmm_ret_fast_weight: f64,
    pub hmm_ret_slow_weight: f64,
    pub snr_synth_weight: f64,
    pub snr_usdc_weight: f64,
    pub snr_mad_multiplier: f64,
    pub snr_tanh_scale: f64,
    pub mace_min_bars: usize,
    pub mace_vol_ema_span: f64,
    pub mace_z_window_bars: usize,
    pub mace_target_horizon_bars: usize,
    pub mace_train_window_bars: usize,
    pub mace_ic_window_bars: usize,
    pub mace_dynamic_weight_min: f64,
    pub mace_dynamic_weight_max: f64,
    pub mace_ic_neg_slope: f64,
    pub mace_ic_pos_slope: f64,
    pub btc_macro_ema_fast_span: f64,
    pub btc_macro_ema_slow_span: f64,
    pub btc_macro_ret_slow_bars: usize,
    pub btc_macro_ret_fast_bars: usize,
    pub rs_composite_snr_weight: f64,
    pub rs_composite_fast_weight: f64,
    pub rs_composite_mid_weight: f64,
    pub rs_composite_fast_scale: f64,
    pub rs_composite_mid_scale: f64,
    pub action_top_k_leaders: usize,
    pub action_bottom_k_laggards: usize,
    pub action_bull_min_prob: f64,
    pub action_bull_min_btc_trend: f64,
    pub action_bull_min_btc_r24: f64,
    pub action_bull_min_btc_r8: f64,
    pub action_bull_min_snr: f64,
    pub action_bull_min_snr_synth: f64,
    pub action_bull_min_mace: f64,
    pub action_bull_min_rs96: f64,
    pub action_bull_min_r_usdc_8: f64,
    pub action_bull_max_funding: f64,
    pub action_bear_min_prob: f64,
    pub action_bear_max_btc_trend: f64,
    pub action_bear_max_btc_r24: f64,
    pub action_bear_max_btc_r8: f64,
    pub action_bear_max_snr: f64,
    pub action_bear_max_snr_synth: f64,
    pub action_bear_max_mace: f64,
    pub action_bear_max_rs96: f64,
    pub action_bear_max_r_usdc_8: f64,
    pub action_bear_min_funding: f64,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            hmm_refit_step_bars: 12,
            hmm_min_fit_obs_bars: 14,
            hmm_ret_fast_weight: 0.65,
            hmm_ret_slow_weight: 0.35,
            snr_synth_weight: 0.60,
            snr_usdc_weight: 0.40,
            snr_mad_multiplier: 2.5,
            snr_tanh_scale: 1.25,
            mace_min_bars: 55,
            mace_vol_ema_span: 20.0,
            mace_z_window_bars: 120,
            mace_target_horizon_bars: 4,
            mace_train_window_bars: 220,
            mace_ic_window_bars: 36,
            mace_dynamic_weight_min: 0.15,
            mace_dynamic_weight_max: 0.60,
            mace_ic_neg_slope: 0.60,
            mace_ic_pos_slope: 0.85,
            btc_macro_ema_fast_span: 18.0,
            btc_macro_ema_slow_span: 54.0,
            btc_macro_ret_slow_bars: 24,
            btc_macro_ret_fast_bars: 8,
            rs_composite_snr_weight: 0.45,
            rs_composite_fast_weight: 0.35,
            rs_composite_mid_weight: 0.20,
            rs_composite_fast_scale: 6.0,
            rs_composite_mid_scale: 4.0,
            action_top_k_leaders: 2,
            action_bottom_k_laggards: 2,
            action_bull_min_prob: 0.54,
            action_bull_min_btc_trend: -0.004,
            action_bull_min_btc_r24: -0.015,
            action_bull_min_btc_r8: -0.010,
            action_bull_min_snr: 0.28,
            action_bull_min_snr_synth: 0.22,
            action_bull_min_mace: 0.02,
            action_bull_min_rs96: 0.015,
            action_bull_min_r_usdc_8: 0.004,
            action_bull_max_funding: 0.0005,
            action_bear_min_prob: 0.56,
            action_bear_max_btc_trend: -0.002,
            action_bear_max_btc_r24: -0.010,
            action_bear_max_btc_r8: -0.005,
            action_bear_max_snr: -0.28,
            action_bear_max_snr_synth: -0.22,
            action_bear_max_mace: -0.02,
            action_bear_max_rs96: -0.020,
            action_bear_max_r_usdc_8: -0.006,
            action_bear_min_funding: -0.0004,
        }
    }
}

impl SignalConfig {
    pub fn from_config_map(cfg: &ConfigMap) -> Self {
        let d = Self::default();
        let snr_synth_weight = cfg.get_f64("snr_synth_weight", d.snr_synth_weight);
        Self {
            hmm_refit_step_bars: cfg.get_usize(
                "hmm_refit_step_bars",
                cfg.get_usize("hmm_walkforward_refit_bars", d.hmm_refit_step_bars),
            ),
            hmm_min_fit_obs_bars: cfg.get_usize("hmm_min_fit_obs_bars", d.hmm_min_fit_obs_bars),
            hmm_ret_fast_weight: cfg.get_f64("hmm_ret_fast_weight", d.hmm_ret_fast_weight),
            hmm_ret_slow_weight: cfg.get_f64("hmm_ret_slow_weight", d.hmm_ret_slow_weight),
            snr_synth_weight,
            snr_usdc_weight: cfg.get_f64(
                "snr_usdc_weight",
                (1.0 - snr_synth_weight).clamp(0.0, 1.0),
            ),
            snr_mad_multiplier: cfg.get_f64("snr_mad_multiplier", d.snr_mad_multiplier),
            snr_tanh_scale: cfg.get_f64(
                "snr_tanh_scale",
                cfg.get_f64("mace_tanh_scale", d.snr_tanh_scale),
            ),
            mace_min_bars: cfg.get_usize("mace_min_bars", d.mace_min_bars),
            mace_vol_ema_span: cfg.get_f64("mace_vol_ema_span", d.mace_vol_ema_span),
            mace_z_window_bars: cfg.get_usize("mace_z_window_bars", d.mace_z_window_bars),
            mace_target_horizon_bars: cfg.get_usize(
                "mace_target_horizon_bars",
                cfg.get_usize("mace_forward_horizon_bars", d.mace_target_horizon_bars),
            ),
            mace_train_window_bars: cfg.get_usize(
                "mace_train_window_bars",
                d.mace_train_window_bars,
            ),
            mace_ic_window_bars: cfg.get_usize("mace_ic_window_bars", d.mace_ic_window_bars),
            mace_dynamic_weight_min: cfg.get_f64(
                "mace_dynamic_weight_min",
                d.mace_dynamic_weight_min,
            ),
            mace_dynamic_weight_max: cfg.get_f64(
                "mace_dynamic_weight_max",
                d.mace_dynamic_weight_max,
            ),
            mace_ic_neg_slope: cfg.get_f64("mace_ic_neg_slope", d.mace_ic_neg_slope),
            mace_ic_pos_slope: cfg.get_f64("mace_ic_pos_slope", d.mace_ic_pos_slope),
            btc_macro_ema_fast_span: cfg.get_f64(
                "btc_macro_ema_fast_span",
                d.btc_macro_ema_fast_span,
            ),
            btc_macro_ema_slow_span: cfg.get_f64(
                "btc_macro_ema_slow_span",
                d.btc_macro_ema_slow_span,
            ),
            btc_macro_ret_slow_bars: cfg.get_usize(
                "btc_macro_ret_slow_bars",
                d.btc_macro_ret_slow_bars,
            ),
            btc_macro_ret_fast_bars: cfg.get_usize(
                "btc_macro_ret_fast_bars",
                d.btc_macro_ret_fast_bars,
            ),
            rs_composite_snr_weight: cfg.get_f64(
                "rs_composite_snr_weight",
                d.rs_composite_snr_weight,
            ),
            rs_composite_fast_weight: cfg.get_f64(
                "rs_composite_fast_weight",
                d.rs_composite_fast_weight,
            ),
            rs_composite_mid_weight: cfg.get_f64(
                "rs_composite_mid_weight",
                d.rs_composite_mid_weight,
            ),
            rs_composite_fast_scale: cfg.get_f64(
                "rs_composite_fast_scale",
                d.rs_composite_fast_scale,
            ),
            rs_composite_mid_scale: cfg.get_f64(
                "rs_composite_mid_scale",
                d.rs_composite_mid_scale,
            ),
            action_top_k_leaders: cfg.get_usize(
                "action_top_k_leaders",
                cfg.get_usize("max_concurrent_long_positions", d.action_top_k_leaders),
            ),
            action_bottom_k_laggards: cfg.get_usize(
                "action_bottom_k_laggards",
                cfg.get_usize("max_concurrent_short_positions", d.action_bottom_k_laggards),
            ),
            action_bull_min_prob: cfg.get_f64(
                "action_bull_min_prob",
                cfg.get_f64("bull_prob_entry_gate", d.action_bull_min_prob),
            ),
            action_bull_min_btc_trend: cfg.get_f64(
                "action_bull_min_btc_trend",
                d.action_bull_min_btc_trend,
            ),
            action_bull_min_btc_r24: cfg.get_f64(
                "action_bull_min_btc_r24",
                d.action_bull_min_btc_r24,
            ),
            action_bull_min_btc_r8: cfg.get_f64(
                "action_bull_min_btc_r8",
                d.action_bull_min_btc_r8,
            ),
            action_bull_min_snr: cfg.get_f64("action_bull_min_snr", d.action_bull_min_snr),
            action_bull_min_snr_synth: cfg.get_f64(
                "action_bull_min_snr_synth",
                cfg.get_f64("bull_snr_synth_gate", d.action_bull_min_snr_synth),
            ),
            action_bull_min_mace: cfg.get_f64(
                "action_bull_min_mace",
                cfg.get_f64("bull_mace_gate", d.action_bull_min_mace),
            ),
            action_bull_min_rs96: cfg.get_f64(
                "action_bull_min_rs96",
                cfg.get_f64("bull_rs_96_gate", d.action_bull_min_rs96),
            ),
            action_bull_min_r_usdc_8: cfg.get_f64(
                "action_bull_min_r_usdc_8",
                d.action_bull_min_r_usdc_8,
            ),
            action_bull_max_funding: cfg.get_f64(
                "action_bull_max_funding",
                d.action_bull_max_funding,
            ),
            action_bear_min_prob: cfg.get_f64(
                "action_bear_min_prob",
                cfg.get_f64("bear_prob_entry_gate", d.action_bear_min_prob),
            ),
            action_bear_max_btc_trend: cfg.get_f64(
                "action_bear_max_btc_trend",
                d.action_bear_max_btc_trend,
            ),
            action_bear_max_btc_r24: cfg.get_f64(
                "action_bear_max_btc_r24",
                d.action_bear_max_btc_r24,
            ),
            action_bear_max_btc_r8: cfg.get_f64(
                "action_bear_max_btc_r8",
                d.action_bear_max_btc_r8,
            ),
            action_bear_max_snr: cfg.get_f64("action_bear_max_snr", d.action_bear_max_snr),
            action_bear_max_snr_synth: cfg.get_f64(
                "action_bear_max_snr_synth",
                cfg.get_f64("bear_snr_synth_gate", d.action_bear_max_snr_synth),
            ),
            action_bear_max_mace: cfg.get_f64(
                "action_bear_max_mace",
                cfg.get_f64("bear_mace_gate", d.action_bear_max_mace),
            ),
            action_bear_max_rs96: cfg.get_f64(
                "action_bear_max_rs96",
                cfg.get_f64("bear_rs_96_gate", d.action_bear_max_rs96),
            ),
            action_bear_max_r_usdc_8: cfg.get_f64(
                "action_bear_max_r_usdc_8",
                cfg.get_f64("bear_r_usdc_8_gate", d.action_bear_max_r_usdc_8),
            ),
            action_bear_min_funding: cfg.get_f64(
                "action_bear_min_funding",
                cfg.get_f64("short_max_adverse_funding_rate_hourly", d.action_bear_min_funding),
            ),
        }
    }
}

/// Validated Relative-Strength and USDC momentum lookback helper (`C6`).
/// Returns `(rs_fast, rs_mid, rs_slow, r_usdc_mom)` and requires `s_closes.len() > lb_slow`
/// so short series never silently clamp to index `0` via `saturating_sub`.
pub fn compute_rs_lookbacks(
    s_closes: &[f64],
    u_closes: &[f64],
    lb_fast: usize,
    lb_mid: usize,
    lb_slow: usize,
    lb_u8: usize,
) -> (f64, f64, f64, f64) {
    let ns = s_closes.len();
    let nu = u_closes.len();
    let (rs_24, rs_72, rs_96) = if ns > lb_slow && lb_slow > 0 && lb_mid > 0 && lb_fast > 0 {
        let cur = s_closes[ns - 1].max(1e-12);
        let r_f = (cur / s_closes[ns - 1 - lb_fast].max(1e-12)).ln();
        let r_m = (cur / s_closes[ns - 1 - lb_mid].max(1e-12)).ln();
        let r_s = (cur / s_closes[ns - 1 - lb_slow].max(1e-12)).ln();
        (r_f, r_m, r_s)
    } else {
        (0.0, 0.0, 0.0)
    };
    let r_usdc_8 = if nu > lb_u8 && lb_u8 > 0 {
        (u_closes[nu - 1].max(1e-12) / u_closes[nu - 1 - lb_u8].max(1e-12)).ln()
    } else {
        0.0
    };
    (rs_24, rs_72, rs_96, r_usdc_8)
}

#[derive(Clone)]
pub struct SignalEngine {
    pub db: DatabaseManager,
    pub(crate) hmm_cache: Arc<Mutex<HmmCacheState>>,
    pub(crate) mace_cache: Arc<Mutex<HashMap<String, CachedMaceModel>>>,
}

impl SignalEngine {
    pub fn new(db: DatabaseManager) -> Self {
        Self {
            db,
            hmm_cache: Arc::new(Mutex::new(HmmCacheState::default())),
            mace_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn clear_caches(&self) {
        *self.hmm_cache.lock() = HmmCacheState::default();
        self.mace_cache.lock().clear();
    }

    /// Computes causal Exponential Moving Average (`alpha = 2 / (span + 1)`).
    pub fn causal_ema(series: &[f64], span: f64) -> Vec<f64> {
        if series.is_empty() {
            return Vec::new();
        }
        let alpha = 2.0 / (span + 1.0);
        let mut out = Vec::with_capacity(series.len());
        out.push(series[0]);
        for i in 1..series.len() {
            let prev = out[i - 1];
            out.push(alpha * series[i] + (1.0 - alpha) * prev);
        }
        out
    }

    /// Computes causal continuous SNR series (`[-1.0, +1.0]`) blending `snr_synth_weight` `ALT/BTC:SYNTH`
    /// and `snr_usdc_weight` `ALT/USDC:USDC` causal EMA trend/volatility ratio (`fast_span` vs `slow_span`).
    pub fn compute_continuous_snr_series(
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
        fast_span: usize,
        slow_span: usize,
    ) -> (Vec<f64>, Vec<f64>) {
        Self::compute_continuous_snr_series_with_config(
            synth_bars,
            usdc_bars,
            fast_span,
            slow_span,
            &SignalConfig::default(),
        )
    }

    pub fn compute_continuous_snr_series_with_config(
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
        fast_span: usize,
        slow_span: usize,
        scfg: &SignalConfig,
    ) -> (Vec<f64>, Vec<f64>) {
        let n_s = synth_bars.len();
        let n_u = usdc_bars.len();
        if n_s < 10 || n_u < 10 {
            let n = n_s.max(n_u).max(1);
            return (vec![0.0; n], vec![0.0; n]);
        }

        let mad_mult = scfg.snr_mad_multiplier.max(1e-4);
        let tanh_scale = scfg.snr_tanh_scale.max(1e-4);
        let calc_series = |bars: &[OhlcvBar]| -> Vec<f64> {
            let closes: Vec<f64> = bars.iter().map(|b| b.close.max(1e-12)).collect();
            let ema_fast = Self::causal_ema(&closes, fast_span as f64);
            let ema_slow = Self::causal_ema(&closes, slow_span as f64);
            let mut abs_diff = vec![0.0; closes.len()];
            for i in 1..closes.len() {
                abs_diff[i] = (closes[i] - closes[i - 1]).abs();
            }
            let mad = Self::causal_ema(&abs_diff, slow_span as f64);
            let mut out = Vec::with_capacity(closes.len());
            for i in 0..closes.len() {
                let raw = (ema_fast[i] - ema_slow[i]) / (mad[i] * mad_mult + 1e-12);
                out.push((raw * tanh_scale).tanh());
            }
            out
        };

        let snr_s = calc_series(synth_bars);
        let snr_u = calc_series(usdc_bars);
        let m = snr_s.len().min(snr_u.len());
        let s_tail = &snr_s[snr_s.len() - m..];
        let u_tail = &snr_u[snr_u.len() - m..];

        let mut composite = Vec::with_capacity(m);
        let mut synth_only = Vec::with_capacity(m);
        for i in 0..m {
            let c =
                (scfg.snr_synth_weight * s_tail[i] + scfg.snr_usdc_weight * u_tail[i]).clamp(-1.0, 1.0);
            composite.push(round_dp(c, 4));
            synth_only.push(round_dp(s_tail[i].clamp(-1.0, 1.0), 4));
        }
        (composite, synth_only)
    }

    pub fn compute_continuous_snr(
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
        fast_span: usize,
        slow_span: usize,
    ) -> (f64, f64) {
        let (comp, synth) =
            Self::compute_continuous_snr_series(synth_bars, usdc_bars, fast_span, slow_span);
        (
            comp.last().copied().unwrap_or(0.0),
            synth.last().copied().unwrap_or(0.0),
        )
    }

    /// Computes BTC macro trend confirmation (`trend_pct`, `r_btc_24`, `r_btc_8`).
    pub fn compute_btc_macro_confirmation(btc_bars: &[OhlcvBar]) -> (f64, f64, f64) {
        Self::compute_btc_macro_confirmation_with_config(btc_bars, &SignalConfig::default())
    }

    pub fn compute_btc_macro_confirmation_with_config(
        btc_bars: &[OhlcvBar],
        scfg: &SignalConfig,
    ) -> (f64, f64, f64) {
        let slow_lb = scfg.btc_macro_ret_slow_bars.max(1);
        let fast_lb = scfg.btc_macro_ret_fast_bars.max(1);
        if btc_bars.len() < slow_lb + 6 {
            return (0.0, 0.0, 0.0);
        }
        let closes: Vec<f64> = btc_bars.iter().map(|b| b.close.max(1e-8)).collect();
        let n = closes.len();
        let ema_fast = Self::causal_ema(&closes, scfg.btc_macro_ema_fast_span.max(2.0));
        let ema_slow = Self::causal_ema(&closes, scfg.btc_macro_ema_slow_span.max(4.0));
        let trend_pct = (ema_fast[n - 1] - ema_slow[n - 1]) / ema_slow[n - 1].max(1e-8);
        let r_btc_24 = if n > slow_lb {
            (closes[n - 1] / closes[n - 1 - slow_lb]).ln()
        } else {
            0.0
        };
        let r_btc_8 = if n > fast_lb {
            (closes[n - 1] / closes[n - 1 - fast_lb]).ln()
        } else {
            0.0
        };
        (trend_pct, r_btc_24, r_btc_8)
    }

    /// Calibration 2 symmetric directional action gate (`LONG`, `SHORT`, `BASIS_HARVEST`, `FLAT`).
    #[allow(clippy::too_many_arguments)]
    pub fn classify_signal_action(
        regime_name: &str,
        prob_bull: f64,
        prob_bear: f64,
        prob_chop: f64,
        blended_conviction: f64,
        snr_score: f64,
        snr_synth: f64,
        mace_score: f64,
        rs_96: f64,
        r_usdc_8: f64,
        rs_rank: usize,
        total_assets: usize,
        funding_rate_hourly: f64,
        long_threshold: f64,
        short_threshold: f64,
        chop_threshold: f64,
        basis_funding_floor: f64,
        btc_macro_confirm: (f64, f64, f64),
    ) -> String {
        Self::classify_signal_action_with_config(
            regime_name,
            prob_bull,
            prob_bear,
            prob_chop,
            blended_conviction,
            snr_score,
            snr_synth,
            mace_score,
            rs_96,
            r_usdc_8,
            rs_rank,
            total_assets,
            funding_rate_hourly,
            long_threshold,
            short_threshold,
            chop_threshold,
            basis_funding_floor,
            btc_macro_confirm,
            &SignalConfig::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn classify_signal_action_with_config(
        regime_name: &str,
        prob_bull: f64,
        prob_bear: f64,
        prob_chop: f64,
        blended_conviction: f64,
        snr_score: f64,
        snr_synth: f64,
        mace_score: f64,
        rs_96: f64,
        r_usdc_8: f64,
        rs_rank: usize,
        total_assets: usize,
        funding_rate_hourly: f64,
        long_threshold: f64,
        short_threshold: f64,
        chop_threshold: f64,
        basis_funding_floor: f64,
        btc_macro_confirm: (f64, f64, f64),
        scfg: &SignalConfig,
    ) -> String {
        let (btc_trend_pct, r_btc_24, r_btc_8) = btc_macro_confirm;

        // 1. Chop / Funding Basis Harvest check
        if (regime_name == "CHOP" || prob_chop >= chop_threshold)
            && funding_rate_hourly >= basis_funding_floor
        {
            return "BASIS_HARVEST".to_string();
        }

        // 2. Bull Regime Long Entry (Top K Alt/BTC Relative Strength Leaders)
        let top_k = scfg.action_top_k_leaders.max(1).min(total_assets.max(1));
        let bull_macro_ok = prob_bull >= scfg.action_bull_min_prob
            && btc_trend_pct >= scfg.action_bull_min_btc_trend
            && r_btc_24 >= scfg.action_bull_min_btc_r24
            && r_btc_8 >= scfg.action_bull_min_btc_r8;

        if regime_name == "BULL"
            && bull_macro_ok
            && rs_rank <= top_k
            && blended_conviction >= long_threshold
            && snr_score >= scfg.action_bull_min_snr
            && snr_synth >= scfg.action_bull_min_snr_synth
            && mace_score >= scfg.action_bull_min_mace
            && rs_96 > scfg.action_bull_min_rs96
            && r_usdc_8 > scfg.action_bull_min_r_usdc_8
            && funding_rate_hourly <= scfg.action_bull_max_funding
        {
            return "LONG".to_string();
        }

        // 3. Bear Regime Short Entry (Bottom K Alt/BTC Relative Strength Laggards)
        let bottom_k = scfg.action_bottom_k_laggards.max(1);
        let bottom_cutoff = total_assets.saturating_sub(bottom_k - 1).max(1);
        let bear_macro_ok = prob_bear >= scfg.action_bear_min_prob
            && btc_trend_pct <= scfg.action_bear_max_btc_trend
            && r_btc_24 <= scfg.action_bear_max_btc_r24
            && r_btc_8 <= scfg.action_bear_max_btc_r8;

        if regime_name == "BEAR"
            && bear_macro_ok
            && rs_rank >= bottom_cutoff
            && blended_conviction <= short_threshold
            && snr_score <= scfg.action_bear_max_snr
            && snr_synth <= scfg.action_bear_max_snr_synth
            && mace_score <= scfg.action_bear_max_mace
            && rs_96 < scfg.action_bear_max_rs96
            && r_usdc_8 < scfg.action_bear_max_r_usdc_8
            && funding_rate_hourly >= scfg.action_bear_min_funding
        {
            return "SHORT".to_string();
        }

        "FLAT".to_string()
    }

    /// Evaluates the full cross-sectional altcoin universe (`ALT/USDC:USDC` + `ALT/BTC:SYNTH`).
    pub fn evaluate_universe(&self, save_to_db: bool) -> Result<Vec<SignalEvaluation>> {
        let cfg = self.db.load_config()?;
        let scfg = SignalConfig::from_config_map(&cfg);
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_live_mode = mode == "LIVE" || mode == "TESTNET";
        let is_wall_clock = self.db.get_pit_timestamp_ms().is_none();
        let max_staleness_ms = cfg.get_i64("max_bar_staleness_ms", 28_800_000).max(60_000);
        // Read `hmm_lookback_bars` (seeded at 540), falling back to legacy `hmm_lookback_candles` (`C4`)
        let hmm_window = cfg.get_usize(
            "hmm_lookback_bars",
            cfg.get_usize("hmm_lookback_candles", 540),
        );
        let snr_fast = cfg.get_usize(
            "snr_ema_fast_period",
            cfg.get_usize("snr_fast_span", 16),
        );
        let snr_slow = cfg.get_usize(
            "snr_ema_slow_period",
            cfg.get_usize("snr_slow_span", 64),
        );
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
        let basis_floor = cfg.get_f64(
            "basis_harvest_min_funding_hourly",
            cfg.get_f64("basis_min_hourly_funding_rate", 0.000015),
        );
        let lb_fast = cfg.get_usize("rs_lookback_fast_bars", 24);
        let lb_mid = cfg.get_usize("rs_lookback_mid_bars", 72);
        let lb_slow = cfg.get_usize("rs_lookback_slow_bars", 96);
        let lb_u8 = cfg.get_usize("rs_usdc_momentum_bars", 8);

        let btc_bars = self
            .db
            .get_ohlcv_bars("BTC/USDC:USDC", "4h", hmm_window + 10, None)?;
        let live_btc_close = btc_bars.last().map(|b| b.close);
        let regime = self.decode_hmm_regime(&btc_bars, hmm_window, None, live_btc_close);
        let btc_macro_confirm = Self::compute_btc_macro_confirmation_with_config(&btc_bars, &scfg);

        let alt_pairs = self.db.get_alt_pairs_universe()?;
        let now_ms = self.db.now_ms();
        let btc_stale_live = is_live_mode
            && is_wall_clock
            && btc_bars
                .last()
                .map(|b| (now_ms - b.timestamp_ms) > max_staleness_ms)
                .unwrap_or(true);
        if btc_stale_live {
            let _ = self.db.log_telemetry(
                "WARNING",
                "engine.signals",
                "Stale BTC/USDC:USDC candles detected in live mode; forcing FLAT signal actions",
                Some(json!({ "max_bar_staleness_ms": max_staleness_ms })),
            );
        }

        struct RawMetrics {
            base_asset: String,
            exec_symbol: String,
            synth_symbol: String,
            snr: f64,
            snr_synth: f64,
            mace: f64,
            blended_conviction: f64,
            rs_composite: f64,
            rs_96: f64,
            r_usdc_8: f64,
            funding_rate_hourly: f64,
            is_unsupported_live: bool,
        }

        let mut raw_metrics = Vec::with_capacity(alt_pairs.len());
        for pair in &alt_pairs {
            let mut usdc_bars =
                self.db
                    .get_ohlcv_bars(&pair.exec_symbol, &pair.exec_timeframe, 260, None)?;
            if usdc_bars.len() < 20 && pair.exec_timeframe != "4h" {
                usdc_bars = self
                    .db
                    .get_ohlcv_bars(&pair.exec_symbol, "4h", 260, None)?;
            }
            let synth_bars =
                self.db
                    .get_ohlcv_bars(&pair.synth_symbol, &pair.rs_timeframe, 260, None)?;
            if usdc_bars.len() < 20 || synth_bars.len() < 20 {
                continue;
            }

            let usdc_stale = usdc_bars
                .last()
                .map(|b| (now_ms - b.timestamp_ms) > max_staleness_ms)
                .unwrap_or(true);
            let synth_stale = synth_bars
                .last()
                .map(|b| (now_ms - b.timestamp_ms) > max_staleness_ms)
                .unwrap_or(true);
            let bars_misaligned = match (usdc_bars.last(), synth_bars.last()) {
                (Some(u), Some(s)) => (u.timestamp_ms - s.timestamp_ms).abs() > max_staleness_ms,
                _ => true,
            };
            let is_stale_live = is_live_mode
                && is_wall_clock
                && (btc_stale_live || usdc_stale || synth_stale || bars_misaligned);
            let is_unsupported_live = is_stale_live
                || (is_live_mode
                    && usdc_bars
                        .last()
                        .map(|b| b.is_synthetic == 1)
                        .unwrap_or(false));

            let (snr_series, snr_synth_series) =
                Self::compute_continuous_snr_series_with_config(
                    &synth_bars,
                    &usdc_bars,
                    snr_fast,
                    snr_slow,
                    &scfg,
                );
            let snr = snr_series.last().copied().unwrap_or(0.0);
            let snr_synth = snr_synth_series.last().copied().unwrap_or(0.0);
            let (mace, _ic, dyn_mace_w) = self.compute_mace_and_ic(
                &synth_bars,
                &usdc_bars,
                base_mace_w,
                Some(&pair.synth_symbol),
            );
            let dyn_snr_w = round_dp(1.0 - dyn_mace_w, 4);
            let blended = round_dp((dyn_snr_w * snr + dyn_mace_w * mace).clamp(-1.0, 1.0), 4);

            let s_closes: Vec<f64> = synth_bars.iter().map(|b| b.close.max(1e-12)).collect();
            let u_closes: Vec<f64> = usdc_bars.iter().map(|b| b.close.max(1e-12)).collect();
            let (rs_24, rs_72, rs_96, r_usdc_8) =
                compute_rs_lookbacks(&s_closes, &u_closes, lb_fast, lb_mid, lb_slow, lb_u8);
            let rs_composite = scfg.rs_composite_snr_weight * snr_synth
                + scfg.rs_composite_fast_weight * (rs_24 * scfg.rs_composite_fast_scale).tanh()
                + scfg.rs_composite_mid_weight * (rs_72 * scfg.rs_composite_mid_scale).tanh();
            let funding = usdc_bars
                .last()
                .map(|b| b.funding_rate_hourly)
                .unwrap_or(0.0);

            raw_metrics.push(RawMetrics {
                base_asset: pair.base_asset.clone(),
                exec_symbol: pair.exec_symbol.clone(),
                synth_symbol: pair.synth_symbol.clone(),
                snr,
                snr_synth,
                mace,
                blended_conviction: blended,
                rs_composite,
                rs_96,
                r_usdc_8,
                funding_rate_hourly: funding,
                is_unsupported_live,
            });
        }

        // Rank tradable live assets ahead of unsupported/stale assets
        raw_metrics.sort_by(|a, b| match (a.is_unsupported_live, b.is_unsupported_live) {
            (false, true) => std::cmp::Ordering::Less,
            (true, false) => std::cmp::Ordering::Greater,
            _ => b
                .rs_composite
                .partial_cmp(&a.rs_composite)
                .unwrap_or(std::cmp::Ordering::Equal),
        });
        let tradable_assets = raw_metrics
            .iter()
            .filter(|m| !m.is_unsupported_live)
            .count()
            .max(1);
        let mut evaluations = Vec::with_capacity(raw_metrics.len());

        for (idx, m) in raw_metrics.into_iter().enumerate() {
            let rank = idx + 1;
            let action = if m.is_unsupported_live {
                "FLAT".to_string()
            } else {
                Self::classify_signal_action_with_config(
                    &regime.regime,
                    regime.prob_bull,
                    regime.prob_bear,
                    regime.prob_chop,
                    m.blended_conviction,
                    m.snr,
                    m.snr_synth,
                    m.mace,
                    m.rs_96,
                    m.r_usdc_8,
                    rank,
                    tradable_assets,
                    m.funding_rate_hourly,
                    long_thresh,
                    short_thresh,
                    chop_thresh,
                    basis_floor,
                    btc_macro_confirm,
                    &scfg,
                )
            };

            evaluations.push(SignalEvaluation {
                eval_id: None,
                timestamp_ms: now_ms,
                base_asset: m.base_asset,
                exec_symbol: m.exec_symbol,
                synth_symbol: m.synth_symbol,
                hmm_regime: regime.regime.clone(),
                prob_bull: regime.prob_bull,
                prob_bear: regime.prob_bear,
                prob_chop: regime.prob_chop,
                snr_score: m.snr,
                snr_synth: m.snr_synth,
                rs_96: round_dp(m.rs_96, 5),
                r_usdc_8: round_dp(m.r_usdc_8, 5),
                mace_score: m.mace,
                blended_conviction: m.blended_conviction,
                rs_rank: rank as i64,
                signal_action: action,
                funding_rate_hourly: m.funding_rate_hourly,
                created_at: None,
            });
        }

        if save_to_db && !evaluations.is_empty() {
            self.db.save_signal_evaluations(&evaluations)?;
        }
        Ok(evaluations)
    }
}

/// Walk-Forward Multi-Timeframe Calibrator (`engine/calibration.py`).
#[derive(Clone)]
pub struct WalkForwardCalibrator {
    pub db: DatabaseManager,
    pub signal_engine: SignalEngine,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationReportRow {
    pub base_asset: String,
    pub tier: i64,
    pub current_exec_tf: String,
    pub current_rs_tf: String,
    pub optimal_exec_tf: String,
    pub optimal_rs_tf: String,
    pub best_oos_sharpe: f64,
    pub best_oos_ic: f64,
    pub updated: bool,
}

impl WalkForwardCalibrator {
    pub const CANDIDATE_TIMEFRAMES: [&'static str; 2] = ["1h", "4h"];

    pub fn new(db: DatabaseManager) -> Self {
        let signal_engine = SignalEngine::new(db.clone());
        Self { db, signal_engine }
    }

    pub fn resolve_timeframe_hours(timeframe: &str) -> Result<f64> {
        let tf = timeframe.trim().to_ascii_lowercase();
        let hours = match tf.as_str() {
            "15m" => 0.25,
            "30m" => 0.50,
            "1h" => 1.0,
            "2h" => 2.0,
            "4h" => 4.0,
            "6h" => 6.0,
            "8h" => 8.0,
            "12h" => 12.0,
            "1d" => 24.0,
            _ if tf.ends_with('m') => tf[..tf.len() - 1]
                .parse::<f64>()
                .map(|m| m / 60.0)
                .map_err(|_| {
                    AegisError::Validation(format!("Unsupported timeframe '{timeframe}'"))
                })?,
            _ if tf.ends_with('h') => tf[..tf.len() - 1].parse::<f64>().map_err(|_| {
                AegisError::Validation(format!("Unsupported timeframe '{timeframe}'"))
            })?,
            _ if tf.ends_with('d') => tf[..tf.len() - 1]
                .parse::<f64>()
                .map(|d| d * 24.0)
                .map_err(|_| {
                    AegisError::Validation(format!("Unsupported timeframe '{timeframe}'"))
                })?,
            _ => {
                return Err(AegisError::Validation(format!(
                    "Unsupported timeframe '{timeframe}' for Sharpe annualization."
                )))
            }
        };
        if hours <= 0.0 {
            return Err(AegisError::Validation(format!(
                "Invalid non-positive timeframe duration for '{timeframe}'"
            )));
        }
        Ok(hours)
    }

    /// Computes annualized Sharpe ratio from bar-level returns using `sqrt(8760 / dt_hours)`
    /// and sample standard deviation (`ddof = 1`).
    pub fn compute_annualized_sharpe(bar_returns: &[f64], timeframe: &str) -> Result<f64> {
        let dt_hours = Self::resolve_timeframe_hours(timeframe)?;
        let clean: Vec<f64> = bar_returns.iter().copied().filter(|x| x.is_finite()).collect();
        if clean.len() < 2 {
            return Ok(0.0);
        }
        let std_r = sample_std(&clean, 1);
        if std_r <= 1e-12 {
            return Ok(0.0);
        }
        let mean_r = sample_mean(&clean);
        let ann_factor = (8760.0 / dt_hours).sqrt();
        let sharpe = (mean_r / std_r) * ann_factor;
        Ok(if sharpe.is_finite() { sharpe } else { 0.0 })
    }

    pub fn evaluate_pair_on_timeframe(
        &self,
        exec_symbol: &str,
        synth_symbol: &str,
        timeframe: &str,
    ) -> (f64, f64, usize) {
        let usdc_bars = self
            .db
            .get_ohlcv_bars(exec_symbol, timeframe, 300, None)
            .unwrap_or_default();
        let synth_bars = self
            .db
            .get_ohlcv_bars(synth_symbol, timeframe, 300, None)
            .unwrap_or_default();
        let n = usdc_bars.len().min(synth_bars.len());
        if n < 80 {
            return (0.0, 0.0, n);
        }

        let split = (n as f64 * 0.65) as usize;
        let mut positions = Vec::new();
        let mut realized_rets = Vec::new();

        for t in split..(n - 1) {
            let sub_u = &usdc_bars[..=t];
            let sub_s = &synth_bars[..=t];
            let (snr, _) = SignalEngine::compute_continuous_snr(sub_s, sub_u, 16, 64);
            let pos = if snr > 0.25 {
                1.0
            } else if snr < -0.25 {
                -1.0
            } else {
                0.0
            };
            let fwd_ret =
                (usdc_bars[t + 1].close.max(1e-12) / usdc_bars[t].close.max(1e-12)).ln();
            positions.push(snr);
            realized_rets.push(pos * fwd_ret);
        }

        if realized_rets.len() < 10 {
            return (0.0, 0.0, n);
        }
        let sharpe = Self::compute_annualized_sharpe(&realized_rets, timeframe).unwrap_or(0.0);
        let ic = SignalEngine::safe_spearman_ic(&positions, &realized_rets);
        (round_dp(sharpe, 3), round_dp(ic, 4), n)
    }

    pub fn calibrate_universe(&self, apply_updates: bool) -> Result<Vec<CalibrationReportRow>> {
        let alt_pairs = self.db.get_alt_pairs_universe()?;
        let mut results = Vec::with_capacity(alt_pairs.len());

        for pair in alt_pairs {
            let mut best_tf = pair.exec_timeframe.clone();
            let mut best_sharpe = -999.0;
            let mut best_ic = 0.0;

            for tf in Self::CANDIDATE_TIMEFRAMES {
                let (sharpe, ic, n_bars) =
                    self.evaluate_pair_on_timeframe(&pair.exec_symbol, &pair.synth_symbol, tf);
                if n_bars >= 80 && sharpe > best_sharpe {
                    best_sharpe = sharpe;
                    best_ic = ic;
                    best_tf = tf.to_string();
                }
            }

            if best_sharpe == -999.0 {
                best_sharpe = 0.0;
            }
            let updated = apply_updates && best_tf != pair.exec_timeframe;
            if updated {
                self.db
                    .update_asset_timeframes(&pair.base_asset, &best_tf, &best_tf)?;
            }

            results.push(CalibrationReportRow {
                base_asset: pair.base_asset,
                tier: pair.tier,
                current_exec_tf: pair.exec_timeframe,
                current_rs_tf: pair.rs_timeframe,
                optimal_exec_tf: best_tf.clone(),
                optimal_rs_tf: best_tf,
                best_oos_sharpe: best_sharpe,
                best_oos_ic: best_ic,
                updated,
            });
        }

        if apply_updates {
            self.signal_engine.clear_caches();
        }

        self.db.log_telemetry(
            "INFO",
            "engine.calibration",
            &format!(
                "Completed walk-forward timeframe calibration for {} pairs",
                results.len()
            ),
            Some(json!({ "apply_updates": apply_updates })),
        )?;
        Ok(results)
    }
}

#[inline]
pub(crate) fn sample_mean(slice: &[f64]) -> f64 {
    if slice.is_empty() {
        return 0.0;
    }
    slice.iter().sum::<f64>() / (slice.len() as f64)
}

#[inline]
pub(crate) fn sample_std(slice: &[f64], ddof: usize) -> f64 {
    if slice.len() <= ddof {
        return 0.0;
    }
    let mu = sample_mean(slice);
    let var = slice.iter().map(|x| (x - mu).powi(2)).sum::<f64>() / ((slice.len() - ddof) as f64);
    var.max(0.0).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_synthetic_bars(symbol: &str, count: usize, start_px: f64, drift: f64) -> Vec<OhlcvBar> {
        let mut bars = Vec::with_capacity(count);
        let mut px = start_px;
        let base_ts = 1_700_000_000_000i64;
        for i in 0..count {
            let cyc = ((i as f64) * 0.23).sin() * 0.015 + drift;
            let open = px;
            px = (px * (1.0 + cyc)).max(1.0);
            let high = open.max(px) * 1.008;
            let low = open.min(px) * 0.992;
            bars.push(OhlcvBar {
                symbol: symbol.to_string(),
                timeframe: "4h".to_string(),
                timestamp_ms: base_ts + (i as i64) * 14_400_000,
                open,
                high,
                low,
                close: px,
                volume: 1000.0 + ((i % 17) as f64) * 50.0,
                funding_rate_hourly: 0.0001,
                is_synthetic: 0,
            });
        }
        bars
    }

    #[test]
    fn test_sharpe_and_spearman_ic() {
        let rets = vec![0.01, -0.005, 0.012, 0.008, -0.002, 0.015, 0.004, 0.009];
        let sharpe_4h = WalkForwardCalibrator::compute_annualized_sharpe(&rets, "4h").unwrap();
        let sharpe_1h = WalkForwardCalibrator::compute_annualized_sharpe(&rets, "1h").unwrap();
        assert!((sharpe_1h / sharpe_4h - 2.0).abs() < 1e-6);

        let preds = vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6];
        let reals = vec![0.12, 0.19, 0.31, 0.42, 0.49, 0.65];
        let ic = SignalEngine::safe_spearman_ic(&preds, &reals);
        assert!(ic > 0.95);
    }

    #[test]
    fn test_hmm_posterior_probabilities_sum_to_one_and_live_tick_updates() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let engine = SignalEngine::new(db);

        let btc_bars = make_synthetic_bars("BTC/USDC:USDC", 120, 50_000.0, 0.002);
        let states = engine.decode_hmm_regime_states_shared(&btc_bars, 80, None, None);
        let states_cached = engine.decode_hmm_regime_states_shared(&btc_bars, 80, None, None);
        // NL-11: Cache hit returns shared Arc pointer without cloning underlying slice
        assert!(Arc::ptr_eq(&states, &states_cached));
        assert_eq!(states.len(), btc_bars.len());

        for (idx, st) in states.iter().enumerate() {
            let sum = st.prob_bull + st.prob_bear + st.prob_chop;
            assert!(
                (sum - 1.0).abs() <= 1e-3,
                "Bar {idx} posterior probabilities do not sum to 1.0: {sum}"
            );
        }

        // Passing an intrabar live tick close on the same bar timestamp must update the posterior (C5)
        let last_state = states.last().unwrap().clone();
        let shocked_tick = btc_bars.last().unwrap().close * 0.88;
        let updated_state = engine.decode_hmm_regime(&btc_bars, 80, None, Some(shocked_tick));
        assert!(
            (updated_state.prob_bear - last_state.prob_bear).abs() > 1e-4
                || (updated_state.prob_bull - last_state.prob_bull).abs() > 1e-4,
            "Expected intrabar live tick shock to update HMM posterior probabilities"
        );
    }

    #[test]
    fn test_causal_regime_and_mace_invariance_under_future_bar_shuffling() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let engine_a = SignalEngine::new(db.clone());
        let engine_b = SignalEngine::new(db);

        let btc_a = make_synthetic_bars("BTC/USDC:USDC", 150, 60_000.0, 0.001);
        let synth_a = make_synthetic_bars("SOL/BTC:SYNTH", 150, 0.003, 0.0015);
        let usdc_a = make_synthetic_bars("SOL/USDC:USDC", 150, 180.0, 0.0025);

        let mut btc_b = btc_a.clone();
        let mut synth_b = synth_a.clone();
        let mut usdc_b = usdc_a.clone();

        // Heavily perturb all future bars after index 100
        for i in 101..150 {
            btc_b[i].close *= if i % 2 == 0 { 1.45 } else { 0.65 };
            btc_b[i].high = btc_b[i].close * 1.10;
            btc_b[i].low = btc_b[i].close * 0.90;
            synth_b[i].close *= if i % 2 == 0 { 0.55 } else { 1.65 };
            usdc_b[i].close *= if i % 2 == 0 { 0.50 } else { 1.70 };
        }

        let states_a = engine_a.decode_hmm_regime_states(&btc_a, 90, None, None);
        let states_b = engine_b.decode_hmm_regime_states(&btc_b, 90, None, None);
        for t in 0..=100 {
            assert_eq!(states_a[t].regime, states_b[t].regime, "Regime mismatch at bar {t}");
            assert_eq!(states_a[t].prob_bull, states_b[t].prob_bull, "prob_bull mismatch at bar {t}");
            assert_eq!(states_a[t].prob_bear, states_b[t].prob_bear, "prob_bear mismatch at bar {t}");
        }

        let (x_a, y_a, k) = SignalEngine::build_mace_features_and_target(&synth_a, &usdc_a).unwrap();
        let (x_b, y_b, _) = SignalEngine::build_mace_features_and_target(&synth_b, &usdc_b).unwrap();
        for t in 0..=100 {
            assert_eq!(x_a[t], x_b[t], "MACE feature lookahead detected at bar {t}");
        }
        let (mace_a, ic_a) = engine_a.fit_predict_mace_series_from_xy(&x_a, &y_a, k, None);
        let (mace_b, ic_b) = engine_b.fit_predict_mace_series_from_xy(&x_b, &y_b, k, None);
        for t in 0..=96 {
            assert_eq!(mace_a[t], mace_b[t], "MACE prediction lookahead at bar {t}");
            assert_eq!(ic_a[t], ic_b[t], "MACE OOS IC lookahead at bar {t}");
        }
    }

    #[test]
    fn test_mace_cache_refits_on_timestamp_advance_at_fixed_window_length() {
        let db = DatabaseManager::memory();
        db.initialize_schema(true).unwrap();
        let engine = SignalEngine::new(db);

        let full_synth = make_synthetic_bars("ETH/BTC:SYNTH", 180, 0.05, 0.001);
        let full_usdc = make_synthetic_bars("ETH/USDC:USDC", 180, 3000.0, 0.002);

        // Initial fit on window [0..120] (length 120)
        let _ = engine.compute_mace_and_ic(
            &full_synth[0..120],
            &full_usdc[0..120],
            0.35,
            Some("ETH/BTC:SYNTH"),
        );
        let fit_ts_initial = engine
            .mace_cache
            .lock()
            .get("ETH/BTC:SYNTH")
            .unwrap()
            .last_fit_bar_ts;
        assert_eq!(fit_ts_initial, full_synth[119].timestamp_ms);

        // Advance by 10 bars [10..130] (still length 120): cache should be reused (< 36 bars)
        let _ = engine.compute_mace_and_ic(
            &full_synth[10..130],
            &full_usdc[10..130],
            0.35,
            Some("ETH/BTC:SYNTH"),
        );
        let fit_ts_after_10 = engine
            .mace_cache
            .lock()
            .get("ETH/BTC:SYNTH")
            .unwrap()
            .last_fit_bar_ts;
        assert_eq!(fit_ts_after_10, fit_ts_initial);

        // Advance by 40 bars [40..160] (still length 120, >= 36 new bars): must refit!
        let _ = engine.compute_mace_and_ic(
            &full_synth[40..160],
            &full_usdc[40..160],
            0.35,
            Some("ETH/BTC:SYNTH"),
        );
        let fit_ts_after_40 = engine
            .mace_cache
            .lock()
            .get("ETH/BTC:SYNTH")
            .unwrap()
            .last_fit_bar_ts;
        assert_eq!(fit_ts_after_40, full_synth[159].timestamp_ms);
        assert!(fit_ts_after_40 > fit_ts_initial);
    }

    #[test]
    fn test_rs_lookback_requires_full_history() {
        let short_s = vec![1.0; 60];
        let short_u = vec![100.0; 60];
        let (rs24, rs72, rs96, ru8) = compute_rs_lookbacks(&short_s, &short_u, 24, 72, 96, 8);
        assert_eq!((rs24, rs72, rs96), (0.0, 0.0, 0.0));
        assert_eq!(ru8, 0.0);
    }

    #[test]
    fn test_live_stale_candles_forced_flat_nm8() {
        let db = DatabaseManager::memory();
        db.initialize_schema(false).unwrap();
        db.set_config_value("execution_mode", &json!("LIVE")).unwrap();

        // Insert ancient candles from year 2023 (far older than max_bar_staleness_ms)
        let btc_bars = make_synthetic_bars("BTC/USDC:USDC", 120, 68_000.0, 0.005);
        let sol_u = make_synthetic_bars("SOL/USDC:USDC", 120, 180.0, 0.008);
        let sol_s = make_synthetic_bars("SOL/BTC:SYNTH", 120, 0.0026, 0.003);
        db.upsert_ohlcv_batch(&btc_bars).unwrap();
        db.upsert_ohlcv_batch(&sol_u).unwrap();
        db.upsert_ohlcv_batch(&sol_s).unwrap();

        let engine = SignalEngine::new(db);
        let evals = engine.evaluate_universe(false).unwrap();
        assert!(!evals.is_empty());
        assert!(
            evals.iter().all(|e| e.signal_action == "FLAT"),
            "Stale live candles must force FLAT signal actions"
        );
    }
}
