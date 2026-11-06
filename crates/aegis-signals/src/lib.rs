//! Multi-Layer Quantitative Signal & Regime Engine (`aegis-signals`).
//!
//! 1:1 port of `engine/signals.py` and `engine/calibration.py`:
//! - **Layer 1:** 3-State Diagonal Gaussian HMM (`BULL`, `BEAR`, `CHOP`) with MAP directional priors,
//!   sticky transition priors, causal rolling-window decoding (`states.len() == bars.len()`),
//!   4H bar-close cache + 15s single-step forward filtering (`alpha_{T-1} -> alpha_T`),
//!   causal cross-sectional `alt_pc1_ret` via rolling covariance power-iteration, and hysteresis transition gates.
//! - **Layer 2:** Continuous Signal-to-Noise Ratio (`SNR` in `[-1, +1]`) blending `60%` `ALT/BTC:SYNTH`
//!   and `40%` `ALT/USDC:USDC` causal EMA trend/volatility ratio (`16`/`64` spans).
//! - **Layer 3:** Out-of-Sample Purged MACE (`6` base features causally standardized -> `18` nonlinear features,
//!   purged `k=4` bars, timestamp-keyed refit every `36` bars with L2 Ridge + Random Forest Regressor ensemble)
//!   and causal Spearman Rank IC.
//! - **Layer 4:** Calibration 2 Cross-Sectional Ranking, BTC Macro Confirmation, and Walk-Forward Calibrator.

#![allow(clippy::needless_range_loop, clippy::manual_memcpy)]

use aegis_core::{round_dp, AegisError, OhlcvBar, RegimeState, Result, SignalEvaluation};
use aegis_db::{ConfigMap, DatabaseManager};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::f64::consts::PI;
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

/// 3-State Diagonal-Covariance Gaussian Hidden Markov Model (`BULL`, `BEAR`, `CHOP`).
#[derive(Debug, Clone)]
pub struct DiagonalGaussianHMM {
    pub n_states: usize,
    pub max_iter: usize,
    pub tol: f64,
    pub pi: [f64; 3],
    pub trans: [[f64; 3]; 3],
    pub means: [[f64; 3]; 3],
    pub vars: [[f64; 3]; 3],
    pub last_alpha: Option<[f64; 3]>,
}

impl Default for DiagonalGaussianHMM {
    fn default() -> Self {
        Self::new(3, 18, 1e-4)
    }
}

impl DiagonalGaussianHMM {
    pub fn new(_n_states: usize, max_iter: usize, tol: f64) -> Self {
        Self {
            n_states: 3,
            max_iter,
            tol,
            pi: [1.0 / 3.0; 3],
            trans: [
                [0.90, 0.08, 0.02],
                [0.08, 0.84, 0.08],
                [0.02, 0.08, 0.90],
            ],
            means: [
                [-0.55, 0.0, -0.35],
                [0.0, 0.0, 0.0],
                [0.55, 0.0, 0.35],
            ],
            vars: [[1.0; 3]; 3],
            last_alpha: None,
        }
    }

    #[inline]
    fn log_emission_single(&self, obs: &[f64; 3]) -> [f64; 3] {
        let mut log_b = [0.0; 3];
        for k in 0..3 {
            let mut sum = 0.0;
            for d in 0..3 {
                let v = self.vars[k][d].max(1e-4);
                let diff = obs[d] - self.means[k][d];
                sum += -0.5 * ((2.0 * PI * v).ln() + (diff * diff) / v);
            }
            log_b[k] = sum;
        }
        log_b
    }

    fn log_emission(&self, x: &[[f64; 3]]) -> Vec<[f64; 3]> {
        x.iter().map(|obs| self.log_emission_single(obs)).collect()
    }

    #[inline]
    fn logsumexp(arr: &[f64; 3]) -> f64 {
        let m = arr[0].max(arr[1]).max(arr[2]);
        if !m.is_finite() {
            return -1e30;
        }
        m + ((arr[0] - m).exp() + (arr[1] - m).exp() + (arr[2] - m).exp()).ln()
    }

    fn forward(&self, log_b: &[[f64; 3]]) -> (Vec<[f64; 3]>, f64) {
        let t_len = log_b.len();
        let mut log_alpha = vec![[0.0; 3]; t_len];
        let mut log_pi = [0.0; 3];
        let mut log_a = [[0.0; 3]; 3];
        for i in 0..3 {
            log_pi[i] = self.pi[i].max(1e-12).ln();
            for j in 0..3 {
                log_a[i][j] = self.trans[i][j].max(1e-12).ln();
            }
        }

        for k in 0..3 {
            log_alpha[0][k] = log_pi[k] + log_b[0][k];
        }

        for t in 1..t_len {
            for j in 0..3 {
                let candidates = [
                    log_alpha[t - 1][0] + log_a[0][j],
                    log_alpha[t - 1][1] + log_a[1][j],
                    log_alpha[t - 1][2] + log_a[2][j],
                ];
                log_alpha[t][j] = log_b[t][j] + Self::logsumexp(&candidates);
            }
        }

        let log_lik = Self::logsumexp(&log_alpha[t_len - 1]);
        (log_alpha, log_lik)
    }

    fn backward(&self, log_b: &[[f64; 3]]) -> Vec<[f64; 3]> {
        let t_len = log_b.len();
        let mut log_beta = vec![[0.0; 3]; t_len];
        let mut log_a = [[0.0; 3]; 3];
        for i in 0..3 {
            for j in 0..3 {
                log_a[i][j] = self.trans[i][j].max(1e-12).ln();
            }
        }

        for t in (0..(t_len - 1)).rev() {
            for i in 0..3 {
                let candidates = [
                    log_a[i][0] + log_b[t + 1][0] + log_beta[t + 1][0],
                    log_a[i][1] + log_b[t + 1][1] + log_beta[t + 1][1],
                    log_a[i][2] + log_b[t + 1][2] + log_beta[t + 1][2],
                ];
                log_beta[t][i] = Self::logsumexp(&candidates);
            }
        }
        log_beta
    }

    /// Fits the 3-State Gaussian HMM via MAP Baum-Welch EM and caches `last_alpha`.
    pub fn fit(&mut self, x: &[[f64; 3]]) {
        let t_len = x.len();
        if t_len < 6 {
            return;
        }

        let mut sorted_r: Vec<f64> = x.iter().map(|row| row[0]).collect();
        sorted_r.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let q1 = sorted_r[t_len / 3];
        let q2 = sorted_r[(2 * t_len) / 3];

        for state_idx in 0..3 {
            let mut subset: Vec<&[f64; 3]> = x
                .iter()
                .filter(|row| match state_idx {
                    0 => row[0] <= q1,
                    1 => row[0] > q1 && row[0] <= q2,
                    _ => row[0] > q2,
                })
                .collect();
            if subset.len() < 2 {
                subset = x.iter().collect();
            }
            let n = subset.len() as f64;
            for d in 0..3 {
                let mean = subset.iter().map(|r| r[d]).sum::<f64>() / n;
                let var = subset
                    .iter()
                    .map(|r| (r[d] - mean).powi(2))
                    .sum::<f64>()
                    / n
                    + 1e-2;
                self.means[state_idx][d] = mean;
                self.vars[state_idx][d] = var.max(0.08);
            }
        }

        let prior_means_col0 = [-0.55, 0.0, 0.55];
        let prior_weight = 18.0;
        let trans_prior = [
            [48.0, 1.2, 0.05],
            [1.0, 40.0, 1.0],
            [0.05, 1.2, 48.0],
        ];

        let mut prev_ll = f64::NEG_INFINITY;
        for _ in 0..self.max_iter {
            let log_b = self.log_emission(x);
            let (log_alpha, ll) = self.forward(&log_b);
            let log_beta = self.backward(&log_b);

            let mut gamma = vec![[1.0 / 3.0; 3]; t_len];
            for t in 0..t_len {
                let raw = [
                    log_alpha[t][0] + log_beta[t][0],
                    log_alpha[t][1] + log_beta[t][1],
                    log_alpha[t][2] + log_beta[t][2],
                ];
                let norm = Self::logsumexp(&raw);
                let mut row = [
                    (raw[0] - norm).exp(),
                    (raw[1] - norm).exp(),
                    (raw[2] - norm).exp(),
                ];
                let s = (row[0] + row[1] + row[2]).max(1e-12);
                row[0] /= s;
                row[1] /= s;
                row[2] /= s;
                gamma[t] = row;
            }

            let mut log_a = [[0.0; 3]; 3];
            for i in 0..3 {
                for j in 0..3 {
                    log_a[i][j] = self.trans[i][j].max(1e-12).ln();
                }
            }

            let mut xi_sum = [[0.0; 3]; 3];
            for t in 0..(t_len - 1) {
                let mut log_xi_t = [[0.0; 3]; 3];
                let mut max_v = f64::NEG_INFINITY;
                for i in 0..3 {
                    for j in 0..3 {
                        let v =
                            log_alpha[t][i] + log_a[i][j] + log_b[t + 1][j] + log_beta[t + 1][j];
                        log_xi_t[i][j] = v;
                        if v > max_v {
                            max_v = v;
                        }
                    }
                }
                let mut sum_exp = 0.0;
                for i in 0..3 {
                    for j in 0..3 {
                        sum_exp += (log_xi_t[i][j] - max_v).exp();
                    }
                }
                let log_norm = max_v + sum_exp.max(1e-12).ln();
                for i in 0..3 {
                    for j in 0..3 {
                        xi_sum[i][j] += (log_xi_t[i][j] - log_norm).exp();
                    }
                }
            }

            let pi_sum = (gamma[0][0] + gamma[0][1] + gamma[0][2]).max(1e-12);
            for k in 0..3 {
                self.pi[k] = (gamma[0][k] / pi_sum).max(1e-6);
            }
            let pi_norm = self.pi[0] + self.pi[1] + self.pi[2];
            for k in 0..3 {
                self.pi[k] /= pi_norm;
            }

            for i in 0..3 {
                let mut row_sum = 0.0;
                for j in 0..3 {
                    self.trans[i][j] = (xi_sum[i][j] + trans_prior[i][j]).max(1e-6);
                    row_sum += self.trans[i][j];
                }
                for j in 0..3 {
                    self.trans[i][j] /= row_sum.max(1e-12);
                }
            }

            for k in 0..3 {
                let denom = gamma.iter().map(|g| g[k]).sum::<f64>().max(1e-6);
                for d in 0..3 {
                    let weighted_sum = gamma
                        .iter()
                        .zip(x.iter())
                        .map(|(g, row)| g[k] * row[d])
                        .sum::<f64>();
                    let raw_mean = weighted_sum / denom;
                    if d == 0 {
                        self.means[k][0] = (weighted_sum + prior_weight * prior_means_col0[k])
                            / (denom + prior_weight);
                    } else {
                        self.means[k][d] = raw_mean;
                    }
                }
                for d in 0..3 {
                    let var_num = gamma
                        .iter()
                        .zip(x.iter())
                        .map(|(g, row)| g[k] * (row[d] - self.means[k][d]).powi(2))
                        .sum::<f64>();
                    self.vars[k][d] = (var_num / denom + 0.05).max(0.08);
                }
            }

            if (ll - prev_ll).abs() < self.tol {
                break;
            }
            prev_ll = ll;
        }

        // Cache forward posterior at final bar
        let log_b = self.log_emission(x);
        let (log_alpha, _) = self.forward(&log_b);
        let last = &log_alpha[t_len - 1];
        let norm = Self::logsumexp(last);
        let mut alpha = [
            (last[0] - norm).exp(),
            (last[1] - norm).exp(),
            (last[2] - norm).exp(),
        ];
        let s = (alpha[0] + alpha[1] + alpha[2]).max(1e-12);
        alpha[0] /= s;
        alpha[1] /= s;
        alpha[2] /= s;
        self.last_alpha = Some(alpha);
    }

    /// Returns causal filtered posterior probabilities `P(S_t | X_{1:t})` for each bar `t`.
    pub fn predict_proba(&mut self, x: &[[f64; 3]]) -> Vec<[f64; 3]> {
        let log_b = self.log_emission(x);
        let (log_alpha, _) = self.forward(&log_b);
        let mut gamma = Vec::with_capacity(x.len());
        for row in &log_alpha {
            let norm = Self::logsumexp(row);
            let mut g = [
                (row[0] - norm).exp(),
                (row[1] - norm).exp(),
                (row[2] - norm).exp(),
            ];
            let s = (g[0] + g[1] + g[2]).max(1e-12);
            g[0] /= s;
            g[1] /= s;
            g[2] /= s;
            gamma.push(g);
        }
        if let Some(last) = gamma.last() {
            self.last_alpha = Some(*last);
        }
        gamma
    }

    /// Performs a single-step forward Bayesian update from `base_alpha` (or `self.last_alpha`)
    /// without mutating stored HMM parameters (`alpha_{T+1} \propto (alpha_T A) \odot B(x_{T+1})`).
    pub fn step_forward(&self, obs: &[f64; 3], base_alpha: Option<&[f64; 3]>) -> [f64; 3] {
        let alpha_prev = base_alpha
            .copied()
            .or(self.last_alpha)
            .unwrap_or(self.pi);
        let mut prior_next = [0.0; 3];
        for j in 0..3 {
            let mut acc = 0.0;
            for i in 0..3 {
                acc += alpha_prev[i] * self.trans[i][j];
            }
            prior_next[j] = acc.max(1e-12);
        }
        let log_b = self.log_emission_single(obs);
        let log_post = [
            prior_next[0].ln() + log_b[0],
            prior_next[1].ln() + log_b[1],
            prior_next[2].ln() + log_b[2],
        ];
        let norm = Self::logsumexp(&log_post);
        let mut post = [
            (log_post[0] - norm).exp(),
            (log_post[1] - norm).exp(),
            (log_post[2] - norm).exp(),
        ];
        let s = (post[0] + post[1] + post[2]).max(1e-12);
        post[0] /= s;
        post[1] /= s;
        post[2] /= s;
        post
    }
}

/// Simple deterministic Axis-Aligned Regression Tree (`max_depth=4`, `min_samples_leaf=10`)
/// used inside the bagged Random Forest Regressor for Layer 3 MACE.
#[derive(Debug, Clone)]
pub enum RegressionTreeNode {
    Leaf(f64),
    Split {
        feature_idx: usize,
        threshold: f64,
        left: Box<RegressionTreeNode>,
        right: Box<RegressionTreeNode>,
    },
}

impl RegressionTreeNode {
    fn fit(
        x: &[[f64; 18]],
        y: &[f64],
        indices: &[usize],
        feature_subset: &[usize],
        depth: usize,
        max_depth: usize,
        min_samples_leaf: usize,
    ) -> Self {
        if indices.is_empty() {
            return Self::Leaf(0.0);
        }
        let mean_y = indices.iter().map(|&i| y[i]).sum::<f64>() / (indices.len() as f64);
        if depth >= max_depth || indices.len() < 2 * min_samples_leaf {
            return Self::Leaf(mean_y);
        }

        let mut best_gain = 0.0;
        let mut best_feat = 0;
        let mut best_thresh = 0.0;
        let mut best_left = Vec::new();
        let mut best_right = Vec::new();

        let total_var: f64 = indices.iter().map(|&i| (y[i] - mean_y).powi(2)).sum();
        if total_var <= 1e-12 {
            return Self::Leaf(mean_y);
        }

        for &f_idx in feature_subset {
            let mut vals: Vec<f64> = indices.iter().map(|&i| x[i][f_idx]).collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let n = vals.len();
            for q_num in [1, 2, 3, 4, 5, 6] {
                let thresh = vals[(n * q_num) / 8];
                let mut left_idx = Vec::new();
                let mut right_idx = Vec::new();
                let mut left_sum = 0.0;
                let mut right_sum = 0.0;
                for &idx in indices {
                    if x[idx][f_idx] <= thresh {
                        left_idx.push(idx);
                        left_sum += y[idx];
                    } else {
                        right_idx.push(idx);
                        right_sum += y[idx];
                    }
                }
                if left_idx.len() < min_samples_leaf || right_idx.len() < min_samples_leaf {
                    continue;
                }
                let l_mean = left_sum / (left_idx.len() as f64);
                let r_mean = right_sum / (right_idx.len() as f64);
                let l_var: f64 = left_idx.iter().map(|&i| (y[i] - l_mean).powi(2)).sum();
                let r_var: f64 = right_idx.iter().map(|&i| (y[i] - r_mean).powi(2)).sum();
                let gain = total_var - (l_var + r_var);
                if gain > best_gain {
                    best_gain = gain;
                    best_feat = f_idx;
                    best_thresh = thresh;
                    best_left = left_idx;
                    best_right = right_idx;
                }
            }
        }

        if best_gain <= 1e-12 {
            return Self::Leaf(mean_y);
        }

        let left_node = Self::fit(
            x,
            y,
            &best_left,
            feature_subset,
            depth + 1,
            max_depth,
            min_samples_leaf,
        );
        let right_node = Self::fit(
            x,
            y,
            &best_right,
            feature_subset,
            depth + 1,
            max_depth,
            min_samples_leaf,
        );

        Self::Split {
            feature_idx: best_feat,
            threshold: best_thresh,
            left: Box::new(left_node),
            right: Box::new(right_node),
        }
    }

    #[inline]
    fn predict(&self, row: &[f64; 18]) -> f64 {
        match self {
            Self::Leaf(v) => *v,
            Self::Split {
                feature_idx,
                threshold,
                left,
                right,
            } => {
                if row[*feature_idx] <= *threshold {
                    left.predict(row)
                } else {
                    right.predict(row)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CachedMaceModel {
    pub last_fit_len: usize,
    pub last_fit_bar_ts: i64,
    pub last_seen_bar_ts: i64,
    pub bars_advanced_since_fit: usize,
    pub ridge_weights: [f64; 18],
    pub ridge_intercept: f64,
    pub trees: Vec<RegressionTreeNode>,
    pub scale: f64,
    pub last_mace: f64,
    pub last_ic: f64,
}

#[derive(Debug, Clone, Default)]
struct HmmCacheState {
    pub bar_ts: Option<i64>,
    pub bar_len: usize,
    pub window: usize,
    pub hmm: Option<DiagonalGaussianHMM>,
    pub alpha: Option<[f64; 3]>,
    pub alpha_prev: Option<[f64; 3]>,
    pub states: Vec<RegimeState>,
    pub mapping: (usize, usize, usize), // (bear_idx, chop_idx, bull_idx)
    pub r_mean: f64,
    pub r_std: f64,
    pub v_mean: f64,
    pub v_std: f64,
    pub a_mean: f64,
    pub a_std: f64,
    pub prev_close: f64,
    pub last_bar_close: f64,
    pub last_alt_pc1_raw: f64,
    pub last_vol_raw: f64,
    pub prev_ema_r_fast: f64,
    pub prev_ema_r_slow: f64,
    pub last_ema_r_fast: f64,
    pub last_ema_r_slow: f64,
    pub last_ema_v: f64,
    pub last_ema_a: f64,
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
    hmm_cache: Arc<Mutex<HmmCacheState>>,
    mace_cache: Arc<Mutex<HashMap<String, CachedMaceModel>>>,
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

    /// Computes a strictly causal first Principal Component (`alt_pc1_ret`) of standardized
    /// cross-sectional altcoin log returns aligned to `timestamps` using a rolling trailing
    /// window of up to `window` bars ending at each bar `t` (`B6`).
    pub fn compute_alt_pc1_series(
        alt_bars_map: &HashMap<String, Vec<OhlcvBar>>,
        timestamps: &[i64],
        window: usize,
    ) -> Vec<f64> {
        let t_total = timestamps.len();
        if t_total < 10 || alt_bars_map.is_empty() {
            return vec![0.0; t_total];
        }

        let mut aligned_returns: Vec<Vec<f64>> = Vec::new();
        let mut keys: Vec<&String> = alt_bars_map.keys().collect();
        keys.sort();

        for sym in keys {
            let bars = &alt_bars_map[sym];
            if bars.len() < 10 {
                continue;
            }
            let ts_to_close: HashMap<i64, f64> = bars
                .iter()
                .map(|b| (b.timestamp_ms, b.close.max(1e-8)))
                .collect();
            let mut closes = Vec::with_capacity(t_total);
            let mut last_c = bars[0].close.max(1e-8);
            for &ts in timestamps {
                if let Some(&c) = ts_to_close.get(&ts) {
                    last_c = c;
                }
                closes.push(last_c);
            }
            let mut rets = vec![0.0; t_total];
            for i in 1..t_total {
                rets[i] = (closes[i] / closes[i - 1]).ln();
            }
            aligned_returns.push(rets);
        }

        let m_assets = aligned_returns.len();
        if m_assets == 0 {
            return vec![0.0; t_total];
        }

        let eff_win = window.max(10);
        let refit_step = 12usize;
        let mut out = vec![0.0; t_total];
        let mut active_v = vec![1.0 / (m_assets as f64).sqrt(); m_assets];
        let mut active_mu = vec![0.0; m_assets];
        let mut active_sd = vec![1.0; m_assets];
        let mut active_scale = (m_assets as f64).sqrt();

        for t in 10..t_total {
            let w_start = (t + 1).saturating_sub(eff_win);
            let w_len = t + 1 - w_start;

            // Refit causal trailing standardization and M x M covariance eigenvector every `refit_step` bars
            // (and on the final bar if `t_total <= eff_win + 5`)
            if t == 10 || (t - 10) % refit_step == 0 {
                for m in 0..m_assets {
                    let slice = &aligned_returns[m][w_start..=t];
                    active_mu[m] = sample_mean(slice);
                    active_sd[m] = sample_std(slice, 0) + 1e-8;
                }

                let mut cov = vec![vec![0.0; m_assets]; m_assets];
                let mut cs_dot = vec![0.0; m_assets];
                let inv_w = 1.0 / (w_len as f64);

                for tau in w_start..=t {
                    let mut z_row = vec![0.0; m_assets];
                    let mut cs_ret = 0.0;
                    for m in 0..m_assets {
                        let r = aligned_returns[m][tau];
                        cs_ret += r;
                        z_row[m] = (r - active_mu[m]) / active_sd[m];
                    }
                    cs_ret /= m_assets as f64;
                    for a in 0..m_assets {
                        cs_dot[a] += z_row[a] * cs_ret;
                        for b in a..m_assets {
                            cov[a][b] += z_row[a] * z_row[b] * inv_w;
                        }
                    }
                }
                for a in 0..m_assets {
                    for b in 0..a {
                        cov[a][b] = cov[b][a];
                    }
                }

                let mut v = vec![1.0 / (m_assets as f64).sqrt(); m_assets];
                for _ in 0..16 {
                    let mut v_next = vec![0.0; m_assets];
                    for a in 0..m_assets {
                        let mut acc = 0.0;
                        for b in 0..m_assets {
                            acc += cov[a][b] * v[b];
                        }
                        v_next[a] = acc;
                    }
                    let norm = v_next.iter().map(|x| x * x).sum::<f64>().sqrt().max(1e-12);
                    for a in 0..m_assets {
                        v[a] = v_next[a] / norm;
                    }
                }

                let orient: f64 = (0..m_assets).map(|a| v[a] * cs_dot[a]).sum();
                if orient < 0.0 {
                    for val in &mut v {
                        *val = -*val;
                    }
                }

                // Variance of projected score u = Z * v over [w_start..=t] is v^T * cov * v
                let mut lambda1 = 0.0;
                for a in 0..m_assets {
                    for b in 0..m_assets {
                        lambda1 += v[a] * cov[a][b] * v[b];
                    }
                }
                active_scale = lambda1.max(1e-8).sqrt();
                active_v = v;
            }

            let mut proj_t = 0.0;
            for m in 0..m_assets {
                let z_tm = (aligned_returns[m][t] - active_mu[m]) / active_sd[m];
                proj_t += z_tm * active_v[m];
            }
            out[t] = (proj_t / active_scale).clamp(-4.0, 4.0);
        }

        out
    }

    /// Decodes the full causal sequence of `RegimeState` across `btc_bars` (`states.len() == btc_bars.len()`).
    /// Each bar `t` uses only trailing observations `<= t` (zero lookahead, no full-history CHOP padding) (`B6`),
    /// caches the fitted 4H HMM on `last_bar_ts`, and supports 15s single-step forward filtering (`C5`).
    pub fn decode_hmm_regime_states(
        &self,
        btc_bars: &[OhlcvBar],
        window: usize,
        alt_pc1_series: Option<&[f64]>,
        live_tick_close: Option<f64>,
    ) -> Vec<RegimeState> {
        let n_bars = btc_bars.len();
        if n_bars < 15 {
            return vec![
                RegimeState {
                    regime: "CHOP".to_string(),
                    prob_bull: 0.3333,
                    prob_bear: 0.3333,
                    prob_chop: 0.3334,
                    window_bars: n_bars,
                };
                n_bars.max(1)
            ];
        }

        let cfg = self.db.load_config_opts(false).unwrap_or_default();
        let scfg = SignalConfig::from_config_map(&cfg);
        let em_iters = cfg.get_usize("hmm_em_iterations", 18).max(5);
        let em_tol = cfg.get_f64("hmm_em_tolerance", 1e-4).max(1e-7);
        let span_r_fast = cfg.get_f64("hmm_ema_fast_span", 36.0).max(2.0);
        let span_r_slow = cfg.get_f64("hmm_ema_slow_span", 96.0).max(4.0);
        let span_vol = cfg.get_f64("hmm_ema_vol_span", 14.0).max(2.0);
        let span_alt = cfg.get_f64("hmm_ema_alt_span", 14.0).max(2.0);
        let enter_gate = cfg
            .get_f64(
                "hmm_enter_gate_prob",
                cfg.get_f64("hmm_enter_prob_gate", 0.55),
            )
            .clamp(0.45, 0.80);
        let exit_gate = cfg
            .get_f64(
                "hmm_exit_gate_prob",
                cfg.get_f64("hmm_exit_prob_gate", 0.45),
            )
            .clamp(0.25, 0.65);

        let last_bar_ts = btc_bars.last().map(|b| b.timestamp_ms).unwrap_or(0);
        let last_bar_close = btc_bars.last().map(|b| b.close).unwrap_or(0.0);
        {
            let cache = self.hmm_cache.lock();
            if cache.bar_ts == Some(last_bar_ts)
                && cache.bar_len == n_bars
                && cache.window == window
                && cache.states.len() == n_bars
                && cache.hmm.is_some()
            {
                let mut states = cache.states.clone();
                let eff_tick = live_tick_close
                    .filter(|&p| p > 0.0 && (p - cache.last_bar_close).abs() > 1e-9)
                    .or_else(|| {
                        if (last_bar_close - cache.last_bar_close).abs() > 1e-9 {
                            Some(last_bar_close)
                        } else {
                            None
                        }
                    });
                if let Some(tick_px) = eff_tick {
                    if let Some(hmm) = &cache.hmm {
                        let base_alpha = cache.alpha_prev.as_ref().or(cache.alpha.as_ref());
                        let prev_close = cache.prev_close.max(1e-8);
                        let raw_r = (tick_px.max(1e-8) / prev_close).ln();
                        let alpha_f = 2.0 / (span_r_fast + 1.0);
                        let alpha_s = 2.0 / (span_r_slow + 1.0);
                        let ema_r_f = alpha_f * raw_r + (1.0 - alpha_f) * cache.prev_ema_r_fast;
                        let ema_r_s = alpha_s * raw_r + (1.0 - alpha_s) * cache.prev_ema_r_slow;
                        let smooth_r = scfg.hmm_ret_fast_weight * ema_r_f
                            + scfg.hmm_ret_slow_weight * ema_r_s;
                        let r_z = ((smooth_r - cache.r_mean) / cache.r_std).clamp(-3.5, 3.5);
                        let v_z = ((cache.last_ema_v - cache.v_mean) / cache.v_std).clamp(-3.5, 3.5);
                        let a_z = ((cache.last_ema_a - cache.a_mean) / cache.a_std).clamp(-3.5, 3.5);
                        let obs = [r_z, v_z, a_z];
                        let post = hmm.step_forward(&obs, base_alpha);
                        let (bear_idx, chop_idx, bull_idx) = cache.mapping;
                        let p_bull = post[bull_idx];
                        let p_bear = post[bear_idx];
                        let p_chop = post[chop_idx];
                        let prev_regime = if states.len() >= 2 {
                            states[states.len() - 2].regime.as_str()
                        } else {
                            "CHOP"
                        };
                        let next_regime = apply_hysteresis_with_gates(
                            prev_regime,
                            p_bull,
                            p_bear,
                            p_chop,
                            enter_gate,
                            exit_gate,
                        );
                        if let Some(last_state) = states.last_mut() {
                            *last_state = RegimeState {
                                regime: next_regime.to_string(),
                                prob_bull: round_dp(p_bull, 4),
                                prob_bear: round_dp(p_bear, 4),
                                prob_chop: round_dp(p_chop, 4),
                                window_bars: last_state.window_bars,
                            };
                        }
                    }
                }
                return states;
            }
        }

        let w_total = n_bars - 1;
        let mut log_ret = Vec::with_capacity(w_total);
        let mut vol_raw = Vec::with_capacity(w_total);
        for i in 1..n_bars {
            let c_prev = btc_bars[i - 1].close.max(1e-8);
            let c_curr = btc_bars[i].close.max(1e-8);
            log_ret.push((c_curr / c_prev).ln());
            vol_raw.push((btc_bars[i].high - btc_bars[i].low) / c_curr);
        }

        let alt_pc1_raw: Vec<f64> = if let Some(series) = alt_pc1_series {
            if series.len() == n_bars {
                series[1..].to_vec()
            } else if series.len() >= w_total {
                series[series.len() - w_total..].to_vec()
            } else {
                let mut padded = vec![0.0; w_total - series.len()];
                padded.extend_from_slice(series);
                padded
            }
        } else {
            let mut alt_map: HashMap<String, Vec<OhlcvBar>> = HashMap::new();
            if let Ok(usdc_assets) = self.db.get_usdc_perp_universe() {
                for a in usdc_assets {
                    if a.base_asset != "BTC" {
                        if let Ok(abars) = self.db.get_ohlcv_bars(
                            &a.symbol,
                            &a.exec_timeframe,
                            n_bars + 5,
                            None,
                        ) {
                            alt_map.insert(a.symbol, abars);
                        }
                    }
                }
            }
            let ts_slice: Vec<i64> = btc_bars[1..].iter().map(|b| b.timestamp_ms).collect();
            Self::compute_alt_pc1_series(&alt_map, &ts_slice, window)
        };

        let ema_r_fast = Self::causal_ema(&log_ret, span_r_fast);
        let ema_r_slow = Self::causal_ema(&log_ret, span_r_slow);
        let mut smooth_ret = Vec::with_capacity(w_total);
        for i in 0..w_total {
            smooth_ret.push(
                scfg.hmm_ret_fast_weight * ema_r_fast[i]
                    + scfg.hmm_ret_slow_weight * ema_r_slow[i],
            );
        }
        let smooth_vol = Self::causal_ema(&vol_raw, span_vol);
        let smooth_alt = Self::causal_ema(&alt_pc1_raw, span_alt);

        let eff_window = window.max(15);
        let hmm_refit_step = scfg.hmm_refit_step_bars.max(1);
        let min_fit_obs = scfg.hmm_min_fit_obs_bars.max(4); // corresponds to bar index 14 (15th bar)

        let mut states = Vec::with_capacity(n_bars);
        states.push(RegimeState {
            regime: "CHOP".to_string(),
            prob_bull: 0.3333,
            prob_bear: 0.3333,
            prob_chop: 0.3334,
            window_bars: 1,
        });

        let mut active_hmm = DiagonalGaussianHMM::new(3, em_iters, em_tol);
        let mut active_mapping = (0usize, 1usize, 2usize);
        let mut active_alpha = [1.0 / 3.0; 3];
        let mut prev_alpha = [1.0 / 3.0; 3];
        let mut r_mean = 0.0;
        let mut r_std = 1.0;
        let mut v_mean = 0.0;
        let mut v_std = 1.0;
        let mut a_mean = 0.0;
        let mut a_std = 1.0;
        let mut current_regime = "CHOP";

        for idx in 0..w_total {
            let w_start = (idx + 1).saturating_sub(eff_window);
            let w_len = idx + 1 - w_start;
            prev_alpha = active_alpha;

            if idx < min_fit_obs {
                states.push(RegimeState {
                    regime: "CHOP".to_string(),
                    prob_bull: 0.3333,
                    prob_bear: 0.3333,
                    prob_chop: 0.3334,
                    window_bars: w_len,
                });
                continue;
            }

            // Refit HMM parameters causally on trailing window [w_start..=idx] on deterministic schedule
            if idx == min_fit_obs || (idx - min_fit_obs).is_multiple_of(hmm_refit_step) {
                let sr_slice = &smooth_ret[w_start..=idx];
                let sv_slice = &smooth_vol[w_start..=idx];
                let sa_slice = &smooth_alt[w_start..=idx];

                r_mean = sample_mean(sr_slice);
                r_std = sample_std(sr_slice, 0) + 1e-8;
                v_mean = sample_mean(sv_slice);
                v_std = sample_std(sv_slice, 0) + 1e-8;
                a_mean = sample_mean(sa_slice);
                a_std = sample_std(sa_slice, 0) + 1e-8;

                let mut x_obs = Vec::with_capacity(w_len);
                for k in w_start..=idx {
                    let rz = ((smooth_ret[k] - r_mean) / r_std).clamp(-3.5, 3.5);
                    let vz = ((smooth_vol[k] - v_mean) / v_std).clamp(-3.5, 3.5);
                    let az = ((smooth_alt[k] - a_mean) / a_std).clamp(-3.5, 3.5);
                    x_obs.push([rz, vz, az]);
                }

                let mut hmm = DiagonalGaussianHMM::new(3, em_iters, em_tol);
                hmm.fit(&x_obs);
                let gamma = hmm.predict_proba(&x_obs);
                if gamma.len() >= 2 {
                    prev_alpha = gamma[gamma.len() - 2];
                }
                active_alpha = *gamma.last().unwrap_or(&[1.0 / 3.0; 3]);

                let mut order = [0usize, 1, 2];
                order.sort_by(|&i, &j| {
                    hmm.means[i][0]
                        .partial_cmp(&hmm.means[j][0])
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                active_mapping = (order[0], order[1], order[2]);
                active_hmm = hmm;
            } else {
                let rz = ((smooth_ret[idx] - r_mean) / r_std).clamp(-3.5, 3.5);
                let vz = ((smooth_vol[idx] - v_mean) / v_std).clamp(-3.5, 3.5);
                let az = ((smooth_alt[idx] - a_mean) / a_std).clamp(-3.5, 3.5);
                let obs = [rz, vz, az];
                active_alpha = active_hmm.step_forward(&obs, Some(&prev_alpha));
            }

            let (bear_idx, chop_idx, bull_idx) = active_mapping;
            let p_bull = active_alpha[bull_idx];
            let p_bear = active_alpha[bear_idx];
            let p_chop = active_alpha[chop_idx];
            current_regime = apply_hysteresis_with_gates(
                current_regime,
                p_bull,
                p_bear,
                p_chop,
                enter_gate,
                exit_gate,
            );
            let p_b_round = round_dp(p_bull, 4);
            let p_r_round = round_dp(p_bear, 4);
            let p_c_round = round_dp((1.0 - p_b_round - p_r_round).max(0.0), 4);
            states.push(RegimeState {
                regime: current_regime.to_string(),
                prob_bull: p_b_round,
                prob_bear: p_r_round,
                prob_chop: p_c_round,
                window_bars: w_len,
            });
        }

        debug_assert_eq!(states.len(), n_bars);

        {
            let mut cache = self.hmm_cache.lock();
            cache.bar_ts = Some(last_bar_ts);
            cache.bar_len = n_bars;
            cache.window = window;
            cache.alpha = Some(active_alpha);
            cache.alpha_prev = Some(prev_alpha);
            cache.hmm = Some(active_hmm);
            cache.states = states.clone();
            cache.mapping = active_mapping;
            cache.r_mean = r_mean;
            cache.r_std = r_std;
            cache.v_mean = v_mean;
            cache.v_std = v_std;
            cache.a_mean = a_mean;
            cache.a_std = a_std;
            cache.prev_close = if n_bars >= 2 {
                btc_bars[n_bars - 2].close
            } else {
                btc_bars[n_bars - 1].close
            };
            cache.last_bar_close = last_bar_close;
            cache.last_alt_pc1_raw = *alt_pc1_raw.last().unwrap_or(&0.0);
            cache.last_vol_raw = *vol_raw.last().unwrap_or(&0.0);
            cache.prev_ema_r_fast = if ema_r_fast.len() >= 2 {
                ema_r_fast[ema_r_fast.len() - 2]
            } else {
                *ema_r_fast.first().unwrap_or(&0.0)
            };
            cache.prev_ema_r_slow = if ema_r_slow.len() >= 2 {
                ema_r_slow[ema_r_slow.len() - 2]
            } else {
                *ema_r_slow.first().unwrap_or(&0.0)
            };
            cache.last_ema_r_fast = *ema_r_fast.last().unwrap_or(&0.0);
            cache.last_ema_r_slow = *ema_r_slow.last().unwrap_or(&0.0);
            cache.last_ema_v = *smooth_vol.last().unwrap_or(&0.0);
            cache.last_ema_a = *smooth_alt.last().unwrap_or(&0.0);
        }

        if let Some(tick_px) = live_tick_close.filter(|&p| p > 0.0 && (p - last_bar_close).abs() > 1e-9) {
            return self.decode_hmm_regime_states(
                btc_bars,
                window,
                alt_pc1_series,
                Some(tick_px),
            );
        }
        states
    }

    pub fn decode_hmm_regime(
        &self,
        btc_bars: &[OhlcvBar],
        window: usize,
        alt_pc1_series: Option<&[f64]>,
        live_tick_close: Option<f64>,
    ) -> RegimeState {
        let states =
            self.decode_hmm_regime_states(btc_bars, window, alt_pc1_series, live_tick_close);
        states.into_iter().last().unwrap_or_default()
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

    /// Builds 18 nonlinear feature columns `[X_norm, tanh(X_norm), X_norm^2 - 1]` and forward `k=4` target `y`
    /// using strictly causal trailing standardization (`B6`).
    pub fn build_mace_features_and_target(
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
    ) -> Option<(Vec<[f64; 18]>, Vec<f64>, usize)> {
        Self::build_mace_features_and_target_with_config(
            synth_bars,
            usdc_bars,
            &SignalConfig::default(),
        )
    }

    pub fn build_mace_features_and_target_with_config(
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
        scfg: &SignalConfig,
    ) -> Option<(Vec<[f64; 18]>, Vec<f64>, usize)> {
        let n = synth_bars.len().min(usdc_bars.len());
        if n < scfg.mace_min_bars.max(40) {
            return None;
        }
        let s_bars = &synth_bars[synth_bars.len() - n..];
        let u_bars = &usdc_bars[usdc_bars.len() - n..];

        let s_close: Vec<f64> = s_bars.iter().map(|b| b.close.max(1e-12)).collect();
        let u_close: Vec<f64> = u_bars.iter().map(|b| b.close.max(1e-12)).collect();
        let u_vol: Vec<f64> = u_bars.iter().map(|b| b.volume.max(0.0)).collect();

        let lag_ret = |arr: &[f64], lag: usize| -> Vec<f64> {
            let mut out = vec![0.0; arr.len()];
            for i in lag..arr.len() {
                out[i] = (arr[i] / arr[i - lag]).ln();
            }
            out
        };

        let r_s4 = lag_ret(&s_close, 4);
        let r_s16 = lag_ret(&s_close, 16);
        let r_s32 = lag_ret(&s_close, 32);
        let r_u8 = lag_ret(&u_close, 8);
        let r_u24 = lag_ret(&u_close, 24);
        let vol_ema = Self::causal_ema(&u_vol, scfg.mace_vol_ema_span.max(2.0));

        let mut base_cols = vec![vec![0.0; n]; 6];
        for i in 0..n {
            let vol_z = (u_vol[i] - vol_ema[i]) / (vol_ema[i] + 1.0);
            base_cols[0][i] = r_s4[i];
            base_cols[1][i] = r_s16[i];
            base_cols[2][i] = r_s32[i];
            base_cols[3][i] = r_u8[i];
            base_cols[4][i] = r_u24[i];
            base_cols[5][i] = vol_z;
        }

        // Causally standardize base 6 features over trailing window [w_start..=i] (zero future lookahead)
        let mut x_raw = vec![[0.0; 18]; n];
        let z_win = scfg.mace_z_window_bars.max(16);
        for d in 0..6 {
            let col = &base_cols[d];
            for i in 0..n {
                let w_start = if i >= 32 {
                    (i + 1).saturating_sub(z_win).max(32)
                } else {
                    0
                };
                let slice = &col[w_start..=i];
                let mu = sample_mean(slice);
                let sd = sample_std(slice, 0) + 1e-8;
                let z = if slice.len() >= 2 {
                    ((col[i] - mu) / sd).clamp(-3.5, 3.5)
                } else {
                    0.0
                };
                x_raw[i][d] = z;
                x_raw[i][d + 6] = z.tanh();
                x_raw[i][d + 12] = z * z - 1.0;
            }
        }

        let k = scfg.mace_target_horizon_bars.max(1).min(n.saturating_sub(1));
        let mut y_target = vec![0.0; n];
        for i in 0..(n - k) {
            y_target[i] = scfg.snr_synth_weight * (s_close[i + k] / s_close[i]).ln()
                + scfg.snr_usdc_weight * (u_close[i + k] / u_close[i]).ln();
        }
        Some((x_raw, y_target, k))
    }

    /// Fits L2 Ridge + Random Forest Regressor on purged rolling slice `[train_start : t - k]`
    /// and caches fitted models across `mace_refit_interval_bars` (36) bars using bar timestamps (`B7`).
    pub fn fit_predict_mace_series_from_xy(
        &self,
        x_raw: &[[f64; 18]],
        y_target: &[f64],
        k_horizon: usize,
        symbol_key: Option<&str>,
    ) -> (Vec<f64>, Vec<f64>) {
        self.fit_predict_mace_series_with_timestamps(x_raw, y_target, k_horizon, symbol_key, None)
    }

    pub fn fit_predict_mace_series_with_timestamps(
        &self,
        x_raw: &[[f64; 18]],
        y_target: &[f64],
        k_horizon: usize,
        symbol_key: Option<&str>,
        bar_timestamps: Option<&[i64]>,
    ) -> (Vec<f64>, Vec<f64>) {
        let n = x_raw.len();
        if n == 0 {
            return (Vec::new(), Vec::new());
        }
        let cfg = self.db.load_config_opts(false).unwrap_or_default();
        let scfg = SignalConfig::from_config_map(&cfg);
        let warmup = cfg.get_usize("mace_warmup_bars", 32).max(8);
        let min_train_rows = cfg.get_usize("mace_min_train_rows", 16).max(8);
        let train_window = scfg.mace_train_window_bars.max(min_train_rows);
        let refit_interval = cfg.get_usize("mace_refit_interval_bars", 36).max(1);
        let ridge_alpha = cfg.get_f64("mace_ridge_alpha", 1.0).max(1e-4);
        let rf_trees = cfg.get_usize("mace_rf_trees", 30).max(5);
        let rf_max_depth = cfg.get_usize("mace_rf_max_depth", 4).max(2);
        let rf_min_leaf = cfg.get_usize("mace_rf_min_samples_leaf", 10).max(2);
        let rf_seed = cfg.get_i64("mace_rng_seed", 42) as u64;
        let ic_win = scfg.mace_ic_window_bars.max(8);

        let mut mace_series = vec![0.0; n];
        let mut ic_series = vec![0.0; n];

        let last_bar_ts = bar_timestamps
            .and_then(|ts| ts.last().copied())
            .unwrap_or(0);

        // Fast path for live single-bar evaluation when cached model has advanced fewer than `refit_interval` bars (`B7`)
        if let Some(sym) = symbol_key {
            let mut cache = self.mace_cache.lock();
            if let Some(cached) = cache.get_mut(sym) {
                let can_use_cache = if let Some(ts_slice) = bar_timestamps {
                    if cached.last_fit_bar_ts > 0 && last_bar_ts >= cached.last_fit_bar_ts {
                        let bars_elapsed = ts_slice
                            .iter()
                            .filter(|&&ts| ts > cached.last_fit_bar_ts)
                            .count();
                        let has_anchor = ts_slice.iter().any(|&ts| ts <= cached.last_fit_bar_ts);
                        has_anchor && bars_elapsed < refit_interval
                    } else {
                        false
                    }
                } else {
                    if n > cached.last_fit_len {
                        cached.bars_advanced_since_fit += n - cached.last_fit_len;
                        cached.last_fit_len = n;
                    } else {
                        cached.bars_advanced_since_fit += 1;
                    }
                    cached.bars_advanced_since_fit < refit_interval
                };

                if can_use_cache {
                    let x_cur = &x_raw[n - 1];
                    let pred_ridge =
                        predict_ridge(&cached.ridge_weights, cached.ridge_intercept, x_cur);
                    let pred_rf = predict_forest(&cached.trees, x_cur);
                    let raw_pred = 0.50 * pred_ridge + 0.50 * pred_rf;
                    let mace_t = (raw_pred / cached.scale).tanh().clamp(-1.0, 1.0);
                    mace_series[n - 1] = round_dp(mace_t, 4);
                    cached.last_mace = mace_series[n - 1];
                    cached.last_seen_bar_ts = last_bar_ts;
                    return (mace_series, vec![cached.last_ic; n]);
                }
            }
        }

        let start_t = warmup + min_train_rows + k_horizon;
        if start_t >= n {
            return (mace_series, ic_series);
        }

        let eval_indices: Vec<usize> = if symbol_key.is_some() {
            let mut v: Vec<usize> = (start_t..n).step_by(refit_interval).collect();
            if v.last().copied() != Some(n - 1) {
                v.push(n - 1);
            }
            v
        } else {
            (start_t..n).collect()
        };

        let mut active_ridge_w = [0.0; 18];
        let mut active_ridge_b = 0.0;
        let mut active_trees: Vec<RegressionTreeNode> = Vec::new();
        let mut active_scale = 0.015;
        let mut last_fit_t: Option<usize> = None;

        let mut raw_preds = vec![0.0; n];
        let mut oos_preds = Vec::new();
        let mut oos_reals = Vec::new();
        let mut current_ic = 0.0;

        for t in start_t..n {
            let train_end = t - k_horizon;
            if train_end <= warmup + min_train_rows {
                continue;
            }
            let need_refit = active_trees.is_empty()
                || last_fit_t
                    .map(|lt| (t - lt) >= refit_interval)
                    .unwrap_or(true);

            if need_refit && (symbol_key.is_none() || eval_indices.contains(&t)) {
                let train_start = train_end.saturating_sub(train_window).max(warmup);
                let x_train = &x_raw[train_start..train_end];
                let y_train = &y_target[train_start..train_end];
                let (rw, rb) = fit_ridge_l2(x_train, y_train, ridge_alpha);
                let trees =
                    fit_random_forest(x_train, y_train, rf_trees, rf_max_depth, rf_min_leaf, rf_seed);
                let y_sd = sample_std(y_train, 0);
                active_ridge_w = rw;
                active_ridge_b = rb;
                active_trees = trees;
                active_scale = 1.5 * y_sd + 1e-6;
                last_fit_t = Some(t);
            }

            if !active_trees.is_empty() {
                // Only add a prediction to the OOS IC evaluation buffer once its k_horizon forward return
                // has causally completed at or before bar `t` (`tau + k_horizon == t`) (`B6`).
                if t >= start_t + k_horizon {
                    let tau = t - k_horizon;
                    oos_preds.push(raw_preds[tau]);
                    oos_reals.push(y_target[tau]);
                    if oos_preds.len() >= 8 {
                        let tail = oos_preds.len().min(ic_win);
                        current_ic = Self::safe_spearman_ic(
                            &oos_preds[oos_preds.len() - tail..],
                            &oos_reals[oos_reals.len() - tail..],
                        );
                    }
                }

                let x_cur = &x_raw[t];
                let pred_ridge = predict_ridge(&active_ridge_w, active_ridge_b, x_cur);
                let pred_rf = predict_forest(&active_trees, x_cur);
                let raw_pred = 0.50 * pred_ridge + 0.50 * pred_rf;
                raw_preds[t] = raw_pred;
                let mace_t = (raw_pred / active_scale).tanh().clamp(-1.0, 1.0);
                mace_series[t] = round_dp(mace_t, 4);
                ic_series[t] = current_ic;
            }
        }

        if let Some(sym) = symbol_key {
            if !active_trees.is_empty() {
                self.mace_cache.lock().insert(
                    sym.to_string(),
                    CachedMaceModel {
                        last_fit_len: n,
                        last_fit_bar_ts: last_bar_ts,
                        last_seen_bar_ts: last_bar_ts,
                        bars_advanced_since_fit: 0,
                        ridge_weights: active_ridge_w,
                        ridge_intercept: active_ridge_b,
                        trees: active_trees,
                        scale: active_scale,
                        last_mace: mace_series[n - 1],
                        last_ic: current_ic,
                    },
                );
            }
        }

        (mace_series, ic_series)
    }

    /// Computes Spearman Rank Information Coefficient (`IC` in `[-1.0, +1.0]`) with zero-variance guard.
    pub fn safe_spearman_ic(preds: &[f64], reals: &[f64]) -> f64 {
        let n = preds.len().min(reals.len());
        if n < 5 {
            return 0.0;
        }
        let p = &preds[..n];
        let r = &reals[..n];
        if sample_std(p, 0) < 1e-10 || sample_std(r, 0) < 1e-10 {
            return 0.0;
        }
        let rank_p = rank_slice(p);
        let rank_r = rank_slice(r);
        let mp = sample_mean(&rank_p);
        let mr = sample_mean(&rank_r);
        let mut cov = 0.0;
        let mut vp = 0.0;
        let mut vr = 0.0;
        for i in 0..n {
            let dp = rank_p[i] - mp;
            let dr = rank_r[i] - mr;
            cov += dp * dr;
            vp += dp * dp;
            vr += dr * dr;
        }
        let denom = (vp * vr).sqrt();
        if denom < 1e-12 {
            return 0.0;
        }
        let corr = cov / denom;
        if corr.is_finite() {
            round_dp(corr.clamp(-1.0, 1.0), 4)
        } else {
            0.0
        }
    }

    /// Computes dynamic MACE conviction weight bounded in `[mace_dynamic_weight_min, mace_dynamic_weight_max]` based on rolling OOS Spearman IC.
    pub fn compute_dynamic_mace_weight(rolling_ic: f64, base_weight: f64) -> f64 {
        Self::compute_dynamic_mace_weight_with_config(
            rolling_ic,
            base_weight,
            &SignalConfig::default(),
        )
    }

    pub fn compute_dynamic_mace_weight_with_config(
        rolling_ic: f64,
        base_weight: f64,
        scfg: &SignalConfig,
    ) -> f64 {
        let w = if rolling_ic <= 0.0 {
            base_weight + rolling_ic * scfg.mace_ic_neg_slope
        } else {
            base_weight + rolling_ic * scfg.mace_ic_pos_slope
        };
        round_dp(
            w.clamp(scfg.mace_dynamic_weight_min, scfg.mace_dynamic_weight_max),
            4,
        )
    }

    pub fn compute_mace_and_ic(
        &self,
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
        base_mace_weight: f64,
        symbol_key: Option<&str>,
    ) -> (f64, f64, f64) {
        let cfg = self.db.load_config_opts(false).unwrap_or_default();
        let scfg = SignalConfig::from_config_map(&cfg);
        let Some((x_raw, y_target, k)) =
            Self::build_mace_features_and_target_with_config(synth_bars, usdc_bars, &scfg)
        else {
            return (0.0, 0.0, base_mace_weight);
        };
        let n = x_raw.len();
        let s_tail = &synth_bars[synth_bars.len() - n..];
        let ts_vec: Vec<i64> = s_tail.iter().map(|b| b.timestamp_ms).collect();
        let cache_key = symbol_key.or_else(|| synth_bars.last().map(|b| b.symbol.as_str()));
        let (mace_series, ic_series) = self.fit_predict_mace_series_with_timestamps(
            &x_raw,
            &y_target,
            k,
            cache_key,
            Some(&ts_vec),
        );
        let mace = mace_series.last().copied().unwrap_or(0.0);
        let ic = ic_series.last().copied().unwrap_or(0.0);
        let dyn_w = Self::compute_dynamic_mace_weight_with_config(ic, base_mace_weight, &scfg);
        (mace, ic, dyn_w)
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
            let usdc_bars =
                self.db
                    .get_ohlcv_bars(&pair.exec_symbol, &pair.exec_timeframe, 260, None)?;
            let synth_bars =
                self.db
                    .get_ohlcv_bars(&pair.synth_symbol, &pair.rs_timeframe, 260, None)?;
            if usdc_bars.len() < 20 || synth_bars.len() < 20 {
                continue;
            }

            let is_unsupported_live = is_live_mode
                && usdc_bars
                    .last()
                    .map(|b| b.is_synthetic == 1)
                    .unwrap_or(false);

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

        // Rank tradable live assets ahead of unsupported synthetic assets
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

fn apply_hysteresis_with_gates(
    current_regime: &str,
    p_bull: f64,
    p_bear: f64,
    p_chop: f64,
    enter_gate: f64,
    exit_gate: f64,
) -> &'static str {
    match current_regime {
        "BULL" => {
            if p_bull >= exit_gate && p_bull >= p_bear {
                "BULL"
            } else if p_bear >= enter_gate {
                "BEAR"
            } else {
                "CHOP"
            }
        }
        "BEAR" => {
            if p_bear >= exit_gate && p_bear >= p_bull {
                "BEAR"
            } else if p_bull >= enter_gate {
                "BULL"
            } else {
                "CHOP"
            }
        }
        _ => {
            if p_bull >= enter_gate && p_bull > p_bear && p_bull > p_chop {
                "BULL"
            } else if p_bear >= enter_gate && p_bear > p_bull && p_bear > p_chop {
                "BEAR"
            } else {
                "CHOP"
            }
        }
    }
}

#[inline]
fn sample_mean(slice: &[f64]) -> f64 {
    if slice.is_empty() {
        return 0.0;
    }
    slice.iter().sum::<f64>() / (slice.len() as f64)
}

#[inline]
fn sample_std(slice: &[f64], ddof: usize) -> f64 {
    if slice.len() <= ddof {
        return 0.0;
    }
    let mu = sample_mean(slice);
    let var = slice.iter().map(|x| (x - mu).powi(2)).sum::<f64>() / ((slice.len() - ddof) as f64);
    var.max(0.0).sqrt()
}

fn rank_slice(slice: &[f64]) -> Vec<f64> {
    let mut indexed: Vec<(usize, f64)> = slice.iter().copied().enumerate().collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut ranks = vec![0.0; slice.len()];
    for (rank, (orig_idx, _)) in indexed.into_iter().enumerate() {
        ranks[orig_idx] = rank as f64;
    }
    ranks
}

fn fit_ridge_l2(x: &[[f64; 18]], y: &[f64], alpha: f64) -> ([f64; 18], f64) {
    let n = x.len().min(y.len());
    if n == 0 {
        return ([0.0; 18], 0.0);
    }
    let y_mean = sample_mean(&y[..n]);
    let mut x_mean = [0.0; 18];
    for row in &x[..n] {
        for j in 0..18 {
            x_mean[j] += row[j] / (n as f64);
        }
    }

    let mut a_mat = [[0.0f64; 18]; 18];
    let mut b_vec = [0.0f64; 18];

    for i in 0..n {
        let dy = y[i] - y_mean;
        for r in 0..18 {
            let xr = x[i][r] - x_mean[r];
            b_vec[r] += xr * dy;
            for c in r..18 {
                let xc = x[i][c] - x_mean[c];
                a_mat[r][c] += xr * xc;
            }
        }
    }
    for r in 0..18 {
        a_mat[r][r] += alpha.max(1e-6);
        for c in 0..r {
            a_mat[r][c] = a_mat[c][r];
        }
    }

    // Solve 18x18 SPD system (A * w = b) via Gaussian elimination with partial pivoting
    let mut weights = [0.0f64; 18];
    for k in 0..18 {
        let mut pivot_row = k;
        let mut max_val = a_mat[k][k].abs();
        for i in (k + 1)..18 {
            if a_mat[i][k].abs() > max_val {
                max_val = a_mat[i][k].abs();
                pivot_row = i;
            }
        }
        if max_val < 1e-12 {
            continue;
        }
        if pivot_row != k {
            a_mat.swap(k, pivot_row);
            b_vec.swap(k, pivot_row);
        }
        let pivot = a_mat[k][k];
        for i in (k + 1)..18 {
            let factor = a_mat[i][k] / pivot;
            b_vec[i] -= factor * b_vec[k];
            for j in k..18 {
                a_mat[i][j] -= factor * a_mat[k][j];
            }
        }
    }
    for i in (0..18).rev() {
        let mut sum = b_vec[i];
        for j in (i + 1)..18 {
            sum -= a_mat[i][j] * weights[j];
        }
        let diag = a_mat[i][i];
        weights[i] = if diag.abs() > 1e-12 { sum / diag } else { 0.0 };
    }

    let mut dot_mean = 0.0;
    for j in 0..18 {
        dot_mean += weights[j] * x_mean[j];
    }
    let intercept = y_mean - dot_mean;
    (weights, intercept)
}

#[inline]
fn predict_ridge(weights: &[f64; 18], intercept: f64, row: &[f64; 18]) -> f64 {
    let mut acc = intercept;
    for j in 0..18 {
        acc += weights[j] * row[j];
    }
    acc
}

fn fit_random_forest(
    x: &[[f64; 18]],
    y: &[f64],
    n_trees: usize,
    max_depth: usize,
    min_samples_leaf: usize,
    seed: u64,
) -> Vec<RegressionTreeNode> {
    let n = x.len().min(y.len());
    if n == 0 {
        return Vec::new();
    }
    let mut trees = Vec::with_capacity(n_trees);
    let mut rng = StdRng::seed_from_u64(seed);

    for _ in 0..n_trees {
        let mut sample_indices = Vec::with_capacity(n);
        for _ in 0..n {
            sample_indices.push(rng.gen_range(0..n));
        }
        let mut feat_subset = Vec::with_capacity(8);
        for k in 0..8 {
            feat_subset.push((rng.gen_range(0..18) + k * 3) % 18);
        }
        feat_subset.sort_unstable();
        feat_subset.dedup();
        trees.push(RegressionTreeNode::fit(
            x,
            y,
            &sample_indices,
            &feat_subset,
            0,
            max_depth,
            min_samples_leaf,
        ));
    }
    trees
}

#[inline]
fn predict_forest(trees: &[RegressionTreeNode], row: &[f64; 18]) -> f64 {
    if trees.is_empty() {
        return 0.0;
    }
    let sum: f64 = trees.iter().map(|t| t.predict(row)).sum();
    sum / (trees.len() as f64)
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
        let states = engine.decode_hmm_regime_states(&btc_bars, 80, None, None);
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
}
