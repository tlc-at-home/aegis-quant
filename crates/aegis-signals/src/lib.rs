//! Multi-Layer Quantitative Signal & Regime Engine (`aegis-signals`).
//!
//! 1:1 port of `engine/signals.py` and `engine/calibration.py`:
//! - **Layer 1:** 3-State Diagonal Gaussian HMM (`BULL`, `BEAR`, `CHOP`) with MAP directional priors,
//!   sticky transition priors, 4H bar-close cache + 15s single-step forward filtering (`alpha_T`),
//!   cross-sectional `alt_pc1_ret` via SVD/power-iteration, and hysteresis transition gates (`0.55`/`0.45`).
//! - **Layer 2:** Continuous Signal-to-Noise Ratio (`SNR` in `[-1, +1]`) blending `60%` `ALT/BTC:SYNTH`
//!   and `40%` `ALT/USDC:USDC` causal EMA trend/volatility ratio (`16`/`64` spans).
//! - **Layer 3:** Out-of-Sample Purged MACE (`6` base features -> `18` nonlinear features, purged `k=4` bars,
//!   refit every `36` bars with L2 Ridge + 60-tree Random Forest Regressor ensemble) and Spearman Rank IC.
//! - **Layer 4:** Calibration 2 Cross-Sectional Ranking, BTC Macro Confirmation, and Walk-Forward Calibrator.

use aegis_core::{round_dp, AegisError, OhlcvBar, RegimeState, Result, SignalEvaluation};
use aegis_db::DatabaseManager;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::f64::consts::PI;
use std::sync::Arc;

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
    pub fn new(n_states: usize, max_iter: usize, tol: f64) -> Self {
        Self {
            n_states: n_states.min(3).max(3),
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
                        let v = log_alpha[t][i] + log_a[i][j] + log_b[t + 1][j] + log_beta[t + 1][j];
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
                    let weighted_sum = gamma.iter().zip(x.iter()).map(|(g, row)| g[k] * row[d]).sum::<f64>();
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
/// used inside the 60-tree bagged Random Forest Regressor for Layer 3 MACE.
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
            // Evaluate 7 quantile candidate split points for speed and robustness
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
    pub states: Vec<RegimeState>,
    pub mapping: (usize, usize, usize), // (bear_idx, chop_idx, bull_idx)
    pub r_mean: f64,
    pub r_std: f64,
    pub v_mean: f64,
    pub v_std: f64,
    pub a_mean: f64,
    pub a_std: f64,
    pub prev_close: f64,
    pub last_alt_pc1_raw: f64,
    pub last_vol_raw: f64,
    pub last_ema_r_fast: f64,
    pub last_ema_r_slow: f64,
    pub last_ema_v: f64,
    pub last_ema_a: f64,
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

    /// Computes the first Principal Component (`alt_pc1_ret`) of standardized cross-sectional
    /// altcoin log returns aligned to `timestamps` via power iteration / SVD on the covariance matrix.
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
            let std_val = sample_std(&rets, 0);
            if std_val > 1e-10 {
                aligned_returns.push(rets);
            }
        }

        let m_assets = aligned_returns.len();
        if m_assets == 0 {
            return vec![0.0; t_total];
        }

        let w = window.min(t_total);
        let start_idx = t_total - w;

        // Build standardized W x M matrix Z
        let mut z_mat = vec![vec![0.0; m_assets]; w];
        let mut mean_cs = vec![0.0; w];
        for (m, col) in aligned_returns.iter().enumerate() {
            let slice = &col[start_idx..];
            let mu = sample_mean(slice);
            let sd = sample_std(slice, 0) + 1e-8;
            for t in 0..w {
                let z = (slice[t] - mu) / sd;
                z_mat[t][m] = z;
                mean_cs[t] += slice[t] / (m_assets as f64);
            }
        }

        // Compute M x M covariance C = Z^T Z and dominant eigenvector v via 20 power iterations
        let mut v = vec![1.0 / (m_assets as f64).sqrt(); m_assets];
        for _ in 0..20 {
            // u = Z * v (length W)
            let mut u = vec![0.0; w];
            for t in 0..w {
                let mut acc = 0.0;
                for m in 0..m_assets {
                    acc += z_mat[t][m] * v[m];
                }
                u[t] = acc;
            }
            // v_next = Z^T * u (length M)
            let mut v_next = vec![0.0; m_assets];
            for m in 0..m_assets {
                let mut acc = 0.0;
                for t in 0..w {
                    acc += z_mat[t][m] * u[t];
                }
                v_next[m] = acc;
            }
            let norm = v_next.iter().map(|x| x * x).sum::<f64>().sqrt().max(1e-12);
            for m in 0..m_assets {
                v[m] = v_next[m] / norm;
            }
        }

        let mut pc1 = vec![0.0; w];
        let mut dot_cs = 0.0;
        for t in 0..w {
            let mut acc = 0.0;
            for m in 0..m_assets {
                acc += z_mat[t][m] * v[m];
            }
            pc1[t] = acc;
            dot_cs += pc1[t] * mean_cs[t];
        }
        if dot_cs < 0.0 {
            for val in &mut pc1 {
                *val = -*val;
            }
        }
        let pc1_mu = sample_mean(&pc1);
        let pc1_sd = sample_std(&pc1, 0) + 1e-8;
        for val in &mut pc1 {
            *val = (*val - pc1_mu) / pc1_sd;
        }

        let mut full = vec![0.0; t_total];
        for (i, val) in pc1.into_iter().enumerate() {
            full[start_idx + i] = val;
        }
        full
    }

    /// Decodes the full causal sequence of `RegimeState` across `btc_bars`.
    /// Caches the fitted 4H HMM on `last_bar_ts` and supports 15s single-step forward filtering.
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

        let last_bar_ts = btc_bars.last().map(|b| b.timestamp_ms).unwrap_or(0);
        {
            let cache = self.hmm_cache.lock();
            if cache.bar_ts == Some(last_bar_ts)
                && cache.bar_len == n_bars
                && cache.window == window
                && !cache.states.is_empty()
                && cache.hmm.is_some()
            {
                let mut states = cache.states.clone();
                if let Some(tick_px) = live_tick_close.filter(|&p| p > 0.0) {
                    if let (Some(hmm), Some(alpha)) = (&cache.hmm, &cache.alpha) {
                        let prev_close = cache.prev_close.max(1e-8);
                        let raw_r = (tick_px.max(1e-8) / prev_close).ln();
                        let alpha_f = 2.0 / (36.0 + 1.0);
                        let alpha_s = 2.0 / (96.0 + 1.0);
                        let ema_r_f = alpha_f * raw_r + (1.0 - alpha_f) * cache.last_ema_r_fast;
                        let ema_r_s = alpha_s * raw_r + (1.0 - alpha_s) * cache.last_ema_r_slow;
                        let smooth_r = 0.65 * ema_r_f + 0.35 * ema_r_s;
                        let r_z = ((smooth_r - cache.r_mean) / cache.r_std).clamp(-3.5, 3.5);
                        let v_z = ((cache.last_ema_v - cache.v_mean) / cache.v_std).clamp(-3.5, 3.5);
                        let a_z = ((cache.last_ema_a - cache.a_mean) / cache.a_std).clamp(-3.5, 3.5);
                        let obs = [r_z, v_z, a_z];
                        let post = hmm.step_forward(&obs, Some(alpha));
                        let (bear_idx, chop_idx, bull_idx) = cache.mapping;
                        let p_bull = post[bull_idx];
                        let p_bear = post[bear_idx];
                        let p_chop = post[chop_idx];
                        let prev_regime = states
                            .last()
                            .map(|s| s.regime.as_str())
                            .unwrap_or("CHOP");
                        let next_regime =
                            apply_hysteresis(prev_regime, p_bull, p_bear, p_chop);
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

        let slice_start = n_bars.saturating_sub(window + 1);
        let recent = &btc_bars[slice_start..];
        let m_len = recent.len();
        let w = m_len - 1;

        let mut log_ret = Vec::with_capacity(w);
        let mut vol_raw = Vec::with_capacity(w);
        for i in 1..m_len {
            let c_prev = recent[i - 1].close.max(1e-8);
            let c_curr = recent[i].close.max(1e-8);
            log_ret.push((c_curr / c_prev).ln());
            vol_raw.push((recent[i].high - recent[i].low) / c_curr);
        }

        let alt_pc1_raw: Vec<f64> = if let Some(series) = alt_pc1_series {
            if series.len() >= w {
                series[series.len() - w..].to_vec()
            } else {
                vec![0.0; w]
            }
        } else {
            let mut alt_map: HashMap<String, Vec<OhlcvBar>> = HashMap::new();
            if let Ok(usdc_assets) = self.db.get_usdc_perp_universe() {
                for a in usdc_assets {
                    if a.base_asset != "BTC" {
                        if let Ok(abars) =
                            self.db.get_ohlcv_bars(&a.symbol, &a.exec_timeframe, window + 5, None)
                        {
                            alt_map.insert(a.symbol, abars);
                        }
                    }
                }
            }
            let ts_slice: Vec<i64> = recent[1..].iter().map(|b| b.timestamp_ms).collect();
            Self::compute_alt_pc1_series(&alt_map, &ts_slice, w)
        };

        let ema_r_fast = Self::causal_ema(&log_ret, 36.0);
        let ema_r_slow = Self::causal_ema(&log_ret, 96.0);
        let mut smooth_ret = Vec::with_capacity(w);
        for i in 0..w {
            smooth_ret.push(0.65 * ema_r_fast[i] + 0.35 * ema_r_slow[i]);
        }
        let smooth_vol = Self::causal_ema(&vol_raw, 14.0);
        let smooth_alt = Self::causal_ema(&alt_pc1_raw, 14.0);

        let r_mean = sample_mean(&smooth_ret);
        let r_std = sample_std(&smooth_ret, 0) + 1e-8;
        let v_mean = sample_mean(&smooth_vol);
        let v_std = sample_std(&smooth_vol, 0) + 1e-8;
        let a_mean = sample_mean(&smooth_alt);
        let a_std = sample_std(&smooth_alt, 0) + 1e-8;

        let mut x_obs = Vec::with_capacity(w);
        for i in 0..w {
            let r_z = ((smooth_ret[i] - r_mean) / r_std).clamp(-3.5, 3.5);
            let v_z = ((smooth_vol[i] - v_mean) / v_std).clamp(-3.5, 3.5);
            let a_z = ((smooth_alt[i] - a_mean) / a_std).clamp(-3.5, 3.5);
            x_obs.push([r_z, v_z, a_z]);
        }

        let mut hmm = DiagonalGaussianHMM::new(3, 18, 1e-4);
        hmm.fit(&x_obs);
        let gamma = hmm.predict_proba(&x_obs);

        // Identify states by directional mean return (bear < chop < bull)
        let mut order = [0usize, 1, 2];
        order.sort_by(|&i, &j| {
            hmm.means[i][0]
                .partial_cmp(&hmm.means[j][0])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let (bear_idx, chop_idx, bull_idx) = (order[0], order[1], order[2]);

        let mut states = Vec::with_capacity(n_bars);
        states.push(RegimeState {
            regime: "CHOP".to_string(),
            prob_bull: 0.3333,
            prob_bear: 0.3333,
            prob_chop: 0.3334,
            window_bars: w,
        });

        let mut current_regime = "CHOP";
        for probs in &gamma {
            let p_bull = probs[bull_idx];
            let p_bear = probs[bear_idx];
            let p_chop = probs[chop_idx];
            current_regime = apply_hysteresis(current_regime, p_bull, p_bear, p_chop);
            states.push(RegimeState {
                regime: current_regime.to_string(),
                prob_bull: round_dp(p_bull, 4),
                prob_bear: round_dp(p_bear, 4),
                prob_chop: round_dp(p_chop, 4),
                window_bars: w,
            });
        }

        if states.len() < n_bars {
            let pad = states[0].clone();
            let mut padded = vec![pad; n_bars - states.len()];
            padded.extend(states);
            states = padded;
        } else if states.len() > n_bars {
            states = states[states.len() - n_bars..].to_vec();
        }

        {
            let mut cache = self.hmm_cache.lock();
            cache.bar_ts = Some(last_bar_ts);
            cache.bar_len = n_bars;
            cache.window = window;
            cache.alpha = hmm.last_alpha;
            cache.hmm = Some(hmm);
            cache.states = states.clone();
            cache.mapping = (bear_idx, chop_idx, bull_idx);
            cache.r_mean = r_mean;
            cache.r_std = r_std;
            cache.v_mean = v_mean;
            cache.v_std = v_std;
            cache.a_mean = a_mean;
            cache.a_std = a_std;
            cache.prev_close = if m_len >= 2 {
                recent[m_len - 2].close
            } else {
                recent[m_len - 1].close
            };
            cache.last_alt_pc1_raw = *alt_pc1_raw.last().unwrap_or(&0.0);
            cache.last_vol_raw = *vol_raw.last().unwrap_or(&0.0);
            cache.last_ema_r_fast = *ema_r_fast.last().unwrap_or(&0.0);
            cache.last_ema_r_slow = *ema_r_slow.last().unwrap_or(&0.0);
            cache.last_ema_v = *smooth_vol.last().unwrap_or(&0.0);
            cache.last_ema_a = *smooth_alt.last().unwrap_or(&0.0);
        }

        if let Some(tick_px) = live_tick_close.filter(|&p| p > 0.0) {
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

    /// Computes causal continuous SNR series (`[-1.0, +1.0]`) blending `60%` `ALT/BTC:SYNTH`
    /// and `40%` `ALT/USDC:USDC` causal EMA trend/volatility ratio (`16` vs `64` spans).
    pub fn compute_continuous_snr_series(
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
        fast_span: usize,
        slow_span: usize,
    ) -> (Vec<f64>, Vec<f64>) {
        let n_s = synth_bars.len();
        let n_u = usdc_bars.len();
        if n_s < 10 || n_u < 10 {
            let n = n_s.max(n_u).max(1);
            return (vec![0.0; n], vec![0.0; n]);
        }

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
                let raw = (ema_fast[i] - ema_slow[i]) / (mad[i] * 2.5 + 1e-12);
                out.push((raw * 1.25).tanh());
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
            let c = (0.60 * s_tail[i] + 0.40 * u_tail[i]).clamp(-1.0, 1.0);
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

    /// Builds 18 nonlinear feature columns `[X_norm, tanh(X_norm), X_norm^2 - 1]` and forward `k=4` target `y`.
    pub fn build_mace_features_and_target(
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
    ) -> Option<(Vec<[f64; 18]>, Vec<f64>, usize)> {
        let n = synth_bars.len().min(usdc_bars.len());
        if n < 55 {
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
        let vol_ema = Self::causal_ema(&u_vol, 20.0);

        let mut x_raw = vec![[0.0; 18]; n];
        for i in 0..n {
            let vol_z = (u_vol[i] - vol_ema[i]) / (vol_ema[i] + 1.0);
            let base = [r_s4[i], r_s16[i], r_s32[i], r_u8[i], r_u24[i], vol_z];
            for d in 0..6 {
                x_raw[i][d] = base[d];
            }
        }

        // Standardize base 6 features over [32..n] and expand to 18 features
        for d in 0..6 {
            let slice: Vec<f64> = (32..n).map(|i| x_raw[i][d]).collect();
            let mu = sample_mean(&slice);
            let sd = sample_std(&slice, 0) + 1e-8;
            for i in 0..n {
                let z = ((x_raw[i][d] - mu) / sd).clamp(-3.5, 3.5);
                x_raw[i][d] = z;
                x_raw[i][d + 6] = z.tanh();
                x_raw[i][d + 12] = z * z - 1.0;
            }
        }

        let k = 4usize;
        let mut y_target = vec![0.0; n];
        for i in 0..(n - k) {
            y_target[i] = 0.60 * (s_close[i + k] / s_close[i]).ln()
                + 0.40 * (u_close[i + k] / u_close[i]).ln();
        }
        Some((x_raw, y_target, k))
    }

    /// Fits L2 Ridge (`alpha = 1.0`) + 60-tree Random Forest Regressor on purged slice `[warmup : t - k]`
    /// and caches fitted models across 36 bars.
    pub fn fit_predict_mace_series_from_xy(
        &self,
        x_raw: &[[f64; 18]],
        y_target: &[f64],
        k_horizon: usize,
        symbol_key: Option<&str>,
    ) -> (Vec<f64>, Vec<f64>) {
        let n = x_raw.len();
        let warmup = 32usize;
        let min_train_rows = 16usize;
        let refit_interval = 36usize;

        let mut mace_series = vec![0.0; n];
        let mut oos_preds = Vec::new();
        let mut oos_reals = Vec::new();

        // Fast path for live single-bar evaluation when cached model is within 36 bars
        if let Some(sym) = symbol_key {
            let mut cache = self.mace_cache.lock();
            if let Some(cached) = cache.get_mut(sym) {
                if n >= cached.last_fit_len && (n - cached.last_fit_len) < refit_interval {
                    let x_cur = &x_raw[n - 1];
                    let pred_ridge = predict_ridge(&cached.ridge_weights, cached.ridge_intercept, x_cur);
                    let pred_rf = predict_forest(&cached.trees, x_cur);
                    let raw_pred = 0.50 * pred_ridge + 0.50 * pred_rf;
                    let mace_t = (raw_pred / cached.scale).tanh().clamp(-1.0, 1.0);
                    mace_series[n - 1] = round_dp(mace_t, 4);
                    cached.last_mace = mace_series[n - 1];
                    return (mace_series, vec![cached.last_ic; n]);
                }
            }
        }

        let start_t = warmup + min_train_rows + k_horizon;
        if start_t >= n {
            return (mace_series, vec![0.0; n]);
        }

        // For live symbol evaluation, only fit on the latest purged window once!
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

        for t in start_t..n {
            let train_end = t - k_horizon;
            if train_end <= warmup + min_train_rows {
                continue;
            }
            let need_refit = active_trees.is_empty()
                || last_fit_t.map(|lt| (t - lt) >= refit_interval).unwrap_or(true);

            if need_refit && (symbol_key.is_none() || eval_indices.contains(&t)) {
                let x_train = &x_raw[warmup..train_end];
                let y_train = &y_target[warmup..train_end];
                let (rw, rb) = fit_ridge_l2(x_train, y_train, 1.0);
                let trees = fit_random_forest(x_train, y_train, 30, 4, 10);
                let y_sd = sample_std(y_train, 0);
                active_ridge_w = rw;
                active_ridge_b = rb;
                active_trees = trees;
                active_scale = 1.5 * y_sd + 1e-6;
                last_fit_t = Some(t);
            }

            if !active_trees.is_empty() {
                let x_cur = &x_raw[t];
                let pred_ridge = predict_ridge(&active_ridge_w, active_ridge_b, x_cur);
                let pred_rf = predict_forest(&active_trees, x_cur);
                let raw_pred = 0.50 * pred_ridge + 0.50 * pred_rf;
                let mace_t = (raw_pred / active_scale).tanh().clamp(-1.0, 1.0);
                mace_series[t] = round_dp(mace_t, 4);

                if t < n - k_horizon {
                    oos_preds.push(raw_pred);
                    oos_reals.push(y_target[t]);
                }
            }
        }

        let ic = if oos_preds.len() >= 8 {
            let tail = oos_preds.len().min(36);
            Self::safe_spearman_ic(
                &oos_preds[oos_preds.len() - tail..],
                &oos_reals[oos_reals.len() - tail..],
            )
        } else {
            0.0
        };

        if let Some(sym) = symbol_key {
            if !active_trees.is_empty() {
                self.mace_cache.lock().insert(
                    sym.to_string(),
                    CachedMaceModel {
                        last_fit_len: n,
                        ridge_weights: active_ridge_w,
                        ridge_intercept: active_ridge_b,
                        trees: active_trees,
                        scale: active_scale,
                        last_mace: mace_series[n - 1],
                        last_ic: ic,
                    },
                );
            }
        }

        (mace_series, vec![ic; n])
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

    /// Computes dynamic MACE conviction weight bounded in `[0.15, 0.60]` based on rolling OOS Spearman IC.
    pub fn compute_dynamic_mace_weight(rolling_ic: f64, base_weight: f64) -> f64 {
        let w = if rolling_ic <= 0.0 {
            base_weight + rolling_ic * 0.60
        } else {
            base_weight + rolling_ic * 0.85
        };
        round_dp(w.clamp(0.15, 0.60), 4)
    }

    pub fn compute_mace_and_ic(
        &self,
        synth_bars: &[OhlcvBar],
        usdc_bars: &[OhlcvBar],
        base_mace_weight: f64,
        symbol_key: Option<&str>,
    ) -> (f64, f64, f64) {
        let Some((x_raw, y_target, k)) =
            Self::build_mace_features_and_target(synth_bars, usdc_bars)
        else {
            return (0.0, 0.0, base_mace_weight);
        };
        let cache_key = symbol_key.or_else(|| synth_bars.last().map(|b| b.symbol.as_str()));
        let (mace_series, ic_series) =
            self.fit_predict_mace_series_from_xy(&x_raw, &y_target, k, cache_key);
        let mace = mace_series.last().copied().unwrap_or(0.0);
        let ic = ic_series.last().copied().unwrap_or(0.0);
        let dyn_w = Self::compute_dynamic_mace_weight(ic, base_mace_weight);
        (mace, ic, dyn_w)
    }

    /// Computes BTC macro trend confirmation (`trend_pct`, `r_btc_24`, `r_btc_8`).
    pub fn compute_btc_macro_confirmation(btc_bars: &[OhlcvBar]) -> (f64, f64, f64) {
        if btc_bars.len() < 30 {
            return (0.0, 0.0, 0.0);
        }
        let closes: Vec<f64> = btc_bars.iter().map(|b| b.close.max(1e-8)).collect();
        let n = closes.len();
        let ema_fast = Self::causal_ema(&closes, 18.0);
        let ema_slow = Self::causal_ema(&closes, 54.0);
        let trend_pct = (ema_fast[n - 1] - ema_slow[n - 1]) / ema_slow[n - 1].max(1e-8);
        let r_btc_24 = if n >= 25 {
            (closes[n - 1] / closes[n - 25]).ln()
        } else {
            0.0
        };
        let r_btc_8 = if n >= 9 {
            (closes[n - 1] / closes[n - 9]).ln()
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
        let (btc_trend_pct, r_btc_24, r_btc_8) = btc_macro_confirm;

        // 1. Chop / Funding Basis Harvest check
        if (regime_name == "CHOP" || prob_chop >= chop_threshold)
            && funding_rate_hourly >= basis_funding_floor
        {
            return "BASIS_HARVEST".to_string();
        }

        // 2. Bull Regime Long Entry (Top 3 Alt/BTC Relative Strength Leaders)
        let top_k = 3.min(total_assets.max(1));
        let bull_macro_ok =
            prob_bull >= 0.48 && btc_trend_pct >= -0.010 && r_btc_24 >= -0.030 && r_btc_8 >= -0.020;

        if regime_name == "BULL"
            && bull_macro_ok
            && rs_rank <= top_k
            && blended_conviction >= long_threshold
            && snr_score >= 0.25
            && snr_synth >= 0.05
            && mace_score >= -0.05
            && rs_96 > 0.0
            && r_usdc_8 > 0.0
            && funding_rate_hourly <= 0.0005
        {
            return "LONG".to_string();
        }

        // 3. Bear Regime Short Entry (Bottom 3 Alt/BTC Relative Strength Laggards)
        let bottom_cutoff = total_assets.saturating_sub(2).max(1);
        let bear_macro_ok =
            prob_bear >= 0.50 && btc_trend_pct <= 0.006 && r_btc_24 <= 0.015 && r_btc_8 <= 0.015;

        if regime_name == "BEAR"
            && bear_macro_ok
            && rs_rank >= bottom_cutoff
            && blended_conviction <= short_threshold
            && snr_score <= -0.25
            && snr_synth <= -0.05
            && mace_score <= 0.05
            && rs_96 < -0.005
            && r_usdc_8 < 0.0
            && funding_rate_hourly >= -0.0004
        {
            return "SHORT".to_string();
        }

        "FLAT".to_string()
    }

    /// Evaluates the full cross-sectional 15-altcoin universe (`ALT/USDC:USDC` + `ALT/BTC:SYNTH`).
    pub fn evaluate_universe(&self, save_to_db: bool) -> Result<Vec<SignalEvaluation>> {
        let cfg = self.db.load_config()?;
        let mode = cfg.get_str("execution_mode", "PAPER").to_ascii_uppercase();
        let is_live_mode = mode == "LIVE" || mode == "TESTNET";
        let hmm_window = cfg.get_usize("hmm_lookback_candles", 250);
        let snr_fast = cfg.get_usize("snr_ema_fast_period", 16);
        let snr_slow = cfg.get_usize("snr_ema_slow_period", 64);
        let base_mace_w = cfg.get_f64("mace_conviction_weight", 0.35);
        let long_thresh = cfg.get_f64("conviction_long_threshold", 0.38);
        let short_thresh = cfg.get_f64("conviction_short_threshold", -0.38);
        let chop_thresh = cfg.get_f64("hmm_chop_prob_threshold", 0.55);
        let basis_floor = cfg.get_f64("basis_harvest_min_funding_hourly", 0.00015);

        let btc_bars = self
            .db
            .get_ohlcv_bars("BTC/USDC:USDC", "4h", hmm_window + 10, None)?;
        let regime = self.decode_hmm_regime(&btc_bars, hmm_window, None, None);
        let btc_macro_confirm = Self::compute_btc_macro_confirmation(&btc_bars);

        let alt_pairs = self.db.get_alt_pairs_universe()?;
        let now_ms = btc_bars
            .last()
            .map(|b| b.timestamp_ms)
            .unwrap_or_else(|| self.db.now_ms());

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

            let (snr, snr_synth) =
                Self::compute_continuous_snr(&synth_bars, &usdc_bars, snr_fast, snr_slow);
            let (mace, _ic, dyn_mace_w) = self.compute_mace_and_ic(
                &synth_bars,
                &usdc_bars,
                base_mace_w,
                Some(&pair.synth_symbol),
            );
            let dyn_snr_w = round_dp(1.0 - dyn_mace_w, 4);
            let blended =
                round_dp((dyn_snr_w * snr + dyn_mace_w * mace).clamp(-1.0, 1.0), 4);

            let s_closes: Vec<f64> = synth_bars.iter().map(|b| b.close.max(1e-12)).collect();
            let u_closes: Vec<f64> = usdc_bars.iter().map(|b| b.close.max(1e-12)).collect();
            let ns = s_closes.len();
            let nu = u_closes.len();
            let rs_24 = (s_closes[ns - 1] / s_closes[ns.saturating_sub(24)]).ln();
            let rs_72 = (s_closes[ns - 1] / s_closes[ns.saturating_sub(72)]).ln();
            let rs_96 = (s_closes[ns - 1] / s_closes[ns.saturating_sub(96)]).ln();
            let r_usdc_8 = (u_closes[nu - 1] / u_closes[nu.saturating_sub(8)]).ln();
            let rs_composite = 0.45 * snr_synth + 0.35 * (rs_24 * 6.0).tanh() + 0.20 * (rs_72 * 4.0).tanh();
            let funding = usdc_bars.last().map(|b| b.funding_rate_hourly).unwrap_or(0.0);

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
        raw_metrics.sort_by(|a, b| {
            match (a.is_unsupported_live, b.is_unsupported_live) {
                (false, true) => std::cmp::Ordering::Less,
                (true, false) => std::cmp::Ordering::Greater,
                _ => b
                    .rs_composite
                    .partial_cmp(&a.rs_composite)
                    .unwrap_or(std::cmp::Ordering::Equal),
            }
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
                Self::classify_signal_action(
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
                .map_err(|_| AegisError::Validation(format!("Unsupported timeframe '{timeframe}'")))?,
            _ if tf.ends_with('h') => tf[..tf.len() - 1]
                .parse::<f64>()
                .map_err(|_| AegisError::Validation(format!("Unsupported timeframe '{timeframe}'")))?,
            _ if tf.ends_with('d') => tf[..tf.len() - 1]
                .parse::<f64>()
                .map(|d| d * 24.0)
                .map_err(|_| AegisError::Validation(format!("Unsupported timeframe '{timeframe}'")))?,
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
            let fwd_ret = (usdc_bars[t + 1].close.max(1e-12) / usdc_bars[t].close.max(1e-12)).ln();
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

fn apply_hysteresis(
    current_regime: &str,
    p_bull: f64,
    p_bear: f64,
    p_chop: f64,
) -> &'static str {
    let enter_gate = 0.55;
    let exit_gate = 0.45;
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
) -> Vec<RegressionTreeNode> {
    let n = x.len().min(y.len());
    if n == 0 {
        return Vec::new();
    }
    let mut trees = Vec::with_capacity(n_trees);
    let mut state: u64 = 42;
    let next_u64 = |s: &mut u64| -> u64 {
        *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *s
    };

    for _ in 0..n_trees {
        let mut sample_indices = Vec::with_capacity(n);
        for _ in 0..n {
            sample_indices.push((next_u64(&mut state) as usize) % n);
        }
        let mut feat_subset = Vec::with_capacity(8);
        for k in 0..8 {
            feat_subset.push(((next_u64(&mut state) as usize) + k * 3) % 18);
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

    #[test]
    fn test_sharpe_and_spearman_ic() {
        let rets = vec![0.01, -0.005, 0.012, 0.008, -0.002, 0.015, 0.004, 0.009];
        let sharpe_4h = WalkForwardCalibrator::compute_annualized_sharpe(&rets, "4h").unwrap();
        let sharpe_1h = WalkForwardCalibrator::compute_annualized_sharpe(&rets, "1h").unwrap();
        // 1H annualization factor is 2x the 4H annualization factor (sqrt(8760/1) vs sqrt(8760/4))
        assert!((sharpe_1h / sharpe_4h - 2.0).abs() < 1e-6);

        let preds = vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6];
        let reals = vec![0.12, 0.19, 0.31, 0.42, 0.49, 0.65];
        let ic = SignalEngine::safe_spearman_ic(&preds, &reals);
        assert!(ic > 0.95);
    }
}
