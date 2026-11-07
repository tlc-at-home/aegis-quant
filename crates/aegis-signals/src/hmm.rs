//! Layer 1: 3-State Diagonal Gaussian HMM & Cross-Sectional Alt PC1 (`aegis-signals::hmm`).

use crate::{sample_mean, sample_std, SignalConfig, SignalEngine};
use aegis_core::{round_dp, OhlcvBar, RegimeState};
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

#[derive(Debug, Clone)]
pub(crate) struct HmmCacheState {
    pub bar_ts: Option<i64>,
    pub bar_len: usize,
    pub window: usize,
    pub hmm: Option<DiagonalGaussianHMM>,
    pub alpha: Option<[f64; 3]>,
    pub alpha_prev: Option<[f64; 3]>,
    pub states: Arc<[RegimeState]>,
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

impl Default for HmmCacheState {
    fn default() -> Self {
        Self {
            bar_ts: None,
            bar_len: 0,
            window: 0,
            hmm: None,
            alpha: None,
            alpha_prev: None,
            states: Arc::from([]),
            mapping: (0, 1, 2),
            r_mean: 0.0,
            r_std: 1.0,
            v_mean: 0.0,
            v_std: 1.0,
            a_mean: 0.0,
            a_std: 1.0,
            prev_close: 0.0,
            last_bar_close: 0.0,
            last_alt_pc1_raw: 0.0,
            last_vol_raw: 0.0,
            prev_ema_r_fast: 0.0,
            prev_ema_r_slow: 0.0,
            last_ema_r_fast: 0.0,
            last_ema_r_slow: 0.0,
            last_ema_v: 0.0,
            last_ema_a: 0.0,
        }
    }
}

pub(crate) fn apply_hysteresis_with_gates(
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

impl SignalEngine {
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

    /// Decodes the full causal sequence of `RegimeState` as a shared `Arc<[RegimeState]>` (`NL-11`).
    /// On a 4H cache hit with no intrabar tick shift, returns `Arc::clone(&cache.states)` with O(1) atomic increment.
    pub fn decode_hmm_regime_states_shared(
        &self,
        btc_bars: &[OhlcvBar],
        window: usize,
        alt_pc1_series: Option<&[f64]>,
        live_tick_close: Option<f64>,
    ) -> Arc<[RegimeState]> {
        let n_bars = btc_bars.len();
        if n_bars < 15 {
            return Arc::from(vec![
                RegimeState {
                    regime: "CHOP".to_string(),
                    prob_bull: 0.3333,
                    prob_bear: 0.3333,
                    prob_chop: 0.3334,
                    window_bars: n_bars,
                };
                n_bars.max(1)
            ]);
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
                let eff_tick = live_tick_close
                    .filter(|&p| p > 0.0 && (p - cache.last_bar_close).abs() > 1e-9)
                    .or_else(|| {
                        if (last_bar_close - cache.last_bar_close).abs() > 1e-9 {
                            Some(last_bar_close)
                        } else {
                            None
                        }
                    });
                let Some(tick_px) = eff_tick else {
                    return Arc::clone(&cache.states);
                };

                let mut states = cache.states.to_vec();
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
                return Arc::from(states);
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
        let min_fit_obs = scfg.hmm_min_fit_obs_bars.max(4);

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
        let shared_states: Arc<[RegimeState]> = Arc::from(states);

        {
            let mut cache = self.hmm_cache.lock();
            cache.bar_ts = Some(last_bar_ts);
            cache.bar_len = n_bars;
            cache.window = window;
            cache.alpha = Some(active_alpha);
            cache.alpha_prev = Some(prev_alpha);
            cache.hmm = Some(active_hmm);
            cache.states = Arc::clone(&shared_states);
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

        if let Some(tick_px) =
            live_tick_close.filter(|&p| p > 0.0 && (p - last_bar_close).abs() > 1e-9)
        {
            return self.decode_hmm_regime_states_shared(
                btc_bars,
                window,
                alt_pc1_series,
                Some(tick_px),
            );
        }
        shared_states
    }

    /// Decodes the full causal sequence of `RegimeState` across `btc_bars` (`states.len() == btc_bars.len()`).
    pub fn decode_hmm_regime_states(
        &self,
        btc_bars: &[OhlcvBar],
        window: usize,
        alt_pc1_series: Option<&[f64]>,
        live_tick_close: Option<f64>,
    ) -> Vec<RegimeState> {
        self.decode_hmm_regime_states_shared(btc_bars, window, alt_pc1_series, live_tick_close)
            .to_vec()
    }

    pub fn decode_hmm_regime(
        &self,
        btc_bars: &[OhlcvBar],
        window: usize,
        alt_pc1_series: Option<&[f64]>,
        live_tick_close: Option<f64>,
    ) -> RegimeState {
        let states = self.decode_hmm_regime_states_shared(
            btc_bars,
            window,
            alt_pc1_series,
            live_tick_close,
        );
        states.last().cloned().unwrap_or_default()
    }
}
