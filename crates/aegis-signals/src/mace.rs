//! Layer 3: Out-of-Sample Purged MACE Ensemble (`aegis-signals::mace`).

use crate::{sample_mean, sample_std, SignalConfig, SignalEngine};
use aegis_core::{round_dp, OhlcvBar};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

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

impl SignalEngine {
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
