"""3-State Diagonal-Covariance Gaussian HMM, Purged Out-of-Sample MACE, and Dynamic IC Signal Gates."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.mixture import GaussianMixture

from engine.database import DatabaseManager

logger = logging.getLogger("aegis.signals")

RegimeType = Literal["BULL", "BEAR", "CHOP"]


@dataclass
class RegimeState:
    """Decoded 3-State Gaussian HMM posterior regime state."""

    regime: str
    prob_bull: float
    prob_bear: float
    prob_chop: float
    window_bars: int


class DiagonalGaussianHMM:
    """
    3-State Diagonal-Covariance Gaussian Hidden Markov Model (HMM)
    with Baum-Welch EM transition updates and log-domain forward filtering.
    """

    def __init__(self, n_states: int = 3, n_iter: int = 25, random_state: int = 42) -> None:
        self.n_states = n_states
        self.n_iter = n_iter
        self.random_state = random_state

    @staticmethod
    def _log_gaussian_diag_pdf(
        X: np.ndarray, means: np.ndarray, variances: np.ndarray
    ) -> np.ndarray:
        """Compute log emission probabilities log P(x_t | state = k) for diagonal covariance."""
        safe_var = np.maximum(variances, 1e-8)
        diff = X[:, None, :] - means[None, :, :]
        log_det = np.sum(np.log(2.0 * np.pi * safe_var), axis=-1)
        mahalanobis = np.sum((diff ** 2) / safe_var[None, :, :], axis=-1)
        return -0.5 * (log_det[None, :] + mahalanobis)

    @staticmethod
    def _logsumexp(arr: np.ndarray, axis: int = -1, keepdims: bool = False) -> np.ndarray:
        max_val = np.max(arr, axis=axis, keepdims=True)
        res = max_val + np.log(
            np.maximum(np.sum(np.exp(arr - max_val), axis=axis, keepdims=True), 1e-300)
        )
        if not keepdims:
            res = np.squeeze(res, axis=axis)
        return res

    def fit_decode_posterior(
        self, features: np.ndarray
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Fit the 3-State Diagonal-Covariance Gaussian HMM over `features` (T, D)
        and return (posterior_gamma (T, K), means (K, D), variances (K, D)).
        """
        T, D = features.shape
        K = self.n_states

        gmm = GaussianMixture(
            n_components=K,
            covariance_type="diag",
            max_iter=50,
            random_state=self.random_state,
            reg_covar=1e-5,
        )
        gmm.fit(features)
        means = np.array(gmm.means_, dtype=np.float64)
        variances = np.maximum(np.array(gmm.covariances_, dtype=np.float64), 1e-6)
        pi = np.maximum(np.array(gmm.weights_, dtype=np.float64), 1e-4)
        pi /= np.sum(pi)

        trans = np.full((K, K), 0.05 / max(K - 1, 1), dtype=np.float64)
        np.fill_diagonal(trans, 0.95)

        gamma = np.full((T, K), 1.0 / K, dtype=np.float64)

        for _ in range(self.n_iter):
            log_B = self._log_gaussian_diag_pdf(features, means, variances)
            log_pi = np.log(np.maximum(pi, 1e-12))
            log_A = np.log(np.maximum(trans, 1e-12))

            log_alpha = np.zeros((T, K), dtype=np.float64)
            log_alpha[0] = log_pi + log_B[0]
            log_alpha[0] -= self._logsumexp(log_alpha[0], axis=0)

            for t in range(1, T):
                trans_scores = log_alpha[t - 1][:, None] + log_A
                log_alpha[t] = log_B[t] + self._logsumexp(trans_scores, axis=0)
                log_alpha[t] -= self._logsumexp(log_alpha[t], axis=0)

            log_beta = np.zeros((T, K), dtype=np.float64)
            for t in range(T - 2, -1, -1):
                back_scores = log_A + log_B[t + 1][None, :] + log_beta[t + 1][None, :]
                log_beta[t] = self._logsumexp(back_scores, axis=1)
                log_beta[t] -= self._logsumexp(log_beta[t], axis=0)

            log_gamma = log_alpha + log_beta
            log_gamma -= self._logsumexp(log_gamma, axis=1, keepdims=True)
            gamma = np.exp(log_gamma)

            state_weights = np.maximum(np.sum(gamma, axis=0), 1e-8)
            pi = gamma[0] / np.maximum(np.sum(gamma[0]), 1e-8)
            means = (gamma.T @ features) / state_weights[:, None]
            diff = features[:, None, :] - means[None, :, :]
            variances = np.maximum(
                np.sum(gamma[:, :, None] * (diff ** 2), axis=0) / state_weights[:, None],
                1e-6,
            )
            if T > 1:
                log_xi = (
                    log_alpha[:-1, :, None]
                    + log_A[None, :, :]
                    + log_B[1:, None, :]
                    + log_beta[1:, None, :]
                )
                log_xi_norm = self._logsumexp(
                    log_xi.reshape(T - 1, K * K), axis=1, keepdims=True
                )[:, :, None]
                xi_sum = np.sum(np.exp(log_xi - log_xi_norm), axis=0)
                sticky_prior = np.eye(K, dtype=np.float64) * 15.0 + 0.5
                trans = xi_sum + sticky_prior
                trans /= np.maximum(np.sum(trans, axis=1, keepdims=True), 1e-8)

        return gamma, means, variances


class SignalEngine:
    """
    Evaluates the cached warm-start 3-State Gaussian HMM regime gate with hysteresis,
    purged out-of-sample MACE Random Forest + Ridge non-linear ensemble, and dynamic
    Spearman Rank IC-weighted MACE/SNR blending.
    """

    def __init__(self, db: DatabaseManager | None = None) -> None:
        self.db = db
        self._hmm_cache: dict[str, Any] = {}

    @staticmethod
    def _sanitize_positive_series(s: pd.Series, fallback: float = 1.0) -> pd.Series:
        """Causally forward-fill non-finite or non-positive price values."""
        arr = pd.to_numeric(s, errors="coerce").to_numpy(dtype=np.float64, copy=True)
        invalid = ~np.isfinite(arr) | (arr <= 0.0)
        if np.any(invalid):
            cleaned = pd.Series(np.where(invalid, np.nan, arr), index=s.index, dtype=np.float64)
            cleaned = cleaned.ffill().bfill().fillna(float(fallback))
            return cleaned
        return pd.Series(arr, index=s.index, dtype=np.float64)

    @staticmethod
    def _sanitize_finite_series(s: pd.Series, fallback: float = 0.0) -> pd.Series:
        """Causally forward-fill non-finite (NaN / +-inf) series values."""
        arr = pd.to_numeric(s, errors="coerce").to_numpy(dtype=np.float64, copy=True)
        invalid = ~np.isfinite(arr)
        if np.any(invalid):
            cleaned = pd.Series(np.where(invalid, np.nan, arr), index=s.index, dtype=np.float64)
            cleaned = cleaned.ffill().bfill().fillna(float(fallback))
            return cleaned
        return pd.Series(arr, index=s.index, dtype=np.float64)

    @classmethod
    def calculate_atr(cls, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """Compute causal Average True Range (ATR) series over `period` bars."""
        if df.empty:
            return pd.Series(dtype=np.float64)
        close = cls._sanitize_positive_series(df["close"], fallback=1.0)
        high = (
            cls._sanitize_positive_series(df["high"], fallback=1.0)
            if "high" in df.columns
            else close * 1.005
        )
        low = (
            cls._sanitize_positive_series(df["low"], fallback=1.0)
            if "low" in df.columns
            else close * 0.995
        )
        high = np.maximum(high, np.maximum(close, low))
        low = np.minimum(low, np.minimum(close, high))
        prev_close = close.shift(1).fillna(close)
        tr = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        return tr.rolling(window=max(1, int(period)), min_periods=1).mean().bfill().fillna(0.0)

    def compute_alt_pc1_series(
        self,
        df_btc: pd.DataFrame,
        alt_dfs: list[pd.DataFrame] | dict[str, pd.DataFrame] | None = None,
        limit: int = 540,
    ) -> pd.Series:
        """
        Compute the First Principal Component (alt_pc1_ret) of universe altcoin log returns
        aligned to `df_btc.index`.
        """
        if df_btc.empty:
            return pd.Series(dtype=np.float64)

        ts_col = "timestamp_utc" if "timestamp_utc" in df_btc.columns else (
            "timestamp_ms" if "timestamp_ms" in df_btc.columns else None
        )
        if alt_dfs is None and self.db is not None:
            alt_pairs = self.db.get_alt_pairs_universe()
            alt_dfs = [
                self.db.get_ohlcv_df(str(p["exec_symbol"]), "4h", limit=limit + 20)
                for p in alt_pairs
            ]
        elif isinstance(alt_dfs, dict):
            alt_dfs = list(alt_dfs.values())

        c_btc = self._sanitize_positive_series(df_btc["close"], fallback=1.0)
        fallback_ret = np.nan_to_num(
            np.log(c_btc / c_btc.shift(1)).fillna(0.0).to_numpy(dtype=np.float64),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )

        if not alt_dfs:
            return pd.Series(fallback_ret, index=df_btc.index, dtype=np.float64)

        ret_cols: list[np.ndarray] = []
        btc_len = len(df_btc)
        btc_ts = df_btc[ts_col].to_numpy() if ts_col is not None else None

        for adf in alt_dfs:
            if adf is None or adf.empty or "close" not in adf.columns:
                continue
            a_ts_col = "timestamp_utc" if "timestamp_utc" in adf.columns else (
                "timestamp_ms" if "timestamp_ms" in adf.columns else None
            )
            clean_adf_close = self._sanitize_positive_series(adf["close"], fallback=1.0)
            if btc_ts is not None and a_ts_col is not None:
                s_close = pd.Series(
                    clean_adf_close.to_numpy(dtype=np.float64),
                    index=adf[a_ts_col].to_numpy(),
                )
                s_close = s_close[~s_close.index.duplicated(keep="last")]
                aligned_close = s_close.reindex(btc_ts).ffill().bfill().fillna(1.0)
                c_arr = aligned_close.to_numpy(dtype=np.float64)
            else:
                c_raw = clean_adf_close.to_numpy(dtype=np.float64)
                if len(c_raw) >= btc_len:
                    c_arr = c_raw[-btc_len:]
                else:
                    c_arr = np.pad(c_raw, (btc_len - len(c_raw), 0), mode="edge")

            c_safe = np.maximum(np.nan_to_num(c_arr, nan=1.0, posinf=1.0, neginf=1.0), 1e-12)
            r = np.diff(np.log(c_safe), prepend=np.log(c_safe[0]))
            r = np.nan_to_num(r, nan=0.0, posinf=0.0, neginf=0.0)
            ret_cols.append(r)

        if not ret_cols:
            return pd.Series(fallback_ret, index=df_btc.index, dtype=np.float64)

        R = np.column_stack(ret_cols)
        try:
            _, _, Vt = np.linalg.svd(R, full_matrices=False)
            weights = np.abs(Vt[0])
            w_sum = float(np.sum(weights))
            if w_sum > 1e-12 and np.isfinite(w_sum):
                weights /= w_sum
            else:
                weights = np.full(R.shape[1], 1.0 / R.shape[1], dtype=np.float64)
            pc1 = np.nan_to_num(R @ weights, nan=0.0, posinf=0.0, neginf=0.0)
        except Exception:
            pc1 = np.nan_to_num(np.mean(R, axis=1), nan=0.0, posinf=0.0, neginf=0.0)

        return pd.Series(pc1, index=df_btc.index, dtype=np.float64)

    @staticmethod
    def _build_effective_hmm_obs(obs: np.ndarray) -> np.ndarray:
        """Transform raw 3-feature obs [log_ret, vol_norm, pc1] into causal smoothed regime features."""
        D = obs.shape[1]
        s_ret_fast = pd.Series(obs[:, 0]).ewm(span=36, adjust=False).mean().to_numpy(dtype=np.float64)
        s_ret_slow = pd.Series(obs[:, 0]).ewm(span=96, adjust=False).mean().to_numpy(dtype=np.float64)
        s_ret = 0.45 * s_ret_fast + 0.55 * s_ret_slow

        s_vol = pd.Series(obs[:, 1]).ewm(span=14, adjust=False).mean().to_numpy(dtype=np.float64)

        has_pc1 = D >= 3 and float(np.std(obs[:, 2])) > 1e-8
        if has_pc1:
            s_pc1_fast = pd.Series(obs[:, 2]).ewm(span=36, adjust=False).mean().to_numpy(dtype=np.float64)
            s_pc1_slow = pd.Series(obs[:, 2]).ewm(span=96, adjust=False).mean().to_numpy(dtype=np.float64)
            s_pc1 = 0.45 * s_pc1_fast + 0.55 * s_pc1_slow
            joint_drift = 0.45 * s_ret + 0.55 * s_pc1
        else:
            s_pc1 = s_ret.copy()
            joint_drift = s_ret.copy()

        return np.column_stack([joint_drift, s_vol, s_pc1])

    def _fit_baum_welch_log(
        self,
        obs: np.ndarray,
        means: np.ndarray,
        vars_diag: np.ndarray,
        trans: np.ndarray,
        n_iter: int = 25,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Run log-domain Baum-Welch EM over 3-feature observation matrix `obs` (T, 3)
        and return updated (means, vars_diag, trans, gamma).
        """
        T, D = obs.shape
        K = 3

        eff_obs = self._build_effective_hmm_obs(obs)
        joint_drift = eff_obs[:, 0]
        s_vol = eff_obs[:, 1]
        s_pc1 = eff_obs[:, 2]

        sigma_d = max(float(np.std(joint_drift)), 3.0e-4)
        sigma_p = max(float(np.std(s_pc1)), 3.0e-4)
        mean_v = float(np.mean(s_vol))
        var_v = max(float(np.var(s_vol)) * 4.0, 1e-6)

        # Prior directional state prototypes: 0=BEAR (<0), 1=CHOP (~0), 2=BULL (>0)
        prior_means = np.array(
            [
                [-0.85 * sigma_d, mean_v, -0.85 * sigma_p],
                [0.0, mean_v, 0.0],
                [+0.85 * sigma_d, mean_v, +0.85 * sigma_p],
            ],
            dtype=np.float64,
        )
        prior_vars = np.array(
            [
                [1.25 * (sigma_d ** 2), var_v, 1.25 * (sigma_p ** 2)],
                [0.70 * (sigma_d ** 2), var_v, 0.70 * (sigma_p ** 2)],
                [1.25 * (sigma_d ** 2), var_v, 1.25 * (sigma_p ** 2)],
            ],
            dtype=np.float64,
        )
        prior_vars = np.maximum(prior_vars, 1e-8)

        cur_means = prior_means.copy()
        cur_vars = prior_vars.copy()
        if (
            isinstance(means, np.ndarray)
            and means.shape == (K, D)
            and np.all(np.isfinite(means))
            and float(np.max(np.abs(means[:, 0]))) < 0.05
        ):
            warm_order = np.argsort(means[:, 0])
            m_sorted = means[warm_order]
            cur_means = 0.50 * m_sorted + 0.50 * prior_means
            cur_means[:, 1] = mean_v
            if (
                isinstance(vars_diag, np.ndarray)
                and vars_diag.shape == (K, D)
                and np.all(np.isfinite(vars_diag))
                and float(np.max(vars_diag)) < 1.0
            ):
                v_sorted = np.maximum(vars_diag[warm_order], 1e-8)
                cur_vars = np.maximum(0.50 * v_sorted + 0.50 * prior_vars, 1e-8)
                cur_vars[:, 1] = var_v

        cur_trans = np.array(trans, dtype=np.float64, copy=True)
        if cur_trans.shape != (K, K) or not np.all(np.isfinite(cur_trans)):
            cur_trans = np.array([[0.93, 0.05, 0.02], [0.04, 0.92, 0.04], [0.02, 0.05, 0.93]])
        cur_trans /= np.maximum(np.sum(cur_trans, axis=1, keepdims=True), 1e-12)

        pi = np.full(K, 1.0 / K, dtype=np.float64)
        gamma = np.full((T, K), 1.0 / K, dtype=np.float64)
        alpha = np.zeros((T, K), dtype=np.float64)
        beta = np.ones((T, K), dtype=np.float64)

        sticky_prior = np.array(
            [
                [48.0, 1.2, 0.05],
                [1.0, 40.0, 1.0],
                [0.05, 1.2, 48.0],
            ],
            dtype=np.float64,
        )
        prior_strength = 0.55 * T / K

        for _ in range(max(1, int(n_iter))):
            log_B = DiagonalGaussianHMM._log_gaussian_diag_pdf(eff_obs, cur_means, cur_vars)
            B = np.maximum(
                np.exp(log_B - np.max(log_B, axis=1, keepdims=True)),
                1e-300,
            )

            # Scaled forward pass (mathematically equivalent to log-domain forward pass)
            a0 = pi * B[0]
            alpha[0] = a0 / max(float(np.sum(a0)), 1e-300)
            for t in range(1, T):
                at = (alpha[t - 1] @ cur_trans) * B[t]
                alpha[t] = at / max(float(np.sum(at)), 1e-300)

            # Scaled backward pass
            beta[T - 1] = 1.0 / K
            for t in range(T - 2, -1, -1):
                bt = cur_trans @ (B[t + 1] * beta[t + 1])
                beta[t] = bt / max(float(np.sum(bt)), 1e-300)

            gamma = alpha * beta
            gamma /= np.maximum(np.sum(gamma, axis=1, keepdims=True), 1e-300)

            # MAP M-step anchored by directional Bear/Chop/Bull prior
            state_weights = np.maximum(np.sum(gamma, axis=0), 1e-8)
            pi = (gamma[0] + 0.1) / np.sum(gamma[0] + 0.1)
            emp_means = (gamma.T @ eff_obs) / state_weights[:, None]
            cur_means = (
                state_weights[:, None] * emp_means + prior_strength * prior_means
            ) / (state_weights[:, None] + prior_strength)

            cur_means[0, 0] = min(cur_means[0, 0], -0.45 * sigma_d)
            cur_means[1, 0] = float(np.clip(cur_means[1, 0], -0.15 * sigma_d, 0.15 * sigma_d))
            cur_means[2, 0] = max(cur_means[2, 0], +0.45 * sigma_d)
            cur_means[:, 1] = mean_v

            diff = eff_obs[:, None, :] - cur_means[None, :, :]
            emp_vars = np.sum(gamma[:, :, None] * (diff ** 2), axis=0) / state_weights[:, None]
            cur_vars = np.maximum(0.35 * emp_vars + 0.65 * prior_vars, 1e-8)
            cur_vars[:, 1] = var_v

            if T > 1:
                xi_unnorm = (
                    alpha[:-1, :, None]
                    * cur_trans[None, :, :]
                    * (B[1:] * beta[1:])[:, None, :]
                )
                xi_norm = np.maximum(np.sum(xi_unnorm, axis=(1, 2), keepdims=True), 1e-300)
                xi_sum = np.sum(xi_unnorm / xi_norm, axis=0)
                cur_trans = xi_sum + sticky_prior
                cur_trans /= np.maximum(np.sum(cur_trans, axis=1, keepdims=True), 1e-8)

        order = np.argsort(cur_means[:, 0])
        if T >= 2:
            alpha_prev = alpha[-2, order].copy()
            alpha_prev /= max(float(np.sum(alpha_prev)), 1e-12)
        else:
            alpha_prev = np.full(K, 1.0 / K, dtype=np.float64)
        self._hmm_cache["alpha_prev"] = alpha_prev
        self._hmm_cache["last_eff_obs"] = eff_obs[-1].copy()

        return cur_means, np.maximum(cur_vars, 1e-7), cur_trans, gamma

    def decode_hmm_regime_states(
        self,
        df_btc: pd.DataFrame,
        alt_pc1_series: pd.Series | None = None,
        cfg: dict[str, Any] | None = None,
    ) -> tuple[RegimeType, float, float, float]:
        """
        Decode 3-state Gaussian HMM regime using 4H bar caching, warm-started Baum-Welch EM,
        single-step forward filtering on 15s ticks, 3-feature observation vector
        [log_ret, vol_norm, alt_pc1_ret], and 0.55 / 0.45 hysteresis confirmation gates.
        """
        cfg = cfg or (self.db.load_config() if self.db is not None else {})
        lookback = int(cfg.get("hmm_lookback_bars", cfg.get("hmm_window_bars", 540)))
        if len(df_btc) < 60:
            return "CHOP", 0.33, 0.33, 0.34

        sub = df_btc.iloc[-lookback:].copy()
        ts_col = "timestamp_utc" if "timestamp_utc" in sub.columns else (
            "timestamp_ms" if "timestamp_ms" in sub.columns else None
        )
        raw_ts = sub[ts_col].iloc[-1] if ts_col is not None else len(df_btc)
        latest_bar_ts = int(raw_ts) if pd.notna(raw_ts) and np.isfinite(float(raw_ts)) else int(len(df_btc))

        close = self._sanitize_positive_series(sub["close"], fallback=1.0)
        log_ret_raw = np.log(close / close.shift(1)).fillna(0.0).to_numpy(dtype=np.float64)
        log_ret = np.clip(np.nan_to_num(log_ret_raw, nan=0.0, posinf=0.0, neginf=0.0), -0.50, 0.50)

        atr = self.calculate_atr(sub, period=int(cfg.get("atr_period", 14))).to_numpy(dtype=np.float64)
        vol_norm_raw = np.divide(atr, np.maximum(close.to_numpy(dtype=np.float64), 1e-8))
        vol_norm = np.clip(np.nan_to_num(vol_norm_raw, nan=0.01, posinf=0.10, neginf=0.001), 1e-6, 0.50)

        if alt_pc1_series is None:
            alt_pc1_series = self.compute_alt_pc1_series(sub, limit=lookback)
        pc1_raw = alt_pc1_series.reindex(sub.index).fillna(0.0).to_numpy(dtype=np.float64)
        pc1_aligned = np.clip(np.nan_to_num(pc1_raw, nan=0.0, posinf=0.0, neginf=0.0), -0.50, 0.50)

        obs = np.column_stack([log_ret, vol_norm, pc1_aligned])

        # Refit Baum-Welch EM only on a new 4H bar close
        if self._hmm_cache.get("ts") != latest_bar_ts:
            is_warm = "means" in self._hmm_cache
            if is_warm:
                means = self._hmm_cache["means"].copy()
                vars_diag = self._hmm_cache["vars_diag"].copy()
                trans = self._hmm_cache["trans"].copy()
            else:
                q33, q67 = np.percentile(log_ret, [33, 67])
                m_low = obs[log_ret <= q33]
                m_mid = obs[(log_ret > q33) & (log_ret < q67)]
                m_high = obs[log_ret >= q67]
                base_mean = np.mean(obs, axis=0)
                means = np.array(
                    [
                        np.mean(m_low, axis=0) if len(m_low) > 0 else base_mean - 1e-4,
                        np.mean(m_mid, axis=0) if len(m_mid) > 0 else base_mean,
                        np.mean(m_high, axis=0) if len(m_high) > 0 else base_mean + 1e-4,
                    ],
                    dtype=float,
                )
                vars_diag = np.maximum(np.array([np.var(obs, axis=0) for _ in range(3)]), 1e-7)
                trans = np.array([[0.90, 0.08, 0.02], [0.07, 0.86, 0.07], [0.02, 0.08, 0.90]])

            cfg_iters = int(cfg.get("hmm_em_iterations", 25))
            em_iters = min(cfg_iters, 3) if is_warm else cfg_iters
            means, vars_diag, trans, gamma = self._fit_baum_welch_log(
                obs, means, vars_diag, trans, n_iter=em_iters
            )
            order = np.argsort(means[:, 0])
            self._hmm_cache.update(
                {
                    "ts": latest_bar_ts,
                    "means": means[order],
                    "vars_diag": vars_diag[order],
                    "trans": trans[np.ix_(order, order)],
                    "posterior": gamma[-1, order],
                    "last_obs": obs[-1].copy(),
                }
            )
        else:
            # 15s intermediate tick: run a single forward-filtering step alpha_T propto (alpha_{T-1} A) * b(O_T)
            last_cached_obs = self._hmm_cache.get("last_obs")
            if (
                last_cached_obs is not None
                and np.all(np.isfinite(obs[-1]))
                and np.all(np.isfinite(last_cached_obs))
                and not np.allclose(obs[-1], last_cached_obs, atol=1e-12)
                and "alpha_prev" in self._hmm_cache
            ):
                alpha_prev = np.asarray(self._hmm_cache["alpha_prev"], dtype=np.float64)
                A = np.asarray(self._hmm_cache["trans"], dtype=np.float64)
                means_c = np.asarray(self._hmm_cache["means"], dtype=np.float64)
                vars_c = np.asarray(self._hmm_cache["vars_diag"], dtype=np.float64)
                prev_eff = np.asarray(
                    self._hmm_cache.get("last_eff_obs", means_c[1]), dtype=np.float64
                )
                delta_ret = float(obs[-1, 0] - last_cached_obs[0])
                eff_T = prev_eff.copy()
                eff_T[0] += 0.05 * delta_ret
                eff_T[2] += 0.05 * delta_ret
                log_b_T = DiagonalGaussianHMM._log_gaussian_diag_pdf(
                    eff_T[None, :], means_c, vars_c
                )[0]
                b_T = np.exp(log_b_T - np.max(log_b_T))
                alpha_T = (alpha_prev @ A) * b_T
                norm_sum = float(np.sum(alpha_T))
                if norm_sum > 1e-12 and np.isfinite(norm_sum):
                    self._hmm_cache["posterior"] = alpha_T / norm_sum
                self._hmm_cache["last_obs"] = obs[-1].copy()

        bear_prob, chop_prob, bull_prob = map(float, self._hmm_cache["posterior"])
        total_p = bear_prob + chop_prob + bull_prob
        if total_p > 0.0 and np.isfinite(total_p):
            bear_prob /= total_p
            chop_prob /= total_p
            bull_prob /= total_p
        else:
            bear_prob, chop_prob, bull_prob = 0.33, 0.34, 0.33

        prev_regime: RegimeType = self._hmm_cache.get("regime", "CHOP")

        enter_gate = float(cfg.get("hmm_enter_prob_gate", 0.55))
        exit_gate = float(cfg.get("hmm_exit_prob_gate", 0.45))

        if prev_regime == "BULL":
            regime: RegimeType = (
                "BULL"
                if bull_prob >= exit_gate
                else ("BEAR" if bear_prob >= enter_gate else "CHOP")
            )
        elif prev_regime == "BEAR":
            regime = (
                "BEAR"
                if bear_prob >= exit_gate
                else ("BULL" if bull_prob >= enter_gate else "CHOP")
            )
        else:
            regime = (
                "BEAR"
                if bear_prob >= enter_gate
                else ("BULL" if bull_prob >= enter_gate else "CHOP")
            )

        self._hmm_cache["regime"] = regime
        return regime, bull_prob, bear_prob, chop_prob

    def decode_hmm_regime(
        self,
        config: dict[str, Any] | None = None,
        alt_pc1_series: pd.Series | None = None,
    ) -> RegimeState:
        """Decode Bull, Bear, and Chop posterior probabilities over 540 4H bars."""
        cfg = config or (self.db.load_config() if self.db is not None else {})
        benchmark = str(cfg.get("benchmark_symbol", "BTC/USDC:USDC"))
        timeframe = str(cfg.get("hmm_timeframe", "4h"))
        window_bars = int(cfg.get("hmm_lookback_bars", cfg.get("hmm_window_bars", 540)))

        if self.db is None:
            return RegimeState(
                regime="CHOP",
                prob_bull=0.33,
                prob_bear=0.33,
                prob_chop=0.34,
                window_bars=0,
            )

        df_btc = self.db.get_ohlcv_df(benchmark, timeframe, limit=window_bars + 20)
        if len(df_btc) < 60:
            return RegimeState(
                regime="CHOP",
                prob_bull=0.33,
                prob_bear=0.33,
                prob_chop=0.34,
                window_bars=len(df_btc),
            )

        sub = df_btc.iloc[-window_bars:].copy()
        if alt_pc1_series is None:
            alt_pc1_series = self.compute_alt_pc1_series(sub, limit=window_bars)

        regime, bull_prob, bear_prob, chop_prob = self.decode_hmm_regime_states(
            sub, alt_pc1_series, cfg
        )
        return RegimeState(
            regime=regime,
            prob_bull=round(float(bull_prob), 4),
            prob_bear=round(float(bear_prob), 4),
            prob_chop=round(float(chop_prob), 4),
            window_bars=len(sub),
        )

    @classmethod
    def _compute_causal_snr_series(
        cls,
        closes: np.ndarray,
        fast_span: int = 16,
        slow_span: int = 64,
        tanh_scale: float = 1.25,
    ) -> np.ndarray:
        """Compute causal bar-by-bar continuous SNR series over a close price array."""
        if len(closes) < 5:
            return np.zeros(len(closes), dtype=np.float64)
        clean_closes = cls._sanitize_positive_series(pd.Series(closes), fallback=1.0).to_numpy(dtype=np.float64)
        s_arr = np.log(np.maximum(clean_closes, 1e-12))
        s = pd.Series(s_arr)
        rets = s.diff().fillna(0.0)
        vol = rets.rolling(window=max(4, slow_span), min_periods=2).std().bfill().fillna(1e-4)
        vol_reg = 0.25 * np.maximum(vol.to_numpy(dtype=np.float64), 1e-4) + 0.75 * 0.012

        ema_fast = s.ewm(span=max(2, fast_span), adjust=False).mean().to_numpy(dtype=np.float64)
        ema_slow = s.ewm(span=max(4, slow_span), adjust=False).mean().to_numpy(dtype=np.float64)
        trend_diff = 0.55 * (ema_fast - ema_slow) + 0.45 * (s_arr - ema_slow)
        raw_signal = trend_diff / (vol_reg * np.sqrt(max(1.0, fast_span * 0.5)))
        return np.nan_to_num(np.tanh(raw_signal * float(tanh_scale)), nan=0.0, posinf=0.0, neginf=0.0)

    @classmethod
    def compute_continuous_snr(
        cls,
        exec_df: pd.DataFrame,
        synth_df: pd.DataFrame | None = None,
        fast_span: int = 16,
        slow_span: int = 64,
        snr_synth_weight: float = 0.60,
        tanh_scale: float = 1.25,
    ) -> float:
        """
        Compute continuous multi-horizon Signal-to-Noise Ratio (SNR) forecast in [-1.0, 1.0]
        combining ALT/BTC:SYNTH relative strength (snr_synth_weight) and ALT/USDC execution trend.
        """
        def _series_snr(closes: np.ndarray) -> float:
            if len(closes) < 20:
                return 0.0
            arr = cls._compute_causal_snr_series(
                closes, fast_span=fast_span, slow_span=slow_span, tanh_scale=tanh_scale
            )
            return float(arr[-1])

        snr_exec = (
            _series_snr(exec_df["close"].to_numpy(dtype=np.float64))
            if (exec_df is not None and not exec_df.empty)
            else 0.0
        )
        if synth_df is None or synth_df.empty:
            return float(np.clip(snr_exec, -1.0, 1.0))

        snr_synth = _series_snr(synth_df["close"].to_numpy(dtype=np.float64))
        w_synth = float(np.clip(snr_synth_weight, 0.0, 1.0))
        return float(np.clip(w_synth * snr_synth + (1.0 - w_synth) * snr_exec, -1.0, 1.0))

    @classmethod
    def _build_mace_features_and_target(
        cls,
        exec_df: pd.DataFrame,
        synth_df: pd.DataFrame | None = None,
        forward_horizon_bars: int = 4,
    ) -> tuple[pd.DataFrame, pd.Series]:
        """
        Construct causal backward-looking MACE feature DataFrame and k-bar forward return target.
        """
        k = max(1, int(forward_horizon_bars))

        if synth_df is not None and not synth_df.empty and "timestamp_ms" in exec_df.columns and "timestamp_ms" in synth_df.columns:
            exec_cols = [c for c in ("timestamp_ms", "close", "high", "low", "volume") if c in exec_df.columns]
            exec_sorted = (
                exec_df[exec_cols]
                .sort_values("timestamp_ms", ascending=True)
                .reset_index(drop=True)
            )
            synth_sorted = (
                synth_df[["timestamp_ms", "close"]]
                .sort_values("timestamp_ms", ascending=True)
                .reset_index(drop=True)
            )
            merged = (
                pd.merge_asof(
                    exec_sorted,
                    synth_sorted,
                    on="timestamp_ms",
                    suffixes=("_usdc", "_synth"),
                    direction="backward",
                )
                .dropna(subset=["close_usdc", "close_synth"])
                .reset_index(drop=True)
            )
            clean_usdc = cls._sanitize_positive_series(merged["close_usdc"], fallback=1.0).to_numpy(dtype=np.float64)
            clean_synth = cls._sanitize_positive_series(merged["close_synth"], fallback=1.0).to_numpy(dtype=np.float64)
            c_usdc = np.log(np.maximum(clean_usdc, 1e-12))
            c_synth = np.log(np.maximum(clean_synth, 1e-12))
            vol_arr = (
                cls._sanitize_finite_series(merged["volume"], fallback=1.0).to_numpy(dtype=np.float64)
                if "volume" in merged.columns
                else np.ones(len(merged), dtype=np.float64)
            )
            idx = merged.index
        else:
            base_df = exec_df.reset_index(drop=True)
            clean_usdc = cls._sanitize_positive_series(base_df["close"], fallback=1.0).to_numpy(dtype=np.float64)
            c_usdc = np.log(np.maximum(clean_usdc, 1e-12))
            if synth_df is not None and not synth_df.empty and "close" in synth_df.columns and len(synth_df) >= len(base_df):
                clean_synth = cls._sanitize_positive_series(
                    synth_df["close"].iloc[-len(base_df) :], fallback=1.0
                ).to_numpy(dtype=np.float64)
                c_synth = np.log(np.maximum(clean_synth, 1e-12))
            else:
                c_synth = c_usdc.copy()
            vol_arr = (
                cls._sanitize_finite_series(base_df["volume"], fallback=1.0).to_numpy(dtype=np.float64)
                if "volume" in base_df.columns
                else np.ones(len(base_df), dtype=np.float64)
            )
            idx = base_df.index

        s_synth = pd.Series(c_synth, index=idx)
        s_usdc = pd.Series(c_usdc, index=idx)
        s_vol = pd.Series(np.maximum(vol_arr, 0.0), index=idx)

        vol_roll_mean = s_vol.rolling(24, min_periods=4).mean().bfill().fillna(0.0)
        vol_roll_std = np.maximum(s_vol.rolling(24, min_periods=4).std().bfill().fillna(1.0), 1e-6)
        vol_z = np.nan_to_num(((s_vol - vol_roll_mean) / vol_roll_std).to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0)

        features = pd.DataFrame(
            {
                "r_synth_4": np.nan_to_num(s_synth.diff(4).fillna(0.0).to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0),
                "r_synth_16": np.nan_to_num(s_synth.diff(16).fillna(0.0).to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0),
                "r_synth_32": np.nan_to_num(s_synth.diff(32).fillna(0.0).to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0),
                "r_usdc_8": np.nan_to_num(s_usdc.diff(8).fillna(0.0).to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0),
                "r_usdc_24": np.nan_to_num(s_usdc.diff(24).fillna(0.0).to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0),
                "vol_z": vol_z,
            },
            index=idx,
        )

        # k-bar forward log return target: close.shift(-k) - close
        target = s_synth.shift(-k) - s_synth
        return features, target

    @classmethod
    def _compute_mace_and_ic(
        cls,
        exec_df: pd.DataFrame,
        synth_df: pd.DataFrame | None = None,
        cfg: dict[str, Any] | None = None,
        n_estimators: int | None = None,
        max_depth: int | None = None,
        min_samples_leaf: int | None = None,
        ridge_alpha: float | None = None,
    ) -> tuple[float, float, float]:
        """
        Internal helper returning (mace_score, ic_mace, ic_snr) using purged feature-target
        slicing `iloc[warmup : -k]` and out-of-sample inference on `iloc[[-1]]`.
        """
        config = cfg or {}
        k = max(1, int(config.get("mace_forward_horizon_bars", 4)))
        warmup = 32
        tanh_scale = float(config.get("mace_tanh_scale", 1.25))
        rf_trees = int(n_estimators if n_estimators is not None else config.get("mace_rf_n_estimators", 60))
        rf_depth = int(max_depth if max_depth is not None else config.get("mace_rf_max_depth", 4))
        rf_leaf = int(
            min_samples_leaf if min_samples_leaf is not None else config.get("mace_rf_min_samples_leaf", 10)
        )
        alpha = float(ridge_alpha if ridge_alpha is not None else config.get("mace_ridge_alpha", 1.0))
        fast_span = int(config.get("snr_fast_span", 16))
        slow_span = int(config.get("snr_slow_span", 64))

        if exec_df is None or len(exec_df) < warmup + k + 25:
            return 0.0, 0.05, 0.05
        if synth_df is not None and not synth_df.empty and len(synth_df) < 40:
            return 0.0, 0.05, 0.05

        features, target = cls._build_mace_features_and_target(
            exec_df=exec_df, synth_df=synth_df, forward_horizon_bars=k
        )
        if len(features) < warmup + k + 25:
            return 0.0, 0.05, 0.05

        # 1. Purged Feature-Target Slicing: strictly up to -k (never overlaps [-k:])
        X_train_df = features.iloc[warmup:-k]
        y_train_s = target.iloc[warmup:-k]
        X_latest_df = features.iloc[[-1]]

        # Fit normalization strictly on purged X_train_df and y_train_s (zero look-ahead leakage)
        X_train_raw = np.nan_to_num(
            X_train_df.to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0
        )
        X_mean = np.mean(X_train_raw, axis=0)
        X_center = np.zeros_like(X_mean)
        X_center[-1] = X_mean[-1]
        X_std = np.maximum(np.std(X_train_raw, axis=0), 1e-6)

        X_train_norm = np.clip((X_train_raw - X_center) / X_std, -3.0, 3.0)
        X_train = np.hstack([X_train_norm, np.tanh(X_train_norm), X_train_norm ** 2 - 1.0])

        y_raw = y_train_s.to_numpy(dtype=np.float64)
        valid_mask = np.isfinite(y_raw)
        if np.sum(valid_mask) < 20:
            return 0.0, 0.05, 0.05

        X_train = X_train[valid_mask]
        y_raw = y_raw[valid_mask]
        ret_std = max(float(np.std(y_raw)), 1e-6)
        y_train = np.clip(y_raw / ret_std, -3.0, 3.0)

        # 2. Out-of-Sample Inference: predict strictly on X_latest = features.iloc[[-1]]
        X_latest_raw = np.nan_to_num(
            X_latest_df.to_numpy(dtype=np.float64), nan=0.0, posinf=0.0, neginf=0.0
        )
        X_latest_norm = np.clip((X_latest_raw - X_center) / X_std, -3.0, 3.0)
        X_latest = np.hstack([X_latest_norm, np.tanh(X_latest_norm), X_latest_norm ** 2 - 1.0])

        rf = RandomForestRegressor(
            n_estimators=rf_trees,
            max_depth=rf_depth,
            min_samples_leaf=rf_leaf,
            random_state=42,
            n_jobs=1,
        )
        ridge = Ridge(alpha=alpha, random_state=42)

        rf.fit(X_train, y_train)
        ridge.fit(X_train, y_train)

        pred_rf = float(np.asarray(rf.predict(X_latest)).reshape(-1)[0])
        pred_ridge = float(np.asarray(ridge.predict(X_latest)).reshape(-1)[0])
        mace_raw = 0.55 * pred_rf + 0.45 * pred_ridge
        mace_score = float(np.tanh(mace_raw * tanh_scale))

        # Evaluate dynamic Spearman Rank IC on purged validation tail of training window
        eval_tail = min(len(X_train), 80)
        mace_train_preds = 0.55 * rf.predict(X_train[-eval_tail:]) + 0.45 * ridge.predict(X_train[-eval_tail:])
        snr_train_proxy = np.tanh(
            (0.6 * X_train_norm[valid_mask][-eval_tail:, 1] + 0.4 * X_train_norm[valid_mask][-eval_tail:, 3])
            / max(np.sqrt(fast_span / max(slow_span, 1)), 0.25)
        )
        y_tail = y_train[-eval_tail:]

        ic_mace = cls._safe_spearman_ic(mace_train_preds, y_tail)
        ic_snr = cls._safe_spearman_ic(snr_train_proxy, y_tail)
        return mace_score, ic_mace, ic_snr

    @staticmethod
    def _safe_spearman_ic(preds: np.ndarray, realized: np.ndarray) -> float:
        """Compute Spearman Rank Information Coefficient safely without NaN warnings."""
        if len(preds) < 5 or len(realized) < 5:
            return 0.05
        if np.std(preds) <= 1e-12 or np.std(realized) <= 1e-12:
            return 0.05
        res = spearmanr(preds, realized)
        corr = float(res.statistic if hasattr(res, "statistic") else res[0])
        return corr if np.isfinite(corr) else 0.05

    @classmethod
    def compute_mace_nonlinear_score(
        cls,
        df_or_exec: pd.DataFrame,
        synth_df_or_cfg: pd.DataFrame | dict[str, Any] | None = None,
        cfg: dict[str, Any] | None = None,
        *,
        n_estimators: int | None = None,
        max_depth: int | None = None,
        min_samples_leaf: int | None = None,
        ridge_alpha: float | None = None,
    ) -> float:
        """
        Compute purged out-of-sample MACE non-linear score in [-1.0, 1.0].
        Supports both:
          - compute_mace_nonlinear_score(df, cfg)
          - compute_mace_nonlinear_score(exec_df, synth_df, cfg)
        """
        if isinstance(synth_df_or_cfg, dict):
            synth_df: pd.DataFrame | None = None
            eff_cfg = synth_df_or_cfg
        else:
            synth_df = synth_df_or_cfg
            eff_cfg = cfg

        score, _, _ = cls._compute_mace_and_ic(
            exec_df=df_or_exec,
            synth_df=synth_df,
            cfg=eff_cfg,
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            ridge_alpha=ridge_alpha,
        )
        return score

    @classmethod
    def compute_mace_score(
        cls,
        exec_df: pd.DataFrame,
        synth_df: pd.DataFrame | None = None,
        n_estimators: int = 60,
        max_depth: int = 4,
        min_samples_leaf: int = 10,
        ridge_alpha: float = 1.0,
        cfg: dict[str, Any] | None = None,
    ) -> float:
        """
        Compute Multivariate Alternating Conditional Expectations (MACE) non-linear score
        using purged feature-target slicing (`iloc[warmup : -k]`) and out-of-sample inference (`iloc[[-1]]`).
        """
        score, _, _ = cls._compute_mace_and_ic(
            exec_df=exec_df,
            synth_df=synth_df,
            cfg=cfg,
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            ridge_alpha=ridge_alpha,
        )
        return score

    @staticmethod
    def compute_dynamic_mace_weight(
        ic_mace: float,
        ic_snr: float,
        min_ic_floor: float = 0.05,
    ) -> float:
        """
        Compute dynamic Spearman Rank Information Coefficient (IC) weight for MACE:
            Weight_MACE = max(IC_MACE, floor) / (max(IC_MACE, floor) + max(IC_SNR, floor))
        bounded strictly within [0.15, 0.60].
        """
        floor = float(min_ic_floor)
        m_ic = max(float(ic_mace) if np.isfinite(ic_mace) else 0.0, floor)
        s_ic = max(float(ic_snr) if np.isfinite(ic_snr) else 0.0, floor)
        raw_weight = m_ic / max(m_ic + s_ic, 1e-12)
        return float(np.clip(raw_weight, 0.15, 0.60))

    @classmethod
    def compute_btc_macro_confirmation(
        cls,
        df_btc: pd.DataFrame,
        cfg: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """
        Compute causal multi-horizon BTC macro trend confirmation metrics at the latest bar:
        - btc_snr: fast/slow (16/64) continuous SNR on BTC/USDC
        - btc_snr_macro: macro (48/192) EMA continuous SNR on BTC/USDC
        - btc_ret_30: 30-bar (5-day at 4H) BTC log return
        - btc_ret_60: 60-bar (10-day at 4H) BTC log return
        - btc_ret_180: 180-bar (30-day at 4H) BTC log return
        """
        config = cfg or {}
        if df_btc is None or df_btc.empty or "close" not in df_btc.columns:
            return {
                "btc_snr": 0.0,
                "btc_snr_macro": 0.0,
                "btc_ret_30": 0.0,
                "btc_ret_60": 0.0,
                "btc_ret_180": 0.0,
            }
        fast_span = int(config.get("snr_fast_span", 16))
        slow_span = int(config.get("snr_slow_span", 64))
        tanh_scale = float(config.get("mace_tanh_scale", 1.25))

        closes = np.maximum(
            cls._sanitize_positive_series(df_btc["close"], fallback=1.0).to_numpy(dtype=np.float64),
            1e-12,
        )
        snr_fast_slow = cls._compute_causal_snr_series(
            closes, fast_span=fast_span, slow_span=slow_span, tanh_scale=tanh_scale
        )
        snr_macro = cls._compute_causal_snr_series(
            closes, fast_span=48, slow_span=192, tanh_scale=tanh_scale
        )
        log_c = np.log(closes)
        last_idx = len(log_c) - 1
        ret_30 = float(log_c[last_idx] - log_c[max(0, last_idx - 30)]) if last_idx >= 0 else 0.0
        ret_60 = float(log_c[last_idx] - log_c[max(0, last_idx - 60)]) if last_idx >= 0 else 0.0
        ret_180 = float(log_c[last_idx] - log_c[max(0, last_idx - 180)]) if last_idx >= 0 else 0.0

        return {
            "btc_snr": float(snr_fast_slow[-1]) if len(snr_fast_slow) > 0 else 0.0,
            "btc_snr_macro": float(snr_macro[-1]) if len(snr_macro) > 0 else 0.0,
            "btc_ret_30": ret_30,
            "btc_ret_60": ret_60,
            "btc_ret_180": ret_180,
        }

    @classmethod
    def classify_signal_action(
        cls,
        *,
        regime: str,
        prob_bull: float,
        prob_bear: float,
        rs_rank: int,
        total_assets: int,
        blended_conviction: float,
        snr_synth: float,
        mace_score: float,
        rs_96: float,
        r_usdc_8: float,
        funding_rate_hourly: float,
        btc_macro: dict[str, float],
        cfg: dict[str, Any] | None = None,
    ) -> str:
        """
        Evaluate Calibration 2 Symmetric High-Conviction Directional Gates + Multi-Horizon
        BTC Macro Confirmation for LONG / SHORT / BASIS_HARVEST / FLAT classification.
        """
        config = cfg or {}
        entry_threshold = float(config.get("conviction_entry_threshold", 0.02))
        max_longs = int(config.get("max_concurrent_long_positions", 2))
        max_shorts = int(config.get("max_concurrent_short_positions", 2))
        short_funding_floor = float(config.get("short_max_adverse_funding_rate_hourly", -0.0005))
        basis_enabled = bool(config.get("basis_harvest_enabled", True))
        basis_min_rate = float(config.get("basis_min_hourly_funding_rate", 0.000015))

        bull_prob_gate = float(config.get("bull_prob_entry_gate", 0.58))
        bull_btc_snr_gate = float(config.get("bull_btc_snr_gate", 0.12))
        bull_conv_gate = max(entry_threshold, float(config.get("bull_conviction_gate", 0.32)))
        bull_snr_synth_gate = float(config.get("bull_snr_synth_gate", 0.30))
        bull_mace_gate = float(config.get("bull_mace_gate", 0.05))
        bull_rs_96_gate = float(config.get("bull_rs_96_gate", 0.02))

        bear_prob_gate = float(config.get("bear_prob_entry_gate", 0.58))
        bear_btc_snr_gate = float(config.get("bear_btc_snr_gate", -0.45))
        bear_btc_snr_macro_gate = float(config.get("bear_btc_snr_macro_gate", -0.20))
        bear_btc_ret_30_gate = float(config.get("bear_btc_ret_30_gate", -0.015))
        bear_btc_ret_60_gate = float(config.get("bear_btc_ret_60_gate", -0.04))
        bear_btc_ret_180_gate = float(config.get("bear_btc_ret_180_gate", 0.002))
        bear_conv_gate = min(-abs(entry_threshold), float(config.get("bear_conviction_gate", -0.32)))
        bear_snr_synth_gate = float(config.get("bear_snr_synth_gate", -0.30))
        bear_mace_gate = float(config.get("bear_mace_gate", -0.05))
        bear_rs_96_gate = float(config.get("bear_rs_96_gate", -0.03))
        bear_r_usdc_8_gate = float(config.get("bear_r_usdc_8_gate", -0.008))

        btc_snr_val = float(btc_macro.get("btc_snr", 0.0))
        btc_snr_macro_val = float(btc_macro.get("btc_snr_macro", 0.0))
        btc_ret_30_val = float(btc_macro.get("btc_ret_30", 0.0))
        btc_ret_60_val = float(btc_macro.get("btc_ret_60", 0.0))
        btc_ret_180_val = float(btc_macro.get("btc_ret_180", 0.0))

        if (
            regime == "BULL"
            and prob_bull >= bull_prob_gate
            and btc_snr_val >= bull_btc_snr_gate
            and rs_rank <= max_longs
            and blended_conviction > bull_conv_gate
            and snr_synth > bull_snr_synth_gate
            and mace_score > bull_mace_gate
            and rs_96 > bull_rs_96_gate
        ):
            return "LONG"
        if (
            regime == "BEAR"
            and prob_bear >= bear_prob_gate
            and btc_snr_val <= bear_btc_snr_gate
            and btc_snr_macro_val <= bear_btc_snr_macro_gate
            and btc_ret_30_val <= bear_btc_ret_30_gate
            and btc_ret_60_val <= bear_btc_ret_60_gate
            and btc_ret_180_val <= bear_btc_ret_180_gate
            and rs_rank > (total_assets - max_shorts)
            and blended_conviction < bear_conv_gate
            and snr_synth < bear_snr_synth_gate
            and mace_score < bear_mace_gate
            and rs_96 < bear_rs_96_gate
            and r_usdc_8 < bear_r_usdc_8_gate
            and funding_rate_hourly >= short_funding_floor
        ):
            return "SHORT"
        if basis_enabled and funding_rate_hourly >= basis_min_rate:
            return "BASIS_HARVEST"
        return "FLAT"

    def evaluate_universe(
        self,
        config: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> tuple[RegimeState, list[dict[str, Any]]]:
        """
        Decode the cached warm-start 3-State HMM regime and evaluate purged MACE + SNR
        with dynamic Spearman IC blending across all 15 paired assets in v_universe_alt_pairs.
        """
        assert self.db is not None
        cfg = config or self.db.load_config()
        alt_pairs = self.db.get_alt_pairs_universe()

        window_limit = int(cfg.get("hmm_lookback_bars", cfg.get("hmm_window_bars", 540)))
        rf_trees = int(cfg.get("mace_rf_n_estimators", 60))
        rf_depth = int(cfg.get("mace_rf_max_depth", 4))
        rf_leaf = int(cfg.get("mace_rf_min_samples_leaf", 10))
        ridge_alpha = float(cfg.get("mace_ridge_alpha", 1.0))
        fast_span = int(cfg.get("snr_fast_span", 16))
        slow_span = int(cfg.get("snr_slow_span", 64))
        snr_synth_weight = float(cfg.get("snr_synth_weight", 0.60))
        mace_tanh_scale = float(cfg.get("mace_tanh_scale", 1.25))
        min_ic_floor = float(cfg.get("mace_min_ic_floor", 0.05))

        benchmark_symbol = str(cfg.get("benchmark_symbol", "BTC/USDC:USDC"))
        hmm_tf = str(cfg.get("hmm_timeframe", "4h"))
        df_btc = self.db.get_ohlcv_df(benchmark_symbol, hmm_tf, limit=max(window_limit, 220))
        btc_macro = self.compute_btc_macro_confirmation(df_btc, cfg)

        regime_state = self.decode_hmm_regime(cfg)

        now_ms = (
            int(self.db.pit_timestamp_ms)
            if self.db.pit_timestamp_ms is not None
            else int(time.time() * 1000)
        )
        raw_evals: list[dict[str, Any]] = []

        for pair in alt_pairs:
            base_asset = pair["base_asset"]
            exec_sym = pair["exec_symbol"]
            synth_sym = pair["synth_symbol"]
            exec_tf = str(pair["exec_timeframe"])
            rs_tf = str(pair["rs_timeframe"])

            exec_df = self.db.get_ohlcv_df(exec_sym, exec_tf, limit=window_limit)
            synth_df = self.db.get_ohlcv_df(synth_sym, rs_tf, limit=window_limit)

            snr_score = self.compute_continuous_snr(
                exec_df=exec_df,
                synth_df=synth_df,
                fast_span=fast_span,
                slow_span=slow_span,
                snr_synth_weight=snr_synth_weight,
                tanh_scale=mace_tanh_scale,
            )
            if synth_df is not None and not synth_df.empty and len(synth_df) >= 20:
                snr_synth_arr = self._compute_causal_snr_series(
                    synth_df["close"].to_numpy(dtype=np.float64),
                    fast_span=fast_span,
                    slow_span=slow_span,
                    tanh_scale=mace_tanh_scale,
                )
                snr_synth_val = float(snr_synth_arr[-1])
            else:
                snr_synth_val = snr_score

            if synth_df is not None and not synth_df.empty:
                s_closes = np.maximum(
                    self._sanitize_positive_series(synth_df["close"], fallback=1.0).to_numpy(dtype=np.float64),
                    1e-12,
                )
                s_log = np.log(s_closes)
                rs_96_val = float(s_log[-1] - s_log[max(0, len(s_log) - 1 - 96)])
            else:
                rs_96_val = 0.0

            if exec_df is not None and not exec_df.empty:
                e_closes = np.maximum(
                    self._sanitize_positive_series(exec_df["close"], fallback=1.0).to_numpy(dtype=np.float64),
                    1e-12,
                )
                e_log = np.log(e_closes)
                r_usdc_8_val = float(e_log[-1] - e_log[max(0, len(e_log) - 1 - 8)])
            else:
                r_usdc_8_val = 0.0

            mace_score, ic_mace, ic_snr = self._compute_mace_and_ic(
                exec_df=exec_df,
                synth_df=synth_df,
                cfg=cfg,
                n_estimators=rf_trees,
                max_depth=rf_depth,
                min_samples_leaf=rf_leaf,
                ridge_alpha=ridge_alpha,
            )
            mace_weight = self.compute_dynamic_mace_weight(
                ic_mace=ic_mace, ic_snr=ic_snr, min_ic_floor=min_ic_floor
            )
            snr_weight = 1.0 - mace_weight
            blended = float(np.clip(mace_weight * mace_score + snr_weight * snr_score, -1.0, 1.0))
            funding_hourly = (
                float(exec_df["funding_rate_hourly"].iloc[-1]) if not exec_df.empty else 0.000018
            )

            raw_evals.append(
                {
                    "timestamp_ms": now_ms,
                    "base_asset": base_asset,
                    "exec_symbol": exec_sym,
                    "synth_symbol": synth_sym,
                    "hmm_regime": regime_state.regime,
                    "prob_bull": regime_state.prob_bull,
                    "prob_bear": regime_state.prob_bear,
                    "prob_chop": regime_state.prob_chop,
                    "snr_score": round(snr_score, 4),
                    "snr_synth": round(snr_synth_val, 4),
                    "rs_96": round(rs_96_val, 5),
                    "r_usdc_8": round(r_usdc_8_val, 5),
                    "mace_score": round(mace_score, 4),
                    "blended_conviction": round(blended, 4),
                    "funding_rate_hourly": round(funding_hourly, 6),
                }
            )

        # Rank descending by blended_conviction (Rank 1 = strongest leader, Rank 15 = weakest laggard)
        raw_evals.sort(key=lambda x: x["blended_conviction"], reverse=True)
        total_assets = len(raw_evals)

        for idx, item in enumerate(raw_evals):
            rank = idx + 1
            item["rs_rank"] = rank
            item["signal_action"] = self.classify_signal_action(
                regime=regime_state.regime,
                prob_bull=regime_state.prob_bull,
                prob_bear=regime_state.prob_bear,
                rs_rank=rank,
                total_assets=total_assets,
                blended_conviction=float(item["blended_conviction"]),
                snr_synth=float(item["snr_synth"]),
                mace_score=float(item["mace_score"]),
                rs_96=float(item["rs_96"]),
                r_usdc_8=float(item["r_usdc_8"]),
                funding_rate_hourly=float(item["funding_rate_hourly"]),
                btc_macro=btc_macro,
                cfg=cfg,
            )

        if persist:
            self.db.save_signal_evaluations(raw_evals)
        return regime_state, raw_evals

