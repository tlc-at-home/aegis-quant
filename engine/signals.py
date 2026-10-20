"""3-State Diagonal-Covariance Gaussian HMM, MACE Ensemble, and Long/Short Signal Gates."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.mixture import GaussianMixture

from engine.database import DatabaseManager

logger = logging.getLogger("aegis.signals")


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
        # X: (T, D), means: (K, D), variances: (K, D)
        safe_var = np.maximum(variances, 1e-8)
        diff = X[:, None, :] - means[None, :, :]  # (T, K, D)
        log_det = np.sum(np.log(2.0 * np.pi * safe_var), axis=-1)  # (K,)
        mahalanobis = np.sum((diff ** 2) / safe_var[None, :, :], axis=-1)  # (T, K)
        return -0.5 * (log_det[None, :] + mahalanobis)

    @staticmethod
    def _logsumexp(arr: np.ndarray, axis: int = -1, keepdims: bool = False) -> np.ndarray:
        max_val = np.max(arr, axis=axis, keepdims=True)
        res = max_val + np.log(np.maximum(np.sum(np.exp(arr - max_val), axis=axis, keepdims=True), 1e-300))
        if not keepdims:
            res = np.squeeze(res, axis=axis)
        return res

    def fit_decode_posterior(self, features: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Fit the 3-State Diagonal-Covariance Gaussian HMM over `features` (T, D)
        and return (posterior_gamma (T, K), means (K, D), variances (K, D)).
        """
        T, D = features.shape
        K = self.n_states

        # Initialize emission parameters via Diagonal Gaussian Mixture
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

        # Sticky initial Markov transition matrix
        trans = np.full((K, K), 0.05 / max(K - 1, 1), dtype=np.float64)
        np.fill_diagonal(trans, 0.95)

        gamma = np.full((T, K), 1.0 / K, dtype=np.float64)

        for _ in range(self.n_iter):
            log_B = self._log_gaussian_diag_pdf(features, means, variances)  # (T, K)
            log_pi = np.log(np.maximum(pi, 1e-12))
            log_A = np.log(np.maximum(trans, 1e-12))

            # Forward pass in log-space
            log_alpha = np.zeros((T, K), dtype=np.float64)
            log_alpha[0] = log_pi + log_B[0]
            log_alpha[0] -= self._logsumexp(log_alpha[0], axis=0)

            for t in range(1, T):
                # log_alpha[t, j] = log_B[t, j] + logsumexp_i(log_alpha[t-1, i] + log_A[i, j])
                trans_scores = log_alpha[t - 1][:, None] + log_A  # (K, K)
                log_alpha[t] = log_B[t] + self._logsumexp(trans_scores, axis=0)
                log_alpha[t] -= self._logsumexp(log_alpha[t], axis=0)

            # Backward pass in log-space
            log_beta = np.zeros((T, K), dtype=np.float64)
            for t in range(T - 2, -1, -1):
                # log_beta[t, i] = logsumexp_j(log_A[i, j] + log_B[t+1, j] + log_beta[t+1, j])
                back_scores = log_A + log_B[t + 1][None, :] + log_beta[t + 1][None, :]
                log_beta[t] = self._logsumexp(back_scores, axis=1)
                log_beta[t] -= self._logsumexp(log_beta[t], axis=0)

            # Posterior state probabilities gamma_t(k)
            log_gamma = log_alpha + log_beta
            log_gamma -= self._logsumexp(log_gamma, axis=1, keepdims=True)
            gamma = np.exp(log_gamma)

            # Update parameters (M-step)
            state_weights = np.maximum(np.sum(gamma, axis=0), 1e-8)
            pi = gamma[0] / np.maximum(np.sum(gamma[0]), 1e-8)
            means = (gamma.T @ features) / state_weights[:, None]
            diff = features[:, None, :] - means[None, :, :]
            variances = np.maximum(
                np.sum(gamma[:, :, None] * (diff ** 2), axis=0) / state_weights[:, None],
                1e-6,
            )
            if T > 1:
                # Baum-Welch expected transitions xi_t(i, j) with sticky Dirichlet prior
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
    Evaluates the 540-bar 3-State Gaussian HMM regime gate, MACE Random Forest +
    Ridge non-linear ensemble (35%), and continuous SNR momentum forecast (65%).
    """

    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def decode_hmm_regime(self, config: dict[str, Any] | None = None) -> RegimeState:
        """Decode Bull, Bear, and Chop posterior probabilities over 540 4H bars."""
        cfg = config or self.db.load_config()
        benchmark = str(cfg.get("benchmark_symbol", "BTC/USDC:USDC"))
        timeframe = str(cfg.get("hmm_timeframe", "4h"))
        window_bars = int(cfg.get("hmm_window_bars", 540))
        n_states = int(cfg.get("hmm_num_states", 3))
        em_iters = int(cfg.get("hmm_em_iterations", 25))

        df = self.db.get_ohlcv_df(benchmark, timeframe, limit=window_bars + 20)
        if len(df) < 40:
            return RegimeState(
                regime="CHOP",
                prob_bull=0.3333,
                prob_bear=0.3333,
                prob_chop=0.3334,
                window_bars=len(df),
            )

        closes = df["close"].to_numpy(dtype=np.float64)
        highs = df["high"].to_numpy(dtype=np.float64)
        lows = df["low"].to_numpy(dtype=np.float64)

        log_ret = np.diff(np.log(np.maximum(closes, 1e-12)), prepend=np.log(max(closes[0], 1e-12)))
        ret_series = pd.Series(log_ret)
        ema_drift = ret_series.ewm(span=12, adjust=False).mean().to_numpy(dtype=np.float64)
        realized_vol = (
            ret_series.rolling(14, min_periods=2).std().bfill().fillna(0.01).to_numpy(dtype=np.float64)
        )
        hl_range = np.log(np.maximum(highs, 1e-12) / np.maximum(lows, 1e-12))

        feat_matrix = np.column_stack([ema_drift, realized_vol, hl_range])
        if len(feat_matrix) > window_bars:
            feat_matrix = feat_matrix[-window_bars:]

        # Standardize features for numerical stability
        feat_mean = np.mean(feat_matrix, axis=0)
        feat_std = np.maximum(np.std(feat_matrix, axis=0), 1e-6)
        norm_feats = (feat_matrix - feat_mean) / feat_std

        hmm = DiagonalGaussianHMM(n_states=n_states, n_iter=em_iters, random_state=42)
        gamma, means, _ = hmm.fit_decode_posterior(norm_feats)

        # Rank states by mean drift (feature index 0): lowest = Bear, middle = Chop, highest = Bull
        state_order = np.argsort(means[:, 0])
        bear_idx = int(state_order[0])
        chop_idx = int(state_order[1])
        bull_idx = int(state_order[-1])

        last_post = gamma[-1]
        prob_bull = float(np.clip(last_post[bull_idx], 0.0, 1.0))
        prob_bear = float(np.clip(last_post[bear_idx], 0.0, 1.0))
        prob_chop = float(np.clip(last_post[chop_idx], 0.0, 1.0))
        total_p = prob_bull + prob_bear + prob_chop
        if total_p > 0:
            prob_bull /= total_p
            prob_bear /= total_p
            prob_chop /= total_p

        bull_thresh = float(cfg.get("hmm_bull_prob_threshold", 0.50))
        bear_thresh = float(cfg.get("hmm_bear_prob_threshold", 0.50))

        if prob_bull >= bull_thresh and prob_bull >= prob_bear:
            regime = "BULL"
        elif prob_bear >= bear_thresh and prob_bear > prob_bull:
            regime = "BEAR"
        else:
            regime = "CHOP"

        return RegimeState(
            regime=regime,
            prob_bull=round(prob_bull, 4),
            prob_bear=round(prob_bear, 4),
            prob_chop=round(prob_chop, 4),
            window_bars=len(feat_matrix),
        )

    @staticmethod
    def compute_continuous_snr(
        exec_df: pd.DataFrame,
        synth_df: pd.DataFrame,
        fast_span: int = 16,
        slow_span: int = 64,
    ) -> float:
        """
        Compute continuous multi-horizon Signal-to-Noise Ratio (SNR) forecast in [-1.0, 1.0]
        combining ALT/BTC:SYNTH relative strength (60%) and ALT/USDC execution trend (40%).
        """
        def _series_snr(closes: np.ndarray) -> float:
            if len(closes) < 20:
                return 0.0
            s = pd.Series(np.log(np.maximum(closes, 1e-12)))
            rets = s.diff().fillna(0.0)
            vol = float(rets.iloc[-slow_span:].std()) if len(rets) >= slow_span else float(rets.std())
            vol = max(vol, 1e-6)

            ema_fast = float(s.ewm(span=fast_span, adjust=False).mean().iloc[-1])
            ema_slow = float(s.ewm(span=slow_span, adjust=False).mean().iloc[-1])
            raw_signal = (ema_fast - ema_slow) / (vol * np.sqrt(fast_span))
            return float(np.tanh(raw_signal * 1.25))

        snr_synth = _series_snr(synth_df["close"].to_numpy(dtype=np.float64)) if not synth_df.empty else 0.0
        snr_exec = _series_snr(exec_df["close"].to_numpy(dtype=np.float64)) if not exec_df.empty else 0.0
        return float(np.clip(0.60 * snr_synth + 0.40 * snr_exec, -1.0, 1.0))

    @staticmethod
    def compute_mace_score(
        exec_df: pd.DataFrame,
        synth_df: pd.DataFrame,
        n_estimators: int = 60,
        max_depth: int = 4,
        min_samples_leaf: int = 10,
        ridge_alpha: float = 1.0,
    ) -> float:
        """
        Compute Multivariate Alternating Conditional Expectations (MACE) non-linear score
        using a regularized Random Forest Regressor (60 trees, max depth 4) + L2 Ridge Regression.
        """
        if len(exec_df) < 80 or len(synth_df) < 40:
            return 0.0

        exec_sorted = (
            exec_df[["timestamp_ms", "close", "high", "low", "volume"]]
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

        if len(merged) < 80:
            return 0.0

        c_usdc = np.log(np.maximum(merged["close_usdc"].to_numpy(dtype=np.float64), 1e-12))
        c_synth = np.log(np.maximum(merged["close_synth"].to_numpy(dtype=np.float64), 1e-12))
        vol_arr = merged["volume"].to_numpy(dtype=np.float64)

        df_feat = pd.DataFrame(
            {
                "r_synth_4": pd.Series(c_synth).diff(4).fillna(0.0),
                "r_synth_16": pd.Series(c_synth).diff(16).fillna(0.0),
                "r_synth_32": pd.Series(c_synth).diff(32).fillna(0.0),
                "r_usdc_8": pd.Series(c_usdc).diff(8).fillna(0.0),
                "r_usdc_24": pd.Series(c_usdc).diff(24).fillna(0.0),
                "vol_z": (
                    (pd.Series(vol_arr) - pd.Series(vol_arr).rolling(24, min_periods=4).mean())
                    / np.maximum(pd.Series(vol_arr).rolling(24, min_periods=4).std(), 1e-6)
                ).fillna(0.0),
            }
        )
        # Alternating conditional transformation: rank-gauss / cubic spline basis expansion
        X_raw = df_feat.to_numpy(dtype=np.float64)
        X_mean = np.mean(X_raw, axis=0)
        X_std = np.maximum(np.std(X_raw, axis=0), 1e-6)
        X_norm = np.clip((X_raw - X_mean) / X_std, -3.0, 3.0)
        X_mace = np.hstack([X_norm, np.tanh(X_norm), X_norm ** 2 - 1.0])

        # Target: 4-bar forward volatility-normalized relative-strength return
        fwd_ret = pd.Series(c_synth).shift(-4) - pd.Series(c_synth)
        ret_std = max(float(fwd_ret.dropna().std()), 1e-6)
        y_all = (fwd_ret / ret_std).fillna(0.0).to_numpy(dtype=np.float64)

        X_train = X_mace[:-4]
        y_train = np.clip(y_all[:-4], -3.0, 3.0)
        X_latest = X_mace[-1:]

        rf = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            random_state=42,
            n_jobs=1,
        )
        ridge = Ridge(alpha=ridge_alpha, random_state=42)

        rf.fit(X_train, y_train)
        ridge.fit(X_train, y_train)

        pred_rf = float(rf.predict(X_latest)[0])
        pred_ridge = float(ridge.predict(X_latest)[0])
        mace_raw = 0.55 * pred_rf + 0.45 * pred_ridge
        return float(np.tanh(mace_raw))

    def evaluate_universe(self) -> tuple[RegimeState, list[dict[str, Any]]]:
        """
        Decode the 3-State HMM regime and evaluate MACE (35%) + SNR (65%)
        across all 15 paired assets in v_universe_alt_pairs.
        """
        config = self.db.load_config()
        regime_state = self.decode_hmm_regime(config)
        alt_pairs = self.db.get_alt_pairs_universe()

        window_limit = int(config.get("hmm_window_bars", 540))
        mace_weight = float(config.get("mace_blend_weight", 0.35))
        snr_weight = float(config.get("snr_blend_weight", 0.65))
        rf_trees = int(config.get("mace_rf_n_estimators", 60))
        rf_depth = int(config.get("mace_rf_max_depth", 4))
        rf_leaf = int(config.get("mace_rf_min_samples_leaf", 10))
        ridge_alpha = float(config.get("mace_ridge_alpha", 1.0))
        fast_span = int(config.get("snr_fast_span", 16))
        slow_span = int(config.get("snr_slow_span", 64))

        max_longs = int(config.get("max_concurrent_long_positions", 2))
        max_shorts = int(config.get("max_concurrent_short_positions", 2))
        short_funding_floor = float(config.get("short_max_adverse_funding_rate_hourly", -0.0005))
        basis_enabled = bool(config.get("basis_harvest_enabled", True))
        basis_min_funding = float(config.get("basis_min_hourly_funding_rate", 0.000015))

        now_ms = int(time.time() * 1000)
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
                exec_df=exec_df, synth_df=synth_df, fast_span=fast_span, slow_span=slow_span
            )
            mace_score = self.compute_mace_score(
                exec_df=exec_df,
                synth_df=synth_df,
                n_estimators=rf_trees,
                max_depth=rf_depth,
                min_samples_leaf=rf_leaf,
                ridge_alpha=ridge_alpha,
            )
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
            conv = item["blended_conviction"]
            funding = item["funding_rate_hourly"]

            action = "FLAT"
            if regime_state.regime == "BULL" and rank <= max_longs and conv > 0.02:
                action = "LONG"
            elif (
                regime_state.regime == "BEAR"
                and rank > (total_assets - max_shorts)
                and conv < -0.02
                and funding >= short_funding_floor
            ):
                action = "SHORT"
            elif basis_enabled and funding >= basis_min_funding:
                action = "BASIS_HARVEST"

            item["signal_action"] = action

        self.db.save_signal_evaluations(raw_evals)
        return regime_state, raw_evals
