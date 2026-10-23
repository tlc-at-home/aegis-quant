"""Point-in-time event-driven backtesting engine for Aegis Quant (v0.2.0-alpha1)."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from engine.calibration import WalkForwardCalibrator
from engine.database import DEFAULT_DB_PATH, PROJECT_ROOT, DatabaseManager
from engine.ingestion import BASE_PRICES_USDC, MarketIngestionEngine
from engine.risk import PositionSizeResult, RiskEngine
from engine.signals import SignalEngine

logger = logging.getLogger("aegis.backtest")

MIGRATION_001_PATH = PROJECT_ROOT / "sql" / "migrations" / "001_v020_config_expansion.sql"
MIGRATION_002_PATH = PROJECT_ROOT / "sql" / "migrations" / "002_v020_order_fsm.sql"
MIGRATION_003_PATH = PROJECT_ROOT / "sql" / "migrations" / "003_v020_alerts_systemd.sql"
MIGRATION_004_PATH = PROJECT_ROOT / "sql" / "migrations" / "004_v020_rc2_remediation.sql"


def verify_data_provenance(backfill_path: str | Path = "data/real_4h_backfill.json") -> bool:
    """Verifies SHA-256 checksum of the historical backfill dataset."""
    file_path = Path(backfill_path)
    hash_path = Path(f"{backfill_path}.sha256")

    if not file_path.exists():
        logger.error("Historical backfill data file missing at %s", backfill_path)
        return False

    if not hash_path.exists():
        logger.warning("SHA-256 checksum file missing at %s. Hash verification skipped.", hash_path)
        return True

    expected_hash = hash_path.read_text(encoding="utf-8").strip().split()[0]
    actual_hash = hashlib.sha256(file_path.read_bytes()).hexdigest()

    if actual_hash != expected_hash:
        raise ValueError(
            f"BACKFILL DATA CORRUPTION: SHA-256 mismatch for {backfill_path}.\n"
            f"Expected: {expected_hash}\nActual:   {actual_hash}"
        )
    logger.info("SHA-256 provenance verified for %s", backfill_path)
    return True


@dataclass
class BacktestTearSheet:
    """Comprehensive point-in-time backtest tear sheet and acceptance gate verification."""

    start_date: str
    end_date: str
    duration_days: float
    bars_replayed: int
    initial_equity_usdc: float
    final_equity_usdc: float
    total_return_pct: float
    annualized_return_pct: float
    annualized_sharpe: float
    sortino_ratio: float
    calmar_ratio: float
    max_drawdown_pct: float
    profit_factor: float
    trade_profit_factor: float
    win_rate_pct: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    gross_profit_usdc: float
    gross_loss_usdc: float
    total_fees_usdc: float
    total_slippage_usdc: float
    wick_stop_outs: int
    fee_drag_pct: float
    funding_net_usdc: float
    basis_yield_usdc: float
    bull_regime_pnl_usdc: float
    bear_regime_pnl_usdc: float
    chop_regime_pnl_usdc: float
    bull_bars_count: int
    bear_bars_count: int
    chop_bars_count: int
    gate_sharpe_pass: bool
    gate_mdd_pass: bool
    gate_profit_factor_pass: bool
    all_gates_passed: bool

    @property
    def sharpe(self) -> float:
        """Alias for annualized_sharpe used by holdout validation scripts."""
        return float(self.annualized_sharpe)

    @property
    def max_drawdown(self) -> float:
        """Fractional maximum drawdown (e.g. 0.0385 for 3.85%) used by holdout validation scripts."""
        return round(float(self.max_drawdown_pct) / 100.0, 4)

    def print_summary(self) -> None:
        """Print formatted tear sheet summary to stdout."""
        print(BacktestEngine.format_tear_sheet(self))


class BacktestEngine:
    """
    Event-driven historical replay engine running in an isolated in-memory SQLite (:memory:)
    clone with strict point-in-time data slicing and full tear sheet generation.
    """

    def __init__(
        self,
        source_db_path: Path | str = DEFAULT_DB_PATH,
        initial_equity: float = 10000.0,
        data_path: Path | str | None = None,
        config_overrides: dict[str, Any] | None = None,
    ) -> None:
        self.source_db_path = Path(source_db_path)
        self.initial_equity = float(initial_equity)
        self._explicit_data_path = data_path is not None
        self.data_path = (
            Path(data_path)
            if data_path is not None
            else (PROJECT_ROOT / "data" / "real_4h_backfill.json")
        )
        self.config_overrides: dict[str, Any] = (
            dict(config_overrides) if config_overrides is not None else {}
        )
        self.mem_db = DatabaseManager(":memory:")
        self.signals = SignalEngine(self.mem_db)
        self.risk = RiskEngine(self.mem_db)
        self._mace_model_cache: dict[str, dict[str, Any]] = {}

    def run(
        self,
        start_date: str = "2026-07-20",
        end_date: str = "2026-10-20",
    ) -> BacktestTearSheet:
        """Alias for run_backtest() supporting HistoricalBacktester interface."""
        return self.run_backtest(start_date=start_date, end_date=end_date)

    @staticmethod
    def _parse_date_utc_ms(date_str: str, end_of_day: bool = False) -> int:
        """Parse YYYY-MM-DD or ISO-8601 timestamp into UTC milliseconds."""
        cleaned = date_str.strip()
        if "T" in cleaned:
            parsed = dt.datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return int(parsed.timestamp() * 1000)
        d = dt.date.fromisoformat(cleaned)
        t = dt.time(23, 59, 59) if end_of_day else dt.time(0, 0, 0)
        combined = dt.datetime.combine(d, t, tzinfo=dt.timezone.utc)
        return int(combined.timestamp() * 1000)

    def _clone_to_memory_db(self, start_ms: int, end_ms: int) -> None:
        """
        Initialize isolated :memory: SQLite database, apply Migrations 001..004, clone historical
        market_ohlcv data from source DB, and backfill pre-window warmup bars if needed.
        """
        self.mem_db.init_db()
        for mig_path in (MIGRATION_001_PATH, MIGRATION_002_PATH, MIGRATION_003_PATH, MIGRATION_004_PATH):
            if mig_path.exists():
                self.mem_db.apply_migration(mig_path)

        if self.source_db_path.exists():
            src_db = DatabaseManager(self.source_db_path)
            with src_db.connection() as s_conn, self.mem_db.connection() as m_conn:
                cfg_rows = s_conn.execute(
                    "SELECT config_key, config_value, value_type, category, description FROM system_config;"
                ).fetchall()
                m_conn.executemany(
                    """
                    INSERT OR REPLACE INTO system_config
                        (config_key, config_value, value_type, category, description)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    [tuple(r) for r in cfg_rows],
                )
                if MIGRATION_001_PATH.exists():
                    m_conn.executescript(MIGRATION_001_PATH.read_text(encoding="utf-8"))
                if MIGRATION_004_PATH.exists():
                    m_conn.executescript(MIGRATION_004_PATH.read_text(encoding="utf-8"))

                ohlcv_rows = s_conn.execute(
                    """
                    SELECT symbol, timeframe, timestamp_ms, open, high, low, close,
                           volume, funding_rate_hourly, is_synthetic
                    FROM market_ohlcv
                    WHERE timeframe = '4h' AND timestamp_ms <= ?
                    ORDER BY timestamp_ms ASC;
                    """,
                    (int(end_ms),),
                ).fetchall()
                if ohlcv_rows:
                    m_conn.executemany(
                        """
                        INSERT OR REPLACE INTO market_ohlcv (
                            symbol, timeframe, timestamp_ms, open, high, low, close,
                            volume, funding_rate_hourly, is_synthetic
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        [tuple(r) for r in ohlcv_rows],
                    )

        real_backfill_path = self.data_path
        should_load_backfill = self._explicit_data_path or (
            self.source_db_path.resolve() == DEFAULT_DB_PATH.resolve()
        )
        if should_load_backfill and real_backfill_path.exists():
            verify_data_provenance(real_backfill_path)
            try:
                bf_data = json.loads(real_backfill_path.read_text(encoding="utf-8"))
                bf_rows: list[tuple[Any, ...]] = []
                for coin, info in bf_data.get("coins", {}).items():
                    sym = f"{coin}/USDC:USDC"
                    fr = float(info.get("funding_rate_hourly", 0.000015))
                    for c in info.get("candles", []):
                        ts_c = int(c["t"])
                        if ts_c <= end_ms:
                            bf_rows.append(
                                (
                                    sym,
                                    "4h",
                                    ts_c,
                                    float(c["o"]),
                                    float(c["h"]),
                                    float(c["l"]),
                                    float(c["c"]),
                                    float(c["v"]),
                                    fr,
                                    0,
                                )
                            )
                if bf_rows:
                    self.mem_db.upsert_ohlcv_batch(bf_rows)
            except Exception as exc:
                logger.warning("Could not load real_4h_backfill.json: %s", exc)

        self.mem_db.set_config_value("initial_equity_usdc", self.initial_equity)
        self.mem_db.set_config_value("execution_mode", "PAPER")
        for cfg_k, cfg_v in self.config_overrides.items():
            self.mem_db.set_config_value(str(cfg_k), cfg_v)

        # Ensure all 16 USDC_PERP and 15 BTC_SYNTH series have >= 120 4h warmup bars before start_ms
        self._ensure_warmup_and_timeframes(start_ms, end_ms)

        # Reset trades and portfolio_snapshots in :memory: clone for clean backtest replay
        with self.mem_db.connection() as m_conn:
            m_conn.execute("DELETE FROM market_ohlcv WHERE timestamp_ms > ?;", (int(end_ms),))
            m_conn.execute("DELETE FROM trades;")
            m_conn.execute("DELETE FROM signal_evaluations;")
            m_conn.execute("DELETE FROM portfolio_snapshots;")
            m_conn.execute(
                """
                INSERT INTO portfolio_snapshots (
                    timestamp_ms, total_equity_usdc, available_cash_usdc,
                    deployed_notional_usdc, gross_leverage,
                    portfolio_heat_usdc, portfolio_heat_pct,
                    basis_harvest_notional_usdc, accrued_yield_usdc,
                    hmm_regime, prob_bull, prob_bear, prob_chop,
                    open_long_count, open_short_count
                ) VALUES (?, ?, ?, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 'CHOP', 0.3333, 0.3333, 0.3334, 0, 0);
                """,
                (start_ms - 14_400_000, self.initial_equity, self.initial_equity),
            )

    def _ensure_warmup_and_timeframes(self, start_ms: int, end_ms: int) -> None:
        """
        Ensure every USDC_PERP and BTC_SYNTH symbol has causal pre-window warmup bars prior to
        `start_ms` and consistent 4h/1h series across `[start_ms - warmup, end_ms]`.
        """
        tf_4h_ms = 14_400_000
        warmup_bars = 140
        required_min_ts = ((start_ms // tf_4h_ms) - warmup_bars) * tf_4h_ms
        query_limit = max(2500, int((end_ms - required_min_ts) // tf_4h_ms) + 200)
        usdc_universe = self.mem_db.get_usdc_perp_universe()
        alt_pairs = self.mem_db.get_alt_pairs_universe()
        benchmark_symbol = "BTC/USDC:USDC"

        for row in usdc_universe:
            sym = str(row["symbol"])
            df_4h = self.mem_db.get_ohlcv_df(sym, "4h", limit=query_limit, max_timestamp_ms=end_ms)
            if df_4h.empty:
                gen_rows = [
                    r
                    for r in MarketIngestionEngine._generate_synthetic_usdc_series(sym, "4h", limit=750)
                    if int(r[2]) <= end_ms
                ]
                if not gen_rows:
                    base_px = BASE_PRICES_USDC.get(sym, 10.0)
                    gen_rows = [
                        (sym, "4h", int(end_ms), base_px, base_px * 1.002, base_px * 0.998, base_px, 1000.0, 0.00001, 1)
                    ]
                logger.warning(
                    "Missing 4h candle history for %s; inserting %d synthetic fallback bars.",
                    sym,
                    len(gen_rows),
                )
                self.mem_db.upsert_ohlcv_batch(gen_rows)
                df_4h = self.mem_db.get_ohlcv_df(sym, "4h", limit=query_limit, max_timestamp_ms=end_ms)

            first_ts = int(df_4h["timestamp_ms"].iloc[0])
            if first_ts > required_min_ts:
                first_open = float(df_4h["open"].iloc[0])
                funding = float(df_4h["funding_rate_hourly"].iloc[0])
                n_missing = int((first_ts - required_min_ts) // tf_4h_ms)
                logger.warning(
                    "Insufficient pre-window warmup data for %s (first_ts=%d > required_min_ts=%d); "
                    "inserting %d synthetic warmup bars.",
                    sym,
                    first_ts,
                    required_min_ts,
                    n_missing,
                )
                seed_int = int(hashlib.sha256(f"{sym}:warmup".encode()).hexdigest()[:8], 16)
                rng = np.random.default_rng(seed_int)
                vol_scale = 0.0075 if sym.startswith("BTC/") else 0.012
                asset_tilt = ((seed_int % 21) - 10) * 0.00008
                drift = -0.00055 + asset_tilt
                rets = drift + rng.normal(0.0, vol_scale, size=n_missing)

                # Anchor end of warmup exactly to `first_open`
                rev_cum = np.cumsum(rets[::-1])[::-1]
                closes = first_open * np.exp(-rev_cum)
                opens = np.roll(closes, 1)
                opens[0] = closes[0] * math.exp(-rets[0])
                spread = np.abs(rng.normal(0.0035, 0.0015, size=n_missing))
                highs = np.maximum(opens, closes) * (1.0 + spread)
                lows = np.minimum(opens, closes) * np.maximum(0.0001, 1.0 - spread)
                base_px = BASE_PRICES_USDC.get(sym, 10.0)
                vols = rng.uniform(500.0, 4000.0, size=n_missing) * (1000.0 / max(base_px, 0.01))

                pre_rows = [
                    (
                        sym,
                        "4h",
                        int(required_min_ts + idx * tf_4h_ms),
                        float(opens[idx]),
                        float(highs[idx]),
                        float(lows[idx]),
                        float(closes[idx]),
                        float(vols[idx]),
                        funding,
                        1,
                    )
                    for idx in range(n_missing)
                ]
                self.mem_db.upsert_ohlcv_batch(pre_rows)

        # Synthesize full 4h ALT/BTC:SYNTH cross-pairs across the complete warmup + test window
        btc_4h = self.mem_db.get_ohlcv_df(benchmark_symbol, "4h", limit=query_limit, max_timestamp_ms=end_ms)
        for pair in alt_pairs:
            exec_sym = str(pair["exec_symbol"])
            synth_sym = str(pair["synth_symbol"])
            alt_4h = self.mem_db.get_ohlcv_df(exec_sym, "4h", limit=query_limit, max_timestamp_ms=end_ms)
            synth_rows = MarketIngestionEngine.synthesize_alt_btc_candles(
                alt_df=alt_4h,
                btc_df=btc_4h,
                synth_symbol=synth_sym,
                timeframe="4h",
            )
            self.mem_db.upsert_ohlcv_batch(synth_rows)

        # Align universe execution timeframe to 4h for consistent 90-day point-in-time replay
        with self.mem_db.connection() as m_conn:
            m_conn.execute("UPDATE universe_assets SET exec_timeframe = '4h', rs_timeframe = '4h';")

    def _precompute_causal_pair_tables(
        self,
        alt_pairs: list[dict[str, Any]],
        full_exec_dfs: dict[str, pd.DataFrame],
        full_synth_dfs: dict[str, pd.DataFrame],
        cfg: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """
        Precompute strictly backward-looking causal feature arrays (SNR, MACE basis, ATR)
        for each asset so point-in-time slicing at index `pos` has zero future dependency
        and O(1) lookup latency.
        """
        fast_span = int(cfg.get("snr_fast_span", 16))
        slow_span = int(cfg.get("snr_slow_span", 64))
        w_synth = float(np.clip(cfg.get("snr_synth_weight", 0.60), 0.0, 1.0))
        tanh_scale = float(cfg.get("mace_tanh_scale", 1.25))
        k = max(1, int(cfg.get("mace_forward_horizon_bars", 4)))

        tables: dict[str, dict[str, Any]] = {}
        for pair in alt_pairs:
            exec_sym = str(pair["exec_symbol"])
            synth_sym = str(pair["synth_symbol"])
            e_df = full_exec_dfs[exec_sym]
            s_df = full_synth_dfs[synth_sym]
            if len(s_df) != len(e_df) or not np.array_equal(
                s_df["timestamp_ms"].to_numpy(), e_df["timestamp_ms"].to_numpy()
            ):
                if s_df.empty:
                    s_df = e_df.copy()
                else:
                    s_indexed = s_df.drop_duplicates(subset=["timestamp_ms"], keep="last").set_index("timestamp_ms")
                    s_aligned = s_indexed.reindex(e_df["timestamp_ms"].to_numpy()).ffill().bfill()
                    s_aligned["timestamp_ms"] = e_df["timestamp_ms"].to_numpy()
                    if "timestamp_utc" in e_df.columns:
                        s_aligned["timestamp_utc"] = e_df["timestamp_utc"].to_numpy()
                    s_df = s_aligned.reset_index(drop=True)

            snr_exec = SignalEngine._compute_causal_snr_series(
                e_df["close"].to_numpy(dtype=np.float64),
                fast_span=fast_span,
                slow_span=slow_span,
                tanh_scale=tanh_scale,
            )
            snr_synth = SignalEngine._compute_causal_snr_series(
                s_df["close"].to_numpy(dtype=np.float64),
                fast_span=fast_span,
                slow_span=slow_span,
                tanh_scale=tanh_scale,
            )
            snr_blended = np.clip(w_synth * snr_synth + (1.0 - w_synth) * snr_exec, -1.0, 1.0)

            features_df, target_s = SignalEngine._build_mace_features_and_target(
                exec_df=e_df, synth_df=s_df, forward_horizon_bars=k
            )
            ts_arr = e_df["timestamp_ms"].to_numpy(dtype=np.int64)
            ts_to_pos = {int(ts): idx for idx, ts in enumerate(ts_arr)}

            s_closes = np.maximum(s_df["close"].to_numpy(dtype=np.float64), 1e-12)
            s_log = np.log(s_closes)
            s_log_lag96 = np.array(
                [s_log[max(0, idx - 96)] for idx in range(len(s_log))],
                dtype=np.float64,
            )
            rs_96_series = s_log - s_log_lag96

            tables[exec_sym] = {
                "snr_series": snr_blended,
                "snr_synth_series": snr_synth,
                "rs_96_series": rs_96_series,
                "features_raw": features_df.to_numpy(dtype=np.float64),
                "target_raw": target_s.to_numpy(dtype=np.float64),
                "funding_series": e_df["funding_rate_hourly"].to_numpy(dtype=np.float64),
                "ts_to_pos": ts_to_pos,
            }
        return tables

    def _evaluate_pair_signal_pit(
        self,
        exec_sym: str,
        pair_table: dict[str, Any],
        pos_idx: int,
        step_idx: int,
        cfg: dict[str, Any],
    ) -> tuple[float, float, float]:
        """
        Compute point-in-time (snr_score, mace_score, blended_conviction) at bar `pos_idx`
        using purged training slice `[warmup : pos_idx + 1 - k]` and strictly single-bar
        OOS inference at `pos_idx` (never accessing any index > `pos_idx`).
        """
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.linear_model import Ridge

        tanh_scale = float(cfg.get("mace_tanh_scale", 1.25))
        min_ic_floor = float(cfg.get("mace_min_ic_floor", 0.05))
        k = max(1, int(cfg.get("mace_forward_horizon_bars", 4)))
        warmup = 32
        window_limit = int(cfg.get("hmm_lookback_bars", cfg.get("hmm_window_bars", 540)))

        snr_score = float(pair_table["snr_series"][pos_idx])
        feats_all = pair_table["features_raw"]
        targ_all = pair_table["target_raw"]

        start_i = max(0, pos_idx + 1 - window_limit) + warmup
        end_train_i = pos_idx + 1 - k
        if end_train_i - start_i < 25:
            return snr_score, 0.0, snr_score

        refit_interval = int(cfg.get("mace_refit_interval_bars", 36))
        cached = self._mace_model_cache.get(exec_sym)
        if cached is None or (step_idx - int(cached["fit_step"])) >= refit_interval:
            X_train_raw = feats_all[start_i:end_train_i]
            y_train_raw = targ_all[start_i:end_train_i]
            valid = np.isfinite(y_train_raw)
            X_train_raw = X_train_raw[valid]
            y_train_raw = y_train_raw[valid]

            X_mean = np.mean(X_train_raw, axis=0)
            X_center = np.zeros_like(X_mean)
            X_center[-1] = X_mean[-1]
            X_std = np.maximum(np.std(X_train_raw, axis=0), 1e-6)
            X_norm = np.clip((X_train_raw - X_center) / X_std, -3.0, 3.0)
            X_train = np.hstack([X_norm, np.tanh(X_norm), X_norm ** 2 - 1.0])

            ret_std = max(float(np.std(y_train_raw)), 1e-6)
            y_train = np.clip(y_train_raw / ret_std, -3.0, 3.0)

            rf = RandomForestRegressor(
                n_estimators=int(cfg.get("mace_rf_n_estimators", 60)),
                max_depth=int(cfg.get("mace_rf_max_depth", 4)),
                min_samples_leaf=int(cfg.get("mace_rf_min_samples_leaf", 10)),
                random_state=42,
                n_jobs=1,
            )
            ridge = Ridge(alpha=float(cfg.get("mace_ridge_alpha", 1.0)), random_state=42)
            rf.fit(X_train, y_train)
            ridge.fit(X_train, y_train)

            eval_tail = min(len(X_train), 80)
            mace_preds = 0.55 * rf.predict(X_train[-eval_tail:]) + 0.45 * ridge.predict(X_train[-eval_tail:])
            snr_slice = pair_table["snr_series"][start_i:end_train_i][valid][-eval_tail:]
            ic_mace = SignalEngine._safe_spearman_ic(mace_preds, y_train[-eval_tail:])
            ic_snr = SignalEngine._safe_spearman_ic(snr_slice, y_train[-eval_tail:])
            mace_weight = SignalEngine.compute_dynamic_mace_weight(
                ic_mace=ic_mace, ic_snr=ic_snr, min_ic_floor=min_ic_floor
            )

            cached = {
                "fit_step": step_idx,
                "rf_trees": [est.tree_ for est in rf.estimators_],
                "ridge_coef": np.asarray(ridge.coef_, dtype=np.float64).reshape(-1),
                "ridge_intercept": float(np.asarray(ridge.intercept_).reshape(-1)[0]),
                "X_center": X_center,
                "X_std": X_std,
                "mace_weight": mace_weight,
            }
            self._mace_model_cache[exec_sym] = cached

        # Strictly single-bar out-of-sample inference at pos_idx (zero future row access)
        x_latest_raw = feats_all[[pos_idx]]
        x_latest_norm = np.clip((x_latest_raw - cached["X_center"]) / cached["X_std"], -3.0, 3.0)
        x_latest = np.hstack([x_latest_norm, np.tanh(x_latest_norm), x_latest_norm ** 2 - 1.0])
        x_latest_f32 = np.ascontiguousarray(x_latest, dtype=np.float32)
        pred_rf = float(np.mean([t.predict(x_latest_f32)[0, 0] for t in cached["rf_trees"]]))
        pred_ridge = float(x_latest[0] @ cached["ridge_coef"] + cached["ridge_intercept"])
        mace_score = float(np.tanh((0.55 * pred_rf + 0.45 * pred_ridge) * tanh_scale))

        mace_weight = float(cached["mace_weight"])
        snr_weight = 1.0 - mace_weight
        blended = float(np.clip(mace_weight * mace_score + snr_weight * snr_score, -1.0, 1.0))
        return snr_score, mace_score, blended

    def run_backtest(
        self,
        start_date: str = "2026-07-20",
        end_date: str = "2026-10-20",
    ) -> BacktestTearSheet:
        """
        Execute point-in-time event-driven backtest across [start_date, end_date]
        and return the complete BacktestTearSheet.
        """
        start_ms = self._parse_date_utc_ms(start_date, end_of_day=False)
        end_ms = self._parse_date_utc_ms(end_date, end_of_day=True)

        self._clone_to_memory_db(start_ms, end_ms)
        cfg = self.mem_db.load_config()
        benchmark_symbol = str(cfg.get("benchmark_symbol", "BTC/USDC:USDC"))
        window_limit = int(cfg.get("hmm_lookback_bars", cfg.get("hmm_window_bars", 540)))
        max_longs = int(cfg.get("max_concurrent_long_positions", 2))
        max_shorts = int(cfg.get("max_concurrent_short_positions", 2))
        short_funding_floor = float(cfg.get("short_max_adverse_funding_rate_hourly", -0.0005))
        basis_enabled = bool(cfg.get("basis_harvest_enabled", True))
        basis_min_rate = float(cfg.get("basis_min_hourly_funding_rate", 0.000015))
        basis_max_alloc = float(cfg.get("basis_max_idle_cash_allocation_pct", 0.50))
        maker_fee_rate = float(cfg.get("maker_fee_bps", 1.5)) / 10000.0
        taker_fee_rate = float(cfg.get("taker_fee_bps", 4.5)) / 10000.0
        atr_period = int(cfg.get("atr_period", 14))
        long_atr_mult = float(cfg.get("chandelier_atr_multiplier_long", 3.0))
        short_atr_mult = float(cfg.get("chandelier_atr_multiplier_short", 3.8))
        min_atr_pct = float(cfg.get("chandelier_min_atr_pct", 0.022))
        cooldown_bars = int(cfg.get("post_stop_cooldown_bars", 14))
        entry_slippage_rate = float(cfg.get("entry_exit_slippage_bps", 3.0)) / 10000.0

        alt_pairs = self.mem_db.get_alt_pairs_universe()
        alt_pairs_by_sym = {str(p["exec_symbol"]): p for p in alt_pairs}

        # Pre-load chronological DataFrames bounded by <= end_ms from :memory: clone
        self.mem_db.pit_timestamp_ms = int(end_ms)
        query_limit = max(2500, int((end_ms - start_ms) // 14_400_000) + 400)
        full_btc_df = self.mem_db.get_ohlcv_df(benchmark_symbol, "4h", limit=query_limit, max_timestamp_ms=end_ms)
        full_exec_dfs = {
            str(p["exec_symbol"]): self.mem_db.get_ohlcv_df(
                str(p["exec_symbol"]), "4h", limit=query_limit, max_timestamp_ms=end_ms
            )
            for p in alt_pairs
        }
        full_synth_dfs = {
            str(p["synth_symbol"]): self.mem_db.get_ohlcv_df(
                str(p["synth_symbol"]), "4h", limit=query_limit, max_timestamp_ms=end_ms
            )
            for p in alt_pairs
        }

        # Pre-align causal 1-bar log returns per altcoin (row t depends strictly on close[t] / close[t-1]);
        # SVD principal component weights are computed strictly point-in-time on [btc_start : btc_pos + 1]
        btc_ts_arr = full_btc_df["timestamp_ms"].to_numpy(dtype=np.int64)
        alt_ret_cols: list[np.ndarray] = []
        for adf in full_exec_dfs.values():
            clean_c = SignalEngine._sanitize_positive_series(adf["close"], fallback=1.0)
            s_c = pd.Series(clean_c.to_numpy(dtype=np.float64), index=adf["timestamp_ms"].to_numpy(dtype=np.int64))
            s_c = s_c[~s_c.index.duplicated(keep="last")]
            c_aligned = s_c.reindex(btc_ts_arr).ffill().bfill().fillna(1.0).to_numpy(dtype=np.float64)
            c_safe = np.maximum(c_aligned, 1e-12)
            r_col = np.nan_to_num(
                np.diff(np.log(c_safe), prepend=np.log(c_safe[0])),
                nan=0.0,
                posinf=0.0,
                neginf=0.0,
            )
            alt_ret_cols.append(r_col)
        causal_alt_ret_matrix = np.column_stack(alt_ret_cols)

        pair_tables = self._precompute_causal_pair_tables(
            alt_pairs, full_exec_dfs, full_synth_dfs, cfg
        )
        btc_snr_series = SignalEngine._compute_causal_snr_series(
            full_btc_df["close"].to_numpy(dtype=np.float64),
            fast_span=int(cfg.get("snr_fast_span", 16)),
            slow_span=int(cfg.get("snr_slow_span", 64)),
            tanh_scale=float(cfg.get("mace_tanh_scale", 1.25)),
        )
        btc_snr_macro_series = SignalEngine._compute_causal_snr_series(
            full_btc_df["close"].to_numpy(dtype=np.float64),
            fast_span=48,
            slow_span=192,
            tanh_scale=float(cfg.get("mace_tanh_scale", 1.25)),
        )
        btc_closes_all = np.maximum(full_btc_df["close"].to_numpy(dtype=np.float64), 1e-12)
        btc_log_all = np.log(btc_closes_all)
        btc_ret_30_series = np.array(
            [btc_log_all[idx] - btc_log_all[max(0, idx - 30)] for idx in range(len(btc_log_all))],
            dtype=np.float64,
        )
        btc_ret_60_series = np.array(
            [btc_log_all[idx] - btc_log_all[max(0, idx - 60)] for idx in range(len(btc_log_all))],
            dtype=np.float64,
        )
        btc_ret_180_series = np.array(
            [btc_log_all[idx] - btc_log_all[max(0, idx - 180)] for idx in range(len(btc_log_all))],
            dtype=np.float64,
        )
        btc_ts_to_pos = {
            int(ts): idx for idx, ts in enumerate(btc_ts_arr)
        }

        replay_mask = (full_btc_df["timestamp_ms"] >= start_ms) & (full_btc_df["timestamp_ms"] <= end_ms)
        replay_timestamps = full_btc_df.loc[replay_mask, "timestamp_ms"].to_numpy(dtype=np.int64)

        realized_pnl_total = 0.0
        accrued_basis_yield = 0.0
        total_fees_paid = 0.0
        total_slippage_usdc = 0.0
        wick_stop_outs = 0
        total_funding_net = 0.0
        bull_pnl_usdc = 0.0
        bear_pnl_usdc = 0.0
        chop_pnl_usdc = 0.0
        bull_bars = 0
        bear_bars = 0
        chop_bars = 0

        open_positions: dict[str, dict[str, Any]] = {}
        cooldown_until_step: dict[str, int] = {}
        closed_trades_ledger: list[dict[str, Any]] = []
        equity_curve: list[dict[str, Any]] = []

        for step_idx, step_ts in enumerate(replay_timestamps):
            ts_int = int(step_ts)
            self.mem_db.pit_timestamp_ms = ts_int

            # 1. Point-in-time slice strictly <= ts_int (including point-in-time SVD for alt_pc1_ret)
            btc_pos = btc_ts_to_pos[ts_int]
            btc_start = max(0, btc_pos + 1 - window_limit)
            btc_pit = full_btc_df.iloc[btc_start : btc_pos + 1]
            R_pit = causal_alt_ret_matrix[btc_start : btc_pos + 1]
            try:
                _, _, Vt = np.linalg.svd(R_pit, full_matrices=False)
                w_pc1 = np.abs(Vt[0])
                w_sum = float(np.sum(w_pc1))
                w_pc1 = (
                    w_pc1 / w_sum
                    if (w_sum > 1e-12 and np.isfinite(w_sum))
                    else np.full(R_pit.shape[1], 1.0 / R_pit.shape[1], dtype=np.float64)
                )
                pc1_vals = R_pit @ w_pc1
            except Exception:
                pc1_vals = np.mean(R_pit, axis=1)
            pc1_pit = pd.Series(pc1_vals, index=btc_pit.index, dtype=np.float64)

            regime, prob_bull, prob_bear, prob_chop = self.signals.decode_hmm_regime_states(
                btc_pit, pc1_pit, cfg
            )

            if regime == "BULL":
                bull_bars += 1
            elif regime == "BEAR":
                bear_bars += 1
            else:
                chop_bars += 1

            # 2. Point-in-time universe signal evaluation across all 15 pairs
            step_evals: list[dict[str, Any]] = []
            exec_pit_map: dict[str, pd.DataFrame] = {}

            for pair in alt_pairs:
                base_asset = str(pair["base_asset"])
                exec_sym = str(pair["exec_symbol"])
                synth_sym = str(pair["synth_symbol"])

                ptab = pair_tables[exec_sym]
                pos_idx = ptab["ts_to_pos"][ts_int]
                e_df = full_exec_dfs[exec_sym]
                exec_pit_map[exec_sym] = e_df.iloc[max(0, pos_idx + 1 - 80) : pos_idx + 1]

                snr_score, mace_score, blended = self._evaluate_pair_signal_pit(
                    exec_sym=exec_sym,
                    pair_table=ptab,
                    pos_idx=pos_idx,
                    step_idx=step_idx,
                    cfg=cfg,
                )
                snr_synth_val = float(ptab["snr_synth_series"][pos_idx])
                rs_96_val = float(ptab["rs_96_series"][pos_idx])
                r_usdc_8_val = float(ptab["features_raw"][pos_idx, 3])
                funding_hr = float(ptab["funding_series"][pos_idx])

                step_evals.append(
                    {
                        "timestamp_ms": ts_int,
                        "base_asset": base_asset,
                        "exec_symbol": exec_sym,
                        "synth_symbol": synth_sym,
                        "hmm_regime": regime,
                        "prob_bull": round(prob_bull, 4),
                        "prob_bear": round(prob_bear, 4),
                        "prob_chop": round(prob_chop, 4),
                        "snr_score": round(snr_score, 4),
                        "snr_synth": round(snr_synth_val, 4),
                        "rs_96": round(rs_96_val, 5),
                        "r_usdc_8": round(r_usdc_8_val, 5),
                        "mace_score": round(mace_score, 4),
                        "blended_conviction": round(blended, 4),
                        "funding_rate_hourly": round(funding_hr, 6),
                    }
                )

            btc_macro_pit = {
                "btc_snr": float(btc_snr_series[btc_pos]),
                "btc_snr_macro": float(btc_snr_macro_series[btc_pos]),
                "btc_ret_30": float(btc_ret_30_series[btc_pos]),
                "btc_ret_60": float(btc_ret_60_series[btc_pos]),
                "btc_ret_180": float(btc_ret_180_series[btc_pos]),
            }
            step_evals.sort(key=lambda x: x["blended_conviction"], reverse=True)
            total_assets = len(step_evals)
            eval_by_sym: dict[str, dict[str, Any]] = {}
            for idx, item in enumerate(step_evals):
                rank = idx + 1
                item["rs_rank"] = rank
                item["signal_action"] = SignalEngine.classify_signal_action(
                    regime=regime,
                    prob_bull=round(prob_bull, 4),
                    prob_bear=round(prob_bear, 4),
                    rs_rank=rank,
                    total_assets=total_assets,
                    blended_conviction=float(item["blended_conviction"]),
                    snr_synth=float(item["snr_synth"]),
                    mace_score=float(item["mace_score"]),
                    rs_96=float(item["rs_96"]),
                    r_usdc_8=float(item["r_usdc_8"]),
                    funding_rate_hourly=float(item["funding_rate_hourly"]),
                    btc_macro=btc_macro_pit,
                    cfg=cfg,
                )
                eval_by_sym[item["exec_symbol"]] = item

            # 3. Reconcile open positions, 1H funding carry (4 hours per 4H bar), and Two-Tier Intra-Bar Wick Chandelier stops
            eff_long_mult = long_atr_mult
            eff_short_mult = short_atr_mult

            for sym in list(open_positions.keys()):
                pos = open_positions[sym]
                e_pit = exec_pit_map.get(sym)
                if e_pit is None or e_pit.empty:
                    continue

                bar_open = float(e_pit["open"].iloc[-1])
                bar_high = float(e_pit["high"].iloc[-1])
                bar_low = float(e_pit["low"].iloc[-1])
                current_price = float(e_pit["close"].iloc[-1])
                funding_hr = float(e_pit["funding_rate_hourly"].iloc[-1])
                atr_val = self.risk.compute_atr(e_pit.iloc[-60:], period=atr_period)

                side = str(pos["side"])
                entry_price = float(pos["entry_price"])
                qty = float(pos["quantity"])
                notional = qty * current_price
                pos["bars_held"] = int(pos.get("bars_held", 0)) + 1

                # 4 hours of 1H funding settlement per 4H bar
                if side == "LONG":
                    funding_step = -notional * funding_hr * 4.0
                else:
                    funding_step = +notional * funding_hr * 4.0
                pos["funding_accrued_usdc"] = float(pos["funding_accrued_usdc"]) + funding_step
                total_funding_net += funding_step

                ev_sym = eval_by_sym.get(sym, {})
                cur_snr_s = float(ev_sym.get("snr_synth", 0.0))

                stop_eval = self.risk.evaluate_two_tier_chandelier_stop(
                    side=side,
                    entry_price=entry_price,
                    active_stop=float(pos["chandelier_stop_price"]),
                    highest_price_seen=float(pos["highest_price_seen"]),
                    lowest_price_seen=float(pos["lowest_price_seen"]),
                    bar_open=bar_open,
                    bar_high=bar_high,
                    bar_low=bar_low,
                    bar_close=current_price,
                    atr_value=atr_val,
                    quantity=qty,
                    config=cfg,
                )
                pos["highest_price_seen"] = stop_eval.highest_price_seen
                pos["lowest_price_seen"] = stop_eval.lowest_price_seen
                pos["chandelier_stop_price"] = stop_eval.chandelier_stop_price
                if stop_eval.is_wick_stop:
                    wick_stop_outs += 1

                exit_reason: str | None = stop_eval.exit_reason
                exit_fill_price = stop_eval.exit_fill_price
                exit_slippage_usdc = stop_eval.exit_slippage_usdc

                if side == "LONG":
                    if exit_reason is None:
                        if regime == "BEAR":
                            exit_reason = "HMM_REGIME_FLIP_TO_BEAR"
                            exit_fill_price = current_price * (1.0 - entry_slippage_rate)
                            exit_slippage_usdc = current_price * entry_slippage_rate * qty
                        elif cur_snr_s < -0.10 and pos["bars_held"] >= 8:
                            exit_reason = "RS_SYNTH_BREAKDOWN_LONG"
                            exit_fill_price = current_price * (1.0 - entry_slippage_rate)
                            exit_slippage_usdc = current_price * entry_slippage_rate * qty
                    unrealized = (exit_fill_price - entry_price) * qty + float(pos["funding_accrued_usdc"])
                    pos["unrealized_pnl_usdc"] = unrealized
                else:
                    if exit_reason is None:
                        if funding_hr < short_funding_floor:
                            exit_reason = "ADVERSE_1H_FUNDING_CARRY_PROTECTION"
                            exit_fill_price = current_price * (1.0 + entry_slippage_rate)
                            exit_slippage_usdc = current_price * entry_slippage_rate * qty
                        elif regime == "BULL":
                            exit_reason = "HMM_REGIME_FLIP_TO_BULL"
                            exit_fill_price = current_price * (1.0 + entry_slippage_rate)
                            exit_slippage_usdc = current_price * entry_slippage_rate * qty
                        elif cur_snr_s > 0.10 and pos["bars_held"] >= 8:
                            exit_reason = "RS_SYNTH_REVERSAL_SHORT"
                            exit_fill_price = current_price * (1.0 + entry_slippage_rate)
                            exit_slippage_usdc = current_price * entry_slippage_rate * qty
                    unrealized = (entry_price - exit_fill_price) * qty + float(pos["funding_accrued_usdc"])
                    pos["unrealized_pnl_usdc"] = unrealized

                if exit_reason is not None:
                    exit_notional = qty * exit_fill_price
                    exit_fee = exit_notional * taker_fee_rate
                    total_trade_fees = float(pos["fees_paid_usdc"]) + exit_fee
                    total_fees_paid += exit_fee
                    total_slippage_usdc += exit_slippage_usdc
                    net_realized = float(pos["unrealized_pnl_usdc"]) - total_trade_fees
                    realized_pnl_total += net_realized

                    if pos.get("entry_regime") == "BULL":
                        bull_pnl_usdc += net_realized
                    elif pos.get("entry_regime") == "BEAR":
                        bear_pnl_usdc += net_realized
                    else:
                        chop_pnl_usdc += net_realized

                    self.mem_db.close_trade(
                        trade_id=int(pos["trade_id"]),
                        exit_price=exit_fill_price,
                        realized_pnl_usdc=round(net_realized, 4),
                        fees_paid_usdc=round(total_trade_fees, 4),
                        exit_reason=exit_reason,
                    )
                    closed_trades_ledger.append(
                        {
                            **pos,
                            "exit_price": exit_fill_price,
                            "realized_pnl_usdc": net_realized,
                            "fees_paid_usdc": total_trade_fees,
                            "exit_reason": exit_reason,
                            "closed_ts_ms": ts_int,
                        }
                    )
                    del open_positions[sym]
                    cooldown_until_step[sym] = step_idx + cooldown_bars

            # 4. Calculate current point-in-time portfolio equity
            unrealized_total = sum(float(p["unrealized_pnl_usdc"]) for p in open_positions.values())
            current_equity = self.initial_equity + realized_pnl_total + unrealized_total + accrued_basis_yield

            # 5. Open new Long Leaders (Bull) or Short Laggards (Bear) within risk caps
            open_longs = [p for p in open_positions.values() if p["side"] == "LONG"]
            open_shorts = [p for p in open_positions.values() if p["side"] == "SHORT"]

            for ev in step_evals:
                action = str(ev["signal_action"])
                sym = str(ev["exec_symbol"])
                if action not in ("LONG", "SHORT") or sym in open_positions:
                    continue
                if step_idx < cooldown_until_step.get(sym, -1):
                    continue
                if action == "LONG" and len(open_longs) >= max_longs:
                    continue
                if action == "SHORT" and len(open_shorts) >= max_shorts:
                    continue

                pair_meta = alt_pairs_by_sym.get(sym)
                if not pair_meta:
                    continue
                e_pit = exec_pit_map[sym]
                sizing: PositionSizeResult = self.risk.evaluate_order_risk(
                    pair_meta=pair_meta,
                    side=action,
                    blended_conviction=float(ev["blended_conviction"]),
                    exec_df=e_pit.iloc[-80:],
                    open_trades=list(open_positions.values()),
                    current_equity_usdc=current_equity,
                    config=cfg,
                )
                if not sizing.approved:
                    continue

                eff_entry_atr = max(sizing.atr_value, sizing.entry_price * min_atr_pct)
                if action == "LONG":
                    fill_entry_px = sizing.entry_price * (1.0 + entry_slippage_rate)
                    init_stop = fill_entry_px - eff_long_mult * eff_entry_atr
                else:
                    fill_entry_px = sizing.entry_price * (1.0 - entry_slippage_rate)
                    init_stop = fill_entry_px + eff_short_mult * eff_entry_atr

                entry_slip_cost = abs(fill_entry_px - sizing.entry_price) * sizing.quantity
                total_slippage_usdc += entry_slip_cost
                entry_fee = round(sizing.notional_usdc * maker_fee_rate, 4)
                total_fees_paid += entry_fee
                trade_record = {
                    "symbol": sym,
                    "base_asset": str(pair_meta["base_asset"]),
                    "side": action,
                    "entry_price": fill_entry_px,
                    "current_price": fill_entry_px,
                    "quantity": sizing.quantity,
                    "notional_usdc": sizing.notional_usdc,
                    "risk_budget_usdc": sizing.risk_budget_usdc,
                    "conviction_multiplier": sizing.conviction_multiplier,
                    "initial_stop_price": init_stop,
                    "chandelier_stop_price": init_stop,
                    "highest_price_seen": fill_entry_px,
                    "lowest_price_seen": fill_entry_px,
                    "fees_paid_usdc": entry_fee,
                    "funding_accrued_usdc": 0.0,
                    "unrealized_pnl_usdc": 0.0,
                    "exchange_order_id": f"hl-bt-{ts_int}-{pair_meta['base_asset']}",
                    "entry_regime": regime,
                    "bars_held": 0,
                }
                trade_id = self.mem_db.insert_trade(trade_record)
                trade_record["trade_id"] = trade_id
                open_positions[sym] = trade_record
                if action == "LONG":
                    open_longs.append(trade_record)
                else:
                    open_shorts.append(trade_record)

            # 6. Idle-Cash Delta-Neutral Basis & HLP Yield Harvesting (4 hours per 4H bar)
            deployed_notional = sum(float(p["notional_usdc"]) for p in open_positions.values())
            portfolio_heat_usdc = sum(float(p["risk_budget_usdc"]) for p in open_positions.values())
            available_cash = max(0.0, current_equity - deployed_notional)
            best_funding = max(
                (float(e.get("funding_rate_hourly", 0.0)) for e in step_evals),
                default=0.0,
            )
            enable_hlp_floor = bool(cfg.get("enable_hlp_floor_backtest", False))
            basis_notional = 0.0
            if basis_enabled and available_cash > 100.0 and (
                best_funding >= basis_min_rate
                or (enable_hlp_floor and bool(cfg.get("hlp_yield_fallback_enabled", True)))
            ):
                basis_notional = round(available_cash * basis_max_alloc, 2)
                eff_hourly_rate = (
                    max(best_funding, basis_min_rate)
                    if enable_hlp_floor
                    else max(0.0, best_funding)
                )
                step_yield = basis_notional * eff_hourly_rate * 4.0
                accrued_basis_yield += step_yield
                chop_pnl_usdc += step_yield

            current_equity = round(
                self.initial_equity + realized_pnl_total + unrealized_total + accrued_basis_yield,
                4,
            )
            snapshot = {
                "timestamp_ms": ts_int,
                "total_equity_usdc": round(current_equity, 2),
                "available_cash_usdc": round(available_cash, 2),
                "deployed_notional_usdc": round(deployed_notional, 2),
                "gross_leverage": round(deployed_notional / max(current_equity, 1.0), 4),
                "portfolio_heat_usdc": round(portfolio_heat_usdc, 2),
                "portfolio_heat_pct": round(portfolio_heat_usdc / max(current_equity, 1.0), 4),
                "basis_harvest_notional_usdc": basis_notional,
                "accrued_yield_usdc": round(accrued_basis_yield, 4),
                "hmm_regime": regime,
                "prob_bull": round(prob_bull, 4),
                "prob_bear": round(prob_bear, 4),
                "prob_chop": round(prob_chop, 4),
                "open_long_count": len(open_longs),
                "open_short_count": len(open_shorts),
            }
            self.mem_db.save_portfolio_snapshot(snapshot)
            equity_curve.append(snapshot)

        # Settle any remaining open positions at end of replay window for complete trade attribution
        for sym, pos in list(open_positions.items()):
            e_pit = exec_pit_map.get(sym)
            if e_pit is None or e_pit.empty:
                continue
            close_px = float(e_pit["close"].iloc[-1])
            qty = float(pos["quantity"])
            exit_fee = qty * close_px * taker_fee_rate
            total_trade_fees = float(pos["fees_paid_usdc"]) + exit_fee
            total_fees_paid += exit_fee
            net_realized = float(pos["unrealized_pnl_usdc"]) - total_trade_fees
            realized_pnl_total += net_realized
            if pos.get("entry_regime") == "BULL":
                bull_pnl_usdc += net_realized
            elif pos.get("entry_regime") == "BEAR":
                bear_pnl_usdc += net_realized
            else:
                chop_pnl_usdc += net_realized
            closed_trades_ledger.append(
                {
                    **pos,
                    "exit_price": close_px,
                    "realized_pnl_usdc": net_realized,
                    "fees_paid_usdc": total_trade_fees,
                    "exit_reason": "END_OF_BACKTEST_MARK_TO_MARKET",
                }
            )
            del open_positions[sym]

        final_equity = round(self.initial_equity + realized_pnl_total + accrued_basis_yield, 2)
        if equity_curve:
            equity_curve[-1]["total_equity_usdc"] = final_equity

        return self._build_tear_sheet(
            start_date=start_date,
            end_date=end_date,
            equity_curve=equity_curve,
            closed_trades=closed_trades_ledger,
            total_fees_usdc=total_fees_paid,
            total_slippage_usdc=total_slippage_usdc,
            wick_stop_outs=wick_stop_outs,
            funding_net_usdc=total_funding_net,
            basis_yield_usdc=accrued_basis_yield,
            bull_pnl_usdc=bull_pnl_usdc,
            bear_pnl_usdc=bear_pnl_usdc,
            chop_pnl_usdc=chop_pnl_usdc,
            bull_bars=bull_bars,
            bear_bars=bear_bars,
            chop_bars=chop_bars,
        )

    def _build_tear_sheet(
        self,
        *,
        start_date: str,
        end_date: str,
        equity_curve: list[dict[str, Any]],
        closed_trades: list[dict[str, Any]],
        total_fees_usdc: float,
        total_slippage_usdc: float = 0.0,
        wick_stop_outs: int = 0,
        funding_net_usdc: float,
        basis_yield_usdc: float,
        bull_pnl_usdc: float,
        bear_pnl_usdc: float,
        chop_pnl_usdc: float,
        bull_bars: int,
        bear_bars: int,
        chop_bars: int,
    ) -> BacktestTearSheet:
        """Compute quantitative performance metrics and evaluate Blueprint 1 acceptance gates."""
        eq_values = np.array(
            [self.initial_equity] + [float(s["total_equity_usdc"]) for s in equity_curve],
            dtype=np.float64,
        )
        final_equity = float(eq_values[-1])
        total_ret = (final_equity / max(self.initial_equity, 1.0)) - 1.0

        bars_count = len(equity_curve)
        duration_days = max((bars_count * 4.0) / 24.0, 1.0)
        ann_return = (1.0 + total_ret) ** (365.0 / duration_days) - 1.0

        bar_rets = np.diff(eq_values) / np.maximum(eq_values[:-1], 1e-8)
        sharpe_ann = WalkForwardCalibrator.compute_annualized_sharpe(bar_rets, dt_hours=4.0)

        downside_rets = np.minimum(bar_rets, 0.0)
        downside_std = float(np.sqrt(np.mean(downside_rets ** 2)))
        sortino_ann = (
            float((np.mean(bar_rets) / downside_std) * np.sqrt(8760.0 / 4.0))
            if downside_std > 1e-8
            else 0.0
        )

        running_peaks = np.maximum.accumulate(eq_values)
        drawdowns = (running_peaks - eq_values) / np.maximum(running_peaks, 1e-8)
        max_dd_pct = float(np.max(drawdowns)) * 100.0
        calmar = float((ann_return * 100.0) / max(max_dd_pct, 0.05))

        pnls = np.array([float(t["realized_pnl_usdc"]) for t in closed_trades], dtype=np.float64)
        wins = pnls[pnls > 0.0]
        losses = pnls[pnls < 0.0]

        gross_profit_trades = float(np.sum(wins)) if len(wins) > 0 else 0.0
        gross_loss_abs = float(np.abs(np.sum(losses))) if len(losses) > 0 else 0.0

        trade_pf = (
            gross_profit_trades / gross_loss_abs
            if gross_loss_abs > 1e-6
            else (gross_profit_trades if gross_profit_trades > 0 else 0.0)
        )
        total_pf = (
            (gross_profit_trades + max(basis_yield_usdc, 0.0)) / gross_loss_abs
            if gross_loss_abs > 1e-6
            else trade_pf
        )

        total_trades = len(closed_trades)
        win_count = int(len(wins))
        loss_count = int(len(losses))
        win_rate_pct = (win_count / total_trades * 100.0) if total_trades > 0 else 0.0
        fee_drag_pct = (total_fees_usdc / max(self.initial_equity, 1.0)) * 100.0

        gate_sharpe = sharpe_ann > 1.50
        gate_mdd = max_dd_pct < 20.0
        gate_pf = total_pf > 1.65

        return BacktestTearSheet(
            start_date=start_date,
            end_date=end_date,
            duration_days=round(duration_days, 2),
            bars_replayed=bars_count,
            initial_equity_usdc=round(self.initial_equity, 2),
            final_equity_usdc=round(final_equity, 2),
            total_return_pct=round(total_ret * 100.0, 2),
            annualized_return_pct=round(ann_return * 100.0, 2),
            annualized_sharpe=round(sharpe_ann, 2),
            sortino_ratio=round(sortino_ann, 2),
            calmar_ratio=round(calmar, 2),
            max_drawdown_pct=round(max_dd_pct, 2),
            profit_factor=round(total_pf, 2),
            trade_profit_factor=round(trade_pf, 2),
            win_rate_pct=round(win_rate_pct, 2),
            total_trades=total_trades,
            winning_trades=win_count,
            losing_trades=loss_count,
            gross_profit_usdc=round(gross_profit_trades, 2),
            gross_loss_usdc=round(-gross_loss_abs, 2),
            total_fees_usdc=round(total_fees_usdc, 2),
            total_slippage_usdc=round(total_slippage_usdc, 2),
            wick_stop_outs=int(wick_stop_outs),
            fee_drag_pct=round(fee_drag_pct, 2),
            funding_net_usdc=round(funding_net_usdc, 2),
            basis_yield_usdc=round(basis_yield_usdc, 2),
            bull_regime_pnl_usdc=round(bull_pnl_usdc, 2),
            bear_regime_pnl_usdc=round(bear_pnl_usdc, 2),
            chop_regime_pnl_usdc=round(chop_pnl_usdc, 2),
            bull_bars_count=bull_bars,
            bear_bars_count=bear_bars,
            chop_bars_count=chop_bars,
            gate_sharpe_pass=gate_sharpe,
            gate_mdd_pass=gate_mdd,
            gate_profit_factor_pass=gate_pf,
            all_gates_passed=bool(gate_sharpe and gate_mdd and gate_pf),
        )

    @staticmethod
    def format_tear_sheet(ts: BacktestTearSheet) -> str:
        """Render a formatted quantitative tear sheet and acceptance gate table."""
        sharpe_status = "PASS" if ts.gate_sharpe_pass else "FAIL"
        mdd_status = "PASS" if ts.gate_mdd_pass else "FAIL"
        pf_status = "PASS" if ts.gate_profit_factor_pass else "FAIL"
        overall = "PASSED ALL ACCEPTANCE GATES" if ts.all_gates_passed else "FAILED ACCEPTANCE GATES"

        lines = [
            "=" * 78,
            " AEGIS QUANT (v0.2.0-alpha1) — POINT-IN-TIME BACKTEST TEAR SHEET",
            "=" * 78,
            f" Replay Window        : {ts.start_date} -> {ts.end_date} ({ts.duration_days:.1f} days / {ts.bars_replayed} 4H bars)",
            f" Execution Isolation  : SQLite (:memory:) Point-in-Time Clone | Hyperliquid L1 Fees",
            f" Initial Equity       : ${ts.initial_equity_usdc:,.2f} USDC",
            f" Final Equity         : ${ts.final_equity_usdc:,.2f} USDC ({ts.total_return_pct:+.2f}% | CAGR: {ts.annualized_return_pct:+.2f}%)",
            "-" * 78,
            " CORE RISK-ADJUSTED METRICS",
            f"   Annualized Sharpe  : {ts.annualized_sharpe:.2f}    [Gate: > 1.50]  -> {sharpe_status}",
            f"   Sortino Ratio      : {ts.sortino_ratio:.2f}",
            f"   Calmar Ratio       : {ts.calmar_ratio:.2f}",
            f"   Maximum Drawdown   : {ts.max_drawdown_pct:.2f}%   [Gate: < 20.0%] -> {mdd_status}",
            f"   Profit Factor      : {ts.profit_factor:.2f}    [Gate: > 1.65]  -> {pf_status} (Directional Trade PF: {ts.trade_profit_factor:.2f})",
            "-" * 78,
            " EXECUTION, FEE DRAG & CARRY ATTRIBUTION",
            f"   Total Trades       : {ts.total_trades} ({ts.winning_trades} Wins / {ts.losing_trades} Losses | Win Rate: {ts.win_rate_pct:.2f}%)",
            f"   Wick Stop-Outs     : {ts.wick_stop_outs} triggered on intra-bar high/low wicks",
            f"   Gross Profit/Loss  : +${ts.gross_profit_usdc:,.2f} / -${abs(ts.gross_loss_usdc):,.2f} USDC",
            f"   Fee Drag           : ${ts.total_fees_usdc:,.2f} USDC ({ts.fee_drag_pct:.2f}% of initial equity)",
            f"   Slippage Drag      : ${ts.total_slippage_usdc:,.2f} USDC (3 bps entry/exit + 8 bps/15% wick stop)",
            f"   1H Funding Carry   : ${ts.funding_net_usdc:+,.2f} USDC",
            f"   Basis/HLP Yield    : +${ts.basis_yield_usdc:,.2f} USDC",
            "-" * 78,
            " 3-STATE HMM REGIME ATTRIBUTION (BULL / BEAR / CHOP)",
            f"   BULL Regime PnL    : ${ts.bull_regime_pnl_usdc:+,.2f} USDC ({ts.bull_bars_count} bars | Long Relative-Strength Leaders)",
            f"   BEAR Regime PnL    : ${ts.bear_regime_pnl_usdc:+,.2f} USDC ({ts.bear_bars_count} bars | Short Relative-Weakness Laggards)",
            f"   CHOP / Basis Yield : ${ts.chop_regime_pnl_usdc:+,.2f} USDC ({ts.chop_bars_count} bars | Idle-Cash Delta-Neutral Harvest)",
            "=" * 78,
            f" COMMIT POINT 1 ACCEPTANCE STATUS: {overall}",
            "=" * 78,
        ]
        return "\n".join(lines)


HistoricalBacktester = BacktestEngine


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aegis Quant v0.2.0-alpha1 Point-in-Time Event-Driven Backtester"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-07-20",
        help="Backtest start date (YYYY-MM-DD, default: 2026-07-20)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="2026-10-20",
        help="Backtest end date (YYYY-MM-DD, default: 2026-10-20)",
    )
    parser.add_argument(
        "--initial-equity",
        type=float,
        default=10000.0,
        help="Initial portfolio equity in USDC (default: 10000)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help="Source SQLite database path (default: data/aegis_quant.db)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output tear sheet metrics as JSON in addition to formatted table",
    )
    args = parser.parse_args()

    engine = BacktestEngine(
        source_db_path=args.db_path,
        initial_equity=args.initial_equity,
    )
    try:
        tear_sheet = engine.run_backtest(
            start_date=args.start_date,
            end_date=args.end_date,
        )
        print(engine.format_tear_sheet(tear_sheet))
        if args.json:
            print(json.dumps(asdict(tear_sheet), indent=2))
    finally:
        engine.mem_db.close()


if __name__ == "__main__":
    main()
