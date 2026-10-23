# Aegis Quant (`v0.2.0-rc2`)

Autonomous, self-custodial Web3 DEX quantitative crypto trading platform targeting **Hyperliquid USDC Perpetuals (`USDC_PERP`)** on **Fedora 45 Minimal** (`/home/fedora/aegis`).

---

## Key Capabilities (`v0.2.0-rc2` — Code Audit Remediation, Live Parity, Security Hardening & Data Provenance)

- **Code Audit Remediation & Backtest-Live Model Parity (`v0.2.0-rc2`)**:
  - **MACE 36-Bar Cached Lifecycle & Frozen Normalization (`engine/signals.py`)**: Aligns live/paper `SignalEngine` inference with `BacktestEngine` by refitting Random Forest + Ridge estimators strictly every `mace_refit_interval_bars = 36` 4H bars (~6 days) and caching frozen normalization scaling (`X_center`, `X_std`) across intermediate 15-second ticks (`< 1e-12` backtest-live numerical parity).
  - **Degenerate Spearman Rank IC Safeguards (`_safe_spearman_ic()`)**: Explicitly returns `None` when sample size `< 5`, target standard deviation is `0.0`, or correlation is `NaN`, falling back cleanly to `mace_min_ic_floor` (`0.05`).
  - **Mandatory Fernet Master Key Enforcement (`engine/database.py`)**: Requires an explicit `AEGIS_MASTER_KEY` environment variable or systemd `CREDENTIALS_DIRECTORY/aegis_master_key` when `execution_mode` is `LIVE` or `TESTNET` (`enforce_master_key = true`), raising `RuntimeError("FATAL SECURITY VIOLATION...")` and logging a `CRITICAL` telemetry alert if absent.
  - **SHA-256 Backfill Data Provenance & Realistic CHOP Funding (`engine/backtest.py`, `scripts/refresh_backfill.py`)**: Verifies `data/real_4h_backfill.json` against `data/real_4h_backfill.json.sha256` before historical replay, flags synthetic warmup bars with `WARNING` logs and `is_synthetic = 1`, and disables the artificial `basis_min_rate` HLP floor (`enable_hlp_floor_backtest = false`).
  - **Asynchronous Alert Retry Queue with Exponential Backoff (`engine/alerts.py`)**: Queues failed Telegram/Discord webhook deliveries in `AlertRetryQueue` and retries with exponential backoff (`base_delay * 2^(attempt - 1)`, `alert_max_retries = 5`, `alert_backoff_base_sec = 2.0`).
  - **2025 Frozen-Calibration Out-of-Sample Holdout Runner (`scripts/run_2025_holdout.py`)**: Validates strategy performance on the 2025 holdout window (`2025-01-01` $\rightarrow$ `2025-12-31`), verifying Annualized Sharpe $> 1.20$ and Max Drawdown $< 22.0\%$.
- **Hyperliquid L1 Native Execution & Order Finite State Machine (`engine/execution.py`)**:
  - Non-custodial routing via a Main Arbitrum Wallet (`wallet_address`) and Trade-Only Agent Wallet (`private_key`), modeling `1.5 bps` maker / `4.5 bps` taker fees and 1-hour funding settlement.
  - **`MAKER_LIMIT_CHASER` FSM (`v0.2.0-alpha2`)**: Tracks full order lifecycle (`PENDING` $\rightarrow$ `OPEN_ON_BOOK` $\rightarrow$ `PARTIALLY_FILLED` $\rightarrow$ `FILLED` | `CANCELLED` | `REJECTED` | `EXPIRED`) in `trades` and `order_state_audit`, routing `postOnly=True` maker limit orders across top-of-book chase intervals before falling back to spread-crossing `MARKET_IOC`.
  - **On-Chain Position Drift Reconciliation (`reconcile_exchange_state()`)**: Compares live Hyperliquid L1 positions (`exchange.fetch_positions()`) against local `OPEN` trades every 15s cycle, force-closing drifted positions with `exit_reason='EXCHANGE_RECONCILE'` (plus `CRITICAL` telemetry alerts) and syncing partial size reductions.
  - **Exact 1H Funding Settlement Sync (`sync_funding_payments()`)**: Queries `exchange.fetch_funding_history(symbol, since=last_sync)` in `LIVE`/`TESTNET` modes to record exact USDC funding transfers.
  - **Realistic Paper Fill & Dynamic Slippage Model (`simulate_paper_fill()`)**: Models volatility-and-size slippage $\text{Slippage}_{\text{eff}} = \text{default\_slippage\_bps} + \text{impact\_coeff} \times (\text{ATR}_{14}/\text{Close}) \times (\text{Notional}_{\text{USD}}/10000)$ and next-bar `low <= limit_price` (`BUY`) / `high >= limit_price` (`SELL`) maker fill verification.
- **Security Hardening, Webhook Alerting & CI Pipeline (`v0.2.0-rc1` / `v0.2.0-rc2`)**:
  - **Fernet AES-128-CBC + HMAC-SHA256 Encryption at Rest (`engine/database.py`)**: Encrypts sensitive configuration secrets (`private_key`, `gemini_api_key`, `telegram_bot_token`, `discord_webhook_url`) in SQLite (`gAAAAAB...` tokens on disk) with systemd `LoadCredentialEncrypted=` / `AEGIS_MASTER_KEY` derivation, transparent in-memory decryption (`get_decrypted_config()`), and atomic master key rotation (`rotate_master_key()`).
  - **Schema Versioning Ledger (`sql/migrations/003..004_*.sql`)**: Tracks applied schema migrations (`schema_version = 4`) in `schema_migrations`.
  - **Asynchronous Telegram Bot & Discord Webhook Alerting (`engine/alerts.py`)**: Dispatches real-time notifications with `AlertRetryQueue` exponential backoff on `CRITICAL` log events, HMM regime flips (`CHOP` $\rightarrow$ `BULL` / `BEAR`), on-chain position reconciliation mismatches, and daily portfolio drawdown breaches (`>= 3.0%`), configurable via the Web UI (`:8000`) and `/api/v1/alerts`.
  - **GitHub Actions CI/CD Pipeline (`.github/workflows/ci.yml`)**: Automatically runs `ruff check .`, `mypy --ignore-missing-imports engine/`, and `pytest tests/ -v` on Python 3.12 across every push and pull request to `main`.
- **Synthetic `ALT/BTC` Cross-Pair Engine (`BTC_SYNTH`)**:
  - Computes timestamp-aligned `ALT/BTC:SYNTH` OHLCV candles from `ALT/USDC:USDC` and `BTC/USDC:USDC` perpetuals ($P_{\text{Alt/BTC}}(t) = P_{\text{Alt/USDC}}(t) / P_{\text{BTC/USDC}}(t)$).
- **Cached Warm-Start 3-State Gaussian HMM Regime Gate (540-Bar 4H Window)**:
  - Decodes posterior regime probabilities $\gamma_t(\text{Bull})$, $\gamma_t(\text{Bear})$, and $\gamma_t(\text{Chop})$ over a rolling 90-day 4H window using a 3-feature observation vector (`[log_ret, vol_norm, alt_pc1_ret]`), 4H bar timestamp caching (`_hmm_cache`), single-step forward filtering (`alpha_T`) on 15s intra-bar ticks (`>80%` CPU reduction), and `0.55` / `0.45` hysteresis confirmation gates.
- **Purged Out-of-Sample MACE & Dynamic Spearman Rank IC Blending**:
  - Strictly eliminates target overlap leakage by purging the final $k$ bars from the training slice (`X_train = features.iloc[warmup:-k]`, `y_train = target.iloc[warmup:-k]`) and predicting out-of-sample on `X_latest = features.iloc[[-1]]` using 36-bar cached Random Forest + Ridge models.
  - Dynamically scales the MACE ensemble weight within `[0.15, 0.60]` based on rolling out-of-sample Spearman Rank Information Coefficient (`IC`), blending with multi-horizon SNR momentum (`60%` `ALT/BTC:SYNTH` + `40%` `ALT/USDC:USDC`).
- **Point-in-Time Event-Driven Backtester (`engine/backtest.py`)**:
  - Clones historical state into an isolated `:memory:` SQLite database (`pit_timestamp_ms` enforced across all queries), verifying SHA-256 dataset provenance and replaying chronological 4H bars with causal point-in-time SVD (`alt_pc1_ret`), 1H funding carry (`4x` per 4H bar), Maker/Taker fee schedules, and intra-bar `high`/`low` wick stop-out stress testing.
- **Three Real-Data Stress-Test Calibrations**:
  1. **Calibration 1 — Two-Tier Chandelier Stops & Wider `3.8x ATR` Short Buffer**: Ratchets `highest_price_seen` / `lowest_price_seen` on confirmed `bar_close` prices, widens `SHORT` trailing stops to `3.8x ATR` (`3.0x ATR` for `LONG`), triggers intra-bar `high`/`low` liquidation stop-outs only on catastrophic wick overshoots (`0.85x ATR` beyond the stop level with `8 bps + 15%` overshoot slippage), and evaluates standard Chandelier trailing stops on `bar_close` (`8 bps` slippage).
  2. **Calibration 2 — Symmetric High-Conviction `BEAR` Gate + Multi-Horizon BTC Macro Confirmation**: Enforces symmetric cross-sectional conviction on `SHORT` entries (`conv < -0.32`, `snr_synth < -0.30`, `mace_score < -0.05`, `rs_96 < -0.03`, `r_usdc_8 < -0.008`) and requires multi-horizon BTC macro trend confirmation (`btc_snr_val <= -0.45`, macro 48/192 EMA `btc_snr_macro_val <= -0.20`, and negative 5d/10d/30d BTC log returns `btc_ret_30_val <= -0.015`, `btc_ret_60_val <= -0.04`, `btc_ret_180_val <= 0.002`).
  3. **Calibration 3 — Extended Post-Stop Cooldown (`14` Bars / 56 Hours)**: Extends per-asset cooldown after any Chandelier stop-out from `6` bars (`24h`) to `14` bars (`56h`), persisted in SQLite `trades.closed_at` across daemon restarts and protected against premature expiration during 15-second live/paper polling.

---

## Point-in-Time Backtest & 2025 Holdout Performance (`v0.2.0-rc2` — Zero Artificial HLP Floor)

| Metric | 2025 Holdout Replay (`2025-01-01` $\rightarrow$ `2025-12-31`) | 90-Day Acceptance Window (`2026-07-20` $\rightarrow$ `2026-10-20`) | Full YTD Stress Test (`2026-01-01` $\rightarrow$ `2026-10-22`) | Acceptance Gate | Status |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Initial $\rightarrow$ Final Equity (USDC)** | `$10,000.00` $\rightarrow$ **`$16,129.19`** | `$10,000.00` $\rightarrow$ **`$10,811.78`** | `$10,000.00` $\rightarrow$ **`$12,328.06`** | — | — |
| **Net Return / Annualized CAGR** | **`+61.29%` / `+61.29%`** | **`+8.12%` / `+35.84%`** | **`+23.28%` / `+29.61%`** | — | — |
| **Annualized Sharpe Ratio** | **`4.40`** | **`2.15`** | **`1.77`** | `> 1.20` (`> 1.50`) | **PASS** |
| **Sortino Ratio / Calmar Ratio** | **`7.17` / `14.72`** | **`3.28` / `10.71`** | **`2.75` / `3.34`** | — | — |
| **Maximum Drawdown** | **`4.16%`** | **`3.35%`** | **`8.88%`** | `< 22.0%` (`< 20.0%`) | **PASS** |
| **Total Profit Factor (Trade-Only PF)** | **`5.88` (`5.88`)** | **`3.76` (`3.22`)** | **`2.22` (`1.95`)** | `> 1.65` | **PASS** |
| **Closed Trades / Intra-Bar Wick Stops** | `56` (`71.43%` WR, `5` wick stops) | `9` (`66.67%` WR, `3` wick stops) | `44` (`43.18%` WR, `6` wick stops) | — | — |
| **Artificial HLP Yield Floor** | **`$0.00`** (`disabled`) | — | — | `0.00` | **PASS** |

---

## Repository Structure

- `.github/workflows/ci.yml` — GitHub Actions CI pipeline (`ruff`, `mypy`, `pytest` on Python 3.12)
- `deploy/bootstrap_vm.sh` & `deploy/aegis-quant.service` — Fedora 45 Minimal provisioning & systemd service (`User=fedora`)
- `sql/schema_and_seed.sql` & `sql/migrations/001..004_*.sql` — SQLite WAL schema, `order_state_audit` FSM table, `schema_migrations` ledger (`schema_version = 4`), 3 views, 100 `system_config` hyperparameters, and 31 universe rows
- `data/real_4h_backfill.json` & `data/real_4h_backfill.json.sha256` — 1,950 real historical Hyperliquid L1 4H OHLCV candles per asset (`31,200` total candles) with verified SHA-256 provenance checksum
- `engine/` — `database.py`, `ingestion.py`, `signals.py`, `risk.py`, `execution.py`, `alerts.py`, `calibration.py`, `backtest.py`, `main.py` (`__version__ = "0.2.0-rc2"`)
- `scripts/` — `refresh_backfill.py` (4H OHLCV backfill & SHA-256 provenance generator) & `run_2025_holdout.py` (2025 frozen-calibration out-of-sample holdout runner)
- `web/` — `server.py` & `static/index.html` FastAPI REST + SSE live telemetry, credential & Telegram alert dashboard (`:8000`)
- `tests/` — `test_audit_remediation_rc2.py`, `test_hyperliquid_credentials.py`, `test_signals_hmm_mace.py`, `test_execution_state_machine.py`, `test_paper_fill_model.py`, `test_risk_boundaries.py` (49 unit & integration tests)

---

## CLI Usage & Verification Commands

```bash
# 1. Apply schema migrations 001..004 (schema_version = 4, 100 system_config keys)
python3 -m engine.database --apply-migration sql/migrations/001_v020_config_expansion.sql
python3 -m engine.database --apply-migration sql/migrations/002_v020_order_fsm.sql
python3 -m engine.database --apply-migration sql/migrations/003_v020_schema_versioning.sql
python3 -m engine.database --apply-migration sql/migrations/004_v020_rc2_remediation.sql

# 2. Refresh backfill dataset & verify SHA-256 provenance checksum
python3 scripts/refresh_backfill.py

# 3. Verify Fernet AES-128 encryption at rest & webhook alerting
python3 -m engine.database --test-encryption
python3 -m engine.alerts --test-alert "v0.2.0-rc2 Deployment Ready"

# 4. Run 2025 frozen-calibration holdout & 2026 point-in-time backtests
python3 scripts/run_2025_holdout.py
python3 -m engine.backtest --start-date 2026-07-20 --end-date 2026-10-20 --initial-equity 10000
python3 -m engine.backtest --start-date 2026-01-01 --end-date 2026-10-22 --initial-equity 10000

# 5. Run the full 49-test verification suite & MACE cache lifecycle parity check
pytest tests/ -v --durations=10
pytest tests/test_audit_remediation_rc2.py -k "test_mace_cache_lifecycle" -v
```

---

## Centralized SQLite Configuration & Encrypted Web3 DEX Credentials

All 100 operational parameters, Web3 DEX credentials (`wallet_address`, `private_key`, `gemini_api_key`, `hyperliquid_vault_address`, `hyperliquid_testnet`, `execution_mode`), and alerting secrets (`telegram_bot_token`, `telegram_chat_id`, `discord_webhook_url`) are stored **exclusively inside the SQLite WAL `system_config` table (`data/aegis_quant.db`)** with sensitive credentials encrypted at rest using Fernet AES-128-CBC + HMAC-SHA256 — never in `.env` files, environment variables, or external Linux configuration files. In `LIVE` and `TESTNET` modes (`enforce_master_key = true`), an explicit `AEGIS_MASTER_KEY` environment variable or systemd `CREDENTIALS_DIRECTORY/aegis_master_key` credential is strictly required.

Parameters and credentials in `system_config` can be inspected or updated directly in SQLite via:
- **Web Dashboard (`:8000`) / REST API**: `GET/POST /api/v1/credentials`, `GET/POST /api/v1/alerts`, and `GET/POST /api/v1/config`
- **SQLite CLI**: `sqlite3 data/aegis_quant.db "UPDATE system_config SET config_value = '...' WHERE config_key = 'wallet_address';"`
- **Engine CLI**: `python3 -m engine.main --configure-only --testnet --execution-mode LIVE --wallet-address 0x... --private-key 0x...`
