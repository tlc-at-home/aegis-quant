# Aegis Quant (`v0.2.0-rc2` + Mainnet-Readiness Line)

Autonomous, self-custodial Web3 DEX quantitative crypto trading platform targeting **Hyperliquid USDC Perpetuals (`USDC_PERP`)** on **Fedora 45 Minimal** (`/home/fedora/aegis`).

> **Repository state**: `engine.__version__ = "0.2.0-rc2"`. HEAD (`4d846bd`, 2026-10-27) is **4 commits beyond the v0.2.0-rc2 core tag**, adding the Mainnet Go-Live readiness line: on-chain HLP vault harvesting, wallet equity sync, the System Settings WebUI, proportional risk scaling, and the readiness-gate CLI.

---

## What's New Since `v0.2.0-rc2`

1. **Mainnet Go-Live Readiness Gate CLI (`scripts/check_mainnet_readiness.py`)** — Evaluates 5 Go/No-Go gates against `data/aegis_quant.db` and prints a structured `PASS` / `WARN` / `FAIL` checklist (also available as `--json`):
   - **Gate 1 — Plumbing & Soak**: continuous Testnet soak duration (default ≥ 120h), ≥ 3 closed round-trip trades with FILLED FSM orders, CHOP ⇄ BULL/BEAR regime coverage with HLP vault harvest activity, and zero `ERROR`/`CRITICAL` logs in `engine.execution` / `engine.orchestrator` / `database` over a trailing 72h window.
   - **Gate 2 — Security & Custody**: schema version ≥ 4 in WAL mode with ≥ 100 config keys and 31 universe rows, mandatory explicit Fernet master key, sensitive secrets encrypted at rest, and valid wallet / agent private key / HLP vault configuration.
   - **Gate 3 — Provenance & Parity**: verified `data/real_4h_backfill.json.sha256` provenance, MACE model lifecycle parity config, and an optional `--with-backtest` 90-day acceptance replay (Sharpe > 1.50, Max DD < 20%).
   - **Gate 4 — Alerting & Risk Caps**: Telegram/Discord alert channel with exponential-backoff retry queue (optional `--send-test-alert`) and proportional risk/leverage guardrails.
   - **Gate 5 — Cutover & Canary**: Mainnet cutover target and canary sizing guidance.
2. **Persistent Fernet Keyfile Enforcement** — The AES-128 master key resolution chain now includes a persistent local keyfile (`<db_dir>/.aegis_master_key`, `chmod 0600`) wired into the systemd unit via `LoadCredential=aegis_master_key:/home/fedora/aegis/data/.aegis_master_key`, alongside `AEGIS_MASTER_KEY` / `FERNET_MASTER_KEY` env vars and systemd `CREDENTIALS_DIRECTORY` credentials. `LIVE`/`TESTNET` modes with `enforce_master_key = true` refuse to boot on the hardcoded fallback key (`RuntimeError: FATAL SECURITY VIOLATION...`).
3. **On-Chain Hyperliquid HLP Vault Harvesting & Equity Sync (`engine/execution.py`)**:
   - `fetch_hlp_vault_state()` queries the Hyperliquid L1 Info API (`userVaultEquities` + `vaultDetails`) for vault equity, accrued all-time PnL yield, lockup expiry, APR, deposit permissions, and perp collateral balances.
   - `execute_hlp_vault_transfer()` signs and submits EIP-712 `vaultTransfer` actions to deposit idle USDC into — or withdraw from — the HLP protocol vault (Mainnet `0xdfc2...f303`, Testnet `0xa150...b0a0`).
   - `reconcile_cycle()` deploys up to `basis_max_idle_cash_allocation_pct` (50%) of idle cash into the HLP vault (respecting lockup, `hlp_min_transfer_usdc`, and a 5-minute transfer cooldown), and **recalls vault collateral** before order placement whenever perp free margin is insufficient.
   - `sync_wallet_equity()` fetches the live Mainnet/Testnet USDC balance (perps, falling back to spot, plus `userVaultEquities`) and updates `initial_equity_usdc` — optionally auto-scaling all dollar risk caps.
4. **System Settings WebUI, Proportional Risk Scaling & Wallet Sync (`web/`, `engine/database.py`)**:
   - A full **System Settings** dashboard (editable `system_config` with validation, secret masking, and risk-cap scaling), clarifying **execution mode vs active routing state** (`PAPER_SIMULATION` vs `LIVE_MAINNET` / `LIVE_TESTNET`).
   - `scale_risk_caps_for_equity()` proportionally rescales all dollar-denominated risk caps from `initial_equity_usdc`: baseline risk 1.0%, per-trade risk band 0.5–1.5×, portfolio heat 4.0%, single-asset cap 25%, gross leverage 1.5× — so a $25,000 wallet gets $250 baseline risk / $1,000 heat / $6,250 single-asset / $37,500 gross, not the $10,000-baseline defaults.
   - New REST endpoints: `POST /api/v1/config/batch` (batch config updates) and `POST /api/v1/config/sync-wallet` (live wallet equity sync).
5. **Unsupported Testnet Symbol Isolation (`engine/ingestion.py`, `engine/signals.py`)** — Symbols not listed on the active network (e.g. certain perps on Testnet) are detected via `BadSymbol`-class errors, marked `is_synthetic = 1`, excluded from live ranking (tradable symbols rank ahead), and forced to `FLAT` signal actions in LIVE/TESTNET modes instead of trading on synthetic fallback data.
6. **CI Hardening** — `httpx` added for FastAPI `TestClient`, default DB initialization in clean CI checkouts, and all Ruff `BLE001`/`S110`/`UP035`/`F841` and MyPy static-type errors resolved across `engine/` (CI is green: `ruff` + `mypy` + `pytest` on Python 3.12).

---

## Key Capabilities (`v0.2.0-rc2` Core)

- **Code Audit Remediation & Backtest-Live Model Parity**:
  - **MACE 36-Bar Cached Lifecycle & Frozen Normalization (`engine/signals.py`)**: Aligns live/paper `SignalEngine` inference with `BacktestEngine` by refitting Random Forest + Ridge estimators strictly every `mace_refit_interval_bars = 36` 4H bars (~6 days) and caching frozen normalization scaling (`X_center`, `X_std`) across intermediate 15-second ticks (< 1e-12 backtest-live numerical parity).
  - **Degenerate Spearman Rank IC Safeguards (`_safe_spearman_ic()`)**: Explicitly returns `None` when sample size < 5, target standard deviation is `0.0`, or correlation is `NaN`, falling back cleanly to `mace_min_ic_floor` (`0.05`).
  - **Mandatory Fernet Master Key Enforcement (`engine/database.py`)**: `LIVE`/`TESTNET` modes require an explicit master key (env var, systemd credential, or persistent keyfile) when `enforce_master_key = true`.
  - **SHA-256 Backfill Data Provenance & Realistic CHOP Funding (`engine/backtest.py`, `scripts/refresh_backfill.py`)**: Verifies `data/real_4h_backfill.json` against its `.sha256` checksum before historical replay, flags synthetic warmup bars (`is_synthetic = 1`), and disables the artificial `basis_min_rate` HLP floor (`enable_hlp_floor_backtest = false`).
  - **Asynchronous Alert Retry Queue with Exponential Backoff (`engine/alerts.py`)**: Queues failed Telegram/Discord deliveries and retries with `base_delay * 2^(attempt-1)` (`alert_max_retries = 5`, `alert_backoff_base_sec = 2.0`).
- **Hyperliquid L1 Native Execution & Order Finite State Machine (`engine/execution.py`)**:
  - Non-custodial routing via a Main Arbitrum Wallet (`wallet_address`) and Trade-Only Agent Wallet (`private_key`), modeling `1.5 bps` maker / `4.5 bps` taker fees and 1-hour funding settlement.
  - **`MAKER_LIMIT_CHASER` FSM**: Full order lifecycle (`PENDING → OPEN_ON_BOOK → PARTIALLY_FILLED → FILLED | CANCELLED | REJECTED | EXPIRED`) tracked in `trades` and `order_state_audit`, routing `postOnly=True` maker limit orders with top-of-book chase repricing before falling back to spread-crossing `MARKET_IOC`.
  - **On-Chain Position Drift Reconciliation (`reconcile_exchange_state()`)**: Compares live Hyperliquid L1 positions against local `OPEN` trades every 15s cycle, force-closing drifted positions (`exit_reason='EXCHANGE_RECONCILE'` + `CRITICAL` telemetry) and syncing partial size reductions.
  - **Exact 1H Funding Settlement Sync (`sync_funding_payments()`)**: Queries `exchange.fetch_funding_history()` in `LIVE`/`TESTNET` modes to record exact USDC funding transfers.
  - **Realistic Paper Fill & Dynamic Slippage Model (`simulate_paper_fill()`)**: Models volatility-and-size slippage (`default_slippage_bps + impact_coeff × ATR₁₄/Close × Notional/10000`) and next-bar `low ≤ limit` (BUY) / `high ≥ limit` (SELL) maker fill verification.
- **Security Hardening, Webhook Alerting & CI Pipeline (`v0.2.0-rc1`)**:
  - **Fernet AES-128-CBC + HMAC-SHA256 Encryption at Rest**: Sensitive secrets (`private_key`, `gemini_api_key`, `telegram_bot_token`, `discord_webhook_url`) stored as `gAAAAAB...` ciphertext tokens in SQLite, with transparent in-memory decryption (`get_decrypted_config()`) and atomic master key rotation (`rotate_master_key()`).
  - **Schema Versioning Ledger**: `schema_migrations` table tracking applied migrations (`schema_version = 4`).
  - **Asynchronous Telegram Bot & Discord Webhook Alerting**: Real-time notifications on `CRITICAL` log events, HMM regime flips, on-chain reconciliation mismatches, and daily drawdown breaches (≥ 3.0%).
  - **GitHub Actions CI/CD (`.github/workflows/ci.yml`)**: `ruff check .`, `mypy --ignore-missing-imports engine/`, and `pytest tests/ -v` on Python 3.12 for every push/PR to `main`.
- **Synthetic `ALT/BTC` Cross-Pair Engine (`BTC_SYNTH`)**: Timestamp-aligned `ALT/BTC:SYNTH` OHLCV candles computed as `P_Alt/USDC(t) / P_BTC/USDC(t)` across all 15 altcoin pairs.
- **Cached Warm-Start 3-State Gaussian HMM Regime Gate (540-Bar 4H Window)**: Posterior regime probabilities γₜ(Bull/Bear/Chop) over a rolling 90-day 4H window using a 3-feature observation vector (`[log_ret, vol_norm, alt_pc1_ret]`), 4H bar timestamp caching, single-step forward filtering (`alpha_T`) on 15s intra-bar ticks (>80% CPU reduction), and `0.55`/`0.45` hysteresis confirmation gates.
- **Purged Out-of-Sample MACE & Dynamic Spearman Rank IC Blending**: Strictly purges the final k bars from the training slice to eliminate target overlap leakage, predicts out-of-sample on the latest bar, and dynamically scales the MACE ensemble weight within `[0.15, 0.60]` based on rolling out-of-sample Spearman Rank IC, blending with multi-horizon SNR momentum (60% `ALT/BTC:SYNTH` + 40% `ALT/USDC:USDC`).
- **Point-in-Time Event-Driven Backtester (`engine/backtest.py`)**: Clones historical state into an isolated `:memory:` SQLite database (`pit_timestamp_ms` enforced across all queries), verifies SHA-256 dataset provenance, and replays chronological 4H bars with causal point-in-time SVD (`alt_pc1_ret`), 1H funding carry (4× per 4H bar), maker/taker fee schedules, and intra-bar `high`/`low` wick stop-out stress testing.
- **Three Real-Data Stress-Test Calibrations**:
  1. **Two-Tier Chandelier Stops & Wider `3.8x ATR` Short Buffer** — Ratchets `highest/lowest_price_seen` on confirmed `bar_close` prices, widens SHORT trailing stops to `3.8x ATR` (`3.0x` LONG), triggers intra-bar wick liquidation only on catastrophic overshoots (`0.85x ATR` beyond the stop with `8 bps + 15%` overshoot slippage), and evaluates standard trailing stops on `bar_close` (`8 bps` slippage).
  2. **Symmetric High-Conviction `BEAR` Gate + Multi-Horizon BTC Macro Confirmation** — Symmetric cross-sectional conviction on SHORT entries (`conv < -0.32`, `snr_synth < -0.30`, `mace_score < -0.05`, `rs_96 < -0.03`, `r_usdc_8 < -0.008`) plus multi-horizon BTC macro confirmation (48/192 EMA SNR and negative 5d/10d/30d BTC log returns).
  3. **Extended Post-Stop Cooldown (`14` Bars / 56 Hours)** — Per-asset cooldown after any Chandelier stop-out, persisted in SQLite `trades.closed_at` across daemon restarts and protected against premature expiration during 15-second live/paper polling.

---

## Verified Repository State (2026-10-28, HEAD `4d846bd`)

| Verification | Result |
| :--- | :--- |
| Test suite (`pytest tests/ -v`) | **54/54 passed** (181s) |
| Lint (`ruff check .`) | **All checks passed** |
| Data provenance (SHA-256) | **Verified** — 16 coins, 69,090 real 4H candles, 2024-11-01 → 2026-10-23 |
| Schema | `schema_version = 4`, WAL mode, 100 `system_config` keys, 31 universe rows |

---

## Point-in-Time Backtest Performance (Re-verified at HEAD `4d846bd`)

> ⚠️ **Honest disclosure — 2025 holdout regression**: The previously documented 2025 holdout result (`+61.29%`, Sharpe `4.40`, PASS) was produced when the backfill dataset only covered ~Oct 2024 → Oct 2025 (1,950 candles/asset; only 187 bars inside the 2025 window). The October 2026 data refresh extended coverage to the **full 2025 calendar year plus 2026** (4,331 candles/asset). Replaying the complete, previously-unseen 2025 year **fails** the holdout acceptance gates — the BEAR-regime short book lost `$1,714.41 USDC` across 499 bars in FY-2025. The 2026 windows below still pass all gates. The 2025 holdout failure is an **open calibration item** that should be resolved before Mainnet go-live.

| Metric | 2025 Full-Year Holdout | 90-Day Acceptance (`2026-07-20` → `2026-10-20`) | 2026 YTD Stress (`2026-01-01` → `2026-10-22`) | Acceptance Gate |
| :--- | :---: | :---: | :---: | :---: |
| Initial → Final Equity (USDC) | `$10,000` → **`$8,894.14`** | `$10,000` → **`$10,795.73`** | `$10,000` → **`$12,575.18`** | — |
| Net Return / Annualized CAGR | **`-11.06%` / `-11.06%`** | **`+7.96%` / `+35.05%`** | **`+25.75%` / `+32.78%`** | — |
| Annualized Sharpe Ratio | **`-0.74`** ❌ | **`2.11`** ✅ | **`1.94`** ✅ | `> 1.20` (`> 1.50`) |
| Sortino / Calmar Ratio | `-1.00` / `-0.63` | `3.22` / `10.40` | `3.01` / `5.44` | — |
| Maximum Drawdown | `17.42%` ✅ | `3.37%` ✅ | `6.03%` ✅ | `< 22.0%` (`< 20.0%`) |
| Profit Factor (Trade-Only PF) | **`0.71` (`0.57`)** ❌ | **`3.69` (`3.16`)** ✅ | **`2.57` (`2.24`)** ✅ | `> 1.65` |
| Closed Trades (Win Rate, Wick Stops) | `68` (`33.82%`, `15` wick) | `9` (`66.67%`, `3` wick) | `41` (`46.34%`, `5` wick) | — |
| Regime PnL (Bull / Bear / Chop+Yield) | `+$78` / `-$1,714` / `+$530` | `+$637` / `$0` / `+$159` | `+$1,078` / `+$965` / `+$532` | — |
| Fee / Slippage / Funding Drag | `$48` / `$134` / `-$11` | `$8` / `$18` / `-$38` | `$40` / `$73` / `-$17` | — |
| **Overall Gate Status** | **FAIL** | **PASS** | **PASS** | — |

Reproduce with:

```bash
python3 scripts/run_2025_holdout.py
python3 -m engine.backtest --start-date 2026-07-20 --end-date 2026-10-20 --initial-equity 10000
python3 -m engine.backtest --start-date 2026-01-01 --end-date 2026-10-22 --initial-equity 10000
```

---

## Repository Structure

- `.github/workflows/ci.yml` — GitHub Actions CI pipeline (`ruff`, `mypy`, `pytest` on Python 3.12)
- `deploy/bootstrap_vm.sh` & `deploy/aegis-quant.service` — Fedora 45 Minimal provisioning & systemd service (`User=fedora`, `LoadCredential=aegis_master_key`)
- `sql/schema_and_seed.sql` & `sql/migrations/001..004_*.sql` — SQLite WAL schema, `order_state_audit` FSM table, `schema_migrations` ledger (`schema_version = 4`), 3 views, 100 `system_config` hyperparameters, and 31 universe rows (16 `USDC_PERP` + 15 `BTC_SYNTH`)
- `data/real_4h_backfill.json` & `data/real_4h_backfill.json.sha256` — Real historical Hyperliquid L1 4H OHLCV candles (16 coins, 69,090 candles, 2024-11-01 → 2026-10-23) with verified SHA-256 provenance
- `engine/` — `database.py` (WAL manager, Fernet encryption, config, risk-cap scaling), `ingestion.py` (Hyperliquid poller + `ALT/BTC:SYNTH` synthesis), `signals.py` (HMM + MACE + SNR), `risk.py` (conviction sizing + Chandelier stops), `execution.py` (Order FSM, reconciliation, HLP vault transfers), `alerts.py` (Telegram/Discord + retry queue), `calibration.py` (walk-forward timeframe calibrator), `backtest.py` (point-in-time backtester), `main.py` (asyncio orchestrator) — `__version__ = "0.2.0-rc2"`
- `scripts/` — `refresh_backfill.py` (4H OHLCV backfill & SHA-256 provenance generator), `run_2025_holdout.py` (2025 frozen-calibration holdout runner), `check_mainnet_readiness.py` (5-gate Mainnet Go/No-Go readiness checker)
- `web/` — `server.py` & `static/index.html` — FastAPI REST + SSE live telemetry, credential & alert management, and System Settings dashboard (`:8000`)
- `tests/` — **54 unit & integration tests**: `test_audit_remediation_rc2.py` (9), `test_execution_state_machine.py` (11), `test_hyperliquid_credentials.py` (13), `test_paper_fill_model.py` (4), `test_risk_boundaries.py` (7), `test_signals_hmm_mace.py` (10)

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
python3 -m engine.alerts --test-alert "Deployment Ready"

# 4. Run backtests (2025 holdout + 2026 windows)
python3 scripts/run_2025_holdout.py
python3 -m engine.backtest --start-date 2026-07-20 --end-date 2026-10-20 --initial-equity 10000
python3 -m engine.backtest --start-date 2026-01-01 --end-date 2026-10-22 --initial-equity 10000

# 5. Run the full 54-test verification suite & MACE cache lifecycle parity check
pytest tests/ -v --durations=10
pytest tests/test_audit_remediation_rc2.py -k "test_mace_cache_lifecycle" -v

# 6. Evaluate Mainnet Go-Live readiness (5 gates; requires an initialized DB with soak history)
python3 scripts/check_mainnet_readiness.py
python3 scripts/check_mainnet_readiness.py --with-backtest --send-test-alert   # full deep-check
python3 scripts/check_mainnet_readiness.py --json                              # machine-readable report
```

Engine CLI flags: `python3 -m engine.main --once | --show-credentials | --configure-only`, `--execution-mode {PAPER,LIVE,TESTNET}`, `--testnet | --mainnet`, `--wallet-address 0x...`, `--private-key 0x...`, `--vault-address 0x...`, `--gemini-api-key ...`.

---

## Centralized SQLite Configuration & Encrypted Web3 DEX Credentials

All 100 operational parameters, Web3 DEX credentials (`wallet_address`, `private_key`, `gemini_api_key`, `hyperliquid_vault_address`, `hyperliquid_testnet`, `execution_mode`), and alerting secrets (`telegram_bot_token`, `telegram_chat_id`, `discord_webhook_url`) are stored **exclusively inside the SQLite WAL `system_config` table (`data/aegis_quant.db`)** with sensitive credentials encrypted at rest using Fernet AES-128-CBC + HMAC-SHA256 — never in `.env` files, environment variables, or external Linux configuration files. In `LIVE`/`TESTNET` modes (`enforce_master_key = true`), an explicit master key is strictly required, resolved in order from: `AEGIS_MASTER_KEY` / `FERNET_MASTER_KEY` env vars → systemd `CREDENTIALS_DIRECTORY` credentials → persistent keyfile `<db_dir>/.aegis_master_key` (`chmod 0600`).

Parameters and credentials can be inspected or updated via:
- **Web Dashboard (`:8000`) / REST API**: `GET/POST /api/v1/config` (single), `POST /api/v1/config/batch` (batch, with `scale_risk_caps`), `POST /api/v1/config/sync-wallet` (live equity sync + proportional risk-cap rescaling), `GET/POST /api/v1/credentials`, `GET/POST /api/v1/alerts`, `POST /api/v1/alerts/test`, plus `GET /api/v1/{status,portfolio,universe,signals,trades,logs,stream}`
- **SQLite CLI**: `sqlite3 data/aegis_quant.db "UPDATE system_config SET config_value = '...' WHERE config_key = 'wallet_address';"`
- **Engine CLI**: `python3 -m engine.main --configure-only --testnet --execution-mode LIVE --wallet-address 0x... --private-key 0x...`

### Proportional Risk Scaling

When `initial_equity_usdc` changes (manually or via wallet equity sync with `scale_risk_caps = true`), all dollar-denominated risk caps rescale proportionally while percentage caps stay fixed:

| Cap | Fraction of Equity | `$10,000` baseline | `$25,000` wallet |
| :--- | :---: | :---: | :---: |
| Baseline risk per trade | 1.0% | `$100` | `$250` |
| Per-trade risk band | 0.5% – 1.5% | `$50` – `$150` | `$125` – `$375` |
| Portfolio heat ceiling | 4.0% | `$400` | `$1,000` |
| Single-asset notional cap | 25% | `$2,500` | `$6,250` |
| Gross leverage ceiling | 1.5× | `$15,000` | `$37,500` |

---

## Operational Notes & Known Items

- **2025 Full-Year Holdout FAIL (open item)**: See the performance section above. Root cause is the extended dataset exposing previously-unseen Q4-2025 BEAR-regime shorts (`-$1,714.41 USDC`); the BEAR gate calibration (Calibration 2) should be re-tuned against the full-year dataset before Mainnet cutover.
- **Readiness checker requires soak history**: `scripts/check_mainnet_readiness.py` evaluates against a production `data/aegis_quant.db` with Testnet soak telemetry (snapshots, trades, `order_state_audit`, `telemetry_logs`); it is not meaningful on a fresh checkout.
- **Unsupported Testnet symbols**: Assets not listed on the active network are isolated automatically in LIVE/TESTNET (synthetic fallback data is never traded); on Mainnet all 16 universe perps are expected to be listed.
- **HLP vault lockup**: Withdrawals respect the on-chain 4-day lockup (`lockedUntilTimestamp`); queued withdrawals are logged hourly until expiry.
