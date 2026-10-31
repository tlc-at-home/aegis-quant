# Aegis Quant (`v0.2.0-rc2` — Native 9-Crate Rust Workspace)

Autonomous, self-custodial Web3 DEX quantitative crypto trading platform targeting **Hyperliquid USDC Perpetuals (`USDC_PERP`)** on **Fedora 45 Minimal** (`/home/fedora/aegis`).

> **Architecture**: Standalone 9-crate Rust workspace (`Cargo.toml`, `rustc 1.99.1`) compiling to a single stripped 8.4 MB native binary (`/home/fedora/aegis/bin/aegis-quant`) with ~4 MB runtime RSS.
> **Legacy Python Archive**: The prior Python codebase (`engine/`, `web/server.py`, `scripts/`, `tests/`) is permanently archived on GitHub under branch **`archive/python-v0.2.0-rc2`** and tag **`v0.2.0-rc2-python`**.

---

## 9-Crate Rust Workspace Architecture

| Crate | Path | Responsibility |
| :--- | :--- | :--- |
| **`aegis-core`** | `crates/aegis-core` | Domain types (`PortfolioSnapshot`, `SignalEvaluation`, `TradeRecord`, `OrderExecutionResult`), EVM/secp256k1 validation, and SHA-256 dataset provenance (`verify_data_provenance`). |
| **`aegis-db`** | `crates/aegis-db` | SQLite WAL manager (`rusqlite` bundled), wire-compatible Fernet (`AES-128-CBC + HMAC-SHA256`) encryption at rest (`0x80` token format), key rotation, schema migrations (`001..004`), and proportional risk-cap scaling (`scale_risk_caps_for_equity`). |
| **`aegis-risk`** | `crates/aegis-risk` | Wilder 14-bar ATR, conviction-scaled position sizing (`0.5x..1.5x` of 1% baseline risk), portfolio heat ceiling (`4.0%`), single-asset notional cap (`25%`), gross leverage cap (`1.5x`), and Two-Tier Chandelier Trailing Stops (`3.0x ATR` Long, `3.8x ATR` Short + `0.85x ATR` catastrophic wick guard). |
| **`aegis-signals`** | `crates/aegis-signals` | 3-State Diagonal-Covariance Gaussian HMM regime gate (`BULL`, `BEAR`, `CHOP` with log-sum-exp Baum-Welch EM & 4H bar cache), Purged Out-of-Sample MACE (60-tree Random Forest + L2 Ridge ensemble cached every 36 4H bars with frozen normalization), Multi-Horizon SNR (60% `ALT/BTC:SYNTH` + 40% `ALT/USDC:USDC`), Dynamic Spearman Rank IC blending (`[0.15, 0.60]`), and Walk-Forward Timeframe Calibrator (`1h` vs `4h`). |
| **`aegis-alerts`** | `crates/aegis-alerts` | Real-time Telegram Bot & Discord webhook alerting with SQLite-backed exponential-backoff retry queue (`alert_retry_queue`, up to 5 retries) and loopback self-test server. |
| **`aegis-execution`** | `crates/aegis-execution` | Hyperliquid L1 REST ingestion (`candleSnapshot`, `metaAndAssetCtxs`), synthetic `ALT/BTC:SYNTH` cross-pair builder, `MAKER_LIMIT_CHASER` Order FSM (`PENDING → OPEN_ON_BOOK → PARTIALLY_FILLED → FILLED | CANCELLED | REJECTED | EXPIRED`), EIP-712 `secp256k1` recoverable action signing (`k256` + `sha3`), on-chain position drift reconciliation, Unified Spot + Perp + HLP Vault (`userVaultEquities` / `vaultDetails` / `spotClearinghouseState`) equity & yield sync, and dynamic slippage paper simulator. |
| **`aegis-backtest`** | `crates/aegis-backtest` | Zero-lookahead Point-in-Time (`:memory:`) event-driven historical backtester with SHA-256 dataset provenance verification and institutional Tear Sheet generator (Sharpe, Sortino, Calmar, Max DD, Profit Factor, Regime Attribution). |
| **`aegis-web`** | `crates/aegis-web` | High-concurrency `axum` + `tokio` HTTP & Server-Sent Events (`GET /api/v1/stream`) telemetry server on port `8000` serving `web/static/index.html` and all `/api/v1/*` endpoints. |
| **`aegis-cli`** | `crates/aegis-cli` | Unified `aegis-quant` binary (`clap`) combining the 15s daemon orchestrator, embedded Axum WebUI server, `db`, `backtest`, `alerts`, and 5-gate `readiness` subcommands. |

---

## Repository Structure

- `Cargo.toml` & `Cargo.lock` — Workspace manifest and locked dependency graph
- `.cargo/config.toml` — Build target configuration (`target-dir = "/tmp/aegis-cargo-target"` for fast RAM builds)
- `crates/` — All 9 Rust workspace crates (`aegis-core`, `aegis-db`, `aegis-risk`, `aegis-signals`, `aegis-alerts`, `aegis-execution`, `aegis-backtest`, `aegis-web`, `aegis-cli`)
- `bin/aegis-quant` — Compiled & stripped release binary executed by `systemd`
- `deploy/aegis-quant.service` & `deploy/bootstrap_vm.sh` — Fedora 45 Minimal `systemd` unit (`LoadCredential=aegis_master_key:/home/fedora/aegis/data/.aegis_master_key`) and bootstrap script
- `sql/schema_and_seed.sql` & `sql/migrations/001..004_*.sql` — SQLite WAL schema (`schema_version = 4`), `order_state_audit` FSM table, 100 `system_config` keys, and 31 universe instruments (16 `USDC_PERP` + 15 `BTC_SYNTH`)
- `web/static/index.html` — Single-page live telemetry, credential & alert management, and System Settings dashboard (`:8000`)
- `data/real_4h_backfill.json` & `data/real_4h_backfill.json.sha256` — Historical Hyperliquid L1 4H OHLCV dataset (16 coins, 69,090 candles) with SHA-256 provenance

---

## Build, Test & CLI Usage

```bash
# 1. Run workspace unit & integration tests
cargo test --workspace

# 2. Build optimized & stripped release binary
cargo build --release
cp /tmp/aegis-cargo-target/release/aegis-quant ./bin/aegis-quant

# 3. Database initialization, migrations & Fernet AES-128 encryption verification
./bin/aegis-quant db --init
./bin/aegis-quant db --migrate
./bin/aegis-quant db --test-encryption

# 4. Verify Telegram/Discord operational alerting
./bin/aegis-quant alerts --test

# 5. Run isolated Point-in-Time (:memory:) historical backtest
./bin/aegis-quant backtest --days 365

# 6. Run 5-Gate Institutional Mainnet Go/No-Go Readiness Suite
./bin/aegis-quant readiness --mode PAPER
./bin/aegis-quant readiness --mode LIVE --json

# 7. Run single cycle (--once) or start full daemon + Axum WebUI server (:8000)
./bin/aegis-quant --once
./bin/aegis-quant --host 0.0.0.0 --port 8000
```

---

## Centralized SQLite Configuration & Encrypted Web3 DEX Credentials

All 100 operational parameters, Web3 DEX credentials (`wallet_address`, `private_key`, `gemini_api_key`, `hyperliquid_vault_address`, `hyperliquid_testnet`, `execution_mode`), and alerting secrets (`telegram_bot_token`, `telegram_chat_id`, `discord_webhook_url`) are stored **exclusively inside the SQLite WAL `system_config` table (`data/aegis_quant.db`)** with sensitive credentials encrypted at rest using wire-compatible Fernet (`AES-128-CBC + HMAC-SHA256`, `gAAAAAB...` tokens).

In `LIVE`/`TESTNET` modes (`enforce_master_key = true`), an explicit master key is strictly required, resolved in order from:
1. `systemd` `LoadCredential=aegis_master_key:/home/fedora/aegis/data/.aegis_master_key` (`$CREDENTIALS_DIRECTORY`)
2. `AEGIS_MASTER_KEY` / `FERNET_MASTER_KEY` environment variables
3. Persistent local keyfile `<db_dir>/.aegis_master_key` (`chmod 0600`)
