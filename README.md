# Aegis Quant (`v2.1.0`) — Hyperliquid L1 Multi-Sleeve Quantitative Trading Platform

**Aegis Quant v2.1** is an autonomous, self-custodial quantitative perpetuals trading platform written in Rust (`2021` edition, Tokio async runtime, Axum REST/SSE web server, and MariaDB 13.0 Rolling / 12.3 LTS InnoDB) for **Hyperliquid L1**.

---

## 1. Architecture Overview

### 1.1 Three-Sleeve Alpha Allocator (`S1 / S2 / S3`)
Capital is allocated dynamically across three complementary sleeves and scaled by the active portfolio `risk_tier`:

- **Sleeve 1 — Cross-Sectional Carry-Momentum & Market-Beta Hedge (`s1_carry_mom_budget = 0.45`)**: Ranks core liquid perpetuals by annualized carry + risk-adjusted momentum, with beta hedging and range-bottom short protection.
- **Sleeve 2 — Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout (`s2_trend_breakout_budget = 0.40`)**: Evaluates EMA trend alignment, 20-bar breakout levels, and 3-second volume surges gated by market breadth.
- **Sleeve 3 — Live Capitulation Flush & Funding Dislocation (`s3_event_reversion_budget = 0.10`)**: Screens event-eligible perpetuals for extreme funding z-scores (`s3_funding_zscore`) and capitulation ATR flushes (`s3_flush_atr_mult`) with green-candle bounce confirmation.

### 1.2 Dynamic Risk Tiers & Capital Cap Cutoff
- **Selectable Risk Tiers (`POST /api/v1/risk-tier`)**: `conservative` (`0.65x`), `standard` (`1.00x`, fallback default for unknown tier strings), `aggressive` (`1.35x`), `extreme` (`1.85x`, initial v2.0 seed), and `custom` (`0.10x`–`3.00x`).
- **Capital Sizing Cap & Cutoff Epoch (`risk_capital_cap_usdc`, `risk_capital_cap_updated_at_ms`)**:
  - `live_exchange_equity_usdc` continuously tracks live Hyperliquid Perp `marginSummary.accountValue`.
  - Operators can set an optional `risk_capital_cap_usdc` sizing ceiling (`0` disables); updating it automatically advances `risk_capital_cap_updated_at_ms` so pre-cap legacy positions (`opened_at_ms < risk_capital_cap_updated_at_ms`) are isolated from cap-driven `AUTO_DELEVERAGE_RISK_CAP` while all post-cap positions and orders strictly enforce leverage and position-count caps. Daily drawdown and ruin-halt limits are evaluated over the full 24h window against **uncapped** live wallet equity.

### 1.3 Execution, Protective SL/TP & 1:1 Exchange Reconciliation
- **Two-Tier Runtime Loops**:
  - **1s Priority Loop**: Monitors active `WORKING` Maker Limit Chaser (`Alo`) orders (margin reserved in `reserved_margin_usdc`), promotes fills to `OPEN` (committed in `committed_notional_usdc`), attaches/refreshes exchange-native Hyperliquid Stop-Limit & Take-Profit-Limit trigger orders (`protective_stop_px`/`protective_stop_oid` & `take_profit_px`/`take_profit_oid`), and ratchets trailing stops only once positions move into profit.
  - **3s Exchange Scanner**: Consumes Hyperliquid WebSocket (`Market` + `User`) streams with weight-governed REST fallback across 150+ perpetuals, runs S1/S2/S3 signal evaluation, and dispatches rebalances.
- **1:1 Reconciliation (`aegis-quant cutover` / `POST /api/v1/reconcile`)**: Reconciles `clearinghouseState`, `frontendOpenOrders`/`openOrders`, `userFillsByTime`, and `userFunding` (`cumFunding.sinceOpen`), preserving true `opened_at_ms`, funding attribution, multi-IOC close-epoch aggregation, opening+closing fees, and local `order_audit` history.

---

## 2. Persistence: MariaDB 13.0 Rolling & 7 First-Class SQL Views

Defined in [`sql/schema_v2_mariadb.sql`](sql/schema_v2_mariadb.sql):

- **11 InnoDB Tables** (`DECIMAL` money/price/qty precision, `CHECK` constraints, and foreign keys):
  1. `system_config` — Runtime config & Fernet (`AES-128-CBC + HMAC-SHA256`) encrypted secrets
  2. `exchange_assets` — 150+ Hyperliquid perpetual instruments (`BTC-USDC`, `SOL-USDC`, `kPEPE-USDC`, etc.)
  3. `asset_cooldowns` — Post-stop, manual close, volatility shock, and churn-guard blocks
  4. `ohlcv_bars` — 1m–1d candle history (10 supported Hyperliquid timeframes)
  5. `sleeve_signals` — Per-asset S1/S2/S3 scores, target weights, and event triggers
  6. `positions` — Unified `WORKING`, `OPEN`, `CLOSED`, `CANCELLED` lifecycle & SL/TP OIDs
  7. `order_audit` — 1s Maker Limit Chaser state-transition audit trail (`ON DELETE SET NULL`)
  8. `portfolio_snapshots` — Equity (`total_equity_usdc` & `uncapped_equity_usdc`), cash, reserved/committed capital, leverage, and sleeve attribution curve
  9. `telemetry_logs` — Secret-redacted operational logs
  10. `alert_queue` — Persistent Telegram/Discord retry queue (`PENDING`, `DISPATCHING`, `PARTIAL`, `DELIVERED`, `SKIPPED`, `FAILED`)
  11. `funding_events` — Reconciled Hyperliquid per-event funding ledger for windowed open-position PnL attribution
- **7 SQL Views**:
  1. `v_assets_held` — Active `OPEN` positions with mark, signed SL/TP distances, and sleeve weights
  2. `v_assets_ordered` — Active `WORKING` orders with reserved margin and side-aware mid distance (bps)
  3. `v_assets_in_cooldown` — Currently active cooldown blocks and remaining seconds
  4. `v_assets_sleeve_s1` — Sleeve 1 carry-momentum rankings
  5. `v_assets_sleeve_s2` — Sleeve 2 trend & breakout rankings
  6. `v_assets_sleeve_s3` — Sleeve 3 event-reversion rankings
  7. `v_priority_watchlist` — Deduplicated 1s surveillance universe

---

## 3. 11-Crate Workspace Layout (`crates/*`)

| Crate | Responsibility |
| --- | --- |
| `crates/aegis-core` | Domain models, `RiskTier`/`ExecutionMode`/`AlertProvider`, Hyperliquid symbol normalization (`kPEPE`), UTC helpers, shared indicators/HL parsers, secret redaction & `IMMUTABLE_SECURITY_CONFIG_KEYS` |
| `crates/aegis-db` | MariaDB connection pool (`AEGIS_DB_URL` required; isolated test DB helper), schema/index migrations, Fernet vault (`0600` keyfile / `LoadCredential`), transactional position FSM, 7 SQL View queries, 5-query cached multi-horizon PnL & batched retention pruning |
| `crates/aegis-signals` | S1 carry-momentum, S2 trend-breakout, S3 event-reversion, breadth filter, and multi-sleeve allocator |
| `crates/aegis-risk` | Portfolio ledger evaluation, conservative-to-extreme leverage caps, per-asset leverage caps, risk-parity sizing, ruin halt, and profit-gated trailing SL/TP + stagnation stop |
| `crates/aegis-alerts` | HTML Telegram & Markdown Discord formatters (`allowed_mentions` disabled), safe HTML truncation, atomic `FOR UPDATE SKIP LOCKED` queue claiming, per-channel `PARTIAL` retry, and bounded 5-minute dedup |
| `crates/aegis-execution` | Hyperliquid EIP-712/MsgPack signing, lease-backed nonces, 2-conn WebSocket manager + weight-governed HTTP client, 3s scanner, 1s priority monitor & ALO order chaser, Stop-Limit/TP-Limit trigger manager, and 1:1 exchange reconciler |
| `crates/aegis-backtest` | Isolated walk-forward multi-sleeve simulator supporting synthetic and causal `--source db` historical datasets |
| `crates/aegis-telemetry` | Tracing subscriber and dedicated non-blocking OS-thread batch telemetry writer |
| `crates/aegis-web` | Axum REST API, shared 1.5s `tokio::sync::watch` SSE broadcaster, loopback-by-default bearer/cookie auth, trusted proxy CIDR allowlist, strict CSP (`script-src 'self'`, `style-src 'self'`), CSRF Origin/Host protection, ETag static assets, and modular Obsidian UI (`web/static/js/`) |
| `crates/aegis-cli` | `aegis-quant` binary orchestrating daemon lifecycle (`SIGINT`/`SIGTERM` graceful shutdown + hourly retention pruning), DB init/backup/prune/key-migration, cutover, alerts, backtest, and status |
| `crates/aegis-test-support` | Shared loopback mock HTTP and WebSocket servers for deterministic integration testing |

---

## 4. Build, CLI Flags & Web API

### 4.1 Build
```bash
cargo build --release --workspace
cp target/release/aegis-quant bin/aegis-quant
```

### 4.2 `aegis-quant` Global Flags & Subcommands
Global options:
- `--db <PATH>` (alias `--db-path <PATH>`): Path anchor for `.aegis_master_key` and optional legacy SQLite migration; MariaDB DSN is configured via `AEGIS_DB_URL`
- `--json`: Emit machine-readable JSON output
- `--once`: Run a single 3s scan + 1s priority tick + signal cycle and exit
- `--host <IP>` / `--port <PORT>`: WebUI bind address and port (default `127.0.0.1:8000`)
- `--allow-remote-bind`: Permit non-loopback WebUI binding (e.g. `0.0.0.0`)
- `--no-web`: Run trading daemon without embedded WebUI server

Subcommands:
```bash
# Start unified daemon (1s Priority + 3s Scanner + Hourly Retention Prune + WebUI on :8000; default command)
./bin/aegis-quant run --host 127.0.0.1 --port 8000

# Read-only DB status / help (does not mutate MariaDB)
./bin/aegis-quant db

# Generate Fernet key, initialize schema + auto-provision encrypted web_api_token, migrate SQLite, prune, or snapshot
./bin/aegis-quant db --generate-key
./bin/aegis-quant db --init --migrate
./bin/aegis-quant db --init --force --confirm-destructive-reset   # Interactive TTY prompts for `DROP <db_name>`
./bin/aegis-quant db --prune
./bin/aegis-quant db --migrate-legacy-key
./bin/aegis-quant db --backup

# Run 1:1 Hyperliquid reconciliation, seed perp universe & signals
./bin/aegis-quant cutover
./bin/aegis-quant cutover --close-legacy

# Read-only alert_queue summary vs live formatted HTML test card
./bin/aegis-quant alert
./bin/aegis-quant alert --test

# Run multi-sleeve walk-forward backtest (synthetic or persisted MariaDB 1h/4h bars + funding)
./bin/aegis-quant backtest --days 30
./bin/aegis-quant backtest --days 30 --source db --timeframe 1h

# Print live MariaDB health, 7 SQL Views counts & snapshot status
./bin/aegis-quant status --json
```

### 4.3 Web Dashboard & REST/SSE API (`crates/aegis-web`)
- **UI (`web/static/{index.html,styles.css,app.js,js/*.js}`)**: Modular vanilla ES modules (`script-src 'self'`), zero `innerHTML` or inline `style=` attributes (`style-src 'self'; style-src-attr 'none'`), local browser timestamps, confirmation prompts on position/order closes, active `v_assets_in_cooldown` blocks, MariaDB outage & surveillance alert banners, uncapped vs capped equity comparison, and bearer login modal on `401`/`auth_expired`.
- **Unauthenticated Probe**: `GET /api/v1/health` (returns HTTP `503 Service Unavailable` with sanitized correlation ID if MariaDB ping/query fails).
- **Authentication (Loopback Auth-by-Default)**: `POST /api/v1/auth/login` (exchanges `web_api_token` for `HttpOnly; SameSite=Strict` `aegis_session` cookie) and `POST /api/v1/auth/logout`. Authentication is required on **all** `/api/v1/*` routes (`GET` and `POST`, except `/api/v1/health` and `/api/v1/auth/login`) **including on `127.0.0.1` loopback**, unless explicitly disabled via DB-immutable `web_auth_enabled=false`. `aegis-quant db --init` auto-provisions a random 32-byte Fernet-encrypted `web_api_token` if unset/empty and displays it once during provisioning (never logged in steady-state daemon runs). Forwarded proxy headers (`X-Forwarded-For`, `X-Forwarded-Proto`) are accepted right-to-left only from `web_trusted_proxies` CIDRs.
- **Read & Stream Endpoints**: `GET /api/v1/dashboard`, `GET /api/v1/state`, `GET /api/v1/stream` (shared 1.5s `watch` SSE broadcaster with continuous session re-validation).
- **Mutating Endpoints (CSRF `Origin`/`Referer` vs `Host` enforced)**: `POST /api/v1/run-mode` (5-state Operator Run-Mode & Kill-Switch: `ACTIVE`, `DRAIN`, `PAUSED`, `HALTED`, `STANDBY`), `POST /api/v1/risk-tier`, `POST /api/v1/config` (typed bounds & key validation), `POST /api/v1/alerts/test`, `POST /api/v1/positions/close`, `POST /api/v1/reconcile`, `POST /api/v1/vault/inspect`, `POST /api/v1/vault/sync`.

---

## 5. Production Deployment, Hyperliquid Wallet & Security (`deploy/`)

### 5.1 Database DSN & MariaDB 13.0 Configuration
- **`AEGIS_DB_URL` Requirement**: Compiled binaries and deployment scripts contain zero hardcoded default database credentials. Production requires `AEGIS_DB_URL` (loaded from `0600 root:root` `/etc/aegis-quant.env` or `$CREDENTIALS_DIRECTORY/aegis_db_url`). Automated tests use `AEGIS_TEST_DB_URL` (falling back to `AEGIS_DB_URL`) for isolated `aegis_quant_test_*` databases.
- [`deploy/99-aegis-mariadb.cnf`](deploy/99-aegis-mariadb.cnf): Binds MariaDB to `127.0.0.1`, enforces `default_time_zone = '+00:00'`, **`innodb_flush_log_at_trx_commit = 1`** (full per-transaction ACID durability), **`innodb_log_file_size = 256M`** (MariaDB 10.9–13.0.2's active dynamically-resizeable redo-log variable; note `innodb_redo_log_size` does not exist in MariaDB 13.0.2 and `innodb_redo_log_capacity` is Oracle MySQL 8 only), and `innodb_lock_wait_timeout = 10`.
- [`deploy/aegis-quant.service`](deploy/aegis-quant.service): Hardened `systemd` unit loading `AEGIS_DB_URL` from `0600` `/etc/aegis-quant.env`, `LoadCredential=aegis_master_key:...`, `ProtectClock=true`, `ProtectKernelLogs=true`, `SystemCallFilter=@system-service`, and socket-only MariaDB access (`-/var/lib/mysql/mysql.sock`).
- [`deploy/bootstrap_vm.sh`](deploy/bootstrap_vm.sh): Automates `chronyd`, `firewalld`, Btrfs `chattr +C /var/lib/mysql`, GPG-verified MariaDB installation (`gpgcheck=1`, `repo_gpgcheck=1`, verified MariaDB signing key fingerprint `177F4010FE56CA3336300305F1656F24C74CD1D8`), `LimitNOFILE=65536`, 36-char CSPRNG DB password in `0600` `/etc/aegis-quant.env`, least-privilege `aegis_quant.*` grants (no `DROP`, `SUPER`, `FILE`, `PROCESS`, or wildcard test grants unless `--enable-dev-test-grants`), daily compressed `mariadb-dump` timer (`aegis-mariadb-backup.timer`), and `aegis-quant.service` verification.

### 5.2 Hyperliquid API Agent Wallet & Monitoring-Only Vault Configuration
- **Trade-Only API Agent Wallet Setup & Expiry Best Practice**:
  1. Keep your master Hyperliquid L1 account (`wallet_address`) cold; approve a dedicated **Trade-Only API Agent Wallet** on Hyperliquid under `More -> API` (`hyperliquid_agent_address`).
  2. Store **only the Trade-Only API Agent private key** in `system_config.private_key` (Fernet-encrypted); never store a withdrawal-capable master private key on the trading host.
  3. Hyperliquid API agent approvals carry an expiration window (up to 180 days). Rotate and re-register a fresh agent wallet before expiration; Aegis Quant queries `extraAgents` at startup and raises a warning if the active signer is unregistered or expires within 7 days.
- **Monitoring-Only `hyperliquid_vault_address`**:
  - `hyperliquid_vault_address` (alongside `vault_role`, `vault_target_allocation_pct`, and `vault_profit_sweep_threshold_usdc`) is strictly **monitoring-only**: all live trading `/exchange` payloads always set `vaultAddress: null`, and vault deposits remain informational and segregated from perp sizing equity (`live_exchange_equity_usdc`).

---

## 6. Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
AEGIS_TEST_DB_URL="mysql://user:pass@127.0.0.1:3306/aegis_quant" \
  AEGIS_DB_URL="mysql://user:pass@127.0.0.1:3306/aegis_quant" \
  cargo test --workspace -- --test-threads=4
bash -n deploy/bootstrap_vm.sh
mariadbd --defaults-file=deploy/99-aegis-mariadb.cnf --verbose --help >/dev/null
```
