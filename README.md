# Aegis Quant (`v2.0.0`) — Hyperliquid L1 Multi-Sleeve Quantitative Trading Platform

**Aegis Quant v2.0** is an autonomous, self-custodial quantitative perpetuals trading platform written in Rust (`2021` edition, Tokio async runtime, Axum REST/SSE web server, and MariaDB 13.0 Rolling InnoDB) for **Hyperliquid L1**.

---

## 1. Architecture Overview

### 1.1 Three-Sleeve Alpha Allocator (`S1 / S2 / S3`)
Capital is allocated dynamically across three complementary sleeves and scaled by the active portfolio `risk_tier`:

- **Sleeve 1 — Cross-Sectional Carry-Momentum & Market-Beta Hedge (`s1_carry_mom_budget = 0.45`)**: Ranks core liquid perpetuals by annualized carry + risk-adjusted momentum, with beta hedging and range-bottom short protection.
- **Sleeve 2 — Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout (`s2_trend_breakout_budget = 0.40`)**: Evaluates EMA trend alignment, 20-bar breakout levels, and 3-second volume surges gated by market breadth.
- **Sleeve 3 — Live Capitulation Flush & Funding Dislocation (`s3_event_reversion_budget = 0.25`)**: Screens event-eligible perpetuals for extreme funding z-scores (`s3_funding_zscore`) and capitulation ATR flushes (`s3_flush_atr_mult`) with green-candle bounce confirmation.

### 1.2 Dynamic Risk Tiers & Capital Cap Cutoff
- **Selectable Risk Tiers (`POST /api/v1/risk-tier`)**: `conservative` (`0.65x`), `standard` (`1.00x`, fallback default for unknown tier strings), `aggressive` (`1.35x`), `extreme` (`1.85x`, initial v2.0 seed), and `custom` (`0.10x`–`3.00x`).
- **Capital Sizing Cap & Cutoff Epoch (`risk_capital_cap_usdc`, `risk_capital_cap_updated_at_ms`)**:
  - `live_exchange_equity_usdc` continuously tracks live Hyperliquid Perp `marginSummary.accountValue`.
  - Operators can set an optional `risk_capital_cap_usdc` sizing ceiling (`0` disables); updating it automatically advances `risk_capital_cap_updated_at_ms` so pre-cap legacy positions (`opened_at_ms < risk_capital_cap_updated_at_ms`) are isolated from cap-driven `AUTO_DELEVERAGE_RISK_CAP` while all post-cap positions and orders strictly enforce leverage and position-count caps.

### 1.3 Execution, Protective SL/TP & 1:1 Exchange Reconciliation
- **Two-Tier Runtime Loops**:
  - **1s Priority Loop**: Monitors active `WORKING` Maker Limit Chaser (`Alo`) orders (margin reserved in `reserved_margin_usdc`), promotes fills to `OPEN` (committed in `committed_notional_usdc`), attaches/refreshes exchange-native Hyperliquid SL & TP trigger orders (`protective_stop_px`/`protective_stop_oid` & `take_profit_px`/`take_profit_oid`), and ratchets trailing stops only once positions move into profit.
  - **3s Exchange Scanner**: Polls `metaAndAssetCtxs`, `allMids`, and 1h/4h OHLCV candles across 150+ Hyperliquid perpetuals, runs S1/S2/S3 signal evaluation, and dispatches rebalances.
- **1:1 Reconciliation (`aegis-quant cutover` / `POST /api/v1/reconcile`)**: Reconciles `clearinghouseState`, `frontendOpenOrders`/`openOrders`, `userFills`, and `userFunding` (`cumFunding.sinceOpen`), preserving true `opened_at_ms`, funding attribution, opening+closing fees, and local `order_audit` history.

---

## 2. Persistence: MariaDB 13.0 Rolling & 7 First-Class SQL Views

Defined in [`sql/schema_v2_mariadb.sql`](sql/schema_v2_mariadb.sql):

- **10 InnoDB Tables**:
  1. `system_config` — Runtime config & Fernet (`AES-128-CBC + HMAC-SHA256`) encrypted secrets
  2. `exchange_assets` — 150+ Hyperliquid perpetual instruments (`BTC-USDC`, `SOL-USDC`, `kPEPE-USDC`, etc.)
  3. `asset_cooldowns` — Post-stop, manual close, volatility shock, and churn-guard blocks
  4. `ohlcv_bars` — 1h & 4h candle history
  5. `sleeve_signals` — Per-asset S1/S2/S3 scores, target weights, and event triggers
  6. `positions` — Unified `WORKING`, `OPEN`, `CLOSED`, `CANCELLED` lifecycle & SL/TP OIDs
  7. `order_audit` — 1s Maker Limit Chaser state-transition audit trail (`ON DELETE SET NULL`)
  8. `portfolio_snapshots` — Equity, cash, reserved/committed capital, leverage, and sleeve attribution curve
  9. `telemetry_logs` — Secret-redacted operational logs
  10. `alert_queue` — Persistent Telegram/Discord retry queue (`PENDING`, `DISPATCHING`, `PARTIAL`, `DELIVERED`, `SKIPPED`, `FAILED`)
- **7 SQL Views**:
  1. `v_assets_held` — Active `OPEN` positions with mark, signed SL/TP distances, and sleeve weights
  2. `v_assets_ordered` — Active `WORKING` orders with reserved margin and side-aware mid distance (bps)
  3. `v_assets_in_cooldown` — Currently active cooldown blocks and remaining seconds
  4. `v_assets_sleeve_s1` — Sleeve 1 carry-momentum rankings
  5. `v_assets_sleeve_s2` — Sleeve 2 trend & breakout rankings
  6. `v_assets_sleeve_s3` — Sleeve 3 event-reversion rankings
  7. `v_priority_watchlist` — Deduplicated 1s surveillance universe

---

## 3. 10-Crate Workspace Layout (`crates/*`)

| Crate | Responsibility |
| --- | --- |
| `crates/aegis-core` | Domain models, `RiskTier`/`ExecutionMode`/`AlertProvider`, Hyperliquid symbol normalization (`kPEPE`), UTC helpers, secret redaction & `IMMUTABLE_SECURITY_CONFIG_KEYS` |
| `crates/aegis-db` | MariaDB connection pool, schema/index migrations, Fernet vault (`0600` keyfile / `LoadCredential`), transactional position FSM, 7 SQL View queries, multi-horizon PnL & retention pruning |
| `crates/aegis-signals` | S1 carry-momentum, S2 trend-breakout, S3 event-reversion, breadth filter, and multi-sleeve allocator |
| `crates/aegis-risk` | Portfolio ledger evaluation, conservative-to-extreme leverage caps, per-asset leverage caps, and profit-gated trailing SL/TP |
| `crates/aegis-alerts` | HTML Telegram & Markdown Discord formatters, safe HTML truncation, atomic `FOR UPDATE SKIP LOCKED` queue claiming, per-channel `PARTIAL` retry, and bounded 5-minute dedup |
| `crates/aegis-execution` | Hyperliquid EIP-712/MsgPack signing, HTTP client, 3s scanner, 1s priority monitor & ALO order chaser, SL/TP trigger manager, and 1:1 exchange reconciler |
| `crates/aegis-backtest` | Isolated walk-forward multi-sleeve simulator |
| `crates/aegis-telemetry` | Tracing subscriber and async telemetry bus |
| `crates/aegis-web` | Axum REST API, shared 1.5s `tokio::sync::watch` SSE broadcaster, unified bearer/cookie auth, strict CSP (`script-src 'self'`), CSRF Origin/Host protection, and Obsidian UI (`web/static/`) |
| `crates/aegis-cli` | `aegis-quant` binary orchestrating daemon lifecycle (with `SIGINT`/`SIGTERM` graceful shutdown), DB init/backup, cutover, alerts, backtest, and status |

---

## 4. Build, CLI Flags & Web API

### 4.1 Build
```bash
cargo build --release --workspace
cp target/release/aegis-quant bin/aegis-quant
```

### 4.2 `aegis-quant` Global Flags & Subcommands
Global options:
- `--db <PATH>` (alias `--db-path <PATH>`): Keyfile anchor hint (or `:memory:` for ephemeral test DB)
- `--json`: Emit machine-readable JSON output
- `--once`: Run a single 3s scan + 1s priority tick + signal cycle and exit
- `--host <IP>` / `--port <PORT>`: WebUI bind address and port (default `127.0.0.1:8000`)
- `--allow-remote-bind`: Permit non-loopback WebUI binding (e.g. `0.0.0.0`)
- `--no-web`: Run trading daemon without embedded WebUI server

Subcommands:
```bash
# Start unified daemon (1s Priority + 3s Scanner + WebUI on :8000; default command)
./bin/aegis-quant run --host 127.0.0.1 --port 8000

# Initialize / migrate schema, generate Fernet key, or snapshot state
./bin/aegis-quant db --generate-key
./bin/aegis-quant db --init --migrate
./bin/aegis-quant db --init --force
./bin/aegis-quant db --backup

# Run 1:1 Hyperliquid reconciliation, seed perp universe & signals
./bin/aegis-quant cutover
./bin/aegis-quant cutover --close-legacy

# Dispatch live HTML Telegram/Discord test card
./bin/aegis-quant alert --test

# Run multi-sleeve walk-forward backtest
./bin/aegis-quant backtest --days 30

# Print live MariaDB health, 7 SQL Views counts & snapshot status
./bin/aegis-quant status --json
```

### 4.3 Web Dashboard & REST/SSE API (`crates/aegis-web`)
- **UI (`web/static/{index.html,app.js,styles.css}`)**: Displays all timestamps in the browser's **local timezone**, removes all inline event handlers (`script-src 'self'`), requires confirmation on position/order closes, renders active `v_assets_in_cooldown` blocks, prompts for bearer login on `401`, and disables redundant polling while SSE is `OPEN`.
- **Unauthenticated Probe**: `GET /api/v1/health` (returns HTTP `503 Service Unavailable` if MariaDB ping/query fails).
- **Authentication**: `POST /api/v1/auth/login` (exchanges `web_api_token` for `HttpOnly; SameSite=Strict` `aegis_session` cookie). When `web_auth_enabled` or `web_require_auth` is enabled (or bound non-loopback with a token set), **all** `/api/v1/*` routes (`GET` and `POST`) require auth except `/api/v1/health` and `/api/v1/auth/login`.
- **Read & Stream Endpoints**: `GET /api/v1/dashboard`, `GET /api/v1/state`, `GET /api/v1/stream` (shared 1.5s `watch` SSE broadcaster).
- **Mutating Endpoints (CSRF `Origin`/`Referer` vs `Host` enforced)**: `POST /api/v1/risk-tier`, `POST /api/v1/config` (typed bounds & key validation), `POST /api/v1/alerts/test`, `POST /api/v1/positions/close`, `POST /api/v1/reconcile`.

---

## 5. Production Deployment (`deploy/`)

- [`deploy/99-aegis-mariadb.cnf`](deploy/99-aegis-mariadb.cnf): Binds MariaDB to `127.0.0.1`, enforces `default_time_zone = '+00:00'`, `innodb_flush_log_at_trx_commit = 1`, and `innodb_lock_wait_timeout = 10`.
- [`deploy/aegis-quant.service`](deploy/aegis-quant.service): Hardened `systemd` unit loading `AEGIS_DB_URL` from `0600` `/etc/aegis-quant.env`, `LoadCredential=aegis_master_key:...`, `ProtectClock=true`, `ProtectKernelLogs=true`, `SystemCallFilter=@system-service`, and socket-only MariaDB access (`-/var/lib/mysql/mysql.sock`).
- [`deploy/bootstrap_vm.sh`](deploy/bootstrap_vm.sh): Automates `chronyd`, `firewalld`, Btrfs `chattr +C /var/lib/mysql`, MariaDB `LimitNOFILE=65536` drop-in, schema provisioning, `0600` `/etc/aegis-quant.env`, daily compressed `mariadb-dump` timer (`aegis-mariadb-backup.timer`), and `aegis-quant.service` verification.

---

## 6. Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --test-threads=4
```
