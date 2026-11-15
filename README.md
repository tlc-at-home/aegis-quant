# Aegis Quant (`v0.3.0-rc1`) — PP-50 Two-Book Quantitative Trading Platform

**Aegis Quant** (`v0.3.0-rc1`, SQLite WAL Schema `v16`, hardened through Round 20) is an autonomous, self-custodial quantitative perpetuals trading and portfolio management platform written in pure Rust (`2021` edition, Tokio async runtime, Axum web server, `rusqlite` bundled SQLite WAL) for **Hyperliquid L1**.

---

## 1. Overview & PP-50 Dual-Book Architecture

Aegis Quant partitions trading capital 50/50 across two orthogonal strategy books coordinated by a daily `00:05 UTC` cross-book settling protocol:

1. **Book A — The Pit (`crates/aegis-pit`)**:
   - Intraday / 4H multi-factor directional strategy operating across a seeded 31-instrument universe (`16 USDC_PERP` + `15 BTC_SYNTH` / `15 Alt Pairs`).
   - Combines a causal 3-State Gaussian Hidden Markov Model (`BULL`, `BEAR`, `CHOP`) with intrabar hysteresis (`hmm_intrabar_enter_margin = 0.03`), MACE multi-grid momentum indicators, and a Continuous Signal-to-Noise Ratio (`SNR`) conviction ensemble.
   - Executes via a Maker Limit Chaser (`Alo` post-only with passive BBO repricing and IOC fallback) protected by two-tier Chandelier ATR trailing stops and a 20-asset correlation-cluster risk engine.
2. **Book B — The Pond (`crates/aegis-pond`)**:
   - Champion `NPS-01` Naked-Option Perpetual Analogue S-Cycle scanner operating across an 80-coin dynamic Hyperliquid universe (`pond_universe_target_size = 80`).
   - Screens for two-sided structural exhaustion and funding-rate extremes (`|z| >= 2.0` with `2h` persistence and hourly funding floor) across a 6-Gate scanner.
   - Manages each active cell through a 10-state Finite State Machine (`DORMANT -> SCREENED -> CANDIDATE -> ARMED -> OPEN -> TRAILED -> CLOSED -> COOLDOWN`) governed by a deterministic 6-Tier Exit Priority Engine (Tier 1 Catastrophic Wick Stop down to Tier 6 Delisting Exit).
3. **Daily `00:05 UTC` 50/50 Settling (`crates/aegis-settle`)**:
   - Computes cross-book equity drift (`settling_split_pct = 0.50`, `settling_deadband_bps = 65`) and transfers only unlocked idle collateral (`available_idle_usdc`) from the rich book to the poor book without ever liquidating open positions.
   - Persists a crash-safe `PENDING` ledger row before network dispatch, tracks 2-leg L1 progress (`legs_completed`, `leg1_tx_ref`), and quarantines interrupted transfers (`PARTIAL` / `AMBIGUOUS`) with operator alerting (`STRANDED_BOOK_TRANSFER`).

### Six Non-Negotiable Two-Book Firewall Invariants (`F1`–`F6`)

Enforced at compile time, via AST/source-token inspection, and at runtime by `crates/aegis-cli/tests/test_firewall_ast.rs`:

| Invariant | Scope | Enforcement Mechanism |
| --- | --- | --- |
| **F1** | Crate Dependency Graph Isolation | `aegis-pit`, `aegis-pond`, and `aegis-settle` have zero mutual `Cargo.toml` dependencies (`aegis-pond` and `aegis-settle` also never depend on `aegis-signals`). |
| **F2** | AST Source-Token & Table Whitelist Isolation | Static source scan forbids cross-book symbols in `aegis-pit`, `aegis-pond`, and `aegis-settle` (and forbids direct DML on `trades`, `pond_cells`, or `book_transfers` in `schema_ops.rs` / `config_ops.rs`); `BookScopedDb::check_table_access` enforces runtime SQL table whitelists. |
| **F3** | Independent Book Row & Subaccount Isolation | `PIT` and `POND` maintain independent `books` rows (`equity_allocation_pct = 0.50`), separate trade/cell tables (`trades` vs `pond_cells`), and isolated PnL ledgers (`compute_pit_pnl_components` vs `compute_pond_pnl_components_with_marks`). |
| **F4** | Config Namespace Isolation | `BookScopedDb::load_pit_config` (`PitConfig`) and `BookScopedDb::load_pond_config` (`PondConfig`) enforce per-book configuration key whitelists via `check_config_key_access`. |
| **F5** | Idle-Only Settling Transfer (Zero Forced Liquidation) | `SettleController` caps every rebalance at `executed_transfer = required_transfer.min(available_idle)` and logs any shortfall as `blocked_remainder_usdc`. |
| **F6** | Panic & Task Supervision Isolation | Daemon execution steps wrap Book A, Book B, and combined ticks in `std::panic::catch_unwind(AssertUnwindSafe(...))` so a panic in one book is isolated without terminating the sibling book or WebUI server. |

---

## 2. 13-Crate Workspace Layout (`crates/*`)

| Crate | Type | Responsibility & Key Modules |
| --- | --- | --- |
| `crates/aegis-core` | Lib | Domain models (`BookId`, `BookType`, `Side`, `HmmRegime`, `SignalAction`, `PondCell`, `PondCellState`, `TradeRecord`, `BookTransfer`, `BookTransferStatus`, `BacktestTearSheet`, `OhlcvBar`), typed financial primitives (`Usdc`, `Price`, `Quantity`, `Leverage`, `Bps`), secret redaction (`sanitize_telemetry_message`, `sanitize_telemetry_details`, `SENSITIVE_CONFIG_KEYS`, `IMMUTABLE_SECURITY_CONFIG_KEYS`), and SHA-256 dataset provenance (`verify_data_provenance`). |
| `crates/aegis-db` | Lib + Bin (`aegis-db-tool`) | Bundled SQLite WAL manager (`DatabaseManager`), canonical Schema `v16` initialization & migrations (`schema_ops.rs`, `sql/schema_and_seed.sql`), Fernet `AES-128-CBC + HMAC-SHA256` encryption (`fernet.rs`), non-reentrant advisory cycle lock with heartbeat renewal (`DbCycleLockGuard`), generation-stamped connection pool & online `VACUUM INTO` / chunked `rusqlite::backup` restore, and `BookScopedDb` (`scoped.rs`). |
| `crates/aegis-signals` | Lib | Causal 3-State Gaussian HMM regime filter (`BULL`, `BEAR`, `CHOP`), MACE multi-grid momentum indicators, Continuous Signal-to-Noise Ratio (`SNR`) ensemble, and `WalkForwardCalibrator` (`1H` vs `4H`). |
| `crates/aegis-risk` | Lib | Dynamic open-heat position sizing (`0.50%`–`1.50%` per trade), `4.0%` book heat cap, `25%` single-asset cap, `35%` 20-asset correlation-cluster cap (`25%` `OTHER` cluster cap), `1.50x` gross leverage cap, and two-tier Chandelier stop calculator. |
| `crates/aegis-alerts` | Lib | Real-time Telegram Bot API & Discord webhook dispatcher (`AlertEngine`), 2-cycle `RegimeFlip` confirmation hysteresis, bounded exponential-backoff retry queue, and secret-redacted telemetry logging. |
| `crates/aegis-telemetry` | Lib | Asynchronous inter-task event bus (`TelemetryBus`) with bounded SQLite background writer and broadcast channel. |
| `crates/aegis-execution` | Lib | Hyperliquid L1 REST & EIP-712 / MsgPack order router (`signing`, `http_client`, `ingestion`, `live_dispatch`, `manual_close`, `reconcile`, `cycle`, `resting_reconcile`), post-only `Alo` BBO parser & passive retry, atomic protective stop replacement, `userFills` Fix-4 true-up, and HLP vault margin rebalancing. |
| `crates/aegis-pit` | Lib | Book A (`PIT`) orchestrator (`PitOrchestrator`) executing scoped 4H/intraday signal evaluation, entry dispatch, and freeze-window Chandelier trailing. |
| `crates/aegis-pond` | Lib | Book B (`POND`) orchestrator (`PondOrchestrator`), 80-coin universe manager (`universe.rs`), Champion `NPS-01` 6-Gate S-Cycle scanner (`scanner.rs`), 10-state cell FSM & 6-Tier Exit Priority Engine (`cell.rs`), relative ATR floor, and frozen-trail detector. |
| `crates/aegis-settle` | Lib | Daily `00:05 UTC` 50/50 Cross-Book Settling controller (`SettleController`), idle-only transfer cap, crash-safe `PENDING` journal, and `PARTIAL`/`AMBIGUOUS` quarantine. |
| `crates/aegis-backtest` | Lib | Isolated `:memory:` 365-day point-in-time walk-forward simulator (`BacktestEngine`) modeling taker/maker fees, dynamic impact slippage, 1H funding accrual, and stop-regime substitution disclosure. |
| `crates/aegis-web` | Lib | Axum REST API (`/api/v1/*`), digest-deduplicated Server-Sent Events (`/api/v1/stream`), session-nonce cookie auth, two-step guarded database restore, and 4-page dark-mode dashboard (`web/static/index.html`, `web/static/app.js`). |
| `crates/aegis-cli` | Bin (`aegis-quant`) | Unified CLI & daemon orchestrator (`AegisOrchestrator`) providing `run`, `pond`, `settle`, `db`, `backtest`, `alerts`, and 6-Gate (`21`-check) `readiness` subcommands. |

---

## 3. Core Subsystems & Hardening (Rounds 16–20)

### 3.1 Security & At-Rest Cryptography
- **Fernet `AES-128-CBC + HMAC-SHA256` Encryption (`crates/aegis-db/src/fernet.rs`)**: All secrets in `SENSITIVE_CONFIG_KEYS` (`private_key`, `gemini_api_key`, `telegram_bot_token`, `discord_webhook_url`, `web_api_token`) are stored on disk with `ENC:gAAAA...` ciphertext envelopes. Master key resolution follows strict precedence:
  1. `CREDENTIALS_DIRECTORY/aegis_master_key` (`systemd` `LoadCredential`)
  2. `AEGIS_MASTER_KEY` or `FERNET_MASTER_KEY` environment variable
  3. `<db_dir>/.aegis_master_key` (`chmod 0600`)
- **Immutable Security Policy Keys (`IMMUTABLE_SECURITY_CONFIG_KEYS`)**: Critical security and routing keys (`enforce_master_key`, `web_auth_enabled`, `web_allow_remote_bind`, `web_bind_host`, `web_csp_policy`, `web_cors_allowed_origins`, `web_allowed_hosts`, `web_api_token`, `pit_subaccount_address`, `pond_subaccount_address`, `settling_allow_single_wallet_virtual`, and Hyperliquid endpoint/vault addresses) cannot be weakened or mutated via the Web API.
- **Session-Nonce Cookie Auth, CSRF & Proxy Trust Gate (`crates/aegis-web/src/lib.rs`)**:
  - `POST /api/v1/auth/login` validates the operator token and issues an ephemeral 32-byte hex session nonce stored in `AppState::active_sessions` (`HttpOnly; SameSite=Strict` cookie); raw master tokens in cookies are rejected (`ME-13`, `R15-N7`).
  - Mutating routes enforce `Origin` / `Host` allowlists (`web_allowed_hosts`) and `X-CSRF-Token` / `X-Requested-With` headers.
  - Per-IP login rate limiting ignores `X-Forwarded-For` / `X-Real-IP` headers unless `web_trust_proxy_headers = true` is explicitly enabled (`ME-14`).
  - Automated CI secret-hygiene scanning (`R16-U2` in `.github/workflows/ci.yml` and `crates/aegis-cli`) blocks any contiguous Telegram bot token or API key literal across tracked files.

### 3.2 Persistence, Online Backup/Restore & Cross-Process Concurrency (`crates/aegis-db`)
- **Generation-Stamped Pool & Chunked Online Restore (`R16-M1`, `R19-N2(c)`)**: `DatabaseManager` stamps pooled SQLite connections with an atomic `pool_generation` epoch (`discarded_stale_connections` counter) and coordinates online `VACUUM INTO` backups and `rusqlite::backup::Backup` restores under `file_swap_lock.write()`. Step 3 of `restore_sqlite_backup_with_options` copies pages in chunks (`backup.step(256)`), refreshing the preserved `web.restore` lease expiration on each step until `StepResult::Done`.
- **Multi-Layered Live-Activity Restore Guard (`R16-M2`, `R17-M1`, `R17-L3`, `R19-L1`, `R19-L2`)**:
  - `detect_live_database_activity` and `detect_external_pid_holding_db` scan `/proc/<pid>/fd` on Linux (with a `5ms` retry on transient `read_dir` errors when `/proc/<pid>` still exists, and a `30ms` cache `detect_external_pid_holding_db_cached` on WebUI precondition polls) to block restores whenever an external daemon holds `<db>`, `<db>-wal`, or `<db>-shm`.
  - Check `(b)` retries read-only SQLite connection open once after `25ms` on non-empty DB files and fails closed if the open still fails (`R17-L3`).
- **Strictly Non-Reentrant Advisory Cycle Lock & Renewal Heartbeat (`R18-L1`, `R18-N1`, `R18-N2`, `R19-M1`, `R19-N1`, `R19-N2`, `R20-N2`)**:
  - `try_acquire_cycle_lock` is strictly non-reentrant while unexpired (`if !cur_owner.trim().is_empty() && cur_exp > now_ms { return Ok(false); }`). CLI one-shot commands use PID-suffixed owners (`cli-pond-scan.pid<pid>`, `cli-run-once.pid<pid>`).
  - `acquire_cycle_lock_guard` spawns a background renewal heartbeat thread (`renew_cycle_lock`, which returns `Ok(false)` immediately if `file_swap_lock.try_read().is_none()` so it never blocks Step 3 backup copies).
  - Renewal is bounded by `MAX_CYCLE_LOCK_RENEWAL_TICKS = 90` (`90` ticks = `min(30x initial TTL, 45 min)` under the `30s` renewal-interval clamp; `~47 min` total hold at the `120s` production TTL). Renewal failures increment `DatabaseManager::cycle_lock_renew_failures` (exposed in `GET /api/v1/health` and `check_web_restore_preconditions`).
  - `apply_post_copy_cycle_lock_state` and `release_cycle_lock` preserve `"web.restore"` across in-service quiesced restores (`R18-N1`), unconditionally clear any backup-inherited `engine_cycle_lock_owner` on CLI restores (`R18-N2`), and scrub any rows in the legacy `db_cycle_lock` table (`R19-N1`).
- **Two-Step Guarded WebUI Restore (`R16-U1`, `R17-L2`, `R17-N2`, `R18-N3`)**:
  - `GET /api/v1/db/backups` and `POST /api/v1/db/restore` share `evaluate_web_restore_gate`.
  - Step 1 (`confirm: false`) runs `PRAGMA integrity_check;`, hashes the snapshot in `1 MiB` chunks (`compute_file_sha256_streaming`), and mints a 60-second single-use `restore_token` with a per-IP cap of `3` and global cap of `64`.
  - Step 2 (`confirm: true`, `confirm_text: "RESTORE"`, `restore_token`) performs non-destructive token pre-validation (`tokens.get`) before calling `acquire_cycle_lock_guard("web.restore", restore_ttl_ms)` (where `restore_ttl_ms = recommended_restore_lock_ttl_ms` in `[120_000, 3_600_000]` ms), ensuring a transient `409 Conflict` leaves the operator's token unburned for retry (`R18-N3`).

### 3.3 Execution, Pond & Risk Hardening
- **Post-Only `Alo` BBO Parser & Single-Shot Passive Retry (`R19-M2`)**: When Hyperliquid rejects a post-only `Alo` order with `"Post only order would have immediately matched. bbo was <bid>@<ask>"`, `submit_hyperliquid_order_live` extracts `(bid, ask)` directly from the rejection message via `parse_hl_rejection_bbo` (avoiding a redundant `metaAndAssetCtxs` HTTP round-trip on the fast path), computes a strictly passive tick-aligned retry price via `compute_alo_retry_passive_limit_price`, logs `ALO_IMMEDIATE_MATCH_PASSIVE_RETRY`, and dispatches a single `-retry1` passive maker order.
- **Atomic Protective Stop Replacement & Belt-and-Braces Cleanup (`R17-L1`, `R18 Section 8`)**: Exchange-native trigger stop updates submit the replacement stop order before canceling the prior `protective_stop_oid`. Precondition checks scope `protective_stop_oid` queries to active positions (`status IN ('OPEN', 'PENDING')` on `trades`; `state IN ('ARMED', 'OPEN', 'TRAILED')` on `pond_cells`), and `cancel_resting_trade`, `close_trade`, and `close_open_pond_cell` unconditionally set `protective_stop_oid = NULL` in SQL alongside explicit `set_*_protective_stop_oid(..., None)` calls in `aegis-execution` and `aegis-pond`.
- **Book A (`PIT`) Rejection Telemetry & HLP Vault Guardrails (`R16-L3`, `R19-L3`)**: All four Pit entry rejection paths emit structured `WARN` logs and `engine.execution` telemetry (`PIT_ENTRY_RISK_REJECTED`, `PIT_ENTRY_VAULT_MARGIN_CAPPED`, `PIT_ENTRY_EXECUTION_REJECTED`) in live/paper runs. Automated HLP vault withdrawals (`hlp_vault_auto_rebalance_enabled`) default to `false` and emit `HLP_VAULT_AUTO_REBALANCE_INITIATED` when opted into.
- **Book B (`POND`) Relative 1H ATR Floor for Sub-Dollar Assets (`R20-M1`)**: `compute_wick_context_with_stops_and_atr_floor` applies a relative 1H ATR floor `pond_min_atr_pct = 0.0015` (`15 bps` of reference close/mark price, clamped to `[1e-6, 0.10]`, with `1e-8` numerical safety floor) at both `pre_open_atr_1h` and `atr_14_1h` call sites instead of a fixed `$0.01` floor. Sub-dollar tokens (e.g., `WLFI` at `~$0.0457` or `MON` at `~$0.0159`) retain accurate ~`0.56%` Tier-1 wick buffers (`0.85 * ATR_1H`) and `stop_dist > wick_buf`.
- **Book B (`POND`) 4H-Close Chandelier Ratchet, Dead-Zone Note & `POND_FROZEN_TRAIL` Telemetry (`R20-L1`, `R20-N1`)**:
  - `update_cell_mark_and_chandelier` ratchets `chandelier_stop` monotonically on 4H bar closes (`3.8 * ATR_4H`), transitioning `OPEN -> TRAILED` and triggering atomic `submit_or_refresh_protective_stop_order` sync.
  - **Trailing-Stop Dead-Zone & `ME-2` Sequencing (`R20-N1`)**: Because `initial_stop = 2.5 * ATR_1H(entry)` while `chandelier_stop = 3.8 * ATR_4H(current)` with `ATR_4H >= 1.8 * ATR_1H`, early gains remain inside the dead zone where the raw Chandelier candidate stays clamped at `initial_stop`. Unifying `initial_stop` with `t=0` Chandelier (`Option (b)`) alters cell sizing (`risk_budget / stop_distance`) and remains gated behind `ME-2` backtest stop-fidelity closure plus walk-forward validation.
  - **Frozen-Trail Observability (`R20-L1`)**: `PondOrchestrator` tracks consecutive 4H closes where a cell is in profit (`mark > entry` for `LONG`, `mark < entry` for `SHORT`) while `state == "OPEN"` and `chandelier_stop` remains pinned at `initial_stop` (persisted in `system_config` key `pond_frozen_trail_closes_json`). When the streak reaches `pond_frozen_trail_warn_closes` (default `6`), it emits a structured `WARN` log and `POND_FROZEN_TRAIL` telemetry event (`pond.ratchet`), and resets the counter as soon as the stop ratchets (`OPEN -> TRAILED`) or the cell closes.

---

## 4. Build, Configuration & CLI Reference

### 4.1 Build & Install Release Binary

```bash
cargo build --release --workspace
cp target/release/aegis-quant bin/aegis-quant
```

### 4.2 Global Flags & Subcommands (`aegis-quant`)

Global options accepted before or after any subcommand:
- `--db <PATH>` (alias `--db-path <PATH>`): Path to SQLite WAL database (default `data/aegis.db`; production convention `data/aegis_quant.db`).
- `--book <pit|pond|both>`: Active book mode for `run` (default `both`).
- `--once`: Run a single ingestion + signal + execution cycle and exit (`cli-run-once.pid<pid>` cycle lock).
- `--calibrate`: Run walk-forward timeframe calibration (`1H` vs `4H`) before executing the cycle.
- `--host <IP>` / `--port <PORT>`: WebUI bind address and port (default `127.0.0.1:8000`). Non-loopback binding requires `--allow-remote-bind`, `AEGIS_ALLOW_REMOTE_BIND=1`, or `web_allow_remote_bind=true`.
- `--no-web`: Disable the embedded Axum WebUI server.
- `--json`: Emit machine-readable JSON output.

```bash
# 1. Start the Dual-Book Daemon + 4-Page Axum WebUI Server
./bin/aegis-quant --db data/aegis_quant.db run --host 127.0.0.1 --port 8000 --book both

# 2. Run a single evaluation cycle (optionally with walk-forward calibration)
./bin/aegis-quant --db data/aegis_quant.db run --once --book both
./bin/aegis-quant --db data/aegis_quant.db run --once --calibrate

# 3. Book B (The Pond) 80-coin universe refresh & hourly S-Cycle scan
./bin/aegis-quant --db data/aegis_quant.db pond --refresh-universe
./bin/aegis-quant --db data/aegis_quant.db pond --scan

# 4. Daily 00:05 UTC 50/50 Cross-Book Settling (dry-run simulation or live execution)
./bin/aegis-quant --db data/aegis_quant.db settle --dry-run
./bin/aegis-quant --db data/aegis_quant.db settle --dry-run --initial-split 5000:5000 --rich-idle-usdc 500
./bin/aegis-quant --db data/aegis_quant.db settle

# 5. Database lifecycle, encryption, backup/restore & ledger maintenance
./bin/aegis-quant --db data/aegis_quant.db db --init
./bin/aegis-quant --db data/aegis_quant.db db --migrate
./bin/aegis-quant --db data/aegis_quant.db db --test-encryption
./bin/aegis-quant --db data/aegis_quant.db db --generate-key
./bin/aegis-quant --db data/aegis_quant.db db --rotate-key --old-key-file /path/to/old.key --new-key-file /path/to/new.key
./bin/aegis-quant --db data/aegis_quant.db db --backup
./bin/aegis-quant --db data/aegis_quant.db db --restore data/backups/aegis_quant_YYYYMMDD_HHMMSS_000.db --confirm
./bin/aegis-quant --db data/aegis_quant.db db --restore data/backups/aegis_quant_YYYYMMDD_HHMMSS_000.db --confirm --force
./bin/aegis-quant --db data/aegis_quant.db db --audit-dry-run-residue
./bin/aegis-quant --db data/aegis_quant.db db --reclassify-dry-run-before 2026-11-08T00:00:00Z
./bin/aegis-quant --db data/aegis_quant.db db --show-web-token

# 6. 365-Day Point-in-Time Walk-Forward Backtest & Tear Sheet
./bin/aegis-quant --db data/aegis_quant.db backtest --days 365
./bin/aegis-quant --db data/aegis_quant.db backtest --days 365 --synthetic

# 7. Real-Time Telegram/Discord Alerting Verification
./bin/aegis-quant --db data/aegis_quant.db alerts --test

# 8. 6-Gate (21-Check) Institutional Mainnet Go/No-Go Readiness Audit
./bin/aegis-quant --db data/aegis_quant.db readiness
./bin/aegis-quant --db data/aegis_quant.db readiness --mode TESTNET
```

### 4.3 REST API & SSE Endpoints (`crates/aegis-web`)

- **Health & Status (Unauthenticated Probe)**: `GET /health`, `GET /api/v1/health` (exposes `status`, `schema_version`, `data_fresh`, `last_bar_timestamp_ms`, `max_bar_staleness_ms`, `pool_generation`, `discarded_stale_connections`, `cycle_lock_renew_failures`), `GET /api/v1/status`.
- **Authentication**: `POST /api/v1/auth/login`, `POST /api/v1/auth/logout`.
- **Dashboard, Books & Real-Time Stream**: `GET /api/v1/dashboard`, `GET /api/v1/portfolio`, `GET /api/v1/pond`, `GET /api/v1/pond/universe`, `GET /api/v1/pond/cells`, `GET /api/v1/pond/cycles`, `POST /api/v1/pond/scan`, `POST /api/v1/pond/cells/close`, `GET /api/v1/settle`, `POST /api/v1/settle/run`, `GET /api/v1/signals`, `GET /api/v1/trades`, `POST /api/v1/trades/close`, `GET /api/v1/logs`, `GET /api/v1/stream` (3-second SSE stream emitting digest-deduplicated `event: heartbeat` up to 5 ticks and full `message` state at least every 18 seconds, `R16-N2`).
- **Engine, Config, Wallet & Guarded Backup/Restore**: `POST /api/v1/engine/cycle`, `POST /api/v1/engine/backtest`, `POST /api/v1/engine/calibrate`, `GET|POST /api/v1/config`, `POST /api/v1/config/batch`, `GET|POST /api/v1/credentials`, `GET|POST /api/v1/alerts`, `POST /api/v1/alerts/test`, `POST /api/v1/wallet/sync`, `POST /api/v1/db/backup`, `GET /api/v1/db/backups`, `POST /api/v1/db/restore`.

---

## 5. Operational Runbooks

### 5.1 Database Initialization, Migration, Backup/Restore & Fernet Key Rotation
- **Initialize & Migrate (`db --init` / `db --migrate`)**: Both commands idempotently apply `sql/schema_and_seed.sql` and compiled incremental migrations in `crates/aegis-db/src/schema_ops.rs` to bring any fresh or historical database to Schema `v16`. Section 6f of `sql/schema_and_seed.sql` normalizes legacy `>= $5,000` or `< $1,000` values of `pond_testnet_min_vol_cap_usdc` to `$1,000` while preserving custom operator values strictly inside `(1000.0, 5000.0)` (`R16-N3`).
- **CLI Backup & Restore (`db --backup` / `db --restore <PATH> --confirm [--force]`)**:
  - Stop running `aegis-quant` / `aegis-pit` / `aegis-pond` daemons before executing a CLI restore. By default (`--force` omitted), `restore_sqlite_backup_with_options` runs `detect_live_database_activity()` and refuses the restore if another PID holds the DB/WAL/SHM open in `/proc`, if an unexpired `engine_cycle_lock` lease exists, or if a non-empty `<db>-shm` was modified within the last 120s.
  - The restore flow verifies the backup via `PRAGMA integrity_check;`, writes a pre-restore safety snapshot (`aegis_quant_pre_restore_<ts>.db`), increments `pool_generation` under `file_swap_lock.write()`, copies pages in `256`-page steps, clears any backup-inherited cycle lock (`R18-N2`, `R19-N1`), re-runs migrations, and verifies `PRAGMA quick_check;`.
- **Fernet Master-Key Rotation (`db --rotate-key`)**:
  ```bash
  ./bin/aegis-quant --db data/aegis_quant.db db --generate-key > data/.aegis_master_key.new
  chmod 600 data/.aegis_master_key.new
  ./bin/aegis-quant --db data/aegis_quant.db db --rotate-key \
    --old-key-file data/.aegis_master_key \
    --new-key-file data/.aegis_master_key.new
  mv data/.aegis_master_key.new data/.aegis_master_key
  ./bin/aegis-quant --db data/aegis_quant.db db --test-encryption
  ```
- **Read-Only Web Token Inspection (`db --show-web-token`)**: Strictly read-only (`R4-N8`); decrypts and prints `web_api_token` to `stdout` without mutating rows or keys. Do not run in shared or unredacted logs.

### 5.2 Single-Wallet Virtual Settling vs Two-Subaccount L1 Settling (`NB-1` & `R20-N3`)
- **`PAPER` Mode (Default Virtual Ledger)**: When `execution_mode = 'PAPER'`, daily `00:05 UTC` settling (`aegis-settle`) always rebalances book equity via the internal SQLite `book_transfers` ledger (`VIRTUAL_SINGLE_WALLET_LEDGER`) by default.
- **Single-Wallet `TESTNET` / `LIVE` Mode — Option 2B (`settling_allow_single_wallet_virtual`)**:
  - When running `TESTNET` or `LIVE` on a single Hyperliquid wallet where `pit_subaccount_address` and `pond_subaccount_address` are both empty, `ExecutionEngine::execute_subaccount_transfer_for_record` refuses virtual ledger rebalancing by default so operators never unknowingly run live trading without on-chain subaccount separation.
  - To explicitly opt into single-wallet virtual book rebalancing (**Option 2B**, e.g. on Hyperliquid Testnet where subaccount creation is unavailable or when intentionally sharing one wallet margin pool across both books), enable `settling_allow_single_wallet_virtual` directly in SQLite and verify via `readiness`:
    ```bash
    sqlite3 data/aegis_quant.db "UPDATE system_config SET config_value='true' WHERE config_key='settling_allow_single_wallet_virtual';"
    ./bin/aegis-quant --db data/aegis_quant.db readiness
    ```
  - **Why Immutable in the Web API**: `settling_allow_single_wallet_virtual`, `pit_subaccount_address`, and `pond_subaccount_address` are listed in `IMMUTABLE_SECURITY_CONFIG_KEYS` (`crates/aegis-core/src/lib.rs`) so a compromised dashboard session or API bearer token cannot silently downgrade L1 subaccount settling into virtual ledger transfers or redirect funds to an unauthorized address.
- **Two-Subaccount L1 Settling for Mainnet Isolation**:
  - For hard L1 margin isolation between Book A (`PIT`) and Book B (`POND`) on Mainnet, configure distinct Hyperliquid sub-account EVM addresses in `pit_subaccount_address` and `pond_subaccount_address` and keep `settling_allow_single_wallet_virtual = 'false'`.
  - `SettleController` will then execute real 2-leg L1 `subAccountTransfer` transactions (`from_subaccount -> Master wallet -> to_subaccount`) with crash-safe Leg-1 journaling (`legs_completed`, `leg1_tx_ref`).

### 5.3 Daemon Cadence, Accounting Identity & Testnet Faucet Offset
- **Loop Cadence Keys**: `pit_tick_interval_seconds` (default `900`s for Book A evaluation), `pond_tick_interval_seconds` (default `15`s for Book B Tier-1 wick monitoring, 4H close ratchets, and hourly S-Cycle scans), and `polling_interval_seconds` (default `15`s top-level wake ceiling). Periodic maintenance (`maybe_run_maintenance`) is wall-clock driven: exchange reconciliation runs every tick in `LIVE`/`TESTNET` and every `5` minutes in `PAPER`; retention pruning runs hourly.
- **Dual-Book Accounting Identity**: `total_net_pnl_usdc = round_dp(realized_pnl_usdc + unrealized_pnl_usdc + funding_pnl_usdc - open_fees_usdc + yield_pnl_usdc, 4)` is unfloored, while `book_equity_usdc = round_dp((initial_capital_usdc + transfer_adj_usdc + total_net_pnl_usdc).max(0.0), 2)` applies a `.max(0.0)` solvency floor for risk-sizing denominators (`R10-QA`, `R11-QA`).
- **Testnet Faucet Spot Offset & Conservative Live Equity (`R9-QA`)**: `testnet_faucet_offset_usdc` (default `10000.0`) automatically subtracts the unallocated `$10,000` Hyperliquid Testnet Spot faucet grant from unified equity when `initial_equity_usdc <= testnet_faucet_offset_usdc`, and live reconciliation adopts `min(live_wallet_equity, accounting_equity)` so unallocated Spot balances never inflate risk caps.

---

## 6. Deployment (`deploy/` Systemd Units) & 6-Gate (`21`-Check) Mainnet Readiness Suite

### 6.1 Hardened Systemd Units (`deploy/`)

The `deploy/` directory provides 5 hardened `systemd` units plus an automated Fedora/Linux bootstrap script (`deploy/bootstrap_vm.sh`):

| File | Role |
| --- | --- |
| `deploy/aegis-quant.service` | Unified daemon (`aegis-quant --db .../data/aegis_quant.db run --host 127.0.0.1 --port 8000 --book both`) with `Conflicts=aegis-pit.service aegis-pond.service`. |
| `deploy/aegis-pit.service` | Standalone Book A (`--book pit --no-web`) service for split-daemon deployments (`Conflicts=aegis-quant.service`). |
| `deploy/aegis-pond.service` | Standalone Book B (`--book pond --no-web`) service for split-daemon deployments (`Conflicts=aegis-quant.service`). |
| `deploy/aegis-settle.service` | Oneshot daily settling unit (`aegis-quant --db .../data/aegis_quant.db settle`). |
| `deploy/aegis-settle.timer` | Daily `OnCalendar=*-*-* 00:05:00 UTC` (`Persistent=true`) timer triggering `aegis-settle.service` (`R19-L4`). |
| `deploy/bootstrap_vm.sh` | Automated provisioning script that configures `chronyd` and `firewalld`, generates a `0600` Fernet master key, initializes/migrates Schema `v16`, installs all 5 systemd units (`chmod 644`), enables `aegis-settle.timer` and `aegis-quant.service`, and verifies `/api/v1/health`. |

### 6.2 Six-Gate (`21`-Check) Institutional Readiness Suite (`aegis-quant readiness`)

Implemented in `run_readiness_suite` (`crates/aegis-cli/src/main.rs`), all **21 checks across 6 Gates** must pass (`0 FAIL`, plus `SEC-01` and `SEC-03` `PASS` in `LIVE`/`TESTNET`) to earn a `GO FOR TESTNET` / `GO FOR LIVE` verdict:

| Gate | Check ID | Check Name | Requirement Summary |
| --- | --- | --- | --- |
| **Gate 1: Database & Schema** | `DB-01` | SQLite WAL Mode & Foreign Keys | `journal_mode=wal`, `foreign_keys=1` |
| **Gate 1: Database & Schema** | `DB-02` | Schema Migration Version | `schema_version >= 16` (`CANONICAL_SCHEMA_VERSION`) |
| **Gate 1: Database & Schema** | `DB-03` | 31-Instrument Universe Seeded | `16 USDC_PERP` + `15 BTC_SYNTH` (`15 Alt Pairs`) |
| **Gate 2: Security & Key Mgmt** | `SEC-01` | Fernet Master Key Isolation | External `AEGIS_MASTER_KEY` / `LoadCredential` or `chmod 600` `.aegis_master_key` |
| **Gate 2: Security & Key Mgmt** | `SEC-02` | Fernet AES-128 Encryption at Rest | `0` plaintext sensitive keys on disk; `AES-128-CBC + HMAC-SHA256` round-trip verified |
| **Gate 2: Security & Key Mgmt** | `SEC-03` | Trade-Only Agent Wallet & Key | Valid 42-char `0x` `wallet_address` + 64-hex secp256k1 `private_key` + signer binding (`hyperliquid_testnet=false` in `LIVE`) |
| **Gate 2: Security & Key Mgmt** | `SEC-04` | Backfill Dataset SHA-256 Provenance | Exact SHA-256 match on `data/real_4h_backfill.json` against `data/real_4h_backfill.json.sha256` |
| **Gate 3: Risk Guardrails** | `RISK-01` | Institutional Capital & Cap Alignment | Heat = `4.0%` book equity, Single-Asset = `25%` book equity, Gross Leverage = `1.50x` book equity |
| **Gate 3: Risk Guardrails** | `RISK-02` | Two-Tier Chandelier Stop Parameters | Valid tight-trail or classic Chandelier ATR multipliers and wick buffer bounds in `system_config` |
| **Gate 3: Risk Guardrails** | `RISK-03` | Live Guardrail Rejection Self-Test | `RiskEngine::evaluate_order_risk` approves valid order `<= 25%` cap and rejects order when heat `>= 4.0%` |
| **Gate 4: Operational Readiness** | `OPS-01` | Real-Time Alert Channel & Retry Queue | Loopback self-test `DELIVERED` + Telegram/Discord webhook configured for `LIVE` |
| **Gate 4: Operational Readiness** | `OPS-02` | Order FSM & Audit Trail Integrity | `PENDING -> OPEN_ON_BOOK -> PARTIALLY_FILLED -> FILLED` transitions logged in `order_state_audit` |
| **Gate 4: Operational Readiness** | `OPS-03` | Exchange Position Drift Reconciliation | `reconcile_exchange_state` detects and reconciles simulated orphan/phantom/size-drift positions |
| **Gate 5: Quant Acceptance** | `QUANT-01` | Out-of-Sample Annualized Sharpe Ratio | 365-day walk-forward `Annualized Sharpe >= 1.20` (with explicit `stop_regime_label`, `active_stop_set`, and `fired_stop_substitutions`) |
| **Gate 5: Quant Acceptance** | `QUANT-02` | Out-of-Sample Maximum Drawdown (MDD) | 365-day walk-forward `Maximum Drawdown <= 20.0%` |
| **Gate 5: Quant Acceptance** | `QUANT-03` | Out-of-Sample Profit Factor | 365-day walk-forward `Profit Factor >= 1.30` |
| **Gate 5: Quant Acceptance** | `QUANT-04` | In-Memory Backtest DB Isolation | 365-day backtest executes in `:memory:` without mutating live SQLite WAL `trades` |
| **Gate 6: Deployment & Backup** | `DEPLOY-01` | Systemd Settle Unit Argument Parity | `deploy/aegis-settle.service` `ExecStart` invokes `aegis-quant settle` without invalid `--execute` flag |
| **Gate 6: Deployment & Backup** | `DEPLOY-02` | Systemd Mutual-Exclusion Conflicts | `deploy/aegis-quant.service` declares `Conflicts=aegis-pit.service aegis-pond.service` |
| **Gate 6: Deployment & Backup** | `DEPLOY-03` | SQLite VACUUM INTO Backup Freshness | Writable `data/backups/` directory with verified snapshot `<= backup_max_age_hours` (`36h`) old |
| **Gate 6: Deployment & Backup** | `DEPLOY-04` | Systemd Settle Timer & 5-Unit Bootstrap | All 5 `deploy/aegis-*.{service,timer}` units present, installed by `deploy/bootstrap_vm.sh` (`enable --now aegis-settle.timer`), and present in `/etc/systemd/system/` when host service is installed (`R19-L4`) |

---

## 7. Verification & Workspace Test Inventory (`172` Tests Across 13 Crates)

### 7.1 Standard Verification Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace -- --test-threads=4
./bin/aegis-quant --db data/aegis_quant.db readiness
```

### 7.2 Per-Crate Test Suite Inventory (`172` Passing Tests)

All **172** unit and integration tests across the 13 workspace crates pass with zero ignored or skipped cases, enforced in lockstep (`>= 172`) by `.github/workflows/ci.yml` (`R20-L2`):

| Crate / Target | Tests | Coverage Highlights |
| --- | ---: | --- |
| `aegis-core` (`lib`) | 10 | Domain types, step/tick rounding, dynamic open-heat helpers, bare-hex & Gemini key redaction |
| `aegis-db` (`33` `lib` + `1` `aegis-db-tool` `bin`) | 34 | Fernet AES-128 encryption, Schema `v16` migrations, WAL pool generation epochs (`R16-M1`), `/proc` & read-only open restore guards (`R16-M2`, `R17-M1`, `R17-L3`, `R19-L1`, `R19-L2`), non-reentrant cycle lock & 90-tick renewal heartbeat (`R18-L1`, `R19-M1`, `R19-N1`, `R19-N2`, `R20-N2`), streaming SHA-256 (`R17-L2`) |
| `aegis-signals` (`lib`) | 9 | Causal 3-State Gaussian HMM, MACE momentum, SNR ensemble, intrabar hysteresis margin & `prev_ema_v` cache persistence |
| `aegis-risk` (`lib`) | 8 | Dynamic open heat, two-tier Chandelier stops, 20-asset correlation-cluster map & `OTHER` cluster cap |
| `aegis-alerts` (`lib`) | 5 | Token redaction, 2-cycle `RegimeFlip` confirmation, per-channel exponential-backoff retry, chat-id preservation |
| `aegis-telemetry` (`lib`) | 2 | Bounded async SQLite log queue & broadcast bus |
| `aegis-execution` (`lib`) | 32 | EIP-712 signing, stale-oracle IOC clamping, post-only `Alo` BBO passive retry (`R19-M2`), Pit rejection telemetry (`R19-L3`), atomic submit-then-cancel stops (`R17-L1`), `userFills` Fix-4 promotion true-up, `cancelByCloid`, HLP rebalance (`R16-L3`), `ME-12` leg-1 journal |
| `aegis-pit` (`lib`) | 3 | Scoped Book A execution cycle & freeze-window Chandelier trailing |
| `aegis-pond` (`lib`) | 22 | `NPS-01` 6-gate scanner, 10-state FSM & 6-tier exits, relative `0.0015` 1H ATR floor for sub-dollar assets & large-ratchet stop sync (`R20-M1`), `POND_FROZEN_TRAIL` consecutive-close telemetry & ratchet reset (`R20-L1`), synthetic-refresh guard, persisted exit retry counters |
| `aegis-settle` (`lib`) | 13 | PP-50 idle-only settling, single-wallet virtual settling gate (`settling_allow_single_wallet_virtual`, `NB-1`, `R20-N3`), `HaltedPp50` vs `SkippedNoIdle`, funding-offset `pond_heat`, `PARTIAL`/`AMBIGUOUS` transfer quarantine |
| `aegis-backtest` (`lib`) | 4 | Zero-lookahead 365-day replay, taker default + entry slippage parity, `stop_regime_label` / `active_stop_set` / `fired_stop_substitutions` disclosure (`R19-N3`) |
| `aegis-web` (`lib`) | 18 | Session-nonce cookie auth, SSE `DefaultHasher` `heartbeat`, backup cooldown, two-step guarded WebUI restore, per-IP token cap (`R17-N2`), lock-conflict token preservation (`R18-N3`), health pool & cycle-lock metrics (`R17-N1`, `R19-N2`) |
| `aegis-cli` (`6` `aegis-quant` `bin` + `6` `test_firewall_ast`) | 12 | CLI flag validation, Firewall Invariants `F1`–`F6` AST & runtime checks, `R16-U2` secret hygiene scan, readiness `DEPLOY-01..04` (`R19-L4`), shared backup cadence & `--force` restore |
| **Total Workspace** | **172** | Enforced as minimum floor (`>= 172`) in `.github/workflows/ci.yml` (`R20-L2`) |
