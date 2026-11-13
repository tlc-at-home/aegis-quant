# Aegis-Quant v0.3.0-rc1 — Institutional PP-50 Dual-Book Trading System (Rust Workspace)

An autonomous, single-tenant quantitative perpetuals trading platform built in Rust (2021 edition, Tokio async runtime, Axum web server, rusqlite bundled SQLite WAL) for **Hyperliquid L1**.

---

## Architecture Overview (PP-50 Dual-Book)

1. **Workspace Layout (`crates/*`)**:
   - `aegis-core`: Shared domain types (`BookId`, `Side`, `HmmRegime`, `SignalAction`, `PondCellState`, `BookTransferStatus`, `OhlcvBar`, `Usdc`, `Price`, `Quantity`, `Leverage`, `Bps`), telemetry secret redaction (`sanitize_telemetry_message`, `sanitize_telemetry_details`), timeframe validators, and financial math (`calculate_sharpe_sortino_calmar`, `compute_chandelier_stop`, `round_step`, `accrue_chop_basis_harvest_4h`).
   - `aegis-db`: Bundled SQLite WAL manager (`rusqlite`), canonical single-script schema version v14 initialization (`sql/schema_and_seed.sql`), Fernet secret encryption (`AEGIS_MASTER_KEY`), advisory cycle lock (`DbCycleLockGuard`), online `VACUUM INTO` backups, and `BookScopedDb` compile-time book isolation.
   - `aegis-signals`: 3-State Gaussian HMM regime filter (`BULL`, `BEAR`, `CHOP`), MACE multi-grid momentum indicators, and Continuous Signal-to-Noise Ratio (SNR) ensemble.
   - `aegis-risk`: Risk sizing (`0.50%`–`1.50%` dynamic risk per trade), `4.0%` portfolio heat cap, `1.50x` gross leverage cap, `25%` single-asset concentration cap, and `35%` correlation-cluster concentration cap.
   - `aegis-execution`: Hyperliquid L1 REST/EIP-712 order router (`signing`, `http_client`, `ingestion`, `live_dispatch`, `manual_close`, `reconcile`, `cycle`, `resting_reconcile`), live mark-to-market synchronization, and config validator.
   - `aegis-pit`: Book A (The Pit) directional BTC/Altcoin strategy engine.
   - `aegis-pond`: Book B (The Pond) Champion `NPS-01` Naked-Option Perpetual Analogue Scanner across an 80-coin dynamic universe (10-state cell FSM, 6-tier exit priority engine, two-sided structural exhaustion gate, funding floor & 2h persistence gate).
   - `aegis-settle`: Daily `00:05 UTC` 50/50 Cross-Book Settling Engine enforcing Invariant F5 (`available_idle_usdc` transfer cap), per-leg L1 progress tracking, `PARTIAL`/`AMBIGUOUS` quarantine, and atomic SQLite transactions.
   - `aegis-backtest`: 365-Day Walk-Forward Backtest & Tear Sheet simulator (`0.35%` round-trip friction + 1H funding accrual).
   - `aegis-alerts`: Telegram Bot API and Discord webhook alerting dispatcher with redacted error handling and bounded retry queue.
   - `aegis-telemetry`: Inter-task communication bus (`TelemetryBus`) with background SQLite event writer and snapshot broadcasting.
   - `aegis-web`: Axum REST API (`/api/v1/*`, including `/api/v1/db/backup`), real-time Server-Sent Events (`/api/v1/stream`), and 4-page institutional dark-mode dashboard (`web/static/index.html` + `web/static/app.js`).
   - `aegis-cli`: Unified binary (`aegis-quant`) supporting `run`, `pond`, `settle`, `db`, `backtest`, `alerts`, and `readiness` subcommands.

---

## Quick Start

### 1. Build & Test Workspace

```bash
cargo build --release --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### 2. Initialize / Upgrade SQLite Database (Schema v14)

Both `--db <PATH>` and `--db-path <PATH>` are accepted globally across all subcommands (built binaries are output to `./target/release/aegis-quant` and installed to `./bin/aegis-quant`):

```bash
./target/release/aegis-quant --db data/aegis_quant.db db --init
./target/release/aegis-quant --db data/aegis_quant.db db --migrate
./target/release/aegis-quant --db data/aegis_quant.db db --backup
./target/release/aegis-quant --db data/aegis_quant.db db --audit-dry-run-residue
./target/release/aegis-quant --db data/aegis_quant.db db --show-web-token
```

> **Security Note (`--show-web-token`)**: `db --show-web-token` is strictly read-only (`R4-N8`)—it never creates or mutates database rows or keys, and prints the decrypted `web_api_token` bearer secret to `stdout`. Do not run it in shared terminals or unredacted CI logs.

All SQLite WAL tables, columns, views, indexes, triggers, and seed data (Schema `v14`) are consolidated in the single canonical schema build script `sql/schema_and_seed.sql` (separate `sql/migrations/*.sql` files have been removed and folded into `sql/schema_and_seed.sql`). Both `db --init` and `db --migrate` idempotently initialize fresh databases and upgrade existing databases to Schema `v14`.

### 3. Run the 4-Page Web Dashboard & Dual-Book Background Daemons

```bash
./target/release/aegis-quant --db data/aegis_quant.db run --host 127.0.0.1 --port 8000 --book both
```

Then open `http://127.0.0.1:8000` in your browser and authenticate via the dashboard session banner (`POST /api/v1/auth/login` -> `HttpOnly; SameSite=Strict` session cookie).

### 4. CLI Subcommands

```bash
# Run a single dual-book evaluation cycle (optionally with walk-forward calibration)
./target/release/aegis-quant --db data/aegis_quant.db run --once --book both
./target/release/aegis-quant --db data/aegis_quant.db run --once --calibrate

# Refresh the 80-coin Pond universe or run an hourly S-Cycle scan
./target/release/aegis-quant --db data/aegis_quant.db pond --refresh-universe
./target/release/aegis-quant --db data/aegis_quant.db pond --scan

# Run Daily 00:05 UTC 50/50 Settling (dry-run or live)
./target/release/aegis-quant --db data/aegis_quant.db settle --dry-run

# Verify operational alert webhooks
./target/release/aegis-quant --db data/aegis_quant.db alerts --test

# Run 365-day walk-forward tear sheet simulation and institutional readiness audit
./target/release/aegis-quant --db data/aegis_quant.db backtest --days 365
./target/release/aegis-quant --db data/aegis_quant.db readiness
```

### 5. Operational Cadence & Cross-Book Settling Runbook

- **Loop Cadence Keys (`system_config`, `R5-M3`, `R5-QB`, `R6-N1`, `R6-N5`, `R7-S5`)**:
  - `pit_tick_interval_seconds` (default `900`): Book A (`PitOrchestrator`) evaluation cadence. In `--book pit` mode (`aegis-pit.service`), the daemon sleeps for `pit_tick_interval_seconds` between cycles and reloads the key from `system_config` on every tick (`R5-N3`). In `--book both` mode (`aegis-quant.service`), full Book A cycles (`ingest_universe` + `evaluate_universe` + `reconcile_cycle_with_options`, including Chandelier trailing-stop ratchets and regime exit evaluations) execute on Cycle #1 and whenever `pit_tick_interval_seconds` has elapsed since `last_pit_cycle_ms`. Operators should note that Book A position management reacts at `pit_tick_interval_seconds` granularity (up to 900s), whereas Book B position management (wick-stop monitoring, Chandelier trailing, and resting-order state) and LIVE/TESTNET exchange reconciliation (`maybe_run_maintenance`) evaluate at `pond_tick_interval_seconds` granularity (15s) (`R6-N1`, `R7-S5`).
  - `pond_tick_interval_seconds` (default `15`): Book B (`PondOrchestrator`) 15s Tier 1 wick-stop and resting-order evaluation cadence (with hourly S-Cycle scans on UTC hour boundaries and 4H close ratchets on 4H boundaries). Reloaded dynamically on every tick.
  - `polling_interval_seconds` (default `15`): Top-level wake cadence ceiling in `--book both` mode (`sleep_sec = min(pond_tick_interval_seconds, polling_interval_seconds, pit_tick_interval_seconds).max(1)`). Note the wake formula asymmetry (`R6-N5`): in `--book both` mode, the Pond step executes on every daemon wake iteration unless a Book A cycle is due. Consequently, `pond_tick_interval_seconds` only slows down Pond evaluations if it is configured smaller than `polling_interval_seconds`; setting `pond_tick_interval_seconds` higher than `polling_interval_seconds` will not throttle Pond ticks unless `polling_interval_seconds` is also increased.
  - **Time-Based Periodic Maintenance Triggers (`R6-N1`, `R7-S5`, `R8-N5`)**: Rather than relying on cycle-count multiples (which drift when `pit_tick_interval_seconds` is lengthened), periodic maintenance triggers (`maybe_run_maintenance`) in `AegisOrchestrator` evaluate elapsed wall-clock time across all daemon book modes (`--book pit`, `--book pond`, and `--book both`): exchange state reconciliation in PAPER mode runs every 5 minutes (`now_ms - last_reconcile_ms >= 300_000`) and surfaces any SQLite query failure as `ERROR_DB_UNAVAILABLE`, while LIVE/TESTNET reconciles on every daemon tick, emits `ERROR` telemetry if the exchange clearinghouse is unreachable (`SKIPPED_OFFLINE`) or the DB is unavailable (`ERROR_DB_UNAVAILABLE`), and triggers a critical alert (`EXCHANGE_RECONCILE_DEGRADED`) on consecutive reconcile failures (`reconcile_error_streak == 3` and every 20th streak thereafter); retention data pruning runs every 1 hour (`now_ms - last_retention_prune_ms >= 3_600_000`), preserving intended operational intervals regardless of cadence configuration.
  - **Mode-Dependent 15-Second Dashboard Mark-to-Market & `/health` Liveness (`PAPER` vs `LIVE`/`TESTNET`, `R11-N3`, `R12-N1`)**: 15-second dashboard mark-to-market liveness (`reconcile_exchange_state` updating open PIT trade `current_price`/`unrealized_pnl_usdc` and Pond live-candle refresh via `merge_recent_bars`) operates in `LIVE` and `TESTNET` modes, whereas in `PAPER` mode `reconcile_exchange_state` is throttled to every 5 minutes and Pond evaluates bar-close synthetic candles. Similarly, on `GET /health` and `GET /api/v1/health` (`api_health` in `crates/aegis-web/src/handlers.rs`), `data_fresh` in `LIVE`/`TESTNET` modes requires both `last_ms > 0` and `(now_ms - last_ms) <= max_bar_staleness_ms` (default `28_800_000` ms = 8 hours in `system_config`, fallback `21_600_000` ms = 6 hours) so stale market ingestion surfaces `503 Service Unavailable` (`status: "DEGRADED"`, `data_fresh: false`), while in `PAPER` mode `data_fresh` only requires `last_ms > 0` and `api_health` returns `200 OK` (`status: "ok"`).
- **Dual-Book Accounting Identity & `.max(0.0)` Solvency Floor (`R10-QA`, `R11-QA`, `R12-N2`)**:
  - Both `compute_pit_pnl_components` (`crates/aegis-db/src/trade_ops.rs`) and `compute_pond_pnl_components_with_marks` (`crates/aegis-db/src/pond_ops.rs`) compute `total_net_pnl_usdc = round_dp(realized_pnl_usdc + unrealized_pnl_usdc + funding_pnl_usdc - open_fees_usdc + yield_pnl_usdc, 4)` without any floor, preserving the full unfloored cumulative loss if a book goes underwater.
  - By contrast, `book_equity_usdc = round_dp((initial_capital_usdc + transfer_adj_usdc + total_net_pnl_usdc).max(0.0), 2)` applies a `.max(0.0)` solvency floor so risk-sizing and gross-leverage denominators never become negative; in all solvent states (`initial_capital_usdc + transfer_adj_usdc + total_net_pnl_usdc >= 0.0`), the exact additive identity `book_equity_usdc == round_dp(initial_capital_usdc + transfer_adj_usdc + total_net_pnl_usdc, 2)` holds.
- **Testnet Faucet Spot USDC Offset & Conservative Equity Rule (`R9-QA`, `R9-N2`)**:
  - **`testnet_faucet_offset_usdc` (default `10000.0`, category `risk`)**: Hyperliquid Testnet unified accounts receive an unallocated `$10,000.0` Spot USDC faucet grant alongside actual Perps test capital (e.g., `$1,000.0`). When running on Testnet (`is_testnet == true`) with `initial_equity_usdc <= testnet_faucet_offset_usdc` and `raw_spot_tot >= testnet_faucet_offset_usdc` (and no separate non-zero Perps clearinghouse account value), `ExecutionEngine::adjust_testnet_faucet_spot_usdc` automatically subtracts `testnet_faucet_offset_usdc` from the Spot USDC balance in both `fetch_hlp_vault_state` and `sync_wallet_equity`. This prevents the unallocated Testnet faucet grant from inflating `total_equity_usdc` or scaling up Book A/B risk caps by `10x`. Operators using a non-standard Testnet Spot grant can adjust or disable (`0.0`) `testnet_faucet_offset_usdc` via `system_config`.
  - **Conservative Live Equity Rule (`min(live_wallet_equity, accounting_equity)`)**: During `reconcile_cycle_with_options`, `total_equity_usdc` adopts the live on-chain wallet equity (`perp_equity_usdc + vault_equity_usdc`) **only when it is lower** than the book accounting identity (`initial_equity_usdc + realized_pnl + unrealized_pnl + pond_total_pnl + accrued_yield_usdc`). Unrecorded on-chain drawdowns, fees, or funding debits immediately reduce sizing equity, whereas unallocated Spot balances or external wallet transfers never silently inflate position sizing above the configured capital baseline.
  - **Raising Capital Baseline After a Real Deposit**: After depositing additional trading capital into the Hyperliquid Perps account, operators should explicitly raise `initial_equity_usdc` and rescale dollar risk caps via either:
    1. `POST /api/v1/config/batch` with `{"updates": {"initial_equity_usdc": <NEW_EQUITY_USDC>}, "scale_risk_caps": true}`, or
    2. `POST /api/v1/wallet/sync` from the Settings / Wallet Sync panel (which syncs `initial_equity_usdc` to the verified on-chain equity and invokes `scale_risk_caps_for_equity`).
- **Cross-Book Transfer Quarantine (`PARTIAL` / `AMBIGUOUS`)**:
  - Only `COMPLETED`, `CONFIRMED`, and live `PARTIAL_IDLE_CAPPED` rows in `book_transfers` are credited in virtual book equity (`BookTransferStatus::is_ledger_counted()`).
  - If a 2-leg subaccount transfer fails after Leg 1 (`attempts = 1`), it transitions to `PARTIAL` (USDC stranded in master wallet). If an L1 transfer times out post-broadcast (`POST_SEND_UNCERTAIN`), it transitions to `AMBIGUOUS`. Both states emit a throttled critical alert (`STRANDED_BOOK_TRANSFER`) until reconciled by an operator.
- **Pre-Live Dry-Run Residue Audit & Remediation (`R5-M2`, `R6-N4`)**:
  - Before executing the first live settle on a database upgraded from pre-v8 builds, audit `book_transfers` for any historical `PARTIAL_IDLE_CAPPED` rows that lack a `DRY_RUN` marker:
    ```bash
    ./target/release/aegis-quant --db data/aegis_quant.db db --audit-dry-run-residue
    ```
    Or run the SQL detection query directly:
    ```sql
    SELECT transfer_id, settle_date, amount_usdc, tx_ref, transfer_mode, created_at
    FROM book_transfers
    WHERE status = 'PARTIAL_IDLE_CAPPED'
      AND UPPER(COALESCE(transfer_mode, '')) NOT LIKE 'DRY_RUN%'
      AND LOWER(COALESCE(tx_ref, '')) NOT LIKE 'dryrun-%';
    ```
  - If any historical dry-run rows remain from pre-v8 test windows, remediate using either:
    1. **Operator CLI Reclassification (`R6-N4`)**: Reclassify historical `PARTIAL_IDLE_CAPPED` rows before an ISO-8601 UTC timestamp directly via CLI without raw SQL:
       ```bash
       ./target/release/aegis-quant --db data/aegis_quant.db db --reclassify-dry-run-before 2026-11-08T00:00:00Z
       # Or via standalone db tool:
       ./target/release/aegis-db-tool --db data/aegis_quant.db --reclassify-dry-run-before 2026-11-08T00:00:00Z
       ```
    2. **Migration Cutoff Reclassification**: Set `dry_run_reclassify_before_utc` in `system_config` to the ISO-8601 UTC cutoff timestamp before running `db --migrate`.
    3. **One-Step Ledger Baseline Reset**: Set `equity_baseline_reset_at` in `system_config` to the current ISO-8601 UTC upgrade timestamp (`YYYY-MM-DDTHH:MM:SSZ`), which rebases both books' ledgers and zeroes any prior historical transfer or PnL distortion in a single step.
