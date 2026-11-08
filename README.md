# Aegis-Quant v0.3.0-rc1 — Institutional PP-50 Dual-Book Trading System (Rust Workspace)

An autonomous, single-tenant quantitative perpetuals trading platform built in Rust (2021 edition, Tokio async runtime, Axum web server, rusqlite bundled SQLite WAL) for **Hyperliquid L1**.

---

## Architecture Overview (PP-50 Dual-Book)

1. **Workspace Layout (`crates/*`)**:
   - `aegis-core`: Shared domain types (`BookId`, `Side`, `HmmRegime`, `SignalAction`, `PondCellState`, `BookTransferStatus`, `OhlcvBar`, `Usdc`, `Price`, `Quantity`, `Leverage`, `Bps`), telemetry secret redaction (`sanitize_telemetry_message`, `sanitize_telemetry_details`), timeframe validators, and financial math (`calculate_sharpe_sortino_calmar`, `compute_chandelier_stop`, `round_step`, `accrue_chop_basis_harvest_4h`).
   - `aegis-db`: Bundled SQLite WAL manager (`rusqlite`), schema version v9 auto-migration (`001..009`), Fernet secret encryption (`AEGIS_MASTER_KEY`), and `BookScopedDb` compile-time book isolation.
   - `aegis-signals`: 3-State Gaussian HMM regime filter (`BULL`, `BEAR`, `CHOP`), MACE multi-grid momentum indicators, and Continuous Signal-to-Noise Ratio (SNR) ensemble.
   - `aegis-risk`: Risk sizing (`0.50%`–`1.50%` dynamic risk per trade), `4.0%` portfolio heat cap, `1.50x` gross leverage cap, `25%` single-asset concentration cap, and correlation cluster limits.
   - `aegis-execution`: Hyperliquid L1 REST/EIP-712 order router (`signing`, `http_client`, `ingestion`, `live_dispatch`, `reconcile`), live mark-to-market synchronization, and config validator.
   - `aegis-pit`: Book A (The Pit) directional BTC/Altcoin strategy engine.
   - `aegis-pond`: Book B (The Pond) Champion `NPS-01` Naked-Option Perpetual Analogue Scanner across an 80-coin dynamic universe (10-state cell FSM, 6-tier exit priority engine).
   - `aegis-settle`: Daily `00:05 UTC` 50/50 Cross-Book Settling Engine enforcing Invariant F5 (`available_idle_usdc` transfer cap), per-leg L1 progress tracking, `PARTIAL`/`AMBIGUOUS` quarantine, and atomic SQLite transactions.
   - `aegis-backtest`: 365-Day Walk-Forward Backtest & Tear Sheet simulator (`0.35%` round-trip friction + 1H funding accrual).
   - `aegis-alerts`: Telegram Bot API and Discord webhook alerting dispatcher with redacted error handling and bounded retry queue.
   - `aegis-telemetry`: Inter-task communication bus (`TelemetryBus`) with background SQLite event writer and snapshot broadcasting.
   - `aegis-web`: Axum REST API (`/api/v1/*`), real-time Server-Sent Events (`/api/v1/stream`), and 4-page institutional dark-mode dashboard (`web/static/index.html` + `web/static/app.js`).
   - `aegis-cli`: Unified binary (`aegis-quant`) supporting `run`, `pond`, `settle`, `db`, `backtest`, `alerts`, and `readiness` subcommands.

---

## Quick Start

### 1. Build Release Binaries

```bash
cargo build --release --workspace
```

### 2. Initialize / Migrate SQLite Database (Schema v9)

Both `--db <PATH>` and `--db-path <PATH>` are accepted globally across all subcommands (built binaries are output to `./target/release/aegis-quant` and installed to `./bin/aegis-quant`):

```bash
./target/release/aegis-quant --db data/aegis_quant.db db --init
./target/release/aegis-quant --db data/aegis_quant.db db --migrate
./target/release/aegis-quant --db data/aegis_quant.db db --show-web-token
```

> **Security Note (`--show-web-token`)**: `db --show-web-token` is strictly read-only (`R4-N8`)—it never creates or mutates database rows or keys, and prints the decrypted `web_api_token` bearer secret to `stdout`. Do not run it in shared terminals or unredacted CI logs.

Incremental SQL migrations are stored under `sql/migrations/`:
- `sql/migrations/001_v020_config_expansion.sql`
- `sql/migrations/002_v020_order_fsm.sql`
- `sql/migrations/003_v020_schema_versioning.sql`
- `sql/migrations/004_v020_rc2_remediation.sql`
- `sql/migrations/005_v030_pp50_two_book.sql`
- `sql/migrations/006_v030_review_remediation.sql`
- `sql/migrations/007_v030_round2_remediation.sql`
- `sql/migrations/008_v030_round3_remediation.sql`
- `sql/migrations/009_v030_round4_remediation.sql`

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

- **Loop Cadence Keys (`system_config`)**:
  - `polling_interval_seconds` (default `15`): Top-level synchronous orchestrator loop sleep cadence.
  - `pit_tick_interval_seconds` (default `900`): Background Tokio task interval for Book A (`PitOrchestrator`).
  - `pond_tick_interval_seconds` (default `15`): Background Tokio task interval for Book B (`PondOrchestrator` 15s Tier 1 wick-stop evaluation).
- **Cross-Book Transfer Quarantine (`PARTIAL` / `AMBIGUOUS`)**:
  - Only `COMPLETED`, `CONFIRMED`, and live `PARTIAL_IDLE_CAPPED` rows in `book_transfers` are credited in virtual book equity (`BookTransferStatus::is_ledger_counted()`).
  - If a 2-leg subaccount transfer fails after Leg 1 (`attempts = 1`), it transitions to `PARTIAL` (USDC stranded in master wallet). If an L1 transfer times out post-broadcast (`POST_SEND_UNCERTAIN`), it transitions to `AMBIGUOUS`. Both states emit a throttled critical alert (`STRANDED_BOOK_TRANSFER`) until reconciled by an operator.
  - To reset book equity baselines after a deposit/withdrawal or manual L1 reconciliation, update `equity_baseline_reset_at` in `system_config` to the current ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SSZ`).

