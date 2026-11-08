# Aegis-Quant v0.3.0-rc1 — Institutional PP-50 Dual-Book Trading System (Rust Workspace)

An autonomous, single-tenant quantitative perpetuals trading platform built in Rust (2021 edition, Tokio async runtime, Axum web server, rusqlite bundled SQLite WAL) for **Hyperliquid L1**.

---

## Architecture Overview (PP-50 Dual-Book)

1. **Workspace Layout (`crates/*`)**:
   - `aegis-core`: Shared domain types (`BookId`, `Side`, `HmmRegime`, `SignalAction`, `PondCellState`, `OhlcvBar`, `Usdc`, `Price`, `Quantity`, `Leverage`, `Bps`), constants, and financial math (`calculate_sharpe_sortino_calmar`, `compute_chandelier_stop`, `round_step`, `accrue_chop_basis_harvest_4h`).
   - `aegis-db`: Bundled SQLite WAL manager (`rusqlite`), schema version v8 auto-migration, Fernet secret encryption (`AEGIS_MASTER_KEY`), and `BookScopedDb` compile-time book isolation.
   - `aegis-signals`: 3-State Gaussian HMM regime filter (`BULL`, `BEAR`, `CHOP`), MACE multi-grid momentum indicators, and Continuous Signal-to-Noise Ratio (SNR) ensemble.
   - `aegis-risk`: Risk sizing (`0.50%`–`1.50%` dynamic risk per trade), `4.0%` portfolio heat cap, `1.50x` gross leverage cap, `25%` single-asset concentration cap, and correlation cluster limits.
   - `aegis-execution`: Hyperliquid L1 REST/EIP-712 order router (`signing`, `http_client`, `ingestion`, `live_dispatch`, `reconcile`), live mark-to-market synchronization, and config validator.
   - `aegis-pit`: Book A (The Pit) directional BTC/Altcoin strategy engine.
   - `aegis-pond`: Book B (The Pond) Champion `NPS-01` Naked-Option Perpetual Analogue Scanner across an 80-coin dynamic universe (8-state cell FSM, 6-tier exit priority engine).
   - `aegis-settle`: Daily `00:05 UTC` 50/50 Cross-Book Settling Engine enforcing Invariant F5 (`available_idle_usdc` transfer cap), per-leg L1 progress tracking, and atomic SQLite transactions.
   - `aegis-backtest`: 365-Day Walk-Forward Backtest & Tear Sheet simulator (`0.35%` round-trip friction + 1H funding accrual).
   - `aegis-alerts`: Telegram Bot API and Discord webhook alerting dispatcher with redacted error handling and bounded retry queue.
   - `aegis-telemetry`: Inter-task communication bus (`TelemetryBus`) with background SQLite event writer and snapshot broadcasting.
   - `aegis-web`: Axum REST API (`/api/v1/*`), real-time Server-Sent Events (`/api/v1/stream`), and 4-page institutional dark-mode dashboard (`web/static/index.html` + `web/static/app.js`).
   - `aegis-cli`: Unified binary (`aegis-quant`) supporting `run`, `db`, `pond`, `settle`, `backtest`, `calibrate`, `ingest`, `signals`, `alerts`, `vault`, `web`, and `readiness` subcommands.

---

## Quick Start

### 1. Build Release Binaries

```bash
cargo build --release --workspace
```

### 2. Initialize / Migrate SQLite Database (Schema v8)

```bash
./bin/aegis-quant --db-path data/aegis_quant.db db --status
./bin/aegis-quant --db-path data/aegis_quant.db db --show-web-token
```

Incremental SQL migrations are stored under `sql/migrations/`:
- `sql/migrations/001_v020_config_expansion.sql`
- `sql/migrations/002_v020_order_fsm.sql`
- `sql/migrations/003_v020_schema_versioning.sql`
- `sql/migrations/004_v020_rc2_remediation.sql`
- `sql/migrations/005_v030_pp50_two_book.sql`
- `sql/migrations/006_v030_review_remediation.sql`
- `sql/migrations/007_v030_round2_remediation.sql`
- `sql/migrations/008_v030_round3_remediation.sql`

### 3. Run the 4-Page Web Dashboard & Dual-Book Background Daemons

```bash
./bin/aegis-quant --db-path data/aegis_quant.db run --host 127.0.0.1 --port 8000 --book both
```

Then open `http://127.0.0.1:8000` in your browser and authenticate via the dashboard session banner (`POST /api/v1/auth/login` -> `HttpOnly; SameSite=Strict` session cookie).

### 4. CLI Subcommands

```bash
# Run a single dual-book evaluation cycle
./bin/aegis-quant --db-path data/aegis_quant.db run --once --book both

# Inspect Pond status or run an hourly 80-coin S-Cycle scan
./bin/aegis-quant --db-path data/aegis_quant.db pond --status
./bin/aegis-quant --db-path data/aegis_quant.db pond --scan-once

# Run Daily 00:05 UTC 50/50 Settling (dry-run or live)
./bin/aegis-quant --db-path data/aegis_quant.db settle --dry-run

# Run 365-day walk-forward tear sheet simulation and institutional readiness audit
./bin/aegis-quant --db-path data/aegis_quant.db backtest --days 365
./bin/aegis-quant --db-path data/aegis_quant.db readiness
```
