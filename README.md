# Aegis-Quant v0.3.0-rc1 — Institutional PP-50 Dual-Book Trading System (Rust Workspace)

An autonomous, single-tenant quantitative perpetuals trading platform built in Rust (2021 edition, Tokio async runtime, Axum web server, rusqlite bundled SQLite WAL) for **Hyperliquid L1**.

---

## Architecture Overview (PP-50 Dual-Book)

1. **Workspace Layout (`crates/*`)**:
   - `aegis-core`: Shared domain types (`BookId`, `Side`, `HmmRegime`, `SignalAction`, `PondCellState`, `OhlcvBar`), constants (`ROUND_TRIP_FRICTION_PCT`), and financial math (`calculate_sharpe_sortino_calmar`, `compute_chandelier_stop`, `round_significant_figures`).
   - `aegis-db`: Bundled SQLite WAL connection pool (`r2d2` + `rusqlite`), schema version v7 auto-migration, Fernet secret encryption (`AEGIS_MASTER_KEY`), and `BookScopedDb` compile-time book isolation.
   - `aegis-signals`: 3-State Gaussian HMM regime filter (`BULL`, `BEAR`, `CHOP`), MACE multi-grid momentum indicators, and Continuous Signal-to-Noise Ratio (SNR) ensemble.
   - `aegis-risk`: Risk sizing (`0.50%`–`1.50%` dynamic risk per trade), `4.0%` ($400) portfolio heat cap, `1.50x` gross leverage cap, `25%` single-asset concentration cap, and correlation cluster limits.
   - `aegis-execution`: Hyperliquid L1 REST/EIP-712 order router (`signing`, `http_client`, `ingestion`, `live_dispatch`, `reconcile`), live mark-to-market synchronization, and config validator.
   - `aegis-pit`: Book A (The Pit) directional BTC/Altcoin strategy engine ($5,000 baseline).
   - `aegis-pond`: Book B (The Pond) Champion `NPS-01` Naked-Option Perpetual Analogue Scanner across an 80-coin dynamic universe ($5,000 baseline, 10-state cell FSM, 6-tier exit priority engine).
   - `aegis-settle`: Daily `00:05 UTC` 50/50 Cross-Book Settling Engine enforcing Invariant F5 (`available_idle_usdc` transfer cap) and atomic SQLite transactions.
   - `aegis-backtest`: 365-Day Walk-Forward Backtest & Tear Sheet simulator (`0.35%` round-trip friction + 1H funding accrual).
   - `aegis-alerts`: Telegram Bot API alerting dispatcher (`@BotFather`) with drawdown circuit-breaker notifications.
   - `aegis-web`: Axum REST API (`/api/v1/*`), real-time Server-Sent Events (`/api/v1/stream`), and 4-page institutional dark-mode dashboard (`web/static/index.html` + `web/static/app.js`).
   - `aegis-cli`: Unified binary (`aegis-quant`) supporting `run`, `db`, `pond`, `settle`, `backtest`, `config`, `status`, `halt`, and `export` subcommands.

---

## Quick Start

### 1. Build Release Binaries

```bash
cargo build --release --workspace
```

### 2. Initialize / Migrate SQLite Database (Schema v7)

```bash
./target/release/aegis-quant --db data/aegis.db db --init --migrate
```

Incremental SQL migrations are stored under `sql/migrations/`:
- `sql/migrations/001_v030_pp50_pond_settle.sql`
- `sql/migrations/002_v030_hyperliquid_universe_80.sql`
- `sql/migrations/003_v030_pit_exec_symbols_unique.sql`
- `sql/migrations/004_v030_pond_universe_unique_base.sql`
- `sql/migrations/005_v030_round1_remediation.sql`
- `sql/migrations/007_v030_round2_remediation.sql`

### 3. Run the 4-Page Web Dashboard & Dual-Book Background Daemons

```bash
export AEGIS_MASTER_KEY="$(openssl rand -base64 32)"
export AEGIS_WEB_TOKEN="your-dashboard-api-token"

./target/release/aegis-quant --db data/aegis.db run --host 127.0.0.1 --port 8000 --book both
```

Then open `http://127.0.0.1:8000` in your browser and authenticate via the dashboard session banner (`POST /api/v1/auth/login` -> `HttpOnly; SameSite=Strict` session cookie).

### 4. CLI Subcommands

```bash
# Run a single dual-book evaluation cycle
./target/release/aegis-quant --db data/aegis.db run --once --book both

# Inspect Pond status or run an hourly 80-coin S-Cycle scan
./target/release/aegis-quant --db data/aegis.db pond --status
./target/release/aegis-quant --db data/aegis.db pond --scan

# Run Daily 00:05 UTC 50/50 Settling (dry-run or live)
./target/release/aegis-quant --db data/aegis.db settle --dry-run

# Run 365-day walk-forward tear sheet simulation
./target/release/aegis-quant --db data/aegis.db backtest --days 365
```
