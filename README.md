# AEGIS-QUANT v0.3.0-rc1 — Two-Book PP-50 Rust Workspace

AEGIS-QUANT v0.3.0-rc1 is a standalone Rust workspace implementing the **Two-Book PP-50** architecture:
- **THE PIT** (`aegis-pit`): Directional perpetuals book with causal 3-State Gaussian HMM regime filter, MACE cross-sectional alpha engine (with periodic rolling-window refits), and 3-Stage Chandelier / Deadman risk cascade.
- **THE POND** (`aegis-pond`): 80-coin perpetual funding harvester with 6-Gate scanner, resting maker order reconciliation (`ARMED` -> `OPEN` only upon exchange fill), and mark-to-market equity attribution.
- **THE SETTLE** (`aegis-settle`): Cross-book allocator, profit sweep (`BOOK_SWEEP`), emergency recall (`EMERGENCY_RECALL`), and `subAccountTransfer` settlement engine with RAII gate protection.
- **Hardened WebUI & API** (`aegis-web`): Loopback-bound by default (`127.0.0.1:8800`), authenticated (`/api/v1/*` protected via `Authorization: Bearer <token>`, `X-Aegis-Token`, or `aegis_session` cookie), strict CORS (`web_cors_allowed_origins`), Content-Security-Policy, and non-blocking `spawn_blocking` handlers.
- **Database-Driven Configuration** (`aegis-db`): All strategy, scanner, risk, execution, retention, and security parameters live in SQLite `system_config` (migrations `001..006`) with Fernet encryption at rest backed by a CSPRNG key (`<db_dir>/.aegis_master_key`, mode `0600`).

---

## Workspace Structure (13 Crates)

```
AEGIS-LOCAL/
├── Cargo.toml                  # Workspace manifest (13 crates)
├── migrations/
│   ├── 001_v020_schema.sql
│   ├── 002_v020_seed_config.sql
│   ├── 003_v030_two_book_schema.sql
│   ├── 004_v030_two_book_seed.sql
│   ├── 005_v030_pond_80coin_universe.sql
│   └── 006_v030_review_remediation.sql
├── web/static/
│   └── index.html              # Authenticated 4-Page Terminal UI (CSP + XSS-hardened)
├── deploy/
│   ├── aegis-quant.service     # Hardened systemd unit (User=aegis, loopback bind)
│   ├── aegis-pit.service       # Hardened systemd unit for THE PIT book
│   ├── aegis-pond.service      # Hardened systemd unit for THE POND book
│   └── bootstrap_vm.sh         # Oracle Cloud ARM64 VM bootstrap with firewalld
└── crates/
    ├── aegis-core/             # Domain models, ConfigMap, math/stat helpers, secret classification
    ├── aegis-db/               # Pooled SQLite WAL manager, Fernet encryption, retention pruning
    ├── aegis-signals/          # Causal 3-State Gaussian HMM + MACE ridge regression + funding carry
    ├── aegis-risk/             # Leverage sizing, drawdown throttle, 3-stage stop cascade
    ├── aegis-execution/        # Order FSM, EIP-712 / msgpack Hyperliquid signer, subAccountTransfer
    ├── aegis-pit/              # THE PIT directional perpetuals orchestrator
    ├── aegis-pond/             # THE POND 80-coin funding harvester & 6-gate scanner
    ├── aegis-settle/           # Cross-book settlement & sweep controller
    ├── aegis-telemetry/        # Structured tracing & bounded telemetry pub/sub bus
    ├── aegis-backtest/         # Causal walk-forward backtester, stress tester, 5-gate readiness suite
    ├── aegis-alerts/           # Isolated-self-test multi-channel alert engine + single-worker retry queue
    ├── aegis-web/              # Axum HTTP + SSE server with bearer/cookie auth & strict CORS
    └── aegis-cli/              # Unified `aegis-quant` binary & multi-book orchestrator
```

---

## Building & Running

### 1. Initialize Database & Master Key
```bash
cargo run --release -p aegis-cli -- --db data/aegis_quant.db db --init --test-encryption
```
This applies migrations `001..006`, provisions `data/.aegis_master_key` (`0600`) with 32 bytes of CSPRNG entropy if absent, and runs an isolated in-memory encryption self-test.

### 2. Rotate Master Key
```bash
AEGIS_OLD_MASTER_KEY="<old_key>" AEGIS_NEW_MASTER_KEY="<new_key>" \
  cargo run --release -p aegis-cli -- --db data/aegis_quant.db db --rotate-key
```
Or using key files:
```bash
cargo run --release -p aegis-cli -- --db data/aegis_quant.db db \
  --rotate-key --old-key-file /path/to/old.key --new-key-file /path/to/new.key
```

### 3. Run the 5-Gate Readiness Suite
```bash
cargo run --release -p aegis-cli -- --db data/aegis_quant.db readiness
```

### 4. Start the Daemon + Authenticated WebUI
By default, the web server binds strictly to `127.0.0.1:8800`:
```bash
cargo run --release -p aegis-cli -- --db data/aegis_quant.db run --mode paper --host 127.0.0.1 --port 8800
```
To bind to a non-loopback interface, `--allow-remote-bind` (or `AEGIS_ALLOW_REMOTE_BIND=1` / `web_allow_remote_bind=true` in `system_config`) and a configured `web_api_token` are required.
