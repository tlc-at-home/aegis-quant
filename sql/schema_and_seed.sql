-- =============================================================================
-- Aegis Quant (v0.3.0-rc1) — Consolidated Canonical SQLite WAL Schema & Seed Build Script
-- Target DB Path: /home/fedora/aegis/data/aegis_quant.db
-- Consolidates base schema + migrations 001..014 into one idempotent build script.
-- =============================================================================

PRAGMA busy_timeout = 15000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

-- -----------------------------------------------------------------------------
-- 1. Schema Versioning & System Configuration Tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_utc INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER)),
    description TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT NOT NULL,
    value_type TEXT NOT NULL DEFAULT 'string',
    category TEXT NOT NULL DEFAULT 'general',
    description TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- -----------------------------------------------------------------------------
-- 2. Universe Assets Table (31 Rows: 16 USDC_PERP + 15 BTC_SYNTH)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS universe_assets (
    asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL UNIQUE,
    base_asset TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    instrument_type TEXT NOT NULL CHECK (instrument_type IN ('USDC_PERP', 'BTC_SYNTH')),
    tier INTEGER NOT NULL CHECK (tier IN (1, 2, 3)),
    tier_name TEXT NOT NULL,
    exec_timeframe TEXT NOT NULL DEFAULT '1h',
    rs_timeframe TEXT NOT NULL DEFAULT '4h',
    tick_size REAL NOT NULL DEFAULT 0.01,
    lot_step_size REAL NOT NULL DEFAULT 0.001,
    min_notional_usdc REAL NOT NULL DEFAULT 10.0,
    max_leverage REAL NOT NULL DEFAULT 5.0,
    is_active INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    reference_price REAL NOT NULL DEFAULT 10.0
);

ALTER TABLE universe_assets ADD COLUMN reference_price REAL NOT NULL DEFAULT 10.0;

INSERT OR IGNORE INTO universe_assets (
    symbol, base_asset, quote_asset, instrument_type, tier, tier_name,
    exec_timeframe, rs_timeframe, tick_size, lot_step_size, min_notional_usdc, max_leverage, is_active, reference_price
) VALUES
    -- 16 Active Hyperliquid USDC_PERP Contracts (1 Benchmark BTC + 15 Altcoins)
    ('BTC/USDC:USDC',  'BTC',  'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.1,     0.0001, 10.0, 5.0, 1, 68500.0),
    ('ETH/USDC:USDC',  'ETH',  'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.05,    0.001,  10.0, 5.0, 1, 2650.0),
    ('SOL/USDC:USDC',  'SOL',  'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.01,    0.01,   10.0, 5.0, 1, 175.0),
    ('HYPE/USDC:USDC', 'HYPE', 'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.001,   0.1,    10.0, 5.0, 1, 24.5),
    ('XRP/USDC:USDC',  'XRP',  'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0001,  1.0,    10.0, 3.0, 1, 0.56),
    ('DOGE/USDC:USDC', 'DOGE', 'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00001, 10.0,   10.0, 3.0, 1, 0.145),
    ('ADA/USDC:USDC',  'ADA',  'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0001,  1.0,    10.0, 3.0, 1, 0.38),
    ('AVAX/USDC:USDC', 'AVAX', 'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.005,   0.1,    10.0, 3.0, 1, 28.5),
    ('LINK/USDC:USDC', 'LINK', 'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.001,   0.1,    10.0, 3.0, 1, 12.4),
    ('SUI/USDC:USDC',  'SUI',  'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0005,  0.1,    10.0, 3.0, 1, 2.15),
    ('DOT/USDC:USDC',  'DOT',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.001,   0.1,    10.0, 2.0, 1, 4.35),
    ('NEAR/USDC:USDC', 'NEAR', 'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.001,   0.1,    10.0, 2.0, 1, 4.85),
    ('APT/USDC:USDC',  'APT',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.001,   0.1,    10.0, 2.0, 1, 9.80),
    ('ARB/USDC:USDC',  'ARB',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0001,  1.0,    10.0, 2.0, 1, 0.58),
    ('OP/USDC:USDC',   'OP',   'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0001,  1.0,    10.0, 2.0, 1, 1.65),
    ('INJ/USDC:USDC',  'INJ',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.005,   0.1,    10.0, 2.0, 1, 21.2),

    -- 15 Active Synthetic ALT/BTC Cross-Pairs (BTC_SYNTH)
    ('ETH/BTC:SYNTH',  'ETH',  'BTC',  'BTC_SYNTH', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.000001,   0.001, 10.0, 5.0, 1, 0.038686),
    ('SOL/BTC:SYNTH',  'SOL',  'BTC',  'BTC_SYNTH', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.0000001,  0.01,  10.0, 5.0, 1, 0.002555),
    ('HYPE/BTC:SYNTH', 'HYPE', 'BTC',  'BTC_SYNTH', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.00000001, 0.1,   10.0, 5.0, 1, 0.000358),
    ('XRP/BTC:SYNTH',  'XRP',  'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 1.0,   10.0, 3.0, 1, 0.00000818),
    ('DOGE/BTC:SYNTH', 'DOGE', 'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 10.0,  10.0, 3.0, 1, 0.00000212),
    ('ADA/BTC:SYNTH',  'ADA',  'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 1.0,   10.0, 3.0, 1, 0.00000555),
    ('AVAX/BTC:SYNTH', 'AVAX', 'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0000001,  0.1,   10.0, 3.0, 1, 0.000416),
    ('LINK/BTC:SYNTH', 'LINK', 'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0000001,  0.1,   10.0, 3.0, 1, 0.000181),
    ('SUI/BTC:SYNTH',  'SUI',  'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 0.1,   10.0, 3.0, 1, 0.0000314),
    ('DOT/BTC:SYNTH',  'DOT',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0000001,  0.1,   10.0, 2.0, 1, 0.0000635),
    ('NEAR/BTC:SYNTH', 'NEAR', 'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.00000001, 0.1,   10.0, 2.0, 1, 0.0000708),
    ('APT/BTC:SYNTH',  'APT',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0000001,  0.1,   10.0, 2.0, 1, 0.000143),
    ('ARB/BTC:SYNTH',  'ARB',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.00000001, 1.0,   10.0, 2.0, 1, 0.00000847),
    ('OP/BTC:SYNTH',   'OP',   'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.00000001, 1.0,   10.0, 2.0, 1, 0.0000241),
    ('INJ/BTC:SYNTH',  'INJ',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0000001,  0.1,   10.0, 2.0, 1, 0.000309);

UPDATE universe_assets SET reference_price = CASE symbol
    WHEN 'BTC/USDC:USDC'  THEN 68500.0
    WHEN 'ETH/USDC:USDC'  THEN 2650.0
    WHEN 'SOL/USDC:USDC'  THEN 175.0
    WHEN 'HYPE/USDC:USDC' THEN 24.5
    WHEN 'XRP/USDC:USDC'  THEN 0.56
    WHEN 'DOGE/USDC:USDC' THEN 0.145
    WHEN 'ADA/USDC:USDC'  THEN 0.38
    WHEN 'AVAX/USDC:USDC' THEN 28.5
    WHEN 'LINK/USDC:USDC' THEN 12.4
    WHEN 'SUI/USDC:USDC'  THEN 2.15
    WHEN 'DOT/USDC:USDC'  THEN 4.35
    WHEN 'NEAR/USDC:USDC' THEN 4.85
    WHEN 'APT/USDC:USDC'  THEN 9.80
    WHEN 'ARB/USDC:USDC'  THEN 0.58
    WHEN 'OP/USDC:USDC'   THEN 1.65
    WHEN 'INJ/USDC:USDC'  THEN 21.2
    WHEN 'ETH/BTC:SYNTH'  THEN 0.038686
    WHEN 'SOL/BTC:SYNTH'  THEN 0.002555
    WHEN 'HYPE/BTC:SYNTH' THEN 0.000358
    WHEN 'XRP/BTC:SYNTH'  THEN 0.00000818
    WHEN 'DOGE/BTC:SYNTH' THEN 0.00000212
    WHEN 'ADA/BTC:SYNTH'  THEN 0.00000555
    WHEN 'AVAX/BTC:SYNTH' THEN 0.000416
    WHEN 'LINK/BTC:SYNTH' THEN 0.000181
    WHEN 'SUI/BTC:SYNTH'  THEN 0.0000314
    WHEN 'DOT/BTC:SYNTH'  THEN 0.0000635
    WHEN 'NEAR/BTC:SYNTH' THEN 0.0000708
    WHEN 'APT/BTC:SYNTH'  THEN 0.000143
    WHEN 'ARB/BTC:SYNTH'  THEN 0.00000847
    WHEN 'OP/BTC:SYNTH'   THEN 0.0000241
    WHEN 'INJ/BTC:SYNTH'  THEN 0.000309
    ELSE reference_price
END
WHERE reference_price = 10.0;

-- -----------------------------------------------------------------------------
-- 3. Time-Series, Book A (Pit) & Two-Book Platform Tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_ohlcv (
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL DEFAULT 0.0,
    funding_rate_hourly REAL NOT NULL DEFAULT 0.0,
    is_synthetic INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (symbol, timeframe, timestamp_ms)
);

CREATE INDEX IF NOT EXISTS idx_market_ohlcv_lookup
    ON market_ohlcv (symbol, timeframe, timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS signal_evaluations (
    eval_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ms INTEGER NOT NULL,
    base_asset TEXT NOT NULL,
    exec_symbol TEXT NOT NULL,
    synth_symbol TEXT NOT NULL,
    hmm_regime TEXT NOT NULL,
    prob_bull REAL NOT NULL,
    prob_bear REAL NOT NULL,
    prob_chop REAL NOT NULL,
    snr_score REAL NOT NULL,
    mace_score REAL NOT NULL,
    blended_conviction REAL NOT NULL,
    rs_rank INTEGER NOT NULL,
    signal_action TEXT NOT NULL,
    funding_rate_hourly REAL NOT NULL DEFAULT 0.0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    snr_synth REAL NOT NULL DEFAULT 0.0,
    rs_96 REAL NOT NULL DEFAULT 0.0,
    r_usdc_8 REAL NOT NULL DEFAULT 0.0
);

ALTER TABLE signal_evaluations ADD COLUMN snr_synth REAL NOT NULL DEFAULT 0.0;
ALTER TABLE signal_evaluations ADD COLUMN rs_96 REAL NOT NULL DEFAULT 0.0;
ALTER TABLE signal_evaluations ADD COLUMN r_usdc_8 REAL NOT NULL DEFAULT 0.0;

CREATE INDEX IF NOT EXISTS idx_signal_evaluations_ts
    ON signal_evaluations (timestamp_ms DESC, rs_rank ASC);

CREATE TABLE IF NOT EXISTS trades (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT', 'BASIS_HARVEST')),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'OPEN', 'CLOSED', 'CANCELLED')) DEFAULT 'OPEN',
    entry_price REAL NOT NULL,
    current_price REAL NOT NULL,
    exit_price REAL,
    quantity REAL NOT NULL,
    notional_usdc REAL NOT NULL,
    risk_budget_usdc REAL NOT NULL,
    conviction_multiplier REAL NOT NULL DEFAULT 1.0,
    initial_stop_price REAL NOT NULL,
    chandelier_stop_price REAL NOT NULL,
    highest_price_seen REAL NOT NULL,
    lowest_price_seen REAL NOT NULL,
    realized_pnl_usdc REAL NOT NULL DEFAULT 0.0,
    unrealized_pnl_usdc REAL NOT NULL DEFAULT 0.0,
    fees_paid_usdc REAL NOT NULL DEFAULT 0.0,
    funding_accrued_usdc REAL NOT NULL DEFAULT 0.0,
    exchange_order_id TEXT,
    opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    closed_at TEXT,
    exit_reason TEXT,
    client_order_id TEXT,
    fsm_state TEXT DEFAULT 'PENDING'
        CHECK(fsm_state IN ('PENDING', 'OPEN_ON_BOOK', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED')),
    avg_fill_price REAL,
    filled_units REAL DEFAULT 0.0,
    slippage_paid_usd REAL DEFAULT 0.0,
    last_funding_hour_bucket INTEGER NOT NULL DEFAULT 0,
    protective_stop_oid TEXT
);

ALTER TABLE trades ADD COLUMN client_order_id TEXT;
ALTER TABLE trades ADD COLUMN fsm_state TEXT DEFAULT 'PENDING'
    CHECK(fsm_state IN ('PENDING', 'OPEN_ON_BOOK', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED'));
ALTER TABLE trades ADD COLUMN exchange_order_id TEXT;
ALTER TABLE trades ADD COLUMN avg_fill_price REAL;
ALTER TABLE trades ADD COLUMN filled_units REAL DEFAULT 0.0;
ALTER TABLE trades ADD COLUMN slippage_paid_usd REAL DEFAULT 0.0;
ALTER TABLE trades ADD COLUMN last_funding_hour_bucket INTEGER NOT NULL DEFAULT 0;
ALTER TABLE trades ADD COLUMN protective_stop_oid TEXT;

CREATE INDEX IF NOT EXISTS idx_trades_status
    ON trades (status, opened_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_client_order_id_unique
    ON trades(client_order_id)
    WHERE client_order_id IS NOT NULL AND client_order_id != '';

CREATE TABLE IF NOT EXISTS order_state_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    timestamp_utc INTEGER NOT NULL,
    reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_order_state_audit_client_id
    ON order_state_audit (client_order_id, id ASC);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ms INTEGER NOT NULL,
    total_equity_usdc REAL NOT NULL,
    available_cash_usdc REAL NOT NULL,
    deployed_notional_usdc REAL NOT NULL DEFAULT 0.0,
    gross_leverage REAL NOT NULL DEFAULT 0.0,
    portfolio_heat_usdc REAL NOT NULL DEFAULT 0.0,
    portfolio_heat_pct REAL NOT NULL DEFAULT 0.0,
    basis_harvest_notional_usdc REAL NOT NULL DEFAULT 0.0,
    accrued_yield_usdc REAL NOT NULL DEFAULT 0.0,
    hmm_regime TEXT NOT NULL DEFAULT 'CHOP',
    prob_bull REAL NOT NULL DEFAULT 0.3333,
    prob_bear REAL NOT NULL DEFAULT 0.3333,
    prob_chop REAL NOT NULL DEFAULT 0.3334,
    open_long_count INTEGER NOT NULL DEFAULT 0,
    open_short_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT INTO portfolio_snapshots (
    timestamp_ms, total_equity_usdc, available_cash_usdc, deployed_notional_usdc,
    gross_leverage, portfolio_heat_usdc, portfolio_heat_pct,
    basis_harvest_notional_usdc, accrued_yield_usdc,
    hmm_regime, prob_bull, prob_bear, prob_chop, open_long_count, open_short_count
)
SELECT
    CAST(strftime('%s', 'now') AS INTEGER) * 1000,
    10000.0, 10000.0, 0.0,
    0.0, 0.0, 0.0,
    0.0, 0.0,
    'CHOP', 0.3333, 0.3333, 0.3334, 0, 0
WHERE NOT EXISTS (SELECT 1 FROM portfolio_snapshots);

CREATE TABLE IF NOT EXISTS telemetry_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ms INTEGER NOT NULL,
    level TEXT NOT NULL DEFAULT 'INFO',
    component TEXT NOT NULL,
    message TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_telemetry_logs_ts
    ON telemetry_logs (timestamp_ms DESC);

CREATE TABLE IF NOT EXISTS books (
    book_id INTEGER PRIMARY KEY AUTOINCREMENT,
    book_code TEXT NOT NULL UNIQUE CHECK (book_code IN ('PIT', 'POND')),
    description TEXT NOT NULL DEFAULT '',
    equity_allocation_pct REAL NOT NULL DEFAULT 0.5,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO books (book_code, description) VALUES
('PIT', 'Book A - incumbent HMM/MACE engine, 16-coin universe'),
('POND', 'Book B - naked-option premium scanner champion, 80-coin pond');

CREATE TABLE IF NOT EXISTS pond_universe (
    symbol TEXT PRIMARY KEY,
    rank INTEGER NOT NULL,
    median_daily_volume_usdc REAL NOT NULL DEFAULT 0.0,
    tick_size REAL NOT NULL DEFAULT 0.01,
    lot_step_size REAL NOT NULL DEFAULT 0.001,
    min_notional_usdc REAL NOT NULL DEFAULT 10.0,
    max_leverage REAL NOT NULL DEFAULT 3.0,
    status TEXT NOT NULL CHECK (status IN ('WARMUP', 'ACTIVE', 'DELISTING', 'RETIRED')) DEFAULT 'WARMUP',
    warmup_until TEXT,
    delist_streak INTEGER NOT NULL DEFAULT 0,
    added_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    delisted_at TEXT,
    reference_price REAL NOT NULL DEFAULT 10.0,
    is_synthetic_volume INTEGER NOT NULL DEFAULT 0
);

ALTER TABLE pond_universe ADD COLUMN reference_price REAL NOT NULL DEFAULT 10.0;
ALTER TABLE pond_universe ADD COLUMN is_synthetic_volume INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_pond_universe_status ON pond_universe (status, rank);

CREATE TABLE IF NOT EXISTS pond_champions (
    champion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    version TEXT NOT NULL UNIQUE,
    params_json TEXT NOT NULL,
    walk_forward_score REAL NOT NULL DEFAULT 0.0,
    is_active INTEGER NOT NULL DEFAULT 0,
    crowned_at TEXT,
    retired_at TEXT
);

INSERT OR IGNORE INTO pond_champions (version, params_json, walk_forward_score, is_active, crowned_at) VALUES
('NPS-01', '{"funding_z_window_hours":168,"funding_z_entry":2.0,"funding_z_exit":0.5,"funding_floor_hourly":0.00005,"trend_veto_ema_fast":16,"trend_veto_ema_slow":64,"trend_veto_overextension_pct":0.08,"initial_stop_atr_mult":2.5,"chandelier_atr_mult":3.8,"wick_buffer_atr":0.85,"max_hold_hours":72,"min_norm_hold_hours":6.0,"require_2h_persistence":true}', 1.85, 1, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));

CREATE TABLE IF NOT EXISTS pond_scan_cycles (
    cycle_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    champion_id INTEGER NOT NULL REFERENCES pond_champions(champion_id),
    coins_scanned INTEGER NOT NULL DEFAULT 0,
    candidates INTEGER NOT NULL DEFAULT 0,
    armed INTEGER NOT NULL DEFAULT 0,
    opened INTEGER NOT NULL DEFAULT 0,
    exits_evaluated INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pond_cells (
    cell_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    champion_id INTEGER NOT NULL REFERENCES pond_champions(champion_id),
    state TEXT NOT NULL CHECK (state IN (
        'DORMANT', 'SCREENED', 'CANDIDATE', 'ARMED', 'OPEN',
        'TRAILED', 'CLOSED', 'COOLDOWN', 'SKIPPED', 'REJECTED'
    )),
    cycle_opened_id INTEGER REFERENCES pond_scan_cycles(cycle_id),
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT')),
    option_analogue TEXT NOT NULL CHECK (option_analogue IN ('SHORT_PUT', 'SHORT_CALL')),
    entry_price REAL,
    quantity REAL,
    notional_usdc REAL,
    risk_budget_usdc REAL,
    conviction_multiplier REAL,
    funding_z_entry REAL,
    funding_rate_entry REAL,
    initial_stop REAL,
    chandelier_stop REAL,
    highest_price_seen REAL,
    lowest_price_seen REAL,
    funding_accrued_usdc REAL NOT NULL DEFAULT 0.0,
    realized_pnl_usdc REAL NOT NULL DEFAULT 0.0,
    fees_paid_usdc REAL NOT NULL DEFAULT 0.0,
    exit_price REAL,
    exit_reason TEXT,
    opened_at TEXT,
    closed_at TEXT,
    cooldown_until TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_funding_hour_bucket INTEGER NOT NULL DEFAULT 0,
    resting_order_id TEXT,
    protective_stop_oid TEXT
);

ALTER TABLE pond_cells ADD COLUMN last_funding_hour_bucket INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pond_cells ADD COLUMN resting_order_id TEXT;
ALTER TABLE pond_cells ADD COLUMN protective_stop_oid TEXT;

CREATE INDEX IF NOT EXISTS idx_pond_cells_state ON pond_cells (state, symbol);

CREATE TABLE IF NOT EXISTS book_transfers (
    transfer_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    settle_date             TEXT NOT NULL,
    from_book               TEXT NOT NULL CHECK (from_book IN ('PIT', 'POND')),
    to_book                 TEXT NOT NULL CHECK (to_book IN ('PIT', 'POND')),
    amount_usdc             REAL NOT NULL CHECK (amount_usdc >= 0.0),
    pre_from_usdc           REAL,
    pre_to_usdc             REAL,
    post_from_usdc          REAL,
    post_to_usdc            REAL,
    drift_bps_before        REAL,
    drift_bps_after         REAL,
    blocked_remainder_usdc  REAL NOT NULL DEFAULT 0.0,
    status                  TEXT NOT NULL CHECK (status IN (
        'PENDING', 'CONFIRMED', 'FAILED',
        'COMPLETED', 'PARTIAL_IDLE_CAPPED', 'PARTIAL', 'AMBIGUOUS',
        'SKIPPED_DEADBAND', 'SKIPPED_WITHIN_BAND', 'NO_ACTION_WITHIN_DEADBAND',
        'BLOCKED_ZERO_IDLE', 'SKIPPED_NO_IDLE', 'HALTED_PP50', 'DRY_RUN'
    )),
    attempts                INTEGER NOT NULL DEFAULT 1,
    tx_ref                  TEXT,
    created_at              TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    transfer_mode           TEXT NOT NULL DEFAULT 'VIRTUAL',
    leg1_tx_ref             TEXT,
    leg2_tx_ref             TEXT,
    legs_completed          INTEGER NOT NULL DEFAULT 0
);

ALTER TABLE book_transfers ADD COLUMN transfer_mode TEXT NOT NULL DEFAULT 'VIRTUAL';
ALTER TABLE book_transfers ADD COLUMN leg1_tx_ref TEXT;
ALTER TABLE book_transfers ADD COLUMN leg2_tx_ref TEXT;
ALTER TABLE book_transfers ADD COLUMN legs_completed INTEGER NOT NULL DEFAULT 0;

DROP TABLE IF EXISTS book_transfers_v9;

CREATE TABLE IF NOT EXISTS pp50_platform_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ms INTEGER NOT NULL,
    platform_equity_usdc REAL NOT NULL,
    pit_equity_usdc REAL NOT NULL,
    pond_equity_usdc REAL NOT NULL,
    split_pct REAL NOT NULL DEFAULT 0.5,
    drift_bps REAL NOT NULL DEFAULT 0.0,
    pit_open_count INTEGER NOT NULL DEFAULT 0,
    pond_open_cells INTEGER NOT NULL DEFAULT 0,
    settling_status TEXT NOT NULL DEFAULT 'NONE',
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

-- -----------------------------------------------------------------------------
-- 4. Database Views
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_universe_usdc_perp;
CREATE VIEW v_universe_usdc_perp AS
SELECT
    asset_id,
    symbol,
    base_asset,
    quote_asset,
    instrument_type,
    tier,
    tier_name,
    exec_timeframe,
    rs_timeframe,
    tick_size,
    lot_step_size,
    min_notional_usdc,
    max_leverage,
    is_active,
    updated_at,
    reference_price
FROM universe_assets
WHERE instrument_type = 'USDC_PERP' AND is_active = 1
ORDER BY tier ASC, asset_id ASC;

DROP VIEW IF EXISTS v_universe_btc_synth;
CREATE VIEW v_universe_btc_synth AS
SELECT
    asset_id,
    symbol,
    base_asset,
    quote_asset,
    instrument_type,
    tier,
    tier_name,
    exec_timeframe,
    rs_timeframe,
    tick_size,
    lot_step_size,
    min_notional_usdc,
    max_leverage,
    is_active,
    updated_at,
    reference_price
FROM universe_assets
WHERE instrument_type = 'BTC_SYNTH' AND is_active = 1
ORDER BY tier ASC, asset_id ASC;

CREATE VIEW IF NOT EXISTS v_universe_alt_pairs AS
SELECT
    p.base_asset,
    p.tier,
    p.tier_name,
    p.symbol AS exec_symbol,
    s.symbol AS synth_symbol,
    p.exec_timeframe,
    s.rs_timeframe,
    p.tick_size,
    p.lot_step_size,
    p.min_notional_usdc,
    p.max_leverage
FROM universe_assets p
INNER JOIN universe_assets s
    ON p.base_asset = s.base_asset
WHERE p.instrument_type = 'USDC_PERP'
  AND s.instrument_type = 'BTC_SYNTH'
  AND p.is_active = 1
  AND s.is_active = 1
ORDER BY p.tier ASC, p.asset_id ASC;

DROP VIEW IF EXISTS v_pond_universe_active;
CREATE VIEW v_pond_universe_active AS
SELECT
    symbol,
    rank,
    median_daily_volume_usdc,
    tick_size,
    lot_step_size,
    min_notional_usdc,
    max_leverage,
    status,
    warmup_until,
    delist_streak,
    added_at,
    delisted_at,
    reference_price,
    is_synthetic_volume
FROM pond_universe
WHERE status = 'ACTIVE'
ORDER BY rank ASC;

DROP VIEW IF EXISTS v_pond_cells_open;
CREATE VIEW v_pond_cells_open AS
SELECT *
FROM pond_cells
WHERE state IN ('OPEN', 'TRAILED')
ORDER BY opened_at DESC;

CREATE VIEW IF NOT EXISTS v_pp50_book_equity_latest AS
    SELECT * FROM pp50_platform_snapshots ORDER BY snapshot_id DESC LIMIT 1;

DROP VIEW IF EXISTS v_book_transfers_recent;
CREATE VIEW v_book_transfers_recent AS
SELECT *
FROM book_transfers
ORDER BY transfer_id DESC
LIMIT 30;

CREATE VIEW IF NOT EXISTS v_trades_pit AS
    SELECT * FROM trades;

-- -----------------------------------------------------------------------------
-- 5. Complete Consolidated System Configuration Seed (`system_config`)
-- -----------------------------------------------------------------------------
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
    -- Platform Identity & Runtime
    ('platform_name', 'Aegis Quant', 'string', 'platform', 'Core platform brand and telemetry identifier'),
    ('platform_version', '0.3.0-rc1', 'string', 'platform', 'SemVer pre-release tag'),
    ('environment', 'sandbox', 'string', 'platform', 'Deployment target environment (sandbox or production)'),
    ('execution_mode', 'PAPER', 'string', 'platform', 'Execution routing mode: PAPER, TESTNET, or LIVE Web3 DEX routing'),
    ('exchange_id', 'hyperliquid', 'string', 'platform', 'CCXT async exchange identifier (Hyperliquid L1 CLOB)'),
    ('schema_version', '14', 'int', 'platform', 'Current SQLite schema migration version'),

    -- Web3 DEX & AI Credentials
    ('wallet_address', '', 'string', 'credentials', 'Main Arbitrum Wallet Address holding USDC collateral'),
    ('private_key', '', 'string', 'credentials', 'Trade-Only API Agent Wallet Private Key (no withdrawal permissions)'),
    ('gemini_api_key', '', 'string', 'credentials', 'Google Gemini API key for quantitative telemetry diagnostics'),
    ('hyperliquid_testnet', 'true', 'bool', 'credentials', 'Route orders to Hyperliquid testnet when true'),
    ('hyperliquid_vault_address', '', 'string', 'credentials', 'Optional Hyperliquid sub-account or vault address'),
    ('hyperliquid_agent_address', '', 'string', 'credentials', 'Optional delegated Hyperliquid API Agent address when signing with an agent key'),

    -- Capital & Conviction-Scaled Risk Engine Parameters (50% Book Scope)
    ('base_currency', 'USDC', 'string', 'risk', 'Base collateral and settlement currency on Hyperliquid'),
    ('benchmark_symbol', 'BTC/USDC:USDC', 'string', 'risk', 'Benchmark major contract used for BTC_SYNTH ratio & HMM'),
    ('initial_equity_usdc', '10000.0', 'float', 'risk', 'Baseline initial portfolio equity in USDC ($10,000)'),
    ('baseline_risk_per_trade_usdc', '50.0', 'float', 'risk', '1.0% baseline risk budget per trade in USDC ($50 on $5k book)'),
    ('min_risk_per_trade_usdc', '25.0', 'float', 'risk', '0.5% minimum conviction-scaled risk per trade in USDC ($25 on $5k book)'),
    ('max_risk_per_trade_usdc', '75.0', 'float', 'risk', '1.5% maximum conviction-scaled risk per trade in USDC ($75 on $5k book)'),
    ('conviction_scale_min', '0.5', 'float', 'risk', 'Minimum conviction multiplier (0.5x baseline risk)'),
    ('conviction_scale_max', '1.5', 'float', 'risk', 'Maximum conviction multiplier (1.5x baseline risk)'),
    ('max_portfolio_heat_usdc', '200.0', 'float', 'risk', 'Maximum total portfolio open stop-loss risk in USDC ($200 on $5k book)'),
    ('max_portfolio_heat_pct', '0.04', 'float', 'risk', 'Maximum total portfolio heat as fraction of equity (4.0%)'),
    ('max_single_asset_notional_usdc', '1250.0', 'float', 'risk', 'Maximum single-asset position notional in USDC ($1,250 on $5k book)'),
    ('max_single_asset_pct', '0.25', 'float', 'risk', 'Maximum single-asset concentration fraction of equity (25%)'),
    ('max_gross_leverage_notional_usdc', '7500.0', 'float', 'risk', 'Maximum total gross notional exposure in USDC ($7,500 on $5k book)'),
    ('max_gross_leverage', '1.5', 'float', 'risk', 'Maximum portfolio gross leverage multiplier (1.5x)'),
    ('max_concurrent_long_positions', '2', 'int', 'risk', 'Maximum concurrent Bull-regime relative-strength Long positions'),
    ('max_concurrent_short_positions', '2', 'int', 'risk', 'Maximum concurrent Bear-regime relative-weakness Short positions'),
    ('max_correlation_cluster_pct', '0.35', 'float', 'risk', 'Maximum same-direction notional exposure across correlated altcoin beta clusters (35% of book equity)'),
    ('risk_caps_reference_scope', 'BOOK', 'string', 'risk', 'Reference equity scope for PIT dollar risk caps: BOOK (50% split) or PLATFORM (100%)'),
    ('testnet_faucet_offset_usdc', '10000.0', 'float', 'risk', 'Unallocated Hyperliquid Testnet Faucet Spot USDC grant to exclude from Perps equity and risk-cap scaling'),
    ('risk_min_stop_pct_floor', '0.002', 'float', 'risk', 'Minimum stop distance floor as fraction of entry price (20 bps)'),
    ('risk_conviction_min_mult', '0.50', 'float', 'risk', 'Minimum conviction risk budget multiplier at entry threshold'),
    ('risk_conviction_max_mult', '1.50', 'float', 'risk', 'Maximum conviction risk budget multiplier at full conviction'),

    -- Hyperliquid Fee, Funding & Basis/HLP Yield Harvester Model
    ('maker_fee_bps', '1.5', 'float', 'execution', 'Hyperliquid perpetual maker fee in basis points (1.5 bps)'),
    ('taker_fee_bps', '4.5', 'float', 'execution', 'Hyperliquid perpetual taker fee in basis points (4.5 bps)'),
    ('default_slippage_bps', '2.5', 'float', 'execution', 'Expected execution slippage model in basis points'),
    ('funding_settlement_interval_hours', '1', 'int', 'execution', 'Hyperliquid 1-hour funding settlement cadence'),
    ('short_max_adverse_funding_rate_hourly', '-0.0005', 'float', 'execution', '1H funding carry protection threshold for open Short positions'),
    ('basis_harvest_enabled', 'true', 'bool', 'execution', 'Enable idle USDC delta-neutral Basis & HLP yield harvesting'),
    ('basis_min_hourly_funding_rate', '0.000015', 'float', 'execution', 'Minimum hourly funding rate (+0.0015%/hr ~13.1% APR) to harvest basis'),
    ('basis_min_apr_pct', '13.14', 'float', 'execution', 'Annualized percentage yield threshold for idle cash deployment'),
    ('basis_max_idle_cash_allocation_pct', '0.50', 'float', 'execution', 'Maximum fraction of idle USDC deployed into Basis/HLP harvesting'),
    ('hlp_yield_fallback_enabled', 'true', 'bool', 'execution', 'Deploy idle cash to Hyperliquidity Provider (HLP) vault when basis is flat'),
    ('exchange_protective_stops_enabled', 'true', 'bool', 'execution', 'Submit exchange-side reduce-only trigger stop orders on Hyperliquid at open and Chandelier ratchet (CR-2)'),
    ('cancel_unknown_open_orders', 'true', 'bool', 'execution', 'Cancel untracked resting open orders during periodic exchange reconciliation (ME-9)'),

    -- 3-State Diagonal-Covariance Gaussian HMM Regime Gate
    ('hmm_num_states', '3', 'int', 'hmm', '3 hidden Markov states: Bull, Bear, Chop'),
    ('hmm_covariance_type', 'diag', 'string', 'hmm', 'Diagonal covariance matrix structure for Gaussian HMM'),
    ('hmm_window_bars', '540', 'int', 'hmm', 'Rolling 540-bar (90-day at 4H) evaluation window'),
    ('hmm_lookback_candles', '540', 'int', 'hmm', 'Rolling 4H candle lookback window for 3-State Gaussian HMM (90 days)'),
    ('hmm_lookback_bars', '540', 'int', 'hmm', 'Rolling 4H bar lookback window for 3-State Gaussian HMM (90 days)'),
    ('hmm_timeframe', '4h', 'string', 'hmm', 'Primary timeframe for HMM regime decoding'),
    ('hmm_em_iterations', '18', 'int', 'hmm', 'Maximum Baum-Welch EM iterations for 3-State Gaussian HMM'),
    ('hmm_bull_prob_threshold', '0.50', 'float', 'hmm', 'Minimum posterior probability gamma_t(Bull) to open Long leaders'),
    ('hmm_bear_prob_threshold', '0.50', 'float', 'hmm', 'Minimum posterior probability gamma_t(Bear) to open Short laggards'),
    ('hmm_chop_prob_threshold', '0.55', 'float', 'hmm', 'Posterior probability threshold identifying sideways Chop regime'),
    ('hmm_min_bars', '15', 'int', 'hmm', 'Minimum OHLCV bars required before fitting 3-State Gaussian HMM'),
    ('hmm_min_fit_obs_bars', '14', 'int', 'hmm', 'Minimum observation index before initial HMM EM fit'),
    ('hmm_refit_step_bars', '12', 'int', 'hmm', 'Causal walk-forward HMM EM refit interval in bars during historical decoding'),
    ('hmm_em_tolerance', '0.0001', 'float', 'hmm', 'Log-likelihood convergence tolerance for Baum-Welch EM'),
    ('hmm_rv_window_bars', '6', 'int', 'hmm', 'Rolling realized volatility window in bars for HMM observation extraction'),
    ('hmm_ema_fast_span', '36', 'int', 'hmm', 'Fast EMA span on BTC log-returns for HMM observation smoothing'),
    ('hmm_ema_slow_span', '96', 'int', 'hmm', 'Slow EMA span on BTC log-returns for HMM observation smoothing'),
    ('hmm_ema_vol_span', '14', 'int', 'hmm', 'EMA span on BTC high-low range volatility for HMM observation smoothing'),
    ('hmm_ema_alt_span', '14', 'int', 'hmm', 'EMA span on cross-sectional alt_pc1_ret for HMM observation smoothing'),
    ('hmm_ret_fast_weight', '0.65', 'float', 'hmm', 'Weight of fast EMA log-return in HMM observation blend'),
    ('hmm_ret_slow_weight', '0.35', 'float', 'hmm', 'Weight of slow EMA log-return in HMM observation blend'),
    ('hmm_enter_gate_prob', '0.55', 'float', 'hmm', 'Hysteresis posterior probability gate to enter BULL or BEAR state'),
    ('hmm_exit_gate_prob', '0.45', 'float', 'hmm', 'Hysteresis posterior probability gate to exit BULL or BEAR state'),
    ('hmm_bull_prior_mean_ret', '0.003', 'float', 'hmm', 'Prior mean log-return for BULL state (State 0)'),
    ('hmm_bull_prior_mean_rv', '0.014', 'float', 'hmm', 'Prior mean realized volatility for BULL state (State 0)'),
    ('hmm_bull_prior_mean_trend', '0.80', 'float', 'hmm', 'Prior mean EMA trend z-score for BULL state (State 0)'),
    ('hmm_bear_prior_mean_ret', '-0.003', 'float', 'hmm', 'Prior mean log-return for BEAR state (State 1)'),
    ('hmm_bear_prior_mean_rv', '0.022', 'float', 'hmm', 'Prior mean realized volatility for BEAR state (State 1)'),
    ('hmm_bear_prior_mean_trend', '-0.80', 'float', 'hmm', 'Prior mean EMA trend z-score for BEAR state (State 1)'),
    ('hmm_chop_prior_mean_ret', '0.0', 'float', 'hmm', 'Prior mean log-return for CHOP state (State 2)'),
    ('hmm_chop_prior_mean_rv', '0.010', 'float', 'hmm', 'Prior mean realized volatility for CHOP state (State 2)'),
    ('hmm_chop_prior_mean_trend', '0.0', 'float', 'hmm', 'Prior mean EMA trend z-score for CHOP state (State 2)'),
    ('hmm_self_transition_prob', '0.90', 'float', 'hmm', 'Initial diagonal self-transition probability in 3-state HMM'),
    ('hmm_off_transition_prob', '0.05', 'float', 'hmm', 'Initial off-diagonal transition probability in 3-state HMM'),
    ('hmm_var_floor', '0.0001', 'float', 'hmm', 'Minimum diagonal variance floor in Gaussian HMM emission updates'),
    ('hmm_trend_gate_threshold', '0.25', 'float', 'hmm', 'Normalized EMA trend gate threshold for BULL/BEAR promotion'),
    ('hmm_chop_prob_gate', '0.62', 'float', 'hmm', 'Posterior CHOP probability ceiling above which CHOP regime is forced'),
    ('hmm_marginal_lead_min', '0.08', 'float', 'hmm', 'Minimum posterior lead over opposite directional state for marginal promotion'),
    ('hmm_walkforward_refit_bars', '12', 'int', 'hmm', 'Causal walk-forward HMM EM refit interval in bars during historical decoding'),
    ('btc_macro_ema_fast_span', '18', 'int', 'hmm', 'BTC macro trend confirmation fast EMA span'),
    ('btc_macro_ema_slow_span', '54', 'int', 'hmm', 'BTC macro trend confirmation slow EMA span'),
    ('btc_macro_ret_slow_bars', '24', 'int', 'hmm', 'BTC macro confirmation slow return lookback in bars'),
    ('btc_macro_ret_fast_bars', '8', 'int', 'hmm', 'BTC macro confirmation fast return lookback in bars'),
    ('action_bull_min_prob', '0.58', 'float', 'hmm', 'Minimum HMM BULL posterior probability for LONG entry'),
    ('action_bull_min_btc_trend', '-0.010', 'float', 'hmm', 'Minimum BTC EMA trend spread for LONG macro confirmation'),
    ('action_bull_min_btc_r24', '-0.030', 'float', 'hmm', 'Minimum 24-bar BTC log-return for LONG macro confirmation'),
    ('action_bull_min_btc_r8', '-0.020', 'float', 'hmm', 'Minimum 8-bar BTC log-return for LONG macro confirmation'),
    ('action_bear_min_prob', '0.58', 'float', 'hmm', 'Minimum HMM BEAR posterior probability for SHORT entry'),
    ('action_bear_max_btc_trend', '0.006', 'float', 'hmm', 'Maximum BTC EMA trend spread permitted for SHORT macro confirmation'),
    ('action_bear_max_btc_r24', '0.015', 'float', 'hmm', 'Maximum 24-bar BTC log-return permitted for SHORT macro confirmation'),
    ('action_bear_max_btc_r8', '0.015', 'float', 'hmm', 'Maximum 8-bar BTC log-return permitted for SHORT macro confirmation'),

    -- MACE Non-Linear Ensemble & Continuous SNR Momentum Filter
    ('mace_rf_n_estimators', '60', 'int', 'mace', 'Random Forest tree count in MACE non-linear ensemble (60 trees)'),
    ('mace_rf_trees', '30', 'int', 'mace', 'Number of decision trees in MACE Random Forest regressor'),
    ('mace_rf_max_depth', '4', 'int', 'mace', 'Regularized maximum tree depth for MACE Random Forest (depth 4)'),
    ('mace_rf_min_samples_leaf', '10', 'int', 'mace', 'Minimum samples per leaf in MACE Random Forest'),
    ('mace_ridge_alpha', '1.0', 'float', 'mace', 'L2 penalty alpha for MACE Ridge regression component'),
    ('mace_blend_weight', '0.35', 'float', 'mace', 'Ensemble blend weight assigned to non-linear MACE score (35%)'),
    ('snr_blend_weight', '0.65', 'float', 'mace', 'Ensemble blend weight assigned to continuous SNR momentum (65%)'),
    ('snr_fast_span', '16', 'int', 'mace', 'Fast exponential lookback span for multi-horizon SNR forecast'),
    ('snr_slow_span', '64', 'int', 'mace', 'Slow exponential lookback span for multi-horizon SNR forecast'),
    ('snr_ema_fast_period', '16', 'int', 'mace', 'Fast EMA period for continuous Signal-to-Noise Ratio (SNR)'),
    ('snr_ema_slow_period', '64', 'int', 'mace', 'Slow EMA period for continuous Signal-to-Noise Ratio (SNR)'),
    ('snr_synth_weight', '0.60', 'float', 'mace', 'Weight of ALT/BTC:SYNTH SNR component in composite SNR'),
    ('snr_usdc_weight', '0.40', 'float', 'mace', 'Weight of ALT/USDC:USDC SNR component in composite SNR'),
    ('snr_mad_multiplier', '2.50', 'float', 'mace', 'Mean absolute deviation denominator multiplier in continuous SNR'),
    ('snr_tanh_scale', '1.25', 'float', 'mace', 'Tanh squashing scale factor for continuous SNR'),
    ('snr_ema_fast_span', '16', 'int', 'mace', 'Fast EMA span alias for Signal-to-Noise Ratio (SNR)'),
    ('snr_ema_med_span', '21', 'int', 'mace', 'Medium EMA span for Signal-to-Noise Ratio (SNR)'),
    ('snr_ema_slow_span', '64', 'int', 'mace', 'Slow EMA span alias for Signal-to-Noise Ratio (SNR)'),
    ('snr_atr_period', '14', 'int', 'mace', 'Wilder ATR period used to normalize SNR directional excursion'),
    ('snr_er_period', '14', 'int', 'mace', 'Kaufman Efficiency Ratio (ER) lookback period for SNR'),
    ('snr_short_lookback', '6', 'int', 'mace', 'Short horizon lookback bars for SNR'),
    ('snr_med_lookback', '18', 'int', 'mace', 'Medium horizon lookback bars for SNR'),
    ('snr_long_lookback', '36', 'int', 'mace', 'Long horizon lookback bars for SNR'),
    ('snr_w_short', '0.50', 'float', 'mace', 'Weight of short-horizon component in SNR blend'),
    ('snr_w_med', '0.30', 'float', 'mace', 'Weight of medium-horizon component in SNR blend'),
    ('snr_w_long', '0.20', 'float', 'mace', 'Weight of long-horizon component in SNR blend'),
    ('snr_er_floor', '0.40', 'float', 'mace', 'Base floor multiplier for Kaufman Efficiency Ratio in SNR'),
    ('snr_er_weight', '0.60', 'float', 'mace', 'Slope multiplier for Kaufman Efficiency Ratio in SNR'),
    ('mace_min_bars', '55', 'int', 'mace', 'Minimum OHLCV bars required before MACE ensemble evaluation'),
    ('mace_vol_ema_span', '20.0', 'float', 'mace', 'Volume EMA span in bars for MACE volume z-score feature'),
    ('mace_z_window_bars', '120', 'int', 'mace', 'Causal rolling z-score window in bars for MACE feature standardization'),
    ('mace_target_horizon_bars', '4', 'int', 'mace', 'Forward k-bar target horizon for purged MACE regression'),
    ('mace_warmup_bars', '32', 'int', 'mace', 'Initial feature warmup bars excluded from MACE training matrix'),
    ('mace_min_train_rows', '16', 'int', 'mace', 'Minimum purged training rows required to fit MACE ensemble'),
    ('mace_train_window_bars', '220', 'int', 'mace', 'Rolling trailing training window in bars for purged MACE refits'),
    ('mace_refit_interval_bars', '36', 'int', 'mace', 'Bar interval between purged MACE Ridge + Random Forest refits'),
    ('mace_rng_seed', '42', 'int', 'mace', 'Deterministic RNG seed for MACE Random Forest bootstrap'),
    ('mace_ic_window_bars', '36', 'int', 'mace', 'Trailing OOS prediction window in bars for Spearman Rank IC'),
    ('mace_dynamic_weight_min', '0.15', 'float', 'mace', 'Minimum adaptive MACE ensemble weight under Dynamic IC blending'),
    ('mace_dynamic_weight_max', '0.60', 'float', 'mace', 'Maximum adaptive MACE ensemble weight under Dynamic IC blending'),
    ('mace_ic_neg_slope', '0.60', 'float', 'mace', 'Adaptive MACE weight slope when rolling OOS Spearman IC <= 0'),
    ('mace_ic_pos_slope', '0.85', 'float', 'mace', 'Adaptive MACE weight slope when rolling OOS Spearman IC > 0'),
    ('rs_lookback_fast_bars', '24', 'int', 'mace', 'Fast cross-sectional Relative Strength lookback in bars'),
    ('rs_lookback_mid_bars', '72', 'int', 'mace', 'Medium cross-sectional Relative Strength lookback in bars'),
    ('rs_lookback_slow_bars', '96', 'int', 'mace', 'Slow cross-sectional Relative Strength lookback in bars'),
    ('rs_usdc_momentum_bars', '8', 'int', 'mace', 'Short-term USDC momentum lookback in bars for entry confirmation'),
    ('rs_composite_snr_weight', '0.45', 'float', 'mace', 'Weight of synthetic SNR in cross-sectional RS composite score'),
    ('rs_composite_fast_weight', '0.35', 'float', 'mace', 'Weight of 24-bar RS in cross-sectional RS composite score'),
    ('rs_composite_mid_weight', '0.20', 'float', 'mace', 'Weight of 72-bar RS in cross-sectional RS composite score'),
    ('rs_composite_fast_scale', '6.0', 'float', 'mace', 'Tanh scale multiplier for 24-bar RS in composite score'),
    ('rs_composite_mid_scale', '4.0', 'float', 'mace', 'Tanh scale multiplier for 72-bar RS in composite score'),
    ('action_top_k_leaders', '3', 'int', 'mace', 'Number of top RS leader assets eligible for LONG entries in BULL regime'),
    ('action_bottom_k_laggards', '3', 'int', 'mace', 'Number of bottom RS laggard assets eligible for SHORT entries in BEAR regime'),
    ('action_bull_min_snr', '0.25', 'float', 'mace', 'Minimum composite SNR score required for LONG entry'),
    ('action_bull_min_snr_synth', '0.30', 'float', 'mace', 'Minimum ALT/BTC synthetic SNR score required for LONG entry'),
    ('action_bull_min_mace', '0.05', 'float', 'mace', 'Minimum MACE score floor permitted for LONG entry'),
    ('action_bull_min_rs96', '0.02', 'float', 'mace', 'Minimum 96-bar Relative Strength required for LONG entry'),
    ('action_bull_min_r_usdc_8', '0.0', 'float', 'mace', 'Minimum 8-bar USDC momentum required for LONG entry'),
    ('action_bull_max_funding', '0.0005', 'float', 'basis', 'Maximum hourly funding rate permitted for LONG entry'),
    ('action_bear_max_snr', '-0.25', 'float', 'mace', 'Maximum composite SNR ceiling permitted for SHORT entry'),
    ('action_bear_max_snr_synth', '-0.30', 'float', 'mace', 'Maximum ALT/BTC synthetic SNR ceiling permitted for SHORT entry'),
    ('action_bear_max_mace', '-0.05', 'float', 'mace', 'Maximum MACE score ceiling permitted for SHORT entry'),
    ('action_bear_max_rs96', '-0.03', 'float', 'mace', 'Maximum 96-bar Relative Strength ceiling permitted for SHORT entry'),
    ('action_bear_max_r_usdc_8', '-0.008', 'float', 'mace', 'Maximum 8-bar USDC momentum ceiling permitted for SHORT entry'),
    ('action_bear_min_funding', '-0.0004', 'float', 'basis', 'Minimum hourly funding rate floor permitted for SHORT entry'),
    ('conviction_long_threshold', '0.32', 'float', 'mace', 'Minimum blended conviction threshold for LONG entry'),
    ('conviction_short_threshold', '-0.32', 'float', 'mace', 'Maximum blended conviction ceiling for SHORT entry'),
    ('basis_harvest_min_funding_hourly', '0.000015', 'float', 'basis', 'Minimum hourly funding rate required for delta-neutral basis harvest'),
    ('chop_funding_harvest_threshold', '0.000015', 'float', 'basis', 'Hourly funding rate threshold to trigger BASIS_HARVEST in CHOP regime'),
    ('funding_carry_penalty_threshold', '0.0003', 'float', 'basis', 'Adverse hourly funding rate threshold for directional conviction penalty'),
    ('funding_carry_penalty_mult', '0.85', 'float', 'basis', 'Conviction multiplier applied when adverse funding exceeds threshold'),
    ('enable_hlp_floor_backtest', 'false', 'BOOL', 'MACE_HMM', 'Enable artificial basis yield floor in backtest (disabled for realistic PnL)'),

    -- Legacy Signal Gate Aliases (Migration 001 compatibility)
    ('conviction_entry_threshold', '0.02', 'FLOAT', 'SIGNAL', 'Minimum conviction entry threshold'),
    ('mace_tanh_scale', '1.25', 'FLOAT', 'SIGNAL', 'Tanh scale for MACE signal'),
    ('hmm_enter_prob_gate', '0.55', 'FLOAT', 'SIGNAL', 'HMM enter probability gate'),
    ('hmm_exit_prob_gate', '0.45', 'FLOAT', 'SIGNAL', 'HMM exit probability gate'),
    ('mace_forward_horizon_bars', '4', 'INT', 'SIGNAL', 'MACE forward target horizon in bars'),
    ('mace_min_ic_floor', '0.05', 'FLOAT', 'SIGNAL', 'MACE minimum Spearman IC floor'),
    ('bull_prob_entry_gate', '0.58', 'FLOAT', 'SIGNAL', 'Bull probability entry gate'),
    ('bear_prob_entry_gate', '0.58', 'FLOAT', 'SIGNAL', 'Bear probability entry gate'),
    ('bull_conviction_gate', '0.32', 'FLOAT', 'SIGNAL', 'Bull conviction gate'),
    ('bear_conviction_gate', '-0.32', 'FLOAT', 'SIGNAL', 'Bear conviction gate'),
    ('bull_snr_synth_gate', '0.30', 'FLOAT', 'SIGNAL', 'Bull synthetic SNR gate'),
    ('bear_snr_synth_gate', '-0.30', 'FLOAT', 'SIGNAL', 'Bear synthetic SNR gate'),
    ('bull_mace_gate', '0.05', 'FLOAT', 'SIGNAL', 'Bull MACE gate'),
    ('bear_mace_gate', '-0.05', 'FLOAT', 'SIGNAL', 'Bear MACE gate'),
    ('bull_rs_96_gate', '0.02', 'FLOAT', 'SIGNAL', 'Bull 96-bar RS gate'),
    ('bear_rs_96_gate', '-0.03', 'FLOAT', 'SIGNAL', 'Bear 96-bar RS gate'),
    ('bear_r_usdc_8_gate', '-0.008', 'FLOAT', 'SIGNAL', 'Bear 8-bar USDC momentum gate'),
    ('bull_btc_snr_gate', '0.12', 'FLOAT', 'SIGNAL', 'Bull BTC SNR gate'),
    ('bear_btc_snr_gate', '-0.45', 'FLOAT', 'SIGNAL', 'Bear BTC SNR gate'),
    ('bear_btc_snr_macro_gate', '-0.20', 'FLOAT', 'SIGNAL', 'Bear BTC SNR macro gate'),
    ('bear_btc_ret_30_gate', '-0.015', 'FLOAT', 'SIGNAL', 'Bear BTC 30-bar return gate'),
    ('bear_btc_ret_60_gate', '-0.04', 'FLOAT', 'SIGNAL', 'Bear BTC 60-bar return gate'),
    ('bear_btc_ret_180_gate', '0.002', 'FLOAT', 'SIGNAL', 'Bear BTC 180-bar return gate'),
    ('signal_short_bounce_veto_pct', '0.020', 'float', 'signals', 'Short-horizon (12h/4h) upward bounce threshold vetoing lagging 4H SHORT entries (2.0%)'),
    ('signal_long_crash_veto_pct', '0.020', 'float', 'signals', 'Short-horizon (12h/4h) downward crash threshold vetoing lagging 4H LONG entries (2.0%)'),

    -- Chandelier Trailing Stops, Two-Tier Tight-Trail, Break-Even & Orchestrator
    ('atr_period', '14', 'int', 'stops', 'Average True Range lookback period in bars'),
    ('atr_default_period', '14', 'int', 'stops', 'Default Wilder ATR lookback period alias in bars'),
    ('chandelier_lookback_bars', '22', 'int', 'stops', 'Lookback bars for Chandelier high/low anchor'),
    ('chandelier_atr_multiplier_long', '2.2', 'float', 'stops', 'Chandelier trailing stop ATR multiplier for Long positions'),
    ('chandelier_atr_multiplier_short', '2.2', 'float', 'stops', 'Inverted Chandelier trailing stop ATR multiplier for Short positions'),
    ('short_sizing_atr_multiplier', '2.5', 'FLOAT', 'stops', 'Short position sizing ATR multiplier'),
    ('chandelier_min_atr_pct', '0.004', 'FLOAT', 'stops', 'Minimum ATR floor fraction for Chandelier stop distance'),
    ('chandelier_wick_overshoot_atr', '0.35', 'FLOAT', 'stops', 'Intrabar wick overshoot ATR buffer'),
    ('chandelier_wick_buffer_atr', '0.35', 'float', 'stops', 'Intrabar wick overshoot buffer in ATR multiples before Tier-1 emergency stop'),
    ('chandelier_trail_atr_multiplier', '0.45', 'float', 'stops', 'Tight current-price trailing stop distance in ATR multiples'),
    ('chandelier_trail_max_pct', '0.008', 'float', 'stops', 'Maximum trailing stop distance as fraction of current price (0.8%)'),
    ('tight_trail_trigger_atr', '1.0', 'float', 'stops', 'Minimum favorable excursion in ATR multiples before tight current-price trailing stop activates'),
    ('orphan_reconcile_stop_pct', '0.015', 'float', 'stops', 'Initial protective stop distance fraction for reconciled on-chain orphan positions (1.5%)'),
    ('long_break_even_trigger_atr', '0.8', 'float', 'stops', 'Favorable excursion in ATR multiples to lock break-even on Long positions'),
    ('long_break_even_lock_atr', '0.25', 'float', 'stops', 'Locked profit buffer in ATR multiples above entry once Long break-even triggers'),
    ('short_break_even_trigger_atr', '0.8', 'float', 'stops', 'Favorable excursion in ATR multiples to lock break-even on Short positions'),
    ('short_break_even_lock_atr', '0.25', 'float', 'stops', 'Locked profit buffer in ATR multiples below entry once Short break-even triggers'),
    ('post_stop_cooldown_bars', '14', 'INT', 'stops', 'Cooldown bars after stop-out'),
    ('execution_stop_cooldown_bars', '14', 'int', 'execution', 'Cooldown bars after stop-out before re-entering the same asset'),
    ('entry_exit_slippage_bps', '3.0', 'FLOAT', 'execution', 'Entry/exit slippage in bps'),
    ('stop_slippage_bps', '8.0', 'FLOAT', 'execution', 'Stop execution slippage in bps'),
    ('wick_overshoot_slippage_fraction', '0.15', 'FLOAT', 'execution', 'Fraction of wick overshoot applied as adverse stop slippage'),
    ('execution_regime_flip_bear_prob', '0.62', 'float', 'execution', 'Posterior BEAR probability threshold to exit LONG positions'),
    ('execution_regime_flip_bull_prob', '0.60', 'float', 'execution', 'Posterior BULL probability threshold to exit SHORT positions'),
    ('execution_high_chop_conviction_max', '0.08', 'float', 'execution', 'Maximum absolute blended conviction for HIGH_CHOP_DELEVERAGE exit'),
    ('execution_high_chop_prob_offset', '0.20', 'float', 'execution', 'Offset added to hmm_chop_prob_threshold for HIGH_CHOP_DELEVERAGE exit'),
    ('execution_high_chop_prob_cap', '0.82', 'float', 'execution', 'Maximum cap on CHOP probability gate for HIGH_CHOP_DELEVERAGE exit'),
    ('execution_rs_breakdown_long_snr', '-0.10', 'float', 'execution', 'Synthetic SNR breakdown threshold to exit open LONG positions'),
    ('execution_rs_reversal_short_snr', '0.10', 'float', 'execution', 'Synthetic SNR reversal threshold to exit open SHORT positions'),
    ('polling_interval_seconds', '15', 'int', 'orchestrator', 'Main engine ingestion and signal evaluation loop interval in seconds'),
    ('pit_tick_interval_seconds', '900', 'int', 'orchestrator', 'Async daemon Book A (Pit) evaluation cadence in seconds (R4-M1)'),
    ('pond_tick_interval_seconds', '15', 'int', 'pond', 'Async daemon Book B (Pond) fast intrabar stop/scan tick cadence in seconds (R4-M1)'),
    ('btc_fallback_base_price_usdc', '95000.0', 'float', 'execution', 'Configured fallback BTC/USDC base price when no prior BTC market bars exist in PAPER/TESTNET mode (R4-N1)'),
    ('pond_volume_min_bars_per_day', '12', 'int', 'pond', 'Minimum 1H bars required within a UTC calendar day bucket for daily volume aggregation (R4-N6)'),
    ('stranded_transfer_alert_cooldown_ms', '300000', 'int', 'settling', 'Minimum cooldown in milliseconds between repeated alerts for stranded PARTIAL/AMBIGUOUS book transfers (R4-QB)'),
    ('pending_transfer_stale_ttl_ms', '120000', 'int', 'settling', 'Age threshold in milliseconds after which an in-flight PENDING book transfer is flagged as stale (R4-QB)'),
    ('ohlcv_fetch_limit', '600', 'int', 'orchestrator', 'Number of historical candles fetched per symbol (>= 540 HMM bars)'),
    ('calibration_walk_forward_days', '90', 'int', 'calibration', 'Walk-forward window in days for per-asset timeframe calibration'),
    ('web_server_port', '8000', 'int', 'web', 'FastAPI REST + SSE telemetry server listening port'),
    ('max_bar_staleness_ms', '28800000', 'int', 'orchestrator', 'Maximum allowed OHLCV bar staleness in milliseconds (8h) before live signal gating'),
    ('max_consecutive_api_failures', '5', 'int', 'orchestrator', 'Consecutive exchange API failures before raising degraded health alert'),

    -- Book B (Pond NPS-01) Parameters
    ('pond_enabled', 'false', 'BOOL', 'pond', 'Master switch for Pond engine'),
    ('pond_execution_mode', 'PAPER', 'STR', 'pond', 'Pond execution routing mode: PAPER, TESTNET, or LIVE Web3 DEX routing'),
    ('pond_direction_policy', 'both', 'STR', 'pond', 'Direction mode: both, short_only, long_only'),
    ('pond_universe_target_size', '80', 'INT', 'pond', 'Target pond membership count'),
    ('pond_universe_refresh_weekday', '0', 'INT', 'pond', 'Weekly refresh day (0 = Monday)'),
    ('pond_universe_delist_rank', '95', 'INT', 'pond', 'Rank below which delist counter starts'),
    ('pond_universe_warmup_hours', '168', 'INT', 'pond', 'Warmup hours before new member is scannable'),
    ('pond_min_daily_volume_usdc', '50000.0', 'FLOAT', 'pond', '30-day median daily volume floor'),
    ('pond_testnet_min_vol_cap_usdc', '1000.0', 'float', 'pond', 'Minimum 24h USDC volume floor for Pond universe admission on Hyperliquid Testnet'),
    ('pond_scan_offset_seconds', '10', 'INT', 'pond', 'Delay after funding settle before S-cycle'),
    ('pond_funding_z_window_hours', '168', 'INT', 'pond', 'Trailing window for funding z-score'),
    ('pond_funding_z_entry', '2.0', 'FLOAT', 'pond', 'Minimum entry z-score elevation'),
    ('pond_funding_z_exit', '0.5', 'FLOAT', 'pond', 'Funding normalisation exit z-score'),
    ('pond_funding_floor_hourly', '0.000050', 'FLOAT', 'pond', 'Minimum hourly funding floor magnitude (+0.0050%/h = 0.50 bps/hr)'),
    ('pond_require_2h_persistence', 'true', 'BOOL', 'pond', 'Require 2-hour funding persistence above floor before entry'),
    ('pond_min_norm_hold_hours', '6.0', 'FLOAT', 'pond', 'Minimum hold hours before Tier 3 funding normalisation exit when net PnL <= 0'),
    ('pond_trend_veto_ema_fast', '16', 'INT', 'pond', 'Fast EMA span for trend veto (4H bars)'),
    ('pond_trend_veto_ema_slow', '64', 'INT', 'pond', 'Slow EMA span for trend veto (4H bars)'),
    ('pond_trend_veto_overextension_pct', '0.08', 'FLOAT', 'pond', 'Veto when 4H close > EMA64 * 1.08 (or < 0.92)'),
    ('pond_liquidity_score_ref_usdc', '250000.0', 'FLOAT', 'pond', 'Volume reference for liquidity factor'),
    ('pond_max_concurrent_cells', '8', 'INT', 'pond', 'Maximum simultaneous open cells'),
    ('pond_max_opens_per_cycle', '3', 'INT', 'pond', 'Maximum cells opened per S-cycle'),
    ('pond_max_hold_hours', '72', 'INT', 'pond', 'Maximum time in open cell'),
    ('pond_cooldown_hours', '24', 'INT', 'pond', 'Post-exit cooldown hours'),
    ('pond_stop_cooldown_hours', '56', 'INT', 'pond', 'Post-stop cooldown hours'),
    ('pond_baseline_risk_usdc', '50.0', 'FLOAT', 'pond', 'Baseline risk budget on $5,000 book'),
    ('pond_min_risk_usdc', '25.0', 'FLOAT', 'pond', 'Band risk floor (0.5% of book)'),
    ('pond_max_risk_usdc', '75.0', 'FLOAT', 'pond', 'Band risk ceiling (1.5% of book)'),
    ('pond_conviction_scale_min', '0.5', 'FLOAT', 'pond', 'Conviction multiplier floor'),
    ('pond_conviction_scale_max', '1.5', 'FLOAT', 'pond', 'Conviction multiplier ceiling'),
    ('pond_max_heat_usdc', '200.0', 'FLOAT', 'pond', 'Total open risk cap ($200 / 4% of book)'),
    ('pond_max_heat_pct', '0.04', 'FLOAT', 'pond', 'Heat cap fraction'),
    ('pond_max_cell_notional_usdc', '1250.0', 'FLOAT', 'pond', 'Per-cell notional cap ($1,250 / 25%)'),
    ('pond_max_cell_pct', '0.25', 'FLOAT', 'pond', 'Per-cell fraction'),
    ('pond_max_gross_leverage', '1.5', 'FLOAT', 'pond', 'Gross leverage ceiling multiple'),
    ('pond_kill_switch', 'false', 'BOOL', 'pond', 'Freeze new cell entries'),
    ('pond_ingest_stagger_seconds', '7.5', 'FLOAT', 'pond', 'Spacing of 1h full-sync calls'),
    ('pond_ohlcv_retention_days', '30', 'int', 'system', 'Retention window in days for historical Pond OHLCV & funding cache'),
    ('pond_champion_active', 'NPS-01', 'STR', 'pond', 'Active champion version'),
    ('pond_champion_recrown_margin_pct', '10.0', 'FLOAT', 'pond', 'Score margin required to re-crown'),
    ('pond_universe_refresh_interval_hours', '24', 'int', 'pond', 'Interval in hours between automated 80-coin Pond universe refreshes (C7)'),
    ('pond_atr_period', '14', 'int', 'pond', 'Wilder ATR period for 1H and 4H Pond scanner & exit evaluations'),
    ('pond_wick_buffer_atr_mult', '0.85', 'float', 'pond', 'Tier 1 catastrophic wick buffer in 1H ATR multiples'),
    ('pond_chandelier_atr_mult', '3.8', 'float', 'pond', 'Tier 2 Chandelier stop distance in 4H ATR multiples'),
    ('pond_initial_stop_atr_mult', '2.5', 'float', 'pond', 'Initial volatility stop distance in 1H ATR multiples'),
    ('pond_blowoff_atr_mult', '3.0', 'float', 'pond', 'Gate 5 1-hour bar range blow-off veto threshold in 1H ATR multiples'),
    ('pond_blowoff_3h_adverse_atr_mult', '2.2', 'float', 'pond', 'Gate 5 3-hour adverse parabolic move veto threshold in 1H ATR multiples'),
    ('pond_trend_veto_4h_atr_mult', '2.5', 'float', 'pond', 'Gate 6 structural trend distance veto threshold in 4H ATR multiples'),
    ('pond_gate5_parabolic_3h_atr_mult', '2.2', 'float', 'pond', 'Gate 5 3-hour adverse parabolic move veto threshold in 1H ATR multiples'),
    ('pond_gate6_atr_4h_floor_mult', '1.8', 'float', 'pond', 'Gate 6 minimum 4H ATR floor as a multiple of 1H ATR'),
    ('pond_gate6_structural_atr_mult', '2.5', 'float', 'pond', 'Gate 6 structural trend distance veto threshold in 4H ATR multiples'),
    ('pond_min_stop_pct_floor', '0.005', 'float', 'pond', 'Minimum initial stop distance floor as fraction of reference price (50 bps)'),
    ('pond_max_stop_distance_pct', '0.06', 'float', 'pond', 'Maximum initial protective stop distance as fraction of reference price for Pond NPS-01 cells (6.0%)'),
    ('pond_conviction_z_span', '2.0', 'float', 'pond', 'Z-score span above entry threshold for full 1.5x Pond cell conviction'),
    ('pond_conviction_min_mult', '0.5', 'float', 'pond', 'Minimum Pond cell conviction multiplier'),
    ('pond_conviction_max_mult', '1.5', 'float', 'pond', 'Maximum Pond cell conviction multiplier'),
    ('pond_maker_offset_bps', '1.5', 'float', 'pond', 'Maker limit offset in bps for Pond entry & exit limit chasers'),
    ('pond_fallback_base_price_usdc', '10.0', 'float', 'pond', 'Base price in USDC for synthetic Pond OHLCV fallback calculation'),
    ('pond_fallback_rank_slope_usdc', '0.75', 'float', 'pond', 'Rank slope in USDC for synthetic Pond OHLCV fallback calculation'),
    ('pond_volume_median_window_days', '7', 'int', 'pond', 'Rolling window in days for computing true 7-day median daily volume in Pond universe'),

    -- Settling & PP-50 Platform Parameters
    ('settling_enabled', 'true', 'BOOL', 'settling', 'Master switch for Settling controller'),
    ('settling_time_utc', '00:05', 'STR', 'settling', 'Daily transfer execution time'),
    ('settling_split_pct', '0.5', 'FLOAT', 'settling', 'Target equity split per book'),
    ('settling_tolerance_bps', '50', 'INT', 'settling', 'Dead-band tolerance in bps'),
    ('settling_max_retries', '3', 'INT', 'settling', 'Verification retries before alert'),
    ('settling_retry_backoff_seconds', '30', 'INT', 'settling', 'Retry backoff delay'),
    ('settling_window_open_utc', '00:00', 'STR', 'settling', 'Advisory no-opens window start'),
    ('settling_window_close_utc', '00:07', 'STR', 'settling', 'Advisory no-opens window end'),
    ('settling_kill_switch', 'false', 'BOOL', 'settling', 'Pause all capital transfers'),
    ('platform_pp50_enabled', 'false', 'BOOL', 'platform', 'Platform-level PP-50 master switch'),
    ('platform_settle_gate_enabled', 'false', 'BOOL', 'platform', 'Default-off open-gate hook during settle'),
    ('platform_pp50_halt', 'false', 'BOOL', 'platform', 'Emergency platform-wide entry halt'),
    ('settle_gate_ttl_ms', '300000', 'int', 'settling', 'Maximum TTL in milliseconds (5m) before a stale platform_settle_gate lock auto-expires'),
    ('platform_settle_gate_armed_at_ms', '0', 'int', 'settling', 'Epoch timestamp (ms) when platform_settle_gate was last armed'),
    ('platform_settle_lock_holder', '', 'string', 'settling', 'Owner identifier of the active daily settling mutex lock'),
    ('dry_run_reclassify_before_utc', '', 'string', 'settling', 'Optional ISO-8601 UTC cutoff timestamp before which historical PARTIAL_IDLE_CAPPED rows are reclassified to DRY_RUN'),

    -- System, Runtime, SQLite, Cycle Lock & Backup Parameters
    ('engine_runtime_label', 'RUST_NATIVE', 'string', 'system', 'Canonical runtime engine label'),
    ('engine_runtime_detail', 'RUST_NATIVE (Axum / Tokio)', 'string', 'system', 'Detailed runtime engine descriptor'),
    ('http_timeout_secs', '8', 'int', 'system', 'Default HTTP client request timeout in seconds'),
    ('http_connect_timeout_secs', '4', 'int', 'system', 'Default HTTP client TCP/TLS connect timeout in seconds'),
    ('sqlite_busy_timeout_ms', '15000', 'int', 'system', 'SQLite WAL busy timeout in milliseconds'),
    ('sqlite_synchronous_mode', 'NORMAL', 'string', 'system', 'SQLite WAL synchronous pragma mode'),
    ('sqlite_cache_size_kib', '-32000', 'int', 'system', 'SQLite page cache size in KiB (-32000 = 32 MiB)'),
    ('sqlite_mmap_size_bytes', '268435456', 'int', 'system', 'SQLite memory-mapped I/O limit in bytes (256 MiB)'),
    ('telemetry_retention_days', '30', 'int', 'system', 'Maximum retention age in days for telemetry_logs rows'),
    ('telemetry_max_rows', '10000', 'int', 'system', 'Maximum row cap for telemetry_logs table before pruning'),
    ('retention_prune_interval_cycles', '60', 'int', 'system', 'Daemon cycle interval between automated DB retention pruning runs'),
    ('reconcile_interval_cycles', '20', 'int', 'system', 'Daemon cycle interval for periodic exchange state reconciliation'),
    ('default_ingest_lookback_bars', '140', 'int', 'system', 'Default OHLCV bar lookback count per ingestion cycle'),
    ('calibration_ingest_lookback_bars', '220', 'int', 'system', 'OHLCV bar lookback count before walk-forward calibration'),
    ('ingest_lookback_bars', '120', 'int', 'system', 'Default OHLCV bar lookback count for Book A (PIT) cycle ingestion'),
    ('backup_retention_days', '7', 'int', 'system', 'Retention window in days for automated SQLite VACUUM INTO backups'),
    ('engine_cycle_lock_owner', '', 'string', 'system', 'Owner identifier of the cross-process database advisory cycle lock (HI-4)'),
    ('engine_cycle_lock_expires_ms', '0', 'int', 'system', 'Epoch expiration timestamp (ms) of the cross-process database advisory cycle lock (HI-4)'),
    ('backtest_warmup_bars', '110', 'int', 'system', 'Initial historical warmup bars before backtest trade execution starts'),
    ('backtest_bars_per_day', '6', 'int', 'system', 'Number of 4H bars per 24-hour trading day'),
    ('backtest_bars_per_year', '2190.0', 'float', 'system', 'Number of 4H bars per 365-day year for Sharpe/Sortino annualization'),
    ('backtest_max_concurrent_positions', '6', 'int', 'risk', 'Maximum simultaneous open positions in Book A backtest & live portfolio'),
    ('backtest_gate_min_sharpe', '1.20', 'float', 'system', 'Readiness Gate 5 minimum annualized Sharpe ratio requirement'),
    ('backtest_gate_max_mdd_pct', '20.0', 'float', 'system', 'Readiness Gate 5 maximum drawdown percentage ceiling'),
    ('backtest_gate_min_profit_factor', '1.30', 'float', 'system', 'Readiness Gate 5 minimum Profit Factor requirement'),

    -- Web API Security, Authentication, CORS & SSE Limits
    ('web_bind_host', '127.0.0.1', 'string', 'security', 'Default WebUI & REST API bind address (loopback by default)'),
    ('web_bind_port', '8000', 'int', 'security', 'Default WebUI & REST API TCP port'),
    ('web_allow_remote_bind', 'false', 'bool', 'security', 'Explicit opt-in required to bind WebUI to non-loopback interfaces'),
    ('web_auth_enabled', 'true', 'bool', 'security', 'Require Bearer token or session authentication on /api/v1/* endpoints'),
    ('web_api_token', '', 'string', 'credentials', 'Optional static Web API Bearer token (Fernet-encrypted at rest when set)'),
    ('web_cors_allowed_origins', 'http://127.0.0.1:8000,http://localhost:8000', 'string', 'security', 'Comma-separated allowlist of origins permitted by CORS'),
    ('web_allowed_hosts', 'localhost,127.0.0.1,[::1],aegis-quant-sandbox,192.168.122.254', 'string', 'security', 'Comma-separated Host header allowlist for DNS-rebinding protection'),
    ('web_sse_interval_secs', '3', 'int', 'system', 'Server-Sent Events (/api/v1/stream) push interval in seconds'),
    ('web_max_sse_clients', '32', 'int', 'system', 'Maximum concurrent SSE stream connections permitted'),
    ('web_csp_policy', 'default-src ''self''; script-src ''self''; style-src ''self'' ''unsafe-inline''; img-src ''self'' data:; connect-src ''self''; frame-ancestors ''none''; base-uri ''self''; form-action ''self''', 'string', 'security', 'Content-Security-Policy header enforced on WebUI responses'),
    ('web_auth_cache_ttl_ms', '2000', 'int', 'security', 'TTL in milliseconds for caching web authentication config in AppState'),
    ('enforce_master_key', 'true', 'bool', 'security', 'Require external or keyfile Fernet master key (immutable via Web API)'),

    -- Hyperliquid Endpoints, Execution & Paper Slippage Parameters
    ('hyperliquid_mainnet_url', 'https://api.hyperliquid.xyz', 'string', 'execution', 'Hyperliquid L1 Mainnet REST API base URL'),
    ('hyperliquid_testnet_url', 'https://api.hyperliquid-testnet.xyz', 'string', 'execution', 'Hyperliquid L1 Testnet REST API base URL'),
    ('hyperliquid_info_url_override', '', 'string', 'execution', 'Optional local test override URL for Hyperliquid /info endpoint'),
    ('hyperliquid_exchange_url_override', '', 'string', 'execution', 'Optional local test override URL for Hyperliquid /exchange endpoint'),
    ('pit_subaccount_address', '', 'string', 'execution', 'Optional dedicated Hyperliquid sub-account EVM address for Book A (PIT)'),
    ('pond_subaccount_address', '', 'string', 'execution', 'Optional dedicated Hyperliquid sub-account EVM address for Book B (POND)'),
    ('maker_fill_poll_interval_ms', '250', 'int', 'execution', 'Polling sleep interval in ms between live Maker Limit Chaser reprices'),
    ('maker_reprice_step_ticks', '1', 'int', 'execution', 'Tick step per reprice iteration in Maker Limit Chaser'),
    ('maker_partial_fill_ratio', '0.40', 'float', 'execution', 'Initial maker partial fill tranche ratio in paper simulation'),
    ('paper_slippage_impact_coeff', '0.10', 'float', 'execution', 'Square-root market impact coefficient for paper dynamic slippage'),
    ('paper_slippage_vol_ref_usdc', '250000.0', 'float', 'execution', 'Fallback 4H bar dollar volume reference (USDC) for paper slippage'),
    ('paper_slippage_min_pct', '0.0001', 'float', 'execution', 'Minimum paper dynamic slippage floor (1 bps)'),
    ('paper_slippage_max_pct', '0.015', 'float', 'execution', 'Maximum paper dynamic slippage ceiling (150 bps)'),
    ('hyperliquid_last_nonce_ms', '0', 'int', 'execution', 'Strictly monotonic millisecond nonce counter for Hyperliquid L1 actions'),
    ('default_fallback_reference_price_usdc', '100.0', 'float', 'universe', 'Last-resort fallback reference price in USDC when symbol is missing from universe_assets and market_ohlcv'),
    ('hyperliquid_paper_asset_indices_mainnet', '{"BTC":0,"ETH":1,"ATOM":2,"MATIC":3,"POL":3,"SOL":5,"AVAX":6,"BNB":7,"OP":9,"LTC":10,"ARB":11,"DOGE":12,"INJ":13,"SUI":14,"LINK":18,"APT":22,"XRP":25,"UNI":38,"DOT":42,"ADA":65,"NEAR":74,"TIA":96,"HYPE":159}', 'json', 'execution', 'Fallback paper/offline Hyperliquid Mainnet coin-to-asset-index map'),
    ('hyperliquid_paper_asset_indices_testnet', '{"SOL":0,"APT":1,"ATOM":2,"BTC":3,"ETH":4,"MATIC":5,"POL":5,"BNB":6,"AVAX":7,"LTC":10,"OP":11,"ARB":13,"INJ":18,"SUI":25,"LINK":28,"UNI":37,"ADA":45,"DOT":47,"NEAR":54,"XRP":62,"TIA":83,"HYPE":135,"DOGE":173}', 'json', 'execution', 'Fallback paper/offline Hyperliquid Testnet coin-to-asset-index map'),
    ('asset_index_cache_ttl_secs', '3600', 'int', 'execution', 'TTL in seconds for in-memory Hyperliquid asset index metadata cache'),
    ('default_funding_rate_hourly', '0.0', 'float', 'execution', 'Fallback hourly funding rate when live funding feed is unavailable (0.0 neutral)'),

    -- Alerting & Telemetry Parameters
    ('telegram_bot_token', '', 'string', 'alerts', 'Telegram Bot API token for real-time operational notifications (Fernet AES-128 encrypted)'),
    ('telegram_chat_id', '', 'string', 'alerts', 'Telegram target chat ID for real-time operational notifications'),
    ('max_daily_drawdown_alert_pct', '0.03', 'float', 'alerts', 'Daily portfolio drawdown threshold (3.0%) triggering critical Telegram alert'),
    ('telegram_api_base_url', 'https://api.telegram.org', 'string', 'alerts', 'Telegram Bot API base endpoint URL'),
    ('alert_http_timeout_sec', '5.0', 'float', 'alerts', 'HTTP timeout in seconds for Telegram/Discord webhook dispatch'),
    ('alert_http_timeout_secs', '5', 'int', 'alerts', 'HTTP timeout in seconds for Telegram/Discord webhook dispatch'),
    ('alert_max_retries', '4', 'int', 'alerts', 'Maximum delivery attempts in exponential-backoff alert retry queue'),
    ('alert_backoff_base_sec', '2.0', 'FLOAT', 'OPS', 'Base exponential backoff delay in seconds for alert retries'),
    ('alert_retry_base_backoff_sec', '0.35', 'float', 'alerts', 'Base backoff delay in seconds for alert retry worker'),
    ('alert_retry_base_backoff_ms', '350', 'int', 'alerts', 'Base backoff delay in milliseconds for alert retry worker'),
    ('alert_max_queue_size', '256', 'int', 'alerts', 'Bounded channel capacity for background alert dispatch worker'),
    ('alert_worker_queue_capacity', '256', 'int', 'alerts', 'Bounded channel capacity alias for background alert dispatch worker'),
    ('alerts_retry_queue_capacity', '64', 'int', 'alerts', 'Bounded retry queue capacity for failed webhook alert deliveries'),
    ('telemetry_queue_capacity', '2048', 'int', 'telemetry', 'Bounded channel capacity for background SQLite telemetry log writer');

DELETE FROM system_config WHERE config_key IN (
    'discord_webhook_url',
    'hyperliquid_default_vault_mainnet',
    'hyperliquid_default_vault_testnet',
    'hyperliquid_mainnet_vault_address',
    'hyperliquid_testnet_vault_address'
);

-- -----------------------------------------------------------------------------
-- 6. Idempotent Upgrade & Data Normalization Rules for Existing Databases
-- -----------------------------------------------------------------------------

-- 6a. Historical dry-run `PARTIAL_IDLE_CAPPED` book_transfers reclassification (R4-H2, R5-M2)
UPDATE book_transfers
SET status = 'DRY_RUN',
    transfer_mode = 'DRY_RUN_RECLASSIFIED'
WHERE status = 'PARTIAL_IDLE_CAPPED'
  AND (
      LOWER(COALESCE(tx_ref, '')) LIKE 'dryrun-%'
      OR UPPER(COALESCE(transfer_mode, '')) LIKE 'DRY_RUN%'
      OR (
          COALESCE(legs_completed, 0) = 0
          AND leg1_tx_ref IS NULL
          AND LOWER(COALESCE(tx_ref, '')) LIKE '0x%'
          AND UPPER(COALESCE(transfer_mode, 'LIVE')) IN ('LIVE', 'VIRTUAL', '')
          AND COALESCE((SELECT config_value FROM system_config WHERE config_key = 'pit_subaccount_address'), '') = ''
          AND COALESCE((SELECT config_value FROM system_config WHERE config_key = 'pond_subaccount_address'), '') = ''
      )
      OR (
          COALESCE((SELECT config_value FROM system_config WHERE config_key = 'dry_run_reclassify_before_utc'), '') != ''
          AND datetime(created_at) <= datetime((SELECT config_value FROM system_config WHERE config_key = 'dry_run_reclassify_before_utc'))
      )
      OR EXISTS (
          SELECT 1 FROM telemetry_logs t
          WHERE t.component LIKE 'aegis-settle%'
            AND (
                t.details_json LIKE '%"transfer_id":' || book_transfers.transfer_id || ',%'
                OR t.details_json LIKE '%"transfer_id": ' || book_transfers.transfer_id || ',%'
                OR t.details_json LIKE '%"transfer_id":' || book_transfers.transfer_id || '}%'
            )
            AND (
                t.message LIKE '%DRY_RUN%'
                OR LOWER(COALESCE(t.details_json, '')) LIKE '%"dry_run":true%'
                OR LOWER(COALESCE(t.details_json, '')) LIKE '%"dry_run": true%'
                OR LOWER(COALESCE(t.details_json, '')) LIKE '%dryrun-%'
            )
      )
  );

-- 6b. Backfill `last_funding_hour_bucket` on open/pending trades with DEFAULT 0 (R5-N2)
UPDATE trades
SET last_funding_hour_bucket = CAST(strftime('%s', 'now') AS INTEGER) / 3600
WHERE status IN ('OPEN', 'PENDING')
  AND COALESCE(last_funding_hour_bucket, 0) = 0
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 10);

-- 6c. One-time stop band normalization for pre-v11 `onchain-reconciled-%` open positions (R7-B1, R7-S2)
UPDATE trades
SET unrealized_pnl_usdc = ROUND(
        (CASE WHEN side = 'LONG' THEN (current_price - entry_price) ELSE (entry_price - current_price) END) * ABS(quantity),
        4
    ),
    chandelier_stop_price = CASE
        WHEN side = 'LONG' AND (
            chandelier_stop_price <= 0.0
            OR chandelier_stop_price >= current_price
            OR chandelier_stop_price < current_price * (1.0 - COALESCE((SELECT CAST(config_value AS REAL) FROM system_config WHERE config_key = 'chandelier_trail_max_pct'), 0.008))
        )
            THEN ROUND(current_price * (1.0 - COALESCE((SELECT CAST(config_value AS REAL) FROM system_config WHERE config_key = 'chandelier_trail_max_pct'), 0.008)), 6)
        WHEN side = 'SHORT' AND (
            chandelier_stop_price <= 0.0
            OR chandelier_stop_price <= current_price
            OR chandelier_stop_price > current_price * (1.0 + COALESCE((SELECT CAST(config_value AS REAL) FROM system_config WHERE config_key = 'chandelier_trail_max_pct'), 0.008))
        )
            THEN ROUND(current_price * (1.0 + COALESCE((SELECT CAST(config_value AS REAL) FROM system_config WHERE config_key = 'chandelier_trail_max_pct'), 0.008)), 6)
        ELSE chandelier_stop_price
    END
WHERE status = 'OPEN'
  AND current_price > 0.0
  AND entry_price > 0.0
  AND COALESCE(exchange_order_id, '') LIKE 'onchain-reconciled-%'
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 11);

-- 6d. Widen legacy 1.2x initial Chandelier ATR multipliers to 2.2x ATR when upgrading from < v13
UPDATE system_config
SET config_value = '2.2',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key IN ('chandelier_atr_multiplier_long', 'chandelier_atr_multiplier_short')
  AND config_value = '1.2'
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 13);

UPDATE system_config
SET config_value = '1000.0',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'pond_testnet_min_vol_cap_usdc'
  AND CAST(config_value AS REAL) < 1000.0
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 13);

-- 6e. Round 14 Pond (`NPS-01`) funding floor upgrade (0.000015 -> 0.000050) and non-null open cell stop backfill (Table 6)
UPDATE system_config
SET config_value = '0.000050',
    description = 'Minimum hourly funding floor magnitude (+0.0050%/h = 0.50 bps/hr)',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'pond_funding_floor_hourly'
  AND config_value IN ('0.000015', '1.5e-5', '0.0000150')
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 14);

UPDATE pond_champions
SET params_json = '{"funding_z_window_hours":168,"funding_z_entry":2.0,"funding_z_exit":0.5,"funding_floor_hourly":0.00005,"trend_veto_ema_fast":16,"trend_veto_ema_slow":64,"trend_veto_overextension_pct":0.08,"initial_stop_atr_mult":2.5,"chandelier_atr_mult":3.8,"wick_buffer_atr":0.85,"max_hold_hours":72,"min_norm_hold_hours":6.0,"require_2h_persistence":true}'
WHERE version = 'NPS-01'
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 14);

-- Backfill any NULL entry_price/quantity on OPEN/TRAILED pond_cells (Table 6)
UPDATE pond_cells
SET entry_price = COALESCE(
        entry_price,
        (SELECT m.close FROM market_ohlcv m WHERE m.symbol = pond_cells.symbol ORDER BY m.timestamp_ms DESC LIMIT 1),
        (SELECT u.reference_price FROM pond_universe u WHERE u.symbol = pond_cells.symbol),
        10.0
    ),
    quantity = COALESCE(quantity, 1.0)
WHERE state IN ('OPEN', 'TRAILED')
  AND (entry_price IS NULL OR entry_price <= 0.0 OR quantity IS NULL OR quantity <= 0.0);

-- Normalize open/trailed Pond cells to enforce 6% max stop distance, initial_stop floor/ceiling, and post-open watermarks
UPDATE pond_cells
SET initial_stop = CASE
        WHEN side = 'LONG' AND entry_price > 0.0 AND (initial_stop IS NULL OR initial_stop < entry_price * 0.94 OR initial_stop >= entry_price)
            THEN ROUND(entry_price * 0.94, 6)
        WHEN side = 'SHORT' AND entry_price > 0.0 AND (initial_stop IS NULL OR initial_stop > entry_price * 1.06 OR initial_stop <= entry_price)
            THEN ROUND(entry_price * 1.06, 6)
        ELSE initial_stop
    END,
    chandelier_stop = CASE
        WHEN side = 'LONG' AND entry_price > 0.0
            THEN MAX(
                COALESCE(chandelier_stop, entry_price * 0.94),
                CASE
                    WHEN initial_stop IS NULL OR initial_stop < entry_price * 0.94 OR initial_stop >= entry_price
                        THEN ROUND(entry_price * 0.94, 6)
                    ELSE initial_stop
                END
            )
        WHEN side = 'SHORT' AND entry_price > 0.0
            THEN MIN(
                CASE WHEN COALESCE(chandelier_stop, 0.0) > 0.0 THEN chandelier_stop ELSE entry_price * 1.06 END,
                CASE
                    WHEN initial_stop IS NULL OR initial_stop > entry_price * 1.06 OR initial_stop <= entry_price
                        THEN ROUND(entry_price * 1.06, 6)
                    ELSE initial_stop
                END
            )
        ELSE chandelier_stop
    END,
    highest_price_seen = COALESCE(
        MAX(
            entry_price,
            (SELECT MAX(m.high) FROM market_ohlcv m
             WHERE m.symbol = pond_cells.symbol AND m.timeframe = '1h'
               AND m.is_synthetic = 0
               AND pond_cells.opened_at IS NOT NULL
               AND (
                   m.timestamp_ms >= CAST(strftime('%s', pond_cells.opened_at) AS INTEGER) * 1000
                   OR (
                       m.timestamp_ms >= (CAST(strftime('%s', pond_cells.opened_at) AS INTEGER) / 3600) * 3600000
                       AND m.high <= pond_cells.entry_price * 1.08
                   )
               ))
        ),
        CASE
            WHEN highest_price_seen IS NULL OR highest_price_seen < entry_price OR highest_price_seen > entry_price * 1.08
                THEN entry_price
            ELSE highest_price_seen
        END
    ),
    lowest_price_seen = COALESCE(
        MIN(
            entry_price,
            (SELECT MIN(m.low) FROM market_ohlcv m
             WHERE m.symbol = pond_cells.symbol AND m.timeframe = '1h'
               AND m.is_synthetic = 0
               AND pond_cells.opened_at IS NOT NULL
               AND m.low > 0.0
               AND (
                   m.timestamp_ms >= CAST(strftime('%s', pond_cells.opened_at) AS INTEGER) * 1000
                   OR (
                       m.timestamp_ms >= (CAST(strftime('%s', pond_cells.opened_at) AS INTEGER) / 3600) * 3600000
                       AND m.low >= pond_cells.entry_price * 0.92
                   )
               ))
        ),
        CASE
            WHEN lowest_price_seen IS NULL OR lowest_price_seen <= 0.0 OR lowest_price_seen > entry_price OR lowest_price_seen < entry_price * 0.92
                THEN entry_price
            ELSE lowest_price_seen
        END
    ),
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE state IN ('OPEN', 'TRAILED')
  AND entry_price > 0.0
  AND (
      NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 13)
      OR initial_stop IS NULL
      OR chandelier_stop IS NULL
  );

-- -----------------------------------------------------------------------------
-- 7. Record Consolidated Schema Versions (1..14)
-- -----------------------------------------------------------------------------
UPDATE system_config
SET config_value = '14',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'schema_version';

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description) VALUES
    (1, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v01: Signal Config Expansion'),
    (2, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v02: Order FSM & Reconciliation Columns'),
    (3, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v03: Schema Versioning Ledger & Alerts'),
    (4, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v04: MACE Lifecycle Parity & Fernet Key Hardening'),
    (5, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v05: PP-50 Two-Book Platform (PIT + POND)'),
    (6, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v06: Zero Hardcoded Values & Security Config'),
    (7, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v07: Reference Prices & ISO-8601 Timestamps'),
    (8, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v08: Per-Hour Funding Bucket & Two-Leg Transfers'),
    (9, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v09: PARTIAL/AMBIGUOUS Transfer Statuses & Resting OID'),
    (10, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v10: Transactional Repair & Dry-Run Reclassification'),
    (11, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v11: Chandelier Tight-Trail & Break-Even Lock'),
    (12, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v12: Testnet Faucet Offset Config'),
    (13, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v13: Two-Tier Chandelier 2.2x ATR & Pond 6% Stop Cap'),
    (14, CAST(strftime('%s', 'now') AS INTEGER), 'Consolidated Schema v14: Pond NPS-01 Post-Mortem & Round 14 Remediation');
