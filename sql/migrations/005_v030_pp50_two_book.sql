-- Migration 005: PP-50 Two-Book Platform (Schema v4 -> v5, Additive)

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_utc INTEGER NOT NULL,
    description TEXT NOT NULL
);

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
    delisted_at TEXT
);
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
('NPS-01', '{"funding_z_window_hours":168,"funding_z_entry":2.0,"funding_z_exit":0.5,"funding_floor_hourly":0.000015,"trend_veto_ema_fast":16,"trend_veto_ema_slow":64,"trend_veto_overextension_pct":0.08,"initial_stop_atr_mult":2.5,"chandelier_atr_mult":3.8,"wick_buffer_atr":0.85,"max_hold_hours":72}', 1.85, 1, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));

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
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_pond_cells_state ON pond_cells (state, symbol);

CREATE TABLE IF NOT EXISTS book_transfers (
    transfer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    settle_date TEXT NOT NULL,
    from_book TEXT NOT NULL CHECK (from_book IN ('PIT', 'POND')),
    to_book TEXT NOT NULL CHECK (to_book IN ('PIT', 'POND')),
    amount_usdc REAL NOT NULL,
    pre_from_usdc REAL,
    pre_to_usdc REAL,
    post_from_usdc REAL,
    post_to_usdc REAL,
    drift_bps_before REAL,
    drift_bps_after REAL,
    blocked_remainder_usdc REAL NOT NULL DEFAULT 0.0,
    status TEXT NOT NULL CHECK (status IN (
        'PENDING', 'CONFIRMED', 'FAILED',
        'COMPLETED', 'PARTIAL_IDLE_CAPPED', 'PARTIAL', 'AMBIGUOUS',
        'SKIPPED_DEADBAND', 'SKIPPED_WITHIN_BAND', 'NO_ACTION_WITHIN_DEADBAND',
        'BLOCKED_ZERO_IDLE', 'SKIPPED_NO_IDLE', 'HALTED_PP50', 'DRY_RUN'
    )),
    attempts INTEGER NOT NULL DEFAULT 0,
    tx_ref TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

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

CREATE VIEW IF NOT EXISTS v_pond_universe_active AS
    SELECT * FROM pond_universe WHERE status = 'ACTIVE' ORDER BY rank;

CREATE VIEW IF NOT EXISTS v_pond_cells_open AS
    SELECT * FROM pond_cells WHERE state IN ('OPEN', 'TRAILED');

CREATE VIEW IF NOT EXISTS v_pp50_book_equity_latest AS
    SELECT * FROM pp50_platform_snapshots ORDER BY snapshot_id DESC LIMIT 1;

CREATE VIEW IF NOT EXISTS v_book_transfers_recent AS
    SELECT * FROM book_transfers WHERE settle_date >= date('now', '-30 day') ORDER BY transfer_id DESC;

CREATE VIEW IF NOT EXISTS v_trades_pit AS
    SELECT * FROM trades;

INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
-- Category: pond (Universe, Scanning & Direction Policy)
('pond_enabled', 'false', 'BOOL', 'pond', 'Master switch for Pond engine'),
('pond_execution_mode', 'PAPER', 'STR', 'pond', 'Pond execution routing mode: PAPER, TESTNET, or LIVE Web3 DEX routing'),
('pond_direction_policy', 'both', 'STR', 'pond', 'Direction mode: both, short_only, long_only'),
('pond_universe_target_size', '80', 'INT', 'pond', 'Target pond membership count'),
('pond_universe_refresh_weekday', '0', 'INT', 'pond', 'Weekly refresh day (0 = Monday)'),
('pond_universe_delist_rank', '95', 'INT', 'pond', 'Rank below which delist counter starts'),
('pond_universe_warmup_hours', '168', 'INT', 'pond', 'Warmup hours before new member is scannable'),
('pond_min_daily_volume_usdc', '50000.0', 'FLOAT', 'pond', '30-day median daily volume floor'),
('pond_scan_offset_seconds', '10', 'INT', 'pond', 'Delay after funding settle before S-cycle'),
('pond_funding_z_window_hours', '168', 'INT', 'pond', 'Trailing window for funding z-score'),
('pond_funding_z_entry', '2.0', 'FLOAT', 'pond', 'Minimum entry z-score elevation'),
('pond_funding_z_exit', '0.5', 'FLOAT', 'pond', 'Funding normalisation exit z-score'),
('pond_funding_floor_hourly', '0.000015', 'FLOAT', 'pond', 'Minimum hourly funding floor magnitude (+0.0015%/h)'),
('pond_trend_veto_ema_fast', '16', 'INT', 'pond', 'Fast EMA span for trend veto (4H bars)'),
('pond_trend_veto_ema_slow', '64', 'INT', 'pond', 'Slow EMA span for trend veto (4H bars)'),
('pond_trend_veto_overextension_pct', '0.08', 'FLOAT', 'pond', 'Veto when 4H close > EMA64 * 1.08 (or < 0.92 for longs)'),
('pond_liquidity_score_ref_usdc', '250000.0', 'FLOAT', 'pond', 'Volume reference for liquidity factor'),

-- Category: pond (Cells, Exits & Risk Grammar)
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

-- Category: settling & platform
('pond_ingest_stagger_seconds', '7.5', 'FLOAT', 'pond', 'Spacing of 1h full-sync calls'),
('pond_ohlcv_retention_days', '14', 'INT', 'pond', 'Pruning retention window for pond candles'),
('pond_champion_active', 'NPS-01', 'STR', 'pond', 'Active champion version'),
('pond_champion_recrown_margin_pct', '10.0', 'FLOAT', 'pond', 'Score margin required to re-crown'),
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
('platform_pp50_halt', 'false', 'BOOL', 'platform', 'Emergency platform-wide entry halt');

UPDATE system_config SET config_value = '10000.0' WHERE config_key = 'initial_equity_usdc' AND config_value = '999.0';

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
VALUES (5, CAST(strftime('%s', 'now') AS INTEGER), 'Migration 005: pp50 two-book platform');
