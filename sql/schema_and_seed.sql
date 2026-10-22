-- =============================================================================
-- Aegis Quant (v0.1.0-dev) — Master SQLite WAL Schema & Seed Configuration
-- Target DB Path: /home/fedora/aegis/data/aegis_quant.db
-- =============================================================================

PRAGMA busy_timeout = 15000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

-- -----------------------------------------------------------------------------
-- 1. System Configuration Table (60 Operational, Risk, ML & Web3 DEX Keys)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_config (
    config_key TEXT PRIMARY KEY,
    config_value TEXT NOT NULL,
    value_type TEXT NOT NULL DEFAULT 'string',
    category TEXT NOT NULL DEFAULT 'general',
    description TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
    -- Platform Identity & Runtime (1-5)
    ('platform_name', 'Aegis Quant', 'string', 'platform', 'Core platform brand and telemetry identifier'),
    ('platform_version', '0.1.0-dev', 'string', 'platform', 'SemVer pre-release tag'),
    ('environment', 'sandbox', 'string', 'platform', 'Deployment target environment (sandbox or production)'),
    ('execution_mode', 'PAPER', 'string', 'platform', 'Execution mode: PAPER or LIVE Web3 DEX routing'),
    ('exchange_id', 'hyperliquid', 'string', 'platform', 'CCXT async exchange identifier (Hyperliquid L1 CLOB)'),

    -- Web3 DEX & AI Credentials (6-10)
    ('wallet_address', '', 'string', 'credentials', 'Main Arbitrum Wallet Address holding USDC collateral'),
    ('private_key', '', 'string', 'credentials', 'Trade-Only API Agent Wallet Private Key (no withdrawal permissions)'),
    ('gemini_api_key', '', 'string', 'credentials', 'Google Gemini API key for quantitative telemetry diagnostics'),
    ('hyperliquid_testnet', 'true', 'bool', 'credentials', 'Route orders to Hyperliquid testnet when true'),
    ('hyperliquid_vault_address', '', 'string', 'credentials', 'Optional Hyperliquid sub-account or vault address'),

    -- Capital & Conviction-Scaled Risk Engine Parameters (11-26)
    ('base_currency', 'USDC', 'string', 'risk', 'Base collateral and settlement currency on Hyperliquid'),
    ('benchmark_symbol', 'BTC/USDC:USDC', 'string', 'risk', 'Benchmark major contract used for BTC_SYNTH ratio & HMM'),
    ('initial_equity_usdc', '10000.0', 'float', 'risk', 'Baseline initial portfolio equity in USDC ($10,000)'),
    ('baseline_risk_per_trade_usdc', '100.0', 'float', 'risk', '1.0% baseline risk budget per trade in USDC ($100)'),
    ('min_risk_per_trade_usdc', '50.0', 'float', 'risk', '0.5% minimum conviction-scaled risk per trade in USDC ($50)'),
    ('max_risk_per_trade_usdc', '150.0', 'float', 'risk', '1.5% maximum conviction-scaled risk per trade in USDC ($150)'),
    ('conviction_scale_min', '0.5', 'float', 'risk', 'Minimum conviction multiplier (0.5x baseline risk)'),
    ('conviction_scale_max', '1.5', 'float', 'risk', 'Maximum conviction multiplier (1.5x baseline risk)'),
    ('max_portfolio_heat_usdc', '400.0', 'float', 'risk', 'Maximum total portfolio open stop-loss risk in USDC ($400)'),
    ('max_portfolio_heat_pct', '0.04', 'float', 'risk', 'Maximum total portfolio heat as fraction of equity (4.0%)'),
    ('max_single_asset_notional_usdc', '2500.0', 'float', 'risk', 'Maximum single-asset position notional in USDC ($2,500)'),
    ('max_single_asset_pct', '0.25', 'float', 'risk', 'Maximum single-asset concentration fraction of equity (25%)'),
    ('max_gross_leverage_notional_usdc', '15000.0', 'float', 'risk', 'Maximum total gross notional exposure in USDC ($15,000)'),
    ('max_gross_leverage', '1.5', 'float', 'risk', 'Maximum portfolio gross leverage multiplier (1.5x)'),
    ('max_concurrent_long_positions', '2', 'int', 'risk', 'Maximum concurrent Bull-regime relative-strength Long positions'),
    ('max_concurrent_short_positions', '2', 'int', 'risk', 'Maximum concurrent Bear-regime relative-weakness Short positions'),

    -- Hyperliquid Fee, Funding & Basis/HLP Yield Harvester Model (27-36)
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

    -- 3-State Diagonal-Covariance Gaussian HMM Regime Gate (37-44)
    ('hmm_num_states', '3', 'int', 'hmm', '3 hidden Markov states: Bull, Bear, Chop'),
    ('hmm_covariance_type', 'diag', 'string', 'hmm', 'Diagonal covariance matrix structure for Gaussian HMM'),
    ('hmm_window_bars', '540', 'int', 'hmm', 'Rolling 540-bar (90-day at 4H) evaluation window'),
    ('hmm_timeframe', '4h', 'string', 'hmm', 'Primary timeframe for HMM regime decoding'),
    ('hmm_em_iterations', '25', 'int', 'hmm', 'Baum-Welch Expectation-Maximization iterations per fit'),
    ('hmm_bull_prob_threshold', '0.50', 'float', 'hmm', 'Minimum posterior probability gamma_t(Bull) to open Long leaders'),
    ('hmm_bear_prob_threshold', '0.50', 'float', 'hmm', 'Minimum posterior probability gamma_t(Bear) to open Short laggards'),
    ('hmm_chop_prob_threshold', '0.55', 'float', 'hmm', 'Posterior probability threshold identifying sideways Chop regime'),

    -- MACE Non-Linear Ensemble & Continuous SNR Momentum Filter (45-52)
    ('mace_rf_n_estimators', '60', 'int', 'mace', 'Random Forest tree count in MACE non-linear ensemble (60 trees)'),
    ('mace_rf_max_depth', '4', 'int', 'mace', 'Regularized maximum tree depth for MACE Random Forest (depth 4)'),
    ('mace_rf_min_samples_leaf', '10', 'int', 'mace', 'Minimum samples per leaf in MACE Random Forest'),
    ('mace_ridge_alpha', '1.0', 'float', 'mace', 'L2 penalty alpha for MACE Ridge regression component'),
    ('mace_blend_weight', '0.35', 'float', 'mace', 'Ensemble blend weight assigned to non-linear MACE score (35%)'),
    ('snr_blend_weight', '0.65', 'float', 'mace', 'Ensemble blend weight assigned to continuous SNR momentum (65%)'),
    ('snr_fast_span', '16', 'int', 'mace', 'Fast exponential lookback span for multi-horizon SNR forecast'),
    ('snr_slow_span', '64', 'int', 'mace', 'Slow exponential lookback span for multi-horizon SNR forecast'),

    -- Chandelier Trailing Stops, Orchestrator & Calibration (53-60)
    ('atr_period', '14', 'int', 'stops', 'Average True Range lookback period in bars'),
    ('chandelier_lookback_bars', '22', 'int', 'stops', 'Lookback bars for Chandelier high/low anchor'),
    ('chandelier_atr_multiplier_long', '3.0', 'float', 'stops', 'Chandelier trailing stop ATR multiplier for Long positions'),
    ('chandelier_atr_multiplier_short', '3.8', 'float', 'stops', 'Inverted Chandelier trailing stop ATR multiplier for Short positions'),
    ('polling_interval_seconds', '15', 'int', 'orchestrator', 'Main engine ingestion and signal evaluation loop interval in seconds'),
    ('ohlcv_fetch_limit', '600', 'int', 'orchestrator', 'Number of historical candles fetched per symbol (>= 540 HMM bars)'),
    ('calibration_walk_forward_days', '90', 'int', 'calibration', 'Walk-forward window in days for per-asset timeframe calibration'),
    ('web_server_port', '8000', 'int', 'web', 'FastAPI REST + SSE telemetry server listening port');

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
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

INSERT OR IGNORE INTO universe_assets (
    symbol, base_asset, quote_asset, instrument_type, tier, tier_name,
    exec_timeframe, rs_timeframe, tick_size, lot_step_size, min_notional_usdc, max_leverage, is_active
) VALUES
    -- 16 Active Hyperliquid USDC_PERP Contracts (1 Benchmark BTC + 15 Altcoins)
    ('BTC/USDC:USDC',  'BTC',  'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.1,     0.0001, 10.0, 5.0, 1),
    ('ETH/USDC:USDC',  'ETH',  'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.05,    0.001,  10.0, 5.0, 1),
    ('SOL/USDC:USDC',  'SOL',  'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.01,    0.01,   10.0, 5.0, 1),
    ('HYPE/USDC:USDC', 'HYPE', 'USDC', 'USDC_PERP', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.001,   0.1,    10.0, 5.0, 1),
    ('XRP/USDC:USDC',  'XRP',  'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0001,  1.0,    10.0, 3.0, 1),
    ('DOGE/USDC:USDC', 'DOGE', 'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00001, 10.0,   10.0, 3.0, 1),
    ('ADA/USDC:USDC',  'ADA',  'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0001,  1.0,    10.0, 3.0, 1),
    ('AVAX/USDC:USDC', 'AVAX', 'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.005,   0.1,    10.0, 3.0, 1),
    ('LINK/USDC:USDC', 'LINK', 'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.001,   0.1,    10.0, 3.0, 1),
    ('SUI/USDC:USDC',  'SUI',  'USDC', 'USDC_PERP', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0005,  0.1,    10.0, 3.0, 1),
    ('DOT/USDC:USDC',  'DOT',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.001,   0.1,    10.0, 2.0, 1),
    ('NEAR/USDC:USDC', 'NEAR', 'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.001,   0.1,    10.0, 2.0, 1),
    ('APT/USDC:USDC',  'APT',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.001,   0.1,    10.0, 2.0, 1),
    ('ARB/USDC:USDC',  'ARB',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0001,  1.0,    10.0, 2.0, 1),
    ('OP/USDC:USDC',   'OP',   'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0001,  1.0,    10.0, 2.0, 1),
    ('INJ/USDC:USDC',  'INJ',  'USDC', 'USDC_PERP', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.005,   0.1,    10.0, 2.0, 1),

    -- 15 Active Synthetic ALT/BTC Cross-Pairs (BTC_SYNTH)
    ('ETH/BTC:SYNTH',  'ETH',  'BTC',  'BTC_SYNTH', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.000001,   0.001, 10.0, 5.0, 1),
    ('SOL/BTC:SYNTH',  'SOL',  'BTC',  'BTC_SYNTH', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.0000001,  0.01,  10.0, 5.0, 1),
    ('HYPE/BTC:SYNTH', 'HYPE', 'BTC',  'BTC_SYNTH', 1, 'TIER_1_MAJOR',     '1h', '4h', 0.00000001, 0.1,   10.0, 5.0, 1),
    ('XRP/BTC:SYNTH',  'XRP',  'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 1.0,   10.0, 3.0, 1),
    ('DOGE/BTC:SYNTH', 'DOGE', 'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 10.0,  10.0, 3.0, 1),
    ('ADA/BTC:SYNTH',  'ADA',  'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 1.0,   10.0, 3.0, 1),
    ('AVAX/BTC:SYNTH', 'AVAX', 'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0000001,  0.1,   10.0, 3.0, 1),
    ('LINK/BTC:SYNTH', 'LINK', 'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.0000001,  0.1,   10.0, 3.0, 1),
    ('SUI/BTC:SYNTH',  'SUI',  'BTC',  'BTC_SYNTH', 2, 'TIER_2_LARGE_CAP', '1h', '4h', 0.00000001, 0.1,   10.0, 3.0, 1),
    ('DOT/BTC:SYNTH',  'DOT',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0000001,  0.1,   10.0, 2.0, 1),
    ('NEAR/BTC:SYNTH', 'NEAR', 'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.00000001, 0.1,   10.0, 2.0, 1),
    ('APT/BTC:SYNTH',  'APT',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0000001,  0.1,   10.0, 2.0, 1),
    ('ARB/BTC:SYNTH',  'ARB',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.00000001, 1.0,   10.0, 2.0, 1),
    ('OP/BTC:SYNTH',   'OP',   'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.00000001, 1.0,   10.0, 2.0, 1),
    ('INJ/BTC:SYNTH',  'INJ',  'BTC',  'BTC_SYNTH', 3, 'TIER_3_HIGH_BETA', '1h', '4h', 0.0000001,  0.1,   10.0, 2.0, 1);

-- -----------------------------------------------------------------------------
-- 3. Database Views (v_universe_usdc_perp, v_universe_btc_synth, v_universe_alt_pairs)
-- -----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS v_universe_usdc_perp AS
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
    updated_at
FROM universe_assets
WHERE instrument_type = 'USDC_PERP' AND is_active = 1
ORDER BY tier ASC, asset_id ASC;

CREATE VIEW IF NOT EXISTS v_universe_btc_synth AS
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
    updated_at
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

-- -----------------------------------------------------------------------------
-- 4. Time-Series & Ledger Tables
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
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_signal_evaluations_ts
    ON signal_evaluations (timestamp_ms DESC, rs_rank ASC);

CREATE TABLE IF NOT EXISTS trades (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    base_asset TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('LONG', 'SHORT', 'BASIS_HARVEST')),
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED', 'CANCELLED')) DEFAULT 'OPEN',
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
    exit_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_trades_status
    ON trades (status, opened_at DESC);

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

-- Seed initial $10,000 USDC baseline snapshot if empty
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
