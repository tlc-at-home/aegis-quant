-- =============================================================================
-- Migration 007: Aegis Quant v0.3.0-rc1 Round-2 Remediation
-- Schema Version: 7
-- =============================================================================

-- 1. Reference prices stored in DB (HP: eliminate hardcoded price maps in code)
ALTER TABLE universe_assets ADD COLUMN reference_price REAL NOT NULL DEFAULT 10.0;

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
    ELSE 10.0
END;

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

-- 2. Pond universe reference_price & synthetic volume provenance flag (HP, NM-13)
ALTER TABLE pond_universe ADD COLUMN reference_price REAL NOT NULL DEFAULT 10.0;
ALTER TABLE pond_universe ADD COLUMN is_synthetic_volume INTEGER NOT NULL DEFAULT 0;

UPDATE pond_universe
SET reference_price = COALESCE(
    (SELECT ua.reference_price FROM universe_assets ua WHERE ua.symbol = pond_universe.symbol),
    10.0
);

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

-- 3. Signal evaluations full component persistence (NL-9)
ALTER TABLE signal_evaluations ADD COLUMN snr_synth REAL NOT NULL DEFAULT 0.0;
ALTER TABLE signal_evaluations ADD COLUMN rs_96 REAL NOT NULL DEFAULT 0.0;
ALTER TABLE signal_evaluations ADD COLUMN r_usdc_8 REAL NOT NULL DEFAULT 0.0;

-- 4. Unique index on trades(client_order_id) for idempotent trade insertion (NM-14)
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_client_order_id_unique
    ON trades(client_order_id)
    WHERE client_order_id IS NOT NULL AND client_order_id != '';

-- 5. Canonicalize all stored timestamps to ISO-8601 UTC (YYYY-MM-DDTHH:MM:SSZ) (NH-5)
UPDATE pond_universe
SET added_at = strftime('%Y-%m-%dT%H:%M:%SZ', added_at)
WHERE added_at IS NOT NULL AND added_at NOT LIKE '%T%';

UPDATE pond_universe
SET warmup_until = strftime('%Y-%m-%dT%H:%M:%SZ', warmup_until)
WHERE warmup_until IS NOT NULL AND warmup_until NOT LIKE '%T%';

UPDATE pond_universe
SET delisted_at = strftime('%Y-%m-%dT%H:%M:%SZ', delisted_at)
WHERE delisted_at IS NOT NULL AND delisted_at NOT LIKE '%T%';

UPDATE pond_scan_cycles
SET started_at = strftime('%Y-%m-%dT%H:%M:%SZ', started_at)
WHERE started_at IS NOT NULL AND started_at NOT LIKE '%T%';

UPDATE pond_cells
SET opened_at = strftime('%Y-%m-%dT%H:%M:%SZ', opened_at)
WHERE opened_at IS NOT NULL AND opened_at NOT LIKE '%T%';

UPDATE pond_cells
SET closed_at = strftime('%Y-%m-%dT%H:%M:%SZ', closed_at)
WHERE closed_at IS NOT NULL AND closed_at NOT LIKE '%T%';

UPDATE pond_cells
SET cooldown_until = strftime('%Y-%m-%dT%H:%M:%SZ', cooldown_until)
WHERE cooldown_until IS NOT NULL AND cooldown_until NOT LIKE '%T%';

UPDATE pond_cells
SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', updated_at)
WHERE updated_at IS NOT NULL AND updated_at NOT LIKE '%T%';

UPDATE book_transfers
SET created_at = strftime('%Y-%m-%dT%H:%M:%SZ', created_at)
WHERE created_at IS NOT NULL AND created_at NOT LIKE '%T%';

UPDATE trades
SET opened_at = strftime('%Y-%m-%dT%H:%M:%SZ', opened_at)
WHERE opened_at IS NOT NULL AND opened_at NOT LIKE '%T%';

UPDATE trades
SET closed_at = strftime('%Y-%m-%dT%H:%M:%SZ', closed_at)
WHERE closed_at IS NOT NULL AND closed_at NOT LIKE '%T%';

-- 6. Scale default PIT risk caps to 50% book scope ($5,000 of $10,000) and record explicit scope (NM-15)
UPDATE system_config SET config_value = '50.0'   WHERE config_key = 'baseline_risk_per_trade_usdc'      AND config_value = '100.0';
UPDATE system_config SET config_value = '25.0'   WHERE config_key = 'min_risk_per_trade_usdc'           AND config_value = '50.0';
UPDATE system_config SET config_value = '75.0'   WHERE config_key = 'max_risk_per_trade_usdc'           AND config_value = '150.0';
UPDATE system_config SET config_value = '200.0'  WHERE config_key = 'max_portfolio_heat_usdc'           AND config_value = '400.0';
UPDATE system_config SET config_value = '1250.0' WHERE config_key = 'max_single_asset_notional_usdc'    AND config_value = '2500.0';
UPDATE system_config SET config_value = '7500.0' WHERE config_key = 'max_gross_leverage_notional_usdc'  AND config_value = '15000.0';

-- 7. Seed Round-2 remediation configuration parameters in system_config (No hardcoded values in code)
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
    ('settle_gate_ttl_ms', '300000', 'int', 'settling', 'Maximum TTL in milliseconds (5m) before a stale platform_settle_gate lock auto-expires'),
    ('platform_settle_gate_armed_at_ms', '0', 'int', 'settling', 'Epoch timestamp (ms) when platform_settle_gate was last armed'),
    ('platform_settle_lock_holder', '', 'string', 'settling', 'Owner identifier of the active daily settling mutex lock'),
    ('hyperliquid_last_nonce_ms', '0', 'int', 'execution', 'Strictly monotonic millisecond nonce counter for Hyperliquid L1 actions'),
    ('hyperliquid_agent_address', '', 'string', 'credentials', 'Optional delegated Hyperliquid API Agent address when signing with an agent key'),
    ('hyperliquid_info_url_override', '', 'string', 'execution', 'Optional local test override URL for Hyperliquid /info endpoint'),
    ('hyperliquid_exchange_url_override', '', 'string', 'execution', 'Optional local test override URL for Hyperliquid /exchange endpoint'),
    ('default_fallback_reference_price_usdc', '100.0', 'float', 'universe', 'Last-resort fallback reference price in USDC when symbol is missing from universe_assets and market_ohlcv'),
    ('pond_fallback_base_price_usdc', '10.0', 'float', 'pond', 'Base price in USDC for synthetic Pond OHLCV fallback calculation'),
    ('pond_fallback_rank_slope_usdc', '0.75', 'float', 'pond', 'Rank slope in USDC for synthetic Pond OHLCV fallback calculation'),
    ('hyperliquid_paper_asset_indices_mainnet', '{"BTC":0,"ETH":1,"ATOM":2,"MATIC":3,"POL":3,"SOL":5,"AVAX":6,"BNB":7,"OP":9,"LTC":10,"ARB":11,"DOGE":12,"INJ":13,"SUI":14,"LINK":18,"APT":22,"XRP":25,"UNI":38,"DOT":42,"ADA":65,"NEAR":74,"TIA":96,"HYPE":159}', 'json', 'execution', 'Fallback paper/offline Hyperliquid Mainnet coin-to-asset-index map'),
    ('hyperliquid_paper_asset_indices_testnet', '{"SOL":0,"APT":1,"ATOM":2,"BTC":3,"ETH":4,"MATIC":5,"POL":5,"BNB":6,"AVAX":7,"LTC":10,"OP":11,"ARB":13,"INJ":18,"SUI":25,"LINK":28,"UNI":37,"ADA":45,"DOT":47,"NEAR":54,"XRP":62,"TIA":83,"HYPE":135,"DOGE":173}', 'json', 'execution', 'Fallback paper/offline Hyperliquid Testnet coin-to-asset-index map'),
    ('asset_index_cache_ttl_secs', '3600', 'int', 'execution', 'TTL in seconds for in-memory Hyperliquid asset index metadata cache'),
    ('max_bar_staleness_ms', '28800000', 'int', 'orchestrator', 'Maximum allowed OHLCV bar staleness in milliseconds (8h) before live signal gating'),
    ('max_consecutive_api_failures', '5', 'int', 'orchestrator', 'Consecutive exchange API failures before raising degraded health alert'),
    ('risk_caps_reference_scope', 'BOOK', 'string', 'risk', 'Reference equity scope for PIT dollar risk caps: BOOK (50% split) or PLATFORM (100%)'),
    ('web_allowed_hosts', 'localhost,127.0.0.1,[::1],aegis-quant-sandbox,192.168.122.254', 'string', 'security', 'Comma-separated Host header allowlist for DNS-rebinding protection');

-- 8. Remove redundant duplicate HLP vault address keys and harden CSP policy (NL-1)
DELETE FROM system_config WHERE config_key IN (
    'hyperliquid_default_vault_mainnet',
    'hyperliquid_default_vault_testnet',
    'hyperliquid_mainnet_vault_address',
    'hyperliquid_testnet_vault_address'
);

UPDATE system_config
SET config_value = 'default-src ''self''; script-src ''self''; style-src ''self'' ''unsafe-inline''; img-src ''self'' data:; connect-src ''self''; frame-ancestors ''none''; base-uri ''self''; form-action ''self'''
WHERE config_key = 'web_csp_policy';

-- 9. Advance schema version to 7
INSERT OR REPLACE INTO system_config (config_key, config_value, value_type, category, description, updated_at)
VALUES ('schema_version', '7', 'int', 'platform', 'Current SQLite schema migration version', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
