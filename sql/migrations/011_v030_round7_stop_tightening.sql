-- =============================================================================
-- AEGIS QUANT v0.3.0 — MIGRATION 011: ROUND-7 CHANDELIER STOP TIGHTENING
-- Schema Version: 11
-- =============================================================================
-- Remediates:
--   1. R7-S2: Additive schema v11 migration replacing post-release edits to
--      frozen migration 010 and removing runtime config/trades mutations from
--      `reconcile_routing_config` (`R7-B1`, `R7-QA`).
--   2. R7-S1: Seed Chandelier tight-trail and break-even lock configuration
--      parameters idempotently (`ON CONFLICT(config_key) DO NOTHING`).
--   3. R7-B1: One-time stop band migration for pre-v11 `onchain-reconciled-%`
--      orphan positions using configured `chandelier_trail_max_pct`.
-- =============================================================================

-- 1. One-time transition of legacy seed stop defaults when upgrading a pre-tight-trail database (R7-S2)
--    Guarded by absence of `chandelier_trail_max_pct` and `schema_migrations.version >= 11` so
--    databases that already ran the tight-trail transition or are re-running migrations never
--    overwrite operator-calibrated values.
UPDATE system_config
SET config_value = '1.2',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key IN ('chandelier_atr_multiplier_long', 'chandelier_atr_multiplier_short')
  AND config_value IN ('3.0', '3.5', '3.8')
  AND NOT EXISTS (SELECT 1 FROM system_config WHERE config_key = 'chandelier_trail_max_pct')
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 11);

UPDATE system_config
SET config_value = '0.004',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'chandelier_min_atr_pct'
  AND config_value = '0.022'
  AND NOT EXISTS (SELECT 1 FROM system_config WHERE config_key = 'chandelier_trail_max_pct')
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 11);

UPDATE system_config
SET config_value = '0.35',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key IN ('chandelier_wick_overshoot_atr', 'chandelier_wick_buffer_atr')
  AND config_value = '0.85'
  AND NOT EXISTS (SELECT 1 FROM system_config WHERE config_key = 'chandelier_trail_max_pct')
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 11);

-- 2. Seed new Chandelier tight-trail, orphan stop, and break-even lock keys idempotently (R7-S1, R7-S2)
INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at) VALUES
    ('chandelier_wick_buffer_atr', '0.35', 'float', 'stops', 'Intrabar wick overshoot buffer in ATR multiples before Tier-1 emergency stop', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('chandelier_trail_atr_multiplier', '0.45', 'float', 'stops', 'Tight current-price trailing stop distance in ATR multiples', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('chandelier_trail_max_pct', '0.008', 'float', 'stops', 'Maximum trailing stop distance as fraction of current price (0.8%)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('orphan_reconcile_stop_pct', '0.015', 'float', 'stops', 'Initial protective stop distance fraction for reconciled on-chain orphan positions (1.5%)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('long_break_even_trigger_atr', '0.8', 'float', 'stops', 'Favorable excursion in ATR multiples to lock break-even on Long positions', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('long_break_even_lock_atr', '0.25', 'float', 'stops', 'Locked profit buffer in ATR multiples above entry once Long break-even triggers', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('short_break_even_trigger_atr', '0.8', 'float', 'stops', 'Favorable excursion in ATR multiples to lock break-even on Short positions', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('short_break_even_lock_atr', '0.25', 'float', 'stops', 'Locked profit buffer in ATR multiples below entry once Short break-even triggers', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
ON CONFLICT(config_key) DO NOTHING;

-- 3. One-time stop band normalization for pre-existing `onchain-reconciled-%` open positions (R7-B1)
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

-- 4. Bump schema_version to 11
UPDATE system_config
SET config_value = '11',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'schema_version';

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
VALUES (11, CAST(strftime('%s', 'now') AS INTEGER), '011_v030_round7_stop_tightening: Additive Chandelier tight-trail and break-even lock config seed and one-time orphan stop normalization');
