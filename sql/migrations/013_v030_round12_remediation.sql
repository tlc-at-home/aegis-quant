-- =============================================================================
-- AEGIS QUANT v0.3.0 — MIGRATION 013: ROUND-12 RISK, POND & RECONCILIATION HARDENING
-- Schema Version: 13
-- =============================================================================
-- Remediates:
--   1. Fix 1: Widen default initial Chandelier ATR multipliers (`chandelier_atr_multiplier_long`,
--      `chandelier_atr_multiplier_short`) from 1.2x to 2.2x ATR while gating tight-trail
--      (`0.45x ATR` / `0.8%` cap) behind `tight_trail_trigger_atr = 1.0` ATR favorable excursion.
--   2. Fix 2 & Fix 6: Seed `pond_max_stop_distance_pct` (`0.06`), `pond_testnet_min_vol_cap_usdc`
--      (`5000.0`), `signal_short_bounce_veto_pct` (`0.020`), `signal_long_crash_veto_pct` (`0.020`),
--      and `max_bar_staleness_ms` (`21600000` = 6h) in `system_config` idempotently.
--   3. Fix 2: Normalize any open `pond_cells` whose `initial_stop` or `chandelier_stop` violated
--      the 6% maximum stop distance cap or `initial_stop` floor/ceiling invariant.
--   4. Bump `schema_version` to 13 and record migration 13 in `schema_migrations`.
-- =============================================================================

-- 1. Widen legacy 1.2x initial Chandelier ATR multipliers to 2.2x ATR (guarded by version < 13)
UPDATE system_config
SET config_value = '2.2',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key IN ('chandelier_atr_multiplier_long', 'chandelier_atr_multiplier_short')
  AND config_value = '1.2'
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 13);

-- 2. Seed Round-12 risk, Pond, signal, and health-staleness configuration keys idempotently
INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at) VALUES
    ('tight_trail_trigger_atr', '1.0', 'float', 'stops', 'Minimum favorable excursion in ATR multiples before tight current-price trailing stop activates', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('pond_max_stop_distance_pct', '0.06', 'float', 'pond', 'Maximum initial protective stop distance as fraction of reference price for Pond NPS-01 cells (6.0%)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('pond_testnet_min_vol_cap_usdc', '5000.0', 'float', 'pond', 'Minimum 24h USDC volume floor for Pond universe admission on Hyperliquid Testnet', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('signal_short_bounce_veto_pct', '0.020', 'float', 'signals', 'Short-horizon (12h/4h) upward bounce threshold vetoing lagging 4H SHORT entries (2.0%)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('signal_long_crash_veto_pct', '0.020', 'float', 'signals', 'Short-horizon (12h/4h) downward crash threshold vetoing lagging 4H LONG entries (2.0%)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('max_bar_staleness_ms', '21600000', 'int', 'execution', 'Maximum allowed primary OHLCV bar staleness in milliseconds before /health reports 503 DEGRADED in LIVE/TESTNET (6h)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
ON CONFLICT(config_key) DO NOTHING;

-- 3. Ensure Pond Testnet volume floor is raised from legacy $100 to $5,000 when upgrading to v13
UPDATE system_config
SET config_value = '5000.0',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'pond_testnet_min_vol_cap_usdc'
  AND CAST(config_value AS REAL) < 5000.0
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 13);

-- 4. One-time normalization of open Pond cells to enforce 6% max stop distance and initial_stop floor/ceiling
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
    END
WHERE state = 'OPEN'
  AND entry_price > 0.0
  AND NOT EXISTS (SELECT 1 FROM schema_migrations WHERE version >= 13);

-- 5. Bump schema_version to 13
UPDATE system_config
SET config_value = '13',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'schema_version';

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
VALUES (13, CAST(strftime('%s', 'now') AS INTEGER), '013_v030_round12_remediation: Two-tier Chandelier 2.2x ATR initial stop + 1.0x ATR tight-trail gate, Pond 6% stop cap & $5k Testnet volume floor, and short-horizon reversal veto');
