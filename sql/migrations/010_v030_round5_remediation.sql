-- =============================================================================
-- AEGIS QUANT v0.3.0 — MIGRATION 010: ROUND-5 REMEDIATION
-- Schema Version: 10
-- =============================================================================
-- Remediates:
--   1. R5-M1: Clean up any leftover `book_transfers_v9` staging table from an
--      interrupted v9 rebuild and ensure `v_book_transfers_recent` view exists.
--   2. R5-M2 / R4-H2: Extend historical dry-run `PARTIAL_IDLE_CAPPED`
--      reclassification to cover Round-2-era `PAPER`-mode rows (where no real
--      subaccount transfer could occur) and support operator-configured
--      cutoff windows via `dry_run_reclassify_before_utc`.
--   3. R5-N2: Backfill `trades.last_funding_hour_bucket` on existing open/pending
--      trades that migrated with `DEFAULT 0` to prevent a one-time boundary
--      double-accrual on upgrade.
-- =============================================================================

-- 1. Clean up any residual staging table from v9 rebuild (R5-M1)
DROP TABLE IF EXISTS book_transfers_v9;

CREATE VIEW IF NOT EXISTS v_book_transfers_recent AS
SELECT *
FROM book_transfers
ORDER BY transfer_id DESC
LIMIT 30;

-- 2. Seed optional operator cutoff timestamp for dry-run reclassification (R5-M2)
INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at)
VALUES (
    'dry_run_reclassify_before_utc',
    '',
    'string',
    'settling',
    'Optional ISO-8601 UTC cutoff timestamp before which historical PARTIAL_IDLE_CAPPED rows are reclassified to DRY_RUN',
    strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
)
ON CONFLICT(config_key) DO NOTHING;

-- 3. Reclassify historical pre-v10 dry-run `PARTIAL_IDLE_CAPPED` rows across PAPER/TESTNET/LIVE (R5-M2)
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

-- 4. Backfill `last_funding_hour_bucket` on open/pending trades that still have DEFAULT 0 (R5-N2)
UPDATE trades
SET last_funding_hour_bucket = CAST(strftime('%s', 'now') AS INTEGER) / 3600
WHERE status IN ('OPEN', 'PENDING')
  AND COALESCE(last_funding_hour_bucket, 0) = 0;

-- 5. Tighten Chandelier trailing stop defaults in system_config
UPDATE system_config
SET config_value = '1.2',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key IN ('chandelier_atr_multiplier_long', 'chandelier_atr_multiplier_short');

UPDATE system_config
SET config_value = '0.004',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'chandelier_min_atr_pct';

UPDATE system_config
SET config_value = '0.35',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key IN ('chandelier_wick_overshoot_atr', 'chandelier_wick_buffer_atr');

INSERT OR REPLACE INTO system_config (config_key, config_value, value_type, category, description, updated_at) VALUES
    ('chandelier_wick_buffer_atr', '0.35', 'float', 'stops', 'Intrabar wick overshoot buffer in ATR multiples before Tier-1 emergency stop', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('chandelier_trail_atr_multiplier', '0.45', 'float', 'stops', 'Tight current-price trailing stop distance in ATR multiples', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('chandelier_trail_max_pct', '0.008', 'float', 'stops', 'Maximum trailing stop distance as fraction of current price (0.8%)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('orphan_reconcile_stop_pct', '0.015', 'float', 'stops', 'Initial protective stop distance fraction for reconciled on-chain orphan positions (1.5%)', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('long_break_even_trigger_atr', '0.8', 'float', 'stops', 'Favorable excursion in ATR multiples to lock break-even on Long positions', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('long_break_even_lock_atr', '0.25', 'float', 'stops', 'Locked profit buffer in ATR multiples above entry once Long break-even triggers', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('short_break_even_trigger_atr', '0.8', 'float', 'stops', 'Favorable excursion in ATR multiples to lock break-even on Short positions', strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    ('short_break_even_lock_atr', '0.25', 'float', 'stops', 'Locked profit buffer in ATR multiples below entry once Short break-even triggers', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));

-- 6. Bump schema_version to 10
UPDATE system_config
SET config_value = '10',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'schema_version';

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
VALUES (10, CAST(strftime('%s', 'now') AS INTEGER), '010_v030_round5_remediation: Transactional migration repair, PAPER dry-run reclassification, and trades funding bucket backfill');

