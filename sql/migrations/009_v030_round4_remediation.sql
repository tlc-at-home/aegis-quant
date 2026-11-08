-- =============================================================================
-- AEGIS QUANT v0.3.0 — MIGRATION 009: ROUND-4 REMEDIATION
-- Schema Version: 9
-- =============================================================================
-- Remediates:
--   1. R4-H1 / R4-QA: Expand `book_transfers.status` CHECK constraint to include
--      'PARTIAL', 'AMBIGUOUS', 'SKIPPED_WITHIN_BAND', 'SKIPPED_NO_IDLE', 'HALTED_PP50'
--      so partial/ambiguous live subaccount transfers are explicitly quarantined.
--   2. R4-H2: Reclassify historical dry-run `PARTIAL_IDLE_CAPPED` rows correlated with
--      dry-run telemetry or `dryrun-` tx_ref prefixes.
--   3. R4-N4: Add `last_funding_hour_bucket` to `trades` for idempotent PIT hourly funding accrual.
--   4. R4-N11: Add dedicated `resting_order_id` column to `pond_cells` so resting exchange
--      order IDs do not overload `exit_reason`.
--   5. R4-M1 / R4-N1 / R4-N6 / R4-QB: Seed DB-backed configuration keys for Pit/Pond tick
--      intervals, BTC synthetic fallback base price, Pond daily volume minimum bars per UTC day,
--      and stranded transfer alert cooldown / stale pending TTL.
-- =============================================================================

-- 1. Rebuild `book_transfers` with expanded status CHECK constraint (R4-H1, R4-QA)
DROP VIEW IF EXISTS v_book_transfers_recent;

CREATE TABLE IF NOT EXISTS book_transfers_v9 (
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

INSERT INTO book_transfers_v9 (
    transfer_id, settle_date, from_book, to_book, amount_usdc,
    pre_from_usdc, pre_to_usdc, post_from_usdc, post_to_usdc,
    drift_bps_before, drift_bps_after, blocked_remainder_usdc,
    status, attempts, tx_ref, created_at,
    transfer_mode, leg1_tx_ref, leg2_tx_ref, legs_completed
)
SELECT
    transfer_id, settle_date, from_book, to_book, amount_usdc,
    pre_from_usdc, pre_to_usdc, post_from_usdc, post_to_usdc,
    drift_bps_before, drift_bps_after, blocked_remainder_usdc,
    status, attempts, tx_ref, created_at,
    COALESCE(transfer_mode, 'VIRTUAL'), leg1_tx_ref, leg2_tx_ref, COALESCE(legs_completed, 0)
FROM book_transfers;

DROP TABLE book_transfers;
ALTER TABLE book_transfers_v9 RENAME TO book_transfers;

CREATE VIEW IF NOT EXISTS v_book_transfers_recent AS
SELECT *
FROM book_transfers
ORDER BY transfer_id DESC
LIMIT 30;

-- 2. Reclassify historical dry-run `PARTIAL_IDLE_CAPPED` rows (R4-H2)
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
          AND COALESCE((SELECT config_value FROM system_config WHERE config_key = 'pit_subaccount_address'), '') = ''
          AND COALESCE((SELECT config_value FROM system_config WHERE config_key = 'pond_subaccount_address'), '') = ''
          AND UPPER(COALESCE((SELECT config_value FROM system_config WHERE config_key = 'execution_mode'), 'PAPER')) IN ('LIVE', 'TESTNET')
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

-- 3. Add `last_funding_hour_bucket` to `trades` for idempotent PIT hourly funding accrual (R4-N4)
ALTER TABLE trades ADD COLUMN last_funding_hour_bucket INTEGER NOT NULL DEFAULT 0;

-- 4. Add dedicated `resting_order_id` column to `pond_cells` (R4-N11)
DROP VIEW IF EXISTS v_pond_cells_open;
ALTER TABLE pond_cells ADD COLUMN resting_order_id TEXT;

UPDATE pond_cells
SET resting_order_id = SUBSTR(exit_reason, 13),
    exit_reason = NULL
WHERE exit_reason LIKE 'RESTING_OID:%';

CREATE VIEW IF NOT EXISTS v_pond_cells_open AS
SELECT *
FROM pond_cells
WHERE state IN ('OPEN', 'TRAILED')
ORDER BY opened_at DESC;

-- 5. Seed DB-backed configuration keys (R4-M1, R4-N1, R4-N6, R4-QB)
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
    ('pit_tick_interval_seconds',            '900',     'int',    'orchestrator', 'Async daemon Book A (Pit) evaluation cadence in seconds (R4-M1)'),
    ('pond_tick_interval_seconds',           '15',      'int',    'pond',         'Async daemon Book B (Pond) fast intrabar stop/scan tick cadence in seconds (R4-M1)'),
    ('btc_fallback_base_price_usdc',         '95000.0', 'float',  'execution',    'Configured fallback BTC/USDC base price when no prior BTC market bars exist in PAPER/TESTNET mode (R4-N1)'),
    ('pond_volume_min_bars_per_day',         '12',      'int',    'pond',         'Minimum 1H bars required within a UTC calendar day bucket for daily volume aggregation (R4-N6)'),
    ('stranded_transfer_alert_cooldown_ms',  '300000',  'int',    'settling',     'Minimum cooldown in milliseconds between repeated alerts for stranded PARTIAL/AMBIGUOUS book transfers (R4-QB)'),
    ('pending_transfer_stale_ttl_ms',        '120000',  'int',    'settling',     'Age threshold in milliseconds after which an in-flight PENDING book transfer is flagged as stale (R4-QB)');

INSERT OR REPLACE INTO system_config (config_key, config_value, value_type, category, description)
VALUES ('schema_version', '9', 'int', 'system', 'Database schema version');

INSERT OR IGNORE INTO schema_migrations (version, description)
VALUES (9, '009_v030_round4_remediation: PARTIAL/AMBIGUOUS book_transfers statuses, trades.last_funding_hour_bucket, pond_cells.resting_order_id, and Round 4 DB config keys');
