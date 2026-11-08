-- =============================================================================
-- Migration 008: Aegis Quant v0.3.0-rc1 Round-3 Remediation
-- Schema Version: 8
-- =============================================================================
-- Remediates:
--   * R3-B1: Per-hour funding idempotency tracking on pond_cells (last_funding_hour_bucket)
--   * R3-B3: Two-leg subaccount transfer state tracking on book_transfers
--            (leg1_tx_ref, leg2_tx_ref, legs_completed)
--   * R3-B5: Retroactive telemetry_logs credential & URL scrubbing
--   * R3-H5: Explicit transfer_mode on book_transfers and dry-run status cleanup
--   * R3-N16 / DB Config: Externalized default_funding_rate_hourly and runtime parameters
-- =============================================================================

ALTER TABLE pond_cells ADD COLUMN last_funding_hour_bucket INTEGER NOT NULL DEFAULT 0;

ALTER TABLE book_transfers ADD COLUMN transfer_mode TEXT NOT NULL DEFAULT '';
ALTER TABLE book_transfers ADD COLUMN leg1_tx_ref TEXT;
ALTER TABLE book_transfers ADD COLUMN leg2_tx_ref TEXT;
ALTER TABLE book_transfers ADD COLUMN legs_completed INTEGER NOT NULL DEFAULT 0;

-- Clean up any rows explicitly tagged with DRY_RUN in tx_ref / transfer_mode (R3-H5).
-- Note (R4-H2): Pre-v8 dry-run rows whose virtual tx_ref was plain '0x...' are reclassified
-- via telemetry correlation in 009_v030_round4_remediation.sql (or excluded via equity_baseline_reset_at).
UPDATE book_transfers
SET status = 'DRY_RUN',
    transfer_mode = 'DRY_RUN'
WHERE status = 'PARTIAL_IDLE_CAPPED'
  AND (
      COALESCE(tx_ref, '') LIKE '%DRY_RUN%'
      OR LOWER(COALESCE(tx_ref, '')) LIKE 'dryrun-%'
      OR COALESCE(transfer_mode, '') LIKE 'DRY_RUN%'
  );

UPDATE book_transfers
SET transfer_mode = 'DRY_RUN'
WHERE status LIKE 'DRY_RUN%'
   OR COALESCE(tx_ref, '') LIKE '%DRY_RUN%';

UPDATE book_transfers
SET transfer_mode = 'LIVE'
WHERE transfer_mode = ''
  AND status NOT LIKE 'DRY_RUN%';

-- Scrub any historical telemetry rows containing spaced key/value secrets or embedded URL tokens (R3-B5)
UPDATE telemetry_logs
SET message = '[REDACTED_HISTORICAL_CREDENTIAL_LOG]',
    details_json = '{}'
WHERE LOWER(message) LIKE '%private_key%=%'
   OR LOWER(message) LIKE '%private_key%:%'
   OR LOWER(message) LIKE '%bot_token%=%'
   OR LOWER(message) LIKE '%bot_token%:%'
   OR LOWER(message) LIKE '%webhook_url%=%'
   OR LOWER(message) LIKE '%webhook_url%:%'
   OR LOWER(message) LIKE '%auth_token%=%'
   OR LOWER(message) LIKE '%auth_token%:%'
   OR LOWER(message) LIKE '%api.telegram.org/bot%'
   OR LOWER(message) LIKE '%discord.com/api/webhooks/%'
   OR LOWER(message) LIKE '%discordapp.com/api/webhooks/%'
   OR LOWER(COALESCE(details_json, '')) LIKE '%hyperliquid_private_key%'
   OR LOWER(COALESCE(details_json, '')) LIKE '%telegram_bot_token%'
   OR LOWER(COALESCE(details_json, '')) LIKE '%discord_webhook_url%'
   OR LOWER(COALESCE(details_json, '')) LIKE '%web_auth_token%'
   OR LOWER(COALESCE(details_json, '')) LIKE '%api.telegram.org/bot%'
   OR LOWER(COALESCE(details_json, '')) LIKE '%discord.com/api/webhooks/%'
   OR LOWER(COALESCE(details_json, '')) LIKE '%discordapp.com/api/webhooks/%';

-- Externalized runtime parameters in system_config (zero hardcoded values)
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
    ('hyperliquid_mainnet_url', 'https://api.hyperliquid.xyz', 'string', 'execution', 'Hyperliquid L1 Mainnet REST API base URL'),
    ('hyperliquid_testnet_url', 'https://api.hyperliquid-testnet.xyz', 'string', 'execution', 'Hyperliquid L1 Testnet REST API base URL'),
    ('default_funding_rate_hourly', '0.0', 'float', 'execution', 'Fallback hourly funding rate when live funding feed is unavailable (0.0 neutral)'),
    ('pond_volume_median_window_days', '7', 'int', 'pond', 'Rolling window in days for computing true 7-day median daily volume in Pond universe'),
    ('web_auth_cache_ttl_ms', '2000', 'int', 'security', 'TTL in milliseconds for caching web authentication config in AppState'),
    ('alerts_retry_queue_capacity', '64', 'int', 'alerts', 'Bounded retry queue capacity for failed webhook alert deliveries'),
    ('telemetry_queue_capacity', '2048', 'int', 'telemetry', 'Bounded channel capacity for background SQLite telemetry log writer');

UPDATE system_config
SET config_value = '3.5'
WHERE config_key = 'chandelier_atr_multiplier_long'
  AND config_value = '3.0';

INSERT OR REPLACE INTO system_config (config_key, config_value, value_type, category, description, updated_at)
VALUES ('schema_version', '8', 'int', 'platform', 'Current SQLite schema migration version', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
