-- =============================================================================
-- AEGIS QUANT v0.3.0 — MIGRATION 012: ROUND-9 REMEDIATION
-- Schema Version: 12
-- =============================================================================
-- Remediates:
--   1. R9-N2: Seed configurable `testnet_faucet_offset_usdc` in `system_config`
--      idempotently (`ON CONFLICT(config_key) DO NOTHING`) so the Hyperliquid
--      Testnet Faucet Spot USDC offset is operator-configurable rather than
--      hardcoded in `fetch_hlp_vault_state` and `sync_wallet_equity`.
--   2. Bump `schema_version` to 12 and record migration 12 in `schema_migrations`.
-- =============================================================================

INSERT INTO system_config (config_key, config_value, value_type, category, description, updated_at) VALUES
    ('testnet_faucet_offset_usdc', '10000.0', 'float', 'risk', 'Unallocated Hyperliquid Testnet Faucet Spot USDC grant to exclude from Perps equity and risk-cap scaling', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
ON CONFLICT(config_key) DO NOTHING;

UPDATE system_config
SET config_value = '12',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'schema_version';

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description)
VALUES (12, CAST(strftime('%s', 'now') AS INTEGER), '012_v030_round9_remediation: Seed configurable testnet_faucet_offset_usdc and Round-9 reconciliation/migration hardening');
