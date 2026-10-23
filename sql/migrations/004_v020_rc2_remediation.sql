-- Migration 004: Code Audit v0.2.0-rc2 Remediation & Parity Parameters
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_utc INTEGER NOT NULL,
    description TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description) VALUES
(4, CAST(strftime('%s', 'now') AS INTEGER), 'Migration 004: MACE Model Lifecycle Parity, Fernet Key Hardening & Alert Retry Queue');

INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('mace_refit_interval_bars', '36', 'INT', 'MACE_HMM', 'Rolling 4H bar interval between MACE RF+Ridge model refits (36 bars = 6 days)'),
('enforce_master_key', 'true', 'BOOL', 'EXCHANGE', 'Enforce explicit AEGIS_MASTER_KEY in LIVE/TESTNET modes'),
('alert_max_retries', '5', 'INT', 'OPS', 'Maximum retry attempts for failed Telegram/Discord webhook alerts'),
('alert_backoff_base_sec', '2.0', 'FLOAT', 'OPS', 'Base exponential backoff delay in seconds for alert retries'),
('enable_hlp_floor_backtest', 'false', 'BOOL', 'MACE_HMM', 'Enable artificial basis yield floor in backtest (disabled for realistic PnL)');
