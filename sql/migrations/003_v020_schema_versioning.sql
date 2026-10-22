-- Migration 003: Schema Versioning & Migrations Ledger + Telegram Alerting Config
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at_utc INTEGER NOT NULL,
    description TEXT NOT NULL
);

INSERT OR IGNORE INTO schema_migrations (version, applied_at_utc, description) VALUES
(1, strftime('%s', 'now'), 'Migration 001: Signal Config Expansion'),
(2, strftime('%s', 'now'), 'Migration 002: Order FSM and Reconciliation Columns'),
(3, strftime('%s', 'now'), 'Migration 003: Schema Versioning Ledger');

DELETE FROM system_config WHERE config_key = 'discord_webhook_url';

INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('telegram_bot_token', '', 'string', 'alerts', 'Telegram Bot API token for real-time operational notifications (Fernet AES-128 encrypted)'),
('telegram_chat_id', '', 'string', 'alerts', 'Telegram target chat ID for real-time operational notifications'),
('max_daily_drawdown_alert_pct', '0.03', 'float', 'alerts', 'Daily portfolio drawdown threshold (3.0%) triggering critical Telegram alert');
