-- =============================================================================
-- Aegis Quant v2.1 — Canonical MariaDB 13.0 Rolling / 12.3 LTS Relational Schema
-- 11 Normalized InnoDB Tables (DECIMAL Money/Price/Qty, CHECK Constraints, FKs)
-- + 7 First-Class SQL Views (v_assets_held, v_assets_ordered, v_assets_in_cooldown,
--   v_assets_sleeve_s1, v_assets_sleeve_s2, v_assets_sleeve_s3, v_priority_watchlist)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS aegis_quant
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE aegis_quant;

-- 1. System Config & Fernet-Encrypted Vault
CREATE TABLE IF NOT EXISTS system_config (
    id            BIGINT PRIMARY KEY AUTO_INCREMENT,
    config_key    VARCHAR(128) NOT NULL,
    config_value  TEXT NOT NULL,
    value_type    VARCHAR(32) NOT NULL DEFAULT 'string',
    description   VARCHAR(512) NOT NULL DEFAULT '',
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    UNIQUE KEY uq_system_config_key (config_key)
) ENGINE=InnoDB;

-- 2. Dynamically Discovered Hyperliquid Perpetual Assets
CREATE TABLE IF NOT EXISTS exchange_assets (
    id                  BIGINT PRIMARY KEY AUTO_INCREMENT,
    symbol              VARCHAR(32) NOT NULL,  -- Canonical: 'BTC-USDC', 'SOL-USDC'
    coin                VARCHAR(24) NOT NULL,  -- Raw coin: 'BTC', 'SOL'
    asset_index         INT UNSIGNED NOT NULL DEFAULT 0,
    sz_decimals         INT UNSIGNED NOT NULL DEFAULT 2,
    tick_size           DECIMAL(20,8) NOT NULL DEFAULT 0.01000000,
    lot_step_size       DECIMAL(20,8) NOT NULL DEFAULT 0.01000000,
    max_leverage        DECIMAL(20,8) NOT NULL DEFAULT 20.00000000,
    mark_price          DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    mid_price           DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    oracle_price        DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    funding_rate_hourly DECIMAL(20,10) NOT NULL DEFAULT 0.0000000000,
    day_volume_usdc     DECIMAL(24,8) NOT NULL DEFAULT 0.00000000,
    open_interest_usdc  DECIMAL(24,8) NOT NULL DEFAULT 0.00000000,
    prev_day_price      DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    is_core_eligible    TINYINT(1) NOT NULL DEFAULT 0,
    is_event_eligible   TINYINT(1) NOT NULL DEFAULT 0,
    is_priority_watched TINYINT(1) NOT NULL DEFAULT 0,
    created_at_ms       BIGINT NOT NULL,
    updated_at_ms       BIGINT NOT NULL,
    CONSTRAINT chk_assets_positive CHECK (tick_size > 0 AND lot_step_size > 0 AND max_leverage > 0),
    CONSTRAINT chk_assets_nonneg CHECK (
        mark_price >= 0 AND mid_price >= 0 AND oracle_price >= 0 AND prev_day_price >= 0
        AND day_volume_usdc >= 0 AND open_interest_usdc >= 0
    ),
    CONSTRAINT chk_assets_flags CHECK (
        is_core_eligible IN (0, 1) AND is_event_eligible IN (0, 1) AND is_priority_watched IN (0, 1)
    ),
    UNIQUE KEY uq_exchange_assets_symbol (symbol),
    UNIQUE KEY uq_exchange_assets_coin (coin),
    KEY idx_assets_eligibility_vol (is_core_eligible, is_event_eligible, day_volume_usdc DESC),
    KEY idx_assets_priority (is_priority_watched, updated_at_ms DESC)
) ENGINE=InnoDB;

-- 3. Asset Cooldowns (Post-Stop / Post-Liquidation / Churn Guard)
CREATE TABLE IF NOT EXISTS asset_cooldowns (
    id              BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_id        BIGINT NOT NULL,
    cooldown_type   VARCHAR(32) NOT NULL, -- 'STOP_OUT', 'VOL_SHOCK', 'CHURN_GUARD', 'MANUAL'
    reason          VARCHAR(255) NOT NULL,
    triggered_at_ms BIGINT NOT NULL,
    expires_at_ms   BIGINT NOT NULL,
    is_active       TINYINT(1) NOT NULL DEFAULT 1,
    created_at_ms   BIGINT NOT NULL,
    updated_at_ms   BIGINT NOT NULL,
    CONSTRAINT fk_cooldowns_asset FOREIGN KEY (asset_id) REFERENCES exchange_assets(id) ON DELETE CASCADE,
    CONSTRAINT chk_cooldowns_active CHECK (is_active IN (0, 1)),
    CONSTRAINT chk_cooldowns_window CHECK (expires_at_ms >= triggered_at_ms),
    KEY idx_cooldowns_active_exp (is_active, expires_at_ms, asset_id),
    KEY idx_cooldowns_asset_active_exp (asset_id, is_active, expires_at_ms)
) ENGINE=InnoDB;

-- 4. Historical & Live OHLCV Candle Series (10 supported Hyperliquid timeframes)
CREATE TABLE IF NOT EXISTS ohlcv_bars (
    id            BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_id      BIGINT NOT NULL,
    timeframe     VARCHAR(8) NOT NULL,
    timestamp_ms  BIGINT NOT NULL,
    open          DECIMAL(20,8) NOT NULL,
    high          DECIMAL(20,8) NOT NULL,
    low           DECIMAL(20,8) NOT NULL,
    close         DECIMAL(20,8) NOT NULL,
    volume        DECIMAL(24,8) NOT NULL,
    funding_rate  DECIMAL(20,10) NOT NULL DEFAULT 0.0000000000,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT fk_bars_asset FOREIGN KEY (asset_id) REFERENCES exchange_assets(id) ON DELETE CASCADE,
    CONSTRAINT chk_bars_ohlcv CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0 AND high >= low AND volume >= 0),
    CONSTRAINT chk_bars_timeframe CHECK (timeframe IN ('1m','5m','15m','30m','1h','2h','4h','8h','12h','1d')),
    UNIQUE KEY uq_bars_asset_tf_ts (asset_id, timeframe, timestamp_ms),
    KEY idx_bars_tf_ts (timeframe, timestamp_ms DESC)
) ENGINE=InnoDB;

-- 5. Latest Per-Asset Multi-Sleeve Signals & Target Weights
CREATE TABLE IF NOT EXISTS sleeve_signals (
    id                   BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_id             BIGINT NOT NULL,
    mark_price           DECIMAL(20,8) NOT NULL,
    day_volume_usdc      DECIMAL(24,8) NOT NULL,
    funding_rate_hourly  DECIMAL(20,10) NOT NULL,
    annualized_vol       DOUBLE NOT NULL,
    market_beta          DOUBLE NOT NULL,
    s1_mom_score         DOUBLE NOT NULL DEFAULT 0.0,
    s1_carry_score       DOUBLE NOT NULL DEFAULT 0.0,
    s2_trend_alignment   INT NOT NULL DEFAULT 0,
    s2_vol_surge_ratio   DOUBLE NOT NULL DEFAULT 1.0,
    s3_funding_zscore    DOUBLE NOT NULL DEFAULT 0.0,
    s3_flush_atr_mult    DOUBLE NOT NULL DEFAULT 0.0,
    w_s1_carry_mom       DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    w_s2_trend_breakout  DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    w_s3_event_reversion DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    raw_combined_weight  DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    target_weight        DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    current_weight       DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    delta_weight         DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    event_trigger        VARCHAR(64),
    rebalance_action     VARCHAR(32) NOT NULL DEFAULT 'HOLD',
    created_at_ms        BIGINT NOT NULL,
    updated_at_ms        BIGINT NOT NULL,
    CONSTRAINT fk_signals_asset FOREIGN KEY (asset_id) REFERENCES exchange_assets(id) ON DELETE CASCADE,
    CONSTRAINT chk_signals_nonneg CHECK (mark_price >= 0 AND day_volume_usdc >= 0),
    UNIQUE KEY uq_signals_asset (asset_id)
) ENGINE=InnoDB;

-- 6. Working Orders & Unified Positions (WORKING reserves margin; OPEN commits funds)
CREATE TABLE IF NOT EXISTS positions (
    id                        BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_id                  BIGINT NOT NULL,
    client_order_id           VARCHAR(64) NOT NULL,
    side                      ENUM('LONG', 'SHORT') NOT NULL,
    status                    ENUM('WORKING', 'OPEN', 'CLOSED', 'CANCELLED') NOT NULL,
    fsm_state                 VARCHAR(32) NOT NULL DEFAULT 'WORKING_ALO',
    entry_price               DECIMAL(20,8) NOT NULL,
    current_price             DECIMAL(20,8) NOT NULL,
    exit_price                DECIMAL(20,8),
    requested_quantity        DECIMAL(20,8) NOT NULL,
    filled_quantity           DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    reserved_margin_usdc      DECIMAL(20,8) NOT NULL DEFAULT 0.00000000, -- Reserved while WORKING
    committed_notional_usdc   DECIMAL(20,8) NOT NULL DEFAULT 0.00000000, -- Committed once OPEN
    portfolio_weight          DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    s1_weight_contrib         DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    s2_weight_contrib         DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    s3_weight_contrib         DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    protective_stop_px        DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    entry_r0                  DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    protective_stop_oid       VARCHAR(64),
    take_profit_px            DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    take_profit_oid           VARCHAR(64),
    highest_price_seen        DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    lowest_price_seen         DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    chase_count               INT UNSIGNED NOT NULL DEFAULT 0,
    realized_pnl_usdc         DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    unrealized_pnl_usdc       DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    fees_paid_usdc            DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    funding_accrued_usdc      DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    slippage_paid_usdc        DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    exchange_order_id         VARCHAR(64),
    created_at_ms             BIGINT NOT NULL,
    opened_at_ms              BIGINT,
    closed_at_ms              BIGINT,
    updated_at_ms             BIGINT NOT NULL,
    exit_reason               VARCHAR(64),
    CONSTRAINT fk_positions_asset FOREIGN KEY (asset_id) REFERENCES exchange_assets(id),
    CONSTRAINT chk_positions_qty CHECK (requested_quantity >= 0 AND filled_quantity >= 0),
    CONSTRAINT chk_positions_prices CHECK (
        entry_price > 0 AND current_price >= 0
        AND (exit_price IS NULL OR exit_price > 0)
        AND protective_stop_px >= 0 AND take_profit_px >= 0
        AND highest_price_seen >= 0 AND lowest_price_seen >= 0
    ),
    CONSTRAINT chk_positions_margin CHECK (reserved_margin_usdc >= 0 AND committed_notional_usdc >= 0),
    CONSTRAINT chk_positions_entry_r0 CHECK (entry_r0 >= 0),
    CONSTRAINT chk_positions_fsm CHECK (
        fsm_state IN ('NEW','WORKING_ALO','CHASING_ALO','CROSSING_IOC','FILLED_OPEN','STOP_TRIGGERED','CLOSED','CANCELLED')
    ),
    UNIQUE KEY uq_positions_client_oid (client_order_id),
    KEY idx_positions_status_asset (status, asset_id),
    KEY idx_positions_status_closed (status, closed_at_ms DESC),
    KEY idx_positions_asset_status (asset_id, status),
    KEY idx_positions_opened (opened_at_ms DESC)
) ENGINE=InnoDB;

-- 7. Maker Limit Chaser 1s Order Monitoring & State Transition Audit
CREATE TABLE IF NOT EXISTS order_audit (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    position_id       BIGINT NULL,
    asset_id          BIGINT NOT NULL,
    client_order_id   VARCHAR(64) NOT NULL,
    side              ENUM('LONG', 'SHORT') NOT NULL,
    from_state        VARCHAR(32) NOT NULL,
    to_state          VARCHAR(32) NOT NULL,
    requested_qty     DECIMAL(20,8) NOT NULL,
    filled_qty        DECIMAL(20,8) NOT NULL,
    limit_price       DECIMAL(20,8) NOT NULL,
    avg_fill_price    DECIMAL(20,8) NOT NULL,
    reserved_usdc     DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    committed_usdc    DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    fees_usdc         DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    reason            VARCHAR(255) NOT NULL DEFAULT '',
    created_at_ms     BIGINT NOT NULL,
    updated_at_ms     BIGINT NOT NULL,
    CONSTRAINT fk_audit_position FOREIGN KEY (position_id) REFERENCES positions(id) ON DELETE SET NULL,
    CONSTRAINT fk_audit_asset FOREIGN KEY (asset_id) REFERENCES exchange_assets(id),
    CONSTRAINT chk_audit_nonneg CHECK (
        requested_qty >= 0 AND filled_qty >= 0 AND limit_price >= 0
        AND avg_fill_price >= 0 AND reserved_usdc >= 0 AND committed_usdc >= 0
    ),
    KEY idx_audit_position_created (position_id, created_at_ms DESC),
    KEY idx_audit_created_id (created_at_ms DESC, id DESC),
    KEY idx_audit_client_oid (client_order_id)
) ENGINE=InnoDB;

-- 8. Unified Portfolio Snapshots & Sleeve Attribution Curve
-- Deliberately omits total_equity_usdc >= 0 CHECK so negative insolvency can be logged (R14-M12).
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id                        BIGINT PRIMARY KEY AUTO_INCREMENT,
    total_equity_usdc         DECIMAL(20,8) NOT NULL,
    uncapped_equity_usdc      DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    available_cash_usdc       DECIMAL(20,8) NOT NULL,
    reserved_order_cash_usdc  DECIMAL(20,8) NOT NULL,
    committed_notional_usdc   DECIMAL(20,8) NOT NULL,
    net_notional_usdc         DECIMAL(20,8) NOT NULL,
    gross_leverage_x          DECIMAL(20,8) NOT NULL,
    net_leverage_x            DECIMAL(20,8) NOT NULL,
    market_beta_exposure      DECIMAL(20,8) NOT NULL,
    market_breadth_bull       DECIMAL(20,8) NOT NULL,
    market_regime_label       VARCHAR(32) NOT NULL,
    risk_tier                 VARCHAR(32) NOT NULL,
    leverage_multiplier       DECIMAL(20,8) NOT NULL,
    s1_gross_weight           DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    s2_gross_weight           DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    s3_gross_weight           DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    working_order_count       INT NOT NULL DEFAULT 0,
    open_long_count           INT NOT NULL DEFAULT 0,
    open_short_count          INT NOT NULL DEFAULT 0,
    eligible_coin_count       INT NOT NULL DEFAULT 0,
    created_at_ms             BIGINT NOT NULL,
    updated_at_ms             BIGINT NOT NULL,
    CONSTRAINT chk_snapshots_nonneg CHECK (
        available_cash_usdc >= 0 AND reserved_order_cash_usdc >= 0 AND committed_notional_usdc >= 0
        AND gross_leverage_x >= 0 AND s1_gross_weight >= 0 AND s2_gross_weight >= 0 AND s3_gross_weight >= 0
        AND working_order_count >= 0 AND open_long_count >= 0 AND open_short_count >= 0 AND eligible_coin_count >= 0
    ),
    CONSTRAINT chk_snapshots_lev_mult CHECK (leverage_multiplier > 0),
    CONSTRAINT chk_snapshots_breadth CHECK (market_breadth_bull BETWEEN 0 AND 1),
    KEY idx_snapshots_created_id (created_at_ms DESC, id DESC)
) ENGINE=InnoDB;

-- 9. Operational Telemetry Logs
CREATE TABLE IF NOT EXISTS telemetry_logs (
    id            BIGINT PRIMARY KEY AUTO_INCREMENT,
    level         VARCHAR(16) NOT NULL,
    component     VARCHAR(64) NOT NULL,
    message       TEXT NOT NULL,
    details_json  JSON,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    CONSTRAINT chk_telemetry_level CHECK (level IN ('TRACE','DEBUG','INFO','WARN','WARNING','ERROR','CRITICAL')),
    KEY idx_telemetry_level_created (level, created_at_ms DESC),
    KEY idx_telemetry_created_id (created_at_ms DESC, id DESC)
) ENGINE=InnoDB;

-- 10. Alert Queue (Telegram Primary, Config-Switchable to Discord)
CREATE TABLE IF NOT EXISTS alert_queue (
    id               BIGINT PRIMARY KEY AUTO_INCREMENT,
    provider         VARCHAR(24) NOT NULL DEFAULT 'telegram',
    event_type       VARCHAR(64) NOT NULL,
    severity         VARCHAR(16) NOT NULL,
    message          TEXT NOT NULL,
    payload_json     JSON,
    status           VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    attempts         INT NOT NULL DEFAULT 0,
    next_retry_ms    BIGINT NOT NULL DEFAULT 0,
    dispatched_at_ms BIGINT NULL,
    created_at_ms    BIGINT NOT NULL,
    updated_at_ms    BIGINT NOT NULL,
    CONSTRAINT chk_alerts_status CHECK (status IN ('PENDING','DISPATCHING','PARTIAL','DELIVERED','SKIPPED','FAILED')),
    CONSTRAINT chk_alerts_attempts CHECK (attempts >= 0),
    KEY idx_alerts_status_retry (status, next_retry_ms),
    KEY idx_alerts_event_created (event_type, created_at_ms DESC)
) ENGINE=InnoDB;

-- 11. Reconciled Hyperliquid Per-Event Funding Ledger (windowed open-position PnL attribution)
CREATE TABLE IF NOT EXISTS funding_events (
    id             BIGINT PRIMARY KEY AUTO_INCREMENT,
    asset_id       BIGINT NOT NULL,
    coin           VARCHAR(24) NOT NULL,
    event_time_ms  BIGINT NOT NULL,
    usdc           DECIMAL(20,8) NOT NULL,
    remaining_usdc DECIMAL(20,8) NOT NULL,
    position_szi   DECIMAL(20,8) NOT NULL DEFAULT 0.00000000,
    funding_rate   DECIMAL(20,10) NOT NULL DEFAULT 0.0000000000,
    dedup_key      VARCHAR(128) NOT NULL,
    created_at_ms  BIGINT NOT NULL,
    updated_at_ms  BIGINT NOT NULL,
    CONSTRAINT fk_funding_asset FOREIGN KEY (asset_id) REFERENCES exchange_assets(id) ON DELETE CASCADE,
    UNIQUE KEY uq_funding_dedup (dedup_key),
    KEY idx_funding_asset_time (asset_id, event_time_ms DESC),
    KEY idx_funding_time (event_time_ms DESC)
) ENGINE=InnoDB;

-- ============================================================================
-- FIRST-CLASS SQL VIEWS (Utilizing Foreign Keys & Covering Indexes)
-- ============================================================================

-- View 1: v_assets_held (All active OPEN holdings with live mark, stop distance & sleeve attribution)
CREATE OR REPLACE VIEW v_assets_held AS
SELECT
    p.id AS position_id,
    a.id AS asset_id,
    a.symbol,
    a.coin,
    p.client_order_id,
    p.side,
    p.filled_quantity AS quantity,
    p.committed_notional_usdc,
    p.portfolio_weight,
    p.entry_price,
    a.mark_price,
    p.protective_stop_px,
    p.entry_r0,
    p.protective_stop_oid,
    CASE
        WHEN p.protective_stop_px > 0 AND a.mark_price > 0 AND p.side = 'LONG'
        THEN ROUND(GREATEST(0.0, (a.mark_price - p.protective_stop_px) / a.mark_price * 100.0), 2)
        WHEN p.protective_stop_px > 0 AND a.mark_price > 0 AND p.side = 'SHORT'
        THEN ROUND(GREATEST(0.0, (p.protective_stop_px - a.mark_price) / a.mark_price * 100.0), 2)
        ELSE NULL
    END AS stop_distance_pct,
    p.take_profit_px,
    p.take_profit_oid,
    CASE
        WHEN p.take_profit_px > 0 AND a.mark_price > 0 AND p.side = 'LONG'
        THEN ROUND((p.take_profit_px - a.mark_price) / a.mark_price * 100.0, 2)
        WHEN p.take_profit_px > 0 AND a.mark_price > 0 AND p.side = 'SHORT'
        THEN ROUND((a.mark_price - p.take_profit_px) / a.mark_price * 100.0, 2)
        ELSE NULL
    END AS tp_distance_pct,
    p.highest_price_seen,
    p.lowest_price_seen,
    p.unrealized_pnl_usdc,
    p.realized_pnl_usdc,
    p.funding_accrued_usdc,
    p.fees_paid_usdc,
    p.s1_weight_contrib,
    p.s2_weight_contrib,
    p.s3_weight_contrib,
    COALESCE(s.target_weight, p.portfolio_weight) AS latest_target_weight,
    COALESCE(s.delta_weight, 0.00000000) AS latest_delta_weight,
    p.created_at_ms,
    p.opened_at_ms,
    p.updated_at_ms
FROM positions p
INNER JOIN exchange_assets a ON a.id = p.asset_id
LEFT JOIN sleeve_signals s ON s.asset_id = a.id
WHERE p.status = 'OPEN';

-- View 2: v_assets_ordered (All WORKING Maker Limit Chaser orders with reserved margin & book distance)
CREATE OR REPLACE VIEW v_assets_ordered AS
SELECT
    p.id AS position_id,
    a.id AS asset_id,
    a.symbol,
    a.coin,
    p.client_order_id,
    p.exchange_order_id,
    p.side,
    p.fsm_state,
    p.requested_quantity,
    p.filled_quantity,
    p.reserved_margin_usdc,
    p.entry_price AS limit_price,
    a.mid_price,
    a.mark_price,
    CASE
        WHEN a.mid_price > 0 AND p.side = 'SHORT'
        THEN ROUND((p.entry_price - a.mid_price) / a.mid_price * 10000.0, 2)
        WHEN a.mid_price > 0
        THEN ROUND((a.mid_price - p.entry_price) / a.mid_price * 10000.0, 2)
        ELSE 0.00
    END AS distance_from_mid_bps,
    p.chase_count,
    p.protective_stop_px,
    p.take_profit_px,
    p.s1_weight_contrib,
    p.s2_weight_contrib,
    p.s3_weight_contrib,
    p.created_at_ms,
    p.updated_at_ms
FROM positions p
INNER JOIN exchange_assets a ON a.id = p.asset_id
WHERE p.status = 'WORKING';

-- View 3: v_assets_in_cooldown (Assets currently blocked by active post-stop / shock / churn cooldown)
CREATE OR REPLACE VIEW v_assets_in_cooldown AS
SELECT
    c.id AS cooldown_id,
    a.id AS asset_id,
    a.symbol,
    a.coin,
    c.cooldown_type,
    c.reason,
    c.triggered_at_ms,
    c.expires_at_ms,
    CAST(GREATEST(0, ROUND((c.expires_at_ms - (UNIX_TIMESTAMP(NOW(3)) * 1000)) / 1000, 0)) AS SIGNED) AS remaining_seconds,
    c.updated_at_ms
FROM asset_cooldowns c
INNER JOIN exchange_assets a ON a.id = c.asset_id
WHERE c.is_active = 1
  AND c.expires_at_ms > (UNIX_TIMESTAMP(NOW(3)) * 1000);

-- View 4: v_assets_sleeve_s1 (Sleeve 1: Cross-Sectional Carry-Momentum & Market-Beta Hedge)
CREATE OR REPLACE VIEW v_assets_sleeve_s1 AS
SELECT
    a.id AS asset_id,
    a.symbol,
    a.coin,
    a.mark_price,
    a.day_volume_usdc,
    a.funding_rate_hourly,
    ROUND(a.funding_rate_hourly * 24.0 * 365.0 * 100.0, 2) AS funding_apr_pct,
    s.annualized_vol,
    s.market_beta,
    s.s1_mom_score,
    s.s1_carry_score,
    s.w_s1_carry_mom,
    s.target_weight,
    s.current_weight,
    s.updated_at_ms
FROM sleeve_signals s
INNER JOIN exchange_assets a ON a.id = s.asset_id
WHERE a.is_core_eligible = 1 OR ABS(s.w_s1_carry_mom) > 0.000001
ORDER BY ABS(s.w_s1_carry_mom) DESC, a.day_volume_usdc DESC;

-- View 5: v_assets_sleeve_s2 (Sleeve 2: Breadth-Confirmed Multi-Speed Trend & Live 3s Breakout)
CREATE OR REPLACE VIEW v_assets_sleeve_s2 AS
SELECT
    a.id AS asset_id,
    a.symbol,
    a.coin,
    a.mark_price,
    a.day_volume_usdc,
    s.annualized_vol,
    s.s2_trend_alignment,
    s.s2_vol_surge_ratio,
    s.w_s2_trend_breakout,
    s.event_trigger,
    s.target_weight,
    s.current_weight,
    s.updated_at_ms
FROM sleeve_signals s
INNER JOIN exchange_assets a ON a.id = s.asset_id
WHERE a.is_core_eligible = 1 OR ABS(s.w_s2_trend_breakout) > 0.000001
ORDER BY ABS(s.w_s2_trend_breakout) DESC, s.s2_vol_surge_ratio DESC;

-- View 6: v_assets_sleeve_s3 (Sleeve 3: Live Capitulation Flush & Funding Dislocation Harvester)
CREATE OR REPLACE VIEW v_assets_sleeve_s3 AS
SELECT
    a.id AS asset_id,
    a.symbol,
    a.coin,
    a.mark_price,
    a.day_volume_usdc,
    a.funding_rate_hourly,
    ROUND(a.funding_rate_hourly * 24.0 * 365.0 * 100.0, 2) AS funding_apr_pct,
    s.s3_funding_zscore,
    s.s3_flush_atr_mult,
    s.w_s3_event_reversion,
    s.event_trigger,
    s.target_weight,
    s.current_weight,
    s.updated_at_ms
FROM sleeve_signals s
INNER JOIN exchange_assets a ON a.id = s.asset_id
WHERE a.is_event_eligible = 1 OR ABS(s.w_s3_event_reversion) > 0.000001
ORDER BY ABS(s.w_s3_event_reversion) DESC, s.s3_funding_zscore ASC;

-- View 7: v_priority_watchlist (All assets under 1-Second High-Frequency Surveillance)
CREATE OR REPLACE VIEW v_priority_watchlist AS
SELECT
    a.id AS asset_id,
    a.symbol,
    a.coin,
    a.mark_price,
    a.mid_price,
    a.funding_rate_hourly,
    p.id AS position_id,
    p.status AS position_status,
    p.side AS position_side,
    p.fsm_state,
    p.entry_price,
    p.requested_quantity,
    p.filled_quantity,
    p.reserved_margin_usdc,
    p.committed_notional_usdc,
    p.protective_stop_px,
    p.protective_stop_oid,
    p.take_profit_px,
    p.take_profit_oid,
    p.unrealized_pnl_usdc,
    COALESCE(s.target_weight, 0.00000000) AS target_weight,
    s.event_trigger,
    a.updated_at_ms
FROM exchange_assets a
LEFT JOIN (
    SELECT
        id, asset_id, status, side, fsm_state, entry_price,
        requested_quantity, filled_quantity, reserved_margin_usdc,
        committed_notional_usdc, protective_stop_px, protective_stop_oid,
        take_profit_px, take_profit_oid, unrealized_pnl_usdc,
        ROW_NUMBER() OVER (
            PARTITION BY asset_id
            ORDER BY FIELD(status, 'OPEN', 'WORKING'), id DESC
        ) AS rn
    FROM positions
    WHERE status IN ('WORKING', 'OPEN')
) p ON p.asset_id = a.id AND p.rn = 1
LEFT JOIN sleeve_signals s ON s.asset_id = a.id
WHERE a.is_priority_watched = 1
   OR p.id IS NOT NULL
   OR s.event_trigger IS NOT NULL;
