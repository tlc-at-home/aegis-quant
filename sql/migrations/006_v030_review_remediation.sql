-- =============================================================================
-- AEGIS QUANT v0.3.0-rc1 — MIGRATION 006: REVIEW REMEDIATION & ZERO HARDCODED VALUES
-- Centralizes all previously hardcoded hyperparameters, URLs, timeouts, retention
-- limits, and security settings into `system_config`, and scrubs historical logs.
-- =============================================================================

-- 1. Bump schema version to 6
INSERT OR REPLACE INTO system_config (config_key, config_value, value_type, category, description)
VALUES ('schema_version', '6', 'int', 'system', 'Database schema migration version (v0.3.0-rc1 review remediation)');

-- 2. System, Runtime, SQLite & Retention Parameters
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('engine_runtime_label',             'RUST_NATIVE',                         'string', 'system', 'Canonical runtime engine label'),
('engine_runtime_detail',            'RUST_NATIVE (Axum / Tokio)',          'string', 'system', 'Detailed runtime engine descriptor'),
('http_timeout_secs',                '8',                                   'int',    'system', 'Default HTTP client request timeout in seconds'),
('http_connect_timeout_secs',        '4',                                   'int',    'system', 'Default HTTP client TCP/TLS connect timeout in seconds'),
('sqlite_busy_timeout_ms',           '15000',                               'int',    'system', 'SQLite WAL busy timeout in milliseconds'),
('sqlite_synchronous_mode',          'NORMAL',                              'string', 'system', 'SQLite WAL synchronous pragma mode'),
('sqlite_cache_size_kib',            '-32000',                              'int',    'system', 'SQLite page cache size in KiB (-32000 = 32 MiB)'),
('sqlite_mmap_size_bytes',           '268435456',                           'int',    'system', 'SQLite memory-mapped I/O limit in bytes (256 MiB)'),
('telemetry_retention_days',         '30',                                  'int',    'system', 'Maximum retention age in days for telemetry_logs rows'),
('telemetry_max_rows',               '10000',                               'int',    'system', 'Maximum row cap for telemetry_logs table before pruning'),
('pond_ohlcv_retention_days',        '30',                                  'int',    'system', 'Retention window in days for historical Pond OHLCV & funding cache'),
('retention_prune_interval_cycles',  '60',                                  'int',    'system', 'Daemon cycle interval between automated DB retention pruning runs'),
('reconcile_interval_cycles',        '20',                                  'int',    'system', 'Daemon cycle interval for periodic exchange state reconciliation'),
('default_ingest_lookback_bars',     '140',                                 'int',    'system', 'Default OHLCV bar lookback count per ingestion cycle'),
('calibration_ingest_lookback_bars', '220',                                 'int',    'system', 'OHLCV bar lookback count before walk-forward calibration');

-- 3. Web API Security, Authentication, CORS & SSE Limits (B1, B2, B3, S3, R1)
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('web_bind_host',                    '127.0.0.1',                           'string', 'security', 'Default WebUI & REST API bind address (loopback by default)'),
('web_bind_port',                    '8000',                                'int',    'security', 'Default WebUI & REST API TCP port'),
('web_allow_remote_bind',            'false',                               'bool',   'security', 'Explicit opt-in required to bind WebUI to non-loopback interfaces'),
('web_auth_enabled',                 'true',                                'bool',   'security', 'Require Bearer token or session authentication on /api/v1/* endpoints'),
('web_api_token',                    '',                                    'string', 'credentials', 'Optional static Web API Bearer token (Fernet-encrypted at rest when set)'),
('web_cors_allowed_origins',         'http://127.0.0.1:8000,http://localhost:8000', 'string', 'security', 'Comma-separated allowlist of origins permitted by CORS'),
('web_sse_interval_secs',            '3',                                   'int',    'system', 'Server-Sent Events (/api/v1/stream) push interval in seconds'),
('web_max_sse_clients',              '32',                                  'int',    'system', 'Maximum concurrent SSE stream connections permitted'),
('web_csp_policy',                   'default-src ''self''; script-src ''self'' ''unsafe-inline''; style-src ''self'' ''unsafe-inline''; img-src ''self'' data:; connect-src ''self''; frame-ancestors ''none''; base-uri ''self''', 'string', 'security', 'Content-Security-Policy header enforced on WebUI responses');

-- Ensure enforce_master_key is enabled by default (S1)
INSERT OR REPLACE INTO system_config (config_key, config_value, value_type, category, description)
VALUES ('enforce_master_key', 'true', 'bool', 'security', 'Require external or keyfile Fernet master key (immutable via Web API)');

-- 4. Hyperliquid Endpoints, Execution & Paper Slippage Parameters
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('hyperliquid_mainnet_url',          'https://api.hyperliquid.xyz',         'string', 'execution', 'Hyperliquid L1 Mainnet REST API base URL'),
('hyperliquid_testnet_url',          'https://api.hyperliquid-testnet.xyz', 'string', 'execution', 'Hyperliquid L1 Testnet REST API base URL'),
('hyperliquid_default_vault_mainnet','0xdfc24b077bc1425ad1dea75bcb6f8158e10df303', 'string', 'execution', 'Default HLP Mainnet Vault address'),
('hyperliquid_default_vault_testnet','0xa15099a30bbf2e68942d6f4c43d70d04faeab0a0', 'string', 'execution', 'Default HLP Testnet Vault address'),
('pit_subaccount_address',           '',                                    'string', 'execution', 'Optional dedicated Hyperliquid sub-account EVM address for Book A (PIT)'),
('pond_subaccount_address',          '',                                    'string', 'execution', 'Optional dedicated Hyperliquid sub-account EVM address for Book B (POND)'),
('maker_fill_poll_interval_ms',      '250',                                 'int',    'execution', 'Polling sleep interval in ms between live Maker Limit Chaser reprices'),
('maker_reprice_step_ticks',         '1',                                   'int',    'execution', 'Tick step per reprice iteration in Maker Limit Chaser'),
('maker_partial_fill_ratio',         '0.40',                                'float',  'execution', 'Initial maker partial fill tranche ratio in paper simulation'),
('paper_slippage_impact_coeff',      '0.10',                                'float',  'execution', 'Square-root market impact coefficient for paper dynamic slippage'),
('paper_slippage_vol_ref_usdc',      '250000.0',                            'float',  'execution', 'Fallback 4H bar dollar volume reference (USDC) for paper slippage'),
('paper_slippage_min_pct',           '0.0001',                              'float',  'execution', 'Minimum paper dynamic slippage floor (1 bps)'),
('paper_slippage_max_pct',           '0.015',                               'float',  'execution', 'Maximum paper dynamic slippage ceiling (150 bps)'),
('orphan_reconcile_stop_pct',        '0.09',                                'float',  'execution', 'Emergency stop distance fraction for reconciled on-chain orphan positions');

-- 5. Book A (PIT) Signal Hyperparameters: HMM, SNR, MACE, Relative Strength & Macro (C4, B6, B7, C6)
INSERT OR REPLACE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('hmm_lookback_candles', '540', 'int', 'hmm', 'Rolling 4H candle lookback window for 3-State Gaussian HMM (90 days)'),
('hmm_em_iterations',    '18',  'int', 'hmm', 'Maximum Baum-Welch EM iterations for 3-State Gaussian HMM');

INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('hmm_lookback_bars',                '540',                                 'int',    'hmm',    'Rolling 4H bar lookback window for 3-State Gaussian HMM (90 days)'),
('hmm_min_bars',                     '15',                                  'int',    'hmm',    'Minimum OHLCV bars required before fitting 3-State Gaussian HMM'),
('hmm_min_fit_obs_bars',             '14',                                  'int',    'hmm',    'Minimum observation index before initial HMM EM fit'),
('hmm_refit_step_bars',              '12',                                  'int',    'hmm',    'Causal walk-forward HMM EM refit interval in bars during historical decoding'),
('hmm_em_tolerance',                 '0.0001',                              'float',  'hmm',    'Log-likelihood convergence tolerance for Baum-Welch EM'),
('hmm_rv_window_bars',               '6',                                   'int',    'hmm',    'Rolling realized volatility window in bars for HMM observation extraction'),
('hmm_ema_fast_span',                '36',                                  'int',    'hmm',    'Fast EMA span on BTC log-returns for HMM observation smoothing'),
('hmm_ema_slow_span',                '96',                                  'int',    'hmm',    'Slow EMA span on BTC log-returns for HMM observation smoothing'),
('hmm_ema_vol_span',                 '14',                                  'int',    'hmm',    'EMA span on BTC high-low range volatility for HMM observation smoothing'),
('hmm_ema_alt_span',                 '14',                                  'int',    'hmm',    'EMA span on cross-sectional alt_pc1_ret for HMM observation smoothing'),
('hmm_ret_fast_weight',              '0.65',                                'float',  'hmm',    'Weight of fast EMA log-return in HMM observation blend'),
('hmm_ret_slow_weight',              '0.35',                                'float',  'hmm',    'Weight of slow EMA log-return in HMM observation blend'),
('hmm_enter_gate_prob',              '0.55',                                'float',  'hmm',    'Hysteresis posterior probability gate to enter BULL or BEAR state'),
('hmm_exit_gate_prob',               '0.45',                                'float',  'hmm',    'Hysteresis posterior probability gate to exit BULL or BEAR state'),
('hmm_bull_prior_mean_ret',          '0.003',                               'float',  'hmm',    'Prior mean log-return for BULL state (State 0)'),
('hmm_bull_prior_mean_rv',           '0.014',                               'float',  'hmm',    'Prior mean realized volatility for BULL state (State 0)'),
('hmm_bull_prior_mean_trend',        '0.80',                                'float',  'hmm',    'Prior mean EMA trend z-score for BULL state (State 0)'),
('hmm_bear_prior_mean_ret',          '-0.003',                              'float',  'hmm',    'Prior mean log-return for BEAR state (State 1)'),
('hmm_bear_prior_mean_rv',           '0.022',                               'float',  'hmm',    'Prior mean realized volatility for BEAR state (State 1)'),
('hmm_bear_prior_mean_trend',        '-0.80',                               'float',  'hmm',    'Prior mean EMA trend z-score for BEAR state (State 1)'),
('hmm_chop_prior_mean_ret',          '0.0',                                 'float',  'hmm',    'Prior mean log-return for CHOP state (State 2)'),
('hmm_chop_prior_mean_rv',           '0.010',                               'float',  'hmm',    'Prior mean realized volatility for CHOP state (State 2)'),
('hmm_chop_prior_mean_trend',        '0.0',                                 'float',  'hmm',    'Prior mean EMA trend z-score for CHOP state (State 2)'),
('hmm_self_transition_prob',         '0.90',                                'float',  'hmm',    'Initial diagonal self-transition probability in 3-state HMM'),
('hmm_off_transition_prob',          '0.05',                                'float',  'hmm',    'Initial off-diagonal transition probability in 3-state HMM'),
('hmm_var_floor',                    '0.0001',                              'float',  'hmm',    'Minimum diagonal variance floor in Gaussian HMM emission updates'),
('hmm_trend_gate_threshold',         '0.25',                                'float',  'hmm',    'Normalized EMA trend gate threshold for BULL/BEAR promotion'),
('hmm_chop_prob_gate',               '0.62',                                'float',  'hmm',    'Posterior CHOP probability ceiling above which CHOP regime is forced'),
('hmm_marginal_lead_min',            '0.08',                                'float',  'hmm',    'Minimum posterior lead over opposite directional state for marginal promotion'),
('hmm_walkforward_refit_bars',       '12',                                  'int',    'hmm',    'Causal walk-forward HMM EM refit interval in bars during historical decoding'),
('snr_ema_fast_period',              '16',                                  'int',    'mace',   'Fast EMA period for continuous Signal-to-Noise Ratio (SNR)'),
('snr_ema_slow_period',              '64',                                  'int',    'mace',   'Slow EMA period for continuous Signal-to-Noise Ratio (SNR)'),
('snr_synth_weight',                 '0.60',                                'float',  'mace',   'Weight of ALT/BTC:SYNTH SNR component in composite SNR'),
('snr_usdc_weight',                  '0.40',                                'float',  'mace',   'Weight of ALT/USDC:USDC SNR component in composite SNR'),
('snr_mad_multiplier',               '2.50',                                'float',  'mace',   'Mean absolute deviation denominator multiplier in continuous SNR'),
('snr_tanh_scale',                   '1.25',                                'float',  'mace',   'Tanh squashing scale factor for continuous SNR'),
('snr_ema_fast_span',                '16',                                  'int',    'mace',   'Fast EMA span alias for Signal-to-Noise Ratio (SNR)'),
('snr_ema_med_span',                 '21',                                  'int',    'mace',   'Medium EMA span for Signal-to-Noise Ratio (SNR)'),
('snr_ema_slow_span',                '64',                                  'int',    'mace',   'Slow EMA span alias for Signal-to-Noise Ratio (SNR)'),
('snr_atr_period',                   '14',                                  'int',    'mace',   'Wilder ATR period used to normalize SNR directional excursion'),
('snr_er_period',                    '14',                                  'int',    'mace',   'Kaufman Efficiency Ratio (ER) lookback period for SNR'),
('snr_short_lookback',               '6',                                   'int',    'mace',   'Short horizon lookback bars for SNR'),
('snr_med_lookback',                 '18',                                  'int',    'mace',   'Medium horizon lookback bars for SNR'),
('snr_long_lookback',                '36',                                  'int',    'mace',   'Long horizon lookback bars for SNR'),
('snr_w_short',                      '0.50',                                'float',  'mace',   'Weight of short-horizon component in SNR blend'),
('snr_w_med',                        '0.30',                                'float',  'mace',   'Weight of medium-horizon component in SNR blend'),
('snr_w_long',                       '0.20',                                'float',  'mace',   'Weight of long-horizon component in SNR blend'),
('snr_er_floor',                     '0.40',                                'float',  'mace',   'Base floor multiplier for Kaufman Efficiency Ratio in SNR'),
('snr_er_weight',                    '0.60',                                'float',  'mace',   'Slope multiplier for Kaufman Efficiency Ratio in SNR'),
('mace_min_bars',                    '55',                                  'int',    'mace',   'Minimum OHLCV bars required before MACE ensemble evaluation'),
('mace_vol_ema_span',                '20.0',                                'float',  'mace',   'Volume EMA span in bars for MACE volume z-score feature'),
('mace_z_window_bars',               '120',                                 'int',    'mace',   'Causal rolling z-score window in bars for MACE feature standardization'),
('mace_target_horizon_bars',         '4',                                   'int',    'mace',   'Forward k-bar target horizon for purged MACE regression'),
('mace_warmup_bars',                 '32',                                  'int',    'mace',   'Initial feature warmup bars excluded from MACE training matrix'),
('mace_min_train_rows',              '16',                                  'int',    'mace',   'Minimum purged training rows required to fit MACE ensemble'),
('mace_train_window_bars',           '220',                                 'int',    'mace',   'Rolling trailing training window in bars for purged MACE refits'),
('mace_refit_interval_bars',         '36',                                  'int',    'mace',   'Bar interval between purged MACE Ridge + Random Forest refits'),
('mace_rf_trees',                    '30',                                  'int',    'mace',   'Number of decision trees in MACE Random Forest regressor'),
('mace_rf_max_depth',                '4',                                   'int',    'mace',   'Maximum tree depth in MACE Random Forest regressor'),
('mace_rf_min_samples_leaf',         '10',                                  'int',    'mace',   'Minimum leaf sample count in MACE Random Forest regressor'),
('mace_ridge_alpha',                 '1.0',                                 'float',  'mace',   'L2 regularization penalty alpha for MACE Ridge regression'),
('mace_rng_seed',                    '42',                                  'int',    'mace',   'Deterministic RNG seed for MACE Random Forest bootstrap'),
('mace_ic_window_bars',              '36',                                  'int',    'mace',   'Trailing OOS prediction window in bars for Spearman Rank IC'),
('mace_dynamic_weight_min',          '0.15',                                'float',  'mace',   'Minimum adaptive MACE ensemble weight under Dynamic IC blending'),
('mace_dynamic_weight_max',          '0.60',                                'float',  'mace',   'Maximum adaptive MACE ensemble weight under Dynamic IC blending'),
('mace_ic_neg_slope',                '0.60',                                'float',  'mace',   'Adaptive MACE weight slope when rolling OOS Spearman IC <= 0'),
('mace_ic_pos_slope',                '0.85',                                'float',  'mace',   'Adaptive MACE weight slope when rolling OOS Spearman IC > 0'),
('rs_lookback_fast_bars',            '24',                                  'int',    'mace',   'Fast cross-sectional Relative Strength lookback in bars'),
('rs_lookback_mid_bars',             '72',                                  'int',    'mace',   'Medium cross-sectional Relative Strength lookback in bars'),
('rs_lookback_slow_bars',            '96',                                  'int',    'mace',   'Slow cross-sectional Relative Strength lookback in bars'),
('rs_usdc_momentum_bars',            '8',                                   'int',    'mace',   'Short-term USDC momentum lookback in bars for entry confirmation'),
('rs_composite_snr_weight',          '0.45',                                'float',  'mace',   'Weight of synthetic SNR in cross-sectional RS composite score'),
('rs_composite_fast_weight',         '0.35',                                'float',  'mace',   'Weight of 24-bar RS in cross-sectional RS composite score'),
('rs_composite_mid_weight',          '0.20',                                'float',  'mace',   'Weight of 72-bar RS in cross-sectional RS composite score'),
('rs_composite_fast_scale',          '6.0',                                 'float',  'mace',   'Tanh scale multiplier for 24-bar RS in composite score'),
('rs_composite_mid_scale',           '4.0',                                 'float',  'mace',   'Tanh scale multiplier for 72-bar RS in composite score'),
('btc_macro_ema_fast_span',          '18',                                  'int',    'hmm',    'BTC macro trend confirmation fast EMA span'),
('btc_macro_ema_slow_span',          '54',                                  'int',    'hmm',    'BTC macro trend confirmation slow EMA span'),
('btc_macro_ret_slow_bars',          '24',                                  'int',    'hmm',    'BTC macro confirmation slow return lookback in bars'),
('btc_macro_ret_fast_bars',          '8',                                   'int',    'hmm',    'BTC macro confirmation fast return lookback in bars'),
('action_top_k_leaders',             '3',                                   'int',    'mace',   'Number of top RS leader assets eligible for LONG entries in BULL regime'),
('action_bottom_k_laggards',         '3',                                   'int',    'mace',   'Number of bottom RS laggard assets eligible for SHORT entries in BEAR regime'),
('action_bull_min_prob',             '0.58',                                'float',  'hmm',    'Minimum HMM BULL posterior probability for LONG entry'),
('action_bull_min_btc_trend',        '-0.010',                              'float',  'hmm',    'Minimum BTC EMA trend spread for LONG macro confirmation'),
('action_bull_min_btc_r24',          '-0.030',                              'float',  'hmm',    'Minimum 24-bar BTC log-return for LONG macro confirmation'),
('action_bull_min_btc_r8',           '-0.020',                              'float',  'hmm',    'Minimum 8-bar BTC log-return for LONG macro confirmation'),
('action_bull_min_snr',              '0.25',                                'float',  'mace',   'Minimum composite SNR score required for LONG entry'),
('action_bull_min_snr_synth',        '0.30',                                'float',  'mace',   'Minimum ALT/BTC synthetic SNR score required for LONG entry'),
('action_bull_min_mace',             '0.05',                                'float',  'mace',   'Minimum MACE score floor permitted for LONG entry'),
('action_bull_min_rs96',             '0.02',                                'float',  'mace',   'Minimum 96-bar Relative Strength required for LONG entry'),
('action_bull_min_r_usdc_8',         '0.0',                                 'float',  'mace',   'Minimum 8-bar USDC momentum required for LONG entry'),
('action_bull_max_funding',          '0.0005',                              'float',  'basis',  'Maximum hourly funding rate permitted for LONG entry'),
('action_bear_min_prob',             '0.58',                                'float',  'hmm',    'Minimum HMM BEAR posterior probability for SHORT entry'),
('action_bear_max_btc_trend',        '0.006',                               'float',  'hmm',    'Maximum BTC EMA trend spread permitted for SHORT macro confirmation'),
('action_bear_max_btc_r24',          '0.015',                               'float',  'hmm',    'Maximum 24-bar BTC log-return permitted for SHORT macro confirmation'),
('action_bear_max_btc_r8',           '0.015',                               'float',  'hmm',    'Maximum 8-bar BTC log-return permitted for SHORT macro confirmation'),
('action_bear_max_snr',              '-0.25',                               'float',  'mace',   'Maximum composite SNR ceiling permitted for SHORT entry'),
('action_bear_max_snr_synth',        '-0.30',                               'float',  'mace',   'Maximum ALT/BTC synthetic SNR ceiling permitted for SHORT entry'),
('action_bear_max_mace',             '-0.05',                               'float',  'mace',   'Maximum MACE score ceiling permitted for SHORT entry'),
('action_bear_max_rs96',             '-0.03',                               'float',  'mace',   'Maximum 96-bar Relative Strength ceiling permitted for SHORT entry'),
('action_bear_max_r_usdc_8',         '-0.008',                              'float',  'mace',   'Maximum 8-bar USDC momentum ceiling permitted for SHORT entry'),
('action_bear_min_funding',          '-0.0004',                             'float',  'basis',  'Minimum hourly funding rate floor permitted for SHORT entry'),
('conviction_long_threshold',        '0.32',                                'float',  'mace',   'Minimum blended conviction threshold for LONG entry'),
('conviction_short_threshold',       '-0.32',                               'float',  'mace',   'Maximum blended conviction ceiling for SHORT entry'),
('basis_harvest_min_funding_hourly', '0.000015',                            'float',  'basis',  'Minimum hourly funding rate required for delta-neutral basis harvest'),
('chop_funding_harvest_threshold',   '0.000015',                            'float',  'basis',  'Hourly funding rate threshold to trigger BASIS_HARVEST in CHOP regime'),
('funding_carry_penalty_threshold',  '0.0003',                              'float',  'basis',  'Adverse hourly funding rate threshold for directional conviction penalty'),
('funding_carry_penalty_mult',       '0.85',                                'float',  'basis',  'Conviction multiplier applied when adverse funding exceeds threshold'),
('execution_regime_flip_bear_prob',  '0.62',                                'float',  'execution', 'Posterior BEAR probability threshold to exit LONG positions'),
('execution_regime_flip_bull_prob',  '0.60',                                'float',  'execution', 'Posterior BULL probability threshold to exit SHORT positions'),
('execution_high_chop_conviction_max','0.08',                               'float',  'execution', 'Maximum absolute blended conviction for HIGH_CHOP_DELEVERAGE exit'),
('execution_high_chop_prob_offset',  '0.20',                                'float',  'execution', 'Offset added to hmm_chop_prob_threshold for HIGH_CHOP_DELEVERAGE exit'),
('execution_high_chop_prob_cap',     '0.82',                                'float',  'execution', 'Maximum cap on CHOP probability gate for HIGH_CHOP_DELEVERAGE exit'),
('execution_rs_breakdown_long_snr',  '-0.10',                               'float',  'execution', 'Synthetic SNR breakdown threshold to exit open LONG positions'),
('execution_rs_reversal_short_snr',  '0.10',                                'float',  'execution', 'Synthetic SNR reversal threshold to exit open SHORT positions'),
('execution_stop_cooldown_bars',     '14',                                  'int',    'execution', 'Cooldown bars after stop-out before re-entering the same asset');

-- 6. Risk, Stops & Backtest Gate Parameters
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('atr_period',                       '14',                                  'int',    'stops',  'Default Wilder ATR lookback period in bars'),
('atr_default_period',               '14',                                  'int',    'stops',  'Default Wilder ATR lookback period alias in bars'),
('risk_min_stop_pct_floor',          '0.002',                               'float',  'risk',   'Minimum stop distance floor as fraction of entry price (20 bps)'),
('risk_conviction_min_mult',         '0.50',                                'float',  'risk',   'Minimum conviction risk budget multiplier at entry threshold'),
('risk_conviction_max_mult',         '1.50',                                'float',  'risk',   'Maximum conviction risk budget multiplier at full conviction'),
('backtest_warmup_bars',             '110',                                 'int',    'system', 'Initial historical warmup bars before backtest trade execution starts'),
('backtest_bars_per_day',            '6',                                   'int',    'system', 'Number of 4H bars per 24-hour trading day'),
('backtest_bars_per_year',           '2190.0',                              'float',  'system', 'Number of 4H bars per 365-day year for Sharpe/Sortino annualization'),
('backtest_max_concurrent_positions','6',                                   'int',    'risk',   'Maximum simultaneous open positions in Book A backtest & live portfolio'),
('backtest_gate_min_sharpe',         '1.20',                                'float',  'system', 'Readiness Gate 5 minimum annualized Sharpe ratio requirement'),
('backtest_gate_max_mdd_pct',        '20.0',                                'float',  'system', 'Readiness Gate 5 maximum drawdown percentage ceiling'),
('backtest_gate_min_profit_factor',  '1.30',                                'float',  'system', 'Readiness Gate 5 minimum Profit Factor requirement');

-- 7. Book B (POND) Additional Parameters (All prefixed with `pond_` per Firewall Invariant F4)
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('ingest_lookback_bars',             '120',                                 'int',    'system', 'Default OHLCV bar lookback count for Book A (PIT) cycle ingestion'),
('pond_universe_refresh_interval_hours', '24',                              'int',    'pond',   'Interval in hours between automated 80-coin Pond universe refreshes (C7)'),
('pond_atr_period',                  '14',                                  'int',    'pond',   'Wilder ATR period for 1H and 4H Pond scanner & exit evaluations'),
('pond_wick_buffer_atr_mult',        '0.85',                                'float',  'pond',   'Tier 1 catastrophic wick buffer in 1H ATR multiples'),
('pond_chandelier_atr_mult',         '3.8',                                 'float',  'pond',   'Tier 2 Chandelier stop distance in 4H ATR multiples'),
('pond_initial_stop_atr_mult',       '2.5',                                 'float',  'pond',   'Initial volatility stop distance in 1H ATR multiples'),
('pond_blowoff_atr_mult',            '3.0',                                 'float',  'pond',   'Gate 5 1-hour bar range blow-off veto threshold in 1H ATR multiples'),
('pond_blowoff_3h_adverse_atr_mult', '2.2',                                 'float',  'pond',   'Gate 5 3-hour adverse parabolic move veto threshold in 1H ATR multiples'),
('pond_trend_veto_4h_atr_mult',      '2.5',                                 'float',  'pond',   'Gate 6 structural trend distance veto threshold in 4H ATR multiples'),
('pond_gate5_parabolic_3h_atr_mult', '2.2',                                 'float',  'pond',   'Gate 5 3-hour adverse parabolic move veto threshold in 1H ATR multiples'),
('pond_gate6_atr_4h_floor_mult',     '1.8',                                 'float',  'pond',   'Gate 6 minimum 4H ATR floor as a multiple of 1H ATR'),
('pond_gate6_structural_atr_mult',   '2.5',                                 'float',  'pond',   'Gate 6 structural trend distance veto threshold in 4H ATR multiples'),
('pond_min_stop_pct_floor',          '0.005',                               'float',  'pond',   'Minimum initial stop distance floor as fraction of reference price (50 bps)'),
('pond_conviction_z_span',           '2.0',                                 'float',  'pond',   'Z-score span above entry threshold for full 1.5x Pond cell conviction'),
('pond_conviction_min_mult',         '0.5',                                 'float',  'pond',   'Minimum Pond cell conviction multiplier'),
('pond_conviction_max_mult',         '1.5',                                 'float',  'pond',   'Maximum Pond cell conviction multiplier'),
('pond_maker_offset_bps',            '1.5',                                 'float',  'pond',   'Maker limit offset in bps for Pond entry & exit limit chasers');

-- 8. Alerting & Retry Worker Parameters (S8, Q7)
INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category, description) VALUES
('telegram_api_base_url',            'https://api.telegram.org',            'string', 'alerts', 'Telegram Bot API base endpoint URL'),
('alert_http_timeout_sec',           '5.0',                                 'float',  'alerts', 'HTTP timeout in seconds for Telegram/Discord webhook dispatch'),
('alert_http_timeout_secs',          '5',                                   'int',    'alerts', 'HTTP timeout in seconds for Telegram/Discord webhook dispatch'),
('alert_max_retries',                '4',                                   'int',    'alerts', 'Maximum delivery attempts in exponential-backoff alert retry queue'),
('alert_retry_base_backoff_sec',     '0.35',                                'float',  'alerts', 'Base backoff delay in seconds for alert retry worker'),
('alert_retry_base_backoff_ms',      '350',                                 'int',    'alerts', 'Base backoff delay in milliseconds for alert retry worker'),
('alert_max_queue_size',             '256',                                 'int',    'alerts', 'Bounded channel capacity for background alert dispatch worker'),
('alert_worker_queue_capacity',      '256',                                 'int',    'alerts', 'Bounded channel capacity alias for background alert dispatch worker');

-- 9. Redact any historical plaintext secret values in `telemetry_logs` (B3)
UPDATE telemetry_logs
SET message = CASE
    WHEN message LIKE '%Updated system_config[private_key]%' THEN 'Updated system_config[private_key] = [REDACTED]'
    WHEN message LIKE '%Updated system_config[gemini_api_key]%' THEN 'Updated system_config[gemini_api_key] = [REDACTED]'
    WHEN message LIKE '%Updated system_config[telegram_bot_token]%' THEN 'Updated system_config[telegram_bot_token] = [REDACTED]'
    WHEN message LIKE '%Updated system_config[discord_webhook_url]%' THEN 'Updated system_config[discord_webhook_url] = [REDACTED]'
    WHEN message LIKE '%Updated system_config[web_api_token]%' THEN 'Updated system_config[web_api_token] = [REDACTED]'
    ELSE message
END
WHERE message LIKE '%Updated system_config[private_key]%'
   OR message LIKE '%Updated system_config[gemini_api_key]%'
   OR message LIKE '%Updated system_config[telegram_bot_token]%'
   OR message LIKE '%Updated system_config[discord_webhook_url]%'
   OR message LIKE '%Updated system_config[web_api_token]%';
