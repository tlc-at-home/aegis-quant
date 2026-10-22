-- Migration 001: Expand system_config with unified signal, two-tier stop, and cooldown hyperparameters
UPDATE system_config
SET config_value = '3.8',
    updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
WHERE config_key = 'chandelier_atr_multiplier_short' AND config_value = '2.5';

INSERT OR IGNORE INTO system_config (config_key, config_value, value_type, category) VALUES
('conviction_entry_threshold', '0.02', 'FLOAT', 'SIGNAL'),
('mace_tanh_scale', '1.25', 'FLOAT', 'SIGNAL'),
('snr_synth_weight', '0.60', 'FLOAT', 'SIGNAL'),
('hmm_enter_prob_gate', '0.55', 'FLOAT', 'SIGNAL'),
('hmm_exit_prob_gate', '0.45', 'FLOAT', 'SIGNAL'),
('hmm_lookback_bars', '540', 'INT', 'SIGNAL'),
('mace_forward_horizon_bars', '4', 'INT', 'SIGNAL'),
('mace_min_ic_floor', '0.05', 'FLOAT', 'SIGNAL'),
('short_sizing_atr_multiplier', '2.5', 'FLOAT', 'stops'),
('chandelier_min_atr_pct', '0.022', 'FLOAT', 'stops'),
('chandelier_wick_overshoot_atr', '0.85', 'FLOAT', 'stops'),
('entry_exit_slippage_bps', '3.0', 'FLOAT', 'execution'),
('stop_slippage_bps', '8.0', 'FLOAT', 'execution'),
('wick_overshoot_slippage_fraction', '0.15', 'FLOAT', 'execution'),
('bull_prob_entry_gate', '0.58', 'FLOAT', 'SIGNAL'),
('bear_prob_entry_gate', '0.58', 'FLOAT', 'SIGNAL'),
('bull_conviction_gate', '0.32', 'FLOAT', 'SIGNAL'),
('bear_conviction_gate', '-0.32', 'FLOAT', 'SIGNAL'),
('bull_snr_synth_gate', '0.30', 'FLOAT', 'SIGNAL'),
('bear_snr_synth_gate', '-0.30', 'FLOAT', 'SIGNAL'),
('bull_mace_gate', '0.05', 'FLOAT', 'SIGNAL'),
('bear_mace_gate', '-0.05', 'FLOAT', 'SIGNAL'),
('bull_rs_96_gate', '0.02', 'FLOAT', 'SIGNAL'),
('bear_rs_96_gate', '-0.03', 'FLOAT', 'SIGNAL'),
('bear_r_usdc_8_gate', '-0.008', 'FLOAT', 'SIGNAL'),
('bull_btc_snr_gate', '0.12', 'FLOAT', 'SIGNAL'),
('bear_btc_snr_gate', '-0.45', 'FLOAT', 'SIGNAL'),
('bear_btc_snr_macro_gate', '-0.20', 'FLOAT', 'SIGNAL'),
('bear_btc_ret_30_gate', '-0.015', 'FLOAT', 'SIGNAL'),
('bear_btc_ret_60_gate', '-0.04', 'FLOAT', 'SIGNAL'),
('bear_btc_ret_180_gate', '0.002', 'FLOAT', 'SIGNAL'),
('post_stop_cooldown_bars', '14', 'INT', 'stops');

