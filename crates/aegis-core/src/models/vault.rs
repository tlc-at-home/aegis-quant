//! Hyperliquid vault follower DTOs and pure PnL/aggregation helpers (`aegis-core::models::vault`).

use crate::config::{is_unified_account_mode, ConfigMap};
use crate::time::round_dp;
use serde::{Deserialize, Serialize};

/// Individual Hyperliquid vault follower position details.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VaultPositionInfo {
    #[serde(default)]
    pub vault_address: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub leader: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub equity_usdc: f64,
    #[serde(default)]
    pub pnl_usdc: f64,
    #[serde(default)]
    pub all_time_pnl_usdc: f64,
    #[serde(default)]
    pub pnl_pct: f64,
    #[serde(default)]
    pub all_time_pnl_pct: f64,
    #[serde(default)]
    pub apr_pct: f64,
    #[serde(default)]
    pub days_following: u64,
    #[serde(default)]
    pub locked_until_ms: i64,
    #[serde(default)]
    pub is_locked: bool,
    #[serde(default)]
    pub is_closed: bool,
    #[serde(default)]
    pub tvl_usdc: f64,
    #[serde(default)]
    pub follower_count: usize,
    #[serde(default)]
    pub followers_capped: bool,
    #[serde(default)]
    pub leader_fraction_pct: f64,
    #[serde(default)]
    pub leader_commission_pct: f64,
    #[serde(default)]
    pub allow_deposits: bool,
    #[serde(default)]
    pub always_close_on_withdraw: bool,
    #[serde(default)]
    pub max_distributable_usdc: f64,
    #[serde(default)]
    pub max_withdrawable_usdc: f64,
    #[serde(default)]
    pub relationship_type: String,
    #[serde(default)]
    pub child_vault_count: usize,
    #[serde(default)]
    pub all_time_volume_usdc: f64,
}

/// Aggregated Hyperliquid vault portfolio summary (segregated from perp margin).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct VaultInfoPayload {
    #[serde(default)]
    pub configured_vault_address: String,
    #[serde(default)]
    pub total_vault_equity_usdc: f64,
    #[serde(default)]
    pub combined_portfolio_equity_usdc: f64,
    #[serde(default)]
    pub vault_count: usize,
    #[serde(default)]
    pub synced_at_ms: i64,
    #[serde(default)]
    pub vaults: Vec<VaultPositionInfo>,
}

/// Computes vault PnL percentage (`pnl_usdc / (equity_usdc - pnl_usdc) * 100.0`, rounded to 2dp)
/// when `(equity_usdc - pnl_usdc) > 1e-6`, else `0.0`.
pub fn compute_vault_pnl_pct(equity_usdc: f64, pnl_usdc: f64) -> f64 {
    if !equity_usdc.is_finite() || !pnl_usdc.is_finite() {
        return 0.0;
    }
    let basis = equity_usdc - pnl_usdc;
    if !basis.is_finite() || basis <= 1e-6 {
        return 0.0;
    }
    let pct = (pnl_usdc / basis) * 100.0;
    if pct.is_finite() {
        round_dp(pct, 2)
    } else {
        0.0
    }
}

/// Builds a [`VaultInfoPayload`] purely from cached `system_config` (`live_vault_info_json`)
/// without issuing any network or database calls.
pub fn build_vault_info_payload_from_config(
    cfg: &ConfigMap,
    perp_equity_usdc: f64,
    now_ms_val: i64,
) -> VaultInfoPayload {
    let configured_vault_address = cfg.get_str("hyperliquid_vault_address", "");
    let safe_perp_eq = if perp_equity_usdc.is_finite() {
        perp_equity_usdc
    } else {
        0.0
    };

    if !cfg.execution_mode().is_live_or_testnet() {
        return VaultInfoPayload {
            configured_vault_address,
            total_vault_equity_usdc: 0.0,
            combined_portfolio_equity_usdc: round_dp(safe_perp_eq, 2),
            vault_count: 0,
            synced_at_ms: 0,
            vaults: Vec::new(),
        };
    }

    let is_unified = is_unified_account_mode(&cfg.get_str("live_exchange_account_mode", "default"));
    let raw_spot = cfg.get_f64("live_exchange_spot_usdc", 0.0);
    let spot_usdc = if raw_spot.is_finite() {
        raw_spot.max(0.0)
    } else {
        0.0
    };

    let raw_json = cfg.get_str("live_vault_info_json", "");
    let parsed = (!raw_json.is_empty())
        .then(|| serde_json::from_str::<VaultInfoPayload>(&raw_json).ok())
        .flatten();

    let Some(mut info) = parsed else {
        return VaultInfoPayload {
            configured_vault_address,
            total_vault_equity_usdc: 0.0,
            combined_portfolio_equity_usdc: if is_unified {
                round_dp(safe_perp_eq, 2)
            } else {
                round_dp(safe_perp_eq + spot_usdc, 2)
            },
            vault_count: 0,
            synced_at_ms: 0,
            vaults: Vec::new(),
        };
    };

    let sanitize_f64 = |val: &mut f64| {
        if !val.is_finite() {
            *val = 0.0;
        }
    };

    let mut sum_vault_eq = 0.0_f64;
    for v in &mut info.vaults {
        sanitize_f64(&mut v.equity_usdc);
        sanitize_f64(&mut v.pnl_usdc);
        sanitize_f64(&mut v.all_time_pnl_usdc);
        sanitize_f64(&mut v.pnl_pct);
        sanitize_f64(&mut v.all_time_pnl_pct);
        sanitize_f64(&mut v.apr_pct);
        sanitize_f64(&mut v.tvl_usdc);
        sanitize_f64(&mut v.leader_fraction_pct);
        sanitize_f64(&mut v.leader_commission_pct);
        sanitize_f64(&mut v.max_distributable_usdc);
        sanitize_f64(&mut v.max_withdrawable_usdc);
        sanitize_f64(&mut v.all_time_volume_usdc);
        v.is_locked = v.locked_until_ms > now_ms_val;
        sum_vault_eq += v.equity_usdc;
    }

    let total_vault_equity_usdc = if info.vaults.is_empty() {
        0.0
    } else if info.total_vault_equity_usdc.is_finite() && info.total_vault_equity_usdc > 0.0 {
        round_dp(info.total_vault_equity_usdc, 2)
    } else {
        round_dp(sum_vault_eq, 2)
    };

    info.configured_vault_address = configured_vault_address;
    info.total_vault_equity_usdc = total_vault_equity_usdc;
    info.combined_portfolio_equity_usdc = if is_unified {
        round_dp(safe_perp_eq + total_vault_equity_usdc, 2)
    } else {
        round_dp(safe_perp_eq + spot_usdc + total_vault_equity_usdc, 2)
    };
    info.vault_count = info.vaults.len();
    info
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_vault_pnl_pct_formulas_and_edge_cases() {
        assert!((compute_vault_pnl_pct(202.400584, 0.92) - 0.46).abs() < 1e-6);
        assert!((compute_vault_pnl_pct(202.400584, 1.34) - 0.67).abs() < 1e-6);
        assert!((compute_vault_pnl_pct(95.0, -5.0) - (-5.0)).abs() < 1e-6);
        assert_eq!(compute_vault_pnl_pct(10.0, 10.0), 0.0);
        assert_eq!(compute_vault_pnl_pct(0.0, 0.0), 0.0);
        assert_eq!(compute_vault_pnl_pct(0.0, 5.0), 0.0);
        assert_eq!(compute_vault_pnl_pct(f64::NAN, 1.0), 0.0);
    }
}
