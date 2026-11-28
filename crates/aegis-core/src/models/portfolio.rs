//! Portfolio PnL analytics, equity-basis calculators, drawdown breaker, and surveillance health DTOs (`aegis-core::models::portfolio`).

use crate::config::{is_unified_account_mode, ConfigMap};
use crate::models::domain::{ExchangeAsset, OrderSide, PortfolioSnapshot};
use crate::models::views::{HeldAssetView, OrderedAssetView};
use crate::time::round_dp;
use serde::{Deserialize, Serialize};

/// Performance & PnL metrics for a single time horizon (`daily`, `weekly`, `monthly`, `ytd`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnlHorizonMetrics {
    pub horizon_label: String,
    pub window_start_ms: i64,
    pub baseline_equity_usdc: f64,
    pub net_pnl_usdc: f64,
    pub return_pct: f64,
    pub realized_pnl_usdc: f64,
    pub unrealized_pnl_usdc: f64,
    #[serde(default)]
    pub m2m_carry_usdc: f64,
    pub fees_funding_usdc: f64,
    pub trade_count: usize,
    pub win_count: usize,
    #[serde(default)]
    pub is_cap_clamped: bool,
    #[serde(default)]
    pub pnl_source: String,
}

/// Multi-horizon PnL breakdown (`Daily 24h`, `Weekly 7d`, `Monthly 30d`, `YTD`) for the operator dashboard.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PnlSummary {
    pub daily: PnlHorizonMetrics,
    pub weekly: PnlHorizonMetrics,
    pub monthly: PnlHorizonMetrics,
    pub ytd: PnlHorizonMetrics,
}

/// Compact time-series window parsed from Hyperliquid `/info {"type":"portfolio"}` (`perpDay`, `perpWeek`, `perpMonth`, `perpAllTime`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct HlPortfolioWindowCache {
    #[serde(default)]
    pub pnl_history: Vec<(i64, f64)>,
    #[serde(default)]
    pub vlm: f64,
}

/// Cached Hyperliquid `portfolio` perp and unified-account PnL histories persisted by reconciler (`live_exchange_portfolio_pnl_json`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CachedHlPortfolioPnl {
    #[serde(default)]
    pub synced_at_ms: i64,
    #[serde(default)]
    pub perp_day: HlPortfolioWindowCache,
    #[serde(default)]
    pub perp_week: HlPortfolioWindowCache,
    #[serde(default)]
    pub perp_month: HlPortfolioWindowCache,
    #[serde(default)]
    pub perp_all_time: HlPortfolioWindowCache,
    #[serde(default)]
    pub account_day: HlPortfolioWindowCache,
    #[serde(default)]
    pub account_week: HlPortfolioWindowCache,
    #[serde(default)]
    pub account_month: HlPortfolioWindowCache,
    #[serde(default)]
    pub account_all_time: HlPortfolioWindowCache,
}

impl CachedHlPortfolioPnl {
    /// Returns `true` when `synced_at_ms` is valid and no older than `max_age_ms` at `now_ms_val`.
    pub fn is_fresh(&self, now_ms_val: i64, max_age_ms: i64) -> bool {
        self.synced_at_ms > 0
            && now_ms_val >= self.synced_at_ms
            && now_ms_val.saturating_sub(self.synced_at_ms) <= max_age_ms
    }

    /// Linearly interpolates a cumulative PnL series sorted ascending by timestamp `(ts_ms, pnl)`.
    pub fn interpolate_series_pnl(pts: &[(i64, f64)], target_ms: i64) -> Option<f64> {
        let first = *pts.first()?;
        let last = *pts.last()?;
        if target_ms <= first.0 {
            return first.1.is_finite().then_some(first.1);
        }
        if target_ms >= last.0 {
            return last.1.is_finite().then_some(last.1);
        }
        for pair in pts.windows(2) {
            let (t0, p0) = pair[0];
            let (t1, p1) = pair[1];
            if t0 <= target_ms && target_ms <= t1 {
                if !p0.is_finite() || !p1.is_finite() {
                    return None;
                }
                if t1 <= t0 {
                    return Some(p1);
                }
                let frac = (target_ms - t0) as f64 / (t1 - t0) as f64;
                return Some(p0 + (p1 - p0) * frac);
            }
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_series_horizon_pnl(
        synced_at_ms: i64,
        day: &HlPortfolioWindowCache,
        week: &HlPortfolioWindowCache,
        month: &HlPortfolioWindowCache,
        all_time: &HlPortfolioWindowCache,
        horizon: &str,
        raw_start_ms: i64,
        cap_updated_at_ms: i64,
    ) -> Option<(f64, i64, bool)> {
        let is_cap_clamped = cap_updated_at_ms > 0 && cap_updated_at_ms > raw_start_ms;
        if is_cap_clamped {
            if synced_at_ms < cap_updated_at_ms {
                return None;
            }
            let candidates = [day, week, month, all_time];
            let series = candidates
                .iter()
                .copied()
                .find(|w| {
                    w.pnl_history
                        .first()
                        .is_some_and(|&(first_ts, _)| first_ts <= cap_updated_at_ms)
                })
                .or_else(|| (!all_time.pnl_history.is_empty()).then_some(all_time))?;
            let (_, last_pnl) = *series.pnl_history.last()?;
            let cap_pnl = Self::interpolate_series_pnl(&series.pnl_history, cap_updated_at_ms)?;
            let net = last_pnl - cap_pnl;
            return net.is_finite().then_some((net, cap_updated_at_ms, true));
        }

        let series = match horizon {
            "daily" => day,
            "weekly" => week,
            "monthly" => month,
            "ytd" => all_time,
            _ => return None,
        };
        let &(first_ts, first_pnl) = series.pnl_history.first()?;
        let &(_, last_pnl) = series.pnl_history.last()?;
        if !first_pnl.is_finite() || !last_pnl.is_finite() {
            return None;
        }
        if first_ts < raw_start_ms {
            let start_pnl = Self::interpolate_series_pnl(&series.pnl_history, raw_start_ms)?;
            let net = last_pnl - start_pnl;
            return net.is_finite().then_some((net, raw_start_ms, false));
        }
        let net = last_pnl - first_pnl;
        net.is_finite().then_some((net, first_ts, false))
    }

    /// Resolves `(net_pnl_usdc, window_start_ms, is_cap_clamped)` for `"daily"`, `"weekly"`, `"monthly"`, or `"ytd"` from perp series.
    pub fn horizon_net_pnl(
        &self,
        horizon: &str,
        raw_start_ms: i64,
        cap_updated_at_ms: i64,
    ) -> Option<(f64, i64, bool)> {
        Self::resolve_series_horizon_pnl(
            self.synced_at_ms,
            &self.perp_day,
            &self.perp_week,
            &self.perp_month,
            &self.perp_all_time,
            horizon,
            raw_start_ms,
            cap_updated_at_ms,
        )
    }

    /// Resolves `(net_pnl_usdc, window_start_ms, is_cap_clamped)` choosing unified-account series (minus `vault_window_pnl_usdc`) when `prefer_unified_account` is set and populated, otherwise falling back to `perp_*` without vault subtraction.
    pub fn horizon_net_pnl_for_mode(
        &self,
        horizon: &str,
        raw_start_ms: i64,
        cap_updated_at_ms: i64,
        prefer_unified_account: bool,
        vault_window_pnl_usdc: f64,
    ) -> Option<(f64, i64, bool)> {
        if prefer_unified_account {
            if let Some((raw_net, win_ts, clamped)) = Self::resolve_series_horizon_pnl(
                self.synced_at_ms,
                &self.account_day,
                &self.account_week,
                &self.account_month,
                &self.account_all_time,
                horizon,
                raw_start_ms,
                cap_updated_at_ms,
            ) {
                let vault_adj = if vault_window_pnl_usdc.is_finite() {
                    vault_window_pnl_usdc
                } else {
                    0.0
                };
                let net = raw_net - vault_adj;
                if net.is_finite() {
                    return Some((net, win_ts, clamped));
                }
            }
        }
        self.horizon_net_pnl(horizon, raw_start_ms, cap_updated_at_ms)
    }
}

/// Explicit equity & capital-sizing basis summary for UI transparency and reconciler sync status.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EquityBasisSummary {
    #[serde(default)]
    pub basis_label: String,
    #[serde(default)]
    pub effective_equity_usdc: f64,
    #[serde(default)]
    pub perp_wallet_equity_usdc: f64,
    #[serde(default)]
    pub risk_capital_cap_usdc: f64,
    #[serde(default)]
    pub is_cap_binding: bool,
    #[serde(default)]
    pub spot_usdc: f64,
    #[serde(default)]
    pub total_wallet_equity_usdc: f64,
    #[serde(default)]
    pub margin_used_usdc: f64,
    #[serde(default)]
    pub withdrawable_usdc: f64,
    #[serde(default)]
    pub synced_at_ms: i64,
    #[serde(default)]
    pub is_stale: bool,
    #[serde(default)]
    pub account_mode: String,
    #[serde(default)]
    pub is_unified_account: bool,
    #[serde(default)]
    pub perp_margin_equity_usdc: f64,
}

/// 24-hour peak-to-trough portfolio drawdown & minimum-equity ruin circuit-breaker status.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DrawdownBreakerStatus {
    #[serde(default)]
    pub tripped: bool,
    #[serde(default)]
    pub drawdown_pct: f64,
    #[serde(default)]
    pub max_daily_drawdown_pct: f64,
    #[serde(default)]
    pub peak_24h_equity_usdc: f64,
    #[serde(default)]
    pub latest_equity_usdc: f64,
    #[serde(default)]
    pub window_start_ms: i64,
    #[serde(default)]
    pub basis: String,
    #[serde(default)]
    pub ruin_halt_tripped: bool,
    #[serde(default)]
    pub min_equity_halt_usdc: f64,
}

fn default_priority_health_status() -> String {
    "IDLE".to_string()
}

/// Persistent 1-second surveillance health snapshot written to read-only `system_config.priority_monitor_health_json` (`R14-H3`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PriorityMonitorHealth {
    #[serde(default)]
    pub last_tick_at_ms: i64,
    #[serde(default)]
    pub consecutive_error_ticks: u32,
    #[serde(default)]
    pub last_tick_error_count: usize,
    #[serde(default)]
    pub failed_symbols: Vec<String>,
    #[serde(default)]
    pub unfilled_stop_breach_symbols: Vec<String>,
    #[serde(default)]
    pub unmonitored_alert_active: bool,
    #[serde(default)]
    pub is_stale: bool,
    #[serde(default = "default_priority_health_status")]
    pub status: String,
}

impl Default for PriorityMonitorHealth {
    fn default() -> Self {
        Self {
            last_tick_at_ms: 0,
            consecutive_error_ticks: 0,
            last_tick_error_count: 0,
            failed_symbols: Vec::new(),
            unfilled_stop_breach_symbols: Vec::new(),
            unmonitored_alert_active: false,
            is_stale: false,
            status: default_priority_health_status(),
        }
    }
}

/// Resolves position entry notional from `entry_price * quantity`, falling back to
/// `committed_notional_usdc` only when `entry_price * quantity <= 1e-6` and `committed_notional_usdc > 1e-6`.
pub fn resolve_position_entry_notional_usdc(
    entry_price: f64,
    quantity: f64,
    committed_notional_usdc: f64,
) -> f64 {
    let entry_notional =
        if entry_price.is_finite() && quantity.is_finite() && entry_price > 0.0 && quantity > 0.0 {
            entry_price * quantity
        } else {
            0.0
        };
    if entry_notional.is_finite() && entry_notional > 1e-6 {
        entry_notional
    } else if committed_notional_usdc.is_finite() && committed_notional_usdc > 1e-6 {
        committed_notional_usdc
    } else {
        0.0
    }
}

/// Computes position PnL percentage (`pnl_usdc / entry_notional * 100.0`, rounded to 2dp).
/// Never produces `NaN` or `Infinity`.
pub fn compute_position_pnl_pct(
    pnl_usdc: f64,
    entry_price: f64,
    quantity: f64,
    committed_notional_usdc: f64,
) -> f64 {
    if !pnl_usdc.is_finite() {
        return 0.0;
    }
    let base_notional =
        resolve_position_entry_notional_usdc(entry_price, quantity, committed_notional_usdc);
    if !base_notional.is_finite() || base_notional <= 1e-6 {
        return 0.0;
    }
    let pct = (pnl_usdc / base_notional) * 100.0;
    if pct.is_finite() {
        round_dp(pct, 2)
    } else {
        0.0
    }
}

/// Partitions active `OPEN` holdings into `(post_cap_epoch_count, pre_cap_epoch_count)` (`R14-H1`).
/// Uses immutable `opened_at_ms.unwrap_or(created_at_ms)` so 1s mark updates never reclassify legacy rows.
pub fn partition_cap_epoch_holdings(held: &[HeldAssetView], cap_epoch_ms: i64) -> (usize, usize) {
    if cap_epoch_ms <= 0 {
        return (held.len(), 0);
    }
    let active = held
        .iter()
        .filter(|h| {
            let origin_ts = h.opened_at_ms.unwrap_or(h.created_at_ms);
            origin_ts > 0 && origin_ts >= cap_epoch_ms
        })
        .count();
    (active, held.len().saturating_sub(active))
}

/// Synthesizes a fallback [`PortfolioSnapshot`] when MariaDB `portfolio_snapshots` has no persisted row yet.
pub fn synthesize_fallback_snapshot(
    cfg: &ConfigMap,
    v_assets_held: &[HeldAssetView],
    v_assets_ordered: &[OrderedAssetView],
    exchange_assets: &[ExchangeAsset],
    ts_ms: i64,
) -> PortfolioSnapshot {
    let tier = cfg.risk_tier();
    let mult = cfg.target_leverage_multiplier();
    let is_live_or_testnet = cfg.execution_mode().is_live_or_testnet();
    let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
    let reserved: f64 = v_assets_ordered
        .iter()
        .map(|o| o.reserved_margin_usdc)
        .filter(|v| v.is_finite())
        .sum();
    let committed: f64 = v_assets_held
        .iter()
        .map(|h| h.committed_notional_usdc)
        .filter(|v| v.is_finite())
        .sum();
    let net_notional: f64 = v_assets_held
        .iter()
        .filter(|h| h.committed_notional_usdc.is_finite())
        .map(|h| {
            let sign = OrderSide::parse(&h.side).map(|s| s.sign()).unwrap_or(0.0);
            h.committed_notional_usdc * sign
        })
        .sum();
    let upnl: f64 = v_assets_held
        .iter()
        .map(|h| h.unrealized_pnl_usdc)
        .filter(|v| v.is_finite())
        .sum();

    let paper_eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
    let uncapped_eq = if is_live_or_testnet {
        if live_eq > 0.0 {
            round_dp(live_eq, 2)
        } else {
            let cap_eq = cfg.get_f64("risk_capital_cap_usdc", 0.0);
            if cap_eq >= 10.0 {
                round_dp(cap_eq, 2)
            } else {
                round_dp(paper_eq, 2)
            }
        }
    } else {
        let base_eq = if live_eq > 10.0 { live_eq } else { paper_eq };
        round_dp(base_eq + upnl, 2)
    };

    let total_eq = if is_live_or_testnet {
        let cap_eq = cfg.get_f64("risk_capital_cap_usdc", 0.0);
        let eff_live = if live_eq > 0.0 && cap_eq >= 10.0 {
            live_eq.min(cap_eq)
        } else if live_eq > 0.0 {
            live_eq
        } else if cap_eq >= 10.0 {
            cap_eq
        } else {
            paper_eq
        };
        round_dp(eff_live, 2)
    } else {
        uncapped_eq
    };
    let avail_cash = round_dp((total_eq - committed - reserved).max(0.0), 2);
    let gross_lev = if total_eq > 0.0 {
        round_dp(committed / total_eq, 3)
    } else {
        0.0
    };
    let net_lev = if total_eq > 0.0 {
        round_dp(net_notional / total_eq, 3)
    } else {
        0.0
    };

    PortfolioSnapshot {
        id: 0,
        total_equity_usdc: total_eq,
        uncapped_equity_usdc: uncapped_eq,
        available_cash_usdc: avail_cash,
        reserved_order_cash_usdc: round_dp(reserved, 2),
        committed_notional_usdc: round_dp(committed, 2),
        net_notional_usdc: round_dp(net_notional, 2),
        gross_leverage_x: gross_lev,
        net_leverage_x: net_lev,
        market_beta_exposure: 0.0,
        market_breadth_bull: 0.50,
        market_regime_label: "NEUTRAL_INITIALIZING".to_string(),
        risk_tier: tier.as_str().to_string(),
        leverage_multiplier: mult,
        s1_gross_weight: 0.0,
        s2_gross_weight: 0.0,
        s3_gross_weight: 0.0,
        working_order_count: v_assets_ordered.len() as i32,
        open_long_count: v_assets_held
            .iter()
            .filter(|h| h.side.eq_ignore_ascii_case("LONG"))
            .count() as i32,
        open_short_count: v_assets_held
            .iter()
            .filter(|h| h.side.eq_ignore_ascii_case("SHORT"))
            .count() as i32,
        eligible_coin_count: exchange_assets
            .iter()
            .filter(|a| a.is_core_eligible || a.is_event_eligible)
            .count() as i32,
        created_at_ms: ts_ms,
        updated_at_ms: ts_ms,
    }
}

/// Computes canonical [`EquityBasisSummary`] shared across execution and Web UI (`R14-M28`, `P1-Acct`).
pub fn compute_equity_basis_summary(
    cfg: &ConfigMap,
    snapshot: &PortfolioSnapshot,
    vault_equity_usdc: f64,
    now_ms_val: i64,
) -> EquityBasisSummary {
    let is_live_or_testnet = cfg.execution_mode().is_live_or_testnet();
    let live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
    let safe_snap_eq = if snapshot.total_equity_usdc.is_finite() {
        snapshot.total_equity_usdc
    } else {
        0.0
    };
    let safe_vault_eq = if vault_equity_usdc.is_finite() {
        vault_equity_usdc.max(0.0)
    } else {
        0.0
    };

    let perp_wallet_equity_usdc = if is_live_or_testnet && live_eq > 0.0 {
        round_dp(live_eq, 2)
    } else {
        round_dp(safe_snap_eq, 2)
    };
    let spot_usdc = if is_live_or_testnet {
        round_dp(cfg.get_f64("live_exchange_spot_usdc", 0.0).max(0.0), 2)
    } else {
        0.0
    };
    let account_mode = if is_live_or_testnet {
        let m = cfg.get_str("live_exchange_account_mode", "default");
        if m.trim().is_empty() {
            "default".to_string()
        } else {
            m
        }
    } else {
        "paper".to_string()
    };
    let is_unified_account = is_live_or_testnet && is_unified_account_mode(&account_mode);
    let perp_margin_equity_usdc = if is_unified_account {
        round_dp(
            cfg.get_f64("live_exchange_perp_margin_equity_usdc", 0.0)
                .max(0.0),
            2,
        )
    } else {
        perp_wallet_equity_usdc
    };
    let effective_equity_usdc = round_dp(safe_snap_eq, 2);
    let risk_capital_cap_usdc = if is_live_or_testnet {
        round_dp(cfg.get_f64("risk_capital_cap_usdc", 0.0).max(0.0), 2)
    } else {
        0.0
    };
    let is_cap_binding = is_live_or_testnet
        && risk_capital_cap_usdc >= 10.0
        && perp_wallet_equity_usdc > risk_capital_cap_usdc + 1e-6;
    let basis_label = if !is_live_or_testnet {
        "PAPER_EQUITY"
    } else if is_cap_binding {
        "CAPPED_RISK_CAPITAL"
    } else if is_unified_account {
        "UNIFIED_ACCOUNT_VALUE"
    } else {
        "PERP_ACCOUNT_VALUE"
    }
    .to_string();
    let total_wallet_equity_usdc = if is_unified_account {
        round_dp(perp_wallet_equity_usdc + safe_vault_eq, 2)
    } else {
        round_dp(perp_wallet_equity_usdc + spot_usdc + safe_vault_eq, 2)
    };
    let margin_used_usdc = if is_live_or_testnet {
        round_dp(
            cfg.get_f64("live_exchange_margin_used_usdc", 0.0).max(0.0),
            2,
        )
    } else {
        0.0
    };
    let withdrawable_usdc = if is_live_or_testnet {
        round_dp(
            cfg.get_f64("live_exchange_withdrawable_usdc", 0.0).max(0.0),
            2,
        )
    } else {
        round_dp(snapshot.available_cash_usdc.max(0.0), 2)
    };
    let synced_at_ms = cfg.get_i64("live_exchange_equity_synced_at_ms", 0).max(0);
    let is_standby = cfg.trading_run_mode().suspends_background_loops();
    let is_stale = is_live_or_testnet
        && !is_standby
        && (synced_at_ms <= 0 || now_ms_val.saturating_sub(synced_at_ms) > 90_000);

    EquityBasisSummary {
        basis_label,
        effective_equity_usdc,
        perp_wallet_equity_usdc,
        risk_capital_cap_usdc,
        is_cap_binding,
        spot_usdc,
        total_wallet_equity_usdc,
        margin_used_usdc,
        withdrawable_usdc,
        synced_at_ms,
        is_stale,
        account_mode,
        is_unified_account,
        perp_margin_equity_usdc,
    }
}

/// Computes [`DrawdownBreakerStatus`] from peak & latest equity over the 24h window (`P1-Acct`, `R14-M12`).
pub fn compute_drawdown_breaker_status(
    peak_and_latest: Option<(f64, f64)>,
    max_daily_drawdown_pct: f64,
    min_equity_halt_usdc: f64,
    basis: &str,
    window_start_ms: i64,
) -> DrawdownBreakerStatus {
    let max_dd_frac = if max_daily_drawdown_pct.is_finite() {
        max_daily_drawdown_pct.clamp(0.01, 0.95)
    } else {
        0.15
    };
    let min_halt = if min_equity_halt_usdc.is_finite() && min_equity_halt_usdc > 0.0 {
        round_dp(min_equity_halt_usdc, 2)
    } else {
        0.0
    };

    match peak_and_latest {
        Some((peak, latest)) if peak.is_finite() && peak > 0.0 && latest.is_finite() => {
            let dd_frac = if latest <= 0.0 {
                1.0
            } else {
                ((peak - latest) / peak).clamp(0.0, 1.0)
            };
            let ruin_halt_tripped = latest <= 0.0 || (min_halt > 0.0 && latest < min_halt);
            DrawdownBreakerStatus {
                tripped: dd_frac >= max_dd_frac,
                drawdown_pct: round_dp(dd_frac * 100.0, 2),
                max_daily_drawdown_pct: round_dp(max_dd_frac * 100.0, 2),
                peak_24h_equity_usdc: round_dp(peak, 2),
                latest_equity_usdc: round_dp(latest, 2),
                window_start_ms,
                basis: basis.to_string(),
                ruin_halt_tripped,
                min_equity_halt_usdc: min_halt,
            }
        }
        Some((_, latest)) if latest.is_finite() => {
            let ruin_halt_tripped = latest <= 0.0 || (min_halt > 0.0 && latest < min_halt);
            DrawdownBreakerStatus {
                tripped: false,
                drawdown_pct: 0.0,
                max_daily_drawdown_pct: round_dp(max_dd_frac * 100.0, 2),
                peak_24h_equity_usdc: 0.0,
                latest_equity_usdc: round_dp(latest, 2),
                window_start_ms,
                basis: basis.to_string(),
                ruin_halt_tripped,
                min_equity_halt_usdc: min_halt,
            }
        }
        _ => DrawdownBreakerStatus {
            tripped: false,
            drawdown_pct: 0.0,
            max_daily_drawdown_pct: round_dp(max_dd_frac * 100.0, 2),
            peak_24h_equity_usdc: 0.0,
            latest_equity_usdc: 0.0,
            window_start_ms,
            basis: basis.to_string(),
            ruin_halt_tripped: false,
            min_equity_halt_usdc: min_halt,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_held(created_at_ms: i64, opened_at_ms: Option<i64>) -> HeldAssetView {
        HeldAssetView {
            position_id: 1,
            asset_id: 1,
            symbol: "BTC-USDC".to_string(),
            coin: "BTC".to_string(),
            client_order_id: "c1".to_string(),
            side: "LONG".to_string(),
            quantity: 0.1,
            committed_notional_usdc: 900.0,
            portfolio_weight: 0.9,
            entry_price: 9000.0,
            mark_price: 9000.0,
            protective_stop_px: 8500.0,
            entry_r0: 500.0,
            protective_stop_oid: None,
            stop_distance_pct: None,
            take_profit_px: 9500.0,
            take_profit_oid: None,
            tp_distance_pct: None,
            highest_price_seen: 9000.0,
            lowest_price_seen: 9000.0,
            unrealized_pnl_usdc: 0.0,
            unrealized_pnl_pct: 0.0,
            realized_pnl_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            fees_paid_usdc: 0.0,
            s1_weight_contrib: 0.0,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            latest_target_weight: 0.0,
            latest_delta_weight: 0.0,
            created_at_ms,
            opened_at_ms,
            updated_at_ms: 9_999_999,
        }
    }

    #[test]
    fn test_compute_position_pnl_pct_open_closed_and_edge_cases() {
        let closed_pct = compute_position_pnl_pct(-382.37, 0.23227, 9634.0, 0.0);
        assert!((closed_pct - (-17.09)).abs() < 1e-6);

        let open_pct = compute_position_pnl_pct(12.34, 180.0, 2.0, 380.0);
        assert!((open_pct - 3.43).abs() < 1e-6);

        let fallback_pct = compute_position_pnl_pct(25.0, 0.0, 0.0, 500.0);
        assert!((fallback_pct - 5.0).abs() < 1e-6);

        assert_eq!(compute_position_pnl_pct(10.0, 0.0, 0.0, 0.0), 0.0);
        assert_eq!(compute_position_pnl_pct(f64::NAN, 100.0, 1.0, 100.0), 0.0);
        assert_eq!(
            compute_position_pnl_pct(f64::INFINITY, 100.0, 1.0, 100.0),
            0.0
        );
    }

    #[test]
    fn test_cached_hl_portfolio_pnl_interpolation_and_horizons() {
        let day_pts = vec![(1_000_000, 0.0), (1_086_400, 242.70)];
        let week_pts = vec![
            (500_000, 0.0),
            (700_000, -100.0),
            (800_000, -200.0),
            (1_086_400, 362.50),
        ];
        let all_pts = vec![
            (100_000, 0.0),
            (400_000, 50.0),
            (500_000, 100.0),
            (1_086_400, 462.50),
        ];
        let cache = CachedHlPortfolioPnl {
            synced_at_ms: 1_086_500,
            perp_day: HlPortfolioWindowCache {
                pnl_history: day_pts,
                vlm: 1412.91,
            },
            perp_week: HlPortfolioWindowCache {
                pnl_history: week_pts,
                vlm: 64604.1,
            },
            perp_month: HlPortfolioWindowCache::default(),
            perp_all_time: HlPortfolioWindowCache {
                pnl_history: all_pts,
                vlm: 83291.0,
            },
            ..Default::default()
        };

        assert!(cache.is_fresh(1_100_000, 600_000));
        assert!(!cache.is_fresh(1_700_000, 600_000));

        let (daily_net, daily_ts, daily_clamped) =
            cache.horizon_net_pnl("daily", 1_000_000, 750_000).unwrap();
        assert!(!daily_clamped);
        assert_eq!(daily_ts, 1_000_000);
        assert!((daily_net - 242.70).abs() < 1e-6);

        // When perpDay starts earlier than raw_start_ms, daily interpolates at raw_start_ms.
        let (interp_day_net, interp_day_ts, interp_day_clamped) =
            cache.horizon_net_pnl("daily", 1_043_200, 750_000).unwrap();
        assert!(!interp_day_clamped);
        assert_eq!(interp_day_ts, 1_043_200);
        assert!((interp_day_net - 121.35).abs() < 1e-6);

        let (wk_net, wk_ts, wk_clamped) =
            cache.horizon_net_pnl("weekly", 400_000, 750_000).unwrap();
        assert!(wk_clamped);
        assert_eq!(wk_ts, 750_000);
        assert!((wk_net - 512.50).abs() < 1e-6);

        assert!(cache
            .horizon_net_pnl("weekly", 400_000, 1_090_000)
            .is_none());

        let (ytd_net, ytd_ts, ytd_clamped) = cache.horizon_net_pnl("ytd", 450_000, 0).unwrap();
        assert!(!ytd_clamped);
        assert_eq!(ytd_ts, 450_000);
        assert!((ytd_net - 387.50).abs() < 1e-6);

        assert!(cache.horizon_net_pnl("monthly", 400_000, 0).is_none());

        // Falling back to perp_* when account_* is empty does NOT subtract vault_window_pnl_usdc.
        let (fb_net, _, _) = cache
            .horizon_net_pnl_for_mode("daily", 1_000_000, 0, true, 12.50)
            .unwrap();
        assert!((fb_net - 242.70).abs() < 1e-6);

        // With account_* populated in unifiedAccount mode, vault PnL is subtracted cleanly.
        let mut unified_cache = cache.clone();
        unified_cache.account_day = HlPortfolioWindowCache {
            pnl_history: vec![(900_000, 0.0), (1_000_000, 10.0), (1_086_400, 260.0)],
            vlm: 1412.91,
        };
        unified_cache.account_all_time = HlPortfolioWindowCache {
            pnl_history: vec![(100_000, 0.0), (1_086_400, 480.0)],
            vlm: 83291.0,
        };
        let (uni_day_net, uni_day_ts, _) = unified_cache
            .horizon_net_pnl_for_mode("daily", 1_000_000, 0, true, 5.0)
            .unwrap();
        assert_eq!(uni_day_ts, 1_000_000);
        assert!((uni_day_net - 245.0).abs() < 1e-6);

        // In default perp mode (prefer_unified_account = false), perp_* is used unchanged.
        let (def_day_net, _, _) = unified_cache
            .horizon_net_pnl_for_mode("daily", 1_000_000, 0, false, 5.0)
            .unwrap();
        assert!((def_day_net - 242.70).abs() < 1e-6);
    }

    #[test]
    fn test_partition_cap_epoch_uses_created_at_when_opened_null() {
        let held = vec![
            sample_held(1_000, None),
            sample_held(5_000, Some(6_000)),
            sample_held(2_000, Some(3_000)),
        ];
        assert_eq!(partition_cap_epoch_holdings(&held, 0), (3, 0));
        assert_eq!(partition_cap_epoch_holdings(&held, 4_000), (1, 2));
    }

    #[test]
    fn test_drawdown_breaker_uncapped_and_ruin_halt() {
        let ok = compute_drawdown_breaker_status(
            Some((2_000.0, 1_600.0)),
            0.15,
            50.0,
            "UNCAPPED_WALLET_EQUITY",
            100,
        );
        assert!(ok.tripped);
        assert!(!ok.ruin_halt_tripped);
        assert_eq!(ok.drawdown_pct, 20.0);
        assert_eq!(ok.basis, "UNCAPPED_WALLET_EQUITY");

        let ruin = compute_drawdown_breaker_status(
            Some((1_000.0, -25.0)),
            0.15,
            50.0,
            "UNCAPPED_WALLET_EQUITY",
            100,
        );
        assert!(ruin.tripped);
        assert!(ruin.ruin_halt_tripped);
        assert_eq!(ruin.drawdown_pct, 100.0);

        let floor_only = compute_drawdown_breaker_status(
            Some((40.0, 39.0)),
            0.15,
            50.0,
            "UNCAPPED_WALLET_EQUITY",
            100,
        );
        assert!(!floor_only.tripped);
        assert!(floor_only.ruin_halt_tripped);

        let health = PriorityMonitorHealth::default();
        assert_eq!(health.status, "IDLE");
    }

    #[test]
    fn test_synthesize_fallback_snapshot_and_equity_basis() {
        let mut cfg = ConfigMap::new();
        cfg.insert("execution_mode", "LIVE");
        cfg.insert("live_exchange_equity_usdc", "2400.0");
        cfg.insert("risk_capital_cap_usdc", "1000.0");
        cfg.insert("live_exchange_spot_usdc", "300.0");
        cfg.insert("live_exchange_equity_synced_at_ms", "100000");

        let snap = synthesize_fallback_snapshot(&cfg, &[], &[], &[], 120_000);
        assert_eq!(snap.total_equity_usdc, 1000.0);
        assert_eq!(snap.uncapped_equity_usdc, 2400.0);

        let basis = compute_equity_basis_summary(&cfg, &snap, 500.0, 120_000);
        assert!(basis.is_cap_binding);
        assert_eq!(basis.basis_label, "CAPPED_RISK_CAPITAL");
        assert_eq!(basis.effective_equity_usdc, 1000.0);
        assert_eq!(basis.perp_wallet_equity_usdc, 2400.0);
        assert_eq!(basis.total_wallet_equity_usdc, 3200.0);
        assert!(!basis.is_stale);

        // Old sync timestamp is stale in ACTIVE, but suppressed when STANDBY suspends polling.
        let stale_ts = 300_000;
        let active_stale = compute_equity_basis_summary(&cfg, &snap, 500.0, stale_ts);
        assert!(active_stale.is_stale);
        cfg.insert("trading_run_mode", "STANDBY");
        let standby_basis = compute_equity_basis_summary(&cfg, &snap, 500.0, stale_ts);
        assert!(!standby_basis.is_stale);
    }
}
