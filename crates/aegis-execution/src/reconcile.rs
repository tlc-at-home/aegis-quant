//! Dynamic Cutover & Live 1:1 Hyperliquid Exchange Reconciliation (`aegis-execution::reconcile`).
//!
//! Queries live Hyperliquid Testnet/Mainnet `clearinghouseState` and `openOrders` using the
//! preserved `wallet_address` and decrypted `private_key` from MariaDB `system_config`.
//! Synchronizes MariaDB `positions` 1:1 with Hyperliquid's actual `assetPositions` (`szi`, `entryPx`,
//! `positionValue`, `unrealizedPnl`), cancels orphan/stale open orders on Hyperliquid, closes local
//! phantom/flat rows, and persists live `exchange_equity_usdc`.

use crate::http_client::{is_testnet_configured, HyperliquidHttpClient};
use crate::order_chaser::{extract_order_oid, parse_hl_oid, OrderChaser};
use crate::signing::{
    derive_hl_cloid_hex, format_hl_wire_price, format_hl_wire_size_floor, infer_tick_size,
    snap_to_lot, CancelWire, LimitOrderType, OrderTypeWire, OrderWire,
};
use aegis_alerts::AlertEngine;
use aegis_core::{
    compute_vault_pnl_pct, is_unified_account_mode, mask_identifier, normalize_hyperliquid_symbol,
    now_ms, round_dp, CachedHlPortfolioPnl, ConfigMap, ExchangeAsset, ExecutionMode, HeldAssetView,
    HlPortfolioWindowCache, OrderSide, OrderedAssetView, Result, VaultInfoPayload,
    VaultPositionInfo,
};
use aegis_db::{
    AssetUpsertInput, DatabaseManager, FundingEventUpsertInput, ReconciledClosedTradeInput,
};
use aegis_risk::{
    compute_atr_from_bars, compute_initial_protective_stop, compute_initial_take_profit,
    RiskLimitsConfig,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::OnceLock;

const WORKING_ENTRY_GRACE_MS: i64 = 45_000;
const WORKING_FILL_COMPLETE_RATIO: f64 = 0.85;
const CAUSAL_CHAIN_MAX_DFS_STEPS: usize = 4096;
pub const VAULT_SYNC_MIN_INTERVAL_MS: i64 = 60_000;
pub const ABSTRACTION_SYNC_MIN_INTERVAL_MS: i64 = 300_000;
pub const UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS: i64 = 15_000;
const MAX_VAULTS_TO_FETCH: usize = 20;

type VaultSyncThrottleKey = (String, String, String);
static LAST_VAULT_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<VaultSyncThrottleKey, i64>>> =
    OnceLock::new();
type SpotSyncThrottleKey = (String, String);
static LAST_SPOT_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<SpotSyncThrottleKey, i64>>> =
    OnceLock::new();
static LAST_ABSTRACTION_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<SpotSyncThrottleKey, i64>>> =
    OnceLock::new();
static LAST_PORTFOLIO_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<SpotSyncThrottleKey, i64>>> =
    OnceLock::new();

fn parse_hl_f64_opt(val: Option<&Value>) -> Option<f64> {
    val.and_then(|x| {
        x.as_f64()
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
    })
    .filter(|v| v.is_finite())
}

fn parse_hl_i64_opt(val: Option<&Value>) -> Option<i64> {
    val.and_then(|x| {
        x.as_i64()
            .or_else(|| x.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
            .or_else(|| {
                x.as_f64()
                    .filter(|f| f.is_finite() && *f >= 0.0)
                    .map(|f| f.floor() as i64)
            })
    })
}

fn parse_hl_u64_opt(val: Option<&Value>) -> Option<u64> {
    val.and_then(|x| {
        x.as_u64()
            .or_else(|| x.as_i64().and_then(|n| u64::try_from(n).ok()))
            .or_else(|| {
                x.as_f64()
                    .filter(|f| f.is_finite() && *f >= 0.0)
                    .map(|f| f.floor() as u64)
            })
            .or_else(|| {
                x.as_str()
                    .and_then(|s| s.trim().parse::<f64>().ok())
                    .filter(|f| f.is_finite() && *f >= 0.0)
                    .map(|f| f.floor() as u64)
            })
    })
}

fn truncate_utf8_chars(raw: &str, max_chars: usize) -> String {
    match raw.char_indices().nth(max_chars) {
        Some((byte_idx, _)) => raw[..byte_idx].to_string(),
        None => raw.to_string(),
    }
}

fn extract_vault_portfolio_tvl_and_vlm(details: &Value) -> (Option<f64>, f64) {
    let Some(entries) = details.get("portfolio").and_then(Value::as_array) else {
        return (None, 0.0);
    };

    let latest_account_value = |obj: &Value| -> Option<f64> {
        let arr = obj.get("accountValueHistory")?.as_array()?;
        let mut best: Option<(i64, f64)> = None;
        for pt in arr {
            let Some(items) = pt.as_array() else {
                continue;
            };
            let Some(ts) = parse_hl_i64_opt(items.first()) else {
                continue;
            };
            let Some(val) = parse_hl_f64_opt(items.get(1)).filter(|v| *v >= 0.0) else {
                continue;
            };
            if best.is_none_or(|(best_ts, _)| ts >= best_ts) {
                best = Some((ts, val));
            }
        }
        best.map(|(_, v)| v)
    };

    // Priority order for TVL: "day", "allTime", "week", "month".
    let mut tvl_by_window: [Option<f64>; 4] = [None; 4];
    let mut all_time_vlm = 0.0_f64;

    for item in entries {
        let Some(pair) = item.as_array() else {
            continue;
        };
        let Some(window_name) = pair.first().and_then(Value::as_str).map(str::trim) else {
            continue;
        };
        let Some(obj) = pair.get(1).filter(|v| v.is_object()) else {
            continue;
        };
        match window_name {
            "day" => {
                if let Some(v) = latest_account_value(obj) {
                    tvl_by_window[0] = Some(v);
                }
            }
            "allTime" => {
                if let Some(v) = latest_account_value(obj) {
                    tvl_by_window[1] = Some(v);
                }
                if let Some(vlm) = parse_hl_f64_opt(obj.get("vlm")) {
                    all_time_vlm = vlm.max(0.0);
                }
            }
            "week" => {
                if let Some(v) = latest_account_value(obj) {
                    tvl_by_window[2] = Some(v);
                }
            }
            "month" => {
                if let Some(v) = latest_account_value(obj) {
                    tvl_by_window[3] = Some(v);
                }
            }
            _ => {}
        }
    }

    let tvl = tvl_by_window[0]
        .or(tvl_by_window[1])
        .or(tvl_by_window[2])
        .or(tvl_by_window[3]);
    (tvl, all_time_vlm)
}

/// Parses a Hyperliquid `vaultDetails` JSON object (plus optional `userVaultEquities` fallback)
/// into a [`VaultPositionInfo`] and the raw unrounded vault equity in USDC.
pub fn parse_vault_position_info_with_raw_equity(
    vault_address: &str,
    details: &Value,
    equity_fallback: Option<(f64, i64)>,
    now_ms_val: i64,
) -> (VaultPositionInfo, f64) {
    let resolved_addr = details
        .get("vaultAddress")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| vault_address.trim())
        .to_string();

    let name = details
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| mask_identifier(&resolved_addr));

    let leader = details
        .get("leader")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("")
        .to_string();

    let description = details
        .get("description")
        .and_then(Value::as_str)
        .map(str::trim)
        .map(|s| truncate_utf8_chars(s, 500))
        .unwrap_or_default();

    let is_closed = details
        .get("isClosed")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let raw_apr = parse_hl_f64_opt(details.get("apr")).unwrap_or(0.0);
    let apr_pct = {
        let pct = raw_apr * 100.0;
        if pct.is_finite() {
            round_dp(pct, 2)
        } else {
            0.0
        }
    };

    let leader_fraction_pct = parse_hl_f64_opt(details.get("leaderFraction"))
        .map(|f| round_dp((f * 100.0).clamp(0.0, 100.0), 2))
        .unwrap_or(0.0);

    let leader_commission_pct = parse_hl_f64_opt(details.get("leaderCommission"))
        .map(|f| round_dp((f * 100.0).clamp(0.0, 100.0), 2))
        .unwrap_or(0.0);

    let allow_deposits = details
        .get("allowDeposits")
        .and_then(Value::as_bool)
        .unwrap_or(!is_closed);

    let always_close_on_withdraw = details
        .get("alwaysCloseOnWithdraw")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let max_distributable_usdc = round_dp(
        parse_hl_f64_opt(details.get("maxDistributable"))
            .unwrap_or(0.0)
            .max(0.0),
        2,
    );

    let max_withdrawable_usdc = round_dp(
        parse_hl_f64_opt(details.get("maxWithdrawable"))
            .unwrap_or(0.0)
            .max(0.0),
        2,
    );

    let (follower_count, followers_capped, followers_tvl_sum) = details
        .get("followers")
        .and_then(Value::as_array)
        .map(|arr| {
            let sum: f64 = arr
                .iter()
                .filter_map(|f| parse_hl_f64_opt(f.get("vaultEquity")))
                .filter(|v| *v >= 0.0)
                .sum();
            (arr.len(), arr.len() >= 100, sum)
        })
        .unwrap_or((0, false, 0.0));

    let (portfolio_tvl, portfolio_all_time_vlm) = extract_vault_portfolio_tvl_and_vlm(details);
    let tvl_usdc = round_dp(portfolio_tvl.unwrap_or(followers_tvl_sum).max(0.0), 2);
    let all_time_volume_usdc = round_dp(portfolio_all_time_vlm.max(0.0), 2);

    let relationship_obj = details.get("relationship").filter(|v| v.is_object());
    let relationship_type = relationship_obj
        .and_then(|r| r.get("type"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("normal")
        .to_string();
    let child_vault_count = relationship_obj
        .and_then(|r| r.get("data"))
        .and_then(|d| d.get("childAddresses"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);

    let follower = details.get("followerState").filter(|v| v.is_object());
    let raw_equity = follower
        .and_then(|f| parse_hl_f64_opt(f.get("vaultEquity")))
        .or_else(|| equity_fallback.map(|(eq, _)| eq).filter(|v| v.is_finite()))
        .unwrap_or(0.0)
        .max(0.0);
    let raw_pnl = follower
        .and_then(|f| parse_hl_f64_opt(f.get("pnl")))
        .unwrap_or(0.0);
    let raw_all_time_pnl = follower
        .and_then(|f| parse_hl_f64_opt(f.get("allTimePnl")))
        .unwrap_or(0.0);

    let pnl_pct = compute_vault_pnl_pct(raw_equity, raw_pnl);
    let all_time_pnl_pct = compute_vault_pnl_pct(raw_equity, raw_all_time_pnl);
    let days_following = follower
        .and_then(|f| parse_hl_u64_opt(f.get("daysFollowing")))
        .unwrap_or(0);

    let locked_until_ms = follower
        .and_then(|f| parse_hl_i64_opt(f.get("lockupUntil")))
        .filter(|&ts| ts > 0)
        .or_else(|| equity_fallback.map(|(_, lock)| lock).filter(|&ts| ts > 0))
        .unwrap_or(0);

    let info = VaultPositionInfo {
        vault_address: resolved_addr,
        name,
        leader,
        description,
        equity_usdc: round_dp(raw_equity, 2),
        pnl_usdc: round_dp(raw_pnl, 2),
        all_time_pnl_usdc: round_dp(raw_all_time_pnl, 2),
        pnl_pct,
        all_time_pnl_pct,
        apr_pct,
        days_following,
        locked_until_ms,
        is_locked: locked_until_ms > now_ms_val,
        is_closed,
        tvl_usdc,
        follower_count,
        followers_capped,
        leader_fraction_pct,
        leader_commission_pct,
        allow_deposits,
        always_close_on_withdraw,
        max_distributable_usdc,
        max_withdrawable_usdc,
        relationship_type,
        child_vault_count,
        all_time_volume_usdc,
    };
    (info, raw_equity)
}

/// Parses a Hyperliquid `vaultDetails` JSON object into a [`VaultPositionInfo`].
pub fn parse_vault_position_info(
    vault_address: &str,
    details: &Value,
    equity_fallback: Option<(f64, i64)>,
    now_ms_val: i64,
) -> VaultPositionInfo {
    parse_vault_position_info_with_raw_equity(vault_address, details, equity_fallback, now_ms_val).0
}

/// Parses a Hyperliquid `spotClearinghouseState` JSON payload returning
/// `(total_usdc, hold_usdc, available_after_maintenance_opt)`.
/// Returns `None` if `"balances"` is missing or not a JSON array (so mock `[]` responses never
/// clobber cached spot USDC).
pub fn parse_spot_usdc_details(spot_state: &Value) -> Option<(f64, f64, Option<f64>)> {
    let balances = spot_state.get("balances")?.as_array()?;
    let mut total_usdc = 0.0_f64;
    let mut hold_usdc = 0.0_f64;
    let mut usdc_token_id: Option<i64> = None;
    for entry in balances {
        let coin = entry
            .get("coin")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if coin.eq_ignore_ascii_case("USDC") {
            if usdc_token_id.is_none() {
                usdc_token_id = parse_hl_i64_opt(entry.get("token"));
            }
            if let Some(amt) = parse_hl_f64_opt(entry.get("total")) {
                if amt >= 0.0 {
                    total_usdc += amt;
                }
            }
            if let Some(hold) = parse_hl_f64_opt(entry.get("hold")) {
                if hold >= 0.0 {
                    hold_usdc += hold;
                }
            }
        }
    }
    let target_token = usdc_token_id.unwrap_or(0);
    let avail_after_maint = spot_state
        .get("tokenToAvailableAfterMaintenance")
        .and_then(Value::as_array)
        .and_then(|arr| {
            arr.iter().find_map(|pair| {
                let items = pair.as_array()?;
                let tid = parse_hl_i64_opt(items.first())?;
                if tid == target_token {
                    parse_hl_f64_opt(items.get(1)).filter(|v| *v >= 0.0)
                } else {
                    None
                }
            })
        });
    Some((total_usdc.max(0.0), hold_usdc.max(0.0), avail_after_maint))
}

/// Parses a Hyperliquid `spotClearinghouseState` JSON payload and sums `"USDC"` `total` balances.
/// Returns `None` if `"balances"` is missing or not a JSON array (so mock `[]` responses never
/// clobber cached spot USDC).
pub fn parse_spot_usdc_balance(spot_state: &Value) -> Option<f64> {
    parse_spot_usdc_details(spot_state).map(|(total, _, _)| total)
}

/// Parses a Hyperliquid `/info {"type":"portfolio"}` response into [`CachedHlPortfolioPnl`].
/// Extracts `perpDay`, `perpWeek`, `perpMonth`, and `perpAllTime` `pnlHistory` series sorted
/// ascending and deduplicated by timestamp. Returns `None` when all windows are empty.
pub fn parse_hl_portfolio_pnl(raw: &Value, synced_at_ms: i64) -> Option<CachedHlPortfolioPnl> {
    let entries = raw.as_array()?;
    let parse_window = |obj: &Value| -> HlPortfolioWindowCache {
        let mut raw_pts: Vec<(i64, f64)> = obj
            .get("pnlHistory")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|pair| {
                        let items = pair.as_array()?;
                        let t = parse_hl_i64_opt(items.first())?;
                        let v = parse_hl_f64_opt(items.get(1))?;
                        Some((t, v))
                    })
                    .collect()
            })
            .unwrap_or_default();
        raw_pts.sort_by_key(|(t, _)| *t);
        let mut deduped: Vec<(i64, f64)> = Vec::with_capacity(raw_pts.len());
        for (t, v) in raw_pts {
            if let Some(last) = deduped.last_mut() {
                if last.0 == t {
                    last.1 = v;
                    continue;
                }
            }
            deduped.push((t, v));
        }
        let vlm = parse_hl_f64_opt(obj.get("vlm")).unwrap_or(0.0).max(0.0);
        HlPortfolioWindowCache {
            pnl_history: deduped,
            vlm,
        }
    };

    let mut out = CachedHlPortfolioPnl {
        synced_at_ms,
        ..Default::default()
    };
    for item in entries {
        let Some(pair) = item.as_array() else {
            continue;
        };
        let Some(name) = pair.first().and_then(Value::as_str).map(str::trim) else {
            continue;
        };
        let Some(obj) = pair.get(1) else {
            continue;
        };
        match name {
            "perpDay" => out.perp_day = parse_window(obj),
            "perpWeek" => out.perp_week = parse_window(obj),
            "perpMonth" => out.perp_month = parse_window(obj),
            "perpAllTime" => out.perp_all_time = parse_window(obj),
            _ => {}
        }
    }
    let any_non_empty = !out.perp_day.pnl_history.is_empty()
        || !out.perp_week.pnl_history.is_empty()
        || !out.perp_month.pnl_history.is_empty()
        || !out.perp_all_time.pnl_history.is_empty();
    any_non_empty.then_some(out)
}

/// Structured report produced by [`CutoverReconciler::run_cutover_reconciliation`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CutoverReconcileReport {
    pub reconciled_at_ms: i64,
    pub execution_mode: String,
    pub is_testnet: bool,
    pub wallet_address: String,
    pub exchange_reachable: bool,
    pub exchange_equity_usdc: f64,
    pub stale_orders_found: usize,
    pub stale_orders_cancelled: usize,
    pub legacy_positions_found: usize,
    pub legacy_positions_closed: usize,
    pub legacy_positions_adopted: usize,
    pub local_orphans_closed: usize,
    pub details: Vec<String>,
}

#[derive(Clone, Debug)]
struct ParsedHlFill {
    coin: String,
    side_str: String,
    px: f64,
    sz: f64,
    fee: f64,
    closed_pnl: f64,
    dir: String,
    time_ms: i64,
    oid: u64,
    tid: u64,
    start_position: Option<f64>,
}

fn fill_signed_sz(fill: &ParsedHlFill) -> f64 {
    if fill.side_str == "B" {
        fill.sz
    } else {
        -fill.sz
    }
}

fn szi_approx_eq(a: f64, b: f64) -> bool {
    let tol = 1e-7_f64.max(a.abs().max(b.abs()) * 1e-10);
    (a - b).abs() <= tol
}

fn fill_is_closing_dir(fill: &ParsedHlFill) -> bool {
    fill.closed_pnl.abs() > 1e-9 || fill.dir.starts_with("Close") || fill.dir.contains('>')
}

fn dfs_causal_chain(
    sp: &[f64],
    end: &[f64],
    used: &mut [bool],
    path: &mut Vec<usize>,
    cur_szi: f64,
    steps: &mut usize,
    max_steps: usize,
) -> bool {
    if path.len() == sp.len() {
        return true;
    }
    if *steps >= max_steps {
        return false;
    }
    *steps += 1;

    for idx in 0..sp.len() {
        if used[idx] || !szi_approx_eq(sp[idx], cur_szi) {
            continue;
        }
        used[idx] = true;
        path.push(idx);
        if dfs_causal_chain(sp, end, used, path, end[idx], steps, max_steps) {
            return true;
        }
        path.pop();
        used[idx] = false;
        if *steps >= max_steps {
            return false;
        }
    }
    false
}

fn find_causal_chain_from_start(group: &[ParsedHlFill], start_szi: f64) -> Option<Vec<usize>> {
    let sp: Vec<f64> = group
        .iter()
        .map(|f| f.start_position)
        .collect::<Option<Vec<_>>>()?;
    let end: Vec<f64> = group
        .iter()
        .zip(sp.iter())
        .map(|(f, &s)| s + fill_signed_sz(f))
        .collect();
    let mut used = vec![false; group.len()];
    let mut path = Vec::with_capacity(group.len());
    let mut steps = 0usize;
    dfs_causal_chain(
        &sp,
        &end,
        &mut used,
        &mut path,
        start_szi,
        &mut steps,
        CAUSAL_CHAIN_MAX_DFS_STEPS,
    )
    .then_some(path)
}

fn resolve_same_ms_group(
    group: &[ParsedHlFill],
    running_szi: Option<f64>,
    next_group: Option<&[ParsedHlFill]>,
) -> Option<Vec<usize>> {
    let sp: Vec<f64> = group
        .iter()
        .map(|f| f.start_position)
        .collect::<Option<Vec<_>>>()?;
    let end: Vec<f64> = group
        .iter()
        .zip(sp.iter())
        .map(|(f, &s)| s + fill_signed_sz(f))
        .collect();
    let next_has_positions = next_group
        .is_some_and(|ng| !ng.is_empty() && ng.iter().all(|f| f.start_position.is_some()));

    let matches_next = |chain: &[usize]| -> bool {
        let Some(last_idx) = chain.last().copied() else {
            return false;
        };
        let Some(ng) = next_group else {
            return false;
        };
        next_has_positions && find_causal_chain_from_start(ng, end[last_idx]).is_some()
    };

    if let Some(s0) = running_szi {
        let mut best: Option<(bool, Vec<usize>)> = None;
        for first_idx in 0..group.len() {
            if !szi_approx_eq(sp[first_idx], s0) {
                continue;
            }
            let mut used = vec![false; group.len()];
            used[first_idx] = true;
            let mut path = Vec::with_capacity(group.len());
            path.push(first_idx);
            let mut steps = 0usize;
            if dfs_causal_chain(
                &sp,
                &end,
                &mut used,
                &mut path,
                end[first_idx],
                &mut steps,
                CAUSAL_CHAIN_MAX_DFS_STEPS,
            ) {
                let next_ok = matches_next(&path);
                let better = match &best {
                    None => true,
                    Some((best_next, best_path)) => {
                        (next_ok, Reverse(&path)) > (*best_next, Reverse(best_path))
                    }
                };
                if better {
                    best = Some((next_ok, path));
                }
            }
        }
        if let Some((_, best_path)) = best {
            return Some(best_path);
        }
    }

    // Either `running_szi` is unknown (`None` at history start) or an upstream gap broke `running_szi`.
    // Score candidate causal chains within the group by lookahead match, in-degree-0 root, and flat open.
    let mut best: Option<((bool, bool, bool), Vec<usize>)> = None;
    for first_idx in 0..group.len() {
        let mut used = vec![false; group.len()];
        used[first_idx] = true;
        let mut path = Vec::with_capacity(group.len());
        path.push(first_idx);
        let mut steps = 0usize;
        if dfs_causal_chain(
            &sp,
            &end,
            &mut used,
            &mut path,
            end[first_idx],
            &mut steps,
            CAUSAL_CHAIN_MAX_DFS_STEPS,
        ) {
            let next_ok = matches_next(&path);
            let zero_in_degree =
                !(0..group.len()).any(|j| j != first_idx && szi_approx_eq(end[j], sp[first_idx]));
            let starts_flat_open =
                szi_approx_eq(sp[first_idx], 0.0) && !fill_is_closing_dir(&group[first_idx]);
            let score = (next_ok, zero_in_degree, starts_flat_open);
            let better = match &best {
                None => true,
                Some((best_score, best_path)) => {
                    (score, Reverse(&path)) > (*best_score, Reverse(best_path))
                }
            };
            if better {
                best = Some((score, path));
            }
        }
    }
    best.map(|(_, path)| path)
}

/// Orders `userFills` strictly by `time_ms` ascending, resolving same-millisecond ties
/// causally via `start_position` chaining (`prev_szi + signed_sz(fill) == next.start_position`),
/// falling back to `tid` ascending when `startPosition` is absent or ambiguous.
fn sort_fills_causally(fills: &mut [ParsedHlFill]) {
    if fills.len() <= 1 {
        return;
    }
    fills.sort_by_key(|f| (f.time_ms, f.tid));

    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut i = 0;
    while i < fills.len() {
        let t = fills[i].time_ms;
        let mut j = i + 1;
        while j < fills.len() && fills[j].time_ms == t {
            j += 1;
        }
        ranges.push((i, j));
        i = j;
    }

    let mut running_szi: Option<f64> = None;
    for idx in 0..ranges.len() {
        let (start, end_idx) = ranges[idx];
        let next_group = ranges.get(idx + 1).map(|&(ns, ne)| &fills[ns..ne]);
        if end_idx - start >= 2 {
            if let Some(order) =
                resolve_same_ms_group(&fills[start..end_idx], running_szi, next_group)
            {
                let original = fills[start..end_idx].to_vec();
                for (dst_pos, &src_pos) in order.iter().enumerate() {
                    fills[start + dst_pos] = original[src_pos].clone();
                }
            }
        }
        let last = &fills[end_idx - 1];
        running_szi = if let Some(sp) = last.start_position {
            Some(sp + fill_signed_sz(last))
        } else {
            running_szi.map(|prev| {
                prev + fills[start..end_idx]
                    .iter()
                    .map(fill_signed_sz)
                    .sum::<f64>()
            })
        };
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkingOrderReconcileDisposition {
    ProtectPending,
    ProtectActivePartial,
    AdoptOrExpire,
}

fn is_entry_fill_complete(
    fill_qty: f64,
    requested_qty: f64,
    lot_step_size: f64,
    sz_decimals: u32,
) -> bool {
    if !fill_qty.is_finite() || fill_qty <= 1e-9 {
        return false;
    }
    if !requested_qty.is_finite() || requested_qty <= 1e-9 {
        return true;
    }
    let eff_lot = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        lot_step_size
    } else {
        10_f64.powi(-(sz_decimals.min(6) as i32))
    };
    let rem_qty = snap_to_lot(
        (requested_qty - fill_qty).max(0.0),
        eff_lot,
        sz_decimals,
        true,
    );
    fill_qty + 1e-9 >= requested_qty * WORKING_FILL_COMPLETE_RATIO
        || rem_qty <= eff_lot * 0.5
        || rem_qty <= 1e-9
}

fn resolve_working_entry_oid(
    ordered: &OrderedAssetView,
    cloid_to_oid: &HashMap<String, u64>,
) -> Option<u64> {
    parse_hl_oid(ordered.exchange_order_id.as_deref()).or_else(|| {
        cloid_to_oid
            .get(&ordered.client_order_id.trim().to_ascii_lowercase())
            .copied()
    })
}

#[allow(clippy::too_many_arguments)]
fn classify_working_order_for_reconcile(
    age_ms: i64,
    order_side: &str,
    requested_qty: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    entry_oid: Option<u64>,
    resting_exchange_oids: &HashSet<u64>,
    exchange_szi_opt: Option<f64>,
) -> WorkingOrderReconcileDisposition {
    if age_ms >= WORKING_ENTRY_GRACE_MS {
        return WorkingOrderReconcileDisposition::AdoptOrExpire;
    }
    let Some(szi) = exchange_szi_opt.filter(|s| s.abs() > 1e-9) else {
        return WorkingOrderReconcileDisposition::ProtectPending;
    };
    let same_side = (order_side.eq_ignore_ascii_case("LONG") && szi > 0.0)
        || (order_side.eq_ignore_ascii_case("SHORT") && szi < 0.0);
    if !same_side {
        return WorkingOrderReconcileDisposition::AdoptOrExpire;
    }
    let is_resting = entry_oid.is_some_and(|oid| resting_exchange_oids.contains(&oid));
    if !is_resting {
        return WorkingOrderReconcileDisposition::AdoptOrExpire;
    }
    if is_entry_fill_complete(szi.abs(), requested_qty, lot_step_size, sz_decimals) {
        WorkingOrderReconcileDisposition::AdoptOrExpire
    } else {
        WorkingOrderReconcileDisposition::ProtectActivePartial
    }
}

fn parse_order_timestamp_ms(ord: &Value) -> u64 {
    ord.get("timestamp")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
        })
        .unwrap_or(0)
}

fn normalize_order_coin_upper(ord: &Value) -> Option<String> {
    let raw_coin = ord.get("coin").and_then(Value::as_str)?.trim();
    if raw_coin.is_empty() {
        return None;
    }
    let coin_upper = normalize_hyperliquid_symbol(raw_coin)
        .map(|(_, c)| c.to_ascii_uppercase())
        .unwrap_or_else(|_| raw_coin.to_ascii_uppercase());
    Some(coin_upper)
}

fn is_trigger_order(ord: &Value) -> bool {
    if ord.get("isTrigger").and_then(Value::as_bool) == Some(true)
        || ord.get("isPositionTpsl").and_then(Value::as_bool) == Some(true)
    {
        return true;
    }
    let has_trigger_px = ord
        .get("triggerPx")
        .and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
        })
        .is_some_and(|px| px.is_finite() && px > 0.0);
    if has_trigger_px {
        return true;
    }
    ord.get("orderType")
        .and_then(Value::as_str)
        .is_some_and(|t| {
            let lower = t.to_ascii_lowercase();
            lower.contains("stop") || lower.contains("take profit")
        })
}

/// Returns `true` when a resting `frontendOpenOrders` entry is a non-trigger `reduceOnly`
/// passive Limit close order (e.g. `engine.rs`'s fallback GTC close on an illiquid book).
fn is_passive_reduce_only_close_order(ord: &Value) -> bool {
    if ord.get("reduceOnly").and_then(Value::as_bool) != Some(true) {
        return false;
    }
    if is_trigger_order(ord) {
        return false;
    }
    ord.get("orderType")
        .and_then(Value::as_str)
        .is_none_or(|t| {
            let trimmed = t.trim();
            trimmed.is_empty() || trimmed.eq_ignore_ascii_case("Limit")
        })
}

/// Selects the newest passive `reduceOnly` Limit close OID `(highest timestamp, oid)` per coin
/// currently open on Hyperliquid so reconciler does not cancel active resting close orders.
fn collect_protected_passive_close_oids(
    open_orders: &[Value],
    open_coins: &HashSet<String>,
) -> HashSet<u64> {
    let mut newest_by_coin: HashMap<String, (u64, u64)> = HashMap::new();
    for ord in open_orders {
        if !is_passive_reduce_only_close_order(ord) {
            continue;
        }
        let Some(coin_upper) = normalize_order_coin_upper(ord) else {
            continue;
        };
        if !open_coins.contains(&coin_upper) {
            continue;
        }
        let Some(oid) = extract_order_oid(ord) else {
            continue;
        };
        let candidate = (parse_order_timestamp_ms(ord), oid);
        match newest_by_coin.get(&coin_upper) {
            Some(&current) if current >= candidate => {}
            _ => {
                newest_by_coin.insert(coin_upper, candidate);
            }
        }
    }
    newest_by_coin.into_values().map(|(_, oid)| oid).collect()
}

/// Partitions resting exchange `open_orders` into protected OIDs and orphan `(coin, oid)` pairs to cancel.
fn find_orphan_orders_to_cancel(
    open_orders: &[Value],
    open_coins: &HashSet<String>,
    tracked_protected_oids: &HashSet<u64>,
    close_legacy_positions: bool,
) -> (HashSet<u64>, Vec<(String, u64)>) {
    let protected_oids: HashSet<u64> = if close_legacy_positions {
        HashSet::new()
    } else {
        let mut set = tracked_protected_oids.clone();
        set.extend(collect_protected_passive_close_oids(
            open_orders,
            open_coins,
        ));
        set
    };

    let mut orphans = Vec::new();
    for ord in open_orders {
        let coin = ord.get("coin").and_then(Value::as_str).unwrap_or("").trim();
        let Some(oid) = extract_order_oid(ord) else {
            continue;
        };
        if coin.is_empty() {
            continue;
        }
        if !close_legacy_positions && protected_oids.contains(&oid) {
            continue;
        }
        orphans.push((coin.to_string(), oid));
    }
    (protected_oids, orphans)
}

#[derive(Default)]
struct AggClosingOrder {
    coin: String,
    side: Option<OrderSide>,
    total_sz: f64,
    notional_sum: f64,
    gross_closed_pnl: f64,
    close_fees_usdc: f64,
    open_fees_usdc: f64,
    open_time_ms: Option<i64>,
    min_time_ms: i64,
    max_time_ms: i64,
    funding_accrued_usdc: f64,
}

#[derive(Clone, Debug)]
struct ParsedHlFunding {
    coin: String,
    time_ms: i64,
    usdc: f64,
    szi: f64,
    funding_rate: f64,
    dedup_key: String,
}

#[derive(Default)]
struct FifoFundingReplayOutput {
    closed_orders: Vec<(u64, AggClosingOrder)>,
    open_epoch_booked_funding_by_coin: HashMap<String, f64>,
    funding_events_with_remaining: Vec<(ParsedHlFunding, f64)>,
}

struct OpenLot {
    rem_sz: f64,
    orig_sz: f64,
    fee: f64,
    funding_accrued: f64,
    funding_by_ev: Vec<(usize, f64)>,
    time_ms: i64,
}

fn is_open_or_add_fill(fill: &ParsedHlFill, prev_szi: f64) -> bool {
    let eff_prev = if prev_szi.abs() <= 1e-9 {
        0.0
    } else {
        prev_szi
    };
    (eff_prev == 0.0 && !fill_is_closing_dir(fill))
        || (eff_prev != 0.0 && eff_prev.signum() == fill_signed_sz(fill).signum())
}

struct CoinReplayState {
    open_lots: VecDeque<OpenLot>,
    prev_szi: f64,
    rem_usdc: Vec<f64>,
    active_epoch_ev_indices: Vec<usize>,
    open_epoch_booked_funding: f64,
}

impl CoinReplayState {
    fn new(funding_len: usize) -> Self {
        Self {
            open_lots: VecDeque::new(),
            prev_szi: 0.0,
            rem_usdc: vec![0.0; funding_len],
            active_epoch_ev_indices: Vec::new(),
            open_epoch_booked_funding: 0.0,
        }
    }

    fn apply_funding(&mut self, ev_idx: usize, ev: &ParsedHlFunding) {
        let total_open: f64 = self.open_lots.iter().map(|l| l.rem_sz).sum();
        if total_open > 1e-9 {
            for lot in self.open_lots.iter_mut() {
                let share = ev.usdc * (lot.rem_sz / total_open);
                lot.funding_accrued += share;
                lot.funding_by_ev.push((ev_idx, share));
            }
            self.rem_usdc[ev_idx] = ev.usdc;
            self.active_epoch_ev_indices.push(ev_idx);
        } else {
            self.rem_usdc[ev_idx] = 0.0;
        }
    }

    fn apply_fill(
        &mut self,
        fill: &ParsedHlFill,
        agg_by_key: &mut BTreeMap<(String, u64), AggClosingOrder>,
    ) {
        if self.prev_szi.abs() <= 1e-9 {
            self.prev_szi = 0.0;
        }
        let signed_sz = fill_signed_sz(fill);
        let is_closing_dir = fill_is_closing_dir(fill);

        if is_open_or_add_fill(fill, self.prev_szi) {
            self.open_lots.push_back(OpenLot {
                rem_sz: fill.sz,
                orig_sz: fill.sz,
                fee: fill.fee,
                funding_accrued: 0.0,
                funding_by_ev: Vec::new(),
                time_ms: fill.time_ms,
            });
            self.prev_szi += signed_sz;
            return;
        }

        // Closing or flip fill (`dir.contains('>')`): attribute only closed_sz to the closed side
        let had_open_pos = self.prev_szi.abs() > 1e-9;
        let (closed_sz, closed_pos_side) = if had_open_pos {
            (
                self.prev_szi.abs().min(fill.sz),
                if self.prev_szi > 0.0 {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                },
            )
        } else {
            let fallback_side = if fill.dir.starts_with("Close Long")
                || fill.dir.starts_with("Long >")
                || (!fill.dir.contains("Short") && fill.side_str == "A")
            {
                OrderSide::Long
            } else {
                OrderSide::Short
            };
            (fill.sz, fallback_side)
        };
        let flipped_open_sz = (fill.sz - closed_sz).max(0.0);
        let close_fee = if fill.sz > 0.0 {
            fill.fee * (closed_sz / fill.sz)
        } else {
            0.0
        };
        let open_fee = (fill.fee - close_fee).max(0.0);

        let mut matched_open_fee = 0.0_f64;
        let mut matched_funding = 0.0_f64;
        let mut earliest_open_time_ms: Option<i64> = None;
        let mut rem_to_close = closed_sz;
        while rem_to_close > 1e-9 {
            let Some(front) = self.open_lots.front_mut() else {
                break;
            };
            let take = rem_to_close.min(front.rem_sz);
            let frac = if front.rem_sz > 0.0 {
                (take / front.rem_sz).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let matched_funding_part = front.funding_accrued * frac;
            front.funding_accrued -= matched_funding_part;
            matched_funding += matched_funding_part;
            for (ev_i, lot_share) in &mut front.funding_by_ev {
                let ev_part = *lot_share * frac;
                *lot_share -= ev_part;
                self.rem_usdc[*ev_i] -= ev_part;
                if self.rem_usdc[*ev_i].abs() <= 1e-12 {
                    self.rem_usdc[*ev_i] = 0.0;
                }
            }
            if front.orig_sz > 0.0 {
                matched_open_fee += front.fee * (take / front.orig_sz);
            }
            earliest_open_time_ms =
                Some(earliest_open_time_ms.map_or(front.time_ms, |t| t.min(front.time_ms)));
            front.rem_sz -= take;
            rem_to_close -= take;
            if front.rem_sz <= 1e-9 {
                self.open_lots.pop_front();
            }
        }

        if closed_sz > 1e-9 && (is_closing_dir || had_open_pos) {
            let entry = agg_by_key
                .entry((fill.coin.clone(), fill.oid))
                .or_insert_with(|| AggClosingOrder {
                    coin: fill.coin.clone(),
                    side: Some(closed_pos_side),
                    min_time_ms: fill.time_ms,
                    max_time_ms: fill.time_ms,
                    ..Default::default()
                });
            entry.total_sz += closed_sz;
            entry.notional_sum += fill.px * closed_sz;
            entry.gross_closed_pnl += fill.closed_pnl;
            entry.close_fees_usdc += close_fee;
            entry.open_fees_usdc += matched_open_fee;
            entry.funding_accrued_usdc += matched_funding;
            if let Some(ot) = earliest_open_time_ms {
                entry.open_time_ms = Some(entry.open_time_ms.map_or(ot, |cur| cur.min(ot)));
            }
            entry.min_time_ms = entry.min_time_ms.min(fill.time_ms);
            entry.max_time_ms = entry.max_time_ms.max(fill.time_ms);
        }

        let open_sz_after: f64 = self.open_lots.iter().map(|l| l.rem_sz).sum();
        if open_sz_after <= 1e-9 || flipped_open_sz > 1e-9 {
            for &ev_i in &self.active_epoch_ev_indices {
                self.rem_usdc[ev_i] = 0.0;
            }
            self.active_epoch_ev_indices.clear();
            self.open_epoch_booked_funding = 0.0;
        } else {
            self.open_epoch_booked_funding += matched_funding;
        }

        if flipped_open_sz > 1e-9 {
            self.open_lots.push_back(OpenLot {
                rem_sz: flipped_open_sz,
                orig_sz: flipped_open_sz,
                fee: open_fee,
                funding_accrued: 0.0,
                funding_by_ev: Vec::new(),
                time_ms: fill.time_ms,
            });
        }

        if had_open_pos || fill.start_position.is_some() {
            self.prev_szi += signed_sz;
            if self.prev_szi.abs() <= 1e-9 {
                self.prev_szi = 0.0;
            }
        } else if flipped_open_sz > 1e-9 {
            self.prev_szi = if signed_sz > 0.0 {
                flipped_open_sz
            } else {
                -flipped_open_sz
            };
        } else {
            self.prev_szi = 0.0;
        }
    }
}

fn compute_fifo_closed_orders_and_funding(
    parsed_fills_by_coin: &BTreeMap<String, Vec<ParsedHlFill>>,
    funding_by_coin: &HashMap<String, Vec<ParsedHlFunding>>,
    open_szi_by_coin: &HashMap<String, f64>,
) -> FifoFundingReplayOutput {
    let mut all_coins: BTreeSet<String> = parsed_fills_by_coin.keys().cloned().collect();
    all_coins.extend(funding_by_coin.keys().cloned());

    let mut agg_by_key: BTreeMap<(String, u64), AggClosingOrder> = BTreeMap::new();
    let mut open_epoch_booked_funding_by_coin: HashMap<String, f64> = HashMap::new();
    let mut funding_events_with_remaining: Vec<(ParsedHlFunding, f64)> = Vec::new();

    for coin_upper in all_coins {
        let fills: &[ParsedHlFill] = parsed_fills_by_coin
            .get(&coin_upper)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let funding_raw: &[ParsedHlFunding] = funding_by_coin
            .get(&coin_upper)
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        let mut fund_indices: Vec<usize> = (0..funding_raw.len()).collect();
        fund_indices.sort_by_key(|&idx| (funding_raw[idx].time_ms, idx));

        let mut state = CoinReplayState::new(funding_raw.len());
        let earliest_funding_ts = fund_indices.first().map(|&idx| funding_raw[idx].time_ms);

        if let Some(first_fill) = fills.first() {
            if let Some(sp) = first_fill.start_position {
                if sp.abs() > 1e-9 {
                    let earliest_ts = earliest_funding_ts
                        .map_or(first_fill.time_ms, |ft| first_fill.time_ms.min(ft));
                    state.open_lots.push_back(OpenLot {
                        rem_sz: sp.abs(),
                        orig_sz: sp.abs(),
                        fee: 0.0,
                        funding_accrued: 0.0,
                        funding_by_ev: Vec::new(),
                        time_ms: earliest_ts.saturating_sub(1),
                    });
                    state.prev_szi = sp;
                }
            }
        } else if !fund_indices.is_empty() {
            if let Some(&szi) = open_szi_by_coin.get(&coin_upper) {
                if szi.abs() > 1e-9 {
                    let earliest_ts = earliest_funding_ts.unwrap_or(0);
                    state.open_lots.push_back(OpenLot {
                        rem_sz: szi.abs(),
                        orig_sz: szi.abs(),
                        fee: 0.0,
                        funding_accrued: 0.0,
                        funding_by_ev: Vec::new(),
                        time_ms: earliest_ts.saturating_sub(1),
                    });
                    state.prev_szi = szi;
                }
            }
        }

        let mut f_idx = 0usize;
        let mut e_idx = 0usize;
        while f_idx < fills.len() || e_idx < fund_indices.len() {
            let cur_ms = match (fills.get(f_idx), fund_indices.get(e_idx)) {
                (Some(f), Some(&ev_i)) => f.time_ms.min(funding_raw[ev_i].time_ms),
                (Some(f), None) => f.time_ms,
                (None, Some(&ev_i)) => funding_raw[ev_i].time_ms,
                (None, None) => break,
            };
            while f_idx < fills.len()
                && fills[f_idx].time_ms == cur_ms
                && is_open_or_add_fill(&fills[f_idx], state.prev_szi)
            {
                state.apply_fill(&fills[f_idx], &mut agg_by_key);
                f_idx += 1;
            }
            while e_idx < fund_indices.len() && funding_raw[fund_indices[e_idx]].time_ms == cur_ms {
                let ev_i = fund_indices[e_idx];
                state.apply_funding(ev_i, &funding_raw[ev_i]);
                e_idx += 1;
            }
            while f_idx < fills.len() && fills[f_idx].time_ms == cur_ms {
                state.apply_fill(&fills[f_idx], &mut agg_by_key);
                f_idx += 1;
            }
        }

        let remaining_open_sz: f64 = state.open_lots.iter().map(|l| l.rem_sz).sum();
        if remaining_open_sz > 1e-9 && state.open_epoch_booked_funding.abs() > 1e-12 {
            open_epoch_booked_funding_by_coin
                .insert(coin_upper.clone(), state.open_epoch_booked_funding);
        }
        for &ev_i in &fund_indices {
            funding_events_with_remaining.push((funding_raw[ev_i].clone(), state.rem_usdc[ev_i]));
        }
    }

    let mut closed_orders: Vec<(u64, AggClosingOrder)> = agg_by_key
        .into_iter()
        .map(|((_, oid), agg)| (oid, agg))
        .collect();
    closed_orders.sort_by_key(|(_, agg)| agg.max_time_ms);

    FifoFundingReplayOutput {
        closed_orders,
        open_epoch_booked_funding_by_coin,
        funding_events_with_remaining,
    }
}

fn derive_open_position_opened_at_ms(fills: &[ParsedHlFill], current_szi: f64) -> Option<i64> {
    if fills.is_empty() || current_szi.abs() <= 1e-9 {
        return None;
    }
    let mut pos_after = current_szi;
    let mut oldest_open_ts: Option<i64> = None;
    for f in fills.iter().rev() {
        let fill_signed = if f.side_str == "B" { f.sz } else { -f.sz };
        if fill_signed * current_szi > 0.0 {
            oldest_open_ts = Some(f.time_ms);
        }
        let pos_before = pos_after - fill_signed;
        if pos_before.abs() <= 1e-7 || (pos_before * current_szi) <= 0.0 {
            break;
        }
        pos_after = pos_before;
    }
    oldest_open_ts
}

/// Dynamic Cutover & Live 1:1 Exchange State Reconciler.
#[derive(Clone)]
pub struct CutoverReconciler {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
}

impl CutoverReconciler {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient) -> Self {
        Self { db, http }
    }

    #[allow(clippy::too_many_arguments)]
    fn resolve_or_upsert_asset(
        &self,
        coin: &str,
        symbol: &str,
        ref_px: f64,
        is_core: bool,
        is_event: bool,
        is_prio: bool,
        meta_cache: &mut Option<HashMap<String, (u32, u32, f64)>>,
        cfg: &aegis_core::ConfigMap,
    ) -> Result<Option<ExchangeAsset>> {
        if let Some(existing) = self.db.get_asset_by_symbol(symbol)? {
            return Ok(Some(existing));
        }
        if meta_cache.is_none() {
            let mut map = HashMap::new();
            if let Ok(meta_ctxs) = self.http.fetch_meta_and_asset_ctxs(cfg) {
                if let Some(universe) = meta_ctxs
                    .as_array()
                    .and_then(|arr| arr.first())
                    .and_then(|u| u.get("universe"))
                    .and_then(Value::as_array)
                {
                    for (idx, u) in universe.iter().enumerate() {
                        let raw_name = u.get("name").and_then(Value::as_str).unwrap_or("").trim();
                        if let Ok((_, norm_coin)) = normalize_hyperliquid_symbol(raw_name) {
                            let sz_dec = u
                                .get("szDecimals")
                                .and_then(Value::as_u64)
                                .unwrap_or(2)
                                .min(6) as u32;
                            let max_lev = u
                                .get("maxLeverage")
                                .and_then(|v| v.as_f64().or_else(|| v.as_u64().map(|n| n as f64)))
                                .unwrap_or(20.0)
                                .max(1.0);
                            map.insert(
                                norm_coin.to_ascii_uppercase(),
                                (idx as u32, sz_dec, max_lev),
                            );
                        }
                    }
                }
            }
            *meta_cache = Some(map);
        }

        let Some(&(asset_index, sz_decimals, max_leverage)) = meta_cache
            .as_ref()
            .and_then(|m| m.get(&coin.to_ascii_uppercase()))
        else {
            tracing::warn!(
                coin = %coin,
                symbol = %symbol,
                "Could not resolve asset_index from metaAndAssetCtxs; skipping instead of defaulting to asset_index=0"
            );
            return Ok(None);
        };

        let lot_step_size = 10_f64.powi(-(sz_decimals as i32)).max(1e-6);
        let tick = infer_tick_size(ref_px, sz_decimals);
        let aid = self.db.upsert_exchange_asset(&AssetUpsertInput {
            raw_symbol_or_coin: coin.to_string(),
            asset_index,
            sz_decimals,
            tick_size: tick,
            lot_step_size,
            max_leverage,
            mark_price: ref_px,
            mid_price: ref_px,
            oracle_price: ref_px,
            funding_rate_hourly: 0.0,
            day_volume_usdc: 100_000.0,
            open_interest_usdc: 50_000.0,
            prev_day_price: ref_px,
            is_core_eligible: is_core,
            is_event_eligible: is_event,
            is_priority_watched: is_prio,
        })?;
        self.db.get_asset_by_id(aid)
    }

    /// Performs an exact 1:1 synchronization of MariaDB `positions` and `portfolio_snapshots`
    /// against live Hyperliquid `clearinghouseState` and `openOrders`.
    pub fn sync_db_with_hyperliquid(&self) -> Result<CutoverReconcileReport> {
        self.run_cutover_reconciliation(false)
    }

    /// Queries live Hyperliquid `clearinghouseState` and `openOrders`, cancels stale/untracked open orders,
    /// and either closes (`close_legacy_positions == true`) or 1:1 synchronizes all open Hyperliquid positions
    /// into MariaDB while closing any local rows that are flat on Hyperliquid.
    pub fn run_cutover_reconciliation(
        &self,
        close_legacy_positions: bool,
    ) -> Result<CutoverReconcileReport> {
        let _exec_guard = crate::engine::execution_lock().lock();
        let ts = now_ms();
        let cfg = self.db.load_config_or_default();
        let mode = cfg.execution_mode();
        let is_testnet = is_testnet_configured(&cfg);
        let wallet = cfg.get_str("wallet_address", "");
        let private_key = cfg.get_str("private_key", "");
        let fallback_eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);

        let mut report = CutoverReconcileReport {
            reconciled_at_ms: ts,
            execution_mode: mode.as_str().to_string(),
            is_testnet,
            wallet_address: mask_identifier(&wallet),
            exchange_reachable: false,
            exchange_equity_usdc: fallback_eq,
            ..Default::default()
        };

        if mode == ExecutionMode::Paper || wallet.trim().is_empty() {
            report.details.push(format!(
                "Cutover reconciliation ran in {} mode (wallet_configured={})",
                mode.as_str(),
                !wallet.trim().is_empty()
            ));
            let _ = self.db.log_telemetry(
                "INFO",
                "execution.reconcile",
                "Cutover reconciliation completed in PAPER / local mode",
                Some(serde_json::to_value(&report).unwrap_or_default()),
            );
            return Ok(report);
        }

        let ch_state = match self.http.fetch_clearinghouse_state(&wallet, &cfg) {
            Ok(val) => {
                report.exchange_reachable = true;
                val
            }
            Err(e) => {
                report
                    .details
                    .push(format!("Clearinghouse state unreachable: {e}"));
                let _ = self.db.log_telemetry(
                    "WARN",
                    "execution.reconcile",
                    &format!("Hyperliquid clearinghouseState unreachable during cutover: {e}"),
                    Some(serde_json::to_value(&report).unwrap_or_default()),
                );
                return Ok(report);
            }
        };

        let parse_f_opt = |v: Option<&Value>| -> Option<f64> {
            v.and_then(|x| {
                x.as_f64()
                    .or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok()))
            })
            .filter(|val| val.is_finite())
        };
        let parse_f = |v: Option<&Value>| -> f64 { parse_f_opt(v).unwrap_or(0.0) };

        // 1. Resolve perp margin accountValue plus Hyperliquid userAbstraction & spot collateral
        let perp_eq = {
            let m_eq = parse_f(ch_state.pointer("/marginSummary/accountValue"));
            let cm_eq = parse_f(ch_state.pointer("/crossMarginSummary/accountValue"));
            let w_eq = parse_f(ch_state.get("withdrawable"));
            if m_eq > 0.0 {
                m_eq
            } else if cm_eq > 0.0 {
                cm_eq
            } else {
                w_eq
            }
        };
        let margin_used = parse_f(ch_state.pointer("/marginSummary/totalMarginUsed"));
        let withdrawable = parse_f(ch_state.get("withdrawable"));
        let _ = self.db.set_config_value(
            "live_exchange_perp_margin_equity_usdc",
            &format!("{:.2}", round_dp(perp_eq.max(0.0), 2)),
            Some("Latest synced Hyperliquid perp clearinghouseState marginSummary.accountValue in USDC"),
        );

        let account_mode = self
            .sync_account_mode_if_due(&cfg, ts)
            .unwrap_or_else(|| cfg.get_str("live_exchange_account_mode", "default"));
        let is_unified = is_unified_account_mode(&account_mode);
        let spot_interval = if is_unified {
            UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS
        } else {
            VAULT_SYNC_MIN_INTERVAL_MS
        };
        let fresh_spot = self.sync_spot_usdc_details_with_interval(&cfg, ts, spot_interval);
        if let Some((spot_total, _, _)) = fresh_spot {
            report.details.push(format!(
                "Synced Hyperliquid spot USDC balance (${spot_total:.2}, account_mode={account_mode})"
            ));
        }

        let (effective_wallet_eq, effective_withdrawable) = if is_unified {
            let cached_spot = cfg.get_f64("live_exchange_spot_usdc", 0.0).max(0.0);
            if let Some((st, sh, avail_opt)) = fresh_spot.filter(|(st, _, _)| *st >= 1.0) {
                let free_spot = if sh > 0.0 {
                    (st - sh).max(0.0)
                } else {
                    avail_opt.unwrap_or(st).max(0.0)
                };
                (st, free_spot.max(withdrawable))
            } else if cached_spot >= 1.0 {
                let free_spot = (cached_spot - margin_used).max(withdrawable).max(0.0);
                (cached_spot, free_spot)
            } else {
                (perp_eq, withdrawable)
            }
        } else {
            (perp_eq, withdrawable)
        };

        let configured_live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0);
        let cap_unset = cfg
            .get("risk_capital_cap_usdc")
            .map(|s| s.trim().is_empty())
            .unwrap_or(true);
        let eff_cap_eq = if cap_unset && configured_live_eq > 10.0 {
            let _ = self.db.set_config_value(
                "risk_capital_cap_usdc",
                &format!("{configured_live_eq:.2}"),
                Some("Preserved initial capital sizing cap from legacy live_exchange_equity_usdc"),
            );
            configured_live_eq
        } else {
            cfg.get_f64("risk_capital_cap_usdc", 0.0)
        };

        report.exchange_equity_usdc = if effective_wallet_eq >= 1.0 {
            round_dp(effective_wallet_eq, 2)
        } else {
            fallback_eq
        };
        if effective_wallet_eq >= 1.0 {
            let _ = self.db.set_config_value(
                "live_exchange_equity_usdc",
                &format!("{:.2}", report.exchange_equity_usdc),
                Some("Latest reconciled Hyperliquid trading equity in USDC (unified spot+perp total on unified accounts, else perp marginSummary.accountValue)"),
            );
        }
        let _ = self.db.set_config_value(
            "live_exchange_equity_synced_at_ms",
            &ts.to_string(),
            Some("Timestamp (ms) of latest successful Hyperliquid clearinghouseState equity sync"),
        );
        let _ = self.db.set_config_value(
            "live_exchange_margin_used_usdc",
            &format!("{:.2}", round_dp(margin_used.max(0.0), 2)),
            Some("Latest synced Hyperliquid perp marginSummary.totalMarginUsed in USDC"),
        );
        let _ = self.db.set_config_value(
            "live_exchange_withdrawable_usdc",
            &format!("{:.2}", round_dp(effective_withdrawable.max(0.0), 2)),
            Some("Latest synced Hyperliquid withdrawable / free margin USDC balance"),
        );

        // Identify currently open coins & signed szi on Hyperliquid before clearing orphan openOrders
        let mut open_coins: HashSet<String> = HashSet::new();
        let mut open_szi_by_coin: HashMap<String, f64> = HashMap::new();
        if let Some(asset_positions) = ch_state.get("assetPositions").and_then(Value::as_array) {
            for item in asset_positions {
                let pos = item.get("position").unwrap_or(item);
                let raw_coin = pos.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                let szi = parse_f(pos.get("szi"));
                if !raw_coin.is_empty() && szi.abs() > 1e-9 {
                    if let Ok((_, coin)) = normalize_hyperliquid_symbol(raw_coin) {
                        let coin_upper = coin.to_ascii_uppercase();
                        open_coins.insert(coin_upper.clone());
                        open_szi_by_coin.insert(coin_upper, szi);
                    }
                }
            }
        }

        // 2. Inspect resting orders via frontendOpenOrders (includes SL/TP trigger orders) and preserve active tracked orders
        let (open_orders, open_orders_fetch_ok) = match self
            .http
            .fetch_frontend_open_orders(&wallet, &cfg)
            .or_else(|_| self.http.fetch_open_orders(&wallet, &cfg))
        {
            Ok(v) => (v, true),
            Err(_) => (Vec::new(), false),
        };

        let mut resting_exchange_oids: HashSet<u64> = HashSet::new();
        let mut cloid_to_oid: HashMap<String, u64> = HashMap::new();
        for ord in &open_orders {
            if let Some(oid) = extract_order_oid(ord) {
                resting_exchange_oids.insert(oid);
                if let Some(cloid) = ord.get("cloid").and_then(Value::as_str) {
                    cloid_to_oid.insert(cloid.trim().to_ascii_lowercase(), oid);
                }
            }
        }

        let mut tracked_protected_oids: HashSet<u64> = HashSet::new();
        let mut active_chasing_partial_coins: HashSet<String> = HashSet::new();
        if !close_legacy_positions {
            if let Ok(local_held) = self.db.get_assets_held() {
                for h in local_held {
                    if !open_coins.contains(&h.coin.to_ascii_uppercase()) {
                        continue;
                    }
                    for raw_oid in [
                        h.protective_stop_oid.as_deref(),
                        h.take_profit_oid.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if let Some(oid) = parse_hl_oid(Some(raw_oid)).or_else(|| {
                            cloid_to_oid
                                .get(&raw_oid.trim().to_ascii_lowercase())
                                .copied()
                        }) {
                            tracked_protected_oids.insert(oid);
                        }
                    }
                }
            }
            if let Ok(local_ordered) = self.db.get_assets_ordered() {
                for o in local_ordered {
                    let coin_upper = o.coin.to_ascii_uppercase();
                    let entry_oid = resolve_working_entry_oid(&o, &cloid_to_oid);
                    let (lot_step_size, sz_decimals) = self
                        .db
                        .get_asset_by_id(o.asset_id)
                        .ok()
                        .flatten()
                        .map(|a| (a.lot_step_size, a.sz_decimals))
                        .unwrap_or((1e-6, 6));
                    let disposition = classify_working_order_for_reconcile(
                        ts.saturating_sub(o.created_at_ms),
                        &o.side,
                        o.requested_quantity,
                        lot_step_size,
                        sz_decimals,
                        entry_oid,
                        &resting_exchange_oids,
                        open_szi_by_coin.get(&coin_upper).copied(),
                    );
                    match disposition {
                        WorkingOrderReconcileDisposition::ProtectPending => {
                            if let Some(oid) = entry_oid {
                                tracked_protected_oids.insert(oid);
                            }
                        }
                        WorkingOrderReconcileDisposition::ProtectActivePartial => {
                            if let Some(oid) = entry_oid {
                                tracked_protected_oids.insert(oid);
                            }
                            active_chasing_partial_coins.insert(coin_upper);
                        }
                        WorkingOrderReconcileDisposition::AdoptOrExpire => {}
                    }
                }
            }
        }

        let (_protected_oids, orphan_orders) = find_orphan_orders_to_cancel(
            &open_orders,
            &open_coins,
            &tracked_protected_oids,
            close_legacy_positions,
        );

        let mut batch_cancels: Vec<CancelWire> = Vec::new();
        let mut cancelled_oid_candidates: Vec<u64> = Vec::new();
        for (coin, oid) in orphan_orders {
            report.stale_orders_found += 1;
            if let Ok(Some(asset)) = self.db.get_asset_by_symbol(&coin) {
                batch_cancels.push(CancelWire {
                    a: asset.asset_index,
                    o: oid,
                });
                cancelled_oid_candidates.push(oid);
            }
        }

        let mut cancelled_oid_set: HashSet<u64> = HashSet::new();
        if !private_key.is_empty() && !batch_cancels.is_empty() {
            for (chunk, oid_chunk) in batch_cancels
                .chunks(25)
                .zip(cancelled_oid_candidates.chunks(25))
            {
                if self
                    .http
                    .cancel_orders_batch(chunk.to_vec(), &private_key, Some(&self.db), &cfg)
                    .is_ok()
                {
                    report.stale_orders_cancelled += chunk.len();
                    for oid in oid_chunk {
                        resting_exchange_oids.remove(oid);
                        cancelled_oid_set.insert(*oid);
                    }
                }
            }
            report.details.push(format!(
                "Cancelled {}/{} resting orphan orders on Hyperliquid",
                report.stale_orders_cancelled, report.stale_orders_found
            ));
        }
        let remaining_open_orders: Vec<Value> = open_orders
            .into_iter()
            .filter(|ord| {
                extract_order_oid(ord)
                    .map(|id| !cancelled_oid_set.contains(&id))
                    .unwrap_or(true)
            })
            .collect();
        if open_orders_fetch_ok {
            let _ = self.db.set_config_value(
                "live_exchange_open_orders_count",
                &remaining_open_orders.len().to_string(),
                Some("Total resting Hyperliquid open orders from last reconcile (entry ALO + SL/TP triggers + passive closes)"),
            );
        }

        // Cancel local WORKING rows only if close_legacy_positions, or (not yet open on exchange AND (expired or no longer resting))
        // When `is_open_on_exch` is true, either keep chasing (when in `active_chasing_partial_coins`) or let
        // `sync_open_position_from_exchange_v2` promote it in-place preserving sleeve weights.
        if let Ok(local_ordered) = self.db.get_assets_ordered() {
            for o in local_ordered {
                let is_open_on_exch = open_coins.contains(&o.coin.to_ascii_uppercase());
                let is_expired = ts.saturating_sub(o.created_at_ms) >= WORKING_ENTRY_GRACE_MS;
                let is_resting = resolve_working_entry_oid(&o, &cloid_to_oid)
                    .is_some_and(|oid| resting_exchange_oids.contains(&oid));
                if close_legacy_positions
                    || (!is_open_on_exch && (is_expired || (open_orders_fetch_ok && !is_resting)))
                {
                    let _ = self
                        .db
                        .cancel_working_order(o.position_id, "EXCHANGE_RECONCILED_WORKING_CLEARED");
                }
            }
        }

        // Fetch userFills and userFunding once so both open position timestamps and closed trade FIFO/funding match accurately
        let user_fills_opt = match self.http.fetch_user_fills(&wallet, &cfg) {
            Ok(fills) => Some(fills),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Hyperliquid userFills fetch failed; skipping closed-trade/funding overwrite"
                );
                None
            }
        };
        let user_fills_fetch_ok = user_fills_opt.is_some();
        let mut parsed_fills_by_coin: BTreeMap<String, Vec<ParsedHlFill>> = BTreeMap::new();
        if let Some(ref raw_fills) = user_fills_opt {
            for fill in raw_fills {
                let raw_coin = fill
                    .get("coin")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim();
                if raw_coin.is_empty() {
                    continue;
                }
                let Ok((_, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                    continue;
                };
                let px = parse_f(fill.get("px"));
                let sz = parse_f(fill.get("sz"));
                if px <= 0.0 || sz <= 0.0 {
                    continue;
                }
                let fill_time = fill
                    .get("time")
                    .and_then(Value::as_i64)
                    .filter(|&t| t > 0)
                    .unwrap_or(ts);
                let tid = fill.get("tid").and_then(Value::as_u64).unwrap_or(0);
                let oid = fill
                    .get("oid")
                    .and_then(Value::as_u64)
                    .or_else(|| {
                        fill.get("oid")
                            .and_then(Value::as_str)
                            .and_then(|s| s.parse::<u64>().ok())
                    })
                    .filter(|&id| id > 0)
                    .or((tid > 0).then_some(tid))
                    .unwrap_or(fill_time as u64);
                parsed_fills_by_coin
                    .entry(coin.to_ascii_uppercase())
                    .or_default()
                    .push(ParsedHlFill {
                        coin,
                        side_str: fill
                            .get("side")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .trim()
                            .to_string(),
                        px,
                        sz,
                        fee: parse_f(fill.get("fee")),
                        closed_pnl: parse_f(fill.get("closedPnl")),
                        dir: fill
                            .get("dir")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .trim()
                            .to_string(),
                        time_ms: fill_time,
                        oid,
                        tid,
                        start_position: parse_f_opt(fill.get("startPosition")),
                    });
            }
            for fills in parsed_fills_by_coin.values_mut() {
                sort_fills_causally(fills);
            }
        }

        let local_held_before = self.db.get_assets_held().unwrap_or_default();
        let existing_open_by_coin: HashMap<String, HeldAssetView> = local_held_before
            .iter()
            .map(|h| (h.coin.to_ascii_uppercase(), h.clone()))
            .collect();
        let earliest_fill_ms = parsed_fills_by_coin
            .values()
            .filter_map(|v| v.first().map(|f| f.time_ms))
            .min();
        let earliest_local_open_ms = local_held_before
            .iter()
            .filter_map(|h| h.opened_at_ms.filter(|&t| t > 0))
            .min();
        let base_earliest_ms = match (earliest_fill_ms, earliest_local_open_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let has_untracked_prehistory = parsed_fills_by_coin.iter().any(|(coin_upper, fills)| {
            fills.first().is_some_and(|first_fill| {
                first_fill.start_position.is_some_and(|sp| sp.abs() > 1e-9)
                    && !existing_open_by_coin.get(coin_upper).is_some_and(|h| {
                        h.opened_at_ms
                            .is_some_and(|t| t > 0 && t < first_fill.time_ms)
                    })
            })
        }) || open_szi_by_coin.keys().any(|coin_upper| {
            !parsed_fills_by_coin.contains_key(coin_upper)
                && !existing_open_by_coin
                    .get(coin_upper)
                    .is_some_and(|h| h.opened_at_ms.is_some_and(|t| t > 0))
        });
        let earliest_funding_start_ms = if has_untracked_prehistory {
            base_earliest_ms.map(|t| t.saturating_sub(90 * 86_400_000))
        } else {
            base_earliest_ms
        };

        let (user_funding_raw, funding_fetch_ok) = match self.http.fetch_user_funding(
            &wallet,
            earliest_funding_start_ms,
            &cfg,
        ) {
            Ok(raw) => (raw, true),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Hyperliquid userFunding fetch failed; preserving existing reconciled funding without clobbering"
                );
                (Vec::new(), false)
            }
        };
        let mut funding_by_coin: HashMap<String, Vec<ParsedHlFunding>> = HashMap::new();
        for ev in &user_funding_raw {
            let f_time = parse_hl_i64_opt(ev.get("time")).unwrap_or(0);
            let Some(delta) = ev.get("delta") else {
                continue;
            };
            let d_type = delta.get("type").and_then(Value::as_str).unwrap_or("");
            if !d_type.eq_ignore_ascii_case("funding") {
                continue;
            }
            let raw_coin = delta
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if raw_coin.is_empty() {
                continue;
            }
            let Ok((_, norm_coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                continue;
            };
            let coin_upper = norm_coin.to_ascii_uppercase();
            let usdc = parse_f(delta.get("usdc"));
            if f_time <= 0 || usdc.abs() <= 1e-12 {
                continue;
            }
            let szi = parse_f(delta.get("szi"));
            let funding_rate = parse_f(delta.get("fundingRate"));
            let raw_hash = ev
                .get("hash")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty() && s.trim_start_matches("0x").chars().any(|c| c != '0'));
            let n_suffix = ev
                .get("n")
                .or_else(|| delta.get("nSamples"))
                .and_then(|v| {
                    v.as_i64()
                        .map(|n| n.to_string())
                        .or_else(|| v.as_str().map(|s| s.trim().to_string()))
                })
                .filter(|s| !s.is_empty());
            let mut dedup_key = match raw_hash {
                Some(h) => match n_suffix {
                    Some(n) => format!("{f_time}:{coin_upper}:{h}:{n}"),
                    None => format!("{f_time}:{coin_upper}:{h}"),
                },
                None => match n_suffix {
                    Some(n) => format!("{f_time}:{coin_upper}:{usdc:.6}:{szi:.6}:{n}"),
                    None => format!("{f_time}:{coin_upper}:{usdc:.6}:{szi:.6}"),
                },
            };
            if dedup_key.chars().count() > 128 {
                dedup_key = dedup_key.chars().take(128).collect();
            }
            funding_by_coin
                .entry(coin_upper)
                .or_default()
                .push(ParsedHlFunding {
                    coin: norm_coin,
                    time_ms: f_time,
                    usdc,
                    szi,
                    funding_rate,
                    dedup_key,
                });
        }
        for list in funding_by_coin.values_mut() {
            list.sort_by_key(|ev| ev.time_ms);
        }

        let ledger_sync_ok = user_fills_fetch_ok && funding_fetch_ok;
        let replay_out = if ledger_sync_ok {
            compute_fifo_closed_orders_and_funding(
                &parsed_fills_by_coin,
                &funding_by_coin,
                &open_szi_by_coin,
            )
        } else {
            FifoFundingReplayOutput::default()
        };

        let mut meta_cache: Option<HashMap<String, (u32, u32, f64)>> = None;
        let mut coin_asset_id_cache: HashMap<String, i64> = HashMap::new();

        // 3. Inspect live Hyperliquid `assetPositions` and synchronize 1:1 with MariaDB `positions` + SL/TP orders
        let mut exchange_open_asset_ids: HashSet<i64> = HashSet::new();
        let limits = RiskLimitsConfig::from_config_map(&cfg);
        let taker_fee_bps = limits.taker_fee_bps;
        let chaser = OrderChaser::new(
            self.db.clone(),
            self.http.clone(),
            AlertEngine::new(self.db.clone()),
        );

        if let Some(asset_positions) = ch_state.get("assetPositions").and_then(Value::as_array) {
            for item in asset_positions {
                let pos = item.get("position").unwrap_or(item);
                let raw_coin = pos.get("coin").and_then(Value::as_str).unwrap_or("").trim();
                let szi = parse_f(pos.get("szi"));
                if raw_coin.is_empty() || szi.abs() <= 1e-9 {
                    continue;
                }
                let entry_px = parse_f(pos.get("entryPx")).max(0.0001);
                let pos_val = parse_f(pos.get("positionValue"));
                let upnl = parse_f(pos.get("unrealizedPnl"));
                let since_open = parse_f(pos.pointer("/cumFunding/sinceOpen"));

                let Ok((symbol, coin)) = normalize_hyperliquid_symbol(raw_coin) else {
                    continue;
                };
                let coin_upper = coin.to_ascii_uppercase();

                let Some(asset) = self.resolve_or_upsert_asset(
                    &coin,
                    &symbol,
                    entry_px,
                    true,
                    true,
                    true,
                    &mut meta_cache,
                    &cfg,
                )?
                else {
                    continue;
                };
                coin_asset_id_cache.insert(coin_upper.clone(), asset.id);

                report.legacy_positions_found += 1;
                let side = if szi > 0.0 {
                    OrderSide::Long
                } else {
                    OrderSide::Short
                };
                let funding_accrued_usdc = if ledger_sync_ok {
                    let partial_booked = replay_out
                        .open_epoch_booked_funding_by_coin
                        .get(&coin_upper)
                        .copied()
                        .unwrap_or(0.0);
                    round_dp((-since_open) - partial_booked, 4)
                } else if let Some(existing) = existing_open_by_coin.get(&coin_upper) {
                    if existing.side.eq_ignore_ascii_case(side.as_str())
                        && (existing.funding_accrued_usdc.abs() > 1e-9
                            || parsed_fills_by_coin
                                .get(&coin_upper)
                                .is_some_and(|fs| fs.iter().any(fill_is_closing_dir)))
                    {
                        existing.funding_accrued_usdc
                    } else {
                        round_dp(-since_open, 4)
                    }
                } else {
                    round_dp(-since_open, 4)
                };
                let qty = szi.abs();
                let mark_px = if pos_val > 0.0 && qty > 1e-9 {
                    round_dp(pos_val / qty, 6)
                } else if asset.mark_price > 0.0 {
                    asset.mark_price
                } else {
                    entry_px
                };
                let live_mid = if asset.mid_price > 0.0 {
                    asset.mid_price
                } else {
                    mark_px
                };
                let _ = self.db.update_asset_live_quote(asset.id, mark_px, live_mid);

                let mut closed_on_exchange = false;
                let mut remaining_qty = qty;
                if close_legacy_positions && !private_key.is_empty() {
                    let close_is_buy = !side.is_buy();
                    let l2_bbo = self.http.fetch_l2_book(&coin, &cfg).ok().flatten();
                    let bbo_anchor = l2_bbo
                        .map(|(bid, ask)| if close_is_buy { ask } else { bid })
                        .filter(|&px| px > 0.0)
                        .unwrap_or(mark_px);
                    let band_lo = (mark_px * (1.0 - 0.038)).max(asset.tick_size.max(1e-8));
                    let band_hi = mark_px * (1.0 + 0.038);

                    for (attempt_idx, (base_px, slip_pct)) in [
                        (bbo_anchor, 0.0),
                        (bbo_anchor, 0.01),
                        (bbo_anchor, 0.025),
                        (mark_px, 0.038),
                    ]
                    .into_iter()
                    .enumerate()
                    {
                        if remaining_qty <= asset.lot_step_size * 0.5 || remaining_qty <= 1e-9 {
                            break;
                        }
                        let raw_limit_px = if close_is_buy {
                            base_px * (1.0 + slip_pct)
                        } else {
                            base_px * (1.0 - slip_pct)
                        };
                        let limit_px = raw_limit_px.clamp(band_lo, band_hi);
                        let sz_str = format_hl_wire_size_floor(
                            remaining_qty,
                            asset.lot_step_size,
                            asset.sz_decimals,
                        );
                        if sz_str == "0" {
                            break;
                        }
                        let wire = OrderWire {
                            a: asset.asset_index,
                            b: close_is_buy,
                            p: format_hl_wire_price(limit_px, asset.tick_size, asset.sz_decimals),
                            s: sz_str,
                            r: true,
                            t: OrderTypeWire {
                                limit: Some(LimitOrderType {
                                    tif: "Ioc".to_string(),
                                }),
                                trigger: None,
                            },
                            c: Some(derive_hl_cloid_hex(&format!(
                                "cutover-close-{}-{}-{attempt_idx}-{}",
                                coin.to_ascii_lowercase(),
                                slip_pct,
                                ts
                            ))),
                        };
                        if let Ok(ack) = self.http.submit_order_wire(
                            wire,
                            remaining_qty,
                            base_px,
                            taker_fee_bps,
                            &private_key,
                            Some(&self.db),
                            &cfg,
                        ) {
                            if ack.accepted && ack.status == "FILLED" && ack.filled_quantity > 0.0 {
                                remaining_qty = (remaining_qty - ack.filled_quantity).max(0.0);
                                if remaining_qty <= asset.lot_step_size * 0.5 {
                                    closed_on_exchange = true;
                                    report.legacy_positions_closed += 1;
                                    let _ = self.db.insert_asset_cooldown(
                                        asset.id,
                                        "MANUAL",
                                        "Cutover position flatten cooldown",
                                        900,
                                    );
                                    report.details.push(format!(
                                        "Closed position {} ({} {:.4} @ ${:.4})",
                                        asset.symbol,
                                        side.as_str(),
                                        qty,
                                        ack.avg_fill_price
                                    ));
                                    break;
                                }
                            }
                        }
                    }
                }

                let qty = remaining_qty.max(1e-9);
                let coin_upper = coin.to_ascii_uppercase();

                if !closed_on_exchange && active_chasing_partial_coins.contains(&coin_upper) {
                    let _ = self.db.set_asset_priority_watch(asset.id, true);
                    exchange_open_asset_ids.insert(asset.id);
                    report.details.push(format!(
                        "Preserved active chasing WORKING partial fill on {} ({} {:.4} on Hyperliquid)",
                        asset.symbol,
                        side.as_str(),
                        qty
                    ));
                    continue;
                }

                if !closed_on_exchange {
                    let tick = asset.tick_size.max(1e-8);
                    let bars_1h = self
                        .db
                        .get_ohlcv_bars(asset.id, "1h", 20)
                        .unwrap_or_default();
                    let atr_opt = (!bars_1h.is_empty()).then(|| {
                        compute_atr_from_bars(
                            &bars_1h,
                            14,
                            mark_px,
                            limits.protective_stop_default_pct,
                        )
                    });
                    let entry_stop =
                        compute_initial_protective_stop(side, entry_px, atr_opt, tick, &limits);
                    let stop_px = if match side {
                        OrderSide::Long => entry_stop < mark_px - tick * 0.5,
                        OrderSide::Short => entry_stop > mark_px + tick * 0.5,
                    } {
                        entry_stop
                    } else {
                        compute_initial_protective_stop(side, mark_px, atr_opt, tick, &limits)
                    };
                    let entry_tp =
                        compute_initial_take_profit(side, entry_px, atr_opt, tick, &limits);
                    let tp_px = if match side {
                        OrderSide::Long => entry_tp > mark_px + tick * 0.5,
                        OrderSide::Short => entry_tp < mark_px - tick * 0.5,
                    } {
                        entry_tp
                    } else {
                        compute_initial_take_profit(side, mark_px, atr_opt, tick, &limits)
                    };

                    let committed_usdc = if pos_val > 0.0 {
                        round_dp(pos_val, 4)
                    } else {
                        round_dp(mark_px * qty, 4)
                    };
                    let eff_basis_eq = if eff_cap_eq >= 10.0 {
                        report.exchange_equity_usdc.min(eff_cap_eq)
                    } else {
                        report.exchange_equity_usdc
                    };
                    let weight =
                        round_dp((committed_usdc / eff_basis_eq.max(1.0)) * side.sign(), 4);
                    let opened_at_ms_opt = parsed_fills_by_coin
                        .get(&coin.to_ascii_uppercase())
                        .and_then(|fills| derive_open_position_opened_at_ms(fills, szi));

                    let (pid, _newly_adopted) = self.db.sync_open_position_from_exchange_v2(
                        asset.id,
                        &coin,
                        side,
                        entry_px,
                        mark_px,
                        qty,
                        committed_usdc,
                        upnl,
                        weight,
                        stop_px,
                        tp_px,
                        funding_accrued_usdc,
                        opened_at_ms_opt,
                    )?;
                    if let Ok(Some(synced_pos)) = self.db.get_position_by_id(pid) {
                        let _ = chaser.ensure_exchange_tpsl_orders_with_orders(
                            pid,
                            &asset,
                            side,
                            qty,
                            mark_px,
                            synced_pos.protective_stop_px,
                            synced_pos.take_profit_px,
                            synced_pos.protective_stop_oid.as_deref(),
                            synced_pos.take_profit_oid.as_deref(),
                            false,
                            false,
                            open_orders_fetch_ok.then_some(remaining_open_orders.as_slice()),
                            &cfg,
                        );
                    }
                    let _ = self.db.set_asset_priority_watch(asset.id, true);
                    exchange_open_asset_ids.insert(asset.id);
                    report.legacy_positions_adopted += 1;
                    report.details.push(format!(
                        "Synced 1:1 exchange position {} ({} {:.4} @ entry=${:.4}, mark=${:.4}, notional=${:.2}, uPnL=${:+.2})",
                        asset.symbol,
                        side.as_str(),
                        qty,
                        entry_px,
                        mark_px,
                        committed_usdc,
                        upnl
                    ));
                }
            }
        }

        // 4. Close any local `OPEN` rows in MariaDB that are flat on Hyperliquid
        if let Ok(local_held) = self.db.get_assets_held() {
            for h in local_held {
                if !exchange_open_asset_ids.contains(&h.asset_id) {
                    let _ = self.db.close_position(
                        h.position_id,
                        h.mark_price,
                        0.0,
                        "EXCHANGE_RECONCILED_FLAT",
                    );
                    report.local_orphans_closed += 1;
                    report.details.push(format!(
                        "Closed local position #{} on {} (flat on Hyperliquid)",
                        h.position_id, h.symbol
                    ));
                }
            }
        }

        // 5. Reconcile historical `CLOSED` trades 1:1 & persist `funding_events` when both userFills + userFunding succeeded
        if ledger_sync_ok {
            if !replay_out.closed_orders.is_empty() {
                let mut closed_inputs = Vec::with_capacity(replay_out.closed_orders.len());
                for (oid, agg) in replay_out.closed_orders {
                    if agg.total_sz <= 1e-9 {
                        continue;
                    }
                    let window_start = agg.open_time_ms.unwrap_or(agg.min_time_ms);
                    let exit_px = agg.notional_sum / agg.total_sz;
                    let side = agg.side.unwrap_or(OrderSide::Long);
                    let entry_px = match side {
                        OrderSide::Long => {
                            (exit_px - (agg.gross_closed_pnl / agg.total_sz)).max(1e-8)
                        }
                        OrderSide::Short => {
                            (exit_px + (agg.gross_closed_pnl / agg.total_sz)).max(1e-8)
                        }
                    };
                    let total_fees = agg.close_fees_usdc + agg.open_fees_usdc;
                    let net_rpnl = round_dp(
                        agg.gross_closed_pnl - total_fees + agg.funding_accrued_usdc,
                        4,
                    );

                    let symbol = format!("{}-USDC", agg.coin);
                    let Some(asset) = self.resolve_or_upsert_asset(
                        &agg.coin,
                        &symbol,
                        exit_px,
                        false,
                        true,
                        false,
                        &mut meta_cache,
                        &cfg,
                    )?
                    else {
                        continue;
                    };
                    coin_asset_id_cache.insert(agg.coin.to_ascii_uppercase(), asset.id);

                    closed_inputs.push(ReconciledClosedTradeInput {
                        asset_id: asset.id,
                        client_order_id: format!(
                            "hl-closed-{}-{oid}",
                            agg.coin.to_ascii_lowercase()
                        ),
                        exchange_order_id: format!("hl-{oid}"),
                        side,
                        entry_price: entry_px,
                        exit_price: exit_px,
                        filled_quantity: agg.total_sz,
                        realized_pnl_usdc: net_rpnl,
                        fees_paid_usdc: round_dp(total_fees, 4),
                        funding_accrued_usdc: round_dp(agg.funding_accrued_usdc, 4),
                        created_at_ms: window_start,
                        closed_at_ms: agg.max_time_ms,
                        exit_reason: "HL_USER_FILL_RECONCILED".to_string(),
                    });
                }

                if !closed_inputs.is_empty() {
                    if let Ok(reconciled_closed_count) = self
                        .db
                        .reconcile_closed_positions_from_exchange_fills(&closed_inputs)
                    {
                        report.details.push(format!(
                            "Reconciled {reconciled_closed_count} historical closed trades from Hyperliquid userFills"
                        ));
                    }
                }
            }

            if !replay_out.funding_events_with_remaining.is_empty() {
                let mut funding_inputs =
                    Vec::with_capacity(replay_out.funding_events_with_remaining.len());
                for (ev, remaining_usdc) in replay_out.funding_events_with_remaining {
                    let coin_upper = ev.coin.to_ascii_uppercase();
                    let asset_id = if let Some(&aid) = coin_asset_id_cache.get(&coin_upper) {
                        aid
                    } else {
                        let symbol = format!("{}-USDC", ev.coin);
                        let Some(asset) = self.resolve_or_upsert_asset(
                            &ev.coin,
                            &symbol,
                            1.0,
                            false,
                            true,
                            false,
                            &mut meta_cache,
                            &cfg,
                        )?
                        else {
                            continue;
                        };
                        coin_asset_id_cache.insert(coin_upper, asset.id);
                        asset.id
                    };
                    funding_inputs.push(FundingEventUpsertInput {
                        asset_id,
                        coin: ev.coin,
                        event_time_ms: ev.time_ms,
                        usdc: ev.usdc,
                        remaining_usdc,
                        position_szi: ev.szi,
                        funding_rate: ev.funding_rate,
                        dedup_key: ev.dedup_key,
                    });
                }
                if !funding_inputs.is_empty() {
                    if let Err(e) = self.db.upsert_funding_events_batch(&funding_inputs) {
                        tracing::warn!(
                            error = %e,
                            count = funding_inputs.len(),
                            "Failed persisting reconciled funding_events batch"
                        );
                    }
                }
            }
        } else {
            report.details.push(
                "Skipped closed-trade and funding_events overwrite because userFills/userFunding fetch failed (preserving cached funding)"
                    .to_string(),
            );
        }

        // 6. Throttled Hyperliquid portfolio PnL sync + vault info sync (at most once per 60s)
        let cfg_post_reconcile = self.db.load_config_or_default();
        if let Some(port_cache) = self.sync_portfolio_pnl_if_due(&cfg_post_reconcile, ts) {
            if let Some((_, day_last)) = port_cache.perp_day.pnl_history.last() {
                report.details.push(format!(
                    "Synced Hyperliquid portfolio PnL histories (perpDay={day_last:.2})"
                ));
            }
        }
        if let Some(vault_snap) =
            self.sync_vault_info_if_due(&cfg_post_reconcile, ts, Some(report.exchange_equity_usdc))
        {
            report.details.push(format!(
                "Synced {} Hyperliquid vault(s) (vault_equity=${:.2}, segregated from perp margin)",
                vault_snap.vault_count, vault_snap.total_vault_equity_usdc
            ));
        }

        let _ = self.db.log_telemetry(
            "INFO",
            "execution.reconcile",
            &format!(
                "Hyperliquid 1:1 reconciliation complete: mode={}, equity=${:.2}, orders_cancelled={}/{}, open_positions_synced={}, local_flat_closed={}",
                report.execution_mode,
                report.exchange_equity_usdc,
                report.stale_orders_cancelled,
                report.stale_orders_found,
                report.legacy_positions_adopted,
                report.local_orphans_closed
            ),
            Some(json!(report)),
        );

        Ok(report)
    }

    /// Syncs Hyperliquid `userAbstraction` account mode at most once per [`ABSTRACTION_SYNC_MIN_INTERVAL_MS`] (5 min).
    /// Persists `live_exchange_account_mode` only when a non-empty string mode is returned.
    pub fn sync_account_mode_if_due(&self, cfg: &ConfigMap, now_ms_val: i64) -> Option<String> {
        self.sync_account_mode_with_interval(cfg, now_ms_val, ABSTRACTION_SYNC_MIN_INTERVAL_MS)
    }

    /// Interval-parameterized `userAbstraction` sync helper used by [`Self::sync_account_mode_if_due`] and tests.
    pub fn sync_account_mode_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<String> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let wallet = cfg.get_str("wallet_address", "");
        let wallet_trimmed = wallet.trim();
        if wallet_trimmed.is_empty() {
            return None;
        }
        let key: SpotSyncThrottleKey = (
            self.db.db_name().to_string(),
            wallet_trimmed.to_ascii_lowercase(),
        );
        let eff_min_interval = min_interval_ms.max(0);
        if eff_min_interval > 0 {
            let map = LAST_ABSTRACTION_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
            let mut guard = map.lock();
            if let Some(&last_attempt) = guard.get(&key) {
                if now_ms_val >= last_attempt
                    && now_ms_val.saturating_sub(last_attempt) < eff_min_interval
                {
                    return None;
                }
            }
            if guard.len() > 256 {
                guard.clear();
            }
            guard.insert(key, now_ms_val);
        }

        let mode_opt = match self.http.fetch_user_abstraction(wallet_trimmed, cfg) {
            Ok(opt) => opt,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Hyperliquid userAbstraction fetch failed; keeping cached account mode"
                );
                return None;
            }
        };
        let mode = mode_opt?;
        if let Err(e) = self.db.set_config_value(
            "live_exchange_account_mode",
            &mode,
            Some("Latest synced Hyperliquid userAbstraction mode (default, unifiedAccount, portfolioMargin)"),
        ) {
            tracing::warn!(error = %e, "Failed persisting live_exchange_account_mode");
            return None;
        }
        Some(mode)
    }

    /// Syncs Hyperliquid spot USDC balance at most once per [`VAULT_SYNC_MIN_INTERVAL_MS`] (60s).
    /// Persists `live_exchange_spot_usdc` and `live_exchange_spot_synced_at_ms` without modifying `live_exchange_equity_usdc`.
    /// On fetch errors/timeouts/429s or payloads lacking `"balances"`, logs WARN/DEBUG and keeps the cached value.
    pub fn sync_spot_usdc_if_due(&self, cfg: &ConfigMap, now_ms_val: i64) -> Option<f64> {
        self.sync_spot_usdc_with_interval(cfg, now_ms_val, VAULT_SYNC_MIN_INTERVAL_MS)
    }

    /// Interval-parameterized spot USDC sync helper used by [`Self::sync_spot_usdc_if_due`] and tests.
    pub fn sync_spot_usdc_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<f64> {
        self.sync_spot_usdc_details_with_interval(cfg, now_ms_val, min_interval_ms)
            .map(|(total, _, _)| total)
    }

    fn sync_spot_usdc_details_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<(f64, f64, Option<f64>)> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let wallet = cfg.get_str("wallet_address", "");
        let wallet_trimmed = wallet.trim();
        if wallet_trimmed.is_empty() {
            return None;
        }
        let key: SpotSyncThrottleKey = (
            self.db.db_name().to_string(),
            wallet_trimmed.to_ascii_lowercase(),
        );
        let eff_min_interval = min_interval_ms.max(0);
        if eff_min_interval > 0 {
            let map = LAST_SPOT_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
            let mut guard = map.lock();
            if let Some(&last_attempt) = guard.get(&key) {
                if now_ms_val >= last_attempt
                    && now_ms_val.saturating_sub(last_attempt) < eff_min_interval
                {
                    tracing::debug!(
                        elapsed_ms = now_ms_val.saturating_sub(last_attempt),
                        min_interval_ms = eff_min_interval,
                        "Skipping Hyperliquid spot USDC sync (in-memory throttle active)"
                    );
                    return None;
                }
            }
            let prev_sync_ms = cfg.get_i64("live_exchange_spot_synced_at_ms", 0);
            if prev_sync_ms > 0
                && now_ms_val >= prev_sync_ms
                && now_ms_val.saturating_sub(prev_sync_ms) < eff_min_interval
            {
                guard.insert(key, prev_sync_ms);
                tracing::debug!(
                    elapsed_ms = now_ms_val.saturating_sub(prev_sync_ms),
                    min_interval_ms = eff_min_interval,
                    "Skipping Hyperliquid spot USDC sync (persisted throttle active)"
                );
                return None;
            }
            if guard.len() > 256 {
                guard.clear();
            }
            guard.insert(key, now_ms_val);
        }

        let spot_state = match self
            .http
            .fetch_spot_clearinghouse_state(wallet_trimmed, cfg)
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Hyperliquid spotClearinghouseState fetch failed; keeping last known cached spot USDC"
                );
                return None;
            }
        };

        let Some((raw_usdc, hold_usdc, avail_after_maint)) = parse_spot_usdc_details(&spot_state)
        else {
            tracing::debug!(
                "Hyperliquid spotClearinghouseState payload missing 'balances' array; keeping cached spot USDC"
            );
            return None;
        };

        let spot_usdc = round_dp(raw_usdc.max(0.0), 2);
        if let Err(e) = self.db.set_config_value(
            "live_exchange_spot_usdc",
            &format!("{spot_usdc:.2}"),
            Some("Latest synced Hyperliquid spot USDC balance (informational; segregated from perp margin)"),
        ) {
            tracing::warn!(error = %e, "Failed persisting live_exchange_spot_usdc");
            return None;
        }
        let _ = self.db.set_config_value(
            "live_exchange_spot_synced_at_ms",
            &now_ms_val.to_string(),
            Some("Timestamp (ms) of latest Hyperliquid spotClearinghouseState sync"),
        );

        Some((
            spot_usdc,
            round_dp(hold_usdc.max(0.0), 2),
            avail_after_maint,
        ))
    }

    /// Syncs Hyperliquid `portfolio` PnL histories (`perpDay`, `perpWeek`, `perpMonth`, `perpAllTime`)
    /// at most once per [`VAULT_SYNC_MIN_INTERVAL_MS`] (60s), bypassing throttle once when
    /// `risk_capital_cap_updated_at_ms` advances past the cached snapshot timestamp.
    pub fn sync_portfolio_pnl_if_due(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
    ) -> Option<CachedHlPortfolioPnl> {
        self.sync_portfolio_pnl_with_interval(cfg, now_ms_val, VAULT_SYNC_MIN_INTERVAL_MS)
    }

    /// Interval-parameterized `portfolio` PnL sync helper used by [`Self::sync_portfolio_pnl_if_due`] and tests.
    pub fn sync_portfolio_pnl_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<CachedHlPortfolioPnl> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let wallet = cfg.get_str("wallet_address", "");
        let wallet_trimmed = wallet.trim();
        if wallet_trimmed.is_empty() {
            return None;
        }
        let key: SpotSyncThrottleKey = (
            self.db.db_name().to_string(),
            wallet_trimmed.to_ascii_lowercase(),
        );
        let eff_min_interval = min_interval_ms.max(0);
        let cached_snapshot: Option<CachedHlPortfolioPnl> = {
            let raw = cfg.get_str("live_exchange_portfolio_pnl_json", "");
            (!raw.is_empty())
                .then(|| serde_json::from_str::<CachedHlPortfolioPnl>(&raw).ok())
                .flatten()
        };
        let cap_updated_at_ms = cfg.get_i64("risk_capital_cap_updated_at_ms", 0).max(0);

        if eff_min_interval > 0 {
            let map = LAST_PORTFOLIO_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
            let mut guard = map.lock();
            if let Some(&last_attempt) = guard.get(&key) {
                let cap_advanced = cap_updated_at_ms > 0 && last_attempt < cap_updated_at_ms;
                if !cap_advanced
                    && now_ms_val >= last_attempt
                    && now_ms_val.saturating_sub(last_attempt) < eff_min_interval
                {
                    return None;
                }
            }
            if let Some(ref prev) = cached_snapshot {
                let cap_advanced = cap_updated_at_ms > 0 && prev.synced_at_ms < cap_updated_at_ms;
                if !cap_advanced
                    && prev.synced_at_ms > 0
                    && now_ms_val >= prev.synced_at_ms
                    && now_ms_val.saturating_sub(prev.synced_at_ms) < eff_min_interval
                {
                    guard.insert(key, prev.synced_at_ms);
                    return None;
                }
            }
            if guard.len() > 256 {
                guard.clear();
            }
            guard.insert(key, now_ms_val);
        }

        let raw_portfolio = match self.http.fetch_user_portfolio(wallet_trimmed, cfg) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Hyperliquid portfolio PnL fetch failed; keeping cached portfolio snapshot"
                );
                return None;
            }
        };

        let Some(parsed) = parse_hl_portfolio_pnl(&raw_portfolio, now_ms_val) else {
            tracing::debug!(
                "Hyperliquid portfolio response missing non-empty perp PnL histories; keeping cache"
            );
            return None;
        };

        match serde_json::to_string(&parsed) {
            Ok(json_str) => {
                if let Err(e) = self.db.set_config_value(
                    "live_exchange_portfolio_pnl_json",
                    &json_str,
                    Some("Cached Hyperliquid portfolio perpDay/perpWeek/perpMonth/perpAllTime PnL snapshot JSON synced by reconciler (read-only)"),
                ) {
                    tracing::warn!(
                        error = %e,
                        "Failed persisting live_exchange_portfolio_pnl_json; keeping prior cache"
                    );
                    return None;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed serializing CachedHlPortfolioPnl");
                return None;
            }
        }

        Some(parsed)
    }

    /// Syncs Hyperliquid vault equities and vault details at most once per [`VAULT_SYNC_MIN_INTERVAL_MS`] (60s).
    /// Persists the snapshot to `system_config` key `live_vault_info_json`.
    /// On fetch errors/timeouts/429s, logs WARN and preserves the last known cached snapshot without failing reconciliation.
    /// Vault funds are strictly segregated from perp margin and never modify `live_exchange_equity_usdc`.
    pub fn sync_vault_info_if_due(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        perp_equity_override: Option<f64>,
    ) -> Option<VaultInfoPayload> {
        self.sync_vault_info_with_interval(
            cfg,
            now_ms_val,
            VAULT_SYNC_MIN_INTERVAL_MS,
            perp_equity_override,
        )
    }

    /// Interval-parameterized vault sync helper used by [`Self::sync_vault_info_if_due`] and tests.
    pub fn sync_vault_info_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
        perp_equity_override: Option<f64>,
    ) -> Option<VaultInfoPayload> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let wallet = cfg.get_str("wallet_address", "");
        let wallet_trimmed = wallet.trim();
        let configured_vault = cfg.get_str("hyperliquid_vault_address", "");
        let configured_vault_trimmed = configured_vault.trim();
        if wallet_trimmed.is_empty() && configured_vault_trimmed.is_empty() {
            return None;
        }
        let key: VaultSyncThrottleKey = (
            self.db.db_name().to_string(),
            wallet_trimmed.to_ascii_lowercase(),
            configured_vault_trimmed.to_ascii_lowercase(),
        );
        let eff_min_interval = min_interval_ms.max(0);

        let cached_snapshot: Option<VaultInfoPayload> = {
            let raw = cfg.get_str("live_vault_info_json", "");
            (!raw.is_empty())
                .then(|| serde_json::from_str::<VaultInfoPayload>(&raw).ok())
                .flatten()
        };

        if eff_min_interval > 0 {
            let map = LAST_VAULT_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
            let mut guard = map.lock();
            if let Some(&last_attempt) = guard.get(&key) {
                if now_ms_val >= last_attempt
                    && now_ms_val.saturating_sub(last_attempt) < eff_min_interval
                {
                    tracing::debug!(
                        elapsed_ms = now_ms_val.saturating_sub(last_attempt),
                        min_interval_ms = eff_min_interval,
                        "Skipping Hyperliquid vault sync (in-memory 60s throttle active)"
                    );
                    return None;
                }
            }
            if let Some(ref prev) = cached_snapshot {
                let same_cfg_vault = prev
                    .configured_vault_address
                    .trim()
                    .eq_ignore_ascii_case(configured_vault_trimmed);
                if same_cfg_vault
                    && prev.synced_at_ms > 0
                    && now_ms_val >= prev.synced_at_ms
                    && now_ms_val.saturating_sub(prev.synced_at_ms) < eff_min_interval
                {
                    guard.insert(key, prev.synced_at_ms);
                    tracing::debug!(
                        elapsed_ms = now_ms_val.saturating_sub(prev.synced_at_ms),
                        min_interval_ms = eff_min_interval,
                        "Skipping Hyperliquid vault sync (persisted 60s throttle active)"
                    );
                    return None;
                }
            }
            if guard.len() > 256 {
                guard.clear();
            }
            // Record attempt BEFORE issuing HTTP requests so 429/timeout failures still honor the 60s cooldown.
            guard.insert(key, now_ms_val);
        }

        let raw_equities = if wallet_trimmed.is_empty() {
            Vec::new()
        } else {
            match self.http.fetch_user_vault_equities(wallet_trimmed, cfg) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Hyperliquid userVaultEquities fetch failed; keeping last known cached vault info"
                    );
                    return None;
                }
            }
        };

        let mut equity_fallback_by_addr: HashMap<String, (String, f64, i64)> = HashMap::new();
        let mut ordered_addrs: Vec<String> = Vec::new();
        for item in &raw_equities {
            let raw_addr = item
                .get("vaultAddress")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            if raw_addr.is_empty() {
                continue;
            }
            let norm = raw_addr.to_ascii_lowercase();
            let eq = parse_hl_f64_opt(item.get("equity")).unwrap_or(0.0).max(0.0);
            let locked_until = parse_hl_i64_opt(item.get("lockedUntilTimestamp"))
                .unwrap_or(0)
                .max(0);
            if !equity_fallback_by_addr.contains_key(&norm) {
                ordered_addrs.push(norm.clone());
            }
            equity_fallback_by_addr.insert(norm, (raw_addr.to_string(), eq, locked_until));
        }

        if !configured_vault_trimmed.is_empty() {
            let norm_cfg = configured_vault_trimmed.to_ascii_lowercase();
            if !equity_fallback_by_addr.contains_key(&norm_cfg) {
                ordered_addrs.push(norm_cfg);
            }
        }

        let mut vaults: Vec<VaultPositionInfo> = Vec::with_capacity(ordered_addrs.len());
        let mut raw_total_vault_eq = 0.0_f64;

        for norm_addr in ordered_addrs.into_iter().take(MAX_VAULTS_TO_FETCH) {
            let fallback = equity_fallback_by_addr.get(&norm_addr);
            let query_addr = fallback
                .map(|(orig, _, _)| orig.as_str())
                .unwrap_or(configured_vault_trimmed);
            let details_opt = match self
                .http
                .fetch_vault_details(query_addr, wallet_trimmed, cfg)
            {
                Ok(opt) => opt,
                Err(e) => {
                    tracing::warn!(
                        vault_address = %mask_identifier(query_addr),
                        error = %e,
                        "Hyperliquid vaultDetails fetch failed; keeping last known cached vault info"
                    );
                    return None;
                }
            };
            let Some(details) = details_opt else {
                if let Some((orig_addr, eq, locked_until)) = fallback {
                    if *eq > 0.0 {
                        raw_total_vault_eq += *eq;
                        vaults.push(VaultPositionInfo {
                            vault_address: orig_addr.clone(),
                            name: mask_identifier(orig_addr),
                            leader: String::new(),
                            equity_usdc: round_dp(*eq, 2),
                            pnl_usdc: 0.0,
                            all_time_pnl_usdc: 0.0,
                            pnl_pct: 0.0,
                            all_time_pnl_pct: 0.0,
                            apr_pct: 0.0,
                            days_following: 0,
                            locked_until_ms: *locked_until,
                            is_locked: *locked_until > now_ms_val,
                            is_closed: false,
                            ..Default::default()
                        });
                    }
                }
                continue;
            };

            let (info, raw_eq) = parse_vault_position_info_with_raw_equity(
                query_addr,
                &details,
                fallback.map(|(_, eq, lock)| (*eq, *lock)),
                now_ms_val,
            );
            raw_total_vault_eq += raw_eq;
            vaults.push(info);
        }

        vaults.sort_by(|a, b| {
            b.equity_usdc
                .total_cmp(&a.equity_usdc)
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.vault_address.cmp(&b.vault_address))
        });

        let total_vault_equity_usdc = round_dp(raw_total_vault_eq, 2);
        let perp_eq = perp_equity_override
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or_else(|| cfg.get_f64("live_exchange_equity_usdc", 0.0).max(0.0));
        let is_unified =
            is_unified_account_mode(&cfg.get_str("live_exchange_account_mode", "default"));
        let spot_eq = {
            let from_cfg = cfg.get_f64("live_exchange_spot_usdc", 0.0).max(0.0);
            if from_cfg > 0.0 || cfg.get_i64("live_exchange_spot_synced_at_ms", 0) > 0 {
                from_cfg
            } else {
                self.db
                    .load_config_or_default()
                    .get_f64("live_exchange_spot_usdc", 0.0)
                    .max(0.0)
            }
        };
        let combined_portfolio_equity_usdc = if is_unified {
            round_dp(perp_eq + total_vault_equity_usdc, 2)
        } else {
            round_dp(perp_eq + spot_eq + total_vault_equity_usdc, 2)
        };

        let snapshot = VaultInfoPayload {
            configured_vault_address: configured_vault_trimmed.to_string(),
            total_vault_equity_usdc,
            combined_portfolio_equity_usdc,
            vault_count: vaults.len(),
            synced_at_ms: now_ms_val,
            vaults,
        };

        match serde_json::to_string(&snapshot) {
            Ok(json_str) => {
                if let Err(e) = self.db.set_config_value(
                    "live_vault_info_json",
                    &json_str,
                    Some("Cached Hyperliquid vault portfolio snapshot JSON synced by reconciler (read-only; segregated from perp margin)"),
                ) {
                    tracing::warn!(
                        error = %e,
                        "Failed persisting live_vault_info_json to system_config; keeping prior snapshot"
                    );
                    return None;
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed serializing VaultInfoPayload");
                return None;
            }
        }

        Some(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    fn make_fill(
        coin: &str,
        side_str: &str,
        px: f64,
        sz: f64,
        fee: f64,
        closed_pnl: f64,
        dir: &str,
        time_ms: i64,
        oid: u64,
        tid: u64,
        start_position: Option<f64>,
    ) -> ParsedHlFill {
        ParsedHlFill {
            coin: coin.to_string(),
            side_str: side_str.to_string(),
            px,
            sz,
            fee,
            closed_pnl,
            dir: dir.to_string(),
            time_ms,
            oid,
            tid,
            start_position,
        }
    }

    #[test]
    fn test_sort_fills_causally_chip_same_ms_non_monotonic_tid() {
        // Variant A: Same-ms accumulation at 1795023372707 where startPosition=812.0 has LOWER tid than startPosition=0.0
        let same_ms = 1_795_023_372_707i64;
        let mut fills_a = vec![
            make_fill(
                "CHIP",
                "B",
                0.0278,
                2773.0,
                0.03,
                0.0,
                "Open Long",
                same_ms,
                9001,
                100, // lower tid, but causally second!
                Some(812.0),
            ),
            make_fill(
                "CHIP",
                "B",
                0.0277,
                812.0,
                0.01,
                0.0,
                "Open Long",
                same_ms,
                9001,
                200, // higher tid, causally first!
                Some(0.0),
            ),
            make_fill(
                "CHIP",
                "A",
                0.0285,
                3585.0,
                0.05,
                2.80,
                "Close Long",
                same_ms + 60_000,
                9002,
                300,
                Some(3585.0),
            ),
        ];
        sort_fills_causally(&mut fills_a);
        assert_eq!(fills_a[0].tid, 200);
        assert_eq!(fills_a[1].tid, 100);
        assert_eq!(fills_a[2].tid, 300);

        let mut map_a = BTreeMap::new();
        map_a.insert("CHIP".to_string(), fills_a);
        let closed_a =
            compute_fifo_closed_orders_and_funding(&map_a, &HashMap::new(), &HashMap::new())
                .closed_orders;
        assert_eq!(closed_a.len(), 1);
        assert!((closed_a[0].1.total_sz - 3585.0).abs() < 1e-9);
        assert!((closed_a[0].1.open_fees_usdc - 0.04).abs() < 1e-9);

        // Variant B: Same-ms open+close cycle (0 -> 812 -> 0) at 1795023372707 with inverted tid,
        // followed by #174331 CHIP SHORT (3585 units). Naive (time_ms, tid) would place start=812 first,
        // initializing prev_szi=812 and shrinking #174331 close qty from 3585 to 2773!
        let mut fills_b = vec![
            make_fill(
                "CHIP",
                "A",
                0.0279,
                812.0,
                0.01,
                0.15,
                "Close Long",
                same_ms,
                8001,
                101, // lower tid, causally second!
                Some(812.0),
            ),
            make_fill(
                "CHIP",
                "B",
                0.0277,
                812.0,
                0.01,
                0.0,
                "Open Long",
                same_ms,
                8000,
                201, // higher tid, causally first!
                Some(0.0),
            ),
            make_fill(
                "CHIP",
                "A",
                0.027714,
                3585.0,
                0.045,
                0.0,
                "Open Short",
                same_ms + 500_000,
                8002,
                301,
                Some(0.0),
            ),
            make_fill(
                "CHIP",
                "B",
                0.027498,
                3585.0,
                0.045,
                0.77436,
                "Close Short",
                same_ms + 5_180_000,
                8003,
                302,
                Some(-3585.0),
            ),
        ];
        sort_fills_causally(&mut fills_b);
        assert_eq!(fills_b[0].tid, 201);
        assert_eq!(fills_b[1].tid, 101);

        let mut map_b = BTreeMap::new();
        map_b.insert("CHIP".to_string(), fills_b);
        let closed_b =
            compute_fifo_closed_orders_and_funding(&map_b, &HashMap::new(), &HashMap::new())
                .closed_orders;
        let chip_174331 = closed_b
            .iter()
            .find(|(oid, _)| *oid == 8003)
            .expect("CHIP #174331 closing order matched");
        assert_eq!(chip_174331.1.side, Some(OrderSide::Short));
        assert!((chip_174331.1.total_sz - 3585.0).abs() < 1e-9);
        assert!((chip_174331.1.open_fees_usdc - 0.045).abs() < 1e-9);
        assert!((chip_174331.1.close_fees_usdc - 0.045).abs() < 1e-9);
    }

    #[test]
    fn test_sort_fills_causally_mid_history_chain_and_fallbacks() {
        // Multi-fill chain mid-history: running_szi=10.0 -> 25.0 -> 30.0 -> 20.0
        let mut fills = vec![
            make_fill(
                "SOL",
                "B",
                180.0,
                10.0,
                0.1,
                0.0,
                "Open Long",
                1000,
                1,
                1,
                Some(0.0),
            ),
            make_fill(
                "SOL",
                "B",
                182.0,
                5.0,
                0.1,
                0.0,
                "Open Long",
                2000,
                2,
                10,
                Some(25.0),
            ),
            make_fill(
                "SOL",
                "A",
                183.0,
                10.0,
                0.1,
                10.0,
                "Close Long",
                2000,
                3,
                11,
                Some(30.0),
            ),
            make_fill(
                "SOL",
                "B",
                181.0,
                15.0,
                0.1,
                0.0,
                "Open Long",
                2000,
                2,
                20,
                Some(10.0),
            ),
        ];
        sort_fills_causally(&mut fills);
        let tids: Vec<u64> = fills.iter().map(|f| f.tid).collect();
        assert_eq!(tids, vec![1, 20, 10, 11]);
        assert_eq!(derive_open_position_opened_at_ms(&fills, 20.0), Some(1000));

        // Missing startPosition falls back to (time_ms, tid)
        let mut missing_sp = vec![
            make_fill(
                "ETH",
                "B",
                3000.0,
                1.0,
                0.1,
                0.0,
                "Open Long",
                5000,
                10,
                50,
                None,
            ),
            make_fill(
                "ETH",
                "B",
                3001.0,
                1.0,
                0.1,
                0.0,
                "Open Long",
                5000,
                11,
                20,
                Some(1.0),
            ),
        ];
        sort_fills_causally(&mut missing_sp);
        assert_eq!(missing_sp[0].tid, 20);
        assert_eq!(missing_sp[1].tid, 50);

        // Disconnected / ambiguous startPosition falls back to tid order safely
        let mut ambiguous = vec![
            make_fill(
                "ARB",
                "B",
                1.0,
                10.0,
                0.1,
                0.0,
                "Open Long",
                6000,
                12,
                30,
                Some(100.0),
            ),
            make_fill(
                "ARB",
                "B",
                1.0,
                10.0,
                0.1,
                0.0,
                "Open Long",
                6000,
                13,
                10,
                Some(200.0),
            ),
        ];
        sort_fills_causally(&mut ambiguous);
        assert_eq!(ambiguous[0].tid, 10);
        assert_eq!(ambiguous[1].tid, 30);
    }

    #[test]
    fn test_is_entry_fill_complete_and_working_reconcile_disposition() {
        // Real observed partial fill cases from #53: TAO (36.9%) and PYTH (18.2%)
        assert!(!is_entry_fill_complete(0.101, 0.274, 0.001, 3));
        assert!(!is_entry_fill_complete(388.0, 2127.0, 1.0, 0));
        assert!(is_entry_fill_complete(1808.0, 2127.0, 1.0, 0)); // 85.002%
        assert!(!is_entry_fill_complete(1.0, 2.0, 1.0, 0)); // 50% with 1 lot left
        assert!(is_entry_fill_complete(0.2736, 0.274, 0.001, 3)); // remaining < 0.5 lot

        let mut resting = HashSet::new();
        resting.insert(65_654_266_947u64);

        // 1. Active < 45s partial fill with resting OID -> ProtectActivePartial (do not cancel OID, do not promote)
        assert_eq!(
            classify_working_order_for_reconcile(
                12_000,
                "SHORT",
                2127.0,
                1.0,
                0,
                Some(65_654_266_947),
                &resting,
                Some(-388.0),
            ),
            WorkingOrderReconcileDisposition::ProtectActivePartial
        );

        // 2. Active < 45s with zero fill on HL -> ProtectPending
        assert_eq!(
            classify_working_order_for_reconcile(
                12_000,
                "SHORT",
                2127.0,
                1.0,
                0,
                Some(65_654_266_947),
                &resting,
                None,
            ),
            WorkingOrderReconcileDisposition::ProtectPending
        );

        // 3. >= 85% filled -> AdoptOrExpire
        assert_eq!(
            classify_working_order_for_reconcile(
                12_000,
                "SHORT",
                2127.0,
                1.0,
                0,
                Some(65_654_266_947),
                &resting,
                Some(-1810.0),
            ),
            WorkingOrderReconcileDisposition::AdoptOrExpire
        );

        // 4. Expired >= 45_000 ms -> AdoptOrExpire
        assert_eq!(
            classify_working_order_for_reconcile(
                45_000,
                "SHORT",
                2127.0,
                1.0,
                0,
                Some(65_654_266_947),
                &resting,
                Some(-388.0),
            ),
            WorkingOrderReconcileDisposition::AdoptOrExpire
        );

        // 5. Entry OID no longer resting on HL -> AdoptOrExpire
        assert_eq!(
            classify_working_order_for_reconcile(
                12_000,
                "SHORT",
                2127.0,
                1.0,
                0,
                Some(99_999_999),
                &resting,
                Some(-388.0),
            ),
            WorkingOrderReconcileDisposition::AdoptOrExpire
        );

        // 6. Opposite-side HL position -> AdoptOrExpire
        assert_eq!(
            classify_working_order_for_reconcile(
                12_000,
                "SHORT",
                2127.0,
                1.0,
                0,
                Some(65_654_266_947),
                &resting,
                Some(388.0),
            ),
            WorkingOrderReconcileDisposition::AdoptOrExpire
        );
    }

    #[test]
    fn test_is_passive_reduce_only_close_order_variants() {
        // Real HL frontendOpenOrders shape for passive GTC reduceOnly close (triggerPx: "0.0")
        assert!(is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 65_660_647_244u64,
            "reduceOnly": true,
            "isTrigger": false,
            "isPositionTpsl": false,
            "orderType": "Limit",
            "triggerPx": "0.0",
            "timestamp": 1_795_181_186_927u64
        })));
        // Minimal passive reduceOnly Limit order
        assert!(is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 100u64,
            "reduceOnly": true,
            "orderType": "Limit"
        })));
        // Non-reduceOnly resting entry Limit order -> NOT protected as passive close
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "PUMP",
            "oid": 101u64,
            "reduceOnly": false,
            "isTrigger": false,
            "orderType": "Limit",
            "triggerPx": "0.0"
        })));
        // Missing reduceOnly (e.g. openOrders fallback) -> false
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 102u64
        })));
        // Trigger orders must return false across isTrigger / triggerPx / orderType / isPositionTpsl
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 103u64,
            "reduceOnly": true,
            "isTrigger": true,
            "orderType": "Stop Market",
            "triggerPx": "0.20111"
        })));
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 104u64,
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "triggerPx": "0.20111"
        })));
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 105u64,
            "reduceOnly": true,
            "orderType": "Take Profit Market"
        })));
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 106u64,
            "reduceOnly": true,
            "orderType": "Stop Limit"
        })));
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 107u64,
            "reduceOnly": true,
            "isPositionTpsl": true,
            "orderType": "Limit"
        })));
    }

    #[test]
    fn test_passive_reduce_only_close_order_protection_and_orphan_cancellation() {
        let passive_met = json!({
            "coin": "MET",
            "oid": 65_660_647_244u64,
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "triggerPx": "0.0",
            "timestamp": 1_795_181_186_927u64
        });
        let sl_met = json!({
            "coin": "MET",
            "oid": 65_605_015_926u64,
            "reduceOnly": true,
            "isTrigger": true,
            "orderType": "Stop Market",
            "triggerPx": "0.20111",
            "timestamp": 1_795_181_000_000u64
        });
        let tp_met = json!({
            "coin": "MET",
            "oid": 65_648_930_301u64,
            "reduceOnly": true,
            "isTrigger": true,
            "orderType": "Take Profit Market",
            "triggerPx": "0.25086",
            "timestamp": 1_795_181_000_500u64
        });
        let tracked_triggers: HashSet<u64> =
            [65_605_015_926u64, 65_648_930_301u64].into_iter().collect();

        // (a) Resting passive reduceOnly Limit order on an OPEN coin is protected when !close_legacy_positions
        let mut open_coins: HashSet<String> = ["MET".to_string()].into_iter().collect();
        let (protected_a, orphans_a) = find_orphan_orders_to_cancel(
            &[sl_met.clone(), tp_met.clone(), passive_met.clone()],
            &open_coins,
            &tracked_triggers,
            false,
        );
        assert!(protected_a.contains(&65_660_647_244u64));
        assert!(protected_a.contains(&65_605_015_926u64));
        assert!(protected_a.contains(&65_648_930_301u64));
        assert!(orphans_a.is_empty());

        // (b) Cancelled if open_coins does not contain coin (flat position)
        let empty_open_coins: HashSet<String> = HashSet::new();
        let empty_tracked: HashSet<u64> = HashSet::new();
        let (protected_b, orphans_b) = find_orphan_orders_to_cancel(
            std::slice::from_ref(&passive_met),
            &empty_open_coins,
            &empty_tracked,
            false,
        );
        assert!(!protected_b.contains(&65_660_647_244u64));
        assert_eq!(orphans_b, vec![("MET".to_string(), 65_660_647_244u64)]);

        // (c) Cancelled if close_legacy_positions == true
        let (protected_c, orphans_c) = find_orphan_orders_to_cancel(
            &[sl_met.clone(), tp_met.clone(), passive_met.clone()],
            &open_coins,
            &tracked_triggers,
            true,
        );
        assert!(protected_c.is_empty());
        assert_eq!(
            orphans_c,
            vec![
                ("MET".to_string(), 65_605_015_926u64),
                ("MET".to_string(), 65_648_930_301u64),
                ("MET".to_string(), 65_660_647_244u64),
            ]
        );

        // (d) Duplicate passive limit orders on the same coin cancel all but the newest OID while triggers stay untouched
        let older_passive_met = json!({
            "coin": "MET",
            "oid": 65_660_600_000u64,
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "triggerPx": "0.0",
            "timestamp": 1_795_181_100_000u64
        });
        let newer_passive_met = json!({
            "coin": "MET",
            "oid": 65_660_647_244u64,
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "triggerPx": "0.0",
            "timestamp": 1_795_181_186_927u64
        });
        // Also test k-prefix coin & equal-timestamp tiebreak by highest oid
        open_coins.insert("KPEPE".to_string());
        let kpepe_low_oid = json!({
            "coin": "kPEPE",
            "oid": 1000u64,
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "timestamp": 2_000_000u64
        });
        let kpepe_high_oid = json!({
            "coin": "kPEPE",
            "oid": 1005u64,
            "reduceOnly": true,
            "isTrigger": false,
            "orderType": "Limit",
            "timestamp": 2_000_000u64
        });
        let (protected_d, orphans_d) = find_orphan_orders_to_cancel(
            &[
                sl_met,
                tp_met,
                older_passive_met,
                newer_passive_met,
                kpepe_low_oid,
                kpepe_high_oid,
            ],
            &open_coins,
            &tracked_triggers,
            false,
        );
        assert!(protected_d.contains(&65_605_015_926u64));
        assert!(protected_d.contains(&65_648_930_301u64));
        assert!(protected_d.contains(&65_660_647_244u64));
        assert!(protected_d.contains(&1005u64));
        assert_eq!(
            orphans_d,
            vec![
                ("MET".to_string(), 65_660_600_000u64),
                ("kPEPE".to_string(), 1000u64),
            ]
        );
    }

    #[test]
    fn test_parse_vault_position_info_calculations_and_fallbacks() {
        let now = 1_795_199_000_000i64;
        let past_lock = 1_793_779_590_878i64;
        let future_lock = now + 86_400_000;

        let details = json!({
            "name": "Hyperliquidity Provider (HLP)",
            "vaultAddress": "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
            "isClosed": false,
            "apr": 0.0814,
            "followerState": {
                "vaultEquity": "202.400584",
                "pnl": "0.92",
                "allTimePnl": "1.34",
                "daysFollowing": 25,
                "lockupUntil": past_lock
            }
        });
        let info = parse_vault_position_info(
            "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",
            &details,
            Some((202.400584, past_lock)),
            now,
        );
        assert_eq!(
            info.vault_address,
            "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303"
        );
        assert_eq!(info.name, "Hyperliquidity Provider (HLP)");
        assert_eq!(info.leader, "0x677d831aef5328190852e24f13c46cac05f984e7");
        assert!((info.equity_usdc - 202.40).abs() < 1e-6);
        assert!((info.pnl_usdc - 0.92).abs() < 1e-6);
        assert!((info.all_time_pnl_usdc - 1.34).abs() < 1e-6);
        assert!((info.pnl_pct - 0.46).abs() < 1e-6);
        assert!((info.all_time_pnl_pct - 0.67).abs() < 1e-6);
        assert!((info.apr_pct - 8.14).abs() < 1e-6);
        assert_eq!(info.days_following, 25);
        assert_eq!(info.locked_until_ms, past_lock);
        assert!(!info.is_locked);
        assert!(!info.is_closed);

        // Null followerState falls back to userVaultEquities equity + future lockupTimestamp
        let details_no_follower = json!({
            "name": "Alpha Vault",
            "leader": "0x1111111111111111111111111111111111111111",
            "isClosed": true,
            "apr": "0.125",
            "followerState": null
        });
        let info_fb = parse_vault_position_info(
            "0x2222222222222222222222222222222222222222",
            &details_no_follower,
            Some((50.0, future_lock)),
            now,
        );
        assert_eq!(
            info_fb.vault_address,
            "0x2222222222222222222222222222222222222222"
        );
        assert!((info_fb.equity_usdc - 50.0).abs() < 1e-6);
        assert_eq!(info_fb.pnl_pct, 0.0);
        assert_eq!(info_fb.all_time_pnl_pct, 0.0);
        assert!((info_fb.apr_pct - 12.5).abs() < 1e-6);
        assert_eq!(info_fb.locked_until_ms, future_lock);
        assert!(info_fb.is_locked);
        assert!(info_fb.is_closed);
    }

    #[test]
    fn test_parse_spot_usdc_balance_variants() {
        assert_eq!(parse_spot_usdc_balance(&json!([])), None);
        assert_eq!(parse_spot_usdc_balance(&json!({})), None);
        assert_eq!(parse_spot_usdc_balance(&json!({ "balances": "bad" })), None);
        assert_eq!(
            parse_spot_usdc_balance(&json!({ "balances": [] })),
            Some(0.0)
        );
        assert_eq!(
            parse_spot_usdc_balance(&json!({
                "balances": [
                    { "coin": "USDC", "total": "5000.25", "hold": "0.0" },
                    { "coin": "PURR", "total": "100.0", "hold": "0.0" },
                    { "coin": "usdc", "total": "25.50", "hold": "1.0" },
                    { "coin": "USDC", "total": "-5.0" }
                ]
            })),
            Some(5025.75)
        );
        assert_eq!(
            parse_spot_usdc_details(&json!({
                "balances": [
                    { "coin": "USDC", "token": 0, "total": "10978.37", "hold": "680.05" }
                ],
                "tokenToAvailableAfterMaintenance": [[0, "10641.56"]]
            })),
            Some((10978.37, 680.05, Some(10641.56)))
        );
    }

    #[test]
    fn test_parse_hl_portfolio_pnl_variants() {
        assert!(parse_hl_portfolio_pnl(&json!([]), 1000).is_none());
        assert!(parse_hl_portfolio_pnl(&json!({}), 1000).is_none());
        assert!(
            parse_hl_portfolio_pnl(&json!([["perpDay", { "pnlHistory": [] }]]), 1000).is_none()
        );

        let parsed = parse_hl_portfolio_pnl(
            &json!([
                ["day", { "pnlHistory": [[100, "0.0"], [200, "250.0"]], "vlm": "1412.91" }],
                ["perpDay", { "pnlHistory": [[200, "242.70"], [100, "0.0"], [200, "243.10"]], "vlm": "1412.91" }],
                ["perpWeek", { "pnlHistory": [[50, "0.0"], [200, "362.52"]], "vlm": "64604.1" }],
                ["perpAllTime", { "pnlHistory": [[10, "0.0"], [200, "362.52"]], "vlm": "83291.08" }]
            ]),
            999_999,
        )
        .unwrap();
        assert_eq!(parsed.synced_at_ms, 999_999);
        assert_eq!(parsed.perp_day.pnl_history, vec![(100, 0.0), (200, 243.10)]);
        assert!((parsed.perp_day.vlm - 1412.91).abs() < 1e-6);
        assert_eq!(parsed.perp_week.pnl_history, vec![(50, 0.0), (200, 362.52)]);
        assert!(parsed.perp_month.pnl_history.is_empty());
        assert_eq!(
            parsed.perp_all_time.pnl_history,
            vec![(10, 0.0), (200, 362.52)]
        );
    }

    #[test]
    fn test_fifo_funding_replay_open_only_and_same_ms_ordering() {
        // Open-only coin (no userFills) uses open_szi_by_coin
        let mut funding_by_coin = HashMap::new();
        funding_by_coin.insert(
            "ETH".to_string(),
            vec![ParsedHlFunding {
                coin: "ETH".to_string(),
                time_ms: 1_000_000,
                usdc: 3.25,
                szi: 2.0,
                funding_rate: 0.0001,
                dedup_key: "1000000:ETH:0x1".to_string(),
            }],
        );
        let mut open_szi = HashMap::new();
        open_szi.insert("ETH".to_string(), 2.0);
        let out_open =
            compute_fifo_closed_orders_and_funding(&BTreeMap::new(), &funding_by_coin, &open_szi);
        assert!(out_open.closed_orders.is_empty());
        assert_eq!(out_open.funding_events_with_remaining.len(), 1);
        assert!((out_open.funding_events_with_remaining[0].1 - 3.25).abs() < 1e-9);

        // Same-ms Open + Funding + Close places Funding after Open and before Close
        let mut fills_by_coin = BTreeMap::new();
        fills_by_coin.insert(
            "BTC".to_string(),
            vec![
                make_fill(
                    "BTC",
                    "B",
                    90_000.0,
                    1.0,
                    0.0,
                    0.0,
                    "Open Long",
                    2_000_000,
                    1,
                    1,
                    Some(0.0),
                ),
                make_fill(
                    "BTC",
                    "A",
                    90_100.0,
                    1.0,
                    0.0,
                    100.0,
                    "Close Long",
                    2_000_000,
                    2,
                    2,
                    Some(1.0),
                ),
            ],
        );
        let mut btc_funding = HashMap::new();
        btc_funding.insert(
            "BTC".to_string(),
            vec![ParsedHlFunding {
                coin: "BTC".to_string(),
                time_ms: 2_000_000,
                usdc: 4.5,
                szi: 1.0,
                funding_rate: 0.00005,
                dedup_key: "2000000:BTC:0x2".to_string(),
            }],
        );
        let out_same_ms =
            compute_fifo_closed_orders_and_funding(&fills_by_coin, &btc_funding, &HashMap::new());
        assert_eq!(out_same_ms.closed_orders.len(), 1);
        assert!((out_same_ms.closed_orders[0].1.funding_accrued_usdc - 4.5).abs() < 1e-9);
        assert!((out_same_ms.funding_events_with_remaining[0].1 - 0.0).abs() < 1e-9);

        // Multi-lot DCA + partial close attributes per-lot event shares without double counting:
        // Open 60 -> E0 (+60) -> Add 40 -> E1 (+100) -> Close 30 -> 70 remaining
        // Lot 1 (60) loses 30/60 = 50% (30 of E0, 30 of E1 -> closed=60); remaining: E0=30, E1=70
        let mut dca_fills = BTreeMap::new();
        dca_fills.insert(
            "SOL".to_string(),
            vec![
                make_fill(
                    "SOL",
                    "B",
                    100.0,
                    60.0,
                    0.0,
                    0.0,
                    "Open Long",
                    1_000,
                    1,
                    1,
                    Some(0.0),
                ),
                make_fill(
                    "SOL",
                    "B",
                    100.0,
                    40.0,
                    0.0,
                    0.0,
                    "Open Long",
                    3_000,
                    2,
                    2,
                    Some(60.0),
                ),
                make_fill(
                    "SOL",
                    "A",
                    100.0,
                    30.0,
                    0.0,
                    0.0,
                    "Close Long",
                    5_000,
                    3,
                    3,
                    Some(100.0),
                ),
            ],
        );
        let mut dca_funding = HashMap::new();
        dca_funding.insert(
            "SOL".to_string(),
            vec![
                ParsedHlFunding {
                    coin: "SOL".to_string(),
                    time_ms: 2_000,
                    usdc: 60.0,
                    szi: 60.0,
                    funding_rate: 0.01,
                    dedup_key: "2000:SOL:0xe0".to_string(),
                },
                ParsedHlFunding {
                    coin: "SOL".to_string(),
                    time_ms: 4_000,
                    usdc: 100.0,
                    szi: 100.0,
                    funding_rate: 0.01,
                    dedup_key: "4000:SOL:0xe1".to_string(),
                },
            ],
        );
        let out_dca =
            compute_fifo_closed_orders_and_funding(&dca_fills, &dca_funding, &HashMap::new());
        assert_eq!(out_dca.closed_orders.len(), 1);
        assert!((out_dca.closed_orders[0].1.funding_accrued_usdc - 60.0).abs() < 1e-9);
        assert!((out_dca.open_epoch_booked_funding_by_coin["SOL"] - 60.0).abs() < 1e-9);
        assert!((out_dca.funding_events_with_remaining[0].1 - 30.0).abs() < 1e-9);
        assert!((out_dca.funding_events_with_remaining[1].1 - 70.0).abs() < 1e-9);
    }

    #[test]
    fn test_parse_vault_position_info_enriched_facts_fallback_and_truncation() {
        let hlp_addr = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";
        let now = 1_795_000_000_000_i64;

        // 1. Live-shaped HLP vaultDetails with 100 followers (API cap), portfolio TVL + allTime vlm,
        // parent relationship with 7 child sub-vaults, and rich protocol fields.
        let followers_100: Vec<Value> = (0..100)
            .map(|i| {
                json!({
                    "user": format!("0x{:040x}", i + 1),
                    "vaultEquity": "1000.0"
                })
            })
            .collect();
        let hlp_details = json!({
            "name": "Hyperliquidity Provider (HLP)",
            "vaultAddress": hlp_addr,
            "leader": "0x677d831aef5328190852e24f13c46cac05f984e7",
            "description": "  Protocol market-making and liquidation vault on Hyperliquid L1.  ",
            "apr": 0.14256,
            "leaderFraction": 0.01493,
            "leaderCommission": 0.0,
            "allowDeposits": true,
            "alwaysCloseOnWithdraw": false,
            "maxDistributable": "312450123.456",
            "maxWithdrawable": "305000000.123",
            "isClosed": false,
            "followers": followers_100,
            "portfolio": [
                ["week", {
                    "accountValueHistory": [[1000, "290000000.0"], [2000, "310000000.55"]],
                    "vlm": "9999999.0"
                }],
                ["allTime", {
                    "accountValueHistory": [[500, "1000000.0"], [1500, "311111111.11"]],
                    "vlm": "512345678901.239"
                }],
                ["day", {
                    "accountValueHistory": [[1900, "312000000.0"], [2100, "315432109.876"]],
                    "vlm": "123456789.0"
                }]
            ],
            "relationship": {
                "type": "parent",
                "data": {
                    "childAddresses": [
                        "0x0000000000000000000000000000000000000001",
                        "0x0000000000000000000000000000000000000002",
                        "0x0000000000000000000000000000000000000003",
                        "0x0000000000000000000000000000000000000004",
                        "0x0000000000000000000000000000000000000005",
                        "0x0000000000000000000000000000000000000006",
                        "0x0000000000000000000000000000000000000007"
                    ]
                }
            },
            "followerState": {
                "vaultEquity": "202.400584",
                "pnl": "1.25",
                "allTimePnl": "4.50",
                "daysFollowing": 30,
                "lockupUntil": now + 60_000
            }
        });

        let (info, raw_eq) =
            parse_vault_position_info_with_raw_equity(hlp_addr, &hlp_details, None, now);
        assert!((raw_eq - 202.400584).abs() < 1e-6);
        assert_eq!(info.name, "Hyperliquidity Provider (HLP)");
        assert_eq!(
            info.description,
            "Protocol market-making and liquidation vault on Hyperliquid L1."
        );
        assert!((info.apr_pct - 14.26).abs() < 1e-6);
        assert!((info.leader_fraction_pct - 1.49).abs() < 1e-6);
        assert!((info.leader_commission_pct - 0.0).abs() < 1e-6);
        assert!(info.allow_deposits);
        assert!(!info.always_close_on_withdraw);
        assert!((info.max_distributable_usdc - 312_450_123.46).abs() < 1e-2);
        assert!((info.max_withdrawable_usdc - 305_000_000.12).abs() < 1e-2);
        assert_eq!(info.follower_count, 100);
        assert!(info.followers_capped);
        // Prefer "day" latest accountValueHistory over followers sum or week/allTime
        assert!((info.tvl_usdc - 315_432_109.88).abs() < 1e-2);
        assert!((info.all_time_volume_usdc - 512_345_678_901.24).abs() < 1e-1);
        assert_eq!(info.relationship_type, "parent");
        assert_eq!(info.child_vault_count, 7);
        assert!(info.is_locked);

        // 2. Fallback TVL when `portfolio` is missing, plus >500-char UTF-8 description truncation,
        // leader commission clamping, and default allow_deposits/relationship fields.
        let long_utf8_desc = format!("  {}  ", "é".repeat(540));
        let user_vault_details = json!({
            "name": "Aegis Alpha User Vault",
            "vaultAddress": "0x2222222222222222222222222222222222222222",
            "leader": "0x3333333333333333333333333333333333333333",
            "description": long_utf8_desc,
            "isClosed": true,
            "leaderFraction": 0.125,
            "leaderCommission": 0.10,
            "alwaysCloseOnWithdraw": true,
            "maxDistributable": "-5.0",
            "followers": [
                { "user": "0x1", "vaultEquity": "1500.25" },
                { "user": "0x2", "vaultEquity": "2499.75" },
                { "user": "0x3", "vaultEquity": "-50.0" }
            ]
        });

        let fallback_info = parse_vault_position_info(
            "0x2222222222222222222222222222222222222222",
            &user_vault_details,
            None,
            now,
        );
        assert_eq!(fallback_info.description.chars().count(), 500);
        assert!(fallback_info.description.chars().all(|c| c == 'é'));
        assert_eq!(fallback_info.follower_count, 3);
        assert!(!fallback_info.followers_capped);
        assert!((fallback_info.tvl_usdc - 4000.0).abs() < 1e-6);
        assert!((fallback_info.all_time_volume_usdc - 0.0).abs() < 1e-6);
        assert!((fallback_info.leader_fraction_pct - 12.5).abs() < 1e-6);
        assert!((fallback_info.leader_commission_pct - 10.0).abs() < 1e-6);
        assert!(!fallback_info.allow_deposits); // defaults to !is_closed
        assert!(fallback_info.always_close_on_withdraw);
        assert!((fallback_info.max_distributable_usdc - 0.0).abs() < 1e-6);
        assert_eq!(fallback_info.relationship_type, "normal");
        assert_eq!(fallback_info.child_vault_count, 0);
    }
}
