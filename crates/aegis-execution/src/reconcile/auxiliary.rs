//! Unlocked auxiliary Hyperliquid sync worker (`aegis-execution::reconcile::auxiliary`,
//! `R14-H2`, `R14-H4`, Sec. 11).
//!
//! Executes off-critical-path syncs (`portfolio`, `userVaultEquities`/`vaultDetails` capped at 10,
//! watermarked incremental `userFunding`, 6h `userFees`, and startup `extraAgents` check) strictly
//! without holding `execution_lock()`.

use crate::http_client::{parse_user_fee_rates_bps, HyperliquidHttpClient, RequestOptions};
use crate::reconcile::equity::VAULT_SYNC_MIN_INTERVAL_MS;
use crate::reconcile::ledger::{funding_dedup_key, ParsedHlFunding};
use crate::signing::derive_evm_address_from_private_key;
use crate::ws_client::WsStateCache;
use aegis_core::{
    compute_vault_pnl_pct, is_unified_account_mode, mask_identifier, normalize_hyperliquid_symbol,
    now_ms, parse_hl_f64, parse_hl_f64_opt, parse_hl_i64_opt, parse_hl_u64_opt, round_dp,
    CachedHlPortfolioPnl, ConfigMap, ExchangeAsset, HlPortfolioWindowCache, OrderSide, Result,
    VaultInfoPayload, VaultPositionInfo,
};
use aegis_db::{AssetUpsertInput, DatabaseManager, FundingEventUpsertInput};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

pub const MAX_VAULTS_TO_FETCH: usize = 10;
pub const AUX_SYNC_INTERVAL_MS: i64 = 60_000;
pub const USER_FEES_SYNC_INTERVAL_MS: i64 = 6 * 3_600_000;
pub const EXTRA_AGENT_EXPIRY_WARN_MS: i64 = 7 * 86_400_000;
const HL_USER_FUNDING_PAGE_LIMIT: usize = 500;
const USER_FUNDING_STARTUP_MAX_PAGES: usize = 25;
const USER_FUNDING_PERIODIC_MAX_PAGES: usize = 5;
const USER_FUNDING_WATERMARK_OVERLAP_MS: i64 = 3_600_000;

type VaultSyncThrottleKey = (String, String, String);
static LAST_VAULT_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<VaultSyncThrottleKey, i64>>> =
    OnceLock::new();
type SpotSyncThrottleKey = (String, String);
static LAST_PORTFOLIO_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<SpotSyncThrottleKey, i64>>> =
    OnceLock::new();
static LAST_USER_FEES_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<SpotSyncThrottleKey, i64>>> =
    OnceLock::new();

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

/// Parses a Hyperliquid `/info {"type":"portfolio"}` response into [`CachedHlPortfolioPnl`].
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuxiliarySyncReport {
    pub portfolio_synced: bool,
    pub vaults_synced: usize,
    pub funding_events_inserted: usize,
    pub user_fees_synced: bool,
    pub extra_agent_warning: Option<String>,
}

const USER_FUNDING_WS_REST_VERIFY_MS: i64 = 300_000;

type FundingRestStateMap = HashMap<(String, String), (i64, u64)>;

/// Unlocked 60-second background worker for Hyperliquid auxiliary syncs (`portfolio`, `vaultDetails`,
/// watermarked `userFunding`, 6h `userFees`, and startup `extraAgents` expiry check) (`R14-H2`, Sec. 11).
#[derive(Clone)]
pub struct AuxiliarySyncWorker {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
    ws_cache: Option<Arc<WsStateCache>>,
    last_funding_rest_state: Arc<Mutex<FundingRestStateMap>>,
}

impl AuxiliarySyncWorker {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient) -> Self {
        Self {
            db,
            http,
            ws_cache: None,
            last_funding_rest_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attaches a shared [`WsStateCache`] so steady-state `sync_funding_events` consumes User WS
    /// `userFundings` when connected & gap-filled, falling back to REST on reconnect or 5m check.
    pub fn with_ws_cache(mut self, ws_cache: Arc<WsStateCache>) -> Self {
        self.ws_cache = Some(ws_cache);
        self
    }

    /// Runs startup pre-reconcile auxiliary tasks (`extraAgents` check, `userFees`, and
    /// watermarked `userFunding` backfill up to `25` pages) strictly unlocked.
    pub fn sync_startup_before_reconcile(&self) -> AuxiliarySyncReport {
        let cfg = self.db.load_config_or_default();
        if !cfg.execution_mode().is_live_or_testnet() {
            return AuxiliarySyncReport::default();
        }
        let ts = now_ms();
        let extra_agent_warning = self.verify_extra_agents_on_startup(&cfg, ts);
        let user_fees_synced = self
            .sync_user_fees_with_interval(&cfg, ts, USER_FEES_SYNC_INTERVAL_MS)
            .is_some();
        let funding_events_inserted = self.sync_funding_events(&cfg, ts, true).unwrap_or(0);
        AuxiliarySyncReport {
            portfolio_synced: false,
            vaults_synced: 0,
            funding_events_inserted,
            user_fees_synced,
            extra_agent_warning,
        }
    }

    /// Runs post-reconcile auxiliary view syncs (`portfolio` + `vaultInfo`) strictly unlocked.
    pub fn sync_post_reconcile_views(&self) -> AuxiliarySyncReport {
        let cfg = self.db.load_config_or_default();
        if !cfg.execution_mode().is_live_or_testnet() {
            return AuxiliarySyncReport::default();
        }
        let ts = now_ms();
        let portfolio_synced = self.sync_portfolio_pnl_if_due(&cfg, ts).is_some();
        let vaults_synced = self
            .sync_vault_info_if_due(&cfg, ts, None)
            .map(|v| v.vault_count)
            .unwrap_or(0);
        AuxiliarySyncReport {
            portfolio_synced,
            vaults_synced,
            ..Default::default()
        }
    }

    /// Runs one complete auxiliary sync cycle strictly without `execution_lock()`.
    pub fn run_once(&self, is_startup: bool) -> AuxiliarySyncReport {
        let cfg = self.db.load_config_or_default();
        let ts = now_ms();
        if !cfg.execution_mode().is_live_or_testnet() {
            let funding_events_inserted =
                self.sync_funding_events(&cfg, ts, is_startup).unwrap_or(0);
            return AuxiliarySyncReport {
                funding_events_inserted,
                ..Default::default()
            };
        }
        let extra_agent_warning = if is_startup {
            self.verify_extra_agents_on_startup(&cfg, ts)
        } else {
            None
        };
        let user_fees_synced = self
            .sync_user_fees_with_interval(
                &cfg,
                ts,
                if is_startup {
                    0
                } else {
                    USER_FEES_SYNC_INTERVAL_MS
                },
            )
            .is_some();
        let funding_events_inserted = self.sync_funding_events(&cfg, ts, is_startup).unwrap_or(0);
        let portfolio_synced = self.sync_portfolio_pnl_if_due(&cfg, ts).is_some();
        let vaults_synced = self
            .sync_vault_info_if_due(&cfg, ts, None)
            .map(|v| v.vault_count)
            .unwrap_or(0);
        AuxiliarySyncReport {
            portfolio_synced,
            vaults_synced,
            funding_events_inserted,
            user_fees_synced,
            extra_agent_warning,
        }
    }

    /// Verifies that an API Agent Wallet derived from `private_key` (when distinct from master
    /// `wallet_address`) is registered in Hyperliquid `extraAgents` and not expiring within 7 days.
    pub fn verify_extra_agents_on_startup(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
    ) -> Option<String> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let wallet = cfg.get_str("wallet_address", "").trim().to_string();
        let pk = cfg.get_str("private_key", "").trim().to_string();
        if wallet.is_empty() || pk.is_empty() {
            return None;
        }
        let Ok(agent_addr) = derive_evm_address_from_private_key(&pk) else {
            return None;
        };
        if agent_addr.eq_ignore_ascii_case(&wallet) {
            return None;
        }

        let agents = match self.http.fetch_extra_agents(&wallet, cfg) {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!(error = %e, "Hyperliquid extraAgents lookup skipped/failed");
                return None;
            }
        };

        let matched = agents.iter().find(|entry| {
            entry
                .get("address")
                .and_then(Value::as_str)
                .is_some_and(|a| a.trim().eq_ignore_ascii_case(&agent_addr))
        });
        let warning = match matched {
            None => Some(format!(
                "API agent wallet {} is NOT registered in Hyperliquid extraAgents for master {}",
                mask_identifier(&agent_addr),
                mask_identifier(&wallet)
            )),
            Some(entry) => {
                let valid_until = parse_hl_i64_opt(entry.get("validUntil")).unwrap_or(0);
                if valid_until > 0
                    && valid_until < now_ms_val.saturating_add(EXTRA_AGENT_EXPIRY_WARN_MS)
                {
                    let rem_hours = (valid_until - now_ms_val).max(0) / 3_600_000;
                    Some(format!(
                        "API agent wallet {} expires in ~{rem_hours}h (< 7d) in Hyperliquid extraAgents",
                        mask_identifier(&agent_addr)
                    ))
                } else {
                    None
                }
            }
        };

        if let Some(ref msg) = warning {
            tracing::warn!("{msg}");
            let _ = self
                .db
                .log_telemetry("WARN", "execution.auxiliary.extra_agents", msg, None);
        }
        warning
    }

    /// Syncs Hyperliquid `userFees` on a 6-hour cadence, persisting `live_exchange_maker_fee_bps`
    /// and `live_exchange_taker_fee_bps` without holding `execution_lock()`.
    pub fn sync_user_fees_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<(f64, f64)> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let wallet_trimmed = cfg.get_str("wallet_address", "").trim().to_string();
        if wallet_trimmed.is_empty() {
            return None;
        }
        let key: SpotSyncThrottleKey = (
            self.db.db_name().to_string(),
            wallet_trimmed.to_ascii_lowercase(),
        );
        let eff_min_interval = min_interval_ms.max(0);
        if eff_min_interval > 0 {
            let map = LAST_USER_FEES_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
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

        let raw = match self.http.fetch_user_fees(&wallet_trimmed, cfg) {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!(error = %e, "Hyperliquid userFees fetch failed; keeping existing fee tiers");
                return None;
            }
        };
        let (maker_bps, taker_bps) = parse_user_fee_rates_bps(&raw)?;
        let _ = self.db.set_config_value(
            "live_exchange_maker_fee_bps",
            &format!("{:.4}", round_dp(maker_bps, 4)),
            Some("Synced account Hyperliquid maker fee tier in bps (0 falls back to static maker_fee_bps; read-only)"),
        );
        let _ = self.db.set_config_value(
            "live_exchange_taker_fee_bps",
            &format!("{:.4}", round_dp(taker_bps, 4)),
            Some("Synced account Hyperliquid taker fee tier in bps (0 falls back to static taker_fee_bps; read-only)"),
        );
        Some((maker_bps, taker_bps))
    }

    fn sync_paper_funding_events(&self, now_ms_val: i64) -> Result<usize> {
        const HOUR_MS: i64 = 3_600_000;
        const MAX_PAPER_FUNDING_BACKFILL_HOURS: i64 = 720;

        let current_hour_ms = now_ms_val - now_ms_val.rem_euclid(HOUR_MS);
        let held = self.db.get_assets_held()?;
        if held.is_empty() {
            return Ok(0);
        }
        let assets: HashMap<i64, ExchangeAsset> = self
            .db
            .list_exchange_assets()?
            .into_iter()
            .map(|a| (a.id, a))
            .collect();

        let mut paper_events: Vec<(i64, FundingEventUpsertInput)> = Vec::new();
        for h in held {
            let Some(asset) = assets.get(&h.asset_id) else {
                continue;
            };
            let Ok(side) = OrderSide::parse(&h.side) else {
                continue;
            };
            if !asset.funding_rate_hourly.is_finite() || asset.funding_rate_hourly.abs() <= 1e-12 {
                continue;
            }
            let opened_ts = h.opened_at_ms.unwrap_or(h.created_at_ms);
            let first_hour_ms = (opened_ts - opened_ts.rem_euclid(HOUR_MS)) + HOUR_MS;
            if first_hour_ms > current_hour_ms {
                continue;
            }
            let min_hour_ms = current_hour_ms - MAX_PAPER_FUNDING_BACKFILL_HOURS * HOUR_MS;
            let start_hour_ms = first_hour_ms.max(min_hour_ms);
            let ref_px = if asset.mark_price > 0.0 {
                asset.mark_price
            } else if h.mark_price > 0.0 {
                h.mark_price
            } else {
                h.entry_price
            };
            let mark_notional = (ref_px * h.quantity).abs();
            if !mark_notional.is_finite() || mark_notional <= 0.0 {
                continue;
            }
            let cashflow_usdc =
                round_dp(-side.sign() * asset.funding_rate_hourly * mark_notional, 4);
            if cashflow_usdc.abs() <= 1e-12 {
                continue;
            }
            let pos_szi = round_dp(h.quantity * side.sign(), 8);

            let mut hour_ts_ms = start_hour_ms;
            while hour_ts_ms <= current_hour_ms {
                paper_events.push((
                    h.position_id,
                    FundingEventUpsertInput {
                        asset_id: h.asset_id,
                        coin: asset.coin.clone(),
                        event_time_ms: hour_ts_ms,
                        usdc: cashflow_usdc,
                        remaining_usdc: 0.0,
                        position_szi: pos_szi,
                        funding_rate: asset.funding_rate_hourly,
                        dedup_key: format!("paper:{}:{}", h.position_id, hour_ts_ms),
                    },
                ));
                hour_ts_ms = hour_ts_ms.saturating_add(HOUR_MS);
            }
        }

        if paper_events.is_empty() {
            return Ok(0);
        }
        self.db.accrue_paper_funding_events(&paper_events)
    }

    /// Incrementally fetches watermarked `userFunding` (`startTime = max(event_time_ms) - 3_600_000`,
    /// `max_pages = 5` periodic / `25` startup backfill) and inserts ONLY new `funding_events`
    /// without clobbering already-settled `remaining_usdc`, or accrues deterministic UTC `:00`
    /// hourly funding for `PAPER` mode `OPEN` positions.
    pub fn sync_funding_events(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        is_startup: bool,
    ) -> Result<usize> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return self.sync_paper_funding_events(now_ms_val);
        }
        let wallet_trimmed = cfg.get_str("wallet_address", "").trim().to_string();
        if wallet_trimmed.is_empty() {
            return Ok(0);
        }

        let existing = self.db.list_all_funding_events().unwrap_or_default();
        let existing_keys: HashSet<String> = existing.iter().map(|e| e.dedup_key.clone()).collect();
        let max_ev_ts = existing.iter().map(|e| e.event_time_ms).max();

        let (start_ms, max_pages) = match max_ev_ts {
            Some(wm) if !is_startup || !existing.is_empty() => (
                wm.saturating_sub(USER_FUNDING_WATERMARK_OVERLAP_MS).max(0),
                if is_startup {
                    USER_FUNDING_STARTUP_MAX_PAGES
                } else {
                    USER_FUNDING_PERIODIC_MAX_PAGES
                },
            ),
            _ => (
                now_ms_val.saturating_sub(90 * 86_400_000).max(0),
                USER_FUNDING_STARTUP_MAX_PAGES,
            ),
        };

        let ws_key = (
            self.db.db_name().to_string(),
            wallet_trimmed.to_ascii_lowercase(),
        );
        let mut raw_events = if !is_startup {
            if let Some(ref ws) = self.ws_cache {
                if ws.is_user_ws_ready_for_reconcile() {
                    let cur_reconn = ws.user_reconnect_count();
                    let skip_rest = self
                        .last_funding_rest_state
                        .lock()
                        .get(&ws_key)
                        .is_some_and(|&(last_ms, last_reconn)| {
                            last_reconn == cur_reconn
                                && last_ms > 0
                                && now_ms_val.saturating_sub(last_ms)
                                    < USER_FUNDING_WS_REST_VERIFY_MS
                        });
                    if skip_rest {
                        ws.get_recent_user_fundings().0
                    } else {
                        let fetched = fetch_user_funding_windowed(
                            &self.http,
                            &wallet_trimmed,
                            start_ms,
                            max_pages,
                            cfg,
                        )?;
                        self.last_funding_rest_state
                            .lock()
                            .insert(ws_key, (now_ms_val, cur_reconn));
                        fetched
                    }
                } else {
                    fetch_user_funding_windowed(
                        &self.http,
                        &wallet_trimmed,
                        start_ms,
                        max_pages,
                        cfg,
                    )?
                }
            } else {
                fetch_user_funding_windowed(&self.http, &wallet_trimmed, start_ms, max_pages, cfg)?
            }
        } else {
            let fetched =
                fetch_user_funding_windowed(&self.http, &wallet_trimmed, start_ms, max_pages, cfg)?;
            if let Some(ref ws) = self.ws_cache {
                self.last_funding_rest_state
                    .lock()
                    .insert(ws_key, (now_ms_val, ws.user_reconnect_count()));
            }
            fetched
        };

        if let Some(ref ws) = self.ws_cache {
            if ws.is_user_ws_connected() {
                let (ws_fundings, _) = ws.get_recent_user_fundings();
                if !ws_fundings.is_empty() && !raw_events.is_empty() {
                    raw_events.extend(ws_fundings);
                } else if raw_events.is_empty() {
                    raw_events = ws_fundings;
                }
            }
        }
        if raw_events.is_empty() {
            return Ok(0);
        }

        let parsed = parse_raw_funding_events(&raw_events);
        if parsed.is_empty() {
            return Ok(0);
        }

        let mut asset_id_by_coin: HashMap<String, i64> = HashMap::new();
        for a in self.db.list_exchange_assets().unwrap_or_default() {
            asset_id_by_coin.insert(a.coin.to_ascii_uppercase(), a.id);
        }

        let mut new_inputs: Vec<FundingEventUpsertInput> = Vec::new();
        for ev in parsed {
            if existing_keys.contains(&ev.dedup_key) {
                continue;
            }
            let coin_upper = ev.coin.to_ascii_uppercase();
            let asset_id = if let Some(&aid) = asset_id_by_coin.get(&coin_upper) {
                aid
            } else {
                let Ok(aid) = self.db.upsert_exchange_asset(&AssetUpsertInput {
                    raw_symbol_or_coin: ev.coin.clone(),
                    asset_index: 0,
                    sz_decimals: 2,
                    tick_size: 0.01,
                    lot_step_size: 0.01,
                    max_leverage: 20.0,
                    mark_price: 1.0,
                    mid_price: 1.0,
                    oracle_price: 1.0,
                    funding_rate_hourly: ev.funding_rate,
                    day_volume_usdc: 0.0,
                    open_interest_usdc: 0.0,
                    prev_day_price: 1.0,
                    is_core_eligible: false,
                    is_event_eligible: false,
                    is_priority_watched: false,
                }) else {
                    continue;
                };
                asset_id_by_coin.insert(coin_upper, aid);
                aid
            };
            new_inputs.push(FundingEventUpsertInput {
                asset_id,
                coin: ev.coin,
                event_time_ms: ev.time_ms,
                usdc: ev.usdc,
                remaining_usdc: ev.usdc,
                position_szi: ev.szi,
                funding_rate: ev.funding_rate,
                dedup_key: ev.dedup_key,
            });
        }

        if new_inputs.is_empty() {
            return Ok(0);
        }
        self.db.upsert_funding_events_batch(&new_inputs)
    }

    pub fn sync_portfolio_pnl_if_due(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
    ) -> Option<CachedHlPortfolioPnl> {
        self.sync_portfolio_pnl_with_interval(cfg, now_ms_val, VAULT_SYNC_MIN_INTERVAL_MS)
    }

    pub fn sync_portfolio_pnl_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<CachedHlPortfolioPnl> {
        if !cfg.execution_mode().is_live_or_testnet() {
            return None;
        }
        let wallet_trimmed = cfg.get_str("wallet_address", "").trim().to_string();
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

        let raw_portfolio = match self.http.fetch_user_portfolio(&wallet_trimmed, cfg) {
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
                    Some("Cached Hyperliquid portfolio perpDay/perpWeek/perpMonth/perpAllTime PnL snapshot JSON synced by auxiliary worker (read-only)"),
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
        let wallet_trimmed = cfg.get_str("wallet_address", "").trim().to_string();
        let configured_vault_trimmed = cfg
            .get_str("hyperliquid_vault_address", "")
            .trim()
            .to_string();
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
                    .eq_ignore_ascii_case(&configured_vault_trimmed);
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
            guard.insert(key, now_ms_val);
        }

        let raw_equities = if wallet_trimmed.is_empty() {
            Vec::new()
        } else {
            match self.http.fetch_user_vault_equities(&wallet_trimmed, cfg) {
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

        let mut vaults: Vec<VaultPositionInfo> =
            Vec::with_capacity(ordered_addrs.len().min(MAX_VAULTS_TO_FETCH));
        let mut raw_total_vault_eq = 0.0_f64;

        for norm_addr in ordered_addrs.into_iter().take(MAX_VAULTS_TO_FETCH) {
            let fallback = equity_fallback_by_addr.get(&norm_addr);
            let query_addr = fallback
                .map(|(orig, _, _)| orig.as_str())
                .unwrap_or(&configured_vault_trimmed);
            let details_opt = match self
                .http
                .fetch_vault_details(query_addr, &wallet_trimmed, cfg)
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
            configured_vault_address: configured_vault_trimmed,
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
                    Some("Cached Hyperliquid vault portfolio snapshot JSON synced by auxiliary worker (read-only; segregated from perp margin)"),
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

fn fetch_user_funding_windowed(
    http: &HyperliquidHttpClient,
    user_wallet: &str,
    start_time_ms: i64,
    max_pages: usize,
    cfg: &ConfigMap,
) -> aegis_core::Result<Vec<Value>> {
    let mut cursor_ms = start_time_ms.max(0);
    let trimmed_user = user_wallet.trim();
    let max_pages = max_pages.max(1);
    let mut out: Vec<Value> = Vec::new();
    let mut seen: HashSet<(i64, String, String, String, String)> = HashSet::new();
    let opts = RequestOptions::low_priority_sync().with_timeout_sec(http.timeout_sec.min(8));

    for page_idx in 0..max_pages {
        let raw = match http.post_info_with_options(
            &json!({
                "type": "userFunding",
                "user": trimmed_user,
                "startTime": cursor_ms,
            }),
            cfg,
            opts,
        ) {
            Ok(v) => v,
            Err(e) => {
                if page_idx == 0 {
                    return Err(e);
                }
                tracing::warn!(
                    page = page_idx,
                    cursor_ms,
                    error = %e,
                    "Hyperliquid userFunding pagination interrupted; returning partial ledger"
                );
                break;
            }
        };
        let page = raw.as_array().cloned().unwrap_or_default();
        let page_len = page.len();
        if page_len == 0 {
            break;
        }
        let mut max_time_ms: Option<i64> = None;
        let mut added = 0usize;
        for ev in page {
            if let Some(t) = parse_hl_i64_opt(ev.get("time")) {
                max_time_ms = Some(max_time_ms.map_or(t, |m| m.max(t)));
            }
            let delta = ev.get("delta");
            let time_ms = parse_hl_i64_opt(ev.get("time")).unwrap_or(0);
            let coin = delta
                .and_then(|d| d.get("coin"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_ascii_uppercase();
            let usdc = delta
                .and_then(|d| d.get("usdc"))
                .map(|v| v.to_string())
                .unwrap_or_default();
            let hash = ev
                .get("hash")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            let n_val = ev
                .get("n")
                .or_else(|| delta.and_then(|d| d.get("nSamples")))
                .map(|v| v.to_string())
                .unwrap_or_default();
            if seen.insert((time_ms, coin, usdc, hash, n_val)) {
                out.push(ev);
                added += 1;
            }
        }
        if page_len < HL_USER_FUNDING_PAGE_LIMIT {
            break;
        }
        let Some(page_max) = max_time_ms else {
            break;
        };
        if page_max > cursor_ms {
            cursor_ms = page_max;
        } else if page_max == cursor_ms {
            cursor_ms = page_max.saturating_add(1);
        } else if added > 0 {
            cursor_ms = cursor_ms.saturating_add(1);
        } else {
            break;
        }
    }
    Ok(out)
}

fn parse_raw_funding_events(raw_events: &[Value]) -> Vec<ParsedHlFunding> {
    let mut out = Vec::with_capacity(raw_events.len());
    for ev in raw_events {
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
        let usdc = parse_hl_f64(delta.get("usdc"));
        if f_time <= 0 || usdc.abs() <= 1e-12 {
            continue;
        }
        let szi = parse_hl_f64(delta.get("szi"));
        let funding_rate = parse_hl_f64(delta.get("fundingRate"));
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
        let dedup_key = funding_dedup_key(
            f_time,
            &coin_upper,
            usdc,
            szi,
            raw_hash,
            n_suffix.as_deref(),
        );
        out.push(ParsedHlFunding {
            coin: norm_coin,
            time_ms: f_time,
            usdc,
            szi,
            funding_rate,
            dedup_key,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock_guard::is_execution_lock_held_on_current_thread;
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
    use std::sync::Arc;

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
        assert!((info.equity_usdc - 202.40).abs() < 1e-6);
        assert!((info.pnl_usdc - 0.92).abs() < 1e-6);
        assert!((info.apr_pct - 8.14).abs() < 1e-6);
        assert!(!info.is_locked);

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
        assert!((info_fb.equity_usdc - 50.0).abs() < 1e-6);
        assert!(info_fb.is_locked);
        assert!(info_fb.is_closed);
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
    }

    #[test]
    fn test_parse_vault_position_info_enriched_facts_fallback_and_truncation() {
        let hlp_addr = "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303";
        let now = 1_795_000_000_000_i64;
        let long_utf8_desc = format!("  {}  ", "é".repeat(540));
        let user_vault_details = json!({
            "name": "Aegis Alpha User Vault",
            "vaultAddress": hlp_addr,
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
        let info = parse_vault_position_info(hlp_addr, &user_vault_details, None, now);
        assert_eq!(info.description.chars().count(), 500);
        assert!((info.tvl_usdc - 4000.0).abs() < 1e-6);
        assert!((info.leader_commission_pct - 10.0).abs() < 1e-6);
    }

    #[test]
    fn test_auxiliary_worker_caps_vaults_syncs_fees_extra_agents_and_runs_unlocked() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();

        let test_pk = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let agent_addr = derive_evm_address_from_private_key(test_pk).unwrap();
        let now = now_ms();

        let vault_details_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let vd_srv = vault_details_count.clone();
        let lock_held_during_http = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lock_srv = lock_held_during_http.clone();

        let server = MockHttpServer::start(move |req| {
            if is_execution_lock_held_on_current_thread() {
                lock_srv.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let typ = req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("");
            match typ {
                "extraAgents" => MockHttpResponse::ok_json(&json!([
                    { "address": agent_addr, "name": "aegis", "validUntil": now + 2 * 86_400_000 }
                ])),
                "userFees" => MockHttpResponse::ok_json(&json!({
                    "userAddRate": "0.00012",
                    "userCrossRate": "0.00038"
                })),
                "userVaultEquities" => {
                    let list: Vec<Value> = (1..=15)
                        .map(|i| {
                            json!({
                                "vaultAddress": format!("0x{:040x}", i),
                                "equity": "100.0",
                                "lockedUntilTimestamp": 0
                            })
                        })
                        .collect();
                    MockHttpResponse::ok_json(&Value::Array(list))
                }
                "vaultDetails" => {
                    vd_srv.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let addr = req
                        .json_body
                        .get("vaultAddress")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    MockHttpResponse::ok_json(&json!({
                        "name": "Vault",
                        "vaultAddress": addr,
                        "leader": "0x1111111111111111111111111111111111111111",
                        "followerState": { "vaultEquity": "100.0", "pnl": "1.0", "allTimePnl": "2.0" }
                    }))
                }
                "portfolio" => MockHttpResponse::ok_json(&json!([
                    ["perpDay", { "pnlHistory": [[now - 60_000, "12.5"]], "vlm": "1000.0" }]
                ])),
                "userFunding" => MockHttpResponse::ok_json(&json!([
                    {
                        "time": now - 10_000,
                        "hash": "0xaux1",
                        "delta": { "type": "funding", "coin": "BTC", "usdc": "1.25", "szi": "0.1", "fundingRate": "0.0001" }
                    }
                ])),
                _ => MockHttpResponse::ok_json(&json!([])),
            }
        });

        db.set_config_value("execution_mode", "LIVE", None).unwrap();
        db.set_config_value("hyperliquid_mainnet_url", &server.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
            .unwrap();
        db.set_config_value(
            "wallet_address",
            "0x9999999999999999999999999999999999999999",
            None,
        )
        .unwrap();
        db.set_config_value("private_key", test_pk, None).unwrap();

        let worker = AuxiliarySyncWorker::new(db.clone(), HyperliquidHttpClient::new(2));
        let rep = worker.run_once(true);
        assert!(!lock_held_during_http.load(std::sync::atomic::Ordering::SeqCst));
        assert!(rep.extra_agent_warning.is_some());
        assert!(rep.user_fees_synced);
        assert!(rep.portfolio_synced);
        assert_eq!(rep.vaults_synced, 10);
        assert_eq!(
            vault_details_count.load(std::sync::atomic::Ordering::SeqCst),
            10
        );
        assert_eq!(rep.funding_events_inserted, 1);

        let cfg_after = db.load_config_or_default();
        assert!((cfg_after.get_f64("live_exchange_maker_fee_bps", 0.0) - 1.2).abs() < 1e-6);
        assert!((cfg_after.get_f64("live_exchange_taker_fee_bps", 0.0) - 3.8).abs() < 1e-6);
    }

    #[test]
    fn test_paper_mode_hourly_utc_funding_accrual_and_idempotency() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();
        db.set_config_value("execution_mode", "PAPER", None)
            .unwrap();

        let sol_id = db
            .upsert_exchange_asset(&aegis_db::AssetUpsertInput {
                raw_symbol_or_coin: "SOL".to_string(),
                asset_index: 3,
                sz_decimals: 2,
                tick_size: 0.01,
                lot_step_size: 0.01,
                max_leverage: 20.0,
                mark_price: 100.0,
                mid_price: 100.0,
                oracle_price: 100.0,
                funding_rate_hourly: 0.0005, // +5 bps/hr -> Long pays -0.50/hr on 10 * 100 = $1000
                day_volume_usdc: 20_000_000.0,
                open_interest_usdc: 5_000_000.0,
                prev_day_price: 99.0,
                is_core_eligible: true,
                is_event_eligible: false,
                is_priority_watched: true,
            })
            .unwrap();

        let now = now_ms();
        let current_hour = now - now.rem_euclid(3_600_000);
        // Opened 2h+ ago across exactly 2 completed UTC :00 hours (current_hour - 3_600_000 and current_hour)
        let opened_ts = current_hour - 3_600_000 - 300_000;
        let pid = db
            .create_working_order(&aegis_db::WorkingOrderInput {
                asset_id: sol_id,
                client_order_id: "v2-paper-fund-1".to_string(),
                side: aegis_core::OrderSide::Long,
                limit_price: 100.0,
                requested_quantity: 10.0,
                reserved_margin_usdc: 500.0,
                portfolio_weight: 0.1,
                s1_weight_contrib: 0.1,
                s2_weight_contrib: 0.0,
                s3_weight_contrib: 0.0,
                protective_stop_px: 95.0,
                take_profit_px: Some(110.0),
                exchange_order_id: None,
                reason: "PAPER_FUND_TEST".to_string(),
            })
            .unwrap();
        db.promote_working_to_open(
            pid,
            100.0,
            10.0,
            0.45,
            95.0,
            Some(110.0),
            None,
            "PAPER_FUND_FILL",
        )
        .unwrap();
        // Set opened_at_ms to 2 hours back via sync_open_position or direct? Wait: sync_open_position_from_exchange_v2 updates opened_at_ms!
        let _ = db
            .sync_open_position_from_exchange_v2(&aegis_db::ExchangeOpenPositionSyncInput {
                asset_id: sol_id,
                coin: "SOL",
                side: aegis_core::OrderSide::Long,
                entry_price: 100.0,
                mark_price: 100.0,
                quantity: 10.0,
                committed_notional_usdc: 1000.0,
                unrealized_pnl_usdc: 0.0,
                portfolio_weight: 0.1,
                protective_stop_px: 95.0,
                take_profit_px: 110.0,
                funding_accrued_usdc: 0.0,
                opened_at_ms: Some(opened_ts),
            })
            .unwrap();

        let worker = AuxiliarySyncWorker::new(db.clone(), HyperliquidHttpClient::new(2));
        let rep1 = worker.run_once(true);
        assert_eq!(rep1.funding_events_inserted, 2);

        let pos1 = db.get_position_by_id(pid).unwrap().unwrap();
        // - (10.0 * 100.0 * 0.0005) * 2 = -1.0 USDC
        assert!((pos1.funding_accrued_usdc - (-1.0)).abs() < 1e-4);

        // Second run within same UTC hour must be strictly idempotent (0 new inserts)
        let rep2 = worker.run_once(true);
        assert_eq!(rep2.funding_events_inserted, 0);
        let pos2 = db.get_position_by_id(pid).unwrap().unwrap();
        assert!((pos2.funding_accrued_usdc - (-1.0)).abs() < 1e-4);
    }
}
