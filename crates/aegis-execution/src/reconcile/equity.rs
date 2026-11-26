//! Exchange equity, `userAbstraction`, and unified spot-USDC reconciliation helpers
//! (`aegis-execution::reconcile::equity`, `R14-H2`, `R14-W4`, `P0-Risk`).

use crate::http_client::{HyperliquidHttpClient, RequestOptions};
use crate::lock_guard::acquire_execution_lock;
use aegis_core::{
    is_unified_account_mode, normalize_hyperliquid_symbol, parse_hl_f64, parse_hl_f64_opt,
    parse_hl_i64_opt, round_dp, ConfigMap, Result,
};
use aegis_db::DatabaseManager;
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

pub const VAULT_SYNC_MIN_INTERVAL_MS: i64 = 60_000;
pub const ABSTRACTION_SYNC_MIN_INTERVAL_MS: i64 = 300_000;
pub const DEFAULT_MODE_ABSTRACTION_SYNC_MIN_INTERVAL_MS: i64 = 60_000;
pub const UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS: i64 = 15_000;
pub const EQUITY_CLIFF_DROP_PCT: f64 = 0.25;
pub const EQUITY_CLIFF_MIN_BASELINE_USDC: f64 = 10.0;

/// Minimum cooldown (`60_000 ms`) after an initial forced cliff refresh before another forced
/// (`interval = 0`) REST cache bypass is permitted (`R14-W4`).
pub const CLIFF_ESCALATION_BASE_COOLDOWN_MS: i64 = 60_000;

/// Maximum exponential backoff (`300_000 ms` / 5 minutes) between consecutive forced cliff
/// REST cache bypasses (`R14-W4`).
pub const CLIFF_ESCALATION_MAX_BACKOFF_MS: i64 = 300_000;

/// Summary window (`300_000 ms` / 5 minutes) for repeated identical equity-cliff `WARN`
/// deduplication (`R14-W4`).
pub const CLIFF_WARN_DEDUP_WINDOW_MS: i64 = 300_000;

/// Hard upper bound (`<= 256`) on all per-`(db_name, wallet)` in-memory reconciliation maps (`R14-W4`).
pub const MAX_EQUITY_STATE_MAP_ENTRIES: usize = 256;

type ThrottleKey = (String, String);

static LAST_SPOT_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<ThrottleKey, i64>>> = OnceLock::new();
static LAST_ABSTRACTION_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<ThrottleKey, i64>>> =
    OnceLock::new();
static CLIFF_ESCALATION_STATE: OnceLock<Mutex<HashMap<ThrottleKey, CliffEscalationEntry>>> =
    OnceLock::new();
static CLIFF_WARN_DEDUP_STATE: OnceLock<Mutex<HashMap<ThrottleKey, CliffWarnDedupEntry>>> =
    OnceLock::new();

/// Distinct like-for-like equity-cliff trigger classifications (`R14-W4`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EquityCliffKind {
    /// Cached mode was non-unified (`"default"`), and `perp_eq` dropped `>25%` below `prev_live_eq`
    /// (catching `default -> unifiedAccount` collateral sweeps to spot).
    DefaultModeCutover,
    /// Current `perp_eq` dropped `>25%` below `prev_perp_eq` (`live_exchange_perp_margin_equity_usdc`).
    PerpToPerp,
    /// Account is in `unifiedAccount` / `portfolioMargin` mode and candidate unified wallet equity
    /// dropped `>25%` below `prev_live_eq`.
    UnifiedToUnified,
}

impl EquityCliffKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DefaultModeCutover => "default_mode_cutover",
            Self::PerpToPerp => "perp_to_perp",
            Self::UnifiedToUnified => "unified_to_unified",
        }
    }
}

/// Outcome of recording a repeated equity-cliff `WARN` occurrence (`R14-W4`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarnDedupDecision {
    /// First occurrence for this cliff key (or first occurrence after state transition / clear).
    EmitFirst,
    /// Identical repeat within the 5-minute window; suppress duplicate `WARN` emission.
    Suppress { suppressed_repeats: u64 },
    /// 5-minute summary interval elapsed while cliff remains active; emit with `(suppressed_repeats = N)`.
    EmitSummary { suppressed_repeats: u64 },
}

impl WarnDedupDecision {
    pub fn should_emit(self) -> bool {
        matches!(self, Self::EmitFirst | Self::EmitSummary { .. })
    }

    pub fn suppressed_repeats(self) -> u64 {
        match self {
            Self::EmitFirst => 0,
            Self::Suppress { suppressed_repeats } | Self::EmitSummary { suppressed_repeats } => {
                suppressed_repeats
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct CliffEscalationEntry {
    last_bypass_ms: i64,
    next_allowed_bypass_ms: i64,
    consecutive_triggers: u32,
    last_verification_failed: bool,
    last_touched_ms: i64,
}

/// Read-only snapshot of a `(db_name, wallet)` cliff escalation state (`R14-W4`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CliffEscalationSnapshot {
    pub last_bypass_ms: i64,
    pub next_allowed_bypass_ms: i64,
    pub consecutive_triggers: u32,
    pub last_verification_failed: bool,
}

#[derive(Debug, Clone)]
struct CliffWarnDedupEntry {
    signature: (EquityCliffKind, bool),
    window_start_ms: i64,
    last_seen_ms: i64,
    suppressed_repeats: u64,
}

#[derive(Debug, Clone, Copy)]
enum EscalationDecision {
    BypassGranted,
    InCooldown { active_backoff_ms: i64 },
}

/// Computes the explicit escalation cooldown / exponential backoff (`60s -> 120s -> 240s -> 300s`)
/// after the `consecutive_triggers`-th cliff verification bypass (`R14-W4`).
pub fn compute_cliff_escalation_backoff_ms(consecutive_triggers: u32) -> i64 {
    if consecutive_triggers == 0 {
        return 0;
    }
    let shift = consecutive_triggers.saturating_sub(1).min(4);
    (CLIFF_ESCALATION_BASE_COOLDOWN_MS.saturating_mul(1_i64 << shift))
        .min(CLIFF_ESCALATION_MAX_BACKOFF_MS)
}

fn insert_bounded_timestamp(map: &mut HashMap<ThrottleKey, i64>, key: ThrottleKey, ts: i64) {
    if !map.contains_key(&key) && map.len() >= MAX_EQUITY_STATE_MAP_ENTRIES {
        if let Some(oldest_key) = map
            .iter()
            .min_by_key(|(_, &last_ts)| last_ts)
            .map(|(k, _)| k.clone())
        {
            map.remove(&oldest_key);
        }
    }
    map.insert(key, ts);
}

fn peek_failed_escalation_backoff_floor(key: &ThrottleKey, ts: i64) -> i64 {
    let map = CLIFF_ESCALATION_STATE.get_or_init(|| Mutex::new(HashMap::new()));
    let guard = map.lock();
    let Some(entry) = guard.get(key) else {
        return 0;
    };
    if entry.last_verification_failed
        && entry.next_allowed_bypass_ms > 0
        && ts >= entry.last_bypass_ms
        && ts < entry.next_allowed_bypass_ms
    {
        entry
            .next_allowed_bypass_ms
            .saturating_sub(entry.last_bypass_ms)
            .max(CLIFF_ESCALATION_BASE_COOLDOWN_MS)
    } else {
        0
    }
}

fn try_acquire_cliff_escalation_bypass(key: &ThrottleKey, ts: i64) -> EscalationDecision {
    let map = CLIFF_ESCALATION_STATE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock();
    if let Some(entry) = guard.get_mut(key) {
        entry.last_touched_ms = ts;
        if entry.next_allowed_bypass_ms > 0
            && ts >= entry.last_bypass_ms
            && ts < entry.next_allowed_bypass_ms
        {
            let active_backoff_ms = entry
                .next_allowed_bypass_ms
                .saturating_sub(entry.last_bypass_ms)
                .max(CLIFF_ESCALATION_BASE_COOLDOWN_MS);
            return EscalationDecision::InCooldown { active_backoff_ms };
        }
        entry.consecutive_triggers = entry.consecutive_triggers.saturating_add(1);
        let backoff_ms = compute_cliff_escalation_backoff_ms(entry.consecutive_triggers);
        entry.last_bypass_ms = ts;
        entry.next_allowed_bypass_ms = ts.saturating_add(backoff_ms);
        return EscalationDecision::BypassGranted;
    }

    if guard.len() >= MAX_EQUITY_STATE_MAP_ENTRIES {
        if let Some(oldest_key) = guard
            .iter()
            .min_by_key(|(_, v)| v.last_touched_ms)
            .map(|(k, _)| k.clone())
        {
            guard.remove(&oldest_key);
        }
    }
    let backoff_ms = compute_cliff_escalation_backoff_ms(1);
    guard.insert(
        key.clone(),
        CliffEscalationEntry {
            last_bypass_ms: ts,
            next_allowed_bypass_ms: ts.saturating_add(backoff_ms),
            consecutive_triggers: 1,
            last_verification_failed: false,
            last_touched_ms: ts,
        },
    );
    EscalationDecision::BypassGranted
}

fn complete_cliff_escalation_attempt(key: &ThrottleKey, verification_failed: bool) {
    let map = CLIFF_ESCALATION_STATE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock();
    if let Some(entry) = guard.get_mut(key) {
        entry.last_verification_failed = verification_failed;
    }
}

fn reset_cliff_escalation_on_clear(key: &ThrottleKey, ts: i64) {
    let map = CLIFF_ESCALATION_STATE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock();
    if let Some(entry) = guard.get_mut(key) {
        entry.consecutive_triggers = 0;
        entry.last_verification_failed = false;
        entry.last_touched_ms = ts;
    }
}

fn record_cliff_warn_occurrence(
    key: &ThrottleKey,
    cliff_kind: EquityCliffKind,
    refuse_equity_overwrite: bool,
    ts: i64,
) -> (WarnDedupDecision, Option<u64>) {
    let sig = (cliff_kind, refuse_equity_overwrite);
    let map = CLIFF_WARN_DEDUP_STATE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock();
    if let Some(entry) = guard.get_mut(key) {
        if entry.signature != sig {
            let prior = (entry.suppressed_repeats > 0).then_some(entry.suppressed_repeats);
            entry.signature = sig;
            entry.window_start_ms = ts;
            entry.last_seen_ms = ts;
            entry.suppressed_repeats = 0;
            return (WarnDedupDecision::EmitFirst, prior);
        }
        entry.last_seen_ms = ts;
        if ts < entry.window_start_ms {
            entry.window_start_ms = ts;
        }
        if ts.saturating_sub(entry.window_start_ms) >= CLIFF_WARN_DEDUP_WINDOW_MS {
            let suppressed = entry.suppressed_repeats;
            entry.window_start_ms = ts;
            entry.suppressed_repeats = 0;
            return (
                WarnDedupDecision::EmitSummary {
                    suppressed_repeats: suppressed,
                },
                None,
            );
        }
        entry.suppressed_repeats = entry.suppressed_repeats.saturating_add(1);
        return (
            WarnDedupDecision::Suppress {
                suppressed_repeats: entry.suppressed_repeats,
            },
            None,
        );
    }

    if guard.len() >= MAX_EQUITY_STATE_MAP_ENTRIES {
        if let Some(oldest_key) = guard
            .iter()
            .min_by_key(|(_, v)| v.last_seen_ms)
            .map(|(k, _)| k.clone())
        {
            guard.remove(&oldest_key);
        }
    }
    guard.insert(
        key.clone(),
        CliffWarnDedupEntry {
            signature: sig,
            window_start_ms: ts,
            last_seen_ms: ts,
            suppressed_repeats: 0,
        },
    );
    (WarnDedupDecision::EmitFirst, None)
}

fn clear_cliff_warn_state(key: &ThrottleKey) -> Option<u64> {
    let map = CLIFF_WARN_DEDUP_STATE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock();
    let removed = guard.remove(key)?;
    (removed.suppressed_repeats > 0).then_some(removed.suppressed_repeats)
}

/// Returns the current number of entries in the bounded cliff escalation state map (`<= 256`).
pub fn cliff_escalation_state_len() -> usize {
    CLIFF_ESCALATION_STATE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .len()
}

/// Returns the current number of entries in the bounded cliff WARN deduplication map (`<= 256`).
pub fn cliff_warn_dedup_state_len() -> usize {
    CLIFF_WARN_DEDUP_STATE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .len()
}

/// Inspects the current cliff escalation entry for `(db_name, wallet)`.
pub fn inspect_cliff_escalation_state(
    db_name: &str,
    wallet: &str,
) -> Option<CliffEscalationSnapshot> {
    let key: ThrottleKey = (db_name.to_string(), wallet.trim().to_ascii_lowercase());
    let map = CLIFF_ESCALATION_STATE.get_or_init(|| Mutex::new(HashMap::new()));
    let guard = map.lock();
    guard.get(&key).map(|e| CliffEscalationSnapshot {
        last_bypass_ms: e.last_bypass_ms,
        next_allowed_bypass_ms: e.next_allowed_bypass_ms,
        consecutive_triggers: e.consecutive_triggers,
        last_verification_failed: e.last_verification_failed,
    })
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
/// Returns `None` if `"balances"` is missing or not a JSON array.
pub fn parse_spot_usdc_balance(spot_state: &Value) -> Option<f64> {
    parse_spot_usdc_details(spot_state).map(|(total, _, _)| total)
}

/// Extracts normalized uppercase open coin symbols and signed `szi` from `clearinghouseState`.
pub fn extract_open_coins_and_szi(ch_state: &Value) -> (HashSet<String>, HashMap<String, f64>) {
    let mut open_coins = HashSet::new();
    let mut open_szi_by_coin = HashMap::new();
    if let Some(asset_positions) = ch_state.get("assetPositions").and_then(Value::as_array) {
        for item in asset_positions {
            let pos = item.get("position").unwrap_or(item);
            let raw_coin = pos.get("coin").and_then(Value::as_str).unwrap_or("").trim();
            let szi = parse_hl_f64(pos.get("szi"));
            if !raw_coin.is_empty() && szi.abs() > 1e-9 {
                if let Ok((_, coin)) = normalize_hyperliquid_symbol(raw_coin) {
                    let coin_upper = coin.to_ascii_uppercase();
                    open_coins.insert(coin_upper.clone());
                    open_szi_by_coin.insert(coin_upper, szi);
                }
            }
        }
    }
    (open_coins, open_szi_by_coin)
}

/// Extracts `(perp_eq, margin_used, withdrawable)` from Hyperliquid `clearinghouseState`.
pub fn extract_perp_equity_metrics(ch_state: &Value) -> (f64, f64, f64) {
    let m_eq = parse_hl_f64(ch_state.pointer("/marginSummary/accountValue"));
    let cm_eq = parse_hl_f64(ch_state.pointer("/crossMarginSummary/accountValue"));
    let w_eq = parse_hl_f64(ch_state.get("withdrawable"));
    let perp_eq = if m_eq > 0.0 {
        m_eq
    } else if cm_eq > 0.0 {
        cm_eq
    } else {
        w_eq
    };
    let margin_used = parse_hl_f64(ch_state.pointer("/marginSummary/totalMarginUsed"));
    (perp_eq, margin_used, w_eq)
}

#[derive(Debug, Clone)]
pub struct AccountModeFetchOutcome {
    pub mode: Option<String>,
    pub failed: bool,
}

#[derive(Debug, Clone)]
pub struct SpotUsdcFetchOutcome {
    pub details: Option<(f64, f64, Option<f64>)>,
    pub failed: bool,
}

#[derive(Debug, Clone)]
pub struct EquitySnapshotEvaluation {
    pub perp_eq: f64,
    pub margin_used: f64,
    pub account_mode: String,
    pub fresh_account_mode: Option<String>,
    pub fresh_spot: Option<(f64, f64, Option<f64>)>,
    pub effective_wallet_eq: f64,
    pub effective_withdrawable: f64,
    pub equity_cliff_detected: bool,
    pub cliff_kind: Option<EquityCliffKind>,
    pub refuse_equity_overwrite: bool,
    pub perp_drop_pct: f64,
    pub drop_pct: f64,
    pub baseline_eq: f64,
    pub forced_refresh_bypass: bool,
    pub warn_decision: Option<WarnDedupDecision>,
    pub cleared_suppressed_repeats: Option<u64>,
}

pub fn fetch_account_mode_unlocked(
    http: &HyperliquidHttpClient,
    db_name: &str,
    cfg: &ConfigMap,
    now_ms_val: i64,
    min_interval_ms: i64,
) -> AccountModeFetchOutcome {
    if !cfg.execution_mode().is_live_or_testnet() {
        return AccountModeFetchOutcome {
            mode: None,
            failed: false,
        };
    }
    let wallet_trimmed = cfg.get_str("wallet_address", "").trim().to_string();
    if wallet_trimmed.is_empty() {
        return AccountModeFetchOutcome {
            mode: None,
            failed: false,
        };
    }
    let key: ThrottleKey = (db_name.to_string(), wallet_trimmed.to_ascii_lowercase());
    let eff_min_interval = min_interval_ms.max(0);
    {
        let map = LAST_ABSTRACTION_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock();
        if eff_min_interval > 0 {
            if let Some(&last_attempt) = guard.get(&key) {
                if now_ms_val >= last_attempt
                    && now_ms_val.saturating_sub(last_attempt) < eff_min_interval
                {
                    return AccountModeFetchOutcome {
                        mode: None,
                        failed: false,
                    };
                }
            }
        }
        insert_bounded_timestamp(&mut guard, key, now_ms_val);
    }

    match http.fetch_user_abstraction(&wallet_trimmed, cfg) {
        Ok(Some(mode)) => AccountModeFetchOutcome {
            mode: Some(mode),
            failed: false,
        },
        Ok(None) => {
            tracing::warn!(
                "Hyperliquid userAbstraction returned empty/non-string payload; keeping cached account mode"
            );
            AccountModeFetchOutcome {
                mode: None,
                failed: true,
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Hyperliquid userAbstraction fetch failed; keeping cached account mode"
            );
            AccountModeFetchOutcome {
                mode: None,
                failed: true,
            }
        }
    }
}

pub fn fetch_spot_usdc_details_unlocked(
    http: &HyperliquidHttpClient,
    db_name: &str,
    cfg: &ConfigMap,
    now_ms_val: i64,
    min_interval_ms: i64,
) -> SpotUsdcFetchOutcome {
    if !cfg.execution_mode().is_live_or_testnet() {
        return SpotUsdcFetchOutcome {
            details: None,
            failed: false,
        };
    }
    let wallet_trimmed = cfg.get_str("wallet_address", "").trim().to_string();
    if wallet_trimmed.is_empty() {
        return SpotUsdcFetchOutcome {
            details: None,
            failed: false,
        };
    }
    let key: ThrottleKey = (db_name.to_string(), wallet_trimmed.to_ascii_lowercase());
    let eff_min_interval = min_interval_ms.max(0);
    {
        let map = LAST_SPOT_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock();
        if eff_min_interval > 0 {
            if let Some(&last_attempt) = guard.get(&key) {
                if now_ms_val >= last_attempt
                    && now_ms_val.saturating_sub(last_attempt) < eff_min_interval
                {
                    tracing::debug!(
                        elapsed_ms = now_ms_val.saturating_sub(last_attempt),
                        min_interval_ms = eff_min_interval,
                        "Skipping Hyperliquid spot USDC sync (in-memory throttle active)"
                    );
                    return SpotUsdcFetchOutcome {
                        details: None,
                        failed: false,
                    };
                }
            }
            let prev_sync_ms = cfg.get_i64("live_exchange_spot_synced_at_ms", 0);
            if prev_sync_ms > 0
                && now_ms_val >= prev_sync_ms
                && now_ms_val.saturating_sub(prev_sync_ms) < eff_min_interval
            {
                insert_bounded_timestamp(&mut guard, key, prev_sync_ms);
                tracing::debug!(
                    elapsed_ms = now_ms_val.saturating_sub(prev_sync_ms),
                    min_interval_ms = eff_min_interval,
                    "Skipping Hyperliquid spot USDC sync (persisted throttle active)"
                );
                return SpotUsdcFetchOutcome {
                    details: None,
                    failed: false,
                };
            }
        }
        insert_bounded_timestamp(&mut guard, key, now_ms_val);
    }

    let spot_state = match http.fetch_spot_clearinghouse_state(&wallet_trimmed, cfg) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Hyperliquid spotClearinghouseState fetch failed; keeping last known cached spot USDC"
            );
            return SpotUsdcFetchOutcome {
                details: None,
                failed: true,
            };
        }
    };

    let Some((raw_usdc, hold_usdc, avail_after_maint)) = parse_spot_usdc_details(&spot_state)
    else {
        tracing::debug!(
            "Hyperliquid spotClearinghouseState payload missing 'balances' array; keeping cached spot USDC"
        );
        return SpotUsdcFetchOutcome {
            details: None,
            failed: true,
        };
    };

    SpotUsdcFetchOutcome {
        details: Some((
            round_dp(raw_usdc.max(0.0), 2),
            round_dp(hold_usdc.max(0.0), 2),
            avail_after_maint,
        )),
        failed: false,
    }
}

fn compute_candidate_wallet_equity(
    is_unified: bool,
    cached_spot: f64,
    fresh_spot: Option<(f64, f64, Option<f64>)>,
    perp_eq: f64,
    margin_used: f64,
    withdrawable: f64,
) -> (f64, f64) {
    if !is_unified {
        return (perp_eq, withdrawable);
    }
    if let Some((st, sh, avail_opt)) = fresh_spot {
        if st >= 1.0 {
            let free_spot = if sh > 0.0 {
                (st - sh).max(0.0)
            } else {
                avail_opt.unwrap_or(st).max(0.0)
            };
            return (st, free_spot.max(withdrawable));
        }
        return (perp_eq.max(st), withdrawable);
    }
    if cached_spot >= 1.0 {
        let free_spot = (cached_spot - margin_used).max(withdrawable).max(0.0);
        (cached_spot, free_spot)
    } else {
        (perp_eq, withdrawable)
    }
}

/// Evaluates equity, account mode (`userAbstraction`), and unified spot collateral strictly
/// unlocked (`R14-H2`) using like-for-like comparators, bounded REST escalation backoff, and
/// per-key repeated-`WARN` deduplication (`R14-W4`, `P0-Risk`).
pub fn evaluate_equity_snapshot_unlocked(
    http: &HyperliquidHttpClient,
    db_name: &str,
    cfg: &ConfigMap,
    ch_state: &Value,
    open_coins: &HashSet<String>,
    local_has_held: bool,
    ts: i64,
) -> EquitySnapshotEvaluation {
    let (perp_eq, margin_used, withdrawable) = extract_perp_equity_metrics(ch_state);
    let prev_live_eq = cfg.get_f64("live_exchange_equity_usdc", 0.0).max(0.0);
    let prev_perp_eq = cfg
        .get_f64("live_exchange_perp_margin_equity_usdc", 0.0)
        .max(0.0);
    let cached_mode = cfg.get_str("live_exchange_account_mode", "default");
    let was_unified = is_unified_account_mode(&cached_mode);
    let has_held_positions = !open_coins.is_empty() || local_has_held;

    let wallet_lower = cfg
        .get_str("wallet_address", "")
        .trim()
        .to_ascii_lowercase();
    let has_live_wallet = cfg.execution_mode().is_live_or_testnet() && !wallet_lower.is_empty();
    let throttle_key: ThrottleKey = (db_name.to_string(), wallet_lower);

    // 1. Like-for-like pre-fetch checks:
    //    - DefaultModeCutover: `!was_unified` comparing `perp_eq` vs `prev_live_eq`
    //    - PerpToPerp: comparing `perp_eq` vs `prev_perp_eq`
    let perp_to_perp_drop_pct = if prev_perp_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC {
        ((prev_perp_eq - perp_eq) / prev_perp_eq).max(0.0)
    } else {
        0.0
    };
    let perp_cliff = has_held_positions
        && prev_perp_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC
        && perp_to_perp_drop_pct > EQUITY_CLIFF_DROP_PCT;

    let cutover_drop_pct = if !was_unified && prev_live_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC {
        ((prev_live_eq - perp_eq) / prev_live_eq).max(0.0)
    } else {
        0.0
    };
    let cutover_cliff = has_held_positions
        && !was_unified
        && prev_live_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC
        && cutover_drop_pct > EQUITY_CLIFF_DROP_PCT;

    let pre_cliff_kind = if cutover_cliff {
        Some(EquityCliffKind::DefaultModeCutover)
    } else if perp_cliff {
        Some(EquityCliffKind::PerpToPerp)
    } else {
        None
    };

    // 2. Explicit escalation policy decoupling cliff suspicion from unconditional REST bypass
    let mut forced_refresh_bypass = false;
    let mut backoff_floor_ms = if has_live_wallet {
        peek_failed_escalation_backoff_floor(&throttle_key, ts)
    } else {
        0
    };

    if pre_cliff_kind.is_some() && has_live_wallet {
        match try_acquire_cliff_escalation_bypass(&throttle_key, ts) {
            EscalationDecision::BypassGranted => {
                forced_refresh_bypass = true;
                backoff_floor_ms = 0;
            }
            EscalationDecision::InCooldown { active_backoff_ms } => {
                if backoff_floor_ms > 0 {
                    backoff_floor_ms = backoff_floor_ms.max(active_backoff_ms);
                }
            }
        }
    }

    let base_abstraction_interval = if was_unified {
        ABSTRACTION_SYNC_MIN_INTERVAL_MS
    } else {
        DEFAULT_MODE_ABSTRACTION_SYNC_MIN_INTERVAL_MS
    };
    let abstraction_interval = if forced_refresh_bypass {
        0
    } else {
        base_abstraction_interval.max(backoff_floor_ms)
    };

    let mut mode_outcome =
        fetch_account_mode_unlocked(http, db_name, cfg, ts, abstraction_interval);
    let mut account_mode = mode_outcome
        .mode
        .clone()
        .unwrap_or_else(|| cached_mode.clone());
    let mut is_unified = is_unified_account_mode(&account_mode);

    let base_spot_interval = if !was_unified && is_unified {
        0
    } else if is_unified {
        UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS
    } else {
        VAULT_SYNC_MIN_INTERVAL_MS
    };
    let spot_interval = if forced_refresh_bypass || base_spot_interval == 0 {
        0
    } else {
        base_spot_interval.max(backoff_floor_ms)
    };
    let mut spot_outcome = fetch_spot_usdc_details_unlocked(http, db_name, cfg, ts, spot_interval);

    // 3. Evaluate candidate wallet equity & like-for-like UnifiedToUnified cliff
    let cached_spot = cfg.get_f64("live_exchange_spot_usdc", 0.0).max(0.0);
    let (mut candidate_wallet_eq, mut candidate_withdrawable) = compute_candidate_wallet_equity(
        is_unified,
        cached_spot,
        spot_outcome.details,
        perp_eq,
        margin_used,
        withdrawable,
    );

    let mut unified_drop_pct = if is_unified && prev_live_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC {
        ((prev_live_eq - candidate_wallet_eq) / prev_live_eq).max(0.0)
    } else {
        0.0
    };
    let mut unified_cliff = has_held_positions
        && is_unified
        && prev_live_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC
        && unified_drop_pct > EQUITY_CLIFF_DROP_PCT;

    if pre_cliff_kind.is_none() && unified_cliff && has_live_wallet {
        if let EscalationDecision::BypassGranted =
            try_acquire_cliff_escalation_bypass(&throttle_key, ts)
        {
            forced_refresh_bypass = true;
            if mode_outcome.mode.is_none() && !mode_outcome.failed {
                mode_outcome = fetch_account_mode_unlocked(http, db_name, cfg, ts, 0);
                account_mode = mode_outcome
                    .mode
                    .clone()
                    .unwrap_or_else(|| cached_mode.clone());
                is_unified = is_unified_account_mode(&account_mode);
            }
            if spot_outcome.details.is_none() && !spot_outcome.failed {
                spot_outcome = fetch_spot_usdc_details_unlocked(http, db_name, cfg, ts, 0);
            }
            (candidate_wallet_eq, candidate_withdrawable) = compute_candidate_wallet_equity(
                is_unified,
                cached_spot,
                spot_outcome.details,
                perp_eq,
                margin_used,
                withdrawable,
            );
            unified_drop_pct = if is_unified && prev_live_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC {
                ((prev_live_eq - candidate_wallet_eq) / prev_live_eq).max(0.0)
            } else {
                0.0
            };
            unified_cliff = has_held_positions
                && is_unified
                && prev_live_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC
                && unified_drop_pct > EQUITY_CLIFF_DROP_PCT;
        }
    }

    let cliff_kind = if let Some(kind) = pre_cliff_kind {
        Some(kind)
    } else if unified_cliff {
        Some(EquityCliffKind::UnifiedToUnified)
    } else {
        None
    };
    let equity_cliff_detected = cliff_kind.is_some();

    let wallet_eq_drop_exceeds_cliff = prev_live_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC
        && ((prev_live_eq - candidate_wallet_eq) / prev_live_eq) > EQUITY_CLIFF_DROP_PCT;
    let both_freshly_verified = mode_outcome.mode.is_some()
        && !mode_outcome.failed
        && spot_outcome.details.is_some()
        && !spot_outcome.failed;
    let refuse_equity_overwrite = equity_cliff_detected
        && (mode_outcome.failed
            || spot_outcome.failed
            || (wallet_eq_drop_exceeds_cliff && !both_freshly_verified));

    if has_live_wallet {
        if equity_cliff_detected {
            if forced_refresh_bypass {
                complete_cliff_escalation_attempt(
                    &throttle_key,
                    mode_outcome.failed || spot_outcome.failed,
                );
            }
        } else {
            reset_cliff_escalation_on_clear(&throttle_key, ts);
        }
    }

    let perp_drop_pct = if was_unified {
        perp_to_perp_drop_pct
    } else {
        let non_unified_base = prev_perp_eq.max(prev_live_eq);
        if non_unified_base > 0.0 {
            ((non_unified_base - perp_eq) / non_unified_base).max(0.0)
        } else {
            0.0
        }
    };

    let (baseline_eq, drop_pct) = match cliff_kind {
        Some(EquityCliffKind::DefaultModeCutover) => (prev_live_eq, cutover_drop_pct),
        Some(EquityCliffKind::PerpToPerp) => (prev_perp_eq, perp_to_perp_drop_pct),
        Some(EquityCliffKind::UnifiedToUnified) => (prev_live_eq, unified_drop_pct),
        None => {
            if was_unified {
                (prev_live_eq, unified_drop_pct)
            } else {
                (prev_live_eq.max(prev_perp_eq), perp_drop_pct)
            }
        }
    };

    let (effective_wallet_eq, effective_withdrawable) = if refuse_equity_overwrite {
        (prev_live_eq, withdrawable)
    } else {
        (candidate_wallet_eq, candidate_withdrawable)
    };

    // 4. Per-key repeated-WARN deduplication (5m summary window + clear reporting)
    let mut warn_decision = None;
    let mut cleared_suppressed_repeats = None;

    if let Some(kind) = cliff_kind {
        let (decision, prior_flushed) = if has_live_wallet {
            record_cliff_warn_occurrence(&throttle_key, kind, refuse_equity_overwrite, ts)
        } else {
            (WarnDedupDecision::EmitFirst, None)
        };
        if let Some(prior_repeats) = prior_flushed {
            tracing::info!(
                suppressed_repeats = prior_repeats,
                "Equity-cliff state transitioned (suppressed_repeats = {})",
                prior_repeats
            );
            cleared_suppressed_repeats = Some(prior_repeats);
        }
        match decision {
            WarnDedupDecision::EmitFirst => {
                tracing::warn!(
                    cliff_kind = kind.as_str(),
                    baseline_eq,
                    perp_eq,
                    effective_wallet_eq,
                    drop_pct,
                    perp_drop_pct,
                    account_mode = %account_mode,
                    forced_refresh_bypass,
                    mode_failed = mode_outcome.failed,
                    spot_failed = spot_outcome.failed,
                    refuse_equity_overwrite,
                    suppressed_repeats = 0_u64,
                    "04:13 equity-cliff protection triggered ({})",
                    kind.as_str()
                );
            }
            WarnDedupDecision::EmitSummary { suppressed_repeats } => {
                tracing::warn!(
                    cliff_kind = kind.as_str(),
                    baseline_eq,
                    perp_eq,
                    effective_wallet_eq,
                    drop_pct,
                    perp_drop_pct,
                    account_mode = %account_mode,
                    forced_refresh_bypass,
                    mode_failed = mode_outcome.failed,
                    spot_failed = spot_outcome.failed,
                    refuse_equity_overwrite,
                    suppressed_repeats,
                    "04:13 equity-cliff protection still active ({}) (suppressed_repeats = {})",
                    kind.as_str(),
                    suppressed_repeats
                );
            }
            WarnDedupDecision::Suppress { suppressed_repeats } => {
                tracing::debug!(
                    cliff_kind = kind.as_str(),
                    suppressed_repeats,
                    "Suppressing repeated equity-cliff WARN within 5m deduplication window"
                );
            }
        }
        warn_decision = Some(decision);
    } else if has_live_wallet {
        if let Some(suppressed_repeats) = clear_cliff_warn_state(&throttle_key) {
            tracing::info!(
                account_mode = %account_mode,
                effective_wallet_eq,
                perp_eq,
                suppressed_repeats,
                "04:13 equity-cliff condition cleared (suppressed_repeats = {})",
                suppressed_repeats
            );
            cleared_suppressed_repeats = Some(suppressed_repeats);
        }
    }

    EquitySnapshotEvaluation {
        perp_eq,
        margin_used,
        account_mode,
        fresh_account_mode: mode_outcome.mode,
        fresh_spot: spot_outcome.details,
        effective_wallet_eq,
        effective_withdrawable,
        equity_cliff_detected,
        cliff_kind,
        refuse_equity_overwrite,
        perp_drop_pct,
        drop_pct,
        baseline_eq,
        forced_refresh_bypass,
        warn_decision,
        cleared_suppressed_repeats,
    }
}

/// Persists equity, account mode, spot USDC, and capital cap keys into MariaDB under the
/// caller's short execution lock (`Phase 3`). Refuses to overwrite `live_exchange_equity_usdc`
/// (and leaves cached mode/perp baseline intact) if `eval.refuse_equity_overwrite` is `true`
/// (`P0-Risk`, `R14-W4`).
pub fn commit_equity_snapshot_locked(
    db: &DatabaseManager,
    fresh_cfg: &ConfigMap,
    eval: &EquitySnapshotEvaluation,
    ts: i64,
    fallback_eq: f64,
) -> (f64, f64) {
    if let Some(suppressed_repeats) = eval.cleared_suppressed_repeats {
        let _ = db.log_telemetry(
            "INFO",
            "execution.reconcile.equity",
            &format!(
                "Equity-cliff condition cleared at equity=${:.2} (account_mode={}) (suppressed_repeats = {})",
                eval.effective_wallet_eq, eval.account_mode, suppressed_repeats
            ),
            None,
        );
    }

    let configured_live_eq = fresh_cfg.get_f64("live_exchange_equity_usdc", 0.0);
    let cap_unset = fresh_cfg
        .get("risk_capital_cap_usdc")
        .map(|s| s.trim().is_empty())
        .unwrap_or(true);
    let eff_cap_eq = if cap_unset && configured_live_eq > 10.0 {
        let _ = db.set_config_value(
            "risk_capital_cap_usdc",
            &format!("{configured_live_eq:.2}"),
            Some("Preserved initial capital sizing cap from legacy live_exchange_equity_usdc"),
        );
        configured_live_eq
    } else {
        fresh_cfg.get_f64("risk_capital_cap_usdc", 0.0)
    };

    if eval.refuse_equity_overwrite {
        let preserved_eq = if configured_live_eq >= 1.0 {
            round_dp(configured_live_eq, 2)
        } else {
            fallback_eq
        };
        let kind_label = eval.cliff_kind.map_or("unknown", EquityCliffKind::as_str);
        match eval.warn_decision {
            Some(WarnDedupDecision::EmitFirst) | None => {
                let _ = db.log_telemetry(
                    "WARN",
                    "execution.reconcile.equity",
                    &format!(
                        "Refused live_exchange_equity_usdc overwrite during >25% equity cliff (kind={}, baseline=${:.2}, perp=${:.2}, drop={:.1}%, preserved=${:.2}) because forced abstraction/spot verification did not complete",
                        kind_label,
                        eval.baseline_eq,
                        eval.perp_eq,
                        eval.drop_pct * 100.0,
                        preserved_eq
                    ),
                    None,
                );
            }
            Some(WarnDedupDecision::EmitSummary { suppressed_repeats }) => {
                let _ = db.log_telemetry(
                    "WARN",
                    "execution.reconcile.equity",
                    &format!(
                        "Refused live_exchange_equity_usdc overwrite during >25% equity cliff (kind={}, baseline=${:.2}, perp=${:.2}, drop={:.1}%, preserved=${:.2}) (suppressed_repeats = {})",
                        kind_label,
                        eval.baseline_eq,
                        eval.perp_eq,
                        eval.drop_pct * 100.0,
                        preserved_eq,
                        suppressed_repeats
                    ),
                    None,
                );
            }
            Some(WarnDedupDecision::Suppress { .. }) => {}
        }
        return (preserved_eq, eff_cap_eq);
    }

    let _ = db.set_config_value(
        "live_exchange_perp_margin_equity_usdc",
        &format!("{:.2}", round_dp(eval.perp_eq.max(0.0), 2)),
        Some(
            "Latest synced Hyperliquid perp clearinghouseState marginSummary.accountValue in USDC",
        ),
    );

    if let Some(ref mode) = eval.fresh_account_mode {
        let _ = db.set_config_value(
            "live_exchange_account_mode",
            mode,
            Some("Latest synced Hyperliquid userAbstraction mode (default, unifiedAccount, portfolioMargin)"),
        );
    }

    if let Some((spot_usdc, _, _)) = eval.fresh_spot {
        let _ = db.set_config_value(
            "live_exchange_spot_usdc",
            &format!("{spot_usdc:.2}"),
            Some("Latest synced Hyperliquid spot USDC balance (informational; segregated from perp margin)"),
        );
        let _ = db.set_config_value(
            "live_exchange_spot_synced_at_ms",
            &ts.to_string(),
            Some("Timestamp (ms) of latest Hyperliquid spotClearinghouseState sync"),
        );
    }

    if eval.equity_cliff_detected && !eval.refuse_equity_overwrite {
        if let Some(WarnDedupDecision::EmitSummary { suppressed_repeats }) = eval.warn_decision {
            let _ = db.log_telemetry(
                "WARN",
                "execution.reconcile.equity",
                &format!(
                    "Equity cliff remained active during periodic summary (kind={}, baseline=${:.2}, effective=${:.2}) (suppressed_repeats = {})",
                    eval.cliff_kind.map_or("unknown", EquityCliffKind::as_str),
                    eval.baseline_eq,
                    eval.effective_wallet_eq,
                    suppressed_repeats
                ),
                None,
            );
        } else {
            tracing::info!(
                baseline_eq = eval.baseline_eq,
                perp_eq = eval.perp_eq,
                effective_wallet_eq = eval.effective_wallet_eq,
                "Resolved >25% equity cliff after forced userAbstraction/spotClearinghouseState refresh"
            );
        }
    }

    let exchange_equity_usdc = if eval.effective_wallet_eq >= 1.0 {
        round_dp(eval.effective_wallet_eq, 2)
    } else {
        fallback_eq
    };
    if eval.effective_wallet_eq >= 1.0 {
        let _ = db.set_config_value(
            "live_exchange_equity_usdc",
            &format!("{exchange_equity_usdc:.2}"),
            Some("Latest reconciled Hyperliquid trading equity in USDC (unified spot+perp total on unified accounts, else perp marginSummary.accountValue)"),
        );
    }
    let _ = db.set_config_value(
        "live_exchange_equity_synced_at_ms",
        &ts.to_string(),
        Some("Timestamp (ms) of latest successful Hyperliquid clearinghouseState equity sync"),
    );
    let _ = db.set_config_value(
        "live_exchange_margin_used_usdc",
        &format!("{:.2}", round_dp(eval.margin_used.max(0.0), 2)),
        Some("Latest synced Hyperliquid perp marginSummary.totalMarginUsed in USDC"),
    );
    let _ = db.set_config_value(
        "live_exchange_withdrawable_usdc",
        &format!("{:.2}", round_dp(eval.effective_withdrawable.max(0.0), 2)),
        Some("Latest synced Hyperliquid withdrawable / free margin USDC balance"),
    );

    (exchange_equity_usdc, eff_cap_eq)
}

/// Refreshes live equity snapshot at an explicit millisecond timestamp `ts` strictly unlocked
/// over HTTP (`R14-H2`, `R14-W4`, `P0-Risk`).
pub fn refresh_equity_snapshot_at_ms(
    db: &DatabaseManager,
    http: &HyperliquidHttpClient,
    cfg: &ConfigMap,
    ts: i64,
) -> Result<f64> {
    if !cfg.execution_mode().is_live_or_testnet() {
        return Ok(cfg.get_f64("initial_paper_equity_usdc", 10_000.0));
    }
    let wallet = cfg.get_str("wallet_address", "");
    if wallet.trim().is_empty() {
        return Ok(cfg.get_f64("live_exchange_equity_usdc", 0.0));
    }
    let (local_has_held, snapshot_cfg) = {
        let _guard = acquire_execution_lock();
        let held = db.get_assets_held().unwrap_or_default();
        let fresh = db.load_config_or_default();
        (!held.is_empty(), fresh)
    };
    let ch_state = http.fetch_clearinghouse_state_with_options(
        wallet.trim(),
        &snapshot_cfg,
        RequestOptions::background_sync().with_timeout_sec(http.timeout_sec.min(8)),
    )?;
    let (open_coins, _) = extract_open_coins_and_szi(&ch_state);
    let eval = evaluate_equity_snapshot_unlocked(
        http,
        db.db_name(),
        &snapshot_cfg,
        &ch_state,
        &open_coins,
        local_has_held,
        ts,
    );

    let _guard = acquire_execution_lock();
    let post_cfg = db.load_config_or_default();
    if !post_cfg.execution_mode().is_live_or_testnet() {
        return Ok(post_cfg.get_f64("live_exchange_equity_usdc", 0.0));
    }
    let fallback_eq = post_cfg.get_f64("initial_paper_equity_usdc", 10_000.0);
    let (eq, _) = commit_equity_snapshot_locked(db, &post_cfg, &eval, ts, fallback_eq);
    Ok(eq)
}

/// Refreshes live equity snapshot (`clearinghouseState` + cliff-checked `userAbstraction` +
/// `spotClearinghouseState`) strictly unlocked (`R14-H2`, `R14-W4`, `P0-Risk`).
pub fn refresh_equity_snapshot_unlocked(
    db: &DatabaseManager,
    http: &HyperliquidHttpClient,
    cfg: &ConfigMap,
) -> Result<f64> {
    refresh_equity_snapshot_at_ms(db, http, cfg, aegis_core::now_ms())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aegis_test_support::mock_http::{MockHttpResponse, MockHttpServer};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

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
    fn test_escalation_backoff_schedule_and_map_bounds() {
        assert_eq!(compute_cliff_escalation_backoff_ms(0), 0);
        assert_eq!(compute_cliff_escalation_backoff_ms(1), 60_000);
        assert_eq!(compute_cliff_escalation_backoff_ms(2), 120_000);
        assert_eq!(compute_cliff_escalation_backoff_ms(3), 240_000);
        assert_eq!(compute_cliff_escalation_backoff_ms(4), 300_000);
        assert_eq!(compute_cliff_escalation_backoff_ms(10), 300_000);

        let base_ts = 1_750_000_000_000_i64;
        for idx in 0..300_i64 {
            let key = (format!("unit_db_{idx}"), format!("0xwallet_{idx:04}"));
            let _ = try_acquire_cliff_escalation_bypass(&key, base_ts + idx);
            let _ = record_cliff_warn_occurrence(
                &key,
                EquityCliffKind::PerpToPerp,
                true,
                base_ts + idx,
            );
            assert!(cliff_escalation_state_len() <= MAX_EQUITY_STATE_MAP_ENTRIES);
            assert!(cliff_warn_dedup_state_len() <= MAX_EQUITY_STATE_MAP_ENTRIES);
        }
        assert_eq!(cliff_escalation_state_len(), MAX_EQUITY_STATE_MAP_ENTRIES);
        assert_eq!(cliff_warn_dedup_state_len(), MAX_EQUITY_STATE_MAP_ENTRIES);
    }

    #[test]
    fn test_steady_state_unified_account_has_zero_cliff_false_positives() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();

        let abs_calls = Arc::new(AtomicUsize::new(0));
        let abs_srv = abs_calls.clone();
        let spot_calls = Arc::new(AtomicUsize::new(0));
        let spot_srv = spot_calls.clone();

        let server = MockHttpServer::start(move |req| {
            let typ = req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("");
            match typ {
                "clearinghouseState" => MockHttpResponse::ok_json(&json!({
                    "marginSummary": { "accountValue": "21.80", "totalMarginUsed": "18.50" },
                    "withdrawable": "3.30",
                    "assetPositions": [{
                        "position": { "coin": "SOL", "szi": "1.50", "entryPx": "160.0" }
                    }]
                })),
                "userAbstraction" => {
                    abs_srv.fetch_add(1, Ordering::SeqCst);
                    MockHttpResponse::ok_json(&json!("unifiedAccount"))
                }
                "spotClearinghouseState" => {
                    spot_srv.fetch_add(1, Ordering::SeqCst);
                    MockHttpResponse::ok_json(&json!({
                        "balances": [{ "coin": "USDC", "token": 0, "total": "1871.00", "hold": "18.50" }]
                    }))
                }
                _ => MockHttpResponse::ok_json(&json!([])),
            }
        });

        let now = 1_760_000_000_000_i64;
        db.set_config_value("execution_mode", "LIVE", None).unwrap();
        db.set_config_value("hyperliquid_mainnet_url", &server.base_url(), None)
            .unwrap();
        db.set_config_value("hyperliquid_allowed_hosts", "127.0.0.1", None)
            .unwrap();
        db.set_config_value(
            "wallet_address",
            "0x2222222222222222222222222222222222222222",
            None,
        )
        .unwrap();
        db.set_config_value("live_exchange_account_mode", "unifiedAccount", None)
            .unwrap();
        db.set_config_value("live_exchange_equity_usdc", "1871.00", None)
            .unwrap();
        db.set_config_value("live_exchange_perp_margin_equity_usdc", "21.84", None)
            .unwrap();
        db.set_config_value("live_exchange_spot_usdc", "1871.00", None)
            .unwrap();
        db.set_config_value("live_exchange_spot_synced_at_ms", &now.to_string(), None)
            .unwrap();

        let http = HyperliquidHttpClient::new(2);
        let cfg0 = db.load_config_or_default();
        // Prime abstraction throttle at `now`
        let _ = fetch_account_mode_unlocked(
            &http,
            db.db_name(),
            &cfg0,
            now,
            ABSTRACTION_SYNC_MIN_INTERVAL_MS,
        );
        assert_eq!(abs_calls.load(Ordering::SeqCst), 1);
        assert_eq!(spot_calls.load(Ordering::SeqCst), 0);

        let ch_state = json!({
            "marginSummary": { "accountValue": "21.80", "totalMarginUsed": "18.50" },
            "withdrawable": "3.30",
            "assetPositions": [{
                "position": { "coin": "SOL", "szi": "1.50", "entryPx": "160.0" }
            }]
        });
        let (open_coins, _) = extract_open_coins_and_szi(&ch_state);

        // Within 15s (e.g. +5s): 0 cliff, 0 abstraction calls, 0 spot calls, 0 WARNs
        let eval_5s = evaluate_equity_snapshot_unlocked(
            &http,
            db.db_name(),
            &cfg0,
            &ch_state,
            &open_coins,
            true,
            now + 5_000,
        );
        assert!(!eval_5s.equity_cliff_detected);
        assert_eq!(eval_5s.cliff_kind, None);
        assert!(!eval_5s.forced_refresh_bypass);
        assert_eq!(eval_5s.warn_decision, None);
        assert!((eval_5s.effective_wallet_eq - 1871.00).abs() < 1e-6);
        assert_eq!(abs_calls.load(Ordering::SeqCst), 1);
        assert_eq!(spot_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_equity_cliff_forces_refresh_and_refuses_overwrite_on_failure() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        db.init_schema().unwrap();

        let fail_spot = Arc::new(AtomicBool::new(false));
        let fail_spot_srv = fail_spot.clone();
        let abs_calls = Arc::new(AtomicUsize::new(0));
        let abs_srv = abs_calls.clone();
        let spot_calls = Arc::new(AtomicUsize::new(0));
        let spot_srv = spot_calls.clone();

        let server = MockHttpServer::start(move |req| {
            let typ = req
                .json_body
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("");
            match typ {
                "clearinghouseState" => MockHttpResponse::ok_json(&json!({
                    "marginSummary": { "accountValue": "73.02", "totalMarginUsed": "60.0" },
                    "withdrawable": "13.02",
                    "assetPositions": [{
                        "position": { "coin": "BTC", "szi": "0.01", "entryPx": "90000.0" }
                    }]
                })),
                "userAbstraction" => {
                    abs_srv.fetch_add(1, Ordering::SeqCst);
                    MockHttpResponse::ok_json(&json!("unifiedAccount"))
                }
                "spotClearinghouseState" => {
                    spot_srv.fetch_add(1, Ordering::SeqCst);
                    if fail_spot_srv.load(Ordering::SeqCst) {
                        MockHttpResponse::json(503, &json!({ "error": "unavailable" }))
                    } else {
                        MockHttpResponse::ok_json(&json!({
                            "balances": [{ "coin": "USDC", "token": 0, "total": "1890.79", "hold": "60.0" }]
                        }))
                    }
                }
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
            "0x1111111111111111111111111111111111111111",
            None,
        )
        .unwrap();
        db.set_config_value("live_exchange_account_mode", "default", None)
            .unwrap();
        db.set_config_value("live_exchange_equity_usdc", "1890.79", None)
            .unwrap();
        db.set_config_value("live_exchange_perp_margin_equity_usdc", "1890.79", None)
            .unwrap();
        let now = aegis_core::now_ms();
        db.set_config_value("live_exchange_spot_synced_at_ms", &now.to_string(), None)
            .unwrap();

        let http = HyperliquidHttpClient::new(2);
        // Prime steady-state abstraction throttle so normal interval would skip it!
        let cfg0 = db.load_config_or_default();
        let _ = fetch_account_mode_unlocked(&http, db.db_name(), &cfg0, now, 60_000);
        assert_eq!(abs_calls.load(Ordering::SeqCst), 1);

        // 1. Fail spot during >25% cliff -> BOTH userAbstraction & spot forced immediately, equity NOT overwritten to 73.02!
        fail_spot.store(true, Ordering::SeqCst);
        let eq1 = refresh_equity_snapshot_at_ms(&db, &http, &cfg0, now).unwrap();
        assert!((eq1 - 1890.79).abs() < 1e-6);
        assert_eq!(abs_calls.load(Ordering::SeqCst), 2);
        let spot_calls_after_fail = spot_calls.load(Ordering::SeqCst);
        assert!(spot_calls_after_fail >= 1);
        let cfg1 = db.load_config_or_default();
        assert!((cfg1.get_f64("live_exchange_equity_usdc", 0.0) - 1890.79).abs() < 1e-6);

        // 2. Inside 60s escalation cooldown (now + 15_000) -> no extra unthrottled REST calls, equity STILL protected!
        let eq_cooldown = refresh_equity_snapshot_at_ms(&db, &http, &cfg1, now + 15_000).unwrap();
        assert!((eq_cooldown - 1890.79).abs() < 1e-6);
        assert_eq!(abs_calls.load(Ordering::SeqCst), 2);
        assert_eq!(spot_calls.load(Ordering::SeqCst), spot_calls_after_fail);

        // 3. After 60s escalation cooldown (now + 60_000), spot recovers -> forced retry switches to unifiedAccount & preserves $1890.79!
        fail_spot.store(false, Ordering::SeqCst);
        let eq2 = refresh_equity_snapshot_at_ms(&db, &http, &cfg1, now + 60_000).unwrap();
        assert!((eq2 - 1890.79).abs() < 1e-6);
        assert_eq!(abs_calls.load(Ordering::SeqCst), 3);
        assert!(spot_calls.load(Ordering::SeqCst) > spot_calls_after_fail);
        let cfg2 = db.load_config_or_default();
        assert_eq!(
            cfg2.get_str("live_exchange_account_mode", ""),
            "unifiedAccount"
        );
        assert!((cfg2.get_f64("live_exchange_equity_usdc", 0.0) - 1890.79).abs() < 1e-6);
    }
}
