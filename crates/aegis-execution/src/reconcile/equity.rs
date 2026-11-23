//! Exchange equity, `userAbstraction`, and unified spot-USDC reconciliation helpers
//! (`aegis-execution::reconcile::equity`, `R14-H2`, `P0-Risk`).

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

type ThrottleKey = (String, String);
static LAST_SPOT_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<ThrottleKey, i64>>> = OnceLock::new();
static LAST_ABSTRACTION_SYNC_ATTEMPT_MS: OnceLock<Mutex<HashMap<ThrottleKey, i64>>> =
    OnceLock::new();

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
pub(crate) fn extract_open_coins_and_szi(
    ch_state: &Value,
) -> (HashSet<String>, HashMap<String, f64>) {
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
pub(crate) fn extract_perp_equity_metrics(ch_state: &Value) -> (f64, f64, f64) {
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
pub(crate) struct AccountModeFetchOutcome {
    pub mode: Option<String>,
    pub failed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct SpotUsdcFetchOutcome {
    pub details: Option<(f64, f64, Option<f64>)>,
    pub failed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct EquitySnapshotEvaluation {
    pub perp_eq: f64,
    pub margin_used: f64,
    pub account_mode: String,
    pub fresh_account_mode: Option<String>,
    pub fresh_spot: Option<(f64, f64, Option<f64>)>,
    pub effective_wallet_eq: f64,
    pub effective_withdrawable: f64,
    pub equity_cliff_detected: bool,
    pub refuse_equity_overwrite: bool,
    pub perp_drop_pct: f64,
    pub baseline_eq: f64,
}

pub(crate) fn fetch_account_mode_unlocked(
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
    if eff_min_interval > 0 {
        let map = LAST_ABSTRACTION_SYNC_ATTEMPT_MS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = map.lock();
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
        if guard.len() > 256 {
            guard.clear();
        }
        guard.insert(key, now_ms_val);
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

pub(crate) fn fetch_spot_usdc_details_unlocked(
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
            guard.insert(key, prev_sync_ms);
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
        if guard.len() > 256 {
            guard.clear();
        }
        guard.insert(key, now_ms_val);
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

    Some((
        round_dp(raw_usdc.max(0.0), 2),
        round_dp(hold_usdc.max(0.0), 2),
        avail_after_maint,
    ))
    .map_or(
        SpotUsdcFetchOutcome {
            details: None,
            failed: true,
        },
        |details| SpotUsdcFetchOutcome {
            details: Some(details),
            failed: false,
        },
    )
}

/// Evaluates equity, account mode (`userAbstraction`), and unified spot collateral strictly
/// unlocked (`R14-H2`) while enforcing the 04:13 equity-cliff force-refresh guard (`P0-Risk`).
pub(crate) fn evaluate_equity_snapshot_unlocked(
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
    let baseline_eq = prev_live_eq.max(prev_perp_eq);
    let perp_drop_pct = if baseline_eq > 0.0 {
        (baseline_eq - perp_eq) / baseline_eq
    } else {
        0.0
    };

    let equity_cliff_detected = (!open_coins.is_empty() || local_has_held)
        && baseline_eq >= EQUITY_CLIFF_MIN_BASELINE_USDC
        && perp_drop_pct > EQUITY_CLIFF_DROP_PCT;

    let cached_mode = cfg.get_str("live_exchange_account_mode", "default");
    let was_unified = is_unified_account_mode(&cached_mode);
    let abstraction_interval = if equity_cliff_detected {
        0
    } else if was_unified {
        ABSTRACTION_SYNC_MIN_INTERVAL_MS
    } else {
        DEFAULT_MODE_ABSTRACTION_SYNC_MIN_INTERVAL_MS
    };

    let mode_outcome = fetch_account_mode_unlocked(http, db_name, cfg, ts, abstraction_interval);
    let account_mode = mode_outcome
        .mode
        .clone()
        .unwrap_or_else(|| cached_mode.clone());
    let is_unified = is_unified_account_mode(&account_mode);

    let spot_interval = if equity_cliff_detected || (!was_unified && is_unified) {
        0
    } else if is_unified {
        UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS
    } else {
        VAULT_SYNC_MIN_INTERVAL_MS
    };
    let spot_outcome = fetch_spot_usdc_details_unlocked(http, db_name, cfg, ts, spot_interval);
    let refuse_equity_overwrite =
        equity_cliff_detected && (mode_outcome.failed || spot_outcome.failed);

    if equity_cliff_detected {
        tracing::warn!(
            baseline_eq,
            perp_eq,
            perp_drop_pct,
            account_mode = %account_mode,
            mode_failed = mode_outcome.failed,
            spot_failed = spot_outcome.failed,
            refuse_equity_overwrite,
            "04:13 equity-cliff protection: forced userAbstraction + spotClearinghouseState refresh"
        );
    }

    let (effective_wallet_eq, effective_withdrawable) = if refuse_equity_overwrite {
        (prev_live_eq, withdrawable)
    } else if is_unified {
        let cached_spot = cfg.get_f64("live_exchange_spot_usdc", 0.0).max(0.0);
        if let Some((st, sh, avail_opt)) = spot_outcome.details.filter(|(st, _, _)| *st >= 1.0) {
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

    EquitySnapshotEvaluation {
        perp_eq,
        margin_used,
        account_mode,
        fresh_account_mode: mode_outcome.mode,
        fresh_spot: spot_outcome.details,
        effective_wallet_eq,
        effective_withdrawable,
        equity_cliff_detected,
        refuse_equity_overwrite,
        perp_drop_pct,
        baseline_eq,
    }
}

/// Persists equity, account mode, spot USDC, and capital cap keys into MariaDB under the
/// caller's short execution lock (`Phase 3`). Refuses to overwrite `live_exchange_equity_usdc`
/// if `eval.refuse_equity_overwrite` is `true` (`P0-Risk`).
pub(crate) fn commit_equity_snapshot_locked(
    db: &DatabaseManager,
    fresh_cfg: &ConfigMap,
    eval: &EquitySnapshotEvaluation,
    ts: i64,
    fallback_eq: f64,
) -> (f64, f64) {
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
        let _ = db.log_telemetry(
            "WARN",
            "execution.reconcile.equity",
            &format!(
                "Refused live_exchange_equity_usdc overwrite during >25% equity cliff (baseline=${:.2}, perp=${:.2}, drop={:.1}%, preserved=${:.2}) because forced abstraction/spot refresh failed",
                eval.baseline_eq,
                eval.perp_eq,
                eval.perp_drop_pct * 100.0,
                preserved_eq
            ),
            None,
        );
        return (preserved_eq, eff_cap_eq);
    }

    if eval.equity_cliff_detected && !eval.refuse_equity_overwrite {
        tracing::info!(
            baseline_eq = eval.baseline_eq,
            perp_eq = eval.perp_eq,
            effective_wallet_eq = eval.effective_wallet_eq,
            "Resolved >25% equity cliff after forced userAbstraction/spotClearinghouseState refresh"
        );
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

/// Refreshes live equity snapshot (`clearinghouseState` + cliff-checked `userAbstraction` +
/// `spotClearinghouseState`) strictly unlocked (`R14-H2`, `P0-Risk`).
pub(crate) fn refresh_equity_snapshot_unlocked(
    db: &DatabaseManager,
    http: &HyperliquidHttpClient,
    cfg: &ConfigMap,
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
    let ts = aegis_core::now_ms();
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
        let eq1 = refresh_equity_snapshot_unlocked(&db, &http, &cfg0).unwrap();
        assert!((eq1 - 1890.79).abs() < 1e-6);
        assert_eq!(abs_calls.load(Ordering::SeqCst), 2);
        let spot_calls_after_fail = spot_calls.load(Ordering::SeqCst);
        assert!(spot_calls_after_fail >= 1);
        let cfg1 = db.load_config_or_default();
        assert!((cfg1.get_f64("live_exchange_equity_usdc", 0.0) - 1890.79).abs() < 1e-6);

        // 2. Spot recovers -> cliff forces refresh again, unified spot $1890.79 preserved!
        fail_spot.store(false, Ordering::SeqCst);
        let eq2 = refresh_equity_snapshot_unlocked(&db, &http, &cfg1).unwrap();
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
