//! Dynamic Cutover & Live 1:1 Hyperliquid Exchange Reconciliation (`aegis-execution::reconcile`,
//! `R14-H2`, `R14-M7`, `R14-M29`, `P0-Risk`, `P1-Acct`, Sec. 11).
//!
//! Executes strictly via a 4-phase lock discipline (`acquire_execution_lock` -> unlocked HTTP ->
//! re-validated DB commit -> unlocked batched TP/SL sync), while offloading `portfolio`,
//! `vaultDetails`, incremental watermarked `userFunding`, `userFees`, and `extraAgents` to
//! [`AuxiliarySyncWorker`].

pub mod auxiliary;
pub mod equity;
pub mod ledger;
pub mod orders;
pub mod positions;

pub use auxiliary::{
    parse_hl_portfolio_pnl, parse_vault_position_info, parse_vault_position_info_with_raw_equity,
    AuxiliarySyncReport, AuxiliarySyncWorker, AUX_SYNC_INTERVAL_MS, EXTRA_AGENT_EXPIRY_WARN_MS,
    MAX_VAULTS_TO_FETCH, USER_FEES_SYNC_INTERVAL_MS,
};
pub use equity::{
    parse_spot_usdc_balance, parse_spot_usdc_details, ABSTRACTION_SYNC_MIN_INTERVAL_MS,
    DEFAULT_MODE_ABSTRACTION_SYNC_MIN_INTERVAL_MS, EQUITY_CLIFF_DROP_PCT,
    EQUITY_CLIFF_MIN_BASELINE_USDC, UNIFIED_SPOT_SYNC_MIN_INTERVAL_MS, VAULT_SYNC_MIN_INTERVAL_MS,
};
pub use orders::{find_orphan_orders_to_cancel, find_orphan_orders_to_cancel_with_metadata};

use crate::http_client::{
    is_testnet_configured, HyperliquidHttpClient, OpenOrdersSnapshot, RequestOptions,
};
use crate::lock_guard::acquire_execution_lock;
use aegis_core::{
    extract_hl_oid, mask_identifier, now_ms, CachedHlPortfolioPnl, ConfigMap, ExchangeAsset,
    ExecutionMode, HeldAssetView, Result, VaultInfoPayload,
};
use aegis_db::DatabaseManager;
use aegis_risk::RiskLimitsConfig;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use equity::{
    commit_equity_snapshot_locked, evaluate_equity_snapshot_unlocked, extract_open_coins_and_szi,
    fetch_account_mode_unlocked, fetch_spot_usdc_details_unlocked,
    refresh_equity_snapshot_unlocked,
};
use ledger::{
    compute_fifo_closed_orders_and_funding, fetch_windowed_fills_unlocked,
    group_db_funding_records, FifoFundingReplayOutput, ReconcileLedgerCache,
    LEDGER_REPLAY_MIN_INTERVAL_MS,
};
use orders::{build_orphan_cancel_plan, index_open_orders};
use positions::{
    collect_meta_coins_needed, commit_positions_and_ledger_locked, fetch_meta_catalog_unlocked,
    prepare_exchange_positions_unlocked, sync_tpsl_unlocked,
};

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

/// Dynamic Cutover & Live 1:1 Exchange State Reconciler.
#[derive(Clone)]
pub struct CutoverReconciler {
    pub db: DatabaseManager,
    pub http: HyperliquidHttpClient,
    in_flight_position_ops: Arc<Mutex<HashSet<i64>>>,
    ledger_cache: Arc<Mutex<HashMap<(String, String), ReconcileLedgerCache>>>,
}

impl CutoverReconciler {
    pub fn new(db: DatabaseManager, http: HyperliquidHttpClient) -> Self {
        Self {
            db,
            http,
            in_flight_position_ops: Arc::new(Mutex::new(HashSet::new())),
            ledger_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attaches a shared in-flight position operation set from [`crate::ExecutionEngine`] so
    /// unlocked closes/cancels are never misclassified as flat during Phase-3 DB commits (`R14-H2`).
    pub fn with_in_flight_position_ops(
        mut self,
        in_flight_position_ops: Arc<Mutex<HashSet<i64>>>,
    ) -> Self {
        self.in_flight_position_ops = in_flight_position_ops;
        self
    }

    /// Performs a lightweight steady-state 1:1 synchronization of MariaDB `positions` against live
    /// Hyperliquid `clearinghouseState`, `openOrders`, and windowed `userFillsByTime` (`15s` loop).
    pub fn sync_db_with_hyperliquid(&self) -> Result<CutoverReconcileReport> {
        self.run_reconciliation_internal(false, false)
    }

    /// Queries live Hyperliquid `clearinghouseState`, `openOrders`, and windowed `userFillsByTime`,
    /// cancels stale/untracked open orders (when `metadata_complete == true`), closes legacy positions
    /// when requested via [`crate::close_ladder::submit_reduce_only_close_ladder`], and reconciles
    /// open/closed positions 1:1 into MariaDB without holding `execution_lock()` over HTTP IO (`R14-H2`).
    pub fn run_cutover_reconciliation(
        &self,
        close_legacy_positions: bool,
    ) -> Result<CutoverReconcileReport> {
        self.run_reconciliation_internal(close_legacy_positions, true)
    }

    /// Refreshes equity snapshot (`clearinghouseState` + cliff-checked `userAbstraction` +
    /// `spotClearinghouseState`) strictly without holding `execution_lock()` over network IO (`P0-Risk`).
    pub fn refresh_equity_snapshot(&self, cfg: &ConfigMap) -> Result<f64> {
        refresh_equity_snapshot_unlocked(&self.db, &self.http, cfg)
    }

    fn run_reconciliation_internal(
        &self,
        close_legacy_positions: bool,
        force_ledger_replay: bool,
    ) -> Result<CutoverReconcileReport> {
        // Phase 1: Short lock (<2ms) to snapshot config, local held/ordered views, assets, and funding
        let (
            snapshot_ts_ms,
            cfg,
            mode,
            is_testnet,
            wallet,
            private_key,
            fallback_eq,
            local_held_before,
            local_ordered_before,
            assets_by_id,
            mut assets_by_symbol,
            db_funding_records,
        ) = {
            let _exec_guard = acquire_execution_lock();
            let ts = now_ms();
            let cfg = self.db.load_config_or_default();
            let _ = crate::engine::validate_cap_epoch_with_db(&self.db, &cfg, !force_ledger_replay);
            let mode = cfg.execution_mode();
            let is_testnet = is_testnet_configured(&cfg);
            let wallet = cfg.get_str("wallet_address", "");
            let private_key = cfg.get_str("private_key", "");
            let fallback_eq = cfg.get_f64("initial_paper_equity_usdc", 10_000.0);

            if mode == ExecutionMode::Paper || wallet.trim().is_empty() {
                let report = CutoverReconcileReport {
                    reconciled_at_ms: ts,
                    execution_mode: mode.as_str().to_string(),
                    is_testnet,
                    wallet_address: mask_identifier(&wallet),
                    exchange_reachable: false,
                    exchange_equity_usdc: fallback_eq,
                    details: vec![format!(
                        "Cutover reconciliation ran in {} mode (wallet_configured={})",
                        mode.as_str(),
                        !wallet.trim().is_empty()
                    )],
                    ..Default::default()
                };
                let _ = self.db.log_telemetry(
                    "INFO",
                    "execution.reconcile",
                    "Cutover reconciliation completed in PAPER / local mode",
                    Some(serde_json::to_value(&report).unwrap_or_default()),
                );
                return Ok(report);
            }

            let local_held_before = self.db.get_assets_held().unwrap_or_default();
            let local_ordered_before = self.db.get_assets_ordered().unwrap_or_default();
            let assets_list = self.db.list_exchange_assets().unwrap_or_default();
            let mut assets_by_id: HashMap<i64, ExchangeAsset> =
                HashMap::with_capacity(assets_list.len());
            let mut assets_by_symbol: HashMap<String, ExchangeAsset> =
                HashMap::with_capacity(assets_list.len() * 2);
            for a in assets_list {
                assets_by_symbol.insert(a.symbol.to_ascii_uppercase(), a.clone());
                assets_by_symbol.insert(a.coin.to_ascii_uppercase(), a.clone());
                assets_by_id.insert(a.id, a);
            }
            let db_funding_records = self.db.list_all_funding_events().unwrap_or_default();

            (
                ts,
                cfg,
                mode,
                is_testnet,
                wallet,
                private_key,
                fallback_eq,
                local_held_before,
                local_ordered_before,
                assets_by_id,
                assets_by_symbol,
                db_funding_records,
            )
        };

        let mut report = CutoverReconcileReport {
            reconciled_at_ms: snapshot_ts_ms,
            execution_mode: mode.as_str().to_string(),
            is_testnet,
            wallet_address: mask_identifier(&wallet),
            exchange_reachable: false,
            exchange_equity_usdc: fallback_eq,
            ..Default::default()
        };

        // Phase 2: Unlocked HTTP IO (ZERO execution_lock())
        let bg_opts =
            RequestOptions::background_sync().with_timeout_sec(self.http.timeout_sec.min(8));
        let ch_state = match self
            .http
            .fetch_clearinghouse_state_with_options(&wallet, &cfg, bg_opts)
        {
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

        let (open_coins, open_szi_by_coin) = extract_open_coins_and_szi(&ch_state);
        let equity_eval = evaluate_equity_snapshot_unlocked(
            &self.http,
            self.db.db_name(),
            &cfg,
            &ch_state,
            &open_coins,
            !local_held_before.is_empty(),
            snapshot_ts_ms,
        );
        if let Some((spot_total, _, _)) = equity_eval.fresh_spot {
            report.details.push(format!(
                "Synced Hyperliquid spot USDC balance (${spot_total:.2}, account_mode={})",
                equity_eval.account_mode
            ));
        }

        let (open_orders_snap_opt, open_orders_fetch_ok) = match self
            .http
            .fetch_open_orders_with_metadata_opts(&wallet, &cfg, bg_opts)
        {
            Ok(snap) => (Some(snap), true),
            Err(_) => (None, false),
        };
        let open_orders_snap = open_orders_snap_opt.unwrap_or(OpenOrdersSnapshot {
            orders: Vec::new(),
            metadata_complete: false,
        });

        let mut orders_index = index_open_orders(&open_orders_snap.orders);
        let orphan_plan = build_orphan_cancel_plan(
            &open_orders_snap,
            &orders_index,
            &open_coins,
            &open_szi_by_coin,
            &local_held_before,
            &local_ordered_before,
            &assets_by_id,
            &assets_by_symbol,
            snapshot_ts_ms,
            close_legacy_positions,
        );
        report.stale_orders_found = orphan_plan.orphan_count;

        let mut cancelled_oid_set: HashSet<u64> = HashSet::new();
        if !private_key.is_empty() && !orphan_plan.batch_cancels.is_empty() {
            if self
                .http
                .cancel_orders_batch(
                    orphan_plan.batch_cancels.clone(),
                    &private_key,
                    Some(&self.db),
                    &cfg,
                )
                .is_ok()
            {
                report.stale_orders_cancelled = orphan_plan.batch_cancels.len();
                for oid in orphan_plan.cancelled_oid_candidates {
                    orders_index.resting_exchange_oids.remove(&oid);
                    cancelled_oid_set.insert(oid);
                }
            }
            report.details.push(format!(
                "Cancelled {}/{} resting orphan orders on Hyperliquid",
                report.stale_orders_cancelled, report.stale_orders_found
            ));
        }
        let remaining_open_orders_vec: Vec<Value> = open_orders_snap
            .orders
            .into_iter()
            .filter(|ord| {
                extract_hl_oid(ord)
                    .map(|id| !cancelled_oid_set.contains(&id))
                    .unwrap_or(true)
            })
            .collect();
        let remaining_open_orders_snap = OpenOrdersSnapshot {
            orders: remaining_open_orders_vec,
            metadata_complete: open_orders_snap.metadata_complete,
        };

        let existing_open_by_coin: HashMap<String, HeldAssetView> = local_held_before
            .iter()
            .map(|h| (h.coin.to_ascii_uppercase(), h.clone()))
            .collect();
        let earliest_local_open_ms = local_held_before
            .iter()
            .filter_map(|h| h.opened_at_ms.filter(|&t| t > 0))
            .min();

        let cache_key = (
            self.db.db_name().to_string(),
            wallet.trim().to_ascii_lowercase(),
        );
        let (parsed_fills_by_coin, user_fills_fetch_ok, should_run_ledger_replay) = {
            let mut cache_map = self.ledger_cache.lock();
            if cache_map.len() > 32 {
                cache_map.clear();
            }
            let cache_entry = cache_map.entry(cache_key.clone()).or_default();
            let fills_res = fetch_windowed_fills_unlocked(
                &self.http,
                &wallet,
                cache_entry,
                earliest_local_open_ms,
                snapshot_ts_ms,
                &cfg,
            );
            let pos_delta_or_flat = cache_entry.prev_open_szi_by_coin != open_szi_by_coin
                || local_held_before
                    .iter()
                    .any(|h| !open_coins.contains(&h.coin.to_ascii_uppercase()));
            let replay_due = force_ledger_replay
                || pos_delta_or_flat
                || snapshot_ts_ms.saturating_sub(cache_entry.last_replay_ms)
                    >= LEDGER_REPLAY_MIN_INTERVAL_MS;
            match fills_res {
                Ok(map) => (map, true, replay_due),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Hyperliquid userFillsByTime fetch failed; preserving existing reconciled ledger"
                    );
                    (Default::default(), false, false)
                }
            }
        };

        let funding_by_coin = group_db_funding_records(&db_funding_records);
        let replay_out = if user_fills_fetch_ok {
            compute_fifo_closed_orders_and_funding(
                &parsed_fills_by_coin,
                &funding_by_coin,
                &open_szi_by_coin,
            )
        } else {
            FifoFundingReplayOutput::default()
        };
        if !user_fills_fetch_ok {
            report.details.push(
                "Skipped closed-trade and funding_events overwrite because userFillsByTime fetch failed (preserving cached funding)"
                    .to_string(),
            );
        }

        let limits = RiskLimitsConfig::from_config_map(&cfg);
        let mut meta_catalog = None;
        let prepared_positions = prepare_exchange_positions_unlocked(
            &self.http,
            &self.db,
            &ch_state,
            &assets_by_symbol,
            &existing_open_by_coin,
            &mut meta_catalog,
            open_orders_fetch_ok.then_some(&remaining_open_orders_snap),
            &private_key,
            limits.taker_fee_bps,
            close_legacy_positions,
            &cfg,
        );
        report.legacy_positions_found = prepared_positions.len();

        if meta_catalog.is_none()
            && collect_meta_coins_needed(&prepared_positions, &replay_out, &assets_by_symbol)
        {
            meta_catalog = Some(fetch_meta_catalog_unlocked(&self.http, &cfg));
        }

        // Phase 3: Short lock (<10ms) re-validating execution_mode and committing DB state
        let (post_cfg, tpsl_syncs) = {
            let _exec_guard = acquire_execution_lock();
            let post_cfg = self.db.load_config_or_default();
            if post_cfg.execution_mode() == ExecutionMode::Paper {
                report.details.push(
                    "Aborted Phase-3 reconcile DB commit because execution_mode switched to PAPER"
                        .to_string(),
                );
                return Ok(report);
            }

            let in_flight_positions: HashSet<i64> = self.in_flight_position_ops.lock().clone();
            let post_fallback_eq = post_cfg.get_f64("initial_paper_equity_usdc", fallback_eq);
            let (exchange_equity_usdc, eff_cap_eq) = commit_equity_snapshot_locked(
                &self.db,
                &post_cfg,
                &equity_eval,
                snapshot_ts_ms,
                post_fallback_eq,
            );
            report.exchange_equity_usdc = exchange_equity_usdc;

            if open_orders_fetch_ok {
                let _ = self.db.set_config_value(
                    "live_exchange_open_orders_count",
                    &remaining_open_orders_snap.orders.len().to_string(),
                    Some("Total resting Hyperliquid open orders from last reconcile (entry ALO + SL/TP triggers + passive closes)"),
                );
            }

            let (closed_cnt, adopted_cnt, orphans_cnt, mut tpsl_syncs) =
                commit_positions_and_ledger_locked(
                    &self.db,
                    &post_cfg,
                    snapshot_ts_ms,
                    exchange_equity_usdc,
                    eff_cap_eq,
                    &open_coins,
                    &orders_index.resting_exchange_oids,
                    &orders_index.cloid_to_oid,
                    open_orders_fetch_ok,
                    &orphan_plan.active_chasing_partial_coins,
                    &prepared_positions,
                    &existing_open_by_coin,
                    &parsed_fills_by_coin,
                    &replay_out,
                    user_fills_fetch_ok,
                    should_run_ledger_replay,
                    meta_catalog.as_ref(),
                    &mut assets_by_symbol,
                    &in_flight_positions,
                    close_legacy_positions,
                    &mut report.details,
                )?;
            report.legacy_positions_closed = closed_cnt;
            report.legacy_positions_adopted = adopted_cnt;
            report.local_orphans_closed = orphans_cnt;

            {
                let mut cache_map = self.ledger_cache.lock();
                if let Some(cache_entry) = cache_map.get_mut(&cache_key) {
                    cache_entry.prev_open_szi_by_coin = open_szi_by_coin.clone();
                    if should_run_ledger_replay && user_fills_fetch_ok {
                        cache_entry.last_replay_ms = snapshot_ts_ms;
                    }
                }
            }

            let live_in_flight = self.in_flight_position_ops.lock().clone();
            tpsl_syncs.retain(|t| !live_in_flight.contains(&t.position_id));
            (post_cfg, tpsl_syncs)
        };

        // Phase 4: Unlocked batched TP/SL synchronization
        sync_tpsl_unlocked(
            &self.http,
            &self.db,
            &tpsl_syncs,
            open_orders_fetch_ok.then_some(&remaining_open_orders_snap),
            &post_cfg,
        );

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

    // Backwards-compatible public methods preserved for callers/tests (all strictly unlocked over HTTP)

    pub fn sync_account_mode_if_due(&self, cfg: &ConfigMap, now_ms_val: i64) -> Option<String> {
        let cached_mode = cfg.get_str("live_exchange_account_mode", "default");
        let interval = if aegis_core::is_unified_account_mode(&cached_mode) {
            ABSTRACTION_SYNC_MIN_INTERVAL_MS
        } else {
            DEFAULT_MODE_ABSTRACTION_SYNC_MIN_INTERVAL_MS
        };
        self.sync_account_mode_with_interval(cfg, now_ms_val, interval)
    }

    pub fn sync_account_mode_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<String> {
        let outcome = fetch_account_mode_unlocked(
            &self.http,
            self.db.db_name(),
            cfg,
            now_ms_val,
            min_interval_ms,
        );
        let mode = outcome.mode?;
        let _guard = acquire_execution_lock();
        if self
            .db
            .set_config_value(
                "live_exchange_account_mode",
                &mode,
                Some("Latest synced Hyperliquid userAbstraction mode (default, unifiedAccount, portfolioMargin)"),
            )
            .is_err()
        {
            return None;
        }
        Some(mode)
    }

    pub fn sync_spot_usdc_if_due(&self, cfg: &ConfigMap, now_ms_val: i64) -> Option<f64> {
        self.sync_spot_usdc_with_interval(cfg, now_ms_val, VAULT_SYNC_MIN_INTERVAL_MS)
    }

    pub fn sync_spot_usdc_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<f64> {
        let outcome = fetch_spot_usdc_details_unlocked(
            &self.http,
            self.db.db_name(),
            cfg,
            now_ms_val,
            min_interval_ms,
        );
        let (spot_usdc, _, _) = outcome.details?;
        let _guard = acquire_execution_lock();
        self.db
            .set_config_value(
                "live_exchange_spot_usdc",
                &format!("{spot_usdc:.2}"),
                Some("Latest synced Hyperliquid spot USDC balance (informational; segregated from perp margin)"),
            )
            .ok()?;
        let _ = self.db.set_config_value(
            "live_exchange_spot_synced_at_ms",
            &now_ms_val.to_string(),
            Some("Timestamp (ms) of latest Hyperliquid spotClearinghouseState sync"),
        );
        Some(spot_usdc)
    }

    pub fn sync_portfolio_pnl_if_due(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
    ) -> Option<CachedHlPortfolioPnl> {
        AuxiliarySyncWorker::new(self.db.clone(), self.http.clone())
            .sync_portfolio_pnl_if_due(cfg, now_ms_val)
    }

    pub fn sync_portfolio_pnl_with_interval(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        min_interval_ms: i64,
    ) -> Option<CachedHlPortfolioPnl> {
        AuxiliarySyncWorker::new(self.db.clone(), self.http.clone())
            .sync_portfolio_pnl_with_interval(cfg, now_ms_val, min_interval_ms)
    }

    pub fn sync_vault_info_if_due(
        &self,
        cfg: &ConfigMap,
        now_ms_val: i64,
        perp_equity_override: Option<f64>,
    ) -> Option<VaultInfoPayload> {
        AuxiliarySyncWorker::new(self.db.clone(), self.http.clone()).sync_vault_info_if_due(
            cfg,
            now_ms_val,
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
        AuxiliarySyncWorker::new(self.db.clone(), self.http.clone()).sync_vault_info_with_interval(
            cfg,
            now_ms_val,
            min_interval_ms,
            perp_equity_override,
        )
    }
}
