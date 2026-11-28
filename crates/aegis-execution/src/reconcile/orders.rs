//! Resting open-order reconciliation, working-order disposition, and `R14-M7` metadata-guarded
//! orphan cancellation (`aegis-execution::reconcile::orders`).

use crate::http_client::OpenOrdersSnapshot;
use crate::signing::{derive_hl_cloid_hex, CancelWire};
use aegis_core::{
    extract_hl_oid, is_entry_fill_complete, is_hl_trigger_order, normalize_hyperliquid_symbol,
    parse_hl_f64, parse_hl_oid, parse_hl_u64_opt, OrderSide, OrderedAssetView,
    ENTRY_PARTIAL_GRACE_MS,
};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

const MAX_MAKER_ONLY_REST_PROTECT_MS: i64 = 600_000;

#[derive(Debug, Clone)]
pub(crate) struct AdoptableRestingEntryOrder {
    pub oid: u64,
    pub coin_upper: String,
    pub side: OrderSide,
    pub limit_price: f64,
    pub quantity: f64,
    pub cloid: Option<String>,
}

static RECENT_RESTING_ENTRY_ORDERS: LazyLock<Mutex<HashMap<u64, AdoptableRestingEntryOrder>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) fn lookup_resting_entry_order_by_oid(oid: u64) -> Option<AdoptableRestingEntryOrder> {
    RECENT_RESTING_ENTRY_ORDERS.lock().get(&oid).cloned()
}

/// Generates all candidate Hyperliquid `cloid` strings (raw + Keccak256 hex across initial, chased,
/// passive re-quote, and IOC suffixes) for an internal `client_order_id` (`W1`).
pub(crate) fn candidate_hl_cloids_for_coid(
    client_order_id: &str,
    max_chase_count: u32,
) -> Vec<String> {
    let trimmed = client_order_id.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }
    let max_c = max_chase_count.min(16).saturating_add(2);
    let mut out = Vec::with_capacity(3 + (max_c as usize) * 2);
    out.push(trimmed.to_ascii_lowercase());
    out.push(derive_hl_cloid_hex(trimmed));
    for c in 1..=max_c {
        let chase_coid = format!("{trimmed}-c{c}");
        out.push(derive_hl_cloid_hex(&chase_coid));
        out.push(derive_hl_cloid_hex(&format!("{chase_coid}-r1")));
    }
    out.push(derive_hl_cloid_hex(&format!("{trimmed}-ioc")));
    out
}

pub(crate) fn resolve_working_entry_oid_with_resting(
    ordered: &OrderedAssetView,
    resting_exchange_oids: &HashSet<u64>,
    cloid_to_oid: &HashMap<String, u64>,
) -> Option<u64> {
    let parsed_eoid = parse_hl_oid(ordered.exchange_order_id.as_deref());
    if let Some(oid) = parsed_eoid {
        if resting_exchange_oids.contains(&oid) {
            return Some(oid);
        }
    }
    let cands = candidate_hl_cloids_for_coid(&ordered.client_order_id, ordered.chase_count);
    for cand in &cands {
        if let Some(&oid) = cloid_to_oid.get(cand) {
            if resting_exchange_oids.contains(&oid) {
                return Some(oid);
            }
        }
    }
    parsed_eoid.or_else(|| {
        cands
            .into_iter()
            .find_map(|cand| cloid_to_oid.get(&cand).copied())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkingOrderReconcileDisposition {
    ProtectPending,
    ProtectActivePartial,
    AdoptOrExpire,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn classify_working_order_for_reconcile(
    age_ms: i64,
    order_side: &str,
    requested_qty: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    entry_oid: Option<u64>,
    resting_exchange_oids: &HashSet<u64>,
    exchange_szi_opt: Option<f64>,
) -> WorkingOrderReconcileDisposition {
    if age_ms >= ENTRY_PARTIAL_GRACE_MS {
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
    parse_hl_u64_opt(ord.get("timestamp")).unwrap_or(0)
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

/// Returns `true` when a resting `frontendOpenOrders` entry is a non-trigger `reduceOnly`
/// passive Limit close order (e.g. `close_ladder`'s fallback GTC close on an illiquid book).
pub(crate) fn is_passive_reduce_only_close_order(ord: &Value) -> bool {
    if ord.get("reduceOnly").and_then(Value::as_bool) != Some(true) {
        return false;
    }
    if is_hl_trigger_order(ord) {
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
        let Some(oid) = extract_hl_oid(ord) else {
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
///
/// Enforces `R14-M7`: when `metadata_complete == false && !close_legacy_positions`, skips all orphan
/// cancellation so trigger-stripped `openOrders` fallbacks never delete SL/TP or passive closes.
pub fn find_orphan_orders_to_cancel_with_metadata(
    open_orders: &[Value],
    metadata_complete: bool,
    open_coins: &HashSet<String>,
    tracked_protected_oids: &HashSet<u64>,
    close_legacy_positions: bool,
) -> (HashSet<u64>, Vec<(String, u64)>) {
    if !metadata_complete && !close_legacy_positions {
        tracing::warn!(
            order_count = open_orders.len(),
            "Skipping orphan order cancellation because openOrders metadata_complete=false (R14-M7)"
        );
        let all_oids: HashSet<u64> = open_orders.iter().filter_map(extract_hl_oid).collect();
        return (all_oids, Vec::new());
    }

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
        let Some(oid) = extract_hl_oid(ord) else {
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

/// Convenience wrapper assuming complete `frontendOpenOrders` metadata.
pub fn find_orphan_orders_to_cancel(
    open_orders: &[Value],
    open_coins: &HashSet<String>,
    tracked_protected_oids: &HashSet<u64>,
    close_legacy_positions: bool,
) -> (HashSet<u64>, Vec<(String, u64)>) {
    find_orphan_orders_to_cancel_with_metadata(
        open_orders,
        true,
        open_coins,
        tracked_protected_oids,
        close_legacy_positions,
    )
}

pub(crate) fn is_resting_entry_limit_order(ord: &Value) -> bool {
    if ord.get("reduceOnly").and_then(Value::as_bool) == Some(true) {
        return false;
    }
    if is_hl_trigger_order(ord) {
        return false;
    }
    ord.get("orderType")
        .and_then(Value::as_str)
        .is_none_or(|t| {
            let trimmed = t.trim();
            trimmed.is_empty() || trimmed.eq_ignore_ascii_case("Limit")
        })
}

fn parse_resting_entry_order(ord: &Value) -> Option<AdoptableRestingEntryOrder> {
    if !is_resting_entry_limit_order(ord) {
        return None;
    }
    let oid = extract_hl_oid(ord)?;
    let coin_upper = normalize_order_coin_upper(ord)?;
    let raw_side = ord.get("side").and_then(Value::as_str)?.trim();
    let side = if raw_side.eq_ignore_ascii_case("B") || raw_side.eq_ignore_ascii_case("LONG") {
        OrderSide::Long
    } else if raw_side.eq_ignore_ascii_case("A") || raw_side.eq_ignore_ascii_case("SHORT") {
        OrderSide::Short
    } else {
        return None;
    };
    let limit_price = parse_hl_f64(ord.get("limitPx"));
    let quantity = parse_hl_f64(ord.get("sz"));
    if !limit_price.is_finite() || limit_price <= 0.0 || !quantity.is_finite() || quantity <= 0.0 {
        return None;
    }
    let cloid = ord
        .get("cloid")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty());
    Some(AdoptableRestingEntryOrder {
        oid,
        coin_upper,
        side,
        limit_price,
        quantity,
        cloid,
    })
}

pub(crate) struct ParsedOrdersIndex {
    pub resting_exchange_oids: HashSet<u64>,
    pub cloid_to_oid: HashMap<String, u64>,
    pub adoptable_entry_orders: Vec<AdoptableRestingEntryOrder>,
}

pub(crate) fn index_open_orders(open_orders: &[Value]) -> ParsedOrdersIndex {
    let mut resting_exchange_oids = HashSet::new();
    let mut cloid_to_oid = HashMap::new();
    let mut adoptable_entry_orders = Vec::new();
    for ord in open_orders {
        if let Some(oid) = extract_hl_oid(ord) {
            resting_exchange_oids.insert(oid);
            if let Some(cloid) = ord.get("cloid").and_then(Value::as_str) {
                let trimmed = cloid.trim();
                if !trimmed.is_empty() {
                    cloid_to_oid.insert(trimmed.to_ascii_lowercase(), oid);
                }
            }
            if let Some(entry_ord) = parse_resting_entry_order(ord) {
                adoptable_entry_orders.push(entry_ord);
            }
        }
    }
    if !adoptable_entry_orders.is_empty() {
        let mut cache = RECENT_RESTING_ENTRY_ORDERS.lock();
        if cache.len() > 256 {
            cache.clear();
        }
        for e in &adoptable_entry_orders {
            cache.insert(e.oid, e.clone());
        }
    }
    ParsedOrdersIndex {
        resting_exchange_oids,
        cloid_to_oid,
        adoptable_entry_orders,
    }
}

pub(crate) struct OrphanCancelPlan {
    pub active_chasing_partial_coins: HashSet<String>,
    pub orphan_count: usize,
    pub batch_cancels: Vec<CancelWire>,
    pub cancelled_oid_candidates: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_orphan_cancel_plan(
    snap: &OpenOrdersSnapshot,
    index: &ParsedOrdersIndex,
    open_coins: &HashSet<String>,
    open_szi_by_coin: &HashMap<String, f64>,
    local_held: &[aegis_core::HeldAssetView],
    local_ordered: &[OrderedAssetView],
    assets_by_id: &HashMap<i64, aegis_core::ExchangeAsset>,
    assets_by_symbol: &HashMap<String, aegis_core::ExchangeAsset>,
    snapshot_ts_ms: i64,
    close_legacy_positions: bool,
) -> OrphanCancelPlan {
    let mut tracked_protected_oids: HashSet<u64> = HashSet::new();
    let mut active_chasing_partial_coins: HashSet<String> = HashSet::new();

    if !close_legacy_positions {
        let mut held_same_side: HashSet<(String, String)> = HashSet::new();
        for h in local_held {
            let coin_u = h.coin.to_ascii_uppercase();
            if !open_coins.contains(&coin_u) {
                continue;
            }
            held_same_side.insert((coin_u, h.side.to_ascii_uppercase()));
            for raw_oid in [
                h.protective_stop_oid.as_deref(),
                h.take_profit_oid.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(oid) = parse_hl_oid(Some(raw_oid)).or_else(|| {
                    index
                        .cloid_to_oid
                        .get(&raw_oid.trim().to_ascii_lowercase())
                        .copied()
                }) {
                    tracked_protected_oids.insert(oid);
                }
            }
        }
        for o in local_ordered {
            let coin_upper = o.coin.to_ascii_uppercase();
            let entry_oid = resolve_working_entry_oid_with_resting(
                o,
                &index.resting_exchange_oids,
                &index.cloid_to_oid,
            );
            let age_ms = snapshot_ts_ms.saturating_sub(o.created_at_ms);

            // W1: Same-side WORKING scale-in (`INCREASE`) orders alongside an active OPEN parent
            // are ALWAYS protected while resting on Hyperliquid (and do not block OPEN parent sync).
            if held_same_side.contains(&(coin_upper.clone(), o.side.to_ascii_uppercase())) {
                if let Some(oid) = entry_oid {
                    tracked_protected_oids.insert(oid);
                }
                continue;
            }

            let (lot_step_size, sz_decimals) = assets_by_id
                .get(&o.asset_id)
                .map(|a| (a.lot_step_size, a.sz_decimals))
                .unwrap_or((1e-6, 6));
            let exch_szi = open_szi_by_coin.get(&coin_upper).copied();
            let disposition = classify_working_order_for_reconcile(
                age_ms,
                &o.side,
                o.requested_quantity,
                lot_step_size,
                sz_decimals,
                entry_oid,
                &index.resting_exchange_oids,
                exch_szi,
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
                WorkingOrderReconcileDisposition::AdoptOrExpire => {
                    // Protect resting unfilled MakerOnly WORKING orders up to MAX_MAKER_ONLY_REST_PROTECT_MS
                    if exch_szi.is_none_or(|s| s.abs() <= 1e-9)
                        && age_ms < MAX_MAKER_ONLY_REST_PROTECT_MS
                    {
                        if let Some(oid) = entry_oid {
                            if index.resting_exchange_oids.contains(&oid) {
                                tracked_protected_oids.insert(oid);
                            }
                        }
                    }
                }
            }
        }

        // W1: Protect live non-trigger resting entry orders carrying a client order ID (`cloid`) on a
        // known asset (same-side if position open) so Phase 3 adopts/re-links them instead of orphaning exposure.
        for entry_ord in &index.adoptable_entry_orders {
            if tracked_protected_oids.contains(&entry_ord.oid) {
                continue;
            }
            if entry_ord.cloid.is_none() {
                continue;
            }
            let sym = format!("{}-USDC", entry_ord.coin_upper);
            let known_asset = assets_by_symbol.contains_key(&sym)
                || assets_by_symbol.contains_key(&entry_ord.coin_upper);
            if !known_asset {
                continue;
            }
            let same_or_flat = open_szi_by_coin
                .get(&entry_ord.coin_upper)
                .is_none_or(|&szi| szi.abs() <= 1e-9 || (szi > 0.0) == entry_ord.side.is_buy());
            if same_or_flat {
                tracked_protected_oids.insert(entry_ord.oid);
            }
        }
    }

    let (_protected_oids, orphan_orders) = find_orphan_orders_to_cancel_with_metadata(
        &snap.orders,
        snap.metadata_complete,
        open_coins,
        &tracked_protected_oids,
        close_legacy_positions,
    );

    let orphan_count = orphan_orders.len();
    let mut batch_cancels = Vec::new();
    let mut cancelled_oid_candidates = Vec::new();
    for (coin, oid) in orphan_orders {
        let symbol = normalize_hyperliquid_symbol(&coin)
            .map(|(sym, _)| sym)
            .unwrap_or_else(|_| coin.clone());
        if let Some(asset) = assets_by_symbol
            .get(&symbol.to_ascii_uppercase())
            .or_else(|| assets_by_symbol.get(&coin.to_ascii_uppercase()))
        {
            batch_cancels.push(CancelWire {
                a: asset.asset_index,
                o: oid,
            });
            cancelled_oid_candidates.push(oid);
        }
    }

    OrphanCancelPlan {
        active_chasing_partial_coins,
        orphan_count,
        batch_cancels,
        cancelled_oid_candidates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_is_entry_fill_complete_and_working_reconcile_disposition() {
        assert!(!is_entry_fill_complete(0.101, 0.274, 0.001, 3));
        assert!(!is_entry_fill_complete(388.0, 2127.0, 1.0, 0));
        assert!(is_entry_fill_complete(1808.0, 2127.0, 1.0, 0));
        assert!(!is_entry_fill_complete(1.0, 2.0, 1.0, 0));
        assert!(is_entry_fill_complete(0.2736, 0.274, 0.001, 3));

        let mut resting = HashSet::new();
        resting.insert(65_654_266_947u64);

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
        assert!(is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 100u64,
            "reduceOnly": true,
            "orderType": "Limit"
        })));
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "PUMP",
            "oid": 101u64,
            "reduceOnly": false,
            "isTrigger": false,
            "orderType": "Limit",
            "triggerPx": "0.0"
        })));
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 102u64
        })));
        assert!(!is_passive_reduce_only_close_order(&json!({
            "coin": "MET",
            "oid": 103u64,
            "reduceOnly": true,
            "isTrigger": true,
            "orderType": "Stop Limit",
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
            "orderType": "Take Profit Limit"
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
    fn test_passive_reduce_only_close_order_protection_and_metadata_guard() {
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
            "orderType": "Stop Limit",
            "triggerPx": "0.20111",
            "timestamp": 1_795_181_000_000u64
        });
        let tp_met = json!({
            "coin": "MET",
            "oid": 65_648_930_301u64,
            "reduceOnly": true,
            "isTrigger": true,
            "orderType": "Take Profit Limit",
            "triggerPx": "0.25086",
            "timestamp": 1_795_181_000_500u64
        });
        let tracked_triggers: HashSet<u64> =
            [65_605_015_926u64, 65_648_930_301u64].into_iter().collect();

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

        // R14-M7: When metadata_complete=false (openOrders fallback), skip orphan cancellation completely
        let stripped_sl = json!({
            "coin": "MET",
            "oid": 99_999_001u64,
            "side": "A",
            "limitPx": "0.20",
            "sz": "100.0"
        });
        let (protected_m7, orphans_m7) = find_orphan_orders_to_cancel_with_metadata(
            &[stripped_sl],
            false,
            &HashSet::new(),
            &HashSet::new(),
            false,
        );
        assert!(protected_m7.contains(&99_999_001u64));
        assert!(orphans_m7.is_empty());

        open_coins.insert("KPEPE".to_string());
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
    }

    #[test]
    fn test_w1_scale_in_cloid_matching_and_resting_entry_protection() {
        let base_coid = "v2-doge-long-174773";
        let chase1_cloid = derive_hl_cloid_hex(&format!("{base_coid}-c1"));
        let cands = candidate_hl_cloids_for_coid(base_coid, 1);
        assert!(cands.contains(&derive_hl_cloid_hex(base_coid)));
        assert!(cands.contains(&chase1_cloid));
        assert!(cands.contains(&derive_hl_cloid_hex(&format!("{base_coid}-c1-r1"))));
        assert!(cands.contains(&derive_hl_cloid_hex(&format!("{base_coid}-ioc"))));

        let adopt_cloid = derive_hl_cloid_hex("v2-doge-long-orphan-rest");
        let snap = OpenOrdersSnapshot {
            orders: vec![
                json!({
                    "coin": "DOGE",
                    "oid": 91_001u64,
                    "side": "B",
                    "limitPx": "0.1999",
                    "sz": "1006.0",
                    "reduceOnly": false,
                    "isTrigger": false,
                    "orderType": "Limit",
                    "cloid": chase1_cloid,
                    "timestamp": 1_795_200_000_000u64
                }),
                json!({
                    "coin": "DOGE",
                    "oid": 91_002u64,
                    "side": "B",
                    "limitPx": "0.1985",
                    "sz": "500.0",
                    "reduceOnly": false,
                    "isTrigger": false,
                    "orderType": "Limit",
                    "cloid": adopt_cloid,
                    "timestamp": 1_795_200_001_000u64
                }),
            ],
            metadata_complete: true,
        };
        let index = index_open_orders(&snap.orders);

        let ordered_scale_in = OrderedAssetView {
            position_id: 174_773,
            asset_id: 7,
            symbol: "DOGE-USDC".to_string(),
            coin: "DOGE".to_string(),
            client_order_id: base_coid.to_string(),
            side: "LONG".to_string(),
            fsm_state: "CHASING_ALO".to_string(),
            limit_price: 0.1999,
            mid_price: 0.2000,
            mark_price: 0.2000,
            distance_from_mid_bps: 5.0,
            requested_quantity: 1_006.0,
            filled_quantity: 0.0,
            reserved_margin_usdc: 100.55,
            s1_weight_contrib: 0.05,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            protective_stop_px: 0.1900,
            take_profit_px: 0.2200,
            chase_count: 1,
            exchange_order_id: None,
            created_at_ms: 1_795_199_950_000,
            updated_at_ms: 1_795_200_000_000,
        };
        assert_eq!(
            resolve_working_entry_oid_with_resting(
                &ordered_scale_in,
                &index.resting_exchange_oids,
                &index.cloid_to_oid,
            ),
            Some(91_001u64)
        );

        let doge_asset = aegis_core::ExchangeAsset {
            id: 7,
            symbol: "DOGE-USDC".to_string(),
            coin: "DOGE".to_string(),
            asset_index: 12,
            sz_decimals: 0,
            tick_size: 0.00001,
            lot_step_size: 1.0,
            max_leverage: 10.0,
            mark_price: 0.2000,
            mid_price: 0.2000,
            oracle_price: 0.2000,
            funding_rate_hourly: 0.0001,
            day_volume_usdc: 25_000_000.0,
            open_interest_usdc: 10_000_000.0,
            prev_day_price: 0.1950,
            is_core_eligible: true,
            is_event_eligible: true,
            is_priority_watched: true,
            created_at_ms: 1_795_190_000_000,
            updated_at_ms: 1_795_200_000_000,
        };
        let held_parent = aegis_core::HeldAssetView {
            position_id: 174_700,
            asset_id: 7,
            symbol: "DOGE-USDC".to_string(),
            coin: "DOGE".to_string(),
            client_order_id: "v2-doge-long-parent".to_string(),
            side: "LONG".to_string(),
            quantity: 5_000.0,
            committed_notional_usdc: 1_000.0,
            portfolio_weight: 0.25,
            entry_price: 0.2000,
            mark_price: 0.2000,
            protective_stop_px: 0.1900,
            entry_r0: 0.0100,
            protective_stop_oid: None,
            stop_distance_pct: None,
            take_profit_px: 0.2200,
            take_profit_oid: None,
            tp_distance_pct: None,
            highest_price_seen: 0.2000,
            lowest_price_seen: 0.2000,
            unrealized_pnl_usdc: 0.0,
            unrealized_pnl_pct: 0.0,
            realized_pnl_usdc: 0.0,
            funding_accrued_usdc: 0.0,
            fees_paid_usdc: 0.0,
            s1_weight_contrib: 0.25,
            s2_weight_contrib: 0.0,
            s3_weight_contrib: 0.0,
            latest_target_weight: 0.30,
            latest_delta_weight: 0.05,
            created_at_ms: 1_795_190_000_000,
            opened_at_ms: Some(1_795_190_000_000),
            updated_at_ms: 1_795_200_000_000,
        };
        let open_coins: HashSet<String> = ["DOGE".to_string()].into_iter().collect();
        let open_szi: HashMap<String, f64> = [("DOGE".to_string(), 5_000.0)].into_iter().collect();
        let assets_by_id: HashMap<i64, aegis_core::ExchangeAsset> =
            [(7i64, doge_asset.clone())].into_iter().collect();
        let assets_by_sym: HashMap<String, aegis_core::ExchangeAsset> = [
            ("DOGE-USDC".to_string(), doge_asset.clone()),
            ("DOGE".to_string(), doge_asset),
        ]
        .into_iter()
        .collect();

        let plan = build_orphan_cancel_plan(
            &snap,
            &index,
            &open_coins,
            &open_szi,
            &[held_parent],
            &[ordered_scale_in],
            &assets_by_id,
            &assets_by_sym,
            1_795_200_060_000,
            false,
        );
        assert_eq!(plan.orphan_count, 0);
        assert!(plan.batch_cancels.is_empty());
        assert!(
            !plan.active_chasing_partial_coins.contains("DOGE"),
            "Same-side scale-in WORKING order must not block OPEN parent sync"
        );
    }
}
