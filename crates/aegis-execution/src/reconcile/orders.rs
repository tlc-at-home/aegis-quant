//! Resting open-order reconciliation, working-order disposition, and `R14-M7` metadata-guarded
//! orphan cancellation (`aegis-execution::reconcile::orders`).

use crate::http_client::OpenOrdersSnapshot;
use crate::signing::CancelWire;
use aegis_core::{
    extract_hl_oid, is_entry_fill_complete, is_hl_trigger_order, normalize_hyperliquid_symbol,
    parse_hl_oid, parse_hl_u64_opt, OrderedAssetView, ENTRY_PARTIAL_GRACE_MS,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkingOrderReconcileDisposition {
    ProtectPending,
    ProtectActivePartial,
    AdoptOrExpire,
}

pub(crate) fn resolve_working_entry_oid(
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

pub(crate) struct ParsedOrdersIndex {
    pub resting_exchange_oids: HashSet<u64>,
    pub cloid_to_oid: HashMap<String, u64>,
}

pub(crate) fn index_open_orders(open_orders: &[Value]) -> ParsedOrdersIndex {
    let mut resting_exchange_oids = HashSet::new();
    let mut cloid_to_oid = HashMap::new();
    for ord in open_orders {
        if let Some(oid) = extract_hl_oid(ord) {
            resting_exchange_oids.insert(oid);
            if let Some(cloid) = ord.get("cloid").and_then(Value::as_str) {
                cloid_to_oid.insert(cloid.trim().to_ascii_lowercase(), oid);
            }
        }
    }
    ParsedOrdersIndex {
        resting_exchange_oids,
        cloid_to_oid,
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
            let entry_oid = resolve_working_entry_oid(o, &index.cloid_to_oid);
            let (lot_step_size, sz_decimals) = assets_by_id
                .get(&o.asset_id)
                .map(|a| (a.lot_step_size, a.sz_decimals))
                .unwrap_or((1e-6, 6));
            let disposition = classify_working_order_for_reconcile(
                snapshot_ts_ms.saturating_sub(o.created_at_ms),
                &o.side,
                o.requested_quantity,
                lot_step_size,
                sz_decimals,
                entry_oid,
                &index.resting_exchange_oids,
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
}
