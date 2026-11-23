//! Consolidated Hyperliquid wire parsing, trigger classification, and lot-completion helpers (`aegis-core::hl_parse`).

use crate::time::round_dp;
use serde_json::Value;

/// Canonical Hyperliquid oracle price-band safety clamp (`3.80%` inside HL's `+-4.0%` band) (`R14-M5`).
pub const HL_ORACLE_BAND_PCT: f64 = 0.038;

/// Grace window in milliseconds before an unfilled `WORKING` entry order escalates (`R14-M5`).
pub const ENTRY_PARTIAL_GRACE_MS: i64 = 45_000;

/// Minimum filled fraction (`85%`) treating a partially filled `WORKING` order as complete (`R14-M5`).
pub const ENTRY_FILL_COMPLETE_RATIO: f64 = 0.85;

/// Depth aggregation band around top-of-book (`15 bps = 0.15%`) for `l2Book` liquidity checks (`R14-M5`).
pub const L2_DEPTH_BAND_PCT: f64 = 0.0015;

/// Parses a JSON number or trimmed numeric string into a finite `f64` (`R14-L2`).
pub fn parse_hl_f64_opt(val: Option<&Value>) -> Option<f64> {
    val.and_then(|x| {
        x.as_f64()
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<f64>().ok()))
    })
    .filter(|v| v.is_finite())
}

/// Parses a JSON number or numeric string into a finite `f64`, defaulting to `0.0`.
pub fn parse_hl_f64(val: Option<&Value>) -> f64 {
    parse_hl_f64_opt(val).unwrap_or(0.0)
}

/// Parses a JSON integer, non-negative float, or trimmed numeric string into `i64`.
pub fn parse_hl_i64_opt(val: Option<&Value>) -> Option<i64> {
    val.and_then(|x| {
        x.as_i64()
            .or_else(|| x.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
            .or_else(|| {
                x.as_f64()
                    .filter(|f| f.is_finite() && *f >= 0.0 && *f <= (i64::MAX as f64))
                    .map(|f| f.floor() as i64)
            })
    })
}

/// Parses a JSON unsigned integer or trimmed numeric string into `u64`.
pub fn parse_hl_u64_opt(val: Option<&Value>) -> Option<u64> {
    val.and_then(|x| {
        x.as_u64()
            .or_else(|| x.as_i64().and_then(|i| u64::try_from(i).ok()))
            .or_else(|| x.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
    })
}

/// Parses a persisted Hyperliquid order ID string (`"hl-12345"` or `"12345"`) into a positive `u64`.
pub fn parse_hl_oid(raw: Option<&str>) -> Option<u64> {
    raw.and_then(|s| {
        let trimmed = s.trim();
        let digits = trimmed.strip_prefix("hl-").unwrap_or(trimmed).trim();
        digits.parse::<u64>().ok()
    })
    .filter(|&v| v > 0)
}

/// Extracts a positive Hyperliquid `oid` from a JSON order or ack object.
pub fn extract_hl_oid(ord: &Value) -> Option<u64> {
    parse_hl_u64_opt(ord.get("oid")).filter(|&v| v > 0)
}

/// Returns `true` if a Hyperliquid order JSON object represents a conditional SL/TP trigger order.
pub fn is_hl_trigger_order(ord: &Value) -> bool {
    if ord.get("isTrigger").and_then(Value::as_bool) == Some(true)
        || ord.get("isPositionTpsl").and_then(Value::as_bool) == Some(true)
    {
        return true;
    }
    if parse_hl_f64_opt(ord.get("triggerPx")).is_some_and(|px| px > 0.0) {
        return true;
    }
    ord.get("orderType")
        .and_then(Value::as_str)
        .is_some_and(|t| {
            let lower = t.trim().to_ascii_lowercase();
            lower.contains("stop")
                || lower.contains("take profit")
                || lower == "tp"
                || lower == "sl"
        })
}

/// Classifies a Hyperliquid trigger order JSON object as Take Profit (`Some(true)`),
/// Stop Loss (`Some(false)`), or non-trigger/unknown (`None`).
pub fn classify_hl_trigger_is_tp(ord: &Value) -> Option<bool> {
    if !is_hl_trigger_order(ord) {
        return None;
    }
    let order_type = ord
        .get("orderType")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if order_type.contains("take profit") || order_type == "tp" {
        return Some(true);
    }
    if order_type.contains("stop") || order_type == "sl" {
        return Some(false);
    }
    match ord
        .get("tpsl")
        .and_then(Value::as_str)
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("tp") => Some(true),
        Some("sl") => Some(false),
        _ => None,
    }
}

#[inline]
fn resolve_effective_lot_step(lot_step_size: f64, sz_decimals: u32) -> f64 {
    if lot_step_size.is_finite() && lot_step_size > 0.0 {
        lot_step_size
    } else {
        10_f64.powi(-(sz_decimals.min(6) as i32))
    }
}

/// Computes the lot-floored remaining entry quantity (`requested_qty - filled_qty`) for Hyperliquid sizing.
pub fn remaining_entry_qty_for_lot(
    filled_qty: f64,
    requested_qty: f64,
    lot_step_size: f64,
    sz_decimals: u32,
) -> f64 {
    if !requested_qty.is_finite() || requested_qty <= 0.0 {
        return 0.0;
    }
    let filled = if filled_qty.is_finite() && filled_qty > 0.0 {
        filled_qty
    } else {
        0.0
    };
    let rem = (requested_qty - filled).max(0.0);
    if rem <= 0.0 {
        return 0.0;
    }
    let eff_lot = resolve_effective_lot_step(lot_step_size, sz_decimals);
    let snapped = ((rem / eff_lot) + 1e-9).floor() * eff_lot;
    round_dp(snapped.max(0.0), sz_decimals.min(8) as i32)
}

/// Returns `true` when `filled_qty` satisfies [`ENTRY_FILL_COMPLETE_RATIO`] (`85%`)
/// or the remaining quantity is below half of one lot step (`<= 0.5 * lot_step_size`).
pub fn is_entry_fill_complete(
    filled_qty: f64,
    requested_qty: f64,
    lot_step_size: f64,
    sz_decimals: u32,
) -> bool {
    if !filled_qty.is_finite() || filled_qty <= 1e-9 {
        return false;
    }
    if !requested_qty.is_finite() || requested_qty <= 1e-9 {
        return true;
    }
    let eff_lot = resolve_effective_lot_step(lot_step_size, sz_decimals);
    let rem_qty =
        remaining_entry_qty_for_lot(filled_qty, requested_qty, lot_step_size, sz_decimals);
    filled_qty + 1e-9 >= requested_qty * ENTRY_FILL_COMPLETE_RATIO
        || rem_qty <= eff_lot * 0.5
        || rem_qty <= 1e-9
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_r14_l2_hl_parse_contract() {
        assert_eq!(parse_hl_f64_opt(Some(&json!(" 184.25 "))), Some(184.25));
        assert_eq!(parse_hl_f64_opt(Some(&json!(42.5))), Some(42.5));
        assert_eq!(parse_hl_f64_opt(Some(&json!("NaN"))), None);
        assert_eq!(parse_hl_f64(Some(&json!("bad"))), 0.0);

        assert_eq!(
            parse_hl_i64_opt(Some(&json!("1795000000000"))),
            Some(1_795_000_000_000)
        );
        assert_eq!(parse_hl_u64_opt(Some(&json!(98765))), Some(98765));
        assert_eq!(parse_hl_u64_opt(Some(&json!(-1))), None);

        assert_eq!(parse_hl_oid(Some("hl-123456")), Some(123456));
        assert_eq!(parse_hl_oid(Some(" 777 ")), Some(777));
        assert_eq!(parse_hl_oid(Some("hl-0")), None);
        assert_eq!(parse_hl_oid(None), None);

        assert_eq!(extract_hl_oid(&json!({ "oid": "9001" })), Some(9001));
        assert_eq!(extract_hl_oid(&json!({ "oid": 9002 })), Some(9002));
        assert_eq!(extract_hl_oid(&json!({ "oid": 0 })), None);

        let sl = json!({ "isTrigger": true, "orderType": "Stop Limit", "triggerPx": "175.0" });
        let tp =
            json!({ "isTrigger": true, "orderType": "Take Profit Market", "triggerPx": "200.0" });
        let limit = json!({ "isTrigger": false, "orderType": "Limit", "limitPx": "180.0" });
        assert!(is_hl_trigger_order(&sl));
        assert!(is_hl_trigger_order(&tp));
        assert!(!is_hl_trigger_order(&limit));
        assert_eq!(classify_hl_trigger_is_tp(&sl), Some(false));
        assert_eq!(classify_hl_trigger_is_tp(&tp), Some(true));
        assert_eq!(classify_hl_trigger_is_tp(&limit), None);

        assert!(!is_entry_fill_complete(0.101, 0.274, 0.001, 3));
        assert!(!is_entry_fill_complete(388.0, 2127.0, 1.0, 0));
        assert!(is_entry_fill_complete(1808.0, 2127.0, 1.0, 0));
        assert!(!is_entry_fill_complete(1.0, 2.0, 1.0, 0));
        assert!(is_entry_fill_complete(0.2736, 0.274, 0.001, 3));
        assert_eq!(remaining_entry_qty_for_lot(1.0, 2.5, 0.5, 1), 1.5);
    }
}
