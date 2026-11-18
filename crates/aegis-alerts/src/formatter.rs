//! Rich HTML Telegram Card Templates (`parse_mode = "HTML"`) for Aegis Quant v2.0 (`aegis-alerts::formatter`).
//!
//! Matches Component 5 of `PLAN_V2.md` with aligned `<pre>` monospace blocks, emoji status headers,
//! Order-to-Open capital reservation/commitment stats, sleeve attribution pills, and stop distances.

use aegis_core::{
    format_ms_human_utc, normalize_hyperliquid_symbol, now_ms, HeldAssetView, OrderSide,
    OrderedAssetView, PortfolioSnapshot, RiskTier,
};
use serde::{Deserialize, Serialize};

/// Escapes dynamic text for safe inclusion inside Telegram `parse_mode = "HTML"` tags.
pub fn escape_html(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 8);
    for ch in raw.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Formats a floating-point USDC value with thousands commas and 2 decimal places (e.g. `2,671.63`).
pub fn format_number_commas(val: f64, decimals: usize) -> String {
    let clean = if val.is_finite() { val } else { 0.0 };
    let sign = if clean < 0.0 { "-" } else { "" };
    let abs_val = clean.abs();
    let formatted = format!("{abs_val:.decimals$}");
    let mut parts = formatted.splitn(2, '.');
    let int_part = parts.next().unwrap_or("0");
    let dec_part = parts.next();

    let mut int_with_commas = String::with_capacity(int_part.len() + 4);
    let len = int_part.len();
    for (i, ch) in int_part.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            int_with_commas.push(',');
        }
        int_with_commas.push(ch);
    }

    match dec_part {
        Some(d) if decimals > 0 => format!("{sign}{int_with_commas}.{d}"),
        _ => format!("{sign}{int_with_commas}"),
    }
}

/// Formats a USDC amount as `$2,671.63`.
pub fn format_usdc(val: f64) -> String {
    let clean = if val.is_finite() { val } else { 0.0 };
    if clean < 0.0 {
        format!("-${}", format_number_commas(clean.abs(), 2))
    } else {
        format!("${}", format_number_commas(clean, 2))
    }
}

/// Formats a signed USDC PnL amount as `+$142.80` or `-$38.50`.
pub fn format_signed_usdc(val: f64) -> String {
    let clean = if val.is_finite() { val } else { 0.0 };
    if clean > 1e-9 {
        format!("+${}", format_number_commas(clean, 2))
    } else if clean < -1e-9 {
        format!("-${}", format_number_commas(clean.abs(), 2))
    } else {
        "$0.00".to_string()
    }
}

/// Formats a sleeve weight fraction (e.g. `0.112` -> `+11.2%`, `0.0` -> `0.0%`).
pub fn format_sleeve_pct(weight: f64) -> String {
    let pct = if weight.is_finite() {
        weight * 100.0
    } else {
        0.0
    };
    if pct.abs() < 0.05 {
        "0.0%".to_string()
    } else if pct > 0.0 {
        format!("+{pct:.1}%")
    } else {
        format!("{pct:.1}%")
    }
}

/// Formats the side badge with directional arrow (`LONG 📈` or `SHORT 📉`).
pub fn format_side_badge(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Long => "LONG 📈",
        OrderSide::Short => "SHORT 📉",
    }
}

/// Extracts canonical `(symbol, coin)` for display (e.g. `("SOL-USDC", "SOL")`).
fn canonical_symbol_and_coin(raw_symbol: &str) -> (String, String) {
    normalize_hyperliquid_symbol(raw_symbol)
        .unwrap_or_else(|_| (raw_symbol.trim().to_string(), raw_symbol.trim().to_string()))
}

// ============================================================================
// 1. ORDER FILLED (OPEN) CARD TEMPLATE
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFilledOpenAlert {
    pub symbol: String,
    pub side: OrderSide,
    pub risk_tier: RiskTier,
    pub leverage_multiplier: Option<f64>,
    pub trigger: String,
    pub fill_price: f64,
    pub filled_qty: f64,
    pub committed_notional_usdc: f64,
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub protective_stop_px: f64,
    #[serde(default)]
    pub take_profit_px: f64,
    pub s1_weight: f64,
    pub s2_weight: f64,
    pub s3_weight: f64,
    pub timestamp_ms: i64,
}

impl OrderFilledOpenAlert {
    /// Builds an `OrderFilledOpenAlert` directly from a `HeldAssetView` and `PortfolioSnapshot`.
    pub fn from_held_view(
        held: &HeldAssetView,
        snap: &PortfolioSnapshot,
        trigger: Option<&str>,
    ) -> Self {
        let tier = RiskTier::parse(&snap.risk_tier);
        Self {
            symbol: held.symbol.clone(),
            side: OrderSide::parse(&held.side),
            risk_tier: tier,
            leverage_multiplier: Some(snap.leverage_multiplier),
            trigger: trigger.unwrap_or("MULTI_SLEEVE_ALLOCATION").to_string(),
            fill_price: held.entry_price,
            filled_qty: held.quantity,
            committed_notional_usdc: held.committed_notional_usdc,
            total_equity_usdc: snap.total_equity_usdc,
            available_cash_usdc: snap.available_cash_usdc,
            protective_stop_px: held.protective_stop_px,
            take_profit_px: held.take_profit_px,
            s1_weight: held.s1_weight_contrib,
            s2_weight: held.s2_weight_contrib,
            s3_weight: held.s3_weight_contrib,
            timestamp_ms: if held.updated_at_ms > 0 {
                held.updated_at_ms
            } else {
                now_ms()
            },
        }
    }
}

/// Formats the Component 5 `ORDER FILLED (OPEN)` HTML Telegram card.
pub fn format_order_filled_open_html(alert: &OrderFilledOpenAlert) -> String {
    let (symbol, coin) = canonical_symbol_and_coin(&alert.symbol);
    let safe_symbol = escape_html(&symbol);
    let safe_coin = escape_html(&coin);
    let side_badge = format_side_badge(alert.side);
    let tier_badge = escape_html(&alert.risk_tier.display_badge(alert.leverage_multiplier));
    let safe_trigger = escape_html(if alert.trigger.trim().is_empty() {
        "MULTI_SLEEVE_ALLOCATION"
    } else {
        alert.trigger.trim()
    });

    let eq = if alert.total_equity_usdc > 0.0 {
        alert.total_equity_usdc
    } else {
        (alert.committed_notional_usdc + alert.available_cash_usdc).max(1.0)
    };
    let eq_pct = (alert.committed_notional_usdc / eq) * 100.0;

    let stop_dist_pct = if alert.fill_price > 0.0 && alert.protective_stop_px > 0.0 {
        match alert.side {
            OrderSide::Long => {
                ((alert.protective_stop_px - alert.fill_price) / alert.fill_price) * 100.0
            }
            OrderSide::Short => {
                ((alert.fill_price - alert.protective_stop_px) / alert.fill_price) * 100.0
            }
        }
    } else {
        -4.00
    };

    let tp_line = if alert.fill_price > 0.0 && alert.take_profit_px > 0.0 {
        let tp_dist_pct = match alert.side {
            OrderSide::Long => {
                ((alert.take_profit_px - alert.fill_price) / alert.fill_price) * 100.0
            }
            OrderSide::Short => {
                ((alert.fill_price - alert.take_profit_px) / alert.fill_price) * 100.0
            }
        };
        format!(
            "\nTake Profit  : ${:.4} ({:+.2}%)",
            alert.take_profit_px, tp_dist_pct
        )
    } else {
        String::new()
    };

    let ts_str = format_ms_human_utc(if alert.timestamp_ms > 0 {
        alert.timestamp_ms
    } else {
        now_ms()
    });

    format!(
        "🟢 <b>AEGIS v2.0 // ORDER FILLED (OPEN)</b>\n\
━━━━━━━━━━━━━━━━━━━━━━\n\
<b>Asset:</b> <code>{safe_symbol}</code>  |  <b>Side:</b> <code>{side_badge}</code>\n\
<b>Risk Tier:</b> <code>{tier_badge}</code>\n\
<b>Trigger:</b> <code>{safe_trigger}</code>\n\
\n\
<pre>\n\
Fill Price   : ${fill_px:.4}\n\
Filled Qty   : {qty:.2} {safe_coin}\n\
Committed    : {committed} USDC ({eq_pct:.1}% Eq)\n\
Avail Cash   : {avail} USDC\n\
Protect Stop : ${stop_px:.4} ({stop_dist:+.2}%){tp_line}\n\
</pre>\n\
<b>Sleeve Mix:</b> <code>S1 Carry {s1}</code> · <code>S2 Trend {s2}</code> · <code>S3 Event {s3}</code>\n\
⏱ <i>{ts_str} · 1s Priority Watch Active</i>",
        fill_px = alert.fill_price,
        qty = alert.filled_qty,
        committed = format_usdc(alert.committed_notional_usdc),
        avail = format_usdc(alert.available_cash_usdc),
        stop_px = alert.protective_stop_px,
        stop_dist = stop_dist_pct,
        tp_line = tp_line,
        s1 = format_sleeve_pct(alert.s1_weight),
        s2 = format_sleeve_pct(alert.s2_weight),
        s3 = format_sleeve_pct(alert.s3_weight),
    )
}

// ============================================================================
// 2. WORKING ORDER (RESERVED) CARD TEMPLATE
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingOrderPlacedAlert {
    pub symbol: String,
    pub side: OrderSide,
    pub risk_tier: RiskTier,
    pub leverage_multiplier: Option<f64>,
    pub fsm_state: String,
    pub limit_price: f64,
    pub requested_qty: f64,
    pub reserved_margin_usdc: f64,
    pub available_cash_usdc: f64,
    pub distance_from_mid_bps: f64,
    pub s1_weight: f64,
    pub s2_weight: f64,
    pub s3_weight: f64,
    pub timestamp_ms: i64,
}

impl WorkingOrderPlacedAlert {
    /// Builds a `WorkingOrderPlacedAlert` directly from an `OrderedAssetView` and `PortfolioSnapshot`.
    pub fn from_ordered_view(order: &OrderedAssetView, snap: &PortfolioSnapshot) -> Self {
        let tier = RiskTier::parse(&snap.risk_tier);
        Self {
            symbol: order.symbol.clone(),
            side: OrderSide::parse(&order.side),
            risk_tier: tier,
            leverage_multiplier: Some(snap.leverage_multiplier),
            fsm_state: order.fsm_state.clone(),
            limit_price: order.limit_price,
            requested_qty: order.requested_quantity,
            reserved_margin_usdc: order.reserved_margin_usdc,
            available_cash_usdc: snap.available_cash_usdc,
            distance_from_mid_bps: order.distance_from_mid_bps,
            s1_weight: order.s1_weight_contrib,
            s2_weight: order.s2_weight_contrib,
            s3_weight: order.s3_weight_contrib,
            timestamp_ms: if order.updated_at_ms > 0 {
                order.updated_at_ms
            } else {
                now_ms()
            },
        }
    }
}

/// Formats the `WORKING ORDER (RESERVED)` HTML Telegram card.
pub fn format_working_order_placed_html(alert: &WorkingOrderPlacedAlert) -> String {
    let (symbol, coin) = canonical_symbol_and_coin(&alert.symbol);
    let safe_symbol = escape_html(&symbol);
    let safe_coin = escape_html(&coin);
    let side_badge = format_side_badge(alert.side);
    let tier_badge = escape_html(&alert.risk_tier.display_badge(alert.leverage_multiplier));
    let safe_fsm = escape_html(if alert.fsm_state.trim().is_empty() {
        "WORKING_ALO"
    } else {
        alert.fsm_state.trim()
    });
    let ts_str = format_ms_human_utc(if alert.timestamp_ms > 0 {
        alert.timestamp_ms
    } else {
        now_ms()
    });

    format!(
        "🟡 <b>AEGIS v2.0 // WORKING ORDER (RESERVED)</b>\n\
━━━━━━━━━━━━━━━━━━━━━━\n\
<b>Asset:</b> <code>{safe_symbol}</code>  |  <b>Side:</b> <code>{side_badge}</code>\n\
<b>Risk Tier:</b> <code>{tier_badge}</code>\n\
<b>Order FSM:</b> <code>{safe_fsm}</code>\n\
\n\
<pre>\n\
Limit Price  : ${lim_px:.4}\n\
Requested Qty: {qty:.2} {safe_coin}\n\
Reserved     : {reserved} USDC (WORKING)\n\
Avail Cash   : {avail} USDC\n\
Mid Distance : {dist_bps:+.2} bps\n\
</pre>\n\
<b>Sleeve Mix:</b> <code>S1 Carry {s1}</code> · <code>S2 Trend {s2}</code> · <code>S3 Event {s3}</code>\n\
⏱ <i>{ts_str} · 1s Maker Limit Chaser Active</i>",
        lim_px = alert.limit_price,
        qty = alert.requested_qty,
        reserved = format_usdc(alert.reserved_margin_usdc),
        avail = format_usdc(alert.available_cash_usdc),
        dist_bps = alert.distance_from_mid_bps,
        s1 = format_sleeve_pct(alert.s1_weight),
        s2 = format_sleeve_pct(alert.s2_weight),
        s3 = format_sleeve_pct(alert.s3_weight),
    )
}

// ============================================================================
// 3. POSITION CLOSED & COOLDOWN CARD TEMPLATE
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionClosedAlert {
    pub symbol: String,
    pub side: OrderSide,
    pub exit_reason: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub filled_qty: f64,
    pub realized_pnl_usdc: f64,
    pub available_cash_usdc: f64,
    pub cooldown_type: Option<String>,
    pub cooldown_seconds: i64,
    pub timestamp_ms: i64,
}

/// Formats the `POSITION CLOSED & COOLDOWN` HTML Telegram card.
pub fn format_position_closed_html(alert: &PositionClosedAlert) -> String {
    let (symbol, coin) = canonical_symbol_and_coin(&alert.symbol);
    let safe_symbol = escape_html(&symbol);
    let safe_coin = escape_html(&coin);
    let side_badge = format_side_badge(alert.side);
    let safe_reason = escape_html(if alert.exit_reason.trim().is_empty() {
        "PROTECTIVE_STOP_EXIT"
    } else {
        alert.exit_reason.trim()
    });
    let cd_type = escape_html(alert.cooldown_type.as_deref().unwrap_or("STOP_OUT"));
    let cd_mins = (alert.cooldown_seconds.max(0) as f64) / 60.0;
    let cd_summary = if alert.cooldown_seconds > 0 {
        format!(
            "{cd_type} ({}s / {cd_mins:.0}m)",
            alert.cooldown_seconds.max(0)
        )
    } else {
        "NONE (0s)".to_string()
    };
    let ts_str = format_ms_human_utc(if alert.timestamp_ms > 0 {
        alert.timestamp_ms
    } else {
        now_ms()
    });

    format!(
        "🔴 <b>AEGIS v2.0 // POSITION CLOSED &amp; COOLDOWN</b>\n\
━━━━━━━━━━━━━━━━━━━━━━\n\
<b>Asset:</b> <code>{safe_symbol}</code>  |  <b>Side:</b> <code>{side_badge}</code>\n\
<b>Exit Reason:</b> <code>{safe_reason}</code>\n\
<b>Cooldown:</b> <code>{cd_summary}</code>\n\
\n\
<pre>\n\
Entry Price  : ${entry_px:.4}\n\
Exit Price   : ${exit_px:.4}\n\
Closed Qty   : {qty:.2} {safe_coin}\n\
Realized PnL : {rpnl} USDC\n\
Avail Cash   : {avail} USDC\n\
</pre>\n\
⏱ <i>{ts_str} · Cooldown Guard Active</i>",
        entry_px = alert.entry_price,
        exit_px = alert.exit_price,
        qty = alert.filled_qty,
        rpnl = format_signed_usdc(alert.realized_pnl_usdc),
        avail = format_usdc(alert.available_cash_usdc),
    )
}

// ============================================================================
// 4. LIVE TELEMETRY & CUTOVER VERIFIED / SYSTEM TEST ALERT TEMPLATES
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutoverReconcileAlert {
    pub execution_mode: String,
    pub mariadb_version: String,
    pub risk_tier: RiskTier,
    pub leverage_multiplier: Option<f64>,
    pub discovered_perps: usize,
    pub cancelled_legacy_orders: usize,
    pub closed_legacy_positions: usize,
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub timestamp_ms: i64,
}

/// Formats the `LIVE TELEMETRY & CUTOVER VERIFIED` HTML Telegram card for cutover reconciliation.
pub fn format_cutover_reconcile_html(alert: &CutoverReconcileAlert) -> String {
    let safe_mode = escape_html(alert.execution_mode.trim());
    let safe_db = escape_html(alert.mariadb_version.trim());
    let tier_badge = escape_html(&alert.risk_tier.display_badge(alert.leverage_multiplier));
    let ts_str = format_ms_human_utc(if alert.timestamp_ms > 0 {
        alert.timestamp_ms
    } else {
        now_ms()
    });

    format!(
        "⚡ <b>AEGIS v2.0 // LIVE TELEMETRY &amp; CUTOVER VERIFIED</b>\n\
━━━━━━━━━━━━━━━━━━━━━━\n\
<b>Mode:</b> <code>{safe_mode}</code>  |  <b>DB:</b> <code>MariaDB {safe_db}</code>\n\
<b>Risk Tier:</b> <code>{tier_badge}</code>\n\
<b>Surveillance:</b> <code>1s Priority Loop + 3s Exchange Scanner</code>\n\
\n\
<pre>\n\
Perps Seeded : {perps} Hyperliquid Markets\n\
Legacy Orders: {canc} Cancelled\n\
Legacy Pos   : {closed} Reconciled/Closed\n\
Total Equity : {eq} USDC\n\
Avail Cash   : {avail} USDC\n\
</pre>\n\
<b>Active Views:</b> <code>7/7 v_assets_* SQL Views Verified</code>\n\
⏱ <i>{ts_str} · Aegis Quant v2.0 Online</i>",
        perps = alert.discovered_perps,
        canc = alert.cancelled_legacy_orders,
        closed = alert.closed_legacy_positions,
        eq = format_usdc(alert.total_equity_usdc),
        avail = format_usdc(alert.available_cash_usdc),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemTestAlert {
    pub execution_mode: String,
    pub mariadb_version: String,
    pub risk_tier: RiskTier,
    pub leverage_multiplier: Option<f64>,
    pub total_equity_usdc: f64,
    pub available_cash_usdc: f64,
    pub reserved_order_cash_usdc: f64,
    pub committed_notional_usdc: f64,
    pub working_orders: usize,
    pub open_positions: usize,
    pub eligible_perps: usize,
    pub timestamp_ms: i64,
}

/// Formats the `LIVE TELEMETRY & CUTOVER VERIFIED` HTML Telegram card for `alert --test` / `POST /api/v1/alerts/test`.
pub fn format_system_test_alert_html(alert: &SystemTestAlert) -> String {
    let safe_mode = escape_html(alert.execution_mode.trim());
    let safe_db = escape_html(alert.mariadb_version.trim());
    let tier_badge = escape_html(&alert.risk_tier.display_badge(alert.leverage_multiplier));
    let ts_str = format_ms_human_utc(if alert.timestamp_ms > 0 {
        alert.timestamp_ms
    } else {
        now_ms()
    });

    format!(
        "⚡ <b>AEGIS v2.0 // LIVE TELEMETRY &amp; CUTOVER VERIFIED</b>\n\
━━━━━━━━━━━━━━━━━━━━━━\n\
<b>Mode:</b> <code>{safe_mode}</code>  |  <b>DB:</b> <code>MariaDB {safe_db}</code>\n\
<b>Risk Tier:</b> <code>{tier_badge}</code>\n\
<b>Surveillance:</b> <code>1s Priority Watchlist + 3s Scanner</code>\n\
\n\
<pre>\n\
Total Equity : {eq} USDC\n\
Avail Cash   : {avail} USDC\n\
Reserved     : {res} USDC ({working} WORKING)\n\
Committed    : {comm} USDC ({open} OPEN)\n\
Universe     : {perps} Hyperliquid Perps\n\
</pre>\n\
<b>Sleeves:</b> <code>S1 Carry-Mom</code> · <code>S2 Trend-Breakout</code> · <code>S3 Event-Rev</code>\n\
⏱ <i>{ts_str} · Rich HTML Telemetry Verified</i>",
        eq = format_usdc(alert.total_equity_usdc),
        avail = format_usdc(alert.available_cash_usdc),
        res = format_usdc(alert.reserved_order_cash_usdc),
        working = alert.working_orders,
        comm = format_usdc(alert.committed_notional_usdc),
        open = alert.open_positions,
        perps = alert.eligible_perps,
    )
}

/// Converts an HTML Telegram card into clean Discord markdown for `alert_provider == "discord"` or `"both"`.
pub fn html_to_discord_markdown(html: &str) -> String {
    html.replace("<b>", "**")
        .replace("</b>", "**")
        .replace("<code>", "`")
        .replace("</code>", "`")
        .replace("<i>", "*")
        .replace("</i>", "*")
        .replace("<pre>\n", "```text\n")
        .replace("<pre>", "```text\n")
        .replace("\n</pre>", "\n```")
        .replace("</pre>", "\n```")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_html_special_chars() {
        assert_eq!(
            escape_html("AEGIS <v2.0> & \"LIVE\""),
            "AEGIS &lt;v2.0&gt; &amp; &quot;LIVE&quot;"
        );
    }

    #[test]
    fn test_format_order_filled_open_html_matches_plan_v2() {
        let alert = OrderFilledOpenAlert {
            symbol: "SOL-USDC".to_string(),
            side: OrderSide::Long,
            risk_tier: RiskTier::Extreme,
            leverage_multiplier: Some(1.85),
            trigger: "LIVE_BREAKOUT_SURGE".to_string(),
            fill_price: 184.25,
            filled_qty: 14.50,
            committed_notional_usdc: 2671.63,
            total_equity_usdc: 10000.0,
            available_cash_usdc: 7328.37,
            protective_stop_px: 176.80,
            take_profit_px: 199.15,
            s1_weight: 0.112,
            s2_weight: 0.155,
            s3_weight: 0.0,
            timestamp_ms: 1795053912000,
        };
        let html = format_order_filled_open_html(&alert);
        assert!(html.contains("🟢 <b>AEGIS v2.0 // ORDER FILLED (OPEN)</b>"));
        assert!(html.contains("<b>Asset:</b> <code>SOL-USDC</code>"));
        assert!(html.contains("<b>Side:</b> <code>LONG 📈</code>"));
        assert!(html.contains("<b>Risk Tier:</b> <code>🔥 EXTREME (1.85x)</code>"));
        assert!(html.contains("<b>Trigger:</b> <code>LIVE_BREAKOUT_SURGE</code>"));
        assert!(html.contains("Fill Price   : $184.2500"));
        assert!(html.contains("Filled Qty   : 14.50 SOL"));
        assert!(html.contains("Committed    : $2,671.63 USDC (26.7% Eq)"));
        assert!(html.contains("Avail Cash   : $7,328.37 USDC"));
        assert!(html.contains("Protect Stop : $176.8000 (-4.04%)"));
        assert!(html.contains("Take Profit  : $199.1500 (+8.09%)"));
        assert!(html.contains(
            "<b>Sleeve Mix:</b> <code>S1 Carry +11.2%</code> · <code>S2 Trend +15.5%</code> · <code>S3 Event 0.0%</code>"
        ));
        assert!(html.contains("1s Priority Watch Active</i>"));
    }

    #[test]
    fn test_format_working_closed_and_test_cards() {
        let working = WorkingOrderPlacedAlert {
            symbol: "BTC-USDC".to_string(),
            side: OrderSide::Long,
            risk_tier: RiskTier::Extreme,
            leverage_multiplier: Some(1.85),
            fsm_state: "WORKING_ALO".to_string(),
            limit_price: 92450.0,
            requested_qty: 0.05,
            reserved_margin_usdc: 4622.50,
            available_cash_usdc: 5377.50,
            distance_from_mid_bps: 1.85,
            s1_weight: 0.25,
            s2_weight: 0.20,
            s3_weight: 0.0,
            timestamp_ms: 1795053912000,
        };
        let w_html = format_working_order_placed_html(&working);
        assert!(w_html.contains("🟡 <b>AEGIS v2.0 // WORKING ORDER (RESERVED)</b>"));
        assert!(w_html.contains("Reserved     : $4,622.50 USDC (WORKING)"));

        let closed = PositionClosedAlert {
            symbol: "ETH-USDC".to_string(),
            side: OrderSide::Short,
            exit_reason: "PROTECTIVE_STOP_BREACH".to_string(),
            entry_price: 3150.0,
            exit_price: 3080.0,
            filled_qty: 1.50,
            realized_pnl_usdc: 105.0,
            available_cash_usdc: 10105.0,
            cooldown_type: Some("STOP_OUT".to_string()),
            cooldown_seconds: 1800,
            timestamp_ms: 1795053912000,
        };
        let c_html = format_position_closed_html(&closed);
        assert!(c_html.contains("🔴 <b>AEGIS v2.0 // POSITION CLOSED &amp; COOLDOWN</b>"));
        assert!(c_html.contains("Realized PnL : +$105.00 USDC"));
        assert!(c_html.contains("STOP_OUT (1800s / 30m)"));

        let sys = SystemTestAlert {
            execution_mode: "TESTNET".to_string(),
            mariadb_version: "13.0.2-MariaDB".to_string(),
            risk_tier: RiskTier::Extreme,
            leverage_multiplier: Some(1.85),
            total_equity_usdc: 10000.0,
            available_cash_usdc: 7328.37,
            reserved_order_cash_usdc: 0.0,
            committed_notional_usdc: 2671.63,
            working_orders: 0,
            open_positions: 1,
            eligible_perps: 158,
            timestamp_ms: 1795053912000,
        };
        let s_html = format_system_test_alert_html(&sys);
        assert!(s_html.contains("⚡ <b>AEGIS v2.0 // LIVE TELEMETRY &amp; CUTOVER VERIFIED</b>"));
        assert!(s_html.contains("MariaDB 13.0.2-MariaDB"));
    }
}
