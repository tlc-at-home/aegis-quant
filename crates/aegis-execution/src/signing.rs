//! Hyperliquid L1 EIP-712 / MessagePack / secp256k1 action signing & wire formatting (`aegis-execution::signing`).

pub use aegis_core::{
    derive_evm_address_from_private_key, validate_evm_address, validate_private_key,
    verify_signer_matches_wallet, HL_ORACLE_BAND_PCT,
};
use aegis_core::{now_ms, round_dp, AegisError, OrderSide, Result};
use aegis_db::DatabaseManager;
use k256::ecdsa::{SigningKey, VerifyingKey};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;
use zeroize::Zeroizing;

/// Default Stop-Limit protective trigger limit slippage band (`1.0% = 100 bps`) (`P0-Exec-2`, `R14-M5`).
pub const HL_SL_TRIGGER_LIMIT_SLIPPAGE_PCT: f64 = 0.01;

/// Maximum actions allowed in a single Hyperliquid `/exchange` batch request (`Sec. 11`).
pub const HL_EXCHANGE_MAX_BATCH_ACTIONS: usize = 40;

/// Optimal `/exchange` batch chunk size (`39`, paying weight `1` since `weight = 1 + floor(len / 40)`).
pub const HL_EXCHANGE_OPTIMAL_BATCH_CHUNK: usize = 39;

/// Lease horizon (`10_000 ms`) synchronously persisted to `system_config.hyperliquid_last_nonce`
/// before returning any nonce that reaches or crosses the persisted ceiling (`R14-M6`).
pub const NONCE_PERSIST_LEASE_MS: i64 = 10_000;

/// Safety margin (`5_000 ms`) added to persisted DB nonce floor on daemon restart (`R14-M6`).
pub const NONCE_BOOT_SAFETY_MARGIN_MS: i64 = 5_000;

/// Encapsulated monotonic lease-backed Hyperliquid nonce allocator (`R14-M6`).
pub(crate) struct NonceLeaseState {
    high_water: AtomicI64,
    persisted_ceiling: AtomicI64,
    seeded: Mutex<bool>,
    persist_lock: Mutex<()>,
    mono_anchor: OnceLock<(i64, Instant)>,
}

impl NonceLeaseState {
    const fn new() -> Self {
        Self {
            high_water: AtomicI64::new(0),
            persisted_ceiling: AtomicI64::new(0),
            seeded: Mutex::new(false),
            persist_lock: Mutex::new(()),
            mono_anchor: OnceLock::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_isolated() -> Self {
        Self::new()
    }

    #[inline]
    fn monotonic_candidate(&self, wall_ms: i64, now_inst: Instant) -> i64 {
        let (boot_wall_ms, boot_inst) = *self.mono_anchor.get_or_init(|| (wall_ms, now_inst));
        let elapsed_ms = now_inst.saturating_duration_since(boot_inst).as_millis() as i64;
        let mono_ms = boot_wall_ms.saturating_add(elapsed_ms);
        if (wall_ms - mono_ms).abs() > 60_000 {
            tracing::error!(
                wall_ms,
                mono_ms,
                drift_ms = wall_ms - mono_ms,
                "Detected large wall-clock jump (>60s); guarding nonce monotonicity with Instant anchor"
            );
        }
        wall_ms.max(mono_ms)
    }

    fn ensure_seeded_from_db(&self, db: &DatabaseManager, allow_ephemeral_db: bool) {
        if !allow_ephemeral_db && db.is_ephemeral_test_db() {
            return;
        }
        let mut seeded_guard = self.seeded.lock();
        if *seeded_guard {
            return;
        }
        match db.get_config_i64_raw("hyperliquid_last_nonce") {
            Ok(opt) => {
                if let Some(db_floor) = opt.filter(|&v| v > 0) {
                    let boot_floor = db_floor.saturating_add(NONCE_BOOT_SAFETY_MARGIN_MS);
                    self.high_water.fetch_max(boot_floor, Ordering::SeqCst);
                    self.persisted_ceiling.fetch_max(db_floor, Ordering::SeqCst);
                }
                *seeded_guard = true;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "Failed to seed hyperliquid_last_nonce from DB; will retry on next nonce request"
                );
            }
        }
    }

    fn persist_lease_if_crossed(&self, next: i64, db: &DatabaseManager, allow_ephemeral_db: bool) {
        if !allow_ephemeral_db && db.is_ephemeral_test_db() {
            return;
        }
        if next < self.persisted_ceiling.load(Ordering::Acquire) {
            return;
        }
        let _persist_guard = self.persist_lock.lock();
        if next < self.persisted_ceiling.load(Ordering::Acquire) {
            return;
        }
        let new_ceiling = next.saturating_add(NONCE_PERSIST_LEASE_MS);
        match db.set_config_value(
            "hyperliquid_last_nonce",
            &new_ceiling.to_string(),
            Some("Monotonic Hyperliquid L1 EIP-712 action nonce lease ceiling"),
        ) {
            Ok(()) => {
                self.persisted_ceiling
                    .fetch_max(new_ceiling, Ordering::SeqCst);
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    next_nonce = next,
                    "Failed to synchronously persist hyperliquid_last_nonce lease ceiling; will retry"
                );
            }
        }
    }

    pub(crate) fn next_nonce_with_clock(
        &self,
        db: Option<&DatabaseManager>,
        wall_ms: i64,
        now_inst: Instant,
        allow_ephemeral_db: bool,
    ) -> u64 {
        if let Some(d) = db {
            self.ensure_seeded_from_db(d, allow_ephemeral_db);
        }

        let candidate = self.monotonic_candidate(wall_ms, now_inst);
        let mut prev = self.high_water.load(Ordering::SeqCst);
        let next = loop {
            let n = if candidate > prev {
                candidate
            } else {
                prev.saturating_add(1)
            };
            match self
                .high_water
                .compare_exchange(prev, n, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break n,
                Err(actual) => prev = actual,
            }
        };

        if let Some(d) = db {
            self.persist_lease_if_crossed(next, d, allow_ephemeral_db);
        }
        next as u64
    }

    pub(crate) fn bump_high_water_with_clock(
        &self,
        min_step_ms: i64,
        db: Option<&DatabaseManager>,
        wall_ms: i64,
        now_inst: Instant,
        allow_ephemeral_db: bool,
    ) -> u64 {
        if let Some(d) = db {
            self.ensure_seeded_from_db(d, allow_ephemeral_db);
        }

        let step = min_step_ms.max(1);
        let candidate = self.monotonic_candidate(wall_ms, now_inst);
        let mut prev = self.high_water.load(Ordering::SeqCst);
        let next = loop {
            let n = candidate.max(prev).saturating_add(step);
            match self
                .high_water
                .compare_exchange(prev, n, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break n,
                Err(actual) => prev = actual,
            }
        };

        if let Some(d) = db {
            self.persist_lease_if_crossed(next, d, allow_ephemeral_db);
        }
        next as u64
    }
}

static GLOBAL_NONCE_STATE: NonceLeaseState = NonceLeaseState::new();

/// Generates a strictly monotonic Hyperliquid L1 nonce backed by an in-process `AtomicI64`
/// high-water floor, monotonic `Instant` NTP guard, and synchronous `+10_000 ms` lease persistence (`R14-M6`).
pub fn next_monotonic_nonce(db: Option<&DatabaseManager>) -> u64 {
    GLOBAL_NONCE_STATE.next_nonce_with_clock(db, now_ms(), Instant::now(), false)
}

/// Advances the global nonce high-water mark by at least `min_step_ms` and synchronously persists
/// the extended lease ceiling if crossed (`R14-M6` nonce-collision recovery).
pub fn bump_nonce_high_water(min_step_ms: i64, db: Option<&DatabaseManager>) -> u64 {
    GLOBAL_NONCE_STATE.bump_high_water_with_clock(min_step_ms, db, now_ms(), Instant::now(), false)
}

/// Formats a float into a wire string matching Hyperliquid's `float_to_wire` specification.
pub fn float_to_wire(x: f64) -> String {
    let s = format!("{:.8}", x);
    let mut trimmed = s.trim_end_matches('0');
    if trimmed.ends_with('.') {
        trimmed = &trimmed[..trimmed.len() - 1];
    }
    if trimmed == "-0" || trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Infers Hyperliquid `szDecimals` (`0..=6`) from `lot_step_size` (`10^-szDecimals`).
pub fn infer_sz_decimals_from_lot_step(lot_step_size: f64) -> u32 {
    if !lot_step_size.is_finite() || lot_step_size <= 0.0 {
        return 3;
    }
    (-lot_step_size.log10()).round().clamp(0.0, 6.0) as u32
}

/// Infers the Hyperliquid L1 perpetual tick size from `mark_price` and `sz_decimals`
/// (5 significant figures and at most `6 - sz_decimals` decimal places).
pub fn infer_tick_size(mark_price: f64, sz_decimals: u32) -> f64 {
    if !mark_price.is_finite() || mark_price <= 0.0 {
        return 0.01;
    }
    let max_perp_decimals = 6usize.saturating_sub(sz_decimals as usize);
    let mag = mark_price.log10().floor() as i32;
    let sig_fig_decimals = (4 - mag).max(0) as usize;
    let allowed_decimals = sig_fig_decimals.min(max_perp_decimals);
    10_f64.powi(-(allowed_decimals as i32)).max(1e-8)
}

/// Computes the effective Hyperliquid wire tick size at `price` aligned with `format_hl_wire_price`
/// (5 significant figures and at most `6 - sz_decimals` decimal places).
/// Returns `None` when `price` is non-finite or non-positive (`Sec. 11`).
pub fn effective_hl_tick_size(price: f64, asset_tick: f64, sz_decimals: u32) -> Option<f64> {
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    let wire_tick = infer_tick_size(price, sz_decimals);
    if asset_tick.is_finite() && asset_tick > 0.0 {
        Some(asset_tick.max(wire_tick))
    } else {
        Some(wire_tick)
    }
}

/// Snaps a price to the nearest valid tick increment.
pub fn snap_to_tick(price: f64, tick_size: f64) -> f64 {
    if !price.is_finite() || price <= 0.0 {
        return 0.0;
    }
    let tick = if tick_size.is_finite() && tick_size > 0.0 {
        tick_size
    } else {
        0.01
    };
    let decimals = (-tick.log10()).round().clamp(0.0, 8.0) as i32;
    round_dp((price / tick).round() * tick, decimals)
}

/// Snaps an order size down (`floor == true`) or nearest to `lot_step_size` and `sz_decimals`.
pub fn snap_to_lot(size: f64, lot_step_size: f64, sz_decimals: u32, floor: bool) -> f64 {
    if !size.is_finite() || size <= 0.0 {
        return 0.0;
    }
    let lot = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        lot_step_size
    } else {
        10_f64.powi(-(sz_decimals.min(6) as i32))
    };
    let snapped = if floor {
        ((size / lot) + 1e-9).floor() * lot
    } else {
        (size / lot).round() * lot
    };
    round_dp(snapped.max(0.0), sz_decimals.min(8) as i32)
}

/// Formats a perpetual limit price for Hyperliquid L1 wire transmission:
/// - Snaps to `tick_size` if `tick_size > 0`
/// - Enforces at most 5 significant figures (with integer prices allowed for `>= 100_000`)
/// - Enforces at most `6 - sz_decimals` decimal places for perpetuals
pub fn format_hl_wire_price(price: f64, tick_size: f64, sz_decimals: u32) -> String {
    if !price.is_finite() || price <= 0.0 {
        return "0".to_string();
    }
    let max_perp_decimals = 6usize.saturating_sub(sz_decimals as usize);
    let mag = price.log10().floor() as i32;
    let sig_fig_decimals = (4 - mag).max(0) as usize;
    let allowed_decimals = sig_fig_decimals.min(max_perp_decimals);
    let min_wire_step = 10_f64.powi(-(allowed_decimals as i32));
    let effective_tick = if tick_size.is_finite() && tick_size > 0.0 {
        tick_size.max(min_wire_step)
    } else {
        min_wire_step
    };
    let mut p = (price / effective_tick).round() * effective_tick;
    if p <= 0.0 {
        p = price;
    }
    float_to_wire(round_dp(p, allowed_decimals as i32))
}

/// Formats a perpetual order size (`s`) for Hyperliquid L1 wire transmission:
/// - Snaps to `lot_step_size` if `lot_step_size > 0`
/// - Rounds to `sz_decimals` decimal places
pub fn format_hl_wire_size(size: f64, lot_step_size: f64, sz_decimals: u32) -> String {
    if !size.is_finite() || size <= 0.0 {
        return "0".to_string();
    }
    let s = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        (size / lot_step_size).round() * lot_step_size
    } else {
        size
    };
    let dec = (sz_decimals as usize).min(8);
    float_to_wire(round_dp(s, dec as i32))
}

/// Formats a perpetual order size (`s`) floored to `lot_step_size` so `reduce_only`
/// orders never round above the remaining position quantity on the exchange.
pub fn format_hl_wire_size_floor(size: f64, lot_step_size: f64, sz_decimals: u32) -> String {
    if !size.is_finite() || size <= 0.0 {
        return "0".to_string();
    }
    let s = if lot_step_size.is_finite() && lot_step_size > 0.0 {
        ((size / lot_step_size) + 1e-9).floor() * lot_step_size
    } else {
        size
    };
    let dec = (sz_decimals as usize).min(8);
    float_to_wire(round_dp(s.max(0.0), dec as i32))
}

/// Derives a deterministic 128-bit `0x`-prefixed 32-hex-character Hyperliquid client order ID (`cloid`)
/// from an internal `coid` string.
pub fn derive_hl_cloid_hex(coid: &str) -> String {
    let hash = Keccak256::digest(coid.as_bytes());
    format!("0x{}", hex::encode(&hash[..16]))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LimitOrderType {
    pub tif: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TriggerOrderTypeWire {
    #[serde(rename = "isMarket")]
    pub is_market: bool,
    #[serde(rename = "triggerPx")]
    pub trigger_px: String,
    pub tpsl: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderTypeWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<LimitOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TriggerOrderTypeWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderWire {
    pub a: u32,
    pub b: bool,
    pub p: String,
    pub s: String,
    pub r: bool,
    pub t: OrderTypeWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub orders: Vec<OrderWire>,
    pub grouping: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelWire {
    pub a: u32,
    pub o: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CancelAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub cancels: Vec<CancelWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModifyWire {
    pub oid: u64,
    pub order: OrderWire,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BatchModifyAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub modifies: Vec<ModifyWire>,
}

/// Builds a Hyperliquid L1 limit `OrderWire` (`"Alo"`, `"Ioc"`, or `"Gtc"`).
#[allow(clippy::too_many_arguments)]
pub fn build_limit_order_wire(
    asset_index: u32,
    is_buy: bool,
    limit_price: f64,
    quantity: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    reduce_only: bool,
    tif: &str,
    client_order_id: Option<&str>,
) -> OrderWire {
    let p = format_hl_wire_price(limit_price, tick_size, sz_decimals);
    let s = if reduce_only {
        format_hl_wire_size_floor(quantity, lot_step_size, sz_decimals)
    } else {
        format_hl_wire_size(quantity, lot_step_size, sz_decimals)
    };
    let c = client_order_id
        .filter(|id| !id.trim().is_empty())
        .map(derive_hl_cloid_hex);
    OrderWire {
        a: asset_index,
        b: is_buy,
        p,
        s,
        r: reduce_only,
        t: OrderTypeWire {
            limit: Some(LimitOrderType {
                tif: tif.to_string(),
            }),
            trigger: None,
        },
        c,
    }
}

/// Builds a Hyperliquid L1 reduce-only trigger `OrderWire` (`tpsl = "sl"` or `"tp"`) with explicit `is_market` and `limit_price`.
#[allow(clippy::too_many_arguments)]
fn build_trigger_tpsl_order_wire(
    asset_index: u32,
    position_side: OrderSide,
    quantity: f64,
    trigger_price: f64,
    limit_price: f64,
    is_market: bool,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    tpsl: &str,
    client_order_id: Option<&str>,
) -> OrderWire {
    // Trigger order closes the open position: SELL (b=false) for LONG, BUY (b=true) for SHORT
    let close_is_buy = !position_side.is_buy();
    let trigger_px_str = format_hl_wire_price(trigger_price, tick_size, sz_decimals);
    let limit_px_str = format_hl_wire_price(limit_price, tick_size, sz_decimals);
    let mut sz_str = format_hl_wire_size_floor(quantity, lot_step_size, sz_decimals);
    if sz_str == "0" {
        sz_str = format_hl_wire_size(quantity, lot_step_size, sz_decimals);
    }
    let c = client_order_id
        .filter(|id| !id.trim().is_empty())
        .map(derive_hl_cloid_hex);
    OrderWire {
        a: asset_index,
        b: close_is_buy,
        p: limit_px_str,
        s: sz_str,
        r: true,
        t: OrderTypeWire {
            limit: None,
            trigger: Some(TriggerOrderTypeWire {
                is_market,
                trigger_px: trigger_px_str,
                tpsl: tpsl.to_string(),
            }),
        },
        c,
    }
}

/// Builds a Hyperliquid L1 protective stop-loss trigger `OrderWire` (`reduce_only = true`, `tpsl = "sl"`,
/// `is_market = false` `"Stop Limit"` bounded within `±band_bps` of `trigger_price`) (`P0-Exec-2`).
#[allow(clippy::too_many_arguments)]
pub fn build_protective_stop_order_wire_with_band_bps(
    asset_index: u32,
    position_side: OrderSide,
    quantity: f64,
    trigger_price: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    band_bps: f64,
    client_order_id: Option<&str>,
) -> OrderWire {
    let eff_band_bps = if band_bps.is_finite() {
        band_bps.clamp(20.0, HL_ORACLE_BAND_PCT * 10_000.0)
    } else {
        HL_SL_TRIGGER_LIMIT_SLIPPAGE_PCT * 10_000.0
    };
    let band_pct = eff_band_bps / 10_000.0;
    let eff_tick = effective_hl_tick_size(trigger_price, tick_size, sz_decimals)
        .unwrap_or_else(|| tick_size.max(1e-8));
    let close_is_buy = !position_side.is_buy();
    let bounded_limit_px = if close_is_buy {
        trigger_price * (1.0 + band_pct)
    } else {
        (trigger_price * (1.0 - band_pct)).max(eff_tick)
    };
    build_trigger_tpsl_order_wire(
        asset_index,
        position_side,
        quantity,
        trigger_price,
        bounded_limit_px,
        false,
        tick_size,
        lot_step_size,
        sz_decimals,
        "sl",
        client_order_id,
    )
}

/// Builds a Hyperliquid L1 protective stop-loss trigger `OrderWire` (`reduce_only = true`, `tpsl = "sl"`,
/// `is_market = false` `"Stop Limit"` bounded at `±1.0%` = `100 bps`).
#[allow(clippy::too_many_arguments)]
pub fn build_protective_stop_order_wire(
    asset_index: u32,
    position_side: OrderSide,
    quantity: f64,
    trigger_price: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    client_order_id: Option<&str>,
) -> OrderWire {
    build_protective_stop_order_wire_with_band_bps(
        asset_index,
        position_side,
        quantity,
        trigger_price,
        tick_size,
        lot_step_size,
        sz_decimals,
        HL_SL_TRIGGER_LIMIT_SLIPPAGE_PCT * 10_000.0,
        client_order_id,
    )
}

/// Builds a Hyperliquid L1 take-profit trigger `OrderWire` (`reduce_only = true`, `tpsl = "tp"`,
/// `is_market = false` `"Take Profit Limit"` anchored at exact `trigger_price`) (`P0-Exec-2`).
#[allow(clippy::too_many_arguments)]
pub fn build_take_profit_order_wire(
    asset_index: u32,
    position_side: OrderSide,
    quantity: f64,
    trigger_price: f64,
    tick_size: f64,
    lot_step_size: f64,
    sz_decimals: u32,
    client_order_id: Option<&str>,
) -> OrderWire {
    build_trigger_tpsl_order_wire(
        asset_index,
        position_side,
        quantity,
        trigger_price,
        trigger_price,
        false,
        tick_size,
        lot_step_size,
        sz_decimals,
        "tp",
        client_order_id,
    )
}

/// Computes the exact Hyperliquid MessagePack + Keccak256 `action_hash` / phantom agent connection ID.
pub fn compute_l1_action_hash<T: Serialize>(
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
) -> Result<[u8; 32]> {
    let mut data = rmp_serde::encode::to_vec_named(action)
        .map_err(|e| AegisError::Execution(format!("Msgpack serialization error: {e}")))?;
    data.extend_from_slice(&nonce.to_be_bytes());
    match vault_address {
        None => data.push(0x00),
        Some(v) if v.trim().is_empty() => data.push(0x00),
        Some(v) => {
            data.push(0x01);
            let clean = v.strip_prefix("0x").unwrap_or(v);
            let addr_bytes = hex::decode(clean)
                .map_err(|e| AegisError::Validation(format!("Invalid vault address hex: {e}")))?;
            if addr_bytes.len() != 20 {
                return Err(AegisError::Validation(format!(
                    "Vault address must be 20 bytes, got {}",
                    addr_bytes.len()
                )));
            }
            data.extend_from_slice(&addr_bytes);
        }
    }
    if let Some(exp) = expires_after {
        data.push(0x00);
        data.extend_from_slice(&exp.to_be_bytes());
    }
    let hash: [u8; 32] = Keccak256::digest(&data).into();
    Ok(hash)
}

/// Signs an L1 action payload using MessagePack binary serialization, action_hash, and EIP-712 structured data signing.
/// Zeroizes intermediate normalized key strings and raw secret key bytes on drop.
pub fn sign_l1_action<T: Serialize>(
    private_key_hex: &str,
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after: Option<u64>,
    is_mainnet: bool,
) -> Result<Value> {
    if private_key_hex.trim().is_empty() {
        return Err(AegisError::Crypto(
            "Private key cannot be empty for L1 action signing".to_string(),
        ));
    }
    let normalized_pk = Zeroizing::new(validate_private_key(private_key_hex)?);
    let hex_body = normalized_pk
        .strip_prefix("0x")
        .or_else(|| normalized_pk.strip_prefix("0X"))
        .unwrap_or(normalized_pk.as_str());
    if hex_body.len() != 64 {
        return Err(AegisError::Crypto(format!(
            "Normalized private key must contain 64 hex chars, got {}",
            hex_body.len()
        )));
    }
    let raw_bytes = Zeroizing::new(
        hex::decode(hex_body)
            .map_err(|e| AegisError::Crypto(format!("Invalid private_key hex: {e}")))?,
    );
    let signing_key = SigningKey::from_slice(&raw_bytes)
        .map_err(|e| AegisError::Crypto(format!("Invalid secp256k1 signing key: {e}")))?;

    let action_h = compute_l1_action_hash(action, vault_address, nonce, expires_after)?;

    // 1. EIP-712 Domain Separator for "Exchange"
    let mut domain_buf = Vec::with_capacity(32 * 5);
    domain_buf.extend_from_slice(&Keccak256::digest(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    domain_buf.extend_from_slice(&Keccak256::digest(b"Exchange"));
    domain_buf.extend_from_slice(&Keccak256::digest(b"1"));
    let mut chain_id_bytes = [0u8; 32];
    chain_id_bytes[30..32].copy_from_slice(&1337u16.to_be_bytes());
    domain_buf.extend_from_slice(&chain_id_bytes);
    domain_buf.extend_from_slice(&[0u8; 32]);
    let domain_separator: [u8; 32] = Keccak256::digest(&domain_buf).into();

    // 2. Struct Hash for Agent(string source,bytes32 connectionId)
    let mut agent_buf = Vec::with_capacity(32 * 3);
    agent_buf.extend_from_slice(&Keccak256::digest(
        b"Agent(string source,bytes32 connectionId)",
    ));
    let source_str = if is_mainnet { "a" } else { "b" };
    agent_buf.extend_from_slice(&Keccak256::digest(source_str.as_bytes()));
    agent_buf.extend_from_slice(&action_h);
    let struct_hash: [u8; 32] = Keccak256::digest(&agent_buf).into();

    // 3. EIP-712 Signable Digest
    let mut digest_buf = Vec::with_capacity(2 + 32 + 32);
    digest_buf.extend_from_slice(b"\x19\x01");
    digest_buf.extend_from_slice(&domain_separator);
    digest_buf.extend_from_slice(&struct_hash);
    let digest: [u8; 32] = Keccak256::digest(&digest_buf).into();

    // 4. ECDSA Secp256k1 Recoverable Signature
    let (sig, rec_id) = signing_key
        .sign_prehash_recoverable(&digest)
        .map_err(|e| AegisError::Crypto(format!("ECDSA signing failed: {e}")))?;
    let sig_bytes = sig.to_bytes();
    let r_hex = format!("0x{}", hex::encode(&sig_bytes[..32]));
    let s_hex = format!("0x{}", hex::encode(&sig_bytes[32..64]));
    let v = 27u8 + rec_id.to_byte();

    let verify_key = VerifyingKey::from(&signing_key);
    let uncompressed = verify_key.to_encoded_point(false);
    let pub_bytes = &uncompressed.as_bytes()[1..];
    let pub_hash = Keccak256::digest(pub_bytes);
    let signer_address = format!("0x{}", hex::encode(&pub_hash[12..]));

    Ok(json!({
        "r": r_hex,
        "s": s_hex,
        "v": v,
        "signer": signer_address,
        "connection_id": format!("0x{}", hex::encode(action_h)),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_eip712_sign_l1_action_and_protective_stop_wire() {
        let pk = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let order = build_limit_order_wire(
            0,
            true,
            92450.5,
            0.05,
            0.5,
            0.0001,
            4,
            false,
            "Alo",
            Some("v2-btc-long-1"),
        );
        assert_eq!(order.p, "92451");
        assert_eq!(order.s, "0.05");
        assert!(order.c.as_ref().unwrap().starts_with("0x"));
        assert_eq!(order.c.as_ref().unwrap().len(), 34);

        let stop = build_protective_stop_order_wire(
            3,
            OrderSide::Long,
            14.528,
            176.82,
            0.01,
            0.01,
            2,
            Some("sl-sol-1"),
        );
        assert!(!stop.b);
        assert!(stop.r);
        assert_eq!(stop.s, "14.52");
        assert_eq!(stop.p, "175.05");
        let stop_trig = stop.t.trigger.as_ref().unwrap();
        assert!(!stop_trig.is_market);
        assert_eq!(stop_trig.trigger_px, "176.82");
        assert_eq!(stop_trig.tpsl, "sl");

        let short_stop = build_protective_stop_order_wire(
            3,
            OrderSide::Short,
            14.528,
            176.82,
            0.01,
            0.01,
            2,
            Some("sl-sol-short-1"),
        );
        assert!(short_stop.b);
        assert!(!short_stop.t.trigger.as_ref().unwrap().is_market);
        assert_eq!(short_stop.p, "178.59");

        let tp = build_take_profit_order_wire(
            3,
            OrderSide::Long,
            14.528,
            199.15,
            0.01,
            0.01,
            2,
            Some("tp-sol-1"),
        );
        assert!(!tp.b);
        assert!(tp.r);
        assert_eq!(tp.s, "14.52");
        assert_eq!(tp.p, "199.15");
        let tp_trig = tp.t.trigger.as_ref().unwrap();
        assert!(!tp_trig.is_market);
        assert_eq!(tp_trig.trigger_px, "199.15");
        assert_eq!(tp_trig.tpsl, "tp");

        let action = OrderAction {
            action_type: "order".to_string(),
            orders: vec![order.clone(), stop, tp],
            grouping: "na".to_string(),
        };
        let n1 = next_monotonic_nonce(None);
        let n2 = next_monotonic_nonce(None);
        assert!(n2 > n1);

        let sig = sign_l1_action(pk, &action, None, n1, None, false).unwrap();
        assert_eq!(
            sig["signer"].as_str().unwrap(),
            "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23"
        );
        assert!(sig["r"].as_str().unwrap().starts_with("0x"));
        assert!(sig["s"].as_str().unwrap().starts_with("0x"));
        let v = sig["v"].as_u64().unwrap();
        assert!(v == 27 || v == 28);

        // Verify BatchModifyAction MessagePack hash computes cleanly
        let batch_modify = BatchModifyAction {
            action_type: "batchModify".to_string(),
            modifies: vec![ModifyWire { oid: 42, order }],
        };
        assert!(compute_l1_action_hash(&batch_modify, None, n2, None).is_ok());
    }

    #[test]
    fn test_r14_m6_nonce_lease_survives_restart_backward_ntp_and_db_seed_retry() {
        let db = DatabaseManager::new_isolated_test(None).unwrap();
        let base_wall = 2_000_000_000_000i64;
        let t0 = Instant::now();

        // 1. Seed initial persisted ceiling and simulate daemon restart:
        //    boot high_water jumps by +NONCE_BOOT_SAFETY_MARGIN_MS (+5_000 ms), so the first strictly-greater
        //    boot nonce is base_wall + NONCE_BOOT_SAFETY_MARGIN_MS + 1 and persists +NONCE_PERSIST_LEASE_MS (+10_000 ms).
        db.set_config_value("hyperliquid_last_nonce", &base_wall.to_string(), None)
            .unwrap();
        let state1 = NonceLeaseState::new_isolated();
        let n1 = state1.next_nonce_with_clock(Some(&db), base_wall - 10_000, t0, true);
        assert_eq!(n1 as i64, base_wall + NONCE_BOOT_SAFETY_MARGIN_MS + 1);

        let persisted_after_n1 = db
            .get_config_i64_raw("hyperliquid_last_nonce")
            .unwrap()
            .unwrap();
        assert_eq!(persisted_after_n1, (n1 as i64) + NONCE_PERSIST_LEASE_MS);

        // 2. Subsequent nonce within the 10s lease stays strictly > n1 without overwriting ceiling yet
        let n2 = state1.next_nonce_with_clock(
            Some(&db),
            base_wall - 120_000, // Simulated 2m backward NTP step!
            t0 + Duration::from_millis(500),
            true,
        );
        assert!(n2 > n1);
        let persisted_after_n2 = db
            .get_config_i64_raw("hyperliquid_last_nonce")
            .unwrap()
            .unwrap();
        assert_eq!(persisted_after_n2, persisted_after_n1);

        // 3. Crossing the lease advances persisted ceiling synchronously BEFORE returning nonce
        let n3 = state1.next_nonce_with_clock(
            Some(&db),
            persisted_after_n1 + 250,
            t0 + Duration::from_secs(12),
            true,
        );
        assert_eq!(n3 as i64, persisted_after_n1 + 250);
        let persisted_after_n3 = db
            .get_config_i64_raw("hyperliquid_last_nonce")
            .unwrap()
            .unwrap();
        assert_eq!(persisted_after_n3, (n3 as i64) + NONCE_PERSIST_LEASE_MS);

        // 4. Simulated crash-restart right after n3: new NonceLeaseState starts strictly > n3
        let state2 = NonceLeaseState::new_isolated();
        let n4 = state2.next_nonce_with_clock(Some(&db), base_wall, t0, true);
        assert!(n4 > n3);
        assert!(n4 as i64 >= persisted_after_n3 + NONCE_BOOT_SAFETY_MARGIN_MS);

        // 5. DB seed failure retries instead of latching `seeded = true`
        let bad_db = DatabaseManager::connect(Some(std::path::Path::new(":memory:")), None);
        if let Ok(uninit_db) = bad_db {
            // Even if connected without schema, seed fails cleanly and remains unseeded until success
            let state_retry = NonceLeaseState::new_isolated();
            let _ = state_retry.next_nonce_with_clock(Some(&uninit_db), 1_000, t0, true);
            // When valid DB is passed afterwards, it still reads the persisted floor
            let n_ok = state_retry.next_nonce_with_clock(Some(&db), 1_000, t0, true);
            assert!(n_ok > n4);
        }
    }
}
