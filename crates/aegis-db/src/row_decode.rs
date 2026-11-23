//! Typed MariaDB row decoding and strict `DECIMAL` parameter binding helpers (`aegis-db::row_decode`).
//!
//! Ensures MariaDB `DECIMAL` (`Value::Bytes`), `DOUBLE` (`Value::Double`), `FLOAT` (`Value::Float`),
//! and integer literals (`Value::Int` / `Value::UInt`) decode cleanly to Rust primitives without silent
//! zeroing on unexpected type mismatches, and rejects `NaN`/`±Inf`/overflow prior to `STRICT_TRANS_TABLES`.

use aegis_core::{AegisError, Result};
use mysql::{Row, Value};

/// Safe absolute bound for `DECIMAL(20,8)` (12 integer digits + 8 fractional digits).
pub const DEC_20_8_MAX_ABS: f64 = 999_999_999_999.0;

/// Safe absolute bound for `DECIMAL(24,8)` (16 integer digits + 8 fractional digits, safely within f64 exact range).
pub const DEC_24_8_MAX_ABS: f64 = 9_000_000_000_000_000.0;

/// Safe absolute bound for `DECIMAL(20,10)` (10 integer digits + 10 fractional digits).
pub const DEC_20_10_MAX_ABS: f64 = 9_999_999_999.0;

/// Decodes a nullable numeric column (`Double | Float | Bytes | Int | UInt`) into `Option<f64>`.
/// Emits `tracing::warn!` on unexpected value types, invalid UTF-8/decimal syntax, or non-finite values.
pub fn row_get_opt_f64(row: &Row, idx: usize) -> Option<f64> {
    let Some(val) = row.as_ref(idx) else {
        tracing::warn!(
            col_idx = idx,
            row_len = row.len(),
            "Row index out of bounds decoding f64"
        );
        return None;
    };
    decode_value_opt_f64(val, idx)
}

/// Decodes a numeric column into `f64`, returning `default` when `NULL` or unparseable.
pub fn row_get_f64(row: &Row, idx: usize, default: f64) -> f64 {
    row_get_opt_f64(row, idx).unwrap_or(default)
}

fn decode_value_opt_f64(val: &Value, idx: usize) -> Option<f64> {
    match val {
        Value::NULL => None,
        Value::Double(v) => {
            if v.is_finite() {
                Some(*v)
            } else {
                tracing::warn!(col_idx = idx, value = %v, "Non-finite Double decoded from MariaDB");
                None
            }
        }
        Value::Float(v) => {
            let d = f64::from(*v);
            if d.is_finite() {
                Some(d)
            } else {
                tracing::warn!(col_idx = idx, value = %v, "Non-finite Float decoded from MariaDB");
                None
            }
        }
        Value::Int(v) => Some(*v as f64),
        Value::UInt(v) => Some(*v as f64),
        Value::Bytes(bytes) => {
            let Ok(s) = std::str::from_utf8(bytes) else {
                tracing::warn!(
                    col_idx = idx,
                    "Non-UTF8 Bytes encountered while decoding DECIMAL/f64"
                );
                return None;
            };
            let trimmed = s.trim();
            if trimmed.is_empty() {
                tracing::warn!(col_idx = idx, "Empty Bytes encountered while decoding f64");
                return None;
            }
            match trimmed.parse::<f64>() {
                Ok(v) if v.is_finite() => Some(v),
                Ok(v) => {
                    tracing::warn!(col_idx = idx, raw = %trimmed, parsed = %v, "Non-finite parsed Bytes f64");
                    None
                }
                Err(e) => {
                    tracing::warn!(col_idx = idx, raw = %trimmed, error = %e, "Failed parsing Bytes as f64");
                    None
                }
            }
        }
        other => {
            tracing::warn!(col_idx = idx, value = ?other, "Unexpected MySQL Value type decoding f64");
            None
        }
    }
}

/// Decodes a nullable integer column into `Option<i64>`.
pub fn row_get_opt_i64(row: &Row, idx: usize) -> Option<i64> {
    let Some(val) = row.as_ref(idx) else {
        tracing::warn!(
            col_idx = idx,
            row_len = row.len(),
            "Row index out of bounds decoding i64"
        );
        return None;
    };
    match val {
        Value::NULL => None,
        Value::Int(v) => Some(*v),
        Value::UInt(v) => match i64::try_from(*v) {
            Ok(i) => Some(i),
            Err(_) => {
                tracing::warn!(col_idx = idx, value = *v, "UInt overflows i64");
                None
            }
        },
        Value::Bytes(bytes) => match std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
        {
            Some(v) => Some(v),
            None => {
                tracing::warn!(col_idx = idx, "Failed parsing Bytes as i64");
                None
            }
        },
        Value::Double(v) if v.is_finite() => Some(*v as i64),
        Value::Float(v) if v.is_finite() => Some(*v as i64),
        other => {
            tracing::warn!(col_idx = idx, value = ?other, "Unexpected MySQL Value type decoding i64");
            None
        }
    }
}

/// Decodes an integer column into `i64`, returning `default` when `NULL` or unparseable.
pub fn row_get_i64(row: &Row, idx: usize, default: i64) -> i64 {
    row_get_opt_i64(row, idx).unwrap_or(default)
}

/// Decodes an integer column into `i32`, returning `default` when `NULL` or out of range.
pub fn row_get_i32(row: &Row, idx: usize, default: i32) -> i32 {
    match row_get_opt_i64(row, idx) {
        Some(v) => match i32::try_from(v) {
            Ok(i) => i,
            Err(_) => {
                tracing::warn!(col_idx = idx, value = v, "Integer overflows i32");
                default
            }
        },
        None => default,
    }
}

/// Decodes an unsigned integer column into `u32`, returning `default` when `NULL` or out of range.
pub fn row_get_u32(row: &Row, idx: usize, default: u32) -> u32 {
    match row_get_opt_i64(row, idx) {
        Some(v) => match u32::try_from(v) {
            Ok(u) => u,
            Err(_) => {
                tracing::warn!(col_idx = idx, value = v, "Integer out of range for u32");
                default
            }
        },
        None => default,
    }
}

/// Decodes a boolean `TINYINT(1)` column into `bool`.
pub fn row_get_bool(row: &Row, idx: usize, default: bool) -> bool {
    row_get_opt_i64(row, idx).map_or(default, |v| v != 0)
}

/// Decodes a nullable string/ENUM column into `Option<String>`.
pub fn row_get_opt_string(row: &Row, idx: usize) -> Option<String> {
    let Some(val) = row.as_ref(idx) else {
        tracing::warn!(
            col_idx = idx,
            row_len = row.len(),
            "Row index out of bounds decoding String"
        );
        return None;
    };
    match val {
        Value::NULL => None,
        Value::Bytes(bytes) => match std::str::from_utf8(bytes) {
            Ok(s) => Some(s.to_string()),
            Err(e) => {
                tracing::warn!(col_idx = idx, error = %e, "Non-UTF8 Bytes encountered while decoding String");
                None
            }
        },
        Value::Int(v) => Some(v.to_string()),
        Value::UInt(v) => Some(v.to_string()),
        other => {
            tracing::warn!(col_idx = idx, value = ?other, "Unexpected MySQL Value type decoding String");
            None
        }
    }
}

/// Decodes a string/ENUM column into `String`, returning `default` when `NULL` or unparseable.
pub fn row_get_string(row: &Row, idx: usize, default: &str) -> String {
    row_get_opt_string(row, idx).unwrap_or_else(|| default.to_string())
}

/// Formats a finite `f64` as a fixed-scale ASCII decimal string for binding into MariaDB `DECIMAL`
/// columns under `STRICT_TRANS_TABLES`. Rejects `NaN`, `±Inf`, and values exceeding `max_abs`.
pub fn decimal_param(val: f64, scale: u8, max_abs: f64) -> Result<String> {
    if !val.is_finite() {
        return Err(AegisError::Validation(format!(
            "Decimal parameter must be finite, got {val}"
        )));
    }
    if !max_abs.is_finite() || max_abs <= 0.0 {
        return Err(AegisError::Validation(format!(
            "Invalid max_abs bound {max_abs} for decimal_param"
        )));
    }
    if val.abs() > max_abs {
        return Err(AegisError::Validation(format!(
            "Decimal parameter {val} exceeds allowed bound ±{max_abs}"
        )));
    }
    let prec = usize::from(scale.min(18));
    let mut formatted = format!("{val:.prec$}");
    if formatted.starts_with('-') && formatted[1..].bytes().all(|b| b == b'0' || b == b'.') {
        formatted.remove(0);
    }
    Ok(formatted)
}

/// Nullable variant of [`decimal_param`].
pub fn decimal_opt_param(val: Option<f64>, scale: u8, max_abs: f64) -> Result<Option<String>> {
    val.map(|v| decimal_param(v, scale, max_abs)).transpose()
}

/// Validates a finite `f64` for columns stored as `DOUBLE` (dimensionless z-scores/vol/beta).
pub fn finite_double_param(val: f64, field_name: &str) -> Result<f64> {
    if !val.is_finite() {
        return Err(AegisError::Validation(format!(
            "Numeric field '{field_name}' must be finite, got {val}"
        )));
    }
    Ok(if val == 0.0 { 0.0 } else { val })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_value_opt_f64_handles_all_mysql_numeric_wire_types() {
        assert!((decode_value_opt_f64(&Value::Double(123.45), 0).unwrap() - 123.45).abs() < 1e-12);
        assert!((decode_value_opt_f64(&Value::Float(6.5_f32), 1).unwrap() - 6.5).abs() < 1e-6);
        assert!(
            (decode_value_opt_f64(&Value::Bytes(b"98765432101.12345678".to_vec()), 2).unwrap()
                - 98_765_432_101.123_46)
                .abs()
                < 1e-3
        );
        assert!((decode_value_opt_f64(&Value::Int(-42), 3).unwrap() - (-42.0)).abs() < 1e-12);
        assert!((decode_value_opt_f64(&Value::UInt(99), 4).unwrap() - 99.0).abs() < 1e-12);
        assert_eq!(decode_value_opt_f64(&Value::NULL, 5), None);
        assert_eq!(
            decode_value_opt_f64(&Value::Bytes(b"not-a-number".to_vec()), 6),
            None
        );
        assert_eq!(decode_value_opt_f64(&Value::Double(f64::NAN), 7), None);
        assert_eq!(decode_value_opt_f64(&Value::Double(f64::INFINITY), 8), None);
    }

    #[test]
    fn test_decimal_param_validates_finite_bounds_and_normalizes_negative_zero() {
        assert_eq!(
            decimal_param(184.123456789, 8, DEC_20_8_MAX_ABS).unwrap(),
            "184.12345679"
        );
        assert_eq!(
            decimal_param(0.000000125, 10, DEC_20_10_MAX_ABS).unwrap(),
            "0.0000001250"
        );
        assert_eq!(
            decimal_param(-0.0, 8, DEC_20_8_MAX_ABS).unwrap(),
            "0.00000000"
        );
        assert_eq!(
            decimal_param(-1e-12, 8, DEC_20_8_MAX_ABS).unwrap(),
            "0.00000000"
        );
        assert_eq!(
            decimal_param(1_500_000_000_000.5, 8, DEC_24_8_MAX_ABS).unwrap(),
            "1500000000000.50000000"
        );

        assert!(decimal_param(f64::NAN, 8, DEC_20_8_MAX_ABS).is_err());
        assert!(decimal_param(f64::INFINITY, 8, DEC_20_8_MAX_ABS).is_err());
        assert!(decimal_param(f64::NEG_INFINITY, 8, DEC_20_8_MAX_ABS).is_err());
        assert!(decimal_param(1e13, 8, DEC_20_8_MAX_ABS).is_err());
        assert!(decimal_param(-1e13, 8, DEC_20_8_MAX_ABS).is_err());
    }
}
