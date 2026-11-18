//! Canonical millisecond epoch & ISO-8601 UTC time utilities (`aegis-core::time`).

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};

/// Returns the current UTC timestamp in milliseconds since Unix epoch.
#[inline]
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Returns the millisecond epoch timestamp for Jan 1 00:00:00 UTC of the current year.
pub fn ytd_start_ms() -> i64 {
    let year = Utc::now().year();
    NaiveDate::from_ymd_opt(year, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp_millis())
        .unwrap_or(0)
}

/// Canonical UTC timestamp formatted as ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn now_iso8601_utc() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Canonical UTC timestamp formatted for human-readable alerts (`YYYY-MM-DD HH:MM:SS UTC`).
pub fn format_ms_human_utc(ts_ms: i64) -> String {
    if ts_ms <= 0 {
        return "1970-01-01 00:00:00 UTC".to_string();
    }
    DateTime::from_timestamp_millis(ts_ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string()
}

/// Format a millisecond epoch timestamp as canonical ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn format_ms_iso8601_utc(ts_ms: i64) -> String {
    if ts_ms < 0 {
        return "1970-01-01T00:00:00Z".to_string();
    }
    DateTime::from_timestamp_millis(ts_ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

/// Normalize a legacy timestamp string into canonical ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn normalize_timestamp_iso8601_utc(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return dt
            .with_timezone(&Utc)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        return ndt.and_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    }
    if let Ok(ndt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        return ndt.and_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    }
    trimmed.to_string()
}

/// Round floating-point number to `decimals` decimal places safely.
#[inline]
pub fn round_dp(val: f64, decimals: i32) -> f64 {
    if !val.is_finite() {
        return 0.0;
    }
    if decimals <= 0 {
        return val.round();
    }
    let factor = 10_f64.powi(decimals);
    (val * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_and_rounding_helpers() {
        assert!(now_ms() > 1_700_000_000_000);
        assert_eq!(format_ms_iso8601_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(round_dp(184.256789, 4), 184.2568);
        assert_eq!(round_dp(f64::NAN, 2), 0.0);
    }
}
