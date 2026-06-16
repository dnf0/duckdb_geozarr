//! RFC3339 datetime → epoch-seconds for STAC `properties.datetime`.

/// Parse an RFC3339 timestamp into seconds since the Unix epoch.
pub fn rfc3339_to_epoch_seconds(s: &str) -> Result<f64, String> {
    chrono::DateTime::parse_from_rfc3339(s.trim())
        .map(|dt| dt.timestamp() as f64)
        .map_err(|e| format!("invalid RFC3339 datetime {s:?}: {e}"))
}

/// Format seconds since the Unix epoch into an RFC3339 timestamp.
pub fn epoch_seconds_to_rfc3339(seconds: f64) -> Result<String, String> {
    if !seconds.is_finite() {
        return Err("epoch seconds must be finite".to_string());
    }
    
    let mut secs = seconds.floor() as i64;
    let mut nanos = ((seconds - seconds.floor()) * 1_000_000_000.0).round() as u32;
    
    if nanos >= 1_000_000_000 {
        secs += 1;
        nanos -= 1_000_000_000;
    }
    
    chrono::DateTime::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339())
        .ok_or_else(|| format!("invalid epoch seconds: {seconds}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_utc_z() {
        assert_eq!(
            rfc3339_to_epoch_seconds("2026-01-01T00:00:00Z").unwrap(),
            1767225600.0
        );
    }
    #[test]
    fn parses_offset() {
        // 2026-01-01T01:00:00+01:00 == 2026-01-01T00:00:00Z
        assert_eq!(
            rfc3339_to_epoch_seconds("2026-01-01T01:00:00+01:00").unwrap(),
            1767225600.0
        );
    }
    #[test]
    fn rejects_garbage() {
        assert!(rfc3339_to_epoch_seconds("not-a-date").is_err());
        assert!(rfc3339_to_epoch_seconds("").is_err());
    }

    #[test]
    fn formats_epoch_to_rfc3339() {
        assert_eq!(
            epoch_seconds_to_rfc3339(1767225600.0).unwrap(),
            "2026-01-01T00:00:00+00:00"
        );
        assert_eq!(
            epoch_seconds_to_rfc3339(0.0).unwrap(),
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn formats_epoch_with_subsecond_precision() {
        assert_eq!(
            epoch_seconds_to_rfc3339(1767225600.5).unwrap(),
            "2026-01-01T00:00:00.500+00:00" // Note: chrono format behavior for .5 is .500
        );
    }

    #[test]
    fn formats_epoch_negative_subsecond() {
        assert_eq!(
            epoch_seconds_to_rfc3339(-1.5).unwrap(),
            "1969-12-31T23:59:58.500+00:00"
        );
    }

    #[test]
    fn rejects_non_finite_epoch_seconds() {
        assert!(epoch_seconds_to_rfc3339(f64::NAN).is_err());
        assert!(epoch_seconds_to_rfc3339(f64::INFINITY).is_err());
        assert!(epoch_seconds_to_rfc3339(f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn formats_epoch_fraction_rollover() {
        assert_eq!(
            epoch_seconds_to_rfc3339(0.9999999996).unwrap(),
            "1970-01-01T00:00:01+00:00"
        );
    }
}
