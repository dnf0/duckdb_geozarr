# STAC Temporal Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement temporal pushdown for STAC Search APIs by translating DuckDB SQL time bounds into the STAC `datetime` query parameter.

**Architecture:** We add a helper to `geozarr_core/src/datetime.rs` to format epoch seconds into RFC3339 strings, and extend `build_stac_url` in `geozarr_core/src/feature_collection.rs` to extract `"time"` bounds and append the `&datetime=` parameter, properly handling open and closed intervals.

**Tech Stack:** Rust, chrono

---

### Task 1: Epoch to RFC3339 Helper

**Files:**
- Modify: `geozarr_core/src/datetime.rs`

- [ ] **Step 1: Write the failing test**

Append to `mod tests` inside `geozarr_core/src/datetime.rs`:
```rust
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p geozarr_core -- datetime::tests::formats_epoch_to_rfc3339`
Expected: FAIL (cannot find function `epoch_seconds_to_rfc3339`)

- [ ] **Step 3: Write minimal implementation**

Add the `epoch_seconds_to_rfc3339` function above `mod tests` in `geozarr_core/src/datetime.rs`:
```rust
/// Format seconds since the Unix epoch into an RFC3339 timestamp.
pub fn epoch_seconds_to_rfc3339(seconds: f64) -> Result<String, String> {
    chrono::DateTime::from_timestamp(seconds as i64, 0)
        .map(|dt| dt.to_rfc3339())
        .ok_or_else(|| format!("invalid epoch seconds: {seconds}"))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p geozarr_core -- datetime::tests::formats_epoch_to_rfc3339`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geozarr_core/src/datetime.rs
git commit -m "feat: add epoch_seconds_to_rfc3339 helper for stac datetime formatting"
```

### Task 2: STAC URL Datetime Pushdown

**Files:**
- Modify: `geozarr_core/src/feature_collection.rs`

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` inside `geozarr_core/src/feature_collection.rs`:
```rust
    #[test]
    fn test_stac_time_pushdown_closed() {
        let mut bounds = std::collections::HashMap::new();
        bounds.insert("time".to_string(), (Some(1767225600.0), Some(1798761600.0))); // Jan 1 2026 to Dec 31 2026
        let constraints = crate::query_planner::QueryConstraints {
            bounds,
            pins: std::collections::HashMap::new(),
        };

        let url =
            crate::feature_collection::build_stac_url("https://example.com/search", &constraints);
        assert!(url.contains("datetime=2026-01-01T00:00:00+00:00/2026-12-31T00:00:00+00:00"));
    }

    #[test]
    fn test_stac_time_pushdown_open_end() {
        let mut bounds = std::collections::HashMap::new();
        bounds.insert("time".to_string(), (Some(1767225600.0), None));
        let constraints = crate::query_planner::QueryConstraints {
            bounds,
            pins: std::collections::HashMap::new(),
        };

        let url =
            crate::feature_collection::build_stac_url("https://example.com/search", &constraints);
        assert!(url.contains("datetime=2026-01-01T00:00:00+00:00/.."));
    }

    #[test]
    fn test_stac_time_pushdown_open_start() {
        let mut bounds = std::collections::HashMap::new();
        bounds.insert("time".to_string(), (None, Some(1767225600.0)));
        let constraints = crate::query_planner::QueryConstraints {
            bounds,
            pins: std::collections::HashMap::new(),
        };

        let url =
            crate::feature_collection::build_stac_url("https://example.com/search", &constraints);
        assert!(url.contains("datetime=../2026-01-01T00:00:00+00:00"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p geozarr_core -- feature_collection::tests::test_stac_time_pushdown`
Expected: FAIL (assertions fail because datetime isn't appended)

- [ ] **Step 3: Write minimal implementation**

Update `build_stac_url` in `geozarr_core/src/feature_collection.rs` to extract `"time"` and append the `datetime` parameter. Replace the `build_stac_url` function with:
```rust
pub fn build_stac_url(
    base_url: &str,
    constraints: &crate::query_planner::QueryConstraints,
) -> String {
    let mut url = base_url.to_string();

    let lat_bounds = constraints
        .bounds
        .get("lat")
        .copied()
        .unwrap_or((None, None));
    let lon_bounds = constraints
        .bounds
        .get("lon")
        .copied()
        .unwrap_or((None, None));

    if let (Some(lon_min), Some(lat_min), Some(lon_max), Some(lat_max)) =
        (lon_bounds.0, lat_bounds.0, lon_bounds.1, lat_bounds.1)
    {
        let separator = if url.contains('?') { "&" } else { "?" };
        url = format!(
            "{}{separator}bbox={},{},{},{}",
            url, lon_min, lat_min, lon_max, lat_max
        );
    }

    let time_bounds = constraints
        .bounds
        .get("time")
        .copied()
        .unwrap_or((None, None));

    match (time_bounds.0, time_bounds.1) {
        (None, None) => {}
        (Some(start), None) => {
            if let Ok(start_str) = crate::datetime::epoch_seconds_to_rfc3339(start) {
                let separator = if url.contains('?') { "&" } else { "?" };
                url = format!("{}{separator}datetime={start_str}/..", url);
            }
        }
        (None, Some(end)) => {
            if let Ok(end_str) = crate::datetime::epoch_seconds_to_rfc3339(end) {
                let separator = if url.contains('?') { "&" } else { "?" };
                url = format!("{}{separator}datetime=../{end_str}", url);
            }
        }
        (Some(start), Some(end)) => {
            if let (Ok(start_str), Ok(end_str)) = (
                crate::datetime::epoch_seconds_to_rfc3339(start),
                crate::datetime::epoch_seconds_to_rfc3339(end),
            ) {
                let separator = if url.contains('?') { "&" } else { "?" };
                url = format!("{}{separator}datetime={start_str}/{end_str}", url);
            }
        }
    }

    url
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p geozarr_core -- feature_collection::tests`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geozarr_core/src/feature_collection.rs
git commit -m "feat: implement STAC temporal pushdown via datetime query parameter"
```
