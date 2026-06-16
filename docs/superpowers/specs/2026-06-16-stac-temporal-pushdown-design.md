# STAC Temporal Pushdown Design

## Goal
Implement a temporal pushdown optimization for STAC Search APIs, allowing DuckDB `WHERE time >= ...` clauses to be translated into the STAC `datetime` query parameter. This ensures the remote catalog filters items temporally before returning them, preventing Eider from downloading COG headers/metadata for time slices that the user has excluded.

## Architecture & Data Flow

1. **Extraction (Existing):** `extension/src/table_function.rs` already extracts the `time_min` and `time_max` parameters passed from the user's SQL query (as Unix epoch seconds `f64`) and inserts them into the `QueryConstraints.bounds` map under the `"time"` key.
2. **Formatting Helper:** We will add `epoch_seconds_to_rfc3339(seconds: f64) -> Result<String, String>` to `geozarr_core/src/datetime.rs`. This will leverage the `chrono` crate (`DateTime::from_timestamp`) to correctly format the bounds to an RFC3339 string (e.g. `2026-01-01T00:00:00Z`).
3. **URL Construction:** In `geozarr_core/src/feature_collection.rs`, `build_stac_url` will be extended to look for the `"time"` tuple inside the `bounds` map.
4. **Interval Handling:** Following the STAC API specification, the `datetime` query parameter supports both closed and open intervals:
   - Both bounds provided: append `&datetime={min_rfc3339}/{max_rfc3339}`
   - Only `time_min` provided: append `&datetime={min_rfc3339}/..`
   - Only `time_max` provided: append `&datetime=../{max_rfc3339}`
   - (The `&` or `?` separator will be handled identically to the existing `bbox` logic).

## Component Isolation
This design isolates time formatting strictly within `geozarr_core/src/datetime.rs`, the module already responsible for RFC3339-to-epoch parsing. The extension logic is untouched (it remains agnostic of STAC protocols), and `build_stac_url` simply orchestrates string assembly.

## Testing Strategy
1. **`datetime.rs`:** Unit tests to verify that known epoch values cleanly format into valid RFC3339 strings in UTC.
2. **`feature_collection.rs`:** Unit tests testing `build_stac_url` by injecting mock `QueryConstraints` with:
   - A complete closed time interval.
   - An open-ended start (`../end`).
   - An open-ended end (`start/..`).
   Asserting the resulting URL contains the expected `datetime=` parameter alongside `bbox=`.
