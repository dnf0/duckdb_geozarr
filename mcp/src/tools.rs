//! Pure, rmcp-independent tool logic. Each tool is `&Connection (+args) ->
//! Result<serde_json::Value>` so it can be unit-tested directly and wrapped by
//! the rmcp adapter (added in a later task) without duplicating logic.

use crate::result::{materialize, rows_as_json};
use color_eyre::eyre::{eyre, Result as EyreResult, WrapErr};
use duckdb::Connection;
use serde_json::{json, Value};

/// Default cap on rows inlined in a tool result; full data stays in the temp
/// table referenced by the result handle.
const DEFAULT_LIMIT: usize = 1000;

/// Escape single quotes for embedding a value inside a single-quoted SQL
/// string literal.
fn esc(s: &str) -> String {
    s.replace('\'', "''")
}

/// Build the optional bbox/time/asset named args for `read_geo`/`plan_read_geo`
/// from JSON, e.g. `, lon_min := 1, lat_min := 2, time_min := 0, asset := 'a'`.
/// Returns an empty string when nothing is pushed down.
fn pushdown_args(bbox: Option<&Value>, time: Option<&Value>, asset: Option<&str>) -> String {
    let mut parts = Vec::new();
    if let Some(b) = bbox {
        for (k, p) in [
            ("lon_min", "lon_min"),
            ("lat_min", "lat_min"),
            ("lon_max", "lon_max"),
            ("lat_max", "lat_max"),
        ] {
            if let Some(v) = b.get(k).and_then(|x| x.as_f64()) {
                parts.push(format!("{p} := {v}"));
            }
        }
    }
    if let Some(t) = time {
        if let Some(v) = t.get("min").and_then(|x| x.as_f64()) {
            parts.push(format!("time_min := {v}"));
        }
        if let Some(v) = t.get("max").and_then(|x| x.as_f64()) {
            parts.push(format!("time_max := {v}"));
        }
    }
    if let Some(a) = asset {
        parts.push(format!("asset := '{}'", esc(a)));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(", {}", parts.join(", "))
    }
}

/// Return the Zarr dataset metadata (array/chunk shape, dtype, CRS).
pub fn describe_dataset(conn: &Connection, uri: &str) -> EyreResult<Value> {
    let sql = format!("SELECT * FROM read_zarr_metadata('{}')", esc(uri));
    Ok(json!({ "metadata": rows_as_json(conn, &sql)? }))
}

/// Estimate the read cost (`total_chunks`, `total_bytes`) for a region, with
/// optional bbox/time pushdown.
pub fn estimate_cost(
    conn: &Connection,
    uri: &str,
    bbox: Option<&Value>,
    time: Option<&Value>,
) -> EyreResult<Value> {
    let sql = format!(
        "SELECT * FROM plan_read_geo('{}'{})",
        esc(uri),
        pushdown_args(bbox, time, None)
    );
    Ok(json!({ "estimate": rows_as_json(conn, &sql)? }))
}

/// Read a region with bbox/time/asset pushdown and materialize it into a temp
/// table, returning a capped head + handle.
pub fn read_region(
    conn: &Connection,
    uri: &str,
    bbox: Option<&Value>,
    time: Option<&Value>,
    asset: Option<&str>,
    limit: Option<usize>,
) -> EyreResult<Value> {
    let sql = format!(
        "SELECT * FROM read_geo('{}'{})",
        esc(uri),
        pushdown_args(bbox, time, asset)
    );
    materialize(conn, &sql, limit.unwrap_or(DEFAULT_LIMIT))
}

/// List the tables visible in the current session (including temp result
/// handles).
pub fn list_tables(conn: &Connection) -> EyreResult<Value> {
    Ok(json!({
        "tables": rows_as_json(
            conn,
            "SELECT table_name FROM information_schema.tables ORDER BY table_name",
        )?
    }))
}

/// Describe a table's schema and a small sample. The table name is validated
/// (alphanumeric/underscore only) since it is interpolated into SQL.
pub fn describe_table(conn: &Connection, name: &str) -> EyreResult<Value> {
    if name.is_empty() || !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(eyre!("invalid table name"));
    }
    Ok(json!({
        "schema": rows_as_json(conn, &format!("DESCRIBE {name}"))?,
        "sample": rows_as_json(conn, &format!("SELECT * FROM {name} LIMIT 5"))?,
    }))
}

/// Guarded read-only SQL escape hatch over the shared session.
///
/// The statement is first validated by [`crate::guard::ensure_read_only`]
/// (single statement; read-only/temp-only). `SET VARIABLE` and `CREATE TEMP`
/// statements don't return a result set, so they are executed directly and
/// report `{"status":"ok"}`. Everything else is materialized into a temp table
/// (capped head + handle), like the other read tools.
///
/// Wall-clock timeout: DuckDB `=1.10502.0` exposes no `statement_timeout`
/// setting (verified against `duckdb_settings()` — no timeout knob exists, and
/// `SET/PRAGMA statement_timeout` are rejected as unrecognized parameters), so
/// there is no native mechanism to apply here. v1 ships the row cap + guard;
/// wall-clock cancellation belongs in the rmcp adapter layer, which can run
/// each call on a worker paired with a `Connection::interrupt()` handle.
pub fn run_sql(conn: &Connection, sql: &str, limit: Option<usize>) -> EyreResult<Value> {
    crate::guard::ensure_read_only(sql)?;
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();
    // SET VARIABLE / CREATE TEMP don't produce a result set; just execute them.
    if upper.starts_with("SET ") || upper.starts_with("CREATE ") {
        conn.execute_batch(trimmed).wrap_err("run_sql (stmt)")?;
        return Ok(json!({ "status": "ok" }));
    }
    materialize(conn, trimmed, limit.unwrap_or(DEFAULT_LIMIT))
}

/// Build the per-row aggregate expression for a plain (unweighted) convention.
/// `count` ignores the value column; every other metric aggregates `z.<col>`.
fn agg_expr(metric: &str, alias: &str, col: &str) -> String {
    match metric {
        "count" => "count(*)".to_string(),
        _ => format!("{metric}({alias}.{col})"),
    }
}

/// Per-polygon zonal metric over a grid asset, reading only the chunks that
/// intersect the polygons' combined bounding box.
///
/// `metric` is one of `max`, `min`, `mean`, `sum`, `count`. `convention`:
///  - `centroid`      — a cell counts if its center lies in the polygon
///    (`ST_Contains(poly, ST_Point(lon, lat))`).
///  - `all_touched`   — a cell counts if its box intersects the polygon
///    (`ST_Intersects(poly, cell_box)`); includes boundary cells the centroid
///    rule drops, so for `max`/`min` it is the conservative choice.
///  - `area_weighted` — cells are weighted by their fractional overlap with the
///    polygon (`ST_Intersection` area weights). Only meaningful for `mean`/`sum`,
///    so `max`/`min` are rejected.
///
/// The grid cell is centered on `(lon, lat)` and spans `±step/2`; the half-step
/// `dx`/`dy` is derived from the pruned read itself (`(max-min)/(distinct-1)`),
/// so the cell box is the true cell extent rather than a degenerate point.
///
/// `value_col` defaults to `value` (validated, since it is interpolated into
/// SQL). The result is materialized into a temp table with `convention` and
/// `metric` echoed back.
pub fn zonal_stats(
    conn: &Connection,
    grid_uri: &str,
    polygons: &str,
    metric: &str,
    convention: &str,
    value_col: Option<&str>,
    limit: Option<usize>,
) -> EyreResult<Value> {
    let m = match metric {
        "max" | "min" | "mean" | "sum" | "count" => metric,
        _ => return Err(eyre!("metric must be one of max,min,mean,sum,count")),
    };
    match convention {
        "centroid" | "all_touched" | "area_weighted" => {}
        _ => {
            return Err(eyre!(
                "convention must be centroid|all_touched|area_weighted"
            ))
        }
    }
    if convention == "area_weighted" && (m == "max" || m == "min") {
        return Err(eyre!("area_weighted applies to mean/sum, not max/min"));
    }
    let vc = value_col.unwrap_or("value");
    if vc.is_empty() || !vc.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(eyre!("invalid value column"));
    }

    let poly = esc(polygons);
    let grid = esc(grid_uri);

    // Push the polygons' combined bbox into read_geo so only intersecting
    // chunks are fetched.
    conn.execute_batch(&format!(
        "SET VARIABLE _z_bbox = (SELECT ST_Extent_Agg(geom) FROM ST_Read('{poly}'));"
    ))
    .wrap_err("compute polygons bbox")?;

    // `field` is the pruned read; `step` derives the grid half-cell size from
    // the data so the cell box is correct.
    let ctes = format!(
        "WITH field AS (\
            SELECT lon, lat, {vc} FROM read_geo('{grid}', \
              lon_min := ST_XMin(getvariable('_z_bbox')), lat_min := ST_YMin(getvariable('_z_bbox')), \
              lon_max := ST_XMax(getvariable('_z_bbox')), lat_max := ST_YMax(getvariable('_z_bbox')))), \
         step AS (\
            SELECT (max(lon)-min(lon))/nullif(count(distinct lon)-1,0) AS dx, \
                   (max(lat)-min(lat))/nullif(count(distinct lat)-1,0) AS dy FROM field)"
    );
    // The true cell box: centered on (lon,lat), spanning ±step/2.
    let cell_box = "ST_MakeEnvelope(z.lon-s.dx/2, z.lat-s.dy/2, z.lon+s.dx/2, z.lat+s.dy/2)";

    let select = match convention {
        "centroid" => format!(
            "{ctes} \
             SELECT v.* EXCLUDE (geom), {agg} AS metric \
             FROM ST_Read('{poly}') v, field z, step s \
             WHERE ST_Contains(v.geom, ST_Point(z.lon, z.lat)) \
             GROUP BY ALL",
            agg = agg_expr(m, "z", vc),
        ),
        "all_touched" => format!(
            "{ctes} \
             SELECT v.* EXCLUDE (geom), {agg} AS metric \
             FROM ST_Read('{poly}') v, field z, step s \
             WHERE ST_Intersects(v.geom, {cell_box}) \
             GROUP BY ALL",
            agg = agg_expr(m, "z", vc),
        ),
        // area_weighted: weight each cell by the area of its overlap with the
        // polygon. `sum` totals value*overlap; `mean` divides by total overlap.
        _ => {
            let weighted = format!("sum(z.{vc} * ST_Area(ST_Intersection(v.geom, {cell_box})))");
            let metric_expr = if m == "mean" {
                format!("{weighted} / nullif(sum(ST_Area(ST_Intersection(v.geom, {cell_box}))), 0)")
            } else {
                weighted
            };
            format!(
                "{ctes} \
                 SELECT v.* EXCLUDE (geom), {metric_expr} AS metric \
                 FROM ST_Read('{poly}') v, field z, step s \
                 WHERE ST_Intersects(v.geom, {cell_box}) \
                 GROUP BY ALL"
            )
        }
    };

    let mut out = materialize(conn, &select, limit.unwrap_or(DEFAULT_LIMIT))?;
    out["convention"] = json!(convention);
    out["metric"] = json!(metric);
    Ok(out)
}

/// Extract the timeseries for a specific point.
///
/// Executes a two-phase query:
/// 1. Discovers the grid cell coordinates nearest to the point.
/// 2. Reads the full timeseries for exactly those cells, aggregating if idw.
pub fn extract_point_timeseries(
    conn: &Connection,
    uri: &str,
    lat: f64,
    lon: f64,
    method: &str,
    value_col: Option<&str>,
) -> EyreResult<Value> {
    if method != "nearest" && method != "idw" {
        return Err(eyre!("method must be 'nearest' or 'idw'"));
    }
    let vc = value_col.unwrap_or("value");
    if vc.is_empty() || !vc.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(eyre!("invalid value column"));
    }
    let uri = esc(uri);

    // Phase 1: Coordinate Discovery
    let discovery_sql = format!(
        "WITH grid AS (
             SELECT lat, lon
             FROM read_geo('{uri}', time_min := 0, time_max := 0)
         ),
         distances AS (
             SELECT lat, lon, ST_Distance(ST_Point(lon, lat), ST_Point({lon}, {lat})) AS dist
             FROM grid
         )
         SELECT lat, lon, dist
         FROM distances
         ORDER BY dist ASC
         LIMIT {limit}",
        limit = if method == "nearest" { 1 } else { 4 }
    );
    let closest_rows_value = rows_as_json(conn, &discovery_sql)?;
    let closest_rows = closest_rows_value.as_array().unwrap();
    if closest_rows.is_empty() {
        return Err(eyre!("No grid cells found"));
    }

    let mut lat_min = f64::MAX;
    let mut lat_max = f64::MIN;
    let mut lon_min = f64::MAX;
    let mut lon_max = f64::MIN;
    let mut closest_vals = Vec::new();
    for row in closest_rows {
        let r_lat = row["lat"]
            .as_f64()
            .ok_or_else(|| eyre!("invalid coordinate type"))?;
        let r_lon = row["lon"]
            .as_f64()
            .ok_or_else(|| eyre!("invalid coordinate type"))?;
        let r_dist = row["dist"]
            .as_f64()
            .ok_or_else(|| eyre!("invalid distance type"))?;
        if r_lat < lat_min {
            lat_min = r_lat;
        }
        if r_lat > lat_max {
            lat_max = r_lat;
        }
        if r_lon < lon_min {
            lon_min = r_lon;
        }
        if r_lon > lon_max {
            lon_max = r_lon;
        }
        closest_vals.push(format!("({r_lat}, {r_lon}, {r_dist})"));
    }

    // Phase 2: Exact Pushdown
    let extract_query = if method == "nearest" {
        format!(
            "WITH closest(lat, lon, dist) AS (VALUES {vals})
             SELECT d.time, c.lat, c.lon, d.{vc}
             FROM closest c
             JOIN read_geo('{uri}',
                 lat_min := {lat_min},
                 lat_max := {lat_max},
                 lon_min := {lon_min},
                 lon_max := {lon_max}
             ) d ON c.lat = d.lat AND c.lon = d.lon
             ORDER BY d.time",
            vals = closest_vals.join(", ")
        )
    } else {
        format!(
            "WITH closest(lat, lon, dist) AS (VALUES {vals})
             SELECT d.time, {lat} AS lat, {lon} AS lon,
                    sum(d.{vc} * (1.0 / greatest(c.dist, 1e-9))) / sum(1.0 / greatest(c.dist, 1e-9)) AS {vc}
             FROM closest c
             JOIN read_geo('{uri}',
                 lat_min := {lat_min},
                 lat_max := {lat_max},
                 lon_min := {lon_min},
                 lon_max := {lon_max}
             ) d ON c.lat = d.lat AND c.lon = d.lon
             GROUP BY d.time
             ORDER BY d.time",
            vals = closest_vals.join(", ")
        )
    };

    let mut out = materialize(conn, &extract_query, DEFAULT_LIMIT)?;
    out["method"] = json!(method);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> Option<Connection> {
        std::env::set_var(
            "GEOZARR_ALLOW_PATH",
            env!("CARGO_MANIFEST_DIR").to_string() + "/..",
        );
        if std::env::var("EIDER_EXTENSION_PATH").is_err()
            && !std::path::Path::new("../target/debug/eider.duckdb_extension").exists()
        {
            eprintln!("skip: extension not built");
            return None;
        }
        Some(eider_session::open_session().unwrap())
    }

    fn zarr() -> String {
        format!(
            "{}/../climate_data.zarr/air_temperature",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    #[test]
    fn describe_returns_crs_and_dtype() {
        let Some(c) = session() else { return };
        let v = describe_dataset(&c, &zarr()).unwrap();
        let s = v.to_string();
        assert!(s.contains("EPSG:4326") && s.contains("Float32"), "{s}");
    }

    #[test]
    fn read_region_caps_and_handles() {
        let Some(c) = session() else { return };
        let v = read_region(&c, &zarr(), None, None, None, Some(5)).unwrap();
        assert_eq!(v["rows"].as_array().unwrap().len(), 5);
        assert!(v["row_count"].as_i64().unwrap() > 5);
        assert!(v["table_handle"]
            .as_str()
            .unwrap()
            .starts_with("mcp_result_"));
    }

    #[test]
    fn estimate_cost_returns_chunks() {
        let Some(c) = session() else { return };
        let v = estimate_cost(&c, &zarr(), None, None).unwrap();
        assert!(v.to_string().contains("total_chunks"));
    }

    fn polygons() -> String {
        format!(
            "{}/../scripts/demo_polygons.geojson",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    /// Mean (over polygons) of each polygon's `metric` value, for a given
    /// convention. Used to compare conventions on the same metric.
    fn mean_of_metric(v: &Value) -> f64 {
        let rows = v["rows"].as_array().unwrap();
        assert!(!rows.is_empty(), "expected at least one polygon row");
        let sum: f64 = rows.iter().map(|r| r["metric"].as_f64().unwrap()).sum();
        sum / rows.len() as f64
    }

    #[test]
    fn zonal_centroid_per_polygon_max() {
        let Some(c) = session() else { return };
        let v = zonal_stats(&c, &zarr(), &polygons(), "max", "centroid", None, Some(100)).unwrap();
        assert_eq!(v["convention"], json!("centroid"));
        assert_eq!(v["metric"], json!("max"));
        assert!(v["row_count"].as_i64().unwrap() >= 1);
        // Per-polygon max must be finite and within the dataset's plausible
        // air-temperature range (degC).
        for row in v["rows"].as_array().unwrap() {
            let mx = row["metric"].as_f64().unwrap();
            assert!(mx.is_finite(), "max should be finite: {mx}");
            assert!((-100.0..100.0).contains(&mx), "max out of range: {mx}");
        }
    }

    #[test]
    fn zonal_all_touched_ge_centroid() {
        let Some(c) = session() else { return };
        let centroid =
            zonal_stats(&c, &zarr(), &polygons(), "max", "centroid", None, Some(100)).unwrap();
        let all_touched = zonal_stats(
            &c,
            &zarr(),
            &polygons(),
            "max",
            "all_touched",
            None,
            Some(100),
        )
        .unwrap();
        // all_touched includes boundary cells the centroid rule drops, so its
        // mean-of-per-polygon-max can only be >= the centroid's.
        let c_mean = mean_of_metric(&centroid);
        let a_mean = mean_of_metric(&all_touched);
        assert!(
            a_mean >= c_mean,
            "all_touched mean-of-max ({a_mean}) should be >= centroid ({c_mean})"
        );
    }

    #[test]
    fn run_sql_selects_and_rejects_writes() {
        let Some(c) = session() else { return };
        // A plain SELECT is materialized into a temp table with head rows.
        let v = run_sql(&c, "SELECT 1 AS x", None).unwrap();
        assert_eq!(v["row_count"].as_i64().unwrap(), 1);
        assert_eq!(v["rows"][0]["x"].as_i64().unwrap(), 1);
        assert!(v["table_handle"]
            .as_str()
            .unwrap()
            .starts_with("mcp_result_"));
        // The guard rejects writes before touching the connection.
        assert!(run_sql(&c, "DROP TABLE foo", None).is_err());
    }

    #[test]
    fn zonal_area_weighted_rejects_max() {
        let Some(c) = session() else { return };
        assert!(zonal_stats(
            &c,
            &zarr(),
            &polygons(),
            "max",
            "area_weighted",
            None,
            Some(100)
        )
        .is_err());
    }

    #[test]
    fn extract_point_timeseries_nearest() {
        let Some(c) = session() else { return };
        // Valid lat/lon inside the dataset bounds
        let v = extract_point_timeseries(&c, &zarr(), 50.0, -10.0, "nearest", None).unwrap();
        assert_eq!(v["method"], json!("nearest"));
        assert!(v["row_count"].as_i64().unwrap() >= 1);
        let rows = v["rows"].as_array().unwrap();
        assert!(rows[0].get("time").is_some());
        assert!(rows[0].get("value").is_some());
        assert!(rows[0]["value"].is_number());
        assert!(v["table_handle"]
            .as_str()
            .unwrap()
            .starts_with("mcp_result_"));
    }

    #[test]
    fn extract_point_timeseries_idw() {
        let Some(c) = session() else { return };
        let v = extract_point_timeseries(&c, &zarr(), 50.0, -10.0, "idw", None).unwrap();
        assert_eq!(v["method"], json!("idw"));
        assert!(v["row_count"].as_i64().unwrap() >= 1);
        let rows = v["rows"].as_array().unwrap();
        assert!(rows[0].get("time").is_some());
        assert!(rows[0].get("value").is_some());
    }

    #[test]
    fn extract_point_timeseries_exact_hit() {
        let Some(c) = session() else { return };
        let v_nearest =
            extract_point_timeseries(&c, &zarr(), 50.0, -10.0, "nearest", None).unwrap();
        let rows = v_nearest["rows"].as_array().unwrap();
        let lat = rows[0]["lat"].as_f64().unwrap();
        let lon = rows[0]["lon"].as_f64().unwrap();

        // Use idw directly on the grid cell's own coordinate
        let v_idw = extract_point_timeseries(&c, &zarr(), lat, lon, "idw", None).unwrap();
        assert_eq!(v_idw["method"], json!("idw"));
        let idw_rows = v_idw["rows"].as_array().unwrap();

        // Nearest point value should equal IDW value because distance is 0 and dominates
        let nearest_val = rows[0]["value"].as_f64().unwrap();
        let idw_val = idw_rows[0]["value"].as_f64().unwrap();

        // Assert IDW is very close to nearest. The exact hit clamps distance to 1e-9,
        // so other cells may contribute minutely. We use a combined absolute/relative tolerance.
        let diff = (nearest_val - idw_val).abs();
        let tol = 1e-4 + nearest_val.abs() * 1e-4;
        assert!(
            diff <= tol,
            "IDW value {} should tightly track nearest {} for exact hit, diff {}",
            idw_val,
            nearest_val,
            diff
        );
    }

    #[test]
    fn extract_point_timeseries_missing_col() {
        let Some(c) = session() else { return };
        let res =
            extract_point_timeseries(&c, &zarr(), 50.0, -10.0, "nearest", Some("invalid_col"));
        assert!(res.is_err()); // valid column name check bypasses duckdb, or duckdb throws missing col
    }

    #[test]
    fn extract_point_timeseries_invalid_method() {
        let Some(c) = session() else { return };
        let res = extract_point_timeseries(&c, &zarr(), 50.0, -10.0, "bicubic", None);
        assert!(res.is_err());
    }

    #[test]
    fn extract_point_timeseries_out_of_bounds() {
        let Some(c) = session() else { return };
        let res = extract_point_timeseries(&c, &zarr(), 1000.0, 1000.0, "nearest", None);
        assert!(
            res.is_ok(),
            "nearest should succeed even for distant points, returning the edge cell"
        );
    }
}
