---
sidebar_position: 1
---

# SQL Reference

Eider exposes Zarr/GeoZarr arrays to DuckDB through three table functions. Load
the extension first (see [Installation](./installation.md)): launch
`duckdb -unsigned` and `LOAD '/absolute/path/to/eider.duckdb_extension';`.

## Functions

| Function | Purpose |
|---|---|
| [`read_geo`](./sql_read_geo.md) | Read an array as a relational table, with spatial/temporal pushdown. |
| [`read_zarr_metadata`](./sql_read_zarr_metadata.md) | Inspect an array's shape, chunking, type, and CRS. |
| [`plan_read_geo`](./sql_plan_read_geo.md) | Estimate a read's cost (chunks/bytes) before fetching. |

## Spatial & temporal pushdown

`read_geo` and `plan_read_geo` accept `lat_min`/`lat_max`, `lon_min`/`lon_max`,
and `time_min`/`time_max` (all `DOUBLE`) named parameters. Bounds are applied at
the **chunk** level: chunks lying entirely outside the requested range are never
fetched, so a tightly-scoped query touches only a fraction of the array.

```sql
SELECT lat, AVG(value)
FROM read_geo('s3://bucket/data.zarr', lat_min := 45.0, lat_max := 55.0)
GROUP BY lat;
```

## pins

Use the `pins` parameter (`VARCHAR`) to fix non-spatial dimensions to specific
indices, e.g. a single timestep:

```sql
SELECT * FROM read_geo('climate_data.zarr/air_temperature', pins := 'time=0');
```

## Supported types

Zarr element types map to DuckDB types as follows:

| Zarr type | DuckDB type |
|---|---|
| `bool` | `BOOLEAN` |
| `int8` / `int16` / `int32` / `int64` | `TINYINT` / `SMALLINT` / `INTEGER` / `BIGINT` |
| `uint8` / `uint16` / `uint32` / `uint64` | `UTINYINT` / `USMALLINT` / `UINTEGER` / `UBIGINT` |
| `float32` / `float64` | `FLOAT` / `DOUBLE` |
| `string` | `VARCHAR` |

A Zarr `fill_value` is surfaced as SQL `NULL`.

## Source URIs

The URI argument accepts local paths and remote `s3://` and `http(s)://`
locations (configured via OpenDAL environment variables — see
[Installation](./installation.md)). `read_geo` reads Zarr arrays, Cloud Optimized GeoTIFFs (COGs), and STAC Items and ItemCollections natively.
