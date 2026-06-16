---
sidebar_position: 2
---

# read_geo

`read_geo` reads a geospatial array as a relational table — one row per cell —
streaming only the chunks that intersect the requested bounds.

## Signature

```sql
read_geo(uri VARCHAR, [named parameters])
```

- `uri` — positional. A Zarr array path or `s3://`/`https://` URI (see [Source URIs](./sql_reference.md#source-uris)). A single-band Cloud Optimized GeoTIFF (`.tif`/`.tiff`, uncompressed or Deflate) is also a supported, georeferenced source — for an EPSG:4326 COG the `lat_min`/`lon_max` bounds apply; projected COGs read in their native CRS without geographic bbox pushdown, and multi-band/LZW/JPEG/WebP COGs are not yet supported (see [COG virtualization](../engineering/cog_virtualization.mdx)). A STAC **Item** or **ItemCollection** (local path or `http(s)://` Search API) is also a supported source: pick a COG asset with the `asset` parameter (auto-selected when the Item has exactly one). An ItemCollection stacks that asset across its Items into a `time` dimension — epoch seconds from each Item's `properties.datetime`, filterable with `time_min`/`time_max` — and the collection must be grid-uniform (see [COG virtualization](../engineering/cog_virtualization.mdx)). For remote STAC Search APIs, `lat_min`/`lon_max` and `time_min`/`time_max` bounds are automatically pushed down to the server as STAC `bbox` and `datetime` query parameters. STAC API pagination, multi-resolution collections, multi-asset stacking, and Collection/Catalog traversal are not yet supported and return a clear error.

### Named parameters

| Parameter | Type | Description |
|---|---|---|
| `lat_min`, `lat_max` | `DOUBLE` | Latitude bounds; chunks outside are pruned before fetch. |
| `lon_min`, `lon_max` | `DOUBLE` | Longitude bounds. |
| `time_min`, `time_max` | `DOUBLE` | Time-index bounds (numeric). |
| `pins` | `VARCHAR` | Pin non-spatial dimensions to fixed indices (see [pins](./sql_reference.md#pins)). |
| `asset` | `VARCHAR` | Selects a COG asset from a STAC Item or ItemCollection by name. Optional when there is a single COG asset (auto-selected); required when there are several, otherwise `read_geo` errors listing the available asset names. For an ItemCollection, the chosen asset is stacked across Items along `time`. |

## Output

| Column | Type | Description |
|---|---|---|
| `time` | `DOUBLE` | Time coordinate value. |
| `lat` | `DOUBLE` | Latitude coordinate. |
| `lon` | `DOUBLE` | Longitude coordinate. |
| `value` | `FLOAT` | Array cell value (`NULL` for Zarr `fill_value`). |

Missing cells (Zarr `fill_value`) are returned as SQL `NULL`.

## Examples

### Basic

```sql
SELECT * FROM read_geo('climate_data.zarr/air_temperature') LIMIT 5;
```

```
┌───────────┬────────┬────────┬────────────┐
│   time    │  lat   │  lon   │   value    │
│  double   │ double │ double │   float    │
├───────────┼────────┼────────┼────────────┤
│ 1297320.0 │   90.0 │ -180.0 │ -34.926773 │
│ 1297320.0 │   90.0 │ -177.5 │ -34.926773 │
│ 1297320.0 │   90.0 │ -175.0 │ -34.926773 │
│ 1297320.0 │   90.0 │ -172.5 │ -34.926773 │
│ 1297320.0 │   90.0 │ -170.0 │ -34.926773 │
└───────────┴────────┴────────┴────────────┘
```

### Spatial bounding-box pushdown

```sql
SELECT lat, AVG(value) AS mean_temp
FROM read_geo('climate_data.zarr/air_temperature', lat_min := 30, lat_max := 50)
GROUP BY lat
ORDER BY lat;
```

### Pinning a dimension

```sql
SELECT * FROM read_geo('climate_data.zarr/air_temperature', pins := 'time=0') LIMIT 5;
```

## See also

- [read_zarr_metadata](./sql_read_zarr_metadata.md) — inspect shape/type/CRS first.
- [plan_read_geo](./sql_plan_read_geo.md) — estimate the read cost.
