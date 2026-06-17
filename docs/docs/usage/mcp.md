# MCP server

`eider-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io/)
**stdio** server that exposes eider's geospatial reads to an AI agent. It runs
one sandboxed, stateful DuckDB session with the eider extension and `spatial`
loaded, and surfaces **7 curated tools** instead of a raw SQL prompt.

The point isn't to bolt a chat box onto SQL — it's to give an agent a small set
of tools where the *correct* thing is the *easy* thing:

- **Correctness by construction.** Region reads push your bounding box and time
  window into the Zarr read, so only intersecting chunks are fetched (see
  [Spatial Pruning](../engineering/spatial_pruning.mdx)); zonal stats force you
  to name a [convention](../engineering/zonal_stats.mdx) (centroid /
  all-touched / area-weighted) rather than silently pick one. The geometry and
  the cost model are encoded in the tool, not left to the agent to reinvent.
- **Safe autonomous use.** Every tool is read-only. The `run_sql` escape hatch
  is statically guarded, results are row-capped, full data parks in a session
  temp table (never dumped into the agent's context), filesystem reads are
  sandboxed to a path you choose, and each call has a wall-clock timeout.
- **Stateful composition.** The session persists across calls, so the agent can
  inspect a dataset, estimate cost, read a window into a table, then keep
  querying that table by `table_handle` — building a multi-step analysis
  without re-reading the grid each time.
- **Progress streaming.** Eider automatically streams live DuckDB query progress 
  back to the MCP client through notifications, so agents and users can monitor
  long-running zonal statistics or region extractions without polling.

## Running the server

Build the binary from a clone (see [Installation](./installation.md) for the
extension itself):

```bash
cargo build -p eider-mcp   # binary at target/debug/eider-mcp
```

The server needs two things at runtime:

- **`EIDER_EXTENSION_PATH`** — absolute path to `eider.duckdb_extension`. If
  unset, the server looks for the extension next to the binary or under
  `target/debug`. Set it explicitly to be safe.
- **`GEOZARR_ALLOW_PATH`** — the directory tree that reads are sandboxed to.
  Point it at the data you want the agent to reach (e.g. `export
  GEOZARR_ALLOW_PATH=/data/zarr`), not at `/`.

```bash
export EIDER_EXTENSION_PATH=/absolute/path/to/eider.duckdb_extension
export GEOZARR_ALLOW_PATH=/data/zarr
target/debug/eider-mcp
```

The process speaks MCP over stdio, so you normally don't launch it by hand — an
MCP client spawns it for you (next section). For remote data, set the usual
`AWS_*` credentials in the same environment (see
[Authentication & access](./installation.md#authentication--access)).

## Client configuration

`eider-mcp` is a standard stdio MCP server, so any MCP client that accepts a
`command` + `env` config can run it. Add a `mcpServers` entry like this:

```json
{
  "mcpServers": {
    "eider": {
      "command": "/absolute/path/to/target/debug/eider-mcp",
      "env": {
        "EIDER_EXTENSION_PATH": "/absolute/path/to/eider.duckdb_extension",
        "GEOZARR_ALLOW_PATH": "/data/zarr"
      }
    }
  }
}
```

Use absolute paths throughout — the client launches the binary in its own
working directory.

## Tool reference

All read tools that return tabular data follow the same shape: a capped head of
rows inlined in the result, plus a **`table_handle`** naming a session TEMP
table that holds the *full* result. Follow up with `run_sql` (or
`describe_table` / `list_tables`) against that handle to drill in without
re-reading the grid.

### `describe_dataset(uri)`

Returns `{ metadata: [{ array_shape, chunk_shape, data_type, crs }] }` — wraps
`read_zarr_metadata`. **When to use:** first, to learn an array's shape, dtype,
and CRS before you read it or plan a cost.

### `estimate_cost(uri, bbox?, time?)`

Returns `{ estimate: [{ total_chunks, total_bytes }] }` — wraps
`plan_read_geo`, with the same `bbox`/`time` pushdown as `read_region`.
**When to use:** before a large `read_region`, to gate cost. `bbox` is a JSON
object `{ lon_min, lat_min, lon_max, lat_max }`; `time` is `{ min, max }` in
epoch seconds.

### `read_region(uri, bbox?, time?, asset?, limit?=1000)`

Returns `{ table_handle, row_count, columns, rows, truncated }`. Reads a
bbox / time / asset window; **`bbox` and `time` are pushed down**, so only the
Zarr chunks that intersect the window are fetched (see
[Spatial Pruning](../engineering/spatial_pruning.mdx)). `bbox` and `time` take
the same JSON shapes as `estimate_cost`; `asset` selects a named asset; `limit`
caps the inlined head (full data stays in the handle). **When to use:** to pull
a spatial / temporal slice into a session table for further querying.

### `zonal_stats(grid_uri, polygons, metric, convention, value_col?="value", limit?)`

Returns the same `{ table_handle, row_count, columns, rows, truncated }` shape
plus the `convention` and `metric` echoed back, with one row per polygon.
`polygons` is a path read via `ST_Read`; the polygons' combined bounding box is
pushed into the grid read so only intersecting chunks are fetched.

- `metric` ∈ `{ max, min, mean, sum, count }`.
- `convention` ∈ `{ centroid, all_touched, area_weighted }`:
  - **`centroid`** — a cell counts if its center lies in the polygon. Cheapest;
    drops partially-covered edge cells.
  - **`all_touched`** — a cell counts if its box intersects the polygon.
    Includes the boundary ring the centroid rule drops, so it is the
    conservative choice for `max` / `min` (peak-exposure) queries.
  - **`area_weighted`** — cells are weighted by their fractional overlap with
    the polygon; meaningful only for area-true `mean` / `sum`. Combining it
    with `max` / `min` is **rejected**.

Pick the convention deliberately — that, not the algorithm, is what makes a
per-polygon result correct. See [Zonal statistics](../engineering/zonal_stats.mdx)
for the full reasoning and benchmarks. **When to use:** to aggregate a grid per
vector boundary (e.g. peak hazard per footprint).

### `extract_point_timeseries(uri, points, value_col?="value")`

Returns `{ table_handle, row_count, columns, rows, truncated }` containing timeseries extracted at specific geographic points. `points` is an array of `{lon, lat, id?}` objects. The tool pushes the points' bounding box down to the read, filters the cells exactly matching each point, and returns a time-stacked result. **When to use:** when the user wants to extract the value history at specific locations (e.g. tracking temperature at a list of weather stations).

### `list_tables()`

Returns the tables visible in the session, including the temp result handles
from earlier calls. **When to use:** to see what's available to query.

### `describe_table(name)`

Returns `{ schema, sample }` for a session table. **When to use:** to inspect
the columns and a few rows of a result handle before writing SQL against it.

### `run_sql(sql, limit?)`

A guarded, **read-only** escape hatch over the session. Returns the
`{ table_handle, row_count, columns, rows, truncated }` shape for queries, or
`{ status: "ok" }` for statements that produce no result set. **When to use:**
for joins / aggregations / follow-up analysis over result handles that the
curated tools don't cover.

Only these statement forms are accepted: `SELECT`, `WITH`, `DESCRIBE`,
`EXPLAIN`, `SHOW`, `PRAGMA`, `VALUES`, `FROM`, `CREATE TEMP TABLE/VIEW`, and
`SET VARIABLE`. Writes, multiple statements, and `INSTALL` / `ATTACH` / `COPY` /
`EXPORT` / `INSERT` / `UPDATE` / `DELETE` / `DROP` / `ALTER` / `LOAD` are
rejected before the connection is touched.

## Safety & sandbox model

The server is built to be handed to an autonomous agent without supervision:

- **Read-only guard.** `run_sql` is statically validated (allow-listed first
  keyword, `CREATE` restricted to TEMP objects, `SET` restricted to
  `SET VARIABLE`, denied mutating / IO tokens, single statement only). It is
  deliberately conservative — when in doubt it rejects.
- **Row caps + handles.** Tool results inline only a capped head; the full
  result lives in a session TEMP table referenced by `table_handle`, so large
  reads never flood the agent's context.
- **Per-call timeout.** Each call has a **120 s** wall-clock budget. A watchdog
  interrupts a runaway query at the server layer (this DuckDB version has no
  native `statement_timeout`).
- **Path sandbox.** Filesystem reads are confined to `GEOZARR_ALLOW_PATH`.

## A worked agent flow

A typical "max hazard per building" task composes the tools left to right:

1. **`describe_dataset("hazard.zarr/depth")`** — confirm the array's shape,
   dtype, and CRS so the agent knows it's reading the right grid.
2. **`estimate_cost("hazard.zarr/depth", bbox)`** — with the assets' bounding
   box, check `total_chunks` / `total_bytes` before committing to the read.
   If it's small enough, proceed.
3. **`zonal_stats("hazard.zarr/depth", "assets.geojson", "max", "all_touched")`**
   — compute the per-polygon peak. `all_touched` is the conservative choice for
   a max, and the polygons' bbox is pushed into the read automatically, so only
   the intersecting chunks are fetched. The result comes back capped, with the
   full per-polygon table behind a `table_handle`.
4. **`run_sql("SELECT name, metric FROM mcp_result_0 ORDER BY metric DESC LIMIT 10", limit := 10)`**
   — rank the hottest polygons directly from the handle, no re-read.

For a windowed read instead of a zonal aggregate, swap step 3 for
**`read_region`** with a `bbox` / `time`, then keep querying the resulting
handle. Each step builds on the session state from the last.

## v1 non-goals

- **stdio transport only** — no HTTP / SSE server.
- **No auth** — the trust model is local stdio (the client you configure spawns
  the process).
- **No writes or ingest** — the session is read-only by design.
- **No CLI-mirror tools** — workflows the curated tools don't cover go through
  `run_sql`, not bespoke tools.
- **One connection per process.**
