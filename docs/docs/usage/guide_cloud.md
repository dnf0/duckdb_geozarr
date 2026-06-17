---
sidebar_position: 2
---

# Working with cloud data

Eider reads Zarr arrays directly from cloud storage via
[Apache OpenDAL](https://opendal.apache.org/) — no download step. The same URIs
work from both the SQL extension and the CLI.

> The examples below hit remote endpoints and require valid credentials /
> network access; they are not runnable from the repo as-is.

## Configure access

Eider's CLI and extension support hierarchical configuration. Credentials and settings can be loaded from standard environment variables, or automatically from your `eider.toml` config file or AWS config files.

```bash
# Standard Environment Variables
export AWS_ACCESS_KEY_ID=…
export AWS_SECRET_ACCESS_KEY=…
export AWS_REGION=us-east-1

# Allow local filesystem reads (when mixing local + remote)
export GEOZARR_ALLOW_PATH=/
```

### Hierarchical CLI Configuration and S3 Secret Injection

The `eider` CLI automatically resolves S3 credentials via its configuration hierarchy:
1. Explicit CLI arguments (highest priority)
2. Environment variables (`AWS_ACCESS_KEY_ID`, etc.)
3. `eider.toml` configuration files (current directory or `~/.config/eider/`)
4. Native AWS credential resolution (e.g., `~/.aws/credentials`)

When running DuckDB queries via the CLI's SQL capabilities, these resolved S3 credentials are automatically injected into the DuckDB session, so you don't need to manually configure `INSTALL httpfs; LOAD httpfs; SET s3_access_key_id='...';` inside your SQL scripts.

## Query remote data from SQL

```sql
LOAD '/absolute/path/to/eider.duckdb_extension';

SELECT lat, AVG(value) AS mean_temp
FROM read_geo('s3://my-bucket/data.zarr', lat_min := 45.0, lat_max := 55.0)
GROUP BY lat;
```

`http(s)://` URIs work the same way:

```sql
SELECT * FROM read_zarr_metadata('https://example.com/data.zarr');
```

## Query remote data from the CLI

```bash
eider info s3://my-bucket/data.zarr
eider extract https://example.com/data.zarr ./region.geojson --out analysis.duckdb --yes
```

## Next steps

- [SQL Reference](./sql_reference.md) — `read_geo` parameters and pushdown.
- [CLI Reference](./cli_tui.md) — all commands accept remote URIs.
