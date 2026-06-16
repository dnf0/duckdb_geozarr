---
sidebar_position: 3
---

# eider search

Discover GeoZarr / COG assets from a STAC API. Run interactively to pick a
provider and collection, or pass flags to script it.

## Synopsis

```
eider search [--api URL] [--collection ID] [--bbox MIN_LON,MIN_LAT,MAX_LON,MAX_LAT] [--datetime RANGE] [--output table|json]
```

## Options

| Option | Description |
|---|---|
| `--api URL` | STAC API root, e.g. `https://planetarycomputer.microsoft.com/api/stac/v1`. Prompted if omitted (TUI). |
| `--collection ID` | Collection to search, e.g. `era5-pds`. Prompted if omitted (TUI). |
| `--bbox` | Bounding box `min_lon,min_lat,max_lon,max_lat`. |
| `--datetime` | Datetime range, e.g. `2020-01-01T00:00:00Z/2020-12-31T23:59:59Z`. |

## Behavior

In interactive mode, `search` presents provider and collection pickers, then a
dataset selector. In `--output=json` mode it requires `--api` and `--collection`
and prints the matching STAC feature URIs as `{"status":"success","uris":[…]}`.

> **Note:** `search` currently emits each matching STAC feature's self link.
> These STAC item URLs can be passed directly to `read_geo` as native sources.

## Examples

```bash
eider search --bbox -122.27,37.77,-122.22,37.81
eider search --api https://example.com/stac --collection era5-pds --output=json
```
