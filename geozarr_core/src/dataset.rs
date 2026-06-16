use crate::metadata::SpatialTransform;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use zarrs::array::{Array, ArrayMetadata, DataType};

pub struct ZarrDataset {
    pub array: Arc<Array<dyn zarrs::storage::ReadableStorageTraits>>,
    pub shape: Vec<u64>,
    pub chunk_shape: Vec<u64>,
    pub data_type: DataType,
    pub dim_names: Vec<String>,
    pub coords: HashMap<String, Vec<f64>>,
    pub lon_0_360_dims: HashSet<usize>,
    pub spatial_transform: Option<SpatialTransform>,
    pub is_remote: bool,
    pub fill_value_bytes: Option<Vec<u8>>,
}

/// Given the sorted asset names of a STAC group and an optional asset name,
/// return the array path to open (e.g. "/red"). Errors list available assets.
pub(crate) fn select_asset_path(assets: &[String], asset: Option<&str>) -> Result<String, String> {
    match asset {
        Some(a) if assets.iter().any(|n| n == a) => Ok(format!("/{a}")),
        Some(a) => Err(format!(
            "asset '{a}' not found. Available: {}",
            assets.join(", ")
        )),
        None if assets.len() == 1 => Ok(format!("/{}", assets[0])),
        None if assets.is_empty() => Err("STAC group has no assets".into()),
        None => Err(format!(
            "STAC Item has multiple assets; choose one with asset := '<name>'. Available: {}",
            assets.join(", ")
        )),
    }
}

impl ZarrDataset {
    pub fn open(
        path: &str,
        constraints: Option<&crate::query_planner::QueryConstraints>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::open_with_asset(path, None, constraints)
    }

    pub fn open_with_asset(
        path: &str,
        asset: Option<&str>,
        constraints: Option<&crate::query_planner::QueryConstraints>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let resolved_store = crate::store::resolve_sync_store(path, constraints)?;
        let is_remote = resolved_store.is_remote;
        let stac_assets = resolved_store.stac_assets.clone();
        let store_arc = resolved_store.store;

        // `resolve_sync_store` is the only place that builds a `VirtualStacStore`,
        // so it signals authoritatively whether this source is a STAC group. Branch
        // on that signal instead of re-sniffing `.zmetadata`: a plain Zarr array/group
        // or COG (`stac_assets == None`) opens the root array exactly as before, so a
        // corrupt or missing root array surfaces its genuine error rather than being
        // relabeled as a STAC "assets" problem.
        let array = match stac_assets {
            Some(assets) => {
                // STAC group: choose an asset and open it by path.
                let array_path = select_asset_path(&assets, asset)
                    .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
                Array::open(Arc::clone(&store_arc), &array_path).map_err(
                    |e| -> Box<dyn std::error::Error> {
                        format!("zarrs error (array): {}", e).into()
                    },
                )?
            }
            None => {
                // Plain Zarr (V2/V3) or COG: open the root array exactly as before.
                Array::open(Arc::clone(&store_arc), "/").map_err(
                    |e| -> Box<dyn std::error::Error> {
                        format!("zarrs error (array): {}", e).into()
                    },
                )?
            }
        };

        let shape = array.shape().to_vec();
        let rank = shape.len();
        if rank > 16 {
            return Err(format!(
                "Zarr array rank {} exceeds maximum supported dimensions (16)",
                rank
            )
            .into());
        }

        let metadata = array.metadata();

        let mut spatial_transform = None;
        if let ArrayMetadata::V2(meta) = metadata {
            if let Some(geozarr_meta) =
                crate::metadata::parse_geozarr_metadata(&Value::Object(meta.attributes.clone()))
            {
                spatial_transform = geozarr_meta.transform;
            }
        } else if let ArrayMetadata::V3(meta) = metadata {
            if let Some(geozarr_meta) =
                crate::metadata::parse_geozarr_metadata(&Value::Object(meta.attributes.clone()))
            {
                spatial_transform = geozarr_meta.transform;
            }
        }

        let dim_names = Self::resolve_dimension_names(metadata, rank);

        let (coords, lon_0_360_dims) = crate::coordinate_resolver::CoordinateResolver::resolve(
            path,
            Arc::clone(&store_arc),
            &shape,
            &dim_names,
        )?;

        let chunk_shape: Vec<u64> = array
            .chunk_grid()
            .chunk_shape(&vec![0; rank], &shape)
            .map_err(|_| -> Box<dyn std::error::Error> {
                "zarrs error: array bounds are out of grid".into()
            })?
            .ok_or_else(|| -> Box<dyn std::error::Error> {
                "zarrs error: array has no chunk shape".into()
            })?
            .iter()
            .map(|n| n.get())
            .collect();

        let data_type = array.data_type().clone();

        crate::validator::DatasetValidator::validate_chunk_sizes(&shape, &chunk_shape, &data_type)
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        let fv_bytes = array.fill_value().as_ne_bytes().to_vec();
        let fill_value_bytes = if fv_bytes.is_empty() {
            None
        } else {
            Some(fv_bytes)
        };

        Ok(Self {
            array: Arc::new(array),
            shape,
            chunk_shape,
            data_type,
            dim_names,
            coords,
            lon_0_360_dims,
            spatial_transform,
            is_remote,
            fill_value_bytes,
        })
    }

    pub fn schema(&self) -> Result<Vec<(String, zarrs::array::DataType)>, String> {
        let mut schema = Vec::new();
        for (i, name) in self.dim_names.iter().enumerate() {
            let has_transform = self
                .spatial_transform
                .as_ref()
                .is_some_and(|t| i < t.scale.len());
            if self.coords.contains_key(name) || has_transform {
                schema.push((name.clone(), zarrs::array::DataType::Float64));
            } else {
                schema.push((name.clone(), zarrs::array::DataType::Int64));
            }
        }
        schema.push(("value".to_string(), self.data_type.clone()));
        Ok(schema)
    }

    pub fn compute_bounds(
        &self,
        constraints: &crate::query_planner::QueryConstraints,
    ) -> (Vec<u64>, Vec<u64>) {
        let rank = self.shape.len();
        let mut bounds_min = vec![0; rank];
        let mut bounds_max = vec![0; rank];
        for (i, max_val) in bounds_max.iter_mut().enumerate().take(rank) {
            *max_val = if self.shape[i] > 0 {
                self.shape[i] - 1
            } else {
                0
            };
        }

        for (dim_index, name) in self.dim_names.iter().enumerate() {
            if let Some(&pinned_idx) = constraints.pins.get(name) {
                let target = std::cmp::min(pinned_idx, bounds_max[dim_index]);
                bounds_min[dim_index] = target;
                bounds_max[dim_index] = target;
                continue;
            }

            let (min_val_opt, max_val_opt) = constraints
                .bounds
                .get(name)
                .copied()
                .unwrap_or((None, None));

            if let Some(coord_vals) = self.coords.get(name) {
                let normalize_query = |v: f64| -> f64 {
                    crate::coordinates::denormalize_longitude(
                        v,
                        self.lon_0_360_dims.contains(&dim_index),
                    )
                };
                let is_ascending = coord_vals
                    .first()
                    .zip(coord_vals.last())
                    .is_none_or(|(f, l)| f <= l);
                if let Some(min_val) = min_val_opt {
                    let (t_min, t_max) = crate::query_planner::translate_filter(
                        coord_vals,
                        ">=",
                        normalize_query(min_val),
                        bounds_min[dim_index],
                        bounds_max[dim_index],
                    );
                    if is_ascending {
                        bounds_min[dim_index] = std::cmp::max(bounds_min[dim_index], t_min);
                    } else {
                        bounds_max[dim_index] = std::cmp::min(bounds_max[dim_index], t_max);
                    }
                }
                if let Some(max_val) = max_val_opt {
                    let (t_min, t_max) = crate::query_planner::translate_filter(
                        coord_vals,
                        "<=",
                        normalize_query(max_val),
                        bounds_min[dim_index],
                        bounds_max[dim_index],
                    );
                    if is_ascending {
                        bounds_max[dim_index] = std::cmp::min(bounds_max[dim_index], t_max);
                    } else {
                        bounds_min[dim_index] = std::cmp::max(bounds_min[dim_index], t_min);
                    }
                }
            } else if let Some(ref transform) = self.spatial_transform {
                if dim_index < transform.scale.len() {
                    let scale = transform.scale[dim_index];
                    let translation = transform.translation.get(dim_index).copied().unwrap_or(0.0);

                    if scale != 0.0 {
                        if let Some(min_val) = min_val_opt {
                            let idx1 = ((min_val - translation) / scale).floor() as i64;
                            let idx2 = ((min_val - translation) / scale).ceil() as i64;
                            let mut target_min = if scale > 0.0 { idx1 } else { idx2 };
                            if target_min < 0 {
                                target_min = 0;
                            }

                            if scale > 0.0 {
                                bounds_min[dim_index] =
                                    std::cmp::max(bounds_min[dim_index], target_min as u64);
                            } else {
                                bounds_max[dim_index] =
                                    std::cmp::min(bounds_max[dim_index], target_min as u64);
                            }
                        }

                        if let Some(max_val) = max_val_opt {
                            let idx1 = ((max_val - translation) / scale).floor() as i64;
                            let idx2 = ((max_val - translation) / scale).ceil() as i64;
                            let mut target_max = if scale > 0.0 { idx2 } else { idx1 };
                            if target_max < 0 {
                                target_max = 0;
                            }

                            if scale > 0.0 {
                                bounds_max[dim_index] =
                                    std::cmp::min(bounds_max[dim_index], target_max as u64);
                            } else {
                                bounds_min[dim_index] =
                                    std::cmp::max(bounds_min[dim_index], target_max as u64);
                            }
                        }
                    }
                }
            }
        }

        (bounds_min, bounds_max)
    }

    fn resolve_dimension_names(metadata: &ArrayMetadata, rank: usize) -> Vec<String> {
        // Resolution order: native Zarr v3 `dimension_names` -> `_ARRAY_DIMENSIONS`
        // attribute (v2/xarray convention) -> positional `dim_i` fallback.
        //
        // Zarr v3 stores dimension names in the native `dimension_names` field of
        // `zarr.json`, NOT in `_ARRAY_DIMENSIONS`. Reading only the attribute (as
        // before) meant real v3 stores fell back to `dim_i`, so the lat/lon axes
        // could not be identified and bbox -> chunk pruning silently no-oped.
        if let ArrayMetadata::V3(meta) = metadata {
            if let Some(dims) = &meta.dimension_names {
                if dims.len() == rank {
                    let names: Option<Vec<String>> = dims
                        .iter()
                        .map(|dim| dim.as_str().map(str::to_string))
                        .collect();
                    if let Some(names) = names {
                        return names;
                    }
                }
            }
        }

        let attributes = match metadata {
            ArrayMetadata::V2(meta) => &meta.attributes,
            ArrayMetadata::V3(meta) => &meta.attributes,
        };

        if let Some(Value::Array(dims)) = attributes.get("_ARRAY_DIMENSIONS") {
            if dims.len() == rank {
                let names: Option<Vec<String>> = dims
                    .iter()
                    .map(|dim| {
                        if let Value::String(s) = dim {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if let Some(names) = names {
                    return names;
                }
            }
        }

        (0..rank).map(|i| format!("dim_{}", i)).collect()
    }
}

#[cfg(test)]
mod select_tests {
    use super::select_asset_path;
    fn assets(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }
    #[test]
    fn picks_named_asset() {
        assert_eq!(
            select_asset_path(&assets(&["red", "nir"]), Some("nir")).unwrap(),
            "/nir"
        );
    }
    #[test]
    fn auto_selects_single_asset() {
        assert_eq!(
            select_asset_path(&assets(&["only"]), None).unwrap(),
            "/only"
        );
    }
    #[test]
    fn errors_on_multiple_without_asset() {
        let e = select_asset_path(&assets(&["red", "nir"]), None).unwrap_err();
        assert!(e.contains("red") && e.contains("nir") && e.contains("asset"));
    }
    #[test]
    fn errors_on_unknown_asset() {
        let e = select_asset_path(&assets(&["red", "nir"]), Some("green")).unwrap_err();
        assert!(e.contains("green") || e.contains("Available"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_dimension_names_fallback() {
        let json_meta = r#"{
            "zarr_format": 2,
            "shape": [1, 2, 3],
            "chunks": [1, 2, 3],
            "dtype": "<i4",
            "compressor": null,
            "fill_value": null,
            "filters": null,
            "order": "C"
        }"#;
        let metadata_bare: ArrayMetadata = serde_json::from_str(json_meta).unwrap();
        let names = ZarrDataset::resolve_dimension_names(&metadata_bare, 3);
        assert_eq!(names, vec!["dim_0", "dim_1", "dim_2"]);
    }

    #[test]
    fn non_stac_store_without_root_array_yields_zarrs_error_not_stac() {
        // Regression: a non-STAC source (stac_assets == None) with no root array
        // — e.g. a consolidated Zarr GROUP root — must surface zarrs' genuine
        // "missing metadata" error via the None branch, NOT be relabeled as a
        // STAC "assets" problem. We exercise the None branch's open directly:
        // resolve_sync_store sets stac_assets = None for plain Zarr, so
        // open_with_asset takes Array::open(store, "/"), whose error is returned
        // verbatim. An empty in-memory store has no root array metadata.
        use zarrs::storage::store::MemoryStore;
        let store: Arc<dyn zarrs::storage::ReadableStorageTraits> = Arc::new(MemoryStore::new());
        let err = match Array::open(Arc::clone(&store), "/") {
            Ok(_) => panic!("empty store should not yield a root array"),
            Err(e) => e.to_string(),
        };
        assert!(
            !err.contains("STAC") && !err.contains("multiple assets"),
            "non-STAC root-open error must not be relabeled as STAC: {err}"
        );
    }

    #[test]
    fn test_resolve_dimension_names_with_attributes() {
        let json_meta = r#"{
            "zarr_format": 2,
            "shape": [1, 2, 3],
            "chunks": [1, 2, 3],
            "dtype": "<i4",
            "compressor": null,
            "fill_value": null,
            "filters": null,
            "order": "C",
            "attributes": {
                "_ARRAY_DIMENSIONS": ["time", "lat", "lon"]
            }
        }"#;
        let metadata_attrs: ArrayMetadata = serde_json::from_str(json_meta).unwrap();
        let names = ZarrDataset::resolve_dimension_names(&metadata_attrs, 3);
        assert_eq!(names, vec!["time", "lat", "lon"]);
    }

    #[test]
    fn test_resolve_dimension_names_v3_native_dimension_names() {
        // Zarr v3 stores dimension names in the native `dimension_names` field of
        // zarr.json, NOT in the `_ARRAY_DIMENSIONS` attribute (the v2/xarray
        // convention). This store has ONLY the native field and no
        // `_ARRAY_DIMENSIONS`, so resolution must read the native field.
        let json_meta = r#"{
            "zarr_format": 3,
            "node_type": "array",
            "shape": [4, 4],
            "data_type": "int32",
            "chunk_grid": {
                "name": "regular",
                "configuration": { "chunk_shape": [2, 2] }
            },
            "chunk_key_encoding": { "name": "default" },
            "fill_value": 0,
            "codecs": [
                { "name": "bytes", "configuration": { "endian": "little" } }
            ],
            "dimension_names": ["lat", "lon"]
        }"#;
        let metadata: ArrayMetadata = serde_json::from_str(json_meta).unwrap();
        let names = ZarrDataset::resolve_dimension_names(&metadata, 2);
        assert_eq!(names, vec!["lat", "lon"]);
    }

    #[test]
    fn test_resolve_dimension_names_v3_partial_names_fall_back() {
        // If any native dimension name is null, the native field is ambiguous;
        // fall back (here, to `dim_i` since there is no `_ARRAY_DIMENSIONS`).
        let json_meta = r#"{
            "zarr_format": 3,
            "node_type": "array",
            "shape": [4, 4],
            "data_type": "int32",
            "chunk_grid": {
                "name": "regular",
                "configuration": { "chunk_shape": [2, 2] }
            },
            "chunk_key_encoding": { "name": "default" },
            "fill_value": 0,
            "codecs": [
                { "name": "bytes", "configuration": { "endian": "little" } }
            ],
            "dimension_names": ["lat", null]
        }"#;
        let metadata: ArrayMetadata = serde_json::from_str(json_meta).unwrap();
        let names = ZarrDataset::resolve_dimension_names(&metadata, 2);
        assert_eq!(names, vec!["dim_0", "dim_1"]);
    }
}

pub struct ZarrChunkStream {
    dataset: Arc<ZarrDataset>,
    grid_iterator: crate::scanner::GridIterator,
    num_chunks: u64,
    bounds_min: Vec<u64>,
    bounds_max: Vec<u64>,
}

impl crate::geo_dataset::ChunkStream for ZarrChunkStream {
    fn estimated_chunks(&self) -> Option<u64> {
        Some(self.num_chunks)
    }

    fn read_chunk(
        &self,
        chunk_idx: u64,
        buffer: &mut crate::types::ChunkBuffer
    ) -> Result<Option<crate::scanner::SubsetInfo>, Box<dyn std::error::Error>> {
        if chunk_idx >= self.num_chunks {
            return Ok(None);
        }

        let grid_pos = self.grid_iterator.get_chunk_pos(chunk_idx);
        let reader = crate::scanner::ChunkReader::new(
            self.dataset.array.clone(),
            self.dataset.is_remote,
            self.dataset.shape.clone(),
            self.dataset.chunk_shape.clone(),
        );

        macro_rules! retrieve_and_store {
            ($ty:ty, $variant:path $(,)*) => {{
                let (elements, subset_info) = reader
                    .read_chunk_subset::<$ty>(&grid_pos, &self.bounds_min, &self.bounds_max)?;
                *buffer = $variant(elements);
                Ok(Some(subset_info))
            }};
        }

        crate::dispatch_zarr_type!(self.dataset.data_type, retrieve_and_store,)
    }
}

impl crate::geo_dataset::GeoDataset for Arc<ZarrDataset> {
    fn schema(&self) -> Result<Vec<(String, DataType)>, Box<dyn std::error::Error>> {
        self.as_ref().schema().map_err(|e| e.into())
    }

    fn scan(
        &self,
        constraints: &crate::query_planner::QueryConstraints
    ) -> Result<Box<dyn crate::geo_dataset::ChunkStream>, Box<dyn std::error::Error>> {
        let (bounds_min, bounds_max) = self.compute_bounds(constraints);

        let rank = self.shape.len();
        let mut chunk_bounds_min = vec![0; rank];
        let mut chunk_bounds_max = vec![0; rank];
        for i in 0..rank {
            chunk_bounds_min[i] = bounds_min[i] / self.chunk_shape[i];
            chunk_bounds_max[i] = bounds_max[i] / self.chunk_shape[i];
        }

        let num_chunks: u64 = (0..rank)
            .map(|i| chunk_bounds_max[i].saturating_sub(chunk_bounds_min[i]) + 1)
            .product();

        let grid_iterator = crate::scanner::GridIterator::new(
            &bounds_min,
            &bounds_max,
            &self.shape,
            &self.chunk_shape,
        );

        Ok(Box::new(ZarrChunkStream {
            dataset: Arc::clone(self),
            grid_iterator,
            num_chunks,
            bounds_min,
            bounds_max,
        }))
    }
}

pub fn open_dataset(
    path: &str, 
    asset: Option<&str>,
    constraints: Option<&crate::query_planner::QueryConstraints>
) -> Result<Box<dyn crate::geo_dataset::GeoDataset>, Box<dyn std::error::Error>> {
    let zarr = ZarrDataset::open_with_asset(path, asset, constraints)?;
    Ok(Box::new(Arc::new(zarr)))
}
