use crate::dispatch_write_chunk;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, VTab};
use duckdb::Result;
use std::collections::HashMap;
use std::sync::Mutex;
use zarrs::array::DataType;

fn zarr_to_duckdb_logical_type(data_type: &DataType) -> std::result::Result<LogicalTypeId, String> {
    match data_type {
        DataType::Float32 => Ok(LogicalTypeId::Float),
        DataType::Float64 => Ok(LogicalTypeId::Double),
        DataType::Int32 => Ok(LogicalTypeId::Integer),
        DataType::Int64 => Ok(LogicalTypeId::Bigint),
        DataType::String => Ok(LogicalTypeId::Varchar),
        DataType::Bool => Ok(LogicalTypeId::Boolean),
        DataType::Int8 => Ok(LogicalTypeId::Tinyint),
        DataType::Int16 => Ok(LogicalTypeId::Smallint),
        DataType::UInt8 => Ok(LogicalTypeId::UTinyint),
        DataType::UInt16 => Ok(LogicalTypeId::USmallint),
        DataType::UInt32 => Ok(LogicalTypeId::UInteger),
        DataType::UInt64 => Ok(LogicalTypeId::UBigint),
        _ => Err(format!("Unsupported data type: {:?}", data_type)),
    }
}

pub struct ReadGeoBindData {
    pub stream: std::sync::Arc<dyn geozarr_core::geo_dataset::ChunkStream>,
    pub metadata: geozarr_core::geo_dataset::DatasetMetadata,
    pub bounds_min: Vec<u64>,
    pub bounds_max: Vec<u64>,
}

use geozarr_core::types::ChunkBuffer;

pub struct LocalState {
    pub current_chunk_buffer: Option<ChunkBuffer>,
    pub projected_columns: Vec<usize>,
    /// Cursor into `current_chunk_buffer` (which holds only the valid subset elements).
    pub element_cursor: usize,
    /// Subset info for coordinate reconstruction.
    pub subset_info: Option<geozarr_core::scanner::SubsetInfo>,
    pub grid_batch: Vec<Vec<u64>>,
    pub buffer_pool: Option<Vec<u8>>,
}

pub struct ReadGeoInitData {
    pub next_chunk: std::sync::atomic::AtomicU64,
    pub local_states: Mutex<HashMap<std::thread::ThreadId, LocalState>>,
    pub projected_columns: Vec<usize>,
}

pub struct ReadGeoVTab;

impl VTab for ReadGeoVTab {
    type InitData = ReadGeoInitData;
    type BindData = ReadGeoBindData;

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            ("lat_min".to_string(), LogicalTypeId::Double.into()),
            ("lat_max".to_string(), LogicalTypeId::Double.into()),
            ("lon_min".to_string(), LogicalTypeId::Double.into()),
            ("lon_max".to_string(), LogicalTypeId::Double.into()),
            ("time_min".to_string(), LogicalTypeId::Double.into()),
            ("time_max".to_string(), LogicalTypeId::Double.into()),
            ("pins".to_string(), LogicalTypeId::Varchar.into()),
            ("asset".to_string(), LogicalTypeId::Varchar.into()),
        ])
    }

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        if bind.get_parameter_count() < 1 {
            return Err("read_geo requires at least 1 parameter (path)".into());
        }

        let path = bind.get_parameter(0).to_string();

        let mut bounds = HashMap::new();
        for name in &["lat", "lon", "time", "y", "x"] {
            let min_param_name = format!("{}_min", name);
            let max_param_name = format!("{}_max", name);

            let min_val_opt = bind
                .get_named_parameter(&min_param_name)
                .and_then(|v| v.to_string().parse::<f64>().ok());
            let max_val_opt = bind
                .get_named_parameter(&max_param_name)
                .and_then(|v| v.to_string().parse::<f64>().ok());
            bounds.insert(name.to_string(), (min_val_opt, max_val_opt));
        }

        let mut pins = HashMap::new();
        if let Some(pins_val) = bind.get_named_parameter("pins") {
            let pins_str = pins_val.to_string();
            for pair in pins_str.split(',') {
                let mut parts = pair.splitn(2, '=');
                if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                    if let Ok(idx) = v.trim().parse::<u64>() {
                        pins.insert(k.trim().to_string(), idx);
                    }
                }
            }
        }

        let constraints = geozarr_core::query_planner::QueryConstraints { bounds, pins };

        let is_stac_api =
            path.starts_with("http") && (path.contains("search") || path.contains("collections"));
        if is_stac_api {
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
            match (lon_bounds.0, lat_bounds.0, lon_bounds.1, lat_bounds.1) {
                (Some(lon_min), Some(lat_min), Some(lon_max), Some(lat_max)) => {
                    let area = (lon_max - lon_min).abs() * (lat_max - lat_min).abs();
                    if area > 1000.0 {
                        return Err("Bounding box area too large for STAC API. Please provide a tighter bbox.".into());
                    }
                }
                _ => return Err(
                    "Bounding box (lat_min, lat_max, lon_min, lon_max) is required for STAC APIs."
                        .into(),
                ),
            }
        }

        let asset = bind.get_named_parameter("asset").map(|v| v.to_string());
        let dataset =
            geozarr_core::dataset::open_dataset(&path, asset.as_deref(), Some(&constraints))?;

        let schema = dataset
            .schema()
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;
        for (name, data_type) in schema {
            let type_id = zarr_to_duckdb_logical_type(&data_type)
                .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
            bind.add_result_column(&name, type_id.into());
        }

        let metadata = dataset.metadata();
        let (bounds_min, bounds_max) = dataset.compute_bounds(&constraints);
        let stream = dataset.scan(&constraints)?;

        Ok(ReadGeoBindData {
            stream: stream.into(),
            metadata,
            bounds_min,
            bounds_max,
        })
    }

    fn init(_init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*_init.get_bind_data::<ReadGeoBindData>() };

        if let Some(chunks) = bind_data.stream.estimated_chunks() {
            _init.set_max_threads(chunks);
        }

        Ok(ReadGeoInitData {
            next_chunk: std::sync::atomic::AtomicU64::new(0),
            local_states: std::sync::Mutex::new(HashMap::new()),
            projected_columns: _init
                .get_column_indices()
                .into_iter()
                .map(|i| i as usize)
                .collect(),
        })
    }

    fn func(
        func: &duckdb::vtab::TableFunctionInfo<ReadGeoVTab>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let thread_id = std::thread::current().id();
        let mut local_state = {
            let mut local_states = init_data
                .local_states
                .lock()
                .map_err(|e| format!("Mutex poisoned: {}", e))?;

            if let Some(state) = local_states.remove(&thread_id) {
                state
            } else {
                LocalState {
                    current_chunk_buffer: None,
                    projected_columns: init_data.projected_columns.clone(),
                    element_cursor: 0,
                    subset_info: None,
                    grid_batch: Vec::new(),
                    buffer_pool: None,
                }
            }
        };

        // Dispatch based on data type
        geozarr_core::dispatch_zarr_type!(
            bind_data.metadata.data_type,
            dispatch_write_chunk,
            output,
            &mut local_state,
            &init_data.next_chunk,
            bind_data
        )?;

        let mut local_states = init_data
            .local_states
            .lock()
            .map_err(|e| format!("Mutex poisoned: {}", e))?;
        local_states.insert(thread_id, local_state);

        Ok(())
    }
}

pub struct PlanReadGeoBindData {
    pub total_chunks: u64,
    pub total_bytes: u64,
    pub rank: usize,
}

pub struct PlanReadGeoInitData {
    pub done: std::sync::atomic::AtomicBool,
    pub projected_columns: Vec<usize>,
}

pub struct PlanReadGeoVTab;

impl VTab for PlanReadGeoVTab {
    type InitData = PlanReadGeoInitData;
    type BindData = PlanReadGeoBindData;

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        ReadGeoVTab::parameters()
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        ReadGeoVTab::named_parameters()
    }

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        // Reuse ReadGeoVTab logic to compute bounds
        let read_bind = ReadGeoVTab::bind(bind)?;

        let rank = read_bind.metadata.shape.len();
        let mut total_chunks = 1u64;
        let mut chunk_volume = 1u64;

        for i in 0..rank {
            let min_chunk = read_bind.bounds_min[i] / read_bind.metadata.chunk_shape[i];
            let max_chunk = read_bind.bounds_max[i] / read_bind.metadata.chunk_shape[i];

            let num_chunks = max_chunk.saturating_sub(min_chunk).saturating_add(1);
            total_chunks = total_chunks.saturating_mul(num_chunks);
            chunk_volume = chunk_volume.saturating_mul(read_bind.metadata.chunk_shape[i]);
        }

        let bytes_per_element =
            geozarr_core::types::bytes_per_element(&read_bind.metadata.data_type);

        let total_bytes = total_chunks
            .saturating_mul(chunk_volume)
            .saturating_mul(bytes_per_element);

        bind.add_result_column("total_chunks", LogicalTypeId::Bigint.into());
        bind.add_result_column("total_bytes", LogicalTypeId::Bigint.into());

        Ok(PlanReadGeoBindData {
            total_chunks,
            total_bytes,
            rank,
        })
    }

    fn init(_init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(PlanReadGeoInitData {
            done: std::sync::atomic::AtomicBool::new(false),
            projected_columns: _init
                .get_column_indices()
                .into_iter()
                .map(|i| i as usize)
                .collect(),
        })
    }

    fn func(
        func: &duckdb::vtab::TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        if init_data
            .done
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            output.set_len(0);
            return Ok(());
        }

        let bind_data = func.get_bind_data();

        let total_chunks_idx = bind_data.rank + 1; // 1 for value column + rank for coordinates
        let total_bytes_idx = bind_data.rank + 2;

        for &col_idx in init_data.projected_columns.iter() {
            if col_idx == total_chunks_idx {
                output.flat_vector(col_idx).as_mut_slice::<i64>()[0] =
                    bind_data.total_chunks as i64;
            } else if col_idx == total_bytes_idx {
                output.flat_vector(col_idx).as_mut_slice::<i64>()[0] = bind_data.total_bytes as i64;
            }
        }

        output.set_len(1);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iteration_state_initialization() {
        let local_state = LocalState {
            element_cursor: 0,
            current_chunk_buffer: None,
            projected_columns: vec![0, 1, 2],
            subset_info: None,
            grid_batch: Vec::new(),
            buffer_pool: None,
        };
        assert_eq!(local_state.element_cursor, 0);
    }
}
