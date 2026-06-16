use crate::query_planner::QueryConstraints;
use crate::types::ChunkBuffer;
use zarrs::array::DataType;
use std::collections::{HashMap, HashSet};
use crate::metadata::SpatialTransform;

#[derive(Clone)]
pub struct DatasetMetadata {
    pub shape: Vec<u64>,
    pub chunk_shape: Vec<u64>,
    pub data_type: DataType,
    pub dim_names: Vec<String>,
    pub coords: HashMap<String, Vec<f64>>,
    pub lon_0_360_dims: HashSet<usize>,
    pub fill_value_bytes: Option<Vec<u8>>,
    pub spatial_transform: Option<SpatialTransform>,
}

pub trait GeoDataset: Send + Sync {
    fn schema(&self) -> Result<Vec<(String, DataType)>, Box<dyn std::error::Error>>;

    fn metadata(&self) -> DatasetMetadata;

    fn compute_bounds(&self, constraints: &QueryConstraints) -> (Vec<u64>, Vec<u64>);

    fn scan(
        &self,
        constraints: &QueryConstraints
    ) -> Result<Box<dyn ChunkStream>, Box<dyn std::error::Error>>;
}

pub trait ChunkStream: Send + Sync {
    fn estimated_chunks(&self) -> Option<u64>;

    fn read_chunk(
        &self,
        chunk_idx: u64,
    ) -> Result<Option<(ChunkBuffer, crate::scanner::SubsetInfo)>, Box<dyn std::error::Error>>;
}
