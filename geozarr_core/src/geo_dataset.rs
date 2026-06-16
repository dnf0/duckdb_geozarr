use crate::query_planner::QueryConstraints;
use crate::types::ChunkBuffer;
use zarrs::array::DataType;

pub trait GeoDataset: Send + Sync {
    fn schema(&self) -> Result<Vec<(String, DataType)>, Box<dyn std::error::Error>>;

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
        buffer: &mut ChunkBuffer
    ) -> Result<bool, Box<dyn std::error::Error>>;
}
