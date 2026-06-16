#![allow(clippy::type_complexity)]

use crate::table_function::{GlobalState, LocalState, ReadGeoBindData};
use duckdb::core::DataChunkHandle;
use geozarr_core::types::ChunkBuffer;
use std::sync::Mutex;
use zarrs::array::ElementOwned;

pub trait FillValueCmp {
    fn is_fill_value(&self, fill_bytes: &[u8]) -> bool;
}

macro_rules! impl_fill_value_cmp {
    ($t:ty) => {
        impl FillValueCmp for $t {
            fn is_fill_value(&self, fill_bytes: &[u8]) -> bool {
                self.to_ne_bytes().as_ref() == fill_bytes
            }
        }
    };
}

impl_fill_value_cmp!(f32);
impl_fill_value_cmp!(f64);
impl_fill_value_cmp!(i8);
impl_fill_value_cmp!(i16);
impl_fill_value_cmp!(i32);
impl_fill_value_cmp!(i64);
impl_fill_value_cmp!(u8);
impl_fill_value_cmp!(u16);
impl_fill_value_cmp!(u32);
impl_fill_value_cmp!(u64);

impl FillValueCmp for bool {
    fn is_fill_value(&self, fill_bytes: &[u8]) -> bool {
        let b = if *self { 1u8 } else { 0u8 };
        [b].as_ref() == fill_bytes
    }
}

#[allow(clippy::too_many_arguments)]
pub fn populate_coordinate_batch_f64(
    batch_size: usize,
    cursor: usize,
    subset_info: &geozarr_core::scanner::SubsetInfo,
    dim: usize,
    coords: Option<&Vec<f64>>,
    is_0_360: bool,
    transform: Option<&geozarr_core::metadata::SpatialTransform>,
    out_slice: &mut [f64],
) {
    let stride = subset_info.strides[dim];
    let shape = subset_info.shape[dim];
    let start = subset_info.global_starts[dim];

    let pos = cursor as u64;
    let mut current_mod = (pos / stride) % shape;
    let mut step_in_stride = pos % stride;

    let mut i = 0; // Index into out_slice
    while i < batch_size {
        // Calculate how many elements we can fill with the current g_idx
        // The `g_idx` value (and thus the `val` derived from it) is constant for `stride` consecutive elements.
        let remaining_in_current_stride = stride - step_in_stride;
        let count_to_fill = (remaining_in_current_stride as usize).min(batch_size - i);

        // This check is a safeguard. `count_to_fill` should only be zero if `batch_size - i` is zero,
        // which means the outer `while` loop condition `i < batch_size` is already false.
        if count_to_fill == 0 {
            break;
        }

        let g_idx = start + current_mod;

        if let Some(coord_vals) = coords {
            let raw = coord_vals.get(g_idx as usize).copied().unwrap_or(f64::NAN);
            let val = geozarr_core::coordinates::normalize_longitude(raw, is_0_360);
            out_slice[i..(i + count_to_fill)].fill(val);
        } else if let Some(transform) = transform {
            let val = geozarr_core::coordinates::apply_transform(transform, dim, g_idx);
            out_slice[i..(i + count_to_fill)].fill(val);
        }

        i += count_to_fill;
        step_in_stride += count_to_fill as u64;

        // Use branchless arithmetic to update step_in_stride.
        // `step_in_stride` can only be exactly `stride` or less due to `count_to_fill` calculation.
        let increment_mod = (step_in_stride == stride) as u64;
        step_in_stride -= increment_mod * stride;
        current_mod = (current_mod + increment_mod) % shape;
    }
}

pub fn populate_coordinate_batch_i64(
    batch_size: usize,
    cursor: usize,
    subset_info: &geozarr_core::scanner::SubsetInfo,
    dim: usize,
    out_slice: &mut [i64],
) {
    let stride = subset_info.strides[dim];
    let shape = subset_info.shape[dim];
    let start = subset_info.global_starts[dim];

    let pos = cursor as u64;
    let mut current_mod = (pos / stride) % shape;
    let mut step_in_stride = pos % stride;

    let mut i = 0;
    while i < batch_size {
        // Calculate how many elements we can fill with the current g_idx
        // The `g_idx` value is constant for `stride` consecutive elements.
        let remaining_in_current_stride = stride - step_in_stride;
        let count_to_fill = (remaining_in_current_stride as usize).min(batch_size - i);

        if count_to_fill == 0 {
            break;
        }

        let g_idx = start + current_mod;
        let val = g_idx as i64;
        out_slice[i..(i + count_to_fill)].fill(val);

        i += count_to_fill;
        step_in_stride += count_to_fill as u64;

        // If step_in_stride has reached `stride`, it means we completed a full stride segment.
        // Reset `step_in_stride` and advance `current_mod`.
        if step_in_stride == stride {
            step_in_stride = 0;
            current_mod = (current_mod + 1) % shape;
        }
    }
}

pub fn write_chunk_unified<T, Extract, Insert>(
    extract: Extract,
    wrap: fn(Vec<T>) -> ChunkBuffer,
    mut insert_value: Insert,
    output: &mut DataChunkHandle,
    local_state: &mut LocalState,
    global_state: &Mutex<GlobalState>,
    bind_data: &ReadGeoBindData,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: ElementOwned + Clone,
    Extract: Fn(&ChunkBuffer) -> Result<&Vec<T>, Box<dyn std::error::Error>>,
    Insert: FnMut(
        &mut DataChunkHandle,
        usize,
        usize,
        usize,
        &Vec<T>,
        &ReadGeoBindData,
    ) -> Result<(), Box<dyn std::error::Error>>,
{
    let rank = bind_data.shape.len();
    let mut valid_rows = 0;

    loop {
        if local_state.current_chunk_buffer.is_none() {
            let assigned_grid = if let Some(idx) = local_state.grid_batch.pop() {
                Some(idx)
            } else {
                let mut g_state = global_state
                    .lock()
                    .map_err(|e| format!("Mutex poisoned: {}", e))?;

                let mut next_batch = g_state.grid_iterator.next_batch(16);
                if next_batch.is_empty() {
                    None
                } else {
                    next_batch.reverse();
                    let idx = next_batch.pop().unwrap();
                    local_state.grid_batch = next_batch;
                    Some(idx)
                }
            };

            let assigned_grid = match assigned_grid {
                Some(grid) => grid,
                None => break,
            };
            local_state.assigned_grid = assigned_grid.clone();

            let chunk_reader = geozarr_core::scanner::ChunkReader::new(
                bind_data.array.clone(),
                bind_data.is_remote,
                bind_data.shape.clone(),
                bind_data.chunk_shape.clone(),
            );

            let (elements, subset_info) = chunk_reader
                .read_chunk_subset::<T>(
                    &assigned_grid,
                    &bind_data.bounds_min,
                    &bind_data.bounds_max,
                )
                .map_err(|e| format!("zarrs read error: {}", e))?;

            if elements.is_empty() {
                continue;
            }

            local_state.current_chunk_buffer = Some(wrap(elements));
            local_state.subset_info = Some(subset_info);
            local_state.element_cursor = 0;
        }

        let buffer = extract(local_state.current_chunk_buffer.as_ref().unwrap())?;

        let total = buffer.len();
        let batch_size = (total - local_state.element_cursor).min(2048 - valid_rows);

        let subset_info = local_state.subset_info.as_ref().unwrap();

        for dim in 0..rank {
            if local_state.projected_columns.contains(&dim) {
                if let Some(coord_vals) = bind_data.coords.get(&bind_data.dim_names[dim]) {
                    let is_0_360 = bind_data.lon_0_360_dims.contains(&dim);
                    let mut coord_vector = output.flat_vector(dim);
                    let coord_slice = coord_vector.as_mut_slice::<f64>();
                    populate_coordinate_batch_f64(
                        batch_size,
                        local_state.element_cursor,
                        subset_info,
                        dim,
                        Some(coord_vals),
                        is_0_360,
                        None,
                        &mut coord_slice[valid_rows..valid_rows + batch_size],
                    );
                } else if let Some(ref transform) = bind_data.spatial_transform {
                    if dim < transform.scale.len() {
                        let mut coord_vector = output.flat_vector(dim);
                        let coord_slice = coord_vector.as_mut_slice::<f64>();
                        populate_coordinate_batch_f64(
                            batch_size,
                            local_state.element_cursor,
                            subset_info,
                            dim,
                            None,
                            false,
                            Some(transform),
                            &mut coord_slice[valid_rows..valid_rows + batch_size],
                        );
                    } else {
                        let mut coord_vector = output.flat_vector(dim);
                        let coord_slice = coord_vector.as_mut_slice::<i64>();
                        populate_coordinate_batch_i64(
                            batch_size,
                            local_state.element_cursor,
                            subset_info,
                            dim,
                            &mut coord_slice[valid_rows..valid_rows + batch_size],
                        );
                    }
                } else {
                    let mut coord_vector = output.flat_vector(dim);
                    let coord_slice = coord_vector.as_mut_slice::<i64>();
                    populate_coordinate_batch_i64(
                        batch_size,
                        local_state.element_cursor,
                        subset_info,
                        dim,
                        &mut coord_slice[valid_rows..valid_rows + batch_size],
                    );
                }
            }
        }

        if local_state.projected_columns.contains(&rank) {
            insert_value(
                output,
                valid_rows,
                batch_size,
                local_state.element_cursor,
                buffer,
                bind_data,
            )?;
        }

        valid_rows += batch_size;
        local_state.element_cursor += batch_size;
        if local_state.element_cursor >= total {
            // Note: zarrs retrieve_chunk_subset_elements allocates a Vec internally.
            // For now, we will simply set state.buffer_pool = Some(vec![]) at the end
            // of this buffer, even though zarrs doesn't let us reuse the allocation directly yet.
            // But setting up the state tracking is the goal for this task.
            local_state.buffer_pool = Some(vec![]);
            local_state.current_chunk_buffer = None;
        }

        if valid_rows >= 2048 {
            break;
        }
    }

    output.set_len(valid_rows);
    Ok(())
}

#[macro_export]
macro_rules! dispatch_write_chunk {
    (String, $enum_variant:path, $output:expr, $local_state:expr, $global_state:expr, $bind_data:expr) => {{
        use duckdb::core::Inserter;
        $crate::vector_writer::write_chunk_unified::<String, _, _>(
            |buf| match buf {
                $enum_variant(v) => Ok(v),
                _ => Err("Chunk buffer type mismatch".into()),
            },
            |v| $enum_variant(v),
            |output, valid_rows, batch_size, cursor, buffer, bind_data| {
                let rank = bind_data.shape.len();
                let mut value_vector = output.flat_vector(rank);
                for i in 0..batch_size {
                    let val = buffer
                        .get(cursor + i)
                        .ok_or("Malformed Zarr chunk: unexpected buffer size")?;
                    if Some(val.as_bytes()) == bind_data.fill_value_bytes.as_deref() {
                        value_vector.set_null(valid_rows + i);
                    } else {
                        value_vector.insert(valid_rows + i, val.as_str());
                    }
                }
                Ok(())
            },
            $output,
            $local_state,
            $global_state,
            $bind_data,
        )
    }};
    ($rust_type:ty, $enum_variant:path, $output:expr, $local_state:expr, $global_state:expr, $bind_data:expr) => {{
        $crate::vector_writer::write_chunk_unified::<$rust_type, _, _>(
            |buf| match buf {
                $enum_variant(v) => Ok(v),
                _ => Err("Chunk buffer type mismatch".into()),
            },
            |v| $enum_variant(v),
            |output, valid_rows, batch_size, cursor, buffer, bind_data| {
                let rank = bind_data.shape.len();
                let fill_bytes_slice = bind_data.fill_value_bytes.as_deref().unwrap_or_default();
                let mut value_vector = output.flat_vector(rank);
                let value_slice = value_vector.as_mut_slice::<$rust_type>();
                for i in 0..batch_size {
                    let val = buffer
                        .get(cursor + i)
                        .ok_or("Malformed Zarr chunk: unexpected buffer size")?;
                    value_slice[valid_rows + i] = *val;
                }
                for i in 0..batch_size {
                    let val = buffer.get(cursor + i).unwrap();
                    if $crate::vector_writer::FillValueCmp::is_fill_value(val, fill_bytes_slice) {
                        value_vector.set_null(valid_rows + i);
                    }
                }
                Ok(())
            },
            $output,
            $local_state,
            $global_state,
            $bind_data,
        )
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use geozarr_core::scanner::SubsetInfo;

    #[test]
    fn test_populate_coordinate_batch_f64() {
        let subset_info = SubsetInfo {
            global_starts: vec![0, 0],
            shape: vec![100, 100],
            strides: vec![100, 1],
        };
        let mut out = [0.0; 2];
        let coords = vec![100.0, 101.0, 102.0];

        populate_coordinate_batch_f64(
            2,
            0,
            &subset_info,
            1,
            Some(&coords),
            false,
            None,
            &mut out[..],
        );

        assert_eq!(out[0], 100.0);
        assert_eq!(out[1], 101.0);
    }
}
