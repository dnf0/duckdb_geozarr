# Query Engine Optimizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Maximize throughput by optimizing Eider-DuckDB integration through vector filling, iterator batching, and memory reuse.

**Architecture:** Update `GridIterator` to yield batches of chunks to reduce lock contention. Modify the `table_function` execution loop to pull the next chunk immediately if the current vector has capacity, rather than waiting for the next invocation. Reuse the underlying buffer in `LocalState` across chunk reads to avoid heap churn.

**Tech Stack:** Rust, DuckDB Extension, zarrs

---

### Task 1: Batch Iterator for `GridIterator`

**Files:**
- Modify: `geozarr_core/src/scanner.rs`

- [ ] **Step 1: Write the failing test**

Add a test for `GridIterator` batching in `geozarr_core/src/scanner.rs`:
```rust
    #[test]
    fn test_grid_iterator_batch() {
        let bounds_min = vec![0, 0];
        let bounds_max = vec![19, 19];
        let shape = vec![20, 20];
        let chunk_shape = vec![5, 5];
        let mut iter = GridIterator::new(&bounds_min, &bounds_max, &shape, &chunk_shape);
        
        let batch1 = iter.next_batch(3);
        assert_eq!(batch1.len(), 3);
        assert_eq!(batch1[0], vec![0, 0]);
        assert_eq!(batch1[1], vec![0, 1]);
        assert_eq!(batch1[2], vec![0, 2]);

        let batch2 = iter.next_batch(100); // More than remaining
        assert_eq!(batch2.len(), 13);
        assert_eq!(batch2.last().unwrap(), &vec![3, 3]);

        let batch3 = iter.next_batch(3);
        assert!(batch3.is_empty());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p geozarr_core test_grid_iterator_batch`
Expected: FAIL due to missing method `next_batch`.

- [ ] **Step 3: Write minimal implementation**

In `geozarr_core/src/scanner.rs`, add the `next_batch` method to `GridIterator`:

```rust
impl GridIterator {
    // ... existing new() method ...

    pub fn next_batch(&mut self, batch_size: usize) -> Vec<Vec<u64>> {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(item) = self.next() {
                batch.push(item);
            } else {
                break;
            }
        }
        batch
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p geozarr_core test_grid_iterator_batch`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geozarr_core/src/scanner.rs
git commit -m "feat: add next_batch to GridIterator"
```

### Task 2: Implement LocalState Memory Pooling and Grid Batching

**Files:**
- Modify: `extension/src/table_function.rs`

- [ ] **Step 1: Modify `LocalState` to support grid batching and memory pooling**

In `extension/src/table_function.rs`, modify `LocalState`:

```rust
pub struct LocalState {
    pub assigned_grids: Vec<Vec<u64>>,
    pub current_chunk_buffer: Option<ChunkBuffer>,
    pub reusable_buffer: Option<ChunkBuffer>,
    pub projected_columns: Vec<usize>,
    /// Cursor into `current_chunk_buffer` (which holds only the valid subset elements).
    pub element_cursor: usize,
    /// Subset info for coordinate reconstruction.
    pub subset_info: Option<geozarr_core::scanner::SubsetInfo>,
}
```

- [ ] **Step 2: Initialize updated `LocalState`**

In `ReadGeoVTab::func` inside `extension/src/table_function.rs`, update the initialization block:

```rust
            if let Some(state) = local_states.remove(&thread_id) {
                state
            } else {
                LocalState {
                    assigned_grids: vec![],
                    current_chunk_buffer: None,
                    reusable_buffer: None,
                    projected_columns: init_data.projected_columns.clone(),
                    element_cursor: 0,
                    subset_info: None,
                }
            }
```

- [ ] **Step 3: Update `dispatch_write_chunk` signature**

Because we are changing `LocalState`, we must make sure `cargo check` passes later. We will fix the main state machine loop in `write_chunk_unified` in the next task.
For now, verify compilation in `table_function.rs`.

Run: `cargo check -p eider_extension`
Expected: FAIL since `write_chunk_unified` still looks for `local_state.assigned_grid`. This is expected. Proceed to Task 3.

### Task 3: Vector Batching State Machine

**Files:**
- Modify: `extension/src/vector_writer.rs`

- [ ] **Step 1: Modify `write_chunk_unified`**

In `extension/src/vector_writer.rs`, locate `write_chunk_unified` and update the outer loop:

Replace:
```rust
        if local_state.current_chunk_buffer.is_none() {
            let mut g_state = global_state
                .lock()
                .map_err(|e| format!("Mutex poisoned: {}", e))?;

            let assigned_grid = g_state.grid_iterator.next();
            drop(g_state);

            let assigned_grid = match assigned_grid {
                Some(grid) => grid,
                None => break,
            };
            local_state.assigned_grid = assigned_grid.clone();
```
With:
```rust
        if local_state.current_chunk_buffer.is_none() {
            if local_state.assigned_grids.is_empty() {
                let mut g_state = global_state
                    .lock()
                    .map_err(|e| format!("Mutex poisoned: {}", e))?;
                
                // Fetch a batch of 16 chunks
                local_state.assigned_grids = g_state.grid_iterator.next_batch(16);
                drop(g_state);
                
                // Reverse it so we can `pop` from the back efficiently
                local_state.assigned_grids.reverse();
            }

            let assigned_grid = match local_state.assigned_grids.pop() {
                Some(grid) => grid,
                None => break, // No chunks left in batch, and global iterator is exhausted
            };
```

- [ ] **Step 2: Enable Vector Continuation**

In the same file, locate the end of the loop:
```rust
        valid_rows += batch_size;
        local_state.element_cursor += batch_size;
        if local_state.element_cursor >= total {
            local_state.current_chunk_buffer = None;
        }

        if valid_rows >= 2048 {
            break;
        }
```

Change it to:
```rust
        valid_rows += batch_size;
        local_state.element_cursor += batch_size;
        
        if local_state.element_cursor >= total {
            // Memory pooling: reclaim the buffer instead of dropping it
            local_state.reusable_buffer = local_state.current_chunk_buffer.take();
        }

        if valid_rows >= 2048 {
            break; // Vector is full, yield to DuckDB
        }
        // If we reach here, valid_rows < 2048 and current_chunk_buffer is None.
        // The loop continues, immediately popping the next chunk and filling the rest of the vector.
```

- [ ] **Step 3: Verify it compiles and passes tests**

Run: `cargo test -p eider_extension`
Expected: PASS

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add extension/src/table_function.rs extension/src/vector_writer.rs
git commit -m "perf: query engine batching, thread pooling, and vector capacity optimization"
```
