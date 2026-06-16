# Query Engine / DuckDB Integration Optimizations Design

**Goal:** Optimize the integration between Eider and DuckDB to maximize throughput when scanning geospatial datasets.

**Context:** Profiling indicates three primary bottlenecks in the handoff between Eider's Zarr decoding and DuckDB's execution engine:
1. **Sub-optimal Vector Filling:** Emitting partially filled DuckDB vectors at chunk boundaries.
2. **Mutex Contention:** High thread contention when fetching the next chunk index from the global iterator.
3. **Heap Churn:** Reallocating decode buffers for every chunk.

This design addresses all three to improve overall query engine performance.

## 1. Vector Batch Size Optimization
**Problem:** `eider_extension` yields data to DuckDB in batches. Currently, if a chunk finishes mid-vector (e.g., 500 rows left to fill a 2048-capacity vector), the table function yields the partially full vector and waits for the next function call to fetch the next chunk.
**Solution:** Modify the `table_function` state machine. When a chunk is exhausted but the DuckDB `DataChunkHandle` is not yet full, immediately pull the next Zarr chunk from the grid iterator and continue filling the *same* vector.
**Trade-offs:** Slightly more complex state machine in `dispatch_write_chunk`, but ensures DuckDB's engine always operates on maximally full vectors (`STANDARD_VECTOR_SIZE`), improving downstream pipelining.

## 2. Improving Parallelism (Batch Iterator)
**Problem:** DuckDB launches a thread pool for scanning. Each thread asks for the next chunk by locking a global `Mutex` wrapping the `GridIterator`. If chunks are small, threads spend more time contending for the lock than decoding data.
**Solution:** Update `GridIterator` to yield a batch of chunk coordinates rather than a single coordinate.
- A thread takes the lock, claims N chunks (e.g., 16), releases the lock instantly, and processes all 16 locally.
**Trade-offs:** Might cause slight load imbalance at the very tail end of a scan if one thread gets a batch of 16 while others finish, but the drastic reduction in lock contention far outweighs this.

## 3. Memory Footprint (Buffer Pooling)
**Problem:** `LocalState` drops the `ChunkBuffer` when a chunk is exhausted, and the next chunk allocates a fresh one. This causes massive heap churn and OS allocator overhead.
**Solution:** Add a `reusable_buffer` to `LocalState`. When a chunk is finished, retain its memory capacity. The next chunk read can borrow and overwrite this buffer instead of allocating anew.
**Trade-offs:** Negligible. Peak memory per thread remains the same (size of one chunk), but allocation overhead drops to zero after the first chunk.

## Acceptance Criteria
- [ ] DuckDB vectors are filled to capacity across Zarr chunk boundaries.
- [ ] Global `GridIterator` dispenses chunks in batches to worker threads.
- [ ] `LocalState` reuses chunk buffers between reads.
- [ ] `cargo bench` and `cargo test` pass with performance improvements in the query engine.
