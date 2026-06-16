# Coordinate Vectorization Optimization — Design Spec

## Architecture & Approach
We will optimize the loop inside `extension/src/vector_writer.rs` where we generate `lat`/`lon` values for DuckDB. Currently, it uses branchless modulo arithmetic (`pos % stride`) to determine which coordinate value applies to which row in the batch.

However, modulo (`%`) and integer division (`/`) are notorious for defeating LLVM's auto-vectorization (SIMD) passes because they are expensive hardware instructions.

Our strategy is to **strength-reduce** the loop:
1. We will split the processing into two phases: first, we'll calculate how many elements share the same coordinate value (`stride` elements), and then we'll use a fast inner loop to fill the output slice with that constant value.
2. We'll use `slice::fill()` for the inner loop, which standard libraries heavily optimize (often mapping down to `memset` or native SIMD instructions).
3. We'll benchmark using `cargo bench --bench coordinate_bench` to measure the exact microsecond improvement.

## Code Changes
- **Modify `extension/src/vector_writer.rs`**: Rewrite `populate_coordinate_batch_f64` (and `_i64` if applicable) to avoid per-element modulo math where possible, relying instead on slice fills.
- **Testing**: No new tests are strictly necessary since we have existing test coverage (`test_populate_coordinate_batch_f64`) and benchmarks, but we will ensure tests continue to pass.
