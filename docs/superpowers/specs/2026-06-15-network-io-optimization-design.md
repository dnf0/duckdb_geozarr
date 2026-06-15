# Network I/O & Caching Optimization Design

**Date:** 2026-06-15

## Context & Motivation

Eider heavily relies on asynchronous I/O via `opendal` to fetch data from HTTP and S3 sources. However, the `zarrs` library expects a synchronous storage interface. To bridge this, Eider wraps the asynchronous operations in a synchronous wrapper (`AsyncToSyncOpendalStore`).

Currently, every synchronous call (e.g., `get`, `get_partial_values_key`, `size_key`) spawns a new OS thread and initializes a brand-new Tokio runtime to execute the asynchronous block. For a dataset with thousands of chunks, this introduces massive CPU, memory, and latency overhead, effectively bottlenecking network I/O.

## Proposed Solution: Shared Global Tokio Runtime

We will replace the per-request Tokio runtimes with a single, lazily initialized global Tokio runtime.

### 1. Global Runtime Initialization

We will use `std::sync::OnceLock` to create the global runtime exactly once per process.

```rust
use std::sync::OnceLock;

pub fn global_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("Failed to create global Tokio runtime")
    })
}
```

### 2. Optimizing `AsyncToSyncOpendalStore`

In `geozarr_core/src/store.rs`, we will refactor the `ReadableStorageTraits` implementation for `AsyncToSyncOpendalStore`. We will remove `std::thread::spawn` and the local Tokio runtime initializations. Instead, the methods will directly block on the global runtime.

```rust
// Example for `get`
fn get(&self, key: &zarrs::storage::StoreKey) -> Result<Option<bytes::Bytes>, zarrs::storage::StorageError> {
    let op = self.operator.clone();
    let key_str = key.as_str().to_string();

    global_runtime().block_on(async {
        match op.read(&key_str).await {
            Ok(bytes) => Ok(Some(bytes::Bytes::from(bytes.to_vec()))),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(zarrs::storage::StorageError::Other(e.to_string())),
        }
    })
}
```

We will apply this same pattern to `get_partial_values_key` and `size_key`.

### 3. Sweeping `store.rs`

The codebase currently spawns threads and local runtimes in other places, specifically during the initialization of remote stores (e.g., inside `resolve_sync_store` when fetching COG headers or STAC metadata). We will review `store.rs` and replace all instances of:

```rust
std::thread::spawn(move || {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { ... })
}).join().unwrap()
```

with:

```rust
global_runtime().block_on(async { ... })
```

### 4. Benefits

- **Performance:** Eliminates the overhead of creating and destroying OS threads and Tokio runtimes thousands of times per query.
- **Resource Re-use:** Allows the underlying `reqwest` client (used by `opendal`) to properly reuse TCP connections and HTTP/2 multiplexing streams, since the runtime and its associated I/O drivers live for the lifetime of the process.

## Trade-offs & Considerations

- **Blocking the current thread:** `block_on` still blocks the calling DuckDB worker thread, but this is the necessary and intended behavior when satisfying the synchronous `zarrs` trait.
- **Panic propagation:** If a panic occurs inside `block_on`, it will bubble up to the calling thread directly, which is generally preferable to silently losing the thread or dealing with `join().unwrap()` errors.
