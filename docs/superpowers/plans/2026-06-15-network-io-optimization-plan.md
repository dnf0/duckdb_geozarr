# Network I/O & Caching Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate redundant `std::thread` and Tokio runtime spawns for async I/O by introducing a shared global Tokio runtime.

**Architecture:** Use `std::sync::OnceLock` to hold a static `tokio::runtime::Runtime`. Refactor `AsyncToSyncOpendalStore` and COG/STAC resolution methods in `store.rs` to use `global_runtime().block_on(...)` instead of spawning new threads and runtimes for every asynchronous call.

**Tech Stack:** Rust, Tokio, OpenDAL, `zarrs`.

---

### Task 1: Create the Global Tokio Runtime

**Files:**
- Modify: `geozarr_core/src/store.rs`

- [ ] **Step 1: Add `global_runtime` to `store.rs`**

Add this at the top of the file (e.g., after the `use` statements):

```rust
use std::sync::OnceLock;

pub fn global_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("Failed to create global Tokio runtime")
    })
}
```

- [ ] **Step 2: Commit**

```bash
git add geozarr_core/src/store.rs
git commit -m "feat: introduce global Tokio runtime for async I/O"
```

### Task 2: Optimize `AsyncToSyncOpendalStore`

**Files:**
- Modify: `geozarr_core/src/store.rs`

- [ ] **Step 1: Refactor `get` method**

Replace the current `get` method in `impl ReadableStorageTraits for AsyncToSyncOpendalStore`:

```rust
    fn get(
        &self,
        key: &zarrs::storage::StoreKey,
    ) -> Result<Option<bytes::Bytes>, zarrs::storage::StorageError> {
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

- [ ] **Step 2: Refactor `get_partial_values_key` method**

Replace the `let res = std::thread::spawn(move || ...` block in `get_partial_values_key`:

```rust
        // The object size is only required to resolve ranges measured from the
        // end (`FromEnd`, e.g. the shard index of an end-indexed sharded array)
        // or open-ended `FromStart(_, None)` reads. Skip the extra `stat` round
        // trip when every range is fully bounded from the start.
        let needs_size = ranges
            .iter()
            .any(|r| matches!(r, ByteRange::FromEnd(_, _) | ByteRange::FromStart(_, None)));

        global_runtime().block_on(async {
            // Resolve the object size once (iff any range needs it). A
            // missing object maps to `Ok(None)`, matching `get`/`size_key`.
            let size = if needs_size {
                match op.stat(&key_str).await {
                    Ok(meta) => meta.content_length(),
                    Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(zarrs::storage::StorageError::Other(e.to_string())),
                }
            } else {
                // Unused by `FromStart(_, Some(_))` resolvers; any value is fine.
                0
            };

            let mut out = Vec::with_capacity(ranges.len());
            for r in ranges {
                // Use the zarrs `ByteRange` resolvers for the [start, end)
                // half-open range, matching the crate's exact semantics
                // (notably `FromEnd` offsets measured back from `size`).
                let start = r.start(size);
                let end = r.end(size);
                match op.read_with(&key_str).range(start..end).await {
                    Ok(buf) => out.push(bytes::Bytes::from(buf.to_vec())),
                    Err(e) if e.kind() == opendal::ErrorKind::NotFound => return Ok(None),
                    Err(e) => return Err(zarrs::storage::StorageError::Other(e.to_string())),
                }
            }
            Ok(Some(out))
        })
```

- [ ] **Step 3: Refactor `size_key` method**

Replace the `let res = std::thread::spawn(move || ...` block in `size_key`:

```rust
    fn size_key(
        &self,
        key: &zarrs::storage::StoreKey,
    ) -> Result<Option<u64>, zarrs::storage::StorageError> {
        let op = self.operator.clone();
        let key_str = key.as_str().to_string();

        global_runtime().block_on(async {
            match op.stat(&key_str).await {
                Ok(meta) => Ok(Some(meta.content_length())),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(zarrs::storage::StorageError::Other(e.to_string())),
            }
        })
    }
```

- [ ] **Step 4: Run tests**

Run: `cargo test -p geozarr_core`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add geozarr_core/src/store.rs
git commit -m "perf: use global Tokio runtime in AsyncToSyncOpendalStore"
```

### Task 3: Optimize Store Resolution (`resolve_sync_store` and helpers)

**Files:**
- Modify: `geozarr_core/src/store.rs`

- [ ] **Step 1: Refactor `build_local_cog_child`**

Replace:
```rust
    let header_bytes = std::thread::spawn({
        let operator = operator.clone();
        let fname = fname.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async { operator.read_with(&fname).range(0..header_len).await })
                .map(|b| b.to_vec())
                .map_err(|e| e.to_string())
        }
    })
    .join()
    .unwrap()?;
```
With:
```rust
    let header_bytes = global_runtime()
        .block_on(async { operator.read_with(&fname).range(0..header_len).await })
        .map(|b| b.to_vec())
        .map_err(|e| e.to_string())?;
```

- [ ] **Step 2: Refactor S3 COG header read in `resolve_sync_store`**

Replace (approx line 378):
```rust
            let header_res = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async { async_op_clone.read_with(&root_str).range(0..16384).await })
                    .map_err(|e| e.to_string())
            })
            .join()
            .unwrap();
```
With:
```rust
            let header_res = global_runtime()
                .block_on(async { async_op_clone.read_with(&root_str).range(0..16384).await })
                .map_err(|e| e.to_string());
```

- [ ] **Step 3: Refactor HTTP COG header read in `resolve_sync_store`**

Replace (approx line 851):
```rust
            let header_res = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    async_op_clone
                        .read_with(&root_str_clone)
                        .range(0..16384)
                        .await
                })
                .map_err(|e| e.to_string())
            })
            .join()
            .unwrap();
```
With:
```rust
            let header_res = global_runtime()
                .block_on(async {
                    async_op_clone
                        .read_with(&root_str_clone)
                        .range(0..16384)
                        .await
                })
                .map_err(|e| e.to_string());
```

- [ ] **Step 4: Refactor local Fs COG header read in `resolve_sync_store`**

Replace (approx line 1009):
```rust
            let header_res = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    async_op_clone
                        .read_with(&fname_clone)
                        .range(0..header_len)
                        .await
                })
                .map_err(|e| e.to_string())
            })
            .join()
            .unwrap();
```
With:
```rust
            let header_res = global_runtime()
                .block_on(async {
                    async_op_clone
                        .read_with(&fname_clone)
                        .range(0..header_len)
                        .await
                })
                .map_err(|e| e.to_string());
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p geozarr_core`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add geozarr_core/src/store.rs
git commit -m "perf: use global Tokio runtime for COG header resolution"
```

### Task 4: Optimize Concurrent STAC Asset Resolution

**Files:**
- Modify: `geozarr_core/src/store.rs`

- [x] **Step 1: Refactor `resolve_sync_store` concurrent HTTP STAC item fetch**

Replace the `let built = std::thread::spawn(move || ...` block (approx line 536):

```rust
                    // Concurrent header-fetch, mirroring the single-Item arm.
                    let built = global_runtime()
                        .block_on(async {
                            let mut set = tokio::task::JoinSet::new();
                            for (name, idx, href) in jobs {
                                set.spawn(async move {
                                    let (operator, root_str) = if href.starts_with("s3://") {
                                        let bucket_and_path = href.strip_prefix("s3://").unwrap();
                                        let bucket = bucket_and_path
                                            .split('/')
                                            .next()
                                            .unwrap_or(bucket_and_path);
                                        let root =
                                            bucket_and_path.strip_prefix(bucket).unwrap_or("/");
                                        let builder = opendal::services::S3::default()
                                            .bucket(bucket)
                                            .root(root);
                                        let root_str = bucket_and_path
                                            .strip_prefix(bucket)
                                            .unwrap_or("/")
                                            .to_string();
                                        (opendal::Operator::new(builder).unwrap().finish(), root_str)
                                    } else {
                                        let (endpoint, path) = split_http_endpoint_key(&href).unwrap();
                                        let builder =
                                            opendal::services::Http::default().endpoint(&endpoint);
                                        (opendal::Operator::new(builder).unwrap().finish(), path)
                                    };
                                    let header_bytes = operator
                                        .read_with(&root_str)
                                        .range(0..16384)
                                        .await
                                        .map_err(|e| {
                                            format!(
                                                "failed to fetch COG header for item {idx} asset {name}: {e}"
                                            )
                                        })?
                                        .to_vec();
                                    let meta = crate::cog::parse_cog_metadata(&header_bytes)
                                        .map_err(|e| {
                                            format!(
                                                "failed to parse COG header for item {idx} asset {name}: {e}"
                                            )
                                        })?;
                                    let store = crate::virtual_store::VirtualCogStore::new(
                                        operator, root_str, meta,
                                    );
                                    Ok::<_, String>((name, idx, store))
                                });
                            }
                            let mut results: Vec<(
                                String,
                                usize,
                                crate::virtual_store::VirtualCogStore,
                            )> = Vec::new();
                            while let Some(res) = set.join_next().await {
                                if let Ok(item) = res {
                                    let (name, idx, store) = item?;
                                    results.push((name, idx, store?));
                                }
                            }
                            Ok::<_, String>(results)
                        })?;
```

- [x] **Step 2: Refactor `resolve_sync_store` single-Item STAC fetch**

Replace the `let children = std::thread::spawn(move || ...` block (approx line 674):

```rust
                            // Fetch headers concurrently
                            let children = global_runtime()
                                .block_on(async {
                                    let mut set = tokio::task::JoinSet::new();
                                    for (name, href) in cog_assets {
                                        set.spawn(async move {
                                            let (operator, root_str) = if href.starts_with("s3://")
                                            {
                                                let bucket_and_path =
                                                    href.strip_prefix("s3://").unwrap();
                                                let bucket = bucket_and_path
                                                    .split('/')
                                                    .next()
                                                    .unwrap_or(bucket_and_path);
                                                let root = bucket_and_path
                                                    .strip_prefix(bucket)
                                                    .unwrap_or("/");
                                                let builder = opendal::services::S3::default()
                                                    .bucket(bucket)
                                                    .root(root);
                                                let root_str = bucket_and_path
                                                    .strip_prefix(bucket)
                                                    .unwrap_or("/")
                                                    .to_string();
                                                (
                                                    opendal::Operator::new(builder)
                                                        .unwrap()
                                                        .finish(),
                                                    root_str,
                                                )
                                            } else {
                                                let (endpoint, path) =
                                                    split_http_endpoint_key(&href).unwrap();
                                                let builder = opendal::services::Http::default()
                                                    .endpoint(&endpoint);
                                                (
                                                    opendal::Operator::new(builder)
                                                        .unwrap()
                                                        .finish(),
                                                    path,
                                                )
                                            };

                                            let header_bytes = operator
                                                .read_with(&root_str)
                                                .range(0..16384)
                                                .await
                                                .unwrap_or_default()
                                                .to_vec();
                                            let meta =
                                                crate::cog::parse_cog_metadata(&header_bytes)
                                                    .unwrap_or_default();
                                            (
                                                name,
                                                crate::virtual_store::VirtualCogStore::new(
                                                    operator, root_str, meta,
                                                ),
                                            )
                                        });
                                    }

                                    let mut children_map = std::collections::HashMap::new();
                                    while let Some(res) = set.join_next().await {
                                        if let Ok((name, store)) = res {
                                            // A multi-band / unsupported child COG fails the
                                            // whole STAC open; STAC is not first-class yet.
                                            children_map.insert(name, store?);
                                        }
                                    }
                                    Ok::<_, String>(children_map)
                                })?;
```

- [x] **Step 3: Refactor `resolve_sync_store` parent-is-STAC-item COG fetch**

Replace the `let header_res = std::thread::spawn(move || ...` block (approx line 811):

```rust
                                    let header_res = global_runtime()
                                        .block_on(async {
                                            op_clone.read_with(&path_clone).range(0..16384).await
                                        })
                                        .map_err(|e| e.to_string());
```

- [x] **Step 4: Run tests**

Run: `cargo test -p geozarr_core`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add geozarr_core/src/store.rs
git commit -m "perf: use global Tokio runtime for STAC resolution"
```
