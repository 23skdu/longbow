# Next Steps & Roadmap

This document tracks the remaining work for Performance Optimization and Polymorphic Vector Support.
Completed items have been verified and archived.

## Polymorphic & High-Dimensionality Support

Remaining tasks to support 12 data types and 3072 dimensions.

### 5. Final Integration & Regression

**Goal**: Address legacy debt and validate full matrix.

- **Subtasks**:
  - Fix dimension mismatches in legacy HNSW tests.
  - Final sharded/multi-node validation across all 12 types.

---

## Ingestion Performance Optimizations

Remaining high-priority optimizations for the ingestion pipeline.

### 6. Eliminate Double-Checked Locking in Text Indexing

**Problem**: `runIndexWorker()` uses double-checked locking for inverted index creation, causing contention.
**Solution**:

- Use `sync.Map` for `InvertedIndexes` to eliminate read locks.
- Pre-create inverted indexes during dataset initialization.
- Batch text indexing operations per column.
**Expected Impact**: 10-15% faster hybrid search ingestion.

### 7. Optimize Deferred Connection Updates

**Problem**: `AddBatchBulk()` collects reverse connections with 16-way sharding, but processes serially per shard.
**Solution**:

- Increase sharding from 16 to 256 for better parallelism.
- Use lock-free ring buffer for deferred updates.
- Batch multiple targets per `AddConnectionsBatch` call.
**Expected Impact**: 20-25% faster graph construction in bulk mode.

### 8. Vectorized String Column Indexing

**Problem**: Text indexing processes strings one-by-one with array bounds checks.
**Solution**:

- Batch extract strings.
- Parallelize tokenization across string chunks.
- Use SIMD for whitespace detection/splitting.
**Expected Impact**: 30-40% faster text indexing.

### 9. Lazy BM25 Index Initialization

**Problem**: BM25 index is created eagerly.
**Solution**:

- Defer creation until first hybrid search query.
- Use bloom filter to track presence of text columns.
**Expected Impact**: 20-30% lower memory for vector-only workloads.

### 10. Prefetch Neighbor Chunks During Search

**Problem**: `searchLayerForInsert()` causes cache misses.
**Solution**:

- Prefetch next 4 candidate vectors using `PREFETCHT0`.
- Group candidates by chunk ID.
**Expected Impact**: 10-15% faster search during insertion.

### 11. Batched Metric Updates

**Problem**: Metrics are updated per-job, causing atomic contention.
**Solution**:

- Accumulate metrics in thread-local counters.
- Flush to Prometheus every 100ms.
**Expected Impact**: 5-8% lower CPU overhead.

### 12. Optimistic Locking for Connection Pruning

**Problem**: `PruneConnections()` acquires exclusive lock unnecessarily.
**Solution**:

- Check connection count with atomic load before locking.
- Use RCU-style updates for neighbor lists.
**Expected Impact**: 15-20% lower lock contention.

### 13. Parallel Schema Validation

**Problem**: `ApplyDelta()` validates schema serially.
**Solution**:

- Cache validated schemas in `sync.Map`.
- Skip validation for known schema fingerprints.
- Parallelize field type checking.
**Expected Impact**: 5-10% faster ingestion.

### 14. Automated Schema Evolution & Validation

**Goal**: Fully implement robust, automated schema evolution with comprehensive unit tests as
documented in `schema_evolution.md`.

- **Subtasks**:
  - [ ] Implement automated `Version` incrementing in `ApplyDelta` upon detection of additive
        schema changes.
  - [ ] Add strict validation logic to ensure incoming `RecordBatch` schemas are backward-compatible (additive only).
  - [ ] Develop a suite of unit and integration tests covering various evolution scenarios
        (adding columns, reordering, type mismatches).
  - [ ] Implement schema fingerprinting to skip validation for identical schemas in the ingestion hot path.
**Expected Impact**: Improved data integrity and developer experience during application evolution.

---

## Performance Sprint: 800MB/s Ingest & 1.7GB/s Retrieval

Focused optimization plan to double throughput for 10k-50k vector datasets (128-dim).

### 2. SIMD-Accelerated Distance Calculations (Target: Ingest)

**Problem**: 128-dim vectors might miss optimized SIMD paths or fall back to generic implementations.
**Solution**:

- Audit `internal/simd` for `AVX2/NEON` utilization on 128-dim.
- Implement specialized `Assembly` kernels for 128-dim L2/Cosine distance.
- Force `runtime.KeepAlive` on pointers to prevent GC overhead during SIMD calls.

### 3. Bulk HNSW Insertion with Parallelism (Target: Ingest)

**Problem**: `AddBatchBulk` efficiency drops at 25k if parallelism isn't fully exploited.
**Solution**:

- Implement `ParallelFor` for `AddBatchBulk` vector insertion.
- [x] Tune `efConstruction` dynamically based on dataset size (lower for initial build, higher for refinement).
- Use `Goroutine Pool` to reduce scheduler overhead for small batches.

### 5. Lock Contention Reduction in VectorStore

**Problem**: `RWMutex` on `VectorStore` or `Dataset` might be contended during concurrent Ingest/Get.
**Solution**:

- Shard locks: Use `Partitioned Mutex` for `Dataset` map users.
- `Rcu` (Read-Copy-Update) or `Cow` (Copy-On-Write) for dataset list.

### 6. Memory Allocator Tuning (SlabArena)

**Problem**: `SlabArena` might still have overhead or fragmentation.
**Solution**:

- Implement Thread-Local Allocation Buffers (TLAB) for `SlabArena`.
- Pre-allocate larger slabs (2MB -> 4MB) for 10k+ vector batches.

### 7. Arrow RecordBatch Construction Optimization

**Problem**: Building `RecordBatch` in SDK and Server adds overhead.
**Solution**:

- Reuse `RecordBuilder` instances (sync.Pool).
- Optimize `AppendValues` to use `SetValues` with direct memory copy where possible.

### 8. Disable/Defer Auto-Compaction

**Problem**: Compaction triggering during high-load ingestion steals cycles.
**Solution**:

- Disable auto-compaction during "Bulk Load" mode.
- Trigger compaction only after ingestion idle time.

### 9. Optimize `id` Lookups in Index

**Problem**: Checking for existing IDs during insert (`seen` check) is costly.
**Solution**:

- Use `Bloom Filter` for rapid negative existence checks.
- Optimize `Bitmap` or `RoaringBitmap` for ID tracking.

### 10. GRPC & Network Tuning

**Problem**: Default GRPC settings might cap throughput.
**Solution**:

- Tune `MaxSendMsgSize` and `MaxRecvMsgSize`.
- Enable `LZ4` compression for Flight streams if network limited (CPU tradeoff).
- Increase `InitialWindowSize` for flow control.

### 11. Reference Locality in HNSW Graph

**Problem**: Random graph traversal causes L1/L2 cache misses.
**Solution**:

- Reorder graph nodes (Hilbert curve or BFS) periodically to improve locality.
- Use `Prefetch` instructions (already in plan #10, enforce for 128-dim).

### 12. Eliminate Interface Conversions

**Problem**: `arrow.Array` interface assertions in hot loops.
**Solution**:

- Use concrete types (`*array.Float32`, `*array.FixedSizeList`) in critical paths.
- Generate specialized code for `128` dim to avoid slice headers.

### 13. Tuning Garbage Collector (GOGC)

**Problem**: Frequent GC sweeps stop the world.
**Solution**:

- Set `GOGC=200` or higher during bulk ingest.
- Use `Ballast` allocation to stabilize heap size.

### 14. Efficient Bitset for Filtered Search

**Problem**: Bitset creation/iteration overhead in HNSW search.
**Solution**:

- Optimize `Bitset` intersection/union operations with SIMD.
- Cache common filter bitsets.

### 15. Profile-Guided Optimization (PGO)

**Problem**: Generic Go compile.
**Solution**:

- Collect CPU profiles from `perf_test.py`.
- Recompile Longbow server with `-pgo=default.pgo` using the profile.

### 16. Branchless Optimizations

1. **SIMD Comparisons**: Replace `if/else` in `matchInt64Generic` and `matchFloat32Generic` with branchless bitwise operations.
   - **Target**: `internal/simd/simd_baseline.go`
   - **Reason**: Branch mispredictions inside tight loops destroy throughput.
   - **Plan**: Implement branchless logic (e.g., `res = ((a ^ b) - 1) >> 63` or similar) and benchmark.
2. **HNSW Search Min/Max**: Replace conditional updates for `closest` and `closestDist` in `searchLayer`.
   - **Target**: `internal/store/arrow_hnsw_search.go`
   - **Plan**: Use arithmetic min/max or `math.Min` if inlined/intrinsics are faster (Go's `min` built-in might be branchless).

---

## Phase 7: Optimization Round 2 (Soak Test Findings)

**Source**: `soak_test.py` (15m run, 27k ops, 1.8GB RSS growth).
**Findings**: Massive memory churn in `compactRecords` (290GB allocs) and CPU hotspots in generic distance computation.

### 18. Recycled Bitset Pool for Search

- **Problem**: `bitutil.SetBit` and `make([]byte)` for `Visited` sets cause high frequent GC.
- **Solution**: Use `sync.Pool` to recycle `Visited` bitsets between search queries.
- **Impact**: 5-10% CPU reduction in high-QPS search.

### 19. Slab Defragmentation & Reclamation

- **Problem**: `SlabArena` grows indefinitely (10GB RSS) even with relatively small vector count (27k), implying fragmentation.
- **Solution**: Implement a "Copy-Collection" style defragmenter that moves live objects to new slabs and releases old pages.
- **Impact**: Reduce long-running memory footprint by 40-60%.

### 26. JEMalloc / Alternative Allocator

- **Problem**: Go GC struggles with the massive pointer graph of 10GB+ Arrows.
- **Solution**: Offload Arrow data buffers to C-heap using `jemalloc` (via CGO or specialized allocator) to hide it from Go GC.
- **Impact**: Massive reduction in GC pause times for large heaps.

### 28. Dictionary Encoding for Text Metadata

- **Problem**: Repeated text strings in metadata bloom memory usage.
- **Solution**: Automatically apply Arrow Dictionary Encoding to string columns with low cardinality.
- **Impact**: 2x-5x memory reduction for repetitive metadata.

### 30. Parallel Compaction Sharding

- **Problem**: Compaction is single-threaded per dataset.
- **Solution**: Share compaction work into non-overlapping ID ranges and run in parallel.
- **Impact**: Faster recovery and compaction catch-up.

### 31. Profile-Guided Optimization (PGO)

- **Problem**: Generic compilation misses optimization opportunities.
- **Solution**: Use the `cpu.pprof` collected from this soak test (`soak_profiles/cpu_node0_*.pprof`) to build the release binary.
- **Impact**: 5-10% global performance improvement.
