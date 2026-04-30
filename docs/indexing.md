# High-Performance Indexing & Memory Optimization

Longbow is engineered for large-scale vector search, providing advanced quantization, hardware-aware affinity, and dynamic memory tuning to maximize efficiency and reduce TCO.

---

## 1. Vector Compression (Quantization)

Longbow offers multiple compression strategies to balance memory footprint, search recall, and ingestion throughput.

### Scalar Quantization (SQ8)

Mapping 32-bit floats to 8-bit integers while preserving relative distances.

- **Recall**: ~99% retention with 4x memory reduction.
- **Search Speed**: Native SIMD instructions (VNNI) allow for massive throughput gains.

### Binary Quantization (BQ)

Extreme 1-bit quantization for extremely large-scale retrieval where memory is the primary bottleneck.

- **Search Speed**: Utilizes Popcount and XOR bitwise operations.
- **Compression**: 32x reduction from FP32.

| Quantizer | Compression | Recall | Search Speed |
| :--- | :--- | :--- | :--- |
| **Scalar (SQ8)** | 4x | High | Fast |
| **Product (PQ)** | 4-16x | Medium | Moderate |
| **Binary (BQ)** | **32x** | Moderate | Extreme |
| **TurboQuant V1** | 8x | High | Fast |
| **TurboQuant V2** | **64x** | **Medium** | **Fast** |

---

## 2. Hardware-Aware Indexing: NUMA & CPU Pinning

On multi-socket servers, Longbow optimizes indexing performance by minimizing remote memory access (link latency).

### NUMA Topology Support

- **Detection**: Automatically discovers NUMA nodes and CPUSets via `/sys/devices/system/node` (Linux).
- **Worker Pinning**: Workers are locked to specific OS threads (`runtime.LockOSThread`) and pinned to hardware cores using `sched_setaffinity`.
- **Node-Local Allocation**: Preference is given to memory allocated on the same NUMA node as the worker thread.

> [!CAUTION]
> **Status**: Experimental. Native pinning is implemented in the core engine but may require manual activation in custom `main.go` entry points for non-standard deployments.

---

## 3. Dynamic Memory Management: GOGC Auto-Tuning

Longbow prevents Out-Of-Memory (OOM) killed processes and optimizes CPU usage by dynamically tuning the Go Garbage Collector.

### How GCTuner Works

The tuner monitors heap utilization relative to a configured **soft memory limit**:

- **Low Usage (<50%)**: Increases `GOGC` (e.g., to 500) to save CPU cycles and allow heap growth.
- **High Usage (>90%)**: Decreases `GOGC` (e.g., to 10) to force aggressive reclamation and prevent OOMs.

**Metrics**:

- `longbow_gc_tuner_target_gogc`: Currently active GCPERCENT.
- `longbow_gc_tuner_heap_utilization`: Ratio of heap-to-soft-limit.

---

---

## 4. Adaptive Indexing (Flat to HNSW)

Longbow manages the search strategy automatically based on dataset size. Small datasets use a high-performance **Flat (Linear)** scan to avoid the indexing overhead of HNSW. As the dataset grows, the engine triggers an automated migration.

### Migration Lifecycle

1. **Detection**: Triggered when `dataset.Len()` exceeds the threshold or growth acceleration is detected.
2. **Worker-Pool Construction**: A background indexing pool is spawned to build the HNSW graph using available system cycles.
3. **Memory Impact**: During migration, vectors remain searchable in the Flat index while the HNSW graph is built. This results in **temporarily higher memory usage** as both structures coexist.
4. **Zero-Downtime Cutover**: Once the HNSW graph is ready, the engine atomically swaps the search dispatcher.
5. **Resource Cleanup**: Finalization of the migration releases the Flat index's auxiliary structures to conserve memory.

---

## 5. Adaptive Learned Index (Production Hardened)

For large-scale deployments, Longbow uses a data-driven **IndexPerformancePredictor** to select the optimal ANN index type (HNSW, IVF-PQ, DiskANN) based on real-time query features and hardware characteristics.

### Multi-Signal Prediction (QueryFeatures)

The predictor analyzes 13 distinct signals to determine the optimal index class:

- **Structural Signals**: Vector dimension, Dataset size, Number of collections.
- **Query Context**: Search K, Number of query vectors, Query complexity (estimated).
- **Data Statistics**: Average vector norm (sparsity indicator).
- **State Signals**: Filtering status (roaring bitmaps), Hybrid search flags.
- **Infrastructure Context**: Time of day (load prediction), Day of week.
- **AI Context**: Embedding provider (OpenAI, Cohere, Local), Model dimension ratio.

### Index Switching Lifecycle

1. **Prediction**: The system monitors latency and recall. If a threshold is breached, the k-NN predictor proposes a superior index type (e.g., migrating from HNSW to DiskANN for better scale).
2. **Background Build**: The new index is built in the background from existing records.
3. **Memory Footprint**: During this background build, index-related **memory usage will double** for the specific dataset being migrated.
4. **Atomic Swap**: The switcher atomically replaces the old index once building and training (e.g., for PQ codebooks) are complete.
5. **Rollback**: If the new index fails to achieve performance targets, a rollback is triggered, involving another index swap.

> [!WARNING]
> **Migration Buffer**: Always ensure your environment has at least 50% memory headroom relative to your largest collection's index size to accommodate these background builds.

---

## 6. Scaling: Auto-Sharding

Transparently scales the HNSW index by migrating from a single monolithic graph to a partitioned architecture as the dataset grows.

### Migration Lifecycle

1. **Detection**: Triggered when `dataset.Len() >= LONGBOW_AUTO_SHARDING_THRESHOLD`.
2. **Dual-Index State**: A new `ShardedHNSW` index is built in the background while the old index remains searchable.
3. **Memory Spike**: During migration, **RAM usage doubles** as both indices (the monolithic one and the new sharded one) coexist in memory.
4. **Cutover**: Atomically swaps indices and releases old HNSW resources.

### Sharding Performance

- **Monolithic**: Lower overhead, but index build times degrade as the graph scales.
- **Sharded**: Higher concurrent write throughput (lock-striping across sub-graphs) and parallel scatter-gather search.

---

## 7. IVF-HNSW Composite Index & Optimized Product Quantization (OPQ)

To reach billion-scale scalability, Longbow 0.1.9 introduces the **IVF-HNSW Composite Index**. By combining Inverted File (IVF) structures with an HNSW-based coarse quantizer, the engine reduces search space dramatically while maintaining sub-millisecond latencies.

### Architecture Highlights

- **HNSW Coarse Quantization**: The IVF centroids are indexed using HNSW, meaning that finding the nearest Voronoi cells during a search is heavily optimized, preventing the bottleneck of exhaustive scalar centroid comparisons.
- **Optimized Product Quantization (OPQ)**: Instead of standard Product Quantization, Longbow implements OPQ, which learns an orthogonal transformation matrix to align the data distribution with the Cartesian product structure. This significantly minimizes quantization error and increases recall.
- **GPU-Accelerated Cluster Assignment**: To prevent OPQ training from becoming a bottleneck during indexing, Longbow uses GPU kernels (both **Metal** on Apple Silicon and **CUDA** on NVIDIA hardware) for K-Means clustering and centroid assignments.
- **Persistence**: The full OPQ encoder state, transformation matrices, and cluster mappings are fully serializable, ensuring zero-training reloads across restarts.

---

## 8. Temporal Indexing (Time-Travel Search)

Longbow introduces a dedicated **Temporal Index** for real-time aggregation and time-travel search (historical queries).

### Implementation Details

- **Temporal Tree**: A concurrent, read-optimized B-Tree style index that maps Unix nanosecond timestamps to vector IDs.
- **Tombstoning & Updates**: Updates and deletions are handled via non-blocking tombstones, preserving lock-free historical snapshots.
- **Time-Travel Operations**: Support for `SearchAsOf(timestamp)`, `SearchSlidingWindow(size)`, and `SearchRange(start, end)`.
- **Temporal Aggregation Engine**: Enables instantaneous statistical aggregations (`min`, `max`, `sum`, `mean`) over scalar metadata fields for any set of vectors within a specific time bucket.

---

## 9. Persistent Arrow-Backed Memory Mapping (MMap)

To achieve "instant-on" cold starts and support multi-billion node graphs without loading delays, Longbow 0.1.9 implements **direct memory mapping** for HNSW adjacency lists and SlabArena structures.

### Key Advantages

- **Zero-Copy Loading**: The graph structure is mapped directly from disk into the process address space using the `mmap` syscall.
- **Instant Availability**: No need to read and parse gigabytes of adjacency lists into heap objects on startup. The operating system handles page-faulting the graph data on-demand.
- **Page-Aligned Storage**: Slabs are stored with 16KB alignment (matching Apple Silicon page size) to ensure optimal kernel mapping and hardware-level isolation.
- **Shared Memory Support**: Multiple Longbow processes (or CLI tools) can map the same graph concurrently without duplicating the memory footprint.

### How to Enable

MMap persistence is automatically enabled when `DISK_ENABLED=true` is set during dataset creation. Adjacency lists are flushed to `.lbgraph` files and re-mapped upon restart.

---

## 10. Metal GPU Batch Search Optimization

Longbow 0.1.9+ implements **true batch parallelism** for Metal GPU search operations. When processing multiple query vectors, the GPU can now execute all distance computations in a single kernel dispatch.

### Batch Kernel Architecture

- **Threshold**: Enabled when `len(queries) >= 32` (justifies kernel dispatch overhead)
- **Compute L2 Distances Batch**: Single kernel computes all query×vector distances
- **Top-K Selection Batch**: Parallel top-k selection for all queries simultaneously

### Performance Impact

| Queries | Sequential (ms) | Batch (ms) | Speedup |
| :--- | :--- | :--- | :--- |
| 32 | 12.3 | 4.1 | 3.0x |
| 64 | 24.6 | 5.2 | 4.7x |
| 128 | 49.2 | 6.8 | 7.2x |

---

## 11. PQ Compression During Ingest

To maximize ingest throughput and reduce memory footprint, Longbow supports **Product Quantization compression during Add() operations**.

### Configuration

```bash
export LONGBOW_PQ_INGEST=1
```

When enabled:
1. Vectors are PQ-encoded during Add() instead of stored as full float32
2. Codes are stored directly (skipping float32 buffer)
3. ~75% memory reduction for int8 PQ codes (M=dim/8)

### Requirements

- Must call `TrainPQ(m, k)` before adding vectors
- Set `LONGBOW_PQ_INGEST=1` environment variable

### Memory Savings

| Dimension | PQ M | Full Float32 | PQ Codes | Reduction |
| :--- | :--- | :--- | :--- | :--- |
| 128 | 16 | 512 KB | 128 KB | 75% |
| 384 | 48 | 1.5 MB | 384 KB | 75% |
| 768 | 96 | 3 MB | 768 KB | 75% |
| 1024 | 128 | 4 MB | 1 MB | 75% |
| 3072 | 384 | 12 MB | 3 MB | 75% |

---

## 12. DiskANN: High-Recall Disk-Optimized Index

Longbow 0.2.0-rc1 introduces native support for **DiskANN**, a state-of-the-art graph-based index optimized for datasets that exceed available RAM.

### Vamana Graph Algorithm

DiskANN uses the **Vamana** graph construction algorithm, which produces graphs with smaller diameter and higher connectivity than standard HNSW, specifically designed to minimize disk I/O during traversal.

### Key Features

- **Disk-First Traversal**: Integrated with Longbow's `DiskIOScheduler` for asynchronous prefetching and I/O concurrency control.
- **High Density**: Supports much larger datasets than HNSW by offloading the majority of the graph structure and vector data to SSD/NVMe.
- **Configurable Performance**:
  - `MaxDegree (R)`: Controls graph density and search speed.
  - `BeamWidth (L)`: Balances search recall vs. latency.
  - `BuildThreads`: Parallelizes graph construction during background migrations.

### Performance Comparison

| Metric | HNSW | IVF-PQ | DiskANN |
| :--- | :--- | :--- | :--- |
| **Recall** | Ultra-High | Medium | **High** |
| **RAM Usage** | High | Low | **Low-Medium** |
| **Disk Usage** | Moderate | Moderate | **High** |
| **Search QPS** | Extreme | High | **Moderate-High** |
| **Scale** | Millions | Billions | **Billions** |

---

## 13. Index Factory & Pluggable Architecture

Longbow's `PluggableVectorIndex` interface allows for seamless integration of custom indexing algorithms.

- **Unified Interface**: All indices support `Search`, `Add`, `Save`, `Load`, and `ExportState`.
- **Hot-Swapping**: The `IndexSwitcher` allows for runtime algorithm migration without restart.
- **Legacy Compatibility**: Adapters ensure that new indices (like DiskANN) work with existing `AddByLocation` and `SearchVectors` call sites.
