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
| **TurboQuant** | **6-8x** | **High** | **Fast** |

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
1.  **Detection**: Triggered when `dataset.Len()` exceeds the threshold or growth acceleration is detected.
2.  **Worker-Pool Construction**: A background indexing pool is spawned to build the HNSW graph using available system cycles.
3.  **Zero-Downtime Cutover**: The search path stays hot on the Flat index while the graph builds. Once complete, the engine atomically swaps the search dispatcher.
4.  **Resource Cleanup**: Finalization of the migration releases the Flat index's auxiliary structures to conserve memory.

---

## 5. Scaling: Auto-Sharding

Transparently scales the HNSW index by migrating from a single monolithic graph to a partitioned architecture as the dataset grows.

### Migration Lifecycle
1.  **Detection**: Triggered when `dataset.Len() >= LONGBOW_AUTO_SHARDING_THRESHOLD`.
2.  **Dual-Index State**: A new `ShardedHNSW` index is built in the background while the old index remains searchable.
3.  **Memory Spike**: During migration, **RAM usage doubles** as both indices coexist in memory.
4.  **Cutover**: Atomically swaps indices and releases old HNSW resources.

### Sharding Performance
- **Monolithic**: Lower overhead, but index build times degrade as the graph scales.
- **Sharded**: Higher concurrent write throughput (lock-striping across sub-graphs) and parallel scatter-gather search.
