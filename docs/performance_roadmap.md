# Longbow Deep Analysis & Performance Roadmap

**Date:** January 9, 2026
**Version:** 0.1.4

## 1. Deep Analysis: 6GB Cluster Benchmark (384 Dimensions)

Recent benchmarking of 384-dimensional vectors (typical PyTorch embedding size) on a 3-node cluster (6GB RAM/node) revealed critical insights:

### Performance Characteristics

1. **Ingestion Throughput Instability**:
    * We observed a "Performance Cliff" at ~75k vectors where `DoPut` throughput dropped to **7.07 MB/s** before recovering to **24 MB/s** at 150k.
    * **Diagnosis**: This indicates **Garbage Collection (GC) thrashing**. 384-dim vectors occupy ~1.5KB each. With overhead (HNSW graph nodes, Go interface wrappers), 75k vectors generates substantial pointer pressure. The dip corresponds to a GC cycle effectively pausing the world to reclaim memory from temporary batch allocations.

2. **Dataset Eviction Thrashing**:
    * Prior to the recent fix, the "Dataset not found" error confirmed that **Memory Backpressure** was triggering naive eviction of the *active* dataset.
    * **Implication**: The system operates near the memory red-line. For "several thousand" vector batches, the *burst* memory usage (New Alloc + Old Index + Graph overhead) momentarily exceeds `GOMEMLIMIT`, triggering aggressive, often self-destructive, cleanup.

3. **Search Latency at Scale**:
    * P95 Latency spiked to **60ms** at 25k but dropped to **17ms** at 150k.
    * **Anomaly**: Inverted latency scaling (faster at higher scale) suggests **Amortized Indexing cost** leaking into search threads or efficient caching warming up over time. It implies the initial graph construction is inefficient but the resulting structure is sound.

4. **Dimensionality Specifics (384d)**:
    * 384 is a " awkward" size. It handles 6 cache lines (64 *6 = 384 bytes? No, 384 float32s* 4 bytes = 1536 bytes = 24 cache lines).
    * It fits perfectly into 24 cache lines, which is efficient for streaming reads but "large" for random access, causing significant cache eviction of neighbor nodes during traversal.

## 2. 15 Targeted Improvements for 384-Dim PyTorch Vectors

To optimize specifically for **reading/writing/deleting datasets of several thousand 384-dim vectors at a time**, we propose the following architectural enhancements:

### Memory & Ingestion (Batch Performance)

1. **Slab Allocation for Fixed 384-Dim Vectors**
    * **Context**: We know the vector size (1536 bytes).
    * **Improvement**: Implement a custom `Arena` allocator that reserves massive "Slabs" (e.g., 2MB pages) and slots vectors linearly.
    * **Benefit**: Eliminates billions of small allocations; reduces GC pointer scan overhead to near zero for vector data.

2. **Zero-Copy `DoPut` Protocol**
    * **Context**: Currently, Arrow `DoPut` involves serialization/deserialization copies.
    * **Improvement**: Implement `Arrow Flight` optimizations to map the underlying byte buffer directly from the network frame to the in-memory store (requires using `release` callbacks effectively).
    * **Benefit**: Removes the memory bandwidth bottleneck during high-throughput ingestion.

3. **Parallel "Bulk Load" HNSW Indexing**
    * **Context**: Inserting one-by-one into HNSW is efficient for latency but suboptimal for throughput.
    * **Improvement**: For batches > 1000 vectors, use a "Parallel Bulk Insert" strategy:
        1. Compute all pairwise distances for the batch in parallel (Blocked Matrix Multiply).
        2. Update graph connectivity in a single synchronized phase.
    * **Benefit**: drastically drastically ingestion time for large batches.

4. **Native Float16 (Half-Precision) Storage**
    * **Context**: PyTorch models often output FP32, but embeddings rarely need >FP16 precision.
    * **Improvement**: Auto-detect or config option to store 384 dims as `float16` (768 bytes/vector).
    * **Benefit**: **50% RAM reduction**, 2x effective capacity, 2x cache density.

5. **Sharded Ingestion Routing**
    * **Context**: Global lock contention hurts multi-node setups.
    * **Improvement**: Client-side (or Proxy-side) deterministic routing. Split a batch of 5000 vectors into `N` sub-batches based on `Hash(ID) % ShardCount` *before* sending.
    * **Benefit**: Lock-free parallel ingestion across all cluster nodes.

### Deletion & Management

1. **LSM-Tree Style Deletions (Lazy Bitmap Merging)**
    * **Context**: "Deleting" thousands of vectors updates the tombstone bitmap repeatedly.
    * **Improvement**: Write deletes to a "Delete Buffer" (small log). Merge into the main `RoaringBitmap` only during read-time or background compaction.
    * **Benefit**: O(1) delete acknowledgement for large batches.

2. **Tombstone-Aware HNSW Repair**
    * **Context**: Deleting a node leaves a "hole" in the graph, degrading navigating.
    * **Improvement**: Background worker that scans tombstones and locally "wires around" deleted nodes (connecting neighbors to each other) without full rebuild.
    * **Benefit**: Maintains search quality (Recall) without specific re-indexing ops.

3. **Explicit "Drop Dataset" Fast Path**
    * **Context**: Deleting a whole dataset should be instant.
    * **Improvement**: Ensure `DeleteDataset` simply unlinks the pointer (RCU) and schedules async resource cleanup, rather than iterating rows.
    * **Benefit**: Instant release of resources for new workloads.

### Search & Compute (384-Dim Specifics)

1. **AVX-512 Optimized L2 Kernel (24-Loop)**
    * **Context**: 384 is divisible by 16 (AVX-512 width).
    * **Improvement**: Write a hand-tuned Assembly kernel that unrolls the loop exactly 24 times (384/16).
    * **Benefit**: Removes all loop overhead and branch prediction misses in the hot path distance calculation.

2. **Pre-Computed "Medoids" for Routing**
    * **Context**: Navigating huge graphs starts at random or fixed enter points.
    * **Improvement**: Maintain ~100 "Medoids" (centroids) for the dataset. Start search from the closest medoid.
    * **Benefit**: Skips the top ~5 layers of HNSW, jumping close to the target immediately. Optimized for clustered embedding spaces.

3. **Super-Scalar Prefetching**
    * **Context**: Visiting a node incurs a cache miss.
    * **Improvement**: When evaluating node `A`, issue `prefetch` instructions for its neighbors `N1, N2...` immediately.
    * **Benefit**: Overlaps Memory Access with ALU computation of the current distance.

### Storage & Persistence

1. **Zstd Dictionary Compression for Vectors**
    * **Context**: Embeddings from the same model share distribution characteristics.
    * **Improvement**: Train a Zstd dictionary on the first 10k vectors. Compress subsequent vectors using this dictionary for WAL storage.
    * **Benefit**: Significant reduction in disk usage and IOPS for durability.

2. **Io_uring Buffered WAL**
    * **Context**: `write()` syscalls blocking ingestion.
    * **Improvement**: Use `io_uring` to submit WAL buffers asynchronously.
    * **Benefit**: Decouples write latency from disk mechanics.

3. **Memory-Mapped "Read-Only" Mode**
    * **Context**: Some datasets are static after loading.
    * **Improvement**: "Freeze" a dataset to a compact mmap-friendly file format and `mmap` it back.
    * **Benefit**: OS manages memory; zero heap usage; dataset can be > RAM size.

4. **Adaptive HNSW Construction (Auto-M)**
    * **Context**: Fixed `M=32` is a guess.
    * **Improvement**: Dynamically adjust `M` (max connections) based on the "intrinsic dimensionality" of the first 1k vectors. 384-dim might need higher connectivity than 128.
    * **Benefit**: Optimal graph density for specific model outputs without user tuning.

## 3. Roadmap for Implementation

**Phase 1: Ingestion Efficiency (Weeks 1-2)**
* Implement #1 (Slab Allocation) and #3 (Parallel Indexing).
* *Goal*: Stabilize 150k+ ingestion rate.

**Phase 2: Compute Optimization (Weeks 3-4)**
* Implement #9 (AVX-512 Kernel) and #4 (Float16).
* *Goal*: Double search QPS.

**Phase 3: Lifecycle Management (Weeks 5-6)**
* Implement #6 (LSM Deletes) and #7 (HNSW Repair).
* *Goal*: robust "CRUD" operations at scale.
