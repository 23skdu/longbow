# High-Performance Indexing & Memory Architecture

Longbow is engineered for large-scale vector search, providing advanced quantization, hardware-aware affinity, and dynamic memory tuning to maximize efficiency and reduce TCO. Version 0.2.0 introduces several production-hardened indexing strategies and a sophisticated learned orchestration layer.

---

## 1. Vector Compression (Quantization)

Longbow offers multiple compression strategies to balance memory footprint, search recall, and ingestion throughput.

### Scalar Quantization (SQ8/SQ4)

Mapping 32-bit floats to 8-bit or 4-bit integers while preserving relative distances.

- **Recall**: ~99% retention for SQ8 with 4x memory reduction.
- **Search Speed**: Native SIMD instructions (VNNI/AVX-512/NEON) allow for massive throughput gains.

### Binary Quantization (BQ)

Extreme 1-bit quantization for extremely large-scale retrieval where memory is the primary bottleneck.

- **Search Speed**: Utilizes Popcount and XOR bitwise operations.
- **Compression**: 32x reduction from FP32.

### TurboQuant (TQ2/TQ4/TQ8)

Advanced learned quantization that optimizes the bit-distribution based on the dataset's manifold.

| Quantizer | Compression | Recall | Search Speed |
| :--- | :--- | :--- | :--- |
| **Scalar (SQ8)** | 4x | High | Fast |
| **Product (PQ)** | 4-16x | Medium | Moderate |
| **Binary (BQ)** | **32x** | Moderate | Extreme |
| **TurboQuant V2** | **8-64x** | **High-Medium** | **Fast** |

---

## 2. Adaptive Indexing (Flat to HNSW)

Longbow manages the search strategy automatically based on dataset size. Small datasets use a high-performance **Flat (Linear)** scan to avoid the indexing overhead of HNSW. As the dataset grows, the engine triggers an automated migration.

### Migration Lifecycle

1. **Detection**: Triggered when `dataset.Len()` exceeds a configurable threshold (default: 1000).
2. **Background Construction**: A background indexing pool is spawned to build the HNSW graph using available system cycles.
3. **Atomic Swap**: The `AdaptiveIndex` switcher atomically replaces the BruteForce index with HNSW once ready.
4. **Data Continuity**: New vectors added during migration are added to both indices to ensure zero data loss.

---

## 3. HNSW Engine: SlabArena & PackedAdjacency

Longbow's HNSW implementation is optimized for high-concurrency ingestion and zero-copy retrieval.

### SlabArena Allocation

To minimize GC pressure and fragmentation, Longbow uses a custom **SlabArena** system for all off-heap allocations:

- **Typed Segments**: Separate arenas for vectors, neighbors, and metadata.
- **Chunked Storage**: Vectors are stored in fixed-size chunks (default: 1024) within slabs.
- **NUMA Local**: Allocation prefers memory nodes local to the requesting CPU socket.

### PackedAdjacency Lists

Adjacency lists (neighbors) use a 2-level packed reference system:

- **Indirection**: A directory of "Pages" maps Node IDs to neighbor offsets.
- **Memory Efficiency**: Small neighbor lists are packed efficiently to avoid pointer overhead.
- **Concurrency**: Fine-grained `LockNode` spinlocks (per-node) protect adjacency updates, allowing massive parallel ingestion.

### Bulk Ingestion Pipeline (AddBatchBulk)

Version 0.2.0-rc2 introduces a high-throughput multi-phase bulk ingestion pipeline:

1. **Parallel Vector Ingestion**: Vectors are streamed into SlabArena chunks in parallel.
2. **Layer Probability Sampling**: Nodes are assigned layers according to HNSW probability distribution.
3. **Sequential/Parallel Bootstrap**: Lower layers are linked in bulk using a diversity-aware linkage strategy.
4. **Dynamic EfConstruction**: The indexing pool automatically throttles construction quality based on ingestion queue depth to maintain system responsiveness.

---

## 4. Polymorphic SIMD Dispatch

Search and indexing kernels utilize a type-agnostic **Polymorphic Dispatcher**. This system automatically selects the optimal distance kernel (Euclidean, Cosine, Dot, etc.) based on:

- **Architecture**: NEON (ARM64), AVX-512/AVX2 (AMD64), Metal (Apple GPU), CUDA (Nvidia GPU).
- **DataType**: float32, float64, float16, int8, int16, complex64, etc.
- **Vector Width**: Specialized kernels for 128, 384, 768, 1024, 1536, and 3072 dimensions.

This eliminates runtime branching in the hot loop and ensures that every CPU cycle is utilized for distance computations.

---

## 5. Specialized Indexing Modes

### Geospatial Indexing

- **Structure**: S2-based quadtree integrated with the HNSW graph.
- **Search**: Supports range searches (within X meters) and filtered vector searches.
- **Optimization**: Coordinate packing reduces memory footprint for 2D points.

### Temporal Indexing

- **Structure**: Multi-version timestamped tree.
- **Search**: Supports "As-of" queries, range-based temporal slicing, and time-windowed vector retrieval.
- **Versioning**: Transparently manages document history without impacting search latency.

### Learned Index Orchestration (IndexPredictor)

- **Automatic Selection**: Monitors query patterns and switches between Flat, HNSW, and IVF based on a k-NN predictor.
- **Ollama Integration**: Optionally utilizes local LLM models to analyze complex query semantics for optimal index selection.

### Multi-Signal Prediction (QueryFeatures)

The predictor (powered by an internal k-NN classifier) analyzes 13 distinct signals to determine the optimal index class:

- **Structural Signals**: Vector dimension, Dataset size, Number of collections.
- **Query Context**: Search K, Number of query vectors, Query complexity.
- **Data Statistics**: Average vector norm, data sparsity.
- **State Signals**: Filtering status (roaring bitmaps), Hybrid search flags.
- **AI Context**: Embedding provider (OpenAI, Cohere, Local), Model dimension ratio.

### Index Switching Lifecycle

1. **Prediction**: The system monitors latency and recall. If a threshold is breached, the k-NN predictor proposes a superior index type.
2. **Background Build**: The new index is built in the background from existing records.
3. **Atomic Swap**: The switcher atomically replaces the old index once building and training (e.g., for PQ codebooks) are complete.

---

## 5. IVF-HNSW Composite Index & Optimized PQ (OPQ)

To reach billion-scale scalability, Longbow introduces the **IVF-HNSW Composite Index**. By combining Inverted File (IVF) structures with an HNSW-based coarse quantizer, the engine reduces search space dramatically while maintaining sub-millisecond latencies.

- **HNSW Coarse Quantizer**: Centroids are indexed using HNSW for rapid Voronoi cell discovery.
- **OPQ Alignment**: Optimized Product Quantization learns an orthogonal transformation matrix to minimize quantization error.
- **GPU Training**: Metal (Apple Silicon) and CUDA (NVIDIA) kernels accelerate K-Means clustering and codebook generation.

---

## 6. DiskANN: High-Recall Disk-Optimized Index

For datasets that exceed available RAM, Longbow provides native support for **DiskANN** (Vamana algorithm).

- **Disk-First Traversal**: Integrated with `DiskIOScheduler` for asynchronous prefetching.
- **Vamana Graph**: Produces graphs with smaller diameter and higher connectivity than HNSW, optimized for SSD I/O patterns.
- **Scale**: Designed for multi-billion vector collections on a single node.

---

## 7. Scaling: Auto-Sharding

Transparently scales the HNSW index by migrating from a single monolithic graph to a partitioned architecture as the dataset grows.

- **Detection**: Triggered when `dataset.Len()` exceeds the auto-sharding threshold.
- **ShardedHNSW**: Splits the graph into multiple sub-graphs with lock-striping for higher concurrent write throughput and parallel search performance.
