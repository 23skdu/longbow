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

TurboQuant is Longbow's proprietary two-stage vector compression algorithm that delivers 4x-64x storage reduction for float32 vectors while maintaining fast approximate nearest neighbor search capabilities. It combines Polar Quantization with Quantized JL (QJL) transformations to achieve extreme compression with tunable accuracy.

#### Core Algorithm

TurboQuant implements a two-stage compression pipeline:

1. **Random Rotation (Hadamard Transform)**: Vectors are randomly rotated using a Walsh-Hadamard transform to decorrelate dimensions for effective quantization.

2. **Stage 1 - Recursive PolarQuant**: The rotated vector is converted to polar coordinates:
   - 1 radius value (float32)
   - (pow2-1) angles that are bit-packed (2, 3, 4, or 8 bits per angle)

3. **Stage 2 - QJL Correction**: A Quantized JL correction term that stores the sign bit of the reconstruction residual for improved accuracy.

4. **Packing Format**: `[Radius (4B)][Packed Angles (Variable)][QJL Bits (Variable)]`

#### Compression Ratios

| Original Dims | Bits per Angle | Original Size | TQ Size | Compression |
|--------------|--------------|--------------|---------|--------------|
| 128 | 4-bit | 512 bytes | ~128 bytes | **4x** |
| 384 | 4-bit | 1536 bytes | ~288 bytes | **5.3x** |
| 768 | 3-bit | 3072 bytes | ~385 bytes | **8x** |
| 768 | 2-bit | 3072 bytes | ~256 bytes | **12x** |
| 768 | 8-bit | 3072 bytes | ~640 bytes | **4.8x** |

#### Features

- **Configurable bit depth**: 2, 3, 4, or 8 bits per angle
- **Automatic power-of-2 padding** for dimensions not a power of 2
- **Lossy compression** with tunable accuracy vs. storage trade-off
- **Dimensions supported**: 128 to 3072 (non-power-of-2 dims like 384, 768 work correctly; the SIMD kernel truncates query vectors to the original dimension length, not the padded power-of-2 length)
- **HNSW index support** for fast approximate k-NN search
- **CPU SIMD acceleration** using AVX2/NEON
- **GPU (CUDA) kernels** for accelerated distance computation
- **Distance metrics**: L2, Cosine supported

#### Auto-Tuning

The **QuantizationTuner** automatically selects between float32/int8/PQ/TQ based on:

- Memory pressure
- Query load (QPS)
- Recall requirements

Adaptive re-quantization is supported for live datasets.

#### Supported Data Types

| String | Alias |
|--------|-------|
| `"turboquant"` | `"tq"` |
| `"turboquant2"` | - |
| `"turboquant4"` | - |
| `"turboquant8"` | - |

#### Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `vector_type` | string | - | Set to `"turboquant"` or `"tq"` |
| `turboquant_bits` | int | 4 | Bits per angle (2, 4, or 8) |
| `dimension` | int | - | Vector dimensions (128-3072) |
| `metric` | string | `"cosine"` | Distance metric |

#### Usage

Python SDK:

```python
client.create_dataset(
    name="my_dataset",
    dimensions=768,
    vector_type="turboquant",
    turboquant_bits=4,
    metric="cosine"
)
```

Arrow Flight Action:

```json
{
  "name": "my_dataset",
  "dimension": 768,
  "vector_type": "turboquant",
  "turboquant_bits": 4,
  "metric": "cosine"
}
```

CLI:

```bash
longbow-cli create-namespace -name my_ns -dims 768 -data_type turboquant
```

#### Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `longbow_turboquant_encoding_total` | Counter | `dataset`, `direction` | Encoding operations |
| `longbow_turboquant_encoding_latency_seconds` | Histogram | `dataset` | Server encoding latency |
| `longbow_turboquant_storage_bytes_total` | Gauge | `dataset` | Storage bytes used |
| `longbow_turboquant_search_total` | Counter | `dataset`, `bit_width` | Search count |
| `longbow_turboquant_search_latency_seconds` | Histogram | `dataset`, `bit_width` | Search latency |

#### Architecture

```
┌─────────────────────────────────────┐
│         CLIENT SDK                  │
│  create_dataset(turboquant_bits=4)  │
└─────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│    ARROW FLIGHT ACTION               │
│  store_actions.go: create_dataset   │
└─────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│     DATASET CREATION                 │
│  Stores TQ config in metadata       │
└─────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│       HNSW INDEX                    │
│  Uses TurboQuantCompute             │
└─────────────────────────────────────┘
        │                   │
        ▼                   ▼
┌──────────────┐    ┌──────────────┐
│  ENCODING    │    │   SEARCH     │
│  (Ingest)    │    │   (Query)    │
└──────────────┘    └──────────────┘
        │                   │
        ▼                   ▼
┌──────────────┐    ┌──────────────┐
│  Encode()    │    │  CPU SIMD    │
│  Pack()     │    │  CUDA Kernel │
└──────────────┘    └──────────────┘
```

#### File Reference

| File | Purpose |
|------|---------|
| `internal/store/internal/core/turboquant.go` | Primary encoder/decoder |
| `internal/store/internal/core/arrow_hnsw_compute_tq.go` | HNSW TQ compute |
| `internal/store/turboquant_storage.go` | Storage constants/helpers |
| `internal/gpu/cuda/kernels.cu` | CUDA distance kernel |
| `internal/store/quantization_tuner.go` | Auto-tuner |
| `internal/metrics/storage_metrics.go` | Prometheus metrics |

### Quantization Summary

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

## 6. IVF-HNSW Composite Index & Optimized PQ (OPQ)

To reach billion-scale scalability, Longbow introduces the **IVF-HNSW Composite Index**. By combining Inverted File (IVF) structures with an HNSW-based coarse quantizer, the engine reduces search space dramatically while maintaining sub-millisecond latencies.

- **HNSW Coarse Quantizer**: Centroids are indexed using HNSW for rapid Voronoi cell discovery.
- **OPQ Alignment**: Optimized Product Quantization learns an orthogonal transformation matrix to minimize quantization error.
- **GPU Training**: Metal (Apple Silicon) and CUDA (NVIDIA) kernels accelerate K-Means clustering and codebook generation.

---

## 7. DiskANN: High-Recall Disk-Optimized Index

For datasets that exceed available RAM, Longbow provides native support for **DiskANN** (Vamana algorithm).

- **Disk-First Traversal**: Integrated with `DiskIOScheduler` for asynchronous prefetching.
- **Vamana Graph**: Produces graphs with smaller diameter and higher connectivity than HNSW, optimized for SSD I/O patterns.
- **Scale**: Designed for multi-billion vector collections on a single node.

---

## 8. Scaling: Auto-Sharding

Transparently scales the HNSW index by migrating from a single monolithic graph to a partitioned architecture as the dataset grows.

- **Detection**: Triggered when `dataset.Len()` exceeds the auto-sharding threshold.
- **ShardedHNSW**: Splits the graph into multiple sub-graphs with lock-striping for higher concurrent write throughput and parallel search performance.
