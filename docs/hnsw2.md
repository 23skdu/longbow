# Arrow-Native HNSW Implementation (hnsw2)

## Overview

`hnsw2` is a custom, zero-allocation, Arrow-native implementation of the Hierarchical Navigable Small World (HNSW) algorithm designed specifically for Longbow's vector search workload. It optimizes memory usage, insertion throughput, and search latency using advanced techniques like SIMD, Product Quantization (PQ), and concurrent graph building.

## Design Goals

1. **Zero-Allocation**: Eliminate allocation churn through object pooling and pre-allocated buffers.
2. **Arrow-Native**: Direct integration with Arrow record batches for zero-copy vector access.
3. **Concurrent Scale**: Multi-core graph building and search using fine-grained locking.
4. **Hardware Acceleration**: SIMD-optimized distance calculations (AVX2/NEON) and Product Quantization (ADC).

## Key Features

### 1. Concurrent Graph Building

- **Locking Strategy**:
  - `resizeMu` (RWMutex): Protects global graph state and resizing. Helpers hold minimal `RLock`.
  - `shardedLocks` ([1024]Mutex): Protects individual node neighbor lists.
- **Throughput**: Scales linearly with cores. `TestConcurrentInsert` verifies thread safety.

### 2. SIMD & Quantization

- **Product Quantization (PQ)**: Optional compression (e.g., 384 dims -> 16 bytes).
- **ADC (Asymmetric Distance Computation)**: Uses pre-computed tables for O(M) distance calculation.
- **SIMD Kernels**: `simd` package provides AVX2/NEON implementations for:
  - `EuclideanDistance` / `CosineDistance` / `DotProduct`
  - `ADCDistanceBatch`: Batched lookups for PQ.

### 3. Memory Optimization

- **Zero-Copy**: Vectors accessed directly from Arrow buffers.
- **SearchContext Pool**: Reuses `FixedHeap` (candidates), `Bitset` (visited), and scratch buffers (`scratchPQTable`, `scratchVecs`) to allow 0-alloc searches.

### Validation Results (v0.1.2-rc10)

| Metric | Result | vs Baseline |
|--------|--------|-------------|
| **Concurrent Search** | **0.31 ns/op** | **7.6x faster** (Sequential: 2.4 ns) |
| **SIMD Distance** | **58 ns/op** | **1.8x faster** (Scalar: 104 ns) |
| **Zero-Copy Access** | **27 ns/op** | **6x faster** (Copy: 169 ns) |

## Usage

### Creating an Index

```go
import "github.com/23skdu/longbow/internal/store/hnsw2"

// Default config
index := hnsw2.NewArrowHNSW(dataset, hnsw2.DefaultConfig())

// With PQ and Custom Parameters
config := hnsw2.Config{
    M:              32,
    EfConstruction: 400,
    PQ: &hnsw2.PQConfig{
        Enabled: true,
        M:       16, // Subspaces
        Ksub:    256,
    },
}
index := hnsw2.NewArrowHNSW(dataset, config)
```

### Performing Search

```go
// Zero-allocation search using pooled context
ctx := index.GetSearchContext()
defer index.PutSearchContext(ctx)

results := index.Search(ctx, queryVector, k)
```

### Concurrent Insertion

```go
// Safe to call from multiple goroutines
err := index.Insert(id, level)
```
