# Arrow-Native HNSW Implementation (hnsw2)

## Overview

`hnsw2` is a custom, zero-allocation, Arrow-native implementation of the Hierarchical Navigable Small World (HNSW) algorithm designed specifically for Longbow's vector search workload. It replaces the `github.com/coder/hnsw` library to achieve significant memory and performance improvements.

## Motivation

The original `coder/hnsw` library accounted for **77% of total allocations** (50GB out of 65GB during a 5-minute soak test), causing:

- High memory pressure and GC overhead
- RSS bloat (11GB stable, 11.6GB peak)
- Allocation churn impacting search latency

## Design Goals

1. **Zero-Allocation**: Eliminate allocation churn through object pooling and pre-allocated buffers
2. **Arrow-Native**: Direct integration with Arrow record batches for zero-copy vector access
3. **High Performance**: 90% reduction in allocations, 60% reduction in RSS
4. **Correctness-First**: Extensive validation via property-based tests, fuzzing, and recall validation

## Architecture

### Core Components

#### 1. Data Structures

**Bitset** (`bitset.go`)

- Compact visited tracking for graph traversal
- Zero-allocation design using fixed-size bit array
- O(1) set/check operations

**FixedHeap** (`heap.go`)

- Fixed-capacity min-heap for candidate management
- Pre-allocated array, no dynamic allocations
- Used for priority queue during search

**GraphNode** (`graph.go`)

```go
type GraphNode struct {
    ID    uint32
    Level uint8
    Neighbors [MaxLayers][MaxNeighbors]uint32  // Fixed-size arrays
    NeighborCounts [MaxLayers]uint8
}
```

- Fixed-size neighbor arrays (no allocations on updates)
- Supports up to 16 layers, 32 neighbors per layer

**ArrowHNSW** (`graph.go`)

- Main index container with Arrow integration
- Pooled search contexts for zero-allocation searches
- RWMutex for concurrent access

#### 2. Arrow Integration

**Zero-Copy Vector Access**

- Vectors stored in Arrow `FixedSizeList` arrays
- Graph nodes reference `(BatchIdx, RowIdx)` instead of copying vectors
- Distance calculations use Arrow buffer pointers directly

**Memory Layout**

```
Arrow Records (Dataset)
    ↓
(BatchIdx, RowIdx) ← GraphNode references
    ↓
Zero-copy vector access for distance calculations
```

#### 3. Object Pooling

**SearchContextPool**

```go
type SearchContext struct {
    candidates *FixedHeap    // Pre-allocated priority queue
    visited    *Bitset       // Reusable bitset
    results    []SearchResult // Pre-allocated result buffer
}
```

- Eliminates per-search allocations
- Automatic cleanup and reuse via `sync.Pool`

## Configuration

```go
type Config struct {
    M              int     // Neighbors per layer (default: 16)
    MMax           int     // Max neighbors layer 0 (default: 32)
    MMax0          int     // Max neighbors higher layers (default: 16)
    EfConstruction int     // Construction search depth (default: 200)
    Ml             float64 // Level multiplier (default: 1/ln(2))
}
```

## Validation Strategy

### 1. Property-Based Testing

Using `github.com/leanovate/gopter`:

**Bitset Properties**

- Setting a bit makes it set
- Clear resets all bits

**FixedHeap Properties**

- Pop returns elements in sorted order
- Len matches number of pushes

**Graph Properties** (to be implemented)

- Insertion order independence
- Graph connectivity
- Neighbor count constraints

### 2. Fuzzing

Go 1.18+ native fuzzing:

- Random query vectors
- Random k values
- Edge cases (empty index, k=0, k>size)
- Corpus stored in `testdata/fuzz/`

### 3. Dual-Index Validation

**DualIndexHarness** (`validation_test.go`)

- Runs same operations on both `coder/hnsw` and `hnsw2`
- Measures recall@k to ensure correctness
- Target: >99.5% recall match

## Performance Targets

| Metric | Current (coder/hnsw) | Target (hnsw2) | Improvement |
|--------|---------------------|----------------|-------------|
| Allocation Churn | 65 GB/5min | <7 GB/5min | 90% ↓ |
| RSS Stable | 11 GB | <4 GB | 64% ↓ |
| RSS Peak | 11.6 GB | <5 GB | 57% ↓ |
| Search Latency (p99) | 10 ms | <8 ms | 20% ↓ |
| Insert Throughput | 50k/s | >60k/s | 20% ↑ |

## Implementation Status

### Phase 1: Foundation ✅ (Complete)

- [x] Core data structures (Bitset, FixedHeap, GraphNode)
- [x] ArrowHNSW container
- [x] Property-based test framework
- [x] Dual-index validation harness
- [x] Fuzzing infrastructure

### Phase 2: Search Implementation (In Progress)

- [ ] Core search algorithm
- [ ] Arrow integration for zero-copy access
- [ ] Search context pooling
- [ ] Recall validation vs coder/hnsw

### Phase 3: Insert Implementation (Planned)

- [ ] Core insert algorithm
- [ ] Level assignment
- [ ] Neighbor management
- [ ] Insert context pooling

### Phase 4-6: Optimization, Integration, Rollout (Planned)

- See `arrow_hnsw_plan.md` for details

## Usage

### Creating an Index

```go
import "github.com/23skdu/longbow/internal/store/hnsw2"

// Create index with default config
index := hnsw2.NewArrowHNSW(dataset, hnsw2.DefaultConfig())

// Or with custom config
config := hnsw2.Config{
    M:              16,
    MMax:           32,
    EfConstruction: 200,
}
index := hnsw2.NewArrowHNSW(dataset, config)
```

### Search (To Be Implemented)

```go
// Search for k nearest neighbors
results, err := index.Search(queryVector, k)
```

### Insert (To Be Implemented)

```go
// Insert a vector
err := index.Insert(vectorID, level)
```

## Testing

### Run Unit Tests

```bash
go test ./internal/store/hnsw2/...
```

### Run Property-Based Tests

```bash
go test -v ./internal/store/hnsw2/ -run Properties
```

### Run Fuzzing

```bash
# Fuzz search (when implemented)
go test -fuzz=FuzzSearch -fuzztime=1m ./internal/store/hnsw2

# Fuzz insert (when implemented)
go test -fuzz=FuzzInsert -fuzztime=1m ./internal/store/hnsw2
```

### Run Benchmarks

```bash
go test -bench=. ./internal/store/hnsw2/...
```

## Migration Path

1. **Feature Flag**: `LONGBOW_USE_HNSW2` enables new implementation
2. **Dual-Index Mode**: Run both implementations, compare results
3. **Gradual Rollout**: 10% → 50% → 100% traffic
4. **Rollback**: Instant fallback to `coder/hnsw` if issues detected

## References

- [HNSW Paper](https://arxiv.org/abs/1603.09320) - Original algorithm
- [coder/hnsw](https://github.com/coder/hnsw) - Reference implementation
- [Arrow Format](https://arrow.apache.org/docs/format/Columnar.html) - Arrow memory layout
- [Implementation Plan](../brain/arrow_hnsw_plan.md) - Detailed plan
- [Task Breakdown](../brain/arrow_hnsw_tasks.md) - Task checklist

## Contributing

When contributing to hnsw2:

1. **Write Tests First**: Property-based tests for invariants, unit tests for behavior
2. **Validate Recall**: Use `DualIndexHarness` to ensure correctness
3. **Benchmark**: Compare against baseline before/after changes
4. **Zero-Allocation**: Verify with `go test -benchmem` that allocations are eliminated
5. **Document**: Update this file and code comments

## License

Same as Longbow project.
