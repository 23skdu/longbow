# Longbow v0.0.9 Release Notes

**Release Date:** 2025-12-18  
**Tag:** `0.0.9`  
**Commits since 0.0.8:** 69 merged PRs

---

## 🚀 Highlights

This is a **major release** focused on performance optimization, memory efficiency, and distributed architecture foundations. Key achievements include:

- **41% binary size reduction** by removing DuckDB dependency
- **Zero-copy data paths** throughout the hot path
- **Vector Quantization** (SQ8/PQ) for 4-8x memory reduction
- **Hybrid Search** combining BM25 + vector with configurable alpha weighting
- **Flight Client Pool** foundation for peer-to-peer vector mesh

---

## ⚡ Performance Optimizations

### Memory & Allocation
| Feature | Improvement | PR |
|---------|-------------|----|
| **Arrow Allocator Optimization** | Zero-copy buffers, 130,000x faster | #142 |
| **Vector Quantization (SQ8 + PQ)** | 4-8x memory reduction | #138 |
| **Zero-Allocation JSON Parser** | 2.4x faster ticket parsing, 0 allocs | #140 |
| **Per-Processor Result Pool** | Eliminates sync.Pool contention | #135 |
| **BitmapPool for Filter Masks** | Buffer reuse, reduced GC pressure | #130 |
| **IPC Buffer Pool** | 8.3x faster DoGet, 16x less memory | #119 |
| **RecordBatch Compaction** | Merges small batches into 10k-row chunks | #124 |
| **CachedRecordSize** | 5x faster, O(1) vs O(N) | #136 |

### CPU & SIMD
| Feature | Improvement | PR |
|---------|-------------|----|
| **SIMD 4x Unrolled Loops** | 2-4x faster on non-AVX architectures | #137 |
| **SIMD Dispatch Optimization** | Function pointers eliminate runtime switch | #101 |
| **Batch SIMD Distance** | Maximize CPU pipeline throughput | #103 |
| **Fast JSON Parser (goccy/go-json)** | 5.5x faster parsing | #115 |

### Concurrency & Locking
| Feature | Improvement | PR |
|---------|-------------|----|
| **Sharded Dataset Lock** | 40% faster lock operations | #125 |
| **Fine-Grained HNSW Locking** | Per-shard locks enable parallel inserts | #117 |
| **HNSW Auto-Sharding** | Automatic migration at 10k vectors | #131, #132 |
| **Async WAL Fsync** | Prevents DoPut blocking on disk stalls | #128 |
| **Non-Blocking DoPut** | IndexJobQueue with overflow handling | #112 |
| **Lock Granularity Schema Evolution** | RLock for reads, 25% faster | #109 |
| **Sharded Indexing Channels** | Prevents noisy neighbor issues | #104 |

### I/O & Networking
| Feature | Improvement | PR |
|---------|-------------|----|
| **Decoupled Receive/Processing** | FlightDataQueue + ChunkWorkerPool | #143 |
| **Parallelize DoGet Stream** | Pipelined worker pool, 3.28x speedup | #110 |
| **gRPC Streaming Compression** | 50-70% bandwidth reduction | #89 |
| **S3 Connection Pooling** | HTTP connection reuse | #90 |
| **ZSTD Snapshot Compression** | Reduced snapshot size | #85 |
| **gRPC Keepalive Tuning** | Configurable keepalive parameters | #84 |

### Search & Indexing
| Feature | Improvement | PR |
|---------|-------------|----|
| **SearchArena Integration** | Zero-allocation k-NN queries | #102 |
| **VectorizedFilter (Arrow Compute)** | Replaces row-by-row iteration | #100 |
| **Fast-Path Filters** | Bypass Arrow Compute for primitives | #118, #134 |
| **Inverted Indexing Activation** | O(1) equality lookups | #105 |
| **HNSW Zero-Copy Vectors** | SearchByIDUnsafe with epoch protection | #114 |
| **Batch Distance Calculations** | Optimized HNSW search | #103 |
| **Bloom Filter Pre-Check** | Sparse term optimization | #83 |
| **Zero-Copy Timestamp Enforcement** | Avoids RecordBatch rebuilds | #116 |
| **COW Metadata Map** | Non-blocking ListFlights | #113 |

---

## ✨ New Features

### Hybrid Search
- **BM25 + Vector Search** with configurable alpha weighting (#126, #141)
- **Fusion Modes:** RRF (Reciprocal Rank Fusion), Linear, Cascade
- **HybridSearchPipeline** combining ColumnIndex, BM25, and HNSW

### Adaptive Indexing
- **Auto-Switch BruteForce/HNSW** based on dataset size (#139)
- Brute Force for <1000 vectors, HNSW for larger datasets
- Automatic one-way migration with search equivalence

### Distributed Architecture Foundation
- **Flight Client Pool** for peer-to-peer connections (#120)
- **DoGet/DoPut Peer Integration** for Arrow streaming (#121)
- **Arrow Flight Replication** foundation for vector mesh (#96)
- **NUMA-Aware Memory Allocation** for multi-socket servers (#95)

### Index Optimizations
- **Parallel HNSW Construction** for faster bulk loading (#88)
- **Index Warmup on Startup** eliminates cold-start latency (#94)
- **Adaptive WAL Batching** (1ms high-load, 100ms low-load) (#93)
- **Result Pool Expansion** with ML workload sizes (10, 20, 50, 100, 256, 1000) (#92)
- **SearchArena Bump Allocator** for search hot path (#91)
- **ShardedInvertedIndex** with 32-shard locking (#81)

### Error Handling
- **Structured Error Context** with WALError, S3Error, ReplicationError, ConfigError, ShutdownError (#99)

---

## 📊 Observability

- **72 New Prometheus Metrics** across multiple PRs (#122, #123, #127, #82)
- Coverage includes:
  - WAL operations (fsync duration, batch size, pending entries)
  - Indexing (queue depth, job latency)
  - HNSW (node count, graph height, zero-copy vs copy access)
  - Filter execution (duration, selectivity)
  - Memory (Arrow allocator, scratch pool)
  - Peer replication and Flight pool operations

---

## 🐳 Docker & Deployment

- **DuckDB Removed** - 41% binary size reduction (82MB → 48MB) (#144)
- **Scratch Base Image** - Minimal Docker footprint (~50MB)
- **CGO_ENABLED=0** - Fully static binary
- **Sidecar-Ready** - Optimized for Kubernetes deployments

---

## 🐛 Bug Fixes

- NUMA cross-platform build with build tags (#97)
- Warmup test errcheck handling (#98)
- SIMD arm64 duplicate function declarations (#87)
- Flaky TestObservability_WALBufferPool (#84)
- WALBatcherConfig hugeParam lint issue (#133)
- Lint issues in test files (#145)
- Binary.Write reflection elimination (#111)

---

## 📦 Dependencies

- Upgraded `github.com/aws/aws-sdk-go-v2/config` (#107)
- Upgraded `github.com/aws/aws-sdk-go-v2/credentials` (#106)
- Added `github.com/goccy/go-json` for fast JSON parsing
- **Removed** `github.com/marcboeker/go-duckdb`

---

## 🔄 Upgrade Notes

1. **DuckDB Analytics Removed**: The `DoAction("analytics")` endpoint is no longer available. Use external tools for parquet SQL queries.

2. **Binary Size**: Significant reduction allows lightweight sidecar deployments.

3. **Configuration**: New config options available for:
   - `HybridSearchConfig` - alpha, fusion mode, text columns
   - `AdaptiveIndexConfig` - BruteForce/HNSW threshold
   - `AutoShardingConfig` - HNSW sharding threshold
   - `AsyncFsyncConfig` - WAL fsync behavior

---

## 📈 Benchmark Highlights

| Operation | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Zero-copy Arrow buffers | 36,863 ns/op | 0.28 ns/op | **130,000x** |
| IPC Buffer Pool Get/Put | 19,785 ns/op | 2,382 ns/op | **8.3x** |
| JSON Ticket Parsing | 491 ns/op | 90 ns/op | **5.5x** |
| CachedRecordSize | 22.1 ns/op | 4.37 ns/op | **5x** |
| DoGet Pipeline | 91 ms | 28 ms | **3.28x** |
| Sharded Lock Operations | 54.12 ns/op | 32.22 ns/op | **40%** |

---

## Contributors

Thanks to all contributors who made this release possible!

---

**Full Changelog:** https://github.com/23skdu/longbow/compare/0.0.8...0.0.9
