# Longbow Performance Guide

Complete guide to Longbow performance optimization, testing, and benchmarking.

## Table of Contents

1. [Performance Features](#performance-features)
2. [Performance Testing](#performance-testing)
3. [Latest Results](#latest-results)
4. [Tuning Guide](#tuning-guide)

---

## Performance Features

### 1. NUMA Architecture Support

Longbow includes native support for Non-Uniform Memory Access (NUMA) architectures, commonly found in dual-socket servers (e.g., AWS metal instances, bare metal).

#### How it Works

- **Topology Detection**: Automatically detects NUMA nodes at startup
- **Worker Pinning**: Indexing workers are pinned to specific NUMA nodes to maximize cache locality
- **Local Allocation**: Memory for Arrow buffers is allocated on the same node as the worker processing it

#### Configuration

NUMA support is enabled by default on Linux if multiple nodes are detected. You can verify it via logs:

```
INFO: Detected 2 NUMA nodes
INFO: Started NUMA indexing workers nodes=2 count=16
```

### 2. Zero-Copy Data Access

For `DoGet` retrieve operations, Longbow utilizes Apache Arrow's zero-copy capabilities to map data directly from memory to the network wire without intermediate allocations.

- **Retain vs Copy**: When no filters are applied, record batches are Retained (ref-counted) rather than copied
- **Slicing**: When filtering with a tombstone bitmap, we use zero-copy slicing to create a view of the data

**Impact**: Reduces memory bandwidth usage by ~60% during heavy read workloads. Current measurements show **419 MB/s** read throughput with zero-copy enabled.

### 3. Vector Search Optimization

#### Batch Distance Calculations

Longbow groups vector distance calculations into batches (default 4096) to leverage SIMD instructions effectively. This benefits high-dimensional vectors (e.g., OpenAI 1536-dim) by keeping CPU pipelines full.

#### Stripe Locking

To prevent global lock contention during concurrent writes, HNSW graphs use striped locks based on vector ID. This allows simultaneous updates to different parts of the graph.

---

## Performance Testing

### Overview

The performance test suite (`scripts/perf_test.py`) provides comprehensive benchmarking for:

- **Basic throughput** - DoPut/DoGet operations
- **Vector search** - HNSW similarity search
- **Hybrid search** - Dense vectors + sparse text search
- **Concurrent load** - Multi-client stress testing
- **Large vectors** - OpenAI/Anthropic embedding dimensions
- **S3 snapshots** - Snapshot backend performance
- **Memory pressure** - Behavior under memory limits

### Prerequisites

```bash
# Install dependencies
pip install pyarrow numpy pandas dask boto3

# Start Longbow cluster
./scripts/start_local_cluster.sh
```

### Quick Start

```bash
# Basic test with defaults
python scripts/perf_test.py --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001

# Full benchmark suite
python scripts/perf_test.py --all --rows 50000 --dim 768 \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001

# Export results to JSON
python scripts/perf_test.py --all --json results.json \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001
```

### Benchmarks

#### 1. Basic Throughput (DoPut/DoGet)

Measures raw data ingestion and retrieval throughput.

**Metrics:**

- Throughput (MB/s)
- Duration (seconds)
- Bytes processed

**Usage:**

```bash
python scripts/perf_test.py --rows 100000 --dim 128 \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001
```

#### 2. Vector Similarity Search (HNSW)

Benchmarks HNSW index search performance with configurable k and query count.

**Metrics:**

- Queries per second (QPS)
- Latency percentiles (p50, p95, p99)
- Error rate

**Usage:**

```bash
python scripts/perf_test.py --search --search-k 10 --query-count 1000 \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001
```

#### 3. Hybrid Search (Dense + Sparse)

Benchmarks combined vector similarity and text search using RRF fusion.

**Usage:**

```bash
python scripts/perf_test.py --hybrid --search-k 10 --query-count 500 \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001
```

#### 4. Concurrent Load Testing

Stress tests with multiple parallel clients performing mixed operations.

**Usage:**

```bash
# Mixed read/write workload
python scripts/perf_test.py --concurrent 16 --duration 60 \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001

# Write-only workload
python scripts/perf_test.py --concurrent 16 --duration 60 --operation put \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001
```

#### 5. Large Dimension Vectors

Tests performance with production embedding sizes.

**Common Dimensions:**

| Model | Dimension |
|-------|----------:|
| OpenAI text-embedding-3-small | 1536 |
| OpenAI text-embedding-3-large | 3072 |
| Anthropic | 1024 |
| Cohere | 1024 |
| Custom | 128-4096 |

**Usage:**

```bash
# OpenAI embeddings
python scripts/perf_test.py --rows 50000 --dim 1536 \
  --data-uri grpc://localhost:3000 --meta-uri grpc://localhost:3001
```

### Command Reference

#### Connection Options

| Flag | Default | Description |
|------|---------|-------------|
| `--data-uri` | grpc://0.0.0.0:3000 | Data Server URI |
| `--meta-uri` | grpc://0.0.0.0:3001 | Meta Server URI |

#### Data Generation Options

| Flag | Default | Description |
|------|---------|-------------|
| `--rows` | 10000 | Number of vectors to generate |
| `--dim` | 128 | Vector dimension |
| `--name` | perf_test | Dataset name |

#### Search Options

| Flag | Default | Description |
|------|---------|-------------|
| `--search` | false | Enable vector search benchmark |
| `--hybrid` | false | Enable hybrid search benchmark |
| `--search-k` | 10 | Top-k results to retrieve |
| `--query-count` | 1000 | Number of search queries |
| `--text-field` | meta | Text field for sparse search |

#### Concurrent Load Options

| Flag | Default | Description |
|------|---------|-------------|
| `--concurrent` | 0 | Number of parallel clients |
| `--duration` | 60 | Test duration in seconds |
| `--operation` | mixed | Operation type: put, get, mixed |

#### Output Options

| Flag | Default | Description |
|------|---------|-------------|
| `--json` | none | Export results to JSON file |
| `--all` | false | Run all benchmarks |
| `--check-cluster` | false | Verify cluster health before start |

---

## Latest Results

**Test Date**: 2025-12-22  
**Cluster**: 3-node local cluster  
**Hardware**: Apple M3 Pro (ARM64)

### Data Operations

| Operation | Duration | Throughput | Rows/sec |
|-----------|----------|------------|----------|
| **DoPut** (Upload) | 0.035s | **714.48 MB/s** | 1,418,912 rows/s |
| **DoGet** (Download) | 0.060s | **418.91 MB/s** | 831,936 rows/s |

### Vector Search (HNSW)

| Metric | Value |
|--------|-------|
| **Throughput** | **305.10 queries/sec** |
| **Latency p50** | 2.27ms |
| **Latency p95** | 10.86ms |
| **Latency p99** | 14.11ms |
| **Error Rate** | 0% |

### Key Improvements

- **DoPut**: 4.5x faster than baseline (714 MB/s vs 156 MB/s)
- **DoGet**: 2x faster than baseline (419 MB/s vs 215 MB/s)
- **Search**: Sub-3ms median latency, sub-15ms p99

### Detailed Benchmarks (v0.1.2-rc15)

**Environment**: Apple M3 Pro (12-core), 18GB RAM.

#### 1-Node Configuration

| Dataset Size | Vector Dim | DoPut (MB/s) | DoGet (MB/s) | Search (QPS) | p50 (ms) | p95 (ms) |
|--------------|------------|--------------|--------------|--------------|----------|----------|
| 10,000       | 128        | 735.3        | 1094.8       | 6379         | 0.12     | 0.28     |
| 50,000       | 128        | 832.5        | 872.4        | 4278         | 0.19     | 0.42     |
| 100,000      | 128        | 1121.0       | 1011.1       | 1644         | 0.48     | 1.20     |
| 250,000      | 128        | 625.6        | 1511.6       | 1200         | 0.61     | 1.64     |

#### High-Dimensional Performance (768d/1536d)

| Benchmark | Dimensions | Write (MB/s) | Read (MB/s) | Search (QPS) | p99 Latency (ms) |
|-----------|------------|--------------|-------------|--------------|------------------|
| 384d (Validate) | 384 | 1335 | 1997 | **3535** | < 1ms |
| 768d (Scale) | 768 | 1609 | 2494 | **1202** | 4.66ms |

### Key Optimizations Verified

1. **SIMD Utilization (Fix 6)**: AVX2/NEON dispatched automatically. Validated ~5.3x speedup on distance calcs.
2. **Graph Compaction (Fix 5)**: "Vacuum" process prevents graph degradation from deletes.
3. **Concurrency Safety (Fix 7)**: Zero-allocation lazy chunking with atomic accessors ensures race-free scaling.
4. **Zero-Copy Access**: Direct access to Arrow buffers during distance computation.

---

## Tuning Guide

### Gossip Protocol

- `LONGBOW_GOSSIP_BATCH_SIZE`: Controls maximal UDP packet size (default 1400 bytes)
- `LONGBOW_GOSSIP_INTERVAL`: Frequency of state sync (default 200ms)

### Storage

- `LONGBOW_STORAGE_ASYNC_FSYNC`: Enable background fsync for WAL (default: true)
- `LONGBOW_DOPUT_BATCH_SIZE`: Records to buffer before WAL write (default: 100)
- `LONGBOW_DOGET_PIPELINE_DEPTH`: Prefetch depth for read pipeline (default: 8)

### Search

- `LONGBOW_HNSW_M`: Number of connections per layer (default: 16)
- `LONGBOW_HNSW_EF_CONSTRUCTION`: Size of dynamic candidate list during construction (default: 200)
- `LONGBOW_HNSW_EF_SEARCH`: Size of dynamic candidate list during search (default: 50)

### Memory

- `LONGBOW_MEMORY_LIMIT`: Soft memory limit in bytes (default: auto-detect)
- `LONGBOW_EVICTION_THRESHOLD`: Trigger eviction at % of limit (default: 0.9)

### Indexing

- `LONGBOW_INDEX_WORKERS`: Number of background indexing workers (default: CPU count)
- `LONGBOW_INDEX_QUEUE_SIZE`: Maximum pending index jobs (default: 10000)

## Best Practices

### Baseline Testing

1. Run basic throughput first to establish baseline
2. Use consistent hardware for comparisons
3. Run multiple iterations and average results

### Search Testing

1. Ensure data is indexed before running search benchmarks
2. Use representative query distributions
3. Test with production embedding dimensions
4. Include warmup phase before measurement

### Load Testing

1. Start with few workers and increase gradually
2. Monitor server resource utilization
3. Test both read-heavy and write-heavy workloads
4. Validate cluster coordination under load

### Memory Testing

1. Set realistic memory limits based on deployment
2. Monitor for OOM conditions
3. Test recovery after evictions

## Troubleshooting

### Connection Errors

```bash
# Verify server is running
lsof -nP -iTCP -sTCP:LISTEN | grep longbow

# Check cluster health
python scripts/ops_test.py --data-uri grpc://localhost:3000 \
  --meta-uri grpc://localhost:3001 status
```

### Slow Performance

1. Check vector dimensions match expected
2. Verify SIMD optimizations are active
3. Monitor memory pressure
4. Check for lock contention in metrics

### Search Errors

1. Ensure data is inserted before searching
2. Verify index is built (check `longbow_index_queue_depth` metric)
3. Check query vector dimensions match data
4. Review error logs for dimension mismatches

---

**See Also**:

- [Operations Testing Guide](../scripts/ops_test.py) - Functional validation
- [Metrics Validation](../scripts/validate_metrics.sh) - Observability validation
- [Latest Performance Results](performance_results_20251222.md) - Detailed analysis
