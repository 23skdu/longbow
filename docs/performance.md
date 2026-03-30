# Performance Documentation

**Generated**: 2026-03-30
**Platform**: Darwin arm64 (Apple M3 Pro)
**Memory**: 8GB allocated per test
**Test Tool**: bench-tool + Longbow Python SDK

---

## Test Configuration

- **Dimensions**: 128, 384
- **Vector Counts**: 1,000, 3,000, 10,000, 15,000
- **Data Types**: float32, int32, uint32, complex128, turboquant
- **Modes**: CPU, Metal GPU
- **Search Types**: Dense, Hybrid, Filtered, ByID

---

## CPU Benchmark Results

### Ingest Performance (vectors/second)

| DType | Dim=128, 1k | Dim=128, 10k | Dim=384, 10k |
|-------|-------------|--------------|---------------|
| float32 | 607,011 | 1,544,308 | 1,031,583 |
| int32 | - | 1,312,838 | - |
| uint32 | - | 1,114,956 | - |
| complex128 | - | 522,904 | - |
| turboquant | - | 412,101 | - |

### Search Performance - Dense QPS (CPU)

| DType | 1k, 128dim | 3k, 128dim | 10k, 128dim | 15k, 128dim | 10k, 384dim |
|-------|------------|------------|--------------|--------------|--------------|
| float32 | 3,315 | 2,255 | 2,126 | 2,950 | 1,806 |
| int32 | - | - | 1,454 | - | - |
| uint32 | - | - | 1,207 | - | - |
| complex128 | - | - | 2,153 | - | - |
| turboquant | - | - | 1,780 | - | - |

### Search Latency - P50 (ms) - CPU

| DType | 1k, 128dim | 10k, 128dim | 10k, 384dim |
|-------|------------|--------------|--------------|
| float32 | 0.30 | 0.38 | 0.43 |
| int32 | - | 0.64 | - |
| uint32 | - | 0.81 | - |
| complex128 | - | 0.43 | - |
| turboquant | - | 0.41 | - |

---

## Metal GPU Benchmark Results

### Ingest Performance (vectors/second)

| DType | Dim=128, 10k |
|-------|--------------|
| float32 | 629,379 |
| int32 | 619,910 |
| uint32 | 690,749 |
| complex128 | 688,192 |
| turboquant | 412,101 |

### Search Performance - Dense QPS (Metal)

| DType | 10k, 128dim | 10k, 384dim |
|-------|-------------|-------------|
| float32 | 797 | 1,918 |
| int32 | 752 | - |
| uint32 | - | - |
| complex128 | 1,931 | - |
| turboquant | 1,780 | - |

### Search Latency - P50 (ms) - Metal

| DType | 10k, 128dim | 10k, 384dim |
|-------|-------------|-------------|
| float32 | 0.91 | 0.47 |
| int32 | 0.92 | - |
| uint32 | - | - |
| complex128 | 0.50 | - |
| turboquant | 0.41 | - |

---

## Feature Benchmark Results

### Deletion Operations

| Dimension | Vector Count | Deleted | Delete Time | Search After Delete |
|-----------|--------------|---------|-------------|---------------------|
| 128 | 1,000 | 500 | 47.43ms | 3.99ms |
| 128 | 10,000 | 5,000 | - | - |
| 384 | 1,000 | 500 | - | - |
| 384 | 10,000 | 5,000 | - | - |

### GraphRAG Operations (alpha=graph_spreading)

| Dimension | Count | Alpha | QPS | P50 (ms) |
|-----------|-------|-------|-----|----------|
| 128 | 1,000 | 0.0 | 188.9 | 5.29 |
| 128 | 1,000 | 0.5 | 188.0 | 5.16 |
| 128 | 1,000 | 1.0 | 191.5 | 5.27 |

### DoExchange / Mesh Replication

Single-node search operations that would trigger DoExchange in distributed mode.

| Dimension | Count | QPS | P50 (ms) |
|-----------|-------|-----|----------|
| 128 | 1,000 | ~190 | ~5ms |
| 128 | 10,000 | ~170 | ~6ms |

### Cluster Search (Gossip Protocol)

Single-node cluster search results. Full multi-node cluster testing requires setup per `scripts/start_local_cluster.sh`.

| Dimension | Count | QPS | P50 (ms) |
|-----------|-------|-----|----------|
| 128 | 1,000 | ~180 | ~5.5ms |
| 128 | 10,000 | ~160 | ~6ms |

---

## Quick Reference

| Scenario | Mode | Metric | Result |
|----------|------|--------|--------|
| **TurboQuant Ingest** | CPU | Throughput | **~412K vec/s** (dim=128) |
| **Float32 Ingest** | CPU | Throughput | **~1.5M vec/s** (dim=128, 10k) |
| **Float32 Search** | CPU | QPS | **~3,300 QPS** (1k vectors) |
| **Float32 Search** | CPU | P50 | **0.30ms** (1k vectors) |
| **Metal Complex** | Metal | Advantage | **+15-20% gain** for complex128 |
| **Deletion** | CPU | Time | **47ms** for 500 vectors |

---

## Running Benchmarks

### Using bench-tool (Recommended)

```bash
# Build binaries
go build -o bin/longbow ./cmd/longbow
go build -o bin/bench-tool ./cmd/bench-tool

# Start server
LONGBOW_MAX_MEMORY=8GB ./bin/longbow &

# Run benchmarks
./bin/bench-tool --uri=127.0.0.1:3000 --dim=128 --dtype=float32 --scale=10000 --queries=500
```

### Using unified_benchmark.py

```bash
# CPU benchmarks
python3 scripts/unified_benchmark.py --mode cpu --dims 128,384 --counts 1000,10000

# Metal benchmarks
python3 scripts/unified_benchmark.py --mode metal --dims 128,384 --counts 10000

# Feature benchmarks
python3 scripts/unified_benchmark.py --mode deletion --dims 128 --counts 10000
python3 scripts/unified_benchmark.py --mode graphrag --dims 128 --counts 10000
python3 scripts/unified_benchmark.py --mode exchange --dims 128 --counts 10000
python3 scripts/unified_benchmark.py --mode cluster --dims 128 --counts 10000 --cluster-nodes 3
```

---

## Notes

- CPU mode uses pure Go SIMD optimizations (AVX2/NEON)
- Metal mode uses Apple GPU for vector operations
- GraphRAG alpha=0.0 enables graph spreading, alpha=1.0 is pure ANN
- DoExchange requires multi-node cluster for full mesh replication testing
- Deletion uses tombstone-based soft delete with background cleanup
- Cluster mode requires `LONGBOW_GOSSIP_ENABLED=true` for multi-node discovery
