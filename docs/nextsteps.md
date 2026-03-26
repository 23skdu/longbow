# Longbow Performance Optimization Status

## Current Benchmark Results (2026-03-23)

### M3 Pro (Bahamut) — 61-Config Matrix Complete ✅

**Test**: 8 dtypes × 2 dims × 4 counts = 64 configs (61 completed)
**Memory**: 20GB allocated
**Queries**: 1,000 per test

| DType | Dim=128 Best QPS | Dim=384 Best QPS | Notes |
|-------|-----------------|-----------------|-------|
| int16 | 11,951 (Filtered) | 8,366 (Filtered) | Best overall performer |
| int32 | 11,960 (Filtered) | 8,512 (Filtered) | Excellent |
| uint32 | 12,139 (Filtered) | 9,305 (Filtered) | Best Filtered QPS |
| int8 | 8,797 (Dense) | 7,000 (Dense) | Best DoGet (8.2M/s) |
| float32 | 13,036 (Filtered) | 7,679 (Hybrid) | Excellent at 25k |
| complex64 | 11,869 (Filtered) | 8,055 (Filtered) | Good |
| float64 | 5,093 (Filtered) | 3,823 (Filtered) | Moderate |
| complex128 | 8,287 (Dense) | 2,961 (Filtered) | Slow at high dims |

**Anomalies**:

- `complex64_384_5000`: Very low QPS (347 Dense, 34 Filtered) — indexing not complete
- `complex128_384_5000/10000/25000`: Timed out (>5 min per test)

---

## Previous Benchmark Results (2026-03-16)

### SIMD Microbenchmarks (Apple M3 Pro)

| Operation | ns/op | MB/s |
|-----------|-------|-------|
| Euclidean128 | 31.33 | 16,133 |
| Euclidean384 | 98.91 | 15,433 |
| Euclidean768 | 155.5 | 19,055 |
| Euclidean1536 | 368.4 | 16,548 |

### Integration Benchmarks (float32, dim=384, InitialCapacity=50k)

| Vectors | DoPut (MB/s) | DoGet (MB/s) | Search (QPS) |
|---------|--------------|--------------|--------------|
| 1,000 | 414 | 443 | 1,526 |
| 5,000 | 716 | 1,240 | 622 |
| 10,000 | 1,270 | 1,779 | 944 |
| 15,000 | 1,297 | 1,874 | 897 |
| 25,000 | 1,416 | 2,099 | 812 |

### Validation Tests (25k vectors, dim=128)

| Metric | Result | Target | Status |
|--------|--------|--------|--------|
| Ingest | 1,235 MB/s | 800 MB/s | ✅ PASS |
| DoGet | 2,223 MB/s | 1,700 MB/s | ✅ PASS |

---

### 4. Test Fixes Needed

**Status**: 🔴 Needs Fixes
`arrow_neighbors_test.go:38`: `ValidSelection` subtest skipped due to Arrow memory management issues. `LengthMismatchError` and `EmptySelection` subtests pass.

---

## LOW PRIORITY / BY DESIGN

### Platform-Specific Stubs (No Action Needed)

| Feature | Status | Notes |
|---------|--------|-------|
| Real Remote Storage | ✅ Stable | AWS S3 and GCP GCS backends mapped to `RemoteStorage` |
| Stream Aggregator Slicing | ✅ Stable | `array.NewTableReader` cross-chunk boundary slicing |
| NUMA | `internal/store/numa_*_stub.go` | Linux only |
| io_uring | `internal/storage/wal_backend_stub.go` | Linux only |
| ONNX Metal | `internal/onnx/metal/stub.go` | macOS ARM64 only |
| GPU | `internal/gpu/memory.go` | Build tag gated |

### GPU Memory Operations (Requires `-tags=gpu`)

**File**: `internal/gpu/memory.go:170,175,185,190,195,200`

These return "not implemented yet" when built without GPU support. Build with `-tags=gpu` for actual implementations.

---

## Scripts Updated for 12GB Memory

Updated benchmark scripts to use 20GB memory limit for performance testing:

| Script | Previous | Updated |
|--------|----------|---------|
| `scripts/benchmark_runner.py` | 12GB | 20GB |
| `scripts/run_3node_performance.py` | 12GB | 20GB |
| `scripts/benchmark_suite.sh` | 12GB | 20GB |
| `scripts/start_bench_node.sh` | 12GB | 20GB |
| `scripts/run_iouring_comparison.sh` | 12GB | 20GB |
| `scripts/start_one_node.sh` | 12GB | 20GB |
| `scripts/benchmark_incremental.sh` | 12GB | 20GB |
| `scripts/benchmark_tool_incremental.sh` | 12GB | 20GB |

---

Last Updated: 2026-03-23 (61-config benchmark complete, M3 Pro performance documented)

---

## RaspberryPiZero Platform Plan

### Constraints

- **Memory**: Extremely limited (512MB RAM).
- **CPU**: ARMv6 (Pi Zero) or ARMv8 (Pi Zero 2). No AVX, maybe limited Neon.
- **Storage**: SD Card (slow I/O).

---

## RaspberryPiZero Platform Plan

1. **Low-Memory Mode Configuration**:
    - Introduce a \`low_mem\` profile in configuration or via environment variable.
    - Reduce default \`InitialCapacity\` (e.g., 5,000 instead of 50,000).
    - Downsize or disable memory-heavy pools/caches.
2. **CPU Optimization**:
    - Ensure clean fallback to scalar Go code for architectures without SIMD.
    - Disable high-performance SIMD instructions that require specific instruction sets (AVX/Neon if not available on 32-bit ARM).
3. **Build Configuration**:
    - Exclude GPU, Metal, and io_uring backends by default for \`arm\` builds.
    - Verify build with \`GOOS=linux GOARCH=arm GOARM=6\` (for original Pi Zero) or \`arm64\` (for Pi Zero 2).

---

## 🛠️ Codebase Remediations (TODOs & Stubs)

The following items were identified during a deep codebase review and require implementation to replace mocks, stubs, and incomplete features:

## Feature Parity with Leading Vector Databases

*Last Updated: 2026-03-22 — Updated based on codebase analysis vs Milvus, Qdrant, Weaviate, Pinecone*

### 🔴 HIGH PRIORITY — Production Gaps

### 🟡 MEDIUM PRIORITY — Ecosystem Gaps

### 🟢 LOW PRIORITY — Nice to Have

---

### Feature Comparison Matrix

| Feature | Milvus | Qdrant | Weaviate | Pinecone | Longbow |
|---------|--------|--------|----------|----------|---------|
| HNSW efSearch tuning | ✅ | ✅ | ✅ | ✅ | ✅ |
| IVF-PQ index | ✅ | ✅ | ✅ | ✅ | ✅ |
| Upsert | ✅ | ✅ | ✅ | ✅ | ✅ |
| Rich filter expr | ✅ | ✅ | ✅ | ✅ | ✅ |
| User-facing consistency | ✅ | ✅ | ✅ | ✅ | ✅ |
| REST API | ✅ | ✅ | ✅ | ✅ | ✅ |
| Published TS/JS SDK | ✅ | ✅ | ✅ | ✅ | ✅ |
| DiskANN on-disk | ✅ | ✅ | ✅ | ✅ | ✅ |
| Namespaces | ✅ | ✅ | ✅ | ✅ | ✅ |
| BM25 sparse | ✅ | ✅ | ✅ | ✅ | ✅ (via hybrid) |
| Multi-vector types | ✅ | ✅ | ✅ | ✅ | ✅ |
| Binary quantization | ✅ | ✅ | ✅ | ✅ | 🟡 (test-only) |
| Cross-encoder | ✅ | ❌ | ✅ | ✅ | ✅ |
| Distributed | ✅ | ✅ | ✅ | ✅ | ✅ |

---

### Recommended Priority Order

1. **Stream Aggregator Arrow Table Slicing** — Code cleanup & performance tool.
2. **Upsert** — Table stakes for production RAG / knowledge base workloads.
3. **REST API** — Lowest effort, highest ecosystem impact. gRPC→HTTP wrapper.
4. **Published TS/JS SDK** — Expands reach to web/Node.js ecosystem.
5. **Consistency levels (user-facing)** — Required for multi-region deployments.
