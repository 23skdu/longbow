# Longbow Performance Test Plan — v0.2.1-rc5

## Host

| Attribute | Value |
|-----------|-------|
| CPU | Alder Lake i7-12650H, 16 cores (AVX2, FMA, F16C, **no** AVX-512) |
| RAM | 22 GB total, 16 GB available |
| Swap | 15 GB |
| GPU | NVIDIA GeForce RTX 4060 Laptop GPU (8 GB VRAM, compute 8.9) |
| Drive | NVMe, 266 GB free |
| Kernel | Linux 7.0.0, io_uring enabled |
| Mode | `cuda` (CUDA-accelerated where available) |

## Goals

1. **SIMD kernel coverage** — verify every data type dispatches a SIMD (AVX2) or CUDA kernel, not the generic fallback
2. **Million-scale ingest & search** — all 14 data types at full 1M count, dim=384
3. **All search modes** — dense, hybrid, sparse, filtered (bool/string/op), byid, graphrag, global graphrag, recommend, geo, temporal, learned_index
4. **Disk-backed graph store** — `LONGBOW_USE_DISK=1`
5. **io_uring integration** — `LONGBOW_STORAGE_USE_IOURING=true`
6. **CUDA acceleration** — float32/float64/int8/uint8/int16/uint16/int32/uint32/int64/uint64/half kernels
7. **pprof profiles** — collect per-config CPU, heap, allocs, goroutine, threadcreate, block, mutex
8. **OOM boundary** — detect which dtypes exceed 16 GB RAM at 1M

## Test Matrix

### Dimensions
- `384` (sentence-transformer scale, realistic workload)

### Counts
- `1,000,000` — million-scale, OOM boundary probe

### Data Types (14)
| Category | Types |
|----------|-------|
| float | `float32`, `float64`, `float16` |
| int (signed) | `int8`, `int16`, `int32`, `int64` |
| int (unsigned) | `uint8`, `uint16`, `uint32`, `uint64` |
| complex | `complex64`, `complex128` |
| quantized | `turboquant8` |

### Search Modes (13 — all bench-tool supports)
- `Dense` — standard k-NN HNSW
- `Hybrid` — vector + BM25 fusion
- `Filtered` — k-NN with numeric predicate
- `FilteredBool` — k-NN with boolean predicate
- `FilteredString` — k-NN with string predicate
- `Sparse` — inverted-index only
- `ByID` — lookup by primary key
- `GraphRAG` — k-NN with graph spreading activation
- `GlobalGraphRAG` — GraphRAG across all mesh nodes
- `Recommend` — graph-based recommendation from seed IDs
- `Geo` — geospatial radius search around NYC
- `Temporal` — time-based "as of" search
- `LearnedIndex` — k-NN with learned index scoring

Each config runs all 13 modes → **182 search rows** total.

## Environment Variables

```bash
LONGBOW_MAX_MEMORY=17179869184          # 16 GB
LONGBOW_USE_DISK=1
LONGBOW_STORAGE_USE_IOURING=true
LONGBOW_GPU_ENABLED=true
LONGBOW_TEMPORAL_ENABLED=true
LONGBOW_SPARSE_ENABLED=true
LONGBOW_GEOSPATIAL_ENABLED=true
LONGBOW_GRAPHRAG_ENABLED=true
LONGBOW_LEARNED_INDEX_ENABLED=true
LONGBOW_HYBRID_SEARCH_ENABLED=true
LONGBOW_HNSW_TURBOQUANT_ENABLED=true
LONGBOW_RERANKER_ENABLED=true
LONGBOW_GOGC=200
LONGBOW_INGESTION_WORKER_COUNT=6
LONGBOW_SNAPSHOT_INTERVAL=24h
ARROW_DISABLE_LOCKING=1
LONGBOW_GRPC_MAX_RECV_MSG_SIZE=21474836470
LONGBOW_GRPC_MAX_SEND_MSG_SIZE=21474836470
```

## Procedure

### Phase 1 — Cleanup ✅
- `bin/bench-tool`, `bin/longbow-cuda`, `bin/longbow-avx2` removed
- `data/bench/`, `data/perf_logs/`, `profiles/` removed
- Fresh build: `make build-cuda` + `make build-bench`

### Phase 2 — Execution

```bash
python3 scripts/unified_benchmark.py \
  --mode cuda \
  --dims 384 \
  --counts 1000000 \
  --dtypes float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant8 \
  --memory $((16 * 1024 * 1024 * 1024)) \
  --iouring \
  --use-disk \
  --pprof \
  --timeout 7200 \
  --queries 500 \
  --max-retries 2 \
  --label rc5_cuda_1M
```

### Phase 3 — Monitoring
- Tail `benchmark_rc5_cuda_1M.log` for `ResourceExhausted`, `OOM`, `EXHAUSTED`
- Watch server logs in `data/perf_logs/longbow_cuda_*.log` for errors
- Each config produces 7 pprof profiles → up to 98 pprof files

### Phase 4 — Outputs
| Artifact | Location |
|----------|----------|
| JSON result matrix | `data/perf_logs/perf_matrix_cuda_rc5_cuda_1M_*.json` |
| Markdown report | `data/perf_logs/perf_matrix_cuda_rc5_cuda_1M_*.md` |
| Server logs | `data/perf_logs/longbow_cuda_*.log` |
| Benchmark logs | `data/perf_logs/bench_*.log` |
| pprof profiles | `profiles/*.pprof` |
| Updated docs | `docs/performance.md`, `docs/nextsteps.md` |

## Expected Outcomes

### SIMD/CUDA Kernel Dispatch (per data type)
| Type | Expected Kernel | Notes |
|------|-----------------|-------|
| float32 | CUDA `l2_distance_kernel_v2` / AVX2 `l2SquaredAVX2` | Both exist |
| float64 | CUDA `l2_distance_float64_kernel` / AVX2 `euclideanFloat64AVX2` | Both exist |
| float16 | CUDA `l2_distance_fp16_kernel_optimized` | No AVX2 float16 kernel |
| int8 | CUDA `l2_distance_int8_kernel` / AVX2 `euclideanInt8AVX2Kernel` | Both exist |
| int16 | CUDA `l2_distance_int16_kernel` / AVX2 `euclideanInt16AVX2Kernel` | Both exist |
| int32 | CUDA `l2_distance_int32_kernel` / AVX2 `euclideanInt32AVX2Kernel` | Both exist |
| int64 | CUDA `l2_distance_int64_kernel` / generic path | Simd64 only on AVX-512 |
| uint8 | CUDA `l2_distance_uint8_kernel` / AVX2 `euclideanUint8AVX2Kernel` | Both exist |
| uint16 | CUDA `l2_distance_uint16_kernel` / AVX2 `euclideanUint16AVX2Kernel` | Both exist |
| uint32 | CUDA `l2_distance_uint32_kernel` | No AVX2 for uint32 |
| uint64 | CUDA `l2_distance_uint64_kernel` | Generic fallback on CPU |
| complex64 | generic (unsafe-cast to float32) | No dedicated SIMD |
| complex128 | generic (unsafe-cast to float64) | No dedicated SIMD |
| turboquant8 | CUDA `turboquant_distance_kernel_v2` / CPU reconstruct + AVX2 | Both exist |

### OOM Risk at 384×1M
| Type | Raw Data | Risk |
|------|----------|------|
| float32 | 1.5 GB | LOW |
| float64 | 3.0 GB | LOW |
| float16 | 768 MB | LOW |
| int8 | 384 MB | LOW |
| int16 | 768 MB | LOW |
| int32 | 1.5 GB | LOW |
| int64 | 3.0 GB | LOW |
| uint* | same as int* | LOW |
| complex64 | 3.0 GB | MEDIUM |
| complex128 | 6.0 GB | HIGH — may OOM with graph overhead |
| turboquant8 | 384 MB | LOW |

## Pass/Fail Criteria

- **PASS**: All dtypes complete ingest + 13 search modes without crash. SIMD/CUDA dispatch verified.
- **FAIL**: Any dtype crashes server during ingest. Bench-tool reports 0 QPS for any mode due to kernel dispatch failure.
- **WARN**: complex128 OOMs (expected at 1M). Geo/Temporal QPS < 10 (data quality issue).
