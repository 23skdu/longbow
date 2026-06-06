# Longbow Performance Test Plan — v0.2.1-rc6

## Host

| Attribute | Value |
|-----------|-------|
| CPU | Alder Lake i7-12650H, 16 cores (AVX2, FMA, F16C, no AVX-512) |
| RAM | 22 GB total, 16 GB available |
| Swap | 15 GB |
| GPU | NVIDIA GeForce RTX 4060 Laptop GPU (8 GB VRAM, compute 8.9) |
| Drive | NVMe, 266 GB free |
| Kernel | Linux 7.0.0, io_uring enabled |

## Goals

1. **Performance characterization at dim=384** across all data types and vector counts
2. **All 13 search modes** — dense, hybrid, sparse, filtered (bool/string/op), byid, graphrag, global graphrag, recommend, geo, temporal, learned_index
3. **CPU + CUDA comparison** — measure GPU acceleration benefit per dtype/search mode
4. **Scale exploration** — 10k, 50k, 500k, 1M vectors at 16 GB memory cap
5. **OOM boundary detection** — identify which dtype/count combos exceed 16 GB

## Test Matrix

### Dimensions
- `384` (sentence-transformer scale, realistic workload)

### Counts
- `10,000` — quick sanity / warmup
- `50,000` — moderate scale
- `500,000` — production-relevant scale
- `1,000,000` — million-scale OOM boundary probe

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

Each config runs all 13 modes.

## Environment Variables (High Performance)

```bash
# Memory cap
LONGBOW_MAX_MEMORY=17179869184

# Storage
LONGBOW_USE_DISK=1
LONGBOW_STORAGE_USE_IOURING=true
LONGBOW_STORAGE_DOPUT_BATCH_SIZE=1000

# GPU
LONGBOW_GPU_ENABLED=true
LONGBOW_GPU_DEVICE_ID=0

# Feature toggles
LONGBOW_TEMPORAL_ENABLED=true
LONGBOW_SPARSE_ENABLED=true
LONGBOW_GEOSPATIAL_ENABLED=true
LONGBOW_GRAPHRAG_ENABLED=true
LONGBOW_LEARNED_INDEX_ENABLED=true
LONGBOW_HYBRID_SEARCH_ENABLED=true
LONGBOW_RERANKER_ENABLED=true
LONGBOW_AUTO_SCALE_ENABLED=false
LONGBOW_GOGC=200

# Workers
LONGBOW_INGESTION_WORKER_COUNT=6

# gRPC
LONGBOW_GRPC_MAX_RECV_MSG_SIZE=21474836470
LONGBOW_GRPC_MAX_SEND_MSG_SIZE=21474836470
LONGBOW_GRPC_MAX_CONCURRENT_STREAMS=250

# Compaction
LONGBOW_COMPACTION_ENABLED=true
LONGBOW_COMPACTION_INTERVAL=30s

# Arrow
ARROW_DISABLE_LOCKING=1

# GC
GODEBUG=madvdontneed=1
GOGC=200

# HNSW
LONGBOW_HNSW_M=32
LONGBOW_HNSW_EF_CONSTRUCTION=400
```

## Procedure

### Phase 0 — Cleanup
```
rm -rf data/bench/ data/perf_logs/ profiles/
go clean -cache
```

### Phase 1 — Fresh Build
```
go build -o bin/longbow ./cmd/longbow
go build -o bin/bench-tool ./cmd/bench-tool
```

### Phase 2 — CPU Benchmarks
```bash
LONGBOW_GPU_ENABLED=false \
python3 scripts/unified_benchmark.py \
  --mode cpu \
  --dims 384 \
  --counts 10000,50000,500000,1000000 \
  --dtypes float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant8 \
  --memory $((16 * 1024 * 1024 * 1024)) \
  --iouring \
  --use-disk \
  --timeout 7200 \
  --queries 500 \
  --workers 6 \
  --max-retries 2 \
  --label rc6_cpu
```

### Phase 3 — CUDA Benchmarks
```bash
LONGBOW_GPU_ENABLED=true \
python3 scripts/unified_benchmark.py \
  --mode cuda \
  --dims 384 \
  --counts 10000,50000,500000,1000000 \
  --dtypes float32,float64,float16,int8,int16,int32,int64,uint8,uint16,uint32,uint64,complex64,complex128,turboquant8 \
  --memory $((16 * 1024 * 1024 * 1024)) \
  --iouring \
  --use-disk \
  --timeout 7200 \
  --queries 500 \
  --workers 6 \
  --max-retries 2 \
  --label rc6_cuda
```

### Phase 4 — Monitoring
- Tail bench logs for `ResourceExhausted`, `OOM`, `EXHAUSTED`
- Watch server logs in `data/perf_logs/` for errors
- Check for crash/signal kills in benchmark output

### Phase 5 — Outputs
| Artifact | Location |
|----------|----------|
| JSON result matrix | `data/perf_logs/perf_matrix_rc6_*.json` |
| Markdown report | `data/perf_logs/perf_matrix_rc6_*.md` |
| Server logs | `data/perf_logs/longbow_*.log` |
| Benchmark logs | `data/perf_logs/bench_*.log` |
| Updated docs | `docs/performance.md`, `docs/nextsteps.md` |

## Expected Outcomes

### SIMD/CUDA Kernel Dispatch (per data type)
| Type | Expected Kernel | Notes |
|------|----------------|-------|
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

- **PASS**: All dtypes complete ingest + 13 search modes without crash across both CPU and CUDA modes. Performance numbers recorded for all combos.
- **FAIL**: Any dtype crashes server during ingest. Bench-tool reports 0 QPS for any mode due to kernel dispatch failure.
- **WARN**: complex128 OOMs at 1M (expected). Geo/Temporal QPS < 10 (data quality issue).
