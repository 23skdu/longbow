# Longbow Benchmark & Validation Test Plan

## 1. Overview & Objective

This test plan defines the comprehensive benchmarking, performance validation, and regression testing matrix for Longbow. It establishes testing protocols for vector ingest and query search across multiple execution backends, data types, index sizes, dimensionalities, and persistence configurations.

The primary goals are:

1. Measure and record ingestion throughput (vectors/sec) and search throughput (queries/sec / QPS) with P50, P95, and P99 latency percentiles.
2. Characterize the performance delta between standard Go and EMLGo SIMD kernels on CPU and GPU.
3. Quantify the performance and memory impact of auto-spill disk persistence (`use_disk=yes` vs `use_disk=no`).
4. Catch performance regressions (>10% drop against baseline) and stability anomalies across scaling tiers (50k, 100k, 250k, 500k vectors) and dimensionalities (128 up to 3072 for OpenAI large).

---

## 2. Test Environment & System Specifications

| Component | Specification |
|---|---|
| **CPU** | Intel Core i7-12650H (16 threads, AVX2, x86_64) |
| **System Memory** | 23 GB RAM |
| **GPU** | NVIDIA GeForce RTX 4060 Laptop (8 GB VRAM, sm_89) |
| **CUDA Toolkit** | CUDA 12.4 (`nvcc` V12.4.131) |
| **Host Operating System** | Linux (Ubuntu / Debian x86_64 kernel 6.8) |
| **Go Runtime** | Go 1.24+ / 1.27 with CGO enabled |
| **Benchmark Script** | [scripts/unified_benchmark.py](file:///home/rsd/REPOS/longbow/scripts/unified_benchmark.py) |
| **Benchmark Harness** | [cmd/bench-tool](file:///home/rsd/REPOS/longbow/cmd/bench-tool) (`bin/bench-tool`) |

---

## 3. Test Configuration Matrix

### 3.1 Binary Variants

| Binary | Description | Build Tags / Flags |
|---|---|---|
| `bin/longbow_main` | CPU Standard | `go build -o bin/longbow_main ./cmd/longbow` |
| `bin/longbow_emlgo` | CPU EMLGo SIMD | `go build -tags emlgo -o bin/longbow_emlgo ./cmd/longbow` |
| `bin/longbow-cuda_main` | GPU CUDA Standard | `go build -tags gpu -o bin/longbow-cuda_main ./cmd/longbow` |
| `bin/longbow-cuda_emlgo` | GPU CUDA + EMLGo | `go build -tags "gpu,emlgo" -o bin/longbow-cuda_emlgo ./cmd/longbow` |

### 3.2 Vector Scaling Tiers

Tests are evaluated at four representative dataset scale points:

- **50,000 vectors (50k)**: Low-footprint baseline, in-cache behavior.
- **100,000 vectors (100k)**: Mid-tier working set, SIMD dispatch crossover.
- **250,000 vectors (250k)**: Large-scale index stress, disk spillover threshold testing.
- **500,000 vectors (500k)**: High-density scaling boundary, multi-GB working set, cache eviction stress.

### 3.3 Dimensionalities to Test (OpenAI Large & Transformer Tiers)

Testing covers compact to high-dimensional representation tiers:

- **128 dimensions**: Default compact benchmark embedding, cache-resident indexing.
- **384 dimensions**: MiniLM, BGE-small, and lightweight sentence transformers.
- **768 dimensions**: BERT-base, RoBERTa, and standard transformer dense representations.
- **1536 dimensions**: OpenAI `text-embedding-3-small` / `text-embedding-ada-002`.
- **3072 dimensions**: OpenAI `text-embedding-3-large` (maximum scale stress test).

### 3.4 Data Types (Including TurboQuant Tiers)

Longbow supports a full spectrum of scalar, floating-point, complex, and quantized representations:

1. `int8`: 8-bit signed integer quantization.
2. `uint8`: 8-bit unsigned integer quantization.
3. `int16`: 16-bit signed integer.
4. `uint16`: 16-bit unsigned integer.
5. `int32`: 32-bit signed integer.
6. `uint32`: 32-bit unsigned integer.
7. `int64`: 64-bit signed integer.
8. `uint64`: 64-bit unsigned integer.
9. `float16`: 16-bit half-precision floating point (IEEE 754-2008 / fp16).
10. `float32`: 32-bit single-precision floating point.
11. `float64`: 64-bit double-precision floating point.
12. `complex64`: Paired 32-bit float real/imaginary (quantum & Fourier states).
13. `complex128`: Paired 64-bit float real/imaginary.
14. `turboquant4`: 4-bit quantized polar representation (balanced accuracy/memory).
15. `turboquant8`: 8-bit quantized polar representation (high-fidelity quantized).

### 3.5 Full Search Modes Taxonomy

Longbow engine and harness support 13 distinct search modalities:

1. **`dense`**: Pure vector similarity search via HNSW multi-layer graph navigation.
2. **`hybrid`**: Reciprocal rank fusion (RRF) combining dense vector similarity and sparse BM25 lexical token match.
3. **`sparse`**: Inverted index token matching with term frequency scoring and vector reranking.
4. **`filtered`**: Vector similarity search constrained by numeric metadata range predicates (e.g. `id > 10`).
5. **`filtered_bool`**: Vector similarity search filtered by boolean metadata attributes (e.g. `active == true`).
6. **`filtered_string`**: Vector similarity search filtered by categorical string attributes (e.g. `category == electronics`).
7. **`by_id`**: Direct vector neighbor expansion and retrieval using existing vector ID as seed.
8. **`graphrag`**: Local knowledge graph entity traversal and multi-hop neighborhood aggregation fused with vector distance.
9. **`global_graphrag`**: Cluster-level global knowledge graph traversal and community summarization.
10. **`recommend`**: Multi-seed item-to-item recommendation using BFS graph expansion with decay weighting.
11. **`geo`**: Geospatial radius and bounding box search centered on coordinate pairs (latitude/longitude).
12. **`temporal`**: Time-decay weighted vector ranking with temporal slicing (`temporal_as_of`, `temporal_range`, `temporal_window`).
13. **`learned_index`**: Neural-accelerated candidate pruning using a learned CDF spline predictor.

### 3.6 Storage / Spillover Modes

- **`use_disk=no` (`nodisk`)**: Pure in-memory vector storage and graph indexing.
- **`use_disk=yes` (`disk`)**: Auto-spill mode enabled (`LONGBOW_AUTO_SPILL_DISK=true`), paging vector storage to disk when memory consumption exceeds 60% of the allocated ceiling while keeping graph traversal fast.

---

## 4. Execution Methodology

For each test configuration:

1. Ensure no rogue processes occupy the target port (kill lingering instances on port 3000/random fallback).
2. Clean temporary benchmark data in `data/bench/` to prevent cross-run state pollution.
3. Start the Longbow server instance with appropriate environment variables and memory limits:
   - Memory ceiling: 14 GiB (`15032385536` bytes, within the safe physical envelope on a 22-23 GiB host).
   - Concurrency workers: 4 workers (`--workers 4`, pinned to 4 unthrottled cores via `--cpu-affinity 12-15` matching `docs/performance.md`).
   - Search queries: 500 representative randomized queries per mode.
4. Execute `scripts/unified_benchmark.py`:
   - Warm up the server connection via Arrow Flight DoAction health check.
   - Ingest dataset vectors, recording ingest wall-clock duration and throughput.
   - Build HNSW graph indexing structure.
   - Run search passes across all requested search modes sequentially.
   - Collect latency statistics: P50, P95, P99, and total QPS.
   - Record memory RSS and disk utilization in MB.
5. Save raw JSON results to `data/perf_logs/perf_matrix_<mode>_<label>_<timestamp>.json` and markdown report to `data/perf_logs/perf_matrix_<mode>_<label>.md`.
6. Terminate the server gracefully, releasing port and device memory before the next configuration.

---

## 5. Regression Analysis & Acceptance Criteria

- **Regression Threshold**: Performance drops exceeding **10%** compared to the baseline require root-cause analysis.
- **Stability**: Zero OOM panics, zero unhandled SIGSEGV faults, and zero gRPC/Arrow Flight connection leaks over the entire matrix.
- **Memory Consistency**: RSS consumption must remain bounded by the 60% auto-spill ceiling when `use_disk=yes`.
- **Accuracy Parity**: Top-K search recall between standard and EMLGo SIMD backends must remain $\ge 0.99$.
- **Bit-Identity Parity for Batched Kernels**: All batched SIMD kernels (`TurboQuantDistanceBatch`, integer batch kernels, float kernels) must evaluate bit-identically to their corresponding single-vector kernels across all bit depths and supported architectures (`TestTurboQuantDistanceBatchIsBitIdentical`, `TestTurboQuantBatchKernelsCrossArchitectureParity`, `TestTQComputeBatchMatchesPerCandidate`). Dispatched batch evaluation must never alter traversal order or tie-breaking in graph search.

### 5.1 Which Gate Applies Where (R30, R39)

There are two benchmark gates and they answer different questions. Using the
wrong one is worse than having none: `docs/roadmap.md` records that the
single-run-versus-baseline gate fires falsely **77-89%** of the time at a 10%
threshold, and that interleaving two separate invocations makes it *worse*
(R35/R36), because inter-invocation variation is systematic rather than
zero-mean. Measured against itself, the gate's false-positive rate is under
1.5% only in the interleaved form.

| Gate | Where it runs | Question it answers | Exit codes |
|:---|:---|:---|:---|
| `unified_benchmark.py --compare-baseline` | `ci.yml` → `benchmark-regression`, every push and PR, **advisory only** | "Is today's run far from a recorded baseline?" | 0 = within threshold, 1 = outside, 2 = setup error |
| `scripts/ab_benchmark.py` | `benchmark-ab.yml`, **scheduled only**, and locally before landing a change to a hot path | "Does this specific change make this specific workload worse?" | 0 = no regression, 1 = regression, 2 = too few pairs |

**Policy.**

1. **A pull request is not blocked on a benchmark.** The `benchmark-regression`
   job runs and uploads its artifacts, but a failed `--compare-baseline` is a
   signal to investigate, not a red build. A gate that fires falsely 77-89% of
   the time trains people to re-run it until it goes green, which is worse than
   no gate at all.
2. **The interleaved A/B harness is the only gate that may block**, and only
   where both arms exist: two built binaries, an explicit baseline, and at least
   3 usable reps (it exits 2 otherwise rather than reporting a verdict it cannot
   support).
3. **Scheduled A/B is the qualification run.** It runs the full dtype matrix on
   a schedule, where it has the time budget the interleaved harness needs.
4. **Both search QPS and index time are gated together.** `unified_benchmark.py`
   emits `indexing_duration_seconds` per configuration and `--compare-baseline`
   already fails on an increase. A change that buys QPS by building a cheaper
   graph shows up as an index-time regression, and vice versa; looking at either
   number alone is how a recall regression reaches main.
5. **Recall is not a benchmark output.** Graph-quality gates live in Go tests
   (`internal/store/index/*_quality_test.go`), which are exact and free. If a
   benchmark delta needs explaining, the first move is to run the quality tests
   at the same configuration, not to re-run the benchmark.

### 5.2 Running the A/B Harness

```bash
# Build both arms first; the harness runs them, it does not build them.
go build -o /tmp/baseline/longbow  ./cmd/longbow
go build -o /tmp/candidate/longbow ./cmd/longbow

python3 scripts/ab_benchmark.py \
    --baseline-binary  /tmp/baseline/longbow \
    --candidate-binary /tmp/candidate/longbow \
    --counts 50000 --dtypes float32 --search-modes dense \
    --queries 500 --reps 6 --threshold 5 --cpu-affinity 12-15
```

`--reps` must be at least 3 or the harness exits 2. It reports one of three
verdicts - `regression`, `improvement`, `inconclusive` - and `inconclusive` is
a real answer, not a pass: it distinguishes "consistently below threshold but
too small to call" from "inconsistent, so the noise floor is above the effect".
An `inconclusive` result on a change you believe matters means the harness was
not given enough reps, not that the change is safe.

---

## 6. Maintenance & Artifact Hygiene

- Temporary profiling `.pprof` files and graph dumps (`.bin`) must not be committed to version control.
- Only summary performance metrics, consolidated logs, and documentation (`docs/performance.md`, `docs/roadmap.md`) shall be tracked in git.
- Test artifacts and binaries must be periodically cleared using the project clean guidelines.
