# Longbow Benchmark & Validation Test Plan

## 1. Overview & Objective

This test plan defines the comprehensive benchmarking, performance validation, and regression testing matrix for Longbow. It establishes testing protocols for vector ingest and query search across multiple execution backends, data types, index sizes, dimensionalities, and persistence configurations.

The primary goals are:

1. Measure and record ingestion throughput (vectors/sec) and search throughput (queries/sec / QPS) with P50, P95, and P99 latency percentiles.
2. Characterize the performance delta between standard Go and EMLGo SIMD kernels on CPU and GPU.
3. Quantify the performance and memory impact of auto-spill disk persistence (`use_disk=yes` vs `use_disk=no`).
4. Catch performance regressions (>10% drop against baseline) and stability anomalies across scaling tiers (50k, 100k, 250k, 500k vectors) and dimensionalities (128 up to 3072 for OpenAI large).

---

## 2. Test Environments & System Specifications

Longbow maintains two primary validation environments:

### 2.1 Local Development Testbed (AVX2 Client)

| Component | Specification |
|---|---|
| **CPU** | Intel Core i7-12650H (16 threads, AVX2, hybrid P/E-cores, x86_64) |
| **System Memory** | 23 GB RAM |
| **GPU** | NVIDIA GeForce RTX 4060 Laptop (8 GB VRAM, sm_89) |
| **CUDA Toolkit** | CUDA 12.4 (`nvcc` V12.4.131) |
| **Host Operating System** | Linux (Ubuntu / Debian x86_64 kernel 6.8) |
| **Go Runtime** | Go 1.27.3 with CGO enabled |
| **Benchmark Script** | [scripts/unified_benchmark.py](file:///home/rsd/REPOS/longbow/scripts/unified_benchmark.py) |
| **Benchmark Harness** | [cmd/bench-tool](file:///home/rsd/REPOS/longbow/cmd/bench-tool) (`bin/bench-tool`) |

### 2.2 Dedicated Remote Testbed (AVX-512 Cloud Server)

| Component | Specification |
|---|---|
| **Host / Instance** | Google Cloud Compute Engine (`tf-testbed-70b436`) |
| **CPU** | Intel(R) Xeon(R) CPU @ 2.80GHz (16 vCPUs, AVX-512F, AVX-512DQ, AVX-512CD, AVX-512BW, AVX-512VL, AVX-512_VNNI, FMA) |
| **System Memory** | 125 GiB RAM (high-memory envelope, no memory pressure) |
| **Host Operating System** | Linux 7.0-gcp x86_64 |
| **Go Runtime** | Go 1.27.1 with CGO enabled |
| **Role** | Production AVX-512 kernel validation, EMLGo parity, high-concurrency scaling (8+ workers, unpinned) |


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

## 4. Execution Methodology & Concurrency Rules

### 4.1 CPU Affinity & Worker Concurrency Guidelines

A critical finding from empirical cross-architecture benchmarking is that **CPU affinity and core pinning requirements depend strictly on the underlying CPU topology**:

1. **Older / Heterogeneous AVX2 Architectures (Client CPUs)**:
   - **Requirement**: Use explicit CPU pinning (`--cpu-affinity 12-15`) and restrict client workers (`--workers 4`).
   - **Rationale**: Hybrid client CPUs (e.g., Intel Alder Lake / Raptor Lake i7-12650H) contain asymmetric Performance cores (P-cores) and Efficiency cores (E-cores). Allowing the OS scheduler to migrate threads to E-cores or dynamically frequency-throttle under laptop thermal constraints introduces severe benchmark jitter and false regression signals. Pinning to 4 isolated, high-performance P-cores stabilizes variance.

2. **Modern AVX-512 Server Architectures (Cloud Xeon / EPYC)**:
   - **Requirement**: **Do NOT pin CPU cores** (omit `--cpu-affinity`). Scale client concurrency to **8 or more workers** (`--workers 8`).
   - **Rationale**: Modern server instances (e.g. Google Cloud 16-vCPU Xeon @ 2.80GHz) provide dedicated, homogeneous, unthrottled vCPUs with ample thermal headroom and memory bandwidth (125 GiB RAM).
   - **Observed Benefits**:
     - **Ingest Throughput (`DoPut`)**: Increases from 249,376 vec/s to **537,085 vec/s (+115% / 2.15x faster)**.
     - **HNSW Indexing Speed**: Increases from 2,310 vec/s to **5,713 vec/s (2.47x speedup)** as the parallel builder scales from `addbatch_inflight=2` to `addbatch_inflight=8`.
     - **Query Throughput**: Substantial gains across all 13 search modes without latency degradation (e.g., FilteredString +38.3%, Geo +67.1%, GraphRAG +31.0%).

| Architecture / Platform | CPU Topology | CPU Affinity Flag | Recommended Workers | Memory Ceiling |
|:---|:---|:---|:---|:---|
| **Client / AVX2** (i7-12650H) | Hybrid P+E cores, shared desktop load | `--cpu-affinity 12-15` (pinned) | 4 workers | 14 GiB |
| **Server / AVX-512** (Xeon 16 vCPU) | Homogeneous vCPUs, dedicated cloud host | *Omit* (unpinned) | 8 workers | 75 GiB |

### 4.2 Execution Protocol

For each test configuration:

1. **Port Sanitization**: Ensure no rogue processes occupy the target port (kill lingering instances on port 3000/fallback range 3000–3150).
2. **Scratch Cleansing**: Clean temporary benchmark data in `data/bench/` to prevent cross-run state pollution.
3. **Server Launch**: Start the Longbow server instance with appropriate memory limits and binary tags:
   - Client testbed: 14 GiB memory ceiling (`15032385536` bytes), 4 pinned workers (`--workers 4 --cpu-affinity 12-15`).
   - Server testbed: 75 GiB ceiling, unpinned 8 workers (`--workers 8`), 500 representative randomized queries per mode.
4. **Execution via `scripts/unified_benchmark.py`**:
   - Warm up the server connection via gRPC / Arrow Flight readiness polling.
   - Ingest dataset vectors, recording wall-clock duration and throughput (`vec/s`, `MB/s`).
   - Construct HNSW graph indexing structure, recording `indexing_duration_seconds`.
   - Run search passes across all 13 search modes sequentially.
   - Collect latency statistics: P50, P95, P99, and total QPS.
   - Record peak RSS memory and disk utilization.
5. **Continuous Profiling Capture**:
   - Trigger snapshot pprof collections during queries and a final comprehensive snapshot at the completion of each data type before shutdown.
   - Archive 7 core profile types: `profile` (CPU), `heap`, `allocs`, `goroutine`, `threadcreate`, `block`, and `mutex`.
6. **Persistence & Tear-down**:
   - Commit incremental JSON checkpoints to `data/perf_logs/perf_matrix_<mode>_<label>_<timestamp>.json`.
   - Terminate the server gracefully and wait 1 second before the next test. A longer
     fixed sleep buys nothing: `start_server` already retries each port with an
     exponential backoff until `bind` succeeds, so what it is actually waiting out
     is `TIME_WAIT`, not a fixed cooldown.

### 4.2a Configuration Fidelity: the harness must exercise the server's own code path

Two configuration gaps have produced benchmark rows that are wrong rather than
merely stale. Both are the same shape — a harness default that quietly differs
from what the server does — and both are checkable.

1. **Index dimension.** The store builds `ArrowHNSWConfig` without `Dims` and lets
   the first ingested record establish it (`internal/store/store_ingest.go`). Every
   in-process index test sets `cfg.Dims` explicitly, so a dimension-sensitive
   defect is invisible to the whole Go suite and only appears in a server run.
   Any new configuration the store derives at runtime rather than from the config
   needs an in-process test that reproduces the store's shape. For TurboQuant this
   was worth 8.2% → 99.2% reachability (§ `docs/performance.md`).
2. **Effective HNSW parameters.** The harness sets both `LONGBOW_MAX_M0` and
   `LONGBOW_HNSW_MMAX0`. Only the latter is read by
   `types.DefaultArrowHNSWConfig`; `LONGBOW_MAX_M0` is a cap applied by
   `insertion_core.go` during *adaptive M* growth and does nothing on its own.
   Read the value back from the server (`/metrics`, or the startup log) before
   assuming a parameter took effect.

A benchmark row is a claim about a configuration. If the configuration under
which it was taken cannot be reconstructed from the row, the row cannot gate
anything, and a `>10%` delta against it is not evidence of a regression.

### 4.3 Benchmarking Observations & Hardware Hotspot Analysis

Profiling on AVX-512 hardware revealed key architectural hotspots that guide optimization priorities (documented in `docs/roadmap.md`):

1. **BM25 Inverted Index Heap Churn**: `(*BM25InvertedIndex).Add` accounts for ~60% of total heap allocations during bulk ingest due to copy-on-write map cloning (`MapRCU`) and frequent slice resizing.
2. **Redundant Square Root in L2 Distance**: AVX-512 integer distance kernels in `dispatch.go` previously invoked `euclideanInt8AVX512` (`VSQRTPS`) followed by squaring (`d * d`), adding unnecessary square-root overhead compared to direct accumulator addition.
3. **Lock Contention in Concurrent Index Linking**: Mutex serialization in `AddConnectionsBatch` accounts for significant lock delay under multi-worker bulk indexing.
4. **SlabArena / TypedArena Dereferencing**: High-frequency hop traversal in HNSW incurs pointer arithmetic and generation validation overhead on each candidate evaluation.
5. **Cross-Architecture Batch Kernel Invariant**: Batched SIMD kernels must maintain exact floating-point reduction tree parity with single-vector kernels across both AVX2 and AVX-512 to avoid tie-breaking or routing divergences in HNSW beam search.

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

# On local AVX2 client (pinned to 4 P-cores):
python3 scripts/ab_benchmark.py \
    --baseline-binary  /tmp/baseline/longbow \
    --candidate-binary /tmp/candidate/longbow \
    --counts 50000 --dtypes float32 --search-modes dense \
    --queries 500 --reps 6 --threshold 5 --cpu-affinity 12-15

# On AVX-512 cloud server (unpinned, homogeneous vCPUs):
python3 scripts/ab_benchmark.py \
    --baseline-binary  /tmp/baseline/longbow \
    --candidate-binary /tmp/candidate/longbow \
    --counts 50000 --dtypes float32 --search-modes dense \
    --queries 500 --reps 6 --threshold 5
```

`--reps` must be at least 3 or the harness exits 2. It reports one of three
verdicts - `regression`, `improvement`, `inconclusive` - and `inconclusive` is
a real answer, not a pass: it distinguishes "consistently below threshold but
too small to call" from "inconsistent, so the noise floor is above the effect".
An `inconclusive` result on a change you believe matters means the harness was
not given enough reps, not that the change is safe. Note that on server machines with multiple vCPUs, `--cpu-affinity` should be omitted as core pinning is neither necessary nor beneficial.

### 5.3 Measuring the noise floor before believing a delta

Pass the *same* binary as both arms. That answers a question `--compare-baseline`
cannot: how large a swing this host produces with no change at all, which is the
yardstick every other delta has to be read against.

```bash
python3 scripts/ab_benchmark.py \
    --baseline-binary bin/longbow_main --candidate-binary bin/longbow_main \
    --counts 100000 --dtypes int32,uint64,float32 \
    --search-modes dense,byid,temporal,sparse,filteredstring,geo \
    --queries 500 --workers 4 --reps 3 --threshold 10 --cpu-affinity 12-15 --seed 42
```

Measured on the AVX2 client (2026-10-12, 3 pairs, 18 rows): every median landed
within ±3.9%, and every row came back `inconclusive` — but individual pairs
ranged from −31% to +41%. The median is not the noise floor; the spread is. A
single-run comparison against a recorded baseline that shows a 10–30% drop is
consistent with this spread and is not evidence of anything on its own.

Note what this does *not* excuse: it excuses a delta whose magnitude is inside
the spread. A TurboQuant QPS row that was 4× too high while the graph was 92%
unreachable is not noise, and no number of reps would have hidden it. Reachability
and recall are the checks that catch a fast-but-wrong build, and they are exact.


---

## 6. Maintenance & Artifact Hygiene

- Temporary profiling `.pprof` files and graph dumps (`.bin`) must not be committed to version control.
- Only summary performance metrics, consolidated logs, and documentation (`docs/performance.md`, `docs/roadmap.md`) shall be tracked in git.
- Test artifacts and binaries must be periodically cleared using the project clean guidelines.
- `data/perf_logs/`, `data/bench/` and `scratch/perfruns/` are run output, not
  baselines. Clear them between runs: `unified_benchmark.py` keys its result files
  on `<mode>_<dtype>_<dim>_<count>` and *not* on the variant, so a standard run
  and an EMLGo run at the same configuration overwrite each other's server logs and
  per-dtype result files. The `perf_matrix_<label>_<timestamp>.json` reports survive
  only because `--label` differs between the two runs; archive them explicitly if
  they matter.
- `benchmarks/baseline_matrix.json` is the consolidated multi-variant baseline
  written by `analyze_benchmark_matrix.py`. It records the `perf_matrix_*` files it
  was built from in `source_files`, and it is worthless once those files are gone
  — it does not fail, it just stops describing anything reproducible. Regenerate it
  in the same pass that produces `docs/performance.md`, or delete it.
