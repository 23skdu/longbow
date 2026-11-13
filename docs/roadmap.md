# Longbow Deep Code Analysis: 20 Performance & Accuracy Improvements, Quality Gates, and Verification Roadmap

This document supersedes the previous roadmap with a new set of **20 source-grounded performance and numerical accuracy improvements**, an expanded **Unit & Fuzz Testing architecture**, mandatory **Accuracy Gate (`accuracy-gate`)** and **Oracle Gate (`oracle-gate`)** code quality gates, and a unified **`Makefile` + `scripts/verify.sh` (`make makehooks`)** verification workflow.

---

Completed P0-W WASM/ONNX modernization items have been removed.

## P0 Performance Recommendations from 2026-11-07 Matrix (Memory & Throughput)

Derived from `docs/performance.md` (§0a 2026-11-07 multi-dimension validation). These are P0 blockers because they represent >3x regressions or >10 GB memory footprints at 500k scale.

| ID | Priority | Subsystem & Location | Recommendation | Target Impact |
| :--- | :--- | :--- | :--- | :--- |
| **P0-F** | **P0 Blocker** | `internal/store/index/arrow_hnsw_compute_uint32.go` (suspected) | `uint32` dense QPS collapses to **518.1 QPS** at 500k 128d while `uint16`/`uint64` deliver 3,454/1,360 QPS; investigate dispatch/table lookup regression | Restore uint32 to ~3,000+ QPS |
| **P0-G** | **P0 Blocker** | `internal/simd/complex_batch.go` | `complex128` 500k 384d dense QPS is **951.7 QPS** with 413.5s index time and 11.5 GB RSS; implement tiled 4-way complex batch kernels (P0-B) and zero-copy extraction (P0-E) | 2.0x–3.0x QPS, 5.4x index time (as seen at 100k) |
| **P0-H** | **P0 Blocker** | `internal/store/geo_search.go` | `geo` search is **12–35 QPS** at 500k due to O(N) R-tree scan; add spatial cell/annulus early termination for small k | 10x–25x geo QPS |
| **P0-I** | **P0 Blocker** | `internal/store/index/turboquant.go` | TurboQuant polar reconstruction consumes 47.8% of distance time and index time is 429s at 500k; precompute Sincos LUT and batch reconstruction | 2x encode/index throughput |
| **P0-J** | **P0 Blocker** | `internal/store/index/arrow_hnsw_insert.go` | `float64` 500k 384d index time is 433.5s and dense QPS 1,408; batch `selectNeighbors` via `EuclideanDistanceFloat64Batch` (P0-A) | 2.5x–3.5x index construction |
| **P0-K** | **P0 Blocker** | `internal/store/index/search_string*.go` | `filteredstring` for `complex128` drops to 436 QPS at 500k 384d; optimize string filter fast path | 2x filtered-string QPS |
| **P0-L** | **P0 Blocker** | `internal/store/index/arrow_hnsw_memory.go` | `complex128` RSS 11.5 GB at 500k; cap capacity growth and reuse arenas (P0-E) | ~3 GB saved |

---

## 1. Executive Summary & Immediate P0 Fixes Landed

Completed P0 fixes have been removed from this roadmap; only outstanding work remains below.

### New P0 Blocker Improvements Identified from 100k / 250k / 500k Multi-Dimension Benchmarking (2026-11-07)

| ID | Priority | Subsystem & Location | Blocker Summary | Target Impact |
| :--- | :--- | :--- | :--- | :--- |
| **P0-A** | **P0 Blocker** | `internal/store/index/arrow_hnsw_insert.go:306-374` | `selectNeighbors` for `Float64`, `Complex64`, and `Complex128` executes an $O(M^2)$ scalar distance diversity loop on every node during HNSW construction (`273s–433s` index time at 250k–500k 384d) instead of batched SIMD (`EuclideanDistanceFloat64Batch` / `EuclideanDistanceComplex64Batch` / `EuclideanDistanceComplex128Batch`) | 2.5x–3.5x faster 384d HNSW index construction for 64-bit & 128-bit types |
| **P0-B** | **P0 Blocker** | `internal/simd/complex_batch.go:113-226` | `EuclideanDistanceComplex128Batch`, `DotProductComplex128Batch`, and `CosineDistanceComplex128Batch` allocate/populate `f64Vecs` and loop one vector at a time calling single-vector `EuclideanDistanceFloat64` instead of 4-way tiled AVX2/AVX-512 batch kernels (`951.7 QPS` at 500k 384d) | 2.0x–3.0x higher `complex128` 384d search QPS & lower index time |
| **P0-C** | **P0 Blocker** | `internal/store/geo_search.go:330-370` | `GeoIndex.SearchRadius` performs an R-tree bounding-box scan returning $O(N)$ candidates at 250k–500k (`11.8–28.0 QPS`, P50 `263–563ms` at 500k) without spatial cell/annulus early termination when `k` is small (`k=10`) | 10x–25x faster `geo` search QPS at 250k–500k scale |
| **P0-D** | **P0 Blocker** | `internal/store/index/arrow_hnsw_insert.go:283-374` | `selectNeighbors` allocates `extracted := make([]any, len(candidates))` and `selectedVecs := make([]any, 0, m)` boxing slice headers into `any` on every node/prune call for all non-`float32` types | Zero-allocation typed scratch buffers on `ArrowSearchContext` for all 15 dtypes |
| **P0-E** | **P0 Blocker** | `internal/store/dataset.go:490-497`, `internal/store/store_ingest.go:802-803` | `Dataset.Records` retains uncompressed Arrow `RecordBatch`es in RAM alongside `GraphData`'s typed `SlabArena`s when `SharedVectorSpace` is false, doubling vector memory footprint at 500k 384d (`11.5 GB` RSS for `complex128`) | Halves steady-state vector memory footprint (~3.1 GB saved at 500k 384d `complex128`) |

---

## 2. Top 20 Performance & Accuracy Improvements

### Summary Matrix

| # | Category | Subsystem & Location | Issue Summary | Target Impact | Gate |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **9** | Accuracy & Perf | `internal/store/index/ivf_opq_index.go:304-328, 409-454, 488-490, 800-839` | `IVFOPQIndex.SearchBatch` passes cluster ID as VectorID in `refineBatch`; `SearchVectorsWithBitmap` uses scalar loop + full sort instead of `ADCDistanceBatch` + top-K heap | Fixes batch recall corruption & 4x faster filtered IVF-OPQ search | `oracle-gate` |
| **10** | Accuracy & Perf | `internal/store/index/scalar_quantization.go:208-223, 263-308` | `SQ8Encoder.EncodeInto` truncates via `uint8(normalized)` instead of nearest rounding `uint8(normalized + 0.5)` and uses scalar distance loops | Halves max quantization bias (`-0.5 LSB` -> `0 LSB`) + SIMD SQ8 distance | `oracle-gate` |
| **11** | Performance | `internal/pq/opq.go:122-190` | `OPQEncoder.Encode`, `Decode`, and `RotateVector` convert `float32` <-> `float64` and allocate two `gonum/mat.VecDense` per call | 8x faster OPQ rotation, 0 heap allocations in steady state | `mem-bench` |
| **12** | Accuracy & Perf | `internal/pq/kmeans.go:80-102, 160-188` | `TrainKMeans` uses uniform random centroid seeding (`rand.Perm`) and `MetricEuclidean` (`sqrt` per centroid in E-step) instead of K-Means++ and `FindNearestCentroid` | 15-25% lower PQ distortion + 3x faster codebook training | `oracle-gate` |
| **13** | Performance | `internal/tensor/ops.go:174-208` | `contractNumeric` allocates `cIndices := make([]int, len(aAxes))` on every outer iteration and recomputes `totalIter` and `div/mod` inside the inner loop | Zero-alloc inner contraction loop + SIMD `DotProduct` fast path for 2D `MatMul` | `mem-bench` |
| **15** | Accuracy | `internal/store/hnsw_temporal.go:70-136` | `TemporalHNSWIndex.SearchAsOf`, `SearchRange`, and `SearchSlidingWindow` return raw B-tree IDs with `Score: 0` without executing HNSW vector search | Full temporal-filtered ANN search with exact distance scores | `oracle-gate` |
| **16** | Performance | `internal/store/index/bm25_arena.go:49, 100-140, 222-286` | `BM25ArenaIndex` leaves `postingArena` unused, allocates per-doc maps, scans postings in `O(P^2)`, and stores all TFs in a global `map[uint64]uint32` | Arena-backed contiguous postings + `simd.BM25ScoreBatch` (5x faster scoring) | `mem-bench` |
| **17** | Performance | `internal/store/index/bm25_inverted_index.go:588-634` | `BM25InvertedIndex.SearchBM25` recomputes `blockMaxScores` over the entire posting list on every multi-term query instead of caching per-block max TF/min DocLen | Eliminates `O(N_postings)` pre-scan per query in Block-Max WAND | `perf-check` |
| **19** | Performance | `internal/store/store.go:161`, `internal/store/cluster/cdc.go:158, 182-186` | `VectorStore.hnsw2Config` is unwired during dataset creation and `ChangeDataCapture` bypasses its initialized `batchAggregator` | Enables HNSW2 dataset config propagation and batched CDC replication | `test-short` |
| **20** | Accuracy | `internal/adbc/statement.go:358-410` | `Statement.executeScan` synthesizes a dummy query vector and runs `SearchHybrid` returning `(id, score)` instead of iterating dataset `RecordBatch`es matching `ExecuteSchema` | Schema-consistent Arrow `RecordBatch` table scan in ADBC driver | `test-adbc` |

---

### Detailed Specification of the 20 Improvements

#### Improvement 9: Fix `IVFOPQIndex.SearchBatch` ID Mixup and Vectorize `SearchVectorsWithBitmap`

- **Location**: `internal/store/index/ivf_opq_index.go:304-328, 409-454, 488-490, 800-839`
- **Root Cause**: In `IVFOPQIndex`, `refineBatch` passes coarse `clusterDist.id` (cluster index) to `decodeVector` as a `VectorID`, and `computeResidualScore` treats `queryIdx` (`0..B-1`) as a `VectorID` and compares against a zero slice `make([]float32, idx.dim)`. Meanwhile, `SearchVectorsWithBitmap` evaluates candidates with a scalar loop and `sort.Slice` over all cluster items instead of `ADCDistanceBatch` and a bounded top-K max-heap.
- **Remediation**: Rewrite `SearchBatch` to probe `nprobe` clusters per query using `ADCDistanceBatch` and refine top-R vector IDs against the actual rotated query vector; vectorize `SearchVectorsWithBitmap` with `ADCDistanceBatch` and a bounded `k`-heap.

#### Improvement 10: Eliminate Truncation Bias and Vectorize Distance Kernels in `SQ8Encoder`

- **Location**: `internal/store/index/scalar_quantization.go:208-223, 263-308`
- **Root Cause**: `SQ8Encoder.EncodeInto` converts `normalized` via `uint8(normalized)` (floor truncation), introducing a systematic `-0.5 * invScale` bias per dimension, and `SQ8EuclideanDistance`, `SQ8DistanceFast`, and `SQ8CosineDistance` execute scalar per-element loops.
- **Remediation**: Round to nearest integer via `uint8(normalized + 0.5)` (matching `QuantizeSQ8Adaptive`) and dispatch uniform/isotropic SQ8 distances through `simd.EuclideanDistanceSQ8` and `simd.DotDistanceSQ8`.

#### Improvement 11: Replace `gonum/mat` Float64 Conversions in `OPQEncoder` with Flat Float32 SIMD MatMul

- **Location**: `internal/pq/opq.go:122-190`
- **Root Cause**: Every `OPQEncoder.Encode`, `Decode`, and `RotateVector` call converts `[]float32` to `[]float64`, allocates two `mat.VecDense` wrappers, runs float64 matrix-vector multiplication, and converts back to `[]float32`.
- **Remediation**: Store pre-transposed `[]float32` rotation matrices `R_flat` and `R_T_flat` (`dims * dims`) on `OPQEncoder` after training and compute rotations via `simd.MatMul` or `simd.DotProduct` into pooled scratch buffers with zero heap allocations.

#### Improvement 12: Upgrade `pq.TrainKMeans` to K-Means++ Initialization and `simd.FindNearestCentroid`

- **Location**: `internal/pq/kmeans.go:80-102, 160-188`
- **Root Cause**: `TrainKMeans` initializes centroids via uniform random permutation (`rand.Perm(n)[:k]`) and configures `GetKernel[float32](MetricEuclidean, dims)` in `runCPUEstep`, computing an unnecessary `math.Sqrt` on every `(vector, centroid)` pair instead of using `simd.FindNearestCentroid` (`L2Squared`).
- **Remediation**: Implement $D^2$-weighted K-Means++ initialization and replace the scalar centroid loop in `runCPUEstep` with `simd.FindNearestCentroid`.

#### Improvement 13: Eliminate Inner-Loop Slice Allocation and Modulo Division in `tensor.contractNumeric`

- **Location**: `internal/tensor/ops.go:174-208`
- **Root Cause**: `contractNumeric` allocates `cIndices := make([]int, len(aAxes))` on every outer free-index iteration and recomputes `totalIter` and `rem % dim` / `rem / dim` inside the innermost contraction loop.
- **Remediation**: Pre-allocate `cIndices` and stride tables outside the outer loop, increment offsets with odometer carry addition, and add a direct `simd.MatMul` fast path for rank-2 `float32` contractions.

#### Improvement 15: Wire HNSW Vector Similarity Search into `TemporalHNSWIndex`

- **Location**: `internal/store/hnsw_temporal.go:70-136`
- **Root Cause**: `TemporalHNSWIndex.Add` inserts vectors into `thi.hnsw`, but `SearchAsOf`, `SearchRange`, and `SearchSlidingWindow` never query `thi.hnsw`—they return the first `k` IDs from `temporalTree` with `Score: 0`, and `Delete` only deletes from `temporalTree`.
- **Remediation**: Build a roaring bitmap of valid IDs from `temporalTree` for the requested timestamp/window and pass that bitmap into `thi.hnsw.SearchVectorsWithBitmap(ctx, vector, k, bitset, options)`.

#### Improvement 16: Refactor `BM25ArenaIndex` to Use `postingArena` and `simd.BM25ScoreBatch`

- **Location**: `internal/store/index/bm25_arena.go:49, 100-140, 222-286`
- **Root Cause**: `BM25ArenaIndex` initializes `postingArena` (`memory.NewTypedArena[uint32]`) but never uses it; `IndexDocuments` allocates a map per document, scans `postings` linearly to deduplicate (`O(P^2)`), and stores all term frequencies in a single giant `map[uint64]uint32`.
- **Remediation**: Store parallel `(docID, tf)` slices in `postingArena` sorted by `docID` and evaluate posting blocks with `simd.BM25ScoreBatch`.

#### Improvement 17: Cache Block-Max Scores on `BM25PostingList` in `BM25InvertedIndex`

- **Location**: `internal/store/index/bm25_inverted_index.go:588-634`
- **Root Cause**: `BM25InvertedIndex.SearchBM25` allocates and recomputes `blockMaxScores` by scanning every posting's `TF` and `DocLen` on every multi-term query.
- **Remediation**: Precompute per-128-doc block `(maxTF, minDocLen)` metadata on `BM25PostingList` during `AddBatch` / compaction so multi-term Block-Max WAND computes block upper bounds in `O(N_blocks)` instead of `O(N_postings)`.

#### Improvement 19: Wire `VectorStore.hnsw2Config` and `ChangeDataCapture.batchAggregator`

- **Location**: `internal/store/store.go:161`, `internal/store/cluster/cdc.go:158, 182-186`
- **Root Cause**: `VectorStore.hnsw2Config` is stored on `VectorStore` but never applied when initializing dataset indexes, and `ChangeDataCapture` creates `batchAggregator` in `NewChangeDataCapture` but `RecordChange` bypasses it when `len(batches) == 1`.
- **Remediation**: Apply `s.hnsw2Config` during dataset index creation in `VectorStore` and route single-batch CDC events through `cdc.batchAggregator.AddBatch` when batch aggregation is enabled.

#### Improvement 20: Implement Real Arrow `RecordBatch` Iteration in ADBC `Statement.executeScan`

- **Location**: `internal/adbc/statement.go:97-110, 358-410`
- **Root Cause**: `Statement.ExecuteSchema` returns the dataset's full Arrow schema (`ds.GetSchema()`), whereas `Statement.executeScan` fabricates a dummy query vector `queryVec[i] = 1.0 / float32(i+1)` and runs `vs.SearchHybrid`, returning a 2-column `(id, score)` record that contradicts `ExecuteSchema`.
- **Remediation**: Iterate the dataset's stored Arrow `RecordBatch` slices under `ds.RLock()`, retaining and streaming native dataset records (with optional row limit) through an `array.RecordReader`.

---

## 3. Expanded Unit & Fuzz Testing Scope

Longbow enforces **100% per-package test file presence** across all Go packages in `cmd/`, `internal/`, `client/`, and `tools/`, coupled with deterministic unit tests and property-based fuzz targets (`Fuzz*`). Fuzz coverage is likewise enforced at **100% package presence**: every package must define at least one `Fuzz*` target, verified by `scripts/check_fuzz_coverage.sh` (wired into `make fuzz-coverage`, `make test-fuzz`, `make ci`, pre-commit Gate 3, and the CI `validate` job).

### Fuzz Testing Matrix

| Package | Fuzz Target | Invariants & Oracle Properties Verified |
| :--- | :--- | :--- |
| `internal/simd` | `FuzzBatchDistancesOracleParity` | Fuzzes `EuclideanDistanceBatch`, `L2SquaredDistanceBatch`, and `DotProductBatch` across dims `{16, 128, 384, 768, 1024}` against a 64-bit `float64` reference oracle |
| `internal/simd` | `FuzzDequantizeL2OracleParity` | Fuzzes `DequantizeL2Float32Uint8` across arbitrary `minV`, `scale`, and lengths `8..512` against `dequantL2Float32Uint8Generic` |
| `internal/simd` | `FuzzSQ8AdaptiveOracleParity` | Fuzzes `CalibrateSQ8Channels`, `QuantizeSQ8Adaptive`, `DequantizeSQ8Adaptive`, and `EuclideanDistanceSQ8Adaptive` self-distance identity |
| `internal/simd` | `FuzzDistances` | Fuzzes single-vector `EuclideanDistance`, `CosineDistance`, and `DotProduct` for non-negativity, symmetry, and NaN/Inf safety |
| `internal/simd` | `FuzzDotInt4` | Fuzzes packed 4-bit integer dot product kernels against unpacked scalar reference |
| `internal/simd` | `FuzzTurboQuantDistanceBatch` | Fuzzes `TurboQuantDistanceBatch` across 2-bit, 4-bit, and 8-bit codebooks against scalar `TurboQuantDistance` |
| `internal/pq` | `FuzzPQEncodeDecodeADCOracleParity` | Fuzzes `PQEncoder.Encode`, `Decode`, `BuildADCTable`, and `ADCDistance` against explicit `Decode` + `float64` L2-squared oracle |
| `internal/pq` | `FuzzPQEncoder_TrainAndEncode` | Fuzzes K-Means PQ codebook training and encoding across arbitrary input distributions |
| `internal/tensor` | `FuzzTensorContractOracleParity` | Fuzzes 2D `MatMul` / `TensorContract` against a 64-bit `float64` triple-loop matrix multiplication oracle |
| `internal/store/index` | `FuzzSQ8EncoderOracleParity` | Fuzzes `TrainSQ8Encoder`, `Encode`, `Decode`, and round-trip reconstruction error bounds |
| `internal/store/index` | `FuzzTurboQuantEncodeDecode` / `FuzzTurboQuantCompression` | Fuzzes polar coordinate transform, angle packing, QJL residual bit packing, and `DecodeInto` reconstruction |
| `internal/store/index` | `FuzzHNSWSearch` / `FuzzPackedAdjacency` | Fuzzes HNSW insertion/search invariants and packed adjacency serialization round-trips |
| `internal/query` | `FuzzFilterEvaluator_MatchesAll` / `FuzzZeroAllocTicketParser_Parse` | Fuzzes JSON/structured filter expression parsing and request validation against malformed inputs |
| `internal/memory` | `FuzzSlabArena` | Fuzzes concurrent and sequential `SlabArena` allocations, alignments, and slice header safety |

---

## 4. Code Quality Gates: Accuracy Gate & Oracle Gate

### 4.1 Accuracy Gate (`make accuracy-gate`)

The **Accuracy Gate** (`go test -v ./internal/simd/ -run='^TestSIMDAccuracy'`) enforces strict numerical precision bounds across both `Standard` and `EMLGo` math backends:

- **`TestSIMDAccuracy_All15DTypes`**: Validates `EuclideanDistance`, `L2Squared`, `DotProduct`, and `CosineDistance` across all **15 supported vector data types** (`Float32`, `Float64`, `Float16`, `Complex64`, `Complex128`, `Int8`, `Int16`, `Int32`, `Int64`, `Uint8`, `Uint16`, `Uint32`, `Uint64`, `TurboQuant4`, `TurboQuant8`) at dimensions `128, 384, 768`:
  - `Float64` relative tolerance: $\le 10^{-6}$
  - `Float32` relative tolerance: $\le 10^{-5}$
  - `Float16` relative tolerance: $\le 2 \times 10^{-3}$
  - Integer types (`Int8..Uint64`): exact parity within `float32` representation ($\le 10^{-5}$)
  - `TurboQuant4` / `TurboQuant8`: bounded angular reconstruction error ($\le 0.45\text{ rad}$ for TQ4, $\le 0.05\text{ rad}$ for TQ8)
- **`TestSIMDAccuracy_BatchOperationsAcrossDims`**: Validates `EuclideanDistanceBatch`, `EuclideanDistanceBatchFlat`, `L2SquaredDistanceBatch`, `DotProductBatch`, and `CosineDistanceBatch` across dimensions `{64, 128, 384, 768, 1024, 1536, 3072}`.
- **`TestSIMDAccuracy_DispatchLayersParity`**: Validates that layered dispatch tables (`newGenericDispatch` $\to$ `layerAVX2` $\to$ `layerAVX512`) agree on `Sum`, `Max`, `Min`, `ArgMax`, `ArgMin`, and `BrayCurtisDistance`.
- **`TestSIMDAccuracy_NumericalIdentityEdgeCases`**: Validates exact `0.0` self-distance invariants.

### 4.2 Oracle Gate (`make oracle-gate`)

The **Oracle Gate** (`go test -v ./internal/simd/... ./internal/pq/... ./internal/tensor/... ./internal/memory/... ./internal/store/index/ -run='Parity'`) verifies every hardware-accelerated kernel and compressed index path against a simple, unoptimized **64-bit `float64` or pure-Go scalar reference oracle**:

- **SIMD Batch & Dequantization Parity** (`TestOracleParity_AllBatchKernels`): Compares all batch distance kernels, `DequantizeL2Float32Uint8`, and `DequantizeL2Uint8Uint8` against `float64` scalar loops across dimensions `{17, 64, 128, 256, 384, 512, 768, 1024, 1536, 3072}`.
- **TurboQuant Pack/Unpack & Distance Parity** (`TestPackTQ2AVX2MatchesGeneric`, `TestPackTQ4AVX2MatchesGeneric`, `TestPackTQ2AVX512MatchesGeneric`, `TestPackTQ4AVX512MatchesGeneric`, `TestPackTQ8AVX512MatchesGeneric`, `TestTurboQuantDistanceBatchParity`): Requires bit-identical packing codes and exact distance parity between AVX2/AVX-512/NEON and generic scalar implementations.
- **Product Quantization ADC Parity** (`TestPQOracleParity_ADCvsDecodedL2Squared`): Verifies that `PQEncoder.ADCDistance` and `ADCDistanceBatch` match explicit `Decode()` followed by `float64` L2 distance.
- **Tensor Contraction Parity** (`TestTensorOracleParity_ContractAndTranspose`, `TestEinsumOracleParity`): Verifies tensor contractions and Einstein summations against `float64` reference accumulators.
- **SQ8 Quantization Parity** (`TestSQ8OracleParity_EncodeDecodeDistance`): Verifies trained SQ8 encoding/decoding and distance bounds against `float64` references.

---

## 5. Makefile Targets & `scripts/verify.sh` Pre-Commit Hook

### 5.1 Git Pre-Commit Hook (`scripts/verify.sh` & `make makehooks`)

Install the Git pre-commit hook into `.git/hooks/pre-commit` with:

```bash
make makehooks
```

Every `git commit` (or manual `make verify` / `./scripts/verify.sh` invocation) executes 6 sequential verification gates:

1. **Gate 1 — Formatting & Linting**: Checks `gofmt -l` on staged Go files, runs `go vet`, and runs `golangci-lint run` (`staticcheck`, `unused`) on affected packages.
2. **Gate 2 — Go Security Check**: Runs `gosec -quiet -exclude=G115` AST security analysis on affected packages.
3. **Gate 3 — 100% Package Test & Fuzz Presence**: Verifies zero Go packages report `[no test files]` and runs `./scripts/check_fuzz_coverage.sh` to ensure every package defines at least one `Fuzz*` target.
4. **Gate 4 — Unit & Fuzz Testing (Short Mode)**: Runs `go test -short -cover` and `go test -short -run='^Fuzz'` across staged and core packages.
5. **Gate 5 — Accuracy Gate**: Runs `go test -v ./internal/simd/ -run='^TestSIMDAccuracy'`.
6. **Gate 6 — Oracle Gate**: Runs `go test -v ./internal/simd/... ./internal/pq/... ./internal/tensor/... ./internal/memory/... ./internal/store/index/ -run='Parity'`.

### 5.2 Reference of Makefile Verification Targets

| Category | Target | Command / Purpose |
| :--- | :--- | :--- |
| **Git Hooks** | `make makehooks` | Installs `scripts/verify.sh` as executable `.git/hooks/pre-commit` hook (aliases: `make install-hooks`, `make install-commit-hook`) |
| **Git Hooks** | `make verify` | Executes `./scripts/verify.sh` (alias: `make pre-commit`) |
| **Security Checks** | `make security` | Runs full Go security suite (`sec-gosec` + `sec-vulncheck`) |
| **Security Checks** | `make sec-gosec` | Runs `gosec -quiet -exclude=G115 ./...` AST security scanner |
| **Security Checks** | `make sec-vulncheck` | Runs `govulncheck ./...` against the Go vulnerability database |
| **Security Checks** | `make sec-audit` | Audits module dependencies and runs `govulncheck` |
| **Perf & Memory Profiling** | `make perf-check` | Runs CPU & allocation throughput benchmarks on SIMD and quantization hot paths |
| **Perf & Memory Profiling** | `make cpu-profile` | Generates `cpu.out` CPU profile from SIMD accuracy workloads |
| **Perf & Memory Profiling** | `make profile-check` | Generates `mem.out` and `cpu.out` and prints top-10 `go tool pprof` summaries |
| **Perf & Memory Profiling** | `make mem-check` | Runs compiler escape analysis (`mem-escape`) + bounded arena/SIMD allocation benchmarks |
| **Perf & Memory Profiling** | `make mem-bench` | Runs `-benchmem` benchmarks across `internal/store`, `internal/simd`, and `internal/memory` |
| **Perf & Memory Profiling** | `make mem-escape` | Runs `go build -gcflags="-m -m" ./internal/memory/...` escape & inlining analysis |
| **Perf & Memory Profiling** | `make mem-profile` | Runs index tests producing `mem.out` and `cpu.out` pprof profiles |
| **Perf & Memory Profiling** | `make mem-asan` | Runs `internal/memory/...` tests under Clang AddressSanitizer (`-asan`) |
| **Perf & Memory Profiling** | `make mem-valgrind` | Runs ADBC C-shared library (`liblongbow_adbc.so`) under Valgrind `--leak-check=full` |
| **Linting Checks** | `make lint` | Runs full `golangci-lint run ./...` across the repository |
| **Linting Checks** | `make lint-fast` | Runs `golangci-lint run` on core critical packages |
| **Linting Checks** | `make lint-fix` | Runs `golangci-lint run --fix ./...` |
| **Linting Checks** | `make vet` | Runs `go vet ./...` |
| **Linting Checks** | `make staticcheck` | Runs `golangci-lint run -E staticcheck,unused ./...` |
| **Linting Checks** | `make deadcode` | Runs `deadcode ./cmd/... ./internal/...` |
| **Linting Checks** | `make fmt-check` | Verifies all Go files are formatted with `gofmt` |
| **Linting Checks** | `make markdown-lint` | Lints `docs/**/*.md`, `README.md`, and `CONTRIBUTING.md` with `markdownlint` |
| **Quality & Fuzz Gates** | `make accuracy-gate` | Runs `TestSIMDAccuracy*` across all 15 vector dtypes, batch dims, and dispatch layers |
| **Quality & Fuzz Gates** | `make oracle-gate` | Runs `*Parity*` ground-truth oracle suites across `simd`, `pq`, `tensor`, `memory`, and `index` |
| **Quality & Fuzz Gates** | `make test-fuzz` | Runs all `Fuzz*` targets across the repository in bounded short mode (after `fuzz-coverage`) |
| **Quality & Fuzz Gates** | `make fuzz-coverage` | Verifies 100% package fuzz-target presence via `scripts/check_fuzz_coverage.sh` |
| **Quality & Fuzz Gates** | `make test-coverage` | Verifies 100% package test presence and runs `go test -short -cover ./...` |
| **Quality & Fuzz Gates** | `make coverage-gate` | Enforces $\ge 30\%$ statement coverage across core packages |

---

## P0 Feature Parity Gaps vs Top Vector Databases (Longbow Roadmap)

To maintain parity/superiority against Pinecone, Milvus, Qdrant, Weaviate, Chroma, pgvector, Redis, Elasticsearch/OpenSearch, LanceDB, and Vespa, Longbow must close the following feature gaps:

| ID | Priority | Gap | Competitor Parity | Target |
| :--- | :--- | :--- | :--- | :--- |
| **P0-P1** | **P0** | Managed cloud/SaaS offering & multi-region replication | Pinecone, TencentDB, Milvus Cloud | Longbow Cloud with SLA and global replication |
| **P0-P2** | **P0** | REST/HTTP and GraphQL APIs (currently gRPC/Arrow Flight + SDKs) | Weaviate, Qdrant, Elasticsearch | First-class REST/GraphQL endpoints |
| **P0-P3** | **P0** | RBAC, API keys, tenant isolation, audit logging | Pinecone, Qdrant, Milvus | Enterprise security layer |
| **P0-P4** | **P0** | Backup/restore and point-in-time recovery | Milvus, Vespa, Elasticsearch | Snapshot + PITR |
| **P0-P5** | **P0** | Streaming connectors (Kafka/Pulsar) and real-time CDC | Milvus, Vespa | Event-driven ingestion |
| **P0-P6** | **P0** | Multi-modal embedding pipelines (image/audio/video) | Weaviate, LanceDB | Native multi-modal support |
| **P0-P7** | **P0** | Advanced reranking (cross-encoder, LLM-based) and query rewriting | Weaviate, Pinecone | Pluggable reranker framework |
| **P0-P8** | **P0** | Serverless/edge SDKs and WASM client | Chroma, LanceDB | Browser/edge runtime |
| **P0-P9** | **P0** | Cost/usage metering and quotas | Pinecone, managed offerings | Usage tracking and limits |
| **P0-P10** | **P0** | Auto-scaling and elastic sharding | Milvus, Vespa | Dynamic cluster scaling |
| **P0-P11** | **P0** | Encryption at rest and in transit (TLS/mTLS) | All enterprise offerings | Full encryption |
| **P0-P12** | **P0** | Schema validation and constraints | pgvector, Elasticsearch | Strong schema enforcement |
| **P0-P13** | **P0** | Faceted search, highlighting, and aggregations | Elasticsearch/OpenSearch | Rich result presentation |
| **P0-P14** | **P0** | Explainability and vector provenance | Weaviate, Vespa | Score breakdowns and lineage |
