# Longbow Deep Code Analysis: 20 Performance & Accuracy Improvements, Quality Gates, and Verification Roadmap

This document supersedes the previous roadmap with a new set of **20 source-grounded performance and numerical accuracy improvements**, an expanded **Unit & Fuzz Testing architecture**, mandatory **Accuracy Gate (`accuracy-gate`)** and **Oracle Gate (`oracle-gate`)** code quality gates, and a unified **`Makefile` + `scripts/verify.sh` (`make makehooks`)** verification workflow.

---

## P0 Blockers: WASM/ONNX Modernization & Vector-Engine Integration (10-Part Plan)

The current WASM/ONNX stack (`internal/wasm`, `internal/onnx`, `internal/store/embedding_generator.go`, `internal/store/ml_reranker.go`) is functional but not production-hardened: the WASM runner uses an unchecked `malloc`/`free`/`inference` ABI with ignored errors and leaked output allocations, ONNX sessions guess input/output tensor names, tokenizers are resolved against the process CWD, and embedding/reranking paths invoke models one text at a time. The following ten P0 blockers modernize the implementation and its use across the vector engine. Each is a release blocker until closed.

### Summary Matrix

| # | ID | Subsystem & Location | Blocker Summary | Target Impact | Gate |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **P0-W1** | `internal/wasm/runner.go` | Unchecked WASM ABI: ignored `malloc`/`Write`/`Read` errors, no output free, no bounds/size limits, leaked runtime per model | Eliminates panics/OOMs and ≥2x steady-state throughput | `wasm-gate` |
| **2** | **P0-W2** | `internal/store/embedding_generator.go`, `internal/store/ml_reranker.go`, `internal/onnx` | No common ML backend interface; store wrappers hard-code `wasm`/`onnx` types and lazy-init races | Unified runner selection, graceful fallback, clean lifecycle | `test-short` |
| **3** | **P0-W3** | `internal/store/embedding_generator.go`, `internal/store/ml_reranker.go` | Per-text/per-doc inference loops (`InferenceWithTokens` per text, `Score` per doc) | 4x-16x embedding/rerank throughput via batching | `perf-check` |
| **4** | **P0-W4** | `internal/ml/tokenizer.go`, `internal/wasm`, `internal/onnx` | Tokenizer resolved against CWD (`vocab.txt`), no `tokenizer.json`/HF support, no asset validation | Correct tokenization for any model directory, no CWD dependence | `accuracy-gate` |
| **5** | **P0-W5** | `internal/onnx/onnx.go`, `internal/onnx/metal` | Hard-coded `input_ids`/`attention_mask`/`token_type_ids` and output names; no metadata introspection; tensor leaks | Robust ONNX loading across BERT/MiniLM/cross-encoder layouts | `onnx-gate` |
| **6** | **P0-W6** | `internal/wasm/runner.go`, `internal/onnx/download.go` | WASM runs with full WASI stdio and no module integrity/size checks; downloads have no checksums | Sandboxed, verifiable model execution and supply-chain safety | `security` |
| **7** | **P0-W7** | `internal/wasm`, `internal/onnx`, `internal/store` | One `wazero.Runtime` per model, coarse mutex, no pooling, no context cancellation propagation | Lower memory, higher concurrency, predictable shutdown | `mem-check` |
| **8** | **P0-W8** | `internal/metrics/metrics.go`, `internal/wasm`, `internal/onnx` | `model` label is full filesystem path (high cardinality); no load/error/batch metrics | Operable, low-cardinality observability and SLOs | `perf-check` |
| **9** | **P0-W9** | `internal/store` ingestion/search paths | ML inference not wired into ingestion auto-embed, rerank, or sparse (SPLADE) generation | End-to-end vector-engine use of local ML models | `test-integration` |
| **10** | **P0-W10** | `internal/wasm`, `internal/onnx`, `internal/store` | Sparse tests: only round-trip fuzz for WASM; no ONNX build-tag matrix, no golden-model accuracy tests | Regression-proof ML backends and CI coverage | `oracle-gate` |

### Detailed Specification of the 10 P0 Blockers

#### P0-W1: Harden the WASM Runner ABI and Resource Handling

- **Location**: `internal/wasm/runner.go:49-193`
- **Root Cause**: `Inference` ignores the error from `malloc.Call`, `mem.Write`, `mem.Read`, and `free.Call`; never frees the WASM-allocated output buffer; does not validate that `outputLen*4` fits in module memory; and `InferenceWithTokens` additionally ignores `malloc` errors and writes without checking success. `NewRunner` attaches `os.Stdout`/`os.Stderr` and creates a fresh runtime per model, and `Close` closes the whole runtime without module-level teardown. Malformed modules can panic on nil exports.
- **Remediation**: Define a strict runner contract (`malloc(size) -> ptr`, `free(ptr)`, `inference(...) -> (ptr,len)`), validate export signatures before use, check every `Call`/`Write`/`Read` result, enforce configurable `MaxInputBytes`/`MaxOutputBytes`, free output buffers in `defer`, make `Close` idempotent and module-scoped, remove stdio capture, and return typed errors (`ErrInvalidModule`, `ErrMemoryBounds`, `ErrInferenceFailed`). Add a compiled-module cache keyed by content hash.
- **Target Impact**: Eliminates nil-deref/OOM panics and output-buffer leaks; ≥2x steady-state throughput on repeated inference.
- **Gate**: `wasm-gate` (new).

#### P0-W2: Introduce a Unified ML Backend Interface

- **Location**: `internal/store/embedding_generator.go:249-266`, `internal/store/embedding_generator.go:595-805`, `internal/store/ml_reranker.go:17-126`
- **Root Cause**: `NewEmbeddingGenerator` switches on provider strings and constructs concrete `*localEmbeddingGenerator` wrappers; `wasmEmbeddingModel` and `onnxEmbeddingModel` each lazily create their runner/session inside `Inference` without synchronization, causing races and repeated initialization; `ONNXReranker` similarly lazy-inits. There is no single interface spanning embed + score + close, so runtime selection and fallback are scattered.
- **Remediation**: Define `MLBackend` (`Embed(ctx, []string) ([][]float32, error)`, `Score(ctx, string, []string) ([]float32, error)`, `Dimension() int`, `Close() error`) with `WASMBackend` and `ONNXBackend` implementations; centralize selection in a factory honoring `LONGBOW_ML_RUNNER` and build availability; initialize eagerly (or guarded by `sync.Once`) and expose health; make fallback from ONNX to WASM (and heuristic reranker) explicit and logged.
- **Target Impact**: Single configuration surface, deterministic lifecycle, no lazy-init races.
- **Gate**: `test-short`.

#### P0-W3: Batch Inference End-to-End in the Vector Engine

- **Location**: `internal/store/embedding_generator.go:765-797`, `internal/store/ml_reranker.go:98-122`
- **Root Cause**: `wasmEmbeddingModel.Inference` tokenizes and calls `InferenceWithTokens` once per text; `wasmModelWrapper.Score` loops per document; ONNX `Embed`/`Score` allocate full `numTexts*512` tensors but callers still chunk at `batchSize` with per-batch session reuse only. This underutilizes both runtimes and dominates ingestion/rerank latency.
- **Remediation**: Add `EmbedBatch`/`ScoreBatch` to the backend interface and implementations; have WASM backends pack token batches into a single module call (or an internal batched ABI), reuse pooled input/output buffers, and return `[][]float32`; update `localEmbeddingGenerator.Generate` and `ONNXReranker.Rerank` to submit whole batches; expose batch size/latency metrics.
- **Target Impact**: 4x-16x embedding and reranking throughput; fewer allocations.
- **Gate**: `perf-check`.

#### P0-W4: Model and Tokenizer Asset Resolution

- **Location**: `internal/ml/tokenizer.go:19-45`, `internal/store/embedding_generator.go:774-779`, `internal/store/ml_reranker.go:64`, `internal/onnx/onnx.go:134-145`
- **Root Cause**: Tokenizers are constructed from the literal relative path `"vocab.txt"`, so correctness depends on process CWD; there is no support for `tokenizer.json`/`tokenizer_config.json` downloaded by `DownloadModel`; model directories are not validated before use.
- **Remediation**: Resolve tokenizer assets relative to `filepath.Dir(modelPath)` with a search order (`tokenizer.json`, `vocab.txt`, configured path`); implement a minimal HuggingFace fast-tokenizer loader or reuse an existing library; validate required files at backend construction; cache tokenizers by model directory; update `DownloadModel` to fetch and verify checksums.
- **Target Impact**: Correct tokenization regardless of CWD; portable model directories.
- **Gate**: `accuracy-gate`.

#### P0-W5: Modernize ONNX Session Loading and Tensor I/O

- **Location**: `internal/onnx/onnx.go:79-148`, `internal/onnx/onnx.go:150-365`
- **Root Cause**: `NewSession` hard-codes candidate input/output names and silently keeps whichever subset succeeded; `Score`/`Embed` ignore `ort.NewTensor` errors, assume `outputs[0]` is `*ort.Tensor[float32]`, and may index nil outputs; pooling mode defaults are not derived from model metadata; Metal fallback is best-effort with swallowed errors.
- **Remediation**: Introspect model inputs/outputs via ONNX Runtime metadata (or a small sidecar) before session creation; validate tensor shapes and dtypes; handle tensor creation errors; select pooling mode from `config.json`/model metadata; unify Metal and ORT behind the backend interface; propagate provider fallback errors with context.
- **Target Impact**: Robust loading across model families; no nil-tensor panics; clearer failures.
- **Gate**: `onnx-gate` (new).

#### P0-W6: Secure WASM Sandboxing and Model Supply Chain

- **Location**: `internal/wasm/runner.go:33-36`, `internal/onnx/download.go:16-81`
- **Root Cause**: Modules are instantiated with default WASI and stdout/stderr attached; there is no module hash verification, size cap, or capability restriction; `DownloadModel` performs no checksum/integrity validation and treats any 404 as success.
- **Remediation**: Instantiate with a restricted `wazero.ModuleConfig` (no FS, no network, no env, no stdio), enforce module size caps, verify SHA-256 against a manifest/checksum file, add download retries and atomic file writes, and fail downloads when required files (`model.onnx`/`.wasm`) are missing.
- **Target Impact**: Safe execution of untrusted models and reproducible model acquisition.
- **Gate**: `security`.

#### P0-W7: Concurrency, Pooling, and Lifecycle Management

- **Location**: `internal/wasm/runner.go:19-24`, `internal/onnx/onnx.go:22-57`
- **Root Cause**: Each model creates its own `wazero.Runtime` and ONNX environment initialization is process-global with `sync.Once`; runner mutex serializes all inference; there is no warm-up, no pool, and no context-aware cancellation; `Close` is not safe to call repeatedly.
- **Remediation**: Introduce a shared runtime pool / backend cache with reference counting, per-backend mutexes or lock-free read paths, context propagation and cancellation checks, warm-up on load, and idempotent `Close`; expose lifecycle hooks for graceful shutdown.
- **Target Impact**: Lower memory footprint, better concurrency, predictable shutdown.
- **Gate**: `mem-check`.

#### P0-W8: Low-Cardinality Observability and Health

- **Location**: `internal/wasm/runner.go:54-62`, `internal/metrics/metrics.go:1199-1214`
- **Root Cause**: `WasmInferenceTotal`/`WasmInferenceDurationSeconds` use the full model path as the `model` label, creating unbounded cardinality; ONNX/WASM load duration, batch size, and error causes are not recorded; there is no loaded-model gauge or health check.
- **Remediation**: Key metrics by a stable model ID/name (not path), add load-duration, batch-size, error-cause, and in-flight gauges, and expose backend health (`ModelLoaded`, last error, warm-up status) through the store/metrics layer.
- **Target Impact**: Operable ML subsystem with bounded cardinality and actionable SLOs.
- **Gate**: `perf-check`.

#### P0-W9: Wire ML Backends into the Vector Engine

- **Location**: `internal/store/embedding_generator.go`, `internal/store/ml_reranker.go`, `internal/store/hybrid_search.go`
- **Root Cause**: Local ML is only reachable through explicit `EmbeddingConfig`/reranker construction; ingestion does not auto-embed text fields, search does not invoke rerankers unless manually configured, and sparse (SPLADE/BM25 expansion) generation is not connected to WASM/ONNX models.
- **Remediation**: Add dataset-level ML configuration (model path, provider, pooling, batch size), auto-embed on ingest when configured, invoke configured reranker in `ONNXReranker.Rerank` path from search, and add a sparse-vector generation hook using the same backend; cache and reuse generators per dataset.
- **Target Impact**: End-to-end use of local ML models for dense + sparse + rerank in the vector engine.
- **Gate**: `test-integration` (new).

#### P0-W10: Testing, Fuzzing, and Quality Gates for ML Backends

- **Location**: `internal/wasm/runner_test.go`, `internal/onnx/onnx_test.go`, `internal/onnx/onnx_fuzz_test.go`
- **Root Cause**: WASM tests only cover invalid paths and close; ONNX tests are build-tagged and mostly skip real inference; there are no golden-model accuracy tests, no tokenizer fuzz targets, no download validation tests for the real build, and no CI matrix across `onnx`/`!onnx`.
- **Remediation**: Add unit tests for ABI validation, bounds checks, tokenizer resolution, and batch inference; fuzz tokenizer encode/decode and download repo IDs; add golden-model regression tests for embedding dimensions and rerank ordering; run `go test` with and without `-tags onnx` in CI; introduce `wasm-gate` and `onnx-gate` make targets.
- **Target Impact**: Regression-proof ML backends and enforced coverage.
- **Gate**: `oracle-gate`.

---

## 1. Executive Summary & Immediate P0 Fixes Landed

During deep code analysis of the `internal/simd`, `internal/store/index`, `internal/store`, `internal/pq`, `internal/tensor`, `internal/memory`, and `internal/adbc` subsystems, 20 concrete performance and accuracy gaps were identified. Four critical P0 accuracy and dispatch regressions were fixed immediately and locked down by the expanded Accuracy and Oracle gates:

1. **Fixed `L2SquaredDistanceBatch` 1024-Dim Silent Truncation (`internal/simd/batch_operations.go`)**: `case 1024:` previously invoked `currentDispatch.L2SquaredDistance128(query, v)`, discarding 87.5% of the vector (`dims[128:1024]`). Fixed to dispatch `currentDispatch.L2SquaredDistance1024(query, v)`.
2. **Fixed Scalar 4-Way Loop Shadowing SIMD Batch Dispatch for Dims 128, 384, 768 (`internal/simd/batch_operations.go`)**: `EuclideanDistanceBatch`, `L2SquaredDistanceBatch`, and `DotProductBatch` previously intercepted `dims == 128, 384, 768` and routed them unconditionally to scalar Go loops (`euclideanDistanceBatch4Way` / `dotProductBatch4Way`), bypassing AVX2, AVX-512, NEON, and AMX kernels. Fixed to dispatch directly to `currentDispatch` dimension-specialized SIMD kernels whenever hardware acceleration is active.
3. **Fixed `layerAVX512` Clobbering 12 AVX2 Math & Reduction Kernels (`internal/simd/dispatch_tables.go`)**: `layerAVX512` copied the `avx2` dispatch struct and then overwrote `MatMul`, `BrayCurtisDistance`, `Sum`, `Max`, `Min`, `Sin`, `Cos`, `Sincos`, `Sqrt`, `Atan2`, `ArgMax`, and `ArgMin` back to `*Generic` scalar functions, and `layerAVX2` omitted `PackTQ2AVX2` and `PackTQ4AVX2`. Fixed so `layerAVX512` preserves AVX2 kernels and `layerAVX2` wires `PackTQ2AVX2` and `PackTQ4AVX2`.
4. **Fixed `DequantizeL2Uint8Uint8` Missing Square Root & `float32ToSQ8Computer` False-Zero Fallback (`internal/simd/dequant_distance.go`, `internal/store/index/arrow_hnsw_compute_quantized.go`)**: `DequantizeL2Uint8Uint8` multiplied the squared integer L2 accumulator from `EuclideanDistanceSQ8` by `scale` without taking `math.Sqrt`, and `float32ToSQ8Computer.ComputeSingle` returned `0.0` (`distFuncSquared(c.q, c.q)`) on missing chunks instead of `math.MaxFloat32` and used a scalar loop instead of `simd.DequantizeL2Float32Uint8`. Both are now fixed and covered by `TestOracleParity_AllBatchKernels`.
5. **Fixed `GraphData.PreAllocate` Skipping `Float16` Chunks & Concurrent `AddBatch` Snapshot Clobbering (`internal/store/types/graph_data_offheap.go`, `graph_data_clone.go`, `internal/store/index/arrow_hnsw_memory.go`)**: `PreAllocate` called `GrowMetadataSlices(numChunks)` and then looped `for i := len(g.VectorsF16); i < numChunks; i++`, executing 0 iterations. Concurrent `AddBatch` publishes then overwrote sibling batches' `VectorsF16`, `Neighbors`, and `Levels` offsets to `0`, causing `entry point vector not found` and `0.0 QPS` on `float16` at 50k/100k. Fixed via `0..numChunks-1` pre-allocation and `(*GraphData).MergeFrom`.
6. **Fixed `growInternal` Single-Shot CAS Race & `Neighbors[0] slice too small` Index Worker Panic at 100k/200k/400k Growth Boundaries (`internal/store/index/arrow_hnsw_memory.go`)**: `growInternal` executed a single `compareAndSwapData(oldData, newData)` after `PreAllocate`. When another worker published a snapshot concurrently, the CAS failed and discarded the grown `GraphData`, causing `EnsureChunks` to panic on `Neighbors[0] slice too small` and dropping 10k-vector batches at 250k/500k (`float64`, `complex64`, `complex128`). Fixed via `installData`'s CAS retry loop and `1.25x` capacity growth above 256k vectors.
7. **Fixed Multi-GB Redundant Heap Allocations & `SharedWorkerPool` Starvation During High-Dimensional Ingest (`internal/store/index/arrow_hnsw_data_extract.go`, `arrow_hnsw_insert.go`, `internal/store/store_ingest.go`, `dataset_registry.go`)**: Eliminated double heap allocation (`extractVector` + `slices.Clone`) for `complex64`/`complex128` via zero-copy `unsafe.Slice`, removed duplicate `[]float32` allocations and `SharedWorkerPool` head-of-line blocking in the `TemporalIndex`/`GeoIndex` ingest hooks, and fixed `2 * dim` complex dimension inflation in `ShouldSpillToDisk`.

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
| **1** | Accuracy | `internal/simd/batch_operations.go:305-330` | `L2SquaredDistanceBatch` 1024-dim truncation (`L2SquaredDistance128` called instead of `1024`) | Restores 100% 1024-dim L2Squared accuracy (**Fixed**) | `accuracy-gate` |
| **2** | Performance | `internal/simd/batch_operations.go:40-92, 220-275` | Scalar 4-way loop shadows AVX2/AVX-512/NEON batch kernels for dims 128, 384, 768 | 3.5x-6.2x batch distance speedup (**Fixed**) | `perf-check` |
| **3** | Performance | `internal/simd/dispatch_tables.go:134-142, 237-240, 304-310` | `layerAVX512` resets 12 AVX2 math/reduction kernels to Generic scalar; `PackTQ2/4` unwired on AVX2 | 2.8x-5.0x reduction/math speedup on AVX-512 (**Fixed**) | `accuracy-gate` |
| **4** | Accuracy & Perf | `internal/simd/dequant_distance.go:54-57`, `internal/store/index/arrow_hnsw_compute_quantized.go:406-430` | `DequantizeL2Uint8Uint8` omits `math.Sqrt`; `float32ToSQ8Computer` returns `0.0` on missing chunk and runs scalar loop | Exact SQ8 L2 parity + 4x faster SQ8 HNSW scoring (**Fixed**) | `oracle-gate` |
| **5** | Accuracy & Perf | `internal/store/index/turboquant.go:208-236` | `TurboQuantEncoder.Encode` computes QJL residual sign bits against unquantized `angles` instead of quantized angle codes | +6-11% Recall@10 for TQ4/TQ8 + 3x faster encoding via LUT | `oracle-gate` |
| **6** | Accuracy & Perf | `internal/store/index/turboquant2.go:188-190, 304-311` | `TurboQuant2Encoder` 2-bit angle LUT aliases `q=0 (-pi)` and `q=3 (+pi)` to the same Cartesian point and locks `rngMu` per dimension | Restores full 4-state 2-bit codebook (+12% recall) & eliminates mutex contention | `oracle-gate` |
| **7** | Performance | `internal/store/index/arrow_hnsw_compute_float16.go:26-50`, `arrow_hnsw_compute_complex.go:60-85, 248-271` | `float16Computer`, `complex64Computer`, and `complex128Computer` allocate via `getVectorWithCachedDisk` before checking zero-copy `*ChunkFast` | Eliminates 1 heap alloc/copy per HNSW hop (0 allocs/op) | `mem-check` |
| **8** | Accuracy | `internal/store/hybrid_search.go:126, 257-303, 350-366` | `ReciprocalRankFusion` ignores `alpha` parameter, has non-deterministic tie order, and `HybridSearchWithBitmap` leaks unfiltered BM25 hits | Enforces `alpha`-weighted RRF, deterministic tie-breaking, and strict bitmap filtering | `oracle-gate` |
| **9** | Accuracy & Perf | `internal/store/index/ivf_opq_index.go:304-328, 409-454, 488-490, 800-839` | `IVFOPQIndex.SearchBatch` passes cluster ID as VectorID in `refineBatch`; `SearchVectorsWithBitmap` uses scalar loop + full sort instead of `ADCDistanceBatch` + top-K heap | Fixes batch recall corruption & 4x faster filtered IVF-OPQ search | `oracle-gate` |
| **10** | Accuracy & Perf | `internal/store/index/scalar_quantization.go:208-223, 263-308` | `SQ8Encoder.EncodeInto` truncates via `uint8(normalized)` instead of nearest rounding `uint8(normalized + 0.5)` and uses scalar distance loops | Halves max quantization bias (`-0.5 LSB` -> `0 LSB`) + SIMD SQ8 distance | `oracle-gate` |
| **11** | Performance | `internal/pq/opq.go:122-190` | `OPQEncoder.Encode`, `Decode`, and `RotateVector` convert `float32` <-> `float64` and allocate two `gonum/mat.VecDense` per call | 8x faster OPQ rotation, 0 heap allocations in steady state | `mem-bench` |
| **12** | Accuracy & Perf | `internal/pq/kmeans.go:80-102, 160-188` | `TrainKMeans` uses uniform random centroid seeding (`rand.Perm`) and `MetricEuclidean` (`sqrt` per centroid in E-step) instead of K-Means++ and `FindNearestCentroid` | 15-25% lower PQ distortion + 3x faster codebook training | `oracle-gate` |
| **13** | Performance | `internal/tensor/ops.go:174-208` | `contractNumeric` allocates `cIndices := make([]int, len(aAxes))` on every outer iteration and recomputes `totalIter` and `div/mod` inside the inner loop | Zero-alloc inner contraction loop + SIMD `DotProduct` fast path for 2D `MatMul` | `mem-bench` |
| **14** | Accuracy | `internal/memory/arena.go:162-187, 201-230, 319` | `SlabArena.Alloc` and `AllocDirty` lock-free `allocFast` path omits `a.stats.UsedBytes.Add` and `metrics.ArenaFastPathTotal.Inc()` | 100% accurate `ArenaStats.UsedBytes` accounting across fast and slow paths | `oracle-gate` |
| **15** | Accuracy | `internal/store/hnsw_temporal.go:70-136` | `TemporalHNSWIndex.SearchAsOf`, `SearchRange`, and `SearchSlidingWindow` return raw B-tree IDs with `Score: 0` without executing HNSW vector search | Full temporal-filtered ANN search with exact distance scores | `oracle-gate` |
| **16** | Performance | `internal/store/index/bm25_arena.go:49, 100-140, 222-286` | `BM25ArenaIndex` leaves `postingArena` unused, allocates per-doc maps, scans postings in `O(P^2)`, and stores all TFs in a global `map[uint64]uint32` | Arena-backed contiguous postings + `simd.BM25ScoreBatch` (5x faster scoring) | `mem-bench` |
| **17** | Performance | `internal/store/index/bm25_inverted_index.go:588-634` | `BM25InvertedIndex.SearchBM25` recomputes `blockMaxScores` over the entire posting list on every multi-term query instead of caching per-block max TF/min DocLen | Eliminates `O(N_postings)` pre-scan per query in Block-Max WAND | `perf-check` |
| **18** | Performance | `internal/store/index/arrow_hnsw_compute_float32.go:241-274` | `float32Computer.ComputeBatch` iterates element-by-element calling `distFunc(c.q, v)` instead of batch SIMD (`L2SquaredDistanceBatch` / `DotProductBatch` / `CosineDistanceBatch`) | 2.5x-4.0x faster `float32Computer` batch candidate evaluation | `perf-check` |
| **19** | Performance | `internal/store/store.go:161`, `internal/store/cluster/cdc.go:158, 182-186` | `VectorStore.hnsw2Config` is unwired during dataset creation and `ChangeDataCapture` bypasses its initialized `batchAggregator` | Enables HNSW2 dataset config propagation and batched CDC replication | `test-short` |
| **20** | Accuracy | `internal/adbc/statement.go:358-410` | `Statement.executeScan` synthesizes a dummy query vector and runs `SearchHybrid` returning `(id, score)` instead of iterating dataset `RecordBatch`es matching `ExecuteSchema` | Schema-consistent Arrow `RecordBatch` table scan in ADBC driver | `test-adbc` |

---

### Detailed Specification of the 20 Improvements

#### Improvement 1: Fix `L2SquaredDistanceBatch` 1024-Dimension Silent Truncation

- **Location**: `internal/simd/batch_operations.go:305-330`
- **Root Cause**: In `L2SquaredDistanceBatch`, the fast-path switch for `case 1024:` called `currentDispatch.L2SquaredDistance128(query, v)` instead of `currentDispatch.L2SquaredDistance1024(query, v)`, silently ignoring dimensions `128..1023` (87.5% of every 1024-dim embedding).
- **Remediation**: Route `case 1024:` to `currentDispatch.L2SquaredDistance1024(query, v)` and ensure `newGenericDispatch()` populates `L2SquaredDistance128..3072`. (**Completed & Verified**)

#### Improvement 2: Route 128, 384, and 768 Batch Operations Through Hardware SIMD Dispatch

- **Location**: `internal/simd/batch_operations.go:40-92, 220-275, 296-332`
- **Root Cause**: `EuclideanDistanceBatch`, `DotProductBatch`, and `L2SquaredDistanceBatch` intercepted `dims == 128, 384, 768` and called scalar Go helpers `euclideanDistanceBatch4Way` and `dotProductBatch4Way` even when AVX2, AVX-512, NEON, or AMX was active.
- **Remediation**: Restrict `euclideanDistanceBatch4Way` and `dotProductBatch4Way` to `implementation == "generic"` and dispatch `128, 384, 768, 1024, 3072` directly to `currentDispatch` SIMD kernels. (**Completed & Verified**)

#### Improvement 3: Preserve AVX2 Math & Reduction Kernels in `layerAVX512` and Wire `PackTQ2/4AVX2`

- **Location**: `internal/simd/dispatch_tables.go:237-240, 304-322`
- **Root Cause**: `layerAVX512` started from `d := *avx2` and then overwrote `MatMul`, `BrayCurtisDistance`, `Sum`, `Max`, `Min`, `Sin`, `Cos`, `Sincos`, `Sqrt`, `Atan2`, `ArgMax`, and `ArgMin` back to `*Generic` scalar implementations. In addition, `layerAVX2` only wired `PackTQ8AVX2` and omitted `PackTQ2AVX2` and `PackTQ4AVX2`.
- **Remediation**: Remove the 12 scalar overrides in `layerAVX512` so AVX-512 inherits AVX2 kernels when dedicated 512-bit kernels are not present, and wire `PackTQ2AVX2` and `PackTQ4AVX2` in `layerAVX2`. (**Completed & Verified**)

#### Improvement 4: Fix `DequantizeL2Uint8Uint8` Missing `math.Sqrt` and `float32ToSQ8Computer` False-Zero Fallback

- **Location**: `internal/simd/dequant_distance.go:54-57`, `internal/store/index/arrow_hnsw_compute_quantized.go:406-430`
- **Root Cause**: `DequantizeL2Uint8Uint8` called `EuclideanDistanceSQ8` (which returns squared `int32` L2 distance) and multiplied by `scale` without `math.Sqrt`, contradicting its docstring and its own fallback branch. Meanwhile, `float32ToSQ8Computer.ComputeSingle` returned `c.h.distFuncSquared(c.q, c.q)` (`0.0`) when `chunk == nil` and used a scalar loop instead of `simd.DequantizeL2Float32Uint8`.
- **Remediation**: Apply `math.Sqrt(float64(dist)) * scale` in `DequantizeL2Uint8Uint8`, use `simd.DequantizeL2Float32Uint8` in `float32ToSQ8Computer.ComputeSingle`, and return `math.MaxFloat32, nil` when a chunk is absent. (**Completed & Verified**)

#### Improvement 5: Reconstruct From Quantized Angle Codes in `TurboQuantEncoder.Encode`

- **Location**: `internal/store/index/turboquant.go:208-236`
- **Root Cause**: Step 4 of `TurboQuantEncoder.Encode` calls `e.polarReconstructRecursive(radius, angles, recon, stack)` using continuous *unquantized* `angles` before packing. As a result, `work - recon` in Step 5 measures only `float32` `Sincos` rounding noise (`~1e-7`) rather than the angle quantization residual, rendering the 1-bit QJL residual correction ineffective and paying `pow2 - 1` scalar `math.Sincos` calls per encoded vector.
- **Remediation**: Pack angle codes first (or quantize angle indices in-place) and reconstruct `recon` via `e.polarReconstructCodes(radius, packedAngles, recon, stack)` using the precomputed `Sincos` LUT before computing QJL sign bits `work[i] > recon[i]`.

#### Improvement 6: Fix Degenerate 2-Bit Angle Grid and Per-Dimension Mutex Lock in `TurboQuant2Encoder`

- **Location**: `internal/store/index/turboquant2.go:188-190, 304-311`, `internal/simd/simd_turboquant.go:32-58, 147-167`
- **Root Cause**: `TurboQuant2Encoder` quantizes angles to 4 levels with `scale = 2*pi/3, bias = -pi`, producing angles `{-pi, -pi/3, +pi/3, +pi}`. Because `cos(-pi) == cos(+pi) == -1` and `sin(-pi) == sin(+pi) == 0`, codes `0` and `3` map to the identical Cartesian vector, collapsing a 2-bit (4-state) quantizer into 3 distinct points on the unit circle. Additionally, `Encode` locks `e.rngMu` inside the per-dimension `for i := 0; i < e.pow2; i++` loop.
- **Remediation**: Shift the 2-bit angle grid to cell midpoints `{-3*pi/4, -pi/4, +pi/4, +3*pi/4}` (`scale = pi/2, bias = -3*pi/4`) so all 4 codes represent orthogonal quadrants on the unit circle, and hoist RNG state into a pooled per-worker PRNG or lock once per vector.

#### Improvement 7: Restore Zero-Copy Chunk Fast Path in `float16Computer`, `complex64Computer`, and `complex128Computer`

- **Location**: `internal/store/index/arrow_hnsw_compute_float16.go:26-50`, `internal/store/index/arrow_hnsw_compute_complex.go:60-85, 248-271`
- **Root Cause**: `ComputeSingle` calls `c.h.getVectorWithCachedDisk(c.data, c.cachedDiskGraph, id)` *before* checking `GetVectorsF16ChunkFast` / `GetVectorsComplex64ChunkFast` / `GetVectorsComplex128ChunkFast`. Because `getVectorWithCachedDisk` allocates and copies a new slice on every call, the zero-copy chunk fast path below it is dead code whenever `getVectorWithCachedDisk` succeeds.
- **Remediation**: Check `GetVectorsF16ChunkFast` / `GetVectorsComplex64ChunkFast` / `GetVectorsComplex128ChunkFast` first (matching `float32Computer` and `float64Computer`), falling back to `getVectorWithCachedDisk` only when the in-memory chunk is nil.

#### Improvement 8: Honour `alpha` Weighting, Deterministic Tie-Breaking, and Bitmap Filtering in `HybridSearch`

- **Location**: `internal/store/hybrid_search.go:126, 257-303, 350-366`
- **Root Cause**: `SearchHybrid` accepts `alpha` (`0..1` dense vs. sparse weight) but `ReciprocalRankFusion` weights both legs equally (`1.0 / (k + rank + 1)`). When RRF scores tie, map iteration order causes non-deterministic top-K ordering. Furthermore, `HybridSearchWithBitmap` passes `req.Bitset` to dense search but passes `nil` to `searchBM25Arena` and `bm25Legacy.SearchBM25`, leaking filtered-out documents into hybrid results.
- **Remediation**: Weight dense RRF contributions by `alpha` and sparse contributions by `1.0 - alpha`, break score ties deterministically by ascending `VectorID`, and pass `req.Bitset` into both BM25 search paths in `HybridSearchWithBitmap`.

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

#### Improvement 14: Fix Lock-Free Fast-Path Byte Accounting in `SlabArena`

- **Location**: `internal/memory/arena.go:162-187, 201-230, 319`
- **Root Cause**: When `SlabArena.Alloc` or `AllocDirty` succeeds on the initial lock-free `allocFast` call (lines 166 and 182), it returns immediately without updating `a.stats.UsedBytes.Add(int64(alignedSize))` or `metrics.ArenaFastPathTotal.Inc()`. Those counters are only updated when `allocFast` is called a second time inside `allocCommon`.
- **Remediation**: Move `a.stats.UsedBytes.Add(int64(alignedSize))` and `metrics.ArenaFastPathTotal.Inc()` into `allocFast` itself so all fast-path allocations are accurately tracked.

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

#### Improvement 18: Vectorize `float32Computer.ComputeBatch` Using Batch SIMD Dispatch

- **Location**: `internal/store/index/arrow_hnsw_compute_float32.go:241-274`
- **Root Cause**: Unlike `float32ToFloat32Computer.ComputeBatch` (which gathers a slice of candidate vector pointers and calls `simd.L2SquaredDistanceBatch` / `EuclideanDistanceBatch` / `CosineDistanceBatch` / `DotProductBatch`), `float32Computer.ComputeBatch` loops one ID at a time calling `c.h.distFunc(c.q, v)`.
- **Remediation**: Gather non-nil candidate vectors into a scratch `batchVecs` slice on `float32Computer` and dispatch to the batch SIMD kernels.

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
