# Longbow Roadmap (Performance & Scale)

This document tracks the **remaining** performance and scalability work for
Longbow. Completed items have been removed; their results are recorded in
`docs/performance.md`, `docs/testplan.md`, and the git history.

The removed work covered the 1M complex128 memory-profile hot spots (zero-allocation neighbor updates, batch-scratch reuse, copy-on-write graph cloning, cached distance computers), SIMD parity and zero-allocation kernels across all dtypes, dynamic worker scaling, NUMA-aware allocation, request batching, multi-predicate bitmap intersection, HNSW bulk quality (reachability, recall @10, post-bulk repair, and `addbatch_inflight` / `BulkChainLinks` tuning), memory-scaling tests, P0 performance, OIDC Federation, DevSecOps, geo/temporal/text candidate caps, TurboQuant kernels/LUT, predicate pushdown, disk/tiered storage, spatial hash grid, temporal sorted index, text trigram index, filtered predicate-aware entry points, parallel filter evaluation, geo LRU result cache, the 50k-1M scalability tests for filtered/complex/TurboQuant/uint16, and the complete 1M × 15-dtype × 21-mode validation matrix (C4, `benchmarks/baseline_cpu_1m_all15_20261121.json`, `docs/performance.md` §0e, Recall@10 = 0.8600 at 1M).

See `docs/performance.md` for benchmark results and baselines, and
`docs/testplan.md` for the full search-mode and dtype matrix.

---

## Remaining Work

All planned Performance & Scale roadmap items are **complete** (0 remaining items).

---

## Verification Artifacts

| Gate | Command | Status |
|---|---|---|
| Build | `make build` | pass |
| Vet | `make vet` | pass |
| Unit tests | `make test-short` | pass |
| Static check | `make staticcheck` | 0 issues |
| Dead code | `make deadcode` | pre-existing only |
| Observability | `make observability-check` | pass |
| Accuracy | `make accuracy-gate` | pass |
| Oracle | `make oracle-gate` | pass |
| Release versions | `make check-version` | pass |

## Baselines

- `benchmarks/baseline_cpu_1m_all15_20261121.json` (1M, dim 128, all 15 dtypes × 21 modes + Recall@10 = 0.8600)
- `benchmarks/baseline_cpu_20261116.json` (50k/100k/200k, dim 128 & 384)
- `benchmarks/baseline_cpu_768_20261116.json` (50k/100k/200k, dim 768)
- `benchmarks/baseline_cpu_large_20261116.json` (500k/1M, dim 128)
- `benchmarks/baseline_cpu.json`, `baseline_100k.json`, `baseline_250k.json`, `baseline_25k.json` (legacy)

---

## Codebase Modularization & God-Module Refactoring Plan

### Context & Design Targets

A full repository audit (1,783 code files: 902 production, 881 test) identified **31 production files exceeding 1,000 LOC** (8 exceeding 2,000 LOC) and **3 flat "god packages"** (`internal/store`, `internal/store/index`, `internal/simd`) accounting for ~65% of all production code. Files above ~800 LOC (~6.5k tokens) exceed single-read tool limits and force both humans and AI coding agents into partial-slice reads, losing cross-file context during debugging and refactoring.

Target budgets for all hand-maintained code:

| Scope | Soft Target | Hard Ceiling | Notes |
|---|---:|---:|---|
| Hand-maintained source file | ≤ 500 LOC (~4k tok) | ≤ 800 LOC (~6.5k tok) | Fits in one read with room for sibling files |
| Generated / assembly file | ≤ 2,500 LOC | ≤ 4,000 LOC | Split generator output by dtype family |
| Single function / method | ≤ 80 LOC | ≤ 150 LOC | Extract helpers for setup, dispatch, and encoding |
| Go package (production files) | ≤ 30 files | ≤ 50 files | Extract cohesive domains into subpackages |

### Inventory of Identified God Modules

#### 1. God Packages (Flat Namespace Sprawl)

| Package | Prod Files | Prod LOC | Est. Prod Tokens | Test Files (LOC) | Primary Issue |
|---|---:|---:|---:|---:|---|
| `internal/store` | 194 | 58,263 | ~418k | 280 (51,339) | Core store mixed with 12+ feature domains (REST/GraphQL, RBAC, Temporal, Geo, Text, Graph, Streaming, Cloud, Sharding, Views, Schema, Provenance) |
| `internal/store/index` | 132 | 44,160 | ~308k | 168 (35,217) | HNSW core mixed with IVF/OPQ, Learned Index, BM25/Inverted, DiskGraph, GPU bridges, and Adaptive/Sharded wrappers |
| `internal/simd` | 106 | 42,926 | ~290k | 82 (12,909) | Single 17.1k-LOC generated AMD64 assembly file + monolithic ARM64 assembly file |

#### 2. Largest Hand-Maintained & Generated God Files

| Priority | File | LOC | Size | Est. Tokens | Structural Smell |
|---|---|---:|---:|---:|---|
| P0 | `internal/store/rest_graphql.go` | 3,449 | 122.0 KB | ~31.2k | 78 funcs: REST router, OpenAPI generator, GraphQL parser/executor, and all domain HTTP handlers in one file |
| P0 | `scripts/unified_benchmark.py` | 3,237 | 141.6 KB | ~36.3k | 1 class (`BenchmarkRunner`, 48 methods): server lifecycle, memory watchdog, checkpointing, baseline diffing, reporting, CLI |
| P0 | `longbowclientsdk/src/longbow/client.py` | 2,278 | 81.6 KB | ~20.9k | 1 class (`LongbowClient`, 125 methods): Flight/REST transport + all 12 feature domains |
| P0 | `cmd/bench-tool/main.go` | 2,074 | 69.3 KB | ~17.7k | 34 funcs: 15-dtype corpus gen, ingest, readiness, 21 search modes, hashing, JSON output |
| P1 | `internal/store/index/arrow_hnsw_insert.go` | 1,913 | 50.8 KB | ~13.0k | Only 13 funcs (~147 LOC/func avg): bulk insert, graph linking, repair, and typed dispatch interleaved |
| P1 | `internal/store/text_search_advanced.go` | 1,636 | 42.8 KB | ~11.0k | 25 types, 54 funcs: query lexer/AST parser, trigram/inverted corpus cache, BM25/fuzzy/phrase scoring, facets |
| P1 | `internal/store/dataset.go` | 1,409 | 38.1 KB | ~9.7k | 56 funcs: dataset lifecycle, metadata, schema, locking, memory accounting, and query helpers |
| P1 | `internal/store/index/navigation_search.go` | 1,383 | 43.2 KB | ~11.0k | Only 7 funcs (~198 LOC/func avg): main search entry points, bitmap/predicate filtering, distance computer setup |
| P1 | `cmd/longbow/main.go` | 1,182 | 43.8 KB | ~11.2k | Only 4 funcs (~295 LOC/func avg): monolithic `main()` wiring flags, TLS, RBAC/OIDC, storage, Flight, REST, Mesh, Raft, shutdown |
| P1 | `internal/store/store_ingest.go` | 1,082 | 31.4 KB | ~8.0k | Only 6 funcs (~180 LOC/func avg): giant `DoPut` handling auth, backpressure, WAL, Arrow retention, disk spill, temporal/geo hooks |
| P2 | `internal/store/hnsw_autoshard.go` | 1,253 | 33.8 KB | ~8.6k | 51 funcs: sharding policy, split/merge execution, routing, and metrics |
| P2 | `internal/store/temporal_search.go` | 1,219 | 30.2 KB | ~7.7k | 35 funcs: temporal shards, async ingest worker, as-of/range/sliding-window search, version lookup |
| P2 | `internal/store/streaming_connectors.go` | 1,106 | 32.8 KB | ~8.4k | 11 types, 29 funcs: connector registry, Kafka/Pulsar/memory drivers, schema validation, ingestion loop |
| P2 | `internal/metrics/metrics.go` & `storage_metrics.go` | 2,371 | 73.4 KB | ~18.8k | Monolithic Prometheus registry declarations |
| P2 | `internal/query/zero_alloc_utils.go` & `fast_path_filter.go` | 2,554 | 57.1 KB | ~14.6k | Combined low-level JSON/filter scanners & Arrow column fast paths |
| P2 | `internal/store/index/adaptive_index.go` & `arrow_search_context.go` | 2,217 | 66.1 KB | ~16.9k | Adaptive routing + pooled search scratch state |
| P2 | `internal/gpu/cuda/kernels.cu` & `internal/gpu/metal/metal_gpu*.go` | 4,964 | 183.4 KB | ~46.9k | Single CUDA kernel TU + twin 1.4k-LOC Metal wrappers |
| P3 | `internal/simd/all_kernels_avo_amd64.s` (+ `all_kernels_gen.go`) | 17,122 (+2,453) | 484.5 KB | ~124.0k | Single generated AMD64 assembly file + monolithic AVO generator |
| P3 | `internal/simd/simd_arm64.s` & `turboquant_amd64.s` | 4,768 | 125.2 KB | ~32.0k | Monolithic hand-written ARM64 and TurboQuant assembly files |

---

### Phased Refactoring Plan

| Phase | Area | Priority | Status |
|---|---|---|---|
| M1 | Same-package splits of P0/P1 Go god files (zero API breakage) | HIGH | Completed |
| M2 | Python SDK (`client.py`) & benchmark orchestrator (`unified_benchmark.py`) modularization | HIGH | Completed |
| M3 | Hot-path HNSW & query function decomposition (`arrow_hnsw_insert.go`, `navigation_search.go`, `query/`) | MEDIUM | Planned |
| M4 | SIMD generator/assembly & GPU kernel splits (`internal/simd`, `internal/gpu/{cuda,metal}`) | MEDIUM | Planned |
| M5 | God-package domain extraction (`internal/store` & `internal/store/index` subpackages) | LOW | Planned |
| M6 | Automated module-size guardrails & CI budget enforcement | LOW | Planned |

#### M1 — Same-Package Splits of P0/P1 Go God Files (Zero API Breakage)

Split oversized Go files **within their existing packages** first so no import paths or exported signatures change and every move is verifiable via `go vet` + existing tests (all resulting M1 production files ≤ 680 LOC, well under the 800-LOC hard ceiling):

- [x] **`internal/store/rest_graphql.go` (3,449 LOC)** -> removed and split into 9 focused files (93–680 LOC):
  - `rest_server.go` (93 LOC: auth/permission wrappers, JSON helpers, health handler)
  - `rest_openapi.go` (107 LOC: OpenAPI 3.0 spec generation & handler)
  - `rest_dataset_ops.go` (220 LOC: stream upsert, vector CRUD helpers, dataset info inspection)
  - `rest_dataset_handlers.go` (420 LOC: namespaces, datasets, vectors CRUD, data/schema/BM25/provenance sub-actions)
  - `rest_search_handlers.go` (443 LOC: search/hybrid/text/geo/temporal/multimodal/range/recommend/federated/aggregations/facets/explain)
  - `rest_admin_handlers.go` (413 LOC: usage, RBAC keys/RLS/audit, OIDC providers/exchange, backups, streams HTTP handlers)
  - `rest_cloud_sharding_handlers.go` (492 LOC: rerank, query rewrite, elastic sharding, materialized views, analytics, cloud control-plane)
  - `graphql_parser.go` (680 LOC: GraphQL lexer, AST, parser, variable/arg/projection helpers, schema introspection)
  - `graphql_engine.go` (674 LOC: GraphQL query/mutation executor, HTTP & Flight action handlers)
- [x] **`cmd/bench-tool/main.go` (2,074 LOC)** -> split into `cmd/bench-tool/{main.go,corpus.go,ingest.go,search_tickets.go,search_runner.go,report.go}` (224–491 LOC each).
- [x] **`cmd/longbow/main.go` (1,182 LOC)** -> split into `cmd/longbow/{main.go,config.go,auth_wire.go,runtime_wire.go,servers.go,shutdown.go}` (112–426 LOC each).
- [x] **`internal/store/text_search_advanced.go` (1,636 LOC)** -> split into `text_query_ast.go` (479 LOC), `text_corpus_cache.go` (353 LOC), `text_scoring.go` (563 LOC), and `text_search_advanced.go` (272 LOC: manager/analytics/Flight actions).
- [x] **`internal/store/store_ingest.go` (1,082 LOC)** -> split into `ingest_admission.go` (427 LOC), `ingest_hooks.go` (339 LOC: pressure quantization, DiskStore, secondary temporal/geo/text hooks, norm helper), and `store_ingest.go` (380 LOC).
- [x] **`internal/store/dataset.go` (1,409 LOC)** -> split into `dataset.go` (584 LOC), `dataset_indexing.go` (480 LOC), and `dataset_filtering.go` (366 LOC).
- [x] **`internal/store/hnsw_autoshard.go` (1,253 LOC)** -> split into `hnsw_autoshard.go` (476 LOC), `hnsw_autoshard_migrate.go` (451 LOC), and `hnsw_autoshard_search.go` (347 LOC).
- [x] **`internal/store/temporal_search.go` (1,219 LOC)** -> split into `temporal_index.go` (432 LOC), `temporal_ops.go` (297 LOC), and `temporal_search.go` (505 LOC).
- [x] **`internal/store/streaming_connectors.go` (1,106 LOC)** -> split into `streaming_schema_registry.go` (328 LOC), `streaming_connectors.go` (502 LOC), and `streaming_drivers.go` (298 LOC).
- [x] **`internal/metrics/metrics.go` (1,295 LOC)** -> split into `metrics.go` (495 LOC), `metrics_engine_ops.go` (484 LOC), and `metrics_backends_quant.go` (330 LOC).

#### M2 — Python Monolith Modularization

- [x] **`longbowclientsdk/src/longbow/client.py` (2,278 -> 66 LOC, 125 methods preserved)** -> decomposed `LongbowClient` into transport + domain mixins under `longbowclientsdk/src/longbow/_mixins/` (17–467 LOC each, preserving 100% backwards-compatible `from longbow import LongbowClient` public API):
  - `_transport.py` (432 LOC: Flight/REST call options, headers, `_do_action_json`, `_do_get_df`, `_upload_batch`, `_rest_request`, insert/download/delete, error mapping)
  - `_search.py` (467 LOC: dense, hybrid, filtered, sparse, range, geo, temporal, text, multimodal, federated, explain, rerank)
  - `_rest_search.py` (362 LOC: GraphQL, REST search suite, rerank evaluation, media parquet ingest, provenance debugging)
  - `_graph.py` (173 LOC: edges, traverse, pagerank, communities, GraphRAG, recommend)
  - `_security.py` (260 LOC: RBAC API keys, RLS, audit log, OIDC providers, token exchange, AWS/Google helpers)
  - `_admin.py` (463 LOC: namespaces, datasets, backups, streaming CDC, cloud clusters, sharding, schema registry, materialized views, usage/billing)
- [x] **`scripts/unified_benchmark.py` (3,237 -> 115 LOC)** -> thin CLI entrypoint and compatibility facade delegating modular mixins/utilities under `scripts/benchmark/` (20–567 LOC each):
  - `server_lifecycle.py` (567 LOC: binary resolution, port-scoped cleanup, readiness polling, process teardown)
  - `memory_guard.py` (334 LOC: RAM detection, ceiling calculation, RSS sampler/soak, OOM early-abort)
  - `checkpoint.py` (177 LOC: checkpoint save/load, `--resume` reconciliation, baseline save/compare)
  - `reporting.py` (458 LOC: summary tables, markdown/JSON report formatting)
  - `config_runner.py` (486 LOC), `matrix_runner.py` (421 LOC), `cli.py` (392 LOC), plus synchronized `process.py` (183 LOC) & `telemetry.py` (233 LOC)
  - Updated `scripts/tests/` source checks to inspect the modularized Go/Python layouts (`make test-bench-harness`: 122/122 tests OK).

#### M3 — Hot-Path HNSW & Query Function Decomposition

Preserve zero-allocation hot paths and inlining budgets (verify with `go test -bench` + `make accuracy-gate` + `make oracle-gate` after every split):

- [ ] **`internal/store/index/arrow_hnsw_insert.go` (1,913 LOC, 13 funcs)** -> split into `arrow_hnsw_insert_single.go`, `arrow_hnsw_insert_batch.go`, `arrow_hnsw_link_nodes.go`, and `arrow_hnsw_bulk_repair.go`.
- [ ] **`internal/store/index/navigation_search.go` (1,383 LOC, 7 funcs)** -> split into `navigation_search_entry.go` (validation/dispatch), `navigation_search_beam.go` (beam traversal & predicate/bitmap paths), `navigation_search_range.go` (`SearchVectorsInRange`), and `navigation_distance_setup.go`.
- [ ] **`internal/store/index/adaptive_index.go` (1,160 LOC)** & **`arrow_search_context.go` (1,057 LOC)** -> split config/routing from execution and per-dtype scratch buffers from pool lifecycle.
- [ ] **`internal/query/zero_alloc_utils.go` (1,486 LOC)** & **`fast_path_filter.go` (1,068 LOC)** -> split JSON token scanners from typed Arrow column filter evaluators (`filter_fast_numeric.go`, `filter_fast_string.go`).

#### M4 — SIMD & GPU Kernel Splitting

- [ ] **`internal/simd/gen/all_kernels_gen.go` (2,453 LOC)** -> modularize generator by dtype family (`gen_float.go`, `gen_int.go`, `gen_complex.go`, `gen_quant.go`) and emit 4–5 generated assembly files (`kernels_float_amd64.s`, `kernels_int_amd64.s`, `kernels_complex_amd64.s`, `kernels_quant_amd64.s`) instead of one 17,122-LOC `all_kernels_avo_amd64.s`.
- [ ] **`internal/simd/simd_arm64.s` (3,443 LOC)** -> split by dtype family (`simd_float_arm64.s`, `simd_int_arm64.s`, `simd_complex_arm64.s`, `simd_quant_arm64.s`).
- [ ] **`internal/gpu/cuda/kernels.cu` (2,072 LOC)** -> split into per-metric/dtype `.cu` translation units (`kernels_l2.cu`, `kernels_cosine.cu`, `kernels_quant.cu`, `kernels_common.cuh`).
- [ ] **`internal/gpu/metal/metal_gpu.go` (1,401 LOC)** & **`metal_gpu_optimized.go` (1,491 LOC)** -> consolidate shared Obj-C/CGO bridge helpers and split shader sources into `.metal` / per-op files.

#### M5 — God-Package Domain Extraction (`internal/store` & `internal/store/index`)

Once M1/M3 have established clean file boundaries, progressively extract self-contained domains into dedicated subpackages using Go type aliases (`type X = subpkg.X`) during transition so callers remain unbroken:

- [ ] **`internal/store` (194 -> ≤ 50 prod files)**:
  - `internal/store/api` (REST v1 + GraphQL + OpenAPI)
  - `internal/store/rbac` (RBAC manager, RLS policies, audit log, OIDC actions)
  - `internal/store/temporal` (TemporalIndex, TemporalTree, SegmentTree, VersionHistory, TTL)
  - `internal/store/geo` (GeoIndex, spatial hash grid, Morton/quadtree, geo cache)
  - `internal/store/text` (Advanced text AST, trigram/inverted corpus cache, facets)
  - `internal/store/streaming` (CDC connectors, schema registry, ingestion envelopes)
  - `internal/store/controlplane` (Cloud clusters, elastic sharding, backups/PITR, usage/billing)
- [ ] **`internal/store/index` (132 -> ≤ 50 prod files)**:
  - `internal/store/index/hnsw` (ArrowHNSW core, insertion, search, repair)
  - `internal/store/index/ivf` (IVF-PQ / IVF-OPQ / composite)
  - `internal/store/index/learned` (Learned index models & routing)
  - `internal/store/index/bm25` (BM25 / inverted sparse index)

#### M6 — Guardrails & CI Enforcement

- [ ] Add `scripts/check_module_sizes.sh` (warn > 800 LOC on new/modified hand-maintained files, fail > 1,200 LOC on newly created files, track grandfathered allow-list that shrinks as M1–M5 land).
- [ ] Wire into `Makefile` (`make module-size-check`) and pre-commit advisory output.

### Refactoring Verification & Exit Criteria

- Every PR/phase is **strictly behavior-preserving**: no wire-format, Flight action, REST endpoint, CLI flag, or Python SDK signature changes.
- Required gates per phase: `make fmt-check`, `make vet`, `make lint`, `make sec-gosec`, `make test-short`, `make accuracy-gate`, `make oracle-gate`, and `PYTHONPATH=longbowclientsdk/src python3 -m unittest` (for M2).
- M3/M4 hot-path splits additionally verify zero allocation regressions (`-benchmem`) and ≤ 3% dense QPS variance against `benchmarks/baseline_cpu_20261116.json`.
