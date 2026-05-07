# Longbow Storage Engine Hardening - Next Steps

## P0 Blockers: Incomplete & Stubbed Code Remediation

The following items represent incomplete implementations or stubbed functionality that must be resolved before production deployment.

- [ ] **Remediate `io_uring` Stubs for Linux Production**
  - [ ] Replace `internal/store/disk_writer_uring_stub.go` with full `io_uring` implementation.
  - [ ] Replace `internal/store/uring_reader_stub.go` with full `io_uring` implementation.
  - [ ] *Rationale:* Stubs prevent high-performance asynchronous I/O, a core requirement for the storage engine performance.

- [ ] **Replace Fallback Heuristic Models**
  - [ ] Integrate production ONNX/WASM models in `internal/store/embedding_generator.go` (currently `stubEmbeddingModel`).
  - [ ] Implement real ML reranking models in `internal/store/ml_reranker.go` (currently `stubMLModel`).
  - [ ] *Rationale:* Current heuristic fallbacks are explicitly documented as "NOT recommended for production."


This document tracks the remaining tasks for hardening the Longbow storage engine for production readiness.

## Completed Tasks (v0.2.1-rc1)

- [x] **Interface Compliance & Adapter Remediation**
  - [x] Implemented `GetVectorID` and location mapping in IVF and HNSW adapters.
  - [x] Standardized `Location` type usage via aliases and packing helpers.
- [x] **SIMD Engine Hardening**
  - [x] Verified tiled batching remainder logic for non-standard vector dimensions.
  - [x] Validated alignment safety with unaligned assembly loads (VMOVUPS).
- [x] **Persistence & Integration Validation**
  - [x] Migrated from stubbed RCU tests to full-flow persistence lifecycle validation.
  - [x] Audited and implemented accurate `EstimateMemory` via `SlabArena` stats.
- [x] **HNSW Security Hardening (Gosec G115)**
  - [x] Remediated integer overflow vulnerabilities in `insertion_core.go` and `arrow_hnsw.go`.
  - [x] Implemented robust bounds checking and clamping for atomic configuration parameters (M, MMax, MMax0).
  - [x] Validated fixes with `gosec` and `go test -race`.
- [x] **Release Preparation**
  - [x] Finalized static analysis audit.
  - [x] Verified thread-safety for concurrent ingestion paths.

## Ongoing Work (v0.2.1)

- [ ] **EMLgo Math Library Evaluation**
  - [ ] Implement prototype vector distance benchmarks.
  - [ ] Compare precision and performance against current SIMD kernels.
- [ ] **Enhanced Build Matrix validation**
  - [ ] Extend ARM64 specific optimization tests.
  - [ ] formalize CI bench performance gates.

---

*Document updated for v0.2.1-rc1 release preparation.*
