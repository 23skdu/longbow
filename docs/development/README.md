# Developer Documentation

Maintainer- and contributor-only documents for Longbow. User-facing guides remain in [`docs/`](../README.md).

## Guides

- [Development Guide](development.md) — environment setup, contribution workflow, benchmarking, and regression test plan.
- [Code & Quality Gates](gates.md) — the verification gates enforced by local tooling and CI.
- [Roadmap](roadmap.md) — remaining performance, scalability, and modularization work.

## Performance & Validation

- [Performance Benchmarks](performance.md) — empirical CPU/AVX2/AVX-512/GPU baselines and validation matrices.
- [Benchmark & Validation Test Plan](testplan.md) — single-node benchmark matrix and A/B gating methodology.
- [24-Hour Agent-Memory Soak Test Plan](soaktestplan.md) — long-running multi-agent read/write/delete stability test.
- [Global GraphRAG & Clustering Test Plan](globaltestplan.md) — multi-node GCP topology, fault injection, and distributed validation.

## Internals

- [SIMD Architecture & Dispatch](simd-architecture.md) — runtime dispatch, kernel families, and fallback layers.
