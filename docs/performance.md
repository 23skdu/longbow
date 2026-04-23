# Longbow 0.1.9 Performance Benchmark Report

This document summarizes the empirical performance metrics for the Longbow 0.1.9 production release, validated on Apple Silicon (M3 Pro) with both CPU and Metal GPU backends.

## 1. System Stability & Hardening

The 0.1.9 release includes critical hardening for large-scale (25k–50k node) deployments. The following optimizations were validated:
- **Zero-Allocation Parsers**: Analytical and temporal queries now use specialized parsers that avoid heap allocations, reducing GC churn by ~40% during peak loads.
- **Resilient Temporal Aggregation**: The aggregation engine now supports statistical metrics (`min`, `max`, `mean`, `sum`, `count`) with metadata-driven field selection.
- **Memory Pressure Resilience**: 50,000-node GraphRAG spreading activation tests now complete reliably within a 12GB memory envelope on macOS.

## 2. GraphRAG Performance (Spreading Activation)

| Scale (Nodes) | Alpha | QPS | P50 Latency |
|---------------|-------|-----|-------------|
| 1,000         | 0.3   | 4,405 | 0.23ms      |
| 7,000         | 0.3   | 4,376 | 0.23ms      |
| 25,000        | 0.3   | 1,630 | 0.60ms      |
| 50,000        | 1.0   | 570   | 1.63ms      |

*GraphRAG performance scales sub-linearly with node count, maintaining millisecond-level responsiveness even at maximum scale.*

## 3. Metal GPU Performance (Apple M3 Pro)

| Data Type | 128-Dim QPS | 384-Dim QPS | Ingest Rate |
|-----------|-------------|-------------|-------------|
| float32   | 6,541       | 5,153       | 383k vec/s  |
| int64     | 10,097      | 7,770       | 299k vec/s  |
| turboquant| 6,904       | 5,076       | 366k vec/s  |

*Metal GPU excels at 64-bit integer operations and TurboQuant (bit-packed) vectors, providing significant throughput gains over CPU for high-dimensional search.*

## 4. Temporal Aggregation

| Scale (Nodes) | Operation | Result | Status |
|---------------|-----------|--------|--------|
| 50,000        | Count     | 50,000 | PASS   |
| 50,000        | Mean      | Verified| PASS   |
| 50,000        | History   | 2+ ver | PASS   |

*Temporal queries are now optimized for zero-copy data extraction, ensuring analytical parity between the Go server and Python SDK.*

---
**Build Tag**: `v0.1.9-production`
**Platform**: Darwin arm64 (Apple M3 Pro)
**Date**: 2026-04-23
