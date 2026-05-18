# Longbow Storage Engine - Future Roadmap & Next Steps

This document outlines the outstanding roadmap items and stability enhancements prioritized for the upcoming releases of the Longbow Vector Storage Engine.

---

## 1. P0 Blockers: Hardware Backend Integration

### 🔳 TPU Physical Driver Integration
* **Description**: Replace the currently implemented architectural stubs and metrics instrumentation in `internal/gpu/tpu/tpu_index.go` with native `libtpu.so` physical driver bindings once the hardware-linked libraries are provided by the vendor.
* **Impact**: Enables actual hardware acceleration on TPU-equipped hosts, completing compliance with the v0.2.3 hardware acceleration contract.
* **Metric Observability**: Leverage the existing Prometheus metrics framework (`longbow_tpu_ops_total`, `longbow_tpu_latency_seconds`) to track hardware execution in production environments.
* **Target Release**: `v0.2.3-rc2`
* **Priority**: **P0 (Critical Blocker)**

---

## 2. P1 Tasks: Continuous Performance Assurance

### 🔳 Automated CI/CD Performance Regression Framework
* **Description**: Integrate the streamlined performance benchmarks (`scripts/local_perf.sh` and `scripts/remote_perf_clean.sh`) into the repository's GitHub Actions / GitLab CI pipeline.
* **Impact**: Detects QPS and ingestion rate regressions automatically on every Pull Request, enforcing high performance budgets.
* **Target Release**: `v0.2.4`
* **Priority**: **P1 (High)**

### 🔳 Slab Pool Autoscaling & Metrics Hardening
* **Description**: Instrument the dynamic off-heap `SlabPool` resizing and self-healing operations with Prometheus telemetry to track memory capacity expansion events and buffer hit ratios.
* **Impact**: Provides operational engineers with deep real-time observability of off-heap pool states under heavy burst ingestion loads.
* **Target Release**: `v0.2.4`
* **Priority**: **P1 (High)**

---

## 3. P2 Tasks: Storage Architecture Optimizations

### 🔳 Disk-ANN Solid State Page Tuning
* **Description**: Optimize low-level SSD page alignment and read-ahead block sizing in the Disk-ANN index offloader for sub-millisecond multi-gigabyte vector fetches.
* **Impact**: Achieves ultra-low latency searches on datasets exceeding physical RAM sizes.
* **Target Release**: `v0.2.5`
* **Priority**: **P2 (Medium)**

