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

## 2. P1 Improvements: Core Allocation & Scale Hardening

### 🔳 Self-Resizing SlabPool & Dynamic Slab Capacity Expansion
* **Description**: Implement dynamic slab capacity expansion or a self-resizing `SlabPool` during index migrations.
* **Finding**: Under high-dimensional datasets and large-scale graph migrations (e.g., dynamic autosharding at 10,000+ vector boundaries), contiguous graph blocks may exceed the default pool capacity (e.g., triggering `"alloc request 2097152 exceeds slab capacity 1048576"`), resulting in a graceful abort.
* **Solution**: Automatically expand slab pool allocations or transition gracefully to dynamically sized off-heap slices for large contiguous requests.
* **Target Release**: `v0.2.4`
* **Priority**: **P1 (High)**

---

## 3. P2 Enhancements: Benchmark & Tooling Productivity

### 🔳 In-Place Benchmark Dataset Reset Interface
* **Description**: Add a `-reset` flag to `bench-tool` (or expose a dedicated gRPC endpoint) to drop and recreate indexing datasets in-place without restarting the entire Longbow server.
* **Impact**: Replaces the current server-restart pattern which introduces 5-10 seconds of overhead per test configuration, accelerating total execution of the 400+ benchmark matrix by over **45 minutes**.
* **Target Release**: `v0.2.4`
* **Priority**: **P2 (Medium)**
