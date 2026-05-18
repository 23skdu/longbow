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
