# Longbow Storage Engine - Future Roadmap & Next Steps

This document outlines the outstanding roadmap items, stability enhancements, and long-term architectural designs prioritized for upcoming releases of the Longbow Vector Storage Engine.

---

## 0. Actionable Stability Guidelines & Regression Remediation (Commit: f13ca8ee)

> [!IMPORTANT]
> Historical regressions detected between `v0.2.0` and `v0.2.1` highlighted critical vulnerabilities in core search pathways, lock scheduling, capacity growth, and location pointer indexing. To prevent any future performance regressions, the following strict architectural guidelines and remediation steps must be enforced across all upcoming storage engine developments.

### 🛡️ Core Stability Guidelines for Core Storage Engine Pathways

1. **Avoid Chunk-Space Lookup Thrashing in Inner Loops**:
   - *Guideline*: Never perform index, chunk, or record batch pointer lookups using Go maps or slice iteration inside hot search loops (such as dense/sparse indexing, HNSW neighbor checks, or quantization distance calculation).
   - *Remediation*: All target arrays must be pre-extracted using native Arrow slice wrappers or dedicated memory computers (e.g., `sharedFloat32Computer`/`sharedInt8Computer`) outside the loop, bypassing Go map registry overhead.

2. **Strict Lock Ordering to Avoid HNSW Deadlocks**:
   - *Guideline*: When updating or reading HNSW graph levels and entry nodes, ensure a deterministic lock acquisition hierarchy. Acquire entry locks before node collection lock segments, and release them immediately after traversal.
   - *Remediation*: Restructure graph mutation logic to avoid holding multiple write locks across network-boundary gRPC requests or channel operations.

3. **Atomic Location Pointer and Migration Operations**:
   - *Guideline*: Ensure sharded index migrations or location pointer modifications occur atomically. During page split operations in location stores, protect mapping states with double-checked locks and atomic pointer swaps (`unsafe.Pointer`).
   - *Remediation*: Guarantee that any background compaction or split does not leave standard pointers in a partially migrated state, which previously caused complete index search degradation (0 QPS).

4. **Quantization (TurboQuant) Capacity-Growth Configuration Safekeeping**:
   - *Guideline*: Memory or storage indexing capacity expansions (e.g., expanding from 10k to 25k/100k vectors) must atomically copy and preserve all quantization metadata (e.g., codebook vectors, scale dimensions, and training subsets).
   - *Remediation*: Any slice re-allocation must employ deep copies rather than superficial pointer copies to prevent capacity-growth erasures or `tq vector not found` errors.

5. **Prometheus Telemetry Coverage for Memory Bounds**:
   - *Guideline*: Dynamically instrument all off-heap page pools (`SlabPool`) and registry indices with hit/miss ratios, boundary check violations, and memory growth events. Ensure memory limits are enforced strictly against capping boundaries.

---

## 1. P0 Blockers: Hardware Backend Integration (0.2.3 roadmap)

### 🔳 TPU Physical Driver Integration

* **Description**: Replace the currently implemented architectural stubs and metrics instrumentation in `internal/gpu/tpu/tpu_index.go` with native `libtpu.so` physical driver bindings once the hardware-linked libraries are provided by the vendor.
* **Impact**: Enables actual hardware acceleration on TPU-equipped hosts, completing compliance with the v0.2.3 hardware acceleration contract.
* **Metric Observability**: Leverage the existing Prometheus metrics framework (`longbow_tpu_ops_total`, `longbow_tpu_latency_seconds`) to track hardware execution in production environments.
* **Target Release**: `v0.2.3-rc2`
* **Priority**: **P0 (Critical Blocker)**

---

## 2. 0.2.2 roadmap

This section lists the high-scale enhancements, continuous performance assurance systems, storage optimizations, and security integrations prioritized for the `v0.2.2` release.

### 🔳 Slab Pool Autoscaling & Metrics Hardening

* **Description**: Instrument the dynamic off-heap `SlabPool` resizing and self-healing operations with Prometheus telemetry to track memory capacity expansion events and buffer hit ratios.
* **Impact**: Provides operational engineers with deep real-time observability of off-heap pool states under heavy burst ingestion loads.
* **Target Release**: `v0.2.2`
* **Priority**: **P1 (High)**

### 🔳 Disk-ANN Solid State Page Tuning

* **Description**: Optimize low-level SSD page alignment and read-ahead block sizing in the Disk-ANN index offloader for sub-millisecond multi-gigabyte vector fetches.
* **Impact**: Achieves ultra-low latency searches on datasets exceeding physical RAM sizes.
* **Target Release**: `v0.2.2`
* **Priority**: **P2 (Medium)**

### 🔳 EMLgo Library Math Acceleration

* **Description**: Create a dedicated test branch to integrate and evaluate the **EMLgo** library as a drop-in replacement for standard Go `math` functions inside performance-critical distance metrics and SIMD kernels.
* **Impact**: Unlocks potential performance gains for non-assembly fallback routines and transcendental functions across large vector computations.
* **Subtasks**:
  * [ ] Spin up a dedicated test branch `experiment/emlgo-math-evaluation`.
  * [ ] Replace standard library `math` calls with `emlgo` mathematical equivalents in baseline metric implementations.
  * [ ] Execute the unified benchmark runner across various dimensions (128, 384, 768, 1024, 3072) to capture direct QPS/latency comparisons.
  * [ ] Document compilation stability and any potential hardware dependencies under Go cross-platform builds.
* **Target Release**: `v0.2.2`
* **Priority**: **Medium**

### 🔳 Google ALTS Integration

This 6-part integration plan outlines the architectural and implementation tasks required to support Google's **Application Layer Transport Security (ALTS)** for secure, mutually authenticated, and high-performance service-to-service gRPC communication inside Google Compute Engine (GCE) and Google Kubernetes Engine (GKE).

#### 🔳 Part 1: Design & Protocol Analysis

* **Description**: Analyze GCE/GKE ALTS requirements, transport ciphers (AES-128-GCM vs integrity-only GMAC), and connection lifecycles to design the Longbow gRPC security architecture.
* **Subtasks**:
  * [ ] Map out trust domains and identify peer service account naming formats inside GKE/GCE.
  * [ ] Evaluate ALTS handshake latency and performance overhead compared to traditional TLS.
  * [ ] Document fallback security policies for non-GCP development/local testing.
  * [ ] Define the authorization schema based on Google service account structures.
* **Target Release**: `v0.2.2`
* **Priority**: **High**

#### 🔳 Part 2: gRPC Server ALTS Credentials Integration

* **Description**: Implement server-side ALTS gRPC transport credentials to secure incoming connections from client SDKs.
* **Subtasks**:
  * [ ] Integrate Go gRPC ALTS server credentials (`credentials/alts` package) via `alts.NewServerCreds()`.
  * [ ] Configure server startup configuration to dynamically load ALTS options if GCP environment is detected.
  * [ ] Implement handshake timeout controls to prevent resource exhaustion during connection handshakes.
  * [ ] Build server-side fallback to standard TLS or local credentials for hybrid deployments.
* **Target Release**: `v0.2.2`
* **Priority**: **Critical**

#### 🔳 Part 3: gRPC Client ALTS Credentials Integration

* **Description**: Configure the client-side gRPC dialer to request mutually authenticated ALTS channels.
* **Subtasks**:
  * [ ] Integrate client-side credentials handler via `alts.NewClientCreds()`.
  * [ ] Implement targeted peer verification by configuring target service accounts on connection dial.
  * [ ] Configure client reconnection policies and session resumption handles.
  * [ ] Build client command-line flags (e.g. `--use-alts`) and auto-detection parameters.
* **Target Release**: `v0.2.2`
* **Priority**: **Critical**

#### 🔳 Part 4: Peer Authentication & Context Parsing

* **Description**: Extract client/peer authentication metadata from gRPC incoming context to enforce identity-based access control.
* **Subtasks**:
  * [ ] Implement gRPC interceptors to extract ALTS `AuthInfo` using `alts.AuthInfoFromContext`.
  * [ ] Parse client credentials to extract Google service accounts and project details.
  * [ ] Build a robust service account authorization engine supporting whitelist/blacklist rules.
  * [ ] Log peer identities for all write/ingestion actions to satisfy security auditing requirements.
* **Target Release**: `v0.2.2`
* **Priority**: **High**

#### 🔳 Part 5: Observability, Metrics & Telemetry

* **Description**: Instrument the authentication and handshake layer with Prometheus telemetry to ensure operational visibility.
* **Subtasks**:
  * [ ] Create Prometheus counters for ALTS handshake success (`longbow_alts_handshake_success_total`) and failure (`longbow_alts_handshake_failure_total`).
  * [ ] Implement histogram metrics for ALTS handshake latency (`longbow_alts_handshake_latency_seconds`).
  * [ ] Add counters for unauthorized service account access attempts categorized by identity.
  * [ ] Build alerts for authentication failures and service account authorization mismatches.
* **Target Release**: `v0.2.2`
* **Priority**: **Medium**

#### 🔳 Part 6: Multi-Node Deployment & GCE/GKE Validation

* **Description**: Package, deploy, and validate the ALTS configuration on a multi-node GCP environment.
* **Subtasks**:
  * [ ] Package Longbow within Docker images configured with GKE workload identity support.
  * [ ] Deploy test server and client instances on GKE/GCE instances.
  * [ ] Execute automated connection and data validation suites across nodes using ALTS transport.
  * [ ] Verify session resumption ciphers and validate the security posturing on wide area networks (WAN).
* **Target Release**: `v0.2.2`
* **Priority**: **High**

---

## 3. Historical Performance Observations & Regression Analyses

This section traces and documents historical vector index performance regressions observed across release branches since the `v0.2.0` milestone, along with their engineering resolutions.

### 🔳 Performance Regression Trace & Resolutions

* **Float32 High-Dimensional Fragmentation (v0.1.7-rc2)**:
  - *Symptom*: Rapid drop in `Search_Dense` QPS at high counts due to page-level fragmentation of float32 vector tables during capacity scaling.
  - *Remediation*: Implemented HNSW pre-allocation schemas inside the vector table `Grow` operations and increased the default `InitialCapacity` bounds to 50k. Optimized concurrency parameters for background index worker pools to prevent CPU thread thrashing.
* **Warmup Artifacts and Float16 Discrepancies (v0.1.8-rc4)**:
  - *Symptom*: Apparent float16 throughput degradation during rapid benchmark cycles.
  - *Remediation*: Confirmed to be an instrumentation warmup artifact rather than a true kernel-level regression. Fixed by adding warm-up iteration loops inside `bench-tool` and adding socket cooling cycles.
* **Migration Flow Location Store Distortion (v0.2.1-rc1)**:
  - *Symptom*: Complete regression where `Search_Dense` yielded 0 QPS at vector counts $\ge 10,000$ under shard-level migrations.
  - *Remediation*: Identified location store pointer distortion occurring during sharded database page splits. Corrected migration state mappings to safely retain offset metadata during table grows.
* **TurboQuant Capacity State Erasure (v0.2.1-rc1)**:
  - *Symptom*: Index add crashes (`tq vector N not found` errors) and thread hangs during async batched ingestion at count $\ge 25,000$.
  - *Remediation*: Fixed state corruption bug where TurboQuant-specific quantizer metadata maps were being erased/re-allocated during background off-heap capacity growth. Hardened bit-width persistence to survive dynamic memory transitions.

