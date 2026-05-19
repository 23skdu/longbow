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

---

## 4. 0.2.2 Roadmap Items: EMLgo Math Library Evaluation

### 🔳 EMLgo Library Math Acceleration
* **Description**: Create a dedicated test branch to integrate and evaluate the **EMLgo** library as a drop-in replacement for standard Go `math` functions inside performance-critical distance metrics and SIMD kernels.
* **Impact**: Unlocks potential performance gains for non-assembly fallback routines and transcendental functions across large vector computations.
* **Subtasks**:
  - [ ] Spin up a dedicated test branch `experiment/emlgo-math-evaluation`.
  - [ ] Replace standard library `math` calls with `emlgo` mathematical equivalents in baseline metric implementations.
  - [ ] Execute the unified benchmark runner across various dimensions (128, 384, 768, 1024, 3072) to capture direct QPS/latency comparisons.
  - [ ] Document compilation stability and any potential hardware dependencies under Go cross-platform builds.
* **Priority**: **Medium**
* **Target Release**: `v0.2.2` (Experimental)

---

## 5. 0.2.2 Roadmap Items: Google ALTS Integration

This 6-part integration plan outlines the architectural and implementation tasks required to support Google's **Application Layer Transport Security (ALTS)** for secure, mutually authenticated, and high-performance service-to-service gRPC communication inside Google Compute Engine (GCE) and Google Kubernetes Engine (GKE).

### 🔳 Part 1: Design & Protocol Analysis
* **Description**: Analyze GCE/GKE ALTS requirements, transport ciphers (AES-128-GCM vs integrity-only GMAC), and connection lifecycles to design the Longbow gRPC security architecture.
* **Subtasks**:
  - [ ] Map out trust domains and identify peer service account naming formats inside GKE/GCE.
  - [ ] Evaluate ALTS handshake latency and performance overhead compared to traditional TLS.
  - [ ] Document fallback security policies for non-GCP development/local testing.
  - [ ] Define the authorization schema based on Google service account structures.
* **Priority**: **High**
* **Target Release**: `v0.2.2-rc3`

### 🔳 Part 2: gRPC Server ALTS Credentials Integration
* **Description**: Implement server-side ALTS gRPC transport credentials to secure incoming connections from client SDKs.
* **Subtasks**:
  - [ ] Integrate Go gRPC ALTS server credentials (`credentials/alts` package) via `alts.NewServerCreds()`.
  - [ ] Configure server startup configuration to dynamically load ALTS options if GCP environment is detected.
  - [ ] Implement handshake timeout controls to prevent resource exhaustion during connection handshakes.
  - [ ] Build server-side fallback to standard TLS or local credentials for hybrid deployments.
* **Priority**: **Critical**
* **Target Release**: `v0.2.2-rc3`

### 🔳 Part 3: gRPC Client ALTS Credentials Integration
* **Description**: Configure the client-side gRPC dialer to request mutually authenticated ALTS channels.
* **Subtasks**:
  - [ ] Integrate client-side credentials handler via `alts.NewClientCreds()`.
  - [ ] Implement targeted peer verification by configuring target service accounts on connection dial.
  - [ ] Configure client reconnection policies and session resumption handles.
  - [ ] Build client command-line flags (e.g. `--use-alts`) and auto-detection parameters.
* **Priority**: **Critical**
* **Target Release**: `v0.2.2-rc3`

### 🔳 Part 4: Peer Authentication & Context Parsing
* **Description**: Extract client/peer authentication metadata from gRPC incoming context to enforce identity-based access control.
* **Subtasks**:
  - [ ] Implement gRPC interceptors to extract ALTS `AuthInfo` using `alts.AuthInfoFromContext`.
  - [ ] Parse client credentials to extract Google service accounts and project details.
  - [ ] Build a robust service account authorization engine supporting whitelist/blacklist rules.
  - [ ] Log peer identities for all write/ingestion actions to satisfy security auditing requirements.
* **Priority**: **High**
* **Target Release**: `v0.2.2-rc3`

### 🔳 Part 5: Observability, Metrics & Telemetry
* **Description**: Instrument the authentication and handshake layer with Prometheus telemetry to ensure operational visibility.
* **Subtasks**:
  - [ ] Create Prometheus counters for ALTS handshake success (`longbow_alts_handshake_success_total`) and failure (`longbow_alts_handshake_failure_total`).
  - [ ] Implement histogram metrics for ALTS handshake latency (`longbow_alts_handshake_latency_seconds`).
  - [ ] Add counters for unauthorized service account access attempts categorized by identity.
  - [ ] Build alerts for authentication failures and service account authorization mismatches.
* **Priority**: **Medium**
* **Target Release**: `v0.2.2-rc3`

### 🔳 Part 6: Multi-Node Deployment & GCE/GKE Validation
* **Description**: Package, deploy, and validate the ALTS configuration on a multi-node GCP environment.
* **Subtasks**:
  - [ ] Package Longbow within Docker images configured with GKE workload identity support.
  - [ ] Deploy test server and client instances on GKE/GCE instances.
  - [ ] Execute automated connection and data validation suites across nodes using ALTS transport.
  - [ ] Verify session resumption ciphers and validate the security posturing on wide area networks (WAN).
* **Priority**: **High**
* **Target Release**: `v0.2.2-rc3`

