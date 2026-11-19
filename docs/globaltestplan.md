# Longbow Global GraphRAG & Clustering Test Plan (Google Cloud)

## 1. Overview & Objective

This test plan defines how to provision, configure, execute, and validate a **multi-node Longbow cluster on Google Cloud Platform (GCP)** to test distributed **SWIM gossip clustering**, **GlobalSearch scatter-gather**, **GlobalGraphRAG** multi-hop traversal, **elastic sharding**, and **fault tolerance**.

It complements [docs/testplan.md](testplan.md) (single-node benchmark matrix and A/B gating methodology) and [docs/performance.md](performance.md) (empirical single-node CPU/AVX2/AVX-512 baselines).

### Why a Multi-Node Cloud Cluster Is Required

As documented in [docs/graphrag.md](graphrag.md), when Longbow runs as a standalone single-node process (`s.Mesh == nil`), requests carrying `x-longbow-global: "true"` silently fall back to the local `GraphRAG` execution path. Single-node runs validate ticket parsing and local graph traversal, **but do not exercise**:

1. SWIM UDP gossip membership discovery, failure detection (`suspicionTimeout`), and cluster convergence.
2. Distributed `GlobalSearchCoordinator` scatter-gather fan-out across live peers (`internal/store/global_search.go`).
3. Cross-node Reciprocal Rank Fusion (RRF), global ID deduplication, and multi-node Arrow Flight streaming.
4. Cross-partition `GlobalGraphRAG` expansion, spreading activation across sharded knowledge graphs, and `DoExchange` Merkle delta sync.
5. Circuit-breaker trip/recovery, hedged peer queries, partial-failure degradation, and multi-region failover.

### Primary Goals

1. Validate cluster formation and discovery across GCP VPC (`static` peers) and GKE (`k8s` / `dns` SRV) providers.
2. Verify **GlobalGraphRAG** and **GlobalSearch** correctness (recall@10 ≥ 0.95 vs single-node full-corpus ground truth) across sharded partitions.
3. Measure scatter-gather QPS, fan-out latency overhead (P50/P95/P99), and horizontal throughput scaling across **3, 5, and 7 nodes**.
4. Validate resilience under node kills, network latency/packet-loss injection (`tc netem`), and elastic shard split/merge migrations.

---

## 2. Recommended Google Cloud Topologies

Two GCP topologies are supported depending on the test goal. Both follow the AVX-512 server concurrency rules established in [docs/testplan.md](testplan.md) §4.1: **homogeneous dedicated vCPUs, unpinned CPU cores (omit `--cpu-affinity`), and ≥8 concurrent client workers**.

| Topology | Best For | Discovery Provider | Node Count | Isolation |
| :--- | :--- | :--- | :--- | :--- |
| **Option A: GKE Standard + Helm** | Production-parity validation, K8s/DNS discovery, rolling restarts, HPA/elastic sharding | `k8s` or `dns` (`_gossip._udp`) | 3 / 5 / 7 pods (1 pod/node) | PodAntiAffinity + dedicated node pool |
| **Option B: Multi-VM GCE VPC** | Low-level `pprof`, `tc netem` fault injection, raw NIC latency, deterministic A/B runs | `static` (internal VPC IPs) | 3 / 5 VMs + 1 driver VM | Dedicated VMs in same VPC subnet |

### 2.1 Recommended Machine Types & Sizing

Match or exceed the AVX-512 server baseline in [docs/testplan.md](testplan.md) §2.2 (`16 vCPU Xeon, 125 GiB RAM`):

| Role | GCP Machine Type | CPU / ISA | RAM | Local / Disk | Purpose |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Longbow Data/Meta Node (Standard)** | `c3-highmem-16` (or `n2-highmem-16`) | 16 vCPU Intel Xeon (AVX-512, FMA, VNNI) | 128 GiB | 200 GB `pd-ssd` / Local NVMe | 100k–1M vectors per shard, 75 GiB `LONGBOW_MAX_MEMORY`, headroom for `autoSharding` migration |
| **Longbow Node (Cost-Optimized Smoke)** | `c3-standard-8` (or `n2-standard-8`) | 8 vCPU Intel Xeon (AVX-512) | 32 GiB | 100 GB `pd-ssd` | Functional 3-node mesh & GlobalGraphRAG smoke tests (50k–100k/node) |
| **Benchmark / Load Driver VM** | `c3-standard-16` | 16 vCPU Intel Xeon | 64 GiB | 100 GB `pd-balanced` | Runs `bin/bench-tool`, `longbow-cli`, `scripts/unified_benchmark.py`, and Prometheus without sharing CPU/NIC with server nodes |

> [!IMPORTANT]
> **Dedicated Driver Host**: Always run `bin/bench-tool` or `scripts/ab_benchmark.py` from a separate driver VM/Pod in the same VPC/zone. Running the load generator on one of the Longbow nodes skews that node's P99 latency and distorts scatter-gather tail measurements.

### 2.2 Required Network Ports & GCP VPC Firewall Rules

Every Longbow node exposes four core ports (plus optional RDMA):

| Port | Protocol | Env Var | Purpose |
| :--- | :--- | :--- | :--- |
| `3000` | TCP (gRPC / Arrow Flight) | `LONGBOW_LISTEN_ADDR` | Data Plane (`DoPut`, local `DoGet`, `DoExchange` sync) |
| `3001` | TCP (gRPC / Arrow Flight) | `LONGBOW_META_ADDR` | Control Plane (`GlobalSearch`, `GlobalGraphRAG`, `DoAction`, admin) |
| `7946` | UDP | `LONGBOW_GOSSIP_PORT` | SWIM gossip heartbeats, ping/ack, indirect probes, membership updates |
| `9090` | TCP (HTTP) | `LONGBOW_METRICS_ADDR` | Prometheus `/metrics`, REST v1 `/api/v1/*`, GraphQL `/api/v1/graphql`, `/debug/pprof` |
| `3002` | TCP (optional) | `LONGBOW_RDMA_PORT` | RDMA / RoCEv2 transport (disabled by default on GCE) |

Create an internal VPC firewall rule allowing peer-to-peer cluster traffic and driver-to-cluster traffic:

```bash
gcloud compute firewall-rules create longbow-cluster-internal \
  --network=default \
  --direction=INGRESS \
  --action=ALLOW \
  --rules=tcp:3000,tcp:3001,tcp:9090,udp:7946 \
  --source-tags=longbow-node,longbow-driver \
  --target-tags=longbow-node
```

---

## 3. Cluster Setup Runbooks

### 3.1 Option A: GKE Standard + Helm (`helm/longbow`)

Use a dedicated node pool with one Longbow pod per VM so HNSW memory bandwidth and L3 cache are never shared across pods.

```bash
export GCP_PROJECT="<your-project-id>"
export GCP_ZONE="us-central1-a"
export CLUSTER_NAME="longbow-global-test"

# 1. Create GKE Standard cluster with 3 AVX-512 nodes
gcloud container clusters create "${CLUSTER_NAME}" \
  --project="${GCP_PROJECT}" \
  --zone="${GCP_ZONE}" \
  --machine-type="c3-highmem-16" \
  --num-nodes=3 \
  --disk-type="pd-ssd" \
  --disk-size=200 \
  --scopes="https://www.googleapis.com/auth/cloud-platform"

gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --zone="${GCP_ZONE}" --project="${GCP_PROJECT}"

# 2. Deploy Longbow via Helm with SWIM gossip + K8s discovery enabled
helm upgrade --install longbow ./helm/longbow \
  --namespace longbow --create-namespace \
  --set replicaCount=3 \
  --set image.tag=latest \
  --set gossip.enabled=true \
  --set gossip.port=7946 \
  --set gossip.protocolPeriod="200ms" \
  --set gossip.ackTimeout="100ms" \
  --set gossip.suspicionTimeout="5s" \
  --set gossip.discovery.provider="k8s" \
  --set gossip.discovery.labelSelector="app.kubernetes.io/name=longbow" \
  --set ringSharding.enabled=true \
  --set autoSharding.enabled=false \
  --set memory.maxMemoryHard="75GB" \
  --set memory.autoSpillDisk=true \
  --set memory.spillThresholdRatio="0.75" \
  --set env.LONGBOW_HNSW_MMAX0="32" \
  --set env.LONGBOW_HYBRID_TEXT_COLUMNS="text,category"
```

Verify pod readiness and gossip mesh convergence:

```bash
kubectl -n longbow rollout status statefulset/longbow --timeout=180s
kubectl -n longbow get pods -o wide

# Verify each pod sees all 3 active gossip members
for pod in $(kubectl -n longbow get pods -l app.kubernetes.io/name=longbow -o name); do
  echo "=== ${pod} ==="
  kubectl -n longbow exec "${pod}" -- wget -qO- http://127.0.0.1:9090/metrics | grep -E 'longbow_gossip_active_members|longbow_global_search_'
done
```

### 3.2 Option B: Multi-VM GCE Cluster (Raw Profiling & Chaos Injection)

Use GCE VMs when running `tc netem` chaos injection, collecting `pprof` profiles, or executing interleaved A/B binary comparisons (`scripts/ab_benchmark.py`).

```bash
export GCP_PROJECT="<your-project-id>"
export GCP_ZONE="us-central1-a"

# 1. Provision 3 Longbow server VMs + 1 driver VM
gcloud compute instances create longbow-0 longbow-1 longbow-2 \
  --project="${GCP_PROJECT}" \
  --zone="${GCP_ZONE}" \
  --machine-type="c3-highmem-16" \
  --boot-disk-type="pd-ssd" \
  --boot-disk-size=200GB \
  --image-family="ubuntu-2404-lts-amd64" \
  --image-project="ubuntu-os-cloud" \
  --tags=longbow-node

gcloud compute instances create longbow-driver \
  --project="${GCP_PROJECT}" \
  --zone="${GCP_ZONE}" \
  --machine-type="c3-standard-16" \
  --boot-disk-type="pd-balanced" \
  --boot-disk-size=100GB \
  --image-family="ubuntu-2404-lts-amd64" \
  --image-project="ubuntu-os-cloud" \
  --tags=longbow-driver

# 2. Collect internal VPC IPs
IP0=$(gcloud compute instances describe longbow-0 --zone="${GCP_ZONE}" --format='get(networkInterfaces[0].networkIP)')
IP1=$(gcloud compute instances describe longbow-1 --zone="${GCP_ZONE}" --format='get(networkInterfaces[0].networkIP)')
IP2=$(gcloud compute instances describe longbow-2 --zone="${GCP_ZONE}" --format='get(networkInterfaces[0].networkIP)')
STATIC_PEERS="${IP0}:7946,${IP1}:7946,${IP2}:7946"
echo "STATIC_PEERS=${STATIC_PEERS}"
```

On each Longbow VM (`longbow-0`, `longbow-1`, `longbow-2`), build or pull the binary and launch with its own internal IP as `LONGBOW_GOSSIP_ADVERTISE_ADDR`:

```bash
# Example launch environment on longbow-i (set MY_IP to this VM's internal VPC IP)
export MY_IP="<internal-ip-of-this-vm>"
export STATIC_PEERS="<ip0>:7946,<ip1>:7946,<ip2>:7946"

export LONGBOW_LISTEN_ADDR="0.0.0.0:3000"
export LONGBOW_META_ADDR="0.0.0.0:3001"
export LONGBOW_METRICS_ADDR="0.0.0.0:9090"
export LONGBOW_DATA_PATH="/var/lib/longbow/data"
export LONGBOW_MAX_MEMORY="80530636800"        # 75 GiB (leaves headroom on 128 GiB host)
export LONGBOW_AUTO_SPILL_DISK="true"
export LONGBOW_HNSW_MMAX0="32"
export LONGBOW_HYBRID_TEXT_COLUMNS="text,category"

# SWIM Gossip Mesh
export LONGBOW_GOSSIP_ENABLED="true"
export LONGBOW_GOSSIP_PORT="7946"
export LONGBOW_GOSSIP_INTERVAL="200ms"
export LONGBOW_GOSSIP_ADVERTISE_ADDR="${MY_IP}"
export LONGBOW_GOSSIP_DISCOVERY_PROVIDER="static"
export LONGBOW_GOSSIP_STATIC_PEERS="${STATIC_PEERS}"

# Sharding (keep auto-sharding off for baseline; enable explicitly in Suite 5)
export LONGBOW_RING_SHARDING_ENABLED="true"
export LONGBOW_AUTO_SHARDING_ENABLED="false"

# Optional mTLS for production-grade security validation
# export LONGBOW_TLS_ENABLED="true"
# export LONGBOW_TLS_REQUIRE_CLIENT_CERT="true"
# export LONGBOW_TLS_CERT_FILE="/etc/longbow/tls/server.crt"
# export LONGBOW_TLS_KEY_FILE="/etc/longbow/tls/server.key"
# export LONGBOW_TLS_CA_FILE="/etc/longbow/tls/ca.crt"

./bin/longbow_main > /var/log/longbow.log 2>&1 &
```

### 3.3 Multi-Zone & Multi-Region Placement (Optional)

- **Multi-Zone HA (`us-central1-a`, `us-central1-b`, `us-central1-c`)**: Spread the 3 VMs across 3 zones in one region (~0.5–1.5 ms inter-zone RTT) to validate hedged queries and zone-loss survivability.
- **Multi-Region Failover (`us-central1` + `us-east1`)**: Deploy a secondary 3-node cluster in `us-east1` (~30–40 ms RTT) to validate `CreateCloudCluster`, `AddReplicaRegion`, `ReplicateClusterMutation`, `PromoteClusterRegion`, and `GetCloudSLAReport`.

---

## 4. Configuration Fidelity Pre-Flight Checks

Following [docs/testplan.md](testplan.md) §4.2a, verify runtime state from every node before running any test suite:

1. **Cluster Membership & Identity**:
   - Call `cluster-status` / `MeshStatus` / `DiscoveryStatus` against port `3001` on every node.
   - Confirm `longbow_gossip_active_members == N` (e.g., `3`, `5`, or `7`) on all nodes' `:9090/metrics`.
2. **Effective HNSW & Memory Parameters**:
   - Confirm startup logs report `LONGBOW_HNSW_MMAX0=32` (not just `LONGBOW_MAX_M0`), `Dims` derived from Arrow schema on first ingest, and expected `LONGBOW_MAX_MEMORY`.
3. **Zero-Noise Baseline**:
   - Confirm no stale datasets or WAL/snapshot files exist in `LONGBOW_DATA_PATH`.
   - Measure inter-node baseline RTT (`ping -c 20 <peer-ip>`) and record it alongside benchmark JSON outputs.

---

## 5. Test Suites & Execution Matrix

### Suite 1: SWIM Gossip Mesh Formation & Discovery

| Test ID | Scenario | Method | Pass Criteria |
| :--- | :--- | :--- | :--- |
| **CL-1** | Cold cluster bootstrap (`static`, `k8s`, `dns`) | Start 3 nodes simultaneously; poll `longbow_gossip_active_members` every 100ms | All 3 nodes reach `active_members == 3` within **≤ 2.0s** |
| **CL-2** | Scale-out join (3 → 5 → 7 nodes) | Add 2 nodes at a time while serving 500 QPS background dense search | Zero dropped queries; new nodes converge within **≤ 3.0s** and appear in `MeshStatus` |
| **CL-3** | Clean departure & rejoin | Gracefully stop `longbow-2` (`SIGTERM`), wait 10s, restart | Remaining nodes converge to `active_members == 2` within `suspicionTimeout` (`5s`) + 2s, and back to `3` on rejoin |
| **CL-4** | Split-brain / asymmetric UDP drop | Block UDP `7946` between `longbow-0` and `longbow-2` (keeping `longbow-1` reachable) | SWIM indirect probe via `longbow-1` keeps `longbow-2` alive (zero false eviction) |

### Suite 2: Sharded Ingestion & Cross-Partition Knowledge Graph Construction

Build a deterministic sharded dataset (seed `42`) across all `N` nodes with **explicit cross-partition graph edges** so multi-hop traversal must cross node boundaries:

1. **Vector Corpus**:
   - Scale tiers: **100k** (smoke), **250k** (mid), **500k** (large), **1M** total vectors distributed across `N` nodes via consistent hashing (`ringSharding`).
   - Dims: `128`, `384`, `768`.
   - Dtypes: `float32`, `int8`, `float16`, `complex128`, `turboquant4` (covering dense, quantized, complex, and TurboQuant paths from [docs/performance.md](performance.md)).
2. **Knowledge Graph Edges (`add-edge`)**:
   - Ingest `5–10` semantic edges per node (`related_to`, `cites`, `belongs_to`) with **≥ 40% of edges crossing shard boundaries** (source ID owned by `longbow-i`, target ID owned by `longbow-j`, `i ≠ j`).
   - Verify `GetGraphStats` and `longbow_graph_store_edge_count` match expected edge totals across the cluster.

### Suite 3: Distributed `GlobalGraphRAG` & `GlobalSearch` Correctness and Performance

Run from `longbow-driver` against `longbow-0:3001` (Control Plane) and round-robin across all nodes:

```bash
# 1. Run bench-tool across local vs global GraphRAG and core distributed modes
./bin/bench-tool \
  -uri "${IP0}:3001" \
  -dataset global_graphrag_bench \
  -dim 384 \
  -scale 100000 \
  -dtype float32 \
  -search-modes dense,hybrid,graphrag,globalgraphrag,recommend,federated \
  -queries 500 \
  -workers 8 \
  -seed 42 \
  -json /tmp/global_3node_384d_f32.json

# 2. Verify GlobalGraphRAG header routing via CLI / SDK
# Local GraphRAG (single-node partition only):
longbow-cli search -uri "grpc://${IP0}:3001" -dataset global_graphrag_bench \
  -mode hybrid -vector "..." -text "electronics" -k 10

# GlobalGraphRAG (includes x-longbow-global: true metadata header -> scatter-gather)
```

| Test ID | Scenario | Verification Checks | Pass Criteria |
| :--- | :--- | :--- | :--- |
| **GG-1** | Local `GraphRAG` vs Distributed `GlobalGraphRAG` delta | Compare hit IDs & scores between `LocalOnly=true` and `x-longbow-global: "true"` on a 3-node sharded corpus | `GlobalGraphRAG` returns hits residing on **all 3 nodes**; `longbow_global_search_fanout_size` records `2` remote peers per query |
| **GG-2** | Distributed Recall Parity vs Single-Node Ground Truth | Load identical 100k corpus + graph onto 1 standalone node and onto 3 sharded nodes; run 200 identical `GlobalGraphRAG` queries (`graph_alpha ∈ {0.25, 0.5, 0.75}`, `graph_depth ∈ {1, 2, 3}`) | Top-10 recall parity **≥ 0.95**; zero duplicate IDs in merged RRF response |
| **GG-3** | Multi-Hop Cross-Partition Expansion | Query seed IDs whose 2-hop and 3-hop neighbors live on remote peers (`traverse-graph`, `GraphRAGExpand`, `calculate-pagerank`, `detect-communities`) | `longbow_graphrag_nodes_visited_total` and `longbow_graph_rag_expanded_nodes_total` include cross-shard neighbors; `longbow_graphrag_expansion_errors_total == 0` |
| **GG-4** | `GlobalGeoSearch` & Federated Cross-Collection | Execute `GlobalGeoSearch` (`radius`/`box`/`hybrid`) and `federated_search` across sharded collections | All matching peers contribute; RRF ordering deterministic across repeated runs with same seed |
| **GG-5** | Fan-Out Scaling (3 vs 5 vs 7 nodes) | Hold per-node corpus constant (200k/node → 600k / 1M / 1.4M total) and measure QPS & P50/P95/P99 | P99 fan-out overhead grows sub-linearly with peer count; zero gRPC stream leaks |

### Suite 4: Fault Injection, Partial-Failure Tolerance & Failover

| Test ID | Fault Injected | How to Inject (GCE / GKE) | Expected Behavior & Pass Criteria |
| :--- | :--- | :--- | :--- |
| **FI-1** | Hard node crash mid-search | `gcloud compute instances stop longbow-2 --discard-local-ssd=false` or `kill -9` on `longbow-2` during active 8-worker `GlobalGraphRAG` load | Coordinator tolerates partial failure, increments `longbow_global_search_partial_failures`, trips peer circuit breaker cleanly, and continues returning healthy results from `longbow-0` & `longbow-1` with **0% client 5xx/panic rate** |
| **FI-2** | High inter-node latency & packet loss | `sudo tc qdisc add dev ens4 root netem delay 50ms 15ms loss 5%` on `longbow-2` | Hedged peer queries bound P99 latency; no goroutine leak (`goroutine` pprof returns to baseline after `tc qdisc del`) |
| **FI-3** | Node recovery & `DoExchange` delta sync | Restart `longbow-2` after 60s outage while writes continue on `longbow-0`/`longbow-1` | `longbow-2` rejoins mesh, catches up via `DoExchange` Merkle delta sync (`longbow_mesh_sync_deltas_total` increments), and resumes serving GlobalGraphRAG traffic |
| **FI-4** | Multi-region failover (optional) | Invoke `CreateCloudCluster`, `AddReplicaRegion`, `ReplicateClusterMutation`, then `PromoteClusterRegion` when primary region is isolated | Replica promotes cleanly; `GetCloudSLAReport` reports compliant RPO/RTO |

### Suite 5: Elastic Sharding & Interleaved A/B Performance Validation

1. **Elastic Shard Split/Merge Under Load**:
   - Enable `LONGBOW_AUTO_SHARDING_ENABLED=true` on 128 GiB nodes (remember: `helm/longbow/values.yaml` notes shard migration temporarily doubles memory while the new shard builds).
   - Trigger `TriggerShardSplit`, `TriggerShardMerge`, and `TriggerRebalance`; verify `GetClusterTopology` reflects balanced shard ownership and zero OOMs or lost vectors.
2. **Interleaved A/B Gating (`scripts/ab_benchmark.py`)**:
   - Per [docs/testplan.md](testplan.md) §5.1–5.3, do not rely on single-run comparisons against static baselines to judge small deltas.
   - First measure the cluster noise floor (same binary as both arms, `--reps 3`), then run interleaved A/B (`--reps 6 --threshold 10`, unpinned, `--workers 8`) when evaluating cluster/GlobalGraphRAG code changes.

---

## 6. Observability, Metrics & Profiling

Scrape `:9090/metrics` on every node at 5s intervals from a Prometheus instance on `longbow-driver` (or GKE `ServiceMonitor` via `metrics.serviceMonitor.enabled=true`).

### Key Metrics to Track

| Metric | Layer | Healthy Expectation |
| :--- | :--- | :--- |
| `longbow_gossip_active_members` | Mesh | Equals `N` on every node after convergence |
| `longbow_gossip_pings_total` | Mesh | Monotonically increasing across all peers |
| `longbow_mesh_sync_deltas_total` | Sync | Increments on peer catch-up / delta replication |
| `longbow_global_search_fanout_size` | GlobalSearch | `N - 1` remote peers per global query |
| `longbow_global_search_partial_failures` | GlobalSearch | `0` in steady state; increments cleanly during Suite 4 faults |
| `longbow_global_search_duration` | GlobalSearch | Track P50/P95/P99 against local `GraphRAG` baseline |
| `longbow_graph_rag_operations_total` | GraphRAG | Matches issued query count across cluster |
| `longbow_graphrag_expansion_duration_seconds` | GraphRAG | Bounded multi-hop expansion latency |
| `longbow_graphrag_expansion_errors_total` | GraphRAG | `0` in steady state |
| `longbow_graph_communities_total` | Graph Analytics | Stable after `detect-communities` |

### Continuous `pprof` Capture

Capture all 7 profiles (`profile`, `heap`, `allocs`, `goroutine`, `threadcreate`, `block`, `mutex`) from each node's `:9090/debug/pprof/` during peak GlobalGraphRAG load and before teardown. Archive summary findings only; never commit `.pprof` dumps to git ([docs/testplan.md](testplan.md) §6).

---

## 7. Acceptance Criteria & Go/No-Go Gates

| Category | Gate | Threshold |
| :--- | :--- | :--- |
| **Mesh Convergence** | `CL-1` / `CL-2` / `CL-3` | ≤ 2s cold join (3 nodes), ≤ 3s scale-out, clean dead-node detection within `suspicionTimeout + 2s` |
| **Correctness & Recall** | `GG-1` / `GG-2` / `GG-3` | GlobalGraphRAG recall@10 **≥ 0.95** vs single-node ground truth; **0** duplicate IDs; **0** expansion errors |
| **Fault Resilience** | `FI-1` / `FI-2` / `FI-3` | **0** server panics/SIGSEGVs, **0%** unhandled client errors during single-node kill, full rejoin + delta sync |
| **Memory & Stability** | All suites | Peak RSS stays below configured ceiling (`75 GiB` on 128 GiB nodes); `use_disk=yes` bounds RSS cleanly; goroutine count returns to baseline after load |
| **Performance Regression** | Interleaved A/B (`--reps 6`) | No regression > 10% in GlobalGraphRAG QPS, P99 latency, or ingest/index time vs baseline binary |

---

## 8. Cost Control & Teardown Checklist

Always tear down cloud resources immediately after archiving summary JSON results from `longbow-driver`:

```bash
# Option A (GKE)
gcloud container clusters delete "${CLUSTER_NAME}" \
  --zone="${GCP_ZONE}" --project="${GCP_PROJECT}" --quiet

# Option B (GCE VMs + firewall rule)
gcloud compute instances delete longbow-0 longbow-1 longbow-2 longbow-driver \
  --zone="${GCP_ZONE}" --project="${GCP_PROJECT}" --quiet
gcloud compute firewall-rules delete longbow-cluster-internal \
  --project="${GCP_PROJECT}" --quiet

# Clean temporary GCS test artifacts (if snapshot bucket was used)
# gcloud storage rm -r "gs://<your-bucket>/global-test-runs/*"
```

Archive only consolidated summary tables and markdown updates (`docs/performance.md`, `docs/globaltestplan.md`) in git; delete raw `.pprof`, `.bin`, and temporary `data/perf_logs/` files per [docs/testplan.md](testplan.md) §6.
