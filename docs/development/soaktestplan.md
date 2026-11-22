# Longbow 24-Hour Agent-Memory Soak Test Plan

## 1. Objective

Validate that Longbow can sustain simultaneous read/write/delete workloads from
many autonomous AI agents for 24 hours without degradation, crash, data loss, or
memory leak. The test simulates realistic agent memory access patterns and
exercises every search modality Longbow supports.

## 2. Agent Memory Access Patterns (researched)

Autonomous agents (LLM tool-use loops, RAG pipelines, multi-agent systems)
exhibit the following memory access characteristics, which this soak test
reproduces:

1. **High read/write ratio (≈ 3-5:1)** — agents read context far more often
   than they write new memories. Reads are bursty (tool-call storms).
2. **Small working sets per agent** — each agent owns a small namespace
   (100-5000 memories) but the cluster holds many agents' data.
3. **Frequent updates / supersession** — agents "learn" new facts and replace
   old ones (same ID re-inserted → tombstone + new record).
4. **Selective forgetting** — GDPR/privacy deletes, expired sessions, and
   erroneous facts removed via `delete`.
5. **Temporal bias** — recent memories queried far more than old ones
   (sliding-window / as-of / range).
6. **Hybrid queries** — semantic + keyword (project names, entity IDs).
7. **Geo-aware retrieval** — for embodied/AR agents, coordinate proximity.
8. **GraphRAG traversal** — knowledge-graph edges between entities.
9. **Session isolation** — per-agent namespaces, bulk cleanup on session end.
10. **Mixed embedding dims / dtypes** — different agents use different models.
11. **Long tail of cold data** — compaction must reclaim tombstones online.
12. **Concurrency** — tens to hundreds of agents, each with its own client.

## 3. Workload Mix (per agent, per second)

| Op | Weight | Description |
|---|---|---|
| dense search | 25% | `search(vector=..., k=5..20)` |
| hybrid search | 10% | `search(..., text_query=..., alpha=0.5)` |
| sparse search | 5% | text-only BM25 |
| filtered search | 5% | numeric/bool/string predicates |
| range search | 3% | distance threshold |
| by-id search | 3% | neighbor expansion |
| temporal (as_of/range/window) | 8% | recent-memory retrieval |
| geo (radius/box/hybrid) | 5% | proximity retrieval |
| graphrag / traverse / recommend | 5% | knowledge-graph hops |
| multimodal search | 2% | cross-modal |
| federated search | 2% | cross-collection |
| text (advanced) | 3% | boolean/phrase/fuzzy |
| insert (new memory) | 12% | `insert` |
| upsert (supersede) | 5% | re-insert same id |
| delete (forget) | 3% | `delete` |
| delete-namespace (session end) | 0.5% | bulk cleanup |
| pagerank / communities | 0.5% | analytics, infrequent |
| get_vector / graph_stats | 1% | lookups |

All 21 search modalities from the test plan are exercised.

## 4. Soak Configuration

- **Duration:** 24 hours (configurable via `--duration-hours`).
- **Agents:** 50 concurrent by default (configurable).
- **Namespaces:** one per agent (`agent_<i>`), dims 128, dtype float32.
- **Memory ceiling:** 50% of host RAM, auto-spill disk enabled.
- **Seed:** 42 for reproducibility.
- **Reporting:** every 60s: QPS, error rate, p50/p95/p99 latency, memory RSS,
  index size, tombstone count, compaction count. Written to
  `data/soak_logs/soak_<timestamp>.json` and a rolling CSV.
- **Failure criteria:** any panic, OOM, error rate > 1%, p99 latency > 10x
  baseline, or process crash.

## 5. Success Criteria

- 24h run completes with 0 crashes.
- Error rate < 1% throughout.
- No monotonic memory growth beyond compaction reclamation.
- All 21 search modes return non-zero results.
- Tombstone count stays bounded (compaction reclaims).

## 6. Script

`scripts/agent_soak.py` — drives N concurrent agents against a running
Longbow server using the Python SDK. Each agent runs its own read/write loop
with the workload mix above. Metrics are sampled and persisted every minute.

## 7. Findings (2026-11-21 Soak Investigation)

- Shared namespaces (multiple agents per namespace) stalled after ~60s with 0
  errors and a responsive server. Root cause: lock convoy on dataset-level
  mutexes during concurrent insert/upsert/delete/search, plus `WaitForIndexing`
  blocking indefinitely under sustained load.
- Fixes applied:
  - Bounded `WaitForIndexing` (200ms) so searches proceed under load.
  - Range and by-ID search release `dataMu` before the expensive index call.
  - Auto-scaler hysteresis: higher minimum workers and a busy-signal guard
    (`pendingOverflowJobs`) to avoid draining under load.
- Remaining risk: heavy graph analytics (PageRank, communities, GraphRAG
  expansion) on shared namespaces can still dominate lock hold time; these
  should run on dedicated analytics workers or snapshots.

## 8. Recommendations for Larger Global-GraphRAG Agent Soak Tests

- Use **separate namespaces per agent** for write-heavy paths; use a shared
  "global graph" namespace read-only for GraphRAG expansion.
- Run GraphRAG/PageRank/community detection on a **snapshot or replica** to
  avoid blocking ingest/search locks.
- Add **cross-agent verification**: agent B searches memories written by
  agent A within the shared global namespace.
- Scale to **50+ agents** with **2-4 agents per namespace** only after the
  shared-namespace lock convoy is fully resolved (see roadmap).
- Track **per-op QPS** and **stall detection** (>50% QPS drop for >2 min) as
  failure criteria.
- Include **multi-modal (text+geo+graph)** memories in the global graph to
  exercise cross-modal GraphRAG.

## 9. Multi-Namespace Permission Scenario (3 Agents / 7 Namespaces)

Two equivalent simulators drive this scenario: `scripts/agent_soak_scenario.py`
(Python SDK) and `cmd/agentsoak` (native Go client). Both default to **3 agents
and 7 namespaces** (`2N + 1`), and both load the same
[`configs/multiagent_soak.json`](../../configs/multiagent_soak.json). CLI flags
override any value in the file.

| Namespace | Owner | Owner access | Other agents |
| --- | --- | --- | --- |
| `private_0`, `private_1`, `private_2` | agent `i` | read / write / delete | no access (not even visible) |
| `shared_0`, `shared_1`, `shared_2` | agent `i` | read / write / delete | read / search only |
| `master` | all | read / write / delete | read / write / delete |

Permissions are **enforced by the server's RBAC manager**, not simulated: the
admin client provisions the namespaces and issues two scoped API keys per
agent (`writer` for the agent's private + owned-shared + common namespaces,
`reader` for the other agents' shared namespaces). The simulators probe a
forbidden cross-agent read and write at startup and abort if RBAC is not
actually enforced.

Start the server with a known bootstrap admin key:

```bash
LONGBOW_RBAC_BOOTSTRAP_KEY="local-soak-admin-key-0123456789" \
./bin/longbow
```

Fixed namespaces can be declared with `LONGBOW_STARTUP_NAMESPACE_POLICIES`
before listeners start; durable ACLs survive restarts and `ephemeral: true`
namespaces do not. The simulators still issue their own scoped keys because
namespace ACL key IDs must reference existing API keys. See
[`docs/rbac.md`](../rbac.md) for the JSON schema.

Run either simulator (the admin key is read from `LONGBOW_RBAC_BOOTSTRAP_KEY`,
or can be passed with `--admin-api-key` / `-admin-api-key`):

```bash
python3 scripts/agent_soak_scenario.py --config configs/multiagent_soak.json
./bin/agentsoak -config configs/multiagent_soak.json
```

Customize the layout with flags, for example `--agents 5 --private-prefix own_
--shared-prefix team_ --common global` (Go uses single-dash equivalents). The
permission tiers stay fixed; only the names and agent count change. See
[`configs/README.md`](../../configs/README.md) for the full schema.

There is no unauthenticated smoke mode. Both simulators always require the
bootstrap admin key and abort if the server does not enforce the permission
boundaries. Use `scripts/verify_rbac.py` for a focused RBAC-only check without
running the soak workload.
