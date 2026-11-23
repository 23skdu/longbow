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
[`scripts/configs/multiagent_soak.json`](../../scripts/configs/multiagent_soak.json). CLI flags
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
python3 scripts/agent_soak_scenario.py --config scripts/configs/multiagent_soak.json
./bin/agentsoak -config scripts/configs/multiagent_soak.json
```

Customize the layout with flags, for example `--agents 5 --private-prefix own_
--shared-prefix team_ --common global` (Go uses single-dash equivalents). The
permission tiers stay fixed; only the names and agent count change. See
[`scripts/configs/README.md`](../../scripts/configs/README.md) for the full schema.

There is no unauthenticated smoke mode. Both simulators always require the
bootstrap admin key and abort if the server does not enforce the permission
boundaries. Use `scripts/verify_rbac.py` for a focused RBAC-only check without
running the soak workload.

The shared workload schema in `scripts/configs/multiagent_soak.json` is used by both
simulators. Weight keys are `dense`, `hybrid`, `sparse`, `filtered`, `range`,
`byid`, `temporal_*`, `geo_*`, `graphrag`, `traverse`, `recommend`,
`multimodal`, `federated`, `text`, `insert`, `upsert`, `delete`, `delete_ns`,
`pagerank`, `communities`, `get_vector`, and `graph_stats`. Weights are
relative; `0` disables an operation. `-workload dense=25,federated=2` (Go) or
`--workload ...` (Python) overrides the file. Both simulators emit `by_op` and
`by_op_stats` with per-operation op counts, errors, QPS, p50/p95/p99, plus a
`stalls` array. RBAC probe denials are reported separately in `audit_denials`
and never inflate the client-visible error rate.

## 10. Bounded Variants and CI Gates

`make soak-quick` runs the five release-required variants through
`scripts/run_bounded_soak.sh`. That wrapper starts a disposable RBAC-enforced
server, samples server RSS into JSONL, runs the Go simulator, and validates the
report with `scripts/check_soak_report.py`.

| Variant | Agents | Layout | Duration | Focus |
| --- | --- | --- | --- | --- |
| `shared-8-agents` | 8 | 2 agents per shared namespace (4 groups + common) | 10 min | dataset-lock convoy |
| `permission-default` | 3 | default `2N+1` = 7 namespaces | 10 min | scoped-key boundaries |
| `permission-scaled` | 10 | default `2N+1` = 21 namespaces | 10 min | ACL scale |
| `federated` | 2 | federated-only workload | 5 min | router membership churn |
| `analytics` | 2 | writes + PageRank/communities/GraphRAG/traverse | 10 min | analytics/search isolation |

The grouped layout is selected with `-agents-per-namespace 2` (Go) or
`--agents-per-ns 2` (Python). Agents in the same group share write access to
the group namespace; other groups remain read-only, so RBAC remains enforced.

Failure criteria for every variant:

- error rate above 1%, including any operation-specific error rate;
- a QPS drop greater than 50% sustained for two minutes;
- monotonic RSS growth over the second half of the run;
- zero cross-agent verification hits;
- any `stalls` record or simulator-reported `failures` entry.

Go's `-federated-only` mode issues a real `FederatedSearch` action against
collections registered during provisioning, using a reader key scoped to the
agent's readable namespaces. The permission assertion calls
`GetAuditDenialCounts` with the admin key and requires the startup denial
probes to be queryable.

In `.github/workflows/ci.yml`, the `soak-quick` matrix is a required dependency
of both release jobs. Reports, RSS samples, and server logs are uploaded as
artifacts named `soak-<variant>`.

## 11. Secure Variants

Secure variants are dispatch-only (`workflow_dispatch` with `secure_soak=true`
or `oidc_soak=true`) and can also be run locally with `make soak-secure` and
`make soak-oidc`. They are not release-required because they need TLS test
certificates or an external workload-identity issuer.

| Variant | Server settings | Client settings |
| --- | --- | --- |
| `tls` | `LONGBOW_TLS_ENABLED=true`, server cert/key | `-tls-enabled` with trusted CA |
| `mtls` | plus `LONGBOW_TLS_REQUIRE_CLIENT_CERT=true` | plus client cert/key |
| `encrypted` | `LONGBOW_ENCRYPTION_AT_REST=true`, local KMS key | default scoped keys |
| `combined` | mTLS + encryption | mTLS client settings |
| `oidc` | `LONGBOW_OIDC_ENABLED=true`, configured issuer | `-oidc-mode aws` with `AWS_WEB_IDENTITY_TOKEN_FILE` |

`scripts/generate_test_certs.sh` creates one-day CA/server/client certificates
for local and CI use; never use them in production. The master key used by
local soak variants is a fixed test value and is never logged. OIDC variants
require `SOAK_OIDC_ISSUERS`, `SOAK_OIDC_AUDIENCE`, and
`SOAK_OIDC_TOKEN_FILE_ENV`; they exchange a real workload-identity JWT and do
not grant admin privileges. See
[`development/performance.md`](performance.md) for the plaintext-versus-secure
comparison template.
