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
