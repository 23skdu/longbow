# Deep Analysis: Clustering, PageRank and Global GraphRAG

**Scope:** community detection (label propagation), PageRank centrality, knowledge-graph API
dispatch, and the Global GraphRAG scatter-gather coordinator
(`GlobalSearchCoordinator`).

**Files audited:** `internal/store/graph_analytics.go`, `internal/store/graph_api.go`,
`internal/store/graph_store.go`, `internal/store/global_search.go`,
`internal/store/result_merger.go`, `internal/store/store_actions.go`,
`internal/store/store_meta_actions.go`, `internal/store/rbac_actions.go`,
`cmd/cli/graph_cmd.go`, `internal/store/graph_api_test.go`,
`internal/store/graph_store_test.go`, `internal/store/graph_analytics_test.go`.

---

## 1. What was broken (and fixed)

### 1.1 CLI action names did not match server dispatch
`cmd/cli/graph_cmd.go` sent PascalCase action types (`AddEdge`, `TraverseGraph`,
`CalculatePageRank`, `DetectCommunities`). The server dispatcher
(`store_actions.go`, `store_meta_actions.go`) and the RBAC allowlist
(`rbac_actions.go`) only know kebab-case (`add-edge`, `traverse-graph`,
`calculate-pagerank`, `detect-communities`). Every graph CLI command therefore
failed with `codes.Unimplemented`. The Python SDK already used kebab-case via
the meta client, which is why Python users did not see it.

Fix: renamed CLI action literals to kebab-case and added the missing flags
(`-incoming`/`-weighted` for traverse, `-damping` for pagerank,
`-iterations`→`max_iterations` for detect-communities). Action builders were
extracted into testable helpers.

### 1.2 Graph actions were missing from `DoMetaAction`
The Python SDK sends **all** graph operations through the meta client
(`_require_meta_client`), but `DoMetaAction` had no cases for any of the five
graph actions, so SDK users hit `codes.Unimplemented`.

Fix: added `add-edge`, `traverse-graph`, `calculate-pagerank`,
`detect-communities` (and `GetGraphStats`) cases to `DoMetaAction`.

### 1.3 `add-edge` was not in the RBAC allowlist
`permissionAndDatasetForAction` has no case for `add-edge`, so it fell through
to `default: PermAdmin`, locking ordinary API keys out of graph mutation.

Fix: added `"add-edge"` to the `PermWrite` list.

### 1.4 `MergeSortedStreams` was sorted ascending
`ResultHeap.Less` used `<` (min-heap by score). The house convention is
higher score = better, best-first (see `index/navigation_search.go`
`Score = 1/(1+Dist)`, the `do_get_search_verify_test.go` assertion
`results[0].ID == 10`, and every other merge/fusion/rerank path in the
codebase sorts `Score > Score`). `MergeSortedStreams` is the function
`GlobalSearchCoordinator` uses to combine peer streams, so it silently returned
the *worst* k results and was the underlying cause of the global-GraphRAG
ordering bug.

Fix: flipped `Less` to `>` (max-heap by score) and updated
`result_merger_test.go` to assert best-first. Added
`FuzzMergeSortedStreams` against a brute-force reference.

### 1.5 Hedge winner-dropping race
In all three scatter-gather wait loops (`GlobalSearch`, `GlobalGeoSearch`,
`GlobalTemporalSearch`) the `finishedAll` and `ctxHedge.Done()` cases returned
without draining the buffered winner. The winner writes to `resultHedge`
(buffered, cap 1) *before* calling `cancelHedge()`, so by the time those
cases fire the value is already buffered; Go's `select` picks pseudo-randomly
among ready cases, so a group's results were intermittently discarded.

Fix: non-blocking drain of `resultHedge` in both cases before returning.
Extracted the loop into a testable `awaitHedgeGroup` helper, pinned by
`global_search_hedge_test.go` (200 iterations of the simultaneous-ready
interleaving).

### 1.6 Clustering metrics declared but never emitted
`metrics.GraphClusteringDurationSeconds` and `metrics.GraphCommunitiesTotal`
are documented in `docs/metrics.md` and `docs/globaltestplan.md` but
`handleDetectCommunities` never observed them.

Fix: wired both into `handleDetectCommunities` (duration histogram + count
gauge). Pinned by `TestGraphAPI_DetectCommunities_EmitsMetrics`.

### 1.7 `DetectCommunities` produced a phantom community 0
Nodes whose outgoing-edge targets are all excluded from the label set (leaves)
were assigned `maxLabel == 0` even though no node carries label 0. Fixed by
skipping the update when no neighbour has a label.

### 1.8 `CommunityCount()` was misnamed and misused
`GraphStore.CommunityCount()` returned `forwardEdges.Len()` (the number of
nodes with outgoing edges) and was reported to clients as
`community_count` in `handleGetGraphStats`. It never measured communities — real
community detection lives in `GraphAnalytics.DetectCommunities`. Renamed to
`NodeCount()` and changed `handleGetGraphStats` to report `node_count` and
`community_count` is no longer fabricated. (The `community_count` field is
dropped from the stats response; clients should run `detect-communities` for
real counts.)

### 1.9 `CalculatePageRank` leaked mass on target-only nodes
`validNodes` only included nodes with outgoing edges, so any node that was
referenced by an edge but had no outgoing edges of its own was excluded from
the node count and its incoming rank mass was silently lost. Fixed by also
marking nodes with incoming edges as valid. Scores now sum to 1.0 on graphs
with dangling sinks.

---

## 2. Test coverage added

### 2.1 Accuracy unit tests (`graph_analytics_test.go`)
- LPA: disjoint cliques (2 communities), path, star, complete graph, empty graph,
  determinism (run-twice), max-iterations bound, out-degree-less leaf targets.
- PageRank: mass conservation (sums to 1.0 on a dangling chain), flow ordering,
  zero-iterations uniformity, nil graph errors.
- Fuzz: `FuzzGraphAnalytics_DetectCommunities` (no panics, count == unique labels,
  structural invariants, determinism) and extended `FuzzGraphAnalytics_PageRank`
  (finiteness, positivity, sum ≈ 1, determinism).

### 2.2 Accuracy tests for merge / RRF
`result_merger_test.go` rewritten for the best-first contract, plus
`FuzzMergeSortedStreams` against a brute-force reference (arbitrary stream
counts, sizes, k).

### 2.3 Hollow stubs rewritten (`graph_store_test.go`)
The `TestLouvainClustering_*` stubs (assertions commented out) were renamed to
`TestCommunityDetection_*` and given real assertions. (Longbow implements
label propagation, not Louvain/Leiden — no Louvain implementation exists
anywhere in the tree; the old names were misleading.) `TestGraphStore_TraverseParallel`
was likewise replaced with a real concurrent-traversal test.

### 2.4 Dispatch / wiring tests (`graph_wiring_test.go`, `cmd/cli/graph_cmd_test.go`)
- Data and meta planes dispatch every canonical graph action to its handler
  (not `Unimplemented`).
- PascalCase action types are rejected as `Unimplemented` (regression guard
  against re-introducing the bug).
- RBAC maps `add-edge` → `PermWrite`, the rest → `PermRead`.
- CLI builders emit the exact keys the server parses, and action names are
  pinned to kebab-case.
- `TestGraphAPI_DetectCommunities_EmitsMetrics` pins the two clustering
  metrics.

### 2.5 Network tests (`global_search_network_test.go`)
Real gRPC/Arrow Flight scatter-gather over two live peer servers:
- full merge with no dropped peer results (set equality + best-first ordering),
- top-k truncation to the global best,
- no-peers returns local untouched,
- 25 repeats for race coverage,
- hedged replicas in one shard group (winner must never be dropped),
- total peer failure still returns the local stream.

### 2.6 Hedge wait-loop unit tests (`global_search_hedge_test.go`)
Deterministic 200-iteration tests of the simultaneous-ready interleaving that
the network tests cannot reliably trigger, plus failure/cancellation/drain
helper coverage.

---

## 3. Findings reported but NOT fixed (out of scope)

1. **Sharded HNSW ascending sorts** (`sharded_hnsw_search.go` L169/244/338)
   sort `Dist` ascending and use distance-as-score, which likely returns the
   *worst* k results — the same class of bug as the merger, but in a different
   component. Not touched; flagged.
2. **`handleDetectCommunities` has no sharded fallback**: unlike
   `handleCalculatePageRank`, it only handles `interface{ GetData() }`, so a
   sharded index returns "not supported". Flagged.
3. **LPA has no shuffle** (`graph_analytics.go` L361-362): deterministic
   iteration order means the result is order-dependent; standard LPA shuffles
   each pass. Left as-is because it gives deterministic results (which the
   tests rely on), but it is a known accuracy limitation.
4. **`graphrag_spreading_test.go` tests test-local helpers only** and never
   exercises the real `RankWithGraphDistributed` path. Flagged.
5. **`docs/api.md`** referenced a non-existent `scripts/ops_test.py` in
   many places; the table/field-name corrections were made for the graph
   actions but the script reference itself is a broader docs issue.
6. **`geoResultHeap` comment was wrong** (min-heap, not max-heap) — fixed
   comment only; logic was correct.

---

## 4. Verification

- `go build ./...` — clean.
- `go vet ./...` — clean.
- `go test -short ./...` — all packages pass.
- `go test ./internal/store/` (non-short, incl. new network tests) — pass.
- `go test -race -short ./internal/store/` — no data races.
- `make test-fuzz` — all packages have a `Fuzz*` target and pass.
- `scripts/check_fuzz_coverage.sh` — 74/74 packages covered.
- `go test -run TestAwaitHedgeGroup ...` — 200-iteration race tests pass and
  fail immediately if the drain is reverted.
- Sabotage check: reverting `MergeSortedStreams` to ascending breaks
  `FuzzMergeSortedStreams` and the network ordering assertions.
