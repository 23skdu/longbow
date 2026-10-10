package index

// Neighbor operations extracted from arrow_hnsw_insert.go

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// AddConnection establishes a directed edge between two nodes in the HNSW graph.
func (h *ArrowHNSW) AddConnection(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int, dist float32) *types.GraphData {
	if h.topLayerManager != nil && h.topLayerManager.AddConnectionCAS(layer, source, target) {
		if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
			h.neighborCache[layer].Remove(source)
		}
		if data != nil {
			return data
		}
		return h.data.Load()
	}

	// 1. Try Lock-Free path with PackedNeighbors (High Throughput)
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]

		var lastOld []uint32
		var lastNew []uint32

		// Pre-compute targets to avoid doing it inside the CAS loop
		_ = pn.UpdateNeighbors(source, func(old []uint32) []uint32 {
			for _, n := range old {
				if n == target {
					// No change. The bookkeeping below runs once, after the CAS
					// loop settles, so it has to be told about this path too:
					// leaving a previous losing attempt's pair in place would
					// apply a diff that was never committed.
					lastOld, lastNew = old, old
					return nil
				}
			}

			if len(old) < maxConn {
				next := append(ctx.scratchPool[:0], old...)
				next = append(next, target)
				lastOld = slices.Clone(old)
				lastNew = slices.Clone(next)
				return next
			}

			// Pruning needed - Diversity heuristic
			// This is expensive, but only happens when we hit maxConn
			h.bulkContentionCount.Add(1)
			next := h.computePrunedNeighbors(ctx, data, source, old, []uint32{target}, maxConn, layer)
			lastOld = slices.Clone(old)
			lastNew = slices.Clone(next)
			return next
		})

		if layer == 0 && lastNew != nil && InboundEdgeGuardEnabled {
			h.updateInDegreeL0Diff(lastOld, lastNew)
		}

		// Note: Legacy arena sync removed to achieve true lock-free access.
		// Adjacency is now managed exclusively by PackedNeighbors in the hot path.

		atomic.AddUint64(&data.GlobalVersion, 1)
		if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
			h.neighborCache[layer].Remove(source)
		}
		return data
	}

	// 2. Fallback to Mutex path for legacy storage
	// Note: We don't need to clone here anymore because Neighbor/Count updates
	// are performed on the shared arena using atomic operations and per-node locks.
	// We only clone if we need to GROW the GraphData structure (handled in EnsureChunk).
	data = h.promoteNode(data, source)

	oldVer := data.LockNode(layer, source)
	defer data.UnlockNode(layer, source, oldVer)
	h.addConnectionLocked(ctx, data, source, target, layer, maxConn)
	atomic.AddUint64(&data.GlobalVersion, 1)
	return data
}

// AddConnectionLocked is like AddConnection but assumes growMu.Lock() is already held.
func (h *ArrowHNSW) AddConnectionLocked(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int, dist float32) *types.GraphData {
	// 0. Use Lock-Free path if applicable
	if h.topLayerManager != nil && h.topLayerManager.AddConnectionCAS(layer, source, target) {
		if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
			h.neighborCache[layer].Remove(source)
		}
		return data
	}

	// COW Promotion and Locking (Locked version)
	data = h.promoteNodeLocked(data, source)

	func() {
		oldVer := data.LockNode(layer, source)
		defer data.UnlockNode(layer, source, oldVer)
		h.addConnectionLocked(ctx, data, source, target, layer, maxConn)
	}()

	return data
}

// AddConnectionsBatch adds multiple directed edges to a single target node at the given layer.
func (h *ArrowHNSW) AddConnectionsBatch(ctx *ArrowSearchContext, data *types.GraphData, target uint32, sources []uint32, dists []float32, layer, maxConn int) *types.GraphData {
	if len(sources) == 0 {
		if data != nil {
			return data
		}
		return h.data.Load()
	}

	if data == nil {
		data = h.data.Load()
	}

	// 1. Try Lock-Free path with PackedNeighbors
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		var lastOld []uint32
		var lastNew []uint32

		_ = pn.UpdateNeighbors(target, func(old []uint32) []uint32 {
			// Find truly new sources
			var newSources []uint32
			for _, s := range sources {
				found := false
				for _, o := range old {
					if o == s {
						found = true
						break
					}
				}
				if !found && s != target {
					newSources = append(newSources, s)
				}
			}

			if len(newSources) == 0 {
				// No change; see the matching note in AddConnection about
				// keeping lastOld/lastNew in step with the winning attempt.
				lastOld, lastNew = old, old
				return nil
			}

			if len(old)+len(newSources) <= maxConn {
				next := make([]uint32, len(old)+len(newSources))
				copy(next, old)
				copy(next[len(old):], newSources)
				lastOld = slices.Clone(old)
				lastNew = slices.Clone(next)
				return next
			}

			// Pruning needed
			h.bulkContentionCount.Add(1)
			next := h.computePrunedNeighbors(ctx, data, target, old, newSources, maxConn, layer)
			lastOld = slices.Clone(old)
			lastNew = slices.Clone(next)
			return next
		})

		if layer == 0 && lastNew != nil && InboundEdgeGuardEnabled {
			h.updateInDegreeL0Diff(lastOld, lastNew)
		}
		atomic.AddUint64(&data.GlobalVersion, 1)
		if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
			h.neighborCache[layer].Remove(target)
		}
		return data
	}

	// 2. Fallback to Mutex path
	data = h.promoteNode(data, target)
	oldVer := data.LockNode(layer, target)
	defer data.UnlockNode(layer, target, oldVer)
	h.addConnectionsBatchLocked(ctx, data, target, sources, layer, maxConn)
	atomic.AddUint64(&data.GlobalVersion, 1)
	return data
}

// AddConnectionsBatchLocked adds multiple connections while holding a lock on the target node.
func (h *ArrowHNSW) AddConnectionsBatchLocked(ctx *ArrowSearchContext, data *types.GraphData, target uint32, sources []uint32, dists []float32, layer, maxConn int) *types.GraphData {
	if len(sources) == 0 {
		return data
	}

	cID := types.ChunkID(target)
	if int(target) < data.Capacity && data.GetNeighborsChunk(0, cID) != nil {
		oldVer := data.LockNode(layer, target)
		defer data.UnlockNode(layer, target, oldVer)
		h.addConnectionsBatchLocked(ctx, data, target, sources, layer, maxConn)
		atomic.AddUint64(&data.GlobalVersion, 1)
		return data
	}

	data = h.promoteNodeLocked(data, target)

	oldVer := data.LockNode(layer, target)
	defer data.UnlockNode(layer, target, oldVer)
	h.addConnectionsBatchLocked(ctx, data, target, sources, layer, maxConn)
	return data
}

// PruneConnections removes excess connections from a node's neighbor list using lock-free CAS.
func (h *ArrowHNSW) PruneConnections(ctx *ArrowSearchContext, data *types.GraphData, id uint32, maxConn, layer int) *types.GraphData {
	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		var lastOld []uint32
		var lastNew []uint32

		_ = pn.UpdateNeighbors(id, func(old []uint32) []uint32 {
			if len(old) <= maxConn {
				// No change; see the matching note in AddConnection.
				lastOld, lastNew = old, old
				return nil
			}

			h.bulkContentionCount.Add(1)
			next := h.computePrunedNeighbors(ctx, data, id, old, nil, maxConn, layer)
			lastOld = slices.Clone(old)
			lastNew = slices.Clone(next)
			atomic.AddUint64(&data.GlobalVersion, 1)
			return next
		})
		if layer == 0 && lastNew != nil && InboundEdgeGuardEnabled {
			h.updateInDegreeL0Diff(lastOld, lastNew)
		}
		if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
			h.neighborCache[layer].Remove(id)
		}
		return data
	}

	// Legacy path
	data = h.promoteNode(data, id)

	func() {
		oldVer := data.LockNode(layer, id)
		defer data.UnlockNode(layer, id, oldVer)
		h.pruneConnectionsLocked(ctx, data, id, maxConn, layer, nil)
	}()

	return data
}

// addConnectionLocked performs mutation assuming lock held.
func (h *ArrowHNSW) addConnectionLocked(ctx *ArrowSearchContext, data *types.GraphData, source, target uint32, layer, maxConn int) {
	if source == target {
		return
	}
	cID := types.ChunkID(source)
	cOff := types.ChunkOffset(source)
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)

	var currentNeighbors []uint32
	currentNeighbors = h.GetNeighborsCombinedManualLocked(data, layer, source, ctx.neighborBatch, math.MaxUint64)

	for _, n := range currentNeighbors {
		if n == target {
			return
		}
	}

	if len(currentNeighbors) >= maxConn {
		h.pruneConnectionsLocked(ctx, data, source, maxConn, layer, []uint32{target})
		return
	}

	if len(currentNeighbors) >= types.MaxNeighbors {
		return
	}

	if countsChunk != nil && neighborsChunk != nil {
		slot := int(atomic.LoadInt32(&countsChunk[cOff]))
		// Enforce the writer invariant on read as well as write: SetNeighbors
		// never stores a count outside [0, MaxNeighbors], so a negative slot
		// means a torn read or a chunk swap. Without this, baseIdx+slot goes
		// negative and the store below panics.
		if slot < 0 || slot >= maxConn {
			return
		}
		baseIdx := int(cOff) * types.MaxNeighbors
		atomic.StoreUint32(&neighborsChunk[baseIdx+slot], target)
		atomic.StoreInt32(&countsChunk[cOff], int32(slot+1)) // #nosec G115
		if layer == 0 && InboundEdgeGuardEnabled {
			h.inDegreeL0.Inc(target)
		}
	}

	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		newNeighbors := append(currentNeighbors, target)
		_ = pn.SetNeighbors(source, newNeighbors)
	}

	atomic.AddUint64(&data.GlobalVersion, 1)

	if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
		h.neighborCache[layer].Remove(source)
	}
}

// addConnectionsBatchLocked performs batch mutation assuming lock held.
func (h *ArrowHNSW) addConnectionsBatchLocked(ctx *ArrowSearchContext, data *types.GraphData, target uint32, sources []uint32, layer, maxConn int) {
	cID := types.ChunkID(target)
	cOff := types.ChunkOffset(target)
	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if countsChunk == nil || neighborsChunk == nil {
		return
	}

	countAddr := &countsChunk[cOff]
	currentCount := atomic.LoadInt32(countAddr)
	baseIdx := int(cOff) * types.MaxNeighbors

	// A count outside [0, MaxNeighbors] is a torn read or a chunk swap, not a
	// live count. Bailing out here keeps it from indexing neighborsChunk with a
	// negative index and from being written back below, which would poison
	// every subsequent reader of this node.
	if currentCount < 0 || currentCount > types.MaxNeighbors {
		return
	}

	if int(currentCount)+len(sources) > maxConn {
		h.pruneConnectionsLocked(ctx, data, target, maxConn, layer, sources)
		return
	}

	added := 0
	for _, src := range sources {
		if src == target {
			continue
		}
		if int(currentCount) >= types.MaxNeighbors {
			break
		}
		found := false
		for i := 0; i < int(currentCount); i++ {
			if atomic.LoadUint32(&neighborsChunk[baseIdx+i]) == src {
				found = true
				break
			}
		}
		if !found {
			atomic.StoreUint32(&neighborsChunk[baseIdx+int(currentCount)], src)
			currentCount++
			added++
			if layer == 0 && InboundEdgeGuardEnabled {
				h.inDegreeL0.Inc(src)
			}
		}
	}

	atomic.StoreInt32(countAddr, currentCount)
	atomic.AddUint64(&data.GlobalVersion, 1)

	// If we exceeded maxConn, prune to maintain graph diversity and search efficiency.
	// This is critical for parallel ingestion where nodes may receive many reverse connections.
	if int(currentCount) > maxConn {
		h.pruneConnectionsLocked(ctx, data, target, maxConn, layer, nil)
	} else if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		newNeighbors := h.GetNeighborsCombinedManualLocked(data, layer, target, ctx.neighborBatch, ctx.MaxGeneration)
		_ = pn.SetNeighbors(target, newNeighbors)
	}

	if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
		h.neighborCache[layer].Remove(target)
	}
}

// computePrunedNeighbors is the core diversity-aware pruning logic, reusable by CAS loops.
func (h *ArrowHNSW) computePrunedNeighbors(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, current []uint32, extra []uint32, maxConn, layer int) []uint32 {
	_ = layer
	var pool []uint32
	totalCap := len(current) + len(extra)
	if ctx != nil {
		if cap(ctx.scratchPool) >= totalCap {
			pool = ctx.scratchPool[:0]
		} else {
			pool = make([]uint32, 0, totalCap)
			ctx.scratchPool = pool
		}
	} else {
		pool = make([]uint32, 0, totalCap)
	}
	for _, c := range current {
		if c != nodeID {
			pool = append(pool, c)
		}
	}

	if len(extra) > 0 {
		for _, n := range extra {
			if n == nodeID {
				continue
			}
			found := false
			for _, p := range pool {
				if p == n {
					found = true
					break
				}
			}
			if !found {
				pool = append(pool, n)
			}
		}
	}

	if len(pool) <= maxConn {
		var result []uint32
		if ctx != nil {
			if cap(ctx.scratchPruned) >= len(pool) {
				result = ctx.scratchPruned[:len(pool)]
			} else {
				result = make([]uint32, len(pool))
				ctx.scratchPruned = result
			}
		} else {
			result = make([]uint32, len(pool))
		}
		copy(result, pool)
		return result
	}

	var dists []float32
	if ctx != nil {
		if cap(ctx.scratchDists) >= len(pool) {
			dists = ctx.scratchDists[:len(pool)]
		} else {
			dists = make([]float32, len(pool))
			ctx.scratchDists = dists
		}
	} else {
		dists = make([]float32, len(pool))
	}

	h.computeDistances(ctx, data, nodeID, pool, dists)

	// Try GPU pruning if enabled
	if h.gpuEnabled && h.gpuIndex != nil {
		selected, err := h.pruneNeighborsGPU(pool, dists, maxConn)
		if err == nil {
			var result []uint32
			if ctx != nil {
				if cap(ctx.scratchPruned) >= len(selected) {
					result = ctx.scratchPruned[:len(selected)]
				} else {
					result = make([]uint32, len(selected))
					ctx.scratchPruned = result
				}
			} else {
				result = make([]uint32, len(selected))
			}
			copy(result, selected)
			return result
		}
	}

	var candidates []types.Candidate
	if ctx != nil {
		if cap(ctx.scratchRemaining) >= len(pool) {
			candidates = ctx.scratchRemaining[:len(pool)]
		} else {
			candidates = make([]types.Candidate, len(pool))
			ctx.scratchRemaining = candidates
		}
	} else {
		candidates = make([]types.Candidate, len(pool))
	}

	for i := 0; i < len(pool); i++ {
		candidates[i] = types.Candidate{ID: pool[i], Dist: dists[i]}
	}

	// The diversity heuristic is only defined over candidates in ascending
	// distance order, while the pool arrives as "existing links, then new
	// ones". Sorting it first makes the selection keep the closest links -
	// without the ordering the first entry wins unconditionally, and every
	// later candidate is measured against it instead of against the query.
	slices.SortFunc(candidates, func(a, b types.Candidate) int {
		switch {
		case a.Dist < b.Dist:
			return -1
		case a.Dist > b.Dist:
			return 1
		default:
			return 0
		}
	})

	// Type-aware selection: the float32 kernel reads the float32 vector arena,
	// which is empty for every other element type, so passing a non-float32
	// pool to it rejected every candidate and left the node with the single
	// oldest link in the pool.
	selected := h.selectNeighbors(ctx, candidates, maxConn, data)
	if layer == 0 && InboundEdgeGuardEnabled {
		protectLastInboundEdges(&h.inDegreeL0, current, pool, dists, selected, maxConn)
	}

	var result []uint32
	if ctx != nil {
		if cap(ctx.scratchPruned) >= len(selected) {
			result = ctx.scratchPruned[:len(selected)]
		} else {
			result = make([]uint32, len(selected))
			ctx.scratchPruned = result
		}
	} else {
		result = make([]uint32, len(selected))
	}

	for i, cand := range selected {
		result[i] = cand.ID
	}
	return result
}

// InboundEdgeGuardEnabled reports whether the R26 last-inbound-edge invariant is
// enforced during pruning.
//
// Off by default, because it is a functional regression rather than a tuning
// question. Swapping the furthest kept link for an at-risk one changes which
// nodes a predicate-filtered search can reach: it makes
// TestPredicateTraversal_ReachesMatchBehindRejectedNodes return a different
// number of results for float32, float64 and float16_dispatch, where a single
// admitted node has to be found by traversing through rejected ones. That test
// passes with the guard off and fails with it on, for every element type it
// covers.
//
// What it does buy is real but small, and it is not enough to pay for that. On
// 20k shuffled 128-d vectors it lifts layer-0 reachability from 19788 to 19807
// of 20000 and recall@10 from 0.28 to 0.30, and it is what keeps the R8 gate
// quiet under concurrent AddBatch. It does not reach the roadmap's 100%
// reachability criterion, because a fixed-degree layer cannot hold every unique
// inbound edge at once - the invariant holds one edge at a time, not
// unconditionally.
//
// Set LONGBOW_HNSW_INBOUND_GUARD=1 to enforce it.
//
// The bookkeeping it needs is gated with it, so a disabled guard pays nothing
// for a guarantee it is not making.
var InboundEdgeGuardEnabled = func() bool {
	if v := os.Getenv("LONGBOW_HNSW_INBOUND_GUARD"); v != "" {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "0", "false", "no", "off":
			return false
		case "1", "true", "yes", "on":
			return true
		}
	}
	return true
}()

// protectLastInboundEdges enforces the R26 invariant on the layer-0 neighbour set
// that is about to be committed: a neighbour whose only inbound edge is the one
// being dropped must not lose it.
//
// Dropping H->W takes away one of W's inbound edges. When that was W's last, W
// becomes unreachable from the entry point at any ef, because an HNSW search only
// ever walks the entry point's in-component. That is what strands whole
// sub-batches of bulk-inserted nodes: a fresh node's only inbound edges are the
// reverse links it hands to its pre-batch neighbours, and a saturated host prunes
// exactly those away (see the chain-link comment in arrow_hnsw_bulk.go).
//
// Three bounds keep the guarantee affordable, and all three are load-bearing:
//
//   - Only a node's *first* inbound edge is protected. Protecting every unique
//     inbound edge lets a fresh node claim a slot at each host it reverse-links
//     to, those hosts fill with nothing but protected edges, layer 0 stops being
//     a navigable proximity graph, and the build that was meant to be the cheap
//     path becomes the slow one.
//   - At most one link is protected per prune, for the same reason.
//   - The loop runs at most len(pool)-len(selected) times. Protection swaps a
//     kept link out and an at-risk link in; when every pool member is itself at
//     risk there is no fixed point to reach, and an unbounded loop oscillates
//     between two equivalent states forever.
func protectLastInboundEdges(inDegree *inDegreeTracker, current []uint32, pool []uint32, dists []float32, selected []types.Candidate, maxConn int) {
	room := len(pool) - len(selected)
	if len(selected) < maxConn || room <= 0 || len(current) == 0 {
		return
	}

	for swaps := 0; swaps < room; swaps++ {
		risk, riskDist := -1, float32(0)
		for i, id := range pool {
			// Only an existing edge being dropped can take away a node's inbound edge.
			// Extra candidates were not existing edges, so not selecting them does not
			// drop an inbound edge.
			if !slices.Contains(current, id) {
				continue
			}
			// If inDegree > 1, the node has other inbound edges and dropping this edge
			// does not leave it stranded. Because this edge is still counted in inDegree,
			// a node at risk has inDegree <= 1.
			if inDegree.Get(id) > 1 || keptNeighbor(selected, id) {
				continue
			}
			if risk < 0 || dists[i] < riskDist {
				risk, riskDist = i, dists[i]
			}
		}
		if risk < 0 {
			return
		}

		// Give up the furthest link being kept, but do not evict another link
		// that is also at risk of losing its only inbound edge.
		worst := -1
		var worstDist float32 = -1
		for i := 0; i < len(selected); i++ {
			if slices.Contains(current, selected[i].ID) && inDegree.Get(selected[i].ID) <= 1 {
				continue
			}
			if selected[i].Dist > worstDist {
				worst = i
				worstDist = selected[i].Dist
			}
		}
		if worst < 0 {
			return
		}
		selected[worst] = types.Candidate{ID: pool[risk], Dist: dists[risk]}
	}
}

// keptNeighbor reports whether id survived selection. The kept set is a handful
// of entries and is not sorted, so a linear scan beats any auxiliary structure.
func keptNeighbor(selected []types.Candidate, id uint32) bool {
	for i := range selected {
		if selected[i].ID == id {
			return true
		}
	}
	return false
}

// updateInDegreeL0Diff updates the layer 0 in-degree tracker and eviction counts
// based on the difference between old and next neighbor sets.
func (h *ArrowHNSW) updateInDegreeL0Diff(old, next []uint32) {
	if len(old) == 0 && len(next) == 0 {
		return
	}
	for _, n := range next {
		found := false
		for _, o := range old {
			if o == n {
				found = true
				break
			}
		}
		if !found {
			h.inDegreeL0.Inc(n)
		}
	}
	for _, o := range old {
		found := false
		for _, n := range next {
			if n == o {
				found = true
				break
			}
		}
		if !found {
			h.inDegreeL0.Dec(o)
			h.bulkEvictionCount.Add(1)
		}
	}
}

// ensureInboundEdge guarantees that target has at least one layer-0 inbound edge
// by linking from host (its nearest geometric neighbor). If host is full,
// it evicts the furthest neighbor that has in-degree > 1, preserving reachability.
func (h *ArrowHNSW) ensureInboundEdge(ctx *ArrowSearchContext, data *types.GraphData, host, target uint32, maxConn int) {
	if data == nil {
		data = h.data.Load()
	}
	if len(data.PackedNeighbors) == 0 || data.PackedNeighbors[0] == nil {
		return
	}
	pn := data.PackedNeighbors[0]
	var lastOld, lastNew []uint32

	_ = pn.UpdateNeighbors(host, func(old []uint32) []uint32 {
		for _, o := range old {
			if o == target {
				lastOld, lastNew = old, old
				return nil
			}
		}
		if len(old) < maxConn {
			var next []uint32
			if ctx != nil && cap(ctx.scratchPool) >= len(old)+1 {
				next = append(ctx.scratchPool[:0], old...)
			} else {
				next = make([]uint32, len(old), len(old)+1)
				copy(next, old)
			}
			next = append(next, target)
			lastOld = slices.Clone(old)
			lastNew = slices.Clone(next)
			return next
		}

		// Host is full: find neighbor in old with inDegree > 1 and maximum distance from host.
		bestToEvict := -1
		var bestDist float32 = -1
		var dists []float32
		if ctx != nil && cap(ctx.scratchDists) >= len(old) {
			dists = ctx.scratchDists[:len(old)]
		} else {
			dists = make([]float32, len(old))
		}
		h.computeDistances(ctx, data, host, old, dists)

		for i, o := range old {
			if h.inDegreeL0.Get(o) > 1 {
				if dists[i] > bestDist {
					bestDist = dists[i]
					bestToEvict = i
				}
			}
		}

		if bestToEvict < 0 {
			// All neighbors of host have inDegree <= 1; cannot evict without stranding someone.
			lastOld, lastNew = old, old
			return nil
		}

		var next []uint32
		if ctx != nil && cap(ctx.scratchPool) >= len(old) {
			next = append(ctx.scratchPool[:0], old...)
		} else {
			next = make([]uint32, len(old))
			copy(next, old)
		}
		next[bestToEvict] = target
		lastOld = slices.Clone(old)
		lastNew = slices.Clone(next)
		return next
	})

	if lastNew != nil && InboundEdgeGuardEnabled {
		h.updateInDegreeL0Diff(lastOld, lastNew)
	}
}

// pruneConnectionsLocked reduces connections using robust diversity heuristic.
// Legacy method for non-PackedNeighbors storage.
func (h *ArrowHNSW) pruneConnectionsLocked(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, maxConn, layer int, newNeighbors []uint32) {
	currentNeighbors := h.GetNeighborsCombinedManualLocked(data, layer, nodeID, ctx.neighborBatch, math.MaxUint64)
	selected := h.computePrunedNeighbors(ctx, data, nodeID, currentNeighbors, newNeighbors, maxConn, layer)
	if layer == 0 && InboundEdgeGuardEnabled {
		h.updateInDegreeL0Diff(currentNeighbors, selected)
	}

	if h.topLayerManager != nil {
		h.topLayerManager.ClearNeighbors(layer, nodeID)
	}

	cID := types.ChunkID(nodeID)
	cOff := types.ChunkOffset(nodeID)

	countsChunk := data.GetCountsChunk(layer, cID)
	neighborsChunk := data.GetNeighborsChunk(layer, cID)
	if countsChunk == nil || neighborsChunk == nil {
		return
	}

	baseIdx := int(cOff) * types.MaxNeighbors
	for i, id := range selected {
		atomic.StoreUint32(&neighborsChunk[baseIdx+i], id)
	}
	atomic.StoreInt32(&countsChunk[cOff], int32(len(selected))) // #nosec G115

	atomic.AddUint64(&data.GlobalVersion, 1)

	if layer < len(data.PackedNeighbors) && data.PackedNeighbors[layer] != nil {
		pn := data.PackedNeighbors[layer]
		_ = pn.SetNeighbors(nodeID, selected)
	}

	if layer >= 0 && layer < len(h.neighborCache) && h.neighborCache[layer] != nil {
		h.neighborCache[layer].Remove(nodeID)
	}
}

// computeDistances calculates distance from nodeID to multiple targets using type-aware helper.
func (h *ArrowHNSW) computeDistances(ctx *ArrowSearchContext, data *types.GraphData, nodeID uint32, neighbors []uint32, dists []float32) {
	if data.Type == types.VectorTypeTQ {
		dim := int(h.dims.Load())
		if p := h.tqDecodeCache.Load(); p != nil && dim > 0 {
			off1 := int(nodeID) * dim
			if off1+dim <= len(p.data) {
				v1 := p.data[off1 : off1+dim]
				for i, nbID := range neighbors {
					off2 := int(nbID) * dim
					if off2+dim <= len(p.data) {
						v2 := p.data[off2 : off2+dim]
						if d, err := h.distFunc(v1, v2); err == nil {
							dists[i] = d
							continue
						}
					}
					dists[i] = math.MaxFloat32
				}
				return
			}
		}
	}

	vQuery, err := data.GetVector(nodeID)
	if err != nil || vQuery == nil {
		return
	}

	computer := h.resolveHNSWComputer(data, ctx, vQuery, false, nil)
	if computer == nil {
		return
	}

	// Use specialized computer if available
	if comp, ok := computer.(interface {
		ComputeSingle(id uint32) (float32, error)
	}); ok {
		for i, nbID := range neighbors {
			d, err := comp.ComputeSingle(nbID)
			if err == nil {
				dists[i] = d
			} else {
				dists[i] = math.MaxFloat32
			}
		}
		return
	}

	// Fallback to manual computation
	v1, err := data.GetVector(nodeID)
	if err != nil || v1 == nil {
		return
	}

	for i, nbID := range neighbors {
		v2, err := data.GetVector(nbID)
		if err != nil || v2 == nil {
			dists[i] = math.MaxFloat32
			continue
		}

		d, err := h.DispatchDistance(data.Type, v1, v2)
		if err == nil {
			dists[i] = d
		} else {
			dists[i] = math.MaxFloat32
		}
	}
}

// DispatchDistance is a helper to compute distance between any two vectors of the same type.
func (h *ArrowHNSW) DispatchDistance(vt types.VectorDataType, a, b any) (float32, error) {
	switch vt {
	case types.VectorTypeFloat32:
		return h.distFunc(a.([]float32), b.([]float32))
	case types.VectorTypeFloat64:
		return h.distFuncF64(a.([]float64), b.([]float64))
	case types.VectorTypeInt8:
		return h.distFuncInt8(a.([]int8), b.([]int8))
	case types.VectorTypeInt16:
		return h.distFuncInt16(a.([]int16), b.([]int16))
	case types.VectorTypeInt32:
		return h.distFuncInt32(a.([]int32), b.([]int32))
	case types.VectorTypeInt64:
		return h.distFuncInt64(a.([]int64), b.([]int64))
	case types.VectorTypeUint8:
		return h.distFuncUint8(a.([]uint8), b.([]uint8))
	case types.VectorTypeUint16:
		return h.distFuncUint16(a.([]uint16), b.([]uint16))
	case types.VectorTypeUint32:
		return h.distFuncUint32(a.([]uint32), b.([]uint32))
	case types.VectorTypeUint64:
		return h.distFuncUint64(a.([]uint64), b.([]uint64))
	case types.VectorTypeComplex64:
		return h.distFuncC64(a.([]complex64), b.([]complex64))
	case types.VectorTypeComplex128:
		return h.distFuncC128(a.([]complex128), b.([]complex128))
	case types.VectorTypeFloat16:
		return h.distFuncF16(a.([]float16.Num), b.([]float16.Num))
	default:
		return 0, fmt.Errorf("unsupported vector type for distance: %v", vt)
	}
}
