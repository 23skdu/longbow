package index

// Neighbour appliers: the read-modify-write bodies that UpdateNeighbors runs,
// lifted out of closures and onto the caller's pooled search context.
//
// Every one of these was a func literal capturing the target, the sources, the
// limits, the graph and the context. A closure that escapes its call site - and
// UpdateNeighbors hands it to a retry loop - is heap-allocated on every call,
// and the bulk linker runs the batch one once per node plus once per reverse
// link: 321 MB of the 2.0 GB a 100k float32 ingest allocated was the closure
// alone (roadmap Item 33).
//
// Each applier lives on ArrowSearchContext, so the pooled context pays for it
// once and the retry loop reuses it. An applier must be a function of `old` and
// its own fields only: the packed path calls ApplyNeighbors again after a lost
// CAS.

import (
	"slices"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/store/types"
)

// batchNeighborApplier is the AddConnectionsBatch body: add the batch's
// sources to the target's list, pruning with the diversity heuristic when the
// result would exceed the layer's connection limit.
type batchNeighborApplier struct {
	h       *ArrowHNSW
	data    *types.GraphData
	target  uint32
	sources []uint32
	layer   int
	maxConn int
	d       *neighborDiff
	ctx     *ArrowSearchContext
}

// ApplyNeighbors computes the target's next list. See AddConnectionsBatch for
// why each buffer lives on the context.
func (a *batchNeighborApplier) ApplyNeighbors(old []uint32) []uint32 {
	h, ctx := a.h, a.ctx
	target, sources := a.target, a.sources

	// Find truly new sources.
	//
	// The previous shape built newSources with a bare append from nil
	// (log2(len(sources)) reallocations per call) and then rebuilt `next`
	// with a fresh make plus two slices.Clone. Bulk linkage calls this once
	// per node and once per reverse link, so across a 1M-node build that was
	// the bulk of the 39.4 GB attributed to this path. All three buffers now
	// live on the caller's pooled search context and are reused across calls.
	newSources := newSourcesBuf(ctx, len(sources))
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
	if ctx != nil {
		ctx.batchSrc = newSources[:0]
	}

	if len(newSources) == 0 {
		// No change; see the matching note in AddConnection about
		// keeping the recorded diff in step with the winning
		// attempt.
		a.d.set(old, old)
		return nil
	}

	var next []uint32
	if len(old)+len(newSources) <= a.maxConn {
		merged := newNextBuf(ctx, len(old)+len(newSources))
		next = append(merged, old...)
		next = append(next, newSources...)
		if ctx != nil {
			// Keep the grown array: append reallocates past cap, and
			// without storing it back the next call would start from the
			// stale (smaller) buffer and allocate again.
			ctx.batchNext = next[:0]
		}
	} else {
		// Pruning needed
		h.bulkContentionCount.Add(1)
		next = h.computePrunedNeighbors(ctx, a.data, target, old, newSources, a.maxConn, a.layer)
	}

	// Snapshot for the layer-0 in-degree delta applied after commit.
	a.d.set(old, next)
	return next
}

// addConnNeighborApplier is the AddConnection body: one edge, pruned in place
// when the source is already at its limit.
type addConnNeighborApplier struct {
	h       *ArrowHNSW
	data    *types.GraphData
	source  uint32
	target  uint32
	layer   int
	maxConn int
	d       *neighborDiff
	ctx     *ArrowSearchContext
}

func (a *addConnNeighborApplier) ApplyNeighbors(old []uint32) []uint32 {
	for _, n := range old {
		if n == a.target {
			// No change. The bookkeeping after the CAS loop settles has to
			// be told about this path too: leaving a previous losing
			// attempt's pair in place would apply a diff that was never
			// committed.
			a.d.set(old, old)
			return nil
		}
	}

	var next []uint32
	if len(old) < a.maxConn {
		// ctx.batchNext is reset here rather than reset-then-append so
		// the previous call's contents are never observable. The arena
		// copy in SetNeighbors happens before the next call can reuse
		// the buffer.
		next = append(newNextBuf(a.ctx, len(old)+1), old...)
		next = append(next, a.target)
	} else {
		// Pruning needed - Diversity heuristic
		// This is expensive, but only happens when we hit maxConn
		a.h.bulkContentionCount.Add(1)
		single := newSourcesBuf(a.ctx, 1)
		single = append(single, a.target)
		next = a.h.computePrunedNeighbors(a.ctx, a.data, a.source, old, single, a.maxConn, a.layer)
	}

	// Snapshot for the layer-0 in-degree delta applied after commit.
	a.d.set(old, next)
	return next
}

// pruneNeighborApplier is the PruneConnections body: shrink an over-full list
// to maxConn with the diversity heuristic, keeping the winning pair for the
// in-degree delta.
type pruneNeighborApplier struct {
	h       *ArrowHNSW
	data    *types.GraphData
	id      uint32
	maxConn int
	layer   int
	ctx     *ArrowSearchContext
	lastOld []uint32
	lastNew []uint32
}

func (a *pruneNeighborApplier) ApplyNeighbors(old []uint32) []uint32 {
	if len(old) <= a.maxConn {
		// No change; see the matching note in AddConnection.
		a.lastOld, a.lastNew = old, old
		return nil
	}

	a.h.bulkContentionCount.Add(1)
	next := a.h.computePrunedNeighbors(a.ctx, a.data, a.id, old, nil, a.maxConn, a.layer)
	a.lastOld = slices.Clone(old)
	a.lastNew = slices.Clone(next)
	atomic.AddUint64(&a.data.GlobalVersion, 1)
	return next
}

// inboundNeighborApplier is the ensureInboundEdge body: give `target` an
// inbound edge from `host`, evicting host's furthest neighbour that has other
// inbound edges when host is full.
type inboundNeighborApplier struct {
	h       *ArrowHNSW
	data    *types.GraphData
	host    uint32
	target  uint32
	maxConn int
	ctx     *ArrowSearchContext
	lastOld []uint32
	lastNew []uint32
}

func (a *inboundNeighborApplier) ApplyNeighbors(old []uint32) []uint32 {
	for _, o := range old {
		if o == a.target {
			a.lastOld, a.lastNew = old, old
			return nil
		}
	}
	if len(old) < a.maxConn {
		var next []uint32
		if a.ctx != nil && cap(a.ctx.scratchPool) >= len(old)+1 {
			next = append(a.ctx.scratchPool[:0], old...)
		} else {
			next = make([]uint32, len(old), len(old)+1)
			copy(next, old)
		}
		next = append(next, a.target)
		a.lastOld = slices.Clone(old)
		a.lastNew = slices.Clone(next)
		return next
	}

	// Host is full: find neighbor in old with inDegree > 1 and maximum distance from host.
	bestToEvict := -1
	var bestDist float32 = -1
	var dists []float32
	if a.ctx != nil && cap(a.ctx.scratchDists) >= len(old) {
		dists = a.ctx.scratchDists[:len(old)]
	} else {
		dists = make([]float32, len(old))
	}
	a.h.computeDistances(a.ctx, a.data, a.host, old, dists)

	for i, o := range old {
		if a.h.inDegreeL0.Get(o) > 1 {
			if dists[i] > bestDist {
				bestDist = dists[i]
				bestToEvict = i
			}
		}
	}

	if bestToEvict < 0 {
		// All neighbors of host have inDegree <= 1; cannot evict without stranding someone.
		a.lastOld, a.lastNew = old, old
		return nil
	}

	var next []uint32
	if a.ctx != nil && cap(a.ctx.scratchPool) >= len(old) {
		next = append(a.ctx.scratchPool[:0], old...)
	} else {
		next = make([]uint32, len(old))
		copy(next, old)
	}
	next[bestToEvict] = a.target
	a.lastOld = slices.Clone(old)
	a.lastNew = slices.Clone(next)
	return next
}
