package hnsw

import (
	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store/types"
)

// NewArrowSearchContext constructs a new ArrowSearchContext.
func NewArrowSearchContext() *ArrowSearchContext {
	return &ArrowSearchContext{
		candidates:              make([]types.Candidate, 0, 100),
		visited:                 types.NewArrowBitset(1000),
		dists:                   make([]float32, 0, 100),
		distsTemp:               make([]float32, 100),
		neighborBuf:             make([]uint32, 0, 64),
		layerCandidates:         make([]types.Candidate, 0, 100),
		resultSet:               make(CandidateHeap, 0, 100),
		scratchSelected:         make([]types.Candidate, 0, 100),
		scratchRemaining:        make([]types.Candidate, 0, 100),
		scratchDists:            make([]float32, 0, 128),
		scratchExtractedF32:     make([][]float32, 0, 100),
		scratchSelectedVecsF32:  make([][]float32, 0, 100),
		scratchExtractedF64:     make([][]float64, 0, 100),
		scratchSelectedVecsF64:  make([][]float64, 0, 100),
		scratchExtractedC64:     make([][]complex64, 0, 100),
		scratchSelectedVecsC64:  make([][]complex64, 0, 100),
		scratchExtractedC128:    make([][]complex128, 0, 100),
		scratchSelectedVecsC128: make([][]complex128, 0, 100),
		scratchExtractedAny:     make([]any, 0, 100),
		scratchSelectedVecsAny:  make([]any, 0, 100),
		scratchPool:             make([]uint32, 0, 128),
		scratchPruned:           make([]uint32, 0, 128),
		bulkCandidates:          make([]types.Candidate, 0, 256),
		bulkSources:             make([]uint32, 0, 64),
		bulkDists:               make([]float32, 0, 64),
		linkSrc:                 make([]uint32, 0, 1),
		linkDist:                make([]float32, 0, 1),
		batchSrc:                make([]uint32, 0, 64),
		batchNext:               make([]uint32, 0, 64),
		updateOldBuf:            make([]uint32, 0, 256),
		diffOld:                 make([]uint32, 0, 64),
		diffNew:                 make([]uint32, 0, 64),
		queryBQ:                 make([]uint64, 0, 256),
		querySQ8:                make([]uint8, 0, 1536),
		queryTQ:                 make([]byte, 0, 512),
		vectorBuf:               make([]float32, 0, 384),
		bufF32_2:                make([]float32, 0, 384),
		neighborBatch:           make([]uint32, 0, 64),
		matchResultBuf:          make([]byte, 64),
		dirty:                   false,
		operations:              0,
		cachedChunksF32:         make([]cachedChunkF32, 64),
		cachedChunksF64:         make([]cachedChunkF64, 16),
		cachedChunksInt8:        make([]cachedChunkInt8, 16),
		cachedChunksF16:         make([]cachedChunkF16, 16),
		cachedChunksC128:        make([]cachedChunkC128, 16),
		cachedChunksTQ:          make([]cachedChunkBytes, 16),
		cachedChunksSQ8:         make([]cachedChunkBytes, 16),
		chunkEpoch:              1,
		tqReconEpoch:            1,
	}
}

// NewArrowSearchContextPool creates a new pool for ArrowSearchContext objects.
func NewArrowSearchContextPool() *ArrowSearchContextPool {
	return &ArrowSearchContextPool{
		ring: NewLockFreeRingBuffer[*ArrowSearchContext](8192), // Support up to 8192 concurrent contexts
	}
}

// Get retrieves an ArrowSearchContext from the pool.
func (p *ArrowSearchContextPool) Get() *ArrowSearchContext {
	ctx, ok := p.ring.Pop()
	if !ok {
		// If pool is empty, allocate a new one
		ctx = NewArrowSearchContext()
		ctx.inUse.Store(true) // Mark as in use initially since we just allocated it
		return ctx
	}

	if !ctx.inUse.CompareAndSwap(false, true) {
		panic("ArrowSearchContextPool.Get: retrieved context is already in use! (possible internal pool corruption)")
	}

	ctx.Reset()
	return ctx
}

// Put runs the put operation.
func (p *ArrowSearchContextPool) Put(ctx *ArrowSearchContext) {
	if ctx == nil {
		return
	}

	if !ctx.inUse.CompareAndSwap(true, false) {
		panic("ArrowSearchContextPool.Put: context is not in use! (possible double-put)")
	}

	ctx.clearAppliers()
	ctx.clearComputers()

	// Try to return to pool, if full let GC handle it
	p.ring.Push(ctx)
}

// PutWithMetrics runs the put with metrics operation.
func (p *ArrowSearchContextPool) PutWithMetrics(ctx *ArrowSearchContext, dataType, dims string) {
	if ctx == nil {
		return
	}

	if !ctx.inUse.CompareAndSwap(true, false) {
		panic("ArrowSearchContextPool.PutWithMetrics: context is not in use! (possible double-put)")
	}

	ctx.clearAppliers()
	ctx.clearComputers()

	// Flush accumulated metrics if present
	if ctx.distComputeCount > 0 {
		metrics.RecordSearchBatchMetrics(dataType, dims, "euclidean", ctx.distComputeCount, ctx.distComputeTime)
	}

	// Try to return to pool, if full let GC handle it
	p.ring.Push(ctx)
}

// Reset clears the context for reuse.
// clearAppliers drops the neighbour appliers' references to the graph and the
// index.
//
// They have to be dropped when the context goes back to the pool, not only when
// it comes out: a pooled context is reachable for the life of the process, and
// a context still pointing at the GraphData it last wrote pins that snapshot
// alive. GraphData releases its Arrow references from a finalizer, so a pinned
// snapshot never reaches one and the vector buffers stay resident - 800 KB that
// no test could attribute, found as an Arrow allocator imbalance in
// TestHNSW_BulkInsert (roadmap Item 33).
func (ctx *ArrowSearchContext) clearAppliers() {
	ctx.batchApplier = batchNeighborApplier{}
	ctx.addConnApplier = addConnNeighborApplier{}
	ctx.pruneApplier = pruneNeighborApplier{}
	ctx.inboundApplier = inboundNeighborApplier{}
}

func (ctx *ArrowSearchContext) clearComputers() {
	ctx.compSlot = 0
	for i := range 2 {
		ctx.compF32ToF32[i] = float32ToFloat32Computer{}
		ctx.compF32[i] = float32Computer{}
		ctx.compF64[i] = float64Computer{}
		ctx.compF16[i] = float16Computer{}
		ctx.compC64[i] = complex64Computer{}
		ctx.compC128[i] = complex128Computer{}
		ctx.compInt8[i] = int8Computer{}
		ctx.compInt16[i] = int16Computer{}
		ctx.compUint16[i] = uint16Computer{}
		ctx.compInt32[i] = int32Computer{}
		ctx.compUint32[i] = uint32Computer{}
		ctx.compInt64[i] = int64Computer{}
		ctx.compUint64[i] = uint64Computer{}
		ctx.compTQ[i].data = nil
		ctx.compTQ[i].h = nil
		ctx.compTQ[i].diskGraph = nil
		ctx.compTQ[i].sctx = nil
		ctx.compTQ[i].batchOK = false
		ctx.compPQ[i].data = nil
		ctx.compPQ[i].h = nil
		ctx.compPQ[i].diskGraph = nil
		ctx.compPQ[i].table = nil
		ctx.compSQ8[i] = float32ToSQ8Computer{}
	}
}

func (ctx *ArrowSearchContext) nextCompSlot() int {
	slot := ctx.compSlot & 1
	ctx.compSlot++
	return slot
}

// Reset runs the reset operation.
func (ctx *ArrowSearchContext) Reset() {
	ctx.clearAppliers()
	ctx.candidates = ctx.candidates[:0]
	ctx.visited.Clear()
	ctx.dists = ctx.dists[:0]
	ctx.neighborBuf = ctx.neighborBuf[:0]
	ctx.neighborBatch = ctx.neighborBatch[:0]
	ctx.layerCandidates = ctx.layerCandidates[:0]
	ctx.resultSet = ctx.resultSet[:0]
	ctx.scratchSelected = ctx.scratchSelected[:0]
	ctx.scratchRemaining = ctx.scratchRemaining[:0]
	ctx.scratchDists = ctx.scratchDists[:0]
	ctx.scratchExtractedF32 = ctx.scratchExtractedF32[:0]
	ctx.scratchSelectedVecsF32 = ctx.scratchSelectedVecsF32[:0]
	ctx.scratchExtractedF64 = ctx.scratchExtractedF64[:0]
	ctx.scratchSelectedVecsF64 = ctx.scratchSelectedVecsF64[:0]
	ctx.scratchExtractedC64 = ctx.scratchExtractedC64[:0]
	ctx.scratchSelectedVecsC64 = ctx.scratchSelectedVecsC64[:0]
	ctx.scratchExtractedC128 = ctx.scratchExtractedC128[:0]
	ctx.scratchSelectedVecsC128 = ctx.scratchSelectedVecsC128[:0]
	ctx.scratchExtractedAny = ctx.scratchExtractedAny[:0]
	ctx.scratchSelectedVecsAny = ctx.scratchSelectedVecsAny[:0]
	ctx.scratchPool = ctx.scratchPool[:0]
	ctx.scratchPruned = ctx.scratchPruned[:0]
	ctx.bulkCandidates = ctx.bulkCandidates[:0]
	ctx.bulkSources = ctx.bulkSources[:0]
	ctx.bulkDists = ctx.bulkDists[:0]
	ctx.linkSrc = ctx.linkSrc[:0]
	ctx.linkDist = ctx.linkDist[:0]
	ctx.batchSrc = ctx.batchSrc[:0]
	ctx.batchNext = ctx.batchNext[:0]
	ctx.updateOldBuf = ctx.updateOldBuf[:0]
	ctx.diffOld = ctx.diffOld[:0]
	ctx.diffNew = ctx.diffNew[:0]
	ctx.queryBQ = ctx.queryBQ[:0]
	ctx.querySQ8 = ctx.querySQ8[:0]
	ctx.queryTQ = ctx.queryTQ[:0]
	ctx.rotatedQueryTQ = ctx.rotatedQueryTQ[:0]
	ctx.queryF64 = ctx.queryF64[:0]
	ctx.queryF16 = ctx.queryF16[:0]
	ctx.queryC64 = ctx.queryC64[:0]
	ctx.queryC128 = ctx.queryC128[:0]
	ctx.queryInt8 = ctx.queryInt8[:0]
	ctx.queryUint8 = ctx.queryUint8[:0]
	ctx.bufInt64 = ctx.bufInt64[:0]
	ctx.bufInt32 = ctx.bufInt32[:0]
	ctx.bufF32 = ctx.bufF32[:0]
	ctx.bufF32_2 = ctx.bufF32_2[:0]
	ctx.bufF64 = ctx.bufF64[:0]
	ctx.vectorBuf = ctx.vectorBuf[:0]
	ctx.batchVecsFloat32 = ctx.batchVecsFloat32[:0]
	ctx.batchVecsFloat64 = ctx.batchVecsFloat64[:0]
	ctx.batchVecsFloat16 = ctx.batchVecsFloat16[:0]
	ctx.batchVecsComplex64 = ctx.batchVecsComplex64[:0]
	ctx.batchVecsComplex128 = ctx.batchVecsComplex128[:0]
	ctx.dirty = false
	ctx.operations = 0
	ctx.distComputeTime = 0
	ctx.distComputeCount = 0
	ctx.nodesVisitedCount = 0
	ctx.diskGraph = nil
	ctx.visitedNodesBudget = 0
	ctx.nodesVisitedCount = 0
	ctx.dirty = false
	ctx.predicate = nil
	ctx.queryRadius = 0
	ctx.AllowUncommitted = false
	ctx.filterBitmap = nil
	ctx.filterMask = nil
	ctx.filterBits = ctx.filterBits[:0]

	// Clear temp buffer without reallocating
	for i := range ctx.distsTemp {
		ctx.distsTemp[i] = 0
	}

	// Invalidate chunk cache via generation bump (O(1) CPU cycle, zero allocations)
	ctx.chunkEpoch++
	if ctx.chunkEpoch == 0 {
		clear(ctx.cachedChunksF32)
		clear(ctx.cachedChunksF64)
		clear(ctx.cachedChunksInt8)
		clear(ctx.cachedChunksF16)
		clear(ctx.cachedChunksC128)
		clear(ctx.cachedChunksTQ)
		clear(ctx.cachedChunksSQ8)
		ctx.chunkEpoch = 1
	}

	// Invalidate TQ reconstruction cache via generation bump (O(1), zero allocations)
	ctx.tqReconEpoch++
	if ctx.tqReconEpoch == 0 {
		ctx.tqReconEpoch = 1
		clear(ctx.tqReconTable[:])
	}
}

// EnsureTQReconCapacity ensures the memoization buffer has room for tqReconCacheSlots * pow2 floats.
func (ctx *ArrowSearchContext) EnsureTQReconCapacity(pow2 int) {
	ctx.tqReconPow2 = pow2
	needed := tqReconCacheSlots * pow2
	if cap(ctx.tqReconVecs) < needed {
		ctx.tqReconVecs = make([]float32, needed)
	} else {
		ctx.tqReconVecs = ctx.tqReconVecs[:needed]
	}
}

// ProbeTQRecon probes the ephemeral cross-hop cache for an evaluated distance.
func (ctx *ArrowSearchContext) ProbeTQRecon(id uint32) (float32, bool) {
	set := int((id & 255) << 1)
	if ctx.tqReconTable[set].epoch == ctx.tqReconEpoch && ctx.tqReconTable[set].id == id {
		return ctx.tqReconTable[set].dist, true
	}
	if ctx.tqReconTable[set+1].epoch == ctx.tqReconEpoch && ctx.tqReconTable[set+1].id == id {
		return ctx.tqReconTable[set+1].dist, true
	}
	return 0, false
}

// ProbeTQReconVector probes the ephemeral cross-hop cache for a reconstructed float32 vector.
func (ctx *ArrowSearchContext) ProbeTQReconVector(id uint32) ([]float32, bool) {
	if ctx.tqReconPow2 <= 0 || len(ctx.tqReconVecs) < tqReconCacheSlots*ctx.tqReconPow2 {
		return nil, false
	}
	set := int((id & 255) << 1)
	var slot int
	if ctx.tqReconTable[set].epoch == ctx.tqReconEpoch && ctx.tqReconTable[set].id == id {
		slot = set
	} else if ctx.tqReconTable[set+1].epoch == ctx.tqReconEpoch && ctx.tqReconTable[set+1].id == id {
		slot = set + 1
	} else {
		return nil, false
	}
	start := slot * ctx.tqReconPow2
	return ctx.tqReconVecs[start : start+ctx.tqReconPow2], true
}

// InsertTQRecon inserts an evaluated distance into the ephemeral cross-hop cache.
func (ctx *ArrowSearchContext) InsertTQRecon(id uint32, dist float32) {
	set := int((id & 255) << 1)
	if ctx.tqReconTable[set].epoch != ctx.tqReconEpoch {
		ctx.tqReconTable[set] = tqReconCacheEntry{id: id, epoch: ctx.tqReconEpoch, dist: dist}
		return
	}
	if ctx.tqReconTable[set+1].epoch != ctx.tqReconEpoch {
		ctx.tqReconTable[set+1] = tqReconCacheEntry{id: id, epoch: ctx.tqReconEpoch, dist: dist}
		return
	}
	slot := set | int((id>>8)&1)
	ctx.tqReconTable[slot] = tqReconCacheEntry{id: id, epoch: ctx.tqReconEpoch, dist: dist}
}

// InsertTQReconWithVector inserts an evaluated distance and its reconstructed vector into the cache.
func (ctx *ArrowSearchContext) InsertTQReconWithVector(id uint32, dist float32, vec []float32) {
	set := int((id & 255) << 1)
	var slot int
	if ctx.tqReconTable[set].epoch != ctx.tqReconEpoch {
		slot = set
	} else if ctx.tqReconTable[set+1].epoch != ctx.tqReconEpoch {
		slot = set + 1
	} else {
		slot = set | int((id>>8)&1)
	}
	ctx.tqReconTable[slot] = tqReconCacheEntry{id: id, epoch: ctx.tqReconEpoch, dist: dist}
	if ctx.tqReconPow2 > 0 && len(vec) >= ctx.tqReconPow2 && len(ctx.tqReconVecs) >= (slot+1)*ctx.tqReconPow2 {
		start := slot * ctx.tqReconPow2
		copy(ctx.tqReconVecs[start:start+ctx.tqReconPow2], vec[:ctx.tqReconPow2])
	}
}

// Stats returns pool statistics.
func (p *ArrowSearchContextPool) Stats() (gets, puts int64) {
	return 0, 0
}

// MarkDirty indicates the context has been modified.
func (ctx *ArrowSearchContext) MarkDirty() {
	ctx.dirty = true
	ctx.operations++
}

// IsDirty returns true if the context has been modified.
func (ctx *ArrowSearchContext) IsDirty() bool {
	return ctx.dirty
}

// GetDiskGraph returns the cached DiskGraph reference if available.
func (ctx *ArrowSearchContext) GetDiskGraph() *DiskGraph {
	return ctx.diskGraph
}

// RecordEarlyExit increments the early exit counter with a specific reason.
// The reason is a dynamic label, so the accumulator is resolved through the
// memoized lookup of the sharded counter vector rather than by hashing the
// label set on every call.
func (ctx *ArrowSearchContext) RecordEarlyExit(reason string) {
	if c := metrics.HnswSearchEarlyExitsSharded.For(reason); c != nil {
		c.Inc()
	}
}

// EvaluatePredicateBatch evaluates a batch of IDs against the current predicate.
// It ensures the matchResultBuf is large enough and returns the results slice.
func (ctx *ArrowSearchContext) EvaluatePredicateBatch(ids []uint32) []byte {
	if ctx.predicate == nil {
		return nil
	}
	n := len(ids)
	if len(ctx.matchResultBuf) < n {
		ctx.matchResultBuf = make([]byte, n*2)
	}
	results := ctx.matchResultBuf[:n]
	ctx.predicate.MatchBatch(ids, results)
	return results
}

// SetDiskGraph sets the DiskGraph reference to be cached for this search.
func (ctx *ArrowSearchContext) SetDiskGraph(dg *DiskGraph) {
	ctx.diskGraph = dg
}

// GetChunkF32 returns the cached float32 chunk slice for cID under maxGen.
