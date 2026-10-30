package index

import (
	"container/heap"
	"math"
	"time"

	"github.com/23skdu/longbow/internal/store/types"

	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow/float16"
)

// CandidateHeap implements a max-heap of Candidates for search results
type CandidateHeap []types.Candidate

func (h *CandidateHeap) Len() int           { return len(*h) }
func (h *CandidateHeap) Less(i, j int) bool { return (*h)[i].Dist < (*h)[j].Dist }
func (h *CandidateHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *CandidateHeap) Push(x any) {
	*h = append(*h, x.(types.Candidate))
}

func (h *CandidateHeap) Pop() any {
	old := *h
	n := len(old)
	if n == 0 {
		panic("CandidateHeap.Pop: heap is empty! (possible concurrent modification)")
	}
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Clear removes all elements from the heap.
func (h *CandidateHeap) Clear() {
	*h = (*h)[:0]
}

// PopCandidate pops and returns the top candidate (typed helper)
func (h *CandidateHeap) PopCandidate() (types.Candidate, bool) {
	if len(*h) == 0 {
		return types.Candidate{}, false
	}
	return heap.Pop(h).(types.Candidate), true
}

// PopAndReturn pops the top candidate and returns it (typed helper).
// Used in tests; production code uses PopCandidate.
func (h *CandidateHeap) PopAndReturn() (types.Candidate, bool) {
	if len(*h) == 0 {
		return types.Candidate{}, false
	}
	return heap.Pop(h).(types.Candidate), true
}

// CandidatesMinHeap returns the frontier candidate min-heap adapter backed by ctx.candidates.
func (ctx *ArrowSearchContext) CandidatesMinHeap() *MinCandidateHeapAdapter {
	return (*MinCandidateHeapAdapter)(&ctx.candidates)
}

// ResultSetMaxHeap returns the result set max-heap adapter backed by ctx.resultSet.
func (ctx *ArrowSearchContext) ResultSetMaxHeap() *MaxCandidateHeapAdapter {
	return (*MaxCandidateHeapAdapter)(&ctx.resultSet)
}

// ArrowSearchContext holds pre-allocated buffers for Arrow-based HNSW search operations
// to minimize allocation overhead during high-frequency searches.
type ArrowSearchContext struct {
	QueryID uint32 // ID of the node being searched for (for debugging)

	// Pre-allocated result buffers
	candidates CandidateHeap
	visited    *types.ArrowBitset

	queryBQ  []uint64
	querySQ8 []uint8
	queryTQ  []byte

	// Cached rotated query for TurboQuant search (avoid recomputing per distance call)
	rotatedQueryTQ []float32

	// Pre-converted query buffers for cross-type search
	queryF64    []float64
	queryF16    []float16.Num
	queryC64    []complex64
	queryC128   []complex128
	queryInt8   []int8
	queryUint8  []uint8
	queryInt16  []int16
	queryUint16 []uint16
	queryInt32  []int32
	queryUint32 []uint32
	queryInt64  []int64
	queryUint64 []uint64

	// Zero-allocation pre-sized batch buffers for SIMD batching
	// These avoid per-search allocation by reusing pre-sized slices
	batchVecsFloat32    [][]float32
	batchVecsFloat64    [][]float64
	batchVecsFloat16    [][]float16.Num
	batchVecsComplex64  [][]complex64
	batchVecsComplex128 [][]complex128

	// Distance calculation buffers
	dists     []float32
	distsTemp []float32

	// Neighbor tracking
	neighborBuf    []uint32
	neighborBatch  []uint32
	matchResultBuf []byte

	// Vectorized predicate buffers
	bufInt64 []int64
	bufInt32 []int32
	bufF32   []float32
	bufF32_2 []float32
	bufF64   []float64

	// Layer-specific buffers
	layerCandidates []types.Candidate

	// Scratch buffers for heuristics
	resultSet        CandidateHeap
	scratchSelected  []types.Candidate
	scratchRemaining []types.Candidate

	scratchDists []float32

	// Bulk-linkage scratch.
	//
	// addBatchBulkInternal links one node at a time and each node performs a
	// forward AddConnectionsBatch plus one reverse AddConnectionsBatch per
	// selected neighbour. Without these buffers that is ~3 heap slices per
	// neighbour per node per layer: across a 1M-node build the profile showed
	// 39.4 GB allocated by the CAS retry loop in UpdateNeighbors, 32.3 GB by
	// computePrunedNeighbors and 28.9 GB by AddConnectionsBatch. The buffers
	// below are owned exclusively by the linkage path for the lifetime of one
	// node's linkage, which is what makes them safe to reuse - they are never
	// handed to the reader-facing search path.
	bulkCandidates []types.Candidate // filtered candidate list before selectNeighbors
	bulkSources    []uint32          // forward neighbour IDs
	bulkDists      []float32         // forward neighbour distances
	linkSrc        []uint32          // single-element reverse-link source list
	linkDist       []float32         // single-element reverse-link distance list
	batchSrc       []uint32          // not-yet-present sources of a batch add
	batchNext      []uint32          // merged neighbour list of a batch add
	diffOld        []uint32          // pre-update neighbour list for the in-degree diff
	diffNew        []uint32          // post-update neighbour list for the in-degree diff
	// diff records the committed old/next pair for the call in flight. It
	// lives here rather than in AddConnectionsBatch locals because the
	// UpdateNeighbors closure assigns through it, which would otherwise force
	// two slice headers - and the closure environment - onto the heap on every
	// one of the ~33 calls a single bulk-inserted node makes.
	diff neighborDiff

	// The read-modify-write bodies UpdateNeighbors runs, one per call site.
	// They live here rather than in closures so a pooled context pays for them
	// once instead of every call heap-allocating a new applier (roadmap Item
	// 33). Only one is in flight at a time, which is the same assumption the
	// pooled scratch buffers already rest on.
	batchApplier   batchNeighborApplier
	addConnApplier addConnNeighborApplier
	pruneApplier   pruneNeighborApplier
	inboundApplier inboundNeighborApplier

	// inDegreeView overrides where the pruning guard reads inbound-edge counts
	// from. nil means the index's live tracker. Bulk linkage sets it to a
	// snapshot taken between its passes, because the guard's decision has to be
	// a function of the sub-batch: reading the live tracker from several
	// goroutines at once made the surviving edges depend on how far the other
	// goroutines had got (roadmap Item 31).
	inDegreeView inDegreeReader

	scratchExtractedF32    [][]float32
	scratchSelectedVecsF32 [][]float32
	scratchPool            []uint32
	scratchPruned          []uint32

	vectorBuf []float32

	// BQ (Binary Quantization) search mode
	useBQSearch bool

	// Filter bitmap for early filtering during search
	filterBitmap *roaring.Bitmap

	// filterMask is filterBitmap pre-converted to a dense bitmask, or holding
	// filterBitmap itself when densifying it is not worthwhile, so that graph
	// traversal probes candidates with a single word load. nil means the search
	// is unfiltered.
	filterMask *filterMask
	// filterBits is the reusable dense backing store for filterMask.
	filterBits types.BitVector

	// Cached DiskGraph reference for the duration of the search
	diskGraph *DiskGraph

	// Visited nodes budget for early termination
	visitedNodesBudget int
	nodesVisitedCount  int

	// Reset tracking
	dirty bool

	// inUse tracks if the context is currently being used by a search operation.
	// This is used to detect concurrent access or double-puts to the pool.
	inUse atomic.Bool

	// Thread-local metrics
	operations       int
	distComputeTime  time.Duration
	distComputeCount int

	// HNSW predicate for early-exit filtering
	predicate types.HNSWPredicate

	queryRadius float32
	// AllowUncommitted allows search to see nodes beyond global nodeCount
	// (used during internal bootstrap/linkage operations)
	AllowUncommitted bool
	MaxNodeCount     int64
	MaxGeneration    uint64

	// Chunk cache for zero-overhead vector dereferencing during search (roadmap Item 7)
	chunkEpoch       uint32
	cachedChunksF32  []cachedChunkF32
	cachedChunksF64  []cachedChunkF64
	cachedChunksInt8 []cachedChunkInt8
	cachedChunksF16  []cachedChunkF16
	cachedChunksC128 []cachedChunkC128
	cachedChunksTQ   []cachedChunkBytes
	cachedChunksSQ8  []cachedChunkBytes

	// Ephemeral reconstructed TQ vector memoization across search hops (roadmap Item 26)
	tqReconEpoch uint32
	tqReconTable [tqReconCacheSlots]tqReconCacheEntry
	tqReconVecs  []float32
	tqReconPow2  int
}

const tqReconCacheSlots = 512

type tqReconCacheEntry struct {
	id    uint32
	epoch uint32
	dist  float32
}

type cachedChunkF32 struct {
	slice []float32
	epoch uint32
}

type cachedChunkF64 struct {
	slice []float64
	epoch uint32
}

type cachedChunkInt8 struct {
	slice []int8
	epoch uint32
}

type cachedChunkF16 struct {
	slice []float16.Num
	epoch uint32
}

type cachedChunkC128 struct {
	slice []complex128
	epoch uint32
}

type cachedChunkBytes struct {
	slice []byte
	epoch uint32
}

// neighborDiff records the pre- and post-update neighbour lists a single
// AddConnection/AddConnectionsBatch call committed, so the layer-0 in-degree
// delta can be applied after UpdateNeighbors returns.
type neighborDiff struct {
	old   []uint32
	next  []uint32
	valid bool
}

// ArrowSearchContextPool manages reusable ArrowSearchContext objects.
type ArrowSearchContextPool struct {
	ring *LockFreeRingBuffer[*ArrowSearchContext]
}

// NewArrowSearchContext creates a new ArrowSearchContext with default capacity.
func NewArrowSearchContext() *ArrowSearchContext {
	return &ArrowSearchContext{
		candidates:             make([]types.Candidate, 0, 100),
		visited:                types.NewArrowBitset(1000),
		dists:                  make([]float32, 0, 100),
		distsTemp:              make([]float32, 100),
		neighborBuf:            make([]uint32, 0, 64),
		layerCandidates:        make([]types.Candidate, 0, 100),
		resultSet:              make(CandidateHeap, 0, 100),
		scratchSelected:        make([]types.Candidate, 0, 100),
		scratchRemaining:       make([]types.Candidate, 0, 100),
		scratchDists:           make([]float32, 0, 128),
		scratchExtractedF32:    make([][]float32, 0, 100),
		scratchSelectedVecsF32: make([][]float32, 0, 100),
		scratchPool:            make([]uint32, 0, 128),
		scratchPruned:          make([]uint32, 0, 128),
		bulkCandidates:         make([]types.Candidate, 0, 256),
		bulkSources:            make([]uint32, 0, 64),
		bulkDists:              make([]float32, 0, 64),
		linkSrc:                make([]uint32, 0, 1),
		linkDist:               make([]float32, 0, 1),
		batchSrc:               make([]uint32, 0, 64),
		batchNext:              make([]uint32, 0, 64),
		diffOld:                make([]uint32, 0, 64),
		diffNew:                make([]uint32, 0, 64),
		queryBQ:                make([]uint64, 0, 256),
		querySQ8:               make([]uint8, 0, 1536),
		queryTQ:                make([]byte, 0, 512),
		vectorBuf:              make([]float32, 0, 384),
		bufF32_2:               make([]float32, 0, 384),
		neighborBatch:          make([]uint32, 0, 64),
		matchResultBuf:         make([]byte, 64),
		dirty:                  false,
		operations:             0,
		cachedChunksF32:        make([]cachedChunkF32, 64),
		cachedChunksF64:        make([]cachedChunkF64, 16),
		cachedChunksInt8:       make([]cachedChunkInt8, 16),
		cachedChunksF16:        make([]cachedChunkF16, 16),
		cachedChunksC128:       make([]cachedChunkC128, 16),
		cachedChunksTQ:         make([]cachedChunkBytes, 16),
		cachedChunksSQ8:        make([]cachedChunkBytes, 16),
		chunkEpoch:             1,
		tqReconEpoch:           1,
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

func (p *ArrowSearchContextPool) Put(ctx *ArrowSearchContext) {
	if ctx == nil {
		return
	}

	if !ctx.inUse.CompareAndSwap(true, false) {
		panic("ArrowSearchContextPool.Put: context is not in use! (possible double-put)")
	}

	ctx.clearAppliers()

	// Try to return to pool, if full let GC handle it
	p.ring.Push(ctx)
}

func (p *ArrowSearchContextPool) PutWithMetrics(ctx *ArrowSearchContext, dataType, dims string) {
	if ctx == nil {
		return
	}

	if !ctx.inUse.CompareAndSwap(true, false) {
		panic("ArrowSearchContextPool.PutWithMetrics: context is not in use! (possible double-put)")
	}

	ctx.clearAppliers()

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
	ctx.scratchPool = ctx.scratchPool[:0]
	ctx.scratchPruned = ctx.scratchPruned[:0]
	ctx.bulkCandidates = ctx.bulkCandidates[:0]
	ctx.bulkSources = ctx.bulkSources[:0]
	ctx.bulkDists = ctx.bulkDists[:0]
	ctx.linkSrc = ctx.linkSrc[:0]
	ctx.linkDist = ctx.linkDist[:0]
	ctx.batchSrc = ctx.batchSrc[:0]
	ctx.batchNext = ctx.batchNext[:0]
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
func (ctx *ArrowSearchContext) GetChunkF32(data *types.GraphData, cID int, maxGen uint64) []float32 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksF32) && ctx.cachedChunksF32[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksF32[cID].slice
	}
	return ctx.resolveChunkF32(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkF32(data *types.GraphData, cID int, maxGen uint64) []float32 {
	var chunk []float32
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsChunkFast(cID)
	} else {
		chunk = data.GetVectorsChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksF32) {
		newLen := cID + 1
		if newLen < 64 {
			newLen = 64
		}
		if newLen < len(ctx.cachedChunksF32)*2 {
			newLen = len(ctx.cachedChunksF32) * 2
		}
		newBuf := make([]cachedChunkF32, newLen)
		copy(newBuf, ctx.cachedChunksF32)
		ctx.cachedChunksF32 = newBuf
	}
	ctx.cachedChunksF32[cID] = cachedChunkF32{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorF32 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorF32(data *types.GraphData, id uint32, dims int, maxGen uint64) []float32 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkF32(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeFloat32)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkF64 returns the cached float64 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkF64(data *types.GraphData, cID int, maxGen uint64) []float64 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksF64) && ctx.cachedChunksF64[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksF64[cID].slice
	}
	return ctx.resolveChunkF64(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkF64(data *types.GraphData, cID int, maxGen uint64) []float64 {
	var chunk []float64
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsFloat64ChunkFast(cID)
	} else {
		chunk = data.GetVectorsFloat64ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksF64) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksF64)*2 {
			newLen = len(ctx.cachedChunksF64) * 2
		}
		newBuf := make([]cachedChunkF64, newLen)
		copy(newBuf, ctx.cachedChunksF64)
		ctx.cachedChunksF64 = newBuf
	}
	ctx.cachedChunksF64[cID] = cachedChunkF64{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorF64 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorF64(data *types.GraphData, id uint32, dims int, maxGen uint64) []float64 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkF64(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeFloat64)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkInt8 returns the cached int8 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkInt8(data *types.GraphData, cID int, maxGen uint64) []int8 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksInt8) && ctx.cachedChunksInt8[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksInt8[cID].slice
	}
	return ctx.resolveChunkInt8(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkInt8(data *types.GraphData, cID int, maxGen uint64) []int8 {
	var chunk []int8
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsInt8ChunkFast(cID)
	} else {
		chunk = data.GetVectorsInt8ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksInt8) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksInt8)*2 {
			newLen = len(ctx.cachedChunksInt8) * 2
		}
		newBuf := make([]cachedChunkInt8, newLen)
		copy(newBuf, ctx.cachedChunksInt8)
		ctx.cachedChunksInt8 = newBuf
	}
	ctx.cachedChunksInt8[cID] = cachedChunkInt8{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorInt8 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorInt8(data *types.GraphData, id uint32, dims int, maxGen uint64) []int8 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkInt8(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeInt8)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkSQ8 returns the cached SQ8 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkSQ8(data *types.GraphData, cID int, maxGen uint64) []byte {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksSQ8) && ctx.cachedChunksSQ8[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksSQ8[cID].slice
	}
	return ctx.resolveChunkSQ8(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkSQ8(data *types.GraphData, cID int, maxGen uint64) []byte {
	var chunk []byte
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsSQ8ChunkFast(cID)
	} else {
		chunk = data.GetVectorsSQ8ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksSQ8) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksSQ8)*2 {
			newLen = len(ctx.cachedChunksSQ8) * 2
		}
		newBuf := make([]cachedChunkBytes, newLen)
		copy(newBuf, ctx.cachedChunksSQ8)
		ctx.cachedChunksSQ8 = newBuf
	}
	ctx.cachedChunksSQ8[cID] = cachedChunkBytes{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorSQ8 retrieves the dims-length SQ8 vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorSQ8(data *types.GraphData, id uint32, dims int, maxGen uint64) []byte {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkSQ8(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeUint8)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkF16 returns the cached float16 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkF16(data *types.GraphData, cID int, maxGen uint64) []float16.Num {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksF16) && ctx.cachedChunksF16[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksF16[cID].slice
	}
	return ctx.resolveChunkF16(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkF16(data *types.GraphData, cID int, maxGen uint64) []float16.Num {
	var chunk []float16.Num
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsF16ChunkFast(cID)
	} else {
		chunk = data.GetVectorsF16ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksF16) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksF16)*2 {
			newLen = len(ctx.cachedChunksF16) * 2
		}
		newBuf := make([]cachedChunkF16, newLen)
		copy(newBuf, ctx.cachedChunksF16)
		ctx.cachedChunksF16 = newBuf
	}
	ctx.cachedChunksF16[cID] = cachedChunkF16{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorF16 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorF16(data *types.GraphData, id uint32, dims int, maxGen uint64) []float16.Num {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkF16(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeFloat16)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}

// GetChunkC128 returns the cached complex128 chunk slice for cID under maxGen.
func (ctx *ArrowSearchContext) GetChunkC128(data *types.GraphData, cID int, maxGen uint64) []complex128 {
	if cID < 0 || data == nil {
		return nil
	}
	if cID < len(ctx.cachedChunksC128) && ctx.cachedChunksC128[cID].epoch == ctx.chunkEpoch {
		return ctx.cachedChunksC128[cID].slice
	}
	return ctx.resolveChunkC128(data, cID, maxGen)
}

func (ctx *ArrowSearchContext) resolveChunkC128(data *types.GraphData, cID int, maxGen uint64) []complex128 {
	var chunk []complex128
	if maxGen == math.MaxUint64 {
		chunk = data.GetVectorsComplex128ChunkFast(cID)
	} else {
		chunk = data.GetVectorsComplex128ChunkWithGen(cID, maxGen)
	}
	if chunk == nil {
		return nil
	}
	if cID >= len(ctx.cachedChunksC128) {
		newLen := cID + 1
		if newLen < 32 {
			newLen = 32
		}
		if newLen < len(ctx.cachedChunksC128)*2 {
			newLen = len(ctx.cachedChunksC128) * 2
		}
		newBuf := make([]cachedChunkC128, newLen)
		copy(newBuf, ctx.cachedChunksC128)
		ctx.cachedChunksC128 = newBuf
	}
	ctx.cachedChunksC128[cID] = cachedChunkC128{
		slice: chunk,
		epoch: ctx.chunkEpoch,
	}
	return chunk
}

// GetVectorC128 retrieves the dims-length vector for node id from cached chunks.
func (ctx *ArrowSearchContext) GetVectorC128(data *types.GraphData, id uint32, dims int, maxGen uint64) []complex128 {
	cID := int(id) / types.ChunkSize
	chunk := ctx.GetChunkC128(data, cID, maxGen)
	if chunk == nil {
		return nil
	}
	cOff := int(id) % types.ChunkSize
	pd := data.GetPaddedDimsForType(types.VectorTypeComplex128)
	start := cOff * pd
	if start+dims <= len(chunk) {
		return chunk[start : start+dims]
	}
	return nil
}
