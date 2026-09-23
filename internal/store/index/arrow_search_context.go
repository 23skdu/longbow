package index

import (
	"container/heap"
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
	batchVecsInt8       [][]int8
	batchVecsInt16      [][]int16
	batchVecsInt32      [][]int32
	batchVecsInt64      [][]int64
	batchVecsUint8      [][]uint8
	batchVecsUint16     [][]uint16
	batchVecsUint32     [][]uint32
	batchVecsUint64     [][]uint64

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

	scratchExtractedF32    [][]float32
	scratchSelectedVecsF32 [][]float32
	scratchPool            []uint32
	scratchPruned          []uint32

	vectorBuf []float32

	pruneDepth int

	// BQ (Binary Quantization) search mode
	useBQSearch bool

	// Filter bitmap for early filtering during search
	filterBitmap *roaring.Bitmap

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
		queryBQ:                make([]uint64, 0, 256),
		querySQ8:               make([]uint8, 0, 1536),
		queryTQ:                make([]byte, 0, 512),
		vectorBuf:              make([]float32, 0, 384),
		bufF32_2:               make([]float32, 0, 384),
		neighborBatch:          make([]uint32, 0, 64),
		matchResultBuf:         make([]byte, 64),
		dirty:                  false,
		operations:             0,
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

	// Flush accumulated metrics if present
	if ctx.distComputeCount > 0 {
		metrics.RecordSearchBatchMetrics(dataType, dims, "euclidean", ctx.distComputeCount, ctx.distComputeTime)
	}

	// Try to return to pool, if full let GC handle it
	p.ring.Push(ctx)
}

// Reset clears the context for reuse.
func (ctx *ArrowSearchContext) Reset() {
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
	ctx.queryBQ = ctx.queryBQ[:0]
	ctx.querySQ8 = ctx.querySQ8[:0]
	ctx.queryTQ = ctx.queryTQ[:0]
	ctx.rotatedQueryTQ = ctx.rotatedQueryTQ[:0]
	ctx.queryF64 = ctx.queryF64[:0]
	ctx.queryF16 = ctx.queryF16[:0]
	ctx.queryC64 = ctx.queryC64[:0]
	ctx.queryC128 = ctx.queryC128[:0]
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
	ctx.batchVecsInt8 = ctx.batchVecsInt8[:0]
	ctx.batchVecsInt16 = ctx.batchVecsInt16[:0]
	ctx.batchVecsInt32 = ctx.batchVecsInt32[:0]
	ctx.batchVecsInt64 = ctx.batchVecsInt64[:0]
	ctx.batchVecsUint8 = ctx.batchVecsUint8[:0]
	ctx.batchVecsUint16 = ctx.batchVecsUint16[:0]
	ctx.batchVecsUint32 = ctx.batchVecsUint32[:0]
	ctx.batchVecsUint64 = ctx.batchVecsUint64[:0]
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

	// Clear temp buffer without reallocating
	for i := range ctx.distsTemp {
		ctx.distsTemp[i] = 0
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
func (ctx *ArrowSearchContext) RecordEarlyExit(reason string) {
	metrics.HnswSearchEarlyExitsTotal.WithLabelValues(reason).Inc()
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
