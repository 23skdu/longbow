package index

import (
	"container/heap"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/23skdu/longbow/internal/store/types"
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
	updateOldBuf   []uint32          // pre-allocated buffer for UpdateNeighbors to read old neighbors without heap escape
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

	// Pooled DistanceComputer instances reused per search/operation to eliminate
	// per-resolve heap allocations (roadmap A6).
	compSlot     int
	compF32ToF32 [2]float32ToFloat32Computer
	compF32      [2]float32Computer
	compF64      [2]float64Computer
	compF16      [2]float16Computer
	compC64      [2]complex64Computer
	compC128     [2]complex128Computer
	compInt8     [2]int8Computer
	compInt16    [2]int16Computer
	compUint16   [2]uint16Computer
	compInt32    [2]int32Computer
	compUint32   [2]uint32Computer
	compInt64    [2]int64Computer
	compUint64   [2]uint64Computer
	compTQ       [2]tqComputer
	compPQ       [2]pqComputer
	compSQ8      [2]float32ToSQ8Computer

	// inDegreeView overrides where the pruning guard reads inbound-edge counts
	// from. nil means the index's live tracker. Bulk linkage sets it to a
	// snapshot taken between its passes, because the guard's decision has to be
	// a function of the sub-batch: reading the live tracker from several
	// goroutines at once made the surviving edges depend on how far the other
	// goroutines had got (roadmap Item 31).
	inDegreeView inDegreeReader

	scratchExtractedF32     [][]float32
	scratchSelectedVecsF32  [][]float32
	scratchExtractedF64     [][]float64
	scratchSelectedVecsF64  [][]float64
	scratchExtractedC64     [][]complex64
	scratchSelectedVecsC64  [][]complex64
	scratchExtractedC128    [][]complex128
	scratchSelectedVecsC128 [][]complex128
	scratchExtractedAny     []any
	scratchSelectedVecsAny  []any
	scratchPool             []uint32
	scratchPruned           []uint32

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
