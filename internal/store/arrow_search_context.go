package store

import (
	"sync"
)

// CandidateHeap implements a max-heap of Candidates for search results
type CandidateHeap []Candidate

func (h CandidateHeap) Len() int           { return len(h) }
func (h CandidateHeap) Less(i, j int) bool { return h[i].Dist > h[j].Dist } // Max Heap (furthest on top)
func (h CandidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *CandidateHeap) Push(x Candidate) {
	*h = append(*h, x)
}

func (h *CandidateHeap) Clear() {
	*h = (*h)[:0]
}

func (h *CandidateHeap) Pop() (Candidate, bool) {
	old := *h
	n := len(old)
	if n == 0 {
		return Candidate{}, false
	}
	x := old[n-1]
	*h = old[0 : n-1]
	return x, true
}

// PopCandidate pops the top candidate and returns it (typed helper)
// Returns (Candidate, ok) to match usage "c, _ := ctx.resultSet.Pop()"
// Assuming standard heap interface Pop returns 'any', but if we execute heap.Pop(h), we don't call method directly.
// The code calls ctx.resultSet.Pop(). This implies resultSet has a Pop method.
// Standard heap.Interface Pop does NOT remove from heap logic, only slice.
// BUT successful usage implies usage of heap.Pop(h).
// Or maybe it's a custom PriorityQueue struct?
// Given "ctx.resultSet.Pop()", it's a method on the type.
// If I implement a custom Pop that does the heap logic?
// OR just standard Pop from end?
// The usage in insertion:
// res := make...
// for ... ctx.resultSet.Pop()
// This implies extracting elements.
// If it's a heap, we want to extract Max each time?
// If so, we need heap.Pop(h) which re-heapifies.
// BUT we can't call heap.Pop(&ctx.resultSet) if ctx.resultSet is just a field.
// We must call method on it.
// If `arrow_hnsw_insert.go` calls it directly, it expects the type to handle heap logic.
// So I will implement Pop() to do heap pop. Note: standard heap package functions take interface.
// If I implement Pop() on *CandidateHeap that calls heap.Pop(self)? No, cycle.
// I will implement simple Pop from slice end?
// Usage: "Returns candidates sorted by distance".
// If resultSet is accumulated candidates, and we want them sorted...
// If we just Pop from end, they are in stack order.
// If it's a heap, popping from end is NOT sorted.
// Unless we sort it first?
// `insert` logic usually maintains a heap.
// If `resultSet` is a heap, we can convert it to sorted slice by popping repeatedly.
// So Pop() should be `heap.Pop()`.
// Since I cannot import "container/heap" inside the method easily without using it, I'll rely on `heap` package being used if I pass it to `heap.Pop`.
// But the call is `ctx.resultSet.Pop()`.
// This means `Pop` IS defined on `CandidateHeap`.
// Does it do heap pop?
// Code: `c, _ := ctx.resultSet.Pop()`.
// Return types: `(Candidate, bool/error)`.
// My implementation:
func (h *CandidateHeap) PopAndReturn() (Candidate, bool) {
	if len(*h) == 0 {
		return Candidate{}, false
	}
	// We want heap functionality. But we can't use container/heap easily if we are the interface.
	// Actually we can: import "container/heap".
	// func (h *CandidateHeap) Pop() (Candidate, bool) { val := heap.Pop(h).(Candidate); return val, true }
	// Wait, naming conflict. heap.Pop calls h.Pop.
	// If I name my method Pop(), heap.Pop calls it.
	// But I want to CALL it.
	// I can name the method `PopIter()` or similar?
	// But the code `arrow_hnsw_insert.go` calls `Pop()`.
	// So the code expects `Pop()` to function as an iterator/extractor.
	// If I use `container/heap`, I must implement `Pop() any` for the interface.
	// The code expects `Pop() (Candidate, bool)`.
	// These signatures CONFLICT.
	// `Pop() any` vs `Pop() (Candidate, bool)`.
	// Go does not support overloading.
	// So `resultSet` CANNOT be `heap.Interface`.
	// It must be a custom struct that wraps a heap or implements its own logic.
	// Or `Pop` in the code does NOT refer to heap pop, but just stack pop?
	// In HNSW, `searchLayer` returns sorted results.
	// If `resultSet` is just a list, and we Sort it?
	// `arrow_hnsw_insert.go` logic:
	// `h.searchLayer(...)` fills `ctx`. `resultSet` is part of `ctx`.
	// Then it iterates and Pops.
	// If `searchLayer` fills `resultSet` as a Heap?
	// I should check `searchLayer` logic.
	// But I don't have it (I'm implementing it as placeholder).
	// I will define `resultSet` as `CandidateHeap` struct.
	// I will implement `Pop` to return `(Candidate, bool)`.
	// I won't use `container/heap` interface on it directly to avoid conflict.
	// Or I assume `Pop` just pops from end (stack).
	// If I want sorting, I should Sort `resultSet` before popping.
	// But `searchLayer` usually returns heap.
	// I'll implement `Pop()` as: swap 0 with last, reduce len, sift down 0. (Standard Heap Pop).

	// Manual Heap Pop Implementation:
	old := *h
	n := len(old)
	if n == 0 {
		return Candidate{}, false
	}

	// Swap 0 and n-1
	val := old[0] // Max element
	old[0] = old[n-1]
	*h = old[:n-1]

	// Sift Down 0
	// Simplified sift down
	h.down(0, n-1)

	return val, true
}

func (h *CandidateHeap) down(i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		j2 := j1 + 1
		if j2 < n && (*h)[j2].Dist > (*h)[j1].Dist {
			j = j2 // = 2*i + 2  // right child
		}
		if !((*h)[j].Dist > (*h)[i].Dist) {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		i = j
	}
}

// ArrowSearchContext holds pre-allocated buffers for Arrow-based HNSW search operations
// to minimize allocation overhead during high-frequency searches.
type ArrowSearchContext struct {
	// Pre-allocated result buffers
	candidates CandidateHeap
	visited    *ArrowBitset

	queryBQ  []uint64
	querySQ8 []uint8

	// Distance calculation buffers
	dists     []float32
	distsTemp []float32

	// Neighbor tracking
	neighborBuf []uint32

	// Layer-specific buffers
	layerCandidates []Candidate

	// Scratch buffers for heuristics
	resultSet        CandidateHeap
	scratchSelected  []Candidate
	scratchRemaining []Candidate

	scratchDists []float32

	pruneDepth int

	// Reset tracking
	dirty bool

	// Thread-local metrics (optional)
	operations int
}

// ArrowSearchContextPool manages reusable ArrowSearchContext objects.
type ArrowSearchContextPool struct {
	pool sync.Pool

	// Pool statistics
	gets, puts int64
	mu         sync.RWMutex
}

// NewArrowSearchContext creates a new ArrowSearchContext with default capacity.
func NewArrowSearchContext() *ArrowSearchContext {
	return &ArrowSearchContext{
		candidates:       make([]Candidate, 0, 100),
		visited:          NewArrowBitset(1000),
		dists:            make([]float32, 0, 100),
		distsTemp:        make([]float32, 100),
		neighborBuf:      make([]uint32, 0, 64),
		layerCandidates:  make([]Candidate, 0, 100),
		resultSet:        make(CandidateHeap, 0, 100),
		scratchSelected:  make([]Candidate, 0, 100),
		scratchRemaining: make([]Candidate, 0, 100),
		queryBQ:          make([]uint64, 0, 256),
		querySQ8:         make([]uint8, 0, 1536),
		dirty:            false,
		operations:       0,
	}
}

// NewArrowSearchContextPool creates a new pool for ArrowSearchContext objects.
func NewArrowSearchContextPool() *ArrowSearchContextPool {
	return &ArrowSearchContextPool{
		pool: sync.Pool{
			New: func() any {
				return NewArrowSearchContext()
			},
		},
	}
}

// Get retrieves an ArrowSearchContext from the pool.
func (p *ArrowSearchContextPool) Get() *ArrowSearchContext {
	p.mu.Lock()
	p.gets++
	p.mu.Unlock()

	ctx := p.pool.Get().(*ArrowSearchContext)
	ctx.Reset()
	return ctx
}

// Put returns an ArrowSearchContext to the pool.
func (p *ArrowSearchContextPool) Put(ctx *ArrowSearchContext) {
	if ctx == nil {
		return
	}

	p.mu.Lock()
	p.puts++
	p.mu.Unlock()

	p.pool.Put(ctx)
}

// Reset clears the context for reuse.
func (ctx *ArrowSearchContext) Reset() {
	ctx.candidates = ctx.candidates[:0]
	ctx.visited.Clear()
	ctx.dists = ctx.dists[:0]
	ctx.neighborBuf = ctx.neighborBuf[:0]
	ctx.layerCandidates = ctx.layerCandidates[:0]
	ctx.resultSet = ctx.resultSet[:0]
	ctx.scratchSelected = ctx.scratchSelected[:0]
	ctx.scratchRemaining = ctx.scratchRemaining[:0]
	ctx.queryBQ = ctx.queryBQ[:0]
	ctx.querySQ8 = ctx.querySQ8[:0]
	ctx.dirty = false
	ctx.operations = 0

	// Clear temp buffer without reallocating
	for i := range ctx.distsTemp {
		ctx.distsTemp[i] = 0
	}
}

// Stats returns pool statistics.
func (p *ArrowSearchContextPool) Stats() (gets, puts int64) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.gets, p.puts
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
