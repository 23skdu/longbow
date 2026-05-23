package store

import (
	"sync"
)

// GraphSearchContext provides pooled buffers for GraphRAG search operations
// to eliminate GC pressure during high-frequency graph expansions.
type GraphSearchContext struct {
	scores        []float32
	visited       []uint64
	currentNodes  []uint32
	nextNodes     []uint32
	allInfluenced []uint32

	// results stores the intermediate SearchResult slice to avoid re-allocation
	results []SearchResult

	// distCache provides a thread-local cache for distance calculations
	// during expansion to avoid redundant work for hub nodes.
	distCache map[uint32]float32
}

// EnsureCapacity ensures the scores and visited buffers can accommodate the given ID.
func (ctx *GraphSearchContext) EnsureCapacity(id uint32) {
	idx := int(id)
	if idx < len(ctx.scores) {
		return
	}

	newLen := idx + 16384
	if newLen < len(ctx.scores)*2 {
		newLen = len(ctx.scores) * 2
	}

	// Grow scores
	if newLen <= cap(ctx.scores) {
		oldLen := len(ctx.scores)
		ctx.scores = ctx.scores[:newLen]
		for i := oldLen; i < newLen; i++ {
			ctx.scores[i] = 0
		}
	} else {
		newScores := make([]float32, newLen)
		copy(newScores, ctx.scores)
		ctx.scores = newScores
	}

	// Grow visited
	newVisitedLen := (newLen + 63) / 64
	if newVisitedLen <= cap(ctx.visited) {
		oldVisitedLen := len(ctx.visited)
		ctx.visited = ctx.visited[:newVisitedLen]
		for i := oldVisitedLen; i < newVisitedLen; i++ {
			ctx.visited[i] = 0
		}
	} else {
		newVisited := make([]uint64, newVisitedLen)
		copy(newVisited, ctx.visited)
		ctx.visited = newVisited
	}
}

// SetScore safely sets the score for a node, growing the buffer if needed.
func (ctx *GraphSearchContext) SetScore(id uint32, score float32) {
	ctx.EnsureCapacity(id)
	ctx.scores[id] = score
}

// GetScore safely gets the score for a node.
func (ctx *GraphSearchContext) GetScore(id uint32) float32 {
	if int(id) >= len(ctx.scores) {
		return 0
	}
	return ctx.scores[id]
}

// AddScore safely adds to the score of a node.
func (ctx *GraphSearchContext) AddScore(id uint32, delta float32) {
	ctx.EnsureCapacity(id)
	ctx.scores[id] += delta
}

// SetVisited safely marks a node as visited.
func (ctx *GraphSearchContext) SetVisited(id uint32) {
	ctx.EnsureCapacity(id)
	ctx.visited[id>>6] |= 1 << (id & 63)
}

// IsVisited safely checks if a node was visited.
func (ctx *GraphSearchContext) IsVisited(id uint32) bool {
	if int(id) >= len(ctx.scores) {
		return false
	}
	return (ctx.visited[id>>6] & (1 << (id & 63))) != 0
}

// Reset clears the context for reuse.
func (ctx *GraphSearchContext) Reset(maxID int, initialCount int) {
	requiredLen := maxID + 8192
	if requiredLen < 65536 {
		requiredLen = 65536
	}
	if cap(ctx.scores) < requiredLen {
		ctx.scores = make([]float32, requiredLen)
	} else {
		ctx.scores = ctx.scores[:requiredLen]
		for i := range ctx.scores {
			ctx.scores[i] = 0
		}
	}

	requiredVisited := (requiredLen + 63) / 64
	if cap(ctx.visited) < requiredVisited {
		ctx.visited = make([]uint64, requiredVisited)
	} else {
		ctx.visited = ctx.visited[:requiredVisited]
		for i := range ctx.visited {
			ctx.visited[i] = 0
		}
	}

	if cap(ctx.currentNodes) < initialCount {
		ctx.currentNodes = make([]uint32, 0, initialCount)
	} else {
		ctx.currentNodes = ctx.currentNodes[:0]
	}

	if cap(ctx.nextNodes) < initialCount*2 {
		ctx.nextNodes = make([]uint32, 0, initialCount*2)
	} else {
		ctx.nextNodes = ctx.nextNodes[:0]
	}

	if cap(ctx.allInfluenced) < initialCount*4 {
		ctx.allInfluenced = make([]uint32, 0, initialCount*4)
	} else {
		ctx.allInfluenced = ctx.allInfluenced[:0]
	}

	if cap(ctx.results) < initialCount*2 {
		ctx.results = make([]SearchResult, 0, initialCount*2)
	} else {
		ctx.results = ctx.results[:0]
	}

	if ctx.distCache == nil {
		ctx.distCache = make(map[uint32]float32, 1024)
	} else {
		for k := range ctx.distCache {
			delete(ctx.distCache, k)
		}
	}
}

var graphSearchPool = sync.Pool{
	New: func() any {
		return &GraphSearchContext{
			scores:        make([]float32, 0, 10000),
			visited:       make([]uint64, 0, 10000/64),
			currentNodes:  make([]uint32, 0, 1000),
			nextNodes:     make([]uint32, 0, 2000),
			allInfluenced: make([]uint32, 0, 4000),
			results:       make([]SearchResult, 0, 2000),
			distCache:     make(map[uint32]float32, 1024),
		}
	},
}

func getGraphSearchContext(maxID int, initialCount int) *GraphSearchContext {
	ctx := graphSearchPool.Get().(*GraphSearchContext)
	ctx.Reset(maxID, initialCount)
	return ctx
}

func putGraphSearchContext(ctx *GraphSearchContext) {
	graphSearchPool.Put(ctx)
}
