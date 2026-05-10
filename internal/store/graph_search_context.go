package store

import (
	"sync"
)

// GraphSearchContext provides pooled buffers for GraphRAG search operations
// to eliminate GC pressure during high-frequency graph expansions.
type GraphSearchContext struct {
	scores       []float32
	visited      []uint64
	currentNodes []uint32
	nextNodes    []uint32
	allInfluenced []uint32
	
	// results stores the intermediate SearchResult slice to avoid re-allocation
	results      []SearchResult

	// distCache provides a thread-local cache for distance calculations
	// during expansion to avoid redundant work for hub nodes.
	distCache    map[uint32]float32
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
			scores:       make([]float32, 0, 10000),
			visited:      make([]uint64, 0, 10000/64),
			currentNodes: make([]uint32, 0, 1000),
			nextNodes:    make([]uint32, 0, 2000),
			allInfluenced: make([]uint32, 0, 4000),
			results:      make([]SearchResult, 0, 2000),
			distCache:    make(map[uint32]float32, 1024),
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
