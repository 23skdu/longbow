package store

import (
	"sync"
	"sync/atomic"
)

// InsertContext holds temporary state for a single insert operation.
// Pooling these contexts eliminates allocations during graph construction.
type InsertContext struct {
	// Candidate lists for neighbor search at each layer
	candidates [][]Candidate

	// Visited tracking
	visited *ArrowBitset

	// Temporary neighbor lists
	neighbors []Candidate
}

// InsertContextPool manages reusable insert contexts.
type InsertContextPool struct {
	pool sync.Pool

	// Metrics
	gets atomic.Int64
	puts atomic.Int64
}

// NewInsertContextPool creates a new insert context pool.
func NewInsertContextPool() *InsertContextPool {
	return &InsertContextPool{
		pool: sync.Pool{
			New: func() any {
				return &InsertContext{
					candidates: make([][]Candidate, ArrowMaxLayers),
					visited:    NewArrowBitset(10000),
					neighbors:  make([]Candidate, 0, 100),
				}
			},
		},
	}
}

// Get retrieves an insert context from the pool.
func (p *InsertContextPool) Get() *InsertContext {
	p.gets.Add(1)
	ctx := p.pool.Get().(*InsertContext)

	// Reset candidates
	for i := range ctx.candidates {
		ctx.candidates[i] = ctx.candidates[i][:0]
	}

	// Reset visited
	ctx.visited.Clear()

	// Reset neighbors
	ctx.neighbors = ctx.neighbors[:0]

	return ctx
}

// Put returns an insert context to the pool.
func (p *InsertContextPool) Put(ctx *InsertContext) {
	if ctx == nil {
		return
	}
	p.puts.Add(1)
	p.pool.Put(ctx)
}

// Stats returns pool statistics.
func (p *InsertContextPool) Stats() (gets, puts int64) {
	return p.gets.Load(), p.puts.Load()
}
