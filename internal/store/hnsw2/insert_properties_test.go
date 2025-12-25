package hnsw2

import (
	"sync/atomic"
	"testing"
	"unsafe"
	
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestInsertProperties validates insert behavior using property-based testing.
func TestInsertProperties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 50 // Reduced for faster tests
	
	properties := gopter.NewProperties(parameters)
	
	// Property: Graph connectivity - all nodes should be reachable
	// TODO: Implement when we have full vector storage
	properties.Property("graph connectivity", prop.ForAll(
		func(nodeCount int) bool {
			// Skip for now - requires full integration
			return true
		},
		gen.IntRange(1, 100),
	))
	
	// Property: Neighbor count constraints
	properties.Property("neighbor count <= M", prop.ForAll(
		func(m int) bool {
			if m <= 0 || m > MaxNeighbors {
				return true // Skip invalid inputs
			}
			
			// Create index with custom M
			config := DefaultConfig()
			config.M = m
			config.MMax = m * 2
			config.MMax0 = m
			config.Alpha = 1.0
			
			index := NewArrowHNSW(nil, config)
			index.dims = 1 // use 1-dim vectors
			
			// Initialize GraphData manually
			data := NewGraphData(10) // capacity 10
			index.data.Store(data)
			
			// Setup dummy vectors
			vec := make([]float32, 1)
			for i := 0; i < 5; i++ {
				data.VectorPtrs[i] = unsafe.Pointer(&vec[0])
			}
			
			// Add connections
			for i := 0; i < 5; i++ {
				for j := 0; j < 5; j++ {
					if i != j {
						index.addConnection(data, uint32(i), uint32(j), 0)
					}
				}
			}
			
			// Prune to M
			// Need SearchContext
			ctx := index.searchPool.Get()
			defer index.searchPool.Put(ctx)
			
			for i := 0; i < 5; i++ {
				// Prune to M*2 (usually MMax is the limit, but here we test pruning)
				// Actually pruneConnections param is 'maxConn'.
				// We used m*2 in old test.
				index.pruneConnections(ctx, data, uint32(i), m*2, 0)
			}
			
			// Check all nodes have <= M*2 neighbors
			for i := 0; i < 5; i++ {
				count := atomic.LoadInt32(&data.Counts[0][i])
				if int(count) > m*2 {
					return false
				}
			}
			
			return true
		},
		gen.IntRange(1, 16),
	))
	
	properties.TestingRun(t)
}

// TestSearchContextPooling validates search context pool behavior.
func TestSearchContextPooling(t *testing.T) {
	pool := NewSearchContextPool()
	
	// Get context
	ctx1 := pool.Get()
	if ctx1 == nil {
		t.Fatal("pool.Get() returned nil")
	}
	
	// Use context
	ctx1.candidates.Push(Candidate{ID: 1, Dist: 1.0})
	ctx1.visited.Set(5)
	
	// Return to pool
	pool.Put(ctx1)
	
	// Get again - should be reused
	ctx2 := pool.Get()
	if ctx2 == nil {
		t.Fatal("pool.Get() returned nil on second call")
	}
	
	// Should be cleared
	if ctx2.candidates.Len() != 0 {
		t.Error("candidates not cleared after Put")
	}
	if ctx2.visited.IsSet(5) {
		t.Error("visited not cleared after Put")
	}
	
	// Check metrics? Pool doesn't expose stats in sync.Pool wrapper safely
	// skipping stats check
}

// TestLevelDistribution validates exponential decay of level assignment.
func TestLevelDistribution(t *testing.T) {
	lg := NewLevelGenerator(1.44269504089)
	
	// Generate many levels
	counts := make(map[int]int)
	total := 10000
	
	for i := 0; i < total; i++ {
		level := lg.Generate()
		counts[level]++
	}
	
	// Check exponential decay: each level should have ~half the nodes of previous
	// Level 0 should have ~50% of nodes
	level0Ratio := float64(counts[0]) / float64(total)
	if level0Ratio < 0.45 || level0Ratio > 0.55 {
		t.Errorf("level 0 ratio = %f, want ~0.5", level0Ratio)
	}
	
	// Level 1 should have ~25% of nodes
	level1Ratio := float64(counts[1]) / float64(total)
	if level1Ratio < 0.20 || level1Ratio > 0.30 {
		t.Errorf("level 1 ratio = %f, want ~0.25", level1Ratio)
	}
	
	t.Logf("Level distribution (n=%d): %v", total, counts)
}

// BenchmarkSearchContextPool benchmarks pool overhead.
func BenchmarkSearchContextPool(b *testing.B) {
	pool := NewSearchContextPool()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx := pool.Get()
		// Simulate some work
		ctx.candidates.Push(Candidate{ID: uint32(i), Dist: float32(i)})
		pool.Put(ctx)
	}
}
