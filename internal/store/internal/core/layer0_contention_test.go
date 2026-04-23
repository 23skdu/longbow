package core

import (
	"fmt"
	"sync"
	"testing"
	"github.com/23skdu/longbow/internal/store/types"
)

func BenchmarkLayer0Contention(b *testing.B) {
	config := types.DefaultArrowHNSWConfig()
	config.Dims = 128
	config.InitialCapacity = 1000
	
	idx := NewArrowHNSW(nil, &config, nil)
	data := idx.data.Load()
	
	// Pre-allocate node 0
	_ = data.EnsureChunk(0, 0, 128)
	
	ctx := idx.searchPool.Get()
	defer idx.searchPool.Put(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		target := uint32(0)
		source := uint32(1)
		for pb.Next() {
			// Repeatedly try to add connection (duplicate check will skip most)
			data = idx.AddConnection(ctx, data, target, source, 0, config.MMax0, 0.1)
			source++
		}
	})
}

func TestConcurrentLayer0Adds(t *testing.T) {
	config := types.DefaultArrowHNSWConfig()
	config.MMax0 = 64
	config.PackedAdjacencyEnabled = true
	config.LockFreeThreshold = 2
	config.Dims = 128
	idx := NewArrowHNSW(nil, &config, nil)
	data := idx.data.Load()
	_ = data.EnsureChunk(0, 0, 128)
	
	ctxs := make([]*ArrowSearchContext, 10)
	for i := range ctxs {
		ctxs[i] = idx.searchPool.Get()
	}

	target := uint32(0)
	var wg sync.WaitGroup
	numThreads := 10
	addsPerThread := 100
	
	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < addsPerThread; j++ {
				// Re-load data to handle potential growth/swaps
				d := idx.data.Load()
				src := uint32(100 + id*addsPerThread + j)
				idx.data.Load().SetVector(src, []float32{float32(src)})
				dist := float32(0.1) + float32(j)*0.001
				d = idx.AddConnection(ctxs[id], d, target, src, 0, config.MMax0, dist)
			}
		}(i)
	}
	
	wg.Wait()
	
	neighbors := idx.GetNeighborsCombined(0, target)
	fmt.Printf("Final neighbor count for node 0: %d (Max: %d)\n", len(neighbors), config.MMax0)
	if len(neighbors) > config.MMax0 {
		t.Errorf("Neighbor count %d exceeds MMax0 %d", len(neighbors), config.MMax0)
	}
}
