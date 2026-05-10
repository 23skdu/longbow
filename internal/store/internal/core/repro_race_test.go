package core

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
)

func TestReproSearchRace(t *testing.T) {
	config := types.ArrowHNSWConfig{
		Dimension: 128,
		M:         16,
		EfConstruction: 100,
		DataType: types.VectorTypeFloat32,
	}
	
	h := NewArrowHNSWWithConfig(nil, config, nil)
	
	// Mock some graph data manually since we don't have a dataset
	data := types.NewGraphData(1000, 128, types.VectorTypeFloat32, 16, 0)
	h.compareAndSwapData(data)

	// Insert some data
	ctx := context.Background()
	numNodes := 1000
	for i := 0; i < numNodes; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		// Manually add to data since we are bypassing the dataset
		data.AddVector(uint32(i), vec)
		h.nodeCount.Store(uint32(i + 1))
	}
	
	// Concurrent search
	numConcurrent := 20
	var wg sync.WaitGroup
	wg.Add(numConcurrent)
	
	for i := 0; i < numConcurrent; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				query := make([]float32, 128)
				for k := range query {
					query[k] = rand.Float32()
				}
				// Use a dummy entry point
				_, _ = h.SearchVectors(ctx, query, 10, nil, types.SearchOptions{Ef: 100})
			}
		}()
	}
	
	wg.Wait()
}
