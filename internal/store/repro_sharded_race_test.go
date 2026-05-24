package store

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/store/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestReproShardedSearchRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	mem := memory.NewGoAllocator()
	numVectors := 1000
	dims := 128
	vectors := core.GenerateTestVectors(numVectors, dims)
	rec := core.MakeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		}, nil,
	)
	ds := NewDataset("test", schema)

	config := DefaultShardedHNSWConfig()
	config.Dimension = uint32(dims)
	config.NumShards = 4

	idx := NewShardedHNSW(config, ds)

	// Insert data
	ctx := context.Background()
	rowIdxs := make([]int, numVectors)
	batchIdxs := make([]int, numVectors)
	for i := 0; i < numVectors; i++ {
		rowIdxs[i] = i
		batchIdxs[i] = 0
	}

	_, err := idx.AddBatch(ctx, []arrow.RecordBatch{rec}, rowIdxs, batchIdxs)
	if err != nil {
		t.Fatalf("AddBatch failed: %v", err)
	}

	// Concurrent search
	numConcurrent := 50
	var wg sync.WaitGroup
	wg.Add(numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				query := make([]float32, dims)
				for k := range query {
					query[k] = rand.Float32()
				}
				_, _ = idx.SearchVectors(ctx, query, 10, nil, types.SearchOptions{Ef: 100})
			}
		}()
	}

	wg.Wait()
}
