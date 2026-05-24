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

func (d *Dataset) GetNeighborsBulk(ctx context.Context, dataset string, nodeIDs []uint32) (map[uint32][]uint32, error) {
	res := make(map[uint32][]uint32, len(nodeIDs))
	for _, id := range nodeIDs {
		n, _ := d.Index.GetRawNeighbors(id)
		res[id] = n
	}
	return res, nil
}

func TestReproGraphRAGSearchRace(t *testing.T) {
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
	ds.Index = idx

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

	// Setup some graph edges (GraphRAG requires a graph)
	for i := 0; i < 1000; i++ {
		src := uint32(rand.Intn(numVectors))
		dst := uint32(rand.Intn(numVectors))
		ds.Graph.AddEdge(Edge{
			Subject: types.VectorID(src),
			Object:  types.VectorID(dst),
			Weight:  rand.Float32(),
		})
	}

	// Concurrent GraphRAG search
	numConcurrent := 20
	var wg sync.WaitGroup
	wg.Add(numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				query := make([]float32, dims)
				for k := range query {
					query[k] = rand.Float32()
				}

				// Get initial results as seeds
				results, _ := ds.SearchDataset(ctx, query, 10)

				// RankWithGraphDistributed
				_ = ds.Graph.RankWithGraphDistributed(ctx, "test", query, results, 0.5, 2, ds)
			}
		}()
	}

	wg.Wait()
}
