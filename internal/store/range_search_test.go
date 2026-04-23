package store

import (
	"context"
	"math/rand"
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
)

func TestArrowHNSW_RangeSearch(t *testing.T) {
	dims := 128
	count := 100

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeFloat32
	config.M = 16
	config.EfConstruction = 64
	config.EfSearch = 64

	idx := NewArrowHNSW(nil, &config, nil)

	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	err := idx.AddBatchBulk(context.Background(), 0, count, vectors)
	if err != nil {
		t.Fatalf("failed to add vectors: %v", err)
	}

	ctx := context.Background()
	queryVec := vectors[0]

	results, err := idx.SearchVectorsInRange(ctx, queryVec, 0.5, nil, nil)
	if err != nil {
		t.Fatalf("range search failed: %v", err)
	}

	t.Logf("Range search found %d results within threshold 0.5", len(results))

	results, err = idx.SearchVectorsInRange(ctx, queryVec, 100.0, nil, nil)
	if err != nil {
		t.Fatalf("range search failed: %v", err)
	}

	if len(results) == 0 {
		t.Error("expected results with high threshold")
	}

	t.Logf("Range search found %d results within threshold 100.0", len(results))

	idx.Close()
}

func TestArrowHNSW_RangeSearch_Empty(t *testing.T) {
	dims := 128

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeFloat32

	idx := NewArrowHNSW(nil, &config, nil)

	ctx := context.Background()
	queryVec := make([]float32, dims)
	for i := range queryVec {
		queryVec[i] = rand.Float32()
	}

	results, err := idx.SearchVectorsInRange(ctx, queryVec, 50.0, nil, nil)
	if err != nil {
		t.Fatalf("range search failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected no results for empty index, got %d", len(results))
	}

	idx.Close()
}
