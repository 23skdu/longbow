package store

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSW_SearchPadding(t *testing.T) {
	mem := memory.NewGoAllocator()

	// Create vectors (dim 128)
	dims := 128
	count := 10
	vectors := make([][]float32, count)
	for i := 0; i < count; i++ {
		v := make([]float32, dims)
		for j := 0; j < dims; j++ {
			v[j] = float32(i)
		}
		vectors[i] = v
	}

	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "padding_test",
		Records: []arrow.RecordBatch{rec},
	}

	cfg := DefaultArrowHNSWConfig()
	cfg.M = 16
	cfg.EfConstruction = 64
	idx := NewArrowHNSW(ds, &cfg)

	for i := 0; i < count; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Search
	query := make([]float32, dims)
	k := 5
	// Search signature: (ctx, query, k, filter)
	distances, err := idx.Search(context.Background(), query, k, nil)
	require.NoError(t, err)
	assert.Len(t, distances, k)
}
