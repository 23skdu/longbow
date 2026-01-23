package store

import (
	"context"
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowHNSW_SearchEarlyTermination(t *testing.T) {
	mem := memory.NewGoAllocator()

	dim := 32
	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range dim {
			vectors[i][j] = float32(i) + float32(j)*0.01
		}
	}

	rec := makeHNSWTestRecord(mem, dim, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_search_early",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	config.M = 16
	config.EfConstruction = 40

	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	for i := 0; i < 100; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	query := make([]float32, dim)
	for j := range dim {
		query[j] = 50.0 + float32(j)*0.01
	}

	results, err := idx.Search(context.Background(), query, 5, 10, nil)
	require.NoError(t, err)
	require.Len(t, results, 5)

	for _, r := range results {
		assert.GreaterOrEqual(t, r.Score, float32(0))
	}
}

func TestArrowHNSW_SearchWithEfGreaterThanResults(t *testing.T) {
	mem := memory.NewGoAllocator()

	dim := 16
	vectors := make([][]float32, 50)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range dim {
			vectors[i][j] = float32(i * 2)
		}
	}

	rec := makeHNSWTestRecord(mem, dim, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_ef_large",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	config.M = 8
	config.EfConstruction = 40

	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	for i := 0; i < 50; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	query := make([]float32, dim)
	for j := range dim {
		query[j] = 25.0
	}

	results, err := idx.Search(context.Background(), query, 10, 100, nil)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(results), 10)
}

func TestArrowHNSW_NeedsCompaction_Empty(t *testing.T) {
	mem := memory.NewGoAllocator()

	vectors := [][]float32{{1.0, 2.0}}
	rec := makeHNSWTestRecord(mem, 2, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_empty",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	assert.False(t, idx.NeedsCompaction())
}

func TestArrowHNSW_NeedsCompaction_NoDeleted(t *testing.T) {
	mem := memory.NewGoAllocator()

	dim := 16
	vectors := make([][]float32, 10)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range dim {
			vectors[i][j] = float32(i)
		}
	}

	rec := makeHNSWTestRecord(mem, dim, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_no_deleted",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	for i := 0; i < 10; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	assert.False(t, idx.NeedsCompaction())
}

func TestArrowHNSW_VisitedListGrowth(t *testing.T) {
	mem := memory.NewGoAllocator()

	dim := 16
	vectors := make([][]float32, 1000)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range dim {
			vectors[i][j] = float32(i % 100)
		}
	}

	rec := makeHNSWTestRecord(mem, dim, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_visited",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	config.M = 8
	config.EfConstruction = 40

	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	for i := 0; i < 1000; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	query := make([]float32, dim)
	for j := range dim {
		query[j] = 50.0
	}

	results, err := idx.Search(context.Background(), query, 5, 50, nil)
	require.NoError(t, err)
	assert.Len(t, results, 5)
}

func TestArrowHNSW_SearchEmptyIndex(t *testing.T) {
	mem := memory.NewGoAllocator()

	vectors := [][]float32{{1.0, 2.0}}
	rec := makeHNSWTestRecord(mem, 2, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_empty_index",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	query := []float32{1.0, 2.0}

	results, err := idx.Search(context.Background(), query, 5, 10, nil)
	require.NoError(t, err)
	assert.Len(t, results, 0)
}

func TestArrowHNSW_SearchSingleVector(t *testing.T) {
	mem := memory.NewGoAllocator()

	dim := 8
	vectors := [][]float32{
		{1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0},
	}

	rec := makeHNSWTestRecord(mem, dim, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_single",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	_, err := idx.AddByLocation(context.Background(), 0, 0)
	require.NoError(t, err)

	query := make([]float32, dim)
	query[0] = 1.0

	results, err := idx.Search(context.Background(), query, 1, 10, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, core.VectorID(0), results[0].ID)
	assert.InDelta(t, 0.0, results[0].Score, 0.001)
}

func TestArrowHNSW_EstimateMemory(t *testing.T) {
	mem := memory.NewGoAllocator()

	vectors := [][]float32{{1.0, 2.0}}
	rec := makeHNSWTestRecord(mem, 2, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_mem",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	memBefore := idx.EstimateMemory()

	dim := 16
	vectors = make([][]float32, 10)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range dim {
			vectors[i][j] = float32(i)
		}
	}

	rec = makeHNSWTestRecord(mem, dim, vectors)
	defer rec.Release()

	ds2 := &Dataset{
		Name:    "test_mem2",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	idx2 := NewArrowHNSW(ds2, config, NewChunkedLocationStore())

	for i := 0; i < 10; i++ {
		_, err := idx2.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	memAfter := idx2.EstimateMemory()
	assert.Greater(t, memAfter, memBefore)
}

func TestArrowHNSW_SearchVectors_Empty(t *testing.T) {
	mem := memory.NewGoAllocator()

	vectors := [][]float32{{1.0, 2.0}}
	rec := makeHNSWTestRecord(mem, 2, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test_hnsw_empty",
		Records: []arrow.RecordBatch{rec},
		Schema:  rec.Schema(),
	}

	config := DefaultArrowHNSWConfig()
	hnswIdx := NewArrowHNSW(ds, config, NewChunkedLocationStore())

	query := []float32{1.0, 2.0}

	result, err := hnswIdx.SearchVectors(context.Background(), query, 5, nil, SearchOptions{})
	require.NoError(t, err)
	assert.Len(t, result, 0)
}
