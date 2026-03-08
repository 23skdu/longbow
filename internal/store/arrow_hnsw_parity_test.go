package store

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrowHNSW_EmptyIndex verifies search on empty index returns empty result
func TestArrowHNSW_EmptyIndex(t *testing.T) {
	ds := &Dataset{
		Name: "empty_test",
	}
	cfg := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, &cfg)

	q := []float32{0, 0, 0, 0}
	result, err := idx.Search(context.Background(), q, 10, nil)
	require.NoError(t, err)
	assert.Empty(t, result, "search on empty index should return empty")
}

// TestArrowHNSW_SingleVector verifies single-vector index behavior
func TestArrowHNSW_SingleVector(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 2.0, 3.0, 4.0}}
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "single_test",
		Records: []arrow.RecordBatch{rec},
	}
	cfg := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, &cfg)

	// Add the single vector
	_, err := idx.AddByLocation(context.Background(), 0, 0)
	require.NoError(t, err)

	// Search should return self
	q := []float32{1.0, 2.0, 3.0, 4.0}
	result, err := idx.Search(context.Background(), q, 1, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result, 1)
	assert.Equal(t, uint32(0), result[0].ID)

	// Search with k > 1 should still return only 1
	result, err = idx.Search(context.Background(), q, 10, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result, 1)
}

// TestArrowHNSW_KGreaterThanTotal verifies behavior when k > number of vectors
func TestArrowHNSW_KGreaterThanTotal(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Create 3 vectors
	vectors := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.0, 0.0, 1.0, 0.0},
	}
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "k_greater_test",
		Records: []arrow.RecordBatch{rec},
	}
	cfg := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, &cfg)

	// Add all vectors
	for i := 0; i < 3; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Search with k=10 should return at most 3
	q := []float32{1.0, 0.0, 0.0, 0.0}
	result, err := idx.Search(context.Background(), q, 10, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.LessOrEqual(t, len(result), 3, "should return at most 3 results")
	assert.GreaterOrEqual(t, len(result), 1, "should return at least 1 result")
}

// TestArrowHNSW_DuplicateVectors verifies handling of identical vectors
func TestArrowHNSW_DuplicateVectors(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Create identical vectors
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{1.0, 2.0, 3.0, 4.0}, // duplicate
		{1.0, 2.0, 3.0, 4.0}, // duplicate
	}
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "dup_test",
		Records: []arrow.RecordBatch{rec},
	}
	cfg := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, &cfg)

	// Add all duplicate vectors
	for i := 0; i < 3; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	q := []float32{1.0, 2.0, 3.0, 4.0}
	// All should be searchable
	result, err := idx.Search(context.Background(), q, 3, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result), 1)

	// Search for one
	result, err = idx.Search(context.Background(), q, 1, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check that we found one of them
	foundID := result[0].ID
	assert.Contains(t, []uint32{0, 1, 2}, foundID, "should return a valid duplicate ID")
}

// TestArrowHNSW_HighDimensionalVectors verifies handling of large dimensions
func TestArrowHNSW_HighDimensionalVectors(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 4096

	// Create 3 high-dimensional vectors
	vectors := make([][]float32, 3)
	for i := 0; i < 3; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = float32(i) + float32(j)*0.001
		}
	}
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "high_dim_test",
		Records: []arrow.RecordBatch{rec},
	}
	cfg := DefaultArrowHNSWConfig()
	cfg.Dims = dims
	idx := NewArrowHNSW(ds, &cfg)

	// Add all vectors
	for i := 0; i < 3; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Search should work with high dimensions
	q := vectors[0]
	result, err := idx.Search(context.Background(), q, 2, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result), 1)

	// First result should be self (closest)
	assert.Equal(t, uint32(0), result[0].ID)
}

// TestArrowHNSW_ConcurrentAdd verifies thread-safety of the Add method
func TestArrowHNSW_ConcurrentAdd(t *testing.T) {
	mem := memory.NewGoAllocator()
	numVectors := 100
	dims := 4

	// Create a large record with 100 vectors
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = []float32{float32(i), 0, 0, 0}
	}
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "concurrent_test",
		Records: []arrow.RecordBatch{rec},
	}
	cfg := DefaultArrowHNSWConfig()
	idx := NewArrowHNSW(ds, &cfg)

	// Add vectors concurrently from multiple goroutines
	var wg sync.WaitGroup
	numWorkers := 10
	vectorsPerWorker := numVectors / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * vectorsPerWorker
			for i := start; i < start+vectorsPerWorker; i++ {
				_, err := idx.AddByLocation(context.Background(), 0, i)
				require.NoError(t, err)
			}
		}(w)
	}

	wg.Wait()

	// Verify all vectors were added
	// ArrowHNSW doesn't have Len(), expose logic via nodeCount if needed or analyze graph
	// But we can check via search or AnalyzeGraph
	stats := idx.AnalyzeGraph()
	assert.Equal(t, numVectors, stats.TotalNodes)

	// Search should work
	q := []float32{0, 0, 0, 0}
	result, err := idx.Search(context.Background(), q, 5, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, result)
}
