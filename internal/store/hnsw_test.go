package store

import (
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeHNSWTestRecord creates a test record with configurable dimensions
func makeHNSWTestRecord(mem memory.Allocator, dims int, vectors [][]float32) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	idBuilder := array.NewInt64Builder(mem)
	listBuilder := array.NewFixedSizeListBuilder(mem, int32(dims), arrow.PrimitiveTypes.Float32)
	vecBuilder := listBuilder.ValueBuilder().(*array.Float32Builder)

	for i, vec := range vectors {
		idBuilder.Append(int64(i))
		listBuilder.Append(true)
		for _, v := range vec {
			vecBuilder.Append(v)
		}
	}

	return array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, int64(len(vectors)))
}

// TestHNSW_EmptyIndex verifies search on empty index returns nil
func TestHNSW_EmptyIndex(t *testing.T) {
	ds := &Dataset{}
	idx := NewHNSWIndex(ds)

	result := idx.SearchByID(0, 10)
	assert.Nil(t, result, "search on empty index should return nil")

	result = idx.SearchByID(999, 5)
	assert.Nil(t, result, "search for non-existent ID should return nil")
}

// TestHNSW_SingleVector verifies single-vector index behavior
func TestHNSW_SingleVector(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := [][]float32{{1.0, 2.0, 3.0, 4.0}}
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
		mu:      sync.RWMutex{},
	}
	idx := NewHNSWIndex(ds)

	// Add the single vector
	err := idx.Add(0, 0)
	require.NoError(t, err)

	// Search should return self
	result := idx.SearchByID(0, 1)
	require.NotNil(t, result)
	assert.Len(t, result, 1)
	assert.Equal(t, VectorID(0), result[0])

	// Search with k > 1 should still return only 1
	result = idx.SearchByID(0, 10)
	require.NotNil(t, result)
	assert.Len(t, result, 1)
}

// TestHNSW_KGreaterThanTotal verifies behavior when k > number of vectors
func TestHNSW_KGreaterThanTotal(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Create 3 vectors
	vectors := [][]float32{
		{1.0, 0.0, 0.0, 0.0},
		{0.0, 1.0, 0.0, 0.0},
		{0.0, 0.0, 1.0, 0.0},
	}
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
		mu:      sync.RWMutex{},
	}
	idx := NewHNSWIndex(ds)

	// Add all vectors
	for i := 0; i < 3; i++ {
		require.NoError(t, idx.Add(0, i))
	}

	// Search with k=10 should return at most 3
	result := idx.SearchByID(0, 10)
	require.NotNil(t, result)
	assert.LessOrEqual(t, len(result), 3, "should return at most 3 results")
	assert.GreaterOrEqual(t, len(result), 1, "should return at least 1 result")
}

// TestHNSW_DuplicateVectors verifies handling of identical vectors
func TestHNSW_DuplicateVectors(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Create identical vectors
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{1.0, 2.0, 3.0, 4.0}, // duplicate
		{1.0, 2.0, 3.0, 4.0}, // duplicate
	}
	rec := makeHNSWTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
		mu:      sync.RWMutex{},
	}
	idx := NewHNSWIndex(ds)

	// Add all duplicate vectors
	for i := 0; i < 3; i++ {
		require.NoError(t, idx.Add(0, i))
	}

	// All should be searchable
	result := idx.SearchByID(0, 3)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result), 1)

	// Each duplicate should find itself
	result = idx.SearchByID(1, 1)
	require.NotNil(t, result)
	assert.Contains(t, []VectorID{0, 1, 2}, result[0], "should return a valid duplicate ID")
}

// TestHNSW_HighDimensionalVectors verifies handling of 4096+ dimensions
func TestHNSW_HighDimensionalVectors(t *testing.T) {
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
	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
		mu:      sync.RWMutex{},
	}
	idx := NewHNSWIndex(ds)

	// Add all vectors
	for i := 0; i < 3; i++ {
		require.NoError(t, idx.Add(0, i))
	}

	// Search should work with high dimensions
	result := idx.SearchByID(0, 2)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result), 1)

	// First result should be self (closest)
	assert.Equal(t, VectorID(0), result[0])
}

// TestHNSW_ConcurrentAdd verifies thread-safety of the Add method
func TestHNSW_ConcurrentAdd(t *testing.T) {
	mem := memory.NewGoAllocator()
	numVectors := 100
	dims := 4

	// Create a large record with 100 vectors
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = []float32{float32(i), 0, 0, 0}
	}
	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "concurrent_test",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

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
				err := idx.Add(0, i)
				require.NoError(t, err)
			}
		}(w)
	}

	wg.Wait()

	// Verify all vectors were added
	assert.Equal(t, numVectors, idx.Len())

	// Search should work
	result := idx.SearchByID(0, 5)
	assert.NotEmpty(t, result)
}
