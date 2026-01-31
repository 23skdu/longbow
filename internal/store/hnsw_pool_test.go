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

// TestHNSW_ScratchPoolBasic verifies pooled buffers return correct data
func TestHNSW_ScratchPoolBasic(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0},
	}
	rec := makeBatchTestRecord(mem, 4, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	// Add vectors
	for i := 0; i < 3; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Verify getVector returns correct data (via SearchVectors using a known vector)
	// We want to check if data is retrieved correctly.
	// We'll search for vector 0.
	queryVec := vectors[0]
	result, err := idx.SearchVectors(context.Background(), queryVec, 1, nil, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, result)
	assert.Equal(t, VectorID(0), result[0].ID)

	queryVec1 := vectors[1]
	result, err = idx.SearchVectors(context.Background(), queryVec1, 1, nil, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, result)
	assert.Equal(t, VectorID(1), result[0].ID)
}

// TestHNSW_ScratchPoolConcurrent verifies pool is safe under concurrent access
func TestHNSW_ScratchPoolConcurrent(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	numVectors := 100

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = float32(i*dims + j)
		}
	}
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	for i := 0; i < numVectors; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	// Concurrent searches - should not panic or corrupt data
	var wg sync.WaitGroup
	numGoroutines := 20
	searchesPerGoroutine := 50

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for s := 0; s < searchesPerGoroutine; s++ {
				// Pick a random vector from our set to query
				idInt := (goroutineID*searchesPerGoroutine + s) % numVectors
				queryVec := vectors[idInt]
				result, err := idx.SearchVectors(context.Background(), queryVec, 5, nil, SearchOptions{})
				if result == nil || err != nil {
					t.Errorf("unexpected error/nil result for ID %d: %v", idInt, err)
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestHNSW_ScratchPoolDifferentDimensions verifies pool handles various dimensions
func TestHNSW_ScratchPoolDifferentDimensions(t *testing.T) {
	testCases := []struct {
		name string
		dims int
	}{
		{"small_4", 4},
		{"medium_128", 128},
		{"large_768", 768},
		{"xlarge_1536", 1536},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewGoAllocator()
			vectors := make([][]float32, 5)
			for i := 0; i < 5; i++ {
				vectors[i] = make([]float32, tc.dims)
				for j := 0; j < tc.dims; j++ {
					vectors[i][j] = float32(i) + float32(j)*0.001
				}
			}
			rec := makeBatchTestRecord(mem, tc.dims, vectors)
			defer rec.Release()

			ds := &Dataset{
				Records: []arrow.RecordBatch{rec},
			}
			idx := NewTestHNSWIndex(ds)

			for i := 0; i < 5; i++ {
				_, err := idx.AddByLocation(context.Background(), 0, i)
				require.NoError(t, err)
			}

			queryVec := vectors[0]
			result, err := idx.SearchVectors(context.Background(), queryVec, 3, nil, SearchOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, result)
			assert.Equal(t, VectorID(0), result[0].ID, "first result should be self")
		})
	}
}

// BenchmarkHNSW_ScratchPoolAllocs measures allocations during search
func BenchmarkHNSW_ScratchPoolAllocs(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 768
	numVectors := 1000

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = float32(i*dims + j)
		}
	}
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	for i := 0; i < numVectors; i++ {
		_, _ = idx.AddByLocation(context.Background(), 0, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Mock search with vector retrieval overhead included?
		// We'll just search using a vector from the set.
		queryVec := vectors[i%numVectors]
		_, _ = idx.SearchVectors(context.Background(), queryVec, 10, nil, SearchOptions{})
	}
}

// BenchmarkHNSW_ScratchPoolAllocsParallel measures allocations under contention
func BenchmarkHNSW_ScratchPoolAllocsParallel(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 768
	numVectors := 1000

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = float32(i*dims + j)
		}
	}
	rec := makeBatchTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewTestHNSWIndex(ds)

	for i := 0; i < numVectors; i++ {
		_, _ = idx.AddByLocation(context.Background(), 0, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			queryVec := vectors[i%numVectors]
			_, _ = idx.SearchVectors(context.Background(), queryVec, 10, nil, SearchOptions{})
			i++
		}
	})
}
