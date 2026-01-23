package store

import (
	"context"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBruteForceIndex_ZeroCopyVectorAccess tests that BruteForceIndex can retrieve vectors
// without unnecessary copies, reducing memory allocations during search.
func TestBruteForceIndex_ZeroCopyVectorAccess(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	numVectors := 1000

	// Create test dataset
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idBuilder := b.Field(0).(*array.Int64Builder)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int64(i))
		vecBuilder.Append(true)
		for j := 0; j < dims; j++ {
			valBuilder.Append(float32(i*dims + j))
		}
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	ds := &Dataset{
		Name:    "zerocopy_test",
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewBruteForceIndex(ds)

	// Add vectors
	for i := 0; i < numVectors; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	t.Run("getVectorUnsafe_ReturnsDirectReference", func(t *testing.T) {
		// Get vector using zero-copy method
		vec, release := idx.getVectorUnsafe(Location{BatchIdx: 0, RowIdx: 0})
		require.NotNil(t, vec)
		require.NotNil(t, release)
		defer release()

		// Verify correctness
		assert.Equal(t, dims, len(vec))
		assert.Equal(t, float32(0), vec[0])

		// Get another vector
		vec2, release2 := idx.getVectorUnsafe(Location{BatchIdx: 0, RowIdx: 1})
		require.NotNil(t, vec2)
		defer release2()

		assert.Equal(t, float32(dims), vec2[0])
	})

	t.Run("getVectorUnsafe_NoAllocation", func(t *testing.T) {
		// Warm up
		for i := 0; i < 100; i++ {
			vec, release := idx.getVectorUnsafe(Location{BatchIdx: 0, RowIdx: i})
			if vec != nil && release != nil {
				release()
			}
		}

		runtime.GC()
		var m1, m2 runtime.MemStats
		runtime.ReadMemStats(&m1)

		// Access vectors without allocating
		for i := 0; i < 1000; i++ {
			vec, release := idx.getVectorUnsafe(Location{BatchIdx: 0, RowIdx: i % numVectors})
			if vec != nil && release != nil {
				_ = vec[0] // Use the vector
				release()
			}
		}

		runtime.ReadMemStats(&m2)

		// Should have minimal allocations (< 20KB for 1000 accesses, accounting for function call overhead)
		allocDelta := m2.TotalAlloc - m1.TotalAlloc
		t.Logf("Allocations for 1000 zero-copy accesses: %d bytes", allocDelta)
		assert.Less(t, allocDelta, uint64(20*1024), "Zero-copy should have minimal allocations")
	})

	t.Run("getVector_CreatesAllocations", func(t *testing.T) {
		// Warm up
		for i := 0; i < 100; i++ {
			_ = idx.getVector(Location{BatchIdx: 0, RowIdx: i})
		}

		runtime.GC()
		var m1, m2 runtime.MemStats
		runtime.ReadMemStats(&m1)

		// Access vectors with copy
		for i := 0; i < 1000; i++ {
			vec := idx.getVector(Location{BatchIdx: 0, RowIdx: i % numVectors})
			_ = vec[0] // Use the vector
		}

		runtime.ReadMemStats(&m2)

		// Should have significant allocations (>= 500KB for 1000 copies of 128-dim vectors)
		allocDelta := m2.TotalAlloc - m1.TotalAlloc
		expectedMin := uint64(1000 * dims * 4) // 1000 vectors * 128 dims * 4 bytes
		t.Logf("Allocations for 1000 copy accesses: %d bytes (expected >= %d)", allocDelta, expectedMin)
		assert.GreaterOrEqual(t, allocDelta, expectedMin, "Copy method should allocate memory")
	})
}

// TestBruteForceIndex_SearchWithZeroCopy tests that search operations can use zero-copy access
func TestBruteForceIndex_SearchWithZeroCopy(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 768 // Common embedding dimension
	numVectors := 500

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	idBuilder := b.Field(0).(*array.Int64Builder)
	vecBuilder := b.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int64(i))
		vecBuilder.Append(true)
		for j := 0; j < dims; j++ {
			valBuilder.Append(float32(i + j))
		}
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	ds := &Dataset{
		Name:    "search_zerocopy_test",
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewBruteForceIndex(ds)

	// Add vectors
	for i := 0; i < numVectors; i++ {
		_, err := idx.AddByLocation(context.Background(), 0, i)
		require.NoError(t, err)
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(i)
	}

	t.Run("SearchWithZeroCopy_ReducesAllocations", func(t *testing.T) {
		// Warm up
		_, _ = idx.SearchVectors(context.Background(), query, 10, nil, SearchOptions{})

		runtime.GC()
		var m1, m2 runtime.MemStats
		runtime.ReadMemStats(&m1)

		// Perform searches
		for i := 0; i < 100; i++ {
			results, err := idx.SearchVectors(context.Background(), query, 10, nil, SearchOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, results)
		}

		runtime.ReadMemStats(&m2)

		allocDelta := m2.TotalAlloc - m1.TotalAlloc
		t.Logf("Allocations for 100 searches over %d vectors: %d bytes", numVectors, allocDelta)

		// With zero-copy, allocations should be < 10MB for 100 searches
		// (mainly for result arrays, not vector copies)
		assert.Less(t, allocDelta, uint64(10*1024*1024), "Search with zero-copy should minimize allocations")
	})
}

// BenchmarkBruteForceIndex_VectorAccess benchmarks different vector access patterns
func BenchmarkBruteForceIndex_VectorAccess(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 768
	numVectors := 1000

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBuilder := bldr.Field(0).(*array.Int64Builder)
	vecBuilder := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int64(i))
		vecBuilder.Append(true)
		for j := 0; j < dims; j++ {
			valBuilder.Append(float32(i*dims + j))
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds := &Dataset{
		Name:    "bench_test",
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewBruteForceIndex(ds)
	for i := 0; i < numVectors; i++ {
		_, _ = idx.AddByLocation(context.Background(), 0, i)
	}

	b.Run("getVector_WithCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec := idx.getVector(Location{BatchIdx: 0, RowIdx: i % numVectors})
			_ = vec[0]
		}
	})

	b.Run("getVectorUnsafe_ZeroCopy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vec, release := idx.getVectorUnsafe(Location{BatchIdx: 0, RowIdx: i % numVectors})
			if vec != nil && release != nil {
				_ = vec[0]
				release()
			}
		}
	})
}

// BenchmarkBruteForceIndex_Search benchmarks search performance with different access patterns
func BenchmarkBruteForceIndex_Search(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 768
	numVectors := 5000

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	idBuilder := bldr.Field(0).(*array.Int64Builder)
	vecBuilder := bldr.Field(1).(*array.FixedSizeListBuilder)
	valBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int64(i))
		vecBuilder.Append(true)
		for j := 0; j < dims; j++ {
			valBuilder.Append(float32(i + j))
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	ds := &Dataset{
		Name:    "search_bench",
		Records: []arrow.RecordBatch{rec},
	}

	idx := NewBruteForceIndex(ds)
	for i := 0; i < numVectors; i++ {
		_, _ = idx.AddByLocation(context.Background(), 0, i)
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, _ := idx.SearchVectors(context.Background(), query, 10, nil, SearchOptions{})
		_ = results
	}
}
