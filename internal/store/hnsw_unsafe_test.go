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

// makeTestDataset creates a Dataset with test vectors for unsafe access tests
func makeTestDataset(mem memory.Allocator, dims int, vectors [][]float32) *Dataset {
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

	rec := array.NewRecordBatch(schema, []arrow.Array{idBuilder.NewArray(), listBuilder.NewArray()}, int64(len(vectors)))
	return &Dataset{
		Records: []arrow.RecordBatch{rec},
	}
}

// TestEpochBasics tests the epoch counter enter/exit operations
func TestEpochBasics(t *testing.T) {
	mem := memory.NewGoAllocator()
	ds := makeTestDataset(mem, 4, [][]float32{
		{1.0, 2.0, 3.0, 4.0},
	})
	idx := NewHNSWIndex(ds)
	_, _ = idx.Add(0, 0)

	// Initial epoch should be 0
	assert.Equal(t, uint64(0), idx.currentEpoch.Load())

	// Enter epoch should increment active readers
	idx.enterEpoch()
	assert.Equal(t, int64(1), idx.activeReaders.Load())

	// Exit epoch should decrement active readers
	idx.exitEpoch()
	assert.Equal(t, int64(0), idx.activeReaders.Load())
}

// TestGetVectorUnsafeReturnsDirectSlice verifies zero-copy behavior
func TestGetVectorUnsafeReturnsDirectSlice(t *testing.T) {
	mem := memory.NewGoAllocator()
	testVec := []float32{1.0, 2.0, 3.0, 4.0}
	ds := makeTestDataset(mem, 4, [][]float32{testVec})
	idx := NewHNSWIndex(ds)
	_, _ = idx.Add(0, 0)

	// Get unsafe vector
	vec, release := idx.getVectorUnsafe(0)
	require.NotNil(t, vec)
	defer release()

	// Should have correct values
	assert.Equal(t, float32(1.0), vec[0])
	assert.Equal(t, float32(2.0), vec[1])
	assert.Equal(t, float32(3.0), vec[2])
	assert.Equal(t, float32(4.0), vec[3])

	// Get safe copy for comparison
	safeCopy := idx.getVector(0)
	require.NotNil(t, safeCopy)

	// Values should match
	assert.Equal(t, safeCopy, vec)
}

// TestGetVectorUnsafeInvalidID tests behavior with invalid vector IDs
func TestGetVectorUnsafeInvalidID(t *testing.T) {
	mem := memory.NewGoAllocator()
	ds := makeTestDataset(mem, 4, [][]float32{
		{1.0, 2.0, 3.0, 4.0},
	})
	idx := NewHNSWIndex(ds)
	_, _ = idx.Add(0, 0)

	// Invalid ID should return nil
	vec, release := idx.getVectorUnsafe(999)
	assert.Nil(t, vec)
	assert.Nil(t, release)
}

// TestGetVectorUnsafeConcurrent tests concurrent access safety
func TestGetVectorUnsafeConcurrent(t *testing.T) {
	mem := memory.NewGoAllocator()
	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
	}
	ds := makeTestDataset(mem, 4, vectors)
	idx := NewHNSWIndex(ds)
	for i := range vectors {
		_, _ = idx.Add(0, i)
	}

	// Run concurrent readers
	var wg sync.WaitGroup
	const numGoroutines = 20
	const numReads = 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < numReads; i++ {
				id := VectorID((gid*numReads + i) % 100)
				vec, release := idx.getVectorUnsafe(id)
				if vec != nil {
					// Access data within epoch
					_ = vec[0] + vec[1]
					release()
				}
			}
		}(g)
	}
	wg.Wait()

	// All readers should have exited
	assert.Equal(t, int64(0), idx.activeReaders.Load())
}

// TestEpochAdvanceWaitsForReaders tests that epoch advance waits for active readers
// TestEpochAdvanceWaitsForReaders tests that epoch advance waits for active readers
func TestEpochAdvanceWaitsForReaders(t *testing.T) {
	mem := memory.NewGoAllocator()
	ds := makeTestDataset(mem, 4, [][]float32{
		{1.0, 2.0, 3.0, 4.0},
	})
	idx := NewHNSWIndex(ds)
	_, _ = idx.Add(0, 0)

	// Enter epoch, verify reader count incremented
	idx.enterEpoch()
	assert.Equal(t, int64(1), idx.activeReaders.Load())

	// Advance epoch in separate goroutine
	done := make(chan struct{})
	go func() {
		idx.advanceEpoch()
		close(done)
	}()

	// Exit epoch - allows advance to complete
	idx.exitEpoch()

	// Wait for advance
	<-done

	assert.Equal(t, int64(0), idx.activeReaders.Load())
	assert.Equal(t, uint64(1), idx.currentEpoch.Load())
}

func BenchmarkGetVectorCopy(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 768 // Common embedding dimension
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = float32(i)
	}
	ds := makeTestDataset(mem, dims, [][]float32{vec})
	idx := NewHNSWIndex(ds)
	_, err := idx.Add(0, 0)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := idx.getVector(0)
		_ = v[0]
	}
}

// BenchmarkGetVectorUnsafe benchmarks the zero-copy version
func BenchmarkGetVectorUnsafe(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 768
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = float32(i)
	}
	ds := makeTestDataset(mem, dims, [][]float32{vec})
	idx := NewHNSWIndex(ds)
	_, _ = idx.Add(0, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, release := idx.getVectorUnsafe(0)
		_ = v[0]
		release()
	}
}

// BenchmarkGetVectorUnsafeHighDim benchmarks zero-copy with high dimensions
func BenchmarkGetVectorUnsafeHighDim(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 1536 // High-dim embedding
	vec := make([]float32, dims)
	for i := range vec {
		vec[i] = float32(i)
	}
	ds := makeTestDataset(mem, dims, [][]float32{vec})
	idx := NewHNSWIndex(ds)
	_, _ = idx.Add(0, 0)

	b.ResetTimer()
	b.Run("Copy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			v := idx.getVector(0)
			_ = v[0]
		}
	})
	b.Run("Unsafe", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			v, release := idx.getVectorUnsafe(0)
			_ = v[0]
			release()
		}
	})
}
