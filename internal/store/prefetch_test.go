package store

import (
	"context"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/coder/hnsw"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorPrefetch_Basic(t *testing.T) {
	// Test that prefetching valid memory doesn't crash
	data := make([]byte, 1024)
	for i := 0; i < len(data); i += 64 {
		simd.Prefetch(unsafe.Pointer(&data[i]))
	}

	// Test prefetching nil doesn't crash
	simd.Prefetch(nil)
}

func TestVectorPrefetch_PrefetchDistance(t *testing.T) {
	// Test prefetch distance constants
	prefetchDist := 4 // Prefetch 4 items ahead

	// Create test data
	data := make([]float32, 100)
	for i := range data {
		data[i] = rand.Float32()
	}

	// Prefetch loop should not crash
	for i := 0; i < 100-prefetchDist; i++ {
		if i%8 == 0 { // Prefetch every 8th item
			simd.Prefetch(unsafe.Pointer(&data[i+prefetchDist]))
		}
		_ = data[i] // Access current item
	}
}

func TestVectorPrefetch_ProcessChunkWithPrefetch(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 128
	numVectors := 50

	// Generate random vectors
	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	// Add vectors to index
	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}

	err := idx.AddBatchParallel(context.Background(), locations, 4)
	require.NoError(t, err)

	// Create mock neighbors for search
	neighbors := make([]hnsw.Node[VectorID], numVectors)
	for i := 0; i < numVectors; i++ {
		neighbors[i] = hnsw.Node[VectorID]{Key: VectorID(i)}
	}

	// Create query vector
	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	// Test processChunk with prefetching
	results := idx.processChunk(context.Background(), query, neighbors, nil)

	// Verify results are returned
	assert.NotNil(t, results)
}

func TestVectorPrefetch_EmptyNeighbors(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 64
	numVectors := 10

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	// Empty neighbors should return empty results
	query := make([]float32, dims)
	results := idx.processChunk(context.Background(), query, []hnsw.Node[VectorID]{}, nil)
	assert.NotNil(t, results)
	assert.Equal(t, 0, len(results))
}

func TestVectorPrefetch_PrefetchConfig(t *testing.T) {
	// Test prefetch configuration
	cfg := DefaultParallelSearchConfig()
	assert.True(t, cfg.Enabled)
	assert.Greater(t, cfg.Workers, 0)
	assert.Equal(t, 50, cfg.Threshold)

	// Update config
	cfg.Workers = 8
	cfg.Threshold = 100
	idx := &HNSWIndex{}
	idx.SetParallelSearchConfig(cfg)

	gotCfg := idx.getParallelSearchConfig()
	assert.Equal(t, 8, gotCfg.Workers)
	assert.Equal(t, 100, gotCfg.Threshold)
}

func TestVectorPrefetch_ContextCancellation(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 64
	numVectors := 100

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	// Add vectors
	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}
	err := idx.AddBatchParallel(context.Background(), locations, 4)
	require.NoError(t, err)

	// Create neighbors
	neighbors := make([]hnsw.Node[VectorID], numVectors)
	for i := 0; i < numVectors; i++ {
		neighbors[i] = hnsw.Node[VectorID]{Key: VectorID(i)}
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should return nil quickly when context is cancelled
	results := idx.processChunk(ctx, query, neighbors, nil)
	assert.Nil(t, results)
}

func TestVectorPrefetch_ProcessResultsParallelEmpty(t *testing.T) {
	idx := &HNSWIndex{}

	query := make([]float32, 128)
	neighbors := []hnsw.Node[VectorID]{}

	results := idx.processResultsParallel(context.Background(), query, neighbors, 10, nil)
	assert.NotNil(t, results)
	assert.Equal(t, 0, len(results))
}

func TestVectorPrefetch_SerialFallback(t *testing.T) {
	mem := memory.NewGoAllocator()
	dims := 64
	numVectors := 30 // Small number should trigger serial path

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "test",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	// Add vectors
	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}
	err := idx.AddBatchParallel(context.Background(), locations, 4)
	require.NoError(t, err)

	// Create neighbors
	neighbors := make([]hnsw.Node[VectorID], numVectors)
	for i := 0; i < numVectors; i++ {
		neighbors[i] = hnsw.Node[VectorID]{Key: VectorID(i)}
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	// With small neighbor count, should use serial path
	results := idx.processResultsParallel(context.Background(), query, neighbors, 10, nil)
	assert.NotNil(t, results)
}

func BenchmarkVectorPrefetch_WithPrefetch(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	numVectors := 1000

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "bench",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}
	_ = idx.AddBatchParallel(context.Background(), locations, 8)

	neighbors := make([]hnsw.Node[VectorID], 500)
	for i := 0; i < 500; i++ {
		neighbors[i] = hnsw.Node[VectorID]{Key: VectorID(i % numVectors)}
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = rand.Float32()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx.processChunk(context.Background(), query, neighbors, nil)
	}
}

func BenchmarkVectorPrefetch_SearchWithPrefetch(b *testing.B) {
	mem := memory.NewGoAllocator()
	dims := 128
	numVectors := 5000

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}

	rec := makeHNSWTestRecord(mem, dims, vectors)
	defer rec.Release()

	ds := &Dataset{
		Name:    "bench",
		Records: []arrow.RecordBatch{rec},
	}
	idx := NewHNSWIndex(ds)

	locations := make([]Location, numVectors)
	for i := 0; i < numVectors; i++ {
		locations[i] = Location{BatchIdx: 0, RowIdx: i}
	}
	_ = idx.AddBatchParallel(context.Background(), locations, 8)

	// Generate queries
	queries := make([][]float32, 100)
	for i := range queries {
		q := make([]float32, dims)
		for j := range q {
			q[j] = rand.Float32()
		}
		queries[i] = q
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			_, _ = idx.Search(query, 10)
		}
	}
}

func FuzzVectorPrefetch_SIMDPrefetch(f *testing.F) {
	f.Fuzz(func(t *testing.T, size int, offset int) {
		if size < 0 || size > 10000 {
			t.Skip()
		}
		if offset < 0 || offset > 100 {
			t.Skip()
		}

		data := make([]byte, size)
		for i := 0; i+offset < size; i += 64 {
			simd.Prefetch(unsafe.Pointer(&data[i+offset]))
		}
		_ = data[0]
	})
}

func FuzzVectorPrefetch_NeighborPrefetch(f *testing.F) {
	f.Fuzz(func(t *testing.T, numNeighbors int, prefetchDist int) {
		if numNeighbors < 0 || numNeighbors > 1000 {
			t.Skip()
		}
		if prefetchDist < 0 || prefetchDist > 20 {
			t.Skip()
		}

		neighbors := make([]hnsw.Node[VectorID], numNeighbors)
		for i := range neighbors {
			neighbors[i] = hnsw.Node[VectorID]{Key: VectorID(i)}
		}

		prefetchCount := prefetchDist
		for i := 0; i < prefetchCount && i < len(neighbors); i++ {
			simd.Prefetch(unsafe.Pointer(&neighbors[i]))
		}

		for i := range neighbors {
			nextIdx := i + prefetchCount
			if nextIdx < len(neighbors) {
				simd.Prefetch(unsafe.Pointer(&neighbors[nextIdx]))
			}
			_ = neighbors[i].Key
		}
	})
}

func FuzzVectorPrefetch_FlatBufferPrefetch(f *testing.F) {
	f.Fuzz(func(t *testing.T, numVectors int, dims int) {
		if numVectors < 0 || numVectors > 1000 {
			t.Skip()
		}
		if dims < 0 || dims > 2048 {
			t.Skip()
		}

		flatBuffer := make([]float32, numVectors*dims)
		for i := range flatBuffer {
			flatBuffer[i] = rand.Float32()
		}

		prefetchCount := 4
		for i := 0; i < numVectors; i++ {
			offset := i * dims
			copy(flatBuffer[offset:offset+dims], flatBuffer[offset:offset+dims])

			nextIdx := i + prefetchCount
			if nextIdx < numVectors {
				nextOffset := nextIdx * dims
				simd.Prefetch(unsafe.Pointer(&flatBuffer[nextOffset]))
			}
		}
		_ = flatBuffer[0]
	})
}
