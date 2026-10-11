package index

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/23skdu/longbow/internal/store/types"
)

func TestTQScratchArena_BasicOperations(t *testing.T) {
	dims := 128
	arena := NewTQScratchArena(dims)
	require.NotNil(t, arena)
	assert.True(t, arena.IsEnabled())

	hits, misses, decodes, chunks := arena.Stats()
	assert.Equal(t, int64(0), hits)
	assert.Equal(t, int64(0), misses)
	assert.Equal(t, int64(0), decodes)
	assert.Equal(t, int64(0), chunks)

	// Miss on non-existent vector
	vec, ok := arena.Get(42)
	assert.False(t, ok)
	assert.Nil(t, vec)

	hits, misses, _, _ = arena.Stats()
	assert.Equal(t, int64(0), hits)
	assert.Equal(t, int64(1), misses)

	// Put and Get within chunk 0
	sampleVec := make([]float32, dims)
	for i := range sampleVec {
		sampleVec[i] = float32(i + 1)
	}
	arena.Put(42, sampleVec)

	retrieved, ok := arena.Get(42)
	assert.True(t, ok)
	require.Equal(t, dims, len(retrieved))
	assert.Equal(t, sampleVec, retrieved)

	hits, misses, _, chunks = arena.Stats()
	assert.Equal(t, int64(1), hits)
	assert.Equal(t, int64(1), misses)
	assert.Equal(t, int64(1), chunks)

	// Cross chunk boundary (chunk size is 1024)
	idChunk0 := uint32(1023)
	idChunk1 := uint32(1024)
	idChunk2 := uint32(2500)

	vec0 := make([]float32, dims)
	vec0[0] = 1023.0
	vec1 := make([]float32, dims)
	vec1[0] = 1024.0
	vec2 := make([]float32, dims)
	vec2[0] = 2500.0

	arena.Put(idChunk0, vec0)
	arena.Put(idChunk1, vec1)
	arena.Put(idChunk2, vec2)

	r0, ok0 := arena.Get(idChunk0)
	assert.True(t, ok0)
	assert.Equal(t, float32(1023.0), r0[0])

	r1, ok1 := arena.Get(idChunk1)
	assert.True(t, ok1)
	assert.Equal(t, float32(1024.0), r1[0])

	r2, ok2 := arena.Get(idChunk2)
	assert.True(t, ok2)
	assert.Equal(t, float32(2500.0), r2[0])

	_, _, _, chunks = arena.Stats()
	assert.Equal(t, int64(3), chunks) // chunk 0 (0..1023), chunk 1 (1024..2047), chunk 2 (2048..3071)
}

func TestTQScratchArena_PutBatch(t *testing.T) {
	dims := 64
	arena := NewTQScratchArena(dims)
	require.NotNil(t, arena)

	numVecs := 200
	startID := uint32(500)
	batch := make([][]float32, numVecs)
	for i := range batch {
		batch[i] = make([]float32, dims)
		for j := range batch[i] {
			batch[i][j] = float32(i*dims + j)
		}
	}

	arena.PutBatch(startID, batch)

	for i := 0; i < numVecs; i++ {
		id := startID + uint32(i)
		v, ok := arena.Get(id)
		assert.True(t, ok, "expected vector ID %d to be cached", id)
		assert.Equal(t, batch[i], v)
	}
}

func TestTQScratchArena_GetOrDecode(t *testing.T) {
	dims := 32
	arena := NewTQScratchArena(dims)
	require.NotNil(t, arena)

	decodeCalls := 0
	decodeFn := func(id uint32, dst []float32) error {
		decodeCalls++
		for i := range dst {
			dst[i] = float32(id)*100.0 + float32(i)
		}
		return nil
	}

	// First call should decode
	v1, err := arena.GetOrDecode(77, decodeFn)
	require.NoError(t, err)
	assert.Equal(t, 1, decodeCalls)
	assert.Equal(t, float32(7700.0), v1[0])

	// Second call should hit cache and NOT invoke decodeFn
	v2, err := arena.GetOrDecode(77, decodeFn)
	require.NoError(t, err)
	assert.Equal(t, 1, decodeCalls)
	assert.Equal(t, v1, v2)

	// Error propagation
	errFn := func(id uint32, dst []float32) error {
		return fmt.Errorf("decode error for %d", id)
	}
	_, err = arena.GetOrDecode(88, errFn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode error for 88")

	// Slot should not be valid after error
	_, ok := arena.Get(88)
	assert.False(t, ok)
}

func TestTQScratchArena_ConcurrentGetOrDecode(t *testing.T) {
	dims := 64
	arena := NewTQScratchArena(dims)
	require.NotNil(t, arena)

	numDistinct := 50
	numGoroutines := 16
	iterations := 100

	var decodeCount atomic.Int64
	perVectorDecodes := make([]atomic.Int32, numDistinct)

	decodeFn := func(id uint32, dst []float32) error {
		decodeCount.Add(1)
		perVectorDecodes[id].Add(1)
		for i := range dst {
			dst[i] = float32(id)*10.0 + float32(i)
		}
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for iter := 0; iter < iterations; iter++ {
				id := uint32(rng.Intn(numDistinct))
				vec, err := arena.GetOrDecode(id, decodeFn)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}
				if len(vec) != dims {
					t.Errorf("unexpected dim: %d", len(vec))
					return
				}
				if vec[0] != float32(id)*10.0 {
					t.Errorf("data corruption for id %d: got %f", id, vec[0])
					return
				}
			}
		}(g + 1)
	}

	wg.Wait()

	// Every distinct ID should be decoded EXACTLY ONCE
	for i := 0; i < numDistinct; i++ {
		assert.Equal(t, int32(1), perVectorDecodes[i].Load(), "vector ID %d was decoded multiple times", i)
	}
	assert.Equal(t, int64(numDistinct), decodeCount.Load())

	hits, _, decodes, _ := arena.Stats()
	assert.Equal(t, int64(numDistinct), decodes)
	assert.Greater(t, hits, int64(numDistinct*10))
}

func TestTQScratchArena_Release(t *testing.T) {
	dims := 32
	arena := NewTQScratchArena(dims)
	require.NotNil(t, arena)

	sample := make([]float32, dims)
	arena.Put(1, sample)
	arena.Put(2000, sample)

	assert.True(t, arena.IsEnabled())
	_, _, _, chunks := arena.Stats()
	assert.Equal(t, int64(2), chunks)

	arena.Release()

	assert.False(t, arena.IsEnabled())
	_, _, _, chunksAfter := arena.Stats()
	assert.Equal(t, int64(0), chunksAfter)

	// Get after release returns false
	_, ok := arena.Get(1)
	assert.False(t, ok)

	// Put after release is a no-op
	arena.Put(3, sample)
	_, ok = arena.Get(3)
	assert.False(t, ok)
}

func TestTQScratchArena_NilSafety(t *testing.T) {
	var nilArena *TQScratchArena

	assert.False(t, nilArena.IsEnabled())

	vec, ok := nilArena.Get(10)
	assert.False(t, ok)
	assert.Nil(t, vec)

	nilArena.Put(10, []float32{1.0, 2.0})
	nilArena.PutBatch(10, [][]float32{{1.0}, {2.0}})

	_, err := nilArena.GetOrDecode(10, func(id uint32, dst []float32) error {
		return nil
	})
	assert.Error(t, err)

	nilArena.Release()

	hits, misses, decodes, chunks := nilArena.Stats()
	assert.Equal(t, int64(0), hits)
	assert.Equal(t, int64(0), misses)
	assert.Equal(t, int64(0), decodes)
	assert.Equal(t, int64(0), chunks)
}

func TestTQScratchArena_HNSWIndexingIntegration(t *testing.T) {
	dims := 128
	bits := 4
	n := 1000
	batchSize := 250

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	idB := builder.Field(0).(*array.Int64Builder)
	vecB := builder.Field(1).(*array.FixedSizeListBuilder)
	valB := vecB.ValueBuilder().(*array.Float32Builder)

	rng := rand.New(rand.NewSource(12345))
	for i := 0; i < n; i++ {
		idB.Append(int64(i))
		vecB.Append(true)
		for j := 0; j < dims; j++ {
			valB.Append(rng.Float32())
		}
	}
	rec := builder.NewRecordBatch()
	defer rec.Release()

	ds := NewMockDataset("test_tq_scratch_arena_hnsw", schema)
	ds.Records = append(ds.Records, rec)

	rowIdxs := make([]int, n)
	batchIdx := make([]int, n)
	for i := 0; i < n; i++ {
		rowIdxs[i] = i
		batchIdx[i] = 0
	}

	cfg := types.DefaultArrowHNSWConfig()
	cfg.DataType = types.VectorTypeTQ
	cfg.Dims = dims
	cfg.TurboQuantEnabled = true
	cfg.TurboQuantBits = bits
	cfg.M = 16
	cfg.MMax = 16
	cfg.MMax0 = 16
	cfg.EfConstruction = 100
	cfg.Workers = 4

	idx := NewArrowHNSW(ds, &cfg, nil)
	require.NotNil(t, idx)

	// Verify scratch arena is enabled
	arena := idx.GetTQScratchArena()
	require.NotNil(t, arena)
	assert.True(t, arena.IsEnabled())

	// Insert in batches of 250 rows to exercise multi-batch graph construction
	for off := 0; off < n; off += batchSize {
		end := off + batchSize
		if end > n {
			end = n
		}
		_, err := idx.AddBatch(context.Background(),
			[]arrow.RecordBatch{rec}, rowIdxs[off:end], batchIdx[off:end])
		require.NoError(t, err)
	}

	// Verify that the arena cached reconstructed vectors and was actively hit
	hits, misses, decodes, chunks := arena.Stats()
	t.Logf("TQ Scratch Arena Stats after %d inserts: hits=%d, misses=%d, decodes=%d, chunks=%d",
		n, hits, misses, decodes, chunks)

	// Across multi-batch indexing with M=16 and EfConstruction=100, candidate evaluations
	// against already-inserted nodes must yield substantial cache hits.
	assert.Greater(t, hits, int64(1000), "expected significant cache hits in scratch arena during indexing")
	assert.Greater(t, chunks, int64(0), "expected at least 1 arena chunk allocated")

	// Search verification: index must be searchable and return valid neighbors
	queryVec := make([]float32, dims)
	for j := 0; j < dims; j++ {
		queryVec[j] = rng.Float32()
	}
	results, err := idx.Search(context.Background(), queryVec, 10, 50)
	require.NoError(t, err)
	assert.Len(t, results, 10)

	// Close the index, which should release the ephemeral scratch arena
	err = idx.Close()
	require.NoError(t, err)

	assert.False(t, arena.IsEnabled(), "arena should be disabled after index Close()")
	_, _, _, chunksClosed := arena.Stats()
	assert.Equal(t, int64(0), chunksClosed, "arena chunks should be cleared on Close()")
}
