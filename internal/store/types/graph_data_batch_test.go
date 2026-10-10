package types

import (
	"math"
	"testing"

	"github.com/23skdu/longbow/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchChunkGraphData builds a GraphData with nChunks resident chunks for dt.
func batchChunkGraphData(t *testing.T, dt VectorDataType, dims, nChunks int) *GraphData {
	t.Helper()
	g := NewGraphData(nChunks*ChunkSize, dims, false, false, -1, false, false, false,
		dt, false, false, false, 8, "batch-types", nil, false)
	require.NotNil(t, g)
	for c := 0; c < nChunks; c++ {
		require.NoError(t, g.EnsureChunk(c, 0, dims))
	}
	for id := 0; id < nChunks*ChunkSize; id++ {
		var vec any
		switch dt {
		case VectorTypeFloat32:
			v := make([]float32, dims)
			for i := range v {
				v[i] = float32(id*dims + i)
			}
			vec = v
		case VectorTypeFloat64:
			v := make([]float64, dims)
			for i := range v {
				v[i] = float64(id*dims + i)
			}
			vec = v
		case VectorTypeInt8:
			v := make([]int8, dims)
			for i := range v {
				v[i] = int8(i%127 + 1)
			}
			vec = v
		}
		require.NoError(t, g.SetVector(uint32(id), vec)) // #nosec G115
	}
	return g
}

// TestChunkBatch_Float32Parity pins VectorChunkBatch.Chunk/Vector against the
// reference accessors across generations, chunk boundaries and out-of-range ids.
func TestChunkBatch_Float32Parity(t *testing.T) {
	const nChunks = 3
	g := batchChunkGraphData(t, VectorTypeFloat32, 128, nChunks)
	pd := g.GetPaddedDimsForType(VectorTypeFloat32)

	for _, maxGen := range []uint64{0, 1, 5, math.MaxUint64} {
		batch := g.BeginFloat32ChunkBatch(maxGen)
		for c := 0; c <= nChunks; c++ { // include the empty chunk past the end
			want := g.GetVectorsChunkWithGen(c, maxGen)
			got := batch.Chunk(c)
			if want == nil {
				assert.Nil(t, got, "maxGen=%d chunk=%d", maxGen, c)
			} else {
				require.NotNil(t, got, "maxGen=%d chunk=%d", maxGen, c)
				require.Len(t, got, len(want))
				assert.Equal(t, want, got, "maxGen=%d chunk=%d", maxGen, c)
			}

			for _, idx := range []int{0, 1, 17, ChunkSize - 1, ChunkSize, ChunkSize + 1, -1} {
				v := batch.Vector(c, idx, 128)
				if want == nil {
					assert.Nil(t, v, "maxGen=%d chunk=%d idx=%d", maxGen, c, idx)
					continue
				}
				start := idx * pd
				if idx < 0 || start+128 > len(want) {
					assert.Nil(t, v, "maxGen=%d chunk=%d idx=%d out of chunk", maxGen, c, idx)
					continue
				}
				require.NotNil(t, v, "maxGen=%d chunk=%d idx=%d", maxGen, c, idx)
				assert.Equal(t, want[start:start+128], v, "maxGen=%d chunk=%d idx=%d", maxGen, c, idx)
			}
		}
		// Negative chunk id must be rejected, not wrap.
		assert.Nil(t, batch.Chunk(-1))
		assert.Nil(t, batch.Vector(-1, 0, 128))
	}
}

// TestChunkBatch_Float64Parity mirrors the float32 test for float64.
func TestChunkBatch_Float64Parity(t *testing.T) {
	const nChunks = 2
	g := batchChunkGraphData(t, VectorTypeFloat64, 64, nChunks)
	pd := g.GetPaddedDimsForType(VectorTypeFloat64)

	for _, maxGen := range []uint64{0, 3, math.MaxUint64} {
		batch := g.BeginFloat64ChunkBatch(maxGen)
		for c := 0; c <= nChunks; c++ {
			want := g.GetVectorsFloat64ChunkWithGen(c, maxGen)
			got := batch.Chunk(c)
			if want == nil {
				assert.Nil(t, got, "maxGen=%d chunk=%d", maxGen, c)
				continue
			}
			require.NotNil(t, got)
			assert.Equal(t, len(want), len(got))
			for _, idx := range []int{0, 5, ChunkSize - 1, ChunkSize} {
				v := batch.Vector(c, idx, 64)
				start := idx * pd
				if idx < 0 || start+64 > len(want) {
					assert.Nil(t, v)
					continue
				}
				require.NotNil(t, v)
				assert.Equal(t, want[start:start+64], v)
			}
		}
	}
}

// TestChunkBatch_Int8Parity mirrors the float32 test for int8, and pins the
// historical behaviour that a zero chunk offset is forwarded to the arena
// rather than treated as "not resident".
func TestChunkBatch_Int8Parity(t *testing.T) {
	const nChunks = 2
	g := batchChunkGraphData(t, VectorTypeInt8, 64, nChunks)
	pd := g.GetPaddedDimsForType(VectorTypeInt8)

	for _, maxGen := range []uint64{0, 2, math.MaxUint64} {
		batch := g.BeginInt8ChunkBatch(maxGen)
		for c := 0; c <= nChunks; c++ {
			want := g.GetVectorsInt8ChunkWithGen(c, maxGen)
			got := batch.Chunk(c)
			if want == nil {
				assert.Nil(t, got, "maxGen=%d chunk=%d", maxGen, c)
				continue
			}
			require.NotNil(t, got)
			assert.Equal(t, len(want), len(got))
			for _, idx := range []int{0, 3, ChunkSize - 1, ChunkSize} {
				v := batch.Vector(c, idx, 64)
				start := idx * pd
				if idx < 0 || start+64 > len(want) {
					assert.Nil(t, v)
					continue
				}
				require.NotNil(t, v)
				assert.Equal(t, want[start:start+64], v)
			}
		}
	}
}

// TestChunkBatch_LegacyFallback pins the arena/legacy selection: with no arena
// the batch must serve the legacy slices exactly as GetVectorsChunk does.
func TestChunkBatch_LegacyFallback(t *testing.T) {
	g := &GraphData{Type: VectorTypeFloat32, Dims: 8}
	g.GrowMetadataSlices(2)
	g.Float32Arena = nil
	g.Vectors = [][]float32{make([]float32, ChunkSize*8)}
	for i := range g.Vectors[0] {
		g.Vectors[0][i] = float32(i)
	}

	batch := g.BeginFloat32ChunkBatch(math.MaxUint64)
	require.NotNil(t, batch.Chunk(0))
	assert.Equal(t, g.Vectors[0], batch.Chunk(0))
	assert.Equal(t, g.Vectors[0][16:24], batch.Vector(0, 1, 8)) // pd = 16 for dims 8
	assert.Nil(t, batch.Chunk(1))
	assert.Nil(t, batch.Vector(1, 0, 8))
	assert.True(t, batch.Stale(), "no arena means no batch to track")

	// A nil graph arena must not panic.
	var nilGraph *GraphData
	nilBatch := nilGraph.BeginFloat32ChunkBatch(0)
	assert.Nil(t, nilBatch.Chunk(0))
	assert.Nil(t, nilBatch.Vector(0, 0, 8))
}

// TestChunkBatch_GenerationBump pins that a batch opened under one generation
// cannot see data written into a newer one, and that it tracks a
// generation-gated slab exactly like the reference accessor.
func TestChunkBatch_GenerationBump(t *testing.T) {
	g := NewGraphData(ChunkSize, 64, false, false, -1, false, false, false,
		VectorTypeFloat32, false, false, false, 8, "batch-gen", nil, false)
	require.NotNil(t, g)
	require.Equal(t, uint64(0), g.Float32Arena.Slab().GetGeneration())

	older := g.BeginFloat32ChunkBatch(0)
	require.NotNil(t, older.Chunk(0))
	require.NotNil(t, g.GetVectorsChunkFastWithGen(0, 0))

	// A chunk allocated after the bump lands in a newer slab.
	g.SetGeneration(4)
	g.GrowMetadataSlices(2)
	require.NoError(t, g.EnsureChunk(1, 0, 64))
	require.Equal(t, uint64(4), g.Float32Arena.Slab().GetGeneration())

	// The generation-0 batch must not see the generation-4 chunk, and must
	// agree with the reference accessor about it.
	require.Nil(t, g.GetVectorsChunkFastWithGen(1, 0), "reference must hide the gen-4 chunk at maxGen 0")
	assert.Nil(t, older.Chunk(1))
	assert.Nil(t, older.Vector(1, 0, 64))
	// ... but the generation-0 chunk is still visible to both.
	assert.NotNil(t, older.Chunk(0))

	// A batch at the new generation sees both.
	fresh := g.BeginFloat32ChunkBatch(4)
	require.NotNil(t, fresh.Chunk(0))
	require.NotNil(t, fresh.Chunk(1))
	assert.Equal(t, g.GetVectorsChunkFastWithGen(0, 4), fresh.Chunk(0))
	assert.Equal(t, g.GetVectorsChunkFastWithGen(1, 4), fresh.Chunk(1))
}

// TestChunkBatch_StaleOnFree pins that a batch does not keep serving a released
// arena: the underlying slab table is swapped, the batch detects it and drops
// the data exactly like the reference accessor.
func TestChunkBatch_StaleOnFree(t *testing.T) {
	g := NewGraphData(ChunkSize, 64, false, false, -1, false, false, false,
		VectorTypeFloat32, false, false, false, 8, "batch-free", nil, false)
	require.NotNil(t, g)
	require.NoError(t, g.EnsureChunk(0, 0, 64))
	for id := 0; id < 8; id++ {
		v := make([]float32, 64)
		for i := range v {
			v[i] = float32(id*64 + i)
		}
		require.NoError(t, g.SetVector(uint32(id), v)) // #nosec G115
	}

	batch := g.BeginFloat32ChunkBatch(math.MaxUint64)
	ref := g.GetVectorsChunkFastWithGen(0, math.MaxUint64)
	require.NotNil(t, batch.Chunk(0))
	require.Equal(t, ref[:64], batch.Vector(0, 0, 64))
	require.False(t, batch.Stale())

	require.NotNil(t, g.Float32Arena)
	g.Float32Arena.Slab().Free()

	require.True(t, batch.Stale())
	assert.Nil(t, batch.Chunk(0), "batch must not serve a released arena")
	assert.Nil(t, batch.Vector(0, 0, 64))
}

// TestChunkBatch_OffsetZero pins that a zero chunk offset means "not resident"
// everywhere: in the reference accessor, in the fast accessor, and in the batch.
// It used to be the one exception - GetVectorsInt8ChunkWithGen forwarded the
// zero offset into the arena and returned whatever bytes live at the start of
// the first slab as if they were the chunk's vectors, while
// GetVectorsInt8ChunkFast and the batch both returned nil for the same chunk.
// That made ComputeSingle and ComputeBatch disagree about the contents of the
// same id depending on whether the caller had a generation filter, and fed
// plausible distances computed against unrelated data into neighbour selection.
func TestChunkBatch_OffsetZero(t *testing.T) {
	g := NewGraphData(ChunkSize, 64, false, false, -1, false, false, false,
		VectorTypeFloat32, false, false, false, 8, "batch-zero", nil, false)
	require.NotNil(t, g)
	require.Len(t, g.VectorsF32, 1)

	batch := g.BeginFloat32ChunkBatch(math.MaxUint64)
	// Chunk 1 is outside the offset table: the reference falls through to the
	// legacy slice table and finds nothing.
	require.Nil(t, g.GetVectorsChunkFastWithGen(1, math.MaxUint64))
	assert.Nil(t, batch.Chunk(1))
	assert.Nil(t, batch.Vector(1, 0, 64))

	slab := memory.NewSlabArena(1 << 20)
	ta := memory.NewTypedArena[int8](slab)
	ref, err := ta.AllocSlice(ChunkSize * 64)
	require.NoError(t, err)
	i8 := &GraphData{
		Type:          VectorTypeInt8,
		Dims:          64,
		Int8Arena:     ta,
		VectorsInt8:   []uint64{ref.Offset, 0},
		VectorsF32:    []uint64{},
		Vectors:       nil,
		GlobalVersion: 0,
	}

	// Chunk 1 holds a zero offset, which means it was never allocated.
	assert.Nil(t, i8.GetVectorsInt8ChunkWithGen(1, math.MaxUint64))
	assert.Nil(t, i8.GetVectorsInt8ChunkFast(1))
	i8Batch := i8.BeginInt8ChunkBatch(math.MaxUint64)
	assert.Nil(t, i8Batch.Chunk(1))

	// Chunk 0 is resident, and all three views of it agree byte for byte.
	want := i8.GetVectorsInt8ChunkWithGen(0, math.MaxUint64)
	require.NotNil(t, want)
	assert.Equal(t, want, i8.GetVectorsInt8ChunkFast(0))
	assert.Equal(t, want, i8Batch.Chunk(0))
}

// TestChunkBatch_UsesArenaNotGlobal keeps the accessor honest about which arena
// it reads: swapping the typed arena under the graph must be observed.
func TestChunkBatch_UsesArenaNotGlobal(t *testing.T) {
	oldArena := memory.NewSlabArena(1 << 16)
	newArena := memory.NewSlabArena(1 << 16)
	oldTyped := memory.NewTypedArena[float32](oldArena)
	oldRef, err := oldTyped.AllocSlice(8)
	require.NoError(t, err)
	for i := range oldTyped.Get(oldRef) {
		oldTyped.Get(oldRef)[i] = 1
	}

	batch := oldTyped.BeginBatch(math.MaxUint64)
	require.NotNil(t, batch.Get(oldRef))

	// Compact swaps the underlying arena; the batch must follow it.
	_, err = oldTyped.Compact([]memory.SliceRef{oldRef})
	require.NoError(t, err)
	require.NotSame(t, oldArena, oldTyped.Slab())
	require.True(t, batch.Stale())
	require.NotNil(t, newArena)
}
