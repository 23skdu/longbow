package index

import (
	basecore "github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/require"
	"testing"
	"unsafe"
)

func TestFloat32ToFloat32Computer(t *testing.T) {
	gd := &types.GraphData{
		Dims: 2,
		Type: types.VectorTypeFloat32,
	}

	// Create some chunks
	gd.Vectors = make([][]float32, 2)
	// Chunk 0: vectors 0, 1, 2 padded to 8 dims
	gd.Vectors[0] = make([]float32, 24)
	gd.Vectors[0][0], gd.Vectors[0][1] = 1.0, 1.0
	gd.Vectors[0][8], gd.Vectors[0][9] = 2.0, 2.0

	// Chunk 1 is nil, simulating paged out

	h := &ArrowHNSW{
		distFunc: func(a, b []float32) (float32, error) {
			return 1.0, nil
		},
		distFuncSquared: func(a, b []float32) (float32, error) {
			return 1.0, nil
		},
		config: types.ArrowHNSWConfig{Metric: basecore.MetricEuclidean},
	}

	c := &float32ToFloat32Computer{
		squared:   false,
		data:      gd,
		q:         []float32{0.0, 0.0},
		dims:      2,
		h:         h,
		diskGraph: nil,
		maxGen:    ^uint64(0),
	}

	dst := make([]float32, 2)

	// Test ComputeBatch
	dists, err := c.ComputeBatch([]uint32{0, 1}, dst)
	require.NoError(t, err)
	require.Len(t, dists, 2)

	// Test with a chunk that is nil (paged out)
	_, _ = c.ComputeBatch([]uint32{0, types.ChunkSize}, dst)

	// Test Prefetch
	c.Prefetch(0)
	c.Prefetch(types.ChunkSize)
}

func TestSearchContextChunkCache(t *testing.T) {
	const dims = 16
	const nChunks = 3
	g := batchParityGraphData(t, types.VectorTypeFloat32, dims, nChunks)

	sctx := NewArrowSearchContext()
	require.Equal(t, uint32(1), sctx.chunkEpoch)

	// Fetch vector 0 (chunk 0)
	v0 := sctx.GetVectorF32(g, 0, dims, ^uint64(0))
	require.NotNil(t, v0)
	require.Len(t, v0, dims)
	require.Equal(t, uint32(1), sctx.cachedChunksF32[0].epoch)

	// Cache hit on second call
	v0Again := sctx.GetVectorF32(g, 0, dims, ^uint64(0))
	require.Equal(t, v0, v0Again)
	require.Equal(t, unsafe.Pointer(&v0[0]), unsafe.Pointer(&v0Again[0]))

	// Fetch vector in chunk 1 (id = ChunkSize)
	v1024 := sctx.GetVectorF32(g, types.ChunkSize, dims, ^uint64(0))
	require.NotNil(t, v1024)
	require.Equal(t, uint32(1), sctx.cachedChunksF32[1].epoch)

	// Verify float32ToFloat32Computer with sctx produces identical results
	h := newBatchBenchHNSW()
	q := make([]float32, dims)
	for i := range q {
		q[i] = float32(i) * 0.5
	}

	compNoCtx := &float32ToFloat32Computer{
		data:    g,
		q:       q,
		dims:    dims,
		h:       h,
		maxGen:  ^uint64(0),
		squared: false,
	}

	compWithCtx := &float32ToFloat32Computer{
		data:    g,
		q:       q,
		dims:    dims,
		h:       h,
		maxGen:  ^uint64(0),
		squared: false,
		sctx:    sctx,
	}

	for id := uint32(0); id < uint32(nChunks*types.ChunkSize); id += 257 {
		d1, err1 := compNoCtx.ComputeSingle(id)
		require.NoError(t, err1)
		d2, err2 := compWithCtx.ComputeSingle(id)
		require.NoError(t, err2)
		require.Equal(t, d1, d2, "mismatch at id %d", id)
	}

	// Verify Reset() advances epoch and invalidates cache
	sctx.Reset()
	require.Equal(t, uint32(2), sctx.chunkEpoch)
	require.NotEqual(t, sctx.chunkEpoch, sctx.cachedChunksF32[0].epoch)

	// After Reset, resolving again updates to new epoch
	v0AfterReset := sctx.GetVectorF32(g, 0, dims, ^uint64(0))
	require.Equal(t, v0, v0AfterReset)
	require.Equal(t, uint32(2), sctx.cachedChunksF32[0].epoch)
}

