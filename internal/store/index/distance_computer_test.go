package index

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/23skdu/longbow/internal/store/types"
	basecore "github.com/23skdu/longbow/internal/core"
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
		squared: false,
		data: gd,
		q: []float32{0.0, 0.0},
		dims: 2,
		h: h,
		diskGraph: nil,
		maxGen: ^uint64(0),
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
