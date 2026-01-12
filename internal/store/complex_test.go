package store

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComplex64_Support(t *testing.T) {
	// 128 dimensions, 64-byte alignment implies padding to 16-element multiple for Complex64 (8 bytes/elem).
	// 128 * 8 = 1024 bytes (aligned).
	dims := 128
	count := 100

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = VectorTypeComplex64
	config.M = 16
	config.EfConstruction = 100
	config.Float16Enabled = false // Ensure we use Complex64

	idx := NewArrowHNSW(nil, config, nil)

	// Generate random vectors
	vecs := make([][]float32, count)
	complexVecs := make([][]complex64, count)

	// Complex64 uses []float32 as interleaved input
	for i := 0; i < count; i++ {
		vecs[i] = make([]float32, dims*2)
		complexVecs[i] = make([]complex64, dims)
		for j := 0; j < dims; j++ {
			re := rand.Float32()
			im := rand.Float32()
			vecs[i][2*j] = re
			vecs[i][2*j+1] = im
			complexVecs[i][j] = complex(re, im)
		}
	}

	// Insert
	err := idx.AddBatchBulk(context.Background(), 0, count, vecs)
	require.NoError(t, err)

	// Verify Data Integrity via getVector
	v0, err := idx.getVector(0)
	require.NoError(t, err)
	assert.Equal(t, dims*2, len(v0))
	assert.InDeltaSlice(t, vecs[0], v0, 1e-5)

	// Search
	// Use vec[0] as query. Should find itself.
	results, err := idx.Search(vecs[0], 10, 50, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.EqualValues(t, 0, results[0].ID)
	assert.Equal(t, float32(0), results[0].Score)
}

func TestComplex128_Support(t *testing.T) {
	dims := 8 // Small dim for ease
	count := 50

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = VectorTypeComplex128
	config.M = 16
	config.EfConstruction = 100

	idx := NewArrowHNSW(nil, config, nil)

	vecs := make([][]float32, count)

	for i := 0; i < count; i++ {
		vecs[i] = make([]float32, dims*2)
		for j := 0; j < dims; j++ {
			re := rand.Float32()
			im := rand.Float32()
			vecs[i][2*j] = re
			vecs[i][2*j+1] = im
		}
	}

	err := idx.AddBatchBulk(context.Background(), 0, count, vecs)
	require.NoError(t, err)

	v0, err := idx.getVector(0)
	require.NoError(t, err)
	assert.Equal(t, dims*2, len(v0))
	assert.InDeltaSlice(t, vecs[0], v0, 1e-5)

	results, err := idx.Search(vecs[0], 10, 50, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.EqualValues(t, 0, results[0].ID)
}
