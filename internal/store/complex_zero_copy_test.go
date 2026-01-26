package store

import (
	"context"
	"math"
	"math/rand"
	"testing"

	lbtypes "github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArrowHNSW_Complex64_ZeroCopy validates that we can insert and search
// using native []complex64 slices without intermediate float32 conversion.
func TestArrowHNSW_Complex64_ZeroCopy(t *testing.T) {
	dims := 128
	count := 50

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeComplex64
	config.M = 16
	config.EfConstruction = 100
	config.Metric = "l2"

	idx := NewArrowHNSW(nil, config)

	// 1. Prepare Data
	vecsC64 := make([][]complex64, count)
	for i := 0; i < count; i++ {
		vecsC64[i] = make([]complex64, dims)
		for j := 0; j < dims; j++ {
			// Real/Imag parts in [-1, 1]
			re := rand.Float32()*2 - 1
			im := rand.Float32()*2 - 1
			vecsC64[i][j] = complex(re, im)
		}
	}

	// 2. Insert with Zero-Copy (generic API)
	for i := 0; i < count; i++ {
		err := idx.InsertWithVector(uint32(i), vecsC64[i], -1)
		require.NoError(t, err)
	}

	// 3. Verify Storage Type
	// Read back ID 0
	valAny, err := idx.getVectorAny(0)
	require.NoError(t, err)

	// Should be exactly []complex64
	valC64, ok := valAny.([]complex64)
	require.True(t, ok, "Expected stored vector to be []complex64")
	assert.Equal(t, vecsC64[0], valC64)

	// 4. Search with Zero-Copy Query
	// Passing a native []complex64 slice.
	// The new Search(any) signature should allow this and resolve correctly.
	query := vecsC64[0]
	results, err := idx.Search(context.Background(), query, 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// Expect ID 0 at top
	assert.Equal(t, uint32(0), uint32(results[0].ID))
	// Score should be ~0 (Euclidean distance to itself)
	assert.True(t, math.Abs(float64(results[0].Dist)) < 1e-5, "Expected 0 distance for self-search")
}

// TestArrowHNSW_Complex128_ZeroCopy validates native complex128 support.
func TestArrowHNSW_Complex128_ZeroCopy(t *testing.T) {
	dims := 64
	count := 30

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeComplex128
	config.M = 16
	config.EfConstruction = 100
	config.Metric = "l2"

	idx := NewArrowHNSW(nil, config)

	vecsC128 := make([][]complex128, count)
	for i := 0; i < count; i++ {
		vecsC128[i] = make([]complex128, dims)
		for j := 0; j < dims; j++ {
			re := rand.Float64()*2 - 1
			im := rand.Float64()*2 - 1
			vecsC128[i][j] = complex(re, im)
		}
	}

	for i := 0; i < count; i++ {
		err := idx.InsertWithVector(uint32(i), vecsC128[i], -1)
		require.NoError(t, err)
	}

	// Read Back
	valAny, err := idx.getVectorAny(0)
	require.NoError(t, err)
	valC128, ok := valAny.([]complex128)
	require.True(t, ok, "Expected []complex128 storage")
	assert.Equal(t, vecsC128[0], valC128)

	// Search
	results, err := idx.Search(context.Background(), vecsC128[0], 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)

	assert.Equal(t, uint32(0), uint32(results[0].ID))
	assert.True(t, math.Abs(float64(results[0].Dist)) < 1e-5)
}

func TestArrowHNSW_Complex_FlattenedQuery(t *testing.T) {
	dims := 4 // 4 complex numbers
	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = lbtypes.VectorTypeComplex128
	config.Metric = "l2"

	idx := NewArrowHNSW(nil, config)

	// Create test vector
	vec := []complex128{1 + 1i, 2 + 2i, 3 + 3i, 4 + 4i}
	err := idx.InsertWithVector(0, vec, -1)
	require.NoError(t, err)

	// Flattened Query: 1, 1, 2, 2, 3, 3, 4, 4 (floats)
	// Server JSON parser often gives []float64 (flattened)
	// resolveHNSWComputer currently handles []float32 conversion.
	// We need to pass []float32 as flattened representation of Complex128.
	// 4 complex128 = 8 floats.
	flatQuery := []float32{1, 1, 2, 2, 3, 3, 4, 4}

	results, err := idx.Search(context.Background(), flatQuery, 1, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, uint32(0), uint32(results[0].ID))
	require.InDelta(t, 0.0, results[0].Dist, 1e-5)
}
