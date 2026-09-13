//go:build gpu && linux && cuda

package cuda

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// euclideanComplex128 computes L2 distance between two complex128 vectors (CPU reference).
func euclideanComplex128(a, b []float32) float32 {
	var sum float64
	for i := 0; i < len(a); i += 2 {
		dRe := float64(a[i]) - float64(b[i])
		dIm := float64(a[i+1]) - float64(b[i+1])
		sum += dRe*dRe + dIm*dIm
	}
	return float32(math.Sqrt(sum))
}

// euclideanComplex64 computes L2 distance between two complex64 vectors (CPU reference).
func euclideanComplex64(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a); i += 2 {
		dRe := a[i] - b[i]
		dIm := a[i+1] - b[i+1]
		sum += dRe*dRe + dIm*dIm
	}
	return float32(math.Sqrt(float64(sum)))
}

func TestCUDAIndex_SearchComplex128(t *testing.T) {
	numComplex := 64 // number of complex elements per vector
	dim := numComplex * 2
	count := 500
	k := 10

	config := types.GPUConfig{
		DeviceID:  0,
		MaxMemory: 1024 * 1024 * 1024,
		Dimension: dim,
	}

	idx, err := NewCUDAIndex(config)
	require.NoError(t, err)
	defer idx.Close()

	cudaIdx := idx.(*CUDAIndex)

	// Generate random complex128 vectors (interleaved float32 pairs)
	rng := rand.New(rand.NewSource(42))
	vectors := make([]float32, count*dim)
	for i := 0; i < count*dim; i += 2 {
		vectors[i] = float32(rng.NormFloat64())
		vectors[i+1] = float32(rng.NormFloat64())
	}

	ids := make([]int64, count)
	for i := range ids {
		ids[i] = int64(i)
	}

	err = cudaIdx.Add(ids, vectors)
	require.NoError(t, err)
	err = cudaIdx.Flush()
	require.NoError(t, err)

	// Use a known vector as query
	query := vectors[:dim]

	results, distances, err := idx.SearchComplex128(query, k)
	require.NoError(t, err)
	assert.Len(t, results, k)
	assert.Len(t, distances, k)

	// Compute CPU reference distances
	type scored struct {
		id   int64
		dist float32
		pos  int
	}
	cpuResults := make([]scored, count)
	for i := 0; i < count; i++ {
		cpuResults[i] = scored{
			id:   int64(i),
			dist: euclideanComplex128(query, vectors[i*dim:(i+1)*dim]),
			pos:  i,
		}
	}
	sort.Slice(cpuResults, func(i, j int) bool {
		return cpuResults[i].dist < cpuResults[j].dist
	})

	// Verify top-k IDs match CPU reference
	for i := 0; i < k; i++ {
		assert.Equal(t, cpuResults[i].id, results[i],
			"result %d: expected ID %d, got %d", i, cpuResults[i].id, results[i])
		assert.InDelta(t, cpuResults[i].dist, distances[i], 0.01,
			"result %d: expected dist %.6f, got %.6f", i, cpuResults[i].dist, distances[i])
	}

	// Verify first result is the query vector itself (distance ~0)
	assert.InDelta(t, float32(0), distances[0], 0.001,
		"nearest neighbor should be the query vector itself")
}

func TestCUDAIndex_SearchComplex64(t *testing.T) {
	numComplex := 64 // number of complex elements per vector
	dim := numComplex * 2
	count := 500
	k := 10

	config := types.GPUConfig{
		DeviceID:  0,
		MaxMemory: 1024 * 1024 * 1024,
		Dimension: dim,
	}

	idx, err := NewCUDAIndex(config)
	require.NoError(t, err)
	defer idx.Close()

	cudaIdx := idx.(*CUDAIndex)

	// Generate random complex64 vectors (interleaved float32 pairs)
	rng := rand.New(rand.NewSource(42))
	vectors := make([]float32, count*dim)
	for i := 0; i < count*dim; i += 2 {
		vectors[i] = float32(rng.NormFloat64())
		vectors[i+1] = float32(rng.NormFloat64())
	}

	ids := make([]int64, count)
	for i := range ids {
		ids[i] = int64(i)
	}

	err = cudaIdx.Add(ids, vectors)
	require.NoError(t, err)
	err = cudaIdx.Flush()
	require.NoError(t, err)

	// Use a known vector as query
	query := vectors[:dim]

	results, distances, err := idx.SearchComplex64(float32ToUint16(query), k)
	require.NoError(t, err)
	assert.Len(t, results, k)
	assert.Len(t, distances, k)

	// Compute CPU reference distances
	type scored struct {
		id   int64
		dist float32
		pos  int
	}
	cpuResults := make([]scored, count)
	for i := 0; i < count; i++ {
		cpuResults[i] = scored{
			id:   int64(i),
			dist: euclideanComplex64(query, vectors[i*dim:(i+1)*dim]),
			pos:  i,
		}
	}
	sort.Slice(cpuResults, func(i, j int) bool {
		return cpuResults[i].dist < cpuResults[j].dist
	})

	// Verify top-k IDs match CPU reference
	for i := 0; i < k; i++ {
		assert.Equal(t, cpuResults[i].id, results[i],
			"result %d: expected ID %d, got %d", i, cpuResults[i].id, results[i])
		assert.InDelta(t, cpuResults[i].dist, distances[i], 0.01,
			"result %d: expected dist %.6f, got %.6f", i, cpuResults[i].dist, distances[i])
	}
}

func TestCUDAIndex_Complex128_NearestNeighborIsSelf(t *testing.T) {
	numComplex := 32
	dim := numComplex * 2
	count := 100

	config := types.GPUConfig{
		DeviceID:  0,
		MaxMemory: 1024 * 1024 * 1024,
		Dimension: dim,
	}

	idx, err := NewCUDAIndex(config)
	require.NoError(t, err)
	defer idx.Close()

	cudaIdx := idx.(*CUDAIndex)

	rng := rand.New(rand.NewSource(123))
	vectors := make([]float32, count*dim)
	for i := 0; i < count*dim; i += 2 {
		vectors[i] = float32(rng.NormFloat64())
		vectors[i+1] = float32(rng.NormFloat64())
	}

	ids := make([]int64, count)
	for i := range ids {
		ids[i] = int64(i + 1000) // non-zero IDs
	}

	err = cudaIdx.Add(ids, vectors)
	require.NoError(t, err)
	err = cudaIdx.Flush()
	require.NoError(t, err)

	// Query with vector at index 50
	queryIdx := 50
	query := vectors[queryIdx*dim : (queryIdx+1)*dim]

	results, distances, err := idx.SearchComplex128(query, 1)
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, ids[queryIdx], results[0],
		"nearest neighbor should be the query vector itself")
	assert.InDelta(t, float32(0), distances[0], 0.001,
		"distance to self should be ~0")
}

// float32ToUint16 converts float32 slice to uint16 slice for complex64 search.
// Each float32 is reinterpreted as uint16 via float16 encoding.
func float32ToUint16(f32 []float32) []uint16 {
	u16 := make([]uint16, len(f32))
	for i, v := range f32 {
		// Encode as float16 then extract bits
		f16 := float16Encode(v)
		u16[i] = f16
	}
	return u16
}

func float16Encode(f float32) uint16 {
	// Simple IEEE 754 float32 to float16 conversion
	bits := math.Float32bits(f)
	sign := uint16((bits >> 16) & 0x8000)
	exponent := int32((bits >> 23) & 0xFF)
	mantissa := bits & 0x007FFFFF

	if exponent == 0xFF {
		// Inf or NaN
		if mantissa != 0 {
			return sign | 0x7E00 // NaN
		}
		return sign | 0x7C00 // Inf
	}
	if exponent == 0 {
		// Denormalized
		return sign // flush to zero
	}

	exponent -= 127
	if exponent > 15 {
		return sign | 0x7C00 // overflow to Inf
	}
	if exponent < -14 {
		return sign // underflow to zero
	}

	exp16 := uint16(exponent + 15)
	mant16 := uint16(mantissa >> 13)
	return sign | (exp16 << 10) | mant16
}

func TestCUDAIndex_Complex128_MultipleQueries(t *testing.T) {
	numComplex := 16
	dim := numComplex * 2
	count := 200
	k := 5

	config := types.GPUConfig{
		DeviceID:  0,
		MaxMemory: 1024 * 1024 * 1024,
		Dimension: dim,
	}

	idx, err := NewCUDAIndex(config)
	require.NoError(t, err)
	defer idx.Close()

	cudaIdx := idx.(*CUDAIndex)

	rng := rand.New(rand.NewSource(99))
	vectors := make([]float32, count*dim)
	for i := 0; i < count*dim; i += 2 {
		vectors[i] = float32(rng.NormFloat64())
		vectors[i+1] = float32(rng.NormFloat64())
	}

	ids := make([]int64, count)
	for i := range ids {
		ids[i] = int64(i)
	}

	err = cudaIdx.Add(ids, vectors)
	require.NoError(t, err)
	err = cudaIdx.Flush()
	require.NoError(t, err)

	// Run multiple queries and verify consistency
	for q := 0; q < 5; q++ {
		queryIdx := q * 40
		query := vectors[queryIdx*dim : (queryIdx+1)*dim]

		results, distances, err := idx.SearchComplex128(query, k)
		require.NoError(t, err)
		assert.Len(t, results, k)

		// First result should be the query vector itself
		assert.Equal(t, ids[queryIdx], results[0],
			"query %d: nearest neighbor should be the query vector itself", q)
		assert.InDelta(t, float32(0), distances[0], 0.001,
			"query %d: distance to self should be ~0", q)

		// Distances should be monotonically non-decreasing
		for i := 1; i < len(distances); i++ {
			assert.GreaterOrEqual(t, distances[i], distances[i-1],
				"query %d: distances should be sorted", q)
		}
	}
}
