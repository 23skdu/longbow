package store

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchDistanceCompute_Basic(t *testing.T) {
	queries := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
	}
	candidates := [][]float32{
		{1.0, 0.0, 0.0},
		{1.0, 0.0, 0.0},
		{1.0, 0.0, 0.0},
	}
	results := make([]float32, len(queries))

	BatchDistanceCompute(queries, candidates, results)

	// First pair: identical, distance = 0
	assert.InDelta(t, 0.0, results[0], 0.001)
	// Second pair: orthogonal, distance = sqrt(1^2 + 1^2) = sqrt(2)
	assert.InDelta(t, 1.414214, results[1], 0.001)
	// Third pair: orthogonal, distance = sqrt(1^2 + 1^2) = sqrt(2)
	assert.InDelta(t, 1.414214, results[2], 0.001)
}

func TestBatchDistanceCompute_Empty(t *testing.T) {
	queries := [][]float32{}
	candidates := [][]float32{}
	results := []float32{}

	// Should not panic
	BatchDistanceCompute(queries, candidates, results)
	assert.Empty(t, results)
}

func TestBatchDistanceCompute_SinglePair(t *testing.T) {
	queries := [][]float32{{1.0, 2.0, 3.0}}
	candidates := [][]float32{{4.0, 5.0, 6.0}}
	results := make([]float32, 1)

	BatchDistanceCompute(queries, candidates, results)

	// Distance = sqrt((4-1)^2 + (5-2)^2 + (6-3)^2) = sqrt(9 + 9 + 9) = sqrt(27) ≈ 5.196
	assert.InDelta(t, 5.196152, results[0], 0.001)
}

func TestBatchDistanceCompute_LargeBatch(t *testing.T) {
	batchSize := 1000
	dim := 128

	queries := make([][]float32, batchSize)
	candidates := make([][]float32, batchSize)
	results := make([]float32, batchSize)

	for i := 0; i < batchSize; i++ {
		queries[i] = make([]float32, dim)
		candidates[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			queries[i][j] = rand.Float32()
			candidates[i][j] = rand.Float32()
		}
	}

	// Should not panic
	BatchDistanceCompute(queries, candidates, results)

	// All results should be non-negative
	for i := range results {
		assert.GreaterOrEqual(t, results[i], float32(0))
	}
}

func TestBatchDistanceCompute_IdenticalVectors(t *testing.T) {
	dim := 64
	numPairs := 50

	vectors := make([][]float32, numPairs)
	for i := 0; i < numPairs; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	queries := vectors
	candidates := vectors
	results := make([]float32, numPairs)

	BatchDistanceCompute(queries, candidates, results)

	// All distances should be 0 (identical vectors)
	for i := range results {
		assert.InDelta(t, 0.0, results[i], 0.001, "Pair %d should have distance 0", i)
	}
}

func TestBatchDistanceCompute_OrthogonalVectors(t *testing.T) {
	dim := 32
	numPairs := 20

	queries := make([][]float32, numPairs)
	candidates := make([][]float32, numPairs)

	for i := 0; i < numPairs; i++ {
		queries[i] = make([]float32, dim)
		candidates[i] = make([]float32, dim)
		// Create orthogonal vectors
		for j := 0; j < dim; j++ {
			queries[i][j] = float32(j + 1)
			candidates[i][j] = float32(dim - j)
		}
	}
	results := make([]float32, numPairs)

	BatchDistanceCompute(queries, candidates, results)

	// Each pair should have non-zero distance
	for i := range results {
		assert.Greater(t, results[i], float32(0), "Pair %d should have non-zero distance", i)
	}
}

func TestBatchDistanceCompute_MismatchedDimensions(t *testing.T) {
	queries := [][]float32{{1.0, 2.0, 3.0}}
	candidates := [][]float32{{1.0, 2.0}} // Different dimension
	results := make([]float32, 1)

	// Should handle dimension mismatch gracefully (returns MaxFloat32 for mismatched pair)
	BatchDistanceCompute(queries, candidates, results)
	assert.Equal(t, float32(3.4028235e+38), results[0]) // math.MaxFloat32
}

func TestBatchDistanceCompute_MismatchedLengths(t *testing.T) {
	queries := [][]float32{{1.0}, {2.0}}
	candidates := [][]float32{{3.0}}
	results := make([]float32, 1)

	// Should panic due to length mismatch
	assert.Panics(t, func() {
		BatchDistanceCompute(queries, candidates, results)
	})
}

func FuzzBatchDistanceCompute(f *testing.F) {
	f.Add(10, 64) // batchSize, dim
	f.Add(100, 128)
	f.Add(1000, 384)

	f.Fuzz(func(t *testing.T, batchSize int, dim int) {
		if batchSize <= 0 || batchSize > 10000 {
			t.Skip()
		}
		if dim <= 0 || dim > 2048 {
			t.Skip()
		}

		queries := make([][]float32, batchSize)
		candidates := make([][]float32, batchSize)
		results := make([]float32, batchSize)

		for i := 0; i < batchSize; i++ {
			queries[i] = make([]float32, dim)
			candidates[i] = make([]float32, dim)
			for j := 0; j < dim; j++ {
				queries[i][j] = rand.Float32()
				candidates[i][j] = rand.Float32()
			}
		}

		// Should not panic
		BatchDistanceCompute(queries, candidates, results)

		// Verify all results are non-negative
		for i := range results {
			if results[i] < 0 {
				t.Errorf("Negative distance at index %d: %f", i, results[i])
			}
		}
	})
}
