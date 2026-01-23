package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchDistanceCompute_EmptyInputs(t *testing.T) {
	queries := [][]float32{}
	candidates := [][]float32{}
	results := make([]float32, 0)

	BatchDistanceCompute(queries, candidates, results)
}

func TestBatchDistanceCompute_ValidInputs(t *testing.T) {
	queries := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
	}
	candidates := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
	}
	results := make([]float32, 2)

	BatchDistanceCompute(queries, candidates, results)

	assert.Equal(t, float32(0.0), results[0])
	assert.Equal(t, float32(0.0), results[1])
}

func TestBatchDistanceCompute_MismatchedQueryCandidateLengths_ReturnsError(t *testing.T) {
	queries := [][]float32{
		{1.0, 0.0, 0.0},
	}
	candidates := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
	}
	results := make([]float32, 1)

	assert.PanicsWithValue(t, "hnsw2: batch distance length mismatch", func() {
		BatchDistanceCompute(queries, candidates, results)
	})
}

func TestBatchDistanceCompute_MismatchedResultsLength_ReturnsError(t *testing.T) {
	queries := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
	}
	candidates := [][]float32{
		{1.0, 0.0, 0.0},
	}
	results := make([]float32, 1)

	assert.PanicsWithValue(t, "hnsw2: batch distance length mismatch", func() {
		BatchDistanceCompute(queries, candidates, results)
	})
}

func TestBatchDistanceCompute_SingleQuery(t *testing.T) {
	queries := [][]float32{
		{1.0, 0.0, 0.0},
	}
	candidates := [][]float32{
		{0.5, 0.5, 0.0},
	}
	results := make([]float32, 1)

	BatchDistanceCompute(queries, candidates, results)

	assert.True(t, results[0] > 0, "distance should be positive")
}

func TestBatchDistanceCompute_128Dimensions(t *testing.T) {
	queries := make([][]float32, 4)
	candidates := make([][]float32, 4)
	results := make([]float32, 4)

	for i := range queries {
		queries[i] = make([]float32, 128)
		candidates[i] = make([]float32, 128)
		for j := range 128 {
			queries[i][j] = float32(i+j) * 0.01
			candidates[i][j] = float32(i+j) * 0.01
		}
	}

	BatchDistanceCompute(queries, candidates, results)

	for i, d := range results {
		assert.InDelta(t, 0.0, d, 0.001, "distance %d should be near zero for identical vectors", i)
	}
}

func TestBatchDistanceCompute_384Dimensions(t *testing.T) {
	queries := make([][]float32, 2)
	candidates := make([][]float32, 2)
	results := make([]float32, 2)

	for i := range queries {
		queries[i] = make([]float32, 384)
		candidates[i] = make([]float32, 384)
		for j := range 384 {
			queries[i][j] = float32(i+j) * 0.01
			candidates[i][j] = float32(i+j) * 0.01
		}
	}

	BatchDistanceCompute(queries, candidates, results)

	for i, d := range results {
		assert.InDelta(t, 0.0, d, 0.001, "distance %d should be near zero for identical vectors", i)
	}
}

func TestBatchDistanceCompute_VariableDimensions(t *testing.T) {
	queries := [][]float32{
		{1.0, 2.0},
		{3.0, 4.0},
	}
	candidates := [][]float32{
		{1.0, 2.0},
		{3.0, 4.0},
	}
	results := make([]float32, 2)

	BatchDistanceCompute(queries, candidates, results)

	assert.Equal(t, float32(0.0), results[0])
	assert.Equal(t, float32(0.0), results[1])
}

func TestBatchDistanceCompute_ZeroVectors(t *testing.T) {
	queries := [][]float32{
		{0.0, 0.0, 0.0},
	}
	candidates := [][]float32{
		{0.0, 0.0, 0.0},
	}
	results := make([]float32, 1)

	BatchDistanceCompute(queries, candidates, results)

	assert.True(t, results[0] >= 0, "distance should be non-negative")
}

func TestBatchDistanceCompute_MixedDimensions(t *testing.T) {
	queries := make([][]float32, 3)
	candidates := make([][]float32, 3)
	results := make([]float32, 3)

	queries[0] = []float32{1.0, 0.0}
	candidates[0] = []float32{1.0, 0.0}

	queries[1] = []float32{1.0, 2.0, 3.0, 4.0}
	candidates[1] = []float32{1.0, 2.0, 3.0, 4.0}

	queries[2] = make([]float32, 256)
	candidates[2] = make([]float32, 256)
	for j := range 256 {
		queries[2][j] = float32(j) * 0.01
		candidates[2][j] = float32(j) * 0.01
	}

	BatchDistanceCompute(queries, candidates, results)

	for i, d := range results {
		assert.InDelta(t, 0.0, d, 0.001, "distance %d should be near zero for identical vectors", i)
	}
}

func TestBatchDistanceCompute_PanicIsRecoverable(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			assert.Equal(t, "hnsw2: batch distance length mismatch", r)
		} else {
			t.Error("expected panic but did not panic")
		}
	}()

	queries := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
	}
	candidates := [][]float32{
		{1.0, 0.0, 0.0},
	}
	results := make([]float32, 1)

	BatchDistanceCompute(queries, candidates, results)
}
