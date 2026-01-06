package store


import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchDistanceComputer_ComputeL2Distances(t *testing.T) {
	mem := memory.NewGoAllocator()
	dim := 128

	computer := NewBatchDistanceComputer(mem, dim)

	// Create test vectors
	query := make([]float32, dim)
	for i := range query {
		query[i] = rand.Float32()
	}

	candidates := make([][]float32, 10)
	for i := range candidates {
		candidates[i] = make([]float32, dim)
		for j := range candidates[i] {
			candidates[i][j] = rand.Float32()
		}
	}

	// Compute distances using Arrow
	distances, err := computer.ComputeL2Distances(query, candidates)
	require.NoError(t, err)
	require.Len(t, distances, 10)

	// Verify against reference implementation
	for i, candidate := range candidates {
		expected := l2Distance(query, candidate)
		assert.InDelta(t, expected, distances[i], 1e-5, "Distance mismatch for candidate %d", i)
	}
}

func TestBatchDistanceComputer_EmptyCandidates(t *testing.T) {
	mem := memory.NewGoAllocator()
	computer := NewBatchDistanceComputer(mem, 128)

	query := make([]float32, 128)
	distances, err := computer.ComputeL2Distances(query, [][]float32{})

	require.NoError(t, err)
	assert.Empty(t, distances)
}

func TestBatchDistanceComputer_DimensionMismatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	computer := NewBatchDistanceComputer(mem, 128)

	query := make([]float32, 64) // Wrong dimension
	candidates := [][]float32{make([]float32, 128)}

	_, err := computer.ComputeL2Distances(query, candidates)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
}

func TestBatchDistanceComputer_ShouldUseBatchCompute(t *testing.T) {
	mem := memory.NewGoAllocator()
	computer := NewBatchDistanceComputer(mem, 128)

	// Small batches should use SIMD
	assert.False(t, computer.ShouldUseBatchCompute(10))
	assert.False(t, computer.ShouldUseBatchCompute(31))

	// Large batches should use Arrow compute
	assert.True(t, computer.ShouldUseBatchCompute(32))
	assert.True(t, computer.ShouldUseBatchCompute(100))
	assert.True(t, computer.ShouldUseBatchCompute(1000))
}

func BenchmarkBatchDistanceComputer_Arrow(b *testing.B) {
	mem := memory.NewGoAllocator()
	dim := 384
	computer := NewBatchDistanceComputer(mem, dim)

	query := make([]float32, dim)
	for i := range query {
		query[i] = rand.Float32()
	}

	// Test different batch sizes
	for _, batchSize := range []int{10, 50, 100, 500, 1000} {
		candidates := make([][]float32, batchSize)
		for i := range candidates {
			candidates[i] = make([]float32, dim)
			for j := range candidates[i] {
				candidates[i][j] = rand.Float32()
			}
		}

		b.Run(fmt.Sprintf("Arrow_Batch%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = computer.ComputeL2Distances(query, candidates)
			}
		})
	}
}

func BenchmarkBatchDistanceComputer_SIMD(b *testing.B) {
	mem := memory.NewGoAllocator()
	dim := 384
	computer := NewBatchDistanceComputer(mem, dim)

	query := make([]float32, dim)
	for i := range query {
		query[i] = rand.Float32()
	}

	// Test different batch sizes
	for _, batchSize := range []int{10, 50, 100, 500, 1000} {
		candidates := make([][]float32, batchSize)
		for i := range candidates {
			candidates[i] = make([]float32, dim)
			for j := range candidates[i] {
				candidates[i][j] = rand.Float32()
			}
		}

		b.Run(fmt.Sprintf("SIMD_Batch%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = computer.ComputeL2DistancesSIMDFallback(query, candidates)
			}
		})
	}
}
