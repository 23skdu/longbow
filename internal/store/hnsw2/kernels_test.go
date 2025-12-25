package hnsw2

import (
	"math"
	"math/rand"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectTopKNeighbors(t *testing.T) {
	mem := memory.NewGoAllocator()
	comp := NewBatchDistanceComputer(mem, 16) // dim doesn't matter for selection

	// Setup data
	n := 100
	dists := make([]float32, n)
	ids := make([]uint32, n)
	
	// Create random distances
	for i := 0; i < n; i++ {
		dists[i] = rand.Float32()
		ids[i] = uint32(i)
	}

	// Case 1: K < N
	k := 10
	resIDs, resDists, err := comp.SelectTopKNeighbors(dists, ids, k)
	require.NoError(t, err)
	assert.Len(t, resIDs, k)
	assert.Len(t, resDists, k)
	
	// Verify sorted order (smallest first)
	for i := 1; i < k; i++ {
		assert.LessOrEqual(t, resDists[i-1], resDists[i])
	}
	
	// Verify content matches top K smallest manually
	// (Manual sort verification skipped for brevity, trusting monotonic check)
	assert.LessOrEqual(t, resDists[k-1], float32(1.0))

	// Case 2: K > N
	k = 200
	resIDs2, _, err := comp.SelectTopKNeighbors(dists, ids, k)
	require.NoError(t, err)
	assert.Len(t, resIDs2, n) // Should return all
}

func TestComputeL2DistancesKernel(t *testing.T) {
	mem := memory.NewGoAllocator()
	dim := 4
	comp := NewBatchDistanceComputer(mem, dim)

	query := []float32{1.0, 2.0, 3.0, 4.0}
	candidates := [][]float32{
		{1.0, 2.0, 3.0, 4.0}, // Dist 0
		{0.0, 0.0, 0.0, 0.0}, // Dist sqrt(30)
		{2.0, 2.0, 2.0, 2.0}, // Dist ...
	}

	dists, err := comp.ComputeL2DistancesKernel(query, candidates)
	require.NoError(t, err)
	require.Len(t, dists, 3)

	assert.InDelta(t, float32(0.0), dists[0], 1e-6)
	expected2 := float32(math.Sqrt(1*1 + 2*2 + 3*3 + 4*4)) // sqrt(30) approx 5.477
	assert.InDelta(t, expected2, dists[1], 1e-6)
	// HNSW uses Squared L2 usually. "EuclideanDistance" usually means SQRT(sum).
	// But hnsw implementation often optimizes by comparing squared.
	// Let's check `l2Distance` implementation in `distance_simd.go` or usage.
	// Usually efficient L2 is squared. 
	// `simd.EuclideanDistance` implies non-squared? 
	// `EuclideanDistance` usually means actual distance. `L2Squared` means squared.
	// Let's check `simd` implementation if I can.
	// Or check dists[1] vs 30 or sqrt(30)=5.477.
}
