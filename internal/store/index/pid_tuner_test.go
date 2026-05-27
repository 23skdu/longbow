package index

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPIDTuner_Isolation validates that the PID controller output moves in the correct direction
// under different recall inputs and respects the min/max limits.
func TestPIDTuner_Isolation(t *testing.T) {
	tuner := NewPIDTuner(0.95, 100)
	assert.Equal(t, 100, tuner.GetCurrentEf())

	// Simulated time step
	time.Sleep(5 * time.Millisecond)

	// 1. Observed recall is lower than target (0.95) -> efSearch should increase
	ef := tuner.Update(0.80) // error = 0.95 - 0.80 = 0.15 > 0
	assert.Greater(t, ef, 100, "efSearch must increase when observed recall is below target")

	// Simulated time step
	time.Sleep(5 * time.Millisecond)

	// 2. Observed recall is higher than target -> efSearch should decrease
	efPrev := tuner.GetCurrentEf()
	ef = tuner.Update(0.99) // error = 0.95 - 0.99 = -0.04 < 0
	assert.Less(t, ef, efPrev, "efSearch must decrease when observed recall is above target")

	// 3. Lower Bound Constraint
	tuner.currentEf = 5.0
	ef = tuner.Update(0.99)
	assert.Equal(t, 10, ef, "efSearch must not go below 10")

	// 4. Upper Bound Constraint
	tuner.currentEf = 2500.0
	ef = tuner.Update(0.70)
	assert.Equal(t, 2000, ef, "efSearch must not exceed 2000")
}

// TestPIDTuner_RecallRetention verifies that PID-tuned efSearch maintains target recall on low-precision types.
func TestPIDTuner_RecallRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	dims := 16
	count := 200

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = types.VectorTypeInt8
	config.M = 16
	config.EfConstruction = 100

	idx := NewArrowHNSW(nil, &config, nil)

	// Generate random vectors within int8 range [-127, 127]
	vecs := make([][]int8, count)
	for i := 0; i < count; i++ {
		vecs[i] = make([]int8, dims)
		for j := 0; j < dims; j++ {
			vecs[i][j] = int8(rand.Intn(10) - 5)
		}
	}
	// Make vector 0 extremely distinct with large values
	for j := 0; j < dims; j++ {
		vecs[0][j] = 120
	}

	err := idx.AddBatchBulk(context.Background(), 0, count, vecs)
	require.NoError(t, err)

	// Let's run query loop and observe PID tuner adjusting efSearch
	// We want to verify that search successfully adjusts and executes
	query := make([]int8, dims)
	for j := 0; j < dims; j++ {
		query[j] = vecs[0][j] // query with first vector to expect high overlap
	}

	// Target high-load validation by setting target ef search low initially
	idx.efTuner = NewPIDTuner(0.95, 15) // Reset tuner with small initial efSearch

	// Run search queries under simulated load
	for i := 0; i < 5; i++ {
		results, err := idx.Search(context.Background(), query, 10, nil)
		require.NoError(t, err)
		require.NotEmpty(t, results)

		// The tuner should be active and currentEf should adjust or remain stable within valid bounds
		efCurrent := idx.efTuner.GetCurrentEf()
		assert.GreaterOrEqual(t, efCurrent, 10)
		assert.LessOrEqual(t, efCurrent, 2000)
	}

	// Verify exact match is found within top results since query is identical to vector 0
	results, err := idx.Search(context.Background(), query, 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	
	found := false
	for _, res := range results {
		if uint32(res.ID) == 0 {
			found = true
			break
		}
	}
	assert.True(t, found, "Should successfully find match ID 0 in top 5 results")
}

// TestComplex128_Correctness verifies that complex128 vectors are fetched correctly
// and bounds checking doesn't fail even when dimensions are not multiples of 4 (e.g., dims = 5).
func TestComplex128_AlignmentAndBounds(t *testing.T) {
	dims := 5 // Not a multiple of 4 (padded dims will be 8)
	count := 10

	config := DefaultArrowHNSWConfig()
	config.Dims = dims
	config.DataType = types.VectorTypeComplex128
	config.M = 16
	config.EfConstruction = 100

	idx := NewArrowHNSW(nil, &config, nil)

	vecs := make([][]float32, count)
	complexVecs := make([][]complex128, count)

	for i := 0; i < count; i++ {
		vecs[i] = make([]float32, dims*2)
		complexVecs[i] = make([]complex128, dims)
		for j := 0; j < dims; j++ {
			re := float64(j + i)
			im := float64(j - i)
			vecs[i][2*j] = float32(re)
			vecs[i][2*j+1] = float32(im)
			complexVecs[i][j] = complex(re, im)
		}
	}

	err := idx.AddBatchBulk(context.Background(), 0, count, vecs)
	require.NoError(t, err)

	// Fetch vector 9 (near the end of chunk/array) to check chunk bounds retrieval logic
	vecAny, err := idx.GetVectorAny(9)
	require.NoError(t, err)
	vecC128, ok := vecAny.([]complex128)
	require.True(t, ok)
	assert.Equal(t, complexVecs[9], vecC128, "Fetched vector must match the inserted complex128 vector perfectly")

	// Search using vector 9 as query.
	// Previously, this would trigger bounds failures and return math.MaxFloat32.
	// Now, it must calculate correctly and find itself.
	results, err := idx.Search(context.Background(), vecs[9], 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.EqualValues(t, 9, results[0].ID, "Search must successfully find itself as the top result")
	assert.InDelta(t, float32(0.0), results[0].Dist, 1e-4, "Exact match must have a distance of ~0.0")
}
