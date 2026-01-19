package store

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSQ8_HighDim_Loss validates the quantization error of SQ8 on high-dimensional vectors.
// It simulates OpenAI embeddings (1536d) which are typically normalized unit vectors.
func TestSQ8_HighDim_Loss(t *testing.T) {
	const (
		dims      = 1536
		numVecs   = 1000
		threshold = 0.05 // 5% relative error tolerance
	)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 1. Generate random unit vectors
	t.Logf("Generating %d vectors of dimension %d...", numVecs, dims)
	vectors := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vec := make([]float32, dims)
		var norm float64
		for j := 0; j < dims; j++ {
			v := rng.Float64()*2 - 1 // [-1, 1]
			vec[j] = float32(v)
			norm += v * v
		}
		// Normalize
		scale := 1.0 / math.Sqrt(norm)
		for j := 0; j < dims; j++ {
			vec[j] *= float32(scale)
		}
		vectors[i] = vec
	}

	// 2. Train SQ8 Encoder
	t.Log("Training SQ8 Encoder...")
	encoder, err := TrainSQ8Encoder(vectors)
	require.NoError(t, err)

	minVal, maxVal := encoder.GetBounds()
	t.Logf("Learned Bounds - Min: [sample] %v, Max: [sample] %v", minVal[:5], maxVal[:5])

	// 3. Compare Distances (Exact vs SQ8)
	var totalSqErr float64
	var totalAbsErr float64
	var totalRelErr float64
	var count int

	// Pick random pairs to compare
	numPairs := 5000
	t.Logf("Comparing distances for %d random pairs...", numPairs)

	for i := 0; i < numPairs; i++ {
		idx1 := rng.Intn(numVecs)
		idx2 := rng.Intn(numVecs)
		if idx1 == idx2 {
			continue
		}

		v1 := vectors[idx1]
		v2 := vectors[idx2]

		// Exact Distance (Euclidean)
		exactDist := euclideanDistance(v1, v2)

		// Quantized Distance
		q1 := encoder.Encode(v1)
		q2 := encoder.Encode(v2)
		sq8Dist := SQ8EuclideanDistance(q1, q2, encoder)

		// Metrics
		errDiff := float64(sq8Dist - exactDist)
		absErr := math.Abs(errDiff)

		// Avoid division by zero for relative error
		relErr := 0.0
		if exactDist > 1e-6 {
			relErr = absErr / float64(exactDist)
		}

		totalSqErr += errDiff * errDiff
		totalAbsErr += absErr
		totalRelErr += relErr
		count++
	}

	mse := totalSqErr / float64(count)
	mae := totalAbsErr / float64(count)
	meanRelErr := totalRelErr / float64(count)

	t.Logf("Validation Results:")
	t.Logf("  MSE:              %.6f", mse)
	t.Logf("  MAE:              %.6f", mae)
	t.Logf("  Mean Relative Err: %.2f%%", meanRelErr*100)

	// fail if relative error is too high
	if meanRelErr > threshold {
		t.Errorf("Mean Relative Error %.2f%% exceeds threshold %.2f%%", meanRelErr*100, threshold*100)
	} else {
		t.Logf("SUCCESS: SQ8 Error within acceptable limits.")
	}
}

// euclideanDistance computes exact float32 euclidean distance
func euclideanDistance(v1, v2 []float32) float32 {
	var sum float32
	for i := range v1 {
		diff := v1[i] - v2[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}
