package simd

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQ8Adaptive_CalibrationAndReconstruction(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	dims := 128
	numVecs := 100

	// Generate synthetic skewed vectors where some dimensions have large variance and others tiny
	vectors := make([][]float32, numVecs)
	for i := range vectors {
		vec := make([]float32, dims)
		for j := range vec {
			scale := float32(j+1) * 0.1 // channel-dependent scale
			vec[j] = (rng.Float32()*2.0 - 1.0) * scale
		}
		vectors[i] = vec
	}

	calib, err := CalibrateSQ8Channels(vectors, dims)
	require.NoError(t, err)
	require.NotNil(t, calib)
	assert.Equal(t, dims, calib.Dims)

	// Test single vector quantization and dequantization
	testVec := vectors[0]
	quantized := make([]byte, dims)
	QuantizeSQ8Adaptive(testVec, quantized, calib)

	reconstructed := make([]float32, dims)
	DequantizeSQ8Adaptive(quantized, reconstructed, calib)

	// Verify reconstruction error per channel is bounded by quantization step size
	for j := 0; j < dims; j++ {
		step := (calib.Maxs[j] - calib.Mins[j]) / 255.0
		diff := float32(testVec[j] - reconstructed[j])
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, step*1.1+1e-5, "reconstruction error on channel %d exceeded step size", j)
	}

	// Verify distance fidelity
	vecA := vectors[0]
	vecB := vectors[1]

	quantA := make([]byte, dims)
	quantB := make([]byte, dims)
	QuantizeSQ8Adaptive(vecA, quantA, calib)
	QuantizeSQ8Adaptive(vecB, quantB, calib)

	adaptiveDist := EuclideanDistanceSQ8Adaptive(quantA, quantB, calib)

	// Compare with true squared Euclidean distance
	var trueDist float32
	for j := 0; j < dims; j++ {
		d := vecA[j] - vecB[j]
		trueDist += d * d
	}

	// Relative error should be < 2%
	relError := (adaptiveDist - trueDist) / trueDist
	if relError < 0 {
		relError = -relError
	}
	assert.Less(t, relError, float32(0.02), "relative distance error too high: %f", relError)
}
