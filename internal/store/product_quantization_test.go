package store


import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPQEncoder_TrainAndEncode(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Dimensions: 128
	// M: 4 (Sub-vectors of length 32)
	// K: 256
	dims := 128
	m := 4
	k := 256
	numSamples := 1000

	vectors := make([][]float32, numSamples)
	for i := 0; i < numSamples; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rng.Float32()
		}
	}

	// Create Config
	cfg := &PQConfig{
		Dimensions:    dims,
		NumSubVectors: m,
		NumCentroids:  k,
	}

	encoder, err := TrainPQEncoder(cfg, vectors, 10)
	require.NoError(t, err)
	require.NotNil(t, encoder)

	assert.Equal(t, m, len(encoder.Codebooks))
	assert.Equal(t, k, len(encoder.Codebooks[0]))
	assert.Equal(t, dims/m, len(encoder.Codebooks[0][0]))

	// Test Encoding
	vec := vectors[0]
	code := encoder.Encode(vec)
	assert.Equal(t, m, len(code))

	// Test Decoding
	decoded := encoder.Decode(code)
	assert.Equal(t, dims, len(decoded))

	// Check reconstruction error (MSE)
	// Since data is random uniform [0,1], MSE shouldn't be massive, but depends on K.
	// Just check it's not zero and not Infinity.
	var mse float32
	for i := 0; i < dims; i++ {
		d := vec[i] - decoded[i]
		mse += d * d
	}
	mse /= float32(dims)
	t.Logf("MSE: %f", mse)
	assert.Less(t, mse, float32(0.1)) // Generous threshold for random data
}

func TestPQEncoder_Serialization(t *testing.T) {
	cfg := PQConfig{
		Dimensions:    16,
		NumSubVectors: 2,
		NumCentroids:  4, // Small K for test
	}
	enc, _ := NewPQEncoder(cfg)

	// Populate dummy codebooks
	enc.Codebooks[0] = make([][]float32, 4)
	enc.Codebooks[1] = make([][]float32, 4)
	for i := 0; i < 4; i++ {
		enc.Codebooks[0][i] = []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0}
		enc.Codebooks[1][i] = []float32{8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0}
	}

	data := enc.Serialize()
	assert.NotEmpty(t, data)

	enc2, err := DeserializePQEncoder(data)
	require.NoError(t, err)

	assert.Equal(t, enc.config, enc2.config)
	assert.Equal(t, enc.Codebooks, enc2.Codebooks)
}

func TestPQ_ADC_LUT(t *testing.T) {
	// Simple manually constructed case
	// D=4, M=2, K=2. Sub-dim=2.
	cfg := PQConfig{Dimensions: 4, NumSubVectors: 2, NumCentroids: 2}
	enc, _ := NewPQEncoder(cfg)

	// Subspace 0 Codebook
	// C0_0 = [0, 0]
	// C0_1 = [10, 10]
	enc.Codebooks[0] = [][]float32{{0, 0}, {10, 10}}

	// Subspace 1 Codebook
	// C1_0 = [0, 0]
	// C1_1 = [5, 5]
	enc.Codebooks[1] = [][]float32{{0, 0}, {5, 5}}

	query := []float32{2, 2, 6, 6}
	// Query Sub 0: [2, 2]
	// Dist to C0_0: 2^2 + 2^2 = 8
	// Dist to C0_1: (2-10)^2 + (2-10)^2 = 64 + 64 = 128

	// Query Sub 1: [6, 6]
	// Dist to C1_0: 6^2 + 6^2 = 36 + 36 = 72
	// Dist to C1_1: (6-5)^2 + (6-5)^2 = 1 + 1 = 2

	lut := enc.ComputeDistanceTableFlat(query)
	// LUT layout: [M][K] flattened -> [Sub0_C0, Sub0_C1, Sub1_C0, Sub1_C1]
	// Expected: [8, 128, 72, 2]

	assert.Equal(t, float32(8.0), lut[0])
	assert.Equal(t, float32(128.0), lut[1])
	assert.Equal(t, float32(72.0), lut[2])
	assert.Equal(t, float32(2.0), lut[3])
}
