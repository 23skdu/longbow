package pq

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPQEncoder_Configuration(t *testing.T) {
	_, err := NewPQEncoder(128, 16, 256)
	assert.NoError(t, err)

	_, err = NewPQEncoder(128, 10, 256) // 128 is not divisible by 10
	assert.Error(t, err)
}

func TestPQEncoder_TrainAndEncode(t *testing.T) {
	dims := 32
	M := 4
	K := 256
	numSamples := 1000

	// Generate random data
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	data := make([][]float32, numSamples)
	for i := range data {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rng.Float32()
		}
		data[i] = vec
	}

	encoder, err := NewPQEncoder(dims, M, K)
	require.NoError(t, err)

	// Training should fail strictly because it's not implemented yet
	// But once implemented, it should succeed
	err = encoder.Train(data)
	if err != nil && err.Error() == "not implemented" {
		t.Skip("Train not implemented yet")
	}
	require.NoError(t, err)

	// Test Encoding & Decoding & Quality
	vec := data[0]
	codes, err := encoder.Encode(vec)
	require.NoError(t, err)
	assert.Equal(t, M, len(codes))

	rec, err := encoder.Decode(codes)
	require.NoError(t, err)
	assert.Equal(t, dims, len(rec))

	// Calculate MSE
	var mse float32
	for i := range vec {
		diff := vec[i] - rec[i]
		mse += diff * diff
	}
	mse /= float32(dims)
	t.Logf("MSE: %f", mse)

	// For random uniform [0,1], variance is 1/12 ~= 0.083.
	// With 256 centroids per subspace, we expect much lower error.
	assert.Less(t, mse, float32(0.05), "MSE should be reasonably low")
}
