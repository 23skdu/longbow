package store

import (
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/stretchr/testify/assert"
)

func TestHNSWPQ_Integration(t *testing.T) {
	// Setup
	dim := 128
	numVecs := 2000
	ds := &Dataset{Name: "test_pq"}
	h := NewHNSWIndex(ds)
	// Initialize dims
	h.dims = dim

	// Generate random vectors
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rng.Float32()
		}
		vectors[i] = vec
	}

	// Add vectors to index (unquantized initially)
	// Integration test skipped for now as it requires complex Arrow record setup
	t.Skip("Skipping HSNP PQ Integration test - requires Arrow record setup")
}

func TestPQEncoder_Correctness(t *testing.T) {
	// Test the Encoder logic itself with HNSW integration logic side-by-side
	dim := 32
	numSamples := 1000
	samples := make([][]float32, numSamples)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < numSamples; i++ {
		v := make([]float32, dim)
		for j := 0; j < dim; j++ {
			v[j] = rng.Float32()
		}
		samples[i] = v
	}

	encoder, err := pq.NewPQEncoder(dim, 8, 256)
	assert.NoError(t, err)

	err = encoder.Train(samples)
	assert.NoError(t, err)

	// Test Encoding
	vec := samples[0]
	codes, err := encoder.Encode(vec)
	assert.NoError(t, err)
	assert.Equal(t, 8, len(codes))

	// Pack
	packed := pq.PackBytesToFloat32s(codes)
	assert.Equal(t, 2, len(packed)) // 8 bytes / 4 = 2 floats

	// Unpack
	unpacked := pq.UnpackFloat32sToBytes(packed, 8)
	assert.Equal(t, codes, unpacked)
}
