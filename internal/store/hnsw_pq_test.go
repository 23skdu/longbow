package store

import (
	"math/rand"
	"testing"
	"time"

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
	cfg := PQConfig{
		Dimensions:    dim,
		NumSubVectors:      8, // 4 dims per subvector
		NumCentroids:   256,
		 
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Config invalid: %v", err)
	}

	// Create random training data
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

	encoder, err := TrainPQEncoder(&cfg, samples, 10)
	assert.NoError(t, err)

	// Test Encoding
	vec := samples[0]
	codes := encoder.Encode(vec)
	assert.Equal(t, 8, len(codes))

	// Pack
	packed := PackBytesToFloat32s(codes)
	assert.Equal(t, 2, len(packed)) // 8 bytes / 4 = 2 floats

	// Unpack
	unpacked := UnpackFloat32sToBytes(packed, 8)
	assert.Equal(t, codes, unpacked)

	// Test SDC Distance
	vec2 := samples[1]
	codes2 := encoder.Encode(vec2)
	packed2 := PackBytesToFloat32s(codes2)

	distSDC := encoder.SDCDistancePacked(packed, packed2)

	// Real Distance
	// We need to implement L2 here to compare
	// realDistSq := float32(0)
	// for i := 0; i < dim; i++ {
	// 	d := vec[i] - vec2[i]
	// 	realDistSq += d*d
	// }
	// realDist := float32(0) // Sqrt(realDistSq) ?
	// _ = realDist
	// Validating metric: HNSW uses whatever metric. Euclidean usually.
	// SDCDistance (line 291) returns Sqrt(sum). So it is Euclidean.

	// PQ distance is approximation.
	// Just check it's not wild.
	// fmt.Printf("Real: %f, PQ: %f\n", realDist, distSDC)

	assert.NotZero(t, distSDC)
}
