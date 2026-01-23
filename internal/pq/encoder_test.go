package pq

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/simd"
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

func TestPQEncoder_EncodeConsistency(t *testing.T) {
	dims := 128
	M := 16
	K := 256
	numSamples := 500

	rng := rand.New(rand.NewSource(42))
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

	err = encoder.Train(data)
	if err != nil && err.Error() == "not implemented" {
		t.Skip("Train not implemented yet")
	}
	require.NoError(t, err)

	// Test that encoding produces consistent results
	vec := data[100]
	codes1, err := encoder.Encode(vec)
	require.NoError(t, err)

	codes2, err := encoder.Encode(vec)
	require.NoError(t, err)

	assert.Equal(t, codes1, codes2, "Encoding should be deterministic")
}

func TestPQEncoder_EncodeWithSmallK(t *testing.T) {
	// Test that sequential encoding path works for small K
	dims := 64
	M := 8
	K := 8 // Small K should use sequential path

	rng := rand.New(rand.NewSource(123))
	data := make([][]float32, 200)
	for i := range data {
		vec := make([]float32, dims)
		for j := range vec {
			vec[j] = rng.Float32()
		}
		data[i] = vec
	}

	encoder, err := NewPQEncoder(dims, M, K)
	require.NoError(t, err)

	err = encoder.Train(data)
	if err != nil && err.Error() == "not implemented" {
		t.Skip("Train not implemented yet")
	}
	require.NoError(t, err)

	vec := data[0]
	codes, err := encoder.Encode(vec)
	require.NoError(t, err)
	assert.Equal(t, M, len(codes))
}

func TestFindNearestCentroid(t *testing.T) {
	subDim := 16
	K := 256
	query := make([]float32, subDim)
	centroids := make([]float32, K*subDim)

	rng := rand.New(rand.NewSource(456))
	for i := range query {
		query[i] = rng.Float32()
	}
	for i := range centroids {
		centroids[i] = rng.Float32()
	}

	// Find nearest centroid using SIMD
	idx, dist := simd.FindNearestCentroid(query, centroids, subDim, K)

	// Verify by comparing with sequential search
	bestIdx := 0
	bestDist := float32(math.MaxFloat32)
	for i := 0; i < K; i++ {
		offset := i * subDim
		cent := centroids[offset : offset+subDim]
		var d float32
		for j := 0; j < subDim; j++ {
			diff := query[j] - cent[j]
			d += diff * diff
		}
		if d < bestDist {
			bestDist = d
			bestIdx = i
		}
	}

	assert.Equal(t, bestIdx, idx, "SIMD should find same centroid as sequential")
	// Distance may differ slightly due to computation order, but should be close
	assert.InDelta(t, float64(bestDist), float64(dist), float64(bestDist)*0.02, "Distance should be approximately equal")
}

func TestFindNearestCentroidSmallK(t *testing.T) {
	// Test small K path
	subDim := 8
	K := 8
	query := make([]float32, subDim)
	centroids := make([]float32, K*subDim)

	rng := rand.New(rand.NewSource(789))
	for i := range query {
		query[i] = rng.Float32()
	}
	for i := range centroids {
		centroids[i] = rng.Float32()
	}

	idx, _ := simd.FindNearestCentroid(query, centroids, subDim, K)

	// Verify
	bestIdx := 0
	bestDist := float32(math.MaxFloat32)
	for i := 0; i < K; i++ {
		offset := i * subDim
		cent := centroids[offset : offset+subDim]
		var d float32
		for j := 0; j < subDim; j++ {
			diff := query[j] - cent[j]
			d += diff * diff
		}
		if d < bestDist {
			bestDist = d
			bestIdx = i
		}
	}

	assert.Equal(t, bestIdx, idx)
}

func BenchmarkPQEncoder_Encode(b *testing.B) {
	testCases := []struct {
		name string
		dims int
		M    int
		K    int
	}{
		{"Small_K", 128, 16, 16},
		{"Medium_K", 128, 16, 256},
		{"Large_K", 256, 32, 256},
		{"XL_K", 384, 48, 256},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(42))
			numSamples := 1000
			data := make([][]float32, numSamples)
			for i := range data {
				vec := make([]float32, tc.dims)
				for j := range vec {
					vec[j] = rng.Float32()
				}
				data[i] = vec
			}

			encoder, err := NewPQEncoder(tc.dims, tc.M, tc.K)
			if err != nil {
				b.Fatalf("Failed to create encoder: %v", err)
			}

			err = encoder.Train(data)
			if err != nil {
				b.Fatalf("Failed to train: %v", err)
			}

			// Generate query vector
			query := make([]float32, tc.dims)
			for i := range query {
				query[i] = rng.Float32()
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, _ = encoder.Encode(query)
			}
		})
	}
}

func BenchmarkFindNearestCentroid(b *testing.B) {
	testCases := []struct {
		name   string
		subDim int
		K      int
	}{
		{"SmallK", 8, 8},
		{"MediumK", 16, 256},
		{"LargeK", 32, 256},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			rng := rand.New(rand.NewSource(42))
			query := make([]float32, tc.subDim)
			centroids := make([]float32, tc.K*tc.subDim)
			for i := range query {
				query[i] = rng.Float32()
			}
			for i := range centroids {
				centroids[i] = rng.Float32()
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				simd.FindNearestCentroid(query, centroids, tc.subDim, tc.K)
			}
		})
	}
}
