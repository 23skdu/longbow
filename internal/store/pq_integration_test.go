package store

import (
	"context"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPQ_EndToEnd verifies the full lifecycle of Product Quantization:
// 1. Initial ingestion (uncompressed)
// 2. Training PQ and backfilling
// 3. New ingestion (compressed on the fly)
// 4. Search accuracy (Recall vs Exact)
func TestPQ_EndToEnd(t *testing.T) {
	// 1. Setup
	dims := 128
	pqM := 16 // 16 subspaces -> 8 dim per subspace
	pqK := 256
	numVecs := 1000
	trainVecs := 500

	config := DefaultArrowHNSWConfig()
	config.PQM = pqM
	config.PQK = pqK
	config.Dims = dims
	// Start disabled, enable later via TrainPQ
	config.PQEnabled = false

	// Mock Dataset/LocationStore setup
	locStore := NewChunkedLocationStore()
	// Dataset is used for GetVector fallback, but since we verified InsertWithVector stores data
	// in GraphData (VectorsChunk), we might get away with nil Dataset for HNSW operations
	// IF the vectors are present in GraphData.
	// However, getVector checks locationStore then dataset.
	// Let's rely on GraphData being populated by InsertWithVector.
	hnsw := NewArrowHNSW(nil, config, locStore)

	// 2. Generate Random Data
	rng := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility
	vectors := make([][]float32, numVecs)
	for i := 0; i < numVecs; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = float32(rng.NormFloat64())
		}
	}

	// 3. Initial Ingestion (0 to trainVecs)
	for i := 0; i < trainVecs; i++ {
		err := hnsw.InsertWithVector(uint32(i), vectors[i], int(rng.Int31n(4)))
		require.NoError(t, err)
	}

	// Verify not yet quantized
	gd := hnsw.data.Load()
	require.NotNil(t, gd)
	assert.Nil(t, gd.VectorsPQ, "VectorsPQ should be nil before training")

	// 4. Train PQ
	// We use the first batch as training data
	err := hnsw.TrainPQ(vectors[:trainVecs])
	require.NoError(t, err, "TrainPQ failed")

	// Verify Configuration Updated
	assert.True(t, hnsw.config.PQEnabled, "PQEnabled should be true after training")
	assert.NotNil(t, hnsw.pqEncoder, "PQEncoder should be initialized")

	// Verify Backfill
	gd = hnsw.data.Load()
	require.NotNil(t, gd.VectorsPQ, "VectorsPQ should be allocated after training")

	// Check a few existing vectors
	for i := 0; i < trainVecs; i++ {
		code := gd.GetVectorPQ(uint32(i))
		require.NotNil(t, code, "Backfilled vector %d should have PQ code", i)
		assert.Equal(t, pqM, len(code), "PQ code length mismatch")
	}

	// 5. New Ingestion (trainVecs to numVecs)
	for i := trainVecs; i < numVecs; i++ {
		err := hnsw.InsertWithVector(uint32(i), vectors[i], int(rng.Int31n(4)))
		require.NoError(t, err)

		// Verify Quantized immediately
		gd = hnsw.data.Load()
		code := gd.GetVectorPQ(uint32(i))
		require.NotNil(t, code, "New vector %d should have PQ code", i)
	}

	// 6. Search Verification
	// We search for random vectors from the dataset and expect the ID to be in top results
	// Recall checks
	successCount := 0
	numQueries := 50
	k := 10

	for i := 0; i < numQueries; i++ {
		queryIdx := rng.Intn(numVecs)
		queryVec := vectors[queryIdx]

		// Perform Search
		results, err := hnsw.Search(context.Background(), queryVec, k, 100, nil)
		require.NoError(t, err)

		// Check if queryIdx is in results
		found := false
		for _, res := range results {
			if uint32(res.ID) == uint32(queryIdx) {
				found = true
				break
			}
		}
		if found {
			successCount++
		}
	}

	// PQ is lossy, but retrieving the exact vector itself with HNSW and adequate M should have high recall.
	recall := float64(successCount) / float64(numQueries)
	t.Logf("PQ Recall@%d (Self-Search): %.2f", k, recall)
	assert.Greater(t, recall, 0.5, "Recall should be decent even with PQ")

	// 7. Verify Distance Approximation
	// Pick a vector and calculate exact L2 vs ADC result
	testID := uint32(0)
	testVec := vectors[0]
	code := gd.GetVectorPQ(testID)
	// We need another vector to compute distance FROM
	queryVec := vectors[1]

	// Exact Distance
	exactDist, err := simd.EuclideanDistance(queryVec, testVec)
	require.NoError(t, err)
	exactDistSq := exactDist * exactDist

	// ADC Distance
	table, err := hnsw.pqEncoder.BuildADCTable(queryVec)
	require.NoError(t, err)
	pqDist, err := hnsw.pqEncoder.ADCDistance(table, code)
	require.NoError(t, err)

	t.Logf("Exact DistSq: %.4f, PQ ADC Dist: %.4f", exactDistSq, pqDist)

	// They should be somewhat close, but hard to assert strictly without loose tolerance.
	// Just asserting no error and non-zero (unless identical).
	if exactDistSq > 0.0001 {
		assert.Greater(t, pqDist, float32(0.0))
	}
}
