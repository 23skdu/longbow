package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIVFPQIndex_NewAndConfig(t *testing.T) {
	tests := []struct {
		name        string
		dim         int
		config      IVFPQConfig
		expectError bool
	}{
		{
			name:        "valid 128 dim",
			dim:         128,
			config:      DefaultIVFPQConfig(),
			expectError: false,
		},
		{
			name:        "valid 256 dim",
			dim:         256,
			config:      IVFPQConfig{Nlist: 512, M: 8, K: 256, Nprobe: 16},
			expectError: false,
		},
		{
			name:        "invalid zero dim",
			dim:         0,
			expectError: true,
		},
		{
			name:        "negative dim",
			dim:         -1,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := NewIVFPQIndex(tt.dim, tt.config)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, idx)
			} else {
				require.NoError(t, err)
				require.NotNil(t, idx)
				assert.Equal(t, tt.dim, idx.dim)
				assert.Equal(t, tt.config.Nlist, idx.config.Nlist)
				assert.Equal(t, tt.config.M, idx.config.M)
				assert.Equal(t, tt.config.K, idx.config.K)
			}
		})
	}
}

func TestIVFPQIndex_TrainAndAdd(t *testing.T) {
	dim := 128
	numVectors := 1000
	config := IVFPQConfig{
		Nlist:  64, // Small for test
		M:      8,
		K:      256,
		Nprobe: 8,
	}

	// Generate random test vectors
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			// Clustered around 5 centers
			center := float32(i % 5)
			vec[j] = center + float32(j)*0.001 + float32(i)*0.0001
		}
		vectors[i] = vec
	}

	// Create index
	idx, err := NewIVFPQIndex(dim, config)
	require.NoError(t, err)

	// Train
	err = idx.Train(vectors)
	require.NoError(t, err)
	assert.NotNil(t, idx.coarseCentroids)
	assert.Equal(t, config.Nlist*dim, len(idx.coarseCentroids))

	// Add vectors
	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	// Verify counts
	assert.Equal(t, numVectors, idx.Size())

	// Test dimension getter
	assert.Equal(t, uint32(dim), idx.GetDimension())
}

func TestIVFPQIndex_Search(t *testing.T) {
	dim := 32
	numVectors := 500
	config := IVFPQConfig{
		Nlist:  32,
		M:      4,
		K:      256,
		Nprobe: 4,
	}

	// Generate clustered test data
	vectors := make([][]float32, 0, numVectors)
	for cluster := 0; cluster < 5; cluster++ {
		for i := 0; i < numVectors/5; i++ {
			vec := make([]float32, dim)
			for j := 0; j < dim; j++ {
				// Vary within cluster
				vec[j] = float32(cluster) + float32(j)*0.1 + float32(i)*0.001
			}
			vectors = append(vectors, vec)
		}
	}

	// Create and populate index
	idx, err := NewIVFPQIndex(dim, config)
	require.NoError(t, err)

	err = idx.Train(vectors)
	require.NoError(t, err)

	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	// Search for a point in cluster 0
	query := make([]float32, dim)
	for j := 0; j < dim; j++ {
		query[j] = float32(0) + float32(j)*0.1
	}

	results, err := idx.SearchInternal(context.Background(), query, 10, nil, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// Verify results are sorted
	for i := 1; i < len(results); i++ {
		assert.LessOrEqual(t, results[i-1].Distance, results[i].Distance)
	}

	// Top result should be close (cluster 0)
	// The query is in cluster 0, so results should come from there
	assert.Less(t, results[0].Distance, float32(10.0),
		"Top result should have low distance to cluster 0 query")
}

func TestIVFPQIndex_SearchWithK(t *testing.T) {
	dim := 16
	numVectors := 300
	config := IVFPQConfig{
		Nlist:  8,
		M:      4,
		K:      256,
		Nprobe: 4,
	}

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = float32(i) * 0.1
		}
		vectors[i] = vec
	}

	idx, err := NewIVFPQIndex(dim, config)
	require.NoError(t, err)
	err = idx.Train(vectors)
	require.NoError(t, err)
	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	query := make([]float32, dim)
	for j := 0; j < dim; j++ {
		query[j] = 50.0 // Query in middle
	}

	// Test different k values
	for _, k := range []int{1, 5, 10, 50} {
		results, err := idx.SearchInternal(context.Background(), query, k, nil, SearchOptions{})
		require.NoError(t, err)
		assert.Len(t, results, k, "k=%d should return k results", k)
	}
}

func TestIVFPQIndex_MemoryUsage(t *testing.T) {
	dim := 128
	numVectors := 1000
	config := DefaultIVFPQConfig()
	config.Nlist = 64

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = float32(i % 10)
		}
	}

	idx, err := NewIVFPQIndex(dim, config)
	require.NoError(t, err)
	err = idx.Train(vectors)
	require.NoError(t, err)
	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	memUsage := idx.EstimateMemory()

	// Vector store: numVectors * dim * 4 bytes
	expectedMin := int64(numVectors * dim * 4)
	assert.Greater(t, memUsage, expectedMin, "memory should include vector storage")

	// PQ codes: numVectors * M bytes
	pqCodeSize := numVectors * config.M
	alsoExpected := expectedMin + int64(pqCodeSize)
	assert.Less(t, memUsage, alsoExpected+int64(1024*1024), "memory should be reasonable")
}

func TestIVFPQIndex_EmptyIndexSearch(t *testing.T) {
	dim := 64
	config := IVFPQConfig{
		Nlist:  16,
		M:      4,
		K:      256,
		Nprobe: 4,
	}

	idx, err := NewIVFPQIndex(dim, config)
	require.NoError(t, err)

	// Train with empty data - should fail
	err = idx.Train([][]float32{})
	assert.Error(t, err)

	// Add to untrainined index - should fail
	err = idx.Add(context.Background(), [][]float32{{1, 2, 3}})
	assert.Error(t, err)
}

func TestIVFPQIndex_DimensionMismatch(t *testing.T) {
	dim := 32
	config := IVFPQConfig{
		Nlist: 4,
		M:     4,
		K:     256,
	}

	idx, err := NewIVFPQIndex(dim, config)
	require.NoError(t, err)

	vectors := make([][]float32, 300)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = float32(i % 10)
		}
	}

	err = idx.Train(vectors)
	require.NoError(t, err)
	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	_, err = idx.SearchInternal(context.Background(), []float32{1, 2, 3}, 1, nil, SearchOptions{})
	assert.Error(t, err)
}

func TestIVFPQIndex_SearchQueryInCluster(t *testing.T) {
	dim := 64
	config := IVFPQConfig{
		Nlist:  3,
		M:      4,
		K:      256,
		Nprobe: 2,
	}

	clusters := [][]float32{{10, 10, 10, 10, 10, 10, 10, 10}, {-10, -10, -10, -10, -10, -10, -10, -10}, {0, 0, 0, 0, 0, 0, 0, 0}}

	vectors := make([][]float32, 0)
	for _, center := range clusters {
		for i := 0; i < 300; i++ {
			vec := make([]float32, dim)
			for j := 0; j < dim; j++ {
				vec[j] = center[j%len(center)] + float32(i%10)*0.5
			}
			vectors = append(vectors, vec)
		}
	}

	idx, err := NewIVFPQIndex(dim, config)
	require.NoError(t, err)
	err = idx.Train(vectors)
	require.NoError(t, err)
	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	// Search for a point very close to cluster 0 center
	query := make([]float32, dim)
	for j := 0; j < dim; j++ {
		query[j] = clusters[0][j%len(clusters[0])] + 0.1
	}

	results, err := idx.SearchInternal(context.Background(), query, 20, nil, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	// Top results should have minimal distance
	assert.Less(t, results[0].Distance, float32(2.0),
		"Top result should be very close to query")
}

func TestIVFPQIndex_SearchWithFilter(t *testing.T) {
	t.Skip("Filter support not yet implemented")
}

func BenchmarkIVFPQIndex_Search(b *testing.B) {
	dim := 128
	numVectors := 10000
	config := IVFPQConfig{
		Nlist:  256,
		M:      8,
		K:      256,
		Nprobe: 8,
	}

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = float32(i%100) * 0.1
		}
		vectors[i] = vec
	}

	idx, err := NewIVFPQIndex(dim, config)
	if err != nil {
		b.Fatal(err)
	}
	if err := idx.Train(vectors); err != nil {
		b.Fatal(err)
	}
	if err := idx.Add(context.Background(), vectors); err != nil {
		b.Fatal(err)
	}

	query := make([]float32, dim)
	for j := 0; j < dim; j++ {
		query[j] = 50.0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.SearchInternal(context.Background(), query, 10, nil, SearchOptions{})
	}
}

func BenchmarkIVFPQIndex_MemoryReduction(b *testing.B) {
	dim := 128
	numVectors := 10000
	config := IVFPQConfig{
		Nlist:  1024,
		M:      8,
		K:      256,
		Nprobe: 8,
	}

	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = float32(i%100) * 0.1
		}
		vectors[i] = vec
	}

	idx, err := NewIVFPQIndex(dim, config)
	if err != nil {
		b.Fatal(err)
	}
	if err := idx.Train(vectors); err != nil {
		b.Fatal(err)
	}
	if err := idx.Add(context.Background(), vectors); err != nil {
		b.Fatal(err)
	}

	// Original float32 storage + PQ codes stored separately
	// The index stores PQ codes (M=8 bytes per vector) in clusters
	// Vector store is kept for accurate search scoring
	originalMemory := int64(numVectors * dim * 4)
	indexMemory := idx.EstimateMemory()
	pqCodeMemory := int64(numVectors * config.M)

	b.ReportMetric(float64(originalMemory)/float64(indexMemory+pqCodeMemory), "reduction_factor")
	b.Logf("Original float32: %d bytes", originalMemory)
	b.Logf("PQ codes only: %d bytes", pqCodeMemory)
	b.Logf("IVF-PQ with vectors: %d bytes", indexMemory)
	b.Logf("Reduction (excluding full vectors): %.2fx", float64(originalMemory)/float64(pqCodeMemory))
}
