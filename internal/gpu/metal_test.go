//go:build gpu && darwin && arm64

package gpu

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetalIndex_Basic(t *testing.T) {
	idx, err := NewMetalIndex(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	require.NoError(t, err, "Metal GPU should be available on Apple Silicon")
	defer idx.Close()

	// Test Add
	vectors := make([]float32, 128*10) // 10 vectors
	for i := range vectors {
		vectors[i] = float32(i) * 0.01
	}
	ids := make([]int64, 10)
	for i := range ids {
		ids[i] = int64(i)
	}

	err = idx.Add(ids, vectors)
	require.NoError(t, err)

	// Test Search
	query := vectors[:128] // Use first vector as query
	resultIDs, distances, err := idx.Search(query, 5)
	require.NoError(t, err)
	assert.Len(t, resultIDs, 5)
	assert.Len(t, distances, 5)

	// First result should be the query itself (distance ~0)
	assert.Equal(t, int64(0), resultIDs[0])
	assert.Less(t, distances[0], float32(0.01))
}

func TestMetalIndex_LargeDataset(t *testing.T) {
	idx, err := NewMetalIndex(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	require.NoError(t, err)
	defer idx.Close()

	// Add 1000 vectors
	vectors := make([]float32, 128*1000)
	ids := make([]int64, 1000)
	for i := range ids {
		ids[i] = int64(i)
		for j := 0; j < 128; j++ {
			vectors[i*128+j] = float32(i*128+j) * 0.001
		}
	}

	err = idx.Add(ids, vectors)
	require.NoError(t, err)

	// Search
	query := vectors[:128]
	resultIDs, distances, err := idx.Search(query, 10)
	require.NoError(t, err)
	assert.Len(t, resultIDs, 10)
	assert.Len(t, distances, 10)

	// Verify first result is correct
	assert.Equal(t, int64(0), resultIDs[0])
	assert.Less(t, distances[0], float32(0.01))
}

func BenchmarkMetalSearch(b *testing.B) {
	idx, err := NewMetalIndex(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	if err != nil {
		b.Skipf("Metal GPU not available: %v", err)
	}
	defer idx.Close()

	// Add 10K vectors
	vectors := make([]float32, 128*10000)
	ids := make([]int64, 10000)
	for i := range ids {
		ids[i] = int64(i)
		for j := 0; j < 128; j++ {
			vectors[i*128+j] = float32(i*128+j) * 0.001
		}
	}
	idx.Add(ids, vectors)

	query := vectors[:128]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = idx.Search(query, 10)
	}
}
