//go:build gpu

package gpu

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGPUIndex_Basic(t *testing.T) {
	// Skip if no GPU available
	idx, err := NewIndexWithConfig(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})

	if err != nil {
		t.Skipf("GPU not available: %v", err)
	}
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

func TestGPUIndex_InvalidDimension(t *testing.T) {
	_, err := NewIndexWithConfig(GPUConfig{
		DeviceID:  0,
		Dimension: -1,
	})
	assert.Error(t, err)
}

func BenchmarkGPUSearch(b *testing.B) {
	idx, err := NewIndexWithConfig(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	if err != nil {
		b.Skipf("GPU not available: %v", err)
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
