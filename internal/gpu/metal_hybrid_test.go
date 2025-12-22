//go:build gpu && darwin && arm64

package gpu

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetalHybridIndex_Basic(t *testing.T) {
	idx, err := NewMetalHybridIndex(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	require.NoError(t, err, "Hybrid Metal should be available")
	defer idx.Close()

	vectors := make([]float32, 128*10)
	for i := range vectors {
		vectors[i] = float32(i) * 0.01
	}
	ids := make([]int64, 10)
	for i := range ids {
		ids[i] = int64(i)
	}

	err = idx.Add(ids, vectors)
	require.NoError(t, err)

	query := vectors[:128]
	resultIDs, distances, err := idx.Search(query, 5)
	require.NoError(t, err)
	assert.Len(t, resultIDs, 5)
	assert.Len(t, distances, 5)
	assert.Less(t, distances[0], float32(0.01))
}

func BenchmarkMetalHybridSearch(b *testing.B) {
	idx, err := NewMetalHybridIndex(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	if err != nil {
		b.Skipf("Hybrid Metal not available: %v", err)
	}
	defer idx.Close()

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
