//go:build gpu && darwin && arm64

package gpu

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetalIndexOptimized_Basic(t *testing.T) {
	idx, err := NewMetalIndexOptimized(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	if err != nil {
		t.Skipf("Optimized Metal GPU not available: %v", err)
	}
	defer idx.Close()

	// Test Add
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

	// Test Search
	query := vectors[:128]
	resultIDs, distances, err := idx.Search(query, 5)
	require.NoError(t, err)
	assert.Len(t, resultIDs, 5)
	assert.Len(t, distances, 5)

	// First result should be the query itself (vector 0)
	assert.Equal(t, int64(0), resultIDs[0])
	assert.Less(t, distances[0], float32(0.01))
}

func TestMetalIndexOptimized_SearchCorrectness(t *testing.T) {
	idx, err := NewMetalIndexOptimized(GPUConfig{
		DeviceID:  0,
		Dimension: 64,
	})
	if err != nil {
		t.Skipf("Optimized Metal GPU not available: %v", err)
	}
	defer idx.Close()

	// Create known vectors with specific distances
	vectors := make([]float32, 64*5)
	ids := make([]int64, 5)

	// Vector 0: all zeros (reference)
	// Vector 1: distance ~0.1 from vector 0
	// Vector 2: distance ~0.2 from vector 0
	// etc.
	for i := 0; i < 5; i++ {
		ids[i] = int64(i)
		for j := 0; j < 64; j++ {
			vectors[i*64+j] = float32(i) * 0.01
		}
	}

	err = idx.Add(ids, vectors)
	require.NoError(t, err)

	// Search with vector 0 as query
	query := make([]float32, 64)
	resultIDs, distances, err := idx.Search(query, 3)
	require.NoError(t, err)
	assert.Len(t, resultIDs, 3)

	// First result should be vector 0 (exact match)
	assert.Equal(t, int64(0), resultIDs[0])
	assert.InDelta(t, 0.0, distances[0], 0.001)
}

func TestMetalIndexOptimized_Dimensions(t *testing.T) {
	testDims := []int{64, 128, 256, 384, 768}

	for _, dim := range testDims {
		t.Run("dim_"+string(rune('0'+dim/100))+string(rune('0'+(dim/10)%10))+string(rune('0'+dim%10)), func(t *testing.T) {
			idx, err := NewMetalIndexOptimized(GPUConfig{
				DeviceID:  0,
				Dimension: dim,
			})
			if err != nil {
				t.Skipf("Optimized Metal GPU not available: %v", err)
			}
			defer idx.Close()

			// Add a few vectors
			vectors := make([]float32, dim*3)
			ids := make([]int64, 3)
			for i := range ids {
				ids[i] = int64(i)
				for j := 0; j < dim; j++ {
					vectors[i*dim+j] = float32(i+j) * 0.001
				}
			}

			err = idx.Add(ids, vectors)
			require.NoError(t, err)

			// Search
			query := vectors[:dim]
			resultIDs, distances, err := idx.Search(query, 2)
			require.NoError(t, err)
			assert.Len(t, resultIDs, 2)
			assert.Len(t, distances, 2)
		})
	}
}

func TestMetalIndexOptimized_Close(t *testing.T) {
	idx, err := NewMetalIndexOptimized(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	if err != nil {
		t.Skipf("Optimized Metal GPU not available: %v", err)
	}

	// Close should work multiple times
	err = idx.Close()
	require.NoError(t, err)

	err = idx.Close()
	require.NoError(t, err)

	// Operations after close should fail
	vectors := make([]float32, 128)
	ids := []int64{0}
	err = idx.Add(ids, vectors)
	assert.Error(t, err)
}

func TestMetalIndexOptimized_MetricTypes(t *testing.T) {
	idx, err := NewMetalIndexOptimized(GPUConfig{
		DeviceID:  0,
		Dimension: 64,
	})
	if err != nil {
		t.Skipf("Optimized Metal GPU not available: %v", err)
	}
	defer idx.Close()

	// Add vectors
	vectors := make([]float32, 64*5)
	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = int64(i)
		for j := 0; j < 64; j++ {
			vectors[i*64+j] = float32(i+j) * 0.01
		}
	}
	err = idx.Add(ids, vectors)
	require.NoError(t, err)

	// Test L2 metric (default)
	_, _, err = idx.Search(vectors[:64], 3)
	require.NoError(t, err)
}

func BenchmarkMetalOptimizedSearch(b *testing.B) {
	idx, err := NewMetalIndexOptimized(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	if err != nil {
		b.Skipf("Optimized Metal GPU not available: %v", err)
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
	err = idx.Add(ids, vectors)
	if err != nil {
		b.Fatalf("Failed to add vectors: %v", err)
	}

	query := vectors[:128]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = idx.Search(query, 10)
	}
}

func BenchmarkMetalOptimizedAdd(b *testing.B) {
	idx, err := NewMetalIndexOptimized(GPUConfig{
		DeviceID:  0,
		Dimension: 128,
	})
	if err != nil {
		b.Skipf("Optimized Metal GPU not available: %v", err)
	}
	defer idx.Close()

	// Prepare vectors
	vectors := make([]float32, 128*1000)
	ids := make([]int64, 1000)
	for i := range ids {
		ids[i] = int64(i)
		for j := 0; j < 128; j++ {
			vectors[i*128+j] = float32(i*128+j) * 0.001
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Add(ids, vectors)
	}
}
