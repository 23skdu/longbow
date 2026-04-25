//go:build gpu && linux && cuda

package cuda

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCUDAIndex_FilteredSearch(t *testing.T) {
	dim := 128
	count := 1000
	
	config := types.GPUConfig{
		DeviceID:  0,
		MaxMemory: 1024 * 1024 * 1024,
		Dimension: dim,
	}
	
	idx, err := NewCUDAIndex(config)
	require.NoError(t, err)
	defer idx.Close()
	
	// Add vectors
	ids := make([]int64, count)
	vecs := make([]float32, count*dim)
	for i := 0; i < count; i++ {
		ids[i] = int64(i)
		for j := 0; j < dim; j++ {
			vecs[i*dim+j] = rand.Float32()
		}
	}
	
	err = idx.Add(ids, vecs)
	require.NoError(t, err)
	err = idx.Flush()
	require.NoError(t, err)
	
	// Create bitset: only even IDs allowed
	bitset := make([]uint64, (count+63)/64)
	for i := 0; i < count; i++ {
		if i%2 == 0 {
			bitset[i/64] |= (1 << uint(i%64))
		}
	}
	
	query := make([]float32, dim)
	for i := 0; i < dim; i++ {
		query[i] = rand.Float32()
	}
	
	results, err := idx.SearchWithFilter(query, 10, bitset)
	require.NoError(t, err)
	
	assert.NotEmpty(t, results)
	for _, res := range results {
		assert.Equal(t, 0, int(res.ID)%2, "Filtered ID should be even")
	}
	fmt.Printf("Filtered search found %d results\n", len(results))
}
