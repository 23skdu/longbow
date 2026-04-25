package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIVFOPQIndex_HNSWCoarse(t *testing.T) {
	dim := 16
	nlist := 10
	config := IVFOPQConfig{
		Nlist:         nlist,
		M:             4,
		K:             256,
		Nprobe:        2,
		UseHNSWCoarse: true,
	}

	idx, err := NewIVFOPQIndex(dim, config)
	assert.NoError(t, err)

	// Create synthetic data
	n := 1000
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		v := make([]float32, dim)
		for j := 0; j < dim; j++ {
			v[j] = float32(i*dim + j)
		}
		vectors[i] = v
	}

	// Train
	err = idx.Train(vectors)
	assert.NoError(t, err)
	assert.NotNil(t, idx.coarseHNSW)
	assert.Equal(t, nlist, idx.coarseHNSW.Size())

	// Add
	err = idx.Add(context.Background(), vectors)
	assert.NoError(t, err)

	// Search
	query := vectors[0]
	results, err := idx.SearchVectors(context.Background(), query, 5, nil, nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, results)
	fmt.Printf("Search results: %+v\n", results)
}
