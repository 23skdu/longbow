package store

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIVFOPQIndex_Basic(t *testing.T) {
	dim := 16
	n := 500
	config := IVFOPQConfig{
		Nlist:         10,
		M:             4,
		K:             256,
		Nprobe:        2,
		OPQIterations: 5,
	}

	idx, err := NewIVFOPQIndex(dim, config)
	require.NoError(t, err)

	// Generate data
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	// Train
	err = idx.Train(vectors)
	require.NoError(t, err)

	// Add
	err = idx.Add(context.Background(), vectors)
	require.NoError(t, err)

	// Search
	query := vectors[0]
	results, err := idx.SearchVectorsWithBitmap(context.Background(), query, 5, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, results)
	
	// The first result should be the query vector itself (ID 0)
	assert.Equal(t, uint32(0), uint32(results[0].ID))
}

func TestIVFOPQIndex_Empty(t *testing.T) {
	idx, _ := NewIVFOPQIndex(8, IVFOPQConfig{Nlist: 5})
	results, err := idx.SearchVectorsWithBitmap(context.Background(), make([]float32, 8), 5, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func FuzzIVFOPQIndex_Build(f *testing.F) {
	f.Fuzz(func(t *testing.T, dim int, nlist int, n int) {
		if dim <= 0 || dim > 8192 || nlist <= 0 || nlist > 512 || n <= 0 || n > 100000 {
			t.Skip()
		}

		cfg := IVFOPQConfig{
			Nlist:         nlist,
			M:             4,
			K:             10,
			Nprobe:        2,
			OPQIterations: 2,
		}

		idx, err := NewIVFOPQIndex(dim, cfg)
		if err != nil {
			t.Skip()
		}

		rng := rand.New(rand.NewSource(int64(dim ^ nlist ^ n)))
		vectors := make([][]float32, n)
		for i := 0; i < n; i++ {
			vectors[i] = make([]float32, dim)
			for j := 0; j < dim; j++ {
				vectors[i][j] = rng.Float32()
			}
		}

		err = idx.Train(vectors)
		if err != nil {
			t.Skip()
		}

		err = idx.Add(context.Background(), vectors)
		if err != nil {
			t.Skip()
		}

		query := vectors[0]
		results, err := idx.SearchVectorsWithBitmap(context.Background(), query, 5, nil, nil)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if len(results) == 0 {
			t.Fatalf("No results returned")
		}
	})
}
