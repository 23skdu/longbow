package store

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeIndexPersistence(t *testing.T) {
	if testing.Short() {
			t.Skip("skipping test in short mode")
	}
	dim := 64
	numVectors := 500

	t.Run("IVFHNSWComposite", func(t *testing.T) {
		idx, err := NewIVFHNSWCompositeIndex(dim, IVFHNSWConfig{
			Nlist:  10,
			M:      8,
			Nprobe: 2,
		})
		require.NoError(t, err)
		defer idx.Close()

		// 1. Train
		trainData := make([][]float32, 500)
		for i := 0; i < 500; i++ {
			trainData[i] = randomVector(dim)
		}
		err = idx.Train(trainData)
		require.NoError(t, err)

		// 2. Add some vectors
		for i := 0; i < numVectors; i++ {
			err := idx.Add(uint64(i), randomVector(dim))
			require.NoError(t, err)
		}

		// 3. Search and get baseline
		query := randomVector(dim)
		baseline := idx.SearchVectors(query, 10, SearchOptions{})
		require.Len(t, baseline, 10)

		// 4. Export state
		state, err := idx.ExportState()
		require.NoError(t, err)
		require.NotEmpty(t, state)

		// 5. Create new index and import state
		idx2, err := NewIVFHNSWCompositeIndex(dim, IVFHNSWConfig{
			Nlist:  10,
			M:      8,
			Nprobe: 2,
		})
		require.NoError(t, err)
		defer idx2.Close()

		err = idx2.ImportState(state)
		require.NoError(t, err)
		assert.Equal(t, idx.Size(), idx2.Size())

		// 6. Search and compare
		results := idx2.SearchVectors(query, 10, SearchOptions{})
		require.Len(t, results, 10)

		// The results should be identical since we imported the state
		for i := 0; i < 10; i++ {
			assert.Equal(t, baseline[i].ID, results[i].ID)
			assert.InDelta(t, baseline[i].Distance, results[i].Distance, 1e-5)
		}
	})

	t.Run("IVFOPQ", func(t *testing.T) {
		idx, err := NewIVFOPQIndex(dim, IVFOPQConfig{
			Nlist:  10,
			M:      8,
			K:      256,
			Nprobe: 2,
		})
		require.NoError(t, err)
		defer idx.Close()

		// 1. Train
		trainData := make([][]float32, 500)
		for i := 0; i < 500; i++ {
			trainData[i] = randomVector(dim)
		}
		err = idx.Train(trainData)
		require.NoError(t, err)

		// 2. Add some vectors
		vectors := make([][]float32, numVectors)
		for i := 0; i < numVectors; i++ {
			vectors[i] = randomVector(dim)
		}
		err = idx.Add(context.Background(), vectors)
		require.NoError(t, err)

		// 3. Search and get baseline
		query := randomVector(dim)
		baseline, err := idx.Search(context.Background(), query, 10, nil)
		require.NoError(t, err)
		require.Len(t, baseline, 10)

		// 4. Export state
		state, err := idx.ExportState()
		require.NoError(t, err)
		require.NotEmpty(t, state)

		// 5. Create new index and import state
		idx2, err := NewIVFOPQIndex(dim, IVFOPQConfig{
			Nlist:  10,
			M:      8,
			K:      256,
			Nprobe: 2,
		})
		require.NoError(t, err)
		defer idx2.Close()

		err = idx2.ImportState(state)
		require.NoError(t, err)
		assert.Equal(t, idx.Size(), idx2.Size())

		// 6. Search and compare
		results, err := idx2.Search(context.Background(), query, 10, nil)
		require.NoError(t, err)
		require.Len(t, results, 10)

		// The results should be identical since we imported the state
		for i := 0; i < 10; i++ {
			assert.Equal(t, baseline[i].ID, results[i].ID)
			assert.InDelta(t, baseline[i].Dist, results[i].Dist, 1e-5)
		}
	})
}

func randomVector(dim int) []float32 {
	v := make([]float32, dim)
	for i := 0; i < dim; i++ {
		v[i] = rand.Float32()
	}
	return v
}
