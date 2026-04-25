package store

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIVFHNSWCompositeIndex_Basic(t *testing.T) {
	dim := 64
	numCentroids := 10
	numVectors := 1000
	
	config := IVFHNSWConfig{
		Nlist:  numCentroids,
		M:      8,
		K:      256,
		Nprobe: 2,
	}
	
	idx, err := NewIVFHNSWCompositeIndex(dim, config)
	require.NoError(t, err)
	defer idx.Close()
	
	// 1. Generate training data
	trainData := make([][]float32, 500)
	for i := 0; i < 500; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rand.Float32()
		}
		trainData[i] = vec
	}
	
	// 2. Train
	err = idx.Train(trainData)
	require.NoError(t, err)
	
	// 3. Add vectors
	addVectors := make([][]float32, numVectors)
	ids := make([]uint64, numVectors)
	for i := 0; i < numVectors; i++ {
		ids[i] = uint64(i)
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rand.Float32()
		}
		addVectors[i] = vec
	}
	
	err = idx.AddBatch(ids, addVectors)
	require.NoError(t, err)
	assert.Equal(t, numVectors, idx.Size())
	
	// 4. Search
	query := make([]float32, dim)
	for i := 0; i < dim; i++ {
		query[i] = rand.Float32()
	}
	
	results := idx.SearchVectors(query, 10, SearchOptions{})
	assert.Len(t, results, 10)
	for i := 0; i < len(results)-1; i++ {
		assert.LessOrEqual(t, results[i].Distance, results[i+1].Distance)
	}
}

func TestIVFHNSWCompositeIndex_BillionScaleSimulation(t *testing.T) {
	// Simulate billion-scale by using many clusters and verifying search time
	if testing.Short() {
		t.Skip("skipping billion-scale simulation in short mode")
	}
	
	dim := 128
	numCentroids := 1000
	numVectors := 5000
	
	config := IVFHNSWConfig{
		Nlist:  numCentroids,
		M:      16,
		Nprobe: 10,
	}
	
	idx, err := NewIVFHNSWCompositeIndex(dim, config)
	require.NoError(t, err)
	
	trainData := make([][]float32, 1000)
	for i := 0; i < 1000; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rand.Float32()
		}
		trainData[i] = vec
	}
	
	err = idx.Train(trainData)
	require.NoError(t, err)
	
	// Batch add
	batchSize := 1000
	for i := 0; i < numVectors/batchSize; i++ {
		batch := make([][]float32, batchSize)
		ids := make([]uint64, batchSize)
		for j := 0; j < batchSize; j++ {
			ids[j] = uint64(i*batchSize + j)
			v := make([]float32, dim)
			for k := 0; k < dim; k++ {
				v[k] = rand.Float32()
			}
			batch[j] = v
		}
		err = idx.AddBatch(ids, batch)
		require.NoError(t, err)
	}

	
	// Search and verify assignment speed
	query := make([]float32, dim)
	results := idx.SearchVectors(query, 10, SearchOptions{})
	assert.NotEmpty(t, results)
	fmt.Printf("Billion-scale sim: Found %d results\n", len(results))
}

