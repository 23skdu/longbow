//go:build !windows

package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskBackedLearnedIndex_SaveAndLoad(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "diskann-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	indexPath := filepath.Join(tmpDir, "vamana.index")

	cfg := IndexConfig{
		Dimension: 128,
		DiskANNConfig: &DiskANNConfig{
			MaxDegree:    16,
			BeamWidth:    32,
			BuildThreads: 2,
		},
	}

	// Create index shell
	idx, err := NewDiskBackedLearnedIndex(cfg, indexPath)
	require.NoError(t, err)

	idx.numNodes = 10

	// Save with strict 4KB SSD page alignment
	err = idx.Save(indexPath)
	require.NoError(t, err)

	// Verify offsets are strictly 4KB page aligned
	assert.Equal(t, uint64(4096), idx.vectorOffset, "vectorOffset must be aligned to 4KB page boundary")
	assert.Equal(t, uint64(0), idx.graphOffset%4096, "graphOffset must be aligned to 4KB page boundary")

	// Load the index back using mmap
	idxLoad, err := NewDiskBackedLearnedIndex(cfg, indexPath)
	require.NoError(t, err)

	err = idxLoad.Load(indexPath)
	require.NoError(t, err)
	defer idxLoad.Close()

	assert.Equal(t, idx.numNodes, idxLoad.numNodes)
	assert.Equal(t, idx.dimension, idxLoad.dimension)
	assert.Equal(t, idx.vectorOffset, idxLoad.vectorOffset)
	assert.Equal(t, idx.graphOffset, idxLoad.graphOffset)

	// Run search to exercise distance computation, zero-copy casting, and neighbors prefetching
	query := make([]float32, 128)
	query[0] = 1.0

	res, err := idxLoad.Search(query, 1)
	require.NoError(t, err)
	assert.Len(t, res, 1)

	// Verify other interface methods
	assert.Equal(t, 10, idxLoad.Len())
	assert.Equal(t, IndexTypeDiskANN, idxLoad.Type())

	neighbors, err := idxLoad.GetNeighbors(context.Background(), 0, 5)
	require.NoError(t, err)
	assert.Empty(t, neighbors, "should return zero neighbors for empty graph template")
}

func TestLRUCache(t *testing.T) {
	cache := newLRUCache(3)

	// Add 3 items
	cache.Add(1, []float32{1.0})
	cache.Add(2, []float32{2.0})
	cache.Add(3, []float32{3.0})

	// Get 1 to move it to front
	v, ok := cache.Get(1)
	assert.True(t, ok)
	assert.Equal(t, float32(1.0), v[0])

	// Add 4th item, should evict 2 (least recently used)
	cache.Add(4, []float32{4.0})

	_, ok = cache.Get(2)
	assert.False(t, ok, "item 2 should be evicted")

	_, ok = cache.Get(1)
	assert.True(t, ok, "item 1 should still be in cache")

	_, ok = cache.Get(3)
	assert.True(t, ok, "item 3 should still be in cache")

	_, ok = cache.Get(4)
	assert.True(t, ok, "item 4 should be in cache")
}
