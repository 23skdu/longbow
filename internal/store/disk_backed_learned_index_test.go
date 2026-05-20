//go:build !windows

package store

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
