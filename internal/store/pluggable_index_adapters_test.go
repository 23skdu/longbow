package store

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSWPluggableAdapter_Functional(t *testing.T) {
	adapter := &HNSWPluggableAdapter{
		dimension: 4,
		vectors:   make(map[uint64][]float32),
	}

	// Test Add
	err := adapter.Add(1, []float32{1.0, 0.0, 0.0, 0.0})
	require.NoError(t, err)
	err = adapter.Add(2, []float32{0.0, 1.0, 0.0, 0.0})
	require.NoError(t, err)

	assert.Equal(t, 2, adapter.Size())

	// Test Search
	results, err := adapter.Search([]float32{1.0, 0.1, 0.0, 0.0}, 2)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.True(t, results[0].Distance < results[1].Distance)

	// Test Save/Load
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "hnsw.gob")
	err = adapter.Save(path)
	require.NoError(t, err)

	adapter2 := &HNSWPluggableAdapter{
		dimension: 4,
		vectors:   make(map[uint64][]float32),
	}
	err = adapter2.Load(path)
	require.NoError(t, err)
	assert.Equal(t, 2, adapter2.Size())
	
	results2, _ := adapter2.Search([]float32{1.0, 0.0, 0.0, 0.0}, 1)
	assert.Equal(t, uint64(1), results2[0].ID)
}
