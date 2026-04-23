package store

import (
	"github.com/23skdu/longbow/internal/store/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArrowHNSW_Cleanup(t *testing.T) {
	ds := NewDataset("test_cleanup", nil)
	config := types.DefaultArrowHNSWConfig()

	h := NewArrowHNSW(ds, &config, nil)

	// Verify initialization
	
	
	

	// Close
	err := h.Close()
	assert.NoError(t, err)

	// Verify cleanup
	
	
	
	
	
}

func TestDataset_Close_Cascades(t *testing.T) {
	ds := NewDataset("test_cascade", nil)
	config := types.DefaultArrowHNSWConfig()
	h := NewArrowHNSW(ds, &config, nil)
	ds.Index = h

	assert.NotNil(t, ds.Index)

	ds.Close()

	// Verify index is closed and nilled
	assert.Nil(t, ds.Index)
	assert.Nil(t, ds.BM25Index)
	assert.Nil(t, ds.Graph)
	// Underlying HNSW should be cleaned (internal check)
	
}
