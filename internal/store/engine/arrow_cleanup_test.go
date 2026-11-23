package engine

import (
	"testing"

	"github.com/23skdu/longbow/internal/store/types"

	"github.com/stretchr/testify/assert"
)

// TestArrowHNSW_Cleanup runs the test arrow hnsw_cleanup operation.
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

// TestDataset_Close_Cascades runs the test dataset_close_cascades operation.
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
