package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArrowHNSW_Cleanup(t *testing.T) {
	ds := NewDataset("test_cleanup", nil)
	config := DefaultArrowHNSWConfig()

	h := NewArrowHNSW(ds, &config)

	// Verify initialization
	assert.NotNil(t, h.data.Load())
	assert.NotNil(t, h.deleted)
	assert.NotNil(t, h.searchPool)

	// Close
	err := h.Close()
	assert.NoError(t, err)

	// Verify cleanup
	assert.Nil(t, h.data.Load())
	assert.Nil(t, h.deleted)
	assert.Nil(t, h.searchPool)
	assert.Nil(t, h.dataset)
	assert.Nil(t, h.locationStore)
}

func TestDataset_Close_Cascades(t *testing.T) {
	ds := NewDataset("test_cascade", nil)
	config := DefaultArrowHNSWConfig()
	h := NewArrowHNSW(ds, &config)
	ds.Index = h

	assert.NotNil(t, ds.Index)

	ds.Close()

	// Verify index is closed and nilled
	assert.Nil(t, ds.Index)
	assert.Nil(t, ds.BM25Index)
	assert.Nil(t, ds.Graph)
	// Underlying HNSW should be cleaned (internal check)
	assert.Nil(t, h.data.Load())
}
