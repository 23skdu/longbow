package store_test

import (
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/stretchr/testify/assert"
)

func TestAutoShardingGetNeighbors(t *testing.T) {
	ds := store.NewDataset("test_autoshard", nil)

	config := store.DefaultAutoShardingConfig()
	config.Enabled = true
	config.ShardThreshold = 100 // Trigger sharding quickly

	idx := store.NewAutoShardingIndex(ds, config)

	var _ store.VectorIndex = idx

	// Check GetNeighbors signature
	// Cast to uint32
	_, err := idx.GetNeighbors(uint32(0))
	// Expected error since no vectors inserted, but should build and run
	assert.Error(t, err)
}
