package store


import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestMergeKWay(t *testing.T) {
	agg := NewShardedResultAggregator()

	shard1 := []SearchResult{
		{ID: 1, Score: 0.1},
		{ID: 3, Score: 0.3},
		{ID: 5, Score: 0.5},
	}
	shard2 := []SearchResult{
		{ID: 2, Score: 0.2},
		{ID: 4, Score: 0.4},
		{ID: 6, Score: 0.6},
	}
	shard3 := []SearchResult{
		{ID: 0, Score: 0.05},
		{ID: 7, Score: 0.7},
	}

	shardResults := [][]SearchResult{shard1, shard2, shard3}

	// Test k=3
	merged := agg.MergeKWay(shardResults, 3)
	assert.Equal(t, 3, len(merged))
	assert.Equal(t, VectorID(0), merged[0].ID)
	assert.Equal(t, VectorID(1), merged[1].ID)
	assert.Equal(t, VectorID(2), merged[2].ID)

	// Test k=5
	merged = agg.MergeKWay(shardResults, 5)
	assert.Equal(t, 5, len(merged))
	assert.Equal(t, VectorID(0), merged[0].ID)
	assert.Equal(t, VectorID(1), merged[1].ID)
	assert.Equal(t, VectorID(2), merged[2].ID)
	assert.Equal(t, VectorID(3), merged[3].ID)
	assert.Equal(t, VectorID(4), merged[4].ID)

	// Test k=10 (more than total available)
	merged = agg.MergeKWay(shardResults, 10)
	assert.Equal(t, 8, len(merged))
}

func TestMergeKWayEmptyshards(t *testing.T) {
	agg := NewShardedResultAggregator()
	shardResults := [][]SearchResult{
		{},
		{ {ID: 1, Score: 0.1} },
		{},
	}
	merged := agg.MergeKWay(shardResults, 5)
	assert.Equal(t, 1, len(merged))
	assert.Equal(t, VectorID(1), merged[0].ID)
}
