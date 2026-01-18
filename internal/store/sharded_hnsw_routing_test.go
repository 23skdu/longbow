package store

import (
	"context"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRoutingTestRecord(mem memory.Allocator, dims, numVectors int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
		{Name: "tag", Type: arrow.BinaryTypes.String}, // For filtering
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).Reserve(numVectors)
	listB := b.Field(1).(*array.FixedSizeListBuilder)
	listB.Reserve(numVectors)
	valB := listB.ValueBuilder().(*array.Float32Builder)
	valB.Reserve(numVectors * dims)
	tagB := b.Field(2).(*array.StringBuilder)
	tagB.Reserve(numVectors)

	for i := 0; i < numVectors; i++ {
		b.Field(0).(*array.Int64Builder).UnsafeAppend(int64(i))
		listB.Append(true)
		for j := 0; j < dims; j++ {
			// Create distinct vectors to avoid confusion
			// Vector [i, i, i...]
			valB.UnsafeAppend(float32(i))
		}
		// Tag: "shard_0", "shard_1" style mocking or just "even"/"odd"
		if i%2 == 0 {
			tagB.Append("even")
		} else {
			tagB.Append("odd")
		}
	}

	return b.NewRecordBatch()
}

func TestShardedHNSW_Routing(t *testing.T) {
	mem := memory.NewGoAllocator()
	cfg := DefaultShardedHNSWConfig()
	cfg.ShardSplitThreshold = 25 // 4 shards for 100 vectors
	cfg.NumShards = 1
	cfg.UseRingSharding = false

	ds := &Dataset{Name: "routing_test", dataMu: sync.RWMutex{}}
	idx := NewShardedHNSW(cfg, ds)

	rec := makeRoutingTestRecord(mem, 16, 100)
	defer rec.Release()

	ds.Records = append(ds.Records, rec)

	for i := 0; i < 100; i++ {
		_, err := idx.AddSafe(rec, i, 0)
		require.NoError(t, err)
	}

	stats := idx.ShardStats()
	assert.Equal(t, 4, len(stats))

	totalCount := 0
	for _, stat := range stats {
		t.Logf("Shard %d count: %d", stat.ShardID, stat.Count)
		assert.Equal(t, 25, stat.Count)
		totalCount += stat.Count
	}
	assert.Equal(t, 100, totalCount)
}

func TestShardedHNSW_MergedSearch(t *testing.T) {
	mem := memory.NewGoAllocator()
	cfg := DefaultShardedHNSWConfig()
	cfg.ShardSplitThreshold = 50 // 2 shards for 100 vectors
	cfg.NumShards = 1
	cfg.UseRingSharding = false

	ds := &Dataset{Name: "merged_search_test", dataMu: sync.RWMutex{}}
	idx := NewShardedHNSW(cfg, ds)

	rec := makeRoutingTestRecord(mem, 16, 100)
	defer rec.Release()
	ds.Records = append(ds.Records, rec)

	for i := 0; i < 100; i++ {
		_, err := idx.AddSafe(rec, i, 0)
		require.NoError(t, err)
	}

	q := make([]float32, 16)
	for i := range q {
		q[i] = 50.0
	}

	results, err := idx.SearchVectors(context.Background(), q, 5, nil, SearchOptions{})
	require.NoError(t, err)
	require.Len(t, results, 5)

	// Range based: Shard 0 has 0-49, Shard 1 has 50-99
	// Query 50.0 is ID 50, which is in Shard 1.
	// Nearest neighbors are 50 (Shard 1), 49 (Shard 0), 51 (Shard 1), 48 (Shard 0)...
	foundShards := make(map[int]bool)
	for _, res := range results {
		shardIdx := idx.GetShardForID(res.ID)
		foundShards[shardIdx] = true
	}

	assert.True(t, len(foundShards) > 1, "Results should come from multiple shards")
}

func TestShardedHNSW_Filtering(t *testing.T) {
	mem := memory.NewGoAllocator()
	cfg := DefaultShardedHNSWConfig()
	cfg.ShardSplitThreshold = 50 // IDs 0-49 in Shard 0, 50-99 in Shard 1
	cfg.NumShards = 1

	ds := &Dataset{Name: "filtering_test", dataMu: sync.RWMutex{}}
	idx := NewShardedHNSW(cfg, ds)

	rec := makeRoutingTestRecord(mem, 16, 100)
	defer rec.Release()
	ds.Records = append(ds.Records, rec)

	for i := 0; i < 100; i++ {
		_, err := idx.AddSafe(rec, i, 0)
		require.NoError(t, err)
	}

	filters := []query.Filter{
		{Field: "tag", Operator: "==", Value: "odd"},
	}

	q := make([]float32, 16)
	for i := range q {
		q[i] = 50.0
	}

	results, err := idx.SearchVectors(context.Background(), q, 5, filters, SearchOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	for _, r := range results {
		if r.ID%2 == 0 {
			t.Errorf("Expected odd ID, got %d", r.ID)
		}
	}
}
