package store

import (
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRoutingTestRecord(mem memory.Allocator, dims int, numVectors int) arrow.RecordBatch {
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
	numShards := 4
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = numShards

	ds := &Dataset{Name: "routing_test", dataMu: sync.RWMutex{}}
	idx := NewShardedHNSW(cfg, ds)

	rec := makeRoutingTestRecord(mem, 16, 100)
	defer rec.Release()

	// Set ds.Records so AddSafe/AddByLocation works if it checks bounds
	ds.Records = append(ds.Records, rec)

	// Add all vectors
	for i := 0; i < 100; i++ {
		_, err := idx.AddSafe(rec, i, 0)
		require.NoError(t, err)
	}

	// Verify distribution
	// ID assignments are sequential 0..99
	// Shard = ID % NumShards
	// So shard 0 should have 0, 4, 8...

	stats := idx.ShardStats()
	assert.Equal(t, numShards, len(stats))

	totalCount := 0
	for _, stat := range stats {
		t.Logf("Shard %d count: %d", stat.ShardID, stat.Count)
		assert.Equal(t, 25, stat.Count, "Expected perfect distribution for sequential IDs")
		totalCount += stat.Count
	}
	assert.Equal(t, 100, totalCount)
}

func TestShardedHNSW_MergedSearch(t *testing.T) {
	mem := memory.NewGoAllocator()
	numShards := 2
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = numShards

	ds := &Dataset{Name: "merged_search_test", dataMu: sync.RWMutex{}}
	idx := NewShardedHNSW(cfg, ds)

	// 100 vectors, [0,0..] to [99,99..]
	rec := makeRoutingTestRecord(mem, 16, 100)
	defer rec.Release()
	ds.Records = append(ds.Records, rec)

	for i := 0; i < 100; i++ {
		_, err := idx.AddSafe(rec, i, 0)
		require.NoError(t, err)
	}

	// Search for vector [50, 50...]
	// ID 50 goes to Shard 50%2 = 0
	// But similar vector [51, 51...] goes to Shard 1.
	// A search should find both if they are close.

	query := make([]float32, 16)
	for i := range query {
		query[i] = 50.0
	}

	results := idx.SearchVectors(query, 5, nil)
	require.Len(t, results, 5)

	// We expect ID 50 to be top result (dist 0)
	assert.Equal(t, VectorID(50), results[0].ID)
	// We expect ID 49 or 51 to be practically equidistant
	assert.Contains(t, []VectorID{49, 51}, results[1].ID)

	foundShards := make(map[int]bool)
	for _, res := range results {
		shardIdx := idx.GetShardForID(res.ID)
		foundShards[shardIdx] = true
	}

	assert.True(t, len(foundShards) > 1, "Results should come from multiple shards")
}

func TestShardedHNSW_Filtering(t *testing.T) {
	mem := memory.NewGoAllocator()
	numShards := 2
	cfg := DefaultShardedHNSWConfig()
	cfg.NumShards = numShards

	ds := &Dataset{Name: "filtering_test", dataMu: sync.RWMutex{}}
	idx := NewShardedHNSW(cfg, ds)

	rec := makeRoutingTestRecord(mem, 16, 100)
	defer rec.Release()
	ds.Records = append(ds.Records, rec) // Must be in dataset for FilterEvaluator

	for i := 0; i < 100; i++ {
		_, err := idx.AddSafe(rec, i, 0)
		require.NoError(t, err)
	}

	// Filter for "odd" only using tag
	// Logic: vectors 1, 3, 5... are odd
	// Shards:
	// ID 0 (even) -> Shard 0
	// ID 1 (odd) -> Shard 1
	// ID 2 (even) -> Shard 0
	// So "odd" filter forces results from Shard 1 mostly?
	// Actually ID maps to shards. So odd IDs map to Shard 1 (1%2=1, 3%2=1 if sequential).
	// So only Shard 1 has valid candidates. Shard 0 has only evens.

	filters := []Filter{
		{Field: "tag", Operator: "==", Value: "odd"},
	}

	query := make([]float32, 16)
	for i := range query {
		query[i] = 50.0 // Value 50 is Even (ID 50)
	}
	// Nearest neighbors to 50 are 49 (Odd), 51 (Odd), 48 (Even), 52 (Even)...
	// Without filter: 50, 49/51, 48/52
	// With filter: 49, 51, 47, 53...

	results := idx.SearchVectors(query, 5, filters)
	require.NotEmpty(t, results)

	// Top result should NOT be 50 (Even)
	assert.NotEqual(t, VectorID(50), results[0].ID)

	// Verify all returned IDs are odd
	for _, r := range results {
		if r.ID%2 == 0 {
			t.Errorf("Expected odd ID, got %d", r.ID)
		}
	}
}
