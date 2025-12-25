package hnsw2

import (
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedArrowIndex(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.FixedSizeListOf(16, arrow.PrimitiveTypes.Float32)},
	}, nil)
	ds := store.NewDataset("test_sharded", schema)

	cfg := DefaultConfig()
	cfg.M = 16
	cfg.EfConstruction = 50
	// Recalculate Ml for M=16
	// Ml = 1 / ln(M)
	// Actually DefaultConfig sets Ml based on M=32.
	// We should set it correctly.
	// Imports math? No, just hardcode or approximate.
	// 1/ln(16) ~= 0.36
	cfg.Ml = 0.36

	config := ShardedConfig{
		NumShards: 4,
		Graph:     cfg,
	}

	idx := NewShardedArrowIndex(ds, &config)

	// insert 1000 vectors
	n := 1000
	vecs := make([][]float32, n)
	
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	
	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)

	// Generate batch
	for i := 0; i < n; i++ {
		vec := make([]float32, 16)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vecs[i] = vec
		
		vecBuilder.Append(true)
		floatBuilder.AppendValues(vec, nil)
	}
	
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Add to dataset
	ds.Records = append(ds.Records, rec)
	rec.Retain()

	// Add to index
	// We simulate adding row by row or batch
	ids, err := idx.AddBatch([]arrow.RecordBatch{rec}, makeRange(0, n), make([]int, n)) // batchIdx 0
	require.NoError(t, err)
	assert.Equal(t, n, len(ids))

	// Verify Distribution
	total := 0
	for i, shard := range idx.Shards() {
		cnt := shard.index.Size()
		t.Logf("Shard %d size: %d", i, cnt)
		total += cnt
		// Expect balanced distribution
		assert.InDelta(t, n/4, cnt, float64(n/4)*0.2, "Shard %d unbalanced", i)
	}
	assert.Equal(t, n, total)
	assert.Equal(t, n, idx.Len())

	// Search
	// Search for vector 0, expect it in top 1
	res, err := idx.SearchVectors(vecs[0], 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, res)
	assert.Equal(t, uint32(0), uint32(res[0].ID))
	assert.InDelta(t, 0.0, res[0].Score, 1e-5)
}

func makeRange(minVal, maxVal int) []int {
	a := make([]int, maxVal-minVal)
	for i := range a {
		a[i] = minVal + i
	}
	return a
}
