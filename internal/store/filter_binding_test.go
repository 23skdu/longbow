package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterEvaluator_BindingReproduction(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "category", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"A", "B"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// Test successful binding
	filters := []query.Filter{
		{Field: "category", Operator: "==", Value: "A"},
	}
	evaluator, err := query.NewFilterEvaluator(rec, filters)
	assert.NoError(t, err)
	assert.NotNil(t, evaluator)

	// Test failed binding (missing field)
	filters = []query.Filter{
		{Field: "non_existent", Operator: "==", Value: "A"},
	}
	_, err = query.NewFilterEvaluator(rec, filters)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to bind any filters to schema fields")
}

func TestShardedHNSW_FilterPanicReproduction(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "vector", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32)},
			{Name: "category", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	ds := NewDataset("test_sharded_panic", schema)
	config := DefaultShardedHNSWConfig()
	config.NumShards = 2
	config.Dimension = 3
	config.PackedAdjacencyEnabled = false
	config.UseRingSharding = false
	idx := NewShardedHNSW(config, ds)

	// Add data to Batch 0
	id0Builder := array.NewInt64Builder(pool)
	id0Builder.AppendValues([]int64{1, 2}, nil)
	v0Builder := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Float32)
	fv0Builder := v0Builder.ValueBuilder().(*array.Float32Builder)
	v0Builder.Append(true)
	fv0Builder.AppendValues([]float32{1, 1, 1}, nil)
	v0Builder.Append(true)
	fv0Builder.AppendValues([]float32{2, 2, 2}, nil)
	c0Builder := array.NewStringBuilder(pool)
	c0Builder.AppendValues([]string{"A", "B"}, nil)

	rec0 := array.NewRecordBatch(schema, []arrow.Array{id0Builder.NewArray(), v0Builder.NewArray(), c0Builder.NewArray()}, 2)
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec0)
	ds.dataMu.Unlock()
	if _, err := idx.AddByRecord(rec0, 0, 0); err != nil {
		t.Fatalf("AddByRecord failed: %v", err)
	}
	if _, err := idx.AddByRecord(rec0, 1, 0); err != nil {
		t.Fatalf("AddByRecord failed: %v", err)
	}

	// Add data to Batch 1
	id1Builder := array.NewInt64Builder(pool)
	id1Builder.AppendValues([]int64{3}, nil)
	v1Builder := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Float32)
	fv1Builder := v1Builder.ValueBuilder().(*array.Float32Builder)
	v1Builder.Append(true)
	fv1Builder.AppendValues([]float32{3, 3, 3}, nil)
	c1Builder := array.NewStringBuilder(pool)
	c1Builder.AppendValues([]string{"A"}, nil)

	rec1 := array.NewRecordBatch(schema, []arrow.Array{id1Builder.NewArray(), v1Builder.NewArray(), c1Builder.NewArray()}, 1)
	ds.dataMu.Lock()
	ds.Records = append(ds.Records, rec1)
	ds.dataMu.Unlock()
	if _, err := idx.AddByRecord(rec1, 0, 1); err != nil {
		t.Fatalf("AddByRecord failed: %v", err)
	}

	// Search with filter
	filters := []query.Filter{
		{Field: "category", Operator: "==", Value: "A"},
	}

	// This search will likely panic because ShardedHNSW.SearchVectors uses evaluator bound to rec0
	// while matching rows in rec1 (for globalID 2).
	results, err := idx.SearchVectors(context.Background(), []float32{1, 1, 1}, 10, filters, SearchOptions{})
	require.NoError(t, err)

	fmt.Printf("Results: %v\n", results)
	// We expect IDs 1 and 3 to match "A"
	// ID 1 is GlobalID 0, ID 3 is GlobalID 2
	found0 := false
	found2 := false
	for _, r := range results {
		if r.ID == 0 {
			found0 = true
		}
		if r.ID == 2 {
			found2 = true
		}
	}
	assert.True(t, found0, "Should find ID 1")
	assert.True(t, found2, "Should find ID 3")
}
