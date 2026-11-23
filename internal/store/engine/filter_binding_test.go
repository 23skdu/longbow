package engine

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

// TestFilterEvaluator_BindingReproduction runs the test filter evaluator_binding reproduction operation.
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

// TestShardedHNSW_FilterPanicReproduction runs the test sharded hnsw_filter panic reproduction operation.
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
	defer func() { _ = idx.Close() }()

	// Add data to Batch 0
	id0Builder := array.NewInt64Builder(pool)
	defer id0Builder.Release()
	id0Builder.AppendValues([]int64{1, 2}, nil)
	v0Builder := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Float32)
	defer v0Builder.Release()
	fv0Builder := v0Builder.ValueBuilder().(*array.Float32Builder)
	v0Builder.Append(true)
	fv0Builder.AppendValues([]float32{1, 1, 1}, nil)
	v0Builder.Append(true)
	fv0Builder.AppendValues([]float32{2, 2, 2}, nil)
	c0Builder := array.NewStringBuilder(pool)
	defer c0Builder.Release()
	c0Builder.AppendValues([]string{"A", "B"}, nil)

	id0Arr := id0Builder.NewArray()
	v0Arr := v0Builder.NewArray()
	c0Arr := c0Builder.NewArray()
	rec0 := array.NewRecordBatch(schema, []arrow.Array{id0Arr, v0Arr, c0Arr}, 2)
	id0Arr.Release()
	v0Arr.Release()
	c0Arr.Release()
	defer rec0.Release()
	ds.dataMu.Lock()
	ds.Records.UpdateInPlace(append(append([]arrow.RecordBatch{}, ds.Records.Read()...), rec0))
	ds.dataMu.Unlock()
	if _, err := idx.AddByRecord(context.Background(), rec0, 0, 0); err != nil {
		t.Fatalf("AddByRecord failed: %v", err)
	}
	if _, err := idx.AddByRecord(context.Background(), rec0, 1, 0); err != nil {
		t.Fatalf("AddByRecord failed: %v", err)
	}

	// Add data to Batch 1
	id1Builder := array.NewInt64Builder(pool)
	defer id1Builder.Release()
	id1Builder.AppendValues([]int64{3}, nil)
	v1Builder := array.NewFixedSizeListBuilder(pool, 3, arrow.PrimitiveTypes.Float32)
	defer v1Builder.Release()
	fv1Builder := v1Builder.ValueBuilder().(*array.Float32Builder)
	v1Builder.Append(true)
	fv1Builder.AppendValues([]float32{3, 3, 3}, nil)
	c1Builder := array.NewStringBuilder(pool)
	defer c1Builder.Release()
	c1Builder.AppendValues([]string{"A"}, nil)

	id1Arr := id1Builder.NewArray()
	v1Arr := v1Builder.NewArray()
	c1Arr := c1Builder.NewArray()
	rec1 := array.NewRecordBatch(schema, []arrow.Array{id1Arr, v1Arr, c1Arr}, 1)
	id1Arr.Release()
	v1Arr.Release()
	c1Arr.Release()
	defer rec1.Release()
	ds.dataMu.Lock()
	ds.Records.UpdateInPlace(append(append([]arrow.RecordBatch{}, ds.Records.Read()...), rec1))
	ds.dataMu.Unlock()
	if _, err := idx.AddByRecord(context.Background(), rec1, 0, 1); err != nil {
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
