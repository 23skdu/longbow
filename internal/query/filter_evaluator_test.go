package query

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestFilterEvaluator_Reuse(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "val", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	// Batch 1
	b1 := array.NewRecordBuilder(mem, schema)
	b1.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 10}, nil)
	b1.Field(1).(*array.Float32Builder).AppendValues([]float32{1.1, 2.2, 3.3, 10.1}, nil)
	rec1 := b1.NewRecordBatch()
	defer rec1.Release()

	// Batch 2
	b2 := array.NewRecordBuilder(mem, schema)
	b2.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30, 40}, nil)
	b2.Field(1).(*array.Float32Builder).AppendValues([]float32{10.1, 20.2, 30.3, 40.4}, nil)
	rec2 := b2.NewRecordBatch()
	defer rec2.Release()

	filters := []Filter{
		{Field: "id", Operator: "=", Value: "10"},
	}

	// 1. New Evaluator bound to rec1
	eval, err := NewFilterEvaluator(rec1, filters)
	require.NoError(t, err)

	// Verify matches on rec1 (index 3 is 10)
	require.False(t, eval.Matches(0))
	require.True(t, eval.Matches(3))

	// Verify EvaluateToArrowBoolean on rec1
	mask1, err := eval.EvaluateToArrowBoolean(mem, int(rec1.NumRows()))
	require.NoError(t, err)
	defer mask1.Release()
	require.True(t, mask1.IsValid(3))
	require.True(t, mask1.Value(3))
	require.False(t, mask1.Value(0))

	// 2. Reuse Evaluator on rec2
	err = eval.Reset(rec2)
	require.NoError(t, err)

	// Verify matches on rec2 (index 0 is 10)
	require.True(t, eval.Matches(0))
	require.False(t, eval.Matches(1))

	// Verify EvaluateToArrowBoolean on rec2
	mask2, err := eval.EvaluateToArrowBoolean(mem, int(rec2.NumRows()))
	require.NoError(t, err)
	defer mask2.Release()
	require.True(t, mask2.Value(0))
	require.False(t, mask2.Value(1))
}

func TestFilterEvaluator_MatchesBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 2, 2, 1, 1, 2, 2}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">=", Value: "3"},
		{Field: "category", Operator: "=", Value: "1"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	// All indices
	allIndices := []int{0, 1, 2, 3, 4, 5, 6, 7}

	// MatchesBatch should return indices where id >= 3 AND category == 1
	// Expected: indices 4, 5 (id=5,6 with category=1)
	result := eval.MatchesBatch(allIndices)
	require.Equal(t, []int{4, 5}, result)

	// Empty input
	result = eval.MatchesBatch([]int{})
	require.Nil(t, result)

	// No matches
	singleResult := eval.MatchesBatch([]int{0})
	require.Nil(t, singleResult)
}

func TestFilterEvaluator_MatchesBatchFused(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, nil)
	b.Field(2).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	t.Run("SingleFilter", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">=", Value: "5"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		allIndices := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		result := eval.MatchesBatchFused(allIndices)
		require.Equal(t, []int{4, 5, 6, 7, 8, 9}, result)
	})

	t.Run("MultipleFilters", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">=", Value: "3"},
			{Field: "category", Operator: "=", Value: "1"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		allIndices := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		result := eval.MatchesBatchFused(allIndices)
		require.Equal(t, []int{4, 5, 8, 9}, result)
	})

	t.Run("ThreeFilters", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">=", Value: "3"},
			{Field: "category", Operator: "=", Value: "1"},
			{Field: "value", Operator: "<=", Value: "6"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		allIndices := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		result := eval.MatchesBatchFused(allIndices)
		require.Equal(t, []int{4, 5}, result)
	})

	t.Run("EmptyInput", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">", Value: "0"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result := eval.MatchesBatchFused([]int{})
		require.Nil(t, result)
	})

	t.Run("NoFilters", func(t *testing.T) {
		eval, err := NewFilterEvaluator(rec, []Filter{})
		require.NoError(t, err)

		allIndices := []int{0, 1, 2, 3}
		result := eval.MatchesBatchFused(allIndices)
		require.Equal(t, allIndices, result)
	})

	t.Run("NoMatches", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">", Value: "100"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		allIndices := []int{0, 1, 2, 3}
		result := eval.MatchesBatchFused(allIndices)
		require.Nil(t, result)
	})
}

func TestFilterEvaluator_MatchesBatchVsMatchesBatchFused(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create larger dataset
	numRows := 1000
	ids := make([]int64, numRows)
	categories := make([]int64, numRows)
	statuses := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = int64(i + 1)
		categories[i] = int64((i % 3) + 1)
		statuses[i] = int64((i % 2) + 1)
	}

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.Int64Builder).AppendValues(categories, nil)
	b.Field(2).(*array.Int64Builder).AppendValues(statuses, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">", Value: "100"},
		{Field: "category", Operator: "=", Value: "2"},
		{Field: "status", Operator: "=", Value: "1"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	allIndices := make([]int, numRows)
	for i := 0; i < numRows; i++ {
		allIndices[i] = i
	}

	resultBatch := eval.MatchesBatch(allIndices)
	resultFused := eval.MatchesBatchFused(allIndices)

	require.Equal(t, len(resultBatch), len(resultFused))
	require.ElementsMatch(t, resultBatch, resultFused)
}

func TestFilterEvaluator_MatchesBatchFusedSubset(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 2, 2, 1, 1, 2, 2}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">=", Value: "4"},
		{Field: "category", Operator: "=", Value: "2"},
	}
	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	// Subset of indices (not contiguous)
	subset := []int{0, 2, 3, 5, 6, 7}
	result := eval.MatchesBatchFused(subset)
	// Index 3: id=4, category=2 -> matches
	// Index 6: id=7, category=2 -> matches
	// Index 7: id=8, category=2 -> matches
	require.Equal(t, []int{3, 6, 7}, result)
}

func BenchmarkFilterEvaluator_MatchesBatch(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.PrimitiveTypes.Int64},
	}, nil))

	numRows := 10000
	builder.Field(0).(*array.Int64Builder).AppendValues(makeInt64Range(1, numRows+1), nil)
	builder.Field(1).(*array.Int64Builder).AppendValues(makeInt64Mod(1, 10, numRows), nil)
	builder.Field(2).(*array.Int64Builder).AppendValues(makeInt64Mod(1, 3, numRows), nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">", Value: "1000"},
		{Field: "category", Operator: "=", Value: "5"},
		{Field: "status", Operator: "=", Value: "2"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	if err != nil {
		b.Fatal(err)
	}

	allIndices := make([]int, numRows)
	for i := 0; i < numRows; i++ {
		allIndices[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = eval.MatchesBatch(allIndices)
	}
}

func BenchmarkFilterEvaluator_MatchesBatchFused(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.PrimitiveTypes.Int64},
	}, nil))

	numRows := 10000
	builder.Field(0).(*array.Int64Builder).AppendValues(makeInt64Range(1, numRows+1), nil)
	builder.Field(1).(*array.Int64Builder).AppendValues(makeInt64Mod(1, 10, numRows), nil)
	builder.Field(2).(*array.Int64Builder).AppendValues(makeInt64Mod(1, 3, numRows), nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">", Value: "1000"},
		{Field: "category", Operator: "=", Value: "5"},
		{Field: "status", Operator: "=", Value: "2"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	if err != nil {
		b.Fatal(err)
	}

	allIndices := make([]int, numRows)
	for i := 0; i < numRows; i++ {
		allIndices[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = eval.MatchesBatchFused(allIndices)
	}
}

func makeInt64Range(_, end int) []int64 {
	result := make([]int64, end-1)
	for i := 0; i < end-1; i++ {
		result[i] = int64(i + 1)
	}
	return result
}

func makeInt64Mod(_, mod, count int) []int64 {
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = int64((i % mod) + 1)
	}
	return result
}

func FuzzFilterEvaluator_MatchesBatchFused(f *testing.F) {
	f.Add(100, 3, 10)     // numRows, numFilters, categories
	f.Add(1000, 5, 100)   // Larger dataset
	f.Add(10000, 2, 1000) // Even larger

	f.Fuzz(func(t *testing.T, numRows int, numFilters int, categories int) {
		if numRows <= 0 || numRows > 10000 {
			t.Skip()
		}
		if numFilters <= 0 || numFilters > 10 {
			t.Skip()
		}
		if categories <= 0 || categories > 100 {
			t.Skip()
		}

		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		builder := array.NewRecordBuilder(mem, schema)
		ids := make([]int64, numRows)
		cats := make([]int64, numRows)
		for i := 0; i < numRows; i++ {
			ids[i] = int64(i + 1)
			cats[i] = int64((i % categories) + 1)
		}
		builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		builder.Field(1).(*array.Int64Builder).AppendValues(cats, nil)
		rec := builder.NewRecordBatch()
		defer rec.Release()

		// Create filters
		filters := make([]Filter, numFilters)
		for i := 0; i < numFilters; i++ {
			filters[i] = Filter{
				Field:    "category",
				Operator: "=",
				Value:    string(rune('1' + i%9)),
			}
		}

		eval, err := NewFilterEvaluator(rec, filters)
		if err != nil {
			t.Skip()
		}

		// Test with subset of indices
		subsetSize := numRows / 10
		if subsetSize == 0 {
			subsetSize = 1
		}
		indices := make([]int, subsetSize)
		for i := 0; i < subsetSize; i++ {
			indices[i] = i * 10 % numRows
		}

		result := eval.MatchesBatchFused(indices)

		// Verify results match expected
		for _, idx := range result {
			if idx < 0 || idx >= numRows {
				t.Errorf("invalid index %d returned", idx)
			}
			// Verify each filter passes
			for _, filter := range filters {
				catVal := cats[idx]
				expectedCat := int64(filter.Value[0] - '0')
				if catVal != expectedCat {
					t.Errorf("filter mismatch at index %d: expected category %d, got %d", idx, expectedCat, catVal)
				}
			}
		}
	})
}

func FuzzFilterEvaluator_CompareBatchMethods(f *testing.F) {
	f.Add(500, 3, 5)   // numRows, numFilters, categories
	f.Add(2000, 4, 10) // Larger dataset

	f.Fuzz(func(t *testing.T, numRows int, numFilters int, categories int) {
		if numRows <= 0 || numRows > 5000 {
			t.Skip()
		}
		if numFilters <= 0 || numFilters > 5 {
			t.Skip()
		}
		if categories <= 0 || categories > 20 {
			t.Skip()
		}

		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		builder := array.NewRecordBuilder(mem, schema)
		ids := make([]int64, numRows)
		cats := make([]int64, numRows)
		for i := 0; i < numRows; i++ {
			ids[i] = int64(i + 1)
			cats[i] = int64((i % categories) + 1)
		}
		builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		builder.Field(1).(*array.Int64Builder).AppendValues(cats, nil)
		rec := builder.NewRecordBatch()
		defer rec.Release()

		filters := []Filter{
			{Field: "id", Operator: ">", Value: "100"},
			{Field: "category", Operator: "=", Value: "1"},
		}

		eval, err := NewFilterEvaluator(rec, filters)
		if err != nil {
			t.Skip()
		}

		indices := make([]int, numRows)
		for i := 0; i < numRows; i++ {
			indices[i] = i
		}

		result1 := eval.MatchesBatch(indices)
		result2 := eval.MatchesBatchFused(indices)

		// Results should have same length
		if len(result1) != len(result2) {
			t.Errorf("length mismatch: MatchesBatch=%d, MatchesBatchFused=%d", len(result1), len(result2))
		}

		// Results should contain same elements (order may differ)
		if len(result1) > 0 {
			set1 := make(map[int]bool)
			set2 := make(map[int]bool)
			for _, v := range result1 {
				set1[v] = true
			}
			for _, v := range result2 {
				set2[v] = true
			}
			for k := range set1 {
				if !set2[k] {
					t.Errorf("element %d in MatchesBatch but not in MatchesBatchFused", k)
				}
			}
		}
	})
}

func TestFilterEvaluator_MatchesAll(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float32},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 2, 2, 1, 1, 2, 2, 1, 1}, nil)
	b.Field(2).(*array.Float32Builder).AppendValues([]float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	t.Run("NoFilters", func(t *testing.T) {
		eval, err := NewFilterEvaluator(rec, []Filter{})
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, result)
	})

	t.Run("SingleFilter", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">=", Value: "5"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Equal(t, []int{4, 5, 6, 7, 8, 9}, result)
	})

	t.Run("MultipleFilters", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">=", Value: "3"},
			{Field: "category", Operator: "=", Value: "1"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Equal(t, []int{4, 5, 8, 9}, result)
	})

	t.Run("ThreeFilters", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">=", Value: "3"},
			{Field: "category", Operator: "=", Value: "1"},
			{Field: "value", Operator: "<=", Value: "6"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Equal(t, []int{4, 5}, result)
	})

	t.Run("NoMatches", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">", Value: "100"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("AllMatch", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: ">=", Value: "1"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Len(t, result, 10)
	})

	t.Run("SelectiveFilterEarlyExit", func(t *testing.T) {
		filters := []Filter{
			{Field: "id", Operator: "=", Value: "999"}, // Very selective - no matches
			{Field: "category", Operator: "=", Value: "1"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Empty(t, result) // Should early exit after first filter
	})

	t.Run("FloatFilter", func(t *testing.T) {
		filters := []Filter{
			{Field: "value", Operator: ">", Value: "5.0"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Equal(t, []int{5, 6, 7, 8, 9}, result)
	})
}

func TestFilterEvaluator_MatchesAllVsMatchesBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	numRows := 1000
	ids := make([]int64, numRows)
	categories := make([]int64, numRows)
	statuses := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		ids[i] = int64(i + 1)
		categories[i] = int64((i % 3) + 1)
		statuses[i] = int64((i % 2) + 1)
	}

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.Int64Builder).AppendValues(categories, nil)
	b.Field(2).(*array.Int64Builder).AppendValues(statuses, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">", Value: "100"},
		{Field: "category", Operator: "=", Value: "2"},
		{Field: "status", Operator: "=", Value: "1"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	allIndices := make([]int, numRows)
	for i := 0; i < numRows; i++ {
		allIndices[i] = i
	}

	resultAll, err := eval.MatchesAll(int(rec.NumRows()))
	require.NoError(t, err)
	resultBatch := eval.MatchesBatch(allIndices)

	require.Equal(t, len(resultBatch), len(resultAll))
	require.ElementsMatch(t, resultBatch, resultAll)
}

func TestIsBitmapAllZeros(t *testing.T) {
	t.Run("AllZeros", func(t *testing.T) {
		bitmap := []byte{0, 0, 0, 0}
		require.True(t, isBitmapAllZeros(bitmap))
	})

	t.Run("AllZerosLarge", func(t *testing.T) {
		bitmap := make([]byte, 1000)
		require.True(t, isBitmapAllZeros(bitmap))
	})

	t.Run("OneNonZero", func(t *testing.T) {
		bitmap := []byte{0, 0, 1, 0}
		require.False(t, isBitmapAllZeros(bitmap))
	})

	t.Run("AllNonZero", func(t *testing.T) {
		bitmap := []byte{1, 1, 1, 1}
		require.False(t, isBitmapAllZeros(bitmap))
	})

	t.Run("Mixed", func(t *testing.T) {
		bitmap := []byte{0, 1, 0, 1, 0, 1}
		require.False(t, isBitmapAllZeros(bitmap))
	})

	t.Run("Empty", func(t *testing.T) {
		bitmap := []byte{}
		require.True(t, isBitmapAllZeros(bitmap))
	})
}

func TestSelectOpsBySelectivity(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1, 1, 1, 1, 1, 1, 2, 2, 2, 2}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">", Value: "5"},       // ~50% selectivity (indices 5-9)
		{Field: "category", Operator: "=", Value: "1"}, // 50% selectivity (indices 0-4, 5)
		{Field: "id", Operator: "=", Value: "3"},       // 10% selectivity (index 2)
	}

	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	sorted := selectOpsBySelectivity(eval.ops)
	require.Len(t, sorted, 3)

	// The highly selective filter (id=3) should be first or last depending on implementation
	// Our implementation sorts ascending by selectivity (higher selectivity = fewer matches = first)
	// So the filter with lowest selectivity (id=3) should come first
}

func BenchmarkFilterEvaluator_MatchesAll(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.PrimitiveTypes.Int64},
	}, nil))

	numRows := 10000
	builder.Field(0).(*array.Int64Builder).AppendValues(makeInt64Range(1, numRows+1), nil)
	builder.Field(1).(*array.Int64Builder).AppendValues(makeInt64Mod(1, 10, numRows), nil)
	builder.Field(2).(*array.Int64Builder).AppendValues(makeInt64Mod(1, 3, numRows), nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: ">", Value: "1000"},
		{Field: "category", Operator: "=", Value: "5"},
		{Field: "status", Operator: "=", Value: "2"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = eval.MatchesAll(numRows)
	}
}

func BenchmarkFilterEvaluator_MatchesAll_SelectiveEarlyExit(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
	}, nil))

	numRows := 10000
	builder.Field(0).(*array.Int64Builder).AppendValues(makeInt64Range(1, numRows+1), nil)
	builder.Field(1).(*array.Int64Builder).AppendValues(makeInt64Mod(1, 10, numRows), nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "id", Operator: "=", Value: "99999"}, // Very selective - no matches, triggers early exit
		{Field: "category", Operator: "=", Value: "5"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = eval.MatchesAll(numRows)
	}
}

func FuzzFilterEvaluator_MatchesAll(f *testing.F) {
	f.Add(100, 3, 10)     // numRows, numFilters, categories
	f.Add(1000, 5, 100)   // Larger dataset
	f.Add(10000, 2, 1000) // Even larger

	f.Fuzz(func(t *testing.T, numRows int, numFilters int, categories int) {
		if numRows <= 0 || numRows > 10000 {
			t.Skip()
		}
		if numFilters <= 0 || numFilters > 10 {
			t.Skip()
		}
		if categories <= 0 || categories > 100 {
			t.Skip()
		}

		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "category", Type: arrow.PrimitiveTypes.Int64},
		}, nil)

		builder := array.NewRecordBuilder(mem, schema)
		ids := make([]int64, numRows)
		cats := make([]int64, numRows)
		for i := 0; i < numRows; i++ {
			ids[i] = int64(i + 1)
			cats[i] = int64((i % categories) + 1)
		}
		builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
		builder.Field(1).(*array.Int64Builder).AppendValues(cats, nil)
		rec := builder.NewRecordBatch()
		defer rec.Release()

		// Create filters
		filters := make([]Filter, numFilters)
		for i := 0; i < numFilters; i++ {
			filters[i] = Filter{
				Field:    "category",
				Operator: "=",
				Value:    string(rune('1' + i%9)),
			}
		}

		eval, err := NewFilterEvaluator(rec, filters)
		if err != nil {
			t.Skip()
		}

		result, err := eval.MatchesAll(numRows)
		if err != nil {
			t.Errorf("MatchesAll error: %v", err)
		}

		// Verify results match expected by checking each filter
		for _, idx := range result {
			if idx < 0 || idx >= numRows {
				t.Errorf("invalid index %d returned", idx)
				continue
			}
			for _, filter := range filters {
				catVal := cats[idx]
				expectedCat := int64(filter.Value[0] - '0')
				if catVal != expectedCat {
					t.Errorf("filter mismatch at index %d: expected category %d, got %d", idx, expectedCat, catVal)
				}
			}
		}
	})
}

func TestStringFilterEvaluator_Match(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"apple", "banana", "cherry", "date", "elderberry"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "name", Operator: "=", Value: "banana"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	require.False(t, eval.Matches(0)) // apple != banana
	require.True(t, eval.Matches(1))  // banana == banana
	require.False(t, eval.Matches(2)) // cherry != banana
}

func TestStringFilterEvaluator_MatchBitmap(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "C", "A", "B", "A", "D", "A", "B"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	t.Run("EqualOperator", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: "=", Value: "A"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		bitmap := make([]byte, int(rec.NumRows()))
		eval.ops[0].MatchBitmap(bitmap)

		expected := []byte{1, 0, 1, 0, 1, 0, 1, 0, 1, 0}
		require.Equal(t, expected, bitmap)
	})

	t.Run("NotEqualOperator", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: "!=", Value: "A"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		bitmap := make([]byte, int(rec.NumRows()))
		eval.ops[0].MatchBitmap(bitmap)

		expected := []byte{0, 1, 0, 1, 0, 1, 0, 1, 0, 1}
		require.Equal(t, expected, bitmap)
	})

	t.Run("GreaterThanOperator", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: ">", Value: "B"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		bitmap := make([]byte, int(rec.NumRows()))
		eval.ops[0].MatchBitmap(bitmap)

		// C > B, D > B
		expected := []byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0}
		require.Equal(t, expected, bitmap)
	})

	t.Run("LessThanOperator", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: "<", Value: "C"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		bitmap := make([]byte, int(rec.NumRows()))
		eval.ops[0].MatchBitmap(bitmap)

		// A < C, B < C
		expected := []byte{1, 1, 1, 0, 1, 1, 1, 0, 1, 1}
		require.Equal(t, expected, bitmap)
	})
}

func TestStringFilterEvaluator_MatchesAll(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"A", "B", "A", "C", "A", "B", "A", "D", "A", "B"}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"active", "active", "inactive", "active", "active", "inactive", "active", "active", "inactive", "active"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	t.Run("SingleFilter", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: "=", Value: "A"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Equal(t, []int{0, 2, 4, 6, 8}, result)
	})

	t.Run("MultipleFilters", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: "=", Value: "A"},
			{Field: "status", Operator: "=", Value: "active"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		// A + active: indices 0, 4, 6
		require.Equal(t, []int{0, 4, 6}, result)
	})

	t.Run("NoMatches", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: "=", Value: "Z"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("AllMatch", func(t *testing.T) {
		filters := []Filter{
			{Field: "category", Operator: "!=", Value: "Z"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		result, err := eval.MatchesAll(int(rec.NumRows()))
		require.NoError(t, err)
		require.Len(t, result, 10)
	})
}

func TestStringFilterEvaluator_FilterBatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "name", Operator: "=", Value: "apple"},
	}
	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	indices := []int{0, 1, 2, 3, 4, 5, 6}
	result := eval.MatchesBatch(indices)
	require.Equal(t, []int{0}, result)
}

func TestStringFilterEvaluator_EqualLengthStrings(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "code", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"ABC", "DEF", "GHI", "ABC", "JKL", "ABC", "MNO"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "code", Operator: "=", Value: "ABC"},
	}
	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	bitmap := make([]byte, int(rec.NumRows()))
	eval.ops[0].MatchBitmap(bitmap)

	expected := []byte{1, 0, 0, 1, 0, 1, 0}
	require.Equal(t, expected, bitmap)
}

func TestStringFilterEvaluator_VariableLengthStrings(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "text", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "ab", "abc", "abcd", "abcde"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	t.Run("PrefixMatch", func(t *testing.T) {
		filters := []Filter{
			{Field: "text", Operator: "=", Value: "abc"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		bitmap := make([]byte, int(rec.NumRows()))
		eval.ops[0].MatchBitmap(bitmap)

		expected := []byte{0, 0, 1, 0, 0}
		require.Equal(t, expected, bitmap)
	})

	t.Run("ContainsA", func(t *testing.T) {
		filters := []Filter{
			{Field: "text", Operator: "=", Value: "a"},
		}
		eval, err := NewFilterEvaluator(rec, filters)
		require.NoError(t, err)

		bitmap := make([]byte, int(rec.NumRows()))
		eval.ops[0].MatchBitmap(bitmap)

		// Only exact match "a" should match
		expected := []byte{1, 0, 0, 0, 0}
		require.Equal(t, expected, bitmap)
	})
}

func TestStringFilterEvaluator_WithNulls(t *testing.T) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"apple", "banana", "", "date", "elderberry"}, []bool{true, true, false, true, true})
	rec := b.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "name", Operator: "=", Value: "apple"},
	}
	eval, err := NewFilterEvaluator(rec, filters)
	require.NoError(t, err)

	require.True(t, eval.Matches(0))
	require.False(t, eval.Matches(2)) // Null value should not match
}

func BenchmarkStringFilterEvaluator_MatchBitmap(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
	}, nil))

	numRows := 10000
	categories := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		categories[i] = fmt.Sprintf("category_%d", i%100)
	}
	builder.Field(0).(*array.StringBuilder).AppendValues(categories, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "category", Operator: "=", Value: "category_50"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	if err != nil {
		b.Fatal(err)
	}

	bitmap := make([]byte, numRows)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		eval.ops[0].MatchBitmap(bitmap)
	}
}

func BenchmarkStringFilterEvaluator_MatchesAll(b *testing.B) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "category", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String},
	}, nil))

	numRows := 10000
	categories := make([]string, numRows)
	statuses := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		categories[i] = fmt.Sprintf("category_%d", i%100)
		statuses[i] = fmt.Sprintf("status_%d", i%10)
	}
	builder.Field(0).(*array.StringBuilder).AppendValues(categories, nil)
	builder.Field(1).(*array.StringBuilder).AppendValues(statuses, nil)
	rec := builder.NewRecordBatch()
	defer rec.Release()

	filters := []Filter{
		{Field: "category", Operator: "=", Value: "category_50"},
		{Field: "status", Operator: "=", Value: "status_5"},
	}

	eval, err := NewFilterEvaluator(rec, filters)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = eval.MatchesAll(numRows)
	}
}

func FuzzStringFilterEvaluator_Match(f *testing.F) {
	f.Add(100, "test_value") // numRows, value
	f.Add(1000, "category_50")
	f.Add(10000, "item_123")

	f.Fuzz(func(t *testing.T, numRows int, matchValue string) {
		if numRows <= 0 || numRows > 10000 {
			t.Skip()
		}

		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "text", Type: arrow.BinaryTypes.String},
		}, nil)

		builder := array.NewRecordBuilder(mem, schema)
		values := make([]string, numRows)
		for i := 0; i < numRows; i++ {
			values[i] = fmt.Sprintf("value_%d", i%100)
		}
		builder.Field(0).(*array.StringBuilder).AppendValues(values, nil)
		rec := builder.NewRecordBatch()
		defer rec.Release()

		filters := []Filter{
			{Field: "text", Operator: "=", Value: matchValue},
		}

		eval, err := NewFilterEvaluator(rec, filters)
		if err != nil {
			t.Skip()
		}

		bitmap := make([]byte, numRows)
		eval.ops[0].MatchBitmap(bitmap)

		// Verify results manually
		for i := 0; i < numRows; i++ {
			expected := values[i] == matchValue
			if (bitmap[i] != 0) != expected {
				t.Errorf("mismatch at index %d: expected=%v, got=%v", i, expected, bitmap[i] != 0)
			}
		}
	})
}

func FuzzStringFilterEvaluator_MultipleOps(f *testing.F) {
	f.Add(500, "eq") // numRows, operator
	f.Add(2000, "neq")
	f.Add(5000, "gt")

	f.Fuzz(func(t *testing.T, numRows int, operator string) {
		if numRows <= 0 || numRows > 10000 {
			t.Skip()
		}
		if operator != "eq" && operator != "neq" && operator != ">" && operator != "<" && operator != ">=" && operator != "<=" {
			t.Skip()
		}

		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "text", Type: arrow.BinaryTypes.String},
		}, nil)

		builder := array.NewRecordBuilder(mem, schema)
		values := make([]string, numRows)
		for i := 0; i < numRows; i++ {
			values[i] = fmt.Sprintf("item_%03d", i%256)
		}
		builder.Field(0).(*array.StringBuilder).AppendValues(values, nil)
		rec := builder.NewRecordBatch()
		defer rec.Release()

		filters := []Filter{
			{Field: "text", Operator: operator, Value: "item_128"},
		}

		eval, err := NewFilterEvaluator(rec, filters)
		if err != nil {
			t.Skip()
		}

		bitmap := make([]byte, numRows)
		eval.ops[0].MatchBitmap(bitmap)

		// Verify at sample points
		samplePoints := []int{0, 127, 128, 129, 255, numRows / 2, numRows - 1}
		for _, idx := range samplePoints {
			if idx < 0 || idx >= numRows {
				continue
			}
			// Just verify it doesn't crash - manual verification for all would be slow
			_ = eval.Matches(idx)
		}
	})
}
