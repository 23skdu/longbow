package query

import (
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

func makeInt64Range(start, end int) []int64 {
	result := make([]int64, end-start)
	for i := start; i < end; i++ {
		result[i-start] = int64(i)
	}
	return result
}

func makeInt64Mod(base, mod, count int) []int64 {
	result := make([]int64, count)
	for i := 0; i < count; i++ {
		result[i] = int64((i % mod) + base)
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
