package query

import (
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/stretchr/testify/assert"
)

func TestWindowOperator_Aggregates_Extended(t *testing.T) {
	op := NewWindowOperator()

	results := []core.SearchResult{
		{ID: 1, Metadata: helperEncode(map[string]interface{}{"val": 10, "tag": "A"})},
		{ID: 2, Metadata: helperEncode(map[string]interface{}{"val": 20, "tag": "A"})},
		{ID: 3, Metadata: helperEncode(map[string]interface{}{"val": 5, "tag": "B"})},
	}

	t.Run("MinMax", func(t *testing.T) {
		functions := []WindowFunction{
			{Name: "min", Field: "val", As: "min_val", Over: WindowSpec{}},
			{Name: "max", Field: "val", As: "max_val", Over: WindowSpec{}},
		}
		processed := op.Execute(results, functions)
		for _, r := range processed {
			m := helperDecode(r.Metadata)
			assert.Equal(t, 5.0, m["min_val"])
			assert.Equal(t, 20.0, m["max_val"])
		}
	})

	t.Run("PartitionedAggregates", func(t *testing.T) {
		functions := []WindowFunction{
			{
				Name: "sum",
				Field: "val",
				As: "sum_val",
				Over: WindowSpec{
					PartitionBy: []string{"tag"},
				},
			},
		}
		processed := op.Execute(results, functions)
		resMap := make(map[uint32]float64)
		for _, r := range processed {
			resMap[uint32(r.ID)] = helperDecode(r.Metadata)["sum_val"].(float64)
		}
		assert.Equal(t, 30.0, resMap[1]) // 10 + 20
		assert.Equal(t, 30.0, resMap[2])
		assert.Equal(t, 5.0, resMap[3])  // Just 5
	})
}

func TestWindowOperator_Ranking_Extended(t *testing.T) {
	op := NewWindowOperator()

	results := []core.SearchResult{
		{ID: 1, Distance: 0.1, Score: 0.9},
		{ID: 2, Distance: 0.1, Score: 0.8},
		{ID: 3, Distance: 0.2, Score: 0.7},
	}

	t.Run("DenseRank", func(t *testing.T) {
		functions := []WindowFunction{
			{
				Name: "dense_rank",
				As:   "drk",
				Over: WindowSpec{
					OrderBy: []WindowOrder{{Field: "distance", Descending: false}},
				},
			},
		}
		processed := op.Execute(results, functions)
		resMap := make(map[uint32]int64)
		for _, r := range processed {
			resMap[uint32(r.ID)] = helperDecode(r.Metadata)["drk"].(int64)
		}
		assert.Equal(t, int64(1), resMap[1])
		assert.Equal(t, int64(1), resMap[2])
		assert.Equal(t, int64(2), resMap[3])
	})

	t.Run("OrderBy_Score_Desc", func(t *testing.T) {
		functions := []WindowFunction{
			{
				Name: "row_number",
				As:   "rn",
				Over: WindowSpec{
					OrderBy: []WindowOrder{{Field: "score", Descending: true}},
				},
			},
		}
		processed := op.Execute(results, functions)
		assert.Equal(t, uint32(1), uint32(processed[0].ID))
		assert.Equal(t, uint32(2), uint32(processed[1].ID))
		assert.Equal(t, uint32(3), uint32(processed[2].ID))
	})
}

func TestWindowOperator_InternalMethods(t *testing.T) {
	op := NewWindowOperator()

	t.Run("isLess", func(t *testing.T) {
		assert.True(t, op.isLess(1, 2))
		assert.True(t, op.isLess(int64(1), int64(2)))
		assert.True(t, op.isLess(float32(1.0), float32(2.0)))
		assert.True(t, op.isLess("a", "b"))
		assert.False(t, op.isLess(true, false)) // Not supported
	})

	t.Run("toFloat64", func(t *testing.T) {
		assert.Equal(t, 1.0, op.toFloat64(1))
		assert.Equal(t, 1.0, op.toFloat64(int64(1)))
		assert.Equal(t, 1.0, op.toFloat64(float32(1.0)))
		assert.Equal(t, 1.0, op.toFloat64(float64(1.0)))
		assert.Equal(t, 0.0, op.toFloat64("not-a-number"))
	})

	t.Run("isEqualInternal", func(t *testing.T) {
		a := core.SearchResult{Distance: 0.1, Score: 0.9}
		b := core.SearchResult{Distance: 0.1, Score: 0.8}
		orders := []WindowOrder{{Field: "distance", Descending: false}}
		assert.True(t, op.isEqualInternal(a, nil, b, nil, orders))
		
		orders = []WindowOrder{{Field: "score", Descending: false}}
		assert.False(t, op.isEqualInternal(a, nil, b, nil, orders))
	})
}
