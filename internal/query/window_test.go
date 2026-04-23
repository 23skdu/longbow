package query

import (
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/stretchr/testify/assert"
)

func helperEncode(m map[string]interface{}) []byte {
	b, _ := core.EncodeMetadata(m)
	return b
}

func helperDecode(b []byte) map[string]interface{} {
	m, _ := core.DecodeMetadata(b)
	return m
}

func TestWindowOperator_RowNumber(t *testing.T) {
	results := []core.SearchResult{
		{ID: 1, Distance: 0.1, Metadata: helperEncode(map[string]interface{}{"group": "A"})},
		{ID: 2, Distance: 0.2, Metadata: helperEncode(map[string]interface{}{"group": "A"})},
		{ID: 3, Distance: 0.05, Metadata: helperEncode(map[string]interface{}{"group": "B"})},
	}

	functions := []WindowFunction{
		{
			Name: "row_number",
			As:   "row_num",
			Over: WindowSpec{
				OrderBy: []WindowOrder{{Field: "distance", Descending: false}},
			},
		},
	}

	op := NewWindowOperator()
	processed := op.Execute(results, functions)

	assert.Equal(t, 3, len(processed))
	// Ordered by distance: 3 (0.05), 1 (0.1), 2 (0.2)
	assert.Equal(t, uint32(3), uint32(processed[0].ID))
	assert.Equal(t, int64(1), helperDecode(processed[0].Metadata)["row_num"])
	assert.Equal(t, uint32(1), uint32(processed[1].ID))
	assert.Equal(t, int64(2), helperDecode(processed[1].Metadata)["row_num"])
	assert.Equal(t, uint32(2), uint32(processed[2].ID))
	assert.Equal(t, int64(3), helperDecode(processed[2].Metadata)["row_num"])
}

func TestWindowOperator_PartitionBy(t *testing.T) {
	results := []core.SearchResult{
		{ID: 1, Distance: 0.2, Metadata: helperEncode(map[string]interface{}{"group": "A"})},
		{ID: 2, Distance: 0.1, Metadata: helperEncode(map[string]interface{}{"group": "A"})},
		{ID: 3, Distance: 0.3, Metadata: helperEncode(map[string]interface{}{"group": "B"})},
		{ID: 4, Distance: 0.05, Metadata: helperEncode(map[string]interface{}{"group": "B"})},
	}

	functions := []WindowFunction{
		{
			Name: "rank",
			As:   "rk",
			Over: WindowSpec{
				PartitionBy: []string{"group"},
				OrderBy:      []WindowOrder{{Field: "distance", Descending: false}},
			},
		},
	}

	op := NewWindowOperator()
	processed := op.Execute(results, functions)

	resMap := make(map[uint32]int64)
	for _, r := range processed {
		resMap[uint32(r.ID)] = helperDecode(r.Metadata)["rk"].(int64)
	}

	assert.Equal(t, int64(2), resMap[1])
	assert.Equal(t, int64(1), resMap[2])
	assert.Equal(t, int64(2), resMap[3])
	assert.Equal(t, int64(1), resMap[4])
}

func TestWindowOperator_Aggregates(t *testing.T) {
	results := []core.SearchResult{
		{ID: 1, Metadata: helperEncode(map[string]interface{}{"val": 10.0})},
		{ID: 2, Metadata: helperEncode(map[string]interface{}{"val": 20.0})},
		{ID: 3, Metadata: helperEncode(map[string]interface{}{"val": 30.0})},
	}

	functions := []WindowFunction{
		{Name: "sum", Field: "val", As: "total_val", Over: WindowSpec{}},
		{Name: "avg", Field: "val", As: "avg_val", Over: WindowSpec{}},
	}

	op := NewWindowOperator()
	processed := op.Execute(results, functions)

	for _, r := range processed {
		m := helperDecode(r.Metadata)
		assert.Equal(t, 60.0, m["total_val"])
		assert.Equal(t, 20.0, m["avg_val"])
	}
}
