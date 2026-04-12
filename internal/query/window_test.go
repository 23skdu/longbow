package query

import (
	"testing"

	"github.com/23skdu/longbow/internal/core"
	"github.com/stretchr/testify/assert"
)

func TestWindowOperator_RowNumber(t *testing.T) {
	results := []core.SearchResult{
		{ID: 1, Distance: 0.1, Metadata: map[string]interface{}{"group": "A"}},
		{ID: 2, Distance: 0.2, Metadata: map[string]interface{}{"group": "A"}},
		{ID: 3, Distance: 0.05, Metadata: map[string]interface{}{"group": "B"}},
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
	assert.Equal(t, 1, processed[0].Metadata["row_num"])
	assert.Equal(t, uint32(1), uint32(processed[1].ID))
	assert.Equal(t, 2, processed[1].Metadata["row_num"])
	assert.Equal(t, uint32(2), uint32(processed[2].ID))
	assert.Equal(t, 3, processed[2].Metadata["row_num"])
}

func TestWindowOperator_PartitionBy(t *testing.T) {
	results := []core.SearchResult{
		{ID: 1, Distance: 0.2, Metadata: map[string]interface{}{"group": "A"}},
		{ID: 2, Distance: 0.1, Metadata: map[string]interface{}{"group": "A"}},
		{ID: 3, Distance: 0.3, Metadata: map[string]interface{}{"group": "B"}},
		{ID: 4, Distance: 0.05, Metadata: map[string]interface{}{"group": "B"}},
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

	// Findings for Group A:
	// ID 2 (0.1) -> rank 1
	// ID 1 (0.2) -> rank 2
	// Findings for Group B:
	// ID 4 (0.05) -> rank 1
	// ID 3 (0.3) -> rank 2

	resMap := make(map[uint32]int)
	for _, r := range processed {
		resMap[uint32(r.ID)] = r.Metadata["rk"].(int)
	}

	assert.Equal(t, 2, resMap[1])
	assert.Equal(t, 1, resMap[2])
	assert.Equal(t, 2, resMap[3])
	assert.Equal(t, 1, resMap[4])
}

func TestWindowOperator_Aggregates(t *testing.T) {
	results := []core.SearchResult{
		{ID: 1, Metadata: map[string]interface{}{"val": 10.0}},
		{ID: 2, Metadata: map[string]interface{}{"val": 20.0}},
		{ID: 3, Metadata: map[string]interface{}{"val": 30.0}},
	}

	functions := []WindowFunction{
		{Name: "sum", Field: "val", As: "total_val", Over: WindowSpec{}},
		{Name: "avg", Field: "val", As: "avg_val", Over: WindowSpec{}},
	}

	op := NewWindowOperator()
	processed := op.Execute(results, functions)

	for _, r := range processed {
		assert.Equal(t, 60.0, r.Metadata["total_val"])
		assert.Equal(t, 20.0, r.Metadata["avg_val"])
	}
}
