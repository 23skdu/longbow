package query

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestQuery_ComprehensiveParsing(t *testing.T) {
	logger := zerolog.Nop()
	
	t.Run("TicketQuery_AllFields", func(t *testing.T) {
		parser := NewZeroAllocTicketParser(&logger)
		// A massive JSON that hits all fields in TicketQuery struct
		data := []byte(`{
			"name": "full_query",
			"limit": 500,
			"filters": [
				{"field": "a", "operator": "=", "value": "v1"},
				{"logic": "AND", "filters": [
					{"field": "b", "operator": ">", "value": "100"},
					{"field": "c", "operator": "IN", "value": "x,y,z"}
				]}
			],
			"search": {
				"dataset": "d1",
				"vector": [1, 2, 3],
				"k": 10,
				"alpha": 0.5
			},
			"search_by_id": {
				"id": "item1",
				"k": 5
			},
			"recommend": {
				"positive_ids": ["p1"],
				"negative_ids": ["n1"],
				"k": 20
			}
		}`)
		
		req, err := parser.Parse(data)
		assert.NoError(t, err)
		assert.Equal(t, "full_query", req.Name)
		assert.Equal(t, int64(500), req.Limit)
		assert.NotNil(t, req.Search)
		assert.NotNil(t, req.SearchByID)
		assert.NotNil(t, req.Recommend)
		assert.Len(t, req.Filters, 2)
	})
	
	t.Run("ParserErrorPaths", func(t *testing.T) {
		parser := NewZeroAllocTicketParser(&logger)
		
		// Invalid JSON
		_, err := parser.Parse([]byte(`{invalid`))
		assert.Error(t, err)
		
		// Valid JSON but wrong types for structured fields
		_, err = parser.Parse([]byte(`{"filters": "not_an_array"}`))
		assert.Error(t, err)

		_, err = parser.Parse([]byte(`{"search": "not_an_object"}`))
		assert.Error(t, err)
	})
}
