package query

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rs/zerolog"
)

func TestZeroAllocTicketParser_AdvancedSQL(t *testing.T) {
	nopLogger := zerolog.Nop()
	parser := NewZeroAllocTicketParser(&nopLogger)

	t.Run("SubqueryParsing", func(t *testing.T) {
		data := []byte(`{
			"name": "orders",
			"filters": [
				{
					"field": "user_id",
					"operator": "IN",
					"subquery": {
						"name": "active_users",
						"search": {
							"dataset": "users",
							"filters": [{"field": "status", "operator": "==", "value": "active"}]
						}
					}
				}
			]
		}`)
		query, err := parser.Parse(data)
		require.NoError(t, err)
		require.Equal(t, 1, len(query.Filters))
		assert.Equal(t, "user_id", query.Filters[0].Field)
		assert.NotNil(t, query.Filters[0].Subquery)
		assert.Equal(t, "active_users", query.Filters[0].Subquery.Name)
	})

	t.Run("CTEParsing", func(t *testing.T) {
		data := []byte(`{
			"with": [
				{
					"name": "top_vendors",
					"search": {
						"dataset": "vendors",
						"k": 10
					}
				}
			],
			"name": "products",
			"filters": [{"field": "vendor_id", "operator": "in", "value": "top_vendors"}]
		}`)
		query, err := parser.Parse(data)
		require.NoError(t, err)
		require.Equal(t, 1, len(query.CTEs))
		assert.Equal(t, "top_vendors", query.CTEs[0].Name)
		assert.NotNil(t, query.CTEs[0].Search)
		assert.Equal(t, "vendors", query.CTEs[0].Search.Dataset)
	})
}
