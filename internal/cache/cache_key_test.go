package cache

import (
	"testing"

	qry "github.com/23skdu/longbow/internal/query"
	"github.com/stretchr/testify/assert"
)

func TestHashQuery(t *testing.T) {
	req := &qry.VectorSearchRequest{
		Dataset:    "test",
		Vector:     []float32{1.0, 2.0, 3.0},
		K:          10,
		TextQuery:  "search text",
		Alpha:      0.5,
		GraphAlpha: 1.0,
	}

	h1 := HashQuery(req)
	assert.NotZero(t, h1)

	t.Run("deterministic", func(t *testing.T) {
		h2 := HashQuery(req)
		assert.Equal(t, h1, h2)
	})

	t.Run("different dataset", func(t *testing.T) {
		req2 := *req
		req2.Dataset = "other"
		assert.NotEqual(t, h1, HashQuery(&req2))
	})

	t.Run("different vector", func(t *testing.T) {
		req2 := *req
		req2.Vector = []float32{4.0, 5.0}
		assert.NotEqual(t, h1, HashQuery(&req2))
	})

	t.Run("different k", func(t *testing.T) {
		req2 := *req
		req2.K = 20
		assert.NotEqual(t, h1, HashQuery(&req2))
	})

	t.Run("with filters", func(t *testing.T) {
		req2 := *req
		req2.Filters = []qry.Filter{
			{Field: "color", Operator: "eq", Value: "red", Logic: "and"},
		}
		assert.NotEqual(t, h1, HashQuery(&req2))
	})

	t.Run("nested filters", func(t *testing.T) {
		req2 := *req
		req2.Filters = []qry.Filter{
			{
				Field: "size", Operator: "gt", Value: "10", Logic: "and",
				Filters: []qry.Filter{
					{Field: "color", Operator: "eq", Value: "blue"},
				},
			},
		}
		assert.NotZero(t, HashQuery(&req2))
	})

	t.Run("local only", func(t *testing.T) {
		req2 := *req
		req2.LocalOnly = true
		assert.NotEqual(t, h1, HashQuery(&req2))
	})

	t.Run("empty vector", func(t *testing.T) {
		req2 := *req
		req2.Vector = nil
		h := HashQuery(&req2)
		assert.NotZero(t, h)
	})
}
