package store


import (
	"encoding/json"
	"testing"

	qry "github.com/23skdu/longbow/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZeroAllocVectorSearchParser_Parse(t *testing.T) {
	parser := qry.NewZeroAllocVectorSearchParser(128)

	t.Run("BasicValid", func(t *testing.T) {
		data := []byte(`{"dataset": "movies", "k": 10, "vector": [0.1, 0.2, 0.3]}`)
		req, err := parser.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "movies", req.Dataset)
		assert.Equal(t, 10, req.K)
		assert.Equal(t, []float32{0.1, 0.2, 0.3}, req.Vector)
	})

	t.Run("WithFilters", func(t *testing.T) {
		data := []byte(`{
			"dataset": "books",
			"k": 5,
			"vector": [1.0, 1.0],
			"filters": [{"field": "genre", "operator": "==", "value": "Sci-Fi"}]
		}`)
		req, err := parser.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "books", req.Dataset)
		assert.Equal(t, 5, req.K)
		assert.Equal(t, []float32{1.0, 1.0}, req.Vector)
		require.Equal(t, 1, len(req.Filters))
		assert.Equal(t, "genre", req.Filters[0].Field)
		assert.Equal(t, "==", req.Filters[0].Operator)
		assert.Equal(t, "Sci-Fi", req.Filters[0].Value)
	})

	t.Run("Numbers", func(t *testing.T) {
		data := []byte(`{"vector": [-1, 2.5, 1e2, 0.0001]}`)
		req, err := parser.Parse(data)
		require.NoError(t, err)
		assert.InDeltaSlice(t, []float32{-1, 2.5, 100, 0.0001}, req.Vector, 1e-6)
	})

	t.Run("MalformedVector", func(t *testing.T) {
		data := []byte(`{"vector": [1.0, ]}`)
		_, err := parser.Parse(data)
		assert.Error(t, err)
	})

	t.Run("BufferReuse", func(t *testing.T) {
		// First call
		_, _ = parser.Parse([]byte(`{"vector": [1, 2, 3]}`))

		// Second call with different size
		req, err := parser.Parse([]byte(`{"vector": [4, 5]}`))
		require.NoError(t, err)
		assert.Equal(t, []float32{4, 5}, req.Vector)

		// Check that it didn't leak from previous call
		assert.Equal(t, 2, len(req.Vector))
	})
}

func BenchmarkVectorSearchParser(b *testing.B) {
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = 0.5
	}
	req := qry.VectorSearchRequest{
		Dataset: "test",
		K:       10,
		Vector:  vec,
	}
	data, _ := json.Marshal(req)

	parser := qry.NewZeroAllocVectorSearchParser(128)

	b.Run("ZeroAlloc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = parser.Parse(data)
		}
	})

	b.Run("StandardJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var r qry.VectorSearchRequest
			_ = json.Unmarshal(data, &r)
		}
	})
}
