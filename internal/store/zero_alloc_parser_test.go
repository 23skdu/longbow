package store

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZeroAllocTicketParser_Parse(t *testing.T) {
	parser := NewZeroAllocTicketParser()

	t.Run("BasicValid", func(t *testing.T) {
		data := []byte(`{"name": "cities", "limit": 100}`)
		query, err := parser.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "cities", query.Name)
		assert.Equal(t, int64(100), query.Limit)
	})

	t.Run("WithFilters", func(t *testing.T) {
		data := []byte(`{
			"name": "users",
			"limit": 50,
			"filters": [
				{"field": "country", "operator": "==", "value": "US"},
				{"field": "age", "operator": ">", "value": "30"}
			]
		}`)
		query, err := parser.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "users", query.Name)
		assert.Equal(t, int64(50), query.Limit)
		require.Equal(t, 2, len(query.Filters))
		assert.Equal(t, "country", query.Filters[0].Field)
		assert.Equal(t, "==", query.Filters[0].Operator)
		assert.Equal(t, "US", query.Filters[0].Value)
		assert.Equal(t, "age", query.Filters[1].Field)
		assert.Equal(t, ">", query.Filters[1].Operator)
		assert.Equal(t, "30", query.Filters[1].Value)
	})

	t.Run("MalformedJSON", func(t *testing.T) {
		data := []byte(`{"name": "incomplete"`)
		_, err := parser.Parse(data)
		assert.Error(t, err)
	})

	t.Run("EscapeSequences", func(t *testing.T) {
		data := []byte(`{"name": "escaped\nname", "limit": 5}`)
		query, err := parser.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "escaped\nname", query.Name)
	})

	t.Run("UnknownFields", func(t *testing.T) {
		// Parser should skip unknown fields
		data := []byte(`{"name": "test", "unknown": 123, "limit": 10}`)
		query, err := parser.Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "test", query.Name)
		assert.Equal(t, int64(10), query.Limit)
	})
}

func TestParseTicketQuerySafe(t *testing.T) {
	data := []byte(`{"name": "pooled", "limit": 10}`)

	// Test concurrent access to the pool
	for i := 0; i < 100; i++ {
		query, err := ParseTicketQuerySafe(data)
		require.NoError(t, err)
		assert.Equal(t, "pooled", query.Name)
	}

	stats := GetParserPoolStats()
	assert.True(t, stats.Gets > 0)
}

func BenchmarkZeroAllocParser(b *testing.B) {
	data := []byte(`{"name": "users", "limit": 50, "filters": [{"field": "country", "operator": "==", "value": "US"}]}`)
	parser := NewZeroAllocTicketParser()

	b.Run("ZeroAlloc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = parser.Parse(data)
		}
	})

	b.Run("StandardJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var q TicketQuery
			_ = json.Unmarshal(data, &q)
		}
	})
}
