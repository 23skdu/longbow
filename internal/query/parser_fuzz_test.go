package query

import (
	"testing"

	"github.com/rs/zerolog"
)

func FuzzZeroAllocTicketParser_Parse(f *testing.F) {
	// Seed corpus with valid simple and nested examples
	f.Add([]byte(`{"name": "test", "limit": 10}`))
	f.Add([]byte(`{"filters": [{"field": "age", "operator": ">", "value": "10"}]}`))
	f.Add([]byte(`{
		"filters": [
			{
				"logic": "OR", 
				"filters": [
					{"field": "a", "operator": "=", "value": "1"},
					{"field": "b", "operator": "=", "value": "2"}
				]
			}
		]
	}`))
	f.Add([]byte(`{"search": {"vector": [0.1, 0.2]}}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Use a fresh parser for each interaction to ensure isolation,
		// though reusing generally should work if reset implemented correctly.
		// For fuzzing we mainly care about Panics.
		parser := NewZeroAllocTicketParser(zerolog.Nop())
		_, _ = parser.Parse(data)
	})
}
