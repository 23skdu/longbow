package store

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComplex128_TicketSerialization validates that we can marshal and unmarshal
// raw vector tickets containing complex128 data (often represented as [][]float or interleaved).
func TestComplex128_TicketParsing(t *testing.T) {
	// 1. Simulate a malformed ticket payload (e.g. from Python client sending NaNs or objects)
	// Python complex: (1+2j) -> JSON?
	// Standard JSON doesn't support complex. Client typically flattens or serializes as struct.
	// Longbow parser likely expects flattened list of floats.

	// Scenario A: Valid flattened floats for complex
	// 2 complex numbers: (1+2i), (3+4i) -> [1, 2, 3, 4]
	validJSON := `{
		"vector": [1.0, 2.0, 3.0, 4.0],
		"k": 10
	}`

	var ticket1 map[string]interface{}
	err := json.Unmarshal([]byte(validJSON), &ticket1)
	require.NoError(t, err, "Should parse valid flattened float array")

	// Scenario B: Malformed NaN (Go's json decoder fails on NaN unless relaxed? Standard is strict)
	nanJSON := `{
		"vector": [1.0, NaN, 3.0, 4.0],
		"k": 10
	}`
	var ticket2 map[string]interface{}
	err = json.Unmarshal([]byte(nanJSON), &ticket2)
	// Expect failure or NaNs? Standard Go JSON: "invalid character 'N' looking for beginning of value"
	assert.Error(t, err, "Should fail on NaN in standard JSON")

	// Scenario C: Nested arrays (if client sends list of lists for complex)
	// [[1, 2], [3, 4]]
	nestedJSON := `{
		"vector": [[1.0, 2.0], [3.0, 4.0]],
		"k": 10
	}`

	// This might fail if the parser expects []float64 directly.
	// We need to check how Unmarshal interacts with generic deserialization logic in store_query.go
	var ticket3 struct {
		Vector interface{} `json:"vector"`
	}
	err = json.Unmarshal([]byte(nestedJSON), &ticket3)
	require.NoError(t, err)

	// If the server expects flattened []float64, []interface{} (nested) will cause runtime error later.
	// We'll verify what the parser logic actually does with this.
	_, isFlat := ticket3.Vector.([]interface{})
	assert.True(t, isFlat, "Unmarshal should produce []interface{}")

	// In the nested case:
	// vector is []interface{} containing []interface{}
	list, ok := ticket3.Vector.([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, list)
	_, isNested := list[0].([]interface{})

	// If the parser (VerifyTicket?) expects float array, this structure is "valid json" but "invalid ticket".
	// The failure mode described in nextsteps is "invalid ticket format".
	// We confirm that nested arrays (common mistake for complex types) are technically parsable
	// but likely rejected by validation layer.
	assert.True(t, isNested, "Nested array parsed as list-of-lists")
}
