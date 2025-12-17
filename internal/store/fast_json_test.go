
package store

import (
	"encoding/json"
	"testing"
)

// TestFastParseTicketQuery_SimpleNameOnly tests parsing ticket with only name
func TestFastParseTicketQuery_SimpleNameOnly(t *testing.T) {
	ticket := []byte(`{"name":"test_dataset"}`)

	query, err := FastParseTicketQuery(ticket)
	if err != nil {
		t.Fatalf("FastParseTicketQuery failed: %v", err)
	}

	if query.Name != "test_dataset" {
		t.Errorf("expected name 'test_dataset', got '%s'", query.Name)
	}
	if query.Limit != 0 {
		t.Errorf("expected limit 0, got %d", query.Limit)
	}
	if len(query.Filters) != 0 {
		t.Errorf("expected 0 filters, got %d", len(query.Filters))
	}
}

// TestFastParseTicketQuery_WithLimit tests parsing ticket with name and limit
func TestFastParseTicketQuery_WithLimit(t *testing.T) {
	ticket := []byte(`{"name":"vectors","limit":100}`)

	query, err := FastParseTicketQuery(ticket)
	if err != nil {
		t.Fatalf("FastParseTicketQuery failed: %v", err)
	}

	if query.Name != "vectors" {
		t.Errorf("expected name 'vectors', got '%s'", query.Name)
	}
	if query.Limit != 100 {
		t.Errorf("expected limit 100, got %d", query.Limit)
	}
}

// TestFastParseTicketQuery_WithFilters tests parsing ticket with filters
func TestFastParseTicketQuery_WithFilters(t *testing.T) {
	ticket := []byte(`{"name":"embeddings","limit":50,"filters":[{"field":"category","operator":"=","value":"science"}]}`)

	query, err := FastParseTicketQuery(ticket)
	if err != nil {
		t.Fatalf("FastParseTicketQuery failed: %v", err)
	}

	if query.Name != "embeddings" {
		t.Errorf("expected name 'embeddings', got '%s'", query.Name)
	}
	if query.Limit != 50 {
		t.Errorf("expected limit 50, got %d", query.Limit)
	}
	if len(query.Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(query.Filters))
	}
	if query.Filters[0].Field != "category" {
		t.Errorf("expected field 'category', got '%s'", query.Filters[0].Field)
	}
	if query.Filters[0].Operator != "=" {
		t.Errorf("expected operator '=', got '%s'", query.Filters[0].Operator)
	}
	if query.Filters[0].Value != "science" {
		t.Errorf("expected value 'science', got '%s'", query.Filters[0].Value)
	}
}

// TestFastParseTicketQuery_MultipleFilters tests multiple filter conditions
func TestFastParseTicketQuery_MultipleFilters(t *testing.T) {
	ticket := []byte(`{"name":"docs","filters":[{"field":"status","operator":"=","value":"active"},{"field":"priority","operator":">","value":"5"}]}`)

	query, err := FastParseTicketQuery(ticket)
	if err != nil {
		t.Fatalf("FastParseTicketQuery failed: %v", err)
	}

	if len(query.Filters) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(query.Filters))
	}
	if query.Filters[1].Field != "priority" {
		t.Errorf("expected second filter field 'priority', got '%s'", query.Filters[1].Field)
	}
}

// TestFastParseTicketQuery_EmptyTicket tests empty/invalid input
func TestFastParseTicketQuery_EmptyTicket(t *testing.T) {
	query, err := FastParseTicketQuery([]byte{})
	if err != nil {
		return
	}
	if query.Name != "" {
		t.Errorf("expected empty name for empty input, got '%s'", query.Name)
	}
}

// TestFastParseTicketQuery_PlainStringTicket tests non-JSON ticket
func TestFastParseTicketQuery_PlainStringTicket(t *testing.T) {
	ticket := []byte("my_dataset")
	query, err := FastParseTicketQuery(ticket)
	if err == nil && query.Name != "" {
		t.Logf("Parser extracted name from plain string: %s", query.Name)
	}
}

// TestFastParseTicketQuery_MatchesStdlib verifies parity with encoding/json
func TestFastParseTicketQuery_MatchesStdlib(t *testing.T) {
	testCases := [][]byte{
		[]byte(`{"name":"test"}`),
		[]byte(`{"name":"vectors","limit":100}`),
		[]byte(`{"name":"data","limit":-1,"filters":[{"field":"f","operator":"=","value":"v"}]}`),
		[]byte(`{"limit":50}`),
		[]byte(`{}`),
	}

	for i, tc := range testCases {
		var stdQuery TicketQuery
		stdErr := json.Unmarshal(tc, &stdQuery)

		fastQuery, fastErr := FastParseTicketQuery(tc)

		if (stdErr == nil) != (fastErr == nil) {
			t.Errorf("case %d: error mismatch - stdlib: %v, fast: %v", i, stdErr, fastErr)
			continue
		}

		if stdErr != nil {
			continue
		}

		if stdQuery.Name != fastQuery.Name {
			t.Errorf("case %d: Name mismatch - stdlib: %s, fast: %s", i, stdQuery.Name, fastQuery.Name)
		}
		if stdQuery.Limit != fastQuery.Limit {
			t.Errorf("case %d: Limit mismatch - stdlib: %d, fast: %d", i, stdQuery.Limit, fastQuery.Limit)
		}
		if len(stdQuery.Filters) != len(fastQuery.Filters) {
			t.Errorf("case %d: Filters length mismatch - stdlib: %d, fast: %d", i, len(stdQuery.Filters), len(fastQuery.Filters))
		}
	}
}

// TestFastParseTicketQuery_LargePayload tests performance with larger filter sets
func TestFastParseTicketQuery_LargePayload(t *testing.T) {
	filters := `[`
	for i := 0; i < 20; i++ {
		if i > 0 {
			filters += ","
		}
		filters += `{"field":"field` + string(rune('a'+i)) + `","operator":"=","value":"value` + string(rune('0'+i%10)) + `"}`
	}
	filters += `]`

	ticket := []byte(`{"name":"large_dataset","limit":1000,"filters":` + filters + `}`)

	query, err := FastParseTicketQuery(ticket)
	if err != nil {
		t.Fatalf("FastParseTicketQuery failed: %v", err)
	}

	if query.Name != "large_dataset" {
		t.Errorf("expected name 'large_dataset', got '%s'", query.Name)
	}
	if len(query.Filters) != 20 {
		t.Errorf("expected 20 filters, got %d", len(query.Filters))
	}
}

// BenchmarkFastParseTicketQuery_Simple benchmarks simple ticket parsing
func BenchmarkFastParseTicketQuery_Simple(b *testing.B) {
	ticket := []byte(`{"name":"benchmark_dataset","limit":100}`)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = FastParseTicketQuery(ticket)
	}
}

// BenchmarkStdlibJSON_Simple benchmarks stdlib for comparison
func BenchmarkStdlibJSON_Simple(b *testing.B) {
	ticket := []byte(`{"name":"benchmark_dataset","limit":100}`)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var q TicketQuery
		_ = json.Unmarshal(ticket, &q)
	}
}

// BenchmarkFastParseTicketQuery_WithFilters benchmarks ticket with filters
func BenchmarkFastParseTicketQuery_WithFilters(b *testing.B) {
	ticket := []byte(`{"name":"vectors","limit":50,"filters":[{"field":"category","operator":"=","value":"test"},{"field":"status","operator":"!=","value":"deleted"}]}`)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = FastParseTicketQuery(ticket)
	}
}

// BenchmarkStdlibJSON_WithFilters benchmarks stdlib with filters
func BenchmarkStdlibJSON_WithFilters(b *testing.B) {
	ticket := []byte(`{"name":"vectors","limit":50,"filters":[{"field":"category","operator":"=","value":"test"},{"field":"status","operator":"!=","value":"deleted"}]}`)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var q TicketQuery
		_ = json.Unmarshal(ticket, &q)
	}
}
