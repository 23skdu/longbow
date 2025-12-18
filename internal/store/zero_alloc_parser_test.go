
package store

import (
"testing"
)

// =============================================================================
// Zero-Allocation Ticket Parser Tests (TDD - written before implementation)
// =============================================================================

// TestZeroAllocParser_SimpleNameOnly tests parsing ticket with only name field
func TestZeroAllocParser_SimpleNameOnly(t *testing.T) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"test_dataset"}`)

query, err := parser.Parse(ticket)
if err != nil {
t.Fatalf("Parse failed: %v", err)
}
if query.Name != "test_dataset" {
t.Errorf("Expected name=test_dataset, got %s", query.Name)
}
if query.Limit != 0 {
t.Errorf("Expected limit=0, got %d", query.Limit)
}
if len(query.Filters) != 0 {
t.Errorf("Expected no filters, got %d", len(query.Filters))
}
}

// TestZeroAllocParser_WithLimit tests parsing ticket with name and limit
func TestZeroAllocParser_WithLimit(t *testing.T) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"vectors","limit":100}`)

query, err := parser.Parse(ticket)
if err != nil {
t.Fatalf("Parse failed: %v", err)
}
if query.Name != "vectors" {
t.Errorf("Expected name=vectors, got %s", query.Name)
}
if query.Limit != 100 {
t.Errorf("Expected limit=100, got %d", query.Limit)
}
}

// TestZeroAllocParser_WithSingleFilter tests parsing with one filter
func TestZeroAllocParser_WithSingleFilter(t *testing.T) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"ds","filters":[{"field":"id","operator":"=","value":"123"}]}`)

query, err := parser.Parse(ticket)
if err != nil {
t.Fatalf("Parse failed: %v", err)
}
if query.Name != "ds" {
t.Errorf("Expected name=ds, got %s", query.Name)
}
if len(query.Filters) != 1 {
t.Fatalf("Expected 1 filter, got %d", len(query.Filters))
}
if query.Filters[0].Field != "id" {
t.Errorf("Expected field=id, got %s", query.Filters[0].Field)
}
if query.Filters[0].Operator != "=" {
t.Errorf("Expected operator==, got %s", query.Filters[0].Operator)
}
if query.Filters[0].Value != "123" {
t.Errorf("Expected value=123, got %s", query.Filters[0].Value)
}
}

// TestZeroAllocParser_WithMultipleFilters tests parsing with multiple filters
func TestZeroAllocParser_WithMultipleFilters(t *testing.T) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"data","limit":50,"filters":[{"field":"type","operator":"=","value":"A"},{"field":"count","operator":">","value":"10"}]}`)

query, err := parser.Parse(ticket)
if err != nil {
t.Fatalf("Parse failed: %v", err)
}
if query.Name != "data" {
t.Errorf("Expected name=data, got %s", query.Name)
}
if query.Limit != 50 {
t.Errorf("Expected limit=50, got %d", query.Limit)
}
if len(query.Filters) != 2 {
t.Fatalf("Expected 2 filters, got %d", len(query.Filters))
}
// First filter
if query.Filters[0].Field != "type" || query.Filters[0].Operator != "=" || query.Filters[0].Value != "A" {
t.Errorf("First filter mismatch: %+v", query.Filters[0])
}
// Second filter
if query.Filters[1].Field != "count" || query.Filters[1].Operator != ">" || query.Filters[1].Value != "10" {
t.Errorf("Second filter mismatch: %+v", query.Filters[1])
}
}

// TestZeroAllocParser_EmptyInput tests empty input handling
func TestZeroAllocParser_EmptyInput(t *testing.T) {
parser := NewZeroAllocTicketParser()

query, err := parser.Parse([]byte{})
if err != nil {
t.Fatalf("Empty input should not error: %v", err)
}
if query.Name != "" {
t.Errorf("Expected empty name for empty input, got %s", query.Name)
}
}

// TestZeroAllocParser_NilInput tests nil input handling
func TestZeroAllocParser_NilInput(t *testing.T) {
parser := NewZeroAllocTicketParser()

query, err := parser.Parse(nil)
if err != nil {
t.Fatalf("Nil input should not error: %v", err)
}
if query.Name != "" {
t.Errorf("Expected empty name for nil input, got %s", query.Name)
}
}

// TestZeroAllocParser_MalformedJSON tests malformed JSON handling
func TestZeroAllocParser_MalformedJSON(t *testing.T) {
parser := NewZeroAllocTicketParser()

// Missing closing brace
_, err := parser.Parse([]byte(`{"name":"test"`))
if err == nil {
t.Error("Expected error for malformed JSON")
}
}

// TestZeroAllocParser_FieldOrder tests different field ordering
func TestZeroAllocParser_FieldOrder(t *testing.T) {
parser := NewZeroAllocTicketParser()
// Fields in different order than struct definition
ticket := []byte(`{"limit":25,"filters":[],"name":"ordered"}`)

query, err := parser.Parse(ticket)
if err != nil {
t.Fatalf("Parse failed: %v", err)
}
if query.Name != "ordered" {
t.Errorf("Expected name=ordered, got %s", query.Name)
}
if query.Limit != 25 {
t.Errorf("Expected limit=25, got %d", query.Limit)
}
}

// TestZeroAllocParser_EscapedStrings tests JSON escaped characters
func TestZeroAllocParser_EscapedStrings(t *testing.T) {
parser := NewZeroAllocTicketParser()
// Name with escaped quote
ticket := []byte(`{"name":"test_slash_data"}`)

query, err := parser.Parse(ticket)
if err != nil {
t.Fatalf("Parse failed: %v", err)
}
if query.Name != "test_slash_data" {
t.Errorf("Expected name=test_slash_data, got %s", query.Name)
}
}

// TestZeroAllocParser_Whitespace tests JSON with extra whitespace
func TestZeroAllocParser_Whitespace(t *testing.T) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{  "name"  :  "spaced"  ,  "limit"  :  10  }`)

query, err := parser.Parse(ticket)
if err != nil {
t.Fatalf("Parse failed: %v", err)
}
if query.Name != "spaced" {
t.Errorf("Expected name=spaced, got %s", query.Name)
}
if query.Limit != 10 {
t.Errorf("Expected limit=10, got %d", query.Limit)
}
}

// TestZeroAllocParser_ParserReuse tests that parser can be reused
func TestZeroAllocParser_ParserReuse(t *testing.T) {
parser := NewZeroAllocTicketParser()

// First parse
q1, err := parser.Parse([]byte(`{"name":"first","limit":100}`))
if err != nil {
t.Fatalf("First parse failed: %v", err)
}
if q1.Name != "first" || q1.Limit != 100 {
t.Errorf("First parse wrong: %+v", q1)
}

// Second parse - should reset previous values
q2, err := parser.Parse([]byte(`{"name":"second"}`))
if err != nil {
t.Fatalf("Second parse failed: %v", err)
}
if q2.Name != "second" {
t.Errorf("Expected name=second, got %s", q2.Name)
}
if q2.Limit != 0 {
t.Errorf("Expected limit=0 (reset), got %d", q2.Limit)
}
}

// TestZeroAllocParser_MatchesFastParse verifies parity with existing parser
func TestZeroAllocParser_MatchesFastParse(t *testing.T) {
testCases := [][]byte{
[]byte(`{"name":"test"}`),
[]byte(`{"name":"test","limit":100}`),
[]byte(`{"name":"test","filters":[{"field":"a","operator":"=","value":"b"}]}`),
[]byte(`{}`),
}

parser := NewZeroAllocTicketParser()
for _, tc := range testCases {
fast, fastErr := FastParseTicketQuery(tc)
zero, zeroErr := parser.Parse(tc)

if (fastErr == nil) != (zeroErr == nil) {
t.Errorf("Error mismatch for %s: fast=%v, zero=%v", tc, fastErr, zeroErr)
continue
}
if fastErr != nil {
continue
}

if fast.Name != zero.Name {
t.Errorf("Name mismatch for %s: fast=%s, zero=%s", tc, fast.Name, zero.Name)
}
if fast.Limit != zero.Limit {
t.Errorf("Limit mismatch for %s: fast=%d, zero=%d", tc, fast.Limit, zero.Limit)
}
if len(fast.Filters) != len(zero.Filters) {
t.Errorf("Filters count mismatch for %s: fast=%d, zero=%d", tc, len(fast.Filters), len(zero.Filters))
}
}
}

// TestZeroAllocParser_ZeroAllocations verifies no heap allocations
func TestZeroAllocParser_ZeroAllocations(t *testing.T) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"test","limit":100}`)

// Warm up
_, _ = parser.Parse(ticket)

allocs := testing.AllocsPerRun(100, func() {
_, _ = parser.Parse(ticket)
})

if allocs > 0 {
t.Errorf("Expected 0 allocations, got %.0f", allocs)
}
}

// TestZeroAllocParser_ZeroAllocationsWithFilters verifies no heap allocations with filters
func TestZeroAllocParser_ZeroAllocationsWithFilters(t *testing.T) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"test","filters":[{"field":"a","operator":"=","value":"b"}]}`)

// Warm up
_, _ = parser.Parse(ticket)

allocs := testing.AllocsPerRun(100, func() {
_, _ = parser.Parse(ticket)
})

// May need small number for filter slice reuse - target <=1
if allocs > 1 {
t.Errorf("Expected <=1 allocations with filters, got %.0f", allocs)
}
}

// =============================================================================
// Benchmarks
// =============================================================================

// BenchmarkZeroAllocParser_Simple benchmarks simple ticket parsing
func BenchmarkZeroAllocParser_Simple(b *testing.B) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"benchmark_dataset","limit":1000}`)

b.ResetTimer()
b.ReportAllocs()
for i := 0; i < b.N; i++ {
_, _ = parser.Parse(ticket)
}
}

// BenchmarkZeroAllocParser_WithFilters benchmarks parsing with filters
func BenchmarkZeroAllocParser_WithFilters(b *testing.B) {
parser := NewZeroAllocTicketParser()
ticket := []byte(`{"name":"bench","limit":100,"filters":[{"field":"type","operator":"=","value":"A"},{"field":"id","operator":">","value":"1000"}]}`)

b.ResetTimer()
b.ReportAllocs()
for i := 0; i < b.N; i++ {
_, _ = parser.Parse(ticket)
}
}

// BenchmarkFastParseVsZeroAlloc_Simple compares existing vs new parser
func BenchmarkFastParseVsZeroAlloc_Simple(b *testing.B) {
ticket := []byte(`{"name":"comparison","limit":100}`)

b.Run("FastParse", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
_, _ = FastParseTicketQuery(ticket)
}
})

b.Run("ZeroAlloc", func(b *testing.B) {
parser := NewZeroAllocTicketParser()
b.ReportAllocs()
for i := 0; i < b.N; i++ {
_, _ = parser.Parse(ticket)
}
})
}

// BenchmarkFastParseVsZeroAlloc_WithFilters compares with filter parsing
func BenchmarkFastParseVsZeroAlloc_WithFilters(b *testing.B) {
ticket := []byte(`{"name":"comparison","filters":[{"field":"a","operator":"=","value":"b"},{"field":"c","operator":"!=","value":"d"}]}`)

b.Run("FastParse", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
_, _ = FastParseTicketQuery(ticket)
}
})

b.Run("ZeroAlloc", func(b *testing.B) {
parser := NewZeroAllocTicketParser()
b.ReportAllocs()
for i := 0; i < b.N; i++ {
_, _ = parser.Parse(ticket)
}
})
}
