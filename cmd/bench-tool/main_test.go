package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestGenerateRecordFloat32(t *testing.T) {
	rec, schema, err := generateRecord(10, 128, "float32", 4)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()

	if rec.NumRows() != 10 {
		t.Errorf("expected 10 rows, got %d", rec.NumRows())
	}
	if rec.NumCols() != 6 {
		t.Errorf("expected 6 columns, got %d", rec.NumCols())
	}
	// vector column should be FixedSizeList of 128 float32
	vecField := schema.Field(1)
	if vecField.Name != "vector" {
		t.Errorf("expected field name 'vector', got %s", vecField.Name)
	}
}

func TestGenerateRecordInt32(t *testing.T) {
	rec, _, err := generateRecord(5, 32, "int32", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()

	if rec.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", rec.NumRows())
	}
}

func TestGenerateRecordFloat64(t *testing.T) {
	rec, _, err := generateRecord(5, 16, "float64", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()

	if rec.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", rec.NumRows())
	}
}

func TestGenerateRecordComplex64(t *testing.T) {
	rec, schema, err := generateRecord(3, 8, "complex64", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()

	if rec.NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", rec.NumRows())
	}

	// complex64 should have listLen = dim*2
	vecField := schema.Field(1)
	listType, ok := vecField.Type.(*arrow.FixedSizeListType)
	if !ok {
		t.Fatalf("expected FixedSizeListType, got %T", vecField.Type)
	}
	if listType.Len() != 16 {
		t.Errorf("expected list length 16 (2*dim), got %d", listType.Len())
	}
}

func TestGenerateRecordComplex128(t *testing.T) {
	rec, schema, err := generateRecord(3, 8, "complex128", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()

	vecField := schema.Field(1)
	listType, ok := vecField.Type.(*arrow.FixedSizeListType)
	if !ok {
		t.Fatalf("expected FixedSizeListType, got %T", vecField.Type)
	}
	if listType.Len() != 16 {
		t.Errorf("expected list length 16 (2*dim), got %d", listType.Len())
	}
}

func TestGenerateRecordTurboquant(t *testing.T) {
	rec, schema, err := generateRecord(5, 64, "turboquant", 8)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()

	// Check metadata on the vector field
	vecField := schema.Field(1)
	if !vecField.HasMetadata() {
		t.Fatal("expected metadata on vector field for turboquant")
	}
	val, ok := vecField.Metadata.GetValue("longbow.vector_type")
	if !ok || val != "turboquant" {
		t.Errorf("expected longbow.vector_type=turboquant, got %s", val)
	}
	val, ok = vecField.Metadata.GetValue("longbow.turboquant_bits")
	if !ok || val != "8" {
		t.Errorf("expected longbow.turboquant_bits=8, got %s", val)
	}
}

func TestGenerateRecordFloat16(t *testing.T) {
	rec, _, err := generateRecord(5, 32, "float16", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()

	if rec.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", rec.NumRows())
	}
}

func TestGenerateRecordUnsupportedDtype(t *testing.T) {
	_, _, err := generateRecord(5, 32, "bogus", 0)
	if err == nil {
		t.Fatal("expected error for unsupported dtype")
	}
	if !strings.Contains(err.Error(), "unsupported dtype") {
		t.Errorf("expected 'unsupported dtype' in error, got: %v", err)
	}
}

func TestGenerateRecordInt8(t *testing.T) {
	rec, _, err := generateRecord(4, 16, "int8", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 4 {
		t.Errorf("expected 4 rows, got %d", rec.NumRows())
	}
}

func TestGenerateRecordInt16(t *testing.T) {
	rec, _, err := generateRecord(4, 16, "int16", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()
}

func TestGenerateRecordUint8(t *testing.T) {
	rec, _, err := generateRecord(4, 16, "uint8", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()
}

func TestGenerateRecordUint16(t *testing.T) {
	rec, _, err := generateRecord(4, 16, "uint16", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()
}

func TestGenerateRecordUint32(t *testing.T) {
	rec, _, err := generateRecord(4, 16, "uint32", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()
}

func TestGenerateRecordInt64(t *testing.T) {
	rec, _, err := generateRecord(4, 16, "int64", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()
}

func TestGenerateRecordUint64(t *testing.T) {
	rec, _, err := generateRecord(4, 16, "uint64", 0)
	if err != nil {
		t.Fatalf("generateRecord failed: %v", err)
	}
	defer rec.Release()
}

func TestGenerateRecordColumns(t *testing.T) {
	rec, schema, err := generateRecord(10, 32, "float32", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()

	expectedCols := []string{"id", "vector", "timestamp", "geo_point", "active", "category"}
	for i, name := range expectedCols {
		if schema.Field(i).Name != name {
			t.Errorf("column %d: expected %s, got %s", i, name, schema.Field(i).Name)
		}
	}
}

func TestNewReusableSearchState(t *testing.T) {
	s := NewReusableSearchState(128)
	if s == nil {
		t.Fatal("NewReusableSearchState returned nil")
	}
	if len(s.vector) != 256 {
		t.Errorf("expected vector length 256, got %d", len(s.vector))
	}
}

func TestBuildSearchTicketDense(t *testing.T) {
	s := NewReusableSearchState(128)
	ticket := s.BuildSearchTicket("test_ds", 128, "float32", "Dense", 10)

	if len(ticket) == 0 {
		t.Fatal("empty ticket")
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search, ok := parsed["search"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'search' key")
	}
	if search["dataset"] != "test_ds" {
		t.Errorf("expected dataset 'test_ds', got %v", search["dataset"])
	}
	if search["k"] != float64(10) {
		t.Errorf("expected k=10, got %v", search["k"])
	}
	vec, ok := search["vector"].([]interface{})
	if !ok || len(vec) != 128 {
		t.Errorf("expected vector of length 128, got %v", vec)
	}
}

func TestBuildSearchTicketHybrid(t *testing.T) {
	s := NewReusableSearchState(64)
	ticket := s.BuildSearchTicket("ds", 64, "float32", "Hybrid", 5)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	if search["text_query"] != "benchmark search term" {
		t.Errorf("expected text_query, got %v", search["text_query"])
	}
	if search["alpha"] != 0.5 {
		t.Errorf("expected alpha=0.5, got %v", search["alpha"])
	}
}

func TestBuildSearchTicketSparse(t *testing.T) {
	s := NewReusableSearchState(64)
	ticket := s.BuildSearchTicket("ds", 64, "float32", "Sparse", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	if search["text_query"] != "benchmark search term" {
		t.Errorf("expected text_query, got %v", search["text_query"])
	}
}

func TestBuildSearchTicketFiltered(t *testing.T) {
	s := NewReusableSearchState(64)
	ticket := s.BuildSearchTicket("ds", 64, "float32", "Filtered", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	filters, ok := search["filters"].([]interface{})
	if !ok || len(filters) != 1 {
		t.Fatalf("expected 1 filter, got %v", search["filters"])
	}
	filter := filters[0].(map[string]interface{})
	if filter["field"] != "id" {
		t.Errorf("expected field 'id', got %v", filter["field"])
	}
}

func TestBuildSearchTicketFilteredBool(t *testing.T) {
	s := NewReusableSearchState(64)
	ticket := s.BuildSearchTicket("ds", 64, "float32", "FilteredBool", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	filters := search["filters"].([]interface{})
	filter := filters[0].(map[string]interface{})
	if filter["field"] != "active" {
		t.Errorf("expected field 'active', got %v", filter["field"])
	}
}

func TestBuildSearchTicketFilteredString(t *testing.T) {
	s := NewReusableSearchState(64)
	ticket := s.BuildSearchTicket("ds", 64, "float32", "FilteredString", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	filters := search["filters"].([]interface{})
	filter := filters[0].(map[string]interface{})
	if filter["field"] != "category" {
		t.Errorf("expected field 'category', got %v", filter["field"])
	}
}

func TestBuildSearchTicketGraphRAG(t *testing.T) {
	s := NewReusableSearchState(64)
	ticket := s.BuildSearchTicket("ds", 64, "float32", "GraphRAG", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	if search["graph_alpha"] != 0.5 {
		t.Errorf("expected graph_alpha=0.5, got %v", search["graph_alpha"])
	}
}

func TestBuildSearchTicketLearnedIndex(t *testing.T) {
	s := NewReusableSearchState(64)
	ticket := s.BuildSearchTicket("ds", 64, "float32", "LearnedIndex", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	if search["enable_learned_index"] != true {
		t.Errorf("expected enable_learned_index=true, got %v", search["enable_learned_index"])
	}
}

func TestBuildSearchTicketComplex(t *testing.T) {
	s := NewReusableSearchState(32)
	ticket := s.BuildSearchTicket("ds", 32, "complex64", "Dense", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	vec := search["vector"].([]interface{})
	if len(vec) != 64 {
		t.Errorf("expected vector of length 64 (2*dim) for complex64, got %d", len(vec))
	}
}

func TestBuildSearchTicketComplex128(t *testing.T) {
	s := NewReusableSearchState(32)
	ticket := s.BuildSearchTicket("ds", 32, "complex128", "Dense", 10)

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	search := parsed["search"].(map[string]interface{})
	vec := search["vector"].([]interface{})
	if len(vec) != 64 {
		t.Errorf("expected vector of length 64 (2*dim) for complex128, got %d", len(vec))
	}
}

func TestBuildSpecialTicketRecommend(t *testing.T) {
	s := NewReusableSearchState(128)
	ticket := s.BuildSpecialTicket("test_ds", "Recommend")

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	rec, ok := parsed["recommend"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'recommend' key")
	}
	if rec["dataset"] != "test_ds" {
		t.Errorf("expected dataset 'test_ds', got %v", rec["dataset"])
	}
	if rec["k"] != float64(10) {
		t.Errorf("expected k=10, got %v", rec["k"])
	}
}

func TestBuildSpecialTicketGeo(t *testing.T) {
	s := NewReusableSearchState(128)
	ticket := s.BuildSpecialTicket("test_ds", "Geo")

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	geo, ok := parsed["geo_search"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'geo_search' key")
	}
	if geo["dataset"] != "test_ds" {
		t.Errorf("expected dataset 'test_ds', got %v", geo["dataset"])
	}
	center, ok := geo["center"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'center' in geo_search")
	}
	if center["lat"] != 40.7128 {
		t.Errorf("expected lat=40.7128, got %v", center["lat"])
	}
}

func TestBuildSpecialTicketTemporal(t *testing.T) {
	s := NewReusableSearchState(128)
	ticket := s.BuildSpecialTicket("test_ds", "Temporal")

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	temp, ok := parsed["temporal_search"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'temporal_search' key")
	}
	if temp["dataset"] != "test_ds" {
		t.Errorf("expected dataset 'test_ds', got %v", temp["dataset"])
	}
	if temp["search_type"] != "as_of" {
		t.Errorf("expected search_type 'as_of', got %v", temp["search_type"])
	}
}

func TestBuildSpecialTicketByID(t *testing.T) {
	s := NewReusableSearchState(128)
	ticket := s.BuildSpecialTicket("test_ds", "ByID")

	var parsed map[string]interface{}
	if err := json.Unmarshal(ticket, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	byID, ok := parsed["search_by_id"].(map[string]interface{})
	if !ok {
		t.Fatal("missing 'search_by_id' key")
	}
	if byID["dataset"] != "test_ds" {
		t.Errorf("expected dataset 'test_ds', got %v", byID["dataset"])
	}
}

func TestBuildSearchTicketBufferReuse(t *testing.T) {
	s := NewReusableSearchState(128)
	t1 := s.BuildSearchTicket("ds1", 128, "float32", "Dense", 10)
	t2 := s.BuildSearchTicket("ds2", 128, "float32", "Dense", 10)

	// Second call should have overwritten the first
	var parsed map[string]interface{}
	if err := json.Unmarshal(t2, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	search := parsed["search"].(map[string]interface{})
	if search["dataset"] != "ds2" {
		t.Errorf("expected dataset 'ds2', got %v", search["dataset"])
	}
	_ = t1
}

func BenchmarkGenerateRecord(b *testing.B) {
	for i := 0; i < b.N; i++ {
		rec, _, err := generateRecord(1000, 128, "float32", 4)
		if err != nil {
			b.Fatal(err)
		}
		rec.Release()
	}
}

func BenchmarkBuildSearchTicket(b *testing.B) {
	s := NewReusableSearchState(128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.BuildSearchTicket("bench", 128, "float32", "Dense", 10)
	}
}
