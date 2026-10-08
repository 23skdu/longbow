package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	ticket := s.BuildSearchTicket("test_ds", 128, "float32", "Dense", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 64, "float32", "Hybrid", 5, 0)

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
	ticket := s.BuildSearchTicket("ds", 64, "float32", "Sparse", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 64, "float32", "Filtered", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 64, "float32", "FilteredBool", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 64, "float32", "FilteredString", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 64, "float32", "GraphRAG", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 64, "float32", "LearnedIndex", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 32, "complex64", "Dense", 10, 0)

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
	ticket := s.BuildSearchTicket("ds", 32, "complex128", "Dense", 10, 0)

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
	ticket := s.BuildSpecialTicket("test_ds", "Recommend", 0, 0)

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
	ticket := s.BuildSpecialTicket("test_ds", "Geo", 0, 0)

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
	ticket := s.BuildSpecialTicket("test_ds", "Temporal", 0, 0)

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
	ticket := s.BuildSpecialTicket("test_ds", "ByID", 0, 0)

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
	t1 := s.BuildSearchTicket("ds1", 128, "float32", "Dense", 10, 0)
	t2 := s.BuildSearchTicket("ds2", 128, "float32", "Dense", 10, 0)

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
	for b.Loop() {
		rec, _, err := generateRecord(1000, 128, "float32", 4)
		if err != nil {
			b.Fatal(err)
		}
		rec.Release()
	}
}

func BenchmarkBuildSearchTicket(b *testing.B) {
	s := NewReusableSearchState(128)
	for b.Loop() {
		s.BuildSearchTicket("bench", 128, "float32", "Dense", 10, 0)
	}
}

// TestTicketDeterminismAcrossCalls pins R16: the query a given (mode, index)
// denotes must not depend on the clock or on call order. Before this, query
// vectors came from the global math/rand, which Go seeds per process, so two
// runs of the same binary issued different queries and a percentage comparison
// between them measured noise.
func TestTicketDeterminismAcrossCalls(t *testing.T) {
	first := map[string]string{}
	for _, tc := range []struct {
		mode  string
		dim   int
		dtype string
	}{
		{"Dense", 128, "float32"},
		{"Hybrid", 128, "float32"},
		{"Filtered", 128, "float32"},
		{"GraphRAG", 128, "float32"},
		{"LearnedIndex", 128, "float32"},
	} {
		s := NewReusableSearchState(128)
		ticket := s.BuildSearchTicket("ds", tc.dim, tc.dtype, tc.mode, 10, 7)
		first[tc.mode] = string(ticket)

		// A fresh state, and a fresh process would have a different global
		// seed, must still produce the same bytes.
		s2 := NewReusableSearchState(128)
		if got := string(s2.BuildSearchTicket("ds", tc.dim, tc.dtype, tc.mode, 10, 7)); got != first[tc.mode] {
			t.Errorf("mode %s: fresh state produced different ticket for the same index", tc.mode)
		}

		// A different index must produce a different query, or the mode is
		// measuring one hot vector.
		s3 := NewReusableSearchState(128)
		if got := string(s3.BuildSearchTicket("ds", tc.dim, tc.dtype, tc.mode, 10, 8)); got == first[tc.mode] {
			t.Errorf("mode %s: query index 8 produced the same vector as index 7", tc.mode)
		}
	}
}

// TestByIDUsesQueryIndex covers H6. ByID was hardcoded to id "0", so every query
// hit one permanently hot node: the mode measured a cache hit rather than a
// search, and swung 7x between runs.
func TestByIDUsesQueryIndex(t *testing.T) {
	const corpus = 1000
	seen := map[string]bool{}
	for i := 0; i < 10; i++ {
		s := NewReusableSearchState(8)
		ticket := string(s.BuildSpecialTicket("ds", "ByID", i, corpus))
		if !strings.Contains(ticket, fmt.Sprintf(`"id":"%d"`, i)) {
			t.Fatalf("ByID query %d did not request id %d: %s", i, i, ticket)
		}
		if seen[ticket] {
			t.Fatalf("ByID queries %d produced a duplicate ticket", i)
		}
		seen[ticket] = true
	}

	// Wraps into the corpus rather than running off the end.
	s := NewReusableSearchState(8)
	ticket := string(s.BuildSpecialTicket("ds", "ByID", corpus+5, corpus))
	if !strings.Contains(ticket, `"id":"5"`) {
		t.Errorf("ByID index %d did not wrap into corpus size %d: %s", corpus+5, corpus, ticket)
	}
}

// TestCorpusGenerationIsDeterministic covers the other half of R16/H7: the
// corpus was seeded from time.Now().UnixNano() per chunk.
func TestCorpusGenerationIsDeterministic(t *testing.T) {
	RunSeed = defaultSeed
	defer func() { RunSeed = defaultSeed }()

	gen := func() []float64 {
		rng := rand.New(rand.NewSource(RunSeed))
		_, _, err := generateRecordBatch(rng, 0, 32, 8, "float32", 4)
		if err != nil {
			t.Fatalf("generateRecordBatch: %v", err)
		}
		rec, _, err := generateRecordBatch(rng, 0, 32, 8, "float32", 4)
		if err != nil {
			t.Fatalf("generateRecordBatch: %v", err)
		}
		vals := rec.Column(1).(*array.FixedSizeList).ListValues().(*array.Float32).Float32Values()
		out := make([]float64, len(vals))
		for i, v := range vals {
			out[i] = float64(v)
		}
		rec.Release()
		return out
	}

	a, b := gen(), gen()
	if len(a) != len(b) {
		t.Fatalf("corpus lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("corpus differs at element %d: %v vs %v", i, a[i], b[i])
		}
	}
}

// TestTemporalAsOfIsFixed covers R11's determinism half from the client side.
func TestTemporalAsOfIsFixed(t *testing.T) {
	s := NewReusableSearchState(8)
	a := string(s.BuildSpecialTicket("ds", "Temporal", 0, 0))
	b := string(s.BuildSpecialTicket("ds", "Temporal", 999, 0))
	if a != b {
		t.Errorf("temporal ticket varied with query index:\n  %s\n  %s", a, b)
	}
	if !strings.Contains(a, fmt.Sprintf("%d", defaultTemporalAsOf)) {
		t.Errorf("temporal ticket does not carry the fixed timestamp: %s", a)
	}
}
