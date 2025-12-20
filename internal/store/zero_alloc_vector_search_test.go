package store

import (
	"encoding/json"
	"testing"
)

func TestZeroAllocVectorSearchParser_BasicParse(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128) // pre-alloc 128 dims

	input := []byte(`{"dataset":"test-vectors","vector":[1.5,2.5,3.5],"k":10}`)

	req, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if req.Dataset != "test-vectors" {
		t.Errorf("Dataset = %q, want %q", req.Dataset, "test-vectors")
	}
	if req.K != 10 {
		t.Errorf("K = %d, want %d", req.K, 10)
	}
	if len(req.Vector) != 3 {
		t.Fatalf("Vector len = %d, want 3", len(req.Vector))
	}
	if req.Vector[0] != 1.5 || req.Vector[1] != 2.5 || req.Vector[2] != 3.5 {
		t.Errorf("Vector = %v, want [1.5, 2.5, 3.5]", req.Vector)
	}
}

func TestZeroAllocVectorSearchParser_FieldOrder(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128)

	// Different field order
	input := []byte(`{"k":5,"vector":[0.1,0.2],"dataset":"my-dataset"}`)

	req, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if req.Dataset != "my-dataset" {
		t.Errorf("Dataset = %q, want %q", req.Dataset, "my-dataset")
	}
	if req.K != 5 {
		t.Errorf("K = %d, want %d", req.K, 5)
	}
	if len(req.Vector) != 2 {
		t.Fatalf("Vector len = %d, want 2", len(req.Vector))
	}
}

func TestZeroAllocVectorSearchParser_NegativeFloats(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128)

	input := []byte(`{"dataset":"test","vector":[-1.5,2.5,-3.5],"k":10}`)

	req, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if req.Vector[0] != -1.5 {
		t.Errorf("Vector[0] = %v, want -1.5", req.Vector[0])
	}
	if req.Vector[2] != -3.5 {
		t.Errorf("Vector[2] = %v, want -3.5", req.Vector[2])
	}
}

func TestZeroAllocVectorSearchParser_ScientificNotation(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128)

	input := []byte(`{"dataset":"test","vector":[1e-5,2.5e10,-3.5E-2],"k":10}`)

	req, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Approximate comparison for floats
	if req.Vector[0] < 9e-6 || req.Vector[0] > 1.1e-5 {
		t.Errorf("Vector[0] = %v, want ~1e-5", req.Vector[0])
	}
}

func TestZeroAllocVectorSearchParser_LargeVector(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(1024) // pre-alloc 1024 dims

	// Generate 768-dim vector (common for embeddings)
	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i) * 0.001
	}

	req := VectorSearchRequest{
		Dataset: "embeddings",
		Vector:  vec,
		K:       100,
	}

	input, _ := json.Marshal(req)

	parsed, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(parsed.Vector) != 768 {
		t.Errorf("Vector len = %d, want 768", len(parsed.Vector))
	}
}

func TestZeroAllocVectorSearchParser_Whitespace(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128)

	input := []byte(`{
"dataset": "test",
"vector": [ 1.0 , 2.0 , 3.0 ],
"k": 10
}`)

	req, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(req.Vector) != 3 {
		t.Errorf("Vector len = %d, want 3", len(req.Vector))
	}
}

func TestZeroAllocVectorSearchParser_UnicodeEscape(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128)

	// Dataset with unicode escape (Go's json.Marshal escapes < > &)
	input := []byte(`{"dataset":"test\u003evalue","vector":[1.0],"k":10}`)

	req, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if req.Dataset != "test>value" {
		t.Errorf("Dataset = %q, want %q", req.Dataset, "test>value")
	}
}

func TestZeroAllocVectorSearchParser_EmptyVector(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128)

	input := []byte(`{"dataset":"test","vector":[],"k":10}`)

	req, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if len(req.Vector) != 0 {
		t.Errorf("Vector len = %d, want 0", len(req.Vector))
	}
}

func TestZeroAllocVectorSearchParser_MalformedJSON(t *testing.T) {
	parser := NewZeroAllocVectorSearchParser(128)

	testCases := []struct {
		name  string
		input []byte
	}{
		{"missing brace", []byte(`{"dataset":"test"`)},
		{"invalid vector", []byte(`{"dataset":"test","vector":"notarray","k":10}`)},
		{"truncated", []byte(`{"dataset":`)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parser.Parse(tc.input)
			if err == nil {
				t.Error("Expected error for malformed JSON")
			}
		})
	}
}

// Benchmark: verify zero allocations
func BenchmarkZeroAllocVectorSearchParser(b *testing.B) {
	parser := NewZeroAllocVectorSearchParser(128)
	input := []byte(`{"dataset":"test-vectors","vector":[1.5,2.5,3.5,4.5,5.5],"k":10}`)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := parser.Parse(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: compare with standard json.Unmarshal
func BenchmarkStdJSONVectorSearch(b *testing.B) {
	input := []byte(`{"dataset":"test-vectors","vector":[1.5,2.5,3.5,4.5,5.5],"k":10}`)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var req VectorSearchRequest
		err := json.Unmarshal(input, &req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: large vector (768 dims - common embedding size)
func BenchmarkZeroAllocVectorSearchParser_768Dims(b *testing.B) {
	parser := NewZeroAllocVectorSearchParser(1024)

	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i) * 0.001
	}
	req := VectorSearchRequest{Dataset: "embeddings", Vector: vec, K: 100}
	input, _ := json.Marshal(req)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := parser.Parse(input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStdJSON_768Dims(b *testing.B) {
	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i) * 0.001
	}
	req := VectorSearchRequest{Dataset: "embeddings", Vector: vec, K: 100}
	input, _ := json.Marshal(req)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var r VectorSearchRequest
		err := json.Unmarshal(input, &r)
		if err != nil {
			b.Fatal(err)
		}
	}
}
