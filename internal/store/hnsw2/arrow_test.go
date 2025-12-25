package hnsw2

import (
	"testing"
	
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestExtractVectorFromArrow(t *testing.T) {
	// Create a simple Arrow record with vector column
	pool := memory.NewGoAllocator()
	
	// Define schema: vector column as FixedSizeList of float32
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "vector", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	// Build record with 3 vectors
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	
	idBuilder := builder.Field(0).(*array.Uint32Builder)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	
	// Vector 0: [1.0, 2.0, 3.0]
	idBuilder.Append(0)
	vecBuilder.Append(true)
	floatBuilder.AppendValues([]float32{1.0, 2.0, 3.0}, nil)
	
	// Vector 1: [4.0, 5.0, 6.0]
	idBuilder.Append(1)
	vecBuilder.Append(true)
	floatBuilder.AppendValues([]float32{4.0, 5.0, 6.0}, nil)
	
	// Vector 2: [7.0, 8.0, 9.0]
	idBuilder.Append(2)
	vecBuilder.Append(true)
	floatBuilder.AppendValues([]float32{7.0, 8.0, 9.0}, nil)
	
	rec := builder.NewRecordBatch()
	defer rec.Release()
	
	// Test extracting each vector
	vec0, err := extractVectorFromArrow(rec, 0, 1) // Column 1 is vector
	if err != nil {
		t.Fatalf("extractVector(0) failed: %v", err)
	}
	if len(vec0) != 3 || vec0[0] != 1.0 || vec0[1] != 2.0 || vec0[2] != 3.0 {
		t.Errorf("vec0 = %v, want [1 2 3]", vec0)
	}
	
	vec1, err := extractVectorFromArrow(rec, 1, 1)
	if err != nil {
		t.Fatalf("extractVector(1) failed: %v", err)
	}
	if len(vec1) != 3 || vec1[0] != 4.0 || vec1[1] != 5.0 || vec1[2] != 6.0 {
		t.Errorf("vec1 = %v, want [4 5 6]", vec1)
	}
	
	vec2, err := extractVectorFromArrow(rec, 2, 1)
	if err != nil {
		t.Fatalf("extractVector(2) failed: %v", err)
	}
	if len(vec2) != 3 || vec2[0] != 7.0 || vec2[1] != 8.0 || vec2[2] != 9.0 {
		t.Errorf("vec2 = %v, want [7 8 9]", vec2)
	}
}

func TestExtractVectorCopy(t *testing.T) {
	pool := memory.NewGoAllocator()
	
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	
	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	
	vecBuilder.Append(true)
	floatBuilder.AppendValues([]float32{1.0, 2.0}, nil)
	
	rec := builder.NewRecordBatch()
	defer rec.Release()
	
	// Get copy
	vec, err := extractVectorCopy(rec, 0, 0) // Column 0 is vector
	if err != nil {
		t.Fatalf("extractVectorCopy failed: %v", err)
	}
	
	// Modify copy - should not affect original
	vec[0] = 999.0
	
	// Get original again
	original, err := extractVectorFromArrow(rec, 0, 0)
	if err != nil {
		t.Fatalf("extractVectorFromArrow failed: %v", err)
	}
	
	if original[0] != 1.0 {
		t.Errorf("original was modified! got %f, want 1.0", original[0])
	}
}

func BenchmarkExtractVectorZeroCopy(b *testing.B) {
	pool := memory.NewGoAllocator()
	
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(384, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	
	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	
	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		vecBuilder.Append(true)
		vec := make([]float32, 384)
		for j := range vec {
			vec[j] = float32(i*384 + j)
		}
		floatBuilder.AppendValues(vec, nil)
	}
	
	rec := builder.NewRecordBatch()
	defer rec.Release()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = extractVectorFromArrow(rec, i%1000, 0)
	}
}

func BenchmarkExtractVectorCopy(b *testing.B) {
	pool := memory.NewGoAllocator()
	
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "vector", Type: arrow.FixedSizeListOf(384, arrow.PrimitiveTypes.Float32)},
		},
		nil,
	)
	
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()
	
	vecBuilder := builder.Field(0).(*array.FixedSizeListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	
	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		vecBuilder.Append(true)
		vec := make([]float32, 384)
		for j := range vec {
			vec[j] = float32(i*384 + j)
		}
		floatBuilder.AppendValues(vec, nil)
	}
	
	rec := builder.NewRecordBatch()
	defer rec.Release()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = extractVectorCopy(rec, i%1000, 0)
	}
}
