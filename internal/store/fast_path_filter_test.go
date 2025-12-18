
package store

import (
"context"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestFastPathFilter_IsSupportedType tests type detection for fast path eligibility
func TestFastPathFilter_IsSupportedType(t *testing.T) {
tests := []struct {
name     string
dataType arrow.DataType
want     bool
}{
{"Int64", arrow.PrimitiveTypes.Int64, true},
{"Int32", arrow.PrimitiveTypes.Int32, true},
{"Float64", arrow.PrimitiveTypes.Float64, true},
{"Float32", arrow.PrimitiveTypes.Float32, true},
{"Uint64", arrow.PrimitiveTypes.Uint64, true},
{"Uint32", arrow.PrimitiveTypes.Uint32, true},
{"String", arrow.BinaryTypes.String, true},  // Strings need Arrow Compute
{"Binary", arrow.BinaryTypes.Binary, false},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
got := IsFastPathSupported(tt.dataType, FilterOpEqual)
if got != tt.want {
t.Errorf("IsFastPathSupported(%v) = %v, want %v", tt.dataType, got, tt.want)
}
})
}
}

// TestFastPathFilter_SupportedOperators tests operator eligibility
func TestFastPathFilter_SupportedOperators(t *testing.T) {
tests := []struct {
name string
op   FilterOperator
want bool
}{
{"Equal", FilterOpEqual, true},
{"NotEqual", FilterOpNotEqual, true},
{"Greater", FilterOpGreater, true},      // Only = and != for fast path
{"Less", FilterOpLess, true},
{"GreaterEqual", FilterOpGreaterEqual, true},
{"LessEqual", FilterOpLessEqual, true},
{"In", FilterOpIn, false},
{"NotIn", FilterOpNotIn, false},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
got := IsFastPathSupported(arrow.PrimitiveTypes.Int64, tt.op)
if got != tt.want {
t.Errorf("IsFastPathSupported(Int64, %v) = %v, want %v", tt.op, got, tt.want)
}
})
}
}

// TestFastPathFilter_Int64Equal tests equality filter on int64 column
func TestFastPathFilter_Int64Equal(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

// Build int64 array: [10, 20, 30, 20, 40, 20]
builder := array.NewInt64Builder(alloc)
defer builder.Release()
builder.AppendValues([]int64{10, 20, 30, 20, 40, 20}, nil)
arr := builder.NewArray()
defer arr.Release()

// Filter: value == 20
mask, err := FastPathEqual(ctx, arr, "20")
if err != nil {
t.Fatalf("FastPathEqual failed: %v", err)
}
defer mask.Release()

// Expected: [false, true, false, true, false, true]
boolArr := mask.(*array.Boolean)
expected := []bool{false, true, false, true, false, true}
for i, want := range expected {
if boolArr.Value(i) != want {
t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), want)
}
}
}

// TestFastPathFilter_Int64NotEqual tests not-equal filter on int64 column
func TestFastPathFilter_Int64NotEqual(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewInt64Builder(alloc)
defer builder.Release()
builder.AppendValues([]int64{10, 20, 30, 20, 40}, nil)
arr := builder.NewArray()
defer arr.Release()

// Filter: value != 20
mask, err := FastPathNotEqual(ctx, arr, "20")
if err != nil {
t.Fatalf("FastPathNotEqual failed: %v", err)
}
defer mask.Release()

// Expected: [true, false, true, false, true]
boolArr := mask.(*array.Boolean)
expected := []bool{true, false, true, false, true}
for i, want := range expected {
if boolArr.Value(i) != want {
t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), want)
}
}
}

// TestFastPathFilter_Float64Equal tests equality filter on float64 column
func TestFastPathFilter_Float64Equal(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewFloat64Builder(alloc)
defer builder.Release()
builder.AppendValues([]float64{1.5, 2.5, 3.5, 2.5, 4.5}, nil)
arr := builder.NewArray()
defer arr.Release()

mask, err := FastPathEqual(ctx, arr, "2.5")
if err != nil {
t.Fatalf("FastPathEqual failed: %v", err)
}
defer mask.Release()

boolArr := mask.(*array.Boolean)
expected := []bool{false, true, false, true, false}
for i, want := range expected {
if boolArr.Value(i) != want {
t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), want)
}
}
}

// TestFastPathFilter_Float32Equal tests equality filter on float32 column
func TestFastPathFilter_Float32Equal(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewFloat32Builder(alloc)
defer builder.Release()
builder.AppendValues([]float32{1.0, 2.0, 3.0, 2.0}, nil)
arr := builder.NewArray()
defer arr.Release()

mask, err := FastPathEqual(ctx, arr, "2.0")
if err != nil {
t.Fatalf("FastPathEqual failed: %v", err)
}
defer mask.Release()

boolArr := mask.(*array.Boolean)
expected := []bool{false, true, false, true}
for i, want := range expected {
if boolArr.Value(i) != want {
t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), want)
}
}
}

// TestFastPathFilter_Int32Equal tests equality filter on int32 column
func TestFastPathFilter_Int32Equal(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewInt32Builder(alloc)
defer builder.Release()
builder.AppendValues([]int32{100, 200, 300, 200, 400}, nil)
arr := builder.NewArray()
defer arr.Release()

mask, err := FastPathEqual(ctx, arr, "200")
if err != nil {
t.Fatalf("FastPathEqual failed: %v", err)
}
defer mask.Release()

boolArr := mask.(*array.Boolean)
expected := []bool{false, true, false, true, false}
for i, want := range expected {
if boolArr.Value(i) != want {
t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), want)
}
}
}

// TestFastPathFilter_WithNulls tests handling of null values
func TestFastPathFilter_WithNulls(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewInt64Builder(alloc)
defer builder.Release()
builder.Append(10)
builder.AppendNull()
builder.Append(20)
builder.AppendNull()
builder.Append(20)
arr := builder.NewArray()
defer arr.Release()

mask, err := FastPathEqual(ctx, arr, "20")
if err != nil {
t.Fatalf("FastPathEqual failed: %v", err)
}
defer mask.Release()

// Nulls should be false in mask
boolArr := mask.(*array.Boolean)
expected := []bool{false, false, true, false, true}
for i, want := range expected {
if boolArr.Value(i) != want {
t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), want)
}
}
}

// TestFastPathFilter_LargeArray tests performance with larger dataset
func TestFastPathFilter_LargeArray(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

// Build array with 10000 elements
builder := array.NewInt64Builder(alloc)
defer builder.Release()

data := make([]int64, 10000)
expectedMatches := 0
for i := range data {
data[i] = int64(i % 100)
if data[i] == 42 {
expectedMatches++
}
}
builder.AppendValues(data, nil)
arr := builder.NewArray()
defer arr.Release()

mask, err := FastPathEqual(ctx, arr, "42")
if err != nil {
t.Fatalf("FastPathEqual failed: %v", err)
}
defer mask.Release()

// Count matches
boolArr := mask.(*array.Boolean)
matches := 0
for i := 0; i < boolArr.Len(); i++ {
if boolArr.Value(i) {
matches++
}
}
if matches != expectedMatches {
t.Errorf("got %d matches, want %d", matches, expectedMatches)
}
}

// TestFastPathFilter_Uint64Equal tests equality filter on uint64 column
func TestFastPathFilter_Uint64Equal(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewUint64Builder(alloc)
defer builder.Release()
builder.AppendValues([]uint64{10, 20, 30, 20, 40}, nil)
arr := builder.NewArray()
defer arr.Release()

mask, err := FastPathEqual(ctx, arr, "20")
if err != nil {
t.Fatalf("FastPathEqual failed: %v", err)
}
defer mask.Release()

boolArr := mask.(*array.Boolean)
expected := []bool{false, true, false, true, false}
for i, want := range expected {
if boolArr.Value(i) != want {
t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), want)
}
}
}

// TestFastPathFilter_Integration tests integration with VectorizedFilter
func TestFastPathFilter_Integration(t *testing.T) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

// Create a record batch with int64 column
schema := arrow.NewSchema(
[]arrow.Field{
{Name: "id", Type: arrow.PrimitiveTypes.Int64},
{Name: "value", Type: arrow.PrimitiveTypes.Float64},
},
nil,
)

idBuilder := array.NewInt64Builder(alloc)
defer idBuilder.Release()
idBuilder.AppendValues([]int64{1, 2, 3, 2, 4, 2}, nil)

valBuilder := array.NewFloat64Builder(alloc)
defer valBuilder.Release()
valBuilder.AppendValues([]float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6}, nil)

idArr := idBuilder.NewArray()
defer idArr.Release()
valArr := valBuilder.NewArray()
defer valArr.Release()

rec := array.NewRecordBatch(schema, []arrow.Array{idArr, valArr}, 6)
defer rec.Release()

vf := NewVectorizedFilter(alloc)
filters := []Filter{{Field: "id", Operator: "=", Value: "2"}}

result, err := vf.Apply(ctx, rec, filters)
if err != nil {
t.Fatalf("Apply failed: %v", err)
}
defer result.Release()

// Should have 3 rows where id=2
if result.NumRows() != 3 {
t.Errorf("got %d rows, want 3", result.NumRows())
}
}

// Benchmarks
func BenchmarkFastPath_Int64Equal(b *testing.B) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewInt64Builder(alloc)
data := make([]int64, 100000)
for i := range data {
data[i] = int64(i % 1000)
}
builder.AppendValues(data, nil)
arr := builder.NewArray()
defer arr.Release()
builder.Release()

b.ResetTimer()
for i := 0; i < b.N; i++ {
mask, _ := FastPathEqual(ctx, arr, "500")
mask.Release()
}
}

func BenchmarkArrowCompute_Int64Equal(b *testing.B) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewInt64Builder(alloc)
data := make([]int64, 100000)
for i := range data {
data[i] = int64(i % 1000)
}
builder.AppendValues(data, nil)
arr := builder.NewArray()
defer arr.Release()
builder.Release()

vf := NewVectorizedFilter(alloc)

b.ResetTimer()
for i := 0; i < b.N; i++ {
// Force Arrow Compute path
mask, _ := vf.applyComparisonFilter(ctx, arr, FilterOpEqual, "500")
mask.Release()
}
}

func BenchmarkFastPath_Float64Equal(b *testing.B) {
alloc := memory.NewGoAllocator()
ctx := context.Background()

builder := array.NewFloat64Builder(alloc)
data := make([]float64, 100000)
for i := range data {
data[i] = float64(i % 1000)
}
builder.AppendValues(data, nil)
arr := builder.NewArray()
defer arr.Release()
builder.Release()

b.ResetTimer()
for i := 0; i < b.N; i++ {
mask, _ := FastPathEqual(ctx, arr, "500.0")
mask.Release()
}
}
