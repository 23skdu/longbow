package engine

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestFastPathLessEqual_Int64 runs the test fast path less equal_int64 operation.
func TestFastPathLessEqual_Int64(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt64Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]int64{1, 5, 10, 15, 20}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathLessEqual(context.Background(), arr, "10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{true, true, true, false, false}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathGreaterEqual_Int64 runs the test fast path greater equal_int64 operation.
func TestFastPathGreaterEqual_Int64(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt64Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]int64{1, 5, 10, 15, 20}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathGreaterEqual(context.Background(), arr, "10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{false, false, true, true, true}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathLess_Int32 runs the test fast path less_int32 operation.
func TestFastPathLess_Int32(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt32Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]int32{-5, 0, 5, 10}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathLess(context.Background(), arr, "5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{true, true, false, false}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathGreater_Int16 runs the test fast path greater_int16 operation.
func TestFastPathGreater_Int16(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt16Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]int16{-100, 0, 100, 200}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathGreater(context.Background(), arr, "50")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{false, false, true, true}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathLessEqual_Int8 runs the test fast path less equal_int8 operation.
func TestFastPathLessEqual_Int8(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt8Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]int8{-10, -5, 0, 5, 10}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathLessEqual(context.Background(), arr, "0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{true, true, true, false, false}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathOrdered_NullHandling runs the test fast path ordered_null handling operation.
func TestFastPathOrdered_NullHandling(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt64Builder(alloc)
	defer builder.Release()
	builder.Append(10)
	builder.AppendNull()
	builder.Append(20)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathLess(context.Background(), arr, "15")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	if boolArr.Value(0) != true {
		t.Error("10 < 15 should be true")
	}
	if boolArr.Value(1) != false {
		t.Error("null should return false")
	}
	if boolArr.Value(2) != false {
		t.Error("20 < 15 should be false")
	}
}

// TestFastPathOrdered_InvalidValue runs the test fast path ordered_invalid value operation.
func TestFastPathOrdered_InvalidValue(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewInt64Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	_, err := FastPathLess(context.Background(), arr, "not_a_number")
	if err == nil {
		t.Error("expected error for invalid int64 value")
	}
}

// TestFastPathOrdered_UnsupportedType runs the test fast path ordered_unsupported type operation.
func TestFastPathOrdered_UnsupportedType(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.Append([]byte("test"))
	arr := builder.NewArray()
	defer arr.Release()

	_, err := FastPathLess(context.Background(), arr, "value")
	if err == nil {
		t.Error("expected error for unsupported binary type")
	}
}

// TestFastPathLess_Uint64 runs the test fast path less_uint64 operation.
func TestFastPathLess_Uint64(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewUint64Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]uint64{0, 50, 100, 150}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathLess(context.Background(), arr, "100")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{true, true, false, false}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathGreaterEqual_Uint32 runs the test fast path greater equal_uint32 operation.
func TestFastPathGreaterEqual_Uint32(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewUint32Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]uint32{10, 20, 30, 40}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathGreaterEqual(context.Background(), arr, "25")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{false, false, true, true}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathLessEqual_Uint16 runs the test fast path less equal_uint16 operation.
func TestFastPathLessEqual_Uint16(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewUint16Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]uint16{100, 200, 300}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathLessEqual(context.Background(), arr, "200")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{true, true, false}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathGreater_Uint8 runs the test fast path greater_uint8 operation.
func TestFastPathGreater_Uint8(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewUint8Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]uint8{5, 10, 15, 20}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathGreater(context.Background(), arr, "12")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{false, false, true, true}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathLess_Float64 runs the test fast path less_float64 operation.
func TestFastPathLess_Float64(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]float64{1.5, 2.5, 3.5, 4.5}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathLess(context.Background(), arr, "3.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{true, true, false, false}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathGreaterEqual_Float32 runs the test fast path greater equal_float32 operation.
func TestFastPathGreaterEqual_Float32(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]float32{0.5, 1.0, 1.5, 2.0}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	result, err := FastPathGreaterEqual(context.Background(), arr, "1.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer result.Release()

	boolArr := result.(*array.Boolean)
	expected := []bool{false, true, true, true}
	for i, exp := range expected {
		if boolArr.Value(i) != exp {
			t.Errorf("index %d: got %v, want %v", i, boolArr.Value(i), exp)
		}
	}
}

// TestFastPathOrdered_Float64_InvalidValue runs the test fast path ordered_float64_invalid value operation.
func TestFastPathOrdered_Float64_InvalidValue(t *testing.T) {
	alloc := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(alloc)
	defer builder.Release()
	builder.AppendValues([]float64{1.0, 2.0}, nil)
	arr := builder.NewArray()
	defer arr.Release()

	_, err := FastPathLess(context.Background(), arr, "not_a_float")
	if err == nil {
		t.Error("expected error for invalid float64 value")
	}
}
