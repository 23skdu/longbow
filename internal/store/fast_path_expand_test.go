package store

import (
"context"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestFastPathLessInt64(t *testing.T) {
alloc := memory.NewGoAllocator()
builder := array.NewInt64Builder(alloc)
defer builder.Release()
builder.AppendValues([]int64{10, 20, 30, 40}, nil)
arr := builder.NewArray()
defer arr.Release()

result, err := FastPathLess(context.Background(), arr, "25")
if err != nil {
t.Fatal(err)
}
defer result.Release()

boolArr := result.(*array.Boolean)
if !boolArr.Value(0) || !boolArr.Value(1) || boolArr.Value(2) || boolArr.Value(3) {
t.Error("wrong result")
}
}

func TestFastPathGreaterInt64(t *testing.T) {
alloc := memory.NewGoAllocator()
builder := array.NewInt64Builder(alloc)
defer builder.Release()
builder.AppendValues([]int64{10, 20, 30, 40}, nil)
arr := builder.NewArray()
defer arr.Release()

result, err := FastPathGreater(context.Background(), arr, "25")
if err != nil {
t.Fatal(err)
}
defer result.Release()

boolArr := result.(*array.Boolean)
if boolArr.Value(0) || boolArr.Value(1) || !boolArr.Value(2) || !boolArr.Value(3) {
t.Error("wrong result")
}
}

func TestFastPathInt8Equal(t *testing.T) {
alloc := memory.NewGoAllocator()
builder := array.NewInt8Builder(alloc)
defer builder.Release()
builder.AppendValues([]int8{1, 2, 3}, nil)
arr := builder.NewArray()
defer arr.Release()

result, err := FastPathEqual(context.Background(), arr, "2")
if err != nil {
t.Fatal(err)
}
defer result.Release()

boolArr := result.(*array.Boolean)
if boolArr.Value(0) || !boolArr.Value(1) || boolArr.Value(2) {
t.Error("wrong result")
}
}

func TestFastPathBoolEqual(t *testing.T) {
alloc := memory.NewGoAllocator()
builder := array.NewBooleanBuilder(alloc)
defer builder.Release()
builder.AppendValues([]bool{true, false, true}, nil)
arr := builder.NewArray()
defer arr.Release()

result, err := FastPathEqual(context.Background(), arr, "true")
if err != nil {
t.Fatal(err)
}
defer result.Release()

boolArr := result.(*array.Boolean)
if !boolArr.Value(0) || boolArr.Value(1) || !boolArr.Value(2) {
t.Error("wrong result")
}
}

func TestFastPathStringEqual(t *testing.T) {
alloc := memory.NewGoAllocator()
builder := array.NewStringBuilder(alloc)
defer builder.Release()
builder.AppendValues([]string{"a", "b", "c"}, nil)
arr := builder.NewArray()
defer arr.Release()

result, err := FastPathEqual(context.Background(), arr, "b")
if err != nil {
t.Fatal(err)
}
defer result.Release()

boolArr := result.(*array.Boolean)
if boolArr.Value(0) || !boolArr.Value(1) || boolArr.Value(2) {
t.Error("wrong result")
}
}

func TestIsFastPathExpandedTypes(t *testing.T) {
tests := []struct {
dt arrow.DataType
op FilterOperator
want bool
}{
{arrow.PrimitiveTypes.Int64, FilterOpLess, true},
{arrow.PrimitiveTypes.Int64, FilterOpGreater, true},
{arrow.PrimitiveTypes.Int8, FilterOpEqual, true},
{arrow.PrimitiveTypes.Int16, FilterOpEqual, true},
{arrow.PrimitiveTypes.Uint8, FilterOpEqual, true},
{arrow.PrimitiveTypes.Uint16, FilterOpEqual, true},
{arrow.FixedWidthTypes.Boolean, FilterOpEqual, true},
{arrow.BinaryTypes.String, FilterOpEqual, true},
}
for _, tc := range tests {
if got := IsFastPathSupported(tc.dt, tc.op); got != tc.want {
t.Errorf("IsFastPathSupported(%v, %v) = %v, want %v", tc.dt, tc.op, got, tc.want)
}
}
}
