package store

import (
"context"
"fmt"
"strconv"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// CompareOp represents a comparison operation type
type CompareOp int

const (
CompareOpLess CompareOp = iota
CompareOpGreater
CompareOpLessEqual
CompareOpGreaterEqual
)

// FastPathLess applies less-than filter directly without Arrow Compute overhead.
func FastPathLess(ctx context.Context, arr arrow.Array, value string) (arrow.Array, error) {
return fastPathOrdered(ctx, arr, value, CompareOpLess)
}

// FastPathGreater applies greater-than filter directly without Arrow Compute overhead.
func FastPathGreater(ctx context.Context, arr arrow.Array, value string) (arrow.Array, error) {
return fastPathOrdered(ctx, arr, value, CompareOpGreater)
}

// FastPathLessEqual applies less-than-or-equal filter directly without Arrow Compute overhead.
func FastPathLessEqual(ctx context.Context, arr arrow.Array, value string) (arrow.Array, error) {
return fastPathOrdered(ctx, arr, value, CompareOpLessEqual)
}

// FastPathGreaterEqual applies greater-than-or-equal filter directly without Arrow Compute overhead.
func FastPathGreaterEqual(ctx context.Context, arr arrow.Array, value string) (arrow.Array, error) {
return fastPathOrdered(ctx, arr, value, CompareOpGreaterEqual)
}

// fastPathOrdered handles ordered comparison operations (<, >, <=, >=)
func fastPathOrdered(_ context.Context, arr arrow.Array, value string, op CompareOp) (arrow.Array, error) {
alloc := memory.NewGoAllocator()
builder := array.NewBooleanBuilder(alloc)
defer builder.Release()

n := arr.Len()
builder.Reserve(n)

switch typedArr := arr.(type) {
case *array.Int64:
val, err := strconv.ParseInt(value, 10, 64)
if err != nil {
return nil, fmt.Errorf("invalid int64 value: %w", err)
}
fastPathInt64Ordered(typedArr, val, op, builder)

case *array.Int32:
val, err := strconv.ParseInt(value, 10, 32)
if err != nil {
return nil, fmt.Errorf("invalid int32 value: %w", err)
}
fastPathInt32Ordered(typedArr, int32(val), op, builder)

case *array.Int16:
val, err := strconv.ParseInt(value, 10, 16)
if err != nil {
return nil, fmt.Errorf("invalid int16 value: %w", err)
}
fastPathInt16Ordered(typedArr, int16(val), op, builder)

case *array.Int8:
val, err := strconv.ParseInt(value, 10, 8)
if err != nil {
return nil, fmt.Errorf("invalid int8 value: %w", err)
}
fastPathInt8Ordered(typedArr, int8(val), op, builder)

case *array.Uint64:
val, err := strconv.ParseUint(value, 10, 64)
if err != nil {
return nil, fmt.Errorf("invalid uint64 value: %w", err)
}
fastPathUint64Ordered(typedArr, val, op, builder)

case *array.Uint32:
val, err := strconv.ParseUint(value, 10, 32)
if err != nil {
return nil, fmt.Errorf("invalid uint32 value: %w", err)
}
fastPathUint32Ordered(typedArr, uint32(val), op, builder)

case *array.Uint16:
val, err := strconv.ParseUint(value, 10, 16)
if err != nil {
return nil, fmt.Errorf("invalid uint16 value: %w", err)
}
fastPathUint16Ordered(typedArr, uint16(val), op, builder)

case *array.Uint8:
val, err := strconv.ParseUint(value, 10, 8)
if err != nil {
return nil, fmt.Errorf("invalid uint8 value: %w", err)
}
fastPathUint8Ordered(typedArr, uint8(val), op, builder)

case *array.Float64:
val, err := strconv.ParseFloat(value, 64)
if err != nil {
return nil, fmt.Errorf("invalid float64 value: %w", err)
}
fastPathFloat64Ordered(typedArr, val, op, builder)

case *array.Float32:
val, err := strconv.ParseFloat(value, 32)
if err != nil {
return nil, fmt.Errorf("invalid float32 value: %w", err)
}
fastPathFloat32Ordered(typedArr, float32(val), op, builder)

default:
return nil, fmt.Errorf("unsupported type for ordered comparison: %T", arr)
}

result := builder.NewArray()
return result, nil
}

// Helper functions for ordered comparisons

func fastPathInt64Ordered(arr *array.Int64, val int64, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Int64Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrdered(values[i], val, op))
}
}
}

func fastPathInt32Ordered(arr *array.Int32, val int32, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Int32Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrdered(int64(values[i]), int64(val), op))
}
}
}

func fastPathInt16Ordered(arr *array.Int16, val int16, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Int16Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrdered(int64(values[i]), int64(val), op))
}
}
}

func fastPathInt8Ordered(arr *array.Int8, val int8, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Int8Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrdered(int64(values[i]), int64(val), op))
}
}
}

func fastPathUint64Ordered(arr *array.Uint64, val uint64, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Uint64Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrderedUint(values[i], val, op))
}
}
}

func fastPathUint32Ordered(arr *array.Uint32, val uint32, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Uint32Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrderedUint(uint64(values[i]), uint64(val), op))
}
}
}

func fastPathUint16Ordered(arr *array.Uint16, val uint16, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Uint16Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrderedUint(uint64(values[i]), uint64(val), op))
}
}
}

func fastPathUint8Ordered(arr *array.Uint8, val uint8, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Uint8Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrderedUint(uint64(values[i]), uint64(val), op))
}
}
}

func fastPathFloat64Ordered(arr *array.Float64, val float64, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Float64Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrderedFloat(values[i], val, op))
}
}
}

func fastPathFloat32Ordered(arr *array.Float32, val float32, op CompareOp, builder *array.BooleanBuilder) {
values := arr.Float32Values()
n := arr.Len()
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(compareOrderedFloat(float64(values[i]), float64(val), op))
}
}
}

// Generic comparison helpers

func compareOrdered(a, b int64, op CompareOp) bool {
switch op {
case CompareOpLess:
return a < b
case CompareOpGreater:
return a > b
case CompareOpLessEqual:
return a <= b
case CompareOpGreaterEqual:
return a >= b
default:
return false
}
}

func compareOrderedUint(a, b uint64, op CompareOp) bool {
switch op {
case CompareOpLess:
return a < b
case CompareOpGreater:
return a > b
case CompareOpLessEqual:
return a <= b
case CompareOpGreaterEqual:
return a >= b
default:
return false
}
}

func compareOrderedFloat(a, b float64, op CompareOp) bool {
switch op {
case CompareOpLess:
return a < b
case CompareOpGreater:
return a > b
case CompareOpLessEqual:
return a <= b
case CompareOpGreaterEqual:
return a >= b
default:
return false
}
}
