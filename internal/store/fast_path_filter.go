package store

import (
"context"
"fmt"
"strconv"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// IsFastPathSupported returns true if the data type and operator can use
// the fast path that bypasses Arrow Compute overhead.
// Only primitive numeric types with = or != operators are supported.
func IsFastPathSupported(dt arrow.DataType, op FilterOperator) bool {
// Only equality operators benefit from fast path
if op != FilterOpEqual && op != FilterOpNotEqual {
return false
}

// Check for primitive numeric types
switch dt.ID() {
case arrow.INT64, arrow.INT32, arrow.INT16, arrow.INT8:
return true
case arrow.UINT64, arrow.UINT32, arrow.UINT16, arrow.UINT8:
return true
case arrow.FLOAT64, arrow.FLOAT32:
return true
default:
return false
}
}

// FastPathEqual applies equality filter directly without Arrow Compute overhead.
// Returns a boolean mask array where true indicates matching values.
func FastPathEqual(ctx context.Context, arr arrow.Array, value string) (arrow.Array, error) {
return fastPathCompare(ctx, arr, value, true)
}

// FastPathNotEqual applies not-equal filter directly without Arrow Compute overhead.
// Returns a boolean mask array where true indicates non-matching values.
func FastPathNotEqual(ctx context.Context, arr arrow.Array, value string) (arrow.Array, error) {
return fastPathCompare(ctx, arr, value, false)
}

// fastPathCompare is the core comparison implementation.
// When equal=true, returns mask where arr[i] == value.
// When equal=false, returns mask where arr[i] != value.
func fastPathCompare(_ context.Context, arr arrow.Array, value string, equal bool) (arrow.Array, error) {
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
fastPathInt64(typedArr, val, equal, builder)

case *array.Int32:
val, err := strconv.ParseInt(value, 10, 32)
if err != nil {
return nil, fmt.Errorf("invalid int32 value: %w", err)
}
fastPathInt32(typedArr, int32(val), equal, builder)

case *array.Uint64:
val, err := strconv.ParseUint(value, 10, 64)
if err != nil {
return nil, fmt.Errorf("invalid uint64 value: %w", err)
}
fastPathUint64(typedArr, val, equal, builder)

case *array.Uint32:
val, err := strconv.ParseUint(value, 10, 32)
if err != nil {
return nil, fmt.Errorf("invalid uint32 value: %w", err)
}
fastPathUint32(typedArr, uint32(val), equal, builder)

case *array.Float64:
val, err := strconv.ParseFloat(value, 64)
if err != nil {
return nil, fmt.Errorf("invalid float64 value: %w", err)
}
fastPathFloat64(typedArr, val, equal, builder)

case *array.Float32:
val, err := strconv.ParseFloat(value, 32)
if err != nil {
return nil, fmt.Errorf("invalid float32 value: %w", err)
}
fastPathFloat32(typedArr, float32(val), equal, builder)

default:
return nil, fmt.Errorf("fast path not supported for type: %v", arr.DataType())
}

return builder.NewArray(), nil
}

// fastPathInt64 compares int64 array with scalar value.
// Uses direct memory access for maximum throughput.
func fastPathInt64(arr *array.Int64, val int64, equal bool, builder *array.BooleanBuilder) {
values := arr.Int64Values()
n := arr.Len()

if equal {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] == val)
}
}
} else {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] != val)
}
}
}
}

// fastPathInt32 compares int32 array with scalar value.
func fastPathInt32(arr *array.Int32, val int32, equal bool, builder *array.BooleanBuilder) {
values := arr.Int32Values()
n := arr.Len()

if equal {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] == val)
}
}
} else {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] != val)
}
}
}
}

// fastPathUint64 compares uint64 array with scalar value.
func fastPathUint64(arr *array.Uint64, val uint64, equal bool, builder *array.BooleanBuilder) {
values := arr.Uint64Values()
n := arr.Len()

if equal {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] == val)
}
}
} else {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] != val)
}
}
}
}

// fastPathUint32 compares uint32 array with scalar value.
func fastPathUint32(arr *array.Uint32, val uint32, equal bool, builder *array.BooleanBuilder) {
values := arr.Uint32Values()
n := arr.Len()

if equal {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] == val)
}
}
} else {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] != val)
}
}
}
}

// fastPathFloat64 compares float64 array with scalar value.
func fastPathFloat64(arr *array.Float64, val float64, equal bool, builder *array.BooleanBuilder) {
values := arr.Float64Values()
n := arr.Len()

if equal {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] == val)
}
}
} else {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] != val)
}
}
}
}

// fastPathFloat32 compares float32 array with scalar value.
func fastPathFloat32(arr *array.Float32, val float32, equal bool, builder *array.BooleanBuilder) {
values := arr.Float32Values()
n := arr.Len()

if equal {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] == val)
}
}
} else {
for i := 0; i < n; i++ {
if arr.IsNull(i) {
builder.Append(false)
} else {
builder.Append(values[i] != val)
}
}
}
}
