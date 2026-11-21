package query

import (
	"github.com/apache/arrow-go/v18/arrow/array"
)

func fastPathInt64Ordered(arr *array.Int64, val int64, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Int64Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathInt32Ordered(arr *array.Int32, val int32, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Int32Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathUint64Ordered(arr *array.Uint64, val uint64, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Uint64Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathUint32Ordered(arr *array.Uint32, val uint32, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Uint32Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathFloat64Ordered(arr *array.Float64, val float64, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Float64Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathFloat32Ordered(arr *array.Float32, val float32, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Float32Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathInt16Ordered(arr *array.Int16, val int16, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Int16Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathInt8Ordered(arr *array.Int8, val int8, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Int8Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathUint16Ordered(arr *array.Uint16, val uint16, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Uint16Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}

func fastPathUint8Ordered(arr *array.Uint8, val uint8, op FilterOperator, builder *array.BooleanBuilder) {
	values := arr.Uint8Values()
	n := arr.Len()
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			builder.Append(false)
			continue
		}
		v := values[i]
		switch op {
		case FilterOpLess:
			builder.Append(v < val)
		case FilterOpGreater:
			builder.Append(v > val)
		case FilterOpLessEqual:
			builder.Append(v <= val)
		case FilterOpGreaterEqual:
			builder.Append(v >= val)
		default:
			builder.Append(false)
		}
	}
}
