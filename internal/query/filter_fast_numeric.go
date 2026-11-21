package query

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
)

func fastPathInt64(arr *array.Int64, val int64, equal bool, builder *array.BooleanBuilder) {
	values := arr.Int64Values()
	n := arr.Len()

	var useAVX2 bool
	var useAVX512 bool
	var useNEON bool
	if equal && n >= 2 {
		feats := simd.GetCPUFeatures()
		useAVX512 = feats.HasAVX512
		useAVX2 = feats.HasAVX2 && !useAVX512
		useNEON = feats.HasNEON
	}

	if useNEON {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("neon").Inc()
		results := make([]int64, n)
		fastPathInt64EqualNEONKernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX512 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx512").Inc()
		results := make([]int64, n)
		fastPathInt64EqualAVX512Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX2 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx2").Inc()
		results := make([]int64, n)
		fastPathInt64EqualAVX2Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

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

	var useAVX2 bool
	var useAVX512 bool
	var useNEON bool
	if equal && n >= 4 {
		feats := simd.GetCPUFeatures()
		useAVX512 = feats.HasAVX512
		useAVX2 = feats.HasAVX2 && !useAVX512
		useNEON = feats.HasNEON
	}

	if useNEON {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("neon").Inc()
		results := make([]int32, n)
		fastPathInt32EqualNEONKernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX512 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx512").Inc()
		results := make([]int32, n)
		fastPathInt32EqualAVX512Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX2 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx2").Inc()
		// SIMD Fast Path
		results := make([]int32, n)

		fastPathInt32EqualAVX2Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

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

	var useAVX2 bool
	var useAVX512 bool
	var useNEON bool
	if equal && n >= 2 {
		feats := simd.GetCPUFeatures()
		useAVX512 = feats.HasAVX512
		useAVX2 = feats.HasAVX2 && !useAVX512
		useNEON = feats.HasNEON
	}

	if useNEON {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("neon").Inc()
		results := make([]float64, n)
		fastPathFloat64EqualNEONKernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX512 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx512").Inc()
		results := make([]float64, n)
		fastPathFloat64EqualAVX512Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX2 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx2").Inc()
		results := make([]float64, n)
		fastPathFloat64EqualAVX2Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

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

	var useAVX2 bool
	var useAVX512 bool
	var useNEON bool
	if equal && n >= 4 {
		feats := simd.GetCPUFeatures()
		useAVX512 = feats.HasAVX512
		useAVX2 = feats.HasAVX2 && !useAVX512
		useNEON = feats.HasNEON
	}

	if useNEON {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("neon").Inc()
		results := make([]float32, n)
		fastPathFloat32EqualNEONKernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX512 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx512").Inc()
		results := make([]float32, n)
		fastPathFloat32EqualAVX512Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(results[i] != 0)
			}
		}
		return
	}

	if useAVX2 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx2").Inc()

		// SIMD Fast Path
		results := make([]float32, n)

		fastPathFloat32EqualAVX2Kernel(unsafe.Pointer(&values[0]), n, val, unsafe.Pointer(&results[0])) // #nosec G103
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				// VCMPPS returns all 1s in the lane if true
				builder.Append(results[i] != 0)
			}
		}
		return
	}

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

// fastPathInt16 compares int16 array with scalar value.
func fastPathInt16(arr *array.Int16, val int16, equal bool, builder *array.BooleanBuilder) {
	values := arr.Int16Values()
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

// fastPathInt8 compares int8 array with scalar value.
func fastPathInt8(arr *array.Int8, val int8, equal bool, builder *array.BooleanBuilder) {
	values := arr.Int8Values()
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

// fastPathUint16 compares uint16 array with scalar value.
func fastPathUint16(arr *array.Uint16, val uint16, equal bool, builder *array.BooleanBuilder) {
	values := arr.Uint16Values()
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

// fastPathUint8 compares uint8 array with scalar value.
func fastPathUint8(arr *array.Uint8, val uint8, equal bool, builder *array.BooleanBuilder) {
	values := arr.Uint8Values()
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

// fastPathBool compares boolean array with scalar value.
