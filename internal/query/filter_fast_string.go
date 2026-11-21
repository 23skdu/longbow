package query

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/simd"
)

func fastPathBool(arr *array.Boolean, val, equal bool, builder *array.BooleanBuilder) {
	n := arr.Len()
	negate := val != equal

	var useAVX2 bool
	if n >= 64 {
		feats := simd.GetCPUFeatures()
		useAVX2 = feats.HasAVX2
	}

	if useAVX2 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx2").Inc()

		// SIMD Fast Path for bit-packed data
		data := arr.Data().Buffers()[1].Bytes()

		nBytes := (n + 7) / 8
		results := make([]byte, nBytes)
		fastPathBoolAVX2Kernel(unsafe.Pointer(&data[0]), nBytes, negate, unsafe.Pointer(&results[0])) // #nosec G103

		// Fill the builder from the transformed bitmap
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				byteIdx := i / 8
				bitIdx := uint(i % 8)
				val := (results[byteIdx] & (1 << bitIdx)) != 0
				builder.Append(val)
			}
		}
		return
	}

	if equal {
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(arr.Value(i) == val)
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(arr.Value(i) != val)
			}
		}
	}
}

// fastPathString compares string array with scalar value.
func fastPathString(arr *array.String, val string, equal bool, builder *array.BooleanBuilder) {
	n := arr.Len()

	var useAVX2 bool
	if equal && n >= 8 {
		feats := simd.GetCPUFeatures()
		useAVX2 = feats.HasAVX2
	}

	if useAVX2 {
		metrics.HNSWFilterVectorizedOpsTotal.WithLabelValues("avx2").Inc()
		results := make([]int32, n)

		offsets := arr.ValueOffsets()
		data := arr.ValueBytes()

		var targetPtr unsafe.Pointer
		if len(val) > 0 {
			targetPtr = unsafe.Pointer(unsafe.StringData(val)) // #nosec G103
		}

		fastPathStringEqualAVX2Kernel(
			unsafe.Pointer(&offsets[0]), // #nosec G103
			unsafe.Pointer(&data[0]),    // #nosec G103
			n,
			targetPtr,
			len(val),
			unsafe.Pointer(&results[0]), // #nosec G103
		)

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
				builder.Append(arr.Value(i) == val)
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if arr.IsNull(i) {
				builder.Append(false)
			} else {
				builder.Append(arr.Value(i) != val)
			}
		}
	}
}
