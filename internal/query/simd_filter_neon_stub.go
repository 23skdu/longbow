//go:build (!arm64) || noasm

package query

import "unsafe"

func fastPathInt64EqualNEONKernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*int64)(src), n)
	resultSlice := unsafe.Slice((*int64)(result), n)
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = -1
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathInt32EqualNEONKernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*int32)(src), n)
	resultSlice := unsafe.Slice((*int32)(result), n)
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = -1
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathFloat64EqualNEONKernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*float64)(src), n)
	resultSlice := unsafe.Slice((*float64)(result), n)
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = 1
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathFloat32EqualNEONKernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*float32)(src), n)
	resultSlice := unsafe.Slice((*float32)(result), n)
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = 1
		} else {
			resultSlice[i] = 0
		}
	}
}