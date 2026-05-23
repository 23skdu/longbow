//go:build !amd64

package query

import "unsafe"

func fastPathInt32EqualAVX2Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*int32)(src), n)       // #nosec G103
	resultSlice := unsafe.Slice((*int32)(result), n) // #nosec G103
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = -1
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathFloat32EqualAVX2Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*float32)(src), n)     // #nosec G103
	resultSlice := unsafe.Slice((*int32)(result), n) // #nosec G103
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = 1
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathBoolAVX2Kernel(src unsafe.Pointer, nBytes int, negate bool, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*uint8)(src), nBytes)       // #nosec G103
	resultSlice := unsafe.Slice((*uint8)(result), nBytes) // #nosec G103
	copy(resultSlice, srcSlice)
	if negate {
		for i := 0; i < nBytes; i++ {
			resultSlice[i] = ^resultSlice[i]
		}
	}
}

func fastPathStringEqualAVX2Kernel(offsets unsafe.Pointer, data unsafe.Pointer, n int, target unsafe.Pointer, targetLen int, result unsafe.Pointer) {
	offsetsSlice := unsafe.Slice((*int32)(offsets), n+1)      // #nosec G103
	dataSlice := unsafe.Slice((*byte)(data), offsetsSlice[n]) // #nosec G103
	targetSlice := unsafe.Slice((*byte)(target), targetLen)   // #nosec G103
	resultSlice := unsafe.Slice((*int32)(result), n)          // #nosec G103
	for i := 0; i < n; i++ {
		strStart := offsetsSlice[i]
		strLen := offsetsSlice[i+1] - strStart
		if strLen == int32(targetLen) && len(dataSlice) >= int(strStart+strLen) {
			resultSlice[i] = 1
			for j := 0; j < int(strLen); j++ {
				if dataSlice[strStart+int32(j)] != targetSlice[j] {
					resultSlice[i] = 0
					break
				}
			}
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathInt64EqualAVX2Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*int64)(src), n)       // #nosec G103
	resultSlice := unsafe.Slice((*int64)(result), n) // #nosec G103
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = -1
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathFloat64EqualAVX2Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer) {
	srcSlice := unsafe.Slice((*float64)(src), n)     // #nosec G103
	resultSlice := unsafe.Slice((*int64)(result), n) // #nosec G103
	for i := 0; i < n; i++ {
		if srcSlice[i] == val {
			resultSlice[i] = 1
		} else {
			resultSlice[i] = 0
		}
	}
}

func fastPathInt32EqualAVX512Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer) {
	fastPathInt32EqualAVX2Kernel(src, n, val, result)
}

func fastPathFloat32EqualAVX512Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer) {
	fastPathFloat32EqualAVX2Kernel(src, n, val, result)
}

func fastPathInt64EqualAVX512Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer) {
	fastPathInt64EqualAVX2Kernel(src, n, val, result)
}

func fastPathFloat64EqualAVX512Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer) {
	fastPathFloat64EqualAVX2Kernel(src, n, val, result)
}
