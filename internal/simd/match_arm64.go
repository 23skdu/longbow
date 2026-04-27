//go:build arm64
// +build arm64

package simd

import (
	"unsafe"
)

func matchInt64NeonKernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*int64)(src), n) // #nosec G103 -- intentional unsafe for SIMD
	dstSlice := unsafe.Slice((*byte)(dst), n) // #nosec G103 -- intentional unsafe for SIMD
	compareOp := CompareOp(op)
	// Error is always nil, ignore it
	_ = matchInt64Generic(s, val, compareOp, dstSlice)
}

func matchInt32NeonKernel(src unsafe.Pointer, val int32, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*int32)(src), n) // #nosec G103 -- intentional unsafe for SIMD
	dstSlice := unsafe.Slice((*byte)(dst), n) // #nosec G103 -- intentional unsafe for SIMD
	compareOp := CompareOp(op)
	_ = matchInt32Generic(s, val, compareOp, dstSlice)
}

func matchFloat32NeonKernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*float32)(src), n) // #nosec G103 -- intentional unsafe for SIMD
	dstSlice := unsafe.Slice((*byte)(dst), n) // #nosec G103 -- intentional unsafe for SIMD
	compareOp := CompareOp(op)
	_ = matchFloat32Generic(s, val, compareOp, dstSlice)
}

func matchFloat64NeonKernel(src unsafe.Pointer, val float64, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*float64)(src), n) // #nosec G103 -- intentional unsafe for SIMD
	dstSlice := unsafe.Slice((*byte)(dst), n) // #nosec G103 -- intentional unsafe for SIMD
	compareOp := CompareOp(op)
	_ = matchFloat64Generic(s, val, compareOp, dstSlice)
}
