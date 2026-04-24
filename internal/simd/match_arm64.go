//go:build arm64
// +build arm64

package simd

import (
	"unsafe"
)

func matchInt64NeonKernel(src unsafe.Pointer, val int64, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*int64)(src), n)
	d := unsafe.Slice((*byte)(dst), n)
	compareOp := CompareOp(op)
	matchInt64Generic(s, val, compareOp, d)
}

func matchInt32NeonKernel(src unsafe.Pointer, val int32, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*int32)(src), n)
	d := unsafe.Slice((*byte)(dst), n)
	compareOp := CompareOp(op)
	matchInt32Generic(s, val, compareOp, d)
}

func matchFloat32NeonKernel(src unsafe.Pointer, val float32, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*float32)(src), n)
	d := unsafe.Slice((*byte)(dst), n)
	compareOp := CompareOp(op)
	matchFloat32Generic(s, val, compareOp, d)
}

func matchFloat64NeonKernel(src unsafe.Pointer, val float64, op int, dst unsafe.Pointer, n int) {
	s := unsafe.Slice((*float64)(src), n)
	d := unsafe.Slice((*byte)(dst), n)
	compareOp := CompareOp(op)
	matchFloat64Generic(s, val, compareOp, d)
}
