//go:build !amd64

package query

import "unsafe"

func fastPathInt32EqualAVX2Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathFloat32EqualAVX2Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathBoolAVX2Kernel(src unsafe.Pointer, nBytes int, negate bool, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathStringEqualAVX2Kernel(offsets unsafe.Pointer, data unsafe.Pointer, n int, target unsafe.Pointer, targetLen int, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathInt64EqualAVX2Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathFloat64EqualAVX2Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathInt32EqualAVX512Kernel(src unsafe.Pointer, n int, val int32, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathFloat32EqualAVX512Kernel(src unsafe.Pointer, n int, val float32, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathInt64EqualAVX512Kernel(src unsafe.Pointer, n int, val int64, result unsafe.Pointer) {
	panic("not implemented on this arch")
}

func fastPathFloat64EqualAVX512Kernel(src unsafe.Pointer, n int, val float64, result unsafe.Pointer) {
	panic("not implemented on this arch")
}
