//go:build amd64

package simd

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasPOPCNT {
		hammingImpl = hammingAVX2
	}
}

//go:noescape
func hammingAVX2Kernel(a, b unsafe.Pointer, n int) int

func hammingAVX2(a, b []uint64) int {
	if len(a) == 0 {
		return 0
	}
	return hammingAVX2Kernel(unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), len(a))
}

//go:noescape
func andBytesAVX2Kernel(dst, src unsafe.Pointer, n int)

//go:noescape
func orBytesAVX2Kernel(dst, src unsafe.Pointer, n int)

func andBytesAVX2(dst, src []byte) {
	if len(dst) == 0 {
		return
	}
	andBytesAVX2Kernel(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), len(dst))
}

func orBytesAVX2(dst, src []byte) {
	if len(dst) == 0 {
		return
	}
	orBytesAVX2Kernel(unsafe.Pointer(&dst[0]), unsafe.Pointer(&src[0]), len(dst))
}

//go:noescape
func isAllZerosAVX2Kernel(src unsafe.Pointer, n int) bool

func isAllZerosAVX2(src []byte) bool {
	if len(src) == 0 {
		return true
	}
	return isAllZerosAVX2Kernel(unsafe.Pointer(&src[0]), len(src))
}
