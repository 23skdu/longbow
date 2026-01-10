package simd

import (
	"testing"
	"unsafe"
)

func TestPrefetch(t *testing.T) {
	// 1. Prefetching valid memory should not crash
	data := make([]byte, 1024)
	for i := 0; i < len(data); i += 64 {
		Prefetch(unsafe.Pointer(&data[i]))
	}

	// 2. Prefetching nil should not crash (hardware ignores it)
	Prefetch(nil)

	// 3. Prefetching "invalid" pointers might segfault on some archs if not handled,
	// but strictly speaking x86 and arm64 prefetch instructions usually ignore faults.
	// However, we should be careful. We can try a pointer that is likely unmapped.
	// Ideally we don't test this to avoid flakiness if specific OS/HW combinations are strict.
	// But ensuring nil safety is critical.
}
