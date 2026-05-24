//go:build linux && cgo && numa

package core

/*
#cgo LDFLAGS: -lnuma
#include <numa.h>
*/
import "C"
import "unsafe"

func allocateNUMAArena(size int, node int) []byte {
	if C.numa_available() == -1 {
		return make([]byte, size)
	}
	ptr := C.numa_alloc_onnode(C.size_t(size), C.int(node))
	return unsafe.Slice((*byte)(ptr), size)
}

func freeNUMAArena(b []byte, size int) {
	if C.numa_available() == -1 {
		return
	}
	C.numa_free(unsafe.Pointer(&b[0]), C.size_t(size))
}

// PinGoroutineToNode binds the current thread to the specified NUMA node.
// Callers should ensure runtime.LockOSThread() is called prior.
func PinGoroutineToNode(node int) {
	if C.numa_available() != -1 {
		C.numa_run_on_node(C.int(node))
	}
}
