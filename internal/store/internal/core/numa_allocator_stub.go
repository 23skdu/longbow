//go:build !linux || !cgo || !numa

package core

func allocateNUMAArena(size int, node int) []byte {
	return make([]byte, size)
}

func freeNUMAArena(b []byte, size int) {
	// Let GC handle it
}

// PinGoroutineToNode binds the current thread to the specified NUMA node.
// Callers should ensure runtime.LockOSThread() is called prior.
func PinGoroutineToNode(node int) {
	// No-op on non-linux/non-numa platforms
}
