package memory

import (
	"github.com/apache/arrow-go/v18/arrow/memory"
	"unsafe"
)

// NUMAAllocator is a NUMA-aware memory allocator for Arrow buffers.
// On Linux with NUMA, it attempts to allocate memory on the specified node.
// On other platforms, it falls back to the default Go allocator.
type NUMAAllocator struct {
	nodeID int
	base   memory.Allocator
	topo   *NUMATopology
}

// NewNUMAAllocator creates a new NUMA-aware allocator for the specified node.
func NewNUMAAllocator(topo *NUMATopology, nodeID int) *NUMAAllocator {
	return &NUMAAllocator{
		nodeID: nodeID,
		base:   memory.NewGoAllocator(),
		topo:   topo,
	}
}

// Allocate allocates memory, preferably on the NUMA node.
func (a *NUMAAllocator) Allocate(size int) []byte {
	// Use base allocator (Go's allocator respects thread affinity on Linux)
	// When goroutine is pinned to NUMA node, allocations tend to be local
	b := a.base.Allocate(size)
	if len(b) > 0 && a.nodeID >= 0 {
		// Explicitly bind the allocated memory to the target node
		_ = MbindMemory(unsafe.Pointer(&b[0]), len(b), a.nodeID) // #nosec G103
	}
	return b
}

// Reallocate reallocates memory.
func (a *NUMAAllocator) Reallocate(size int, b []byte) []byte {
	newBuf := a.base.Reallocate(size, b)
	if len(newBuf) > 0 && a.nodeID >= 0 {
		_ = MbindMemory(unsafe.Pointer(&newBuf[0]), len(newBuf), a.nodeID) // #nosec G103
	}
	return newBuf
}

// Free frees memory (handled by Go GC).
func (a *NUMAAllocator) Free(b []byte) {
	a.base.Free(b)
}
