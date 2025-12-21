package store

import (
	"runtime"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/sys/unix"
)

// MemoryAdvice identifies the type of access pattern for a memory region.
type MemoryAdvice int

const (
	AdviceNormal MemoryAdvice = iota
	AdviceRandom
	AdviceSequential
	AdviceWillNeed
	AdviceDontNeed
	AdviceHugePage
)

// AdviseMemory provides hints to the kernel about the memory usage pattern.
// This is a no-op on non-Unix systems.
func AdviseMemory(ptr unsafe.Pointer, size uintptr, advice MemoryAdvice) error {
	if runtime.GOOS == "windows" {
		return nil
	}

	var unixAdvice int
	switch advice {
	case AdviceNormal:
		unixAdvice = unix.MADV_NORMAL
	case AdviceRandom:
		unixAdvice = unix.MADV_RANDOM
	case AdviceSequential:
		unixAdvice = unix.MADV_SEQUENTIAL
	case AdviceWillNeed:
		unixAdvice = unix.MADV_WILLNEED
	case AdviceDontNeed:
		unixAdvice = unix.MADV_DONTNEED
	case AdviceHugePage:
		// MADV_HUGEPAGE is only available on Linux
		if runtime.GOOS == "linux" {
			unixAdvice = 14 // unix.MADV_HUGEPAGE usually 14
		} else {
			return nil
		}
	default:
		return nil
	}

	// Create a slice header to pass to Madvise
	// Note: Madvise expects a byte slice
	b := unsafe.Slice((*byte)(ptr), size)
	return unix.Madvise(b, unixAdvice)
}

// LockMemory pins the memory region in RAM, preventing it from being swapped out.
func LockMemory(ptr unsafe.Pointer, size uintptr) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	b := unsafe.Slice((*byte)(ptr), size)
	return unix.Mlock(b)
}

// UnlockMemory unpins a previously locked memory region.
func UnlockMemory(ptr unsafe.Pointer, size uintptr) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	b := unsafe.Slice((*byte)(ptr), size)
	return unix.Munlock(b)
}

// PinThreadToCore pins the current goroutine's thread to a specific CPU core.
// This is most effective when combined with runtime.LockOSThread().
// On non-Linux systems, this returns nil (no-op).
func PinThreadToCore(core int) error {
	if runtime.GOOS == "linux" {
		return pinThreadToCoreLinux(core)
	}
	return nil
}

// GetNumaNode returns the NUMA node (memory bank) where the page containing
// the given pointer resides.
// On non-Linux systems, returns -1 and nil error.
func GetNumaNode(ptr unsafe.Pointer) (int, error) {
	if runtime.GOOS == "linux" {
		return getNumaNodeLinux(ptr)
	}
	return -1, nil
}

// PinThreadToNode pins the current goroutine's thread to the set of CPUs
// associated with the given NUMA node.
// On non-Linux systems, this returns nil (no-op).
func PinThreadToNode(node int) error {
	if runtime.GOOS == "linux" {
		return pinThreadToNodeLinux(node)
	}
	return nil
}

// Linux-specific hooks (implemented in memory_linux.go)
// We define them here as stubs to allow compilation on non-Linux if memory_linux.go isn't compiled,
// but usually file suffixes handle that.
// However, since we call them from here conditionally, we need build tags or stubs.
// Better approach: Use build tags completely.
// Let's refactor:
// memory.go: contains the exported functions calling internal platform-specific implementations.
// But Go build tags are file-level.
// To keep it simple in one file for non-Linux and one for Linux:
// We can have `memory_generated.go` or similar?
// Or just:
// memory.go:
//   func PinThreadToCore(core int) error { return pinThreadToCoreImpl(core) }
// memory_stub.go (build !linux):
//   func pinThreadToCoreImpl(core int) error { return nil }
// memory_linux.go (build linux):
//   func pinThreadToCoreImpl(core int) error { <real impl> }
//
// Let's adopt this pattern.

// AdviseRecord provides memory hints for all buffers in an Arrow RecordBatch.
func AdviseRecord(rec arrow.RecordBatch, advice MemoryAdvice) {
	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		adviseData(col.Data(), advice)
	}
}

func adviseData(data arrow.ArrayData, advice MemoryAdvice) {
	if data == nil {
		return
	}
	for _, buf := range data.Buffers() {
		if buf == nil {
			continue
		}
		b := buf.Bytes()
		size := uintptr(len(b))
		if size > 0 {
			ptr := unsafe.Pointer(&b[0])
			_ = AdviseMemory(ptr, size, advice)
		}
	}
	for _, child := range data.Children() {
		adviseData(child, advice)
	}
}
