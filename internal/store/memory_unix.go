//go:build linux || darwin

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
	// AdviceNormal indicates the default memory access pattern.
	AdviceNormal MemoryAdvice = iota
	// AdviceRandom indicates that memory access is expected to be random.
	AdviceRandom
	// AdviceSequential indicates that memory access is expected to be sequential.
	AdviceSequential
	// AdviceWillNeed indicates that the memory will be needed in the near future.
	AdviceWillNeed
	// AdviceDontNeed indicates that the memory will not be needed in the near future.
	AdviceDontNeed
	// AdviceHugePage indicates that the memory should use huge pages if available.
	AdviceHugePage
)

// AdviseMemory provides hints to the kernel about the memory usage pattern.
func AdviseMemory(ptr unsafe.Pointer, size uintptr, advice MemoryAdvice) error {
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

	b := unsafe.Slice((*byte)(ptr), size) // #nosec G103
	return unix.Madvise(b, unixAdvice)
}

// LockMemory pins the memory region in RAM, preventing it from being swapped out.
func LockMemory(ptr unsafe.Pointer, size uintptr) error {
	b := unsafe.Slice((*byte)(ptr), size) // #nosec G103
	return unix.Mlock(b)
}

// UnlockMemory unpins a previously locked memory region.
func UnlockMemory(ptr unsafe.Pointer, size uintptr) error {
	b := unsafe.Slice((*byte)(ptr), size) // #nosec G103
	return unix.Munlock(b)
}

// PinThreadToCore pins the current goroutine's thread to a specific CPU core.
func PinThreadToCore(core int) error {
	if runtime.GOOS == "linux" {
		return pinThreadToCoreLinux(core)
	}
	return nil
}

// GetNumaNode returns the NUMA node (memory bank) where the page containing
// the given pointer resides.
func GetNumaNode(ptr unsafe.Pointer) (int, error) {
	if runtime.GOOS == "linux" {
		return getNumaNodeLinux(ptr)
	}
	return -1, nil
}

// PinThreadToNode pins the current goroutine's thread to the set of CPUs
// associated with the given NUMA node.
func PinThreadToNode(node int) error {
	if runtime.GOOS == "linux" {
		return pinThreadToNodeLinux(node)
	}
	return nil
}

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
			ptr := unsafe.Pointer(&b[0]) // #nosec G103
			_ = AdviseMemory(ptr, size, advice)
		}
	}
	for _, child := range data.Children() {
		adviseData(child, advice)
	}
}
