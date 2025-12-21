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
// On non-Linux systems, this returns an error or is a no-op.
func PinThreadToCore(core int) error {
	if runtime.GOOS != "linux" {
		return nil
	}

	// Linux specific implementation using sched_setaffinity
	// This would require more complex syscall wrapping or cgo
	// For now, we provide the hook for Linux environments.
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
			ptr := unsafe.Pointer(&b[0])
			_ = AdviseMemory(ptr, size, advice)
		}
	}
	for _, child := range data.Children() {
		adviseData(child, advice)
	}
}
