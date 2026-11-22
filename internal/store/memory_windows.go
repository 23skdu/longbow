//go:build windows

package store

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
)

// MemoryAdvice identifies the type of access pattern for a memory region.
type MemoryAdvice int

const (
	// AdviceNormal is a advice normal constant.
	AdviceNormal MemoryAdvice = iota
	// AdviceRandom is a advice random constant.
	AdviceRandom
	// AdviceSequential is a advice sequential constant.
	AdviceSequential
	// AdviceWillNeed is a advice will need constant.
	AdviceWillNeed
	// AdviceDontNeed is a advice dont need constant.
	AdviceDontNeed
	// AdviceHugePage is a advice huge page constant.
	AdviceHugePage
)

// AdviseMemory is a no-op on Windows.
func AdviseMemory(ptr unsafe.Pointer, size uintptr, advice MemoryAdvice) error {
	return nil
}

// LockMemory is a no-op on Windows stub.
func LockMemory(ptr unsafe.Pointer, size uintptr) error {
	return nil
}

// UnlockMemory is a no-op on Windows stub.
func UnlockMemory(ptr unsafe.Pointer, size uintptr) error {
	return nil
}

// PinThreadToCore is a no-op on Windows.
func PinThreadToCore(core int) error {
	return nil
}

// SaveThreadAffinity is a no-op on Windows.
func SaveThreadAffinity() func() {
	return func() {}
}

// GetNumaNode is a no-op on Windows.
func GetNumaNode(ptr unsafe.Pointer) (int, error) {
	return -1, nil
}

// PinThreadToNode is a no-op on Windows.
func PinThreadToNode(node int) error {
	return nil
}

// AdviseRecord provides memory hints for all buffers in an Arrow RecordBatch.
func AdviseRecord(rec arrow.RecordBatch, advice MemoryAdvice) {
	// No-op
}
