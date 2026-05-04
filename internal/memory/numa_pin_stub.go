//go:build !linux

package memory

import "unsafe"

// PinToNUMANode is a no-op on non-Linux platforms.
func PinToNUMANode(topo *NUMATopology, nodeID int) error {
	// No-op on platforms without NUMA support
	return nil
}

// PinThreadToCore is a no-op on non-Linux platforms.
func PinThreadToCore(coreID int) error {
	return nil
}

// MbindMemory is a no-op on non-Linux platforms.
func MbindMemory(ptr unsafe.Pointer, size int, nodeID int) error {
	return nil
}
