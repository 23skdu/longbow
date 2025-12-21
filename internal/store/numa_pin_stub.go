//go:build !linux

package store

// PinToNUMANode is a no-op on non-Linux platforms.
func PinToNUMANode(topo *NUMATopology, nodeID int) error {
	// No-op on platforms without NUMA support
	return nil
}
