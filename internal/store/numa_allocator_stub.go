//go:build !linux

package store

import "errors"

// ErrNotSupported is returned when NUMA operations are attempted on unsupported platforms.
var ErrNotSupported = errors.New("NUMA CPU affinity not supported on this platform")

// GetCurrentCPU is a stub for non-Linux platforms.
// Returns -1 as getcpu syscall is Linux-specific.
func GetCurrentCPU() int {
	return -1
}
