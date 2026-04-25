//go:build (!linux && !darwin) || (darwin && !cgo)

package memory

import "errors"

// ErrNotSupported is returned when NUMA operations are attempted on unsupported platforms.
var ErrNotSupported = errors.New("NUMA CPU affinity not supported on this platform")

// GetCurrentCPU is a stub for non-Linux and non-Darwin-CGO platforms.
// Returns -1 as getcpu syscall is Linux-specific and Mach is only available via CGO.
func GetCurrentCPU() int {
	return -1
}
