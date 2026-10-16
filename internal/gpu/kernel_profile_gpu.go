//go:build gpu && linux && amd64

package gpu

// Per-kernel GPU time, for the CUDA backend (roadmap Item 34).
//
// The types live here rather than in internal/gpu/cuda so that callers - the
// server's action handler, the benchmark client - compile in every build,
// including CPU-only ones where the answer is simply empty.

import "github.com/23skdu/longbow/internal/gpu/cuda"

// KernelProfile is the per-kernel breakdown of a run's device time, and
// KernelOpTime one named operation's share of it. Both come from the CUDA
// package, which owns the event pool and the interned names.
type (
	KernelProfile = cuda.GPUKernelProfile
	KernelOpTime  = cuda.GPUOpTime
)

// SetKernelProfiling turns per-kernel device timing on or off. It costs one
// branch per launch when off.
func SetKernelProfiling(on bool) { cuda.SetKernelProfiling(on) }

// KernelProfilingEnabled reports whether launches are being timed.
func KernelProfilingEnabled() bool { return cuda.KernelProfilingEnabled() }

// ResetKernelProfile clears the accumulated totals.
func ResetKernelProfile() { cuda.ResetKernelProfile() }

// CurrentKernelProfile collects device time into per-op totals. It
// synchronizes the device, so call it between phases rather than inside one.
// Named for the current profile rather than the type it returns.
func CurrentKernelProfile() KernelProfile { return cuda.KernelProfile() }
