//go:build !gpu || !linux || !amd64 || !cgo

package gpu

import "fmt"

// Per-kernel GPU time for builds with no CUDA backend (roadmap Item 34).
//
// Same API as the CUDA build so the server's action handler and the benchmark
// client compile everywhere; there is simply nothing to report.

// KernelOpTime is one named GPU operation's share of the device's time, and
// KernelProfile the whole table.
type (
	KernelOpTime struct {
		Name     string  `json:"name"`
		Calls    int64   `json:"calls"`
		TotalMS  float64 `json:"total_ms"`
		SharePct float64 `json:"share_pct"`
	}

	KernelProfile struct {
		Ops           []KernelOpTime `json:"ops"`
		TotalMS       float64        `json:"total_ms"`
		UntimedCalls  int64          `json:"untimed_calls"`
		PoolExhausted int64          `json:"pool_exhausted"`
		// Supported is false when there is no CUDA backend in this build, so a
		// report can say "no kernels were measured" instead of "zero milliseconds".
		Supported bool `json:"supported"`
	}
)

// AttributedFraction is the share of timed GPU time a named kernel accounts
// for. It is 1 when every op has a name.
func (p KernelProfile) AttributedFraction() float64 {
	if p.TotalMS <= 0 {
		return 0
	}
	var named float64
	for _, op := range p.Ops {
		if op.Name != "" && op.Name != "unattributed" {
			named += op.TotalMS
		}
	}
	return named / p.TotalMS
}

// Table renders the profile as a fixed-width table.
func (p KernelProfile) Table() string {
	out := ""
	for _, op := range p.Ops {
		out += fmt.Sprintf("%-44s %10d calls %10.3f ms %6.2f%%\n", op.Name, op.Calls, op.TotalMS, op.SharePct)
	}
	return out
}

// SetKernelProfiling is a no-op without a CUDA backend.
func SetKernelProfiling(bool) {}

// KernelProfilingEnabled reports that nothing is being timed.
func KernelProfilingEnabled() bool { return false }

// ResetKernelProfile is a no-op without a CUDA backend.
func ResetKernelProfile() {}

// CurrentKernelProfile returns an empty, unsupported profile.
func CurrentKernelProfile() KernelProfile { return KernelProfile{} }
