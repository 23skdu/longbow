//go:build gpu && linux
// +build gpu,linux

package cuda

// GPU kernel profiling (roadmap Item 34).
//
// The CUDA path had aggregate QPS and nothing below it: when a run got slower
// there was no way to tell which kernel got slower. This times each named GPU
// operation with a CUDA event pair recorded on its own stream - so the number
// is device time, not the host cost of the launch - and aggregates per kernel.
//
// Profiling is off by default and costs one branch per launch when off; turn it
// on with LONGBOW_GPU_KERNEL_PROF=1 or SetKernelProfiling(true). Every launch
// made while it is on is either timed or counted as untimed, so the report can
// say how much of the GPU work the table covers rather than implying it is all
// of it.

/*
#cgo CFLAGS: -DUSE_GPU
#include "cuda_prof_linux.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

// maxGPUProfilingOps mirrors LB_GPU_PROF_MAX_OPS in cuda_prof_linux.h.
const maxGPUProfilingOps = 64

// GPUOpTime is one named GPU operation's share of the device's time.
type GPUOpTime struct {
	Name     string  `json:"name"`
	Calls    int64   `json:"calls"`
	TotalMS  float64 `json:"total_ms"`
	SharePct float64 `json:"share_pct"`
}

// GPUKernelProfile is the per-kernel breakdown of a run's GPU time.
type GPUKernelProfile struct {
	// Ops is sorted by TotalMS descending, so the first entry is the thing to
	// look at.
	Ops []GPUOpTime `json:"ops"`
	// TotalMS is the device time of every timed operation.
	TotalMS float64 `json:"total_ms"`
	// UntimedCalls counts launches that were not timed - profiling disabled
	// for them, or the event pool exhausted by concurrent searches.
	UntimedCalls int64 `json:"untimed_calls"`
	// PoolExhausted is the subset of UntimedCalls lost to pool exhaustion.
	PoolExhausted int64 `json:"pool_exhausted"`
}

// AttributedFraction is the share of timed GPU time that a named kernel
// accounts for. It is 1 when every op has a name, which is the point: an
// operation that had to be lumped in as "other" would pull it down.
func (p GPUKernelProfile) AttributedFraction() float64 {
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

// MarshalText renders the profile as a fixed-width table, for a log line or a
// benchmark report.
func (p GPUKernelProfile) Table() string {
	out := ""
	for _, op := range p.Ops {
		out += fmt.Sprintf("%-44s %10d calls %10.3f ms %6.2f%%\n", op.Name, op.Calls, op.TotalMS, op.SharePct)
	}
	return out
}

var (
	profilingMu    sync.Mutex
	profilingOn    = os.Getenv("LONGBOW_GPU_KERNEL_PROF") != "" && os.Getenv("LONGBOW_GPU_KERNEL_PROF") != "0"
	profilingReady bool
	// profActive mirrors the C flag for the launch path. Names are interned
	// whether or not profiling is on - init happens before anything can turn it
	// on - so without this a call site would make two cgo calls per launch in
	// production to learn that there is nothing to record. One atomic load
	// instead, and the calls disappear with the flag.
	profActive atomic.Bool
)

// SetKernelProfiling turns GPU kernel timing on or off for the process.
//
// Enabling it creates the event pool on first use; disabling it stops the
// launches but keeps the accumulated totals, so a caller can enable, run a
// phase, and read the phase's numbers.
func SetKernelProfiling(on bool) {
	C.lb_gpu_prof_init()
	C.lb_gpu_prof_set_enabled(C.int(boolToInt(on)))
	profActive.Store(on)
	profilingMu.Lock()
	profilingOn = on
	profilingReady = true
	profilingMu.Unlock()
}

// KernelProfilingEnabled reports whether launches are being timed.
func KernelProfilingEnabled() bool { return profActive.Load() }

func ensureProfiling() {
	profilingMu.Lock()
	defer profilingMu.Unlock()
	if profilingReady {
		return
	}
	C.lb_gpu_prof_init()
	C.lb_gpu_prof_set_enabled(C.int(boolToInt(profilingOn)))
	profActive.Store(profilingOn)
	profilingReady = true
}

// ResetKernelProfile clears the accumulated per-op totals.
func ResetKernelProfile() {
	ensureProfiling()
	C.lb_gpu_prof_reset()
}

// KernelProfile collects the device into per-op totals and returns the
// breakdown. It synchronizes the device, so call it between phases rather than
// inside one.
func KernelProfile() GPUKernelProfile {
	ensureProfiling()
	n := C.lb_gpu_prof_op_count()
	if n > maxGPUProfilingOps {
		n = maxGPUProfilingOps
	}
	ms := make([]C.double, maxGPUProfilingOps)
	calls := make([]C.longlong, maxGPUProfilingOps)
	if n > 0 {
		C.lb_gpu_prof_collect(&ms[0], &calls[0], C.int(maxGPUProfilingOps))
	}

	prof := GPUKernelProfile{
		UntimedCalls:  int64(C.lb_gpu_prof_untimed()),
		PoolExhausted: int64(C.lb_gpu_prof_pool_exhausted()),
	}
	for i := 0; i < int(n); i++ {
		name := C.GoString(C.lb_gpu_prof_op_name(C.int(i)))
		total := float64(ms[i])
		prof.Ops = append(prof.Ops, GPUOpTime{
			Name:    name,
			Calls:   int64(calls[i]),
			TotalMS: total,
		})
		prof.TotalMS += total
	}
	for i := range prof.Ops {
		if prof.TotalMS > 0 {
			prof.Ops[i].SharePct = 100 * prof.Ops[i].TotalMS / prof.TotalMS
		}
	}
	sort.Slice(prof.Ops, func(i, j int) bool { return prof.Ops[i].TotalMS > prof.Ops[j].TotalMS })
	return prof
}

// profOp is a call-site handle: the interned id of one GPU operation, resolved
// once at package init so a launch pays no map lookup.
type profOp int32

// registerOp interns a name for a call site. Registering while profiling is
// disabled yields -1, and the call sites treat that as "do not time me".
func registerOp(name string) profOp {
	ensureProfiling()
	return profOp(C.lb_gpu_prof_op_id(C.CString(name)))
}

// streamHandle coerces the stream the package already passes around to the one
// type the C helpers take. Call sites hold it as either an unsafe.Pointer (the
// helpers in this package) or a C.cudaStream_t (the kernel launches), and a nil
// stream is the default stream - which is what a synchronous cudaMemcpy runs on.
func streamHandle(stream any) unsafe.Pointer {
	switch s := stream.(type) {
	case nil:
		return nil
	case unsafe.Pointer:
		return s
	case C.cudaStream_t:
		return unsafe.Pointer(s)
	default:
		return nil
	}
}

func (op profOp) begin(stream any) {
	if op < 0 || !profActive.Load() {
		return
	}
	C.lb_gpu_prof_begin(C.int(op), streamHandle(stream))
}

func (op profOp) end(stream any) {
	if op < 0 || !profActive.Load() {
		return
	}
	C.lb_gpu_prof_end(streamHandle(stream))
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// profStream coerces the stream handle the package already passes around.
func profStream(stream any) unsafe.Pointer {
	if stream == nil {
		return nil
	}
	return stream.(unsafe.Pointer)
}

// The named operations. Each is interned once at package init, so a launch pays
// no map lookup and an unprofiled build pays one comparison against -1.
//
// The kernel names match the __global__ symbols they time, so a number in the
// report can be looked up in a profiler: l2_distance_kernel_v2_batched in this
// table is that kernel in kernels.cu.
var (
	profL2BatchedLarge = registerOp("l2_distance_kernel_large_v2_batched")
	profL2Batched      = registerOp("l2_distance_kernel_v2_batched")
	profL2Complex64          = registerOp("l2_distance_complex64_kernel")
	profL2Complex128         = registerOp("l2_distance_complex128_kernel")
	profL2Complex128Batched  = registerOp("l2_distance_complex128_kernel_v2_batched")
	profL2Candidates         = registerOp("l2_distance_candidates_kernel")
	profTQDistant            = registerOp("turboquant_distance_kernel_v2_batched")
	profTQDescent      = registerOp("turboquant_greedy_descent_kernel")
	profGraphBFS       = registerOp("graph_bfs_expand_kernel")
	profGraphActivate  = registerOp("graph_activation_propagate_kernel")
	profPQDistance     = registerOp("pq_distance_kernel")
	profCopyH2D        = registerOp("memcpy_host_to_device")
	profCopyD2H        = registerOp("memcpy_device_to_host")

	// The C helpers that launch a kernel internally. They are timed as a
	// whole, from Go, so the name covers the kernel and the transfers inside
	// it - which is the honest unit: the Go side cannot bracket a kernel it
	// does not launch. PruneNeighbors is the one that matters here: it is the
	// only GPU work a CPU-indexed server actually does, from the bulk
	// linker's neighbour selection.
	profHNSWPrune   = registerOp("hnsw_prune_neighbors_with_copies")
	profHaversine   = registerOp("haversine_distance_with_copies")
	profNormBatch   = registerOp("l2_squared_norm_with_copies")
	profPQEncode    = registerOp("pq_encode_with_copies")
	profKMeansTrain = registerOp("kmeans_train_with_copies")
)
