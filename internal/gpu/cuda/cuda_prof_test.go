//go:build gpu && linux && cuda

package cuda

// Roadmap Item 34: the kernel profile has to say something true.
//
// These drive a real index on a real device, so the report's claims - a kernel
// that ran is timed, an operation that did not run carries no time, a second
// launch adds to the total - are checked against launches rather than against
// a stub. A profiler that reports zeros for a real kernel is worse than none,
// because it reads as "the kernel was free".

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/gpu/types"
	"github.com/stretchr/testify/require"
)

// profiledIndex builds a float32 index of n vectors and enables profiling.
func profiledIndex(t *testing.T, n, dim int) *CUDAIndex {
	t.Helper()
	if !IsAvailable() {
		t.Skip("CUDA device not available")
	}
	cfg := types.GPUConfig{DeviceID: 0, MaxMemory: 4 * 1024 * 1024 * 1024, Dimension: dim}
	idx, err := NewCUDAIndex(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })

	cudaIdx := idx.(*CUDAIndex)
	ids := make([]int64, n)
	vecs := make([]float32, n*dim)
	rng := rand.New(rand.NewSource(7)) // #nosec G404 -- deterministic corpus
	for i := 0; i < n; i++ {
		ids[i] = int64(i)
		for j := 0; j < dim; j++ {
			vecs[i*dim+j] = rng.Float32()
		}
	}
	require.NoError(t, cudaIdx.Add(ids, vecs))
	require.NoError(t, cudaIdx.Flush())

	SetKernelProfiling(true)
	t.Cleanup(func() { SetKernelProfiling(false) })
	ResetKernelProfile()
	return cudaIdx
}

func opTime(p GPUKernelProfile, name string) (calls int64, ms float64) {
	for _, op := range p.Ops {
		if op.Name == name {
			return op.Calls, op.TotalMS
		}
	}
	return 0, 0
}

func TestKernelProfileTimesTheDistanceKernel(t *testing.T) {
	idx := profiledIndex(t, 50_000, 128)

	query := make([]float32, 128)
	rng := rand.New(rand.NewSource(11)) // #nosec G404 -- deterministic query
	for i := range query {
		query[i] = rng.Float32()
	}
	// A few searches: the first pays for context setup, and timing only that
	// would measure the setup rather than the kernel.
	for i := 0; i < 3; i++ {
		_, _, err := idx.Search(query, 10)
		require.NoError(t, err)
	}

	prof := KernelProfile()
	calls, ms := opTime(prof, "l2_distance_kernel_v2_batched")
	require.Greater(t, calls, int64(0),
		"the distance kernel ran but the profile recorded no call for it:\n%s", prof.Table())
	require.Greater(t, ms, 0.0,
		"the distance kernel ran but the profile recorded no device time for it:\n%s", prof.Table())

	// Everything timed is named. An unnamed bucket would be a regression in
	// what the report means, not in the report.
	require.Equal(t, 1.0, prof.AttributedFraction(),
		"every timed operation must carry a name:\n%s", prof.Table())
	require.Zero(t, prof.UntimedCalls, "no launch should have gone untimed")
	require.Zero(t, prof.PoolExhausted, "the event pool should not have been exhausted")

	// The op rows are sorted by device time and their shares add up.
	var share float64
	for _, op := range prof.Ops {
		share += op.SharePct
	}
	require.InDelta(t, 100.0, share, 0.5, "shares must account for all measured device time")
	t.Logf("kernel profile over 3 searches of 50k x 128:\n%s", prof.Table())
}

func TestKernelProfileIsOffByDefault(t *testing.T) {
	idx := profiledIndex(t, 20_000, 64)
	SetKernelProfiling(false)
	ResetKernelProfile()

	query := make([]float32, 64)
	for i := range query {
		query[i] = 0.25
	}
	_, _, err := idx.Search(query, 10)
	require.NoError(t, err)

	require.Zero(t, KernelProfile().TotalMS,
		"profiling was off: nothing may be timed")
}

func TestKernelProfileAccumulatesAndResets(t *testing.T) {
	idx := profiledIndex(t, 50_000, 128)
	query := make([]float32, 128)

	_, _, err := idx.Search(query, 10)
	require.NoError(t, err)
	first := KernelProfile()
	require.Positive(t, first.TotalMS)

	_, _, err = idx.Search(query, 10)
	require.NoError(t, err)
	second := KernelProfile()
	firstCalls, firstMS := opTime(first, "l2_distance_kernel_v2_batched")
	secondCalls, secondMS := opTime(second, "l2_distance_kernel_v2_batched")
	require.Greater(t, secondCalls, firstCalls, "a second search must add calls, not replace them")
	require.Greater(t, secondMS, firstMS, "a second search must add device time, not replace it")

	ResetKernelProfile()
	require.Zero(t, KernelProfile().TotalMS, "reset must clear the accumulated device time")
}

// TestKernelProfile_250k is the roadmap Item 34 criterion: a full-scale run
// whose GPU time is attributed to named kernels.
//
// 250k float32 dim-128 vectors and 500 searches over 4 workers is the shape the
// item names, at the size a CUDA run is meant for. It is the same code the
// server calls, so the numbers are the ones a regression would move.
//
// The test asserts the criterion rather than printing it: at least 90% of the
// device time measured must be attributed to a named operation, nothing may be
// untimed, and the run must have actually launched kernels. A report that
// attributes 100% of zero milliseconds is not a pass.
func TestKernelProfile_250k(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and searches a 250k CUDA index")
	}
	const (
		n, dim  = 250_000, 128
		queries = 500
		workers = 4
		// Two different claims, and conflating them would hide the more
		// useful one. Attribution asks whether every millisecond the device
		// spent is explained by something named; kernel share asks how much
		// of that is compute rather than PCIe. A regression that moved time
		// out of the kernel and into the copy is exactly what this
		// instrumentation exists to show, so the copy time is reported, not
		// folded into "other".
		minKernelShare = 0.80
	)

	if !IsAvailable() {
		t.Skip("CUDA device not available")
	}
	cfg := types.GPUConfig{DeviceID: 0, MaxMemory: 6 * 1024 * 1024 * 1024, Dimension: dim}
	idx, err := NewCUDAIndex(cfg)
	require.NoError(t, err)
	defer idx.Close()
	cudaIdx := idx.(*CUDAIndex)

	SetKernelProfiling(true)
	defer SetKernelProfiling(false)
	ResetKernelProfile()

	rng := rand.New(rand.NewSource(19)) // #nosec G404 -- deterministic corpus
	ids := make([]int64, n)
	vecs := make([]float32, n*dim)
	buildStart := time.Now()
	const chunk = 25_000
	for off := 0; off < n; off += chunk {
		end := off + chunk
		if end > n {
			end = n
		}
		for i := off; i < end; i++ {
			ids[i] = int64(i)
			for j := 0; j < dim; j++ {
				vecs[i*dim+j] = rng.Float32()
			}
		}
		require.NoError(t, cudaIdx.Add(ids[off:end], vecs[off*dim:end*dim]))
	}
	require.NoError(t, cudaIdx.Flush())
	buildWall := time.Since(buildStart)

	queries512 := make([][]float32, queries)
	for i := range queries512 {
		q := make([]float32, dim)
		for j := range q {
			q[j] = rng.Float32()
		}
		queries512[i] = q
	}
	searchStart := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < queries; i += workers {
				_, _, err := cudaIdx.Search(queries512[i], 10)
				require.NoError(t, err)
			}
		}(w)
	}
	wg.Wait()
	searchWall := time.Since(searchStart)

	prof := KernelProfile()
	require.Positive(t, prof.TotalMS, "no GPU work was measured: the run launched no kernels")

	var kernelMS, otherMS float64
	for _, op := range prof.Ops {
		if op.Name == "memcpy_host_to_device" || op.Name == "memcpy_device_to_host" ||
			len(op.Name) > 9 && op.Name[len(op.Name)-9:] == "_with_copies" {
			otherMS += op.TotalMS
			continue
		}
		kernelMS += op.TotalMS
	}
	kernelShare := kernelMS / prof.TotalMS
	t.Logf("250k x %d float32: build %v, %d searches in %v\n%s", dim, buildWall, queries, searchWall, prof.Table())
	t.Logf("device time %.3f ms: %.2f%% attributed to named operations, %.2f%% of it kernel compute, %.2f%% transfers",
		prof.TotalMS, 100*prof.AttributedFraction(), 100*kernelShare, 100*(1-kernelShare))

	// The criterion: the run's GPU time is attributable, not mysterious.
	require.GreaterOrEqual(t, prof.AttributedFraction(), 0.90,
		"at least 90%% of GPU time must be attributed to named operations:\n%s", prof.Table())
	require.InDelta(t, 1.0, prof.AttributedFraction(), 1e-9,
		"every timed operation must carry a name")

	// And the bulk of it must still be compute: a run where the transfers
	// dominate has moved its bottleneck to PCIe and should not be reported as
	// a healthy kernel profile.
	require.GreaterOrEqual(t, kernelShare, minKernelShare,
		"at least %.0f%% of GPU time should be kernel compute, not transfers:\n%s", 100*minKernelShare, prof.Table())
	require.Zero(t, prof.UntimedCalls, "no launch may go untimed in a reported run")
	require.Zero(t, prof.PoolExhausted, "the event pool must not be exhausted at this scale")
}
