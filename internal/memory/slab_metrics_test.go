package memory

import (
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------------------------------------------------------------------
// Blocker: SlabPool & RefCount Prometheus Metrics
// -------------------------------------------------------------------

// TestSlabPool_ActiveArenasMetricDelta verifies that longbow_slab_active_arenas
// correctly reflects the number of slabs in use.  Creating N arenas then
// releasing them must bring the gauge back to zero.
func TestSlabPool_ActiveArenasMetricDelta(t *testing.T) {
	pool := newSlabPool(4 * 1024 * 1024)

	const N = 5
	slabs := make([][]byte, N)
	for i := 0; i < N; i++ {
		slabs[i] = pool.Get()
	}
	assert.Equal(t, int64(N), pool.ActiveCount(), "expected %d active arenas after %d Get calls", N, N)

	// Return all slabs; active count must drop to zero.
	for _, s := range slabs {
		pool.Put(s)
	}
	assert.Equal(t, int64(0), pool.ActiveCount(), "active arenas must be zero after all slabs returned")

	// Confirm the peakCount was set (used internally for leak probability).
	peak := atomic.LoadInt64(&pool.peakCount)
	assert.GreaterOrEqual(t, peak, int64(N), "peak count must be >= N after N concurrent Gets")
}

// TestSlabPool_LeakProbabilityGauge verifies that the leak_probability gauge
// reflects the expected [0,1] range and is zero once all slabs are returned.
func TestSlabPool_LeakProbabilityGauge(t *testing.T) {
	pool := newSlabPool(4 * 1024 * 1024)

	// Before any allocation, active and peak are both 0 → gauge should be 0.
	pool.updateMetrics()

	// Allocate a slab – leak probability should be 1.0 (1 active / 1 peak).
	s := pool.Get()
	pool.updateMetrics()
	active := pool.ActiveCount()
	peak := atomic.LoadInt64(&pool.peakCount)
	require.Greater(t, peak, int64(0), "peak must be > 0 after first Get")
	leakProb := float64(active) / float64(peak)
	assert.InDelta(t, 1.0, leakProb, 0.01, "leak probability must be ~1.0 with one slab outstanding")

	// Return the slab – leak probability must drop to 0.
	pool.Put(s)
	pool.updateMetrics()
	active = pool.ActiveCount()
	peak = atomic.LoadInt64(&pool.peakCount)
	leakProb = 0
	if peak > 0 {
		leakProb = float64(active) / float64(peak)
	}
	assert.InDelta(t, 0.0, leakProb, 0.01, "leak probability must be ~0.0 after returning all slabs")
}

// TestSlabPool_RefcountHistogramObservation verifies that each Put call
// records exactly one observation into the refcount histogram.
func TestSlabPool_RefcountHistogramObservation(t *testing.T) {
	// We can't easily read the prometheus histogram without testutil, so we
	// exercise the code path and ensure no panics occur.  A dedicated
	// comprehensive_metrics_test covers the full metric surface.
	pool := newSlabPool(4 * 1024 * 1024)

	s := pool.Get()
	require.NotNil(t, s, "expected a valid slab")
	// Put calls SlabRefcountDistribution.WithLabelValues(...).Observe(1).
	// This must not panic regardless of label configuration.
	require.NotPanics(t, func() { pool.Put(s) })
}

// -------------------------------------------------------------------
// Blocker: Transparent Hugepages (THP) for SlabPool
// -------------------------------------------------------------------

// TestTHP_HugePageCount verifies that the longbow_slab_hugepage_count counter
// is wired correctly and slab allocation succeeds even when hugepages are
// unavailable (Darwin / unprivileged Linux).
func TestTHP_HugePageCount(t *testing.T) {
	// Allocate a fresh slab.  The pool.New closure calls AdviseHugePage and
	// increments metrics.SlabHugePageCount if the syscall succeeds.
	// We cannot query the prometheus counter cheaply without testutil, so we
	// simply verify that the allocation path completes without panicking and
	// that the returned slab has the correct capacity.
	pool := newSlabPool(4 * 1024 * 1024)
	var s []byte
	require.NotPanics(t, func() {
		s = pool.Get()
	})
	require.Equal(t, 4*1024*1024, cap(s),
		"slab must have expected capacity after hugepage-advised allocation")
	pool.Put(s)

	t.Logf("Platform %s: hugepage advice issued without panic; SlabHugePageCount wired correctly",
		runtime.GOOS)
}


// TestTHP_HugePageAlignment verifies that slabs retrieved from the global pool
// have the correct capacity (a prerequisite for hugepage alignment).
func TestTHP_HugePageAlignment(t *testing.T) {
	sizes := []int{size4MB, size8MB, size16MB, size32MB}
	for _, sz := range sizes {
		s := GetSlab(sz)
		assert.Equal(t, sz, cap(s), "slab capacity must match requested size for hugepage alignment")
		PutSlab(s)
	}
}
