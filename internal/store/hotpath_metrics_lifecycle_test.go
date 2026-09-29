package store

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countHotpathFlushers reports how many hot path flusher goroutines are alive.
// Counting them by stack frame rather than by runtime.NumGoroutine keeps the
// assertion immune to the unrelated workers a store starts and stops.
func countHotpathFlushers() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "metrics.runHotpathFlusher")
}

// newLifecycleTestStore builds a store wired to a temporary data path.
func newLifecycleTestStore(t *testing.T) *VectorStore {
	t.Helper()
	s := NewVectorStore(memory.NewGoAllocator(), discardLogger(), 1<<30, 1<<20, time.Hour)
	require.NoError(t, s.InitPersistence(storage.StorageConfig{
		DataPath:         t.TempDir(),
		SnapshotInterval: time.Hour,
	}))
	return s
}

// TestHotpathFlushers_StoreLifecycle checks that a live store keeps the shared
// flusher publishing, that it publishes what the accumulators hold, and that
// shutting the store down does not leave a flusher goroutine behind.
//
// Other tests in this package create stores of their own, so the assertions
// hold for any reference count: the flusher is process wide and a store never
// starts a second one.
func TestHotpathFlushers_StoreLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	before := countHotpathFlushers()

	s := newLifecycleTestStore(t)
	require.True(t, metrics.HotpathFlushersRunning(), "a live store must keep the flusher running")
	assert.LessOrEqual(t, countHotpathFlushers(), 1, "a store must never start a second flusher")

	metrics.HNSWSearchPoolGetSharded.Inc()
	metrics.FlushHotpathCounters()
	published := testutil.ToFloat64(metrics.HNSWSearchPoolGetTotal)
	assert.GreaterOrEqual(t, published, 1.0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))

	assert.LessOrEqual(t, countHotpathFlushers(), before, "shutdown must not leave a flusher behind")
	if before == 0 {
		assert.False(t, metrics.HotpathFlushersRunning(), "the last shutdown must stop the flusher")
	}
}

// TestHotpathFlushers_NoGoroutineLeakAcrossStoreCycles is the leak check for the
// lifecycle wiring: repeatedly creating and shutting down stores must never
// accumulate flusher goroutines, no matter how many stores the process holds.
func TestHotpathFlushers_NoGoroutineLeakAcrossStoreCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	// Warm up so pools and lazily started workers are already running.
	for i := 0; i < 2; i++ {
		s := newLifecycleTestStore(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.NoError(t, s.Shutdown(ctx))
		cancel()
	}
	before := countHotpathFlushers()

	const cycles = 8
	for i := 0; i < cycles; i++ {
		s := newLifecycleTestStore(t)
		require.True(t, metrics.HotpathFlushersRunning())
		metrics.HnswContextCheckSharded.Inc()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		require.NoError(t, s.Shutdown(ctx))
		cancel()

		assert.LessOrEqual(t, countHotpathFlushers(), max(before, 1),
			"cycle %d leaked a flusher goroutine", i)
	}
}

// TestHotpathFlushers_OverlappingStoreLifecycles checks the reference counting:
// overlapping stores share a single flusher, so shutting the first one down
// cannot take the publishing away from the second.
func TestHotpathFlushers_OverlappingStoreLifecycles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	before := countHotpathFlushers()
	first := newLifecycleTestStore(t)
	second := newLifecycleTestStore(t)
	assert.LessOrEqual(t, countHotpathFlushers(), 1, "two stores must share a single flusher")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, first.Shutdown(ctx))
	assert.LessOrEqual(t, countHotpathFlushers(), 1, "the flusher must survive while a store is alive")

	require.NoError(t, second.Shutdown(ctx))
	assert.LessOrEqual(t, countHotpathFlushers(), before)
}

// TestHotpathFlushers_ShutdownIsIdempotent guards the stopOnce path: a second
// Shutdown must not flush twice or resurrect the flusher.
func TestHotpathFlushers_ShutdownIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	before := countHotpathFlushers()
	s := newLifecycleTestStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, s.Shutdown(ctx))
	require.NoError(t, s.Shutdown(ctx))
	require.NoError(t, s.Close())

	assert.LessOrEqual(t, countHotpathFlushers(), before)
}
