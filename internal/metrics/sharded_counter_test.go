package metrics

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gateCounter wraps a real counter and blocks inside Add until released, so a
// test can hold a flush in its publish phase deterministically.
type gateCounter struct {
	prometheus.Counter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGateCounter() *gateCounter {
	return &gateCounter{
		Counter: prometheus.NewCounter(prometheus.CounterOpts{Name: "gate_total", Help: "test"}),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *gateCounter) Add(v float64) {
	g.once.Do(func() { close(g.entered) })
	<-g.release
	g.Counter.Add(v)
}

func newTestCounter(t *testing.T) prometheus.Counter {
	t.Helper()
	return prometheus.NewCounter(prometheus.CounterOpts{Name: "test_sharded_" + t.Name(), Help: "test"})
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	return testutil.ToFloat64(c)
}

func waitForGoroutines(t *testing.T, baseline int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		runtime.Gosched()
		if runtime.NumGoroutine() <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: baseline %d, current %d", baseline, runtime.NumGoroutine())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNewShardedCounter_Defaults(t *testing.T) {
	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{Name: "defaults"})

	assert.Equal(t, "defaults", sc.Name())
	assert.Equal(t, DefaultShardedFlushInterval, sc.FlushInterval())
	assert.Equal(t, nextPowerOfTwo(runtime.GOMAXPROCS(0)), sc.Shards())
	assert.Equal(t, target, sc.target)

	cfg := NewShardedCounter(target, ShardedCounterOptions{Shards: 5, FlushInterval: 5 * time.Millisecond})
	assert.Equal(t, 8, cfg.Shards())
	assert.Equal(t, 5*time.Millisecond, cfg.FlushInterval())
}

func TestShardedCounter_ShardLayoutIsCacheLinePadded(t *testing.T) {
	sc := NewShardedCounter(newTestCounter(t), ShardedCounterOptions{Name: "layout"})

	require.Equal(t, counterCacheLineBytes, int(unsafe.Sizeof(counterShard{})))

	base := uintptr(unsafe.Pointer(&sc.shards[0]))
	for i := range sc.shards {
		addr := uintptr(unsafe.Pointer(&sc.shards[i]))
		assert.Equal(t, uintptr(0), addr%counterCacheLineBytes, "shard %d is not cache line aligned", i)
		assert.Equal(t, uintptr(i)*counterCacheLineBytes, addr-base)
	}
	assert.Zero(t, sc.Shards()&(sc.Shards()-1), "shard count must be a power of two")
}

func TestShardedCounter_SingleThreadedIncrementAndFlush(t *testing.T) {
	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{Name: "single"})

	for i := 0; i < 1000; i++ {
		sc.Inc()
	}
	assert.Equal(t, 1000.0, sc.Pending())
	assert.Equal(t, 0.0, counterValue(t, target), "nothing is published before a flush")

	assert.Equal(t, 1000.0, sc.Flush())
	assert.Equal(t, 1000.0, counterValue(t, target))
	assert.Equal(t, 0.0, sc.Pending())

	assert.Equal(t, 0.0, sc.Flush(), "flushing an empty accumulator publishes nothing")
	assert.Equal(t, 1000.0, counterValue(t, target))

	sc.AddInt(41)
	sc.Add(2.5)
	sc.Add(6.5)
	assert.InDelta(t, 50.0, sc.Pending(), 1e-9)
	sc.Flush()
	assert.InDelta(t, 1050.0, counterValue(t, target), 1e-9)

	stats := sc.Stats()
	assert.Equal(t, "single", stats.Name)
	assert.Equal(t, sc.Shards(), stats.Shards)
	assert.InDelta(t, 1050.0, stats.PublishedTotal, 1e-9)
	assert.Equal(t, uint64(3), stats.Flushes)
	assert.Greater(t, stats.LastFlushUnixNano, int64(0))
}

func TestShardedCounter_ConcurrentAddsAreExactlyCounted(t *testing.T) {
	const (
		workers   = 8
		perWorker = 20000
	)

	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{
		Name:          "concurrent",
		FlushInterval: time.Millisecond,
	})
	require.NoError(t, sc.Start(context.Background()))
	defer func() { require.NoError(t, sc.Stop(context.Background())) }()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				sc.Inc()
			}
		}()
	}
	wg.Wait()

	// Mid-flight invariant: everything added so far is either already published
	// or still pending, never both and never neither.
	mid := sc.Stats()
	assert.LessOrEqual(t, mid.PublishedTotal+mid.Pending, float64(workers*perWorker))

	require.NoError(t, sc.Stop(context.Background()))

	stats := sc.Stats()
	assert.Equal(t, float64(workers*perWorker), counterValue(t, target),
		"published total must be exactly N*M, no loss and no duplication")
	assert.InDelta(t, float64(workers*perWorker), stats.PublishedTotal, 0)
	assert.Equal(t, 0.0, stats.Pending)
	assert.GreaterOrEqual(t, stats.Flushes, uint64(1))
}

func TestShardedCounter_PeriodicFlush(t *testing.T) {
	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{
		Name:          "periodic",
		FlushInterval: time.Millisecond,
	})

	require.NoError(t, sc.Start(context.Background()))
	for i := 0; i < 10; i++ {
		sc.Inc()
		time.Sleep(3 * time.Millisecond)
	}
	require.NoError(t, sc.Stop(context.Background()))

	stats := sc.Stats()
	assert.Greater(t, stats.Flushes, uint64(5), "the ticker must publish repeatedly")
	assert.Equal(t, 10.0, counterValue(t, target))
}

func TestShardedCounter_ConcurrentAddsAcrossAllShards(t *testing.T) {
	const (
		workers   = 16
		perWorker = 5000
		mixed     = 4
	)

	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{
		Name:          "allshards",
		FlushInterval: 500 * time.Microsecond,
	})
	require.NoError(t, sc.Start(context.Background()))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var flushWG sync.WaitGroup
	for g := 0; g < mixed; g++ {
		wg.Add(1)
		go func() { // integral fast path on its own goroutines
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				sc.Inc()
			}
		}()
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() { // fractional CAS path, exercised with the flusher running
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				sc.Add(0.5)
			}
		}()
	}

	flushWG.Add(1)
	go func() {
		defer flushWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				sc.Flush()
			}
		}
	}()

	wg.Wait()
	close(stop)
	flushWG.Wait()
	require.NoError(t, sc.Stop(context.Background()))

	assert.InDelta(t, float64(mixed*perWorker)+0.5*float64(workers*perWorker), counterValue(t, target), 1e-6)
}

// TestShardedCounter_AddDuringFlushIsCountedExactlyOnce pins a flush inside its
// publish phase (after the drain, before target.Add returns) and adds a value
// from another goroutine. The in-flight value must reach the target exactly
// once: it is not part of the drained total and not lost either.
func TestShardedCounter_AddDuringFlushIsCountedExactlyOnce(t *testing.T) {
	gate := newGateCounter()
	sc := NewShardedCounter(gate, ShardedCounterOptions{
		Name:          "concurrent-with-flush",
		FlushInterval: time.Millisecond,
	})

	sc.Inc()
	require.NoError(t, sc.Start(context.Background()))

	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flusher never reached the publish phase")
	}

	// The flusher has already drained, so this Add lands in a shard behind the
	// drained total.
	sc.Inc()
	sc.AddInt(7)

	close(gate.release)
	require.NoError(t, sc.Stop(context.Background()))

	assert.Equal(t, 9.0, counterValue(t, gate.Counter), "value added during the flush must be counted exactly once")
	assert.Equal(t, 0.0, sc.Pending())
}

func TestShardedCounter_AddConcurrentWithRepeatedFlushes(t *testing.T) {
	const rounds = 2000

	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{Name: "rounds"})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			sc.Flush()
		}
	}()

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds/4; i++ {
				sc.Inc()
			}
		}()
	}
	wg.Wait()
	sc.Flush()

	assert.Equal(t, float64(rounds), counterValue(t, target))
}

func TestShardedCounter_StartStopLifecycle(t *testing.T) {
	t.Run("start and stop flushes pending values", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{
			Name:          "lifecycle",
			FlushInterval: time.Hour, // only the shutdown flush can publish
		})

		require.NoError(t, sc.Start(context.Background()))
		sc.Inc()
		sc.Inc()
		assert.Equal(t, 0.0, counterValue(t, target), "no tick has fired yet")

		require.NoError(t, sc.Stop(context.Background()))
		assert.Equal(t, 2.0, counterValue(t, target), "shutdown must flush pending values")
	})

	t.Run("start twice is a no-op", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{
			Name:          "double-start",
			FlushInterval: time.Hour,
		})

		before := runtime.NumGoroutine()
		require.NoError(t, sc.Start(context.Background()))
		sc.Inc()
		require.NoError(t, sc.Start(context.Background()))
		require.NoError(t, sc.Start(context.Background()))

		require.NoError(t, sc.Stop(context.Background()))
		waitForGoroutines(t, before, 2*time.Second)

		assert.Equal(t, 1.0, counterValue(t, target), "a single flusher must publish each value once")
		assert.Equal(t, 0.0, sc.Flush(), "the shutdown flush already published the only pending value")
		assert.Equal(t, 1.0, counterValue(t, target), "the extra flush must not double count")
	})

	t.Run("stop twice is a no-op", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "double-stop", FlushInterval: time.Hour})

		require.NoError(t, sc.Start(context.Background()))
		sc.Inc()
		require.NoError(t, sc.Stop(context.Background()))
		require.NoError(t, sc.Stop(context.Background()))

		assert.Equal(t, 1.0, counterValue(t, target))
	})

	t.Run("stop without start flushes inline", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "never-started"})

		sc.AddInt(11)
		require.NoError(t, sc.Stop(context.Background()))
		assert.Equal(t, 11.0, counterValue(t, target))
		assert.Equal(t, 0.0, sc.Pending())
	})

	t.Run("start after stop is rejected", func(t *testing.T) {
		sc := NewShardedCounter(newTestCounter(t), ShardedCounterOptions{Name: "restart"})
		require.NoError(t, sc.Stop(context.Background()))
		require.ErrorIs(t, sc.Start(context.Background()), ErrShardedCounterStopped)
	})

	t.Run("nil context is accepted", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "nil-ctx", FlushInterval: time.Hour})
		//nolint:staticcheck // exercising the defensive nil guard on purpose
		require.NoError(t, sc.Start(nil))
		sc.Inc()
		//nolint:staticcheck // exercising the defensive nil guard on purpose
		require.NoError(t, sc.Stop(nil))
		assert.Equal(t, 1.0, counterValue(t, target))
	})
}

func TestShardedCounter_ContextCancelStopsFlusher(t *testing.T) {
	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{
		Name:          "ctx-cancel",
		FlushInterval: time.Hour,
	})

	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, sc.Start(ctx))
	sc.AddInt(5)
	cancel()

	// The flusher exits on its own after the final flush. The poll runs on the
	// test goroutine so it does not observe its own bookkeeping goroutine.
	waitForGoroutines(t, before, 5*time.Second)
	assert.Equal(t, 5.0, counterValue(t, target), "cancellation must still flush pending values")

	require.NoError(t, sc.Stop(context.Background()))
}

func TestShardedCounter_StopHonorsContextDeadline(t *testing.T) {
	gate := newGateCounter()
	sc := NewShardedCounter(gate, ShardedCounterOptions{
		Name:          "stop-deadline",
		FlushInterval: time.Millisecond,
	})

	sc.Inc()
	require.NoError(t, sc.Start(context.Background()))
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flusher never reached the publish phase")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := sc.Stop(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "stop must give up when ctx expires")

	close(gate.release)
	// The flusher finishes on its own and still publishes everything.
	require.Eventually(t, func() bool {
		return counterValue(t, gate.Counter) == 1.0
	}, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, 0.0, sc.Pending())
}

func TestShardedCounter_StopMidFlush(t *testing.T) {
	gate := newGateCounter()
	sc := NewShardedCounter(gate, ShardedCounterOptions{
		Name:          "stop-mid-flush",
		FlushInterval: time.Millisecond,
	})

	sc.AddInt(3)
	require.NoError(t, sc.Start(context.Background()))
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("flusher never reached the publish phase")
	}

	// Add while a flush is mid-flight, then stop: Stop must not return before
	// the in-flight flush and the shutdown flush have both landed.
	sc.AddInt(4)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		assert.NoError(t, sc.Stop(context.Background()))
	}()

	select {
	case <-stopped:
		t.Fatal("Stop returned while a flush was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(gate.release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the flush completed")
	}

	assert.Equal(t, 7.0, counterValue(t, gate.Counter))
}

func TestShardedCounter_NoGoroutineLeakAcrossCycles(t *testing.T) {
	// Warm up so lazily started test/runtime goroutines are already running.
	for i := 0; i < 3; i++ {
		sc := NewShardedCounter(newTestCounter(t), ShardedCounterOptions{
			Name:          "leak-probe",
			FlushInterval: time.Millisecond,
		})
		require.NoError(t, sc.Start(context.Background()))
		sc.Inc()
		require.NoError(t, sc.Stop(context.Background()))
	}
	waitForGoroutines(t, runtime.NumGoroutine(), time.Second)
	before := runtime.NumGoroutine()

	const cycles = 25
	for i := 0; i < cycles; i++ {
		sc := NewShardedCounter(newTestCounter(t), ShardedCounterOptions{
			Name:          "leak",
			FlushInterval: 200 * time.Microsecond,
		})
		require.NoError(t, sc.Start(context.Background()))
		sc.AddInt(int64(i))
		require.NoError(t, sc.Stop(context.Background()))
		require.NoError(t, sc.Stop(context.Background()))
	}

	waitForGoroutines(t, before, 5*time.Second)
}

func TestShardedCounter_EdgeCases(t *testing.T) {
	t.Run("zero adds are free and publish nothing", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "zero"})

		sc.Inc()
		assert.Equal(t, 1.0, sc.Pending())
		sc.Add(0)
		sc.AddInt(0)
		sc.Add(-0.0)
		assert.Equal(t, 1.0, sc.Pending(), "zero must not change the accumulator")

		assert.Equal(t, 1.0, sc.Flush())
		assert.Equal(t, 1.0, counterValue(t, target))
	})

	t.Run("stop with no data leaves the target untouched", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "empty"})

		require.NoError(t, sc.Stop(context.Background()))
		assert.Equal(t, 0.0, counterValue(t, target))
		assert.Equal(t, 0.0, sc.Pending())
	})

	t.Run("negative adds are rejected", func(t *testing.T) {
		sc := NewShardedCounter(newTestCounter(t), ShardedCounterOptions{Name: "negative"})

		assert.PanicsWithError(t, ErrShardedCounterDecrease.Error(), func() { sc.Add(-1) })
		assert.PanicsWithError(t, ErrShardedCounterDecrease.Error(), func() { sc.AddInt(-1) })
		assert.Equal(t, 0.0, sc.Pending())
	})

	t.Run("negative values never reach the target", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "negative-publish"})
		sc.AddInt(2)
		sc.Flush()
		assert.Equal(t, 2.0, counterValue(t, target))
	})

	t.Run("nil target panics", func(t *testing.T) {
		assert.PanicsWithError(t, ErrShardedCounterNilTarget.Error(), func() {
			NewShardedCounter(nil, ShardedCounterOptions{})
		})
	})

	t.Run("tiny shard counts still work", func(t *testing.T) {
		target := newTestCounter(t)
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "one-shard", Shards: 1})
		assert.Equal(t, 1, sc.Shards())
		for i := 0; i < 10; i++ {
			sc.Inc()
		}
		sc.Flush()
		assert.Equal(t, 10.0, counterValue(t, target))
	})
}

func TestShardedCounter_LargeValuesStayExact(t *testing.T) {
	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{Name: "large"})

	const workers = 8
	const perWorker = 2000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				sc.AddInt(1 << 20) // 1MiB per event, still exact in float64
			}
		}()
	}
	wg.Wait()
	sc.Flush()

	expected := float64(workers*perWorker) * (1 << 20)
	assert.Equal(t, expected, counterValue(t, target))
}

func TestShardedCounter_FractionalAddsUseTheFLOATAccumulator(t *testing.T) {
	target := newTestCounter(t)
	sc := NewShardedCounter(target, ShardedCounterOptions{Name: "float"})

	const n = 1000
	for i := 0; i < n; i++ {
		sc.Add(0.25)
	}
	assert.Equal(t, float64(n)*0.25, sc.Pending())
	sc.Flush()
	assert.Equal(t, float64(n)*0.25, counterValue(t, target))

	// The integral accumulator must have been left alone.
	assert.Equal(t, 0.0, math.Float64frombits(sc.shards[0].intAcc.Load()))
}

func TestNewRegisteredShardedCounter_DoesNotDoubleRegister(t *testing.T) {
	reg := prometheus.NewRegistry()
	counterOpts := prometheus.CounterOpts{
		Name: "longbow_test_sharded_registered_total",
		Help: "sharded counter registration test",
	}

	first, err := NewRegisteredShardedCounter(reg, counterOpts, ShardedCounterOptions{
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)
	second, err := NewRegisteredShardedCounter(reg, counterOpts, ShardedCounterOptions{
		FlushInterval: time.Hour,
	})
	require.NoError(t, err)

	assert.Same(t, first.target, second.target, "the already registered collector must be reused")
	assert.Equal(t, counterOpts.Name, first.Name())

	require.NoError(t, first.Start(context.Background()))
	require.NoError(t, first.Start(context.Background()))
	first.Inc()
	require.NoError(t, first.Stop(context.Background()))
	require.NoError(t, second.Stop(context.Background()))

	assert.Equal(t, 1.0, counterValue(t, first.target))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1, "the collector must be registered exactly once")
	assert.Equal(t, counterOpts.Name, families[0].GetName())
}

func TestNewRegisteredShardedCounter_NilRegisterer(t *testing.T) {
	_, err := NewRegisteredShardedCounter(nil, prometheus.CounterOpts{Name: "x_total", Help: "x"}, ShardedCounterOptions{})
	require.Error(t, err)
}

func TestShardedCounterVec_LabelResolution(t *testing.T) {
	vec := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "longbow_test_sharded_vec_total", Help: "vec test"},
		[]string{"dataset", "search_type"},
	)
	//nolint:staticcheck // the registration counter in the helper is intentional
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(vec))

	var resolveCalls atomic.Int64
	scv := NewShardedCounterVec(vec, func(labelValues []string) prometheus.Counter {
		resolveCalls.Add(1)
		return vec.WithLabelValues(labelValues...)
	}, ShardedCounterOptions{
		Name:          "vec",
		FlushInterval: time.Hour,
	})
	assert.Same(t, vec, scv.Vec())

	dense := scv.For("ds1", "dense")
	require.NotNil(t, dense)
	sparse := scv.For("ds1", "sparse")
	require.NotNil(t, sparse)
	assert.NotSame(t, dense, sparse)

	// Memoized: repeat lookups return the same accumulator and do not resolve
	// the labels again.
	assert.Same(t, dense, scv.For("ds1", "dense"))
	assert.Equal(t, int64(2), resolveCalls.Load())
	assert.Equal(t, []string{"ds1\xffdense", "ds1\xffsparse"}, scv.Labels())

	dense.Inc()
	dense.AddInt(4)
	sparse.Add(0.5)
	assert.Equal(t, 0.0, counterValue(t, vec.WithLabelValues("ds1", "dense")))

	require.NoError(t, scv.Start(context.Background()))
	require.NoError(t, scv.Start(context.Background()))

	// A label combination created after Start is picked up by the same flusher.
	hybrid := scv.For("ds2", "hybrid")
	hybrid.AddInt(2)
	assert.Equal(t, int64(3), resolveCalls.Load())

	require.NoError(t, scv.Stop(context.Background()))
	require.NoError(t, scv.Stop(context.Background()))

	assert.Equal(t, 5.0, counterValue(t, vec.WithLabelValues("ds1", "dense")))
	assert.Equal(t, 0.5, counterValue(t, vec.WithLabelValues("ds1", "sparse")))
	assert.Equal(t, 2.0, counterValue(t, vec.WithLabelValues("ds2", "hybrid")))
	assert.Equal(t, 0.0, scv.Flush(), "nothing is left pending")
}

func TestShardedCounterVec_ConcurrentForAndFlush(t *testing.T) {
	vec := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "longbow_test_sharded_vec_concurrent_total", Help: "vec test"},
		[]string{"dataset"},
	)
	scv := NewShardedCounterVec(vec, func(labelValues []string) prometheus.Counter {
		return vec.WithLabelValues(labelValues...)
	}, ShardedCounterOptions{Name: "vec-concurrent", FlushInterval: time.Millisecond})

	require.NoError(t, scv.Start(context.Background()))

	const datasets = 8
	const perWorker = 5000
	var wg sync.WaitGroup
	for d := 0; d < datasets; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			name := "ds"
			label := []string{name + string(rune('a'+d))}
			for i := 0; i < perWorker; i++ {
				scv.For(label...).Inc()
				if i%1000 == 0 {
					scv.Flush()
				}
			}
		}(d)
	}
	wg.Wait()
	require.NoError(t, scv.Stop(context.Background()))

	for d := 0; d < datasets; d++ {
		label := "ds" + string(rune('a'+d))
		assert.Equal(t, float64(perWorker), counterValue(t, vec.WithLabelValues(label)))
	}
}

func TestShardedCounterVec_NilResolution(t *testing.T) {
	scv := NewShardedCounterVec(
		prometheus.NewCounterVec(prometheus.CounterOpts{Name: "longbow_test_sharded_vec_nil_total", Help: "x"}, []string{"k"}),
		func([]string) prometheus.Counter { return nil },
		ShardedCounterOptions{Name: "vec-nil"},
	)
	assert.Nil(t, scv.For("missing"))
	require.NoError(t, scv.Stop(context.Background()))
}

func TestShardedCounterVec_InvalidConstruction(t *testing.T) {
	assert.PanicsWithError(t, ErrShardedCounterNilTarget.Error(), func() {
		NewShardedCounterVec(nil, func([]string) prometheus.Counter { return nil }, ShardedCounterOptions{})
	})
	assert.Panics(t, func() {
		NewShardedCounterVec(
			prometheus.NewCounterVec(prometheus.CounterOpts{Name: "longbow_test_vec_noresolver_total", Help: "x"}, []string{"k"}),
			nil,
			ShardedCounterOptions{},
		)
	})
}

func TestShardedCounter_ShardsAreIndependent(t *testing.T) {
	sc := NewShardedCounter(newTestCounter(t), ShardedCounterOptions{Name: "independent", Shards: 8})

	p := runtimeProcPin()
	sc.Inc()
	sc.AddInt(2)
	runtimeProcUnpin()

	assert.Equal(t, 3.0, float64(sc.shards[p&int(sc.mask)].intAcc.Load()), "the calling P owns the value it added")

	var total float64
	for i := range sc.shards {
		total += float64(sc.shards[i].intAcc.Load())
	}
	assert.Equal(t, 3.0, total)
}
