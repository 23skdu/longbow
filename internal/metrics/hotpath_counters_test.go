package metrics

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hotpathFamily describes the exported shape of one hot path counter: the
// family name a dashboard queries, the label names of its series, and the
// collector the sharded twin must publish into.
type hotpathFamily struct {
	name   string
	labels []string
	sample string
	plain  prometheus.Counter
	vec    *prometheus.CounterVec
	twin   *ShardedCounterVec
}

func hotpathFamilies() []hotpathFamily {
	return []hotpathFamily{
		{name: "longbow_hnsw_search_pool_get_total", plain: HNSWSearchPoolGetTotal},
		{name: "longbow_hnsw_search_pool_put_total", plain: HNSWSearchPoolPutTotal},
		{name: "longbow_hnsw_context_check_total", plain: HnswContextCheckTotal},
		{name: "longbow_prefetch_operations_total", plain: PrefetchOperationsTotal},
		{name: "longbow_cache_blocked_traversal_chunks_total", plain: CacheBlockedTraversalChunksTotal},
		{
			name: "longbow_hnsw_branch_prediction_total", labels: []string{"branch_type"}, sample: "guard",
			vec: HnswBranchPredictionTotal, twin: HnswBranchPredictionSharded,
		},
		{
			name: "longbow_hnsw_nodes_skipped_total", labels: []string{"dataset"}, sample: "guard",
			vec: HNSWNodesSkippedTotal, twin: HNSWNodesSkippedSharded,
		},
		{
			name: "longbow_hnsw_search_early_exits_total", labels: []string{"reason"}, sample: "guard",
			vec: HnswSearchEarlyExitsTotal, twin: HnswSearchEarlyExitsSharded,
		},
		{
			name: "longbow_hnsw_early_termination_total", labels: []string{"reason"}, sample: "guard",
			vec: HNSWEarlyTerminationTotal, twin: HNSWEarlyTerminationSharded,
		},
		{
			name: "longbow_hnsw_prefiltered_searches_total", labels: []string{"dataset"}, sample: "guard",
			vec: HNSWPreFilteredSearchesTotal, twin: HNSWPreFilteredSearchesSharded,
		},
		{
			name: "longbow_filter_early_exit_total", labels: []string{"dataset"}, sample: "guard",
			vec: HNSWFilterEarlyExitTotal, twin: HNSWFilterEarlyExitSharded,
		},
		{
			name: "longbow_search_result_pool_get_total", labels: []string{"capacity"}, sample: "guard",
			vec: SearchResultPoolGetTotal, twin: SearchResultPoolGetSharded,
		},
		{
			name: "longbow_search_result_pool_hits_total", labels: []string{"capacity"}, sample: "guard",
			vec: SearchResultPoolHitsTotal, twin: SearchResultPoolHitsSharded,
		},
		{
			name: "longbow_search_result_pool_put_total", labels: []string{"capacity"}, sample: "guard",
			vec: SearchResultPoolPutTotal, twin: SearchResultPoolPutSharded,
		},
	}
}

// gatherFamily returns the family with name from the default registry, or nil
// when the collector has no child yet.
func gatherFamily(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	return nil
}

// TestHotpathCounters_ExportedShapeIsUnchanged pins the contract a sharded twin
// must not break: it publishes into the very collector the inline
// WithLabelValues call used, so the family a dashboard queries, its label names
// and its series are the ones that existed before the hot path was converted.
// A twin that registered a second collector, or that resolved a child of a
// different vector, fails here.
func TestHotpathCounters_ExportedShapeIsUnchanged(t *testing.T) {
	for _, hf := range hotpathFamilies() {
		t.Run(hf.name, func(t *testing.T) {
			// Materialise one child so the family is exported, exactly as the
			// first inline call site would have done.
			if hf.vec != nil {
				require.NotNil(t, hf.vec.WithLabelValues(hf.sample))
				require.Same(t, hf.vec, hf.twin.Vec(), "the twin must publish into the registered vector")
			}

			family := gatherFamily(t, hf.name)
			require.NotNil(t, family, "family must stay exported")
			assert.Equal(t, dto.MetricType_COUNTER, family.GetType())

			// Every series of the family keeps the original label names.
			require.NotEmpty(t, family.GetMetric())
			for _, m := range family.GetMetric() {
				var names []string
				for _, lp := range m.GetLabel() {
					names = append(names, lp.GetName())
				}
				assert.Equal(t, hf.labels, names)
			}
		})
	}
}

// TestHotpathCounters_TwinSharesTheSeriesWithTheInlineCall is the functional
// half of the dashboard contract: a value written through the sharded twin and
// a value written inline through WithLabelValues end up in the same series, so
// an operator watching either call site sees the same total.
func TestHotpathCounters_TwinSharesTheSeriesWithTheInlineCall(t *testing.T) {
	const label = "hotpath_shared_series_guard"

	for _, hf := range hotpathFamilies() {
		if hf.vec == nil {
			continue
		}
		t.Run(hf.name, func(t *testing.T) {
			inline := hf.vec.WithLabelValues(label)
			before := testutil.ToFloat64(inline)
			sharded := hf.twin.For(label)
			require.NotNil(t, sharded)

			inline.Inc()
			sharded.AddInt(2)

			FlushHotpathCounters()

			assert.Equal(t, before+3, testutil.ToFloat64(hf.vec.WithLabelValues(label)),
				"twin and inline increments must land in the same series")
			assert.Same(t, hf.twin.For(label), sharded, "the label lookup must be memoized")
		})
	}

	// The same holds for a counter without a label set.
	before := testutil.ToFloat64(HnswContextCheckTotal)
	HnswContextCheckTotal.Inc()
	HnswContextCheckSharded.Inc()
	FlushHotpathCounters()
	assert.Equal(t, before+2, testutil.ToFloat64(HnswContextCheckTotal))
}

// TestHotpathCounters_FlusherEventuallyPublishes checks the asynchronous
// contract: a hot path increment reaches the registry without the caller doing
// anything, and it arrives within a bounded time.
func TestHotpathCounters_FlusherEventuallyPublishes(t *testing.T) {
	const (
		label        = "hotpath_eventual_guard"
		goroutines   = 8
		perGoroutine = 500
	)

	target := HNSWNodesSkippedTotal.WithLabelValues(label)
	before := testutil.ToFloat64(target)

	StartHotpathFlushers()
	require.True(t, HotpathFlushersRunning())

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			child := HNSWNodesSkippedSharded.For(label)
			for i := 0; i < perGoroutine; i++ {
				child.Inc()
			}
		}()
	}
	wg.Wait()

	want := before + float64(goroutines*perGoroutine)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(target) == want
	}, 5*time.Second, 5*time.Millisecond, "the background flusher must publish within a bounded time")

	require.NoError(t, StopHotpathFlushers(context.Background()))
	assert.Equal(t, want, testutil.ToFloat64(target), "no value may be lost or double counted")
	assert.Zero(t, HotpathPending(), "the shutdown flush must leave nothing pending")
	assert.False(t, HotpathFlushersRunning())
}

// TestHotpathCounters_ConcurrentIncrementsAreExact runs producers against the
// live flusher, so every shard is drained while it is being written to, and
// pins the published total to the exact number of increments.
func TestHotpathCounters_ConcurrentIncrementsAreExact(t *testing.T) {
	const (
		label        = "hotpath_exactness_guard"
		goroutines   = 16
		perGoroutine = 4000
	)

	target := HnswBranchPredictionTotal.WithLabelValues("location_found")
	before := testutil.ToFloat64(target)
	want := before + float64(goroutines*perGoroutine)

	StartHotpathFlushers()
	defer func() { require.NoError(t, StopHotpathFlushers(context.Background())) }()

	stop := make(chan struct{})
	flusherDone := make(chan struct{})
	go func() {
		defer close(flusherDone)
		for {
			select {
			case <-stop:
				return
			default:
				FlushHotpathCounters()
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			child := HnswBranchPredictionSharded.For("location_found")
			for i := 0; i < perGoroutine; i++ {
				child.Inc()
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-flusherDone

	// Everything is either published or still pending, never both, never
	// neither; the final flush must therefore make the total exact.
	assert.LessOrEqual(t, testutil.ToFloat64(target)+HotpathPending(), want)
	FlushHotpathCounters()
	assert.Equal(t, want, testutil.ToFloat64(target), "published total must be exactly the number of increments")
}

// TestHotpathCounters_FlushIsIdempotent guards the double flush the lifecycle
// can perform when a shutdown races with the ticker.
func TestHotpathCounters_FlushIsIdempotent(t *testing.T) {
	const label = "hotpath_idempotent_guard"

	before := testutil.ToFloat64(SearchResultPoolGetTotal.WithLabelValues(label))
	child := SearchResultPoolGetSharded.For(label)
	child.Inc()
	child.Inc()

	assert.Equal(t, 2.0, FlushHotpathCounters())
	assert.Zero(t, FlushHotpathCounters(), "a flush with nothing pending must publish nothing")
	assert.Equal(t, before+2, testutil.ToFloat64(SearchResultPoolGetTotal.WithLabelValues(label)))
}

// TestHotpathFlushers_LifecycleIsReferenceCounted covers the store lifecycle
// contract: several stores share one flusher and the last shutdown stops it.
func TestHotpathFlushers_LifecycleIsReferenceCounted(t *testing.T) {
	StartHotpathFlushers()
	StartHotpathFlushers()
	require.True(t, HotpathFlushersRunning(), "the first acquisition starts the flusher")

	require.NoError(t, StopHotpathFlushers(context.Background()))
	assert.True(t, HotpathFlushersRunning(), "the flusher outlives the first release")

	require.NoError(t, StopHotpathFlushers(context.Background()))
	assert.False(t, HotpathFlushersRunning(), "the last release stops the flusher")

	// A release without a holder is a no-op that still publishes, so a store
	// that never acquired cannot strand a pending value.
	HnswContextCheckSharded.Inc()
	require.NoError(t, StopHotpathFlushers(context.Background()))
	assert.False(t, HotpathFlushersRunning())
}

// TestHotpathFlushers_NoGoroutineLeakAcrossLifecycle runs the store lifecycle
// many times over and pins the goroutine count, which is what a flusher started
// per store instance would break.
func TestHotpathFlushers_NoGoroutineLeakAcrossLifecycle(t *testing.T) {
	// Warm up so lazily started runtime goroutines are already counted.
	for i := 0; i < 3; i++ {
		StartHotpathFlushers()
		HnswContextCheckSharded.Inc()
		require.NoError(t, StopHotpathFlushers(context.Background()))
	}
	waitForGoroutines(t, runtime.NumGoroutine(), time.Second)
	before := runtime.NumGoroutine()

	const cycles = 25
	for i := 0; i < cycles; i++ {
		StartHotpathFlushers()
		StartHotpathFlushers()
		HnswContextCheckSharded.Inc()
		require.NoError(t, StopHotpathFlushers(context.Background()))
		require.NoError(t, StopHotpathFlushers(context.Background()))
		require.NoError(t, StopHotpathFlushers(context.Background())) // unbalanced release stays a no-op
	}

	waitForGoroutines(t, before, 5*time.Second)
}

// TestHotpathFlushers_StopHonorsContextDeadline checks that a shutdown that runs
// out of time does not block forever on the flusher.
func TestHotpathFlushers_StopHonorsContextDeadline(t *testing.T) {
	StartHotpathFlushers()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // let the deadline expire

	_ = StopHotpathFlushers(ctx)

	// The flusher exits on its own and the last release already ran a flush, so
	// a follow up release is a no-op.
	assert.Eventually(t, func() bool { return !HotpathFlushersRunning() }, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, StopHotpathFlushers(context.Background()))
}

// TestHotpathCounters_PlainTwinsWrapTheRegisteredCounter pins the identity of
// the unlabelled twins: they must add into the registered collector, which is
// the only way the exported value stays exact when a counter has two writers.
func TestHotpathCounters_PlainTwinsWrapTheRegisteredCounter(t *testing.T) {
	plain := map[prometheus.Counter]*ShardedCounter{
		HNSWSearchPoolGetTotal:           HNSWSearchPoolGetSharded,
		HNSWSearchPoolPutTotal:           HNSWSearchPoolPutSharded,
		HnswContextCheckTotal:            HnswContextCheckSharded,
		PrefetchOperationsTotal:          PrefetchOperationsSharded,
		CacheBlockedTraversalChunksTotal: CacheBlockedTraversalChunksSharded,
	}
	for target, twin := range plain {
		assert.Same(t, target, twin.target, "twin %s must publish into the registered counter", twin.Name())
		assert.Equal(t, hotpathFlushInterval, twin.FlushInterval())
	}
}
