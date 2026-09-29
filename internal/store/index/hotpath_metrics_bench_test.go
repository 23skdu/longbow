package index

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Per-increment costs measured by BenchmarkHotpathMetricIncrementParallel at 16
// goroutines on the development machine, used to turn the increment count of a
// query into nanoseconds of query latency. They are only a scale factor for the
// report; the benchmark itself prints the increment count it observed.
const (
	hotpathInlineCost  = 135.6 // ns, WithLabelValues(...).Inc() under contention
	hotpathHoistedCost = 27.4  // ns, resolved child, shared atomic
	hotpathShardedCost = 1.3   // ns, resolved child, per-P accumulator
)

// counterValue reads a collector through its exported series.
func counterValue(c prometheus.Counter) float64 { return testutil.ToFloat64(c) }

// metricSink keeps the accumulators observable so the loop is not elided.
var metricSink atomic.Uint64

// hotpathIncrementCase is one hot path counter expressed in the three forms the
// conversion went through:
//
//   - inline: what the query path did before, resolving the child of the counter
//     vector on every candidate and adding to the shared Prometheus atomic;
//   - hoisted: the child resolved once, still adding to the shared atomic, the
//     intermediate step the conversion had to pass through;
//   - hot: the child resolved once and adding to the per-P accumulator, which is
//     what the query path does now.
type hotpathIncrementCase struct {
	name    string
	vec     *prometheus.CounterVec
	plain   prometheus.Counter
	labels  []string
	twin    *metrics.ShardedCounterVec
	sharded *metrics.ShardedCounter
}

func hotpathIncrementCases() []hotpathIncrementCase {
	return []hotpathIncrementCase{
		{
			// Per candidate: the branch prediction counters of the parallel
			// result processing loop.
			name:    "per_candidate/branch_prediction",
			vec:     metrics.HnswBranchPredictionTotal,
			labels:  []string{"location_found"},
			twin:    metrics.HnswBranchPredictionSharded,
			sharded: hotpathBranchLocationFound,
		},
		{
			// Per candidate: the nodes skipped by a metadata predicate, with
			// the dataset label the traversal uses.
			name:   "per_candidate/nodes_skipped",
			vec:    metrics.HNSWNodesSkippedTotal,
			labels: []string{"bench_ds"},
			twin:   metrics.HNSWNodesSkippedSharded,
		},
		{
			// Per query: the result pool Get.
			name:   "per_query/result_pool_get",
			vec:    metrics.SearchResultPoolGetTotal,
			labels: []string{"128"},
			twin:   metrics.SearchResultPoolGetSharded,
		},
		{
			// Per query: the HNSW search context pool Get, a counter with no
			// label set at all.
			name:    "per_query/search_pool_get",
			plain:   metrics.HNSWSearchPoolGetTotal,
			sharded: metrics.HNSWSearchPoolGetSharded,
		},
	}
}

// benchmarkHotpathIncrement runs the three forms over the same accumulator. The
// hoisted handles are resolved before the timer starts, exactly as the query
// path resolves them once per index.
func benchmarkHotpathIncrement(b *testing.B, parallel bool) {
	for _, tc := range hotpathIncrementCases() {
		var inline, hoisted func()
		switch {
		case tc.vec != nil:
			labels := tc.labels
			hoistedChild := tc.vec.WithLabelValues(labels...)
			inline = func() { tc.vec.WithLabelValues(labels...).Inc() }
			hoisted = func() { hoistedChild.Inc() }
			tc.sharded = tc.twin.For(labels...)
		default:
			// A counter without a label set has nothing to hoist: the inline
			// form already is the hoisted form.
			hoisted = func() { tc.plain.Inc() }
			inline = hoisted
		}
		shardedChild := tc.sharded
		if tc.vec != nil {
			shardedChild = tc.twin.For(tc.labels...)
		}

		forms := []struct {
			name string
			fn   func()
		}{
			{name: "inline", fn: inline},
			{name: "hoisted", fn: hoisted},
			{name: "hoisted_sharded", fn: shardedChild.Inc},
		}
		for _, form := range forms {
			b.Run(tc.name+"/"+form.name, func(b *testing.B) {
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							form.fn()
						}
					})
				} else {
					for b.Loop() {
						form.fn()
					}
				}
			})
		}
	}

	metricSink.Add(uint64(metrics.HotpathPending()))
	metrics.FlushHotpathCounters()
}

// BenchmarkHotpathMetricIncrement is the serial form: the instruction cost of
// one metric increment, which is what a query pays on an idle core.
func BenchmarkHotpathMetricIncrement(b *testing.B) { benchmarkHotpathIncrement(b, false) }

// BenchmarkHotpathMetricIncrementParallel is the same three forms driven by
// GOMAXPROCS goroutines at once, which is what a loaded server pays. Run it with
// -cpu 1,4,16 to walk the contention curve; the shared Prometheus atomic of the
// inline and hoisted forms is what degrades, the per-P accumulator of the sharded
// form does not.
func BenchmarkHotpathMetricIncrementParallel(b *testing.B) { benchmarkHotpathIncrement(b, true) }

// hotpathWatchedCounters are the counters the conversion touched, each read
// through the collector the dashboard queries.
var hotpathWatchedCounters = []func() float64{
	func() float64 { return counterValue(metrics.HNSWSearchPoolGetTotal) },
	func() float64 { return counterValue(metrics.HNSWSearchPoolPutTotal) },
	func() float64 { return counterValue(metrics.HnswContextCheckTotal) },
	func() float64 { return counterValue(metrics.PrefetchOperationsTotal) },
	func() float64 { return counterValue(metrics.CacheBlockedTraversalChunksTotal) },
	func() float64 { return counterValue(metrics.HNSWNodesSkippedTotal.WithLabelValues("hotpath_inc_ds")) },
	func() float64 {
		return counterValue(metrics.HNSWPreFilteredSearchesTotal.WithLabelValues("hotpath_inc_ds"))
	},
	func() float64 {
		return counterValue(metrics.HnswBranchPredictionTotal.WithLabelValues("location_found"))
	},
	func() float64 {
		return counterValue(metrics.HnswBranchPredictionTotal.WithLabelValues("location_miss"))
	},
	func() float64 { return counterValue(metrics.HnswBranchPredictionTotal.WithLabelValues("filter_match")) },
	func() float64 { return counterValue(metrics.HnswBranchPredictionTotal.WithLabelValues("filter_miss")) },
}

func hotpathCounterSum() float64 {
	metrics.FlushHotpathCounters()
	var total float64
	for _, read := range hotpathWatchedCounters {
		total += read()
	}
	return total
}

// hotpathSearchShape is one filter configuration of the end-to-end search.
type hotpathSearchShape struct {
	name   string
	filter *roaring.Bitmap
}

func hotpathSearchShapes() []hotpathSearchShape {
	sparse := roaring.New()
	sparse.Add(0)
	sparse.Add(5_000)
	sparse.Add(9_999)
	return []hotpathSearchShape{
		{name: "sparse_filter", filter: sparse},
		{name: "selective_1pct", filter: selectiveFilter(10_000, 1, 4)},
	}
}

// BenchmarkHotpathSearch_MetricIncrementsPerQuery runs the filtered search of
// BenchmarkHNSW_Filtered_SparseFilter and reports how much the converted call
// sites add to the exported counters for one query. Every converted call site is
// an Inc, so the delta is also the number of atomic read-modify-writes the query
// performs on the hot path; multiplying it by the isolated per-increment cost of
// BenchmarkHotpathMetricParallel is what turns "the end to end benchmark cannot
// resolve the change" into a number. The query is dominated by
// longbow_cache_blocked_traversal_chunks_total, one increment per 64 candidate
// vectors, which is the reason that counter is on this list.
func BenchmarkHotpathSearch_MetricIncrementsPerQuery(b *testing.B) {
	bi := buildFilteredBenchIndex(b, 10_000, 64, 1)

	for _, shape := range hotpathSearchShapes() {
		b.Run(shape.name, func(b *testing.B) {
			// One untimed pass to resolve the label handles and warm the pools.
			if _, err := bi.idx.SearchVectorsWithBitmap(context.Background(), bi.query, 10, shape.filter, nil); err != nil {
				b.Fatal(err)
			}

			const measured = 20
			before := hotpathCounterSum()
			for i := 0; i < measured; i++ {
				if _, err := bi.idx.SearchVectorsWithBitmap(context.Background(), bi.query, 10, shape.filter, nil); err != nil {
					b.Fatal(err)
				}
			}
			increments := (hotpathCounterSum() - before) / measured

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := bi.idx.SearchVectorsWithBitmap(context.Background(), bi.query, 10, shape.filter, nil); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(increments, "hotpath-increments/search")
			b.ReportMetric(increments*hotpathInlineCost, "inline-ns/search")
			b.ReportMetric(increments*hotpathHoistedCost, "hoisted-ns/search")
			b.ReportMetric(increments*hotpathShardedCost, "hoisted-sharded-ns/search")
		})
	}
}

// BenchmarkHotpathSearch_Parallel is the end-to-end query under concurrency,
// which is the regime the conversion targets: the pre-conversion call sites all
// add to one shared Prometheus atomic per family, so N query goroutines bounce
// that cache line. Run it with -cpu 1,4,16.
func BenchmarkHotpathSearch_Parallel(b *testing.B) {
	bi := buildFilteredBenchIndex(b, 10_000, 64, 1)
	filter := selectiveFilter(10_000, 1, 4)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := bi.idx.SearchVectorsWithBitmap(context.Background(), bi.query, 10, filter, nil); err != nil {
				continue
			}
		}
	})
}
