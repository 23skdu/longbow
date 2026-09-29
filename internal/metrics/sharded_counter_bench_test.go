package metrics

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// benchParallel runs op across exactly goroutines goroutines, splitting b.N
// between them, and reports allocations of the whole run.
func benchParallel(b *testing.B, goroutines int, op func()) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	base := b.N / goroutines
	extra := b.N % goroutines
	for g := 0; g < goroutines; g++ {
		n := base
		if g < extra {
			n++
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				op()
			}
		}(n)
	}
	wg.Wait()
}

func newBenchSharded(b *testing.B, name string) *ShardedCounter {
	b.Helper()
	target := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: "benchmark"})
	return NewShardedCounter(target, ShardedCounterOptions{Name: name})
}

func BenchmarkShardedCounter_Add_1Goroutine(b *testing.B) {
	sc := newBenchSharded(b, "bench_sharded_add_1")
	benchParallel(b, 1, func() { sc.Add(1) })
}

func BenchmarkShardedCounter_Add_4Goroutines(b *testing.B) {
	sc := newBenchSharded(b, "bench_sharded_add_4")
	benchParallel(b, 4, func() { sc.Add(1) })
}

func BenchmarkShardedCounter_Add_16Goroutines(b *testing.B) {
	sc := newBenchSharded(b, "bench_sharded_add_16")
	benchParallel(b, 16, func() { sc.Add(1) })
}

func BenchmarkShardedCounter_Inc_1Goroutine(b *testing.B) {
	sc := newBenchSharded(b, "bench_sharded_inc_1")
	benchParallel(b, 1, func() { sc.Inc() })
}

func BenchmarkShardedCounter_Inc_4Goroutines(b *testing.B) {
	sc := newBenchSharded(b, "bench_sharded_inc_4")
	benchParallel(b, 4, func() { sc.Inc() })
}

func BenchmarkShardedCounter_Inc_16Goroutines(b *testing.B) {
	sc := newBenchSharded(b, "bench_sharded_inc_16")
	benchParallel(b, 16, func() { sc.Inc() })
}

func BenchmarkPrometheusCounter_Inc_1Goroutine(b *testing.B) {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "bench_prom_inc_1", Help: "benchmark"})
	benchParallel(b, 1, func() { c.Inc() })
}

func BenchmarkPrometheusCounter_Inc_4Goroutines(b *testing.B) {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "bench_prom_inc_4", Help: "benchmark"})
	benchParallel(b, 4, func() { c.Inc() })
}

func BenchmarkPrometheusCounter_Inc_16Goroutines(b *testing.B) {
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "bench_prom_inc_16", Help: "benchmark"})
	benchParallel(b, 16, func() { c.Inc() })
}

// BenchmarkCounterVec_LabelLookup_16Goroutines measures the second half of the
// roadmap finding: the label hashing (prometheus.hashAdd) that a per-query
// WithLabelValues call pays before it ever reaches the shared counter.
func BenchmarkCounterVec_LabelLookup_16Goroutines(b *testing.B) {
	vec := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "bench_prom_vec_total", Help: "benchmark"},
		[]string{"dataset", "search_type"},
	)
	benchParallel(b, 16, func() { vec.WithLabelValues("ds", "dense").Inc() })
}

// BenchmarkCounterVec_CachedHandle_16Goroutines is the pre-resolved handle
// baseline the sharded accumulator builds on.
func BenchmarkCounterVec_CachedHandle_16Goroutines(b *testing.B) {
	vec := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "bench_prom_vec_cached_total", Help: "benchmark"},
		[]string{"dataset", "search_type"},
	)
	handle := vec.WithLabelValues("ds", "dense")
	benchParallel(b, 16, func() { handle.Inc() })
}

func BenchmarkShardedCounter_Flush(b *testing.B) {
	b.Run("WithPending", func(b *testing.B) {
		sc := newBenchSharded(b, "bench_sharded_flush_pending")
		b.ReportAllocs()
		for b.Loop() {
			// Keep one value in flight so every cycle performs a real publish.
			sc.shards[0].intAcc.Store(1)
			sc.Flush()
		}
		b.ReportMetric(float64(sc.Stats().PublishedTotal)/float64(b.N), "published/op")
	})

	b.Run("NoPending", func(b *testing.B) {
		sc := newBenchSharded(b, "bench_sharded_flush_empty")
		b.ReportAllocs()
		for b.Loop() {
			sc.Flush()
		}
	})
}

func BenchmarkShardedCounterVec_For_16Goroutines(b *testing.B) {
	vec := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "bench_sharded_vec_for_total", Help: "benchmark"},
		[]string{"dataset"},
	)
	scv := NewShardedCounterVec(vec, func(labelValues []string) prometheus.Counter {
		return vec.WithLabelValues(labelValues...)
	}, ShardedCounterOptions{Name: "bench_vec_for"})
	// Hoisted once, the way a hot path would keep its handle.
	handle := scv.For("ds")
	if handle == nil {
		b.Fatal("sharded counter vec resolution failed")
	}

	benchParallel(b, 16, func() { handle.Inc() })
}

func BenchmarkShardedCounter_Lifecycle(b *testing.B) {
	target := prometheus.NewCounter(prometheus.CounterOpts{Name: "bench_sharded_lifecycle_total", Help: "benchmark"})
	ctx := context.Background()

	// A stopped counter is terminal, so every cycle builds a fresh one. A one
	// hour interval keeps the ticker from firing, which makes the measurement
	// the start/stop plumbing plus the shutdown flush.
	b.ReportAllocs()
	for b.Loop() {
		sc := NewShardedCounter(target, ShardedCounterOptions{Name: "bench_lifecycle", FlushInterval: time.Hour})
		if err := sc.Start(ctx); err != nil {
			b.Fatal(err)
		}
		sc.Inc()
		if err := sc.Stop(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(testutil.ToFloat64(target))/float64(b.N), "published/op")
}
