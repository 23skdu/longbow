package store

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Mock metric for benchmarking to avoid polluting global state too much,
// although we can also just use a vector.
var (
	benchVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bench_metric_total",
			Help: "Benchmark metric",
		},
		[]string{"label1", "label2"},
	)
)

func init() {
	// Register isn't strictly necessary for the vector to work,
	// but good for completeness if we were running a real registry.
}

func BenchmarkMetrics_WithLabelValues(b *testing.B) {
	// Simulate the "Hot Path" behaviour: calling WithLabelValues every time
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchVec.WithLabelValues("val1", "val2").Inc()
	}
}

func BenchmarkMetrics_CachedHandle(b *testing.B) {
	// Simulate the optimized behaviour: looking up once, then reusing
	handle := benchVec.WithLabelValues("val1", "val2")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handle.Inc()
	}
}

func BenchmarkMetrics_Curried(b *testing.B) {
	// Simulate currying
	curried := benchVec.MustCurryWith(prometheus.Labels{"label1": "val1"})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		curried.WithLabelValues("val2").Inc()
	}
}
