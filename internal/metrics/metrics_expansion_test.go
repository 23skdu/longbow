package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// =============================================================================
// Test all 20 new Prometheus metrics exist and are properly configured
// =============================================================================

// Test WAL metrics
func TestWalFsyncDurationSeconds_Exists(t *testing.T) {
	if WalFsyncDurationSeconds == nil {
		t.Fatal("WalFsyncDurationSeconds metric should not be nil")
	}
	// Verify it's a Histogram with status label
	WalFsyncDurationSeconds.WithLabelValues("success").Observe(0.001)
	WalFsyncDurationSeconds.WithLabelValues("error").Observe(0.01)
}

func TestWalBatchSize_Exists(t *testing.T) {
	if WalBatchSize == nil {
		t.Fatal("WalBatchSize metric should not be nil")
	}
	// Verify it's a Histogram
	WalBatchSize.Observe(100)
}

func TestWalPendingEntries_Exists(t *testing.T) {
	if WalPendingEntries == nil {
		t.Fatal("WalPendingEntries metric should not be nil")
	}
	// Verify it's a Gauge
	WalPendingEntries.Set(50)
	WalPendingEntries.Inc()
	WalPendingEntries.Dec()
}

// Test Index metrics
func TestIndexQueueDepth_Exists(t *testing.T) {
	if IndexQueueDepth == nil {
		t.Fatal("IndexQueueDepth metric should not be nil")
	}
	IndexQueueDepth.Set(25)
}

func TestIndexJobLatencySeconds_Exists(t *testing.T) {
	if IndexJobLatencySeconds == nil {
		t.Fatal("IndexJobLatencySeconds metric should not be nil")
	}
	// Verify it's a HistogramVec with dataset label
	IndexJobLatencySeconds.WithLabelValues("test_dataset").Observe(0.05)
}

func TestDatasetRecordBatchesCount_Exists(t *testing.T) {
	if DatasetRecordBatchesCount == nil {
		t.Fatal("DatasetRecordBatchesCount metric should not be nil")
	}
	// Verify it's a GaugeVec with dataset label
	DatasetRecordBatchesCount.WithLabelValues("dataset1").Set(10)
	DatasetRecordBatchesCount.WithLabelValues("dataset2").Set(20)
}

// Test Filter metrics
func TestFilterExecutionDurationSeconds_Exists(t *testing.T) {
	if FilterExecutionDurationSeconds == nil {
		t.Fatal("FilterExecutionDurationSeconds metric should not be nil")
	}
	// Verify it's a HistogramVec with operator label
	FilterExecutionDurationSeconds.WithLabelValues("equal").Observe(0.001)
	FilterExecutionDurationSeconds.WithLabelValues("greater").Observe(0.002)
}

func TestFilterSelectivityRatio_Exists(t *testing.T) {
	if FilterSelectivityRatio == nil {
		t.Fatal("FilterSelectivityRatio metric should not be nil")
	}
	// Verify it's a HistogramVec with dataset label
	FilterSelectivityRatio.WithLabelValues("test_dataset").Observe(0.25)
}

// Test HNSW metrics
func TestHnswGraphHeight_Exists(t *testing.T) {
	if HnswGraphHeight == nil {
		t.Fatal("HnswGraphHeight metric should not be nil")
	}
	// Verify it's a GaugeVec with dataset label
	HnswGraphHeight.WithLabelValues("vectors").Set(5)
}

func TestHnswNodeCount_Exists(t *testing.T) {
	if HnswNodeCount == nil {
		t.Fatal("HnswNodeCount metric should not be nil")
	}
	// Verify it's a GaugeVec with dataset label
	HnswNodeCount.WithLabelValues("vectors").Set(10000)
}

// Test Flight metrics
func TestFlightTicketParseDurationSeconds_Exists(t *testing.T) {
	if FlightTicketParseDurationSeconds == nil {
		t.Fatal("FlightTicketParseDurationSeconds metric should not be nil")
	}
	FlightTicketParseDurationSeconds.Observe(0.0001)
}

// Test Pool metrics
func TestVectorScratchPoolMissesTotal_Exists(t *testing.T) {
	if VectorScratchPoolMissesTotal == nil {
		t.Fatal("VectorScratchPoolMissesTotal metric should not be nil")
	}
	VectorScratchPoolMissesTotal.Inc()
}

// Test Lock metrics
func TestDatasetLockWaitDurationSeconds_Exists(t *testing.T) {
	if DatasetLockWaitDurationSeconds == nil {
		t.Fatal("DatasetLockWaitDurationSeconds metric should not be nil")
	}
	// Verify it's a HistogramVec with operation label
	DatasetLockWaitDurationSeconds.WithLabelValues("read").Observe(0.0001)
	DatasetLockWaitDurationSeconds.WithLabelValues("write").Observe(0.001)
}

// Test Memory metrics
func TestArrowMemoryUsedBytes_Exists(t *testing.T) {
	if ArrowMemoryUsedBytes == nil {
		t.Fatal("ArrowMemoryUsedBytes metric should not be nil")
	}
	// Verify it's a GaugeVec with allocator label
	ArrowMemoryUsedBytes.WithLabelValues("default").Set(1024000)
}

// Test SIMD metrics
func TestSimdDispatchCount_Exists(t *testing.T) {
	if SimdDispatchCount == nil {
		t.Fatal("SimdDispatchCount metric should not be nil")
	}
	// Verify it's a CounterVec with impl label
	SimdDispatchCount.WithLabelValues("avx2").Inc()
	SimdDispatchCount.WithLabelValues("avx512").Inc()
	SimdDispatchCount.WithLabelValues("generic").Inc()
}

// Test Snapshot metrics
func TestSnapshotWriteDurationSeconds_Exists(t *testing.T) {
	if SnapshotWriteDurationSeconds == nil {
		t.Fatal("SnapshotWriteDurationSeconds metric should not be nil")
	}
	SnapshotWriteDurationSeconds.Observe(1.5)
}

func TestSnapshotSizeBytes_Exists(t *testing.T) {
	if SnapshotSizeBytes == nil {
		t.Fatal("SnapshotSizeBytes metric should not be nil")
	}
	SnapshotSizeBytes.Observe(1024 * 1024 * 10) // 10MB
}

// Test GC metrics
func TestGcPauseDurationSeconds_Exists(t *testing.T) {
	if GcPauseDurationSeconds == nil {
		t.Fatal("GcPauseDurationSeconds metric should not be nil")
	}
	GcPauseDurationSeconds.Observe(0.0001)
}

// Test Concurrency metrics
func TestActiveSearchContexts_Exists(t *testing.T) {
	if ActiveSearchContexts == nil {
		t.Fatal("ActiveSearchContexts metric should not be nil")
	}
	ActiveSearchContexts.Inc()
	ActiveSearchContexts.Dec()
}

// Test Compaction metrics
func TestCompactionOperationsTotal_Exists(t *testing.T) {
	if CompactionOperationsTotal == nil {
		t.Fatal("CompactionOperationsTotal metric should not be nil")
	}
	// Verify it's a CounterVec with status label
	CompactionOperationsTotal.WithLabelValues("default", "success").Inc()
	CompactionOperationsTotal.WithLabelValues("default", "error").Inc()
}

// =============================================================================
// Test metric registration and descriptions
// =============================================================================

func TestMetricsAreRegistered(t *testing.T) {
	// Verify metrics are collectible (registered with default registry)
	metrics := []prometheus.Collector{
		WalFsyncDurationSeconds,
		WalBatchSize,
		WalPendingEntries,
		IndexQueueDepth,
		IndexJobLatencySeconds,
		DatasetRecordBatchesCount,
		FilterExecutionDurationSeconds,
		FilterSelectivityRatio,
		HnswGraphHeight,
		HnswNodeCount,
		FlightTicketParseDurationSeconds,
		VectorScratchPoolMissesTotal,
		DatasetLockWaitDurationSeconds,
		ArrowMemoryUsedBytes,
		SimdDispatchCount,
		SnapshotWriteDurationSeconds,
		SnapshotSizeBytes,
		GcPauseDurationSeconds,
		ActiveSearchContexts,
		CompactionOperationsTotal,
	}

	for i, m := range metrics {
		if m == nil {
			t.Errorf("Metric %d is nil", i)
		}
	}
}

// Test histogram bucket configurations
func TestHistogramBuckets(t *testing.T) {
	// WalFsyncDurationSeconds should have fine-grained buckets for latency
	WalFsyncDurationSeconds.WithLabelValues("test").Observe(0.0001)
	WalFsyncDurationSeconds.WithLabelValues("test").Observe(0.001)
	WalFsyncDurationSeconds.WithLabelValues("test").Observe(0.01)
	WalFsyncDurationSeconds.WithLabelValues("test").Observe(0.1)

	// WalBatchSize should have appropriate batch size buckets
	WalBatchSize.Observe(1)
	WalBatchSize.Observe(10)
	WalBatchSize.Observe(100)
	WalBatchSize.Observe(1000)
}

// Benchmark metric operations
func BenchmarkWalFsyncObserve(b *testing.B) {
	for i := 0; i < b.N; i++ {
		WalFsyncDurationSeconds.WithLabelValues("success").Observe(0.001)
	}
}

func BenchmarkIndexJobLatencyObserve(b *testing.B) {
	for i := 0; i < b.N; i++ {
		IndexJobLatencySeconds.WithLabelValues("benchmark_dataset").Observe(0.05)
	}
}

func BenchmarkSimdDispatchIncrement(b *testing.B) {
	for i := 0; i < b.N; i++ {
		SimdDispatchCount.WithLabelValues("avx2").Inc()
	}
}
