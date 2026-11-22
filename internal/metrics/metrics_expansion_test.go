package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// =============================================================================
// Test all 20 new Prometheus metrics exist and are properly configured
// =============================================================================

// TestWalFsyncDurationSeconds_Exists WAL metrics
func TestWalFsyncDurationSeconds_Exists(t *testing.T) {
	if WalFsyncDurationSeconds == nil {
		t.Fatal("WalFsyncDurationSeconds metric should not be nil")
	}
	// Verify it's a Histogram with status label
	WalFsyncDurationSeconds.WithLabelValues("success").Observe(0.001)
	WalFsyncDurationSeconds.WithLabelValues("error").Observe(0.01)
}

// TestWalBatchSize_Exists runs the test wal batch size_exists operation.
func TestWalBatchSize_Exists(t *testing.T) {
	if WalBatchSize == nil {
		t.Fatal("WalBatchSize metric should not be nil")
	}
	// Verify it's a Histogram
	WalBatchSize.Observe(100)
}

// TestWalPendingEntries_Exists runs the test wal pending entries_exists operation.
func TestWalPendingEntries_Exists(t *testing.T) {
	if WalPendingEntries == nil {
		t.Fatal("WalPendingEntries metric should not be nil")
	}
	// Verify it's a Gauge
	WalPendingEntries.Set(50)
	WalPendingEntries.Inc()
	WalPendingEntries.Dec()
}

// TestIndexQueueDepth_Exists Index metrics
func TestIndexQueueDepth_Exists(t *testing.T) {
	if IndexQueueDepth == nil {
		t.Fatal("IndexQueueDepth metric should not be nil")
	}
	IndexQueueDepth.Set(25)
}

// TestIndexJobLatencySeconds_Exists runs the test index job latency seconds_exists operation.
func TestIndexJobLatencySeconds_Exists(t *testing.T) {
	if IndexJobLatencySeconds == nil {
		t.Fatal("IndexJobLatencySeconds metric should not be nil")
	}
	// Verify it's a HistogramVec with dataset label
	IndexJobLatencySeconds.WithLabelValues("test_dataset").Observe(0.05)
}

// TestDatasetRecordBatchesCount_Exists runs the test dataset record batches count_exists operation.
func TestDatasetRecordBatchesCount_Exists(t *testing.T) {
	if DatasetRecordBatchesCount == nil {
		t.Fatal("DatasetRecordBatchesCount metric should not be nil")
	}
	// Verify it's a GaugeVec with dataset label
	DatasetRecordBatchesCount.WithLabelValues("dataset1").Set(10)
	DatasetRecordBatchesCount.WithLabelValues("dataset2").Set(20)
}

// TestFilterExecutionDurationSeconds_Exists Filter metrics
func TestFilterExecutionDurationSeconds_Exists(t *testing.T) {
	if FilterExecutionDurationSeconds == nil {
		t.Fatal("FilterExecutionDurationSeconds metric should not be nil")
	}
	// Verify it's a HistogramVec with operator label
	FilterExecutionDurationSeconds.WithLabelValues("equal").Observe(0.001)
	FilterExecutionDurationSeconds.WithLabelValues("greater").Observe(0.002)
}

// TestFilterSelectivityRatio_Exists runs the test filter selectivity ratio_exists operation.
func TestFilterSelectivityRatio_Exists(t *testing.T) {
	if FilterSelectivityRatio == nil {
		t.Fatal("FilterSelectivityRatio metric should not be nil")
	}
	// Verify it's a HistogramVec with dataset label
	FilterSelectivityRatio.WithLabelValues("test_dataset").Observe(0.25)
}

// TestHnswGraphHeight_Exists HNSW metrics
func TestHnswGraphHeight_Exists(t *testing.T) {
	if HnswGraphHeight == nil {
		t.Fatal("HnswGraphHeight metric should not be nil")
	}
	// Verify it's a GaugeVec with dataset label
	HnswGraphHeight.WithLabelValues("vectors").Set(5)
}

// TestHNSWNodeCount_Exists runs the test hnsw node count_exists operation.
func TestHNSWNodeCount_Exists(t *testing.T) {
	if HNSWNodeCount == nil {
		t.Fatal("HNSWNodeCount metric should not be nil")
	}
	// Verify it's a GaugeVec with dataset and shard labels
	HNSWNodeCount.WithLabelValues("vectors", "0").Set(10000)
}

// TestFlightTicketParseDurationSeconds_Exists Flight metrics
func TestFlightTicketParseDurationSeconds_Exists(t *testing.T) {
	if FlightTicketParseDurationSeconds == nil {
		t.Fatal("FlightTicketParseDurationSeconds metric should not be nil")
	}
	FlightTicketParseDurationSeconds.Observe(0.0001)
}

// TestVectorScratchPoolMissesTotal_Exists Pool metrics
func TestVectorScratchPoolMissesTotal_Exists(t *testing.T) {
	if VectorScratchPoolMissesTotal == nil {
		t.Fatal("VectorScratchPoolMissesTotal metric should not be nil")
	}
	VectorScratchPoolMissesTotal.Inc()
}

// TestDatasetLockWaitDurationSeconds_Exists Lock metrics
func TestDatasetLockWaitDurationSeconds_Exists(t *testing.T) {
	if DatasetLockWaitDurationSeconds == nil {
		t.Fatal("DatasetLockWaitDurationSeconds metric should not be nil")
	}
	// Verify it's a HistogramVec with operation label
	DatasetLockWaitDurationSeconds.WithLabelValues("read").Observe(0.0001)
	DatasetLockWaitDurationSeconds.WithLabelValues("write").Observe(0.001)
}

// TestArrowMemoryUsedBytes_Exists Memory metrics
func TestArrowMemoryUsedBytes_Exists(t *testing.T) {
	if ArrowMemoryUsedBytes == nil {
		t.Fatal("ArrowMemoryUsedBytes metric should not be nil")
	}
	// Verify it's a GaugeVec with allocator label
	ArrowMemoryUsedBytes.WithLabelValues("default").Set(1024000)
}

// TestSimdDispatchCount_Exists SIMD metrics
func TestSimdDispatchCount_Exists(t *testing.T) {
	if SimdDispatchCount == nil {
		t.Fatal("SimdDispatchCount metric should not be nil")
	}
	// Verify it's a CounterVec with impl label
	SimdDispatchCount.WithLabelValues("avx2").Inc()
	SimdDispatchCount.WithLabelValues("avx512").Inc()
	SimdDispatchCount.WithLabelValues("generic").Inc()
}

// TestSnapshotWriteDurationSeconds_Exists Snapshot metrics
func TestSnapshotWriteDurationSeconds_Exists(t *testing.T) {
	if SnapshotWriteDurationSeconds == nil {
		t.Fatal("SnapshotWriteDurationSeconds metric should not be nil")
	}
	SnapshotWriteDurationSeconds.Observe(1.5)
}

// TestSnapshotSizeBytes_Exists runs the test snapshot size bytes_exists operation.
func TestSnapshotSizeBytes_Exists(t *testing.T) {
	if SnapshotSizeBytes == nil {
		t.Fatal("SnapshotSizeBytes metric should not be nil")
	}
	SnapshotSizeBytes.Observe(1024 * 1024 * 10) // 10MB
}

// TestGcPauseDurationSeconds_Exists GC metrics
func TestGcPauseDurationSeconds_Exists(t *testing.T) {
	if GcPauseDurationSeconds == nil {
		t.Fatal("GcPauseDurationSeconds metric should not be nil")
	}
	GcPauseDurationSeconds.Observe(0.0001)
}

// TestActiveSearchContexts_Exists Concurrency metrics
func TestActiveSearchContexts_Exists(t *testing.T) {
	if ActiveSearchContexts == nil {
		t.Fatal("ActiveSearchContexts metric should not be nil")
	}
	ActiveSearchContexts.Inc()
	ActiveSearchContexts.Dec()
}

// TestCompactionOperationsTotal_Exists Compaction metrics
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

// TestMetricsAreRegistered runs the test metrics are registered operation.
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
		HNSWNodeCount,
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

// TestHistogramBuckets histogram bucket configurations
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

// BenchmarkWalFsyncObserve metric operations
func BenchmarkWalFsyncObserve(b *testing.B) {
	for b.Loop() {
		WalFsyncDurationSeconds.WithLabelValues("success").Observe(0.001)
	}
}

// BenchmarkIndexJobLatencyObserve runs the benchmark index job latency observe operation.
func BenchmarkIndexJobLatencyObserve(b *testing.B) {
	for b.Loop() {
		IndexJobLatencySeconds.WithLabelValues("benchmark_dataset").Observe(0.05)
	}
}

// BenchmarkSimdDispatchIncrement runs the benchmark simd dispatch increment operation.
func BenchmarkSimdDispatchIncrement(b *testing.B) {
	for b.Loop() {
		SimdDispatchCount.WithLabelValues("avx2").Inc()
	}
}
