package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// =============================================================================
// TDD Tests for Comprehensive Prometheus Metrics Expansion
// Written FIRST before implementation (Red phase)
// =============================================================================

// -----------------------------------------------------------------------------
// SearchArena Metrics Tests
// -----------------------------------------------------------------------------

// TestArenaAllocBytesTotal_Exists runs the test arena alloc bytes total_exists operation.
func TestArenaAllocBytesTotal_Exists(t *testing.T) {
	if ArenaAllocBytesTotal == nil {
		t.Fatal("ArenaAllocBytesTotal metric should not be nil")
	}
	// Should be a Counter
	ArenaAllocBytesTotal.Add(1024)
}

// TestArenaOverflowTotal_Exists runs the test arena overflow total_exists operation.
func TestArenaOverflowTotal_Exists(t *testing.T) {
	if ArenaOverflowTotal == nil {
		t.Fatal("ArenaOverflowTotal metric should not be nil")
	}
	// Should be a Counter
	ArenaOverflowTotal.Inc()
}

// TestArenaResetsTotal_Exists runs the test arena resets total_exists operation.
func TestArenaResetsTotal_Exists(t *testing.T) {
	if ArenaResetsTotal == nil {
		t.Fatal("ArenaResetsTotal metric should not be nil")
	}
	// Should be a Counter
	ArenaResetsTotal.Inc()
}

// -----------------------------------------------------------------------------
// Result Pool Metrics Tests
// -----------------------------------------------------------------------------

// TestResultPoolHitsTotal_Exists runs the test result pool hits total_exists operation.
func TestResultPoolHitsTotal_Exists(t *testing.T) {
	if ResultPoolHitsTotal == nil {
		t.Fatal("ResultPoolHitsTotal metric should not be nil")
	}
	// Should be CounterVec with k_size label
	ResultPoolHitsTotal.WithLabelValues("10").Inc()
	ResultPoolHitsTotal.WithLabelValues("20").Inc()
	ResultPoolHitsTotal.WithLabelValues("50").Inc()
	ResultPoolHitsTotal.WithLabelValues("100").Inc()
	ResultPoolHitsTotal.WithLabelValues("256").Inc()
	ResultPoolHitsTotal.WithLabelValues("1000").Inc()
}

// TestResultPoolMissesTotal_Exists runs the test result pool misses total_exists operation.
func TestResultPoolMissesTotal_Exists(t *testing.T) {
	if ResultPoolMissesTotal == nil {
		t.Fatal("ResultPoolMissesTotal metric should not be nil")
	}
	// Should be CounterVec with k_size label
	ResultPoolMissesTotal.WithLabelValues("10").Inc()
	ResultPoolMissesTotal.WithLabelValues("custom").Inc()
}

// -----------------------------------------------------------------------------
// Bloom Filter Metrics Tests
// -----------------------------------------------------------------------------

// TestBloomLookupsTotal_Exists runs the test bloom lookups total_exists operation.
func TestBloomLookupsTotal_Exists(t *testing.T) {
	if BloomLookupsTotal == nil {
		t.Fatal("BloomLookupsTotal metric should not be nil")
	}
	// Should be CounterVec with result label (hit/miss)
	BloomLookupsTotal.WithLabelValues("hit").Inc()
	BloomLookupsTotal.WithLabelValues("miss").Inc()
}

// TestBloomFalsePositiveRate_Exists runs the test bloom false positive rate_exists operation.
func TestBloomFalsePositiveRate_Exists(t *testing.T) {
	if BloomFalsePositiveRate == nil {
		t.Fatal("BloomFalsePositiveRate metric should not be nil")
	}
	// Should be Gauge (observed FP rate)
	BloomFalsePositiveRate.WithLabelValues("test_dataset", "test_column").Set(0.01)
}

// -----------------------------------------------------------------------------
// Column Index Metrics Tests
// -----------------------------------------------------------------------------

// TestColumnIndexSize_Exists runs the test column index size_exists operation.
func TestColumnIndexSize_Exists(t *testing.T) {
	if ColumnIndexSize == nil {
		t.Fatal("ColumnIndexSize metric should not be nil")
	}
	// Should be GaugeVec with dataset and column labels
	ColumnIndexSize.WithLabelValues("dataset1", "user_id").Set(10000)
	ColumnIndexSize.WithLabelValues("dataset1", "category").Set(500)
}

// TestColumnIndexLookupDuration_Exists runs the test column index lookup duration_exists operation.
func TestColumnIndexLookupDuration_Exists(t *testing.T) {
	if ColumnIndexLookupDuration == nil {
		t.Fatal("ColumnIndexLookupDuration metric should not be nil")
	}
	// Should be HistogramVec with dataset and column labels
	ColumnIndexLookupDuration.WithLabelValues("dataset1", "column1").Observe(0.0001)
	ColumnIndexLookupDuration.WithLabelValues("dataset2", "column1").Observe(0.001)
}

// -----------------------------------------------------------------------------
// HNSW Search Metrics Tests
// -----------------------------------------------------------------------------

// TestHnswNodesVisited_Exists runs the test hnsw nodes visited_exists operation.
func TestHnswNodesVisited_Exists(t *testing.T) {
	if HnswNodesVisited == nil {
		t.Fatal("HnswNodesVisited metric should not be nil")
	}
	// Should be HistogramVec with dataset label
	HnswNodesVisited.WithLabelValues("vectors").Observe(150)
	HnswNodesVisited.WithLabelValues("embeddings").Observe(200)
}

// TestHnswDistanceCalculations_Exists runs the test hnsw distance calculations_exists operation.
func TestHnswDistanceCalculations_Exists(t *testing.T) {
	if HnswDistanceCalculations == nil {
		t.Fatal("HnswDistanceCalculations metric should not be nil")
	}
	// Should be Counter (total distance computations)
	HnswDistanceCalculations.Add(1000)
}

// -----------------------------------------------------------------------------
// Peer Replication Metrics Tests
// -----------------------------------------------------------------------------

// TestReplicationLagSeconds_Exists runs the test replication lag seconds_exists operation.
func TestReplicationLagSeconds_Exists(t *testing.T) {
	if ReplicationLagSeconds == nil {
		t.Fatal("ReplicationLagSeconds metric should not be nil")
	}
	// Should be GaugeVec with peer label
	ReplicationLagSeconds.WithLabelValues("peer1:8080").Set(0.5)
	ReplicationLagSeconds.WithLabelValues("peer2:8080").Set(1.2)
}

// TestPeerHealthStatus_Exists runs the test peer health status_exists operation.
func TestPeerHealthStatus_Exists(t *testing.T) {
	if PeerHealthStatus == nil {
		t.Fatal("PeerHealthStatus metric should not be nil")
	}
	// Should be GaugeVec with peer label (0=down, 1=up)
	PeerHealthStatus.WithLabelValues("peer1:8080").Set(1)
	PeerHealthStatus.WithLabelValues("peer2:8080").Set(0)
}

// -----------------------------------------------------------------------------
// Flight Client Pool Metrics Tests
// -----------------------------------------------------------------------------

// TestFlightPoolConnectionsActive_Exists runs the test flight pool connections active_exists operation.
func TestFlightPoolConnectionsActive_Exists(t *testing.T) {
	if FlightPoolConnectionsActive == nil {
		t.Fatal("FlightPoolConnectionsActive metric should not be nil")
	}
	// Should be GaugeVec with host label
	FlightPoolConnectionsActive.WithLabelValues("host1:8080").Set(5)
	FlightPoolConnectionsActive.WithLabelValues("host2:8080").Set(3)
}

// TestFlightPoolWaitDuration_Exists runs the test flight pool wait duration_exists operation.
func TestFlightPoolWaitDuration_Exists(t *testing.T) {
	if FlightPoolWaitDuration == nil {
		t.Fatal("FlightPoolWaitDuration metric should not be nil")
	}
	// Should be HistogramVec with host label
	FlightPoolWaitDuration.WithLabelValues("host1:8080").Observe(0.001)
	FlightPoolWaitDuration.WithLabelValues("host2:8080").Observe(0.01)
}

// -----------------------------------------------------------------------------
// DoGet Pipeline Metrics Tests
// -----------------------------------------------------------------------------

// TestPipelineBatchesPerSecond_Exists runs the test pipeline batches per second_exists operation.
func TestPipelineBatchesPerSecond_Exists(t *testing.T) {
	if PipelineBatchesPerSecond == nil {
		t.Fatal("PipelineBatchesPerSecond metric should not be nil")
	}
	// Should be Gauge (throughput rate)
	PipelineBatchesPerSecond.Set(4200000)
}

// TestPipelineWorkerUtilization_Exists runs the test pipeline worker utilization_exists operation.
func TestPipelineWorkerUtilization_Exists(t *testing.T) {
	if PipelineWorkerUtilization == nil {
		t.Fatal("PipelineWorkerUtilization metric should not be nil")
	}
	// Should be GaugeVec with worker_id label (0-1 percentage)
	PipelineWorkerUtilization.WithLabelValues("0").Set(0.85)
	PipelineWorkerUtilization.WithLabelValues("1").Set(0.72)
}

// -----------------------------------------------------------------------------
// Adaptive WAL Metrics Tests
// -----------------------------------------------------------------------------

// TestWalAdaptiveIntervalMs_Exists runs the test wal adaptive interval ms_exists operation.
func TestWalAdaptiveIntervalMs_Exists(t *testing.T) {
	if WalAdaptiveIntervalMs == nil {
		t.Fatal("WalAdaptiveIntervalMs metric should not be nil")
	}
	// Should be Gauge (current adaptive flush interval in ms)
	WalAdaptiveIntervalMs.Set(15) // 15ms
}

// TestWalWriteRatePerSecond_Exists runs the test wal write rate per second_exists operation.
func TestWalWriteRatePerSecond_Exists(t *testing.T) {
	if WalWriteRatePerSecond == nil {
		t.Fatal("WalWriteRatePerSecond metric should not be nil")
	}
	// Should be Gauge (current write rate)
	WalWriteRatePerSecond.Set(50000)
}

// -----------------------------------------------------------------------------
// Zero-Copy / HNSW Epoch Metrics Tests
// -----------------------------------------------------------------------------

// TestHnswEpochTransitions_Exists runs the test hnsw epoch transitions_exists operation.
func TestHnswEpochTransitions_Exists(t *testing.T) {
	if HnswEpochTransitions == nil {
		t.Fatal("HnswEpochTransitions metric should not be nil")
	}
	// Should be Counter (epoch advancement count)
	HnswEpochTransitions.Inc()
}

// -----------------------------------------------------------------------------
// Fast Path Filter Metrics Tests
// -----------------------------------------------------------------------------

// TestFastPathUsageTotal_Exists runs the test fast path usage total_exists operation.
func TestFastPathUsageTotal_Exists(t *testing.T) {
	if FastPathUsageTotal == nil {
		t.Fatal("FastPathUsageTotal metric should not be nil")
	}
	// Should be CounterVec with path label (fast/fallback)
	FastPathUsageTotal.WithLabelValues("fast").Inc()
	FastPathUsageTotal.WithLabelValues("fallback").Inc()
}

// -----------------------------------------------------------------------------
// IPC Buffer Pool Metrics Tests
// -----------------------------------------------------------------------------

// TestIpcBufferPoolUtilization_Exists runs the test ipc buffer pool utilization_exists operation.
func TestIpcBufferPoolUtilization_Exists(t *testing.T) {
	if IpcBufferPoolUtilization == nil {
		t.Fatal("IpcBufferPoolUtilization metric should not be nil")
	}
	// Should be Gauge (0-1 utilization ratio)
	IpcBufferPoolUtilization.Set(0.75)
}

// TestIpcBufferPoolHits_Exists runs the test ipc buffer pool hits_exists operation.
func TestIpcBufferPoolHits_Exists(t *testing.T) {
	if IpcBufferPoolHits == nil {
		t.Fatal("IpcBufferPoolHits metric should not be nil")
	}
	// Should be Counter
	IpcBufferPoolHits.Inc()
}

// TestIpcBufferPoolMisses_Exists runs the test ipc buffer pool misses_exists operation.
func TestIpcBufferPoolMisses_Exists(t *testing.T) {
	if IpcBufferPoolMisses == nil {
		t.Fatal("IpcBufferPoolMisses metric should not be nil")
	}
	// Should be Counter
	IpcBufferPoolMisses.Inc()
}

// -----------------------------------------------------------------------------
// S3 Backend Metrics Tests
// -----------------------------------------------------------------------------

// TestS3RequestDuration_Exists runs the test s3 request duration_exists operation.
func TestS3RequestDuration_Exists(t *testing.T) {
	if S3RequestDuration == nil {
		t.Fatal("S3RequestDuration metric should not be nil")
	}
	// Should be HistogramVec with operation label
	S3RequestDuration.WithLabelValues("PutObject").Observe(0.5)
	S3RequestDuration.WithLabelValues("GetObject").Observe(0.3)
	S3RequestDuration.WithLabelValues("ListObjects").Observe(0.1)
}

// TestS3RetriesTotal_Exists runs the test s3 retries total_exists operation.
func TestS3RetriesTotal_Exists(t *testing.T) {
	if S3RetriesTotal == nil {
		t.Fatal("S3RetriesTotal metric should not be nil")
	}
	// Should be CounterVec with operation label
	S3RetriesTotal.WithLabelValues("PutObject").Inc()
	S3RetriesTotal.WithLabelValues("GetObject").Inc()
}

// -----------------------------------------------------------------------------
// Warmup Metrics Tests
// -----------------------------------------------------------------------------

// TestWarmupProgressPercent_Exists runs the test warmup progress percent_exists operation.
func TestWarmupProgressPercent_Exists(t *testing.T) {
	if WarmupProgressPercent == nil {
		t.Fatal("WarmupProgressPercent metric should not be nil")
	}
	// Should be Gauge (0-100 percentage)
	WarmupProgressPercent.Set(75.5)
}

// TestWarmupDatasetsTotal_Exists runs the test warmup datasets total_exists operation.
func TestWarmupDatasetsTotal_Exists(t *testing.T) {
	if WarmupDatasetsTotal == nil {
		t.Fatal("WarmupDatasetsTotal metric should not be nil")
	}
	// Should be Gauge
	WarmupDatasetsTotal.Set(10)
}

// TestWarmupDatasetsCompleted_Exists runs the test warmup datasets completed_exists operation.
func TestWarmupDatasetsCompleted_Exists(t *testing.T) {
	if WarmupDatasetsCompleted == nil {
		t.Fatal("WarmupDatasetsCompleted metric should not be nil")
	}
	// Should be Gauge
	WarmupDatasetsCompleted.Set(7)
}

// -----------------------------------------------------------------------------
// Sharded HNSW Metrics Tests
// -----------------------------------------------------------------------------

// TestShardedHnswLoadFactor_Exists runs the test sharded hnsw load factor_exists operation.
func TestShardedHnswLoadFactor_Exists(t *testing.T) {
	if ShardedHnswLoadFactor == nil {
		t.Fatal("ShardedHnswLoadFactor metric should not be nil")
	}
	// Should be GaugeVec with shard_id label (0-1 load factor)
	ShardedHnswLoadFactor.WithLabelValues("test_dataset", "0").Set(0.65)
	ShardedHnswLoadFactor.WithLabelValues("test_dataset", "1").Set(0.72)
	ShardedHnswLoadFactor.WithLabelValues("test_dataset", "2").Set(0.58)
}

// TestShardedHnswShardSize_Exists runs the test sharded hnsw shard size_exists operation.
func TestShardedHnswShardSize_Exists(t *testing.T) {
	if ShardedHnswShardSize == nil {
		t.Fatal("ShardedHnswShardSize metric should not be nil")
	}
	// Should be GaugeVec with shard_id label
	ShardedHnswShardSize.WithLabelValues("test_dataset", "0").Set(10000)
	ShardedHnswShardSize.WithLabelValues("test_dataset", "1").Set(12000)
}

// -----------------------------------------------------------------------------
// Record Size Cache Metrics Tests
// -----------------------------------------------------------------------------

// =============================================================================
// Test all comprehensive metrics are registered
// =============================================================================

// TestComprehensiveMetricsRegistered runs the test comprehensive metrics registered operation.
func TestComprehensiveMetricsRegistered(t *testing.T) {
	metrics := []prometheus.Collector{
		// SearchArena
		ArenaAllocBytesTotal,
		ArenaOverflowTotal,
		ArenaResetsTotal,
		// Result Pool
		ResultPoolHitsTotal,
		ResultPoolMissesTotal,
		// Bloom Filter
		BloomLookupsTotal,
		BloomFalsePositiveRate,
		// Column Index
		ColumnIndexSize,
		ColumnIndexLookupDuration,
		// HNSW Search
		HnswNodesVisited,
		HnswDistanceCalculations,
		// Peer Replication
		ReplicationLagSeconds,
		PeerHealthStatus,
		// Flight Pool
		FlightPoolConnectionsActive,
		FlightPoolWaitDuration,
		// DoGet Pipeline
		PipelineBatchesPerSecond,
		PipelineWorkerUtilization,
		// Adaptive WAL
		WalAdaptiveIntervalMs,
		WalWriteRatePerSecond,
		// Zero-Copy
		HnswEpochTransitions,
		// Fast Path
		FastPathUsageTotal,
		// IPC Buffer Pool
		IpcBufferPoolUtilization,
		IpcBufferPoolHits,
		IpcBufferPoolMisses,
		// S3
		S3RequestDuration,
		S3RetriesTotal,
		// Warmup
		WarmupProgressPercent,
		WarmupDatasetsTotal,
		WarmupDatasetsCompleted,
		// Sharded HNSW
		ShardedHnswLoadFactor,
		ShardedHnswShardSize,
	}

	for i, m := range metrics {
		if m == nil {
			t.Errorf("Comprehensive metric %d is nil", i)
		}
	}
}

// =============================================================================
// Benchmarks for new metrics
// =============================================================================

// BenchmarkArenaAllocBytesAdd runs the benchmark arena alloc bytes add operation.
func BenchmarkArenaAllocBytesAdd(b *testing.B) {
	for b.Loop() {
		ArenaAllocBytesTotal.Add(1024)
	}
}

// BenchmarkResultPoolHitsInc runs the benchmark result pool hits inc operation.
func BenchmarkResultPoolHitsInc(b *testing.B) {
	for b.Loop() {
		ResultPoolHitsTotal.WithLabelValues("100").Inc()
	}
}

// BenchmarkHnswNodesVisitedObserve runs the benchmark hnsw nodes visited observe operation.
func BenchmarkHnswNodesVisitedObserve(b *testing.B) {
	for b.Loop() {
		HnswNodesVisited.WithLabelValues("dataset").Observe(150)
	}
}

// BenchmarkS3RequestDurationObserve runs the benchmark s3 request duration observe operation.
func BenchmarkS3RequestDurationObserve(b *testing.B) {
	for b.Loop() {
		S3RequestDuration.WithLabelValues("PutObject").Observe(0.5)
	}
}
