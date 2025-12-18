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

func TestArenaAllocBytesTotal_Exists(t *testing.T) {
if ArenaAllocBytesTotal == nil {
t.Fatal("ArenaAllocBytesTotal metric should not be nil")
}
// Should be a Counter
ArenaAllocBytesTotal.Add(1024)
}

func TestArenaOverflowTotal_Exists(t *testing.T) {
if ArenaOverflowTotal == nil {
t.Fatal("ArenaOverflowTotal metric should not be nil")
}
// Should be a Counter
ArenaOverflowTotal.Inc()
}

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

func TestBloomLookupsTotal_Exists(t *testing.T) {
if BloomLookupsTotal == nil {
t.Fatal("BloomLookupsTotal metric should not be nil")
}
// Should be CounterVec with result label (hit/miss)
BloomLookupsTotal.WithLabelValues("hit").Inc()
BloomLookupsTotal.WithLabelValues("miss").Inc()
}

func TestBloomFalsePositiveRate_Exists(t *testing.T) {
if BloomFalsePositiveRate == nil {
t.Fatal("BloomFalsePositiveRate metric should not be nil")
}
// Should be Gauge (observed FP rate)
BloomFalsePositiveRate.Set(0.01)
}

// -----------------------------------------------------------------------------
// Column Index Metrics Tests
// -----------------------------------------------------------------------------

func TestColumnIndexSize_Exists(t *testing.T) {
if ColumnIndexSize == nil {
t.Fatal("ColumnIndexSize metric should not be nil")
}
// Should be GaugeVec with dataset and column labels
ColumnIndexSize.WithLabelValues("dataset1", "user_id").Set(10000)
ColumnIndexSize.WithLabelValues("dataset1", "category").Set(500)
}

func TestColumnIndexLookupDuration_Exists(t *testing.T) {
if ColumnIndexLookupDuration == nil {
t.Fatal("ColumnIndexLookupDuration metric should not be nil")
}
// Should be HistogramVec with dataset label
ColumnIndexLookupDuration.WithLabelValues("dataset1").Observe(0.0001)
ColumnIndexLookupDuration.WithLabelValues("dataset2").Observe(0.001)
}

// -----------------------------------------------------------------------------
// HNSW Search Metrics Tests
// -----------------------------------------------------------------------------

func TestHnswNodesVisited_Exists(t *testing.T) {
if HnswNodesVisited == nil {
t.Fatal("HnswNodesVisited metric should not be nil")
}
// Should be HistogramVec with dataset label
HnswNodesVisited.WithLabelValues("vectors").Observe(150)
HnswNodesVisited.WithLabelValues("embeddings").Observe(200)
}

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

func TestReplicationLagSeconds_Exists(t *testing.T) {
if ReplicationLagSeconds == nil {
t.Fatal("ReplicationLagSeconds metric should not be nil")
}
// Should be GaugeVec with peer label
ReplicationLagSeconds.WithLabelValues("peer1:8080").Set(0.5)
ReplicationLagSeconds.WithLabelValues("peer2:8080").Set(1.2)
}

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

func TestFlightPoolConnectionsActive_Exists(t *testing.T) {
if FlightPoolConnectionsActive == nil {
t.Fatal("FlightPoolConnectionsActive metric should not be nil")
}
// Should be GaugeVec with host label
FlightPoolConnectionsActive.WithLabelValues("host1:8080").Set(5)
FlightPoolConnectionsActive.WithLabelValues("host2:8080").Set(3)
}

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

func TestPipelineBatchesPerSecond_Exists(t *testing.T) {
if PipelineBatchesPerSecond == nil {
t.Fatal("PipelineBatchesPerSecond metric should not be nil")
}
// Should be Gauge (throughput rate)
PipelineBatchesPerSecond.Set(4200000)
}

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

func TestWalAdaptiveIntervalMs_Exists(t *testing.T) {
if WalAdaptiveIntervalMs == nil {
t.Fatal("WalAdaptiveIntervalMs metric should not be nil")
}
// Should be Gauge (current adaptive flush interval in ms)
WalAdaptiveIntervalMs.Set(15) // 15ms
}

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

func TestIpcBufferPoolUtilization_Exists(t *testing.T) {
if IpcBufferPoolUtilization == nil {
t.Fatal("IpcBufferPoolUtilization metric should not be nil")
}
// Should be Gauge (0-1 utilization ratio)
IpcBufferPoolUtilization.Set(0.75)
}

func TestIpcBufferPoolHits_Exists(t *testing.T) {
if IpcBufferPoolHits == nil {
t.Fatal("IpcBufferPoolHits metric should not be nil")
}
// Should be Counter
IpcBufferPoolHits.Inc()
}

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

func TestS3RequestDuration_Exists(t *testing.T) {
if S3RequestDuration == nil {
t.Fatal("S3RequestDuration metric should not be nil")
}
// Should be HistogramVec with operation label
S3RequestDuration.WithLabelValues("PutObject").Observe(0.5)
S3RequestDuration.WithLabelValues("GetObject").Observe(0.3)
S3RequestDuration.WithLabelValues("ListObjects").Observe(0.1)
}

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

func TestWarmupProgressPercent_Exists(t *testing.T) {
if WarmupProgressPercent == nil {
t.Fatal("WarmupProgressPercent metric should not be nil")
}
// Should be Gauge (0-100 percentage)
WarmupProgressPercent.Set(75.5)
}

func TestWarmupDatasetsTotal_Exists(t *testing.T) {
if WarmupDatasetsTotal == nil {
t.Fatal("WarmupDatasetsTotal metric should not be nil")
}
// Should be Gauge
WarmupDatasetsTotal.Set(10)
}

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

func TestShardedHnswLoadFactor_Exists(t *testing.T) {
if ShardedHnswLoadFactor == nil {
t.Fatal("ShardedHnswLoadFactor metric should not be nil")
}
// Should be GaugeVec with shard_id label (0-1 load factor)
ShardedHnswLoadFactor.WithLabelValues("0").Set(0.65)
ShardedHnswLoadFactor.WithLabelValues("1").Set(0.72)
ShardedHnswLoadFactor.WithLabelValues("2").Set(0.58)
}

func TestShardedHnswShardSize_Exists(t *testing.T) {
if ShardedHnswShardSize == nil {
t.Fatal("ShardedHnswShardSize metric should not be nil")
}
// Should be GaugeVec with shard_id label
ShardedHnswShardSize.WithLabelValues("0").Set(10000)
ShardedHnswShardSize.WithLabelValues("1").Set(12000)
}

// -----------------------------------------------------------------------------
// Record Size Cache Metrics Tests
// -----------------------------------------------------------------------------

func TestRecordSizeCacheHitRate_Exists(t *testing.T) {
if RecordSizeCacheHitRate == nil {
t.Fatal("RecordSizeCacheHitRate metric should not be nil")
}
// Should be Gauge (0-1 hit rate)
RecordSizeCacheHitRate.Set(0.95)
}

// =============================================================================
// Test all comprehensive metrics are registered
// =============================================================================

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
// Record Size Cache
RecordSizeCacheHitRate,
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

func BenchmarkArenaAllocBytesAdd(b *testing.B) {
for i := 0; i < b.N; i++ {
ArenaAllocBytesTotal.Add(1024)
}
}

func BenchmarkResultPoolHitsInc(b *testing.B) {
for i := 0; i < b.N; i++ {
ResultPoolHitsTotal.WithLabelValues("100").Inc()
}
}

func BenchmarkHnswNodesVisitedObserve(b *testing.B) {
for i := 0; i < b.N; i++ {
HnswNodesVisited.WithLabelValues("dataset").Observe(150)
}
}

func BenchmarkS3RequestDurationObserve(b *testing.B) {
for i := 0; i < b.N; i++ {
S3RequestDuration.WithLabelValues("PutObject").Observe(0.5)
}
}
