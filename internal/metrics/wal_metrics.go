package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// WAL Metrics
// =============================================================================

var (
	// WalWritesTotal counts WAL write operations
	WalWritesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_wal_writes_total",
			Help: "Total number of WAL write operations",
		},
		[]string{"status"},
	)

	// WalBytesWritten tracks bytes written to WAL
	WalBytesWritten = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_bytes_written_total",
			Help: "Total bytes written to the Write-Ahead Log",
		},
	)

	// WalReplayDurationSeconds measures time taken to replay WAL on startup
	WalReplayDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_replay_duration_seconds",
			Help:    "Time taken to replay the Write-Ahead Log",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
	)

	// WalBufferPoolOperations counts buffer pool Get/Put operations
	WalBufferPoolOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_wal_buffer_pool_operations_total",
			Help: "Total number of WAL buffer pool operations",
		},
		[]string{"operation"},
	)

	// WalFsyncDurationSeconds - Time taken for walFile.Sync() calls
	WalFsyncDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_fsync_duration_seconds",
			Help:    "Time taken for WAL fsync operations",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"status"},
	)

	// WalBatchSize - Number of entries flushed per batch
	WalBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_batch_size",
			Help:    "Number of entries flushed per WAL batch",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
		},
	)

	// WalPendingEntries - Current length of WAL entries channel
	WalPendingEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_pending_entries",
			Help: "Current number of pending WAL entries (backpressure indicator)",
		},
	)

	// WalAdaptiveIntervalMs tracks current adaptive flush interval
	WalAdaptiveIntervalMs = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_adaptive_interval_ms",
			Help: "Current adaptive WAL flush interval in milliseconds",
		},
	)

	// WalWriteRatePerSecond tracks current write rate
	WalWriteRatePerSecond = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_write_rate_per_second",
			Help: "Current WAL write rate per second",
		},
	)

	// WalRingBufferUtilization tracks ring buffer usage (0-1)
	WalRingBufferUtilization = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_ring_buffer_utilization",
			Help: "Current utilization of WAL ring buffer (0-1)",
		},
	)

	// WalRingBufferPushesTotal counts successful ring buffer pushes
	WalRingBufferPushesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_ring_buffer_pushes_total",
			Help: "Total number of successful ring buffer push operations",
		},
	)

	// WalRingBufferDrainsTotal counts ring buffer drain operations
	WalRingBufferDrainsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_ring_buffer_drains_total",
			Help: "Total number of ring buffer drain operations",
		},
	)

	// WalRingBufferFullTotal counts times buffer was full
	WalRingBufferFullTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_wal_ring_buffer_full_total",
			Help: "Total number of times ring buffer was full (backpressure)",
		},
	)

	// WalUringSubmissionQueueDepth tracks the number of entries in the submission queue
	WalUringSubmissionQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_uring_sq_depth",
			Help: "Current depth of the io_uring submission queue",
		},
	)

	// WalUringCompletionQueueDepth tracks the number of entries in the completion queue
	WalUringCompletionQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_wal_uring_cq_depth",
			Help: "Current depth of the io_uring completion queue",
		},
	)

	// WalUringSubmitLatencySeconds measures the latency of io_uring submission calls
	WalUringSubmitLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_uring_submit_latency_seconds",
			Help:    "Latency of io_uring Enter/Submit calls",
			Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01}, // Microsecond resolution
		},
	)
)
