package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// I/O & Storage Optimization Metrics
// =============================================================================

var (
	// IOReadBytesTotal tracks total bytes read from disk
	IOReadBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_io_read_bytes_total",
			Help: "Total bytes read from disk storage",
		},
		[]string{"component"}, // "wal", "index", "store"
	)

	// IOWriteBytesTotal tracks total bytes written to disk
	IOWriteBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_io_write_bytes_total",
			Help: "Total bytes written to disk storage",
		},
		[]string{"component"},
	)

	// IOReadOpsTotal tracks total read operations
	IOReadOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_io_read_ops_total",
			Help: "Total read operations (syscalls)",
		},
		[]string{"component"},
	)

	// IOWriteOpsTotal tracks total write operations
	IOWriteOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_io_write_ops_total",
			Help: "Total write operations (syscalls)",
		},
		[]string{"component"},
	)

	// IOFsyncDurationSeconds measures fsync latency
	IOFsyncDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_io_fsync_duration_seconds",
			Help:    "Latency of fsync syscalls",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"component"},
	)

	// IOSystemReadThroughputBytes samples /proc/diskstats if available
	IOSystemReadThroughputBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_system_disk_read_bytes_per_second",
			Help: "System-wide disk read throughput (from /proc/diskstats)",
		},
		[]string{"device"},
	)

	// IOSystemWriteThroughputBytes samples /proc/diskstats if available
	IOSystemWriteThroughputBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_system_disk_write_bytes_per_second",
			Help: "System-wide disk write throughput (from /proc/diskstats)",
		},
		[]string{"device"},
	)

	// IOPageCacheUsageBytes estimates page cache usage
	IOPageCacheUsageBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_io_page_cache_usage_bytes",
			Help: "Estimated resident set size (RSS) attributed to file mappings",
		},
	)
)
