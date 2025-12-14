package metrics

import (
"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
// FlightOperationsTotal counts the number of Flight operations (DoGet, DoPut)
FlightOperationsTotal = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "longbow_flight_operations_total",
Help: "The total number of processed Arrow Flight operations",
},
[]string{"method", "status"},
)

// FlightDurationSeconds measures the latency of Flight operations
FlightDurationSeconds = promauto.NewHistogramVec(
prometheus.HistogramOpts{
Name: "longbow_flight_duration_seconds",
Help: "Duration of Arrow Flight operations",
Buckets: prometheus.DefBuckets,
},
[]string{"method"},
)

// FlightBytesProcessed tracks the estimated bytes processed
FlightBytesProcessed = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "longbow_flight_bytes_processed_total",
Help: "Total bytes processed in Flight operations",
},
[]string{"method"},
)

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
Name: "longbow_wal_replay_duration_seconds",
Help: "Time taken to replay the Write-Ahead Log",
Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
},
)

// SnapshotTotal counts snapshot operations
SnapshotTotal = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "longbow_snapshot_operations_total",
Help: "Total number of snapshot operations",
},
[]string{"status"},
)

// SnapshotDurationSeconds measures duration of snapshot creation
SnapshotDurationSeconds = promauto.NewHistogram(
prometheus.HistogramOpts{
Name: "longbow_snapshot_duration_seconds",
Help: "Duration of snapshot creation operations",
Buckets: prometheus.DefBuckets,
},

)
// EvictionsTotal counts the number of evicted records
EvictionsTotal = promauto.NewCounterVec(
prometheus.CounterOpts{
Name: "longbow_evictions_total",
Help: "Total number of evicted records due to memory limits",
},
[]string{"reason"},
)
)
