package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// LockContentionDuration measures time waiting for any generic instrumented lock
	LockContentionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_lock_contention_duration_seconds",
		Help:    "Time spent waiting for generic instrumented locks",
		Buckets: []float64{1e-6, 1e-5, 1e-4, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	}, []string{"type"})

	// WALLockWaitDuration measures time waiting for WAL locks
	WALLockWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_wal_lock_wait_duration_seconds",
			Help:    "Time spent waiting for WAL locks",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
		},
		[]string{"type"}, // "data", "cond", "append", etc.
	)

	// PoolLockWaitDuration measures time waiting for connection pool locks
	PoolLockWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_pool_lock_wait_duration_seconds",
			Help:    "Time spent waiting for connection pool locks",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01},
		},
		[]string{"type"},
	)

	// IndexLockWaitDuration measures time waiting for index structure locks
	IndexLockWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_index_lock_wait_duration_seconds",
			Help:    "Time spent waiting for HNSW index structure locks",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.05},
		},
		[]string{"dataset", "type"}, // dataset name, "read" or "write"
	)

	// DatasetLockWaitDurationSeconds measures time waiting for dataset-level locks
	DatasetLockWaitDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_dataset_lock_wait_duration_seconds",
			Help:    "Time spent waiting for dataset-level locks",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.005, 0.01, 0.05},
		},
		[]string{"operation"},
	)

	// ShardLockWaitDuration measures time waiting for shard-level locks
	ShardLockWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_shard_lock_wait_duration_seconds",
			Help:    "Time spent waiting for shard-level locks",
			Buckets: []float64{0.00001, 0.0001, 0.001, 0.005, 0.01, 0.05},
		},
		[]string{"shard", "type"},
	)

	// Compatibility aliases if needed (can't really alias vars this way in Go easily, so just naming them right)
	DatasetLockWaitDuration = DatasetLockWaitDurationSeconds
)
