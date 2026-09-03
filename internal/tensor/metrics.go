package tensor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TensorOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_tensor_operations_total",
			Help: "Total number of tensor calculus operations executed",
		},
		[]string{"op", "device", "dtype"},
	)

	TensorOperationDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_tensor_operation_duration_seconds",
			Help:    "Execution duration of tensor calculus operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"op", "device"},
	)

	TensorBytesProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_tensor_bytes_processed_total",
			Help: "Total bytes processed during tensor operations",
		},
		[]string{"op"},
	)

	TensorOptimizerPassesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_tensor_optimizer_passes_total",
			Help: "Total number of optimization rewrite passes applied to tensor DAGs",
		},
		[]string{"rule"},
	)

	TensorOptimizerFlopsSavedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_tensor_optimizer_flops_saved_total",
			Help: "Estimated floating-point operations saved by tensor DAG optimizations",
		},
	)
)
