package tensor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TensorOperationsTotal is a tensor operations total value.
	TensorOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_tensor_operations_total",
			Help: "Total number of tensor calculus operations executed",
		},
		[]string{"op", "device", "dtype"},
	)

	// TensorOperationDurationSeconds is a tensor operation duration seconds value.
	TensorOperationDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_tensor_operation_duration_seconds",
			Help:    "Execution duration of tensor calculus operations in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"op", "device"},
	)

	// TensorBytesProcessedTotal is a tensor bytes processed total value.
	TensorBytesProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_tensor_bytes_processed_total",
			Help: "Total bytes processed during tensor operations",
		},
		[]string{"op"},
	)

	// TensorOptimizerPassesTotal is a tensor optimizer passes total value.
	TensorOptimizerPassesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_tensor_optimizer_passes_total",
			Help: "Total number of optimization rewrite passes applied to tensor DAGs",
		},
		[]string{"rule"},
	)

	// TensorOptimizerFlopsSavedTotal is a tensor optimizer flops saved total value.
	TensorOptimizerFlopsSavedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_tensor_optimizer_flops_saved_total",
			Help: "Estimated floating-point operations saved by tensor DAG optimizations",
		},
	)
)
