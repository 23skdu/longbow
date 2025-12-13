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
			Name:    "longbow_flight_duration_seconds",
			Help:    "Duration of Arrow Flight operations",
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
)
