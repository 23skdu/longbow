package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RDMABytesProcessedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_rdma_bytes_processed_total",
		Help: "The total number of bytes processed via RDMA transport",
	})
	RDMAErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_rdma_errors_total",
		Help: "The total number of RDMA-related errors encountered",
	})
)
