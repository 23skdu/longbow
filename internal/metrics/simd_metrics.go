package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// SIMDActivationDuration tracks the latency of SIMD activation kernels.
	SIMDActivationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_simd_activation_duration_seconds",
		Help:    "Latency of SIMD activation kernels (exp, log, softmax, sigmoid)",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "architecture"})
)
