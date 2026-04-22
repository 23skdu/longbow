package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TPUHBMUsageBytes tracks the High Bandwidth Memory usage on TPU
	TPUHBMUsageBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_tpu_hbm_usage_bytes",
			Help: "TPU High Bandwidth Memory usage in bytes",
		},
		[]string{"device_id", "type"}, // type: total, used, free
	)

	// TPUCoreUtilizationRatio tracks TensorCore and SparseCore activity
	TPUCoreUtilizationRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_tpu_core_utilization_ratio",
			Help: "TPU core utilization ratio (0.0 to 1.0)",
		},
		[]string{"device_id", "core_type"}, // core_type: tensor, sparse
	)

	// TPUD2DLatencySeconds tracks die-to-die interconnect latency
	TPUD2DLatencySeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_tpu_d2d_latency_seconds",
			Help:    "TPU die-to-die (D2D) interconnect latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"device_id"},
	)

	// TPUInferenceDurationSeconds tracks XLA/Pallas inference duration
	TPUInferenceDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_tpu_inference_duration_seconds",
			Help:    "TPU inference duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"device_id", "kernel"},
	)
)

// RecordTPUMemory records TPU HBM usage metrics
func RecordTPUMemory(deviceID int, total, used int64) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	TPUHBMUsageBytes.WithLabelValues(deviceLabel, "total").Set(float64(total))
	TPUHBMUsageBytes.WithLabelValues(deviceLabel, "used").Set(float64(used))
	TPUHBMUsageBytes.WithLabelValues(deviceLabel, "free").Set(float64(total - used))
}

// RecordTPUUtilization records TPU core utilization
func RecordTPUUtilization(deviceID int, tensorRatio, sparseRatio float64) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	TPUCoreUtilizationRatio.WithLabelValues(deviceLabel, "tensor").Set(tensorRatio)
	TPUCoreUtilizationRatio.WithLabelValues(deviceLabel, "sparse").Set(sparseRatio)
}

// RecordTPUD2DLatency records D2D interconnect latency
func RecordTPUD2DLatency(deviceID int, latency time.Duration) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	TPUD2DLatencySeconds.WithLabelValues(deviceLabel).Observe(latency.Seconds())
}

// RecordTPUInference records TPU inference duration
func RecordTPUInference(deviceID int, kernel string, duration time.Duration) {
	deviceLabel := fmt.Sprintf("%d", deviceID)
	TPUInferenceDurationSeconds.WithLabelValues(deviceLabel, kernel).Observe(duration.Seconds())
}
