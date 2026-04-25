package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Dynamic Quantization Metrics
// =============================================================================

var (
	// QuantizationActiveType indicates the current quantization mode for a dataset.
	// Labels: dataset, type ("float32", "float16", "int8")
	QuantizationActiveType = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_quantization_active_type",
			Help: "Current active quantization type for the dataset (1 if active).",
		},
		[]string{"dataset", "type"},
	)

	// QuantizationRecallEstimate tracks the estimated recall of quantized searches.
	// Labels: dataset, type
	QuantizationRecallEstimate = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_quantization_recall_estimate",
			Help:    "Estimated search recall for quantized index compared to full precision.",
			Buckets: []float64{0.5, 0.7, 0.8, 0.85, 0.9, 0.95, 0.98, 0.99, 1.0},
		},
		[]string{"dataset", "type"},
	)

	// QuantizationSwitchesTotal counts the number of auto-tuning transitions.
	// Labels: dataset, from, to, reason ("recall", "memory")
	QuantizationSwitchesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_quantization_switches_total",
			Help: "Total number of quantization type transitions triggered by auto-tuning.",
		},
		[]string{"dataset", "from", "to", "reason"},
	)

	// QuantizationMemorySavingsBytes tracks the estimated bytes saved by quantization.
	// Labels: dataset
	QuantizationMemorySavingsBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_quantization_memory_savings_bytes",
			Help: "Estimated memory savings in bytes achieved through quantization.",
		},
		[]string{"dataset"},
	)

	// RequantizationDurationSeconds tracks the time taken for background re-quantization.
	// Labels: dataset, from, to
	RequantizationDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_requantization_duration_seconds",
			Help:    "Time taken to re-quantize a dataset in the background.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120},
		},
		[]string{"dataset", "from", "to"},
	)
)
