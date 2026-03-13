package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// ONNX Metal Metrics
// =============================================================================

var (
	OnnxMetalInferenceDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_onnx_metal_inference_duration_seconds",
			Help:    "Duration of ONNX Metal inference operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"operation"}, // "single", "batch"
	)

	OnnxMetalBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_onnx_metal_batch_size",
			Help:    "Batch size for ONNX Metal inference",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		},
	)

	OnnxMetalMemoryUsed = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_onnx_metal_memory_used_bytes",
			Help: "Memory currently used by ONNX Metal engine",
		},
	)

	OnnxMetalModelLoadDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_onnx_metal_model_load_duration_seconds",
			Help:    "Duration of ONNX model loading",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
	)

	OnnxMetalInferenceErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_onnx_metal_inference_errors_total",
			Help: "Total number of ONNX Metal inference errors",
		},
	)

	OnnxMetalTensorAllocations = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_onnx_metal_tensor_allocations_total",
			Help: "Total number of ONNX Metal tensor allocations",
		},
	)

	OnnxMetalInferenceRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_onnx_metal_inference_requests_total",
			Help: "Total number of ONNX Metal inference requests",
		},
		[]string{"result"}, // "success", "error"
	)

	OnnxMetalModelLoaded = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_onnx_metal_model_loaded",
			Help: "Whether ONNX Metal model is loaded (1 = yes, 0 = no)",
		},
	)
)

// =============================================================================
// ONNX Generic Metrics
// =============================================================================

var (
	OnnxInferenceDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_onnx_inference_duration_seconds",
			Help:    "Duration of ONNX inference operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"backend", "operation"}, // "cpu", "metal", "cuda"
	)

	OnnxInferenceErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_onnx_inference_errors_total",
			Help: "Total number of ONNX inference errors",
		},
		[]string{"backend", "error_type"},
	)

	OnnxModelLoadDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_onnx_model_load_duration_seconds",
			Help:    "Duration of ONNX model loading",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"backend"},
	)
)

// =============================================================================
// Reranker Metrics
// =============================================================================

var (
	RerankerInferenceDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_reranker_inference_duration_seconds",
			Help:    "Duration of reranker inference operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
	)

	RerankerScoresComputed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_reranker_scores_computed_total",
			Help: "Total number of reranker scores computed",
		},
	)

	RerankerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_reranker_errors_total",
			Help: "Total number of reranker errors",
		},
		[]string{"type"}, // "model_load", "inference", "tokenization"
	)

	RerankerBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_reranker_batch_size",
			Help:    "Batch size for reranker operations",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
		},
	)
)
