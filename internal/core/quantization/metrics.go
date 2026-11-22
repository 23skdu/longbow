package quantization

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// QuantizationError is a quantization error value.
	QuantizationError = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_quantization_error_mse",
		Help: "Mean Squared Error of current quantization bit-depth",
	}, []string{"dataset", "bits"})

	// CompressionRatio is a compression ratio value.
	CompressionRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_compression_ratio",
		Help: "Compression ratio achieved by Turboquant V2",
	}, []string{"dataset"})
)
