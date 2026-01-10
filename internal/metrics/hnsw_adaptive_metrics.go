package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HNSWAdaptiveMValue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_hnsw_adaptive_m_value",
		Help: "Current value of M parameter in HNSW graph",
	}, []string{"index_name"})

	HNSWIntrinsicDimensionality = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_hnsw_intrinsic_dimensionality",
		Help: "Estimated intrinsic dimensionality of the data",
	}, []string{"index_name"})

	HNSWAdaptiveAdjustments = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_hnsw_adaptive_adjustments_total",
		Help: "Total number of times M has been adjusted dynamically",
	}, []string{"index_name"})
)
