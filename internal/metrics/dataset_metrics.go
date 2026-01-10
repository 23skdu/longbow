package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	StoreDroppedDatasets = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_store_dropped_datasets_total",
		Help: "Total number of datasets explicitly dropped",
	})

	StoreActiveDatasets = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_store_active_datasets",
		Help: "Current number of active datasets in memory",
	})
)
