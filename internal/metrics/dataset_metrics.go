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

	DatasetExportTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_export_total",
		Help: "Total number of dataset exports",
	}, []string{"dataset"})

	DatasetExportFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_export_failures_total",
		Help: "Total number of dataset export failures",
	}, []string{"dataset"})

	DatasetExportEmpty = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_export_empty_total",
		Help: "Total number of empty dataset exports",
	}, []string{"dataset"})

	DatasetExportVectors = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_dataset_export_vectors",
		Help: "Number of vectors exported",
	}, []string{"dataset"})

	DatasetExportDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_export_duration_seconds",
		Help:    "Duration of dataset export in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"dataset"})

	DatasetExportBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_export_bytes",
		Help:    "Number of bytes exported",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
	}, []string{"dataset"})

	DatasetImportTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_import_total",
		Help: "Total number of dataset imports",
	}, []string{"dataset"})

	DatasetImportFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_import_failures_total",
		Help: "Total number of dataset import failures",
	}, []string{"dataset"})

	DatasetImportVectors = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_dataset_import_vectors",
		Help: "Number of vectors imported",
	}, []string{"dataset"})

	DatasetImportDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_import_duration_seconds",
		Help:    "Duration of dataset import in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"dataset"})

	DatasetImportBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_import_bytes",
		Help:    "Number of bytes imported",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
	}, []string{"dataset"})
)
