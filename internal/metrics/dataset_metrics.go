package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// StoreDroppedDatasets tracks the total number of datasets explicitly dropped.
	StoreDroppedDatasets = promauto.NewCounter(prometheus.CounterOpts{
		Name: "longbow_store_dropped_datasets_total",
		Help: "Total number of datasets explicitly dropped",
	})

	// StoreActiveDatasets is a store active datasets value.
	StoreActiveDatasets = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "longbow_store_active_datasets",
		Help: "Current number of active datasets in memory",
	})

	// DatasetExportTotal is a dataset export total value.
	DatasetExportTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_export_total",
		Help: "Total number of dataset exports",
	}, []string{"dataset"})

	// DatasetExportFailures is a dataset export failures value.
	DatasetExportFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_export_failures_total",
		Help: "Total number of dataset export failures",
	}, []string{"dataset"})

	// DatasetExportEmpty is a dataset export empty value.
	DatasetExportEmpty = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_export_empty_total",
		Help: "Total number of empty dataset exports",
	}, []string{"dataset"})

	// DatasetExportVectors is a dataset export vectors value.
	DatasetExportVectors = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_dataset_export_vectors",
		Help: "Number of vectors exported",
	}, []string{"dataset"})

	// DatasetExportDuration is a dataset export duration value.
	DatasetExportDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_export_duration_seconds",
		Help:    "Duration of dataset export in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"dataset"})

	// DatasetExportBytes is a dataset export bytes value.
	DatasetExportBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_export_bytes",
		Help:    "Number of bytes exported",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
	}, []string{"dataset"})

	// DatasetImportTotal is a dataset import total value.
	DatasetImportTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_import_total",
		Help: "Total number of dataset imports",
	}, []string{"dataset"})

	// DatasetImportFailures is a dataset import failures value.
	DatasetImportFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "longbow_dataset_import_failures_total",
		Help: "Total number of dataset import failures",
	}, []string{"dataset"})

	// DatasetImportVectors is a dataset import vectors value.
	DatasetImportVectors = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "longbow_dataset_import_vectors",
		Help: "Number of vectors imported",
	}, []string{"dataset"})

	// DatasetImportDuration is a dataset import duration value.
	DatasetImportDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_import_duration_seconds",
		Help:    "Duration of dataset import in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"dataset"})

	// DatasetImportBytes is a dataset import bytes value.
	DatasetImportBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "longbow_dataset_import_bytes",
		Help:    "Number of bytes imported",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 20),
	}, []string{"dataset"})
)
