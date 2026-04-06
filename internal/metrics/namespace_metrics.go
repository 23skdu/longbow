package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	NamespaceQPS = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_namespace_queries_total",
			Help: "Total number of queries per namespace",
		},
		[]string{"namespace"},
	)

	NamespaceLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_namespace_query_latency_seconds",
			Help:    "Query latency per namespace",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"namespace"},
	)

	NamespaceStorageBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_namespace_storage_bytes",
			Help: "Current storage usage per namespace",
		},
		[]string{"namespace"},
	)

	NamespaceVectorCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_namespace_vector_count",
			Help: "Current vector count per namespace",
		},
		[]string{"namespace"},
	)

	NamespaceIngestRate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_namespace_ingest_rate_vectors_per_sec",
			Help: "Ingestion rate (vectors per second) per namespace",
		},
		[]string{"namespace"},
	)

	NamespaceQuotaLimit = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_namespace_quota_limit",
			Help: "Quota limit per namespace (0 = unlimited)",
		},
		[]string{"namespace", "quota_type"}, // "vectors", "storage", "dimensions"
	)

	NamespaceQuotaUsed = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_namespace_quota_used",
			Help: "Quota usage per namespace",
		},
		[]string{"namespace", "quota_type"}, // "vectors", "storage", "dimensions"
	)

	NamespaceRateLimitHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_namespace_rate_limit_hits_total",
			Help: "Total number of rate-limited requests per namespace",
		},
		[]string{"namespace"},
	)

	NamespaceCacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_namespace_cache_hits_total",
			Help: "Total cache hits per namespace",
		},
		[]string{"namespace"},
	)

	NamespaceCacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_namespace_cache_misses_total",
			Help: "Total cache misses per namespace",
		},
		[]string{"namespace"},
	)
)

func RecordNamespaceQuery(namespace string, latencySeconds float64) {
	NamespaceQPS.WithLabelValues(namespace).Inc()
	NamespaceLatency.WithLabelValues(namespace).Observe(latencySeconds)
}

func RecordNamespaceStorage(namespace string, bytes int64) {
	NamespaceStorageBytes.WithLabelValues(namespace).Set(float64(bytes))
}

func RecordNamespaceVectors(namespace string, count int64) {
	NamespaceVectorCount.WithLabelValues(namespace).Set(float64(count))
}

func RecordNamespaceIngestRate(namespace string, rate float64) {
	NamespaceIngestRate.WithLabelValues(namespace).Set(rate)
}

func SetNamespaceQuotaLimit(namespace, quotaType string, limit float64) {
	NamespaceQuotaLimit.WithLabelValues(namespace, quotaType).Set(limit)
}

func SetNamespaceQuotaUsed(namespace, quotaType string, used float64) {
	NamespaceQuotaUsed.WithLabelValues(namespace, quotaType).Set(used)
}

func RecordNamespaceRateLimitHit(namespace string) {
	NamespaceRateLimitHits.WithLabelValues(namespace).Inc()
}

func RecordNamespaceCacheHit(namespace string) {
	NamespaceCacheHits.WithLabelValues(namespace).Inc()
}

func RecordNamespaceCacheMiss(namespace string) {
	NamespaceCacheMisses.WithLabelValues(namespace).Inc()
}
