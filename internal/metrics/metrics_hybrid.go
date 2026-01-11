package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Hybrid Search & BM25 Metrics
// =============================================================================

var (
	// HybridBM25ArenaBytes tracks memory used by BM25 arena
	HybridBM25ArenaBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_hybrid_bm25_arena_bytes",
			Help: "Memory used by BM25 arena storage",
		},
		[]string{"dataset"},
	)

	// HybridBM25TokensTotal counts total tokens indexed
	HybridBM25TokensTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hybrid_bm25_tokens_total",
			Help: "Total number of tokens indexed in BM25",
		},
		[]string{"dataset"},
	)

	// HybridBM25PostingListSize tracks posting list sizes
	HybridBM25PostingListSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hybrid_bm25_posting_list_size",
			Help:    "Size of BM25 posting lists",
			Buckets: []float64{1, 5, 10, 50, 100, 500, 1000, 5000},
		},
		[]string{"dataset"},
	)

	// HybridSearchDuration tracks total hybrid search duration
	HybridSearchDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hybrid_search_duration_seconds",
			Help:    "Total duration of hybrid search operations",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		},
		[]string{"dataset"},
	)

	// HybridSearchBM25Duration tracks BM25 component duration
	HybridSearchBM25Duration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hybrid_search_bm25_duration_seconds",
			Help:    "Duration of BM25 scoring in hybrid search",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
		[]string{"dataset"},
	)

	// HybridSearchVectorDuration tracks vector search component duration
	HybridSearchVectorDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hybrid_search_vector_duration_seconds",
			Help:    "Duration of vector search in hybrid search",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
		[]string{"dataset"},
	)

	// HybridSearchMergeDuration tracks result merging duration
	HybridSearchMergeDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_hybrid_search_merge_duration_seconds",
			Help:    "Duration of result merging (RRF) in hybrid search",
			Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01},
		},
		[]string{"dataset"},
	)

	// HybridSearchCacheHits tracks cache hit rate
	HybridSearchCacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hybrid_search_cache_hits_total",
			Help: "Total number of hybrid search cache hits",
		},
		[]string{"dataset"},
	)

	// HybridSearchCacheMisses tracks cache miss rate
	HybridSearchCacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_hybrid_search_cache_misses_total",
			Help: "Total number of hybrid search cache misses",
		},
		[]string{"dataset"},
	)
)

// =============================================================================
// Float16 Graph Metrics
// =============================================================================

var (
	// GraphF16NeighborsTotal counts neighbors stored as Float16
	GraphF16NeighborsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_f16_neighbors_total",
			Help: "Total number of neighbors stored as Float16",
		},
		[]string{"dataset"},
	)

	// GraphF16ConversionErrors counts Float16 conversion errors
	GraphF16ConversionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_f16_conversion_errors_total",
			Help: "Total number of Float16 conversion errors",
		},
		[]string{"dataset"},
	)

	// GraphF16MemorySavings tracks memory savings from Float16
	GraphF16MemorySavings = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_graph_f16_memory_savings_bytes",
			Help: "Memory saved by using Float16 instead of Float32",
		},
		[]string{"dataset"},
	)
)
