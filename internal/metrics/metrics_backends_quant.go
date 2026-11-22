package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// =============================================================================
	// Remote Storage Metrics (S3/GCS)
	// =============================================================================

	// RemoteStorageOpsTotal is a remote storage ops total value.
	RemoteStorageOpsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_remote_storage_ops_total",
			Help: "Total number of remote storage operations",
		},
		[]string{"provider", "operation", "status"}, // provider: "s3", "gcs". operation: "put", "get", "delete", "exists". status: "success", "error"
	)

	// RemoteStorageDurationSeconds is a remote storage duration seconds value.
	RemoteStorageDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_remote_storage_duration_seconds",
			Help:    "Duration of remote storage operations",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 15, 30, 60},
		},
		[]string{"provider", "operation"},
	)

	// RemoteStorageUploadBytes is a remote storage upload bytes value.
	RemoteStorageUploadBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_remote_storage_upload_bytes_total",
			Help: "Total bytes uploaded to remote storage",
		},
		[]string{"provider"},
	)

	// RemoteStorageDownloadBytes is a remote storage download bytes value.
	RemoteStorageDownloadBytes = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_remote_storage_download_bytes_total",
			Help: "Total bytes downloaded from remote storage",
		},
		[]string{"provider"},
	)

	// LockNodeSpinCyclesTotal Metrics
	LockNodeSpinCyclesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_lock_node_spin_cycles_total",
			Help: "Total number of spin cycles performed by LockNode spinlocks",
		},
		[]string{"dataset", "layer"},
	)

	// InsertMuWaitDurationSeconds is a insert mu wait duration seconds value.
	InsertMuWaitDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_insert_mu_wait_duration_seconds",
			Help:    "Time spent waiting for insertMus sharded locks",
			Buckets: []float64{0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1},
		},
		[]string{"dataset"},
	)

	// =============================================================================
	// ML & Embedding Metrics
	// =============================================================================

	// EmbeddingGenerationDurationSeconds is a embedding generation duration seconds value.
	EmbeddingGenerationDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_embedding_generation_duration_seconds",
			Help:    "Time spent generating a batch of embeddings",
			Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"provider", "model"},
	)

	// EmbeddingPoolingDurationSeconds is a embedding pooling duration seconds value.
	EmbeddingPoolingDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_embedding_pooling_duration_seconds",
			Help:    "Time spent pooling token embeddings",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05},
		},
	)

	// =============================================================================
	// Vector Quantization (PQ/OPQ) Metrics
	// =============================================================================

	// VQTrainingDurationSeconds is a vq training duration seconds value.
	VQTrainingDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_vq_training_duration_seconds",
			Help:    "Time spent training PQ/OPQ codebooks",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120},
		},
		[]string{"type"}, // "pq", "opq"
	)

	// OPQEncoderWarmupDurationSeconds is a opq encoder warmup duration seconds value.
	OPQEncoderWarmupDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_opq_encoder_warmup_duration_seconds",
			Help:    "Time spent warming up the OPQ encoder during training",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
		[]string{"type"}, // "ivf-opq", etc.
	)

	// VQReconstructionError is a vq reconstruction error value.
	VQReconstructionError = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_vq_reconstruction_error",
			Help: "Current average reconstruction error (MSE) of PQ/OPQ",
		},
		[]string{"type"}, // "pq", "opq"
	)

	// =============================================================================
	// IVF Index Metrics
	// =============================================================================

	// IVFClusterSearchTotal is a ivf cluster search total value.
	IVFClusterSearchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_ivf_cluster_search_total",
			Help: "Total number of clusters searched in IVF-PQ/OPQ",
		},
		[]string{"dataset", "type"},
	)

	// IVFLoadBalanceRatio is a ivf load balance ratio value.
	IVFLoadBalanceRatio = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "longbow_ivf_load_balance_ratio",
			Help: "Ratio of max cluster size to average cluster size (1.0 = perfect balance)",
		},
		[]string{"dataset"},
	)

	// IVFOPQPQcodesPerCluster specific metrics
	IVFOPQPQcodesPerCluster = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_ivfopq_pqcodes_per_cluster",
			Help:    "Number of PQ codes per cluster in IVF-OPQ",
			Buckets: []float64{1, 10, 50, 100, 500, 1000, 5000},
		},
	)

	// IVFOPQLookupHits is a ivfopq lookup hits value.
	IVFOPQLookupHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_ivfopq_lookup_hits_total",
			Help: "Total number of ADC lookup cache hits in IVF-OPQ",
		},
		[]string{"dataset"},
	)

	// TQ2EncodeDurationSeconds metrics (TQ2/TQ4/TQ8)
	TQ2EncodeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq2_encode_duration_seconds",
			Help:    "Time spent encoding vectors with TurboQuant2 (2-bit)",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// TQ2DecodeDurationSeconds is a tq2 decode duration seconds value.
	TQ2DecodeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq2_decode_duration_seconds",
			Help:    "Time spent decoding vectors with TurboQuant2 (2-bit)",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// TQ2CodesPerVector is a tq2 codes per vector value.
	TQ2CodesPerVector = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq2_codes_per_vector",
			Help:    "Number of codes per vector for TurboQuant2 (2-bit)",
			Buckets: []float64{1, 2, 4, 8, 16, 32},
		},
	)

	// TQ4EncodeDurationSeconds is a tq4 encode duration seconds value.
	TQ4EncodeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq4_encode_duration_seconds",
			Help:    "Time spent encoding vectors with TurboQuant4 (4-bit)",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// TQ4DecodeDurationSeconds is a tq4 decode duration seconds value.
	TQ4DecodeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq4_decode_duration_seconds",
			Help:    "Time spent decoding vectors with TurboQuant4 (4-bit)",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// TQ4CodesPerVector is a tq4 codes per vector value.
	TQ4CodesPerVector = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq4_codes_per_vector",
			Help:    "Number of codes per vector for TurboQuant4 (4-bit)",
			Buckets: []float64{1, 2, 4, 8, 16, 32},
		},
	)

	// TQ8EncodeDurationSeconds is a tq8 encode duration seconds value.
	TQ8EncodeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq8_encode_duration_seconds",
			Help:    "Time spent encoding vectors with TurboQuant8 (8-bit)",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// TQ8DecodeDurationSeconds is a tq8 decode duration seconds value.
	TQ8DecodeDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq8_decode_duration_seconds",
			Help:    "Time spent decoding vectors with TurboQuant8 (8-bit)",
			Buckets: []float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// TQ8CodesPerVector is a tq8 codes per vector value.
	TQ8CodesPerVector = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_tq8_codes_per_vector",
			Help:    "Number of codes per vector for TurboQuant8 (8-bit)",
			Buckets: []float64{1, 2, 4, 8, 16, 32},
		},
	)

	// EmbeddingNormalizationDurationSeconds is a embedding normalization duration seconds value.
	EmbeddingNormalizationDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_embedding_normalization_duration_seconds",
			Help:    "Time spent normalizing embedding vectors",
			Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001},
		},
	)

	// WasmInferenceTotal is a wasm inference total value.
	WasmInferenceTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_wasm_inference_total",
			Help: "Total number of WASM-based model inferences",
		},
		[]string{"model", "status"},
	)

	// WasmInferenceDurationSeconds is a wasm inference duration seconds value.
	WasmInferenceDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_wasm_inference_duration_seconds",
			Help:    "Duration of WASM-based model inferences",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"model"},
	)

	// SimdActivationKernelDurationSeconds Activation Kernel Metrics
	SimdActivationKernelDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_simd_activation_kernel_duration_seconds",
			Help:    "Duration of SIMD activation kernel execution",
			Buckets: []float64{0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001},
		},
		[]string{"kernel", "arch"}, // kernel: "exp", "softmax", "sigmoid"; arch: "avx512", "neon", "avx2"
	)

	// SimdActivationKernelCallsTotal is a simd activation kernel calls total value.
	SimdActivationKernelCallsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_simd_activation_kernel_calls_total",
			Help: "Total number of SIMD activation kernel calls",
		},
		[]string{"kernel", "arch"},
	)

	// GraphGPUDispatchTotal GPU Adaptive Dispatch Metrics
	GraphGPUDispatchTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_gpu_dispatch_total",
			Help: "Total GraphRAG expansion calls to GPU backend",
		},
		[]string{"dataset"},
	)

	// GraphGPUDispatchFallbackTotal is a graph gpu dispatch fallback total value.
	GraphGPUDispatchFallbackTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_graph_gpu_dispatch_fallback_total",
			Help: "Total GraphRAG expansion calls falling back to CPU due to threshold",
		},
		[]string{"dataset"},
	)
)

var (
	simdDispatchCache sync.Map // map[string]prometheus.Counter
)

// RecordSimdBatch records multiple SIMD calls at once to reduce contention
func RecordSimdBatch(impl string, op string, count int) {
	if count <= 0 {
		return
	}

	key := impl + ":" + op
	if val, ok := simdDispatchCache.Load(key); ok {
		val.(prometheus.Counter).Add(float64(count))
		return
	}

	counter := SimdDispatchTotal.WithLabelValues(impl, op)
	simdDispatchCache.Store(key, counter)
	counter.Add(float64(count))
}

// =============================================================================
// TPU Backend Metrics
// =============================================================================

var (
	// TPUOperationsTotal tracks the total number of TPU operations.
	TPUOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_tpu_operations_total",
			Help: "Total number of TPU operations",
		},
		[]string{"operation", "status"},
	)
	// TPUOperationLatency is a tpu operation latency value.
	TPUOperationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "longbow_tpu_operation_latency_seconds",
			Help:    "Latency of TPU operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
)
