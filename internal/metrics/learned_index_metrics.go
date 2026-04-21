package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// =============================================================================
// Learned Index (k-NN Classifier) Metrics
// =============================================================================

var (
	// LearnedIndexPredictionsTotal counts index-type recommendations by scoring method.
	// method label: "knn" (k-NN scorer over training samples),
	//               "heuristic" (hand-coded fallback used when kNN is unavailable),
	//               "default" (pre-training fallback, MinTrainingSamples not yet met).
	LearnedIndexPredictionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_learned_index_predictions_total",
			Help: "Total number of index-type recommendations issued by the learned index, labeled by chosen index and scoring method.",
		},
		[]string{"index_type", "method"},
	)

	// LearnedIndexPredictionCorrectTotal counts predictions that were subsequently
	// confirmed correct via AddTrainingSample feedback.
	LearnedIndexPredictionCorrectTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_learned_index_prediction_correct_total",
			Help: "Total number of learned index predictions confirmed correct by subsequent training sample feedback.",
		},
	)

	// LearnedIndexTrainingSamplesTotal is the current depth of the training sample buffer.
	// Gauge (not counter) because old samples are evicted when the 10k cap is reached.
	LearnedIndexTrainingSamplesTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "longbow_learned_index_training_samples_total",
			Help: "Current number of training samples held in the learned index buffer (max 10,000).",
		},
	)

	// LearnedIndexKNNDurationSeconds measures wall-clock time for a single k-NN scoring pass.
	LearnedIndexKNNDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_learned_index_knn_duration_seconds",
			Help:    "Wall-clock time for one k-NN scoring pass over the training sample buffer.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5},
		},
	)

	// LearnedIndexWeightUpdateDurationSeconds measures wall-clock time for one LDA-based
	// online feature weight update triggered after sufficient new samples accumulate.
	LearnedIndexWeightUpdateDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_learned_index_weight_update_duration_seconds",
			Help:    "Wall-clock time for one online feature-weight update (LDA between-class variance).",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
	)

	// LearnedIndexAdaptationsTotal counts adaptation lifecycle events.
	// status label: "triggered", "completed", "failed", "rolled_back", "rollback_failed".
	LearnedIndexAdaptationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "longbow_learned_index_adaptations_total",
			Help: "Total number of index adaptation events by lifecycle status.",
		},
		[]string{"status"},
	)

	// LearnedIndexAdaptationLatencyGainMs measures the latency delta (before − after)
	// following a completed adaptation. Positive values indicate improvement.
	LearnedIndexAdaptationLatencyGainMs = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "longbow_learned_index_adaptation_latency_gain_ms",
			Help:    "Observed latency delta (before_ms - after_ms) after a completed index adaptation. Positive = improvement.",
			Buckets: []float64{-100, -50, -10, 0, 10, 25, 50, 100, 250, 500},
		},
	)

	// LearnedIndexSampleOverflowTotal counts how many times the 10k sample cap was
	// reached and the oldest batch was discarded.
	LearnedIndexSampleOverflowTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "longbow_learned_index_sample_overflow_total",
			Help: "Total number of times the training sample buffer exceeded 10,000 entries and oldest samples were evicted.",
		},
	)
)
