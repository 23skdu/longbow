package index

import (
	"context"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/metrics"
)

// IndexPerformancePredictor predicts the best index type based on query features.
type IndexPerformancePredictor struct {
	logger           zerolog.Logger
	config           LearnedIndexConfig
	samples          []TrainingSample
	samplesMu        sync.RWMutex
	featureWeights   map[string]float64
	stats            PredictorStats
	wg               sync.WaitGroup
	normalizer       *FeatureNormalizer
	lastWeightUpdate time.Time
	updateInProgress atomic.Bool
	lastPredictedIdx atomic.Value // stores IndexType; written by Predict, read by AddTrainingSample
}

// LearnedIndexRateLimiter protects search hot-paths during background model training.
type LearnedIndexRateLimiter struct {
	predictor *IndexPerformancePredictor
	logger    zerolog.Logger
}

// NewLearnedIndexRateLimiter creates a new rate limiter for the predictor.
func NewLearnedIndexRateLimiter(predictor *IndexPerformancePredictor, logger zerolog.Logger) *LearnedIndexRateLimiter {
	return &LearnedIndexRateLimiter{
		predictor: predictor,
		logger:    logger,
	}
}

// Wait blocks if background training is in progress and search load is high.
// This ensures that model training doesn't starve the search hot-path.
func (rl *LearnedIndexRateLimiter) Wait(ctx context.Context) error {
	if rl.predictor == nil {
		return nil
	}

	// If background update is in progress, introduce a small adaptive delay
	if rl.predictor.updateInProgress.Load() {
		// During training, we introduce a 10-50ms delay to give the trainer some breathing room
		// while still allowing searches to complete.
		select {
		case <-time.After(20 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// PredictorStats tracks the performance and accuracy of the predictor.
type PredictorStats struct {
	TrainingSamplesCollected atomic.Int64
	PredictionsMade          atomic.Int64
	PredictionCorrect        atomic.Int64
}

// LearnedIndexConfig defines the configuration for the learned index predictor.
type LearnedIndexConfig struct {
	EnableAutoSelection bool          `json:"enable_auto_selection"`
	MinTrainingSamples  int           `json:"min_training_samples"`
	ConfidenceThreshold float64       `json:"confidence_threshold"`
	ModelType           string        `json:"model_type"`
	UpdateInterval      time.Duration `json:"update_interval"`
	// KNN is the number of nearest neighbours used when scoring index candidates.
	// Defaults to 7 (odd, avoids ties, smooths noise). Configurable per deployment.
	KNN int `json:"knn"`
}

// NewIndexPerformancePredictor creates a new predictor with the given configuration.
func NewIndexPerformancePredictor(logger zerolog.Logger, config LearnedIndexConfig) *IndexPerformancePredictor {
	if val := os.Getenv("LONGBOW_LEARNED_MIN_SAMPLES"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			config.MinTrainingSamples = i
		}
	}
	if config.MinTrainingSamples <= 0 {
		config.MinTrainingSamples = 100
	}

	if val := os.Getenv("LONGBOW_LEARNED_CONFIDENCE_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			config.ConfidenceThreshold = f
		}
	}
	if config.ConfidenceThreshold <= 0 {
		config.ConfidenceThreshold = 0.7
	}

	if val := os.Getenv("LONGBOW_LEARNED_UPDATE_INTERVAL"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			config.UpdateInterval = d
		}
	}
	if config.UpdateInterval <= 0 {
		config.UpdateInterval = time.Hour
	}
	if config.KNN <= 0 {
		config.KNN = 7
	}

	p := &IndexPerformancePredictor{
		logger:         logger,
		config:         config,
		samples:        make([]TrainingSample, 0, 10000),
		featureWeights: make(map[string]float64),
		normalizer:     newFeatureNormalizer(),
	}

	p.initializeWeights()

	return p
}

func (p *IndexPerformancePredictor) initializeWeights() {
	p.featureWeights = map[string]float64{
		"vector_dimension":  0.15,
		"num_query_vectors": 0.1,
		"search_k":          0.1,
		"dataset_size":      0.2,
		"num_collections":   0.05,
		"query_complexity":  0.1,
		"avg_vector_norm":   0.05,
		"is_filtered":       0.05,
		"is_hybrid":         0.10,
		"time_of_day":       0.05,
		"day_of_week":       0.05,
		// Embedding-generator features: initially low weight, LDA will raise them as data accumulates.
		"embedding_provider":  0.05,
		"embedding_model_dim": 0.05,
	}
}

// Predict estimates the optimal index type and performance for a given set of query features.
func (p *IndexPerformancePredictor) Predict(features QueryFeatures) IndexPrediction {
	p.stats.PredictionsMade.Add(1)

	p.samplesMu.RLock()
	sampleCount := len(p.samples)
	p.samplesMu.RUnlock()

	// If we have ZERO data, fall back to a safe default.
	// Otherwise, we ALWAYS use the k-NN model (data-driven).
	if sampleCount == 0 {
		pred := p.getDefaultPrediction(features)
		metrics.LearnedIndexPredictionsTotal.WithLabelValues(string(pred.RecommendedIndex), "default").Inc()
		return pred
	}

	// k-NN scoring: use accumulated TrainingSamples as our knowledge base.
	knnStart := time.Now()
	scores := p.kNNPredict(features, p.config.KNN)
	metrics.LearnedIndexKNNDurationSeconds.Observe(time.Since(knnStart).Seconds())

	var bestIndex IndexType
	var bestScore float64 = -math.MaxFloat64

	for idx, score := range scores {
		if score > bestScore {
			bestScore = score
			bestIndex = idx
		}
	}

	// Record the prediction so AddTrainingSample can check correctness in the feedback loop.
	p.lastPredictedIdx.Store(bestIndex)

	confidence := p.calculateConfidence(scores)
	latency := p.estimateLatency(features, bestIndex)
	recall := p.estimateRecall(features, bestIndex)
	alternatives := p.getAlternatives(scores, bestIndex)

	metrics.LearnedIndexPredictionsTotal.WithLabelValues(string(bestIndex), "knn").Inc()

	return IndexPrediction{
		RecommendedIndex: bestIndex,
		Confidence:       confidence,
		EstimatedLatency: latency,
		EstimatedRecall:  recall,
		Alternatives:     alternatives,
	}
}

// Pool for knnHeap entries to avoid allocations in kNNPredict
var knnHeapPool = sync.Pool{
	New: func() any {
		return &knnHeap{entries: make([]distEntry, 0, 32)} // typical k=7-15
	},
}

type distEntry struct {
	dist  float64
	index IndexType
}

// kNNPredict scores candidate index types using weighted k-nearest-neighbour
// classification over the accumulated TrainingSamples. Neighbours are ranked by
// weighted Euclidean distance in normalised feature space; each neighbour casts
// an inverse-distance-weighted vote for its recorded index type.
//
// Returns a map of index type → aggregated vote score (higher = preferred).
func (p *IndexPerformancePredictor) kNNPredict(features QueryFeatures, k int) map[IndexType]float64 {
	if k <= 0 {
		k = 7
	}

	queryVec := extractFeatureVector(features)
	normalisedQuery := p.normalizer.Normalize(queryVec)

	// Snapshot weights and sample length under RLock.
	p.samplesMu.RLock()
	if len(p.samples) == 0 {
		p.samplesMu.RUnlock()
		return map[IndexType]float64{IndexTypeHNSW: 1.0}
	}
	weights := p.featureWeights

	// Build a weight vector aligned to featureKeys.
	var wVec [numFeatures]float64
	total := 0.0
	for i, key := range featureKeys {
		w := weights[key]
		if w <= 0 {
			w = 0.01 // floor to prevent zero-weight dimensions
		}
		wVec[i] = w
		total += w
	}
	if total > 0 {
		for i := range wVec {
			wVec[i] /= total
		}
	}

	// Use a max-heap to track top-k neighbors without sorting the entire sample set.
	// This reduces complexity from O(N log N) to O(N log K).
	h := knnHeapPool.Get().(*knnHeap)
	h.entries = h.entries[:0]
	h.k = k

	for _, s := range p.samples {
		sVec := extractFeatureVector(s.Features)
		normS := p.normalizer.Normalize(sVec)
		dist := weightedEuclidean(normalisedQuery, normS, wVec)

		if h.Len() < k {
			h.push(distEntry{dist: dist, index: s.Index})
		} else if dist < h.peek().dist {
			h.pop()
			h.push(distEntry{dist: dist, index: s.Index})
		}
	}
	p.samplesMu.RUnlock()

	scores := map[IndexType]float64{
		IndexTypeHNSW:    0.0,
		LearnedIVFPQ:     0.0,
		IndexTypeDiskANN: 0.0,
	}

	const eps = 1e-9
	for _, e := range h.entries {
		scores[e.index] += 1.0 / (e.dist + eps)
	}

	knnHeapPool.Put(h)
	return scores
}

// knnHeap implements a simple max-heap for distEntry.
type knnHeap struct {
	entries []distEntry
	k       int
}

func (h *knnHeap) Len() int           { return len(h.entries) }
func (h *knnHeap) Less(i, j int) bool { return h.entries[i].dist > h.entries[j].dist } // Max-heap
func (h *knnHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

func (h *knnHeap) push(x distEntry) {
	h.entries = append(h.entries, x)
	h.up(h.Len() - 1)
}

func (h *knnHeap) pop() distEntry {
	n := h.Len() - 1
	h.Swap(0, n)
	h.down(0, n)
	x := h.entries[n]
	h.entries = h.entries[:n]
	return x
}

func (h *knnHeap) peek() distEntry {
	if len(h.entries) == 0 {
		return distEntry{dist: math.MaxFloat64}
	}
	return h.entries[0]
}

func (h *knnHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *knnHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.Less(j2, j) {
			j = j2 // = 2*i + 2  // right child
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

func weightedEuclidean(a, b, w [numFeatures]float64) float64 {
	sum := 0.0
	for i := range a {
		diff := a[i] - b[i]
		sum += w[i] * diff * diff
	}
	return math.Sqrt(sum)
}

// updateWeights recomputes featureWeights using LDA-derived between-class variance.
// For each feature dimension, the weight is proportional to how well that dimension
// separates the three index-type classes (HNSW, IVF-PQ, DiskANN). Run in a goroutine.
func (p *IndexPerformancePredictor) updateWeights() {
	start := time.Now()

	p.samplesMu.RLock()
	snap := make([]TrainingSample, len(p.samples))
	copy(snap, p.samples)
	p.samplesMu.RUnlock()

	if len(snap) < 3 {
		return
	}

	// Compute global mean per feature.
	var globalMean [numFeatures]float64
	for _, s := range snap {
		v := extractFeatureVector(s.Features)
		for i, val := range v {
			globalMean[i] += val
		}
	}
	n := float64(len(snap))
	for i := range globalMean {
		globalMean[i] /= n
	}

	// Accumulate per-class sum and count.
	classSum := make(map[IndexType][numFeatures]float64)
	classCount := make(map[IndexType]float64)
	for _, s := range snap {
		v := extractFeatureVector(s.Features)
		cm := classSum[s.Index]
		for i, val := range v {
			cm[i] += val
		}
		classSum[s.Index] = cm
		classCount[s.Index]++
	}

	// Between-class variance per feature (Fisher criterion numerator).
	var betweenVar [numFeatures]float64
	const floor = 0.01 // prevents zero-weight features
	for idx, sum := range classSum {
		count := classCount[idx]
		if count == 0 {
			continue
		}
		for i, s := range sum {
			classMean := s / count
			diff := classMean - globalMean[i]
			betweenVar[i] += count * diff * diff
		}
	}

	// Normalise to weights summing to 1 (with floor).
	total := 0.0
	for i := range betweenVar {
		betweenVar[i] += floor
		total += betweenVar[i]
	}
	newWeights := make(map[string]float64, numFeatures)
	for i, key := range featureKeys {
		newWeights[key] = betweenVar[i] / total
	}

	p.samplesMu.Lock()
	p.featureWeights = newWeights
	p.lastWeightUpdate = time.Now()
	p.samplesMu.Unlock()

	metrics.LearnedIndexWeightUpdateDurationSeconds.Observe(time.Since(start).Seconds())
	p.logger.Debug().Int("samples", len(snap)).Msg("Learned index: feature weights updated via LDA between-class variance")
}

func (p *IndexPerformancePredictor) getDefaultPrediction(features QueryFeatures) IndexPrediction {
	var defaultIndex IndexType

	if features.DatasetSize < 100000 {
		defaultIndex = IndexTypeHNSW
	} else if features.DatasetSize < 5000000 {
		defaultIndex = LearnedIVFPQ
	} else {
		defaultIndex = IndexTypeDiskANN
	}

	return IndexPrediction{
		RecommendedIndex: defaultIndex,
		Confidence:       0.5,
		EstimatedLatency: 100 * time.Millisecond,
		EstimatedRecall:  0.95,
		Alternatives:     []IndexType{IndexTypeHNSW, LearnedIVFPQ},
	}
}

func (p *IndexPerformancePredictor) calculateConfidence(scores map[IndexType]float64) float64 {
	var sum float64
	var maxScore float64 = -math.MaxFloat64

	for _, score := range scores {
		sum += score
		if score > maxScore {
			maxScore = score
		}
	}

	if sum == 0 {
		return 0.5
	}

	variance := 0.0
	for _, score := range scores {
		diff := score - (sum / 3.0)
		variance += diff * diff
	}
	variance /= 3.0

	confidence := 1.0 - (math.Sqrt(variance) / (maxScore + 0.001))
	confidence = math.Max(0.0, math.Min(1.0, confidence))

	return confidence
}

func (p *IndexPerformancePredictor) estimateLatency(features QueryFeatures, index IndexType) time.Duration {
	baseLatency := time.Millisecond * 50

	switch index {
	case IndexTypeHNSW:
		baseLatency = time.Millisecond * 10
		baseLatency += time.Duration(features.SearchK) * time.Microsecond * 5
	case LearnedIVFPQ:
		baseLatency = time.Millisecond * 20
		baseLatency += time.Duration(features.SearchK) * time.Microsecond * 3
	case IndexTypeDiskANN:
		baseLatency = time.Millisecond * 30
		baseLatency += time.Duration(features.SearchK) * time.Microsecond * 2
	}

	baseLatency += time.Duration(features.NumQueryVectors) * time.Millisecond * 2

	return baseLatency
}

func (p *IndexPerformancePredictor) estimateRecall(_ QueryFeatures, index IndexType) float64 {
	switch index {
	case IndexTypeHNSW:
		return 0.98
	case LearnedIVFPQ:
		return 0.90
	case IndexTypeDiskANN:
		return 0.95
	}
	return 0.95
}

func (p *IndexPerformancePredictor) getAlternatives(scores map[IndexType]float64, best IndexType) []IndexType {
	type scorePair struct {
		index IndexType
		score float64
	}

	var pairs []scorePair
	for idx, score := range scores {
		pairs = append(pairs, scorePair{index: idx, score: score})
	}

	for i := 0; i < len(pairs)-1; i++ {
		for j := i + 1; j < len(pairs); j++ {
			if pairs[j].score > pairs[i].score {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			}
		}
	}

	var alternatives []IndexType
	for _, pair := range pairs {
		if pair.index != best {
			alternatives = append(alternatives, pair.index)
		}
	}

	return alternatives
}
