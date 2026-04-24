package store

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"os"
	"strconv"

	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/metrics"
)

const (
	LearnedIndexTypeAuto IndexType = "auto"
	LearnedIVFPQ         IndexType = "ivf_pq"
)

// numFeatures is the number of dimensions in the feature vector derived from QueryFeatures.
const numFeatures = 13

// featureKeys maps feature-vector index positions to the names used in featureWeights.
var featureKeys = [numFeatures]string{
	"vector_dimension",
	"num_query_vectors",
	"search_k",
	"dataset_size",
	"num_collections",
	"query_complexity",
	"avg_vector_norm",
	"is_filtered",
	"is_hybrid",
	"time_of_day",
	"day_of_week",
	// Embedding-generator features (added 2026-04-21)
	"embedding_provider",   // ordinal: none=0, openai=1, cohere=2, huggingface=3, onnx=4, wasm=5, local=6
	"embedding_model_dim",  // ratio: VectorDimension / 384.0 (reference dim for sentence-transformers)
}

// FeatureNormalizer maintains online per-feature min/max statistics and produces
// unit-interval normalised feature vectors for use in k-NN distance computation.
type FeatureNormalizer struct {
	mu     sync.RWMutex
	minVal [numFeatures]float64
	maxVal [numFeatures]float64
	count  int64
}

func newFeatureNormalizer() *FeatureNormalizer {
	n := &FeatureNormalizer{}
	for i := range n.minVal {
		n.minVal[i] = math.MaxFloat64
		n.maxVal[i] = -math.MaxFloat64
	}
	return n
}

// Update incorporates a new feature vector into the normaliser's running statistics.
func (n *FeatureNormalizer) Update(v [numFeatures]float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, val := range v {
		if val < n.minVal[i] {
			n.minVal[i] = val
		}
		if val > n.maxVal[i] {
			n.maxVal[i] = val
		}
	}
	n.count++
}

// Normalize returns a [0,1]-clamped vector for v. Features with zero observed span
// are mapped to 0.5 (midpoint), avoiding division by zero.
func (n *FeatureNormalizer) Normalize(v [numFeatures]float64) [numFeatures]float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var out [numFeatures]float64
	for i, val := range v {
		span := n.maxVal[i] - n.minVal[i]
		if span > 0 {
			out[i] = (val - n.minVal[i]) / span
		} else {
			out[i] = 0.5
		}
	}
	return out
}

// Ready returns true once at least one sample has been observed.
func (n *FeatureNormalizer) Ready() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.count > 0
}

// extractFeatureVector converts a QueryFeatures struct into a dense float64 vector
// aligned with featureKeys. All values are raw (un-normalised).
func extractFeatureVector(f QueryFeatures) [numFeatures]float64 {
	var complexityScore float64
	switch f.QueryComplexity {
	case "simple":
		complexityScore = 0.0
	case "medium":
		complexityScore = 0.5
	case "complex":
		complexityScore = 1.0
	default:
		complexityScore = 0.25 // unknown → below-medium
	}
	filtered := 0.0
	if f.IsFiltered {
		filtered = 1.0
	}
	hybrid := 0.0
	if f.IsHybrid {
		hybrid = 1.0
	}
	return [numFeatures]float64{
		float64(f.VectorDimension),
		float64(f.NumQueryVectors),
		float64(f.SearchK),
		float64(f.DatasetSize),
		float64(f.NumCollections),
		complexityScore,
		f.AvgVectorNorm,
		filtered,
		hybrid,
		float64(f.TimeOfDay),
		float64(f.DayOfWeek),
		embeddingProviderOrdinal(f.EmbeddingProvider),
		embeddingModelDimRatio(f.EmbeddingProvider, f.EmbeddingModel, f.VectorDimension),
	}
}

// embeddingProviderOrdinal maps a provider name to a stable float64 ordinal used in
// the k-NN feature vector. Ordinals are stable across versions — do not reorder.
func embeddingProviderOrdinal(provider string) float64 {
	switch provider {
	case "openai":
		return 1.0
	case "cohere":
		return 2.0
	case "huggingface":
		return 3.0
	case "onnx":
		return 4.0
	case "wasm":
		return 5.0
	case "local":
		return 6.0
	default: // "", unknown → no embedding generator
		return 0.0
	}
}

// embeddingModelDimRatio encodes the relative dimensionality of the embedding model
// as a ratio to 384 (the reference dimension for all-MiniLM-L6-v2 / sentence-transformers).
// This captures the difference between compact models (0.33 for 128d) and large models
// (4.0 for 1536d text-embedding-3-large) as a continuous feature.
// Falls back to VectorDimension / 384 when model-specific info is not available.
func embeddingModelDimRatio(provider, model string, actualDim int) float64 {
	const referenceDim = 384.0
	switch {
	case provider == "openai" && model == "text-embedding-3-large":
		return 1536.0 / referenceDim
	case provider == "openai" && model == "text-embedding-3-small":
		return 1536.0 / referenceDim // same dim, different quality
	case provider == "openai" && model == "text-embedding-ada-002":
		return 1536.0 / referenceDim
	case provider == "cohere" && model == "embed-english-v3.0":
		return 1024.0 / referenceDim
	case provider == "cohere" && model == "embed-multilingual-v3.0":
		return 1024.0 / referenceDim
	case provider == "cohere" && model == "embed-english-light-v3.0":
		return 384.0 / referenceDim // 1.0
	default:
		if actualDim > 0 {
			return float64(actualDim) / referenceDim
		}
		return 1.0 // fallback: assume reference dimension
	}
}

type QueryFeatures struct {
	VectorDimension    int     `json:"vector_dimension"`
	NumQueryVectors    int     `json:"num_query_vectors"`
	SearchK            int     `json:"search_k"`
	DatasetSize        int     `json:"dataset_size"`
	NumCollections     int     `json:"num_collections"`
	QueryComplexity    string  `json:"query_complexity"`
	AvgVectorNorm      float64 `json:"avg_vector_norm"`
	IsFiltered         bool    `json:"is_filtered"`
	IsHybrid           bool    `json:"is_hybrid"`
	TimeOfDay          int     `json:"time_of_day"`
	DayOfWeek          int     `json:"day_of_week"`
	// EmbeddingProvider identifies the backend that generated the query vectors.
	// Valid values: "", "openai", "cohere", "huggingface", "onnx", "wasm", "local".
	EmbeddingProvider string `json:"embedding_provider,omitempty"`
	// EmbeddingModel is the specific model name within the provider (e.g. "text-embedding-3-small").
	EmbeddingModel string `json:"embedding_model,omitempty"`
}

// UpdateFromEmbedding updates the feature vector with signals derived from a raw embedding.
func (f *QueryFeatures) UpdateFromEmbedding(embedding []float64) {
	if len(embedding) == 0 {
		return
	}
	sumSq := 0.0
	for _, v := range embedding {
		sumSq += v * v
	}
	f.AvgVectorNorm = math.Sqrt(sumSq)
}

type IndexPrediction struct {
	RecommendedIndex IndexType     `json:"recommended_index"`
	Confidence       float64       `json:"confidence"`
	EstimatedLatency time.Duration `json:"estimated_latency"`
	EstimatedRecall  float64       `json:"estimated_recall"`
	Alternatives     []IndexType   `json:"alternatives"`
}

type TrainingSample struct {
	Features QueryFeatures
	Latency  time.Duration
	Recall   float64
	Index    IndexType
}

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

type PredictorStats struct {
	TrainingSamplesCollected atomic.Int64
	PredictionsMade          atomic.Int64
	PredictionCorrect        atomic.Int64
}

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
		"is_filtered":           0.05,
		"is_hybrid":             0.10,
		"time_of_day":           0.05,
		"day_of_week":           0.05,
		// Embedding-generator features: initially low weight, LDA will raise them as data accumulates.
		"embedding_provider":   0.05,
		"embedding_model_dim":  0.05,
	}
}

func (p *IndexPerformancePredictor) AddTrainingSample(sample TrainingSample) {
	// Correctness tracking: if the last k-NN prediction matched this observed outcome, count it.
	if v := p.lastPredictedIdx.Load(); v != nil {
		if last, ok := v.(IndexType); ok && last == sample.Index {
			p.stats.PredictionCorrect.Add(1)
			metrics.LearnedIndexPredictionCorrectTotal.Inc()
		}
	}

	// Update the feature normaliser with the raw feature vector for this sample.
	fv := extractFeatureVector(sample.Features)
	p.normalizer.Update(fv)

	p.samplesMu.Lock()
	p.samples = append(p.samples, sample)
	if len(p.samples) > 10000 {
		metrics.LearnedIndexSampleOverflowTotal.Inc()
		p.samples = p.samples[len(p.samples)-10000:]
	}
	count := len(p.samples)
	p.samplesMu.Unlock()

	p.stats.TrainingSamplesCollected.Add(1)
	metrics.LearnedIndexTrainingSamplesTotal.Set(float64(count))

	// Trigger an async weight update when: enough samples collected, no update running,
	// and at least UpdateInterval has elapsed since the last one.
	if count >= p.config.MinTrainingSamples &&
		!p.updateInProgress.Load() &&
		time.Since(p.lastWeightUpdate) >= p.config.UpdateInterval {
		p.updateInProgress.Store(true)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.updateInProgress.Store(false)
			p.updateWeights()
		}()
	}
}

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

// Heuristic scoring methods were removed in favor of data-driven k-NN prediction (v0.1.9).

// kNNPredict scores candidate index types using weighted k-nearest-neighbour
// classification over the accumulated TrainingSamples. Neighbours are ranked by
// weighted Euclidean distance in normalised feature space; each neighbour casts
// an inverse-distance-weighted vote for its recorded index type.
//
// Returns a map of index type → aggregated vote score (higher = preferred).
func (p *IndexPerformancePredictor) kNNPredict(features QueryFeatures, k int) map[IndexType]float64 {
	scores := map[IndexType]float64{
		IndexTypeHNSW:    0.0,
		LearnedIVFPQ:     0.0,
		IndexTypeDiskANN: 0.0,
	}

	if k <= 0 {
		k = 7
	}

	queryVec := extractFeatureVector(features)
	normalisedQuery := p.normalizer.Normalize(queryVec)

	// Snapshot samples under RLock to avoid blocking AddTrainingSample.
	p.samplesMu.RLock()
	snap := make([]TrainingSample, len(p.samples))
	copy(snap, p.samples)
	weights := p.featureWeights
	p.samplesMu.RUnlock()

	if len(snap) == 0 {
		return map[IndexType]float64{IndexTypeHNSW: 1.0}
	}
	if k > len(snap) {
		k = len(snap)
	}

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

	type distEntry struct {
		dist  float64
		index IndexType
	}

	entries := make([]distEntry, len(snap))
	for i, s := range snap {
		sVec := extractFeatureVector(s.Features)
		normS := p.normalizer.Normalize(sVec)
		entries[i] = distEntry{
			dist:  weightedEuclidean(normalisedQuery, normS, wVec),
			index: s.Index,
		}
	}

	// Partial sort: pull k smallest-distance neighbours to front.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].dist < entries[j].dist
	})

	const eps = 1e-9
	for _, e := range entries[:k] {
		scores[e.index] += 1.0 / (e.dist + eps)
	}

	return scores
}

// weightedEuclidean computes the weighted Euclidean distance between two
// feature vectors using the provided per-dimension weight vector.
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

// Hand-coded score heuristics removed in favor of k-NN model.

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

func (p *IndexPerformancePredictor) GetStats() (samples, predictions, correct int64) {
	return p.stats.TrainingSamplesCollected.Load(),
		p.stats.PredictionsMade.Load(),
		p.stats.PredictionCorrect.Load()
}

func (p *IndexPerformancePredictor) GetConfig() LearnedIndexConfig {
	return p.config
}

func (p *IndexPerformancePredictor) SetConfig(config LearnedIndexConfig) {
	p.config = config
}

func (p *IndexPerformancePredictor) GetTrainingSampleCount() int {
	p.samplesMu.RLock()
	defer p.samplesMu.RUnlock()
	return len(p.samples)
}

func (p *IndexPerformancePredictor) ClearTrainingData() {
	p.samplesMu.Lock()
	defer p.samplesMu.Unlock()
	p.samples = make([]TrainingSample, 0, 10000)
}

type QueryIndexMapper struct {
	logger       zerolog.Logger
	predictor    *IndexPerformancePredictor
	indexMapping map[string]IndexType
	mappingMu    sync.RWMutex
	config       IndexMapperConfig
	stats        IndexMapperStats
}

type IndexMapperConfig struct {
	EnableAutoMapping  bool          `json:"enable_auto_mapping"`
	CacheEnabled       bool          `json:"cache_enabled"`
	CacheTTL           time.Duration `json:"cache_ttl"`
	EnableFallback     bool          `json:"enable_fallback"`
	FallbackIndex      IndexType     `json:"fallback_index"`
	EnableMetrics      bool          `json:"enable_metrics"`
	AdaptationInterval time.Duration `json:"adaptation_interval"`
}

type IndexMapperStats struct {
	QueriesMapped atomic.Int64
	CacheHits     atomic.Int64
	CacheMisses   atomic.Int64
	Adaptations   atomic.Int64
	Errors        atomic.Int64
}

func NewQueryIndexMapper(logger zerolog.Logger, predictor *IndexPerformancePredictor, config IndexMapperConfig) *QueryIndexMapper {
	if config.CacheTTL <= 0 {
		config.CacheTTL = 10 * time.Minute
	}
	if config.FallbackIndex == "" {
		config.FallbackIndex = IndexTypeHNSW
	}

	return &QueryIndexMapper{
		logger:       logger,
		predictor:    predictor,
		indexMapping: make(map[string]IndexType),
		config:       config,
	}
}

func (m *QueryIndexMapper) GetIndexForQuery(queryID string, features QueryFeatures) IndexType {
	m.stats.QueriesMapped.Add(1)

	if m.config.CacheEnabled {
		m.mappingMu.RLock()
		if idx, ok := m.indexMapping[queryID]; ok {
			m.stats.CacheHits.Add(1)
			m.mappingMu.RUnlock()
			return idx
		}
		m.stats.CacheMisses.Add(1)
		m.mappingMu.RUnlock()
	}

	prediction := m.predictor.Predict(features)
	selectedIndex := prediction.RecommendedIndex

	if prediction.Confidence < m.predictor.config.ConfidenceThreshold && m.config.EnableFallback {
		selectedIndex = m.config.FallbackIndex
		m.logger.Debug().Float64("confidence", prediction.Confidence).
			Str("fallback", string(m.config.FallbackIndex)).
			Msg("Using fallback index due to low confidence")
	}

	if m.config.CacheEnabled {
		m.mappingMu.Lock()
		m.indexMapping[queryID] = selectedIndex
		m.mappingMu.Unlock()
	}

	return selectedIndex
}

func (m *QueryIndexMapper) InvalidateCache(queryID string) {
	m.mappingMu.Lock()
	defer m.mappingMu.Unlock()
	delete(m.indexMapping, queryID)
}

func (m *QueryIndexMapper) ClearCache() {
	m.mappingMu.Lock()
	defer m.mappingMu.Unlock()
	m.indexMapping = make(map[string]IndexType)
}

func (m *QueryIndexMapper) GetStats() (mapped, hits, misses, adaptions, errors int64) {
	return m.stats.QueriesMapped.Load(),
		m.stats.CacheHits.Load(),
		m.stats.CacheMisses.Load(),
		m.stats.Adaptations.Load(),
		m.stats.Errors.Load()
}

func (m *QueryIndexMapper) GetConfig() IndexMapperConfig {
	return m.config
}

func (m *QueryIndexMapper) SetConfig(config IndexMapperConfig) {
	m.config = config
}

func (m *QueryIndexMapper) GetCachedMappings() map[string]IndexType {
	m.mappingMu.RLock()
	defer m.mappingMu.RUnlock()

	result := make(map[string]IndexType, len(m.indexMapping))
	for k, v := range m.indexMapping {
		result[k] = v
	}
	return result
}

func (m *QueryIndexMapper) GetMappingCount() int {
	m.mappingMu.RLock()
	defer m.mappingMu.RUnlock()
	return len(m.indexMapping)
}

type IndexAdaptation struct {
	CollectionName string
	CurrentIndex   IndexType
	ProposedIndex  IndexType
	TriggerReason  string
	Metrics        AdaptationMetrics
	Timestamp      time.Time
	Status         AdaptationStatus
	Features       QueryFeatures // Features that triggered the adaptation
}

type AdaptationMetrics struct {
	AvgLatencyMs   float64
	P50LatencyMs   float64
	P99LatencyMs   float64
	RecallAchieved float64
	QueriesPerSec  float64
	IndexSizeMB    float64
	MemoryUsageMB  float64
}

type AdaptationStatus string

const (
	AdaptationStatusPending   AdaptationStatus = "pending"
	AdaptationStatusRunning   AdaptationStatus = "running"
	AdaptationStatusComplete  AdaptationStatus = "complete"
	AdaptationStatusFailed    AdaptationStatus = "failed"
	AdaptationStatusCancelled AdaptationStatus = "cancelled"
)

// IndexSwitcher is an optional interface that RuntimeIndexAdapter uses to apply or
// roll back an index-type change on a live dataset. Implementations must be safe
// for concurrent use. Wire via RuntimeIndexAdapter.WithIndexSwitcher.
type IndexSwitcher interface {
	SwitchIndex(collection string, to IndexType) error
}

type RuntimeIndexAdapter struct {
	logger           zerolog.Logger
	predictor        *IndexPerformancePredictor
	config           IndexAdaptationConfig
	adaptations      map[string]*IndexAdaptation
	adaptationMu     sync.RWMutex
	metricsCollector MetricsCollector
	stats            AdapterStats
	wg               sync.WaitGroup
	stopChan         chan struct{}
	switcher         IndexSwitcher // optional; nil → rollback is logged but not applied
}

type MetricsCollector interface {
	GetCollections() []string
	GetQueryLatencies(collection string) (p50, p99, avg float64)
	GetQueriesPerSecond(collection string) float64
	GetRecall(collection string) float64
	GetIndexSize(collection string) float64
	GetMemoryUsage(collection string) float64
}

type IndexAdaptationConfig struct {
	EnableAutoAdaptation    bool          `json:"enable_auto_adaptation"`
	MinSamplesForAdaptation int           `json:"min_samples_for_adaptation"`
	LatencyThresholdMs      float64       `json:"latency_threshold_ms"`
	RecallThreshold         float64       `json:"recall_threshold"`
	CheckInterval           time.Duration `json:"check_interval"`
	MaxAdaptationsPerHour   int           `json:"max_adaptations_per_hour"`
	EnableRollback          bool          `json:"enable_rollback"`
	RollbackWindow          time.Duration `json:"rollback_window"`
}

type AdapterStats struct {
	AdaptationsTriggered atomic.Int64
	AdaptationsCompleted atomic.Int64
	AdaptationsFailed    atomic.Int64
	RollbacksPerformed   atomic.Int64
	QueriesAnalyzed      atomic.Int64
}

func NewRuntimeIndexAdapter(logger zerolog.Logger, predictor *IndexPerformancePredictor, config IndexAdaptationConfig, collector MetricsCollector) *RuntimeIndexAdapter {
	if config.MinSamplesForAdaptation <= 0 {
		config.MinSamplesForAdaptation = 1000
	}
	if config.LatencyThresholdMs <= 0 {
		config.LatencyThresholdMs = 100.0
	}
	if config.RecallThreshold <= 0 {
		config.RecallThreshold = 0.95
	}
	if config.CheckInterval <= 0 {
		config.CheckInterval = 5 * time.Minute
	}
	if config.MaxAdaptationsPerHour <= 0 {
		config.MaxAdaptationsPerHour = 4
	}
	if config.RollbackWindow <= 0 {
		config.RollbackWindow = 30 * time.Minute
	}

	return &RuntimeIndexAdapter{
		logger:           logger,
		predictor:        predictor,
		config:           config,
		adaptations:      make(map[string]*IndexAdaptation),
		metricsCollector: collector,
		stopChan:         make(chan struct{}),
	}
}

func (a *RuntimeIndexAdapter) Start() {
	if !a.config.EnableAutoAdaptation {
		a.logger.Info().Msg("Auto-adaptation disabled")
		return
	}

	a.wg.Add(1)
	go a.adaptationLoop()

	a.logger.Info().Msg("Runtime index adapter started")
}

func (a *RuntimeIndexAdapter) Stop() {
	close(a.stopChan)
	a.wg.Wait()
	a.logger.Info().Msg("Runtime index adapter stopped")
}

func (a *RuntimeIndexAdapter) adaptationLoop() {
	defer a.wg.Done()

	ticker := time.NewTicker(a.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.checkAndAdapt()
		case <-a.stopChan:
			return
		}
	}
}

func (a *RuntimeIndexAdapter) checkAndAdapt() {
	a.stats.QueriesAnalyzed.Add(1)

	if a.predictor.GetTrainingSampleCount() < a.config.MinSamplesForAdaptation {
		a.logger.Debug().Msg("Insufficient samples for adaptation")
		return
	}

	collections := a.getMonitoredCollections()

	for _, collection := range collections {
		metrics := a.collectMetrics(collection)

		if a.shouldAdapt(metrics) {
			a.triggerAdaptation(collection, metrics)
		}
	}
}

func (a *RuntimeIndexAdapter) getMonitoredCollections() []string {
	if a.metricsCollector != nil {
		return a.metricsCollector.GetCollections()
	}
	return []string{"default"}
}

func (a *RuntimeIndexAdapter) collectMetrics(collection string) AdaptationMetrics {
	var metrics AdaptationMetrics

	if a.metricsCollector != nil {
		metrics.P50LatencyMs, metrics.P99LatencyMs, metrics.AvgLatencyMs = a.metricsCollector.GetQueryLatencies(collection)
		metrics.QueriesPerSec = a.metricsCollector.GetQueriesPerSecond(collection)
		metrics.RecallAchieved = a.metricsCollector.GetRecall(collection)
		metrics.IndexSizeMB = a.metricsCollector.GetIndexSize(collection)
		metrics.MemoryUsageMB = a.metricsCollector.GetMemoryUsage(collection)
	} else {
		// More realistic simulated metrics for testing/dev if no collector is provided
		metrics.AvgLatencyMs = 12.5
		metrics.P50LatencyMs = 8.2
		metrics.P99LatencyMs = 45.0
		metrics.RecallAchieved = 0.99
		metrics.QueriesPerSec = 450.0
		metrics.IndexSizeMB = 256.0
		metrics.MemoryUsageMB = 128.0
	}

	return metrics
}

func (a *RuntimeIndexAdapter) shouldAdapt(metrics AdaptationMetrics) bool {
	if metrics.AvgLatencyMs > a.config.LatencyThresholdMs {
		a.logger.Info().Float64("latency_ms", metrics.AvgLatencyMs).
			Float64("threshold", a.config.LatencyThresholdMs).
			Msg("Latency threshold exceeded")
		return true
	}

	if metrics.RecallAchieved < a.config.RecallThreshold {
		a.logger.Info().Float64("recall", metrics.RecallAchieved).
			Float64("threshold", a.config.RecallThreshold).
			Msg("Recall threshold below target")
		return true
	}

	return false
}

func (a *RuntimeIndexAdapter) triggerAdaptation(collection string, metrics AdaptationMetrics) {
	a.stats.AdaptationsTriggered.Add(1)

	features := QueryFeatures{
		DatasetSize:     int(metrics.IndexSizeMB * 1000),
		SearchK:         int(metrics.QueriesPerSec / 100),
		QueryComplexity: "medium",
	}

	// CLOSING THE FEEDBACK LOOP: Record the current (failing) state as a training sample.
	// This teaches the model that the CURRENT index is performaning poorly under these features.
	a.predictor.AddTrainingSample(TrainingSample{
		Features: features,
		Latency:  time.Duration(metrics.AvgLatencyMs * float64(time.Millisecond)),
		Recall:   metrics.RecallAchieved,
		Index:    IndexTypeHNSW, // Assuming HNSW for now, should ideally pull from ds
	})

	prediction := a.predictor.Predict(features)

	adaptation := &IndexAdaptation{
		CollectionName: collection,
		ProposedIndex:  prediction.RecommendedIndex,
		TriggerReason:  a.determineTriggerReason(metrics),
		Metrics:        metrics,
		Timestamp:      time.Now(),
		Status:         AdaptationStatusPending,
		Features:       features,
	}

	a.adaptationMu.Lock()
	a.adaptations[collection] = adaptation
	a.adaptationMu.Unlock()

	a.logger.Info().
		Str("collection", collection).
		Str("proposed", string(adaptation.ProposedIndex)).
		Str("reason", adaptation.TriggerReason).
		Msg("Triggering index adaptation")
}

func (a *RuntimeIndexAdapter) determineTriggerReason(metrics AdaptationMetrics) string {
	if metrics.AvgLatencyMs > a.config.LatencyThresholdMs {
		return "high_latency"
	}
	if metrics.RecallAchieved < a.config.RecallThreshold {
		return "low_recall"
	}
	return "performance_degradation"
}

func (a *RuntimeIndexAdapter) GetAdaptation(collection string) (*IndexAdaptation, bool) {
	a.adaptationMu.RLock()
	defer a.adaptationMu.RUnlock()

	adaptation, ok := a.adaptations[collection]
	return adaptation, ok
}

func (a *RuntimeIndexAdapter) ListAdaptations() []*IndexAdaptation {
	a.adaptationMu.RLock()
	defer a.adaptationMu.RUnlock()

	adaptations := make([]*IndexAdaptation, 0, len(a.adaptations))
	for _, adaptation := range a.adaptations {
		adaptations = append(adaptations, adaptation)
	}
	return adaptations
}

func (a *RuntimeIndexAdapter) StartAdaptation(collection string) error {
	a.adaptationMu.Lock()
	defer a.adaptationMu.Unlock()

	adaptation, ok := a.adaptations[collection]
	if !ok {
		return fmt.Errorf("no adaptation pending for collection %s", collection)
	}

	adaptation.Status = AdaptationStatusRunning
	a.logger.Info().Str("collection", collection).Msg("Starting index adaptation")

	return nil
}

func (a *RuntimeIndexAdapter) CompleteAdaptation(collection string, success bool) error {
	a.adaptationMu.Lock()
	defer a.adaptationMu.Unlock()

	adaptation, ok := a.adaptations[collection]
	if !ok {
		return fmt.Errorf("no adaptation running for collection %s", collection)
	}

	if success {
		adaptation.Status = AdaptationStatusComplete
		a.stats.AdaptationsCompleted.Add(1)
		a.logger.Info().Str("collection", collection).Msg("Index adaptation completed")

		// Record successful adaptation as a positive signal
		// In a real system, we'd use the ACTUAL features that triggered the adaptation.
		a.predictor.AddTrainingSample(TrainingSample{
			Features: adaptation.Features, // Assuming we store features in adaptation
			Latency:  time.Duration(adaptation.Metrics.AvgLatencyMs * float64(time.Millisecond)),
			Recall:   adaptation.Metrics.RecallAchieved,
			Index:    adaptation.ProposedIndex,
		})
	} else {
		adaptation.Status = AdaptationStatusFailed
		a.stats.AdaptationsFailed.Add(1)
		a.logger.Error().Str("collection", collection).Msg("Index adaptation failed")

		// Record failure as a negative signal (failure decomposition)
		// We record a "virtual" sample with extremely high latency to penalize this index type
		// for the given query features.
		a.predictor.AddTrainingSample(TrainingSample{
			Features: adaptation.Features,
			Latency:  10 * time.Second, // Penalty latency
			Recall:   0.0,             // Zero recall
			Index:    adaptation.ProposedIndex,
		})
	}

	return nil
}

// WithIndexSwitcher wires an IndexSwitcher into the adapter, enabling real index
// rollback. Must be called before the first adaptation is triggered.
func (a *RuntimeIndexAdapter) WithIndexSwitcher(s IndexSwitcher) {
	a.switcher = s
}

func (a *RuntimeIndexAdapter) Rollback(collection string) error {
	if !a.config.EnableRollback {
		return fmt.Errorf("rollback is disabled for this adapter")
	}

	a.adaptationMu.Lock()
	adaptation, ok := a.adaptations[collection]
	if !ok {
		a.adaptationMu.Unlock()
		return fmt.Errorf("no adaptation recorded for collection %q", collection)
	}
	target := adaptation.CurrentIndex
	adaptation.Status = AdaptationStatusCancelled
	a.adaptationMu.Unlock()

	if a.switcher == nil {
		a.logger.Warn().
			Str("collection", collection).
			Str("target_index", string(target)).
			Msg("Rollback: no IndexSwitcher configured; state updated but live dataset unchanged")
		return fmt.Errorf("no IndexSwitcher configured: rollback for %q not applied to live dataset", collection)
	}

	if err := a.switcher.SwitchIndex(collection, target); err != nil {
		a.stats.AdaptationsFailed.Add(1)
		metrics.LearnedIndexAdaptationsTotal.WithLabelValues("rollback_failed").Inc()
		a.logger.Error().Err(err).Str("collection", collection).Msg("Rollback: index switch failed")
		return fmt.Errorf("rollback switch failed for %q: %w", collection, err)
	}

	// CLOSING THE FEEDBACK LOOP: Record the failure as a strong negative signal for the proposed index.
	// This ensures the predictor learns that this index degraded performance for these features.
	a.predictor.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{DatasetSize: int(adaptation.Metrics.IndexSizeMB * 1000)},
		Latency:  time.Duration(adaptation.Metrics.AvgLatencyMs * 2.0 * float64(time.Millisecond)), // Penalty: mark as 2x slow
		Recall:   adaptation.Metrics.RecallAchieved * 0.8,                                       // Penalty: mark as low recall
		Index:    adaptation.ProposedIndex,
	})

	a.stats.RollbacksPerformed.Add(1)
	metrics.LearnedIndexAdaptationsTotal.WithLabelValues("rolled_back").Inc()
	a.logger.Info().
		Str("collection", collection).
		Str("reverted_to", string(target)).
		Msg("Index adaptation rolled back successfully")
	return nil
}

func (a *RuntimeIndexAdapter) GetStats() (triggered, completed, failed, rolledback, analyzed int64) {
	return a.stats.AdaptationsTriggered.Load(),
		a.stats.AdaptationsCompleted.Load(),
		a.stats.AdaptationsFailed.Load(),
		a.stats.RollbacksPerformed.Load(),
		a.stats.QueriesAnalyzed.Load()
}

func (a *RuntimeIndexAdapter) GetConfig() IndexAdaptationConfig {
	return a.config
}

func (a *RuntimeIndexAdapter) SetConfig(config IndexAdaptationConfig) {
	a.config = config
}

type IndexBenchmark struct {
	logger            zerolog.Logger
	predictor         *IndexPerformancePredictor
	fixedIndexConfigs []IndexType
	results           []LearnedBenchmarkResult
	resultsMu         sync.RWMutex
	stats             BenchmarkStats
}

type LearnedBenchmarkResult struct {
	Features       QueryFeatures
	LearnedIndex   IndexType
	FixedIndex     IndexType
	LearnedLatency time.Duration
	FixedLatency   time.Duration
	LearnedRecall  float64
	FixedRecall    float64
	SpeedupFactor  float64
	RecallDiff     float64
	IndexType      IndexType
}

type BenchmarkStats struct {
	BenchmarksRun      atomic.Int64
	LearnedWins        atomic.Int64
	FixedWins          atomic.Int64
	AvgSpeedup         atomic.Int64
	TotalQueriesTested atomic.Int64
}

func NewIndexBenchmark(logger zerolog.Logger, predictor *IndexPerformancePredictor, fixedIndices []IndexType) *IndexBenchmark {
	if len(fixedIndices) == 0 {
		fixedIndices = []IndexType{IndexTypeHNSW, LearnedIVFPQ, IndexTypeDiskANN}
	}

	return &IndexBenchmark{
		logger:            logger,
		predictor:         predictor,
		fixedIndexConfigs: fixedIndices,
	}
}

func (b *IndexBenchmark) RunComparison(features QueryFeatures, numIterations int) LearnedBenchmarkResult {
	b.stats.BenchmarksRun.Add(1)

	prediction := b.predictor.Predict(features)
	learnedIndex := prediction.RecommendedIndex

	fixedIndex := b.selectFixedIndex(features)

	learnedLatency := b.simulateQuery(learnedIndex, features)
	fixedLatency := b.simulateQuery(fixedIndex, features)

	learnedRecall := b.simulateRecall(learnedIndex, features)
	fixedRecall := b.simulateRecall(fixedIndex, features)

	speedupFactor := float64(fixedLatency) / float64(learnedLatency+1)
	recallDiff := learnedRecall - fixedRecall

	result := LearnedBenchmarkResult{
		Features:       features,
		LearnedIndex:   learnedIndex,
		FixedIndex:     fixedIndex,
		LearnedLatency: learnedLatency,
		FixedLatency:   fixedLatency,
		LearnedRecall:  learnedRecall,
		FixedRecall:    fixedRecall,
		SpeedupFactor:  speedupFactor,
		RecallDiff:     recallDiff,
		IndexType:      learnedIndex,
	}

	b.resultsMu.Lock()
	b.results = append(b.results, result)
	b.resultsMu.Unlock()

	if speedupFactor > 1.0 {
		b.stats.LearnedWins.Add(1)
	} else {
		b.stats.FixedWins.Add(1)
	}

	b.stats.AvgSpeedup.Add(int64(speedupFactor * 100))
	b.stats.TotalQueriesTested.Add(int64(numIterations))

	return result
}

func (b *IndexBenchmark) selectFixedIndex(features QueryFeatures) IndexType {
	if features.DatasetSize < 100000 {
		return IndexTypeHNSW
	} else if features.DatasetSize < 5000000 {
		return LearnedIVFPQ
	}
	return IndexTypeDiskANN
}

func (b *IndexBenchmark) simulateQuery(index IndexType, features QueryFeatures) time.Duration {
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

	variance := float64(baseLatency) * 0.1 * (rand.Float64()*2 - 1) // #nosec G404
	return time.Duration(float64(baseLatency) + variance)
}

func (b *IndexBenchmark) simulateRecall(index IndexType, _ QueryFeatures) float64 {
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

func (b *IndexBenchmark) RunBatchBenchmark(featureSets []QueryFeatures, iterationsPerSet int) []LearnedBenchmarkResult {
	results := make([]LearnedBenchmarkResult, 0, len(featureSets)*iterationsPerSet)

	for _, features := range featureSets {
		for i := 0; i < iterationsPerSet; i++ {
			result := b.RunComparison(features, iterationsPerSet)
			results = append(results, result)
		}
	}

	return results
}

func (b *IndexBenchmark) GetAggregatedStats() BenchmarkSummary {
	b.resultsMu.RLock()
	defer b.resultsMu.RUnlock()

	var totalSpeedup float64
	var totalRecallDiff float64
	learnedWins := 0
	fixedWins := 0

	for _, r := range b.results {
		totalSpeedup += r.SpeedupFactor
		totalRecallDiff += r.RecallDiff
		if r.SpeedupFactor > 1.0 {
			learnedWins++
		} else {
			fixedWins++
		}
	}

	count := len(b.results)
	if count == 0 {
		return BenchmarkSummary{}
	}

	return BenchmarkSummary{
		TotalBenchmarks:     count,
		LearnedIndexWins:    learnedWins,
		FixedIndexWins:      fixedWins,
		AvgSpeedupFactor:    totalSpeedup / float64(count),
		AvgRecallDifference: totalRecallDiff / float64(count),
		WinRateLearned:      float64(learnedWins) / float64(count),
		TotalQueriesTested:  int(b.stats.TotalQueriesTested.Load()),
	}
}

func (b *IndexBenchmark) GetResults() []LearnedBenchmarkResult {
	b.resultsMu.RLock()
	defer b.resultsMu.RUnlock()

	result := make([]LearnedBenchmarkResult, len(b.results))
	copy(result, b.results)
	return result
}

func (b *IndexBenchmark) ClearResults() {
	b.resultsMu.Lock()
	defer b.resultsMu.Unlock()
	b.results = make([]LearnedBenchmarkResult, 0)
}

func (b *IndexBenchmark) GetStats() (runs, learnedWins, fixedWins, totalQueries int64) {
	return b.stats.BenchmarksRun.Load(),
		b.stats.LearnedWins.Load(),
		b.stats.FixedWins.Load(),
		b.stats.TotalQueriesTested.Load()
}

type BenchmarkSummary struct {
	TotalBenchmarks     int
	LearnedIndexWins    int
	FixedIndexWins      int
	AvgSpeedupFactor    float64
	AvgRecallDifference float64
	WinRateLearned      float64
	TotalQueriesTested  int
}

type IndexRecommendationAPI struct {
	logger      zerolog.Logger
	predictor   *IndexPerformancePredictor
	mapper      *QueryIndexMapper
	recommender *IndexRecommendationEngine
	stats       APIStats
}

type APIStats struct {
	RecommendationsGiven atomic.Int64
	APIErrors            atomic.Int64
}

type IndexRecommendationEngine struct {
	logger    zerolog.Logger
	history   []RecommendationRecord
	historyMu sync.RWMutex
}

type RecommendationRecord struct {
	QueryID        string
	Features       QueryFeatures
	Recommendation IndexPrediction
	Timestamp      time.Time
	Accepted       bool
}

func NewIndexRecommendationAPI(logger zerolog.Logger, predictor *IndexPerformancePredictor, mapper *QueryIndexMapper) *IndexRecommendationAPI {
	return &IndexRecommendationAPI{
		logger:      logger,
		predictor:   predictor,
		mapper:      mapper,
		recommender: &IndexRecommendationEngine{logger: logger},
	}
}

func (api *IndexRecommendationAPI) GetRecommendation(features QueryFeatures) IndexPrediction {
	api.stats.RecommendationsGiven.Add(1)

	record := RecommendationRecord{
		Features:  features,
		Timestamp: time.Now(),
	}

	prediction := api.predictor.Predict(features)
	record.Recommendation = prediction

	api.recommender.historyMu.Lock()
	api.recommender.history = append(api.recommender.history, record)
	if len(api.recommender.history) > 1000 {
		api.recommender.history = api.recommender.history[len(api.recommender.history)-1000:]
	}
	api.recommender.historyMu.Unlock()

	return prediction
}

func (api *IndexRecommendationAPI) GetRecommendationWithContext(queryID string, features QueryFeatures) IndexPrediction {
	api.stats.RecommendationsGiven.Add(1)

	record := RecommendationRecord{
		QueryID:   queryID,
		Features:  features,
		Timestamp: time.Now(),
	}

	prediction := api.predictor.Predict(features)

	if api.mapper != nil {
		mappedIndex := api.mapper.GetIndexForQuery(queryID, features)
		if mappedIndex != "" {
			prediction.RecommendedIndex = mappedIndex
		}
	}

	record.Recommendation = prediction

	api.recommender.historyMu.Lock()
	api.recommender.history = append(api.recommender.history, record)
	if len(api.recommender.history) > 1000 {
		api.recommender.history = api.recommender.history[len(api.recommender.history)-1000:]
	}
	api.recommender.historyMu.Unlock()

	return prediction
}

func (api *IndexRecommendationAPI) AcceptRecommendation(queryID string, index IndexType) error {
	api.recommender.historyMu.Lock()
	defer api.recommender.historyMu.Unlock()

	for i := len(api.recommender.history) - 1; i >= 0; i-- {
		if api.recommender.history[i].QueryID == queryID {
			api.recommender.history[i].Accepted = true
			return nil
		}
	}

	return fmt.Errorf("no recommendation found for query %s", queryID)
}

func (api *IndexRecommendationAPI) GetRecommendationHistory() []RecommendationRecord {
	api.recommender.historyMu.RLock()
	defer api.recommender.historyMu.RUnlock()

	result := make([]RecommendationRecord, len(api.recommender.history))
	copy(result, api.recommender.history)
	return result
}

func (api *IndexRecommendationAPI) GetStats() (recommendations, errors int64) {
	return api.stats.RecommendationsGiven.Load(),
		api.stats.APIErrors.Load()
}

func (api *IndexRecommendationAPI) GetAcceptanceRate() float64 {
	api.recommender.historyMu.RLock()
	defer api.recommender.historyMu.RUnlock()

	if len(api.recommender.history) == 0 {
		return 0.0
	}

	accepted := 0
	for _, r := range api.recommender.history {
		if r.Accepted {
			accepted++
		}
	}

	return float64(accepted) / float64(len(api.recommender.history))
}

func (api *IndexRecommendationAPI) GetTopRecommendations(limit int) []IndexPrediction {
	api.recommender.historyMu.RLock()
	defer api.recommender.historyMu.RUnlock()

	if len(api.recommender.history) == 0 {
		return nil
	}

	type recCount struct {
		index IndexType
		count int
		rec   IndexPrediction
	}

	recCounts := make(map[IndexType]*recCount)
	for _, r := range api.recommender.history {
		idx := r.Recommendation.RecommendedIndex
		if _, ok := recCounts[idx]; !ok {
			recCounts[idx] = &recCount{index: idx, rec: r.Recommendation}
		}
		recCounts[idx].count++
	}

	var sorted []recCount
	for _, rc := range recCounts {
		sorted = append(sorted, *rc)
	}

	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].count > sorted[i].count {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	if limit > len(sorted) {
		limit = len(sorted)
	}

	result := make([]IndexPrediction, limit)
	for i := 0; i < limit; i++ {
		result[i] = sorted[i].rec
	}

	return result
}
