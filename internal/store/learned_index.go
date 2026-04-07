package store

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

const (
	LearnedIndexTypeAuto IndexType = "auto"
	LearnedIVFPQ         IndexType = "ivf_pq"
)

type QueryFeatures struct {
	VectorDimension int     `json:"vector_dimension"`
	NumQueryVectors int     `json:"num_query_vectors"`
	SearchK         int     `json:"search_k"`
	DatasetSize     int     `json:"dataset_size"`
	NumCollections  int     `json:"num_collections"`
	QueryComplexity string  `json:"query_complexity"`
	AvgVectorNorm   float64 `json:"avg_vector_norm"`
	IsFiltered      bool    `json:"is_filtered"`
	IsHybrid        bool    `json:"is_hybrid"`
	TimeOfDay       int     `json:"time_of_day"`
	DayOfWeek       int     `json:"day_of_week"`
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
	logger         zerolog.Logger
	config         LearnedIndexConfig
	samples        []TrainingSample
	samplesMu      sync.RWMutex
	featureWeights map[string]float64
	stats          PredictorStats
	wg             sync.WaitGroup
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
}

func NewIndexPerformancePredictor(logger zerolog.Logger, config LearnedIndexConfig) *IndexPerformancePredictor {
	if config.MinTrainingSamples <= 0 {
		config.MinTrainingSamples = 100
	}
	if config.ConfidenceThreshold <= 0 {
		config.ConfidenceThreshold = 0.7
	}
	if config.UpdateInterval <= 0 {
		config.UpdateInterval = time.Hour
	}

	p := &IndexPerformancePredictor{
		logger:         logger,
		config:         config,
		samples:        make([]TrainingSample, 0, 10000),
		featureWeights: make(map[string]float64),
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
		"is_hybrid":         0.1,
		"time_of_day":       0.05,
		"day_of_week":       0.05,
	}
}

func (p *IndexPerformancePredictor) AddTrainingSample(sample TrainingSample) {
	p.samplesMu.Lock()
	defer p.samplesMu.Unlock()

	p.samples = append(p.samples, sample)
	if len(p.samples) > 10000 {
		p.samples = p.samples[len(p.samples)-10000:]
	}

	p.stats.TrainingSamplesCollected.Add(1)
}

func (p *IndexPerformancePredictor) Predict(features QueryFeatures) IndexPrediction {
	p.stats.PredictionsMade.Add(1)

	p.samplesMu.RLock()
	sampleCount := len(p.samples)
	p.samplesMu.RUnlock()

	if sampleCount < p.config.MinTrainingSamples {
		return p.getDefaultPrediction(features)
	}

	scores := p.calculateIndexScores(features)

	var bestIndex IndexType
	var bestScore float64 = -math.MaxFloat64

	for idx, score := range scores {
		if score > bestScore {
			bestScore = score
			bestIndex = idx
		}
	}

	confidence := p.calculateConfidence(scores)
	latency := p.estimateLatency(features, bestIndex)
	recall := p.estimateRecall(features, bestIndex)

	alternatives := p.getAlternatives(scores, bestIndex)

	return IndexPrediction{
		RecommendedIndex: bestIndex,
		Confidence:       confidence,
		EstimatedLatency: latency,
		EstimatedRecall:  recall,
		Alternatives:     alternatives,
	}
}

func (p *IndexPerformancePredictor) PredictWithEmbedding(features QueryFeatures, embedding []float64) IndexPrediction {
	p.stats.PredictionsMade.Add(1)

	if len(embedding) == 0 {
		return p.getDefaultPrediction(features)
	}

	embeddingNorm := 0.0
	for _, v := range embedding {
		embeddingNorm += v * v
	}
	embeddingNorm = math.Sqrt(embeddingNorm)

	scores := p.calculateIndexScoresWithEmbedding(features, embedding, embeddingNorm)

	var bestIndex IndexType
	var bestScore float64 = -math.MaxFloat64

	for idx, score := range scores {
		if score > bestScore {
			bestScore = score
			bestIndex = idx
		}
	}

	confidence := p.calculateConfidence(scores)
	latency := p.estimateLatency(features, bestIndex)
	recall := p.estimateRecall(features, bestIndex)

	alternatives := p.getAlternatives(scores, bestIndex)

	return IndexPrediction{
		RecommendedIndex: bestIndex,
		Confidence:       confidence,
		EstimatedLatency: latency,
		EstimatedRecall:  recall,
		Alternatives:     alternatives,
	}
}

func (p *IndexPerformancePredictor) calculateIndexScoresWithEmbedding(features QueryFeatures, embedding []float64, norm float64) map[IndexType]float64 {
	scores := map[IndexType]float64{
		IndexTypeHNSW:    0.0,
		LearnedIVFPQ:     0.0,
		IndexTypeDiskANN: 0.0,
	}

	dimBias := 0.0
	if len(embedding) >= 4 {
		dimBias = math.Abs(embedding[0]) * 0.3
	}
	scores[IndexTypeHNSW] = p.scoreHNSW(features) + dimBias

	complexityBias := 0.0
	if len(embedding) >= 8 {
		complexityBias = embedding[7] * 0.2
	}
	scores[LearnedIVFPQ] = p.scoreIVFPQ(features) + complexityBias

	scaleBias := 0.0
	if len(embedding) >= 16 {
		scaleBias = math.Min(0.3, math.Abs(embedding[15])*0.3)
	}
	scores[IndexTypeDiskANN] = p.scoreDiskANN(features) + scaleBias

	if norm > 0 {
		normFactor := math.Min(0.2, norm/10.0)
		if features.DatasetSize < 100000 {
			scores[IndexTypeHNSW] += normFactor
		} else if features.DatasetSize >= 1000000 {
			scores[IndexTypeDiskANN] += normFactor
		} else {
			scores[LearnedIVFPQ] += normFactor
		}
	}

	return scores
}

func (p *IndexPerformancePredictor) calculateIndexScores(features QueryFeatures) map[IndexType]float64 {
	scores := map[IndexType]float64{
		IndexTypeHNSW:    0.0,
		LearnedIVFPQ:     0.0,
		IndexTypeDiskANN: 0.0,
	}

	scores[IndexTypeHNSW] = p.scoreHNSW(features)
	scores[LearnedIVFPQ] = p.scoreIVFPQ(features)
	scores[IndexTypeDiskANN] = p.scoreDiskANN(features)

	return scores
}

func (p *IndexPerformancePredictor) scoreHNSW(features QueryFeatures) float64 {
	score := 0.0

	if features.DatasetSize < 100000 {
		score += 0.4
	} else if features.DatasetSize < 1000000 {
		score += 0.2
	}

	if features.SearchK < 100 {
		score += 0.3
	} else if features.SearchK < 1000 {
		score += 0.15
	}

	if !features.IsFiltered && !features.IsHybrid {
		score += 0.2
	}

	if features.QueryComplexity == "simple" {
		score += 0.1
	}

	score += p.featureWeights["dataset_size"] * (1.0 - float64(features.DatasetSize)/float64(10_000_000))

	return score
}

func (p *IndexPerformancePredictor) scoreIVFPQ(features QueryFeatures) float64 {
	score := 0.0

	if features.DatasetSize >= 100000 {
		score += 0.3
	}

	if features.NumQueryVectors > 1 {
		score += 0.2
	}

	if features.SearchK >= 100 {
		score += 0.2
	}

	if features.VectorDimension > 512 {
		score += 0.2
	}

	score += p.featureWeights["num_query_vectors"] * float64(features.NumQueryVectors) / 100.0

	return score
}

func (p *IndexPerformancePredictor) scoreDiskANN(features QueryFeatures) float64 {
	score := 0.0

	if features.DatasetSize >= 1000000 {
		score += 0.4
	}

	if features.IsFiltered || features.IsHybrid {
		score += 0.2
	}

	if features.QueryComplexity == "complex" {
		score += 0.2
	}

	score += p.featureWeights["dataset_size"] * math.Min(1.0, float64(features.DatasetSize)/50_000_000.0)

	return score
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

func (p *IndexPerformancePredictor) estimateRecall(features QueryFeatures, index IndexType) float64 {
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
}

type MetricsCollector interface {
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
		metrics.AvgLatencyMs = 50.0
		metrics.P50LatencyMs = 40.0
		metrics.P99LatencyMs = 100.0
		metrics.RecallAchieved = 0.98
		metrics.QueriesPerSec = 1000.0
		metrics.IndexSizeMB = 1000.0
		metrics.MemoryUsageMB = 500.0
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

	prediction := a.predictor.Predict(features)

	adaptation := &IndexAdaptation{
		CollectionName: collection,
		CurrentIndex:   IndexTypeHNSW,
		ProposedIndex:  prediction.RecommendedIndex,
		TriggerReason:  a.determineTriggerReason(metrics),
		Metrics:        metrics,
		Timestamp:      time.Now(),
		Status:         AdaptationStatusPending,
	}

	a.adaptationMu.Lock()
	a.adaptations[collection] = adaptation
	a.adaptationMu.Unlock()

	a.logger.Info().
		Str("collection", collection).
		Str("current", string(adaptation.CurrentIndex)).
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
	} else {
		adaptation.Status = AdaptationStatusFailed
		a.stats.AdaptationsFailed.Add(1)
		a.logger.Error().Str("collection", collection).Msg("Index adaptation failed")
	}

	return nil
}

func (a *RuntimeIndexAdapter) Rollback(collection string) error {
	if !a.config.EnableRollback {
		return fmt.Errorf("rollback is disabled")
	}

	a.stats.RollbacksPerformed.Add(1)
	a.logger.Info().Str("collection", collection).Msg("Rolling back index adaptation")

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

	variance := float64(baseLatency) * 0.1 * (rand.Float64()*2 - 1)
	return time.Duration(float64(baseLatency) + variance)
}

func (b *IndexBenchmark) simulateRecall(index IndexType, features QueryFeatures) float64 {
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
