package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/23skdu/longbow/internal/metrics"
)

// QueryIndexMapper manages the mapping between queries and optimal index types.
type QueryIndexMapper struct {
	logger       zerolog.Logger
	predictor    *IndexPerformancePredictor
	indexMapping map[string]IndexType
	mappingMu    sync.RWMutex
	config       IndexMapperConfig
	stats        IndexMapperStats
}

// IndexMapperConfig defines the configuration for the query-to-index mapper.
type IndexMapperConfig struct {
	EnableAutoMapping  bool          `json:"enable_auto_mapping"`
	CacheEnabled       bool          `json:"cache_enabled"`
	CacheTTL           time.Duration `json:"cache_ttl"`
	EnableFallback     bool          `json:"enable_fallback"`
	FallbackIndex      IndexType     `json:"fallback_index"`
	EnableMetrics      bool          `json:"enable_metrics"`
	AdaptationInterval time.Duration `json:"adaptation_interval"`
}

// IndexMapperStats tracks the mapping operations and cache performance.
type IndexMapperStats struct {
	QueriesMapped atomic.Int64
	CacheHits     atomic.Int64
	CacheMisses   atomic.Int64
	Adaptations   atomic.Int64
	Errors        atomic.Int64
}

// NewQueryIndexMapper creates a new mapper with specified predictor and configuration.
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

// GetIndexForQuery determines the best index type for a given query and its features.
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

// InvalidateCache removes a specific query from the index mapping cache.
func (m *QueryIndexMapper) InvalidateCache(queryID string) {
	m.mappingMu.Lock()
	defer m.mappingMu.Unlock()
	delete(m.indexMapping, queryID)
}

// ClearCache removes all entries from the index mapping cache.
func (m *QueryIndexMapper) ClearCache() {
	m.mappingMu.Lock()
	defer m.mappingMu.Unlock()
	m.indexMapping = make(map[string]IndexType)
}

// GetStats returns the current mapping and cache performance statistics.
func (m *QueryIndexMapper) GetStats() (mapped, hits, misses, adaptions, errors int64) {
	return m.stats.QueriesMapped.Load(),
		m.stats.CacheHits.Load(),
		m.stats.CacheMisses.Load(),
		m.stats.Adaptations.Load(),
		m.stats.Errors.Load()
}

// GetConfig returns the current index mapper configuration.
func (m *QueryIndexMapper) GetConfig() IndexMapperConfig {
	return m.config
}

// SetConfig updates the index mapper configuration.
func (m *QueryIndexMapper) SetConfig(config IndexMapperConfig) {
	m.config = config
}

// GetCachedMappings returns a copy of the current query-to-index mappings.
func (m *QueryIndexMapper) GetCachedMappings() map[string]IndexType {
	m.mappingMu.RLock()
	defer m.mappingMu.RUnlock()

	result := make(map[string]IndexType, len(m.indexMapping))
	for k, v := range m.indexMapping {
		result[k] = v
	}
	return result
}

// GetMappingCount returns the total number of cached query mappings.
func (m *QueryIndexMapper) GetMappingCount() int {
	m.mappingMu.RLock()
	defer m.mappingMu.RUnlock()
	return len(m.indexMapping)
}

// IndexAdaptation records an instance of the system recommending and applying an index change.
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

// AdaptationMetrics captures the performance state that triggered an index adaptation.
type AdaptationMetrics struct {
	AvgLatencyMs   float64
	P50LatencyMs   float64
	P99LatencyMs   float64
	RecallAchieved float64
	QueriesPerSec  float64
	IndexSizeMB    float64
	MemoryUsageMB  float64
}

// AdaptationStatus defines the lifecycle states of an index adaptation process.
type AdaptationStatus string

const (
	// AdaptationStatusPending indicates that an adaptation has been triggered but not yet started.
	AdaptationStatusPending AdaptationStatus = "pending"
	// AdaptationStatusRunning indicates that an adaptation is currently running.
	AdaptationStatusRunning AdaptationStatus = "running"
	// AdaptationStatusComplete indicates that an adaptation has completed successfully.
	AdaptationStatusComplete AdaptationStatus = "complete"
	// AdaptationStatusFailed indicates that an adaptation has failed.
	AdaptationStatusFailed AdaptationStatus = "failed"
	// AdaptationStatusCancelled indicates that an adaptation has been cancelled.
	AdaptationStatusCancelled AdaptationStatus = "cancelled"
)

// IndexSwitcher is an optional interface that RuntimeIndexAdapter uses to apply or
// roll back an index-type change on a live dataset. Implementations must be safe
// for concurrent use. Wire via RuntimeIndexAdapter.WithIndexSwitcher.
type IndexSwitcher interface {
	SwitchIndex(collection string, to IndexType) error
}

// RuntimeIndexAdapter monitors index performance and applies learned index changes in real-time.
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

// MetricsCollector defines an interface for retrieving operational metrics used by the adaptive indexer.
type MetricsCollector interface {
	GetCollections() []string
	GetQueryLatencies(collection string) (p50, p99, avg float64)
	GetQueriesPerSecond(collection string) float64
	GetRecall(collection string) float64
	GetIndexSize(collection string) float64
	GetMemoryUsage(collection string) float64
	GetCurrentIndex(collection string) IndexType
}

// IndexAdaptationConfig defines thresholds and timing for automatic index adaptation.
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

// AdapterStats tracks the operational performance of the RuntimeIndexAdapter.
type AdapterStats struct {
	AdaptationsTriggered atomic.Int64
	AdaptationsCompleted atomic.Int64
	AdaptationsFailed    atomic.Int64
	RollbacksPerformed   atomic.Int64
	QueriesAnalyzed      atomic.Int64
}

// NewRuntimeIndexAdapter creates a new RuntimeIndexAdapter with the provided configuration and dependencies.
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

// Start begins the background monitoring and adaptation loop.
func (a *RuntimeIndexAdapter) Start() {
	if !a.config.EnableAutoAdaptation {
		a.logger.Info().Msg("Auto-adaptation disabled")
		return
	}

	a.wg.Add(1)
	go a.adaptationLoop()

	a.logger.Info().Msg("Runtime index adapter started")
}

// Stop terminates the background adaptation loop and waits for it to finish.
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
		m := a.collectMetrics(collection)
		currentIndex := IndexTypeHNSW
		if a.metricsCollector != nil {
			currentIndex = a.metricsCollector.GetCurrentIndex(collection)
		}

		if a.shouldAdapt(m) {
			a.triggerAdaptation(collection, m, currentIndex)
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
	var m AdaptationMetrics

	if a.metricsCollector != nil {
		m.P50LatencyMs, m.P99LatencyMs, m.AvgLatencyMs = a.metricsCollector.GetQueryLatencies(collection)
		m.QueriesPerSec = a.metricsCollector.GetQueriesPerSecond(collection)
		m.RecallAchieved = a.metricsCollector.GetRecall(collection)
		m.IndexSizeMB = a.metricsCollector.GetIndexSize(collection)
		m.MemoryUsageMB = a.metricsCollector.GetMemoryUsage(collection)
	} else {
		// More realistic simulated metrics for testing/dev if no collector is provided
		m.AvgLatencyMs = 12.5
		m.P50LatencyMs = 8.2
		m.P99LatencyMs = 45.0
		m.RecallAchieved = 0.99
		m.QueriesPerSec = 450.0
		m.IndexSizeMB = 256.0
		m.MemoryUsageMB = 128.0
	}

	return m
}

func (a *RuntimeIndexAdapter) shouldAdapt(m AdaptationMetrics) bool {
	if m.AvgLatencyMs > a.config.LatencyThresholdMs {
		a.logger.Info().Float64("latency_ms", m.AvgLatencyMs).
			Float64("threshold", a.config.LatencyThresholdMs).
			Msg("Latency threshold exceeded")
		return true
	}

	if m.RecallAchieved < a.config.RecallThreshold {
		a.logger.Info().Float64("recall", m.RecallAchieved).
			Float64("threshold", a.config.RecallThreshold).
			Msg("Recall threshold below target")
		return true
	}

	return false
}

func (a *RuntimeIndexAdapter) triggerAdaptation(collection string, m AdaptationMetrics, currentIndex IndexType) {
	a.stats.AdaptationsTriggered.Add(1)

	features := QueryFeatures{
		DatasetSize:     int(m.IndexSizeMB * 1000),
		SearchK:         int(m.QueriesPerSec / 100),
		QueryComplexity: "medium",
	}

	// CLOSING THE FEEDBACK LOOP: Record the current (failing) state as a training sample.
	// This teaches the model that the CURRENT index is performaning poorly under these features.
	a.predictor.AddTrainingSample(TrainingSample{
		Features: features,
		Latency:  time.Duration(m.AvgLatencyMs * float64(time.Millisecond)),
		Recall:   m.RecallAchieved,
		Index:    currentIndex,
	})

	prediction := a.predictor.Predict(features)

	adaptation := &IndexAdaptation{
		CollectionName: collection,
		CurrentIndex:   currentIndex,
		ProposedIndex:  prediction.RecommendedIndex,
		TriggerReason:  a.determineTriggerReason(m),
		Metrics:        m,
		Timestamp:      time.Now(),
		Status:         AdaptationStatusPending,
		Features:       features,
	}

	a.adaptationMu.Lock()
	a.adaptations[collection] = adaptation
	a.adaptationMu.Unlock()

	// Record metric
	metrics.LearnedIndexAdaptationsTotal.WithLabelValues(string(currentIndex), string(prediction.RecommendedIndex), "triggered").Inc()

	a.logger.Info().
		Str("collection", collection).
		Str("proposed", string(adaptation.ProposedIndex)).
		Str("reason", adaptation.TriggerReason).
		Msg("Triggering index adaptation")
}

func (a *RuntimeIndexAdapter) determineTriggerReason(m AdaptationMetrics) string {
	if m.AvgLatencyMs > a.config.LatencyThresholdMs {
		return "high_latency"
	}
	if m.RecallAchieved < a.config.RecallThreshold {
		return "low_recall"
	}
	return "performance_degradation"
}

// GetAdaptation retrieves the current adaptation state for a specific collection.
func (a *RuntimeIndexAdapter) GetAdaptation(collection string) (*IndexAdaptation, bool) {
	a.adaptationMu.RLock()
	defer a.adaptationMu.RUnlock()

	adaptation, ok := a.adaptations[collection]
	return adaptation, ok
}

// ListAdaptations returns all current index adaptations across all collections.
func (a *RuntimeIndexAdapter) ListAdaptations() []*IndexAdaptation {
	a.adaptationMu.RLock()
	defer a.adaptationMu.RUnlock()

	adaptations := make([]*IndexAdaptation, 0, len(a.adaptations))
	for _, adaptation := range a.adaptations {
		adaptations = append(adaptations, adaptation)
	}
	return adaptations
}

// StartAdaptation marks an adaptation as running for the given collection.
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

// CompleteAdaptation finalizes an adaptation process, recording success or failure signal.
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
		metrics.LearnedIndexAdaptationsTotal.WithLabelValues(string(adaptation.CurrentIndex), string(adaptation.ProposedIndex), "completed").Inc()
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
		metrics.LearnedIndexAdaptationsTotal.WithLabelValues(string(adaptation.CurrentIndex), string(adaptation.ProposedIndex), "failed").Inc()
		a.logger.Error().Str("collection", collection).Msg("Index adaptation failed")

		// Record failure as a negative signal (failure decomposition)
		// We record a "virtual" sample with extremely high latency to penalize this index type
		// for the given query features.
		a.predictor.AddTrainingSample(TrainingSample{
			Features: adaptation.Features,
			Latency:  10 * time.Second, // Penalty latency
			Recall:   0.0,              // Zero recall
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

// Rollback reverts an index adaptation, switching back to the previous index type.
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
		metrics.LearnedIndexAdaptationsTotal.WithLabelValues(string(adaptation.ProposedIndex), string(adaptation.CurrentIndex), "rollback_failed").Inc()
		a.logger.Error().Err(err).Str("collection", collection).Msg("Rollback: index switch failed")
		return fmt.Errorf("rollback switch failed for %q: %w", collection, err)
	}

	// CLOSING THE FEEDBACK LOOP: Record the failure as a strong negative signal for the proposed index.
	// This ensures the predictor learns that this index degraded performance for these features.
	a.predictor.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{DatasetSize: int(adaptation.Metrics.IndexSizeMB * 1000)},
		Latency:  time.Duration(adaptation.Metrics.AvgLatencyMs * 2.0 * float64(time.Millisecond)), // Penalty: mark as 2x slow
		Recall:   adaptation.Metrics.RecallAchieved * 0.8,                                          // Penalty: mark as low recall
		Index:    adaptation.ProposedIndex,
	})

	a.stats.RollbacksPerformed.Add(1)
	metrics.LearnedIndexAdaptationsTotal.WithLabelValues(string(adaptation.ProposedIndex), string(adaptation.CurrentIndex), "rolled_back").Inc()
	a.logger.Info().
		Str("collection", collection).
		Str("reverted_to", string(target)).
		Msg("Index adaptation rolled back successfully")
	return nil
}

// GetStats returns current operational statistics for the index adapter.
func (a *RuntimeIndexAdapter) GetStats() (triggered, completed, failed, rolledback, analyzed int64) {
	return a.stats.AdaptationsTriggered.Load(),
		a.stats.AdaptationsCompleted.Load(),
		a.stats.AdaptationsFailed.Load(),
		a.stats.RollbacksPerformed.Load(),
		a.stats.QueriesAnalyzed.Load()
}

// GetConfig returns the current configuration for the index adapter.
func (a *RuntimeIndexAdapter) GetConfig() IndexAdaptationConfig {
	return a.config
}

// SetConfig updates the configuration for the index adapter.
func (a *RuntimeIndexAdapter) SetConfig(config IndexAdaptationConfig) {
	a.config = config
}
