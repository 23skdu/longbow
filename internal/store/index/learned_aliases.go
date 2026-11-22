package index

import (
	"github.com/23skdu/longbow/internal/store/index/learned"
	"github.com/rs/zerolog"
)

// APIStats represents a api stats value.
type APIStats = learned.APIStats

// AdaptationMetrics represents a adaptation metrics value.
type AdaptationMetrics = learned.AdaptationMetrics

// AdaptationStatus represents a adaptation status value.
type AdaptationStatus = learned.AdaptationStatus

// AdapterStats represents a adapter stats value.
type AdapterStats = learned.AdapterStats

// BenchmarkStats represents a benchmark stats value.
type BenchmarkStats = learned.BenchmarkStats

// BenchmarkSummary represents a benchmark summary value.
type BenchmarkSummary = learned.BenchmarkSummary

// DiskBackedLearnedIndex represents a disk backed learned index value.
type DiskBackedLearnedIndex = learned.DiskBackedLearnedIndex

// FeatureNormalizer represents a feature normalizer value.
type FeatureNormalizer = learned.FeatureNormalizer

// IndexAdaptation represents a index adaptation value.
type IndexAdaptation = learned.IndexAdaptation

// IndexAdaptationConfig represents a index adaptation config value.
type IndexAdaptationConfig = learned.IndexAdaptationConfig

// IndexBenchmark represents a index benchmark value.
type IndexBenchmark = learned.IndexBenchmark

// IndexMapperConfig represents a index mapper config value.
type IndexMapperConfig = learned.IndexMapperConfig

// IndexMapperStats represents a index mapper stats value.
type IndexMapperStats = learned.IndexMapperStats

// IndexPerformancePredictor represents a index performance predictor value.
type IndexPerformancePredictor = learned.IndexPerformancePredictor

// IndexPrediction represents a index prediction value.
type IndexPrediction = learned.IndexPrediction

// IndexRecommendationAPI represents a index recommendation api value.
type IndexRecommendationAPI = learned.IndexRecommendationAPI

// IndexRecommendationEngine represents a index recommendation engine value.
type IndexRecommendationEngine = learned.IndexRecommendationEngine

// IndexSwitcher represents a index switcher value.
type IndexSwitcher = learned.IndexSwitcher

// LRUCache represents a lru cache value.
type LRUCache = learned.LRUCache

// LearnedBenchmarkResult represents a learned benchmark result value.
type LearnedBenchmarkResult = learned.LearnedBenchmarkResult

// LearnedIndexConfig represents a learned index config value.
type LearnedIndexConfig = learned.LearnedIndexConfig

// LearnedIndexRateLimiter represents a learned index rate limiter value.
type LearnedIndexRateLimiter = learned.LearnedIndexRateLimiter

// LearnedIndexWithOllama represents a learned index with ollama value.
type LearnedIndexWithOllama = learned.LearnedIndexWithOllama

// MetricsCollector represents a metrics collector value.
type MetricsCollector = learned.MetricsCollector

// OllamaClient represents a ollama client value.
type OllamaClient = learned.OllamaClient

// OllamaConfig represents a ollama config value.
type OllamaConfig = learned.OllamaConfig

// OllamaEmbedRequest represents a ollama embed request value.
type OllamaEmbedRequest = learned.OllamaEmbedRequest

// OllamaEmbedResponse represents a ollama embed response value.
type OllamaEmbedResponse = learned.OllamaEmbedResponse

// PredictorStats represents a predictor stats value.
type PredictorStats = learned.PredictorStats

// QueryFeatures represents a query features value.
type QueryFeatures = learned.QueryFeatures

// QueryIndexMapper represents a query index mapper value.
type QueryIndexMapper = learned.QueryIndexMapper

// RecommendationRecord represents a recommendation record value.
type RecommendationRecord = learned.RecommendationRecord

// RuntimeIndexAdapter represents a runtime index adapter value.
type RuntimeIndexAdapter = learned.RuntimeIndexAdapter

// TrainingSample represents a training sample value.
type TrainingSample = learned.TrainingSample

const (
	// AdaptationStatusPending is a adaptation status pending constant.
	AdaptationStatusPending = learned.AdaptationStatusPending
	// AdaptationStatusRunning is a adaptation status running constant.
	AdaptationStatusRunning = learned.AdaptationStatusRunning
	// AdaptationStatusComplete is a adaptation status complete constant.
	AdaptationStatusComplete = learned.AdaptationStatusComplete
	// AdaptationStatusFailed is a adaptation status failed constant.
	AdaptationStatusFailed = learned.AdaptationStatusFailed
	// AdaptationStatusCancelled is a adaptation status cancelled constant.
	AdaptationStatusCancelled = learned.AdaptationStatusCancelled

	// LearnedIndexTypeAuto is a learned index type auto constant.
	LearnedIndexTypeAuto = learned.LearnedIndexTypeAuto
	// LearnedIVFPQ is a learned ivfpq constant.
	LearnedIVFPQ = learned.LearnedIVFPQ
)

// NewDiskBackedLearnedIndex constructs a new DiskBackedLearnedIndex.
func NewDiskBackedLearnedIndex(cfg IndexConfig, path string) (*DiskBackedLearnedIndex, error) {
	return learned.NewDiskBackedLearnedIndex(cfg, path)
}

// NewIndexBenchmark constructs a new IndexBenchmark.
func NewIndexBenchmark(logger zerolog.Logger, predictor *IndexPerformancePredictor, fixedIndices []IndexType) *IndexBenchmark {
	return learned.NewIndexBenchmark(logger, predictor, fixedIndices)
}

// NewIndexPerformancePredictor constructs a new IndexPerformancePredictor.
func NewIndexPerformancePredictor(logger zerolog.Logger, config LearnedIndexConfig) *IndexPerformancePredictor {
	return learned.NewIndexPerformancePredictor(logger, config)
}

// NewIndexRecommendationAPI constructs a new IndexRecommendationAPI.
func NewIndexRecommendationAPI(logger zerolog.Logger, predictor *IndexPerformancePredictor, mapper *QueryIndexMapper) *IndexRecommendationAPI {
	return learned.NewIndexRecommendationAPI(logger, predictor, mapper)
}

// NewLearnedIndexRateLimiter constructs a new LearnedIndexRateLimiter.
func NewLearnedIndexRateLimiter(predictor *IndexPerformancePredictor, logger zerolog.Logger) *LearnedIndexRateLimiter {
	return learned.NewLearnedIndexRateLimiter(predictor, logger)
}

// NewLearnedIndexWithOllama constructs a new LearnedIndexWithOllama.
func NewLearnedIndexWithOllama(logger zerolog.Logger, predictor *IndexPerformancePredictor, config OllamaConfig) *LearnedIndexWithOllama {
	return learned.NewLearnedIndexWithOllama(logger, predictor, config)
}

// NewOllamaClient constructs a new OllamaClient.
func NewOllamaClient(logger zerolog.Logger, config OllamaConfig) *OllamaClient {
	return learned.NewOllamaClient(logger, config)
}

// NewQueryIndexMapper constructs a new QueryIndexMapper.
func NewQueryIndexMapper(logger zerolog.Logger, predictor *IndexPerformancePredictor, config IndexMapperConfig) *QueryIndexMapper {
	return learned.NewQueryIndexMapper(logger, predictor, config)
}

// NewRuntimeIndexAdapter constructs a new RuntimeIndexAdapter.
func NewRuntimeIndexAdapter(logger zerolog.Logger, predictor *IndexPerformancePredictor, config IndexAdaptationConfig, collector MetricsCollector) *RuntimeIndexAdapter {
	return learned.NewRuntimeIndexAdapter(logger, predictor, config, collector)
}
