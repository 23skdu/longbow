package index

import (
	"github.com/23skdu/longbow/internal/store/index/learned"
	"github.com/rs/zerolog"
)

type APIStats = learned.APIStats
type AdaptationMetrics = learned.AdaptationMetrics
type AdaptationStatus = learned.AdaptationStatus
type AdapterStats = learned.AdapterStats
type BenchmarkStats = learned.BenchmarkStats
type BenchmarkSummary = learned.BenchmarkSummary
type DiskBackedLearnedIndex = learned.DiskBackedLearnedIndex
type FeatureNormalizer = learned.FeatureNormalizer
type IndexAdaptation = learned.IndexAdaptation
type IndexAdaptationConfig = learned.IndexAdaptationConfig
type IndexBenchmark = learned.IndexBenchmark
type IndexMapperConfig = learned.IndexMapperConfig
type IndexMapperStats = learned.IndexMapperStats
type IndexPerformancePredictor = learned.IndexPerformancePredictor
type IndexPrediction = learned.IndexPrediction
type IndexRecommendationAPI = learned.IndexRecommendationAPI
type IndexRecommendationEngine = learned.IndexRecommendationEngine
type IndexSwitcher = learned.IndexSwitcher
type LRUCache = learned.LRUCache
type LearnedBenchmarkResult = learned.LearnedBenchmarkResult
type LearnedIndexConfig = learned.LearnedIndexConfig
type LearnedIndexRateLimiter = learned.LearnedIndexRateLimiter
type LearnedIndexWithOllama = learned.LearnedIndexWithOllama
type MetricsCollector = learned.MetricsCollector
type OllamaClient = learned.OllamaClient
type OllamaConfig = learned.OllamaConfig
type OllamaEmbedRequest = learned.OllamaEmbedRequest
type OllamaEmbedResponse = learned.OllamaEmbedResponse
type PredictorStats = learned.PredictorStats
type QueryFeatures = learned.QueryFeatures
type QueryIndexMapper = learned.QueryIndexMapper
type RecommendationRecord = learned.RecommendationRecord
type RuntimeIndexAdapter = learned.RuntimeIndexAdapter
type TrainingSample = learned.TrainingSample

const (
	AdaptationStatusPending   = learned.AdaptationStatusPending
	AdaptationStatusRunning   = learned.AdaptationStatusRunning
	AdaptationStatusComplete  = learned.AdaptationStatusComplete
	AdaptationStatusFailed    = learned.AdaptationStatusFailed
	AdaptationStatusCancelled = learned.AdaptationStatusCancelled

	LearnedIndexTypeAuto = learned.LearnedIndexTypeAuto
	LearnedIVFPQ         = learned.LearnedIVFPQ
)

func NewDiskBackedLearnedIndex(cfg IndexConfig, path string) (*DiskBackedLearnedIndex, error) {
	return learned.NewDiskBackedLearnedIndex(cfg, path)
}

func NewIndexBenchmark(logger zerolog.Logger, predictor *IndexPerformancePredictor, fixedIndices []IndexType) *IndexBenchmark {
	return learned.NewIndexBenchmark(logger, predictor, fixedIndices)
}

func NewIndexPerformancePredictor(logger zerolog.Logger, config LearnedIndexConfig) *IndexPerformancePredictor {
	return learned.NewIndexPerformancePredictor(logger, config)
}

func NewIndexRecommendationAPI(logger zerolog.Logger, predictor *IndexPerformancePredictor, mapper *QueryIndexMapper) *IndexRecommendationAPI {
	return learned.NewIndexRecommendationAPI(logger, predictor, mapper)
}

func NewLearnedIndexRateLimiter(predictor *IndexPerformancePredictor, logger zerolog.Logger) *LearnedIndexRateLimiter {
	return learned.NewLearnedIndexRateLimiter(predictor, logger)
}

func NewLearnedIndexWithOllama(logger zerolog.Logger, predictor *IndexPerformancePredictor, config OllamaConfig) *LearnedIndexWithOllama {
	return learned.NewLearnedIndexWithOllama(logger, predictor, config)
}

func NewOllamaClient(logger zerolog.Logger, config OllamaConfig) *OllamaClient {
	return learned.NewOllamaClient(logger, config)
}

func NewQueryIndexMapper(logger zerolog.Logger, predictor *IndexPerformancePredictor, config IndexMapperConfig) *QueryIndexMapper {
	return learned.NewQueryIndexMapper(logger, predictor, config)
}

func NewRuntimeIndexAdapter(logger zerolog.Logger, predictor *IndexPerformancePredictor, config IndexAdaptationConfig, collector MetricsCollector) *RuntimeIndexAdapter {
	return learned.NewRuntimeIndexAdapter(logger, predictor, config, collector)
}
