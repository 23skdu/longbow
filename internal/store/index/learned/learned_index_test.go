package learned

import (
	"github.com/23skdu/longbow/internal/store/types"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestLearnedIndexConfig_Defaults runs the test learned index config_defaults operation.
func TestLearnedIndexConfig_Defaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := LearnedIndexConfig{}

	if config.MinTrainingSamples <= 0 {
		config.MinTrainingSamples = 100
	}
	if config.ConfidenceThreshold <= 0 {
		config.ConfidenceThreshold = 0.7
	}
	if config.UpdateInterval <= 0 {
		config.UpdateInterval = time.Hour
	}

	assert.Equal(t, 100, config.MinTrainingSamples)
	assert.Equal(t, 0.7, config.ConfidenceThreshold)
	assert.Equal(t, time.Hour, config.UpdateInterval)
}

// TestNewIndexPerformancePredictor runs the test new index performance predictor operation.
func TestNewIndexPerformancePredictor(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	config := LearnedIndexConfig{
		EnableAutoSelection: true,
		MinTrainingSamples:  50,
	}

	p := NewIndexPerformancePredictor(logger, config)

	assert.NotNil(t, p)
	assert.Equal(t, 50, p.config.MinTrainingSamples)
	assert.True(t, p.config.EnableAutoSelection)
	assert.NotNil(t, p.featureWeights)
}

// TestIndexPerformancePredictor_AddTrainingSample runs the test index performance predictor_add training sample operation.
func TestIndexPerformancePredictor_AddTrainingSample(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	sample := TrainingSample{
		Features: QueryFeatures{
			VectorDimension: 128,
			DatasetSize:     10000,
			SearchK:         10,
		},
		Latency: 50 * time.Millisecond,
		Recall:  0.95,
		Index:   types.IndexTypeHNSW,
	}

	p.AddTrainingSample(sample)

	assert.Equal(t, int64(1), p.stats.TrainingSamplesCollected.Load())
}

// TestIndexPerformancePredictor_Predict_InsufficientSamples runs the test index performance predictor_predict_insufficient samples operation.
func TestIndexPerformancePredictor_Predict_InsufficientSamples(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	config := LearnedIndexConfig{
		MinTrainingSamples: 100,
	}
	p := NewIndexPerformancePredictor(logger, config)

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     10000,
		SearchK:         10,
	}

	prediction := p.Predict(features)

	assert.Equal(t, types.IndexTypeHNSW, prediction.RecommendedIndex)
	assert.Equal(t, 0.5, prediction.Confidence)
	assert.NotEmpty(t, prediction.Alternatives)
}

// TestIndexPerformancePredictor_Predict_SmallDataset runs the test index performance predictor_predict_small dataset operation.
func TestIndexPerformancePredictor_Predict_SmallDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	for i := 0; i < 150; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension: 128,
				DatasetSize:     50000,
				SearchK:         10,
			},
			Index: types.IndexTypeHNSW,
		})
	}

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     50000,
		SearchK:         10,
	}

	prediction := p.Predict(features)

	assert.Equal(t, types.IndexTypeHNSW, prediction.RecommendedIndex)
	assert.Greater(t, prediction.Confidence, 0.5)
}

// TestIndexPerformancePredictor_Predict_LargeDataset runs the test index performance predictor_predict_large dataset operation.
func TestIndexPerformancePredictor_Predict_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     5000000,
		SearchK:         10,
	}

	prediction := p.Predict(features)

	assert.Equal(t, types.IndexTypeDiskANN, prediction.RecommendedIndex)
}

// TestIndexPerformancePredictor_Predict_MediumDataset runs the test index performance predictor_predict_medium dataset operation.
func TestIndexPerformancePredictor_Predict_MediumDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     500000,
		SearchK:         10,
	}

	prediction := p.Predict(features)

	assert.Equal(t, LearnedIVFPQ, prediction.RecommendedIndex)
}

// TestIndexPerformancePredictor_calculateConfidence runs the test index performance predictor_calculate confidence operation.
func TestIndexPerformancePredictor_calculateConfidence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	scores := map[types.IndexType]float64{
		types.IndexTypeHNSW:    0.8,
		types.IndexTypeIVFFlat: 0.3,
		types.IndexTypeDiskANN: 0.2,
	}

	confidence := p.calculateConfidence(scores)

	assert.Greater(t, confidence, 0.5)
}

// TestIndexPerformancePredictor_estimateLatency runs the test index performance predictor_estimate latency operation.
func TestIndexPerformancePredictor_estimateLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		NumQueryVectors: 5,
		SearchK:         10,
	}

	latency := p.estimateLatency(features, types.IndexTypeHNSW)

	assert.Greater(t, latency, time.Duration(0))
}

// TestIndexPerformancePredictor_estimateRecall runs the test index performance predictor_estimate recall operation.
func TestIndexPerformancePredictor_estimateRecall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		DatasetSize: 10000,
	}

	recallHNSW := p.estimateRecall(features, types.IndexTypeHNSW)
	recallDisk := p.estimateRecall(features, types.IndexTypeDiskANN)

	assert.Equal(t, 0.98, recallHNSW)
	assert.Equal(t, 0.95, recallDisk)
}

// TestIndexPerformancePredictor_getAlternatives runs the test index performance predictor_get alternatives operation.
func TestIndexPerformancePredictor_getAlternatives(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	scores := map[types.IndexType]float64{
		types.IndexTypeHNSW:    0.8,
		types.IndexTypeIVFFlat: 0.3,
		types.IndexTypeDiskANN: 0.2,
	}

	alternatives := p.getAlternatives(scores, types.IndexTypeHNSW)

	assert.Len(t, alternatives, 2)
	assert.NotContains(t, alternatives, types.IndexTypeHNSW)
}

// TestIndexPerformancePredictor_GetStats runs the test index performance predictor_get stats operation.
func TestIndexPerformancePredictor_GetStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	p.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{VectorDimension: 128},
		Index:    types.IndexTypeHNSW,
	})

	p.Predict(QueryFeatures{VectorDimension: 128})

	samples, predictions, correct := p.GetStats()
	assert.Equal(t, int64(1), samples)
	assert.Equal(t, int64(1), predictions)
	assert.Equal(t, int64(0), correct)
}

// TestIndexPerformancePredictor_GetConfig runs the test index performance predictor_get config operation.
func TestIndexPerformancePredictor_GetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	config := LearnedIndexConfig{
		EnableAutoSelection: true,
		ConfidenceThreshold: 0.8,
		MinTrainingSamples:  50,
		UpdateInterval:      time.Minute,
	}

	p := NewIndexPerformancePredictor(logger, config)

	got := p.GetConfig()
	assert.Equal(t, config.EnableAutoSelection, got.EnableAutoSelection)
	assert.Equal(t, config.ConfidenceThreshold, got.ConfidenceThreshold)
}

// TestIndexPerformancePredictor_SetConfig runs the test index performance predictor_set config operation.
func TestIndexPerformancePredictor_SetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	newConfig := LearnedIndexConfig{
		EnableAutoSelection: false,
		ConfidenceThreshold: 0.9,
	}

	p.SetConfig(newConfig)
	assert.Equal(t, 0.9, p.config.ConfidenceThreshold)
}

// TestIndexPerformancePredictor_GetTrainingSampleCount runs the test index performance predictor_get training sample count operation.
func TestIndexPerformancePredictor_GetTrainingSampleCount(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	assert.Equal(t, 0, p.GetTrainingSampleCount())

	p.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{VectorDimension: 128},
	})
	p.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{VectorDimension: 256},
	})

	assert.Equal(t, 2, p.GetTrainingSampleCount())
}

// TestIndexPerformancePredictor_ClearTrainingData runs the test index performance predictor_clear training data operation.
func TestIndexPerformancePredictor_ClearTrainingData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	p.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{VectorDimension: 128},
	})
	p.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{VectorDimension: 256},
	})

	assert.Equal(t, 2, p.GetTrainingSampleCount())

	p.ClearTrainingData()

	assert.Equal(t, 0, p.GetTrainingSampleCount())
}

// TestIndexPerformancePredictor_FeatureWeights runs the test index performance predictor_feature weights operation.
func TestIndexPerformancePredictor_FeatureWeights(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	assert.NotNil(t, p.featureWeights)
	assert.Contains(t, p.featureWeights, "dataset_size")
	assert.Contains(t, p.featureWeights, "search_k")
	assert.Contains(t, p.featureWeights, "vector_dimension")
}

// TestIndexMapperConfig_Defaults runs the test index mapper config_defaults operation.
func TestIndexMapperConfig_Defaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := IndexMapperConfig{}

	if config.CacheTTL <= 0 {
		config.CacheTTL = 10 * time.Minute
	}
	if config.FallbackIndex == "" {
		config.FallbackIndex = types.IndexTypeHNSW
	}

	assert.Equal(t, 10*time.Minute, config.CacheTTL)
	assert.Equal(t, types.IndexTypeHNSW, config.FallbackIndex)
}

// TestNewQueryIndexMapper runs the test new query index mapper operation.
func TestNewQueryIndexMapper(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexMapperConfig{
		EnableAutoMapping: true,
		CacheEnabled:      true,
	}

	mapper := NewQueryIndexMapper(logger, predictor, config)

	assert.NotNil(t, mapper)
	assert.NotNil(t, mapper.indexMapping)
	assert.True(t, mapper.config.EnableAutoMapping)
	assert.True(t, mapper.config.CacheEnabled)
}

// TestQueryIndexMapper_GetIndexForQuery runs the test query index mapper_get index for query operation.
func TestQueryIndexMapper_GetIndexForQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: false,
	})

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     50000,
		SearchK:         10,
	}

	index := mapper.GetIndexForQuery("query1", features)

	assert.Equal(t, types.IndexTypeHNSW, index)
	assert.Equal(t, int64(1), mapper.stats.QueriesMapped.Load())
}

// TestQueryIndexMapper_GetIndexForQuery_WithCache runs the test query index mapper_get index for query_with cache operation.
func TestQueryIndexMapper_GetIndexForQuery_WithCache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: true,
	})

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     50000,
		SearchK:         10,
	}

	index1 := mapper.GetIndexForQuery("query1", features)
	index2 := mapper.GetIndexForQuery("query1", features)

	assert.Equal(t, index1, index2)
	assert.Equal(t, int64(1), mapper.stats.CacheHits.Load())
	assert.Equal(t, int64(1), mapper.stats.CacheMisses.Load())
}

// TestQueryIndexMapper_GetIndexForQuery_LargeDataset runs the test query index mapper_get index for query_large dataset operation.
func TestQueryIndexMapper_GetIndexForQuery_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: false,
	})

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     5000000,
		SearchK:         10,
	}

	index := mapper.GetIndexForQuery("query1", features)

	assert.Equal(t, types.IndexTypeDiskANN, index)
}

// TestQueryIndexMapper_InvalidateCache runs the test query index mapper_invalidate cache operation.
func TestQueryIndexMapper_InvalidateCache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: true,
	})

	features := QueryFeatures{DatasetSize: 50000}
	mapper.GetIndexForQuery("query1", features)
	assert.Equal(t, 1, mapper.GetMappingCount())

	mapper.InvalidateCache("query1")
	assert.Equal(t, 0, mapper.GetMappingCount())
}

// TestQueryIndexMapper_ClearCache runs the test query index mapper_clear cache operation.
func TestQueryIndexMapper_ClearCache(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: true,
	})

	mapper.GetIndexForQuery("query1", QueryFeatures{DatasetSize: 50000})
	mapper.GetIndexForQuery("query2", QueryFeatures{DatasetSize: 50000})
	assert.Equal(t, 2, mapper.GetMappingCount())

	mapper.ClearCache()
	assert.Equal(t, 0, mapper.GetMappingCount())
}

// TestQueryIndexMapper_GetStats runs the test query index mapper_get stats operation.
func TestQueryIndexMapper_GetStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: true,
	})

	mapper.GetIndexForQuery("query1", QueryFeatures{DatasetSize: 50000})

	mapped, _, _, _, _ := mapper.GetStats()
	assert.Greater(t, mapped, int64(0))
}

// TestQueryIndexMapper_GetConfig runs the test query index mapper_get config operation.
func TestQueryIndexMapper_GetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexMapperConfig{
		EnableAutoMapping: true,
		EnableFallback:    true,
	}

	mapper := NewQueryIndexMapper(logger, predictor, config)

	got := mapper.GetConfig()
	assert.Equal(t, config.EnableAutoMapping, got.EnableAutoMapping)
	assert.Equal(t, config.EnableFallback, got.EnableFallback)
}

// TestQueryIndexMapper_SetConfig runs the test query index mapper_set config operation.
func TestQueryIndexMapper_SetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{})

	newConfig := IndexMapperConfig{
		CacheEnabled:   false,
		EnableFallback: false,
	}

	mapper.SetConfig(newConfig)
	assert.False(t, mapper.config.CacheEnabled)
}

// TestQueryIndexMapper_GetCachedMappings runs the test query index mapper_get cached mappings operation.
func TestQueryIndexMapper_GetCachedMappings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: true,
	})

	mapper.GetIndexForQuery("query1", QueryFeatures{DatasetSize: 50000})
	mapper.GetIndexForQuery("query2", QueryFeatures{DatasetSize: 5000000})

	mappings := mapper.GetCachedMappings()
	assert.Len(t, mappings, 2)
	assert.Contains(t, mappings, "query1")
	assert.Contains(t, mappings, "query2")
}

// TestIndexAdaptationConfig_Defaults runs the test index adaptation config_defaults operation.
func TestIndexAdaptationConfig_Defaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	config := IndexAdaptationConfig{}

	if config.MinSamplesForAdaptation <= 0 {
		config.MinSamplesForAdaptation = 1000
	}
	if config.LatencyThresholdMs <= 0 {
		config.LatencyThresholdMs = 100.0
	}
	if config.CheckInterval <= 0 {
		config.CheckInterval = 5 * time.Minute
	}

	assert.Equal(t, 1000, config.MinSamplesForAdaptation)
	assert.Equal(t, 100.0, config.LatencyThresholdMs)
	assert.Equal(t, 5*time.Minute, config.CheckInterval)
}

// TestNewRuntimeIndexAdapter runs the test new runtime index adapter operation.
func TestNewRuntimeIndexAdapter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexAdaptationConfig{
		EnableAutoAdaptation: true,
	}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	assert.NotNil(t, adapter)
	assert.True(t, adapter.config.EnableAutoAdaptation)
}

// TestRuntimeIndexAdapter_ShouldAdapt_Latency runs the test runtime index adapter_should adapt_latency operation.
func TestRuntimeIndexAdapter_ShouldAdapt_Latency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexAdaptationConfig{
		LatencyThresholdMs: 100.0,
	}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	metrics := AdaptationMetrics{
		AvgLatencyMs: 150.0,
	}

	assert.True(t, adapter.shouldAdapt(metrics))
}

// TestRuntimeIndexAdapter_ShouldAdapt_Recall runs the test runtime index adapter_should adapt_recall operation.
func TestRuntimeIndexAdapter_ShouldAdapt_Recall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexAdaptationConfig{
		RecallThreshold: 0.95,
	}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	metrics := AdaptationMetrics{
		AvgLatencyMs:   50.0,
		RecallAchieved: 0.90,
	}

	assert.True(t, adapter.shouldAdapt(metrics))
}

// TestRuntimeIndexAdapter_ShouldAdapt_NoAdapt runs the test runtime index adapter_should adapt_no adapt operation.
func TestRuntimeIndexAdapter_ShouldAdapt_NoAdapt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexAdaptationConfig{
		LatencyThresholdMs: 100.0,
		RecallThreshold:    0.95,
	}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	metrics := AdaptationMetrics{
		AvgLatencyMs:   50.0,
		RecallAchieved: 0.98,
	}

	assert.False(t, adapter.shouldAdapt(metrics))
}

// TestRuntimeIndexAdapter_DetermineTriggerReason runs the test runtime index adapter_determine trigger reason operation.
func TestRuntimeIndexAdapter_DetermineTriggerReason(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexAdaptationConfig{}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	metricsHighLatency := AdaptationMetrics{
		AvgLatencyMs: 150.0,
	}
	reason := adapter.determineTriggerReason(metricsHighLatency)
	assert.Equal(t, "high_latency", reason)

	metricsLowRecall := AdaptationMetrics{
		AvgLatencyMs:   50.0,
		RecallAchieved: 0.80,
	}
	reason = adapter.determineTriggerReason(metricsLowRecall)
	assert.Equal(t, "low_recall", reason)
}

// TestRuntimeIndexAdapter_GetStats runs the test runtime index adapter_get stats operation.
func TestRuntimeIndexAdapter_GetStats(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{}, nil)

	adapter.stats.AdaptationsTriggered.Add(5)
	adapter.stats.AdaptationsCompleted.Add(3)

	triggered, completed, _, _, _ := adapter.GetStats()

	assert.Equal(t, int64(5), triggered)
	assert.Equal(t, int64(3), completed)
}

// TestRuntimeIndexAdapter_GetConfig runs the test runtime index adapter_get config operation.
func TestRuntimeIndexAdapter_GetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexAdaptationConfig{
		EnableAutoAdaptation:  true,
		MaxAdaptationsPerHour: 6,
	}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	got := adapter.GetConfig()
	assert.Equal(t, config.EnableAutoAdaptation, got.EnableAutoAdaptation)
	assert.Equal(t, 6, got.MaxAdaptationsPerHour)
}

// TestRuntimeIndexAdapter_SetConfig runs the test runtime index adapter_set config operation.
func TestRuntimeIndexAdapter_SetConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{}, nil)

	newConfig := IndexAdaptationConfig{
		EnableAutoAdaptation: false,
		LatencyThresholdMs:   200.0,
	}

	adapter.SetConfig(newConfig)
	assert.False(t, adapter.config.EnableAutoAdaptation)
	assert.Equal(t, 200.0, adapter.config.LatencyThresholdMs)
}

// TestRuntimeIndexAdapter_GetAdaptation runs the test runtime index adapter_get adaptation operation.
func TestRuntimeIndexAdapter_GetAdaptation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{}, nil)

	adapter.adaptationMu.Lock()
	adapter.adaptations["collection1"] = &IndexAdaptation{
		CollectionName: "collection1",
		ProposedIndex:  types.IndexTypeDiskANN,
		Status:         AdaptationStatusPending,
	}
	adapter.adaptationMu.Unlock()

	adaptation, ok := adapter.GetAdaptation("collection1")
	assert.True(t, ok)
	assert.Equal(t, types.IndexTypeDiskANN, adaptation.ProposedIndex)

	_, ok = adapter.GetAdaptation("nonexistent")
	assert.False(t, ok)
}

// TestRuntimeIndexAdapter_ListAdaptations runs the test runtime index adapter_list adaptations operation.
func TestRuntimeIndexAdapter_ListAdaptations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{}, nil)

	adapter.adaptationMu.Lock()
	adapter.adaptations["collection1"] = &IndexAdaptation{CollectionName: "collection1"}
	adapter.adaptations["collection2"] = &IndexAdaptation{CollectionName: "collection2"}
	adapter.adaptationMu.Unlock()

	adaptations := adapter.ListAdaptations()
	assert.Len(t, adaptations, 2)
}

// =============================================================================
// k-NN Scorer Tests
// =============================================================================

// TestKNNPredict_LearnFromSamples proves that kNNPredict changes its
// recommendations based on the training data it has observed — the core
// behaviour that was previously broken.
func TestKNNPredict_LearnFromSamples(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 50,
		KNN:                7,
	})

	// Seed: small datasets → HNSW wins.
	for i := 0; i < 80; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension: 128,
				DatasetSize:     10000 + i*100,
				SearchK:         10,
			},
			Latency: 5 * time.Millisecond,
			Recall:  0.98,
			Index:   types.IndexTypeHNSW,
		})
	}

	// Seed: large datasets → DiskANN wins.
	for i := 0; i < 80; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension: 128,
				DatasetSize:     3000000 + i*10000,
				SearchK:         10,
			},
			Latency: 30 * time.Millisecond,
			Recall:  0.95,
			Index:   types.IndexTypeDiskANN,
		})
	}

	// k-NN should recommend HNSW for a small-dataset query.
	smallPred := p.Predict(QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     15000,
		SearchK:         10,
	})
	assert.Equal(t, types.IndexTypeHNSW, smallPred.RecommendedIndex,
		"k-NN must recommend HNSW for small dataset matching training distribution")

	// k-NN should recommend DiskANN for a large-dataset query.
	largePred := p.Predict(QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     4000000,
		SearchK:         10,
	})
	assert.Equal(t, types.IndexTypeDiskANN, largePred.RecommendedIndex,
		"k-NN must recommend DiskANN for large dataset matching training distribution")
}

// TestKNNPredict_OverridesHeuristic verifies that the k-NN scorer overrides the
// static heuristic when training data contradicts it.
func TestKNNPredict_OverridesHeuristic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 30,
		KNN:                5,
	})

	// The heuristic scores HNSW highly for small datasets. Teach k-NN the
	// opposite: always prefer DiskANN for these features.
	for i := 0; i < 60; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension: 64,
				DatasetSize:     50000,
				SearchK:         50,
				IsFiltered:      true,
			},
			Index: types.IndexTypeDiskANN,
		})
	}

	pred := p.Predict(QueryFeatures{
		VectorDimension: 64,
		DatasetSize:     50000,
		SearchK:         50,
		IsFiltered:      true,
	})
	assert.Equal(t, types.IndexTypeDiskANN, pred.RecommendedIndex,
		"k-NN must override heuristic when all training samples point to DiskANN")
}

// TestKNNPredict_ConcurrentAddAndPredict stresses the scorer under concurrent
// reads and writes. Any race will be detected by the -race flag.
func TestKNNPredict_ConcurrentAddAndPredict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 50,
		UpdateInterval:     time.Hour, // disable async updates during this test
		KNN:                7,
	})

	// Pre-seed to cross the MinTrainingSamples threshold.
	for i := 0; i < 60; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{DatasetSize: 10000 * (i + 1)},
			Index:    types.IndexTypeHNSW,
		})
	}

	done := make(chan struct{})

	// 8 writers.
	for i := 0; i < 8; i++ {
		go func(i int) {
			for j := 0; j < 50; j++ {
				p.AddTrainingSample(TrainingSample{
					Features: QueryFeatures{DatasetSize: i*1000 + j},
					Index:    types.IndexTypeHNSW,
				})
			}
			done <- struct{}{}
		}(i)
	}

	// 4 readers.
	for i := 0; i < 4; i++ {
		go func() {
			for j := 0; j < 50; j++ {
				_ = p.Predict(QueryFeatures{DatasetSize: j * 1000})
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 12; i++ {
		<-done
	}
}

// =============================================================================
// FeatureNormalizer Tests
// =============================================================================

// TestFeatureNormalizer_MinMax runs the test feature normalizer_min max operation.
func TestFeatureNormalizer_MinMax(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	n := newFeatureNormalizer()
	assert.False(t, n.Ready())

	// First update sets min and max to the same value.
	v1 := [numFeatures]float64{100, 5, 10, 500000, 1, 0.5, 1.2, 0, 0, 12, 3, 1, 4.0}
	n.Update(v1)
	assert.True(t, n.Ready())

	// Second update with larger values expands max.
	v2 := [numFeatures]float64{512, 20, 100, 3000000, 3, 1.0, 2.5, 1, 1, 18, 6, 3, 2.67}
	n.Update(v2)

	normed := n.Normalize(v1)
	for i, val := range normed {
		assert.GreaterOrEqualf(t, val, 0.0, "feature %d below 0", i)
		assert.LessOrEqualf(t, val, 1.0, "feature %d above 1", i)
	}
}

// TestFeatureNormalizer_NormalizeOutput runs the test feature normalizer_normalize output operation.
func TestFeatureNormalizer_NormalizeOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	n := newFeatureNormalizer()
	lo := [numFeatures]float64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	hi := [numFeatures]float64{1024, 100, 1000, 5000000, 10, 1.0, 5.0, 1, 1, 23, 6, 6, 4.0}
	n.Update(lo)
	n.Update(hi)

	normedLo := n.Normalize(lo)
	normedHi := n.Normalize(hi)
	for i := range normedLo {
		assert.InDelta(t, 0.0, normedLo[i], 1e-9, "low endpoint feature %d", i)
		assert.InDelta(t, 1.0, normedHi[i], 1e-9, "high endpoint feature %d", i)
	}
}

// TestFeatureNormalizer_ZeroSpanReturns05 runs the test feature normalizer_zero span returns05 operation.
func TestFeatureNormalizer_ZeroSpanReturns05(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	n := newFeatureNormalizer()
	// Both updates identical → zero span for all features.
	v := [numFeatures]float64{42, 1, 10, 100000, 1, 0.5, 1.0, 0, 0, 9, 2, 0, 1.0}
	n.Update(v)
	n.Update(v)

	normed := n.Normalize(v)
	for i, val := range normed {
		assert.InDeltaf(t, 0.5, val, 1e-9, "zero-span feature %d should be 0.5", i)
	}
}

// =============================================================================
// Online Weight Update Tests
// =============================================================================

// TestOnlineWeightUpdate_ConvergesDirection verifies that updateWeights assigns
// a higher weight to dataset_size when it is the dominant discriminating feature.
func TestOnlineWeightUpdate_ConvergesDirection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 10,
		UpdateInterval:     0, // force immediate update on next trigger
	})

	// All samples differ only in dataset_size; same dimension across all.
	for i := 0; i < 60; i++ {
		idx := types.IndexTypeHNSW
		ds := 10000
		if i >= 30 {
			idx = types.IndexTypeDiskANN
			ds = 4000000
		}
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension: 128, // constant — zero between-class variance
				DatasetSize:     ds,
				SearchK:         10, // constant
			},
			Index: idx,
		})
	}

	// Run weight update synchronously.
	p.updateWeights()

	p.samplesMu.RLock()
	dsWeight := p.featureWeights["dataset_size"]
	dimWeight := p.featureWeights["vector_dimension"]
	p.samplesMu.RUnlock()

	assert.Greater(t, dsWeight, dimWeight,
		"dataset_size should have higher weight than vector_dimension when it is the sole discriminator")
}

// =============================================================================
// PredictionCorrect / Feedback Loop Tests
// =============================================================================

// TestPredictionCorrect_IncrementOnFeedback runs the test prediction correct_increment on feedback operation.
func TestPredictionCorrect_IncrementOnFeedback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 50,
		KNN:                5,
	})

	// Seed enough samples for k-NN to kick in.
	for i := 0; i < 60; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{DatasetSize: 10000},
			Index:    types.IndexTypeHNSW,
		})
	}

	// Make a prediction — this stores lastPredictedIdx.
	pred := p.Predict(QueryFeatures{DatasetSize: 10000, SearchK: 10})

	beforeCorrect, _, _ := p.GetStats()
	_ = beforeCorrect // TrainingSamplesCollected, not PredictionCorrect

	// Add a sample that matches the prediction.
	p.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{DatasetSize: 10000},
		Index:    pred.RecommendedIndex,
	})

	assert.Greater(t, p.stats.PredictionCorrect.Load(), int64(0),
		"PredictionCorrect must increment when sample.Index matches last k-NN prediction")
}

// =============================================================================
// Rollback Tests
// =============================================================================

// mockSwitcher implements IndexSwitcher for testing.
type mockSwitcher struct {
	switchCalled bool
	switchTarget types.IndexType
	switchErr    error
}

// SwitchIndex runs the switch index operation.
func (m *mockSwitcher) SwitchIndex(_ string, to types.IndexType) error {
	m.switchCalled = true
	m.switchTarget = to
	return m.switchErr
}

// TestRollback_IsNotNoOp runs the test rollback_is not no op operation.
func TestRollback_IsNotNoOp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{
		EnableRollback: true,
	}, nil)

	sw := &mockSwitcher{}
	adapter.WithIndexSwitcher(sw)

	// Register a fake adaptation with a prior index to roll back to.
	adapter.adaptationMu.Lock()
	adapter.adaptations["col1"] = &IndexAdaptation{
		CollectionName: "col1",
		CurrentIndex:   types.IndexTypeHNSW,
		ProposedIndex:  types.IndexTypeDiskANN,
		Status:         AdaptationStatusComplete,
	}
	adapter.adaptationMu.Unlock()

	err := adapter.Rollback("col1")
	assert.NoError(t, err, "Rollback should succeed when IndexSwitcher is wired")
	assert.True(t, sw.switchCalled, "IndexSwitcher.SwitchIndex must be called during rollback")
	assert.Equal(t, types.IndexTypeHNSW, sw.switchTarget, "Rollback must target CurrentIndex (prior index)")
}

// TestRollback_NoSwitcher_ReturnsError runs the test rollback_no switcher_returns error operation.
func TestRollback_NoSwitcher_ReturnsError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{
		EnableRollback: true,
	}, nil)

	// No IndexSwitcher wired.
	adapter.adaptationMu.Lock()
	adapter.adaptations["col1"] = &IndexAdaptation{
		CollectionName: "col1",
		CurrentIndex:   types.IndexTypeHNSW,
		Status:         AdaptationStatusComplete,
	}
	adapter.adaptationMu.Unlock()

	err := adapter.Rollback("col1")
	assert.Error(t, err, "Rollback without an IndexSwitcher must return an error, not silently succeed")
}

// TestRollback_UnknownCollection_ReturnsError runs the test rollback_unknown collection_returns error operation.
func TestRollback_UnknownCollection_ReturnsError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{
		EnableRollback: true,
	}, nil)

	sw := &mockSwitcher{}
	adapter.WithIndexSwitcher(sw)

	err := adapter.Rollback("does-not-exist")
	assert.Error(t, err)
	assert.False(t, sw.switchCalled, "SwitchIndex must not be called for an unknown collection")
}

// =============================================================================
// extractFeatureVector Tests
// =============================================================================

// TestExtractFeatureVector_ComplexityMapping runs the test extract feature vector_complexity mapping operation.
func TestExtractFeatureVector_ComplexityMapping(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	cases := []struct {
		complexity string
		want       float64
	}{
		{"simple", 0.0},
		{"medium", 0.5},
		{"complex", 1.0},
		{"unknown", 0.25},
		{"", 0.25},
	}
	for _, tc := range cases {
		fv := extractFeatureVector(QueryFeatures{QueryComplexity: tc.complexity})
		assert.InDelta(t, tc.want, fv[5], 1e-9, "complexity=%q", tc.complexity)
	}
}

// TestExtractFeatureVector_BoolFields runs the test extract feature vector_bool fields operation.
func TestExtractFeatureVector_BoolFields(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	fvFiltered := extractFeatureVector(QueryFeatures{IsFiltered: true})
	assert.InDelta(t, 1.0, fvFiltered[7], 1e-9)

	fvHybrid := extractFeatureVector(QueryFeatures{IsHybrid: true})
	assert.InDelta(t, 1.0, fvHybrid[8], 1e-9)

	fvBoth := extractFeatureVector(QueryFeatures{})
	assert.InDelta(t, 0.0, fvBoth[7], 1e-9)
	assert.InDelta(t, 0.0, fvBoth[8], 1e-9)
}

// =============================================================================
// weightedEuclidean Tests
// =============================================================================

// TestWeightedEuclidean_ZeroDistance runs the test weighted euclidean_zero distance operation.
func TestWeightedEuclidean_ZeroDistance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	var v [numFeatures]float64
	for i := range v {
		v[i] = float64(i) * 0.1
	}
	var w [numFeatures]float64
	for i := range w {
		w[i] = 1.0 / numFeatures
	}
	dist := weightedEuclidean(v, v, w)
	assert.InDelta(t, 0.0, dist, 1e-9, "distance to self must be zero")
}

// TestWeightedEuclidean_Symmetry runs the test weighted euclidean_symmetry operation.
func TestWeightedEuclidean_Symmetry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	var a, b, w [numFeatures]float64
	for i := range a {
		a[i] = float64(i)
		b[i] = float64(numFeatures - i)
		w[i] = 1.0 / numFeatures
	}
	assert.InDelta(t, weightedEuclidean(a, b, w), weightedEuclidean(b, a, w), 1e-9,
		"weighted Euclidean distance must be symmetric")
}

// TestRuntimeIndexAdapter_FeedbackLoop runs the test runtime index adapter_feedback loop operation.
func TestRuntimeIndexAdapter_FeedbackLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 1,
	})
	config := IndexAdaptationConfig{
		EnableAutoAdaptation: true,
	}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	collection := "test-feedback"
	features := QueryFeatures{DatasetSize: 50000}

	// 1. Manually trigger an adaptation entry
	adapter.adaptationMu.Lock()
	adapter.adaptations[collection] = &IndexAdaptation{
		CollectionName: collection,
		ProposedIndex:  types.IndexTypeDiskANN,
		Status:         AdaptationStatusRunning,
		Features:       features,
		Metrics: AdaptationMetrics{
			AvgLatencyMs: 150.0,
		},
	}
	adapter.adaptationMu.Unlock()

	// 2. Complete with success
	err := adapter.CompleteAdaptation(collection, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), predictor.stats.TrainingSamplesCollected.Load())

	// 3. Trigger another one
	adapter.adaptationMu.Lock()
	adapter.adaptations[collection] = &IndexAdaptation{
		CollectionName: collection,
		ProposedIndex:  LearnedIVFPQ,
		Status:         AdaptationStatusRunning,
		Features:       features,
	}
	adapter.adaptationMu.Unlock()

	// 4. Complete with failure (should record negative signal)
	err = adapter.CompleteAdaptation(collection, false)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), predictor.stats.TrainingSamplesCollected.Load())

	// Verify that the second sample has the failure penalty
	predictor.samplesMu.RLock()
	lastSample := predictor.samples[len(predictor.samples)-1]
	predictor.samplesMu.RUnlock()

	assert.Equal(t, LearnedIVFPQ, lastSample.Index)
	assert.Equal(t, 10*time.Second, lastSample.Latency) // Failure penalty
	assert.Equal(t, 0.0, lastSample.Recall)
}
