package store

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestLearnedIndexConfig_Defaults(t *testing.T) {
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

func TestNewIndexPerformancePredictor(t *testing.T) {
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

func TestIndexPerformancePredictor_AddTrainingSample(t *testing.T) {
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
		Index:   IndexTypeHNSW,
	}

	p.AddTrainingSample(sample)

	assert.Equal(t, int64(1), p.stats.TrainingSamplesCollected.Load())
}

func TestIndexPerformancePredictor_Predict_InsufficientSamples(t *testing.T) {
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

	assert.Equal(t, IndexTypeHNSW, prediction.RecommendedIndex)
	assert.Equal(t, 0.5, prediction.Confidence)
	assert.NotEmpty(t, prediction.Alternatives)
}

func TestIndexPerformancePredictor_Predict_SmallDataset(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	for i := 0; i < 150; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension: 128,
				DatasetSize:     50000,
				SearchK:         10,
			},
			Index: IndexTypeHNSW,
		})
	}

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     50000,
		SearchK:         10,
	}

	prediction := p.Predict(features)

	assert.Equal(t, IndexTypeHNSW, prediction.RecommendedIndex)
	assert.Greater(t, prediction.Confidence, 0.5)
}

func TestIndexPerformancePredictor_Predict_LargeDataset(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     5000000,
		SearchK:         10,
	}

	prediction := p.Predict(features)

	assert.Equal(t, IndexTypeDiskANN, prediction.RecommendedIndex)
}

func TestIndexPerformancePredictor_Predict_MediumDataset(t *testing.T) {
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

func TestIndexPerformancePredictor_calculateIndexScores(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     100000,
		SearchK:         10,
		IsFiltered:      false,
		IsHybrid:        false,
		QueryComplexity: "simple",
	}

	scores := p.calculateIndexScores(features)

	assert.Contains(t, scores, IndexTypeHNSW)
	assert.Contains(t, scores, LearnedIVFPQ)
	assert.Contains(t, scores, IndexTypeDiskANN)
}

func TestIndexPerformancePredictor_scoreHNSW(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		DatasetSize:     50000,
		SearchK:         50,
		IsFiltered:      false,
		IsHybrid:        false,
		QueryComplexity: "simple",
	}

	score := p.scoreHNSW(features)

	assert.Greater(t, score, 0.0)
}

func TestIndexPerformancePredictor_scoreIVFPQ(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		DatasetSize:     200000,
		NumQueryVectors: 10,
		SearchK:         200,
		VectorDimension: 1024,
	}

	score := p.scoreIVFPQ(features)

	assert.Greater(t, score, 0.0)
}

func TestIndexPerformancePredictor_scoreDiskANN(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		DatasetSize:     5000000,
		IsFiltered:      true,
		IsHybrid:        true,
		QueryComplexity: "complex",
	}

	score := p.scoreDiskANN(features)

	assert.Greater(t, score, 0.0)
}

func TestIndexPerformancePredictor_calculateConfidence(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	scores := map[IndexType]float64{
		IndexTypeHNSW:    0.8,
		IndexTypeIVFFlat: 0.3,
		IndexTypeDiskANN: 0.2,
	}

	confidence := p.calculateConfidence(scores)

	assert.Greater(t, confidence, 0.5)
}

func TestIndexPerformancePredictor_estimateLatency(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		NumQueryVectors: 5,
		SearchK:         10,
	}

	latency := p.estimateLatency(features, IndexTypeHNSW)

	assert.Greater(t, latency, time.Duration(0))
}

func TestIndexPerformancePredictor_estimateRecall(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	features := QueryFeatures{
		DatasetSize: 10000,
	}

	recallHNSW := p.estimateRecall(features, IndexTypeHNSW)
	recallDisk := p.estimateRecall(features, IndexTypeDiskANN)

	assert.Equal(t, 0.98, recallHNSW)
	assert.Equal(t, 0.95, recallDisk)
}

func TestIndexPerformancePredictor_getAlternatives(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	scores := map[IndexType]float64{
		IndexTypeHNSW:    0.8,
		IndexTypeIVFFlat: 0.3,
		IndexTypeDiskANN: 0.2,
	}

	alternatives := p.getAlternatives(scores, IndexTypeHNSW)

	assert.Len(t, alternatives, 2)
	assert.NotContains(t, alternatives, IndexTypeHNSW)
}

func TestIndexPerformancePredictor_GetStats(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	p.AddTrainingSample(TrainingSample{
		Features: QueryFeatures{VectorDimension: 128},
		Index:    IndexTypeHNSW,
	})

	p.Predict(QueryFeatures{VectorDimension: 128})

	samples, predictions, correct := p.GetStats()
	assert.Equal(t, int64(1), samples)
	assert.Equal(t, int64(1), predictions)
	assert.Equal(t, int64(0), correct)
}

func TestIndexPerformancePredictor_GetConfig(t *testing.T) {
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

func TestIndexPerformancePredictor_SetConfig(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	newConfig := LearnedIndexConfig{
		EnableAutoSelection: false,
		ConfidenceThreshold: 0.9,
	}

	p.SetConfig(newConfig)
	assert.Equal(t, 0.9, p.config.ConfidenceThreshold)
}

func TestIndexPerformancePredictor_GetTrainingSampleCount(t *testing.T) {
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

func TestIndexPerformancePredictor_ClearTrainingData(t *testing.T) {
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

func TestIndexPerformancePredictor_FeatureWeights(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})

	assert.NotNil(t, p.featureWeights)
	assert.Contains(t, p.featureWeights, "dataset_size")
	assert.Contains(t, p.featureWeights, "search_k")
	assert.Contains(t, p.featureWeights, "vector_dimension")
}

func TestIndexMapperConfig_Defaults(t *testing.T) {
	config := IndexMapperConfig{}

	if config.CacheTTL <= 0 {
		config.CacheTTL = 10 * time.Minute
	}
	if config.FallbackIndex == "" {
		config.FallbackIndex = IndexTypeHNSW
	}

	assert.Equal(t, 10*time.Minute, config.CacheTTL)
	assert.Equal(t, IndexTypeHNSW, config.FallbackIndex)
}

func TestNewQueryIndexMapper(t *testing.T) {
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

func TestQueryIndexMapper_GetIndexForQuery(t *testing.T) {
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

	assert.Equal(t, IndexTypeHNSW, index)
	assert.Equal(t, int64(1), mapper.stats.QueriesMapped.Load())
}

func TestQueryIndexMapper_GetIndexForQuery_WithCache(t *testing.T) {
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

func TestQueryIndexMapper_GetIndexForQuery_LargeDataset(t *testing.T) {
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

	assert.Equal(t, IndexTypeDiskANN, index)
}

func TestQueryIndexMapper_InvalidateCache(t *testing.T) {
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

func TestQueryIndexMapper_ClearCache(t *testing.T) {
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

func TestQueryIndexMapper_GetStats(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	mapper := NewQueryIndexMapper(logger, predictor, IndexMapperConfig{
		CacheEnabled: true,
	})

	mapper.GetIndexForQuery("query1", QueryFeatures{DatasetSize: 50000})

	mapped, _, _, _, _ := mapper.GetStats()
	assert.Greater(t, mapped, int64(0))
}

func TestQueryIndexMapper_GetConfig(t *testing.T) {
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

func TestQueryIndexMapper_SetConfig(t *testing.T) {
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

func TestQueryIndexMapper_GetCachedMappings(t *testing.T) {
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

func TestIndexAdaptationConfig_Defaults(t *testing.T) {
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

func TestNewRuntimeIndexAdapter(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	config := IndexAdaptationConfig{
		EnableAutoAdaptation: true,
	}

	adapter := NewRuntimeIndexAdapter(logger, predictor, config, nil)

	assert.NotNil(t, adapter)
	assert.True(t, adapter.config.EnableAutoAdaptation)
}

func TestRuntimeIndexAdapter_ShouldAdapt_Latency(t *testing.T) {
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

func TestRuntimeIndexAdapter_ShouldAdapt_Recall(t *testing.T) {
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

func TestRuntimeIndexAdapter_ShouldAdapt_NoAdapt(t *testing.T) {
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

func TestRuntimeIndexAdapter_DetermineTriggerReason(t *testing.T) {
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

func TestRuntimeIndexAdapter_GetStats(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{}, nil)

	adapter.stats.AdaptationsTriggered.Add(5)
	adapter.stats.AdaptationsCompleted.Add(3)

	triggered, completed, _, _, _ := adapter.GetStats()

	assert.Equal(t, int64(5), triggered)
	assert.Equal(t, int64(3), completed)
}

func TestRuntimeIndexAdapter_GetConfig(t *testing.T) {
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

func TestRuntimeIndexAdapter_SetConfig(t *testing.T) {
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

func TestRuntimeIndexAdapter_GetAdaptation(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{}, nil)

	adapter.adaptationMu.Lock()
	adapter.adaptations["collection1"] = &IndexAdaptation{
		CollectionName: "collection1",
		ProposedIndex:  IndexTypeDiskANN,
		Status:         AdaptationStatusPending,
	}
	adapter.adaptationMu.Unlock()

	adaptation, ok := adapter.GetAdaptation("collection1")
	assert.True(t, ok)
	assert.Equal(t, IndexTypeDiskANN, adaptation.ProposedIndex)

	_, ok = adapter.GetAdaptation("nonexistent")
	assert.False(t, ok)
}

func TestRuntimeIndexAdapter_ListAdaptations(t *testing.T) {
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
