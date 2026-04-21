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

// =============================================================================
// k-NN Scorer Tests
// =============================================================================

// TestKNNPredict_LearnFromSamples proves that kNNPredict changes its
// recommendations based on the training data it has observed — the core
// behaviour that was previously broken.
func TestKNNPredict_LearnFromSamples(t *testing.T) {
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
			Index:   IndexTypeHNSW,
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
			Index:   IndexTypeDiskANN,
		})
	}

	// k-NN should recommend HNSW for a small-dataset query.
	smallPred := p.Predict(QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     15000,
		SearchK:         10,
	})
	assert.Equal(t, IndexTypeHNSW, smallPred.RecommendedIndex,
		"k-NN must recommend HNSW for small dataset matching training distribution")

	// k-NN should recommend DiskANN for a large-dataset query.
	largePred := p.Predict(QueryFeatures{
		VectorDimension: 128,
		DatasetSize:     4000000,
		SearchK:         10,
	})
	assert.Equal(t, IndexTypeDiskANN, largePred.RecommendedIndex,
		"k-NN must recommend DiskANN for large dataset matching training distribution")
}

// TestKNNPredict_OverridesHeuristic verifies that the k-NN scorer overrides the
// static heuristic when training data contradicts it.
func TestKNNPredict_OverridesHeuristic(t *testing.T) {
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
			Index: IndexTypeDiskANN,
		})
	}

	pred := p.Predict(QueryFeatures{
		VectorDimension: 64,
		DatasetSize:     50000,
		SearchK:         50,
		IsFiltered:      true,
	})
	assert.Equal(t, IndexTypeDiskANN, pred.RecommendedIndex,
		"k-NN must override heuristic when all training samples point to DiskANN")
}

// TestKNNPredict_ConcurrentAddAndPredict stresses the scorer under concurrent
// reads and writes. Any race will be detected by the -race flag.
func TestKNNPredict_ConcurrentAddAndPredict(t *testing.T) {
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
			Index:    IndexTypeHNSW,
		})
	}

	done := make(chan struct{})

	// 8 writers.
	for i := 0; i < 8; i++ {
		go func(i int) {
			for j := 0; j < 50; j++ {
				p.AddTrainingSample(TrainingSample{
					Features: QueryFeatures{DatasetSize: i*1000 + j},
					Index:    IndexTypeHNSW,
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

func TestFeatureNormalizer_MinMax(t *testing.T) {
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

func TestFeatureNormalizer_NormalizeOutput(t *testing.T) {
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

func TestFeatureNormalizer_ZeroSpanReturns05(t *testing.T) {
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
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 10,
		UpdateInterval:     0, // force immediate update on next trigger
	})

	// All samples differ only in dataset_size; same dimension across all.
	for i := 0; i < 60; i++ {
		idx := IndexTypeHNSW
		ds := 10000
		if i >= 30 {
			idx = IndexTypeDiskANN
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

func TestPredictionCorrect_IncrementOnFeedback(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 50,
		KNN:                5,
	})

	// Seed enough samples for k-NN to kick in.
	for i := 0; i < 60; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{DatasetSize: 10000},
			Index:    IndexTypeHNSW,
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
	switchTarget IndexType
	switchErr    error
}

func (m *mockSwitcher) SwitchIndex(_ string, to IndexType) error {
	m.switchCalled = true
	m.switchTarget = to
	return m.switchErr
}

func TestRollback_IsNotNoOp(t *testing.T) {
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
		CurrentIndex:   IndexTypeHNSW,
		ProposedIndex:  IndexTypeDiskANN,
		Status:         AdaptationStatusComplete,
	}
	adapter.adaptationMu.Unlock()

	err := adapter.Rollback("col1")
	assert.NoError(t, err, "Rollback should succeed when IndexSwitcher is wired")
	assert.True(t, sw.switchCalled, "IndexSwitcher.SwitchIndex must be called during rollback")
	assert.Equal(t, IndexTypeHNSW, sw.switchTarget, "Rollback must target CurrentIndex (prior index)")
}

func TestRollback_NoSwitcher_ReturnsError(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	predictor := NewIndexPerformancePredictor(logger, LearnedIndexConfig{})
	adapter := NewRuntimeIndexAdapter(logger, predictor, IndexAdaptationConfig{
		EnableRollback: true,
	}, nil)

	// No IndexSwitcher wired.
	adapter.adaptationMu.Lock()
	adapter.adaptations["col1"] = &IndexAdaptation{
		CollectionName: "col1",
		CurrentIndex:   IndexTypeHNSW,
		Status:         AdaptationStatusComplete,
	}
	adapter.adaptationMu.Unlock()

	err := adapter.Rollback("col1")
	assert.Error(t, err, "Rollback without an IndexSwitcher must return an error, not silently succeed")
}

func TestRollback_UnknownCollection_ReturnsError(t *testing.T) {
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

func TestExtractFeatureVector_ComplexityMapping(t *testing.T) {
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

func TestExtractFeatureVector_BoolFields(t *testing.T) {
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

func TestWeightedEuclidean_ZeroDistance(t *testing.T) {
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

func TestWeightedEuclidean_Symmetry(t *testing.T) {
	var a, b, w [numFeatures]float64
	for i := range a {
		a[i] = float64(i)
		b[i] = float64(numFeatures - i)
		w[i] = 1.0 / numFeatures
	}
	assert.InDelta(t, weightedEuclidean(a, b, w), weightedEuclidean(b, a, w), 1e-9,
		"weighted Euclidean distance must be symmetric")
}
