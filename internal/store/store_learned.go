package store

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// SetIndexPredictor sets the learned index performance predictor.
func (s *VectorStore) SetIndexPredictor(predictor *IndexPerformancePredictor) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	s.indexPredictor = predictor
}

// GetIndexPredictor returns the learned index performance predictor.
func (s *VectorStore) GetIndexPredictor() *IndexPerformancePredictor {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.indexPredictor
}

// GetIndexRecommendation returns an index recommendation based on query features.
func (s *VectorStore) GetIndexRecommendation(features QueryFeatures) IndexPrediction {
	s.configMu.RLock()
	predictor := s.indexPredictor
	s.configMu.RUnlock()

	if predictor == nil {
		return IndexPrediction{
			RecommendedIndex: IndexTypeHNSW,
			Confidence:       0.0,
			EstimatedLatency: 0,
			EstimatedRecall:  0.0,
			Alternatives:     []IndexType{IndexTypeHNSW},
		}
	}

	return predictor.Predict(features)
}

// RecordQueryPerformance records a query—including its embedding context—for training
// the index predictor. provider and model identify the EmbeddingGenerator backend that
// produced the query vectors; pass empty strings when no embedding generator was used.
func (s *VectorStore) RecordQueryPerformance(features QueryFeatures, latency float64, recall float64, index IndexType, provider, model string) {
	s.configMu.RLock()
	predictor := s.indexPredictor
	s.configMu.RUnlock()

	if predictor != nil {
		features.EmbeddingProvider = provider
		features.EmbeddingModel = model
		predictor.AddTrainingSample(TrainingSample{
			Features: features,
			Latency:  0,
			Recall:   recall,
			Index:    index,
		})
	}
}

// SetActiveEmbedding registers the currently active EmbeddingGenerator provider and model
// with the store. Call this after provisioning an EmbeddingGenerator so that SearchHybrid
// and other search paths can automatically attach embedding context to training samples.
func (s *VectorStore) SetActiveEmbedding(provider, model string) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	s.activeEmbeddingProvider = provider
	s.activeEmbeddingModel = model
}

// GetActiveEmbedding returns the currently active embedding provider and model.
func (s *VectorStore) GetActiveEmbedding() (provider, model string) {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.activeEmbeddingProvider, s.activeEmbeddingModel
}


// SetCDC sets the Change Data Capture instance.
func (s *VectorStore) SetCDC(cdc *ChangeDataCapture) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	s.cdc = cdc
}

// GetCDC returns the Change Data Capture instance.
func (s *VectorStore) GetCDC() *ChangeDataCapture {
	s.configMu.RLock()
	defer s.configMu.RUnlock()
	return s.cdc
}

// SwitchIndex performs a live migration of a collection's vector index to a new type.
// It implements the IndexSwitcher interface used by the adaptive learned index loop.
// The migration is performed in the background by building a new index from source 
// records and then atomically swapping the index pointer in the dataset.
func (s *VectorStore) SwitchIndex(collection string, to IndexType) error {
	ds, ok := s.getDataset(collection)
	if !ok {
		return fmt.Errorf("dataset %q not found", collection)
	}

	// 1. Check if a switch is already in progress for this collection
	if _, loaded := s.activeSwitches.LoadOrStore(collection, true); loaded {
		return fmt.Errorf("index switch already in progress for collection %q", collection)
	}

	// 2. Perform the switch in a background goroutine
	go func() {
		defer s.activeSwitches.Delete(collection)

		ds.dataMu.RLock()
		currentIndex := ds.Index
		ds.dataMu.RUnlock()

		if currentIndex != nil {
			if typed, ok := currentIndex.(interface{ Type() IndexType }); ok {
				if typed.Type() == to {
					s.logger.Info().Str("collection", collection).Str("type", string(to)).Msg("Already using target index type, skipping switch")
					return
				}
			}
		}

		fromType := IndexType("unknown")
		if typed, ok := currentIndex.(interface{ Type() IndexType }); ok {
			fromType = typed.Type()
		}

		s.logger.Info().
			Str("collection", collection).
			Str("from", string(fromType)).
			Str("to", string(to)).
			Msg("Starting background live index migration")

		// 3. Create the replacement index using the factory.
		factory := NewIndexFactory()
		cfg := IndexConfig{
			Type:      to,
			Dimension: int(ds.Schema.Field(1).Type.(*arrow.FixedSizeListType).Len()),
		}
		newIdx, err := factory.Create(cfg)
		if err != nil {
			s.logger.Error().Err(err).Str("collection", collection).Msg("Failed to create replacement index")
			if s.indexAdapter != nil {
				_ = s.indexAdapter.CompleteAdaptation(collection, false)
			}
			return
		}

		// 4. Populate the new index from existing records.
		ds.dataMu.RLock()
		records := make([]arrow.RecordBatch, len(ds.Records))
		copy(records, ds.Records)
		ds.dataMu.RUnlock()

		for _, rec := range records {
			ids, vectors, err := s.extractVectorsFromRecord(rec)
			if err != nil {
				s.logger.Error().Err(err).Str("collection", collection).Msg("Failed to extract vectors for migration")
				_ = newIdx.Close()
				if s.indexAdapter != nil {
					_ = s.indexAdapter.CompleteAdaptation(collection, false)
				}
				return
			}

			if err := newIdx.AddBatch(ids, vectors); err != nil {
				s.logger.Error().Err(err).Str("collection", collection).Msg("Failed to add vectors to new index")
				_ = newIdx.Close()
				if s.indexAdapter != nil {
					_ = s.indexAdapter.CompleteAdaptation(collection, false)
				}
				return
			}
		}

		// 5. Build the index if required.
		if newIdx.NeedsBuild() {
			if err := newIdx.Build(); err != nil {
				s.logger.Error().Err(err).Str("collection", collection).Msg("Failed to build replacement index")
				_ = newIdx.Close()
				if s.indexAdapter != nil {
					_ = s.indexAdapter.CompleteAdaptation(collection, false)
				}
				return
			}
		}

		// 6. Atomic Swap.
		ds.dataMu.Lock()
		oldIdx := ds.Index
		ds.Index = NewPluggableInternalAdapter(newIdx)
		ds.dataMu.Unlock()

		// 7. Cleanup and Callback.
		if oldIdx != nil {
			_ = oldIdx.Close()
		}

		if s.indexAdapter != nil {
			_ = s.indexAdapter.CompleteAdaptation(collection, true)
		}

		s.logger.Info().
			Str("collection", collection).
			Str("to", string(to)).
			Msg("Background index migration completed successfully")
	}()

	return nil
}

// extractVectorsFromRecord extracts IDs and vector data from an Arrow RecordBatch.
func (s *VectorStore) extractVectorsFromRecord(rec arrow.RecordBatch) ([]uint64, [][]float32, error) {
	// Identify columns
	idColIdx := -1
	vectorColIdx := -1
	for i, f := range rec.Schema().Fields() {
		switch f.Name {
		case "id":
			idColIdx = i
		case "vector":
			vectorColIdx = i
		}
	}

	if idColIdx == -1 || vectorColIdx == -1 {
		return nil, nil, fmt.Errorf("record missing required columns (id: %d, vector: %d)", idColIdx, vectorColIdx)
	}

	numRows := int(rec.NumRows())
	ids := make([]uint64, numRows)
	vectors := make([][]float32, numRows)

	// Extract IDs
	idCol := rec.Column(idColIdx)
	switch arr := idCol.(type) {
	case *array.Int64:
		for i := 0; i < numRows; i++ {
			ids[i] = uint64(arr.Value(i)) // #nosec G115
		}
	case *array.Uint64:
		for i := 0; i < numRows; i++ {
			ids[i] = arr.Value(i)
		}
	default:
		return nil, nil, fmt.Errorf("unsupported ID column type: %T", idCol)
	}

	// Extract Vectors (assuming FixedSizeList of Float32)
	vecCol := rec.Column(vectorColIdx).(*array.FixedSizeList)
	listValues := vecCol.ListValues().(*array.Float32)
	dim := vecCol.DataType().(*arrow.FixedSizeListType).Len()

	for i := 0; i < numRows; i++ {
		start := i * int(dim)
		end := (i + 1) * int(dim)
		vectors[i] = listValues.Float32Values()[start:end]
	}

	return ids, vectors, nil
}

// GetWebSocketServer returns the WebSocket server instance.
func (s *VectorStore) GetWebSocketServer() *WebSocketServer {
	return nil
}

// MetricsCollector Implementation

func (s *VectorStore) GetCollections() []string {
	var names []string
	s.IterateDatasets(func(name string, _ *Dataset) {
		names = append(names, name)
	})
	return names
}

func (s *VectorStore) GetQueryLatencies(collection string) (p50, p99, avg float64) {
	ds, ok := s.getDataset(collection)
	if !ok || ds.queryStats == nil {
		return 0, 0, 0
	}
	p50, p99, avg, _, _ = ds.queryStats.GetMetrics()
	return
}

func (s *VectorStore) GetQueriesPerSecond(collection string) float64 {
	ds, ok := s.getDataset(collection)
	if !ok || ds.queryStats == nil {
		return 0
	}
	_, _, _, _, qps := ds.queryStats.GetMetrics()
	return qps
}

func (s *VectorStore) GetRecall(collection string) float64 {
	ds, ok := s.getDataset(collection)
	if !ok || ds.queryStats == nil {
		return 0
	}
	_, _, _, recall, _ := ds.queryStats.GetMetrics()
	return recall
}

func (s *VectorStore) GetIndexSize(collection string) float64 {
	ds, ok := s.getDataset(collection)
	if !ok {
		return 0
	}
	return float64(ds.IndexMemoryBytes.Load()) / 1024.0 / 1024.0
}

func (s *VectorStore) GetMemoryUsage(collection string) float64 {
	ds, ok := s.getDataset(collection)
	if !ok {
		return 0
	}
	return float64(ds.SizeBytes.Load()) / 1024.0 / 1024.0
}
