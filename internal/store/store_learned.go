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
// The migration is performed by building a new index in memory from existing records
// and then atomically swapping the index pointer in the dataset.
func (s *VectorStore) SwitchIndex(collection string, to IndexType) error {
	ds, ok := s.getDataset(collection)
	if !ok {
		return fmt.Errorf("dataset %q not found", collection)
	}

	ds.dataMu.RLock()
	currentIndex := ds.Index
	ds.dataMu.RUnlock()

	if currentIndex != nil {
		if typed, ok := currentIndex.(interface{ Type() IndexType }); ok {
			if typed.Type() == to {
				return nil // Already using the target index type
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
		Msg("Starting live index migration")

	// 1. Create the replacement index using the factory.
	factory := NewIndexFactory()
	cfg := IndexConfig{
		Type:      to,
		Dimension: int(ds.Schema.Field(1).Type.(*arrow.FixedSizeListType).Len()), // Simplified dimension extraction
	}
	// TODO: Pull optimal HNSW/DiskANN parameters from a registry or predictor recommendation.
	newIdx, err := factory.Create(cfg)
	if err != nil {
		return fmt.Errorf("failed to create replacement index: %w", err)
	}

	// 2. Populate the new index from existing records.
	// We iterate over the records under RLock to prevent them from being deleted/evicted,
	// though records are append-only.
	ds.dataMu.RLock()
	records := make([]arrow.RecordBatch, len(ds.Records))
	copy(records, ds.Records)
	ds.dataMu.RUnlock()

	for _, rec := range records {
		ids, vectors, err := s.extractVectorsFromRecord(rec)
		if err != nil {
			_ = newIdx.Close() // #nosec G104
			return fmt.Errorf("failed to extract vectors for migration: %w", err)
		}

		if err := newIdx.AddBatch(ids, vectors); err != nil {
			_ = newIdx.Close() // #nosec G104
			return fmt.Errorf("failed to add vectors to new index: %w", err)
		}
	}

	// 3. Build the index if required (e.g. DiskANN, IVF).
	if newIdx.NeedsBuild() {
		if err := newIdx.Build(); err != nil {
			_ = newIdx.Close() // #nosec G104
			return fmt.Errorf("failed to build replacement index: %w", err)
		}
	}

	// 4. Atomic Swap.
	ds.dataMu.Lock()
	oldIdx := ds.Index
	ds.Index = NewPluggableInternalAdapter(newIdx) // Bridge the pluggable and internal interface
	ds.dataMu.Unlock()

	// 5. Cleanup.
	if oldIdx != nil {
		_ = oldIdx.Close() // #nosec G104
	}

	s.logger.Info().
		Str("collection", collection).
		Msg("Live index migration completed successfully")

	return nil
}

// extractVectorsFromRecord extracts IDs and vector data from an Arrow RecordBatch.
func (s *VectorStore) extractVectorsFromRecord(rec arrow.RecordBatch) ([]uint64, [][]float32, error) {
	// Identify columns
	idColIdx := -1
	vectorColIdx := -1
	for i, f := range rec.Schema().Fields() {
		if f.Name == "id" {
			idColIdx = i
		} else if f.Name == "vector" {
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
