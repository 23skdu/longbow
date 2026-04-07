package store

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

// RecordQueryPerformance records a query for training the index predictor.
func (s *VectorStore) RecordQueryPerformance(features QueryFeatures, latency float64, recall float64, index IndexType) {
	s.configMu.RLock()
	predictor := s.indexPredictor
	s.configMu.RUnlock()

	if predictor != nil {
		predictor.AddTrainingSample(TrainingSample{
			Features: features,
			Latency:  0,
			Recall:   recall,
			Index:    index,
		})
	}
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

// GetWebSocketServer returns the WebSocket server instance.
func (s *VectorStore) GetWebSocketServer() *WebSocketServer {
	return nil
}
