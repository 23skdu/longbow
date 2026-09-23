package index

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

const (
	// LearnedIndexTypeAuto allows the system to automatically select the best index type.
	LearnedIndexTypeAuto IndexType = "auto"
	// LearnedIVFPQ specifies a learned IVF-PQ index.
	LearnedIVFPQ IndexType = "ivf_pq"
)

// numFeatures is the number of dimensions in the feature vector derived from QueryFeatures.
const numFeatures = 13

// featureKeys maps feature-vector index positions to the names used in featureWeights.
var featureKeys = [numFeatures]string{
	"vector_dimension",
	"num_query_vectors",
	"search_k",
	"dataset_size",
	"num_collections",
	"query_complexity",
	"avg_vector_norm",
	"is_filtered",
	"is_hybrid",
	"time_of_day",
	"day_of_week",
	// Embedding-generator features (added 2026-04-21)
	"embedding_provider",  // ordinal: none=0, openai=1, cohere=2, huggingface=3, onnx=4, wasm=5, local=6
	"embedding_model_dim", // ratio: VectorDimension / 384.0 (reference dim for sentence-transformers)
}

// QueryFeatures encapsulates signals used to predict the optimal index type for a search query.
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
	// EmbeddingProvider identifies the backend that generated the query vectors.
	// Valid values: "", "openai", "cohere", "huggingface", "onnx", "wasm", "local".
	EmbeddingProvider string `json:"embedding_provider,omitempty"`
	// EmbeddingModel is the specific model name within the provider (e.g. "text-embedding-3-small").
	EmbeddingModel string `json:"embedding_model,omitempty"`
}

// IndexPrediction contains the recommended index type and its estimated performance.
type IndexPrediction struct {
	RecommendedIndex IndexType     `json:"recommended_index"`
	Confidence       float64       `json:"confidence"`
	EstimatedLatency time.Duration `json:"estimated_latency"`
	EstimatedRecall  float64       `json:"estimated_recall"`
	Alternatives     []IndexType   `json:"alternatives"`
}

// TrainingSample represents an observed search performance event used for model training.
type TrainingSample struct {
	Features QueryFeatures
	Latency  time.Duration
	Recall   float64
	Index    IndexType
}

// IndexRecommendationAPI provides external access to index selection recommendations.
type IndexRecommendationAPI struct {
	logger      zerolog.Logger
	predictor   *IndexPerformancePredictor
	mapper      *QueryIndexMapper
	recommender *IndexRecommendationEngine
	stats       APIStats
}

// APIStats tracks the usage and reliability of the recommendation API.
type APIStats struct {
	RecommendationsGiven atomic.Int64
	APIErrors            atomic.Int64
}

// IndexRecommendationEngine maintains history and manages the recommendation logic.
type IndexRecommendationEngine struct {
	logger    zerolog.Logger
	history   []RecommendationRecord
	historyMu sync.RWMutex
}

// RecommendationRecord stores a single recommendation event and its acceptance state.
type RecommendationRecord struct {
	QueryID        string
	Features       QueryFeatures
	Recommendation IndexPrediction
	Timestamp      time.Time
	Accepted       bool
}

// NewIndexRecommendationAPI creates a new recommendation API instance.
func NewIndexRecommendationAPI(logger zerolog.Logger, predictor *IndexPerformancePredictor, mapper *QueryIndexMapper) *IndexRecommendationAPI {
	return &IndexRecommendationAPI{
		logger:      logger,
		predictor:   predictor,
		mapper:      mapper,
		recommender: &IndexRecommendationEngine{logger: logger},
	}
}

// GetRecommendation generates an index recommendation based on query features.
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

// GetRecommendationWithContext generates a recommendation using both query features and historical ID mapping.
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

// AcceptRecommendation records that a generated recommendation was accepted by the user.
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

// GetRecommendationHistory returns a list of recent recommendation events.
func (api *IndexRecommendationAPI) GetRecommendationHistory() []RecommendationRecord {
	api.recommender.historyMu.RLock()
	defer api.recommender.historyMu.RUnlock()

	result := make([]RecommendationRecord, len(api.recommender.history))
	copy(result, api.recommender.history)
	return result
}

// GetStats returns the current API usage statistics.
func (api *IndexRecommendationAPI) GetStats() (recommendations, errors int64) {
	return api.stats.RecommendationsGiven.Load(),
		api.stats.APIErrors.Load()
}

// GetAcceptanceRate calculates the percentage of recommendations that were accepted by users.
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

// GetTopRecommendations returns the most frequently recommended index configurations.
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
