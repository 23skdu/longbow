//go:build gpu && darwin && arm64
// +build gpu,darwin,arm64

package metal

import (
	"context"
	"errors"
	"sync"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/store"
)

// MetalReranker uses Metal ONNX for cross-encoder reranking
type MetalReranker struct {
	engine    *MetalEngine
	modelPath string
	mu        sync.RWMutex
}

// NewMetalReranker creates a new Metal-based reranker
func NewMetalReranker(modelPath string) (*MetalReranker, error) {
	if modelPath == "" {
		return nil, errors.New("model path is required")
	}

	if !IsAvailable() {
		return nil, errors.New("Metal is not available on this platform")
	}

	engine, err := NewMetalEngine()
	if err != nil {
		return nil, err
	}

	// Try to load model
	if err := engine.LoadModel(modelPath); err != nil {
		// Don't fail - we'll use fallback
	}

	r := &MetalReranker{
		engine:    engine,
		modelPath: modelPath,
	}

	metrics.OnnxMetalModelLoaded.Set(1)

	return r, nil
}

// Rerank reranks results using Metal ONNX
func (r *MetalReranker) Rerank(ctx context.Context, query string, results []store.SearchResult) ([]store.SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}

	if r.engine == nil || !r.engine.loaded {
		// Fallback to heuristic
		hr := &store.CrossEncoderReranker{ModelName: "fallback"}
		return hr.Rerank(ctx, query, results)
	}

	// Extract documents
	documents := make([]string, len(results))
	for i, result := range results {
		if result.Metadata != nil {
			if text, ok := result.Metadata["text"].(string); ok {
				documents[i] = text
			} else if content, ok := result.Metadata["content"].(string); ok {
				documents[i] = content
			} else if desc, ok := result.Metadata["description"].(string); ok {
				documents[i] = desc
			}
		}
		if documents[i] == "" {
			documents[i] = result.ID.String()
		}
	}

	// Run inference
	scores, err := r.engine.Score(ctx, query, documents)
	if err != nil {
		metrics.RerankerErrors.WithLabelValues("inference").Inc()
		hr := &store.CrossEncoderReranker{ModelName: "fallback"}
		return hr.Rerank(ctx, query, results)
	}

	// Combine scores
	type scoredResult struct {
		result store.SearchResult
		score  float32
	}

	scored := make([]scoredResult, len(results))
	for i, result := range results {
		mlScore := scores[i]
		distanceScore := 1.0 / (1.0 + float32(result.Distance))
		finalScore := 0.7*mlScore + 0.3*distanceScore
		scored[i] = scoredResult{result: result, score: finalScore}
	}

	// Sort by score
	for i := 0; i < len(scored)-1; i++ {
		for j := i + 1; j < len(scored); j++ {
			if scored[j].score > scored[i].score {
				scored[i], scored[j] = scored[j], scored[i]
			}
		}
	}

	// Build result
	reranked := make([]store.SearchResult, len(results))
	for i, sr := range scored {
		reranked[i] = sr.result
		reranked[i].Score = sr.score
	}

	metrics.RerankerScoresComputed.Add(float64(len(results)))

	return reranked, nil
}

// Close releases resources
func (r *MetalReranker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.engine != nil {
		r.engine.Close()
		r.engine = nil
	}

	metrics.OnnxMetalModelLoaded.Set(0)

	return nil
}
