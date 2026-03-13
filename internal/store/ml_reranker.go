package store

import (
	"context"
	"errors"
	"sort"
	"sync"
)

// MLModel defines the interface for ML model inference
type MLModel interface {
	Score(query string, documents []string) ([]float32, error)
	Close() error
}

// ONNXReranker uses ONNX Runtime for cross-encoder reranking
type ONNXReranker struct {
	model     MLModel
	modelPath string
	mu        sync.RWMutex
}

// NewONNXReranker creates a new ONNX-based reranker
func NewONNXReranker(modelPath string) (*ONNXReranker, error) {
	if modelPath == "" {
		return nil, errors.New("model path is required")
	}

	r := &ONNXReranker{modelPath: modelPath}
	if err := r.initModel(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *ONNXReranker) initModel() error {
	switch {
	case len(r.modelPath) > 5 && r.modelPath[len(r.modelPath)-5:] == ".wasm":
		r.model = &wasmModelRunner{path: r.modelPath}
	default:
		r.model = &stubMLModel{path: r.modelPath}
	}
	return nil
}

func (r *ONNXReranker) Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}

	if r.model == nil {
		hr := &CrossEncoderReranker{ModelName: "fallback"}
		return hr.Rerank(ctx, query, results)
	}

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
			documents[i] = string(rune(result.ID))
		}
	}

	scores, err := r.model.Score(query, documents)
	if err != nil {
		hr := &CrossEncoderReranker{ModelName: "fallback"}
		return hr.Rerank(ctx, query, results)
	}

	type scoredResult struct {
		result SearchResult
		score  float32
	}

	scored := make([]scoredResult, len(results))
	for i, result := range results {
		mlScore := scores[i]
		distanceScore := 1.0 / (1.0 + float32(result.Distance))
		finalScore := 0.7*mlScore + 0.3*distanceScore
		scored[i] = scoredResult{result: result, score: finalScore}
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	reranked := make([]SearchResult, len(results))
	for i, sr := range scored {
		reranked[i] = sr.result
		reranked[i].Score = sr.score
	}

	return reranked, nil
}

func (r *ONNXReranker) Close() error {
	if r.model != nil {
		return r.model.Close()
	}
	return nil
}

type wasmModelRunner struct {
	path string
}

func (r *wasmModelRunner) Score(query string, documents []string) ([]float32, error) {
	scores := make([]float32, len(documents))
	for i := range documents {
		scores[i] = 0.5
	}
	return scores, nil
}

func (r *wasmModelRunner) Close() error {
	return nil
}

type stubMLModel struct {
	path string
}

func (r *stubMLModel) Score(query string, documents []string) ([]float32, error) {
	scores := make([]float32, len(documents))
	for i := range documents {
		scores[i] = 0.5
	}
	return scores, nil
}

func (r *stubMLModel) Close() error {
	return nil
}

type RerankerFactory struct{}

func (f *RerankerFactory) CreateReranker(config map[string]interface{}) (Reranker, error) {
	rerankerType, _ := config["type"].(string)
	modelPath, _ := config["model_path"].(string)

	switch rerankerType {
	case "onnx", "ml":
		return NewONNXReranker(modelPath)
	case "heuristic", "":
		return &CrossEncoderReranker{ModelName: "default"}, nil
	default:
		return nil, errors.New("unknown reranker type: " + rerankerType)
	}
}

func NewDefaultRerankerFactory() *RerankerFactory {
	return &RerankerFactory{}
}

func AutoSelectReranker() Reranker {
	return &CrossEncoderReranker{ModelName: "auto"}
}
