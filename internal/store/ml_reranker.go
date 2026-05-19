package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/23skdu/longbow/internal/onnx"
	"github.com/23skdu/longbow/internal/wasm"
	"github.com/23skdu/longbow/internal/core"
	"github.com/23skdu/longbow/internal/ml"
	"github.com/rs/zerolog"
)

// MLModel defines the interface for ML model inference
// MLModel defines the interface for ML model inference.
type MLModel interface {
	// Score calculates relevance scores for a query and a set of documents.
	Score(query string, documents []string) ([]float32, error)
	// Close releases resources associated with the model.
	Close() error
}

// ONNXReranker uses ONNX Runtime or WASM for cross-encoder reranking.
type ONNXReranker struct {
	model     MLModel
	modelPath string
	logger    zerolog.Logger
	mu        sync.RWMutex
}

// NewONNXReranker creates a new ONNX-based reranker.
func NewONNXReranker(modelPath string, logger zerolog.Logger) (*ONNXReranker, error) {
	if modelPath == "" {
		return nil, errors.New("model path is required")
	}

	r := &ONNXReranker{
		modelPath: modelPath,
		logger:    logger,
	}
	if err := r.initModel(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *ONNXReranker) initModel() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check file extension to determine model type
	if len(r.modelPath) > 5 {
		ext := r.modelPath[len(r.modelPath)-5:]
		switch ext {
		case ".wasm":
			// WebAssembly model - use wazero runtime
			runner, err := wasm.NewRunner(context.Background(), r.modelPath)
			if err != nil {
				r.logger.Warn().Err(err).Str("path", r.modelPath).Msg("Failed to initialize WASM runner, returning error")
				return fmt.Errorf("failed to initialize WASM runner: %w", err)
			}
			tokenizer, _ := ml.NewTokenizer("vocab.txt", 512)
			r.model = &wasmModelWrapper{runner: runner, tokenizer: tokenizer}
			return nil
		case ".onnx":
			// ONNX model - use our internal onnx bridge
			session, err := onnx.NewSession(r.modelPath)
			if err != nil {
				r.logger.Warn().Err(err).Str("path", r.modelPath).Msg("Failed to initialize ONNX session, returning error")
				return fmt.Errorf("failed to initialize ONNX session: %w", err)
			}
			r.model = &onnxModelWrapper{session: session}
			return nil
		}
	}
	return fmt.Errorf("strict model validation failed: unknown model extension for %s (use .onnx or .wasm)", r.modelPath)
}

type onnxModelWrapper struct {
	session *onnx.Session
}

func (w *onnxModelWrapper) Score(query string, documents []string) ([]float32, error) {
	return w.session.Score(context.Background(), query, documents)
}

func (w *onnxModelWrapper) Close() error {
	return w.session.Close()
}

type wasmModelWrapper struct {
	runner    *wasm.Runner
	tokenizer *ml.Tokenizer
}

func (w *wasmModelWrapper) Score(query string, documents []string) ([]float32, error) {
	if w.tokenizer == nil {
		// Fallback to basic tokenizer if not initialized
		tok, _ := ml.NewTokenizer("vocab.txt", 512)
		w.tokenizer = tok
	}

	scores := make([]float32, len(documents))
	for i, doc := range documents {
		combined := query + " " + doc
		ids, mask := w.tokenizer.Encode(combined)
		
		output, err := w.runner.InferenceWithTokens(context.Background(), ids, mask)
		if err != nil {
			return nil, err
		}
		
		if len(output) > 0 {
			scores[i] = output[0]
		} else {
			scores[i] = 0.0
		}
	}
	return scores, nil
}

func (w *wasmModelWrapper) Close() error {
	return w.runner.Close(context.Background())
}

// Rerank performs second-stage reranking on search results using an ML model.
func (r *ONNXReranker) Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}

	r.mu.RLock()
	model := r.model
	r.mu.RUnlock()

	if model == nil {
		hr := &HeuristicReranker{ModelName: "fallback"}
		return hr.Rerank(ctx, query, results)
	}

	documents := make([]string, len(results))
	for i, result := range results {
		if len(result.Metadata) > 0 {
			metaMap, _ := core.DecodeMetadata(result.Metadata)
			if metaMap != nil {
				if text, ok := metaMap["text"].(string); ok {
					documents[i] = text
				} else if content, ok := metaMap["content"].(string); ok {
					documents[i] = content
				} else if desc, ok := metaMap["description"].(string); ok {
					documents[i] = desc
				}
			}
		}
		if documents[i] == "" {
			documents[i] = "placeholder"
		}
	}

	scores, err := model.Score(query, documents)
	if err != nil {
		hr := &HeuristicReranker{ModelName: "fallback"}
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

// Close releases the underlying ML model resources.
func (r *ONNXReranker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.model != nil {
		return r.model.Close()
	}
	return nil
}



// Reranker defines the interface for the second-stage re-ranking
type Reranker interface {
	Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error)
}

// HeuristicReranker implements a second-stage reranker using text-matching heuristics.
type HeuristicReranker struct {
	ModelName string
}

// Rerank re-orders the search results based on a cross-encoder model or heuristic.
func (r *HeuristicReranker) Rerank(ctx context.Context, query string, results []SearchResult) ([]SearchResult, error) {
	if len(results) == 0 {
		return results, nil
	}

	type scoredResult struct {
		result SearchResult
		score  float32
	}

	scored := make([]scoredResult, len(results))
	for i, result := range results {
		score := r.scoreResult(query, result)
		scored[i] = scoredResult{result: result, score: score}
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

func (r *HeuristicReranker) scoreResult(query string, result SearchResult) float32 {
	distanceScore := 1.0 / (1.0 + float32(result.Distance))

	textMatchScore := float32(0.0)
	if len(result.Metadata) > 0 {
		metaMap, _ := core.DecodeMetadata(result.Metadata)
		if metaMap != nil {
			if title, ok := metaMap["title"].(string); ok {
				textMatchScore += r.textMatchScore(query, title)
			}
			if description, ok := metaMap["description"].(string); ok {
				textMatchScore += r.textMatchScore(query, description) * 0.5
			}
			if content, ok := metaMap["content"].(string); ok {
				textMatchScore += r.textMatchScore(query, content) * 0.3
			}
		}
	}

	finalScore := 0.7*distanceScore + 0.3*textMatchScore

	return finalScore
}

func (r *HeuristicReranker) textMatchScore(query, text string) float32 {
	if query == "" || text == "" {
		return 0.0
	}

	queryLower := toLowerCase(query)
	textLower := toLowerCase(text)

	matchCount := 0
	queryTerms := splitWords(queryLower)
	for _, term := range queryTerms {
		if contains(textLower, term) {
			matchCount++
		}
	}

	if len(queryTerms) == 0 {
		return 0.0
	}

	return float32(matchCount) / float32(len(queryTerms))
}

func toLowerCase(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			result[i] = c + 32
		} else {
			result[i] = c
		}
	}
	return string(result)
}

func contains(doc, query string) bool {
	return len(query) > 0 && len(doc) > 0 &&
		(len(doc) >= len(query) &&
			(len(doc) < 100 && findSubstring(doc, query) >= 0))
}

func findSubstring(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func countKeywordMatches(query, doc string) int {
	count := 0
	words := splitWords(query)
	for _, word := range words {
		if len(word) > 2 && findSubstring(doc, word) >= 0 {
			count++
		}
	}
	return count
}

func splitWords(s string) []string {
	words := []string{}
	word := []byte{}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			word = append(word, c)
		} else if len(word) > 0 {
			words = append(words, string(word))
			word = nil
		}
	}
	if len(word) > 0 {
		words = append(words, string(word))
	}
	return words
}


// RerankerFactory creates reranker instances based on configuration.
type RerankerFactory struct{}

// CreateReranker builds a reranker from a configuration map.
func (f *RerankerFactory) CreateReranker(config map[string]interface{}) (Reranker, error) {
	rerankerType, _ := config["type"].(string)
	modelPath, _ := config["model_path"].(string)

	switch rerankerType {
	case "cohere":
		apiKey, _ := config["api_key"].(string)
		model, _ := config["model"].(string)
		if apiKey == "" {
			return nil, errors.New("api_key is required for cohere reranker")
		}
		cr := NewCohereReranker(apiKey, model)
		return &ONNXReranker{model: cr, modelPath: "cohere-external"}, nil
	case "onnx", "ml":
		logger := zerolog.Nop()
		return NewONNXReranker(modelPath, logger)
	case "heuristic", "":
		return &HeuristicReranker{ModelName: "default"}, nil
	default:
		return nil, errors.New("unknown reranker type: " + rerankerType)
	}
}

// NewDefaultRerankerFactory creates a new RerankerFactory instance.
func NewDefaultRerankerFactory() *RerankerFactory {
	return &RerankerFactory{}
}

// AutoSelectReranker returns a default reranker suitable for general use.
func AutoSelectReranker() Reranker {
	return &HeuristicReranker{ModelName: "auto"}
}
