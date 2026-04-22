package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type OllamaClient struct {
	baseURL    string
	model      string
	httpClient *http.Client
	logger     zerolog.Logger
	mu         sync.RWMutex
	enabled    bool
}

type OllamaEmbedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type OllamaEmbedResponse struct {
	Embeddings [][]float64 `json:"embeddings"`
}

type OllamaConfig struct {
	Endpoint string `json:"endpoint"`
	Model    string `json:"model"`
	Timeout  int    `json:"timeout"`
}

func NewOllamaClient(logger zerolog.Logger, config OllamaConfig) *OllamaClient {
	if config.Endpoint == "" {
		config.Endpoint = "http://localhost:11434"
	}
	if config.Timeout == 0 {
		config.Timeout = 30
	}

	return &OllamaClient{
		baseURL: config.Endpoint,
		model:   config.Model,
		httpClient: &http.Client{
			Timeout: time.Duration(config.Timeout) * time.Second,
		},
		logger:  logger,
		enabled: true,
	}
}

func (o *OllamaClient) IsEnabled() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.enabled
}

func (o *OllamaClient) Embed(ctx context.Context, text string) ([]float64, error) {
	o.mu.RLock()
	baseURL := o.baseURL
	model := o.model
	o.mu.RUnlock()

	if !o.IsEnabled() {
		return nil, fmt.Errorf("ollama client is disabled")
	}

	reqBody := map[string]interface{}{
		"model": model,
		"input": text,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", baseURL+"/api/embed", bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call ollama: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama returned status %d", resp.StatusCode)
	}

	var result OllamaEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(result.Embeddings) == 0 {
		return nil, fmt.Errorf("no embeddings returned")
	}

	return result.Embeddings[0], nil
}

func (o *OllamaClient) GetConfig() OllamaConfig {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return OllamaConfig{
		Endpoint: o.baseURL,
		Model:    o.model,
	}
}

func (o *OllamaClient) Disable() {
	o.mu.Lock()
	o.enabled = false
	o.mu.Unlock()
}

func (o *OllamaClient) Enable() {
	o.mu.Lock()
	o.enabled = true
	o.mu.Unlock()
}

type LearnedIndexWithOllama struct {
	predictor *IndexPerformancePredictor
	ollama    *OllamaClient
	logger    zerolog.Logger
}

func NewLearnedIndexWithOllama(logger zerolog.Logger, predictor *IndexPerformancePredictor, config OllamaConfig) *LearnedIndexWithOllama {
	var ollama *OllamaClient
	if config.Model != "" {
		ollama = NewOllamaClient(logger, config)
		logger.Info().
			Str("endpoint", config.Endpoint).
			Str("model", config.Model).
			Msg("Ollama client initialized for learned index")
	}

	return &LearnedIndexWithOllama{
		predictor: predictor,
		ollama:    ollama,
		logger:    logger,
	}
}

func (l *LearnedIndexWithOllama) Predict(ctx context.Context, features QueryFeatures) IndexPrediction {
	if l.ollama != nil && l.ollama.IsEnabled() {
		embedding, err := l.ollama.Embed(ctx, features.String())
		if err != nil {
			l.logger.Warn().Err(err).Msg("Ollama embed failed, falling back to rule-based")
		} else {
			features.UpdateFromEmbedding(embedding)
			prediction := l.predictor.Predict(features)
			if prediction.Confidence >= l.predictor.config.ConfidenceThreshold {
				return prediction
			}
		}
	}

	return l.predictor.Predict(features)
}

func (f QueryFeatures) String() string {
	return fmt.Sprintf(
		"dim=%d,queries=%d,k=%d,dataset=%d,collections=%d,complexity=%s,filtered=%v,hybrid=%v,provider=%s,model=%s",
		f.VectorDimension, f.NumQueryVectors, f.SearchK, f.DatasetSize, f.NumCollections,
		f.QueryComplexity, f.IsFiltered, f.IsHybrid,
		f.EmbeddingProvider, f.EmbeddingModel,
	)
}
