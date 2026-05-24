package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/23skdu/longbow/internal/ml"
	"github.com/23skdu/longbow/internal/onnx"
	"github.com/23skdu/longbow/internal/wasm"
)

// EmbeddingGenerator defines the interface for generating vector embeddings from text.
type EmbeddingGenerator interface {
	Generate(ctx context.Context, texts []string) ([][]float32, error)
	GenerateSingle(ctx context.Context, text string) ([]float32, error)
	Dimension() int
	Close() error
}

// EmbeddingConfig holds configuration for an embedding generator.
type EmbeddingConfig struct {
	ModelPath    string
	ModelType    string
	Dimension    int
	BatchSize    int
	Device       string
	APIKey       string
	Provider     string
	ModelName    string
	ModelVersion string
	MaxRetries   int
	CacheEnabled bool
	CacheTTL     time.Duration
}

// ModelVersion tracks metadata for a specific version of an embedding model.
type ModelVersion struct {
	Version   string    `json:"version"`
	ModelName string    `json:"model_name"`
	Provider  string    `json:"provider"`
	CreatedAt time.Time `json:"created_at"`
	IsDefault bool      `json:"is_default"`
	Dimension int       `json:"dimension"`
	Checksum  string    `json:"checksum,omitempty"`
}

// EmbeddingModelRegistry manages available embedding models and their generators.
type EmbeddingModelRegistry struct {
	mu         sync.RWMutex
	models     map[string]map[string]ModelVersion
	generators map[string]EmbeddingGenerator
	cache      *EmbeddingCache
}

// EmbeddingCache provides a simple LRU-like cache for embeddings.
type EmbeddingCache struct {
	mu         sync.RWMutex
	entries    map[string][]float32
	maxEntries int
	ttl        time.Duration
	hits       int64
	misses     int64
}

// NewEmbeddingModelRegistry creates a new model registry.
func NewEmbeddingModelRegistry(cacheTTL time.Duration, maxCacheEntries int) *EmbeddingModelRegistry {
	return &EmbeddingModelRegistry{
		models:     make(map[string]map[string]ModelVersion),
		generators: make(map[string]EmbeddingGenerator),
		cache:      NewEmbeddingCache(cacheTTL, maxCacheEntries),
	}
}

// NewEmbeddingCache creates a new EmbeddingCache with the given TTL and capacity.
func NewEmbeddingCache(ttl time.Duration, maxEntries int) *EmbeddingCache {
	return &EmbeddingCache{
		entries:    make(map[string][]float32),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
}

// Get retrieves an embedding from the cache if it exists and is not expired.
func (c *EmbeddingCache) Get(key string) ([]float32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	if ok {
		c.hits++
	}
	return entry, ok
}

// Set adds or updates an embedding in the cache.
func (c *EmbeddingCache) Set(key string, value []float32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxEntries {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[key] = value
}

// Stats returns the cache hit/miss statistics and current size.
func (c *EmbeddingCache) Stats() (hits, misses int64, size int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses, len(c.entries)
}

// RegisterModel adds a model version to the registry.
func (r *EmbeddingModelRegistry) RegisterModel(provider, modelName string, version ModelVersion) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.models[provider] == nil {
		r.models[provider] = make(map[string]ModelVersion)
	}
	r.models[provider][modelName] = version
	return nil
}

// GetModel retrieves a model version from the registry.
func (r *EmbeddingModelRegistry) GetModel(provider, modelName string) (ModelVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.models[provider][modelName]
	return v, ok
}

// ListModels returns all model versions for a given provider.
func (r *EmbeddingModelRegistry) ListModels(provider string) []ModelVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := make([]ModelVersion, 0)
	for _, v := range r.models[provider] {
		versions = append(versions, v)
	}
	return versions
}

// SetGenerator registers an active generator for a model key.
func (r *EmbeddingModelRegistry) SetGenerator(key string, gen EmbeddingGenerator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.generators[key] = gen
}

// GetGenerator retrieves an active generator for a model key.
func (r *EmbeddingModelRegistry) GetGenerator(key string) (EmbeddingGenerator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.generators[key]
	return g, ok
}

// GetCache returns the shared embedding cache.
func (r *EmbeddingModelRegistry) GetCache() *EmbeddingCache {
	return r.cache
}

// ModelHealthStatus tracks the availability and performance of an embedding model.
type ModelHealthStatus struct {
	ModelName   string    `json:"model_name"`
	Provider    string    `json:"provider"`
	Status      string    `json:"status"` // "healthy", "degraded", "unhealthy"
	LastChecked time.Time `json:"last_checked"`
	LatencyMs   int64     `json:"latency_ms"`
	ErrorCount  int       `json:"error_count"`
}

// ListAllModels returns all registered model versions grouped by provider.
func (r *EmbeddingModelRegistry) ListAllModels() map[string][]ModelVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string][]ModelVersion)
	for provider, models := range r.models {
		versions := make([]ModelVersion, 0, len(models))
		for _, v := range models {
			versions = append(versions, v)
		}
		result[provider] = versions
	}
	return result
}

// UpdateModelVersion updates an existing model version's metadata.
func (r *EmbeddingModelRegistry) UpdateModelVersion(provider, modelName string, version ModelVersion) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.models[provider] == nil {
		return fmt.Errorf("provider %s not found", provider)
	}
	if _, ok := r.models[provider][modelName]; !ok {
		return fmt.Errorf("model %s not found in provider %s", modelName, provider)
	}
	r.models[provider][modelName] = version
	return nil
}

// SetDefaultModel marks a specific model as the default for its provider.
func (r *EmbeddingModelRegistry) SetDefaultModel(provider, modelName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.models[provider] == nil {
		return fmt.Errorf("provider %s not found", provider)
	}
	for name := range r.models[provider] {
		original := r.models[provider][name]
		isDefault := (name == modelName)
		r.models[provider][name] = ModelVersion{
			Version:   original.Version,
			ModelName: original.ModelName,
			Provider:  original.Provider,
			CreatedAt: original.CreatedAt,
			IsDefault: isDefault,
			Dimension: original.Dimension,
			Checksum:  original.Checksum,
		}
	}
	return nil
}

// GetDefaultModel returns the default model version for a provider.
func (r *EmbeddingModelRegistry) GetDefaultModel(provider string) (ModelVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.models[provider] == nil {
		return ModelVersion{}, false
	}
	for _, v := range r.models[provider] {
		if v.IsDefault {
			return v, true
		}
	}
	return ModelVersion{}, false
}

// NewEmbeddingGenerator creates an embedding generator based on the provided configuration.
func NewEmbeddingGenerator(config EmbeddingConfig) (EmbeddingGenerator, error) {
	switch config.Provider {
	case "openai":
		return NewOpenAIEmbedding(config)
	case "cohere":
		return NewCohereEmbedding(config)
	case "huggingface":
		return NewHuggingFaceEmbedding(config)
	case "local", "":
		if config.ModelPath != "" {
			return NewLocalEmbeddingGenerator(config)
		}
		return nil, errors.New("no embedding provider or model path specified")
	default:
		return nil, errors.New("unknown embedding provider: " + config.Provider)
	}
}

type openAIEmbeddingGenerator struct {
	apiKey     string
	model      string
	dimension  int
	httpClient *http.Client
}

// NewOpenAIEmbedding creates an OpenAI embedding generator.
func NewOpenAIEmbedding(config EmbeddingConfig) (EmbeddingGenerator, error) {
	if config.APIKey == "" {
		return nil, errors.New("API key is required for OpenAI embedding")
	}

	model := config.ModelName
	if model == "" {
		model = "text-embedding-3-small"
	}

	dim := config.Dimension
	if dim <= 0 {
		dim = 1536
	}

	return &openAIEmbeddingGenerator{
		apiKey:     config.APIKey,
		model:      model,
		dimension:  dim,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

type openAIEmbeddingRequest struct {
	Input string `json:"input"`
	Model string `json:"model"`
}

type openAIEmbeddingResponse struct {
	Data  []openAIEmbeddingData `json:"data"`
	Usage openAIUsage           `json:"usage"`
	Error *openAIError          `json:"error,omitempty"`
}

type openAIEmbeddingData struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type openAIUsage struct {
	PromptTokens int `json:"prompt_tokens"`
}

type openAIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

func (g *openAIEmbeddingGenerator) Generate(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	results := make([][]float32, 0, len(texts))

	for _, text := range texts {
		req := openAIEmbeddingRequest{
			Input: text,
			Model: g.model,
		}

		body, err := json.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		httpReq, err := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/embeddings", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := g.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
		defer resp.Body.Close()

		var embResp openAIEmbeddingResponse
		if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		if embResp.Error != nil {
			return nil, fmt.Errorf("OpenAI API error: %s", embResp.Error.Message)
		}

		if len(embResp.Data) == 0 {
			return nil, errors.New("no embedding returned")
		}

		results = append(results, embResp.Data[0].Embedding)
	}

	return results, nil
}

func (g *openAIEmbeddingGenerator) GenerateSingle(ctx context.Context, text string) ([]float32, error) {
	results, err := g.Generate(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no embeddings generated")
	}
	return results[0], nil
}

func (g *openAIEmbeddingGenerator) Dimension() int {
	return g.dimension
}

func (g *openAIEmbeddingGenerator) Close() error {
	return nil
}

type cohereEmbeddingGenerator struct {
	apiKey     string
	model      string
	dimension  int
	httpClient *http.Client
}

// NewCohereEmbedding creates a Cohere embedding generator.
func NewCohereEmbedding(config EmbeddingConfig) (EmbeddingGenerator, error) {
	if config.APIKey == "" {
		return nil, errors.New("API key is required for Cohere embedding")
	}

	model := config.ModelName
	if model == "" {
		model = "embed-english-v3.0"
	}

	dim := config.Dimension
	if dim <= 0 {
		dim = 1024
	}

	return &cohereEmbeddingGenerator{
		apiKey:     config.APIKey,
		model:      model,
		dimension:  dim,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

type cohereEmbeddingRequest struct {
	Texts []string `json:"texts"`
	Model string   `json:"model"`
}

type cohereEmbeddingResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
	ID         string      `json:"id"`
	Meta       cohereMeta  `json:"meta"`
}

type cohereMeta struct {
	APIVersion  cohereAPIVersion `json:"api_version"`
	BilledUnits interface{}      `json:"billed_units"`
}

type cohereAPIVersion struct {
	Version string `json:"version"`
}

func (g *cohereEmbeddingGenerator) Generate(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	req := cohereEmbeddingRequest{
		Texts: texts,
		Model: g.model,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", "https://api.cohere.ai/v1/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Cohere-Version", "2024-01-01")

	resp, err := g.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	var embResp cohereEmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return embResp.Embeddings, nil
}

func (g *cohereEmbeddingGenerator) GenerateSingle(ctx context.Context, text string) ([]float32, error) {
	results, err := g.Generate(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no embeddings generated")
	}
	return results[0], nil
}

func (g *cohereEmbeddingGenerator) Dimension() int {
	return g.dimension
}

func (g *cohereEmbeddingGenerator) Close() error {
	return nil
}

type huggingFaceEmbeddingGenerator struct {
	apiKey     string
	model      string
	dimension  int
	httpClient *http.Client
}

// NewHuggingFaceEmbedding creates a HuggingFace embedding generator.
func NewHuggingFaceEmbedding(config EmbeddingConfig) (EmbeddingGenerator, error) {
	if config.APIKey == "" {
		return nil, errors.New("API key is required for HuggingFace embedding")
	}

	model := config.ModelName
	if model == "" {
		model = "sentence-transformers/all-MiniLM-L6-v2"
	}

	dim := config.Dimension
	if dim <= 0 {
		dim = 384
	}

	return &huggingFaceEmbeddingGenerator{
		apiKey:     config.APIKey,
		model:      model,
		dimension:  dim,
		httpClient: &http.Client{Timeout: 120 * time.Second},
	}, nil
}

type hfEmbeddingRequest struct {
	Inputs []string `json:"inputs"`
}

type hfEmbeddingResponse [][]float32

func (g *huggingFaceEmbeddingGenerator) Generate(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	req := hfEmbeddingRequest{
		Inputs: texts,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	modelURL := strings.Replace(g.model, " ", "%20", -1)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", "https://api-inference.huggingface.co/pipeline/feature-extraction/"+modelURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+g.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	var embResp hfEmbeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return embResp, nil
}

func (g *huggingFaceEmbeddingGenerator) GenerateSingle(ctx context.Context, text string) ([]float32, error) {
	results, err := g.Generate(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no embeddings generated")
	}
	return results[0], nil
}

func (g *huggingFaceEmbeddingGenerator) Dimension() int {
	return g.dimension
}

func (g *huggingFaceEmbeddingGenerator) Close() error {
	return nil
}

type localEmbeddingGenerator struct {
	modelPath   string
	dimension   int
	batchSize   int
	model       EmbeddingModel
	logger      EmbeddingLogger
	initialized bool
}

// EmbeddingModel defines the interface for local model inference.
type EmbeddingModel interface {
	Inference(input []string) ([][]float32, error)
	Close() error
}

// EmbeddingLogger defines the logging interface used by embedding generators.
type EmbeddingLogger interface {
	Debug(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})
}

var _ EmbeddingLogger = noopLogger{}

type noopLogger struct{}

func (l noopLogger) Debug(msg string, keysAndValues ...interface{}) {}
func (l noopLogger) Info(msg string, keysAndValues ...interface{})  {}
func (l noopLogger) Error(msg string, keysAndValues ...interface{}) {}

// NewLocalEmbeddingGenerator creates a local embedding generator using ONNX or WASM.
func NewLocalEmbeddingGenerator(config EmbeddingConfig) (EmbeddingGenerator, error) {
	dim := config.Dimension
	if dim <= 0 {
		dim = 384
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 32
	}

	le := &localEmbeddingGenerator{
		modelPath:   config.ModelPath,
		dimension:   dim,
		batchSize:   batchSize,
		logger:      noopLogger{},
		initialized: false,
	}

	if err := le.initModel(); err != nil {
		return nil, err
	}

	return le, nil
}

func (le *localEmbeddingGenerator) initModel() error {
	if le.modelPath == "" {
		return errors.New("strict model validation failed: no model path specified")
	}

	ext := ""
	if len(le.modelPath) > 5 {
		ext = le.modelPath[len(le.modelPath)-5:]
	}

	switch ext {
	case ".wasm":
		le.model = &wasmEmbeddingModel{path: le.modelPath}
		le.initialized = true
		le.logger.Info("WASM embedding model loaded", "path", le.modelPath)
	case ".onnx":
		le.model = &onnxEmbeddingModel{path: le.modelPath}
		le.initialized = true
		le.logger.Info("ONNX embedding model loaded", "path", le.modelPath)
	default:
		return fmt.Errorf("strict model validation failed: unknown model extension for %s (use .onnx or .wasm)", le.modelPath)
	}

	return nil
}

func (le *localEmbeddingGenerator) Generate(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	if !le.initialized {
		if err := le.initModel(); err != nil {
			return nil, err
		}
	}

	var results [][]float32
	for i := 0; i < len(texts); i += le.batchSize {
		end := i + le.batchSize
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[i:end]

		batchResults, err := le.model.Inference(batch)
		if err != nil {
			return nil, err
		}

		results = append(results, batchResults...)
	}

	return results, nil
}

func (le *localEmbeddingGenerator) GenerateSingle(ctx context.Context, text string) ([]float32, error) {
	results, err := le.Generate(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no embeddings generated")
	}
	return results[0], nil
}

func (le *localEmbeddingGenerator) Dimension() int {
	return le.dimension
}

func (le *localEmbeddingGenerator) Close() error {
	if le.model != nil {
		return le.model.Close()
	}
	return nil
}

type onnxEmbeddingModel struct {
	path    string
	session *onnx.Session
}

func (m *onnxEmbeddingModel) Inference(input []string) ([][]float32, error) {
	if m.session == nil {
		session, err := onnx.NewSession(m.path)
		if err != nil {
			return nil, err
		}
		m.session = session
	}

	start := time.Now()
	defer func() {
		metrics.EmbeddingGenerationDurationSeconds.WithLabelValues("local", "onnx").Observe(time.Since(start).Seconds())
	}()

	return m.session.Embed(context.Background(), input)
}

func (m *onnxEmbeddingModel) Close() error {
	if m.session != nil {
		return m.session.Close()
	}
	return nil
}

type wasmEmbeddingModel struct {
	path      string
	runner    *wasm.Runner
	tokenizer *ml.Tokenizer
}

func (m *wasmEmbeddingModel) Inference(input []string) ([][]float32, error) {
	if m.runner == nil {
		runner, err := wasm.NewRunner(context.Background(), m.path)
		if err != nil {
			return nil, err
		}
		m.runner = runner
	}

	if m.tokenizer == nil {
		tok, err := ml.NewTokenizer("vocab.txt", 512)
		if err != nil {
			return nil, fmt.Errorf("failed to load tokenizer: %w", err)
		}
		m.tokenizer = tok
	}

	start := time.Now()
	defer func() {
		metrics.EmbeddingGenerationDurationSeconds.WithLabelValues("local", "wasm").Observe(time.Since(start).Seconds())
	}()

	results := make([][]float32, len(input))
	for i, text := range input {
		ids, mask := m.tokenizer.Encode(text)
		output, err := m.runner.InferenceWithTokens(context.Background(), ids, mask)
		if err != nil {
			return nil, fmt.Errorf("WASM inference failed for text %d: %w", i, err)
		}
		results[i] = output
	}

	return results, nil
}

func (m *wasmEmbeddingModel) Close() error {
	if m.runner != nil {
		return m.runner.Close(context.Background())
	}
	return nil
}
