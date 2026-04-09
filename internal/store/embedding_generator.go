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
)

type EmbeddingGenerator interface {
	Generate(ctx context.Context, texts []string) ([][]float32, error)
	GenerateSingle(ctx context.Context, text string) ([]float32, error)
	Dimension() int
	Close() error
}

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

type ModelVersion struct {
	Version   string    `json:"version"`
	ModelName string    `json:"model_name"`
	Provider  string    `json:"provider"`
	CreatedAt time.Time `json:"created_at"`
	IsDefault bool      `json:"is_default"`
	Dimension int       `json:"dimension"`
	Checksum  string    `json:"checksum,omitempty"`
}

type EmbeddingModelRegistry struct {
	mu         sync.RWMutex
	models     map[string]map[string]ModelVersion
	generators map[string]EmbeddingGenerator
	cache      *EmbeddingCache
}

type EmbeddingCache struct {
	mu         sync.RWMutex
	entries    map[string][]float32
	maxEntries int
	ttl        time.Duration
	hits       int64
	misses     int64
}

func NewEmbeddingModelRegistry(cacheTTL time.Duration, maxCacheEntries int) *EmbeddingModelRegistry {
	return &EmbeddingModelRegistry{
		models:     make(map[string]map[string]ModelVersion),
		generators: make(map[string]EmbeddingGenerator),
		cache:      NewEmbeddingCache(cacheTTL, maxCacheEntries),
	}
}

func NewEmbeddingCache(ttl time.Duration, maxEntries int) *EmbeddingCache {
	return &EmbeddingCache{
		entries:    make(map[string][]float32),
		maxEntries: maxEntries,
		ttl:        ttl,
	}
}

func (c *EmbeddingCache) Get(key string) ([]float32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[key]
	if ok {
		c.hits++
	}
	return entry, ok
}

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

func (c *EmbeddingCache) Stats() (hits, misses int64, size int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses, len(c.entries)
}

func (r *EmbeddingModelRegistry) RegisterModel(provider, modelName string, version ModelVersion) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.models[provider] == nil {
		r.models[provider] = make(map[string]ModelVersion)
	}
	r.models[provider][modelName] = version
	return nil
}

func (r *EmbeddingModelRegistry) GetModel(provider, modelName string) (ModelVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.models[provider][modelName]
	return v, ok
}

func (r *EmbeddingModelRegistry) ListModels(provider string) []ModelVersion {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions := make([]ModelVersion, 0)
	for _, v := range r.models[provider] {
		versions = append(versions, v)
	}
	return versions
}

func (r *EmbeddingModelRegistry) SetGenerator(key string, gen EmbeddingGenerator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.generators[key] = gen
}

func (r *EmbeddingModelRegistry) GetGenerator(key string) (EmbeddingGenerator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.generators[key]
	return g, ok
}

func (r *EmbeddingModelRegistry) GetCache() *EmbeddingCache {
	return r.cache
}

type ModelHealthStatus struct {
	ModelName   string    `json:"model_name"`
	Provider    string    `json:"provider"`
	Status      string    `json:"status"` // "healthy", "degraded", "unhealthy"
	LastChecked time.Time `json:"last_checked"`
	LatencyMs   int64     `json:"latency_ms"`
	ErrorCount  int       `json:"error_count"`
}

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

func NewOpenAIEmbedding(config EmbeddingConfig) (*openAIEmbeddingGenerator, error) {
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

func NewCohereEmbedding(config EmbeddingConfig) (*cohereEmbeddingGenerator, error) {
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

func NewHuggingFaceEmbedding(config EmbeddingConfig) (*huggingFaceEmbeddingGenerator, error) {
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

type EmbeddingModel interface {
	Inference(input []string) ([][]float32, error)
	Close() error
}

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

func NewLocalEmbeddingGenerator(config EmbeddingConfig) (*localEmbeddingGenerator, error) {
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
		le.model = &stubEmbeddingModel{dimension: le.dimension}
		le.initialized = true
		return nil
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
		le.model = &stubEmbeddingModel{dimension: le.dimension}
		le.initialized = true
		le.logger.Info("Using stub embedding model", "path", le.modelPath)
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

type stubEmbeddingModel struct {
	dimension int
}

func (m *stubEmbeddingModel) Inference(input []string) ([][]float32, error) {
	results := make([][]float32, len(input))
	for i := range input {
		results[i] = make([]float32, m.dimension)
		hash := hashString(input[i])
		for j := 0; j < m.dimension; j++ {
			results[i][j] = float32((hash >> uint(j%32)) & 0xFFFF)
			if results[i][j] > 1 {
				results[i][j] = results[i][j] / 65535
			}
		}
	}
	return results, nil
}

func (m *stubEmbeddingModel) Close() error {
	return nil
}

func hashString(s string) uint64 {
	h := uint64(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 16777619
	}
	return h
}

type onnxEmbeddingModel struct {
	path string
}

func (m *onnxEmbeddingModel) Inference(input []string) ([][]float32, error) {
	return nil, errors.New("ONNX embedding model not yet implemented - requires ONNX Runtime Go bindings")
}

func (m *onnxEmbeddingModel) Close() error {
	return nil
}

type wasmEmbeddingModel struct {
	path string
}

func (m *wasmEmbeddingModel) Inference(input []string) ([][]float32, error) {
	return nil, errors.New("WASM embedding model not yet implemented - requires wazero runtime")
}

func (m *wasmEmbeddingModel) Close() error {
	return nil
}
