package store

import (
	"context"
	"errors"
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
	MaxRetries   int
	CacheEnabled bool
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

func NewOpenAIEmbedding(config EmbeddingConfig) (EmbeddingGenerator, error) {
	return nil, errors.New("OpenAI embedding not yet implemented - requires API client")
}

func NewCohereEmbedding(config EmbeddingConfig) (EmbeddingGenerator, error) {
	return nil, errors.New("Cohere embedding not yet implemented - requires API client")
}

func NewHuggingFaceEmbedding(config EmbeddingConfig) (EmbeddingGenerator, error) {
	return nil, errors.New("HuggingFace embedding not yet implemented - requires API client")
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
