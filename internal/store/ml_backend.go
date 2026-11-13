package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/23skdu/longbow/internal/ml"
	"github.com/23skdu/longbow/internal/onnx"
	"github.com/23skdu/longbow/internal/wasm"
)

// MLBackend is the unified interface for local ML inference used across the
// vector engine (embeddings, reranking, and sparse generation).
type MLBackend interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Score(ctx context.Context, query string, docs []string) ([]float32, error)
	Dimension() int
	Close() error
}

// wasmBackend implements MLBackend on top of the wazero runtime.
type wasmBackend struct {
	path      string
	dimension int
	mu        sync.Mutex
	runner    *wasm.Runner
	tokenizer *ml.Tokenizer
}

func newWasmBackend(path string, dimension int) *wasmBackend {
	return &wasmBackend{path: path, dimension: dimension}
}

func (b *wasmBackend) ensure() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.runner == nil {
		runner, err := wasm.NewRunner(context.Background(), b.path)
		if err != nil {
			return err
		}
		b.runner = runner
	}
	if b.tokenizer == nil {
		tok, err := ml.NewTokenizerForModel(b.path, 512)
		if err != nil {
			return fmt.Errorf("failed to load tokenizer: %w", err)
		}
		b.tokenizer = tok
	}
	return nil
}

func (b *wasmBackend) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if err := b.ensure(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	runner := b.runner
	tokenizer := b.tokenizer
	b.mu.Unlock()

	results := make([][]float32, len(texts))
	for i, text := range texts {
		ids, mask := tokenizer.Encode(text)
		out, err := runner.InferenceWithTokens(ctx, ids, mask)
		if err != nil {
			return nil, fmt.Errorf("WASM inference failed for text %d: %w", i, err)
		}
		results[i] = out
	}
	return results, nil
}

func (b *wasmBackend) Score(ctx context.Context, query string, docs []string) ([]float32, error) {
	if err := b.ensure(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	runner := b.runner
	tokenizer := b.tokenizer
	b.mu.Unlock()

	scores := make([]float32, len(docs))
	for i, doc := range docs {
		ids, mask := tokenizer.Encode(query + " " + doc)
		out, err := runner.InferenceWithTokens(ctx, ids, mask)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 {
			scores[i] = out[0]
		}
	}
	return scores, nil
}

func (b *wasmBackend) Dimension() int { return b.dimension }

func (b *wasmBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.runner != nil {
		err := b.runner.Close(context.Background())
		b.runner = nil
		return err
	}
	return nil
}

// onnxBackend implements MLBackend on top of the ONNX runtime.
type onnxBackend struct {
	path      string
	dimension int
	mu        sync.Mutex
	session   *onnx.Session
}

func newOnnxBackend(path string, dimension int) *onnxBackend {
	return &onnxBackend{path: path, dimension: dimension}
}

func (b *onnxBackend) ensure() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.session == nil {
		session, err := onnx.NewSession(b.path)
		if err != nil {
			return err
		}
		b.session = session
	}
	return nil
}

func (b *onnxBackend) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if err := b.ensure(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	session := b.session
	b.mu.Unlock()
	return session.Embed(ctx, texts)
}

func (b *onnxBackend) Score(ctx context.Context, query string, docs []string) ([]float32, error) {
	if err := b.ensure(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	session := b.session
	b.mu.Unlock()
	return session.Score(ctx, query, docs)
}

func (b *onnxBackend) Dimension() int { return b.dimension }

func (b *onnxBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.session != nil {
		err := b.session.Close()
		b.session = nil
		return err
	}
	return nil
}

// newMLBackend selects a backend based on file extension.
func newMLBackend(path string, dimension int) (MLBackend, error) {
	if len(path) < 5 {
		return nil, fmt.Errorf("unknown model extension for %s", path)
	}
	switch path[len(path)-5:] {
	case ".wasm":
		return newWasmBackend(path, dimension), nil
	case ".onnx":
		return newOnnxBackend(path, dimension), nil
	default:
		return nil, fmt.Errorf("unknown model extension for %s (use .onnx or .wasm)", path)
	}
}
