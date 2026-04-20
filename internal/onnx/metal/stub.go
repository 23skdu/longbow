//go:build !gpu || !darwin || !arm64
// +build !gpu !darwin !arm64

package metal

import (
	"context"
	"errors"
	"sync"
)

// Engine is a stub for non-Metal platforms
type Engine struct {
	mu sync.RWMutex
}

// ModelInfo contains model information
type ModelInfo struct {
	Name       string
	InputLen   int
	OutputLen  int
	Parameters int64
}

// IsAvailable returns false on non-Apple Silicon platforms
func IsAvailable() bool {
	return false
}

// NewEngine creates a stub engine
func NewEngine() (*Engine, error) {
	return &Engine{}, nil
}

// LoadModel returns an error on non-Metal platforms
func (e *Engine) LoadModel(path string) error {
	return errors.New("Metal not available on this platform")
}

// Score returns an error on non-Metal platforms
func (e *Engine) Score(ctx context.Context, query string, documents []string) ([]float32, error) {
	return nil, errors.New("Metal not available on this platform")
}

// ScoreBatch returns an error on non-Metal platforms
func (e *Engine) ScoreBatch(ctx context.Context, queries, documents []string) ([][]float32, error) {
	return nil, errors.New("Metal not available on this platform")
}

// Embed returns an error on non-Metal platforms
func (e *Engine) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	return nil, errors.New("Metal not available on this platform")
}

// Warmup is a no-op on non-Metal platforms
func (e *Engine) Warmup() error {
	return nil
}

// ModelInfo returns an error on non-Metal platforms
func (e *Engine) ModelInfo() (*ModelInfo, error) {
	return nil, errors.New("Metal not available on this platform")
}

// Close is a no-op on non-Metal platforms
func (e *Engine) Close() error {
	return nil
}
