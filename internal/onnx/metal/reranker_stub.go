//go:build !gpu || !darwin || !arm64
// +build !gpu !darwin !arm64

package metal

import (
	"context"
	"errors"

	"github.com/23skdu/longbow/internal/store"
)

// MetalReranker is a stub for non-Metal platforms
type MetalReranker struct {
	modelPath string
}

// NewMetalReranker returns an error on non-Metal platforms
func NewMetalReranker(modelPath string) (*MetalReranker, error) {
	return nil, errors.New("Metal is not available on this platform")
}

// Rerank returns an error on non-Metal platforms
func (r *MetalReranker) Rerank(ctx context.Context, query string, results []store.SearchResult) ([]store.SearchResult, error) {
	return nil, errors.New("Metal is not available on this platform")
}

// Close is a no-op on non-Metal platforms
func (r *MetalReranker) Close() error {
	return nil
}
