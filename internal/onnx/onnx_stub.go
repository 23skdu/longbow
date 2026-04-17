//go:build !onnx
// +build !onnx

package onnx

import (
	"context"
	"errors"
)

// Init is a no-op on non-ONNX platforms
func Init() error {
	return nil
}

// Session is a stub for non-ONNX platforms
type Session struct {
	isMetal bool
}

// NewSession returns an error on non-ONNX platforms
func NewSession(modelPath string) (*Session, error) {
	return nil, errors.New("ONNX Runtime not available in this build")
}

// Score returns an error on non-ONNX platforms
func (s *Session) Score(ctx context.Context, query string, docs []string) ([]float32, error) {
	return nil, errors.New("ONNX Runtime not available in this build")
}

// Close is a no-op on non-ONNX platforms
func (s *Session) Close() error {
	return nil
}
