//go:build !onnx
// +build !onnx

package onnx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStubInit runs the test stub init operation.
func TestStubInit(t *testing.T) {
	assert.NoError(t, Init())
}

// TestStubNewSession runs the test stub new session operation.
func TestStubNewSession(t *testing.T) {
	s, err := NewSession("fake.onnx")
	assert.Error(t, err)
	assert.Nil(t, s)
	assert.Contains(t, err.Error(), "ONNX Runtime not available")
}

// TestStubScore runs the test stub score operation.
func TestStubScore(t *testing.T) {
	s := &Session{}
	res, err := s.Score(context.Background(), "q", []string{"d"})
	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "ONNX Runtime not available")
}

// TestStubClose runs the test stub close operation.
func TestStubClose(t *testing.T) {
	s := &Session{}
	assert.NoError(t, s.Close())
}
