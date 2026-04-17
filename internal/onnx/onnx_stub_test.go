//go:build !onnx
// +build !onnx

package onnx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStubInit(t *testing.T) {
	assert.NoError(t, Init())
}

func TestStubNewSession(t *testing.T) {
	s, err := NewSession("fake.onnx")
	assert.Error(t, err)
	assert.Nil(t, s)
	assert.Contains(t, err.Error(), "ONNX Runtime not available")
}

func TestStubScore(t *testing.T) {
	s := &Session{}
	res, err := s.Score(context.Background(), "q", []string{"d"})
	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "ONNX Runtime not available")
}

func TestStubClose(t *testing.T) {
	s := &Session{}
	assert.NoError(t, s.Close())
}
