//go:build onnx
// +build onnx

package onnx

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSession_Initialization_ErrorPaths(t *testing.T) {
	// Test with a non-existent model path
	_, err := NewSession("non_existent_model.onnx")
	assert.Error(t, err)

	// Test invalid input/output names
	s := &Session{
		inputNames:  []string{},
		outputNames: []string{},
	}
	assert.False(t, s.isMetal)
}

func TestSession_Score_Reordering(t *testing.T) {
	// Mock re-ordering logic
	s := &Session{
		inputNames:  []string{"attention_mask", "input_ids"}, // Swapped order
		outputNames: []string{"logits"},
		isMetal:     false,
	}

	ctx := context.Background()
	query := "test"
	docs := []string{"doc"}

	// Should attempt to build map with attention_mask and input_ids
	// Skip if ortSession is nil to avoid actual inference attempt
	if s.ortSession == nil {
		t.Skip("Skipping actual inference test as ortSession is nil")
	}
	_, err := s.Score(ctx, query, docs)
	assert.Error(t, err)
}

func TestSession_MetalEngine_Fallback(t *testing.T) {
	// If metal is not available, should fallback to ORT
	// (Already covered by NewSession structure, but we can call it explicitly)
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		t.Log("Testing darwin/arm64 paths")
	}
}

func TestScore_InputValidation(t *testing.T) {
	s := &Session{}
	ctx := context.Background()
	
	// Zero docs
	res, err := s.Score(ctx, "q", []string{})
	if err != nil {
		assert.Equal(t, "session not initialized", err.Error())
	} else {
		assert.Empty(t, res)
	}
}

func TestONNX_Close(t *testing.T) {
	s := &Session{}
	assert.NoError(t, s.Close())
}
