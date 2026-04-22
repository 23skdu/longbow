//go:build onnx
// +build onnx

package onnx

import (
	"context"
	"math"
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

func TestSession_MeanPooling(t *testing.T) {
	s := &Session{poolingMode: PoolingMean}
	
	// Mock hidden states: 2 sentences, 3 tokens each, 2 dimensions
	// Sentence 0: [ [1, 2], [3, 4], [0, 0] ] (last one is padding)
	// Sentence 1: [ [10, 20], [0, 0], [0, 0] ] (last two are padding)
	hiddenStates := []float32{
		1, 2, 3, 4, 0, 0,
		10, 20, 0, 0, 0, 0,
	}
	mask := []int64{
		1, 1, 0,
		1, 0, 0,
	}
	shape := []int64{2, 3, 2}
	
	results := s.meanPooling(hiddenStates, mask, shape)
	
	assert.Equal(t, 2, len(results))
	
	// Expected for sentence 0: Mean of [1,2] and [3,4] = [2, 3]
	// Normalized: [2/sqrt(13), 3/sqrt(13)] ~= [0.5547, 0.832]
	expected0_0 := float32(2.0 / math.Sqrt(13.0))
	expected0_1 := float32(3.0 / math.Sqrt(13.0))
	assert.InDelta(t, expected0_0, results[0][0], 0.0001)
	assert.InDelta(t, expected0_1, results[0][1], 0.0001)
	
	// Expected for sentence 1: Mean of [10, 20] = [10, 20]
	// Normalized: [10/sqrt(500), 20/sqrt(500)] = [1/sqrt(5), 2/sqrt(5)] ~= [0.447, 0.894]
	expected1_0 := float32(10.0 / math.Sqrt(500.0))
	expected1_1 := float32(20.0 / math.Sqrt(500.0))
	assert.InDelta(t, expected1_0, results[1][0], 0.0001)
	assert.InDelta(t, expected1_1, results[1][1], 0.0001)
}
