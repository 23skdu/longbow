package metal

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsAvailable(t *testing.T) {
	// Just check that function doesn't panic
	_ = IsAvailable()
}

func TestEngineBehavior(t *testing.T) {
	engine, err := NewEngine()
	if !IsAvailable() {
		require.NoError(t, err)
		assert.Error(t, engine.LoadModel("/fake/path"))
		return
	}

	require.NoError(t, err)
	require.NotNil(t, engine)

	// Create a temp file to satisfy existence check
	tmpFile, err := os.CreateTemp("", "model.onnx")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	// In real Metal, LoadModel currently returns true if file exists
	err = engine.LoadModel(tmpPath)
	assert.NoError(t, err)

	scores, err := engine.Score(context.Background(), "query", []string{"doc"})
	assert.NoError(t, err)
	assert.NotEmpty(t, scores)

	batchScores, err := engine.ScoreBatch(context.Background(), []string{"q1"}, []string{"d1"})
	assert.NoError(t, err)
	assert.NotEmpty(t, batchScores)

	info, err := engine.ModelInfo()
	assert.NoError(t, err)
	assert.NotNil(t, info)

	err = engine.Close()
	assert.NoError(t, err)
}

func TestMetalRerankerBehavior(t *testing.T) {
	reranker, err := NewMetalReranker("/fake/path")
	if !IsAvailable() {
		assert.Error(t, err)
		assert.Nil(t, reranker)
		return
	}
	assert.NoError(t, err)
	assert.NotNil(t, reranker)
}

func TestEngineClose(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	require.NotNil(t, engine)

	// Multiple close calls should be safe
	err = engine.Close()
	assert.NoError(t, err)

	err = engine.Close()
	assert.NoError(t, err)
}
