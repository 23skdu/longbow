package metal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsAvailable(t *testing.T) {
	// Just check that function doesn't panic
	_ = IsAvailable()
}

func TestEngineStub(t *testing.T) {
	// Test stub behavior
	engine, err := NewEngine()
	require.NoError(t, err)
	require.NotNil(t, engine)

	// Check that methods return appropriate errors
	err = engine.LoadModel("/fake/path")
	assert.Error(t, err)

	scores, err := engine.Score(context.Background(), "query", []string{"doc"})
	assert.Error(t, err)
	assert.Nil(t, scores)

	batchScores, err := engine.ScoreBatch(context.Background(), []string{"q1"}, []string{"d1"})
	assert.Error(t, err)
	assert.Nil(t, batchScores)

	info, err := engine.ModelInfo()
	assert.Error(t, err)
	assert.Nil(t, info)

	err = engine.Close()
	assert.NoError(t, err)
}

func TestMetalRerankerStub(t *testing.T) {
	// Test that stub reranker returns error
	reranker, err := NewMetalReranker("/fake/path")
	assert.Error(t, err)
	assert.Nil(t, reranker)
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
