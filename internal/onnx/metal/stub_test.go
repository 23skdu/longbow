//go:build !gpu || !darwin || !arm64 || !cgo

package metal

import (
	"context"
	"sync"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStubIsUnavailable(t *testing.T) {
	assert.False(t, IsAvailable())
}

func TestStubNewEngine(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	require.NotNil(t, engine)
}

func TestStubLoadModel(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	err = engine.LoadModel("/fake/path")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Metal not available")
}

func TestStubScore(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	scores, err := engine.Score(context.Background(), "q", []string{"d"})
	assert.Error(t, err)
	assert.Nil(t, scores)
	assert.Contains(t, err.Error(), "Metal not available")
}

func TestStubScoreBatch(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	batch, err := engine.ScoreBatch(context.Background(), []string{"q"}, []string{"d"})
	assert.Error(t, err)
	assert.Nil(t, batch)
	assert.Contains(t, err.Error(), "Metal not available")
}

func TestStubEmbed(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	embeddings, err := engine.Embed(context.Background(), []string{"a", "b"})
	assert.Error(t, err)
	assert.Nil(t, embeddings)
	assert.Contains(t, err.Error(), "Metal not available")
}

func TestStubWarmupIsNoop(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	assert.NoError(t, engine.Warmup())
}

func TestStubModelInfo(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	info, err := engine.ModelInfo()
	assert.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "Metal not available")
}

func TestStubCloseIsIdempotent(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	assert.NoError(t, engine.Close())
	assert.NoError(t, engine.Close())
}

func TestStubEngineConcurrentAccess(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = engine.LoadModel("/fake/path")
			_, _ = engine.Score(context.Background(), "q", []string{"d"})
			_, _ = engine.Embed(context.Background(), []string{"t"})
			_, _ = engine.ModelInfo()
			_ = engine.Warmup()
			_ = engine.Close()
		}()
	}
	wg.Wait()
}

func TestStubNewMetalReranker(t *testing.T) {
	reranker, err := NewMetalReranker("/fake/path")
	assert.Error(t, err)
	assert.Nil(t, reranker)
	assert.Contains(t, err.Error(), "Metal is not available")
}

func TestStubMetalRerankerRerank(t *testing.T) {
	reranker := &MetalReranker{modelPath: "/fake/path"}

	results := []types.SearchResult{{ID: 1, Distance: 0.5, Score: 0.9}}
	out, err := reranker.Rerank(context.Background(), "q", results)
	assert.Error(t, err)
	assert.Nil(t, out)
	assert.Contains(t, err.Error(), "Metal is not available")
}

func TestStubMetalRerankerClose(t *testing.T) {
	reranker := &MetalReranker{modelPath: "/fake/path"}
	assert.NoError(t, reranker.Close())
}
