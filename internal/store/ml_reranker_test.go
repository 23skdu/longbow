package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMLModel struct {
	scores []float32
	err    error
}

func (m *mockMLModel) Score(query string, documents []string) ([]float32, error) {
	return m.scores, m.err
}

func (m *mockMLModel) Close() error { return nil }

func TestONNXReranker(t *testing.T) {
	mock := &mockMLModel{
		scores: []float32{0.9, 0.1, 0.5},
	}
	
	r := &ONNXReranker{
		model: mock,
	}
	
	results := []SearchResult{
		{ID: 1, Distance: 0.1},
		{ID: 2, Distance: 0.5},
		{ID: 3, Distance: 0.2},
	}
	
	// mock returns 3 scores for 3 docs
	// documents will be retrieved from dataset but since we mock Score, it doesn't matter for the internal call here
	// Wait, ONNXReranker retrieves content from results.Metadata if available?
	
	results[0].Metadata = map[string]interface{}{"content": "doc1"}
	results[1].Metadata = map[string]interface{}{"content": "doc2"}
	results[2].Metadata = map[string]interface{}{"content": "doc3"}

	reranked, err := r.Rerank(context.Background(), "query", results)
	require.NoError(t, err)
	assert.Equal(t, len(results), len(reranked))
	
	// Score for doc1 (0.9) should be highest
	assert.Equal(t, uint32(1), uint32(reranked[0].ID))
	assert.Greater(t, reranked[0].Score, reranked[1].Score)
}
