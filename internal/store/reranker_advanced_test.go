package store

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/23skdu/longbow/internal/core"
)

func makeCandidate(id core.VectorID, dist float32, title, content string) SearchResult {
	meta, _ := core.EncodeMetadata(map[string]any{
		"title":   title,
		"content": content,
	})
	return SearchResult{
		ID:       id,
		Distance: dist,
		Metadata: meta,
	}
}

func TestAdvancedRerankersCacheRewriterAndEval(t *testing.T) {
	ctx := context.Background()

	c1 := makeCandidate(1, 0.05, "Cooking Recipes", "Pasta and tomato sauce")
	c2 := makeCandidate(2, 0.25, "Vector Database Security", strings.Repeat("background ", 20)+"vector database encryption and rbac controls")
	c3 := makeCandidate(3, 0.15, "Auth Guide", "Authentication overview")
	candidates := []SearchResult{c1, c2, c3}

	// Cross-encoder reranker via ML callback
	ce := NewCallbackCrossEncoderReranker(func(ctx context.Context, query string, docs []string) ([]float32, error) {
		scores := make([]float32, len(docs))
		for i, d := range docs {
			if strings.Contains(strings.ToLower(d), "encryption") {
				scores[i] = 4.0
			} else {
				scores[i] = -2.0
			}
		}
		return scores, nil
	}, 0.8)

	hybrid := NewHybridReranker(ce)
	ranked, err := hybrid.Rerank(ctx, "vector database encryption", candidates)
	require.NoError(t, err)
	require.Len(t, ranked, 3)
	require.Equal(t, core.VectorID(2), ranked[0].ID)

	// OpenAI / Local LLM HTTP reranker
	llmSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"message": map[string]string{"content": `{"scores":[0.1,0.95,0.4]}`}},
			},
		})
	}))
	defer llmSrv.Close()

	llmReranker, err := NewLLMReranker(LLMRerankConfig{
		Provider: "openai",
		Endpoint: llmSrv.URL,
		APIKey:   "sk-test",
	})
	require.NoError(t, err)
	llmOut, err := llmReranker.Rerank(ctx, "vector database encryption", candidates)
	require.NoError(t, err)
	require.Equal(t, core.VectorID(2), llmOut[0].ID)

	// Cache hit/miss verification
	cache := NewRerankCache(16, time.Minute)
	_, hit := cache.Get("hybrid", "vector database encryption", candidates)
	require.False(t, hit)
	cache.Put("hybrid", "vector database encryption", candidates, ranked)
	cached, hit := cache.Get("hybrid", "vector database encryption", candidates)
	require.True(t, hit)
	require.Equal(t, ranked[0].ID, cached[0].ID)

	// Query Rewriter (synonyms, multi_query, hyde)
	qr := NewQueryRewriter()
	syn := qr.Rewrite("fast db auth", RewriteSynonyms)
	require.Len(t, syn, 2)
	require.Contains(t, syn[1], "database")
	multi := qr.Rewrite("vector search and rbac auth", RewriteMultiQuery)
	require.GreaterOrEqual(t, len(multi), 3)
	hyde := qr.Rewrite("how HNSW works", RewriteHyDE)
	require.Len(t, hyde, 2)

	// Eval harness NDCG/MRR/Recall
	metricsRes, err := EvaluateReranker(ctx, hybrid, []EvalQueryCase{
		{
			Query:      "vector database encryption",
			Candidates: candidates,
			Relevant:   map[core.VectorID]float64{2: 2.0, 3: 1.0},
		},
	}, 3)
	require.NoError(t, err)
	require.Greater(t, metricsRes.MeanNDCG, 0.85)
	require.Equal(t, 1.0, metricsRes.MeanMRR)
	require.Equal(t, 1.0, metricsRes.MeanRecall)
}

func FuzzQueryRewriterAndRerank(f *testing.F) {
	f.Add("fast db auth and vector search", "synonyms")
	f.Add("q1; q2?", "multi_query")
	f.Add("", "hyde")

	f.Fuzz(func(t *testing.T, query, mode string) {
		qr := NewQueryRewriter()
		_ = qr.Rewrite(query, QueryRewriteMode(mode))

		hr := NewHybridReranker(nil)
		cands := []SearchResult{
			makeCandidate(1, 0.1, query, "sample document"),
			makeCandidate(2, 0.2, "other", query),
		}
		res, err := hr.Rerank(context.Background(), query, cands)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) != 2 {
			t.Fatalf("expected 2 results, got %d", len(res))
		}
	})
}
