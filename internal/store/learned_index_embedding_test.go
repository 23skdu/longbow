package store

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Embedding Provider / Learned Index Integration Tests
// =============================================================================

// TestEmbeddingProviderOrdinal verifies that the ordinal mapping is stable and
// covers all five provider backends plus the no-embedding case.
func TestEmbeddingProviderOrdinal(t *testing.T) {
	cases := []struct {
		provider string
		want     float64
	}{
		{"", 0.0},               // no generator
		{"openai", 1.0},
		{"cohere", 2.0},
		{"huggingface", 3.0},
		{"onnx", 4.0},
		{"wasm", 5.0},
		{"local", 6.0},
		{"unknown_future", 0.0}, // unknown → no-generator fallback
	}
	for _, tc := range cases {
		got := embeddingProviderOrdinal(tc.provider)
		assert.InDeltaf(t, tc.want, got, 1e-9, "provider=%q", tc.provider)
	}
}

// TestEmbeddingModelDimRatio verifies known provider/model pairs map to their
// published embedding dimensions as a ratio relative to 384.
func TestEmbeddingModelDimRatio(t *testing.T) {
	const ref = 384.0
	cases := []struct {
		provider  string
		model     string
		actualDim int
		want      float64
	}{
		{"openai", "text-embedding-3-large", 1536, 1536.0 / ref},
		{"openai", "text-embedding-3-small", 1536, 1536.0 / ref},
		{"openai", "text-embedding-ada-002", 1536, 1536.0 / ref},
		{"cohere", "embed-english-v3.0", 1024, 1024.0 / ref},
		{"cohere", "embed-multilingual-v3.0", 1024, 1024.0 / ref},
		{"cohere", "embed-english-light-v3.0", 384, 384.0 / ref},
		// Fallback: unknown model uses actualDim / ref.
		{"huggingface", "all-mpnet-base-v2", 768, 768.0 / ref},
		{"onnx", "custom", 256, 256.0 / ref},
		// Zero actualDim → 1.0 (reference assumption).
		{"", "", 0, 1.0},
	}
	for _, tc := range cases {
		got := embeddingModelDimRatio(tc.provider, tc.model, tc.actualDim)
		assert.InDeltaf(t, tc.want, got, 1e-6,
			"provider=%q model=%q dim=%d", tc.provider, tc.model, tc.actualDim)
	}
}

// TestExtractFeatureVector_EmbeddingFields verifies that EmbeddingProvider and
// EmbeddingModel correctly populate positions 11 and 12 in the feature vector,
// and that the total vector length matches numFeatures (now 13).
func TestExtractFeatureVector_EmbeddingFields(t *testing.T) {
	fv := extractFeatureVector(QueryFeatures{
		VectorDimension:   1536,
		EmbeddingProvider: "openai",
		EmbeddingModel:    "text-embedding-3-large",
	})
	assert.Equal(t, numFeatures, len(fv), "feature vector length must match numFeatures (13)")
	// Position 11: provider ordinal for openai = 1.0
	assert.InDelta(t, 1.0, fv[11], 1e-9, "openai ordinal must be 1.0")
	// Position 12: model dim ratio (1536/384 = 4.0)
	assert.InDelta(t, 4.0, fv[12], 1e-6, "text-embedding-3-large dim ratio must be 4.0")
}

func TestExtractFeatureVector_NoEmbedding(t *testing.T) {
	fv := extractFeatureVector(QueryFeatures{VectorDimension: 128})
	assert.Equal(t, numFeatures, len(fv))
	assert.InDelta(t, 0.0, fv[11], 1e-9, "no provider → ordinal 0")
	// Fallback: 128/384 ≈ 0.333...
	assert.InDelta(t, 128.0/384.0, fv[12], 1e-6, "no model → actualDim/ref")
}

// TestKNNPredict_EmbeddingProviderDiscriminates verifies that after training with
// mixed provider samples, the k-NN scorer correctly recommends DiskANN for
// high-dimension OpenAI embeddings vs HNSW for low-dimension local embeddings.
func TestKNNPredict_EmbeddingProviderDiscriminates(t *testing.T) {
	logger := zerolog.New(nil).With().Logger()
	p := NewIndexPerformancePredictor(logger, LearnedIndexConfig{
		MinTrainingSamples: 50,
		KNN:                7,
	})

	// Seed: OpenAI 1536d → DiskANN (large-scale production pattern).
	for i := 0; i < 60; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension:   1536,
				DatasetSize:       2000000 + i*1000,
				SearchK:           10,
				IsHybrid:          true,
				EmbeddingProvider: "openai",
				EmbeddingModel:    "text-embedding-3-large",
			},
			Index: IndexTypeDiskANN,
		})
	}

	// Seed: local 128d → HNSW (small-scale dev pattern).
	for i := 0; i < 60; i++ {
		p.AddTrainingSample(TrainingSample{
			Features: QueryFeatures{
				VectorDimension:   128,
				DatasetSize:       10000 + i*100,
				SearchK:           10,
				IsHybrid:          false,
				EmbeddingProvider: "local",
			},
			Index: IndexTypeHNSW,
		})
	}

	// Query matching OpenAI pattern should prefer DiskANN.
	openAIPred := p.Predict(QueryFeatures{
		VectorDimension:   1536,
		DatasetSize:       2500000,
		SearchK:           10,
		IsHybrid:          true,
		EmbeddingProvider: "openai",
		EmbeddingModel:    "text-embedding-3-large",
	})
	assert.Equal(t, IndexTypeDiskANN, openAIPred.RecommendedIndex,
		"k-NN must prefer DiskANN for OpenAI 1536d matching training distribution")

	// Query matching local pattern should prefer HNSW.
	localPred := p.Predict(QueryFeatures{
		VectorDimension:   128,
		DatasetSize:       15000,
		SearchK:           10,
		EmbeddingProvider: "local",
	})
	assert.Equal(t, IndexTypeHNSW, localPred.RecommendedIndex,
		"k-NN must prefer HNSW for local 128d matching training distribution")
}

// TestSetActiveEmbedding_RoundTrip verifies SetActiveEmbedding/GetActiveEmbedding
// round-trips correctly on a VectorStore and that unset fields return empty strings.
func TestSetActiveEmbedding_RoundTrip(t *testing.T) {
	s := &VectorStore{}
	s.SetActiveEmbedding("cohere", "embed-english-v3.0")
	prov, model := s.GetActiveEmbedding()
	assert.Equal(t, "cohere", prov)
	assert.Equal(t, "embed-english-v3.0", model)

	// Default (unset) should be empty strings.
	s2 := &VectorStore{}
	p2, m2 := s2.GetActiveEmbedding()
	assert.Equal(t, "", p2)
	assert.Equal(t, "", m2)
}
