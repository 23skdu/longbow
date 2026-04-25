package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFeatureNormalizer(t *testing.T) {
	n := newFeatureNormalizer()
	assert.False(t, n.Ready())

	v1 := [numFeatures]float64{100, 10, 5, 1000, 1, 0.5, 0.9, 0, 0, 12, 1, 1, 1}
	n.Update(v1)
	assert.True(t, n.Ready())

	norm1 := n.Normalize(v1)
	// With only one sample, all spans are 0, so should return 0.5 (midpoint)
	for i := range norm1 {
		assert.Equal(t, 0.5, norm1[i])
	}

	v2 := [numFeatures]float64{200, 20, 10, 2000, 2, 1.0, 1.0, 1, 1, 24, 7, 6, 4}
	n.Update(v2)

	norm2 := n.Normalize(v2)
	assert.Equal(t, 1.0, norm2[0]) // max value
	
	normMid := n.Normalize([numFeatures]float64{150, 15, 7.5, 1500, 1.5, 0.75, 0.95, 0.5, 0.5, 18, 4, 3.5, 2.5})
	assert.InDelta(t, 0.5, normMid[0], 0.01)
}

func TestExtractFeatureVector_Extended(t *testing.T) {
	f := QueryFeatures{
		VectorDimension: 1536,
		NumQueryVectors: 1,
		SearchK:         10,
		DatasetSize:     100000,
		NumCollections:  5,
		QueryComplexity: "complex",
		AvgVectorNorm:   1.0,
		IsFiltered:      true,
		IsHybrid:        false,
		TimeOfDay:       14,
		DayOfWeek:       3,
		EmbeddingProvider: "openai",
		EmbeddingModel: "text-embedding-3-large",
	}

	vec := extractFeatureVector(f)
	assert.Equal(t, 1536.0, vec[0])
	assert.Equal(t, 1.0, vec[5]) // complex -> 1.0
	assert.Equal(t, 1.0, vec[7]) // filtered -> 1.0
	assert.Equal(t, 0.0, vec[8]) // not hybrid -> 0.0
	assert.Equal(t, 1.0, vec[11]) // openai ordinal
	assert.InDelta(t, 4.0, vec[12], 0.01) // 1536 / 384 = 4.0
}

func TestEmbeddingModelDimRatio_Extended(t *testing.T) {
	assert.InDelta(t, 4.0, embeddingModelDimRatio("openai", "text-embedding-3-large", 1536), 0.01)
	assert.InDelta(t, 2.666, embeddingModelDimRatio("cohere", "embed-english-v3.0", 1024), 0.01)
	assert.InDelta(t, 1.0, embeddingModelDimRatio("cohere", "embed-english-light-v3.0", 384), 0.01)
	assert.InDelta(t, 0.333, embeddingModelDimRatio("local", "any", 128), 0.01)
	assert.InDelta(t, 1.0, embeddingModelDimRatio("unknown", "any", 0), 0.01)
}
