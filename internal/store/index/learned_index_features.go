package index

import (
	"math"
	"sync"
)

// FeatureNormalizer maintains online per-feature min/max statistics and produces
// unit-interval normalised feature vectors for use in k-NN distance computation.
type FeatureNormalizer struct {
	mu     sync.RWMutex
	minVal [numFeatures]float64
	maxVal [numFeatures]float64
	count  int64
}

func newFeatureNormalizer() *FeatureNormalizer {
	n := &FeatureNormalizer{}
	for i := range n.minVal {
		n.minVal[i] = math.MaxFloat64
		n.maxVal[i] = -math.MaxFloat64
	}
	return n
}

// Update incorporates a new feature vector into the normaliser's running statistics.
func (n *FeatureNormalizer) Update(v [numFeatures]float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, val := range v {
		if val < n.minVal[i] {
			n.minVal[i] = val
		}
		if val > n.maxVal[i] {
			n.maxVal[i] = val
		}
	}
	n.count++
}

// Normalize returns a [0,1]-clamped vector for v. Features with zero observed span
// are mapped to 0.5 (midpoint), avoiding division by zero.
func (n *FeatureNormalizer) Normalize(v [numFeatures]float64) [numFeatures]float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var out [numFeatures]float64
	for i, val := range v {
		span := n.maxVal[i] - n.minVal[i]
		if span > 0 {
			out[i] = (val - n.minVal[i]) / span
		} else {
			out[i] = 0.5
		}
	}
	return out
}

// Ready returns true once at least one sample has been observed.
func (n *FeatureNormalizer) Ready() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.count > 0
}

// extractFeatureVector converts a QueryFeatures struct into a dense float64 vector
// aligned with featureKeys. All values are raw (un-normalised).
func extractFeatureVector(f QueryFeatures) [numFeatures]float64 {
	var complexityScore float64
	switch f.QueryComplexity {
	case "simple":
		complexityScore = 0.0
	case "medium":
		complexityScore = 0.5
	case "complex":
		complexityScore = 1.0
	default:
		complexityScore = 0.25 // unknown → below-medium
	}
	filtered := 0.0
	if f.IsFiltered {
		filtered = 1.0
	}
	hybrid := 0.0
	if f.IsHybrid {
		hybrid = 1.0
	}
	return [numFeatures]float64{
		float64(f.VectorDimension),
		float64(f.NumQueryVectors),
		float64(f.SearchK),
		float64(f.DatasetSize),
		float64(f.NumCollections),
		complexityScore,
		f.AvgVectorNorm,
		filtered,
		hybrid,
		float64(f.TimeOfDay),
		float64(f.DayOfWeek),
		embeddingProviderOrdinal(f.EmbeddingProvider),
		embeddingModelDimRatio(f.EmbeddingProvider, f.EmbeddingModel, f.VectorDimension),
	}
}

// embeddingProviderOrdinal maps a provider name to a stable float64 ordinal used in
// the k-NN feature vector. Ordinals are stable across versions — do not reorder.
func embeddingProviderOrdinal(provider string) float64 {
	switch provider {
	case "openai":
		return 1.0
	case "cohere":
		return 2.0
	case "huggingface":
		return 3.0
	case "onnx":
		return 4.0
	case "wasm":
		return 5.0
	case "local":
		return 6.0
	default: // "", unknown → no embedding generator
		return 0.0
	}
}

// embeddingModelDimRatio encodes the relative dimensionality of the embedding model
// as a ratio to 384 (the reference dimension for all-MiniLM-L6-v2 / sentence-transformers).
// This captures the difference between compact models (0.33 for 128d) and large models
// (4.0 for 1536d text-embedding-3-large) as a continuous feature.
// Falls back to VectorDimension / 384 when model-specific info is not available.
func embeddingModelDimRatio(provider, model string, actualDim int) float64 {
	const referenceDim = 384.0
	switch {
	case provider == "openai" && model == "text-embedding-3-large":
		return 1536.0 / referenceDim
	case provider == "openai" && model == "text-embedding-3-small":
		return 1536.0 / referenceDim // same dim, different quality
	case provider == "openai" && model == "text-embedding-ada-002":
		return 1536.0 / referenceDim
	case provider == "cohere" && model == "embed-english-v3.0":
		return 1024.0 / referenceDim
	case provider == "cohere" && model == "embed-multilingual-v3.0":
		return 1024.0 / referenceDim
	case provider == "cohere" && model == "embed-english-light-v3.0":
		return 384.0 / referenceDim // 1.0
	default:
		if actualDim > 0 {
			return float64(actualDim) / referenceDim
		}
		return 1.0 // fallback: assume reference dimension
	}
}

// UpdateFromEmbedding updates the feature vector with signals derived from a raw embedding.
func (f *QueryFeatures) UpdateFromEmbedding(embedding []float64) {
	if len(embedding) == 0 {
		return
	}
	sumSq := 0.0
	for _, v := range embedding {
		sumSq += v * v
	}
	f.AvgVectorNorm = math.Sqrt(sumSq)
}
