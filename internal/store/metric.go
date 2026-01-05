package store

// DistanceMetric defines the distance metric used for vector comparison.
type DistanceMetric string

const (
	// MetricEuclidean is the default L2 distance (lower is closer).
	MetricEuclidean DistanceMetric = "euclidean"
	// MetricCosine is the Cosine distance (1.0 - cosine_similarity).
	MetricCosine DistanceMetric = "cosine"
	// MetricDotProduct is the Inner Product (higher is usually better, but handling depends mostly on impl).
	// Longbow conventionally treats lower scores as "closer" for search, so we might return negative dot product
	// or just raw score. For HNSW, raw score is usually fine if we flip comparison.
	// However, hnsw library assumes lower is better?
	// Let's standardise: HNSW usually minimizes distance.
	// For Dot Product: dist = 1 - dot ? Or just -dot.
	// Simd implementation returns dot product.
	MetricDotProduct DistanceMetric = "dot_product"
)

// DistanceFunc is the function signature for calculating distance between two vectors.
type DistanceFunc func(a, b []float32) float32

// BatchDistanceFunc is the function signature for calculating distances between one query and multiple vectors.
type BatchDistanceFunc func(query []float32, vectors [][]float32, results []float32)

// BatchDistanceSQ8Func is the function signature for SQ8 batch distance.
type BatchDistanceSQ8Func func(query []byte, vectors [][]byte, results []float32)
