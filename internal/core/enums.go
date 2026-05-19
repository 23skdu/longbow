package core

// DistanceMetric defines the distance metric used for vector comparison.
type DistanceMetric string

const (
	// MetricEuclidean is the default L2 distance (lower is closer).
	MetricEuclidean DistanceMetric = "euclidean"
	// MetricCosine is the Cosine distance (1.0 - cosine_similarity).
	MetricCosine DistanceMetric = "cosine"
	// MetricDotProduct is the Inner Product (higher is usually better, but handling depends mostly on impl).
	MetricDotProduct DistanceMetric = "dot_product"
	// MetricL2Squared is the squared Euclidean distance (L2^2).
	MetricL2Squared DistanceMetric = "l2_squared"
)
