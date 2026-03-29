package store

import (
	"errors"
	"fmt"
)

// computeCentroid calculates the mean vector of a set of vectors.
// It assumes all vectors have the same dimension as the first one.
func computeCentroid(vectors [][]float32) ([]float32, error) {
	if len(vectors) == 0 {
		return nil, errors.New("no vectors provided for centroid calculation")
	}

	dim := len(vectors[0])
	if dim == 0 {
		return nil, errors.New("empty vector provided for centroid calculation")
	}

	centroid := make([]float32, dim)
	for i, v := range vectors {
		if len(v) != dim {
			return nil, fmt.Errorf("dimension mismatch at index %d: expected %d, got %d", i, dim, len(v))
		}
		for j, val := range v {
			centroid[j] += val
		}
	}

	count := float32(len(vectors))
	for i := range centroid {
		centroid[i] /= count
	}

	return centroid, nil
}
