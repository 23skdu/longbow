package pq

import (
	"errors"
	"math"
	"math/rand"
	"sync"

	"github.com/23skdu/longbow/internal/simd"
)

// Buffer pool for K-Means training to reduce allocations
var kmeansBufferPool = sync.Pool{
	New: func() any {
		return &kmeansBuffers{}
	},
}

// kmeansBuffers holds reusable buffers for K-Means training
type kmeansBuffers struct {
	assignments []int
	counts      []int
	sums        []float32
}

// getBuffers retrieves buffers from the pool or creates new ones
func getKMeansBuffers(n, k, dim int) *kmeansBuffers {
	buf := kmeansBufferPool.Get().(*kmeansBuffers)

	// Resize if needed
	if cap(buf.assignments) < n {
		buf.assignments = make([]int, n)
	}
	buf.assignments = buf.assignments[:n]

	if cap(buf.counts) < k {
		buf.counts = make([]int, k)
	}
	buf.counts = buf.counts[:k]

	if cap(buf.sums) < k*dim {
		buf.sums = make([]float32, k*dim)
	}
	buf.sums = buf.sums[:k*dim]

	return buf
}

// putBuffers returns buffers to the pool
func putKMeansBuffers(buf *kmeansBuffers) {
	kmeansBufferPool.Put(buf)
}

// TrainKMeans runs K-Means clustering on flattened data.
// data: flattened vector data (n * dim)
// n: number of vectors
// dim: dimension of each vector
// k: number of centroids
// maxIter: maximum number of iterations
//
// Returns:
//   - centroids: flattened centroids (k * dim)
//   - error
func TrainKMeans(data []float32, n, dim, k, maxIter int) ([]float32, error) {
	if n < k {
		return nil, errors.New("insufficient data for k-means: n < k")
	}
	if len(data) != n*dim {
		return nil, errors.New("data length mismatch")
	}

	centroids := make([]float32, k*dim)

	// 1. Initialization: Randomly select k centroids from data
	perm := rand.Perm(n)
	for i := 0; i < k; i++ {
		idx := perm[i]
		copy(centroids[i*dim:(i+1)*dim], data[idx*dim:(idx+1)*dim])
	}

	// Get buffers from pool (reduces allocations for repeated training)
	buf := getKMeansBuffers(n, k, dim)
	defer putKMeansBuffers(buf)

	assignments := buf.assignments
	counts := buf.counts
	sums := buf.sums

	// 2. Iteration
	for iter := 0; iter < maxIter; iter++ {
		// Reset accumulators using clear() (more efficient than manual loops)
		clear(sums)
		clear(counts)

		changed := 0

		// E-step: Assign vectors to nearest centroid
		for i := 0; i < n; i++ {
			vec := data[i*dim : (i+1)*dim]
			bestDist := float32(math.MaxFloat32)
			bestC := -1

			for c := 0; c < k; c++ {
				cent := centroids[c*dim : (c+1)*dim]
				dist, err := simd.L2Squared(vec, cent)
				if err != nil {
					continue
				}
				if dist < bestDist {
					bestDist = dist
					bestC = c
				}
			}

			if assignments[i] != bestC {
				changed++
				assignments[i] = bestC
			}

			// Add to sums for M-step
			counts[bestC]++
			centSum := sums[bestC*dim : (bestC+1)*dim]
			for j := 0; j < dim; j++ {
				centSum[j] += vec[j]
			}
		}

		// M-step: Update centroids
		for c := 0; c < k; c++ {
			count := float32(counts[c])
			if count > 0 {
				cent := centroids[c*dim : (c+1)*dim]
				sum := sums[c*dim : (c+1)*dim]
				for j := 0; j < dim; j++ {
					cent[j] = sum[j] / count
				}
			} else {
				// Re-initialize empty cluster with a random vector from data
				idx := rand.Intn(n)
				copy(centroids[c*dim:(c+1)*dim], data[idx*dim:(idx+1)*dim])
			}
		}

		// Early stop if few assignments changed (e.g. < 0.1%)
		if iter > 0 && changed < (n/1000)+1 {
			break
		}
	}

	return centroids, nil
}
