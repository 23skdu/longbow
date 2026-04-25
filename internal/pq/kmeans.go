package pq

// nosec G404 - math/rand is used for k-means centroid initialization, not security-sensitive
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

// KMeansOptions holds options for K-Means training
type KMeansOptions struct {
	MaxIter     int
	GPUAssigner func(data []float32, centroids []float32) ([]uint32, error)
}

// TrainKMeans runs K-Means clustering on flattened data.
func TrainKMeans(data []float32, n, dim, k, maxIter int) ([]float32, error) {
	return TrainKMeansWithOptions(data, n, dim, k, KMeansOptions{MaxIter: maxIter})
}

// TrainKMeansWithOptions runs K-Means clustering with advanced options.
func TrainKMeansWithOptions(data []float32, n, dim, k int, opts KMeansOptions) ([]float32, error) {
	if n < k {
		return nil, errors.New("insufficient data for k-means: n < k")
	}
	if len(data) != n*dim {
		return nil, errors.New("data length mismatch")
	}

	maxIter := opts.MaxIter
	if maxIter <= 0 {
		maxIter = 20
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
		if opts.GPUAssigner != nil {
			gpuAssignments, err := opts.GPUAssigner(data, centroids)
			if err == nil {
				for i, bestC := range gpuAssignments {
					bc := int(bestC)
					if assignments[i] != bc {
						changed++
						assignments[i] = bc
					}
					counts[bc]++
					centSum := sums[bc*dim : (bc+1)*dim]
					vec := data[i*dim : (i+1)*dim]
					for j := 0; j < dim; j++ {
						centSum[j] += vec[j]
					}
				}
			} else {
				// Fallback to CPU on GPU error
				changed = runCPUEstep(data, centroids, assignments, counts, sums, n, k, dim)
			}
		} else {
			changed = runCPUEstep(data, centroids, assignments, counts, sums, n, k, dim)
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
				idx := rand.Intn(n) // #nosec G404
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

func runCPUEstep(data, centroids []float32, assignments, counts []int, sums []float32, n, k, dim int) int {
	changed := 0
	for i := 0; i < n; i++ {
		vec := data[i*dim : (i+1)*dim]
		bestDist := float32(math.MaxFloat32)
		bestC := -1

		for c := 0; c < k; c++ {
			cent := centroids[c*dim : (c+1)*dim]
			dist, _ := simd.L2Squared(vec, cent)
			if dist < bestDist {
				bestDist = dist
				bestC = c
			}
		}

		if assignments[i] != bestC {
			changed++
			assignments[i] = bestC
		}

		counts[bestC]++
		centSum := sums[bestC*dim : (bestC+1)*dim]
		for j := 0; j < dim; j++ {
			centSum[j] += vec[j]
		}
	}
	return changed
}
