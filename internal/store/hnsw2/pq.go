package hnsw2

import (
	"math"
	"math/rand"
	"github.com/23skdu/longbow/internal/simd"
)

// PQConfig holds the configuration for product quantization.
type PQConfig struct {
	Enabled bool
	M       int // Number of subvectors
	Ksub    int // Number of centroids per subvector (default 256)
	Dim     int // Total vector dimensions
}

// PQEncoder handles encoding/decoding using product quantization.
type PQEncoder struct {
	config   PQConfig
	codebook [][][]float32 // [M][Ksub][SubDim] centroids
	subDim   int
	// SDC Table for fast Symmetric Distance Computation
	// [M][256][256]float32
	sdcTable [][][]float32
}

// NewPQEncoder creates encoder from pre-trained codebook.
func NewPQEncoder(cfg PQConfig, codebook [][][]float32) *PQEncoder {
	// Should call EnableSDC? For now lazy or explicit.
	enc := &PQEncoder{
		config:   cfg,
		codebook: codebook,
		subDim:   cfg.Dim / cfg.M,
	}
	enc.EnableSDC()
	return enc
}

// EnableSDC precomputes the centroid-to-centroid distance table for O(M) search.
func (e *PQEncoder) EnableSDC() {
	m := e.config.M
	k := e.config.Ksub
	if k == 0 { k = 256 }
	e.sdcTable = make([][][]float32, m)

	for i := 0; i < m; i++ {
		e.sdcTable[i] = make([][]float32, k)
		for c1 := 0; c1 < k; c1++ {
			e.sdcTable[i][c1] = make([]float32, k)
			for c2 := 0; c2 < k; c2++ {
				d := squaredL2(e.codebook[i][c1], e.codebook[i][c2])
				e.sdcTable[i][c1][c2] = d
			}
		}
	}
}

// SDCDistanceOneBatch calculates distance from one vector (codeA) to multiple vectors (codesB).
// codeA: [M] bytes.
// codesB: [Count * M] bytes.
// scratch: [M * 256] float32 buffer for the ADC table.
func (e *PQEncoder) SDCDistanceOneBatch(codeA, codesB []byte, m int, results []float32, scratch []float32) {
	// Optimization: Transform SDC (Symmetric Distance) into ADC (Asymmetric Distance)
	// by constructing a lookup table for codeA.
	// table[m][code] = sdcTable[m][codeA[m]][code]
	
	k := e.config.Ksub
	if k == 0 { k = 256 }
	
	// If scratch is not provided or too small, allocate (avoid this in hot path!)
	if len(scratch) < m*k {
		scratch = make([]float32, m*k)
	}
	
	// Build the ADC table for codeA
	// For each subspace m, the row is simply the row from sdcTable corresponding to codeA[m]
	for i := 0; i < m; i++ {
		c1 := int(codeA[i])
		// Copy 256 floats
		copy(scratch[i*k:], e.sdcTable[i][c1])
	}
	
	// Now use the SIMD-optimized ADC batch function
	simd.ADCDistanceBatch(scratch, codesB, m, results)
}

// TrainPQEncoder trains codebook via k-means.
func TrainPQEncoder(cfg PQConfig, vectors [][]float32) (*PQEncoder, error) {
	if cfg.M == 0 || cfg.Dim == 0 || cfg.Dim%cfg.M != 0 {
		// Just panic or error? Simplification for internal usage.
		return nil, nil 
	}
	
	subDim := cfg.Dim / cfg.M
	k := cfg.Ksub
	if k == 0 {
		k = 256
	}

	codebook := make([][][]float32, cfg.M)

	// Train each subspace
	for m := 0; m < cfg.M; m++ {
		start := m * subDim
		end := start + subDim

		// Extract subvectors
		subvecs := make([][]float32, len(vectors))
		for i, vec := range vectors {
			subvecs[i] = vec[start:end]
		}

		// K-means (simplified 10 iterations)
		codebook[m] = kmeans(subvecs, k, subDim, 10)
	}

	enc := &PQEncoder{
		config:   cfg,
		codebook: codebook,
		subDim:   subDim,
	}
	enc.EnableSDC()
	return enc, nil
}

func kmeans(vectors [][]float32, k, dim, iterations int) [][]float32 {
	if len(vectors) < k {
		// Not enough vectors, just use what we have padded
		// (Real impl should handle better)
		return nil
	}

	centroids := make([][]float32, k)
	for i := 0; i < k; i++ {
		centroids[i] = make([]float32, dim)
		// Random init
		copy(centroids[i], vectors[rand.Intn(len(vectors))])
	}

	assignments := make([]int, len(vectors))

	for iter := 0; iter < iterations; iter++ {
		// Assign
		for i, vec := range vectors {
			minDist := float32(math.MaxFloat32)
			best := 0
			for j, center := range centroids {
				d := simd.EuclideanDistance(vec, center) // Use squared L2 normally, but euclidean ok for comparison
				if d < minDist {
					minDist = d
					best = j
				}
			}
			assignments[i] = best
		}

		// Update
		counts := make([]int, k)
		newCentroids := make([][]float32, k)
		for i := 0; i < k; i++ {
			newCentroids[i] = make([]float32, dim)
		}

		for i, vec := range vectors {
			c := assignments[i]
			counts[c]++
			for d := 0; d < dim; d++ {
				newCentroids[c][d] += vec[d]
			}
		}

		for i := 0; i < k; i++ {
			if counts[i] > 0 {
				inv := 1.0 / float32(counts[i])
				for d := 0; d < dim; d++ {
					centroids[i][d] = newCentroids[i][d] * inv
				}
			}
		}
	}
	return centroids
}

// Encode converts vector to codes.
func (e *PQEncoder) Encode(vec []float32) []byte {
	codes := make([]byte, e.config.M)
	e.EncodeInto(vec, codes)
	return codes
}

func (e *PQEncoder) EncodeInto(vec []float32, codes []byte) {
	for m := 0; m < e.config.M; m++ {
		start := m * e.subDim
		end := start + e.subDim
		subvec := vec[start:end]

		minDist := float32(math.MaxFloat32)
		best := 0
		for k, center := range e.codebook[m] {
			d := simd.EuclideanDistance(subvec, center)
			if d < minDist {
				minDist = d
				best = k
			}
		}
		codes[m] = byte(best)
	}
}

// ComputeTableFlat computes the distance table for ADC.
// Returns flat table [M * 256].
func (e *PQEncoder) ComputeTableFlat(query []float32) []float32 {
	mTotal := e.config.M
	ksub := e.config.Ksub
	if ksub == 0 { ksub = 256 }
	
	table := make([]float32, mTotal*ksub) // Allocation here. Should use context buffer?

	for m := 0; m < mTotal; m++ {
		start := m * e.subDim
		end := start + e.subDim
		subquery := query[start:end]

		baseIdx := m * ksub
		for k, center := range e.codebook[m] {
			// Using Squared L2 for ADC table
			d := squaredL2(subquery, center)
			table[baseIdx+k] = d
		}
	}
	return table
}

// ComputeTableFlatInto computes the distance table for ADC into the provided destination buffer.
// If dst is nil or too small, a new buffer is allocated.
// Returns the slice containing the table.
func (e *PQEncoder) ComputeTableFlatInto(query []float32, dst []float32) []float32 {
	mTotal := e.config.M
	ksub := e.config.Ksub
	if ksub == 0 { ksub = 256 }
	
	reqSize := mTotal * ksub
	if cap(dst) < reqSize {
		dst = make([]float32, reqSize)
	}
	dst = dst[:reqSize]

	for m := 0; m < mTotal; m++ {
		start := m * e.subDim
		end := start + e.subDim
		subquery := query[start:end]

		baseIdx := m * ksub
		for k, center := range e.codebook[m] {
			d := squaredL2(subquery, center)
			dst[baseIdx+k] = d
		}
	}
	return dst
}

func squaredL2(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}
