package store

import (
	"errors"
	"math"
	"math/rand"
)

// =============================================================================
// Product Quantization (PQ) - 8-64x Memory Reduction
// =============================================================================
// Divides vectors into M subvectors, each quantized to Ksub centroids.
// Memory: 128-dim float32 (512 bytes) -> 8 bytes (M=8) = 64x reduction

// PQConfig holds the configuration for product quantization.
type PQConfig struct {
	M      int // Number of subvectors (typically 8, 16, or 32)
	Ksub   int // Number of centroids per subvector (typically 256 for uint8)
	Dim    int // Total vector dimensions
	SubDim int // Dimensions per subvector (Dim / M)
}

// DefaultPQConfig returns a default PQ configuration for given dimensions.
func DefaultPQConfig(dim int) *PQConfig {
	m := 8 // Default: 8 subvectors
	return &PQConfig{
		M:      m,
		Ksub:   256, // uint8 codes
		Dim:    dim,
		SubDim: dim / m,
	}
}

// Validate checks if the configuration is valid.
func (c *PQConfig) Validate() error {
	if c == nil {
		return errors.New("PQConfig is nil")
	}
	if c.M <= 0 {
		return errors.New("m must be positive")
	}
	if c.Ksub <= 0 {
		return errors.New("ksub must be positive")
	}
	if c.Ksub > 256 {
		return errors.New("ksub cannot exceed 256 for uint8 codes")
	}
	if c.Dim%c.M != 0 {
		return errors.New("dim must be divisible by M")
	}
	return nil
}

// PQEncoder handles encoding/decoding using product quantization.
type PQEncoder struct {
	config   *PQConfig
	codebook [][][]float32 // [M][Ksub][SubDim] centroids
	// SDC Table for fast Symmetric Distance Computation
	// [M][256][256]float32
	sdcTable [][][]float32
}

// NewPQEncoder creates encoder from pre-trained codebook.
func NewPQEncoder(cfg *PQConfig, codebook [][][]float32) (*PQEncoder, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(codebook) != cfg.M {
		return nil, errors.New("codebook must have M subspaces")
	}
	for m, subspace := range codebook {
		if len(subspace) != cfg.Ksub {
			return nil, errors.New("each subspace must have Ksub centroids")
		}
		for _, centroid := range subspace {
			if len(centroid) != cfg.SubDim {
				return nil, errors.New("centroids must have SubDim dimensions")
			}
		}
		_ = m
	}
	enc := &PQEncoder{config: cfg, codebook: codebook}
	return enc, nil
}

// EnableSDC precomputes the centroid-to-centroid distance table for O(M) search.
func (e *PQEncoder) EnableSDC() {
	m := e.config.M
	k := e.config.Ksub
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

// TrainPQEncoder trains codebook via k-means on sample vectors.
func TrainPQEncoder(cfg *PQConfig, vectors [][]float32, iterations int) (*PQEncoder, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(vectors) < cfg.Ksub {
		return nil, errors.New("need at least Ksub training vectors")
	}

	codebook := make([][][]float32, cfg.M)

	// Train each subspace independently
	for m := 0; m < cfg.M; m++ {
		start := m * cfg.SubDim
		end := start + cfg.SubDim

		// Extract subvectors
		subvecs := make([][]float32, len(vectors))
		for i, vec := range vectors {
			subvecs[i] = vec[start:end]
		}

		// K-means clustering
		codebook[m] = kmeansCluster(subvecs, cfg.Ksub, cfg.SubDim, iterations)
	}
	enc := &PQEncoder{config: cfg, codebook: codebook}
	// Enable SDC by default after training
	enc.EnableSDC()
	return enc, nil
}

// kmeansCluster performs k-means to find centroids.
func kmeansCluster(vectors [][]float32, k, dim, iterations int) [][]float32 {
	if len(vectors) == 0 || k == 0 {
		return nil
	}

	// Initialize centroids
	centroids := make([][]float32, k)
	for i := 0; i < k; i++ {
		centroids[i] = make([]float32, dim)
		// Random selection
		idx := rand.Intn(len(vectors))
		copy(centroids[i], vectors[idx])
	}

	assignments := make([]int, len(vectors))

	for iter := 0; iter < iterations; iter++ {
		// Assign vectors to centroids
		for i, vec := range vectors {
			minDist := float32(math.MaxFloat32)
			for j, centroid := range centroids {
				d := squaredL2(vec, centroid)
				if d < minDist {
					minDist = d
					assignments[i] = j
				}
			}
		}

		// Recompute centroids
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
				for d := 0; d < dim; d++ {
					centroids[i][d] = newCentroids[i][d] / float32(counts[i])
				}
			}
		}
	}

	return centroids
}

func squaredL2(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// Dims returns total vector dimensions.
func (e *PQEncoder) Dims() int { return e.config.Dim }

// CodeSize returns bytes per encoded vector (M bytes for uint8 codes).
func (e *PQEncoder) CodeSize() int { return e.config.M }

// GetCodebook returns the learned codebook.
func (e *PQEncoder) GetCodebook() [][][]float32 { return e.codebook }

// Encode converts float32 vector to PQ codes.
func (e *PQEncoder) Encode(vec []float32) []uint8 {
	codes := make([]uint8, e.config.M)
	e.EncodeInto(vec, codes)
	return codes
}

// EncodeInto encodes vector into pre-allocated codes slice.
func (e *PQEncoder) EncodeInto(vec []float32, codes []uint8) {
	for m := 0; m < e.config.M; m++ {
		start := m * e.config.SubDim
		end := start + e.config.SubDim
		subvec := vec[start:end]

		// Find nearest centroid
		minDist := float32(math.MaxFloat32)
		nearestIdx := 0
		for k, centroid := range e.codebook[m] {
			d := squaredL2(subvec, centroid)
			if d < minDist {
				minDist = d
				nearestIdx = k
			}
		}
		codes[m] = uint8(nearestIdx)
	}
}

// Decode reconstructs approximate vector from PQ codes.
func (e *PQEncoder) Decode(codes []uint8) []float32 {
	vec := make([]float32, e.config.Dim)
	for m, code := range codes {
		start := m * e.config.SubDim
		copy(vec[start:start+e.config.SubDim], e.codebook[m][code])
	}
	return vec
}

// ComputeDistanceTable precomputes distances from query to all centroids.
// Returns table[m][k] = squared distance from query subvector m to centroid k.
func (e *PQEncoder) ComputeDistanceTable(query []float32) [][]float32 {
	table := make([][]float32, e.config.M)
	for m := 0; m < e.config.M; m++ {
		start := m * e.config.SubDim
		end := start + e.config.SubDim
		subquery := query[start:end]

		table[m] = make([]float32, e.config.Ksub)
		for k, centroid := range e.codebook[m] {
			table[m][k] = squaredL2(subquery, centroid)
		}
	}
	return table
}

// ADCDistance computes Asymmetric Distance using precomputed table.
// This is O(M) per vector instead of O(D) - very fast for search.
func (e *PQEncoder) ADCDistance(table [][]float32, codes []uint8) float32 {
	var sum float32
	for m, code := range codes {
		sum += table[m][code]
	}
	return float32(math.Sqrt(float64(sum)))
}

// SDCDistance computes Symmetric Distance between two PQ codes.
// Uses precomputed centroid-centroid distances if EnableSDC was called.
func (e *PQEncoder) SDCDistance(codes1, codes2 []uint8) float32 {
	var sum float32
	if e.sdcTable != nil {
		// O(M) Lookup
		for m, c1 := range codes1 {
			c2 := codes2[m]
			sum += e.sdcTable[m][c1][c2]
		}
	} else {
		// O(D) Calculation fallback
		for m := 0; m < e.config.M; m++ {
			sum += squaredL2(e.codebook[m][codes1[m]], e.codebook[m][codes2[m]])
		}
	}
	return float32(math.Sqrt(float64(sum)))
}

// PackBytesToFloat32s packs uint8 codes into float32 slice for HNSW storage.
// Packs 4 bytes into 1 float32.
func PackBytesToFloat32s(codes []uint8) []float32 {
	nFloats := (len(codes) + 3) / 4
	res := make([]float32, nFloats)

	// Re-slice float array as bytes to copy logic
	// Note: We use manual packing to avoid unsafe pointer casting complexity in pure Go logic if possible,
	// but math.Float32frombits is cleanest.

	for i := 0; i < nFloats; i++ {
		var b0, b1, b2, b3 uint8
		idx := i * 4
		if idx < len(codes) {
			b0 = codes[idx]
		}
		if idx+1 < len(codes) {
			b1 = codes[idx+1]
		}
		if idx+2 < len(codes) {
			b2 = codes[idx+2]
		}
		if idx+3 < len(codes) {
			b3 = codes[idx+3]
		}

		val := uint32(b0) | uint32(b1)<<8 | uint32(b2)<<16 | uint32(b3)<<24
		res[i] = math.Float32frombits(val)
	}
	return res
}

// UnpackFloat32sToBytes reconstructs uint8 codes from packed float32 slice.
func UnpackFloat32sToBytes(packed []float32, length int) []uint8 {
	codes := make([]uint8, length)
	for i, f := range packed {
		val := math.Float32bits(f)
		idx := i * 4
		if idx < length {
			codes[idx] = uint8(val & 0xFF)
		}
		if idx+1 < length {
			codes[idx+1] = uint8((val >> 8) & 0xFF)
		}
		if idx+2 < length {
			codes[idx+2] = uint8((val >> 16) & 0xFF)
		}
		if idx+3 < length {
			codes[idx+3] = uint8((val >> 24) & 0xFF)
		}
	}
	return codes
}

// SDCDistancePacked computes Symmetric Distance between two packed PQ codes.
// 'a' and 'b' are float32 slices containing packed uint8 codes (4 codes per float32).
func (e *PQEncoder) SDCDistancePacked(a, b []float32) float32 {
	var sum float32
	// We iterate through the floats and unpack bytes
	// M is the total number of codes.
	mTotal := e.config.M

	// Direct access to table for speed
	table := e.sdcTable
	useTable := table != nil

	codeIdx := 0
	for i := 0; i < len(a) && codeIdx < mTotal; i++ {
		// Unpack a[i] and b[i]
		// Each float32 holds 4 bytes.
		valA := math.Float32bits(a[i])
		valB := math.Float32bits(b[i])

		// Byte 0
		c1 := uint8(valA & 0xFF)
		c2 := uint8(valB & 0xFF)
		if useTable {
			sum += table[codeIdx][c1][c2]
		} else {
			sum += squaredL2(e.codebook[codeIdx][c1], e.codebook[codeIdx][c2])
		}
		codeIdx++
		if codeIdx >= mTotal {
			break
		}

		// Byte 1
		c1 = uint8((valA >> 8) & 0xFF)
		c2 = uint8((valB >> 8) & 0xFF)
		if useTable {
			sum += table[codeIdx][c1][c2]
		} else {
			sum += squaredL2(e.codebook[codeIdx][c1], e.codebook[codeIdx][c2])
		}
		codeIdx++
		if codeIdx >= mTotal {
			break
		}

		// Byte 2
		c1 = uint8((valA >> 16) & 0xFF)
		c2 = uint8((valB >> 16) & 0xFF)
		if useTable {
			sum += table[codeIdx][c1][c2]
		} else {
			sum += squaredL2(e.codebook[codeIdx][c1], e.codebook[codeIdx][c2])
		}
		codeIdx++
		if codeIdx >= mTotal {
			break
		}

		// Byte 3
		c1 = uint8((valA >> 24) & 0xFF)
		c2 = uint8((valB >> 24) & 0xFF)
		if useTable {
			sum += table[codeIdx][c1][c2]
		} else {
			sum += squaredL2(e.codebook[codeIdx][c1], e.codebook[codeIdx][c2])
		}
		codeIdx++
	}
	return float32(math.Sqrt(float64(sum)))
}
