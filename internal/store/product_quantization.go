package store

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"
)

// =============================================================================
// Product Quantization (PQ)
// =============================================================================
// PQ splits vectors into M sub-vectors and quantizes each sub-vector independently
// using K-Means clustering (usually K=256 to fit in 1 byte).
// This allows high compression (e.g., 128 float32s -> 16 bytes for M=16).

// PQConfig holds configuration for the Product Quantizer.
type PQConfig struct {
	Dimensions    int // Total vector dimensions (D)
	NumSubVectors int // Number of sub-vectors (M). D must be divisible by M.
	NumCentroids  int // Number of centroids per sub-space (K). Usually 256.
}

// Validate checks if the configuration is valid.
func (c *PQConfig) Validate() error {
	if c.Dimensions <= 0 {
		return fmt.Errorf("dimensions must be > 0")
	}
	if c.NumSubVectors <= 0 {
		return fmt.Errorf("numSubVectors must be > 0")
	}
	if c.Dimensions%c.NumSubVectors != 0 {
		return fmt.Errorf("dimensions %d not divisible by numSubVectors %d", c.Dimensions, c.NumSubVectors)
	}
	if c.NumCentroids <= 0 || c.NumCentroids > 256 {
		return fmt.Errorf("numCentroids must be between 1 and 256")
	}
	return nil
}

// PQEncoder handles training, encoding, and decoding using Product Quantization.
type PQEncoder struct {
	config PQConfig
	// Codebooks stores centroids.
	// Dimensions: [M][K][Ds] where Ds = D / M
	Codebooks [][][]float32
	// Pre-computed tables for distance optimization could go here
}

// NewPQEncoder creates a new encoder with the given configuration.
func NewPQEncoder(cfg PQConfig) (*PQEncoder, error) {
	if cfg.Dimensions%cfg.NumSubVectors != 0 {
		return nil, fmt.Errorf("dimensions %d not divisible by numSubVectors %d", cfg.Dimensions, cfg.NumSubVectors)
	}
	if cfg.NumCentroids > 256 {
		return nil, fmt.Errorf("numCentroids %d > 256 not supported (need > 1 byte)", cfg.NumCentroids)
	}

	return &PQEncoder{
		config:    cfg,
		Codebooks: make([][][]float32, cfg.NumSubVectors),
	}, nil
}

// TrainPQEncoder trains a new PQ encoder using K-Means on the provided samples.
func TrainPQEncoder(cfg *PQConfig, vectors [][]float32, iterations int) (*PQEncoder, error) {
	if len(vectors) == 0 {
		return nil, fmt.Errorf("no vectors for training")
	}
	dims := len(vectors[0])
	if dims != cfg.Dimensions {
		return nil, fmt.Errorf("vector dimensions %d mismatch config %d", dims, cfg.Dimensions)
	}

	encoder, err := NewPQEncoder(*cfg)
	if err != nil {
		return nil, err
	}

	m := cfg.NumSubVectors
	k := cfg.NumCentroids
	subDim := dims / m

	// Train each sub-quantizer independently (can be parallelized)
	var wg sync.WaitGroup
	wg.Add(m)

	for i := 0; i < m; i++ {
		go func(subIdx int) {
			defer wg.Done()
			// Extract sub-vectors for this subspace
			subVectors := make([][]float32, len(vectors))
			start := subIdx * subDim
			end := start + subDim

			for vj, vec := range vectors {
				subVectors[vj] = vec[start:end]
			}

			// Run K-Means
			centroids := kMeans(subVectors, k, iterations, 0.001)
			encoder.Codebooks[subIdx] = centroids
		}(i)
	}

	wg.Wait()
	return encoder, nil
}

// SDCDistancePacked computes Symmetric Distance Computation (SDC) between two packed PQ codes.
// Returns squared Euclidean distance approximation.
// Inputs are packed float32s containing the codes.
func (e *PQEncoder) SDCDistancePacked(packed1, packed2 []float32) float32 {
	m := e.CodeSize()
	// Unpack (allocating for now, should optimize later)
	code1 := UnpackFloat32sToBytes(packed1, m)
	code2 := UnpackFloat32sToBytes(packed2, m)

	// SDC logic:
	// dist = sum(distance_table[sub_idx][code1[sub_idx]][code2[sub_idx]])
	// But optimizing: SDC using precomputed centroid-centroid distances.
	// For now, simpler: decode centroids and compute L2.
	// Low perf stub:
	v1 := e.Decode(code1)
	v2 := e.Decode(code2)
	return l2Squared(v1, v2)
}

// Encode quantizes a vector into M bytes.
func (e *PQEncoder) Encode(vec []float32) []byte {
	m := e.config.NumSubVectors
	subDim := e.config.Dimensions / m
	codes := make([]byte, m)

	for i := 0; i < m; i++ {
		start := i * subDim
		end := start + subDim
		subVec := vec[start:end]

		// Find nearest centroid
		bestIdx := 0
		minDist := float32(math.MaxFloat32)

		for k, centroid := range e.Codebooks[i] {
			dist := l2Squared(subVec, centroid)
			if dist < minDist {
				minDist = dist
				bestIdx = k
			}
		}
		codes[i] = byte(bestIdx)
	}

	return codes
}

// CodeSize returns the size of the encoded signature in bytes (M).
func (e *PQEncoder) CodeSize() int {
	return e.config.NumSubVectors
}

// Decode reconstructs the approximate vector from codes.
func (e *PQEncoder) Decode(codes []byte) []float32 {
	m := e.config.NumSubVectors
	subDim := e.config.Dimensions / m
	vec := make([]float32, e.config.Dimensions)

	for i, code := range codes {
		centroid := e.Codebooks[i][code]
		copy(vec[i*subDim:], centroid)
	}

	return vec
}

// BuildLUT (Rename to ComputeDistanceTableFlat)
// BuildLUT builds a Look-Up Table for Asymmetric Distance Computation (ADC).
// Returns a flat table of size M * K.
// lut[i*K + j] stores the partial distance between query_subvector[i] and centroid[i][j].
func (e *PQEncoder) ComputeDistanceTableFlat(query []float32) []float32 {
	m := e.config.NumSubVectors
	k := e.config.NumCentroids
	subDim := e.config.Dimensions / m

	lut := make([]float32, m*k)

	for i := 0; i < m; i++ {
		start := i * subDim
		end := start + subDim
		querySub := query[start:end]

		for j, centroid := range e.Codebooks[i] {
			dist := l2Squared(querySub, centroid)
			lut[i*k+j] = dist
		}
	}

	return lut
}

// ADCDistanceBatch computes distances for a batch of quantized vectors using the precomputed LUT.
// flatCodes contains packed codes for N vectors: [vec0_code... vec1_code...]
// results is a slice of length N.
// Only processes entries where results[j] == -1 (marked for processing).
func (e *PQEncoder) ADCDistanceBatch(lut []float32, flatCodes []byte, results []float32) {
	m := e.config.NumSubVectors
	k := e.config.NumCentroids

	// Basic implementation (can be optimized with SIMD/Assembly later)
	for j := 0; j < len(results); j++ {
		if results[j] == -1 {
			offset := j * m
			var dist float32
			for i := 0; i < m; i++ {
				code := flatCodes[offset+i]
				dist += lut[i*k+int(code)]
			}
			results[j] = dist
		}
	}
}

// Helper functions for packing bytes into float32s (used by HNSW storage)
// This packs 4 bytes into 1 float32 (using Unsafe or simple bit casting).
// We use math.Float32frombits.

func PackBytesToFloat32s(b []byte) []float32 {
	n := len(b)
	numFloats := (n + 3) / 4
	floats := make([]float32, numFloats)

	for i := 0; i < numFloats; i++ {
		var bits uint32
		validBytes := 4
		if (i+1)*4 > n {
			validBytes = n - i*4
		}

		for j := 0; j < validBytes; j++ {
			bits |= uint32(b[i*4+j]) << (8 * j)
		}
		floats[i] = math.Float32frombits(bits)
	}
	return floats
}

func UnpackFloat32sToBytes(floats []float32, length int) []byte {
	b := make([]byte, length)
	for i, f := range floats {
		bits := math.Float32bits(f)
		for j := 0; j < 4; j++ {
			if i*4+j < length {
				b[i*4+j] = byte(bits >> (8 * j))
			}
		}
	}
	return b
}

// =============================================================================
// Simple K-Means Implementation
// =============================================================================

func kMeans(data [][]float32, k int, maxIter int, tol float32) [][]float32 {
	if len(data) < k {
		// Not enough data points, just return data as centroids (padded if needed?)
		// Or duplicate. For now, strict check or just duplicate.
		// For safe implementation in production, we should handle this gracefully.
		// Here: Just take what we have.
		centroids := make([][]float32, k)
		for i := 0; i < k; i++ {
			centroids[i] = make([]float32, len(data[0]))
			copy(centroids[i], data[i%len(data)])
		}
		return centroids
	}

	dims := len(data[0])
	centroids := make([][]float32, k)

	// 1. Init: Randomly select k points
	perm := rand.Perm(len(data))
	for i := 0; i < k; i++ {
		centroids[i] = make([]float32, dims)
		copy(centroids[i], data[perm[i]])
	}

	assign := make([]int, len(data))
	counts := make([]int, k)
	newCentroids := make([][]float32, k)
	for i := range newCentroids {
		newCentroids[i] = make([]float32, dims)
	}

	for iter := 0; iter < maxIter; iter++ {
		changes := 0

		// 2. Assignment
		for i, vec := range data {
			bestCluster := 0
			minDist := float32(math.MaxFloat32)

			for cIdx, cVec := range centroids {
				d := l2Squared(vec, cVec)
				if d < minDist {
					minDist = d
					bestCluster = cIdx
				}
			}

			if assign[i] != bestCluster {
				assign[i] = bestCluster
				changes++
			}
		}

		// Check convergence
		if iter > 0 && float32(changes)/float32(len(data)) < tol {
			break
		}

		// 3. Update
		// Reset new centroids
		for i := 0; i < k; i++ {
			for j := 0; j < dims; j++ {
				newCentroids[i][j] = 0
			}
			counts[i] = 0
		}

		for i, vec := range data {
			c := assign[i]
			add(newCentroids[c], vec)
			counts[c]++
		}

		for i := 0; i < k; i++ {
			if counts[i] > 0 {
				scale(newCentroids[i], 1.0/float32(counts[i]))
				copy(centroids[i], newCentroids[i])
			} else {
				// Empty cluster? Re-init to a random point
				// Heuristic: pick random point
				rnd := rand.Intn(len(data))
				copy(centroids[i], data[rnd])
			}
		}
	}

	return centroids
}

// Helpers
func l2Squared(a, b []float32) float32 {
	var sum float32
	for i, v := range a {
		d := v - b[i]
		sum += d * d
	}
	return sum
}

func add(dst, src []float32) {
	for i, v := range src {
		dst[i] += v
	}
}

func scale(v []float32, s float32) {
	for i := range v {
		v[i] *= s
	}
}

// Serialization Helpers

// Serialize marshals the PQ encoder to bytes.
// Format: Config(12) | Codebooks...
func (e *PQEncoder) Serialize() []byte {
	// Simple custom serialization for now
	// Dims(4) | M(4) | K(4)
	// For each M: For each K: D/M floats

	dims := e.config.Dimensions
	m := e.config.NumSubVectors
	k := e.config.NumCentroids
	subDim := dims / m

	size := 4 + 4 + 4 + (m * k * subDim * 4)
	buf := make([]byte, size)

	offset := 0
	binary.LittleEndian.PutUint32(buf[offset:], uint32(dims))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(m))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(k))
	offset += 4

	for i := 0; i < m; i++ {
		for j := 0; j < k; j++ {
			for d := 0; d < subDim; d++ {
				val := math.Float32bits(e.Codebooks[i][j][d])
				binary.LittleEndian.PutUint32(buf[offset:], val)
				offset += 4
			}
		}
	}
	return buf
}

// DeserializePQEncoder unmarshals a PQ encoder from bytes.
func DeserializePQEncoder(data []byte) (*PQEncoder, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("invalid data size")
	}

	offset := 0
	dims := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	m := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	k := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	cfg := PQConfig{
		Dimensions:    dims,
		NumSubVectors: m,
		NumCentroids:  k,
	}
	enc, err := NewPQEncoder(cfg)
	if err != nil {
		return nil, err
	}

	subDim := dims / m
	expectedSize := 12 + (m * k * subDim * 4)
	if len(data) != expectedSize {
		return nil, fmt.Errorf("size mismatch: expected %d, got %d", expectedSize, len(data))
	}

	for i := 0; i < m; i++ {
		enc.Codebooks[i] = make([][]float32, k)
		for j := 0; j < k; j++ {
			enc.Codebooks[i][j] = make([]float32, subDim)
			for d := 0; d < subDim; d++ {
				bits := binary.LittleEndian.Uint32(data[offset:])
				enc.Codebooks[i][j][d] = math.Float32frombits(bits)
				offset += 4
			}
		}
	}

	return enc, nil
}
