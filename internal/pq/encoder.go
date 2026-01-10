package pq

import (
	"errors"
	"math"
	"unsafe"
)

// PQEncoder implements Product Quantization.
type PQEncoder struct {
	Dims      int         // Total vector dimensions
	M         int         // Number of subvectors (subspaces)
	K         int         // Number of centroids per subspace (default 256)
	SubDim    int         // Dimension of each subvector
	Codebooks [][]float32 // Flattened codebooks: M * (K * SubDim)
}

// NewPQEncoder creates a new PQ encoder.
func NewPQEncoder(dims, m, k int) (*PQEncoder, error) {
	if dims%m != 0 {
		return nil, errors.New("dimension must be divisible by M")
	}
	e := &PQEncoder{
		Dims:      dims,
		M:         m,
		K:         k,
		SubDim:    dims / m,
		Codebooks: make([][]float32, m),
	}
	for i := 0; i < m; i++ {
		e.Codebooks[i] = make([]float32, k*(dims/m))
	}
	return e, nil
}

// Train trains the K-Means codebooks using the provided sample vectors.
func (e *PQEncoder) Train(vectors [][]float32) error {
	if len(vectors) == 0 {
		return errors.New("empty training data")
	}

	n := len(vectors)

	// Pre-allocate codebooks storage: M slices, each K*SubDim

	// We train each subspace independently
	// Gather subvectors for subspace m
	// Since vectors is [][]float32, we can't just pass a flat slice.
	// We need to flatten the subvectors for K-Means.

	flatSubData := make([]float32, n*e.SubDim)

	for m := 0; m < e.M; m++ {
		// 1. Collect all subvectors for subspace m
		for i := 0; i < n; i++ {
			src := vectors[i][m*e.SubDim : (m+1)*e.SubDim]
			dst := flatSubData[i*e.SubDim : (i+1)*e.SubDim]
			copy(dst, src)
		}

		// 2. Run K-Means
		centroids, err := TrainKMeans(flatSubData, n, e.SubDim, e.K, 20) // 20 iters default
		if err != nil {
			return err
		}

		e.Codebooks[m] = centroids
	}

	return nil
}

// Encode compresses a vector into M bytes (indices).
func (e *PQEncoder) Encode(vector []float32) ([]byte, error) {
	if len(vector) != e.M*e.SubDim {
		return nil, errors.New("vector dimension mismatch")
	}
	if len(e.Codebooks) != e.M {
		return nil, errors.New("encoder not trained")
	}

	codes := make([]byte, e.M)

	for m := 0; m < e.M; m++ {
		subVec := vector[m*e.SubDim : (m+1)*e.SubDim]
		centroids := e.Codebooks[m]

		// Find nearest centroid
		bestDist := float32(math.MaxFloat32)
		bestIdx := 0

		for k := 0; k < e.K; k++ {
			cent := centroids[k*e.SubDim : (k+1)*e.SubDim]
			dist := distL2Sq(subVec, cent)
			if dist < bestDist {
				bestDist = dist
				bestIdx = k
			}
		}
		codes[m] = byte(bestIdx)
	}

	return codes, nil
}

// Decode reconstructs the approximated vector from the codes.
func (e *PQEncoder) Decode(codes []byte) ([]float32, error) {
	if len(codes) != e.M {
		return nil, errors.New("code length mismatch")
	}
	if len(e.Codebooks) != e.M {
		return nil, errors.New("encoder not trained")
	}

	vec := make([]float32, e.M*e.SubDim)

	for m := 0; m < e.M; m++ {
		code := codes[m]
		// Retrieve centroid
		cent := e.Codebooks[m][int(code)*e.SubDim : (int(code)+1)*e.SubDim]
		// Copy into place
		copy(vec[m*e.SubDim:], cent)
	}

	return vec, nil
}

// CodeSize returns the number of bytes for an encoded vector.
func (e *PQEncoder) CodeSize() int {
	return e.M
}

// ComputeDistanceTableFlat is an alias for BuildADCTable for backward compatibility.
func (e *PQEncoder) ComputeDistanceTableFlat(query []float32) []float32 {
	table, _ := e.BuildADCTable(query)
	return table
}

// PackBytesToFloat32s packs a byte slice into a float32 slice (bit-casting).
// Used for compatibility with libraries that only accept []float32.
func PackBytesToFloat32s(bytes []byte) []float32 {
	if len(bytes) == 0 {
		return nil
	}
	numFloats := (len(bytes) + 3) / 4
	res := make([]float32, numFloats)
	// Use unsafe to view the float32 slice as a byte slice and copy
	resBytes := unsafe.Slice((*byte)(unsafe.Pointer(&res[0])), len(res)*4)
	copy(resBytes, bytes)
	return res
}

// UnpackFloat32sToBytes unpacks a float32 slice back into a byte slice.
func UnpackFloat32sToBytes(floats []float32, size int) []byte {
	if len(floats) == 0 {
		return nil
	}
	res := make([]byte, size)
	floatBytes := unsafe.Slice((*byte)(unsafe.Pointer(&floats[0])), len(floats)*4)
	copy(res, floatBytes[:size])
	return res
}
