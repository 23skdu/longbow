package pq

import (
	"errors"

	"github.com/23skdu/longbow/internal/simd"
)

// BuildADCTable computes the distance table for Asymmetric Distance Computation (ADC).
// It accepts a query vector and returns a flattened look-up table.
//
// table[m*K + k] stores the partial distance between query_subvector[m] and centroid[m][k].
//
// The table size is M * K.
func (e *PQEncoder) BuildADCTable(query []float32) ([]float32, error) {
	if len(query) != e.M*e.SubDim {
		return nil, errors.New("query dimension mismatch")
	}
	if len(e.Codebooks) != e.M {
		return nil, errors.New("encoder not trained")
	}

	m := e.M
	k := e.K
	subDim := e.SubDim
	table := make([]float32, m*k)

	for i := 0; i < m; i++ {
		start := i * subDim
		end := start + subDim
		querySub := query[start:end]

		// Codebooks[i] is flattened: [k * subDim]
		centroids := e.Codebooks[i]

		for j := 0; j < k; j++ {
			cStart := j * subDim
			cEnd := cStart + subDim
			cent := centroids[cStart:cEnd]

			// Compute squared L2 distance between query subvector and centroid
			dist := distL2Sq(querySub, cent)
			table[i*k+j] = dist
		}
	}

	return table, nil
}

// ADCDistanceBatch calculates asymmetric distances for multiple PQ-encoded vectors.
// table: precomputed distance table using BuildADCTable (size M * K)
// flatCodes: flattened byte slice of PQ codes (N * M)
// results: slice to store distances (N)
func (e *PQEncoder) ADCDistanceBatch(table []float32, flatCodes []byte, results []float32) error {
	if len(results) == 0 {
		return nil
	}
	if len(flatCodes) < len(results)*e.M {
		return errors.New("flatCodes buffer too small")
	}
	if len(table) != e.M*e.K {
		return errors.New("invalid table size")
	}

	// Use SIMD accelerated function
	simd.ADCDistanceBatch(table, flatCodes, e.M, results)
	return nil
}

// ADCDistance computes ADC distance for a single code.
// table: precomputed distance table (M * K)
// code: PQ code (M bytes)
func (e *PQEncoder) ADCDistance(table []float32, code []byte) (float32, error) {
	if len(code) != e.M {
		return 0, errors.New("invalid code length")
	}

	var sum float32
	// Fallback / Scalar version for single distance
	// Can be optimized but usually batch is preferred.
	for m := 0; m < e.M; m++ {
		idx := int(code[m])
		dist := table[m*e.K+idx]
		sum += dist
	}

	return sum, nil
}
