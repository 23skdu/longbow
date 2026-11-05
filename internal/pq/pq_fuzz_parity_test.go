package pq

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPQOracleParity_ADCvsDecodedL2Squared verifies that PQEncoder.ADCDistance and
// PQEncoder.ADCDistanceBatch produce identical squared L2 distances to an explicit
// Decode() followed by a 64-bit float64 L2-squared oracle calculation.
func TestPQOracleParity_ADCvsDecodedL2Squared(t *testing.T) {
	dims := 32
	m := 8
	k := 256
	enc, err := NewPQEncoder(dims, m, k)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(42)) // #nosec G404
	for sub := 0; sub < m; sub++ {
		for idx := range enc.Codebooks[sub] {
			enc.Codebooks[sub][idx] = float32(rng.Float64()*2 - 1)
		}
	}

	query := make([]float32, dims)
	for i := range query {
		query[i] = float32(rng.Float64()*2 - 1)
	}

	table, err := enc.BuildADCTable(query)
	require.NoError(t, err)

	numVecs := 16
	flatCodes := make([]byte, numVecs*m)
	oracleDists := make([]float64, numVecs)

	for v := 0; v < numVecs; v++ {
		vec := make([]float32, dims)
		for i := range vec {
			vec[i] = float32(rng.Float64()*2 - 1)
		}
		code, err := enc.Encode(vec)
		require.NoError(t, err)
		copy(flatCodes[v*m:(v+1)*m], code)

		decoded, err := enc.Decode(code)
		require.NoError(t, err)

		var gtSq float64
		for i := 0; i < dims; i++ {
			diff := float64(query[i]) - float64(decoded[i])
			gtSq += diff * diff
		}
		oracleDists[v] = gtSq

		singleDist, err := enc.ADCDistance(table, code)
		require.NoError(t, err)
		assert.InDelta(t, gtSq, singleDist, gtSq*1e-5+1e-4, "ADCDistance vs Decode+L2Squared oracle v=%d", v)
	}

	batchResults := make([]float32, numVecs)
	require.NoError(t, enc.ADCDistanceBatch(table, flatCodes, batchResults))
	for v := 0; v < numVecs; v++ {
		wantEuc := math.Sqrt(oracleDists[v])
		assert.InDelta(t, wantEuc, batchResults[v], wantEuc*1e-5+1e-4, "ADCDistanceBatch vs Decode+L2 oracle v=%d", v)
	}
}

// FuzzPQEncodeDecodeADCOracleParity fuzzes PQ encoding, decoding, and ADC lookup
// against a float64 L2-squared ground-truth oracle.
func FuzzPQEncodeDecodeADCOracleParity(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	f.Add([]byte{255, 128, 64, 32, 16, 8, 4, 2, 1, 0, 100, 200, 50, 25, 12, 6})

	dims := 16
	m := 4
	k := 256
	enc, _ := NewPQEncoder(dims, m, k)
	rng := rand.New(rand.NewSource(101)) // #nosec G404
	for sub := 0; sub < m; sub++ {
		for idx := range enc.Codebooks[sub] {
			enc.Codebooks[sub][idx] = float32(rng.Float64()*4 - 2)
		}
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < dims {
			return
		}
		query := make([]float32, dims)
		vec := make([]float32, dims)
		for i := 0; i < dims; i++ {
			query[i] = (float32(raw[i%len(raw)]) - 128.0) / 64.0
			vec[i] = (float32(raw[(i+5)%len(raw)]) - 128.0) / 64.0
		}

		code, err := enc.Encode(vec)
		if err != nil {
			t.Fatalf("Encode error: %v", err)
		}
		decoded, err := enc.Decode(code)
		if err != nil {
			t.Fatalf("Decode error: %v", err)
		}
		table, err := enc.BuildADCTable(query)
		if err != nil {
			t.Fatalf("BuildADCTable error: %v", err)
		}
		adcDist, err := enc.ADCDistance(table, code)
		if err != nil {
			t.Fatalf("ADCDistance error: %v", err)
		}

		var gtSq float64
		for i := 0; i < dims; i++ {
			diff := float64(query[i]) - float64(decoded[i])
			gtSq += diff * diff
		}
		if math.Abs(float64(adcDist)-gtSq) > gtSq*1e-4+1e-3 {
			t.Fatalf("ADC distance %v != decoded L2Sq oracle %v", adcDist, gtSq)
		}
	})
}
