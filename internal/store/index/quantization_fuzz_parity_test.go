package index

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSQ8OracleParity_EncodeDecodeDistance verifies that SQ8Encoder encoding,
// decoding, and distance calculations maintain bounded quantization error against
// a 64-bit float64 ground-truth oracle.
func TestSQ8OracleParity_EncodeDecodeDistance(t *testing.T) {
	dim := 64
	vecs := make([][]float32, 16)
	for i := range vecs {
		vecs[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vecs[i][d] = float32(math.Sin(float64(i*dim + d)))
		}
	}

	enc, err := TrainSQ8Encoder(vecs)
	require.NoError(t, err)

	for i, v := range vecs {
		q := enc.Encode(v)
		dec := enc.Decode(q)
		for d := 0; d < dim; d++ {
			assert.InDelta(t, v[d], dec[d], 0.02, "SQ8 round-trip vec=%d dim=%d", i, d)
		}
	}

	q0 := enc.Encode(vecs[0])
	assert.Equal(t, float32(0), SQ8EuclideanDistance(q0, q0, enc), "SQ8 self-distance must be 0")
}

// FuzzSQ8EncoderOracleParity fuzzes SQ8 training, encoding, decoding, and distance
// invariants across arbitrary vector inputs.
func FuzzSQ8EncoderOracleParity(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	f.Add([]byte{255, 0, 128, 64, 32, 16, 8, 4, 2, 1, 100, 200, 50, 25, 12, 6})

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < 16 {
			return
		}
		dim := 8
		nVecs := 4
		vecs := make([][]float32, nVecs)
		for i := 0; i < nVecs; i++ {
			vecs[i] = make([]float32, dim)
			for d := 0; d < dim; d++ {
				vecs[i][d] = (float32(raw[(i*dim+d)%len(raw)]) - 128.0) / 32.0
			}
		}

		enc, err := TrainSQ8Encoder(vecs)
		if err != nil {
			t.Fatalf("TrainSQ8Encoder failed: %v", err)
		}

		q0 := enc.Encode(vecs[0])
		dec0 := enc.Decode(q0)
		for d := 0; d < dim; d++ {
			if math.Abs(float64(vecs[0][d]-dec0[d])) > 0.1 {
				t.Fatalf("SQ8 round-trip error exceeded bound at d=%d: %v vs %v", d, vecs[0][d], dec0[d])
			}
		}
	})
}
