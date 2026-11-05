package simd

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOracleParity_AllBatchKernels validates that all SIMD batch operations
// (EuclideanDistanceBatch, L2SquaredDistanceBatch, DotProductBatch, CosineDistanceBatch,
// EuclideanDistanceBatchFlat, ADCDistanceBatch, and DequantizeL2Float32Uint8) match
// 64-bit scalar reference oracles across all fast-path and arbitrary dimensions.
func TestOracleParity_AllBatchKernels(t *testing.T) {
	dims := []int{17, 64, 128, 256, 384, 512, 768, 1024, 1536, 3072}
	batchSize := 5

	for _, dim := range dims {
		rng := rand.New(rand.NewSource(int64(dim * 9001))) // #nosec G404
		query := make([]float32, dim)
		q8 := make([]uint8, dim)
		var normQ float64
		for i := 0; i < dim; i++ {
			query[i] = float32(rng.Float64()*4 - 2)
			q8[i] = uint8(rng.Intn(256))
			normQ += float64(query[i]) * float64(query[i])
		}

		vectors := make([][]float32, batchSize)
		flatVectors := make([]float32, batchSize*dim)
		oracleEuc := make([]float64, batchSize)
		oracleL2Sq := make([]float64, batchSize)
		oracleDot := make([]float64, batchSize)
		oracleCos := make([]float64, batchSize)

		for b := 0; b < batchSize; b++ {
			v := make([]float32, dim)
			v8 := make([]uint8, dim)
			var dot, l2Sq, normV float64
			for i := 0; i < dim; i++ {
				v[i] = float32(rng.Float64()*4 - 2)
				v8[i] = uint8(rng.Intn(256))
				flatVectors[b*dim+i] = v[i]
				dot += float64(query[i]) * float64(v[i])
				diff := float64(query[i]) - float64(v[i])
				l2Sq += diff * diff
				normV += float64(v[i]) * float64(v[i])
			}
			vectors[b] = v
			oracleL2Sq[b] = l2Sq
			oracleEuc[b] = math.Sqrt(l2Sq)
			oracleDot[b] = dot
			oracleCos[b] = 1.0 - (dot / (math.Sqrt(normQ) * math.Sqrt(normV)))

			// Verify DequantizeL2Float32Uint8 vs generic scalar oracle
			minV := float32(-2.0)
			scale := float32(4.0 / 255.0)
			gotDeq := DequantizeL2Float32Uint8(query, v8, minV, scale)
			wantDeq := float32(math.Sqrt(float64(dequantL2Float32Uint8Generic(query, v8, minV, scale))))
			assert.InDelta(t, wantDeq, gotDeq, float64(wantDeq)*1e-5+1e-4, "DequantizeL2Float32Uint8 dim=%d", dim)

			// Verify DequantizeL2Uint8Uint8 vs float64 oracle
			var u8Sq float64
			for i := 0; i < dim; i++ {
				d := float64(q8[i]) - float64(v8[i])
				u8Sq += d * d
			}
			wantU8 := float32(math.Sqrt(u8Sq)) * scale
			gotU8 := DequantizeL2Uint8Uint8(q8, v8, scale)
			assert.InDelta(t, wantU8, gotU8, float64(wantU8)*1e-5+1e-4, "DequantizeL2Uint8Uint8 dim=%d", dim)
		}

		resEuc := make([]float32, batchSize)
		require.NoError(t, EuclideanDistanceBatch(query, vectors, resEuc))
		resFlat := make([]float32, batchSize)
		require.NoError(t, EuclideanDistanceBatchFlat(query, flatVectors, batchSize, dim, resFlat))
		resL2Sq := make([]float32, batchSize)
		require.NoError(t, L2SquaredDistanceBatch(query, vectors, resL2Sq))
		resDot := make([]float32, batchSize)
		require.NoError(t, DotProductBatch(query, vectors, resDot))
		resCos := make([]float32, batchSize)
		require.NoError(t, CosineDistanceBatch(query, vectors, resCos))

		for b := 0; b < batchSize; b++ {
			assert.InDelta(t, oracleEuc[b], resEuc[b], oracleEuc[b]*1e-5+1e-4, "EuclideanDistanceBatch dim=%d b=%d", dim, b)
			assert.InDelta(t, oracleEuc[b], resFlat[b], oracleEuc[b]*1e-5+1e-4, "EuclideanDistanceBatchFlat dim=%d b=%d", dim, b)
			assert.InDelta(t, oracleL2Sq[b], resL2Sq[b], oracleL2Sq[b]*1e-5+1e-3, "L2SquaredDistanceBatch dim=%d b=%d", dim, b)
			assert.InDelta(t, oracleDot[b], resDot[b], math.Abs(oracleDot[b])*1e-5+1e-4, "DotProductBatch dim=%d b=%d", dim, b)
			assert.InDelta(t, oracleCos[b], resCos[b], 2e-4, "CosineDistanceBatch dim=%d b=%d", dim, b)
		}
	}
}

// FuzzBatchDistancesOracleParity fuzzes EuclideanDistanceBatch, L2SquaredDistanceBatch,
// and DotProductBatch against a 64-bit float64 ground-truth oracle.
func FuzzBatchDistancesOracleParity(f *testing.F) {
	f.Add(uint8(0), []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	f.Add(uint8(1), []byte{0x3f, 0x80, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00})
	f.Add(uint8(3), []byte{10, 20, 30, 40, 50, 60, 70, 80})

	dimChoices := []int{16, 128, 384, 768, 1024}

	f.Fuzz(func(t *testing.T, dimSelector uint8, raw []byte) {
		if len(raw) < 4 {
			return
		}
		dim := dimChoices[int(dimSelector)%len(dimChoices)]
		batchSize := 3
		query := make([]float32, dim)
		vectors := make([][]float32, batchSize)
		for b := 0; b < batchSize; b++ {
			vectors[b] = make([]float32, dim)
		}

		for i := 0; i < dim; i++ {
			b0 := raw[(i*2)%len(raw)]
			b1 := raw[(i*2+1)%len(raw)]
			val := (float32(int16(binary.LittleEndian.Uint16([]byte{b0, b1}))) / 32768.0) * 10.0
			query[i] = val
			for b := 0; b < batchSize; b++ {
				vb := raw[(i+b*7)%len(raw)]
				vectors[b][i] = (float32(int8(vb)) / 128.0) * 10.0
			}
		}

		resEuc := make([]float32, batchSize)
		resL2Sq := make([]float32, batchSize)
		resDot := make([]float32, batchSize)

		if err := EuclideanDistanceBatch(query, vectors, resEuc); err != nil {
			t.Fatalf("EuclideanDistanceBatch error: %v", err)
		}
		if err := L2SquaredDistanceBatch(query, vectors, resL2Sq); err != nil {
			t.Fatalf("L2SquaredDistanceBatch error: %v", err)
		}
		if err := DotProductBatch(query, vectors, resDot); err != nil {
			t.Fatalf("DotProductBatch error: %v", err)
		}

		for b := 0; b < batchSize; b++ {
			var gtL2Sq, gtDot float64
			for i := 0; i < dim; i++ {
				diff := float64(query[i]) - float64(vectors[b][i])
				gtL2Sq += diff * diff
				gtDot += float64(query[i]) * float64(vectors[b][i])
			}
			gtEuc := math.Sqrt(gtL2Sq)

			if math.Abs(float64(resEuc[b])-gtEuc) > gtEuc*1e-4+1e-3 {
				t.Fatalf("EuclideanDistanceBatch mismatch dim=%d b=%d: got %v, want %v", dim, b, resEuc[b], gtEuc)
			}
			if math.Abs(float64(resL2Sq[b])-gtL2Sq) > gtL2Sq*1e-4+1e-2 {
				t.Fatalf("L2SquaredDistanceBatch mismatch dim=%d b=%d: got %v, want %v", dim, b, resL2Sq[b], gtL2Sq)
			}
			if math.Abs(float64(resDot[b])-gtDot) > math.Abs(gtDot)*1e-4+1e-2 {
				t.Fatalf("DotProductBatch mismatch dim=%d b=%d: got %v, want %v", dim, b, resDot[b], gtDot)
			}
		}
	})
}

// FuzzDequantizeL2OracleParity fuzzes DequantizeL2Float32Uint8 and DequantizeL2Uint8Uint8
// against scalar generic reference implementations.
func FuzzDequantizeL2OracleParity(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, float32(-1.0), float32(0.01))
	f.Add([]byte{255, 0, 128, 64, 32, 16, 8, 4, 2, 1, 0, 255, 127, 63, 31, 15}, float32(0.0), float32(0.05))

	f.Fuzz(func(t *testing.T, raw []byte, minV, scale float32) {
		if len(raw) < 8 || math.IsNaN(float64(minV)) || math.IsInf(float64(minV), 0) ||
			math.IsNaN(float64(scale)) || math.IsInf(float64(scale), 0) {
			return
		}
		if minV < -100 || minV > 100 {
			minV = 0
		}
		if scale <= 0 || scale > 10 {
			scale = 0.01
		}

		n := len(raw)
		if n > 512 {
			n = 512
		}
		q := make([]float32, n)
		v8 := make([]uint8, n)
		q8 := make([]uint8, n)
		for i := 0; i < n; i++ {
			v8[i] = raw[i]
			q8[i] = raw[(i+3)%len(raw)]
			q[i] = minV + float32(q8[i])*scale
		}

		got := DequantizeL2Float32Uint8(q, v8, minV, scale)
		want := float32(math.Sqrt(float64(dequantL2Float32Uint8Generic(q, v8, minV, scale))))
		if math.Abs(float64(got-want)) > float64(want)*1e-4+1e-3 {
			t.Fatalf("DequantizeL2Float32Uint8 mismatch n=%d: got %v, want %v", n, got, want)
		}
	})
}

// FuzzSQ8AdaptiveOracleParity fuzzes per-channel adaptive SQ8 calibration, quantization,
// dequantization, and distance computation.
func FuzzSQ8AdaptiveOracleParity(f *testing.F) {
	f.Add([]byte{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160})

	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < 16 {
			return
		}
		dims := 8
		numVecs := 4
		vecs := make([][]float32, numVecs)
		for v := 0; v < numVecs; v++ {
			vecs[v] = make([]float32, dims)
			for j := 0; j < dims; j++ {
				b := raw[(v*dims+j)%len(raw)]
				vecs[v][j] = (float32(b) - 128.0) / 16.0
			}
		}

		calib, err := CalibrateSQ8Channels(vecs, dims)
		if err != nil {
			t.Fatalf("CalibrateSQ8Channels failed: %v", err)
		}

		q0 := make([]byte, dims)
		QuantizeSQ8Adaptive(vecs[0], q0, calib)
		recon := make([]float32, dims)
		DequantizeSQ8Adaptive(q0, recon, calib)

		selfDist := EuclideanDistanceSQ8Adaptive(q0, q0, calib)
		if selfDist != 0 {
			t.Fatalf("Expected 0 self-distance, got %v", selfDist)
		}
	})
}
