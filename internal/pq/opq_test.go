package pq

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOPQ_Basic(t *testing.T) {
	dims := 16
	m := 4
	k := 256
	n := 1000

	// Generate random vectors
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	encoder, err := NewOPQEncoder(dims, m, k)
	require.NoError(t, err)

	// Train OPQ
	err = encoder.TrainOPQ(vectors, 5) // 5 iterations for test
	require.NoError(t, err)

	// Test encode/decode
	testVec := vectors[0]
	codes, err := encoder.Encode(testVec)
	require.NoError(t, err)
	assert.Equal(t, m, len(codes))

	decoded, err := encoder.Decode(codes)
	require.NoError(t, err)
	assert.Equal(t, dims, len(decoded))

	// Check if reconstruction is reasonably close
	// (PQ is lossy, so we don't expect exact match, but it should be better than random)
	var dist float32
	for i := 0; i < dims; i++ {
		diff := testVec[i] - decoded[i]
		dist += diff * diff
	}
	t.Logf("Reconstruction L2 distance: %f", dist)
}

func TestOPQ_RotationOrthogonality(t *testing.T) {
	dims := 8
	m := 2
	k := 16
	n := 100

	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dims)
		for j := 0; j < dims; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	encoder, err := NewOPQEncoder(dims, m, k)
	require.NoError(t, err)

	err = encoder.TrainOPQ(vectors, 3)
	require.NoError(t, err)

	// Check if RotationMatrix is orthogonal: R * R^T = I
	R := encoder.RotationMatrix
	for i := 0; i < dims; i++ {
		for j := 0; j < dims; j++ {
			var sum float64
			for l := 0; l < dims; l++ {
				sum += R.At(i, l) * R.At(j, l)
			}
			if i == j {
				assert.InDelta(t, 1.0, sum, 1e-6, "Diagonal element should be 1")
			} else {
				assert.InDelta(t, 0.0, sum, 1e-6, "Off-diagonal element should be 0")
			}
		}
	}
}

func TestOPQ_ReconstructionImprovement(t *testing.T) {
	// OPQ should generally perform better than standard PQ on correlated data
	dims := 32
	m := 4
	k := 256
	n := 1000

	// Generate strongly correlated data (dimensions are linear functions of a base value)
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dims)
		base := rand.Float32()
		for j := 0; j < dims; j++ {
			// Higher correlation makes OPQ's rotation more effective
			vectors[i][j] = base*float32(j%4+1) + rand.Float32()*0.05
		}
	}

	// 1. Standard PQ
	pqEncoder, _ := NewPQEncoder(dims, m, k)
	_ = pqEncoder.Train(vectors)

	var pqErr float32
	for i := 0; i < 100; i++ {
		codes, _ := pqEncoder.Encode(vectors[i])
		recon, _ := pqEncoder.Decode(codes)
		for j := 0; j < dims; j++ {
			diff := vectors[i][j] - recon[j]
			pqErr += diff * diff
		}
	}

	// 2. OPQ
	opqEncoder, _ := NewOPQEncoder(dims, m, k)
	_ = opqEncoder.TrainOPQ(vectors, 10)

	var opqErr float32
	for i := 0; i < 100; i++ {
		codes, _ := opqEncoder.Encode(vectors[i])
		recon, _ := opqEncoder.Decode(codes)
		for j := 0; j < dims; j++ {
			diff := vectors[i][j] - recon[j]
			opqErr += diff * diff
		}
	}

	t.Logf("PQ Error: %f, OPQ Error: %f", pqErr, opqErr)
	// On correlated data, OPQ should be significantly better
	assert.Less(t, opqErr, pqErr, "OPQ should have lower reconstruction error than PQ on correlated data")
}
