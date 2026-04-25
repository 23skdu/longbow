package pq

import (
	"errors"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"gonum.org/v1/gonum/mat"
	"bytes"
	"encoding/gob"
)

// OPQEncoder implements Optimized Product Quantization (OPQ).
// It wraps a PQEncoder and adds a rotation matrix (orthogonal transformation)
// to minimize the quantization error.
type OPQEncoder struct {
	*PQEncoder
	RotationMatrix *mat.Dense // Dims * Dims rotation matrix
}

// NewOPQEncoder creates a new OPQ encoder.
func NewOPQEncoder(dims, m, k int) (*OPQEncoder, error) {
	pq, err := NewPQEncoder(dims, m, k)
	if err != nil {
		return nil, err
	}
	// Initialize with identity matrix
	rot := mat.NewDense(dims, dims, nil)
	for i := 0; i < dims; i++ {
		rot.Set(i, i, 1.0)
	}
	return &OPQEncoder{
		PQEncoder:      pq,
		RotationMatrix: rot,
	}, nil
}

// TrainOPQ trains the OPQ rotation matrix and PQ codebooks.
// This is an iterative process (Ge et al., 2013).
func (e *OPQEncoder) TrainOPQ(vectors [][]float32, iterations int) error {
	if len(vectors) == 0 {
		return errors.New("empty training data")
	}

	start := time.Now()
	n := len(vectors)
	d := e.Dims

	// 1. Prepare data matrix X (N x D)
	xData := make([]float64, n*d)
	for i := 0; i < n; i++ {
		for j := 0; j < d; j++ {
			xData[i*d+j] = float64(vectors[i][j])
		}
	}
	X := mat.NewDense(n, d, xData)

	// 2. Iterative optimization
	for iter := 0; iter < iterations; iter++ {
		// A. Rotate vectors using current RotationMatrix: X_rot = X * R^T
		// Note: R is DxD, X is NxD. X * R^T gives NxD.
		var Xrot mat.Dense
		Xrot.Mul(X, e.RotationMatrix.T())

		// Convert back to float32 for PQ training
		rotatedVectors := make([][]float32, n)
		for i := 0; i < n; i++ {
			rotatedVectors[i] = make([]float32, d)
			for j := 0; j < d; j++ {
				rotatedVectors[i][j] = float32(Xrot.At(i, j))
			}
		}

		// B. Train PQ codebooks on rotated vectors
		if err := e.PQEncoder.Train(rotatedVectors); err != nil {
			return err
		}

		// C. Update RotationMatrix using SVD
		// Compute reconstruction Y_rot (N x D)
		yRotData := make([]float64, n*d)
		var totalError float64
		for i := 0; i < n; i++ {
			codes, _ := e.PQEncoder.Encode(rotatedVectors[i])
			recon, _ := e.PQEncoder.Decode(codes)
			for j := 0; j < d; j++ {
				val := float64(recon[j])
				yRotData[i*d+j] = val
				diff := val - float64(rotatedVectors[i][j])
				totalError += diff * diff
			}
		}
		Yrot := mat.NewDense(n, d, yRotData)

		// Compute M = X^T * Yrot (D x D)
		var M mat.Dense
		M.Mul(X.T(), Yrot)

		// SVD: M = U * S * V^T
		var svd mat.SVD
		if ok := svd.Factorize(&M, mat.SVDFull); !ok {
			return errors.New("SVD factorization failed")
		}
		var U, V mat.Dense
		svd.UTo(&U)
		svd.VTo(&V)

		// New R = V * U^T
		var newR mat.Dense
		newR.Mul(&V, U.T())
		e.RotationMatrix = &newR

		// Update metrics
		metrics.VQReconstructionError.WithLabelValues("opq").Set(totalError / float64(n*d))
	}

	metrics.VQTrainingDurationSeconds.WithLabelValues("opq").Observe(time.Since(start).Seconds())
	return nil
}

// RotateVector rotates a single vector using the trained rotation matrix.
func (e *OPQEncoder) RotateVector(vector []float32) []float32 {
	if len(vector) != e.Dims {
		return nil
	}
	v := mat.NewVecDense(e.Dims, nil)
	for i, val := range vector {
		v.SetVec(i, float64(val))
	}

	var vRot mat.VecDense
	vRot.MulVec(e.RotationMatrix, v)

	rotated := make([]float32, e.Dims)
	for i := 0; i < e.Dims; i++ {
		rotated[i] = float32(vRot.AtVec(i))
	}
	return rotated
}

// Encode overrides PQEncoder.Encode to include the rotation step.
func (e *OPQEncoder) Encode(vector []float32) ([]byte, error) {
	if len(vector) != e.Dims {
		return nil, errors.New("vector dimension mismatch")
	}

	// 1. Rotate vector: v_rot = R * v
	v := mat.NewVecDense(e.Dims, nil)
	for i, val := range vector {
		v.SetVec(i, float64(val))
	}

	var vRot mat.VecDense
	vRot.MulVec(e.RotationMatrix, v)

	// Convert back to float32
	rotated := make([]float32, e.Dims)
	for i := 0; i < e.Dims; i++ {
		rotated[i] = float32(vRot.AtVec(i))
	}

	// 2. Delegate to PQEncoder
	return e.PQEncoder.Encode(rotated)
}

// Decode overrides PQEncoder.Decode to include the inverse rotation.
func (e *OPQEncoder) Decode(codes []byte) ([]float32, error) {
	// 1. Reconstruct in rotated space
	rotated, err := e.PQEncoder.Decode(codes)
	if err != nil {
		return nil, err
	}

	// 2. Inverse rotate: v = R^T * v_rot (since R is orthogonal, R^-1 = R^T)
	vRot := mat.NewVecDense(e.Dims, nil)
	for i, val := range rotated {
		vRot.SetVec(i, float64(val))
	}

	var v mat.VecDense
	v.MulVec(e.RotationMatrix.T(), vRot)

	// Convert back to float32
	original := make([]float32, e.Dims)
	for i := 0; i < e.Dims; i++ {
		original[i] = float32(v.AtVec(i))
	}

	return original, nil
}

type opqState struct {
	PQState  []byte
	Rotation []byte
}

func (e *OPQEncoder) ExportState() ([]byte, error) {
	pqData, err := e.PQEncoder.ExportState()
	if err != nil {
		return nil, err
	}
	rotData, err := e.RotationMatrix.MarshalBinary()
	if err != nil {
		return nil, err
	}
	state := opqState{PQState: pqData, Rotation: rotData}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (e *OPQEncoder) ImportState(data []byte) error {
	var state opqState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		return err
	}
	if err := e.PQEncoder.ImportState(state.PQState); err != nil {
		return err
	}
	return e.RotationMatrix.UnmarshalBinary(state.Rotation)
}
