package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeVec constructs a float32 slice of length n.
func makeVec(n int) []float32 {
	v := make([]float32, n)
	for i := range v {
		v[i] = float32(i) * 0.1
	}
	return v
}

// TestAutoDimension_FirstVectorSets verifies that a dim=0 guard locks its
// dimension to the size of the first inserted vector.
func TestAutoDimension_FirstVectorSets(t *testing.T) {
	g := NewDimensionGuard("embeddings", 0)
	require.Equal(t, 0, g.Dim(), "pre-condition: guard must start unlocked")

	require.NoError(t, g.CheckOrSet(makeVec(128)))
	assert.Equal(t, 128, g.Dim(), "dimension must be locked to 128 after first insert")
	assert.True(t, g.IsAutoDetected(), "auto-detection flag must be true")
}

// TestAutoDimension_SecondVectorMatchesDim verifies that a second vector with
// the same dimension is accepted without error.
func TestAutoDimension_SecondVectorMatchesDim(t *testing.T) {
	g := NewDimensionGuard("embeddings", 0)
	require.NoError(t, g.CheckOrSet(makeVec(128)))
	assert.NoError(t, g.CheckOrSet(makeVec(128)), "same-dimension second vector must be accepted")
}

// TestAutoDimension_MismatchError verifies that a second vector with a
// different dimension returns ErrDimensionLocked.
func TestAutoDimension_MismatchError(t *testing.T) {
	g := NewDimensionGuard("embeddings", 0)
	require.NoError(t, g.CheckOrSet(makeVec(128)))

	err := g.CheckOrSet(makeVec(256))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDimensionLocked), "mismatch error must wrap ErrDimensionLocked")
}

// TestDimensionMismatchErrorMessage verifies the error message contains both
// the expected and received dimensions.
func TestDimensionMismatchErrorMessage(t *testing.T) {
	g := NewDimensionGuard("embeddings", 768)
	err := g.CheckOrSet(makeVec(4))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "768", "message must include expected dimension")
	assert.Contains(t, err.Error(), "4", "message must include received dimension")
}

// TestDimensionMismatchErrorMessage_WithDatasetName verifies the error message
// includes the dataset name for easy triage.
func TestDimensionMismatchErrorMessage_WithDatasetName(t *testing.T) {
	const ds = "production_embeddings"
	g := NewDimensionGuard(ds, 512)
	err := g.CheckOrSet(makeVec(2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), ds, "message must include dataset name")
}

// TestCreateDataset_ExplicitDimension verifies that a guard created with an
// explicit dimension does NOT auto-detect (IsAutoDetected must be false).
func TestCreateDataset_ExplicitDimension(t *testing.T) {
	g := NewDimensionGuard("embeddings", 384)
	require.NoError(t, g.CheckOrSet(makeVec(384)))
	assert.Equal(t, 384, g.Dim())
	assert.False(t, g.IsAutoDetected(), "explicit dimension must not set auto-detected flag")
}
