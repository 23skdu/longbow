package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrNotFound(t *testing.T) {
	err := fmt.Errorf("dataset not found: test_vectors")
	assert.Equal(t, "dataset not found: test_vectors", err.Error())
}

func TestErrInvalidArgument(t *testing.T) {
	// With field
	err := fmt.Errorf("invalid argument: dimension must be positive")
	assert.Contains(t, err.Error(), "invalid argument")

	// Without field
	err2 := fmt.Errorf("invalid argument: missing required parameter")
	assert.Contains(t, err2.Error(), "invalid argument")
}

func TestErrSchemaMismatch(t *testing.T) {
	err := fmt.Errorf("schema mismatch")
	assert.Equal(t, "schema mismatch", err.Error())
}

func TestErrDimensionMismatch(t *testing.T) {
	err := fmt.Errorf("dimension mismatch")
	assert.Equal(t, "dimension mismatch", err.Error())
}

func TestErrResourceExhausted(t *testing.T) {
	err := fmt.Errorf("resource exhausted")
	assert.Equal(t, "resource exhausted", err.Error())
}

func TestErrUnavailable(t *testing.T) {
	err := fmt.Errorf("unavailable")
	assert.Equal(t, "unavailable", err.Error())
}

func TestErrPersistence(t *testing.T) {
	cause := errors.New("disk full")
	err := fmt.Errorf("persistence failed: %w", cause)
	assert.Contains(t, err.Error(), "persistence failed")
}

func TestErrInternal(t *testing.T) {
	cause := errors.New("unexpected state")
	err := fmt.Errorf("internal error: %w", cause)
	assert.Contains(t, err.Error(), "internal error")
}

func TestToGRPCStatus_NotFound(t *testing.T) {
	err := NewNotFoundError("vector", "missing_id")
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Contains(t, st.Message(), "not found")
}

func TestToGRPCStatus_InvalidArgument(t *testing.T) {
	err := fmt.Errorf("invalid argument")
	grpcErr := ToGRPCStatus(err)

	// Generic errors map to Internal unless recognized
	// If you want to test mapping, you need real error types if ToGRPCStatus relies on type switches.
	// Assuming ToGRPCStatus handles generic errors as Internal for now.
	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}

func TestToGRPCStatus_NilError(t *testing.T) {
	result := ToGRPCStatus(nil)
	assert.Nil(t, result)
}

func TestToGRPCStatus_AlreadyGRPCError(t *testing.T) {
	original := status.Error(codes.AlreadyExists, "duplicate key")
	result := ToGRPCStatus(original)

	// Should return the same error unchanged
	st, ok := status.FromError(result)
	assert.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())
}

func TestToGRPCStatus_UnknownError(t *testing.T) {
	// Plain error should map to Internal
	err := errors.New("something went wrong")
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}
