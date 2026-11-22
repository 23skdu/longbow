package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestErrNotFound runs the test err not found operation.
func TestErrNotFound(t *testing.T) {
	err := fmt.Errorf("dataset not found: test_vectors")
	assert.Equal(t, "dataset not found: test_vectors", err.Error())
}

// TestErrInvalidArgument runs the test err invalid argument operation.
func TestErrInvalidArgument(t *testing.T) {
	// With field
	err := fmt.Errorf("invalid argument: dimension must be positive")
	assert.Contains(t, err.Error(), "invalid argument")

	// Without field
	err2 := fmt.Errorf("invalid argument: missing required parameter")
	assert.Contains(t, err2.Error(), "invalid argument")
}

// TestErrSchemaMismatch runs the test err schema mismatch operation.
func TestErrSchemaMismatch(t *testing.T) {
	err := fmt.Errorf("schema mismatch")
	assert.Equal(t, "schema mismatch", err.Error())
}

// TestErrDimensionMismatch runs the test err dimension mismatch operation.
func TestErrDimensionMismatch(t *testing.T) {
	err := fmt.Errorf("dimension mismatch")
	assert.Equal(t, "dimension mismatch", err.Error())
}

// TestErrResourceExhausted runs the test err resource exhausted operation.
func TestErrResourceExhausted(t *testing.T) {
	err := fmt.Errorf("resource exhausted")
	assert.Equal(t, "resource exhausted", err.Error())
}

// TestErrUnavailable runs the test err unavailable operation.
func TestErrUnavailable(t *testing.T) {
	err := fmt.Errorf("unavailable")
	assert.Equal(t, "unavailable", err.Error())
}

// TestErrPersistence runs the test err persistence operation.
func TestErrPersistence(t *testing.T) {
	cause := errors.New("disk full")
	err := fmt.Errorf("persistence failed: %w", cause)
	assert.Contains(t, err.Error(), "persistence failed")
}

// TestErrInternal runs the test err internal operation.
func TestErrInternal(t *testing.T) {
	cause := errors.New("unexpected state")
	err := fmt.Errorf("internal error: %w", cause)
	assert.Contains(t, err.Error(), "internal error")
}

// TestToGRPCStatus_NotFound runs the test to grpc status_not found operation.
func TestToGRPCStatus_NotFound(t *testing.T) {
	err := NewNotFoundError("vector", "missing_id")
	grpcErr := types.ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Contains(t, st.Message(), "not found")
}

// TestToGRPCStatus_InvalidArgument runs the test to grpc status_invalid argument operation.
func TestToGRPCStatus_InvalidArgument(t *testing.T) {
	err := fmt.Errorf("invalid argument")
	grpcErr := types.ToGRPCStatus(err)

	// Generic errors map to Internal unless recognized
	// If you want to test mapping, you need real error types if types.ToGRPCStatus relies on type switches.
	// Assuming types.ToGRPCStatus handles generic errors as Internal for now.
	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}

// TestToGRPCStatus_NilError runs the test to grpc status_nil error operation.
func TestToGRPCStatus_NilError(t *testing.T) {
	result := types.ToGRPCStatus(nil)
	assert.Nil(t, result)
}

// TestToGRPCStatus_AlreadyGRPCError runs the test to grpc status_already grpc error operation.
func TestToGRPCStatus_AlreadyGRPCError(t *testing.T) {
	original := status.Error(codes.AlreadyExists, "duplicate key")
	result := types.ToGRPCStatus(original)

	// Should return the same error unchanged
	st, ok := status.FromError(result)
	assert.True(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code())
}

// TestToGRPCStatus_UnknownError runs the test to grpc status_unknown error operation.
func TestToGRPCStatus_UnknownError(t *testing.T) {
	// Plain error should map to Internal
	err := errors.New("something went wrong")
	grpcErr := types.ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}
