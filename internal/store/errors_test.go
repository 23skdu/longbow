package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrNotFound(t *testing.T) {
	err := NewNotFoundError("dataset", "test_vectors")
	assert.Equal(t, "dataset not found: test_vectors", err.Error())

	// Verify type assertion works
	var notFoundErr *ErrNotFound
	assert.True(t, errors.As(err, &notFoundErr))
	assert.Equal(t, "dataset", notFoundErr.Resource)
	assert.Equal(t, "test_vectors", notFoundErr.Name)
}

func TestErrInvalidArgument(t *testing.T) {
	// With field
	err := NewInvalidArgumentError("vector", "dimension must be positive")
	assert.Equal(t, "invalid argument for vector: dimension must be positive", err.Error())

	// Without field
	err2 := &ErrInvalidArgument{Message: "missing required parameter"}
	assert.Equal(t, "invalid argument: missing required parameter", err2.Error())
}

func TestErrSchemaMismatch(t *testing.T) {
	err := NewSchemaMismatchError("embeddings", "column count differs")
	assert.Equal(t, "schema mismatch for dataset 'embeddings': column count differs", err.Error())
}

func TestErrDimensionMismatch(t *testing.T) {
	err := NewDimensionMismatchError("vectors", 768, 512)
	assert.Equal(t, "dimension mismatch for dataset 'vectors': expected 768, got 512", err.Error())
}

func TestErrResourceExhausted(t *testing.T) {
	err := NewResourceExhaustedError("memory", "limit exceeded")
	assert.Equal(t, "resource exhausted (memory): limit exceeded", err.Error())
}

func TestErrUnavailable(t *testing.T) {
	err := NewUnavailableError("query", "snapshot in progress")
	assert.Equal(t, "service unavailable for query: snapshot in progress", err.Error())
}

func TestErrPersistence(t *testing.T) {
	cause := errors.New("disk full")
	err := NewPersistenceError("snapshot", cause)
	assert.Equal(t, "persistence failed during snapshot: disk full", err.Error())

	// Verify unwrap works
	assert.True(t, errors.Is(err, cause))
}

func TestErrInternal(t *testing.T) {
	cause := errors.New("unexpected state")
	err := NewInternalError("index rebuild", cause)
	assert.Equal(t, "internal error during index rebuild: unexpected state", err.Error())

	// Without cause
	err2 := &ErrInternal{Operation: "cleanup"}
	assert.Equal(t, "internal error during cleanup", err2.Error())
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
	err := NewInvalidArgumentError("query", "empty string")
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestToGRPCStatus_SchemaMismatch(t *testing.T) {
	err := NewSchemaMismatchError("data", "incompatible types")
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

func TestToGRPCStatus_DimensionMismatch(t *testing.T) {
	err := NewDimensionMismatchError("embeddings", 1536, 768)
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "dimension mismatch")
}

func TestToGRPCStatus_ResourceExhausted(t *testing.T) {
	err := NewResourceExhaustedError("memory", "100MB limit exceeded")
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
}

func TestToGRPCStatus_Unavailable(t *testing.T) {
	err := NewUnavailableError("write", "snapshot in progress")
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.Unavailable, st.Code())
}

func TestToGRPCStatus_Persistence(t *testing.T) {
	err := NewPersistenceError("WAL write", errors.New("I/O error"))
	grpcErr := ToGRPCStatus(err)

	st, ok := status.FromError(grpcErr)
	assert.True(t, ok)
	assert.Equal(t, codes.Unavailable, st.Code())
}

func TestToGRPCStatus_Internal(t *testing.T) {
	err := NewInternalError("search", errors.New("nil pointer"))
	grpcErr := ToGRPCStatus(err)

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
