package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStructuredError_Error(t *testing.T) {
	// Test error without cause
	err := New(ErrorTypeValidation, "test_op", "test message")
	expected := "[validation] test_op: test message"
	assert.Equal(t, expected, err.Error())

	// Test error with cause
	cause := errors.New("underlying error")
	err = Wrap(cause, ErrorTypeStorage, "save_op", "failed to save")
	assert.Contains(t, err.Error(), "[storage] save_op: failed to save")
	assert.Contains(t, err.Error(), "underlying error")
	assert.Equal(t, cause, err.Unwrap())
}

func TestStructuredError_WithContext(t *testing.T) {
	err := New(ErrorTypeValidation, "test_op", "test message")
	err = err.WithContext("user_id", 123).WithContext("dataset", "test_dataset")

	assert.Equal(t, 123, err.Context["user_id"])
	assert.Equal(t, "test_dataset", err.Context["dataset"])
}

func TestErrorConstructors(t *testing.T) {
	// Test New* constructors
	assert.Equal(t, ErrorTypeValidation, NewValidationError("op", "msg").Type)
	assert.Equal(t, ErrorTypeStorage, NewStorageError("op", "msg").Type)
	assert.Equal(t, ErrorTypeNetwork, NewNetworkError("op", "msg").Type)
	assert.Equal(t, ErrorTypeComputation, NewComputationError("op", "msg").Type)
	assert.Equal(t, ErrorTypeConfiguration, NewConfigurationError("op", "msg").Type)
	assert.Equal(t, ErrorTypeTimeout, NewTimeoutError("op", "msg").Type)
}

func TestErrorWrapping(t *testing.T) {
	originalErr := errors.New("original error")

	// Test Wrap* functions
	wrapped := WrapValidationError(originalErr, "validate", "validation failed")
	assert.Equal(t, ErrorTypeValidation, wrapped.Type)
	assert.Equal(t, "validate", wrapped.Operation)
	assert.Equal(t, "validation failed", wrapped.Message)
	assert.Equal(t, originalErr, wrapped.Unwrap())

	// Test that Wrap returns nil for nil error
	assert.Nil(t, Wrap(nil, ErrorTypeStorage, "op", "msg"))
}

func TestErrorTypeString(t *testing.T) {
	assert.Equal(t, "validation", string(ErrorTypeValidation))
	assert.Equal(t, "storage", string(ErrorTypeStorage))
	assert.Equal(t, "network", string(ErrorTypeNetwork))
	assert.Equal(t, "computation", string(ErrorTypeComputation))
	assert.Equal(t, "configuration", string(ErrorTypeConfiguration))
	assert.Equal(t, "timeout", string(ErrorTypeTimeout))
}

func TestStackTraceCapture(t *testing.T) {
	err := New(ErrorTypeValidation, "test", "message")
	// Should have captured some stack frames
	assert.Greater(t, len(err.Stack), 0)
}
