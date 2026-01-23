package errors

import (
	"fmt"
	"runtime"
)

// Error types for different categories of failures
type ErrorType string

const (
	ErrorTypeValidation    ErrorType = "validation"
	ErrorTypeStorage       ErrorType = "storage"
	ErrorTypeNetwork       ErrorType = "network"
	ErrorTypeComputation   ErrorType = "computation"
	ErrorTypeConfiguration ErrorType = "configuration"
	ErrorTypeTimeout       ErrorType = "timeout"
)

// StructuredError provides rich error context
type StructuredError struct {
	Type      ErrorType
	Operation string
	Message   string
	Cause     error
	Context   map[string]interface{}
	Stack     []uintptr
}

// Error implements the error interface
func (e *StructuredError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %s: %v", e.Type, e.Operation, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s: %s", e.Type, e.Operation, e.Message)
}

// Unwrap returns the underlying cause
func (e *StructuredError) Unwrap() error {
	return e.Cause
}

// New creates a new structured error
func New(errType ErrorType, operation, message string) *StructuredError {
	return &StructuredError{
		Type:      errType,
		Operation: operation,
		Message:   message,
		Context:   make(map[string]interface{}),
		Stack:     captureStack(),
	}
}

// Wrap wraps an existing error with additional context
func Wrap(err error, errType ErrorType, operation, message string) *StructuredError {
	if err == nil {
		return nil
	}

	se := &StructuredError{
		Type:      errType,
		Operation: operation,
		Message:   message,
		Cause:     err,
		Context:   make(map[string]interface{}),
		Stack:     captureStack(),
	}

	return se
}

// WithContext adds context information to an error
func (e *StructuredError) WithContext(key string, value interface{}) *StructuredError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// captureStack captures the current stack trace
func captureStack() []uintptr {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(2, pcs[:]) // Skip this function and caller
	return pcs[:n]
}

// Common error constructors for frequent use cases

// NewValidationError creates a validation error
func NewValidationError(operation, message string) *StructuredError {
	return New(ErrorTypeValidation, operation, message)
}

// NewStorageError creates a storage error
func NewStorageError(operation, message string) *StructuredError {
	return New(ErrorTypeStorage, operation, message)
}

// NewNetworkError creates a network error
func NewNetworkError(operation, message string) *StructuredError {
	return New(ErrorTypeNetwork, operation, message)
}

// NewComputationError creates a computation error
func NewComputationError(operation, message string) *StructuredError {
	return New(ErrorTypeComputation, operation, message)
}

// NewConfigurationError creates a configuration error
func NewConfigurationError(operation, message string) *StructuredError {
	return New(ErrorTypeConfiguration, operation, message)
}

// NewTimeoutError creates a timeout error
func NewTimeoutError(operation, message string) *StructuredError {
	return New(ErrorTypeTimeout, operation, message)
}

// WrapValidationError wraps an error as a validation error
func WrapValidationError(err error, operation, message string) *StructuredError {
	return Wrap(err, ErrorTypeValidation, operation, message)
}

// WrapStorageError wraps an error as a storage error
func WrapStorageError(err error, operation, message string) *StructuredError {
	return Wrap(err, ErrorTypeStorage, operation, message)
}

// WrapNetworkError wraps an error as a network error
func WrapNetworkError(err error, operation, message string) *StructuredError {
	return Wrap(err, ErrorTypeNetwork, operation, message)
}

// WrapComputationError wraps an error as a computation error
func WrapComputationError(err error, operation, message string) *StructuredError {
	return Wrap(err, ErrorTypeComputation, operation, message)
}

// WrapConfigurationError wraps an error as a configuration error
func WrapConfigurationError(err error, operation, message string) *StructuredError {
	return Wrap(err, ErrorTypeConfiguration, operation, message)
}

// WrapTimeoutError wraps an error as a timeout error
func WrapTimeoutError(err error, operation, message string) *StructuredError {
	return Wrap(err, ErrorTypeTimeout, operation, message)
}
