package core

import "fmt"

// ErrNotFound indicates a requested resource does not exist.
type ErrNotFound struct {
	Resource string
	Name     string
}

// Error runs the error operation.
func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("%s not found: %s", e.Resource, e.Name)
}

// NewNotFoundError creates a not found error.
func NewNotFoundError(resource, name string) error {
	return &ErrNotFound{Resource: resource, Name: name}
}

// ErrInvalidArgument indicates invalid input.
type ErrInvalidArgument struct {
	Field   string
	Message string
}

// Error runs the error operation.
func (e *ErrInvalidArgument) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("invalid argument for %s: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("invalid argument: %s", e.Message)
}

// NewInvalidArgumentError constructs a new InvalidArgumentError.
func NewInvalidArgumentError(field, message string) error {
	return &ErrInvalidArgument{Field: field, Message: message}
}

// ErrResourceExhausted indicates system resource limits exceeded.
type ErrResourceExhausted struct {
	Resource string
	Message  string
}

// Error runs the error operation.
func (e *ErrResourceExhausted) Error() string {
	return fmt.Sprintf("resource exhausted (%s): %s", e.Resource, e.Message)
}

// NewResourceExhaustedError constructs a new ResourceExhaustedError.
func NewResourceExhaustedError(resource, message string) error {
	return &ErrResourceExhausted{Resource: resource, Message: message}
}

// ErrUnavailable indicates temporary unavailability.
type ErrUnavailable struct {
	Operation string
	Reason    string
}

// Error runs the error operation.
func (e *ErrUnavailable) Error() string {
	return fmt.Sprintf("service unavailable for %s: %s", e.Operation, e.Reason)
}

// NewUnavailableError constructs a new UnavailableError.
func NewUnavailableError(operation, reason string) error {
	return &ErrUnavailable{Operation: operation, Reason: reason}
}

// NewShutdownError creates a formatted error for shutdown failures.
func NewShutdownError(op, target string, err error) error {
	return fmt.Errorf("shutdown %s failed for %s: %w", op, target, err)
}
