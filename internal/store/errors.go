package store

import (
"errors"
"fmt"

"google.golang.org/grpc/codes"
"google.golang.org/grpc/status"
)

// =============================================================================
// Domain-Specific Error Types
// =============================================================================

// ErrNotFound indicates a requested resource does not exist.
type ErrNotFound struct {
Resource string
Name     string
}

func (e *ErrNotFound) Error() string {
return fmt.Sprintf("%s not found: %s", e.Resource, e.Name)
}

// ErrInvalidArgument indicates invalid input from the client.
type ErrInvalidArgument struct {
Field   string
Message string
}

func (e *ErrInvalidArgument) Error() string {
if e.Field != "" {
return fmt.Sprintf("invalid argument for %s: %s", e.Field, e.Message)
}
return fmt.Sprintf("invalid argument: %s", e.Message)
}

// ErrSchemaMismatch indicates incompatible schema between operations.
type ErrSchemaMismatch struct {
Dataset string
Message string
}

func (e *ErrSchemaMismatch) Error() string {
return fmt.Sprintf("schema mismatch for dataset '%s': %s", e.Dataset, e.Message)
}

// ErrDimensionMismatch indicates vector dimension incompatibility.
type ErrDimensionMismatch struct {
Expected int
Actual   int
Dataset  string
}

func (e *ErrDimensionMismatch) Error() string {
return fmt.Sprintf("dimension mismatch for dataset '%s': expected %d, got %d",
e.Dataset, e.Expected, e.Actual)
}

// ErrResourceExhausted indicates system resource limits exceeded.
type ErrResourceExhausted struct {
Resource string
Message  string
}

func (e *ErrResourceExhausted) Error() string {
return fmt.Sprintf("resource exhausted (%s): %s", e.Resource, e.Message)
}

// ErrUnavailable indicates temporary unavailability (e.g., during snapshots).
type ErrUnavailable struct {
Operation string
Reason    string
}

func (e *ErrUnavailable) Error() string {
return fmt.Sprintf("service unavailable for %s: %s", e.Operation, e.Reason)
}

// ErrPersistence indicates a storage/persistence failure.
type ErrPersistence struct {
Operation string
Cause     error
}

func (e *ErrPersistence) Error() string {
return fmt.Sprintf("persistence failed during %s: %v", e.Operation, e.Cause)
}

func (e *ErrPersistence) Unwrap() error {
return e.Cause
}

// ErrInternal indicates an unexpected internal error.
type ErrInternal struct {
Operation string
Cause     error
}

func (e *ErrInternal) Error() string {
if e.Cause != nil {
return fmt.Sprintf("internal error during %s: %v", e.Operation, e.Cause)
}
return fmt.Sprintf("internal error during %s", e.Operation)
}

func (e *ErrInternal) Unwrap() error {
return e.Cause
}

// =============================================================================
// Error Constructors
// =============================================================================

// NewNotFoundError creates a not found error.
func NewNotFoundError(resource, name string) error {
return &ErrNotFound{Resource: resource, Name: name}
}

// NewInvalidArgumentError creates an invalid argument error.
func NewInvalidArgumentError(field, message string) error {
return &ErrInvalidArgument{Field: field, Message: message}
}

// NewSchemaMismatchError creates a schema mismatch error.
func NewSchemaMismatchError(dataset, message string) error {
return &ErrSchemaMismatch{Dataset: dataset, Message: message}
}

// NewDimensionMismatchError creates a dimension mismatch error.
func NewDimensionMismatchError(dataset string, expected, actual int) error {
return &ErrDimensionMismatch{Dataset: dataset, Expected: expected, Actual: actual}
}

// NewResourceExhaustedError creates a resource exhausted error.
func NewResourceExhaustedError(resource, message string) error {
return &ErrResourceExhausted{Resource: resource, Message: message}
}

// NewUnavailableError creates an unavailable error.
func NewUnavailableError(operation, reason string) error {
return &ErrUnavailable{Operation: operation, Reason: reason}
}

// NewPersistenceError creates a persistence error.
func NewPersistenceError(operation string, cause error) error {
return &ErrPersistence{Operation: operation, Cause: cause}
}

// NewInternalError creates an internal error.
func NewInternalError(operation string, cause error) error {
return &ErrInternal{Operation: operation, Cause: cause}
}

// =============================================================================
// gRPC Status Code Mapping
// =============================================================================

// ToGRPCStatus converts a domain error to a gRPC status error with appropriate code.
// This provides specific Flight status codes for client-side debugging.
func ToGRPCStatus(err error) error {
if err == nil {
return nil
}

// Already a gRPC status error
if _, ok := status.FromError(err); ok {
return err
}

// Map domain errors to gRPC codes
var (
notFoundErr        *ErrNotFound
invalidArgErr      *ErrInvalidArgument
schemaMismatchErr  *ErrSchemaMismatch
dimMismatchErr     *ErrDimensionMismatch
resourceExhErr     *ErrResourceExhausted
unavailableErr     *ErrUnavailable
persistenceErr     *ErrPersistence
internalErr        *ErrInternal
)

switch {
case errors.As(err, &notFoundErr):
return status.Error(codes.NotFound, err.Error())

case errors.As(err, &invalidArgErr):
return status.Error(codes.InvalidArgument, err.Error())

case errors.As(err, &schemaMismatchErr):
return status.Error(codes.InvalidArgument, err.Error())

case errors.As(err, &dimMismatchErr):
return status.Error(codes.InvalidArgument, err.Error())

case errors.As(err, &resourceExhErr):
return status.Error(codes.ResourceExhausted, err.Error())

case errors.As(err, &unavailableErr):
return status.Error(codes.Unavailable, err.Error())

case errors.As(err, &persistenceErr):
// Persistence failures during snapshots = Unavailable
return status.Error(codes.Unavailable, err.Error())

case errors.As(err, &internalErr):
return status.Error(codes.Internal, err.Error())

default:
// Unknown errors default to Internal
return status.Error(codes.Internal, err.Error())
}
}

// MustToGRPCStatus is like ToGRPCStatus but panics if conversion fails.
// Useful for testing.
func MustToGRPCStatus(err error) error {
result := ToGRPCStatus(err)
if result == nil && err != nil {
panic(fmt.Sprintf("failed to convert error to gRPC status: %v", err))
}
return result
}
