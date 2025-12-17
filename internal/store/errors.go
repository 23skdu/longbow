package store

import (
"errors"
	"time"
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

// =============================================================================
// Structured Error Types with Rich Context
// =============================================================================

// WALError provides rich context for Write-Ahead Log operations.
type WALError struct {
Op        string    // Operation: "write", "read", "flush", "truncate"
Path      string    // WAL file path
Offset    int64     // Byte offset where error occurred
Cause     error     // Underlying error
Timestamp time.Time // When the error occurred
}

func (e *WALError) Error() string {
if e.Cause != nil {
return fmt.Sprintf("WAL %s failed at %s (offset %d): %v", e.Op, e.Path, e.Offset, e.Cause)
}
return fmt.Sprintf("WAL %s failed at %s (offset %d)", e.Op, e.Path, e.Offset)
}

func (e *WALError) Unwrap() error {
return e.Cause
}

// NewWALError creates a WAL error with timestamp.
func NewWALError(op, path string, offset int64, cause error) error {
return &WALError{
Op:        op,
Path:      path,
Offset:    offset,
Cause:     cause,
Timestamp: time.Now(),
}
}

// S3Error provides rich context for S3 backend operations.
type S3Error struct {
Op        string    // Operation: "upload", "download", "list", "delete"
Bucket    string    // S3 bucket name
Key       string    // S3 object key
Cause     error     // Underlying error
Timestamp time.Time // When the error occurred
}

func (e *S3Error) Error() string {
if e.Cause != nil {
return fmt.Sprintf("S3 %s failed for s3://%s/%s: %v", e.Op, e.Bucket, e.Key, e.Cause)
}
return fmt.Sprintf("S3 %s failed for s3://%s/%s", e.Op, e.Bucket, e.Key)
}

func (e *S3Error) Unwrap() error {
return e.Cause
}

// NewS3Error creates an S3 error with timestamp.
func NewS3Error(op, bucket, key string, cause error) error {
return &S3Error{
Op:        op,
Bucket:    bucket,
Key:       key,
Cause:     cause,
Timestamp: time.Now(),
}
}

// ReplicationError provides rich context for replication operations.
type ReplicationError struct {
Op        string    // Operation: "sync", "replicate", "connect"
PeerAddr  string    // Peer address
Dataset   string    // Dataset being replicated
Cause     error     // Underlying error
Timestamp time.Time // When the error occurred
}

func (e *ReplicationError) Error() string {
if e.Dataset != "" {
return fmt.Sprintf("replication %s to %s for dataset %s: %v", e.Op, e.PeerAddr, e.Dataset, e.Cause)
}
return fmt.Sprintf("replication %s to %s: %v", e.Op, e.PeerAddr, e.Cause)
}

func (e *ReplicationError) Unwrap() error {
return e.Cause
}

// NewReplicationError creates a replication error with timestamp.
func NewReplicationError(op, peerAddr, dataset string, cause error) error {
return &ReplicationError{
Op:        op,
PeerAddr:  peerAddr,
Dataset:   dataset,
Cause:     cause,
Timestamp: time.Now(),
}
}

// ConfigError provides rich context for configuration validation errors.
type ConfigError struct {
Component string    // Component: "NUMA", "S3Backend", "Replication"
Field     string    // Configuration field name
Value     string    // Invalid value (as string)
Message   string    // Validation error message
Timestamp time.Time // When the error occurred
}

func (e *ConfigError) Error() string {
if e.Value != "" {
return fmt.Sprintf("config error in %s.%s (value=%q): %s", e.Component, e.Field, e.Value, e.Message)
}
return fmt.Sprintf("config error in %s.%s: %s", e.Component, e.Field, e.Message)
}

// NewConfigError creates a configuration error with timestamp.
func NewConfigError(component, field, value, message string) error {
return &ConfigError{
Component: component,
Field:     field,
Value:     value,
Message:   message,
Timestamp: time.Now(),
}
}

// ShutdownError provides rich context for graceful shutdown operations.
type ShutdownError struct {
Phase     string    // Phase: "drain", "flush", "close", "truncate"
Component string    // Component: "WAL", "index_queue", "connections"
Cause     error     // Underlying error
Timestamp time.Time // When the error occurred
}

func (e *ShutdownError) Error() string {
if e.Cause != nil {
return fmt.Sprintf("shutdown %s phase failed for %s: %v", e.Phase, e.Component, e.Cause)
}
return fmt.Sprintf("shutdown %s phase failed for %s", e.Phase, e.Component)
}

func (e *ShutdownError) Unwrap() error {
return e.Cause
}

// NewShutdownError creates a shutdown error with timestamp.
func NewShutdownError(phase, component string, cause error) error {
return &ShutdownError{
Phase:     phase,
Component: component,
Cause:     cause,
Timestamp: time.Now(),
}
}
