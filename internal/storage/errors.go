package storage

import (
	"fmt"
	"time"
)

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
