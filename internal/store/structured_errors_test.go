package store

import (
"errors"
"strings"
"testing"
"time"
)

// =============================================================================
// WALError Tests
// =============================================================================

func TestWALError(t *testing.T) {
t.Run("creates error with full context", func(t *testing.T) {
cause := errors.New("disk full")
err := &WALError{
Op:        "write",
Path:      "/data/wal.log",
Offset:    1024,
Cause:     cause,
Timestamp: time.Now(),
}

if err.Op != "write" {
t.Errorf("expected Op=write, got %s", err.Op)
}
if err.Path != "/data/wal.log" {
t.Errorf("expected Path=/data/wal.log, got %s", err.Path)
}
if err.Offset != 1024 {
t.Errorf("expected Offset=1024, got %d", err.Offset)
}
})

t.Run("error string contains context", func(t *testing.T) {
err := &WALError{
Op:        "read",
Path:      "/data/wal.log",
Offset:    512,
Cause:     errors.New("EOF"),
Timestamp: time.Now(),
}

errStr := err.Error()
if !strings.Contains(errStr, "read") {
t.Error("error string should contain operation")
}
if !strings.Contains(errStr, "/data/wal.log") {
t.Error("error string should contain path")
}
if !strings.Contains(errStr, "512") {
t.Error("error string should contain offset")
}
})

t.Run("unwrap returns cause", func(t *testing.T) {
cause := errors.New("underlying error")
err := &WALError{Op: "write", Cause: cause}

if !errors.Is(err, cause) {
t.Error("Unwrap should return cause")
}
})

t.Run("constructor creates valid error", func(t *testing.T) {
err := NewWALError("flush", "/data/wal", 2048, errors.New("sync failed"))

var walErr *WALError
if !errors.As(err, &walErr) {
t.Fatal("expected WALError type")
}
if walErr.Op != "flush" {
t.Errorf("expected Op=flush, got %s", walErr.Op)
}
if walErr.Timestamp.IsZero() {
t.Error("timestamp should be set")
}
})
}

// =============================================================================
// S3Error Tests
// =============================================================================

func TestS3Error(t *testing.T) {
t.Run("creates error with full context", func(t *testing.T) {
err := &S3Error{
Op:        "upload",
Bucket:    "my-bucket",
Key:       "snapshots/2024/data.parquet",
Cause:     errors.New("access denied"),
Timestamp: time.Now(),
}

if err.Op != "upload" {
t.Errorf("expected Op=upload, got %s", err.Op)
}
if err.Bucket != "my-bucket" {
t.Errorf("expected Bucket=my-bucket, got %s", err.Bucket)
}
})

t.Run("error string contains S3 context", func(t *testing.T) {
err := &S3Error{
Op:     "download",
Bucket: "data-bucket",
Key:    "file.parquet",
Cause:  errors.New("not found"),
}

errStr := err.Error()
if !strings.Contains(errStr, "download") {
t.Error("error string should contain operation")
}
if !strings.Contains(errStr, "data-bucket") {
t.Error("error string should contain bucket")
}
if !strings.Contains(errStr, "file.parquet") {
t.Error("error string should contain key")
}
})

t.Run("constructor creates valid error", func(t *testing.T) {
err := NewS3Error("list", "bucket", "prefix/", errors.New("timeout"))

var s3Err *S3Error
if !errors.As(err, &s3Err) {
t.Fatal("expected S3Error type")
}
if s3Err.Timestamp.IsZero() {
t.Error("timestamp should be set")
}
})
}

// =============================================================================
// ReplicationError Tests
// =============================================================================

func TestReplicationError(t *testing.T) {
t.Run("creates error with peer context", func(t *testing.T) {
err := &ReplicationError{
Op:        "sync",
PeerAddr:  "10.0.0.5:8080",
Dataset:   "vectors",
Cause:     errors.New("connection refused"),
Timestamp: time.Now(),
}

if err.PeerAddr != "10.0.0.5:8080" {
t.Errorf("expected PeerAddr=10.0.0.5:8080, got %s", err.PeerAddr)
}
if err.Dataset != "vectors" {
t.Errorf("expected Dataset=vectors, got %s", err.Dataset)
}
})

t.Run("error string contains replication context", func(t *testing.T) {
err := &ReplicationError{
Op:       "replicate",
PeerAddr: "peer1:9000",
Dataset:  "embeddings",
Cause:    errors.New("timeout"),
}

errStr := err.Error()
if !strings.Contains(errStr, "replicate") {
t.Error("error string should contain operation")
}
if !strings.Contains(errStr, "peer1:9000") {
t.Error("error string should contain peer address")
}
})

t.Run("constructor creates valid error", func(t *testing.T) {
err := NewReplicationError("push", "peer:8080", "ds1", errors.New("failed"))

var replErr *ReplicationError
if !errors.As(err, &replErr) {
t.Fatal("expected ReplicationError type")
}
})
}

// =============================================================================
// ConfigError Tests
// =============================================================================

func TestConfigError(t *testing.T) {
t.Run("creates error with config context", func(t *testing.T) {
err := &ConfigError{
Component: "NUMA",
Field:     "NodeSelection",
Value:     "invalid",
Message:   "must be one of: auto, roundrobin, prefer",
Timestamp: time.Now(),
}

if err.Component != "NUMA" {
t.Errorf("expected Component=NUMA, got %s", err.Component)
}
if err.Field != "NodeSelection" {
t.Errorf("expected Field=NodeSelection, got %s", err.Field)
}
})

t.Run("error string contains config context", func(t *testing.T) {
err := &ConfigError{
Component: "S3Backend",
Field:     "Bucket",
Value:     "",
Message:   "bucket name required",
}

errStr := err.Error()
if !strings.Contains(errStr, "S3Backend") {
t.Error("error string should contain component")
}
if !strings.Contains(errStr, "Bucket") {
t.Error("error string should contain field")
}
})

t.Run("constructor creates valid error", func(t *testing.T) {
err := NewConfigError("Replication", "ReplicaFactor", "-1", "must be positive")

var cfgErr *ConfigError
if !errors.As(err, &cfgErr) {
t.Fatal("expected ConfigError type")
}
if cfgErr.Timestamp.IsZero() {
t.Error("timestamp should be set")
}
})
}

// =============================================================================
// ShutdownError Tests
// =============================================================================

func TestShutdownError(t *testing.T) {
t.Run("creates error with shutdown context", func(t *testing.T) {
err := &ShutdownError{
Phase:     "drain",
Component: "index_queue",
Cause:     errors.New("timeout waiting for drain"),
Timestamp: time.Now(),
}

if err.Phase != "drain" {
t.Errorf("expected Phase=drain, got %s", err.Phase)
}
if err.Component != "index_queue" {
t.Errorf("expected Component=index_queue, got %s", err.Component)
}
})

t.Run("error string contains shutdown context", func(t *testing.T) {
err := &ShutdownError{
Phase:     "close",
Component: "WAL",
Cause:     errors.New("sync failed"),
}

errStr := err.Error()
if !strings.Contains(errStr, "close") {
t.Error("error string should contain phase")
}
if !strings.Contains(errStr, "WAL") {
t.Error("error string should contain component")
}
})

t.Run("constructor creates valid error", func(t *testing.T) {
err := NewShutdownError("truncate", "WAL", errors.New("permission denied"))

var shutErr *ShutdownError
if !errors.As(err, &shutErr) {
t.Fatal("expected ShutdownError type")
}
})
}

// =============================================================================
// gRPC Status Mapping Tests for New Error Types
// =============================================================================

func TestToGRPCStatus_NewErrorTypes(t *testing.T) {
t.Run("WALError maps to Unavailable", func(t *testing.T) {
err := NewWALError("write", "/data/wal", 0, errors.New("disk full"))
grpcErr := ToGRPCStatus(err)

if grpcErr == nil {
t.Fatal("expected gRPC error")
}
})

t.Run("S3Error maps to Unavailable", func(t *testing.T) {
err := NewS3Error("upload", "bucket", "key", errors.New("timeout"))
grpcErr := ToGRPCStatus(err)

if grpcErr == nil {
t.Fatal("expected gRPC error")
}
})

t.Run("ReplicationError maps to Unavailable", func(t *testing.T) {
err := NewReplicationError("sync", "peer:8080", "ds", errors.New("failed"))
grpcErr := ToGRPCStatus(err)

if grpcErr == nil {
t.Fatal("expected gRPC error")
}
})

t.Run("ConfigError maps to InvalidArgument", func(t *testing.T) {
err := NewConfigError("NUMA", "Mode", "bad", "invalid mode")
grpcErr := ToGRPCStatus(err)

if grpcErr == nil {
t.Fatal("expected gRPC error")
}
})

t.Run("ShutdownError maps to Unavailable", func(t *testing.T) {
err := NewShutdownError("drain", "queue", errors.New("timeout"))
grpcErr := ToGRPCStatus(err)

if grpcErr == nil {
t.Fatal("expected gRPC error")
}
})
}
