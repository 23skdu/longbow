package store

import (
"bytes"
"context"
"io"
"os"
"strings"
"testing"
"time"
)

// TestS3BackendConfig tests configuration validation
func TestS3BackendConfig(t *testing.T) {
tests := []struct {
name    string
config  S3BackendConfig
wantErr bool
}{
{
name: "valid config",
config: S3BackendConfig{
Endpoint:        "http://localhost:9000",
Bucket:          "longbow-snapshots",
AccessKeyID:     "minioadmin",
SecretAccessKey: "minioadmin",
Region:          "us-east-1",
},
wantErr: false,
},
{
name: "missing bucket",
config: S3BackendConfig{
Endpoint:        "http://localhost:9000",
AccessKeyID:     "minioadmin",
SecretAccessKey: "minioadmin",
},
wantErr: true,
},
{
name: "missing credentials",
config: S3BackendConfig{
Endpoint: "http://localhost:9000",
Bucket:   "longbow-snapshots",
},
wantErr: true,
},
{
name: "with prefix",
config: S3BackendConfig{
Endpoint:        "http://localhost:9000",
Bucket:          "longbow-snapshots",
Prefix:          "prod/vectors",
AccessKeyID:     "minioadmin",
SecretAccessKey: "minioadmin",
Region:          "us-east-1",
},
wantErr: false,
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
err := tt.config.Validate()
if (err != nil) != tt.wantErr {
t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
}
})
}
}

// TestS3BackendInterface ensures S3Backend implements SnapshotBackend
func TestS3BackendInterface(t *testing.T) {
var _ SnapshotBackend = (*S3Backend)(nil)
}

// TestS3BackendKeyGeneration tests S3 key path generation
func TestS3BackendKeyGeneration(t *testing.T) {
tests := []struct {
prefix   string
name     string
expected string
}{
{"", "collection1", "snapshots/collection1.parquet"},
{"prod", "collection1", "prod/snapshots/collection1.parquet"},
{"prod/vectors", "my-data", "prod/vectors/snapshots/my-data.parquet"},
{"trailing/", "test", "trailing/snapshots/test.parquet"},
}

for _, tt := range tests {
t.Run(tt.prefix+"/"+tt.name, func(t *testing.T) {
key := buildS3Key(tt.prefix, tt.name)
if key != tt.expected {
t.Errorf("buildS3Key(%q, %q) = %q, want %q", tt.prefix, tt.name, key, tt.expected)
}
})
}
}

// TestS3BackendWriteSnapshot tests writing snapshots to S3
func TestS3BackendWriteSnapshot(t *testing.T) {
if os.Getenv("S3_TEST_ENDPOINT") == "" {
t.Skip("Skipping S3 integration test: S3_TEST_ENDPOINT not set")
}

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

backend := newTestS3Backend(t)

testData := []byte("test parquet data content")
err := backend.WriteSnapshot(ctx, "test_collection", testData)
if err != nil {
t.Fatalf("WriteSnapshot failed: %v", err)
}

// Verify we can read it back
reader, err := backend.ReadSnapshot(ctx, "test_collection")
if err != nil {
t.Fatalf("ReadSnapshot failed: %v", err)
}
defer func() { _ = reader.Close() }()

readData, err := io.ReadAll(reader)
if err != nil {
t.Fatalf("Failed to read snapshot data: %v", err)
}

if !bytes.Equal(readData, testData) {
t.Errorf("Read data mismatch: got %d bytes, want %d bytes", len(readData), len(testData))
}

// Cleanup
_ = backend.DeleteSnapshot(ctx, "test_collection")
}

// TestS3BackendListSnapshots tests listing snapshots from S3
func TestS3BackendListSnapshots(t *testing.T) {
if os.Getenv("S3_TEST_ENDPOINT") == "" {
t.Skip("Skipping S3 integration test: S3_TEST_ENDPOINT not set")
}

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

backend := newTestS3Backend(t)

// Write test snapshots
collections := []string{"collection_a", "collection_b", "collection_c"}
for _, name := range collections {
err := backend.WriteSnapshot(ctx, name, []byte("test data for "+name))
if err != nil {
t.Fatalf("WriteSnapshot(%s) failed: %v", name, err)
}
}

// List and verify
listed, err := backend.ListSnapshots(ctx)
if err != nil {
t.Fatalf("ListSnapshots failed: %v", err)
}

for _, want := range collections {
found := false
for _, got := range listed {
if got == want {
found = true
break
}
}
if !found {
t.Errorf("Collection %q not found in list: %v", want, listed)
}
}

// Cleanup
for _, name := range collections {
_ = backend.DeleteSnapshot(ctx, name)
}
}

// TestS3BackendDeleteSnapshot tests deleting snapshots from S3
func TestS3BackendDeleteSnapshot(t *testing.T) {
if os.Getenv("S3_TEST_ENDPOINT") == "" {
t.Skip("Skipping S3 integration test: S3_TEST_ENDPOINT not set")
}

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

backend := newTestS3Backend(t)

// Write then delete
err := backend.WriteSnapshot(ctx, "to_delete", []byte("delete me"))
if err != nil {
t.Fatalf("WriteSnapshot failed: %v", err)
}

err = backend.DeleteSnapshot(ctx, "to_delete")
if err != nil {
t.Fatalf("DeleteSnapshot failed: %v", err)
}

// Verify it's gone
_, err = backend.ReadSnapshot(ctx, "to_delete")
if err == nil {
t.Error("Expected error reading deleted snapshot, got nil")
}
}

// TestS3BackendReadNonexistent tests reading a non-existent snapshot
func TestS3BackendReadNonexistent(t *testing.T) {
if os.Getenv("S3_TEST_ENDPOINT") == "" {
t.Skip("Skipping S3 integration test: S3_TEST_ENDPOINT not set")
}

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

backend := newTestS3Backend(t)

_, err := backend.ReadSnapshot(ctx, "nonexistent_collection_xyz")
if err == nil {
t.Error("Expected error reading non-existent snapshot")
}
if !IsNotFoundError(err) {
t.Errorf("Expected NotFoundError, got: %v", err)
}
}

// TestS3BackendConcurrentWrites tests concurrent write operations
func TestS3BackendConcurrentWrites(t *testing.T) {
if os.Getenv("S3_TEST_ENDPOINT") == "" {
t.Skip("Skipping S3 integration test: S3_TEST_ENDPOINT not set")
}

ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
defer cancel()

backend := newTestS3Backend(t)

const numWriters = 10
errChan := make(chan error, numWriters)

for i := 0; i < numWriters; i++ {
go func(idx int) {
name := "concurrent_" + strings.Repeat("x", idx)
data := bytes.Repeat([]byte{byte(idx)}, 1024)
errChan <- backend.WriteSnapshot(ctx, name, data)
}(i)
}

for i := 0; i < numWriters; i++ {
if err := <-errChan; err != nil {
t.Errorf("Concurrent write failed: %v", err)
}
}

// Cleanup
for i := 0; i < numWriters; i++ {
name := "concurrent_" + strings.Repeat("x", i)
_ = backend.DeleteSnapshot(ctx, name)
}
}

// TestS3BackendLargeSnapshot tests writing larger snapshots
func TestS3BackendLargeSnapshot(t *testing.T) {
if os.Getenv("S3_TEST_ENDPOINT") == "" {
t.Skip("Skipping S3 integration test: S3_TEST_ENDPOINT not set")
}

ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
defer cancel()

backend := newTestS3Backend(t)

// 10MB test data
largeData := bytes.Repeat([]byte("PARQUET_DATA_"), 800000)

err := backend.WriteSnapshot(ctx, "large_collection", largeData)
if err != nil {
t.Fatalf("WriteSnapshot (large) failed: %v", err)
}

reader, err := backend.ReadSnapshot(ctx, "large_collection")
if err != nil {
t.Fatalf("ReadSnapshot (large) failed: %v", err)
}
defer func() { _ = reader.Close() }()

readData, err := io.ReadAll(reader)
if err != nil {
t.Fatalf("Failed to read large snapshot: %v", err)
}

if len(readData) != len(largeData) {
t.Errorf("Large data size mismatch: got %d, want %d", len(readData), len(largeData))
}

// Cleanup
_ = backend.DeleteSnapshot(ctx, "large_collection")
}

// Helper to create test backend from environment
func newTestS3Backend(t *testing.T) *S3Backend {
t.Helper()

config := S3BackendConfig{
Endpoint:        os.Getenv("S3_TEST_ENDPOINT"),
Bucket:          os.Getenv("S3_TEST_BUCKET"),
Prefix:          os.Getenv("S3_TEST_PREFIX"),
AccessKeyID:     os.Getenv("S3_TEST_ACCESS_KEY"),
SecretAccessKey: os.Getenv("S3_TEST_SECRET_KEY"),
Region:          os.Getenv("S3_TEST_REGION"),
UsePathStyle:    os.Getenv("S3_TEST_PATH_STYLE") == "true",
}

if config.Bucket == "" {
config.Bucket = "longbow-test"
}
if config.Region == "" {
config.Region = "us-east-1"
}

backend, err := NewS3Backend(&config)
if err != nil {
t.Fatalf("Failed to create S3Backend: %v", err)
}

return backend
}
