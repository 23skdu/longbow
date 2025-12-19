package store

import (
"bytes"
"context"
"errors"
"io"
"strings"
"testing"

"github.com/aws/aws-sdk-go-v2/aws"
"github.com/aws/aws-sdk-go-v2/service/s3"
"github.com/aws/aws-sdk-go-v2/service/s3/types"
"github.com/stretchr/testify/assert"
"github.com/stretchr/testify/require"
)

// MockS3Client implements the S3 operations needed for testing
type MockS3Client struct {
objects      map[string][]byte
putError     error
getError     error
listError    error
deleteError  error
}

func NewMockS3Client() *MockS3Client {
return &MockS3Client{
objects: make(map[string][]byte),
}
}

func (m *MockS3Client) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
if m.putError != nil {
return nil, m.putError
}
data, err := io.ReadAll(input.Body)
if err != nil {
return nil, err
}
m.objects[aws.ToString(input.Key)] = data
return &s3.PutObjectOutput{}, nil
}

func (m *MockS3Client) GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
if m.getError != nil {
return nil, m.getError
}
key := aws.ToString(input.Key)
data, ok := m.objects[key]
if !ok {
return nil, &types.NoSuchKey{}
}
return &s3.GetObjectOutput{
Body: io.NopCloser(bytes.NewReader(data)),
}, nil
}

func (m *MockS3Client) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
if m.listError != nil {
return nil, m.listError
}
prefix := aws.ToString(input.Prefix)
var contents []types.Object
for key := range m.objects {
if strings.HasPrefix(key, prefix) {
contents = append(contents, types.Object{
Key: aws.String(key),
})
}
}
return &s3.ListObjectsV2Output{
Contents: contents,
}, nil
}

func (m *MockS3Client) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
if m.deleteError != nil {
return nil, m.deleteError
}
key := aws.ToString(input.Key)
delete(m.objects, key)
return &s3.DeleteObjectOutput{}, nil
}

// ========== Unit Tests for Helper Functions ==========

func TestBuildS3Key_NoPrefix(t *testing.T) {
key := buildS3Key("", "myCollection")
assert.Equal(t, "snapshots/myCollection.parquet", key)
}

func TestBuildS3Key_WithPrefix(t *testing.T) {
key := buildS3Key("tenant1", "myCollection")
assert.Equal(t, "tenant1/snapshots/myCollection.parquet", key)
}

func TestBuildS3Key_PrefixWithTrailingSlash(t *testing.T) {
key := buildS3Key("tenant1/", "myCollection")
assert.Equal(t, "tenant1/snapshots/myCollection.parquet", key)
}

func TestBuildS3Key_NestedPrefix(t *testing.T) {
key := buildS3Key("env/prod/region1", "myCollection")
assert.Equal(t, "env/prod/region1/snapshots/myCollection.parquet", key)
}

// ========== Error Type Tests ==========

func TestNotFoundError_Error(t *testing.T) {
err := &NotFoundError{Name: "test-collection"}
assert.Equal(t, "snapshot not found: test-collection", err.Error())
}

func TestIsNotFoundError_True(t *testing.T) {
err := &NotFoundError{Name: "test"}
assert.True(t, IsNotFoundError(err))
}

func TestIsNotFoundError_False(t *testing.T) {
err := errors.New("some other error")
assert.False(t, IsNotFoundError(err))
}

func TestIsNotFoundError_WrappedError(t *testing.T) {
inner := &NotFoundError{Name: "test"}
wrapped := errors.Join(errors.New("wrapper"), inner)
assert.True(t, IsNotFoundError(wrapped))
}

// ========== Config Validation Tests ==========

func TestS3BackendConfig_Validate_MissingBucket(t *testing.T) {
cfg := &S3BackendConfig{
AccessKeyID:     "key",
SecretAccessKey: "secret",
}
err := cfg.Validate()
require.Error(t, err)
assert.Contains(t, err.Error(), "bucket")
}

func TestS3BackendConfig_Validate_MissingCredentials(t *testing.T) {
cfg := &S3BackendConfig{
Bucket: "my-bucket",
}
err := cfg.Validate()
require.Error(t, err)
assert.Contains(t, err.Error(), "credentials")
}

func TestS3BackendConfig_Validate_MissingSecretKey(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:      "my-bucket",
AccessKeyID: "key",
}
err := cfg.Validate()
require.Error(t, err)
assert.Contains(t, err.Error(), "credentials")
}

func TestS3BackendConfig_Validate_Success(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:          "my-bucket",
AccessKeyID:     "key",
SecretAccessKey: "secret",
}
err := cfg.Validate()
assert.NoError(t, err)
}

// ========== S3Backend Creation Tests ==========

func TestNewS3Backend_InvalidConfig(t *testing.T) {
cfg := &S3BackendConfig{}
_, err := NewS3Backend(cfg)
require.Error(t, err)
assert.Contains(t, err.Error(), "invalid S3 config")
}

func TestNewS3Backend_DefaultRegion(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:          "test-bucket",
AccessKeyID:     "test-key",
SecretAccessKey: "test-secret",
Endpoint:        "http://localhost:9000",
UsePathStyle:    true,
}
backend, err := NewS3Backend(cfg)
require.NoError(t, err)
assert.NotNil(t, backend)
assert.Equal(t, "test-bucket", backend.bucket)
}

func TestNewS3Backend_CustomRegion(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:          "test-bucket",
AccessKeyID:     "test-key",
SecretAccessKey: "test-secret",
Region:          "eu-west-1",
Endpoint:        "http://localhost:9000",
}
backend, err := NewS3Backend(cfg)
require.NoError(t, err)
assert.NotNil(t, backend)
}

func TestNewS3Backend_WithPrefix(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:          "test-bucket",
AccessKeyID:     "test-key",
SecretAccessKey: "test-secret",
Prefix:          "tenant1/",
Endpoint:        "http://localhost:9000",
}
backend, err := NewS3Backend(cfg)
require.NoError(t, err)
assert.Equal(t, "tenant1", backend.prefix)
}

func TestNewS3Backend_ConnectionPoolSettings(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:              "test-bucket",
AccessKeyID:         "test-key",
SecretAccessKey:     "test-secret",
Endpoint:            "http://localhost:9000",
MaxIdleConns:        50,
MaxIdleConnsPerHost: 25,
}
backend, err := NewS3Backend(cfg)
require.NoError(t, err)
assert.NotNil(t, backend.GetHTTPTransport())
assert.Equal(t, 50, backend.GetHTTPTransport().MaxIdleConns)
assert.Equal(t, 25, backend.GetHTTPTransport().MaxIdleConnsPerHost)
}

func TestNewS3Backend_DefaultConnectionPoolSettings(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:          "test-bucket",
AccessKeyID:     "test-key",
SecretAccessKey: "test-secret",
Endpoint:        "http://localhost:9000",
}
backend, err := NewS3Backend(cfg)
require.NoError(t, err)
transport := backend.GetHTTPTransport()
assert.Equal(t, DefaultMaxIdleConns, transport.MaxIdleConns)
assert.Equal(t, DefaultMaxIdleConnsPerHost, transport.MaxIdleConnsPerHost)
}

func TestS3Backend_GetHTTPClient(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:          "test-bucket",
AccessKeyID:     "test-key",
SecretAccessKey: "test-secret",
Endpoint:        "http://localhost:9000",
}
backend, err := NewS3Backend(cfg)
require.NoError(t, err)
assert.NotNil(t, backend.GetHTTPClient())
}

// ========== Interface Compliance Test ==========

func TestS3Backend_ImplementsSnapshotBackend(t *testing.T) {
cfg := &S3BackendConfig{
Bucket:          "test-bucket",
AccessKeyID:     "test-key",
SecretAccessKey: "test-secret",
Endpoint:        "http://localhost:9000",
}
backend, err := NewS3Backend(cfg)
require.NoError(t, err)

var _ SnapshotBackend = backend
}
