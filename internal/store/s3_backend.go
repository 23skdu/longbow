package store

import (
"bytes"
"context"
"errors"
"fmt"
"io"
"net/http"
"path"
"strings"
"time"

"github.com/aws/aws-sdk-go-v2/aws"
"github.com/aws/aws-sdk-go-v2/config"
"github.com/aws/aws-sdk-go-v2/credentials"
"github.com/aws/aws-sdk-go-v2/service/s3"
"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Connection pool default settings for S3 backend
const (
// DefaultMaxIdleConns is the default maximum number of idle connections across all hosts
DefaultMaxIdleConns = 100
// DefaultMaxIdleConnsPerHost is the default maximum number of idle connections per host
DefaultMaxIdleConnsPerHost = 100
// DefaultIdleConnTimeout is the default timeout for idle connections
DefaultIdleConnTimeout = 90 * time.Second
)


// SnapshotBackend defines the interface for snapshot storage backends
type SnapshotBackend interface {
// WriteSnapshot writes snapshot data for a collection
WriteSnapshot(ctx context.Context, name string, data []byte) error
// ReadSnapshot returns a reader for snapshot data
ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error)
// ListSnapshots returns all collection names with snapshots
ListSnapshots(ctx context.Context) ([]string, error)
// DeleteSnapshot removes a snapshot
DeleteSnapshot(ctx context.Context, name string) error
}

// NotFoundError indicates a snapshot was not found
type NotFoundError struct {
Name string
}

func (e *NotFoundError) Error() string {
return fmt.Sprintf("snapshot not found: %s", e.Name)
}

// IsNotFoundError checks if an error is a NotFoundError
func IsNotFoundError(err error) bool {
var nfe *NotFoundError
return errors.As(err, &nfe)
}

// S3BackendConfig holds configuration for the S3 backend
type S3BackendConfig struct {
Endpoint        string // S3-compatible endpoint URL (e.g., "http://localhost:9000" for MinIO)
Bucket          string // Bucket name
Prefix          string // Optional key prefix for all snapshots
AccessKeyID     string // AWS access key
SecretAccessKey string // AWS secret key
Region          string // AWS region (default: us-east-1)
UsePathStyle    bool   // Use path-style addressing (required for MinIO)

	// Connection pool settings
	MaxIdleConns        int           // Maximum idle connections (0 = use default)
	MaxIdleConnsPerHost int           // Maximum idle connections per host (0 = use default)
	IdleConnTimeout     time.Duration // Idle connection timeout (0 = use default)
}

// Validate checks the configuration for required fields
func (c *S3BackendConfig) Validate() error {
if c.Bucket == "" {
return errors.New("S3 bucket is required")
}
if c.AccessKeyID == "" || c.SecretAccessKey == "" {
return errors.New("S3 credentials are required")
}
return nil
}

// S3Backend implements SnapshotBackend for S3-compatible storage
type S3Backend struct {
client     *s3.Client
bucket     string
prefix     string
transport  *http.Transport
httpClient *http.Client
}

// NewS3Backend creates a new S3 backend from configuration
func NewS3Backend(cfg *S3BackendConfig) (*S3Backend, error) {
if err := cfg.Validate(); err != nil {
return nil, fmt.Errorf("invalid S3 config: %w", err)
}

region := cfg.Region
if region == "" {
region = "us-east-1"
}

// Configure connection pool settings with defaults
maxIdleConns := cfg.MaxIdleConns
if maxIdleConns <= 0 {
maxIdleConns = DefaultMaxIdleConns
}
maxIdleConnsPerHost := cfg.MaxIdleConnsPerHost
if maxIdleConnsPerHost <= 0 {
maxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
}
idleConnTimeout := cfg.IdleConnTimeout
if idleConnTimeout <= 0 {
idleConnTimeout = DefaultIdleConnTimeout
}

// Create HTTP transport with connection pooling
transport := &http.Transport{
MaxIdleConns:        maxIdleConns,
MaxIdleConnsPerHost: maxIdleConnsPerHost,
IdleConnTimeout:     idleConnTimeout,
}

// Create HTTP client with pooled transport
httpClient := &http.Client{
Transport: transport,
}

// Build AWS config with custom HTTP client
awsCfg, err := config.LoadDefaultConfig(context.Background(),
config.WithRegion(region),
config.WithCredentialsProvider(
credentials.NewStaticCredentialsProvider(
cfg.AccessKeyID,
cfg.SecretAccessKey,
"",
),
),
config.WithHTTPClient(httpClient),
)
if err != nil {
return nil, fmt.Errorf("failed to load AWS config: %w", err)
}

// Create S3 client with optional endpoint override
opts := func(o *s3.Options) {
if cfg.Endpoint != "" {
o.BaseEndpoint = aws.String(cfg.Endpoint)
}
o.UsePathStyle = cfg.UsePathStyle
}

client := s3.NewFromConfig(awsCfg, opts)

return &S3Backend{
client:     client,
bucket:     cfg.Bucket,
prefix:     strings.TrimSuffix(cfg.Prefix, "/"),
transport:  transport,
httpClient: httpClient,
}, nil
}


// GetHTTPTransport returns the underlying HTTP transport used for connection pooling
func (b *S3Backend) GetHTTPTransport() *http.Transport {
return b.transport
}

// GetHTTPClient returns the HTTP client used by this S3 backend
func (b *S3Backend) GetHTTPClient() *http.Client {
return b.httpClient
}

// buildS3Key constructs the S3 key for a snapshot
func buildS3Key(prefix, name string) string {
key := path.Join("snapshots", name+".parquet")
if prefix != "" {
prefix = strings.TrimSuffix(prefix, "/")
key = path.Join(prefix, key)
}
return key
}

// WriteSnapshot uploads snapshot data to S3
func (b *S3Backend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
key := buildS3Key(b.prefix, name)

_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
Bucket:      aws.String(b.bucket),
Key:         aws.String(key),
Body:        bytes.NewReader(data),
ContentType: aws.String("application/vnd.apache.parquet"),
})
if err != nil {
return NewS3Error("upload", b.bucket, name, err)
}

return nil
}

// ReadSnapshot downloads snapshot data from S3
func (b *S3Backend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
key := buildS3Key(b.prefix, name)

result, err := b.client.GetObject(ctx, &s3.GetObjectInput{
Bucket: aws.String(b.bucket),
Key:    aws.String(key),
})
if err != nil {
// Check for NoSuchKey error
var nsk *types.NoSuchKey
if errors.As(err, &nsk) {
return nil, &NotFoundError{Name: name}
}
// Also check for NotFound in error message (some S3-compatible services)
if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound") {
return nil, &NotFoundError{Name: name}
}
return nil, NewS3Error("download", b.bucket, name, err)
}

return result.Body, nil
}

// ListSnapshots returns all collection names that have snapshots
func (b *S3Backend) ListSnapshots(ctx context.Context) ([]string, error) {
prefix := path.Join(b.prefix, "snapshots") + "/"
if b.prefix == "" {
prefix = "snapshots/"
}

var collections []string
paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
Bucket: aws.String(b.bucket),
Prefix: aws.String(prefix),
})

for paginator.HasMorePages() {
page, err := paginator.NextPage(ctx)
if err != nil {
return nil, NewS3Error("list", b.bucket, b.prefix, err)
}

for _, obj := range page.Contents {
key := aws.ToString(obj.Key)
// Extract collection name from key
base := path.Base(key)
if strings.HasSuffix(base, ".parquet") {
name := strings.TrimSuffix(base, ".parquet")
collections = append(collections, name)
}
}
}

return collections, nil
}

// DeleteSnapshot removes a snapshot from S3
func (b *S3Backend) DeleteSnapshot(ctx context.Context, name string) error {
key := buildS3Key(b.prefix, name)

_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
Bucket: aws.String(b.bucket),
Key:    aws.String(key),
})
if err != nil {
return NewS3Error("delete", b.bucket, name, err)
}

return nil
}
