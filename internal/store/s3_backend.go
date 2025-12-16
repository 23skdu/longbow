package store

import (
"bytes"
"context"
"errors"
"fmt"
"io"
"path"
"strings"

"github.com/aws/aws-sdk-go-v2/aws"
"github.com/aws/aws-sdk-go-v2/config"
"github.com/aws/aws-sdk-go-v2/credentials"
"github.com/aws/aws-sdk-go-v2/service/s3"
"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
client *s3.Client
bucket string
prefix string
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

// Build AWS config
awsCfg, err := config.LoadDefaultConfig(context.Background(),
config.WithRegion(region),
config.WithCredentialsProvider(
credentials.NewStaticCredentialsProvider(
cfg.AccessKeyID,
cfg.SecretAccessKey,
"",
),
),
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
client: client,
bucket: cfg.Bucket,
prefix: strings.TrimSuffix(cfg.Prefix, "/"),
}, nil
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
return fmt.Errorf("failed to upload snapshot %s: %w", name, err)
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
return nil, fmt.Errorf("failed to download snapshot %s: %w", name, err)
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
return nil, fmt.Errorf("failed to list snapshots: %w", err)
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
return fmt.Errorf("failed to delete snapshot %s: %w", name, err)
}

return nil
}
