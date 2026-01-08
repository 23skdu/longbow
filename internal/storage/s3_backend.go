package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
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
	// WriteSnapshotAsync performs a non-blocking upload
	WriteSnapshotAsync(name string, data []byte)

	// Expose properties for testing/coordination
	Bucket() string
	Prefix() string
	GetHTTPTransport() *http.Transport
	GetHTTPClient() *http.Client
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

	// Async settings
	AsyncEnabled   bool // Enable background uploads
	WorkerCount    int  // Number of parallel upload workers
	QueueSize      int  // Buffer size for pending uploads
	AsyncMultipart bool // Use multipart upload for large files
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

// AsyncS3Backend wraps S3Backend for background execution
type AsyncS3Backend struct {
	*S3Backend
	jobs   chan asyncJob
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

type asyncJob struct {
	name string
	data []byte
}

// NewS3Backend creates a new S3 backend from configuration
func NewS3Backend(cfg *S3BackendConfig) (SnapshotBackend, error) {
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

	backend := &S3Backend{
		client:     client,
		bucket:     cfg.Bucket,
		prefix:     strings.TrimSuffix(cfg.Prefix, "/"),
		transport:  transport,
		httpClient: httpClient,
	}

	if cfg.AsyncEnabled {
		workerCount := cfg.WorkerCount
		if workerCount <= 0 {
			workerCount = 4
		}
		queueSize := cfg.QueueSize
		if queueSize <= 0 {
			queueSize = 100
		}

		ctx, cancel := context.WithCancel(context.Background())
		asb := &AsyncS3Backend{
			S3Backend: backend,
			jobs:      make(chan asyncJob, queueSize),
			ctx:       ctx,
			cancel:    cancel,
		}

		for i := 0; i < workerCount; i++ {
			asb.wg.Add(1)
			go asb.worker()
		}
		return asb, nil
	}

	return backend, nil
}

// Bucket returns the S3 bucket name
func (b *S3Backend) Bucket() string { return b.bucket }

// Prefix returns the S3 key prefix
func (b *S3Backend) Prefix() string { return b.prefix }

// GetHTTPTransport returns the underlying HTTP transport used for connection pooling
func (b *S3Backend) GetHTTPTransport() *http.Transport {
	return b.transport
}

// GetHTTPClient returns the HTTP client used by this S3 backend
func (b *S3Backend) GetHTTPClient() *http.Client {
	return b.httpClient
}

func (b *AsyncS3Backend) Bucket() string                    { return b.S3Backend.Bucket() }
func (b *AsyncS3Backend) Prefix() string                    { return b.S3Backend.Prefix() }
func (b *AsyncS3Backend) GetHTTPTransport() *http.Transport { return b.S3Backend.GetHTTPTransport() }
func (b *AsyncS3Backend) GetHTTPClient() *http.Client       { return b.S3Backend.GetHTTPClient() }

func (b *AsyncS3Backend) WriteSnapshot(ctx context.Context, name string, data []byte) error {
	return b.S3Backend.WriteSnapshot(ctx, name, data)
}

func (b *AsyncS3Backend) ReadSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	return b.S3Backend.ReadSnapshot(ctx, name)
}

func (b *AsyncS3Backend) ListSnapshots(ctx context.Context) ([]string, error) {
	return b.S3Backend.ListSnapshots(ctx)
}

func (b *AsyncS3Backend) DeleteSnapshot(ctx context.Context, name string) error {
	return b.S3Backend.DeleteSnapshot(ctx, name)
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

	const minMultipartSize = 5 * 1024 * 1024 // 5MB
	if len(data) >= minMultipartSize {
		return b.writeMultipart(ctx, key, data)
	}

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

func (b *S3Backend) writeMultipart(ctx context.Context, key string, data []byte) error {
	createOut, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(b.bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/vnd.apache.parquet"),
	})
	if err != nil {
		return err
	}
	uploadID := createOut.UploadId

	completedParts := make([]types.CompletedPart, 0, len(data)/(5*1024*1024)+1)
	const partSize = 5 * 1024 * 1024
	partNumber := int32(1)

	for start := 0; start < len(data); start += partSize {
		end := start + partSize
		if end > len(data) {
			end = len(data)
		}

		partOut, err := b.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(b.bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(partNumber),
			Body:       bytes.NewReader(data[start:end]),
		})
		if err != nil {
			_, _ = b.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(b.bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			})
			return err
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       partOut.ETag,
			PartNumber: aws.Int32(partNumber),
		})
		partNumber++
	}

	_, err = b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	return err
}

// WriteSnapshotAsync queues a snapshot for background upload
func (b *AsyncS3Backend) WriteSnapshotAsync(name string, data []byte) {
	select {
	case b.jobs <- asyncJob{name: name, data: data}:
	default:
		// Drop if full
	}
}

func (b *AsyncS3Backend) worker() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case job := <-b.jobs:
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			_ = b.WriteSnapshot(ctx, job.name, job.data)
			cancel()
		}
	}
}

// WriteSnapshotAsync on sync backend is just a wrapper
func (b *S3Backend) WriteSnapshotAsync(name string, data []byte) {
	_ = b.WriteSnapshot(context.Background(), name, data)
}

// Close gracefully shuts down workers
func (b *AsyncS3Backend) Close() {
	b.cancel()
	b.wg.Wait()
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
