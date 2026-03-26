package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3RemoteStorage is an AWS S3 implementation of RemoteStorage.
type S3RemoteStorage struct {
	client *s3.Client
	bucket string
}

var _ RemoteStorage = (*S3RemoteStorage)(nil)

// NewS3RemoteStorage constructs an S3-backed RemoteStorage.
func NewS3RemoteStorage(ctx context.Context, region, bucket, endpoint, accessKey, secretKey string) (*S3RemoteStorage, error) {
	if region == "" {
		region = "us-east-1"
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	if accessKey != "" && secretKey != "" {
		awsCfg.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	}

	opts := func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}

	client := s3.NewFromConfig(awsCfg, opts)
	return &S3RemoteStorage{
		client: client,
		bucket: bucket,
	}, nil
}

// Put uploads an object to S3. For larger payloads it buffers in memory since PutObject
// expects a stream with known length or seekable reader. In production, consider s3manager.Uploader.
func (s *S3RemoteStorage) Put(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	op := "put"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues("s3", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("s3", op, status).Inc()
	}()

	// We copy to a buffer to ensure it has a size and is seekable.
	buf := new(bytes.Buffer)
	uploaded, err := io.Copy(buf, r)
	if err != nil {
		status = "error"
		return err
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	})

	if err != nil {
		status = "error"
		return err
	}

	metrics.RemoteStorageUploadBytes.WithLabelValues("s3").Add(float64(uploaded))
	return nil
}

// Get downloads an object from S3. The caller is responsible for closing the ReadCloser.
func (s *S3RemoteStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	op := "get"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues("s3", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("s3", op, status).Inc()
	}()

	res, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		status = "error"
		return nil, err
	}

	// We wrap the ReadCloser to capture downloaded bytes correctly when the caller consumes it.
	return &metricsReader{
		r:        res.Body,
		provider: "s3",
	}, nil
}

// Delete permanently removes an object from S3.
func (s *S3RemoteStorage) Delete(ctx context.Context, key string) error {
	start := time.Now()
	op := "delete"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues("s3", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("s3", op, status).Inc()
	}()

	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		status = "error"
		return err
	}
	return nil
}

// Exists checks if an object is present in S3 without downloading it.
func (s *S3RemoteStorage) Exists(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	op := "exists"
	status := "success"
	defer func() {
		metrics.RemoteStorageDurationSeconds.WithLabelValues("s3", op).Observe(time.Since(start).Seconds())
		metrics.RemoteStorageOpsTotal.WithLabelValues("s3", op, status).Inc()
	}()

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nfe *types.NotFound
		if errors.As(err, &nfe) {
			return false, nil
		}
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		status = "error"
		return false, err
	}
	return true, nil
}
