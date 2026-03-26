package storage

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3RemoteStorage_Integration(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("Skipping S3 integration test due to missing credentials")
	}

	bucket := os.Getenv("S3_TEST_BUCKET")
	if bucket == "" {
		t.Skip("Skipping, S3_TEST_BUCKET environment variable not set")
	}

	ctx := context.Background()
	s3r, err := NewS3RemoteStorage(ctx, "us-east-1", bucket, "", os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"))
	require.NoError(t, err)

	testKey := "test/remote_storage_integ_test.bin"
	testData := []byte("hello s3 remote storage")

	err = s3r.Put(ctx, testKey, bytes.NewReader(testData))
	require.NoError(t, err)

	exists, err := s3r.Exists(ctx, testKey)
	require.NoError(t, err)
	require.True(t, exists)

	rc, err := s3r.Get(ctx, testKey)
	require.NoError(t, err)

	downloaded := new(bytes.Buffer)
	_, err = downloaded.ReadFrom(rc)
	require.NoError(t, err)
	err = rc.Close()
	require.NoError(t, err)
	require.Equal(t, testData, downloaded.Bytes())

	err = s3r.Delete(ctx, testKey)
	require.NoError(t, err)

	exists, err = s3r.Exists(ctx, testKey)
	require.NoError(t, err)
	require.False(t, exists)
}
