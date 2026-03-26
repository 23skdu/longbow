package storage

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestGCSRemoteStorage_Integration(t *testing.T) {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("Skipping GCS integration test due to missing credentials")
	}

	bucket := os.Getenv("GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("Skipping, GCS_TEST_BUCKET environment variable not set")
	}

	ctx := context.Background()
	gcsr, err := NewGCSRemoteStorage(ctx, bucket, option.WithCredentialsFile(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")))
	require.NoError(t, err)
	defer gcsr.Close()

	testKey := "test/remote_storage_integ_test.bin"
	testData := []byte("hello gcs remote storage")

	err = gcsr.Put(ctx, testKey, bytes.NewReader(testData))
	require.NoError(t, err)

	exists, err := gcsr.Exists(ctx, testKey)
	require.NoError(t, err)
	require.True(t, exists)

	rc, err := gcsr.Get(ctx, testKey)
	require.NoError(t, err)

	downloaded := new(bytes.Buffer)
	_, err = downloaded.ReadFrom(rc)
	require.NoError(t, err)
	err = rc.Close()
	require.NoError(t, err)
	require.Equal(t, testData, downloaded.Bytes())

	err = gcsr.Delete(ctx, testKey)
	require.NoError(t, err)

	exists, err = gcsr.Exists(ctx, testKey)
	require.NoError(t, err)
	require.False(t, exists)
}
