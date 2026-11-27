package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/stretchr/testify/require"
)

func TestRemoteBackupEncryptionRequired(t *testing.T) {
	cfg := S3BackendConfig{Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s", RequireEncryption: true}
	err := cfg.Validate()
	require.Error(t, err)

	cfg.EncryptionMode = "sse-kms"
	cfg.SSEKMSKeyID = "arn:aws:kms:us-east-1:123:key/abc"
	require.NoError(t, cfg.Validate())

	gcfg := GCSBackendConfig{Bucket: "b", RequireEncryption: true}
	err = gcfg.ValidateRemoteEncryption()
	require.Error(t, err)
	gcfg.EncryptionMode = "cmek"
	gcfg.CMEKKeyID = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
	require.NoError(t, gcfg.ValidateRemoteEncryption())
}

func TestEncryptedRemoteStorageRoundTrip(t *testing.T) {
	ctx := context.Background()
	kp, err := security.NewLocalKeyProvider(nil)
	require.NoError(t, err)
	ce, err := NewCipherEngine(ctx, kp)
	require.NoError(t, err)

	mock := NewMockRemoteStorage("s3")
	enc, err := NewEncryptedRemoteStorage(mock, ce, RemoteEncryptionOptions{Bucket: "bucket", Dataset: "ds", Namespace: "ns", BackupVersion: "v1"})
	require.NoError(t, err)

	data := []byte("remote backup payload")
	require.NoError(t, enc.Put(ctx, "backup.tar.gz", bytes.NewReader(data)))

	rc, err := enc.Get(ctx, "backup.tar.gz")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, got)
	_ = rc.Close()
}
