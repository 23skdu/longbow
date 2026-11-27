package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteBackupEncryptionRequiredBackupConfig(t *testing.T) {
	cfg := BackupConfig{Destination: "s3://bucket/prefix"}
	err := ValidateRemoteBackupConfig(cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRemoteBackupEncryptionRequired)

	cfg.EncryptionMode = "client"
	require.NoError(t, ValidateRemoteBackupConfig(cfg))

	require.NoError(t, ValidateRemoteBackupEncryption("gs://bucket/prefix", "cmek"))
	require.Error(t, ValidateRemoteBackupEncryption("gs://bucket/prefix", "none"))
}
