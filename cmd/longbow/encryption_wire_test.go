package main

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func encryptionTestConfig() Config {
	return Config{
		KeepAliveTime:     2 * time.Hour,
		KeepAliveTimeout:  20 * time.Second,
		KeepAliveMinTime:  5 * time.Minute,
		ListenAddr:        "0.0.0.0:3000",
		MetaAddr:          "0.0.0.0:3001",
		MetricsAddr:       "0.0.0.0:9090",
		MaxMemory:         1073741824,
		MaxWALSize:        104857600,
		LogFormat:         "json",
		LogLevel:          "info",
		SnapshotInterval:  time.Hour,
		KMSProvider:       "local",
		EncryptionAtRest:  false,
		StorageAsyncFsync: true,
	}
}

func testMasterKeyHex() string {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return hex.EncodeToString(key)
}

// TestStorageEncryptionWiringDisabled verifies the plaintext default survives
// the config-to-storage path.
func TestStorageEncryptionWiringDisabled(t *testing.T) {
	cfg := encryptionTestConfig()
	cfg.DataPath = t.TempDir()
	require.NoError(t, ValidateConfig(&cfg))

	encCfg, restore, err := setupStorageEncryption(context.Background(), &cfg, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(restore)
	require.False(t, encCfg.Enabled)

	engine, err := storage.NewStorageEngine(buildStorageConfig(&cfg, encCfg), memory.NewGoAllocator())
	require.NoError(t, err)
	require.Nil(t, engine.CipherEngine(), "plaintext engine must not have a cipher")

	backend, err := storage.NewStorageBackend(filepath.Join(t.TempDir(), "spill.bin"), false, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	_, encrypted := backend.(*storage.EncryptedStorageBackend)
	require.False(t, encrypted, "disabled encryption must preserve the plaintext backend")
}

// TestStorageEncryptionWiringEnabled verifies the configured local KMS key
// reaches both the storage engine and backends created for disk spill.
func TestStorageEncryptionWiringEnabled(t *testing.T) {
	cfg := encryptionTestConfig()
	cfg.DataPath = t.TempDir()
	cfg.EncryptionAtRest = true
	cfg.KMSMasterKey = testMasterKeyHex()
	require.NoError(t, ValidateConfig(&cfg))

	encCfg, restore, err := setupStorageEncryption(context.Background(), &cfg, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(restore)
	require.True(t, encCfg.Enabled)
	require.Equal(t, "local", encCfg.NormalizedProvider())

	engine, err := storage.NewStorageEngine(buildStorageConfig(&cfg, encCfg), memory.NewGoAllocator())
	require.NoError(t, err)
	ce := engine.CipherEngine()
	require.NotNil(t, ce, "encrypted engine must have a cipher")
	sealed, err := ce.Seal([]byte("wiring-check"), []byte("aad"))
	require.NoError(t, err)
	opened, err := ce.Open(context.Background(), sealed, []byte("aad"))
	require.NoError(t, err)
	require.Equal(t, []byte("wiring-check"), opened)

	spillPath := filepath.Join(t.TempDir(), "spill.bin")
	backend, err := storage.NewStorageBackend(spillPath, false, false)
	require.NoError(t, err)
	_, encrypted := backend.(*storage.EncryptedStorageBackend)
	require.True(t, encrypted, "enabled encryption must wrap spill backends")

	secret := []byte("secret-vector-payload")
	n, err := backend.WriteAt(secret, 0)
	require.NoError(t, err)
	require.Equal(t, len(secret), n)
	require.NoError(t, backend.Sync())
	got := make([]byte, len(secret))
	_, err = backend.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, secret, got)
	require.NoError(t, backend.Close())

	onDisk, err := os.ReadFile(spillPath)
	require.NoError(t, err)
	require.NotContains(t, string(onDisk), string(secret))
}

// TestStorageEncryptionWiringInvalid ensures misconfiguration fails early.
func TestStorageEncryptionWiringInvalid(t *testing.T) {
	cfg := encryptionTestConfig()
	cfg.DataPath = t.TempDir()
	cfg.EncryptionAtRest = true

	err := ValidateConfig(&cfg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrInvalidEncryption))
	require.True(t, errors.Is(err, storage.ErrEncryptionMasterKeyRequired))

	encCfg, restore, err := setupStorageEncryption(context.Background(), &cfg, zerolog.Nop())
	require.Error(t, err)
	require.Nil(t, restore)
	require.True(t, encCfg.Enabled)

	cfg.KMSMasterKey = testMasterKeyHex()
	cfg.KMSProvider = "unsupported-kms"
	err = ValidateConfig(&cfg)
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrUnsupportedKMSProvider))
	require.NotContains(t, err.Error(), cfg.KMSMasterKey)
}
