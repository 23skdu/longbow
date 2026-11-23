package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func encryptionTestKeyHex() string {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	return hex.EncodeToString(key)
}

func TestEncryptionConfigValidate(t *testing.T) {
	t.Run("disabled ignores kms settings", func(t *testing.T) {
		cfg := EncryptionConfig{Provider: "bogus", MasterKey: ""}
		require.NoError(t, cfg.Validate())
	})

	t.Run("local requires a master key", func(t *testing.T) {
		cfg := EncryptionConfig{Enabled: true, Provider: "local"}
		err := cfg.Validate()
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrEncryptionMasterKeyRequired))
	})

	t.Run("unsupported provider", func(t *testing.T) {
		cfg := EncryptionConfig{Enabled: true, Provider: "vault", MasterKey: encryptionTestKeyHex()}
		err := cfg.Validate()
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrUnsupportedKMSProvider))
		require.NotContains(t, err.Error(), cfg.MasterKey)
	})

	t.Run("local with key", func(t *testing.T) {
		cfg := EncryptionConfig{Enabled: true, Provider: "local", MasterKey: encryptionTestKeyHex()}
		require.NoError(t, cfg.Validate())
	})
}

func TestNewStorageEngineEncryptionConfig(t *testing.T) {
	t.Run("disabled engine is plaintext", func(t *testing.T) {
		engine, err := NewStorageEngine(StorageConfig{DataPath: t.TempDir()}, memory.NewGoAllocator())
		require.NoError(t, err)
		require.Nil(t, engine.CipherEngine())
	})

	t.Run("enabled engine provisions a cipher", func(t *testing.T) {
		engine, err := NewStorageEngine(StorageConfig{
			DataPath: t.TempDir(),
			Encryption: EncryptionConfig{
				Enabled:   true,
				Provider:  "local",
				MasterKey: encryptionTestKeyHex(),
			},
		}, memory.NewGoAllocator())
		require.NoError(t, err)
		require.NotNil(t, engine.CipherEngine())
	})

	t.Run("invalid config fails", func(t *testing.T) {
		_, err := NewStorageEngine(StorageConfig{
			DataPath:   t.TempDir(),
			Encryption: EncryptionConfig{Enabled: true, Provider: "local"},
		}, memory.NewGoAllocator())
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrEncryptionMasterKeyRequired))
	})
}

func TestEncryptionConfigWiresWALAndSnapshots(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewStorageEngine(StorageConfig{
		DataPath: dir,
		Encryption: EncryptionConfig{
			Enabled:   true,
			Provider:  "local",
			MasterKey: encryptionTestKeyHex(),
		},
	}, memory.NewGoAllocator())
	require.NoError(t, err)
	require.NotNil(t, engine.CipherEngine())
	require.NoError(t, engine.InitWAL())

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "vector", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)},
	}, nil)
	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	builder.Field(0).(*array.Int32Builder).Append(7)
	vecBuilder := builder.Field(1).(*array.FixedSizeListBuilder)
	vecBuilder.Append(true)
	vecBuilder.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{1.5, 2.5}, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	require.NoError(t, engine.WriteToWAL("ds_encrypted", record, 1, 100))
	require.NoError(t, engine.SyncWAL())
	require.NoError(t, engine.Close())

	walBytes, err := os.ReadFile(filepath.Join(dir, walFileName))
	require.NoError(t, err)
	require.NotContains(t, string(walBytes), "ds_encrypted")

	applied := 0
	maxSeq, err := engine.ReplayWAL(func(name string, rec arrow.RecordBatch, seq uint64, ts int64) error {
		require.Equal(t, "ds_encrypted", name)
		require.Equal(t, uint64(1), seq)
		applied++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), maxSeq)
	require.Equal(t, 1, applied)

	// Snapshot files must also be sealed and loadable through the engine.
	secretPQ := []byte("secret-pq-codebook-value")
	secretConfig := []byte("secret-index-config-value")
	require.NoError(t, engine.CreateSnapshot(&SnapshotItem{
		Name:        "ds_encrypted",
		Records:     []arrow.RecordBatch{record},
		PQCodebook:  secretPQ,
		IndexConfig: secretConfig,
	}))
	tempDir := filepath.Join(dir, snapshotDirName+"_tmp")
	snapDir := filepath.Join(dir, snapshotDirName)
	require.NoError(t, os.Rename(tempDir, snapDir))

	for _, name := range []string{"ds_encrypted.parquet", "ds_encrypted.pq", "ds_encrypted.config"} {
		raw, err := os.ReadFile(filepath.Join(snapDir, name))
		require.NoError(t, err)
		require.NotContains(t, string(raw), "secret-", "snapshot file %s is plaintext", name)
	}

	loaded := 0
	require.NoError(t, engine.LoadSnapshots(func(item *SnapshotItem) error {
		loaded++
		require.Equal(t, "ds_encrypted", item.Name)
		require.Len(t, item.Records, 1)
		require.Equal(t, secretPQ, item.PQCodebook)
		require.Equal(t, secretConfig, item.IndexConfig)
		return nil
	}))
	require.Equal(t, 1, loaded)
}

func TestDefaultCipherEngineWrapsStorageBackend(t *testing.T) {
	require.Nil(t, DefaultCipherEngine())
	ce, err := NewCipherEngineFromConfig(context.Background(), EncryptionConfig{
		Enabled:   true,
		Provider:  "local",
		MasterKey: encryptionTestKeyHex(),
	})
	require.NoError(t, err)
	restore := SetDefaultCipherEngine(ce)
	t.Cleanup(restore)

	path := filepath.Join(t.TempDir(), "spill.bin")
	backend, err := NewStorageBackend(path, false, false)
	require.NoError(t, err)
	_, ok := backend.(*EncryptedStorageBackend)
	require.True(t, ok)
	require.NoError(t, backend.Close())
}
