package main

import (
	"context"
	"fmt"

	"github.com/23skdu/longbow/internal/storage"
	"github.com/rs/zerolog"
)

// storageEncryptionConfig maps LONGBOW_* settings onto the storage-layer
// encryption configuration. The master key is carried in memory only.
func storageEncryptionConfig(cfg *Config) storage.EncryptionConfig {
	return storage.EncryptionConfig{
		Enabled:   cfg.EncryptionAtRest,
		Provider:  cfg.KMSProvider,
		MasterKey: cfg.KMSMasterKey,
	}
}

// setupStorageEncryption validates the encryption settings, provisions the
// cipher engine and registers it for storage backends created outside the
// storage engine (disk vector spill). The returned function restores the prior
// process-wide setting; the plaintext default is untouched when disabled.
func setupStorageEncryption(ctx context.Context, cfg *Config, logger zerolog.Logger) (storage.EncryptionConfig, func(), error) {
	encCfg := storageEncryptionConfig(cfg)
	if err := encCfg.Validate(); err != nil {
		return encCfg, nil, fmt.Errorf("%w: %w", ErrInvalidEncryption, err)
	}
	if !encCfg.Enabled {
		return encCfg, func() {}, nil
	}

	ce, err := storage.NewCipherEngineFromConfig(ctx, encCfg)
	if err != nil {
		return encCfg, nil, fmt.Errorf("%w: %w", ErrInvalidEncryption, err)
	}
	restore := storage.SetDefaultCipherEngine(ce)
	// Deliberately log only non-secret settings: never the master key or DEKs.
	logger.Info().
		Str("kms_provider", encCfg.NormalizedProvider()).
		Msg("Encryption at rest enabled")
	return encCfg, restore, nil
}

// buildStorageConfig assembles the persistence settings, including the
// encryption settings consumed by the storage engine for WAL and snapshots.
func buildStorageConfig(cfg *Config, enc storage.EncryptionConfig) storage.StorageConfig {
	return storage.StorageConfig{
		DataPath:         cfg.DataPath,
		SnapshotInterval: cfg.SnapshotInterval,
		AsyncFsync:       cfg.StorageAsyncFsync,
		DoPutBatchSize:   cfg.StorageDoPutBatchSize,
		UseIOUring:       cfg.StorageUseIOUring,
		UseDirectIO:      cfg.StorageUseDirectIO,
		Encryption:       enc,
	}
}
