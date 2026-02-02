package storage

import (
	"context"
	"io"
)

// RemoteStorage defines the interface for cloud/remote storage (e.g., S3, GCS).
type RemoteStorage interface {
	Put(ctx context.Context, key string, r io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// StorageTier represents a tier in the storage hierarchy.
type StorageTier int

const (
	TierHot StorageTier = iota
	TierWarm
	TierCold
)

// TieredStorageConfig configures the tiered storage behavior.
type TieredStorageConfig struct {
	Remote        RemoteStorage
	HotWarmPolicy func(metadata any) bool // Returns true if it should be in Warm tier
	CacheSizeMB   int
}
