package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
)

const (
	// RemoteEncryptionAADVersion is the AAD schema version for encrypted remote objects.
	RemoteEncryptionAADVersion = 1
)

// RemoteEncryptionContext carries tenant/object identity bound into AAD. Values may
// also be supplied through context keys when Put/Get are called.
type RemoteEncryptionContext struct {
	Bucket        string
	ObjectKey     string
	Dataset       string
	Namespace     string
	BackupVersion string
}

type remoteEncryptionContextKey struct{}

// WithRemoteEncryptionContext attaches identity metadata to ctx for AAD binding.
func WithRemoteEncryptionContext(ctx context.Context, rec RemoteEncryptionContext) context.Context {
	return context.WithValue(ctx, remoteEncryptionContextKey{}, rec)
}

// RemoteEncryptionOptions configures an EncryptedRemoteStorage decorator.
type RemoteEncryptionOptions struct {
	Bucket        string
	Dataset       string
	Namespace     string
	BackupVersion string
	AADVersion    int
	// ProviderManagedKeyID requests provider-side encryption (SSE-KMS/CMEK)
	// in addition to Longbow client-side envelope encryption.
	ProviderManagedKeyID string
}

// EncryptedRemoteStorage seals every object with storage.CipherEngine before
// delegating to inner and opens it on Get.
type EncryptedRemoteStorage struct {
	inner    RemoteStorage
	ce       *CipherEngine
	opts     RemoteEncryptionOptions
	provider string
}

var _ RemoteStorage = (*EncryptedRemoteStorage)(nil)

// NewEncryptedRemoteStorage wraps inner with client-side envelope encryption.
func NewEncryptedRemoteStorage(inner RemoteStorage, ce *CipherEngine, opts ...RemoteEncryptionOptions) (*EncryptedRemoteStorage, error) {
	if inner == nil {
		return nil, errors.New("storage: remote storage is required")
	}
	if ce == nil {
		return nil, errors.New("storage: cipher engine is required")
	}
	var o RemoteEncryptionOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.AADVersion <= 0 {
		o.AADVersion = RemoteEncryptionAADVersion
	}
	provider := "remote"
	if b, ok := inner.(interface{ Provider() string }); ok {
		provider = b.Provider()
	}
	return &EncryptedRemoteStorage{inner: inner, ce: ce, opts: o, provider: provider}, nil
}

// Provider returns a stable provider label for metrics.
func (e *EncryptedRemoteStorage) Provider() string { return e.provider }

func (e *EncryptedRemoteStorage) bucketFor(ctx context.Context, key string) string {
	if v, ok := ctx.Value(remoteEncryptionContextKey{}).(RemoteEncryptionContext); ok && v.Bucket != "" {
		return v.Bucket
	}
	if e.opts.Bucket != "" {
		return e.opts.Bucket
	}
	if b, ok := e.inner.(interface{ Bucket() string }); ok {
		return b.Bucket()
	}
	return ""
}

func (e *EncryptedRemoteStorage) aadFor(ctx context.Context, key string) []byte {
	rec := RemoteEncryptionContext{}
	if v, ok := ctx.Value(remoteEncryptionContextKey{}).(RemoteEncryptionContext); ok {
		rec = v
	}
	bucket := rec.Bucket
	if bucket == "" {
		bucket = e.bucketFor(ctx, key)
	}
	dataset := rec.Dataset
	if dataset == "" {
		dataset = e.opts.Dataset
	}
	namespace := rec.Namespace
	if namespace == "" {
		namespace = e.opts.Namespace
	}
	version := rec.BackupVersion
	if version == "" {
		version = e.opts.BackupVersion
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "longbow-remote-encryption-v%d\x00", e.opts.AADVersion)
	fmt.Fprintf(&b, "bucket=%s\x00", bucket)
	fmt.Fprintf(&b, "key=%s\x00", key)
	fmt.Fprintf(&b, "dataset=%s\x00", dataset)
	fmt.Fprintf(&b, "namespace=%s\x00", namespace)
	fmt.Fprintf(&b, "backup_version=%s\x00", version)
	return b.Bytes()
}

func (e *EncryptedRemoteStorage) Put(ctx context.Context, key string, r io.Reader) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	sealed, err := e.ce.Seal(raw, e.aadFor(ctx, key))
	if err != nil {
		return err
	}
	return e.inner.Put(ctx, key, bytes.NewReader(sealed))
}

func (e *EncryptedRemoteStorage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := e.inner.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	sealed, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	plain, err := e.ce.Open(ctx, sealed, e.aadFor(ctx, key))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(plain)), nil
}

func (e *EncryptedRemoteStorage) Delete(ctx context.Context, key string) error {
	return e.inner.Delete(ctx, key)
}

func (e *EncryptedRemoteStorage) Exists(ctx context.Context, key string) (bool, error) {
	return e.inner.Exists(ctx, key)
}
