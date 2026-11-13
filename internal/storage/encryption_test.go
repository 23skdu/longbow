package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/23skdu/longbow/internal/security"
	"github.com/stretchr/testify/require"
)

func TestEncryptedWALAndStorageAndSnapshot(t *testing.T) {
	ctx := context.Background()
	kp, err := security.NewLocalKeyProvider(nil)
	require.NoError(t, err)

	ce, err := NewCipherEngine(ctx, kp)
	require.NoError(t, err)

	dir := t.TempDir()

	// 1. WAL encryption at rest + DEK rotation
	walPath := filepath.Join(dir, "wal.enc")
	rawWAL, err := NewFSBackend(walPath)
	require.NoError(t, err)
	encWAL := NewEncryptedWALBackend(rawWAL, ce)

	_, err = encWAL.Write([]byte("record-1-sensitive-vector"))
	require.NoError(t, err)
	require.NoError(t, ce.RotateDEK(ctx))
	_, err = encWAL.Write([]byte("record-2-after-dek-rotate"))
	require.NoError(t, err)
	require.NoError(t, encWAL.Sync())
	require.NoError(t, encWAL.Close())

	rawBytes, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.NotContains(t, string(rawBytes), "sensitive-vector")

	frames, err := DecryptWALFrames(ctx, bytes.NewReader(rawBytes), ce)
	require.NoError(t, err)
	require.Len(t, frames, 2)
	require.Equal(t, []byte("record-1-sensitive-vector"), frames[0])
	require.Equal(t, []byte("record-2-after-dek-rotate"), frames[1])

	// 2. StorageBackend random positional encrypted I/O & tamper detection
	spillPath := filepath.Join(dir, "spill.enc")
	fsStore, err := NewFSStorageBackend(spillPath, false)
	require.NoError(t, err)
	encStore := NewEncryptedStorageBackend(fsStore, ce, 512)

	payload := bytes.Repeat([]byte("A1B2C3D4"), 150) // 1200 bytes across 3 pages
	n, err := encStore.WriteAt(payload, 100)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.NoError(t, encStore.Sync())

	got := make([]byte, len(payload))
	rn, err := encStore.ReadAt(got, 100)
	require.NoError(t, err)
	require.Equal(t, len(payload), rn)
	require.Equal(t, payload, got)
	require.NoError(t, encStore.Close())

	// Tamper with ciphertext on disk -> Open must reject
	diskBytes, err := os.ReadFile(spillPath)
	require.NoError(t, err)
	diskBytes[60] ^= 0xff
	require.NoError(t, os.WriteFile(spillPath, diskBytes, 0o600))

	fsStore2, err := NewFSStorageBackend(spillPath, false)
	require.NoError(t, err)
	defer func() { _ = fsStore2.Close() }()
	encStore2 := NewEncryptedStorageBackend(fsStore2, ce, 512)
	_, err = encStore2.ReadAt(got[:10], 100)
	require.Error(t, err)

	// 3. Encrypted snapshot backend
	fileSnap, err := NewFileSnapshotBackend(filepath.Join(dir, "snaps"))
	require.NoError(t, err)
	encSnap := NewEncryptedSnapshotBackend(fileSnap, ce)
	require.NoError(t, encSnap.WriteSnapshot(ctx, "ds_secret", []byte("snap-payload")))
	rc, err := encSnap.ReadSnapshot(ctx, "ds_secret")
	require.NoError(t, err)
	snapGot, err := io.ReadAll(rc)
	require.NoError(t, err)
	_ = rc.Close()
	require.Equal(t, []byte("snap-payload"), snapGot)
}

func FuzzEncryptDecryptChunk(f *testing.F) {
	f.Add([]byte("hello vector"), []byte("aad-1"))
	f.Add([]byte(""), []byte(""))
	f.Fuzz(func(t *testing.T, plaintext, aad []byte) {
		ctx := context.Background()
		kp, err := security.NewLocalKeyProvider(nil)
		if err != nil {
			t.Fatal(err)
		}
		ce, err := NewCipherEngine(ctx, kp)
		if err != nil {
			t.Fatal(err)
		}
		sealed, err := ce.Seal(plaintext, aad)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := ce.Open(ctx, sealed, aad)
		if err != nil {
			t.Fatalf("open failed: %v", err)
		}
		if !bytes.Equal(plaintext, opened) {
			t.Fatalf("mismatch after decrypt")
		}
	})
}
