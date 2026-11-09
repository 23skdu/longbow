//go:build linux
// +build linux

package benchmark

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/23skdu/longbow/internal/storage"
)

// FuzzWriteWALEntriesCount verifies the WAL entry emitter for fuzzed entry
// counts: writes must succeed, the encoded byte total must be deterministic
// across independent backends, an empty count must write nothing, and each
// additional entry must strictly grow the log.
func FuzzWriteWALEntriesCount(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(5)
	f.Add(25)
	f.Add(64)
	f.Add(-1)
	f.Add(1000)

	f.Fuzz(func(t *testing.T, count int) {
		if count < 0 || count > 64 {
			return
		}

		dir := t.TempDir()
		pathA := filepath.Join(dir, "wal-a.log")
		pathB := filepath.Join(dir, "wal-b.log")

		backendA, err := storage.NewFSBackend(pathA)
		if err != nil {
			t.Fatalf("create backend A: %v", err)
		}
		defer func() { _ = backendA.Close() }()

		backendB, err := storage.NewFSBackend(pathB)
		if err != nil {
			t.Fatalf("create backend B: %v", err)
		}
		defer func() { _ = backendB.Close() }()

		if err := writeWALEntries(backendA, count); err != nil {
			t.Fatalf("writeWALEntries(A, %d): %v", count, err)
		}
		if err := backendA.Sync(); err != nil {
			t.Fatalf("sync A: %v", err)
		}
		if err := writeWALEntries(backendB, count); err != nil {
			t.Fatalf("writeWALEntries(B, %d): %v", count, err)
		}
		if err := backendB.Sync(); err != nil {
			t.Fatalf("sync B: %v", err)
		}

		sizeA := fileSize(t, pathA)
		sizeB := fileSize(t, pathB)
		if sizeA != sizeB {
			t.Fatalf("encoding not deterministic: %d vs %d bytes for count=%d", sizeA, sizeB, count)
		}
		if count == 0 && sizeA != 0 {
			t.Fatalf("count=0 wrote %d bytes, want 0", sizeA)
		}
		if count > 0 && sizeA <= 0 {
			t.Fatalf("count=%d wrote %d bytes, want > 0", count, sizeA)
		}

		// One more entry must strictly grow the log (monotonic append).
		if err := writeWALEntries(backendA, 1); err != nil {
			t.Fatalf("writeWALEntries(A, 1): %v", err)
		}
		if err := backendA.Sync(); err != nil {
			t.Fatalf("sync A after growth: %v", err)
		}
		if grown := fileSize(t, pathA); grown <= sizeA {
			t.Fatalf("log did not grow: %d bytes after appending to %d bytes", grown, sizeA)
		}
	})
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Size()
}
