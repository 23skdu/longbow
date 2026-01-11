package storage

import (
	"testing"
)

// FuzzFSBackend_Write fuzzes the Write method of FSBackend with random bytes.
// It verifies that writes don't panic and return success for valid inputs (or aligned errors).
func FuzzFSBackend_Write(f *testing.F) {
	f.Add([]byte("some random data"))
	f.Add([]byte(""))
	f.Add([]byte{0x00, 0xff, 0xaa})

	f.Fuzz(func(t *testing.T, data []byte) {
		tmpFile := t.TempDir() + "/fuzz_fs.wal"
		backend, err := NewFSBackend(tmpFile)
		if err != nil {
			t.Fatalf("failed to create backend: %v", err)
		}
		defer backend.Close()

		n, err := backend.Write(data)
		if err != nil {
			// Write error is acceptable (e.g. disk full, permission), but mostly on temp dir it should succeed.
			// However simple write shouldn't fail unless IO error.
			// We track if it panics.
			return
		}

		if n != len(data) {
			t.Errorf("expected write len %d, got %d", len(data), n)
		}

		_ = backend.Sync()
	})
}
