package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// WAL defines the interface for Write-Ahead Logging.
type WAL interface {
	Write(name string, seq uint64, ts int64, record arrow.RecordBatch) error
	Close() error
	Sync() error
}

// StdWAL is a standard file-based WAL implementation (fallback).
type StdWAL struct {
	dir    string
	file   *os.File
	mu     sync.Mutex
	cipher *CipherEngine
	seq    uint64
}

// NewWAL constructs a new WAL.
func NewWAL(dir string) WAL {
	return NewStdWAL(dir)
}

// NewEncryptedWAL constructs a WAL that seals each entry with the supplied engine.
func NewEncryptedWAL(dir string, ce *CipherEngine) WAL {
	return &StdWAL{dir: dir, cipher: ce}
}

// NewStdWAL constructs a new StdWAL.
func NewStdWAL(dir string) *StdWAL {
	return &StdWAL{
		dir: dir,
	}
}

func (w *StdWAL) getFileLocked() (*os.File, error) {
	if w.file != nil {
		return w.file, nil
	}
	path := filepath.Join(w.dir, walFileName)
	f, err := os.OpenFile(filepath.Clean(path), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) // #nosec G304
	if err != nil {
		return nil, err
	}
	w.file = f
	return w.file, nil
}

// Write runs the write operation.
func (w *StdWAL) Write(name string, seq uint64, ts int64, record arrow.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := w.getFileLocked()
	if err != nil {
		return err
	}

	// Use binary format from persistence.go for compatibility
	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	recBytes := buf.Bytes()
	nameBytes := []byte(name)
	nameLen := uint32(len(nameBytes)) // #nosec G115
	recLen := uint64(len(recBytes))

	crc := crc32.NewIEEE()
	_, _ = crc.Write(nameBytes)
	_, _ = crc.Write(recBytes)
	checksum := crc.Sum32()

	// Header: Checksum(4) | Seq(8) | Timestamp(8) | NameLen(4) | RecLen(8) = 32 bytes
	header := make([]byte, 32)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint64(header[4:12], seq)
	binary.LittleEndian.PutUint64(header[12:20], uint64(ts)) // #nosec G115
	binary.LittleEndian.PutUint32(header[20:24], nameLen)
	binary.LittleEndian.PutUint64(header[24:32], recLen)

	var entry bytes.Buffer
	entry.Write(header)
	entry.Write(nameBytes)
	entry.Write(recBytes)

	if w.cipher == nil {
		_, err := f.Write(entry.Bytes())
		return err
	}

	// Same frame format as EncryptedWALBackend so replay can decrypt both
	// the batched and direct WAL paths with DecryptWALFrames.
	w.seq++
	var aad [8]byte
	binary.LittleEndian.PutUint64(aad[:], w.seq)
	sealed, err := w.cipher.Seal(entry.Bytes(), aad[:])
	if err != nil {
		return err
	}
	var frame bytes.Buffer
	var frameHeader [12]byte
	binary.LittleEndian.PutUint64(frameHeader[0:8], w.seq)
	binary.LittleEndian.PutUint32(frameHeader[8:12], uint32(len(sealed))) // #nosec G115
	frame.Write(frameHeader[:])
	frame.Write(sealed)
	_, err = f.Write(frame.Bytes())
	return err
}

// Close runs the close operation.
func (w *StdWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		syncErr := w.file.Sync()
		closeErr := w.file.Close()
		w.file = nil
		if syncErr != nil {
			return syncErr
		}
		return closeErr
	}
	return nil
}

// Sync runs the sync operation.
func (w *StdWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Sync()
	}
	path := filepath.Join(w.dir, walFileName)
	f, err := os.OpenFile(filepath.Clean(path), os.O_RDWR, 0o600) // #nosec G304
	if err != nil {
		return nil // file may not exist yet
	}
	defer f.Close()
	return f.Sync()
}

// WALWriter interface is used by the VectorStore to abstract WAL implementation.
// We will have wal_std.go and wal_linux.go (Item 4).
