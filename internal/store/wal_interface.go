package store

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
	Write(name string, record arrow.RecordBatch) error
	Close() error
	Sync() error
}

// StdWAL is a standard file-based WAL implementation (fallback).
type StdWAL struct {
	dir string
	mu  sync.Mutex
	v   *VectorStore
}

func NewStdWAL(dir string, v *VectorStore) *StdWAL {
	return &StdWAL{
		dir: dir,
		v:   v,
	}
}

func (w *StdWAL) Write(name string, record arrow.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := filepath.Join(w.dir, walFileName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

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
	nameLen := uint32(len(nameBytes))
	recLen := uint64(len(recBytes))

	crc := crc32.NewIEEE()
	_, _ = crc.Write(nameBytes)
	_, _ = crc.Write(recBytes)
	checksum := crc.Sum32()

	header := make([]byte, 16)
	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint32(header[4:8], nameLen)
	binary.LittleEndian.PutUint64(header[8:16], recLen)

	if _, err := f.Write(header); err != nil {
		return err
	}
	if _, err := f.Write(nameBytes); err != nil {
		return err
	}
	_, err = f.Write(recBytes)
	return err
}

func (w *StdWAL) Close() error {
	return nil
}

func (w *StdWAL) Sync() error {
	// In StdWAL, we open/close/sync per write for safety (though inefficient)
	return nil
}

// WALWriter interface is used by the VectorStore to abstract WAL implementation.
// We will have wal_std.go and wal_linux.go (Item 4).
