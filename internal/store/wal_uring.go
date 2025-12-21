//go:build linux

package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/iceber/iouring-go"
)

// UringWAL is a Linux-specific WAL implementation using io_uring.
type UringWAL struct {
	dir      string
	mu       sync.Mutex
	file     *os.File
	ring     *iouring.IOURing
	offset   int64
	closed   bool
	useUring bool
}

// NewUringWAL creates a new WAL backed by io_uring.
func NewUringWAL(dir string, v *VectorStore) (*UringWAL, error) {
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	w := &UringWAL{
		dir:    dir,
		file:   f,
		offset: stat.Size(),
	}

	// Initialize io_uring
	ring, err := iouring.New(2048) // 2k entries
	if err == nil {
		w.ring = ring
		w.useUring = true
		fmt.Println("UringWAL: io_uring initialized successfully")
	} else {
		// Fallback to standard syscalls if uring fails (e.g. kernel too old)
		fmt.Printf("UringWAL: failed to init io_uring (%v), falling back to standard IO\n", err)
	}

	return w, nil
}

func (w *UringWAL) Write(name string, record arrow.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	// Serialize record
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

	// Header: Checksum (4) + NameLen (4) + RecLen (8) = 16 bytes
	header := make([]byte, 16)
	crc := crc32.NewIEEE()
	_, _ = crc.Write(nameBytes)
	_, _ = crc.Write(recBytes)
	checksum := crc.Sum32()

	binary.LittleEndian.PutUint32(header[0:4], checksum)
	binary.LittleEndian.PutUint32(header[4:8], nameLen)
	binary.LittleEndian.PutUint64(header[8:16], recLen)

	data := make([]byte, 0, 16+len(nameBytes)+len(recBytes))
	data = append(data, header...)
	data = append(data, nameBytes...)
	data = append(data, recBytes...)

	if w.useUring {
		// Prepare submission
		req, err := w.ring.SubmitRequest(iouring.Pwrite(int(w.file.Fd()), data, uint64(w.offset)), nil)
		if err != nil {
			return fmt.Errorf("uring submit failed: %w", err)
		}

		// Wait for completion
		<-req.Done()

		res, err := req.ReturnInt()
		if err != nil {
			return fmt.Errorf("uring write failed: %w", err)
		}

		if res < len(data) {
			return fmt.Errorf("short write: %d < %d", res, len(data))
		}

	} else {
		// Fallback
		if _, err := w.file.WriteAt(data, w.offset); err != nil {
			return err
		}
	}

	w.offset += int64(len(data))
	return nil
}

func (w *UringWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true

	var err error
	if w.ring != nil {
		if e := w.ring.Close(); e != nil {
			err = e
		}
	}
	if e := w.file.Close(); e != nil && err == nil {
		err = e
	}
	return err
}

func (w *UringWAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.useUring {
		// fsync via uring
		req, err := w.ring.SubmitRequest(iouring.Fsync(int(w.file.Fd())), nil)
		if err != nil {
			return err
		}
		<-req.Done()
		if err := req.Err(); err != nil {
			return err
		}
		return nil
	}
	return w.file.Sync()
}
