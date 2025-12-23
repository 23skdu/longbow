package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// WALIterator iterators over WAL entries, supporting seeking by Sequence ID.
type WALIterator struct {
	path   string
	f      *os.File
	mem    memory.Allocator
	closed bool
	mu     sync.Mutex
}

func NewWALIterator(dir string, mem memory.Allocator) (*WALIterator, error) {
	path := filepath.Join(dir, walFileName)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &WALIterator{
		path: path,
		f:    f,
		mem:  mem,
	}, nil
}

// Seek fast-forwards the iterator to the first entry with Sequence > seq.
func (it *WALIterator) Seek(seq uint64) error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if _, err := it.f.Seek(0, 0); err != nil {
		return err
	}

	header := make([]byte, 32)
	for {
		// Remember start of record
		startPos, err := it.f.Seek(0, 1)
		if err != nil {
			return err
		}

		if _, err := io.ReadFull(it.f, header); err != nil {
			if err == io.EOF {
				return nil // End of file, no more entries
			}
			return err
		}

		entrySeq := binary.LittleEndian.Uint64(header[4:12])
		nameLen := binary.LittleEndian.Uint32(header[20:24])
		recLen := binary.LittleEndian.Uint64(header[24:32])

		if entrySeq > seq {
			// Found it! Rewind to start of this entry so Next() picks it up.
			if _, err := it.f.Seek(startPos, 0); err != nil {
				return err
			}
			return nil
		}

		// Skip body (Name + Record)
		if _, err := it.f.Seek(int64(nameLen)+int64(recLen), 1); err != nil {
			return err
		}
	}
}

// Next reads the next entry. Returns io.EOF if done.
func (it *WALIterator) Next() (uint64, int64, string, arrow.RecordBatch, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	header := make([]byte, 32)
	if _, err := io.ReadFull(it.f, header); err != nil {
		return 0, 0, "", nil, err
	}

	seq := binary.LittleEndian.Uint64(header[4:12])
	ts := int64(binary.LittleEndian.Uint64(header[12:20]))
	nameLen := binary.LittleEndian.Uint32(header[20:24])
	recLen := binary.LittleEndian.Uint64(header[24:32])

	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(it.f, nameBytes); err != nil {
		return 0, 0, "", nil, err // corrupt
	}
	name := string(nameBytes)

	recBytes := make([]byte, recLen)
	if _, err := io.ReadFull(it.f, recBytes); err != nil {
		return 0, 0, "", nil, err // corrupt
	}

	// Deserialize
	r, err := ipc.NewReader(bytes.NewReader(recBytes), ipc.WithAllocator(it.mem))
	if err != nil {
		return 0, 0, "", nil, err
	}
	defer r.Release()

	if r.Next() {
		rec := r.RecordBatch()
		rec.Retain()
		return seq, ts, name, rec, nil
	}

	if r.Err() != nil {
		return 0, 0, "", nil, r.Err()
	}

	return 0, 0, "", nil, fmt.Errorf("empty record in WAL")
}

func (it *WALIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed {
		return nil
	}
	it.closed = true
	return it.f.Close()
}
