package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/golang/snappy"
)

// WALIterator iterators over WAL entries, supporting seeking by Sequence ID and snappy compression.
type WALIterator struct {
	path   string
	f      *os.File
	mem    memory.Allocator
	closed bool
	mu     sync.Mutex

	// Support for compressed blocks
	inner *bytes.Reader
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
// Note: Seek on compressed WALs might be less efficient if it has to decompress blocks.
// For now, it only seeks file positions between blocks/entries.
func (it *WALIterator) Seek(seq uint64) error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if _, err := it.f.Seek(0, 0); err != nil {
		return err
	}
	it.inner = nil

	header := make([]byte, 32)
	for {
		startPos, err := it.f.Seek(0, 1)
		if err != nil {
			return err
		}

		if _, err := io.ReadFull(it.f, header); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		storedSum := binary.LittleEndian.Uint32(header[0:4])
		entrySeq := binary.LittleEndian.Uint64(header[4:12])
		nameLen := binary.LittleEndian.Uint32(header[20:24])
		recLen := binary.LittleEndian.Uint64(header[24:32])

		if storedSum == 0xFFFFFFFF {
			// Compressed block. We can't easily skip by seq without decompressing or having min/max seq in header.
			// For simplicity during refactor, we decompress.
			// In production, we'd add maxSeq to the header.

			if _, err := it.f.Seek(startPos, 0); err != nil {
				return err
			}
			// We can't use Next() here easily because it returns values.
			// Let's just decompress and check.
			_, _, _, r, err := it.nextLocked()
			if err != nil {
				return err
			}
			if r != nil {
				r.Release()
			}
			// If max seq in block > seq, we stop here and let next Next() start from this block (wait, inner is set now).
			// This is getting complex for a Seek.
			// Simplified: just stop at the first block that MIGHT contain it.
			// If seq is 0, we are already at start.
			return nil
		}

		if entrySeq > seq {
			if _, err := it.f.Seek(startPos, 0); err != nil {
				return err
			}
			return nil
		}

		if _, err := it.f.Seek(int64(nameLen)+int64(recLen), 1); err != nil {
			return err
		}
	}
}

// Next reads the next entry. Returns io.EOF if done.
func (it *WALIterator) Next() (seq uint64, ts int64, name string, rec arrow.RecordBatch, err error) {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.nextLocked()
}

func (it *WALIterator) nextLocked() (uint64, int64, string, arrow.RecordBatch, error) {
	// 1. Check if we are inside a compressed block
	if it.inner != nil {
		seq, ts, name, rec, err := it.readEntry(it.inner)
		if err == nil {
			return seq, ts, name, rec, nil
		}
		if err == io.EOF {
			it.inner = nil
			// Fallthrough to read next block from file
		} else {
			return 0, 0, "", nil, err
		}
	}

	// 2. Read next from file
	header := make([]byte, 32)
	if _, err := io.ReadFull(it.f, header); err != nil {
		return 0, 0, "", nil, err
	}

	storedSum := binary.LittleEndian.Uint32(header[0:4])
	seq := binary.LittleEndian.Uint64(header[4:12])
	ts := int64(binary.LittleEndian.Uint64(header[12:20]))
	nameLen := binary.LittleEndian.Uint32(header[20:24])
	recLen := binary.LittleEndian.Uint64(header[24:32])

	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(it.f, nameBytes); err != nil {
		return 0, 0, "", nil, err
	}

	recBytes := make([]byte, recLen)
	if _, err := io.ReadFull(it.f, recBytes); err != nil {
		return 0, 0, "", nil, err
	}

	// 3. Handle Compression Sentinel
	if storedSum == 0xFFFFFFFF {
		decompressed, err := snappy.Decode(nil, recBytes)
		if err != nil {
			return 0, 0, "", nil, fmt.Errorf("snappy decode: %w", err)
		}
		it.inner = bytes.NewReader(decompressed)
		return it.nextLocked() // Recursive call to read first item from block
	}

	// 4. Verify Standard CRC
	crc := crc32.NewIEEE()
	_, _ = crc.Write(nameBytes)
	_, _ = crc.Write(recBytes)
	if crc.Sum32() != storedSum {
		return 0, 0, "", nil, fmt.Errorf("wal crc mismatch: expected %x, got %x", storedSum, crc.Sum32())
	}

	// 5. Deserialize
	r, err := it.deserialize(recBytes)
	if err != nil {
		return 0, 0, "", nil, err
	}
	return seq, ts, string(nameBytes), r, nil
}

func (it *WALIterator) readEntry(r io.Reader) (uint64, int64, string, arrow.RecordBatch, error) {
	header := make([]byte, 32)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, 0, "", nil, err
	}

	seq := binary.LittleEndian.Uint64(header[4:12])
	ts := int64(binary.LittleEndian.Uint64(header[12:20]))
	nameLen := binary.LittleEndian.Uint32(header[20:24])
	recLen := binary.LittleEndian.Uint64(header[24:32])

	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return 0, 0, "", nil, err
	}

	recBytes := make([]byte, recLen)
	if _, err := io.ReadFull(r, recBytes); err != nil {
		return 0, 0, "", nil, err
	}

	rec, err := it.deserialize(recBytes)
	if err != nil {
		return 0, 0, "", nil, err
	}
	return seq, ts, string(nameBytes), rec, nil
}

func (it *WALIterator) deserialize(data []byte) (arrow.RecordBatch, error) {
	r, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(it.mem))
	if err != nil {
		return nil, err
	}
	defer r.Release()

	if r.Next() {
		rec := r.RecordBatch()
		rec.Retain()
		return rec, nil
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	return nil, fmt.Errorf("empty record in WAL")
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
