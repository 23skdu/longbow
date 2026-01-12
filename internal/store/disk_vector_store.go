package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"

	"github.com/23skdu/longbow/internal/storage"
)

// DiskVectorStore provides mmap-backed append-only vector storage.
type DiskVectorStore struct {
	f    *os.File
	data []byte // mmap region
	dim  int

	mu sync.RWMutex
}

// NewDiskVectorStore opens or creates a disk vector store at path.
// dim is the vector dimension (float32 elements).
func NewDiskVectorStore(path string, dim int) (*DiskVectorStore, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat failed: %w", err)
	}
	size := fi.Size()

	d := &DiskVectorStore{
		f:   f,
		dim: dim,
	}

	if size > 0 {
		if err := d.mmap(size); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return d, nil
}

func (d *DiskVectorStore) mmap(size int64) error {
	data, err := syscall.Mmap(int(d.f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}
	d.data = data
	return nil
}

// Advise provides memory management hints to the kernel.
// Advice is one of the syscall.MADV_* constants (e.g., MADV_RANDOM, MADV_SEQUENTIAL).
func (d *DiskVectorStore) Advise(advice int) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.data == nil {
		return nil
	}

	_, _, errno := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&d.data[0])), uintptr(len(d.data)), uintptr(advice))
	if errno != 0 {
		return fmt.Errorf("madvise failed: %w", errno)
	}
	return nil
}

// SnapshotTo uploads the underlying file to the snapshot backend.
// It uses .bin extension for consistency.
func (d *DiskVectorStore) SnapshotTo(ctx context.Context, backend storage.SnapshotBackend, name string) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Ensure everything is flushed to disk before reading
	// (Though since we use MAP_SHARED, mmap should be coherent with file reads,
	// but explicit Sync is safer)
	if err := d.f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file before snapshot: %w", err)
	}

	// We open a separate read handle to stream the file to avoid race with munmap/remap logic
	// But append only happens with Lock. RLock ensures no remap happens during snapshot.
	// We can just Seek to 0 and stream d.f?
	// But d.f is open RDWR.

	// Better to open a fresh read-only handle or use Seek(0,0) carefully.
	// Since we hold RLock, Appends are blocked.
	// We need to restore position if we change it?
	// d.f is shared.
	// Safer to open the path again. Or use Pread if backend supported it (it takes Reader).

	// Let's open the file path again.
	path := d.f.Name()
	fRaw, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file for snapshot reading: %w", err)
	}
	defer fRaw.Close()

	return backend.WriteSnapshotFile(ctx, name, ".bin", fRaw)
}

// RestoreDiskVectorStore downloads the vector file and returns a new store.
// This is a helper function as it creates the store.
func RestoreDiskVectorStore(ctx context.Context, backend storage.SnapshotBackend, name, destPath string) error {
	r, err := backend.ReadSnapshotFile(ctx, name, ".bin")
	if err != nil {
		return err
	}
	defer r.Close()

	// Ensure dir exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return err
	}

	return nil
}

// Size returns the number of vectors stored.
func (d *DiskVectorStore) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.data == nil {
		return 0
	}
	bytesPerVec := d.dim * 4
	return len(d.data) / bytesPerVec
}

// Append adds a vector to the end of the file and returns its ID (index).
func (d *DiskVectorStore) Append(vec []float32) (uint32, error) {
	ids, err := d.BatchAppend([][]float32{vec})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// BatchAppend adds multiple vectors to the end of the file efficiently.
func (d *DiskVectorStore) BatchAppend(vecs [][]float32) ([]uint32, error) {
	if len(vecs) == 0 {
		return nil, nil
	}

	for _, vec := range vecs {
		if len(vec) != d.dim {
			return nil, fmt.Errorf("vector dimension mismatch: got %d, want %d", len(vec), d.dim)
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	bytesPerVec := d.dim * 4
	totalBytes := int64(len(vecs) * bytesPerVec)
	currentSize := int64(0)
	if d.data != nil {
		currentSize = int64(len(d.data))
	}

	// Unmap old
	if d.data != nil {
		if err := syscall.Munmap(d.data); err != nil {
			return nil, fmt.Errorf("unmap failed: %w", err)
		}
		d.data = nil
	}

	// Write all vectors at once
	buf := make([]byte, totalBytes)
	for i, vec := range vecs {
		offset := i * bytesPerVec
		for j, v := range vec {
			binary.LittleEndian.PutUint32(buf[offset+j*4:], *(*uint32)(unsafe.Pointer(&v)))
		}
	}

	if _, err := d.f.WriteAt(buf, currentSize); err != nil {
		return nil, fmt.Errorf("write failed: %w", err)
	}

	// Re-map with new size
	newSize := currentSize + totalBytes
	if err := d.mmap(newSize); err != nil {
		return nil, err
	}

	ids := make([]uint32, len(vecs))
	for i := range vecs {
		ids[i] = uint32((currentSize + int64(i*bytesPerVec)) / int64(bytesPerVec))
	}
	return ids, nil
}

// Get retrieves a vector by ID.
func (d *DiskVectorStore) Get(id uint32) ([]float32, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	bytesPerVec := d.dim * 4
	offset := int(id) * bytesPerVec

	if d.data == nil || offset+bytesPerVec > len(d.data) {
		return nil, fmt.Errorf("vector id %d out of bounds", id)
	}

	vec := make([]float32, d.dim)
	src := d.data[offset : offset+bytesPerVec]

	// Use unsafe to interpret src as []float32, then copy to vec.
	srcFloats := unsafe.Slice((*float32)(unsafe.Pointer(&src[0])), d.dim)
	copy(vec, srcFloats)

	return vec, nil
}

// Close unmaps and closes the file.
func (d *DiskVectorStore) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.data != nil {
		if err := syscall.Munmap(d.data); err != nil {
			return err
		}
		d.data = nil
	}
	return d.f.Close()
}
