package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
)

var (
	ErrVectorNotFound = errors.New("vector not found")
	ErrInvalidID      = errors.New("invalid vector ID")
)

// DiskVectorStore manages append-only storage of vectors on disk.
type DiskVectorStore struct {
	mu       sync.RWMutex
	f        *os.File
	dim      int // Changed from dims to dim
	count    uint32
	basePath string
}

// NewDiskVectorStore creates or opens a DiskVectorStore.
// It assumes the file contains only the dense vector data (no header for now, or minimal).
// To verify integrity we simply check if file size % (dim*4) == 0.
func NewDiskVectorStore(path string, dim int) (*DiskVectorStore, error) { // Changed dims to dim
	if dim <= 0 { // Changed dims to dim
		return nil, errors.New("dim must be > 0") // Changed dims to dim
	}

	flags := os.O_RDWR | os.O_CREATE // | os.O_APPEND managed manually for Append
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	size := fi.Size()
	stride := int64(dim * 4) // Changed dims to dim
	if size%stride != 0 {
		_ = f.Close()
		return nil, fmt.Errorf("file size %d is not a multiple of stride %d", size, stride)
	}

	count := uint32(size / stride)

	return &DiskVectorStore{
		f:        f,
		dim:      dim, // Changed dims to dim
		count:    count,
		basePath: path,
	}, nil
}

// Append adds a vector to the store.
// Returns the ID of the appended vector.
func (s *DiskVectorStore) Append(vector []float32) (uint32, error) { // Changed signature
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f == nil {
		return 0, errors.New("closed")
	}

	if len(vector) != s.dim { // Changed s.dims to s.dim
		return 0, fmt.Errorf("vector dim mismatch: got %d, want %d", len(vector), s.dim) // Changed s.dims to s.dim
	}

	id := s.count // New: get current count as ID

	// Prepare buffer
	// Avoid alloc if possible? For now, simple alloc is fine compared to IO.
	// Or use unsafe slice cast.
	// Write at end of file.

	// Convert []float32 to []byte
	// 4 bytes per float
	// LittleEndian by default on most modern systems, but let's be safe or fast.
	// Since we read back on same machine usually, native order is fine?
	// Standard persistence usually demands LittleEndian.

	// Fast unsafe cast for write (Native Endian)
	// If we want to be arch-safe, manual loop.
	// Let's use binary.Write? It does reflection, slow.
	// Manual buffer is best.

	// Since we use LittleEndian in Get, we must use LittleEndian here.
	buf := make([]byte, s.dim*4) // Changed s.dims to s.dim
	for i, v := range vector {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(v))
	}

	offset := int64(id) * int64(s.dim) * 4 // Changed s.count to id, s.dims to s.dim

	// WriteAt is safer than Write (no seek races if multiple readers, though we hold Lock)
	if _, err := s.f.WriteAt(buf, offset); err != nil {
		return 0, err // Changed return
	}

	s.count++
	return id, nil // Changed return
}

// BatchAppend adds multiple vectors to the store.
// Returns the ID of the first vector in the batch.
func (s *DiskVectorStore) BatchAppend(vectors [][]float32) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.f == nil {
		return 0, errors.New("closed")
	}

	if len(vectors) == 0 {
		return s.count, nil
	}

	startID := s.count
	totalFloats := len(vectors) * s.dim
	buf := make([]byte, totalFloats*4)

	// Flatten
	idx := 0
	for _, vec := range vectors {
		if len(vec) != s.dim {
			return 0, fmt.Errorf("vector dim mismatch in batch")
		}
		for _, v := range vec {
			binary.LittleEndian.PutUint32(buf[idx:], math.Float32bits(v))
			idx += 4
		}
	}

	offset := int64(startID) * int64(s.dim) * 4
	if _, err := s.f.WriteAt(buf, offset); err != nil {
		return 0, err
	}

	s.count += uint32(len(vectors))
	return startID, nil
}

// Count returns the number of vectors stored.
func (s *DiskVectorStore) Count() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// Get retrieves a vector by ID.
func (s *DiskVectorStore) Get(id uint32) ([]float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.f == nil {
		return nil, errors.New("closed")
	}

	if id >= s.count {
		return nil, ErrVectorNotFound
	}

	offset := int64(id) * int64(s.dim) * 4 // Changed s.dims to s.dim
	buf := make([]byte, s.dim*4)           // Changed s.dims to s.dim

	// ReadAt allows concurrent reads
	if _, err := s.f.ReadAt(buf, offset); err != nil {
		return nil, err
	}

	vec := make([]float32, s.dim) // Changed s.dims to s.dim
	for i := 0; i < s.dim; i++ {  // Changed s.dims to s.dim
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[i*4:]))
	}

	return vec, nil
}

// Close closes the file handle.
func (s *DiskVectorStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		err := s.f.Close()
		s.f = nil
		return err
	}
	return nil
}

// Advise hints the OS about access patterns.
// Currently a no-op as we use Pread, but maintained for interface compatibility.
func (s *DiskVectorStore) Advise(advice int) error {
	// TODO: Maybe implement Fadvise if critical?
	return nil
}

// SnapshotTo writes the current content of the store to the backend.
// We only write the file content.
func (s *DiskVectorStore) SnapshotTo(ctx context.Context, backend interface{}, name string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.f == nil {
		return errors.New("closed")
	}

	// We need to sync to disk before snapshotting?
	if err := s.f.Sync(); err != nil {
		return err
	}

	// We rewind to start to read everything?
	// Or we open a new reader on the file path to avoid seeking the main handle?
	// Opening a new handle is safer.

	rf, err := os.Open(s.basePath)
	if err != nil {
		return err
	}
	defer func() { _ = rf.Close() }()

	// Use reflection or interface assertion for the backend method
	// The test expects WriteSnapshotFile(ctx, name, ext, reader)
	// We assume backend has this method.
	type snapshotWriter interface {
		WriteSnapshotFile(ctx context.Context, name, ext string, r io.Reader) error
	}

	if sw, ok := backend.(snapshotWriter); ok {
		return sw.WriteSnapshotFile(ctx, name, ".bin", rf)
	}
	return errors.New("backend does not support WriteSnapshotFile")
}

// RestoreDiskVectorStore restores a store from a snapshot.
func RestoreDiskVectorStore(ctx context.Context, backend interface{}, name, destPath string) error {
	type snapshotReader interface {
		ReadSnapshotFile(ctx context.Context, name, ext string) (io.ReadCloser, error)
	}

	sr, ok := backend.(snapshotReader)
	if !ok {
		return errors.New("backend does not support ReadSnapshotFile")
	}

	rc, err := sr.ReadSnapshotFile(ctx, name, ".bin")
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	// Ensure dest dir exists
	// os.MkdirAll(filepath.Dir(destPath), 0755)

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	}
	return nil
}
