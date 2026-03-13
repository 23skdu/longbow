package storage

import (
	"fmt"
	"os"
	"sync"
	"unsafe"

	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

type MmapVectorStorage struct {
	file      *os.File
	data      []byte
	mu        sync.RWMutex
	dimension int
	count     int
	size      int64
	readonly  bool
	logger    *zerolog.Logger
}

type MmapOptions struct {
	Dimension   int
	InitialSize int64
	ReadOnly    bool
	Populate    bool
	Directory   string
}

func NewMmapVectorStorage(name string, opts MmapOptions) (*MmapVectorStorage, error) {
	if opts.InitialSize <= 0 {
		opts.InitialSize = 64 * 1024 * 1024
	}

	flags := os.O_RDWR
	if opts.ReadOnly {
		flags = os.O_RDONLY
	}

	flags |= os.O_CREATE

	f, err := os.OpenFile(name, flags, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close() // nosec G104
		return nil, err
	}

	size := stat.Size()
	if size < opts.InitialSize {
		size = opts.InitialSize
		if !opts.ReadOnly {
			if err := f.Truncate(size); err != nil {
				_ = f.Close() // nosec G104
				return nil, err
			}
		}
	}

	mmap, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close() // nosec G104
		return nil, fmt.Errorf("mmap failed: %w", err)
	}

	mvs := &MmapVectorStorage{
		file:      f,
		data:      mmap,
		dimension: opts.Dimension,
		size:      size,
		readonly:  opts.ReadOnly,
		logger:    &zerolog.Logger{},
	}

	if opts.Dimension > 0 {
		mvs.count = int(size) / (opts.Dimension * 4)
	}

	return mvs, nil
}

func (m *MmapVectorStorage) WriteVector(id int, vector []float32) error {
	if m.readonly {
		return fmt.Errorf("cannot write to read-only storage")
	}

	offset := id * m.dimension * 4
	if offset+len(vector)*4 > len(m.data) {
		newSize := int64(offset + len(vector)*4)
		if err := m.grow(newSize); err != nil {
			return err
		}
	}

	dst := m.data[offset:]
	for i, v := range vector {
		*(*float32)(unsafe.Pointer(&dst[i*4])) = v
	}

	m.mu.Lock()
	if id >= m.count {
		m.count = id + 1
	}
	m.mu.Unlock()

	return nil
}

func (m *MmapVectorStorage) ReadVector(id int) ([]float32, error) {
	offset := id * m.dimension * 4
	if offset >= len(m.data) {
		return nil, fmt.Errorf("vector not found at id %d", id)
	}

	vector := make([]float32, m.dimension)
	src := m.data[offset : offset+m.dimension*4]

	for i := 0; i < m.dimension; i++ {
		vector[i] = *(*float32)(unsafe.Pointer(&src[i*4]))
	}

	return vector, nil
}

func (m *MmapVectorStorage) ReadVectorInto(id int, vector []float32) error {
	offset := id * m.dimension * 4
	if offset >= len(m.data) {
		return fmt.Errorf("vector not found at id %d", id)
	}

	src := m.data[offset : offset+m.dimension*4]
	for i := 0; i < m.dimension && i < len(vector); i++ {
		vector[i] = *(*float32)(unsafe.Pointer(&src[i*4]))
	}

	return nil
}

func (m *MmapVectorStorage) grow(newSize int64) error {
	if newSize <= m.size {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if newSize <= m.size {
		return nil
	}

	if err := unix.Munmap(m.data); err != nil {
		return fmt.Errorf("munmap failed: %w", err)
	}

	if err := m.file.Truncate(newSize); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}

	mmap, err := unix.Mmap(int(m.file.Fd()), 0, int(newSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}

	m.data = mmap
	m.size = newSize

	return nil
}

func (m *MmapVectorStorage) Sync() error {
	if m.readonly {
		return nil
	}

	if err := unix.Msync(m.data, unix.MS_SYNC); err != nil {
		return fmt.Errorf("msync failed: %w", err)
	}

	return nil
}

func (m *MmapVectorStorage) Close() error {
	if err := unix.Munmap(m.data); err != nil {
		return fmt.Errorf("munmap failed: %w", err)
	}

	return m.file.Close()
}

func (m *MmapVectorStorage) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

func (m *MmapVectorStorage) Size() int64 {
	return m.size
}

func (m *MmapVectorStorage) Dimension() int {
	return m.dimension
}
