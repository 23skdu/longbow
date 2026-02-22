//go:build linux && iouring

package storage

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/iceber/iouring-go"
)

// UringStorageBackend implements StorageBackend using io_uring.
type UringStorageBackend struct {
	f    *os.File
	ring *iouring.IOURing
	mu   sync.Mutex
	path string
}

func NewUringStorageBackend(path string) (StorageBackend, error) {
	// Open for Read/Write
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Initialize io_uring with reasonable queue depth
	ring, err := iouring.New(2048)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &UringStorageBackend{
		f:    f,
		ring: ring,
		path: path,
	}, nil
}

func (b *UringStorageBackend) ReadAt(p []byte, off int64) (int, error) {
	start := time.Now()

	req, err := b.ring.SubmitRequest(iouring.Pread(int(b.f.Fd()), p, uint64(off)), nil)
	if err != nil {
		return 0, err
	}

	<-req.Done()
	metrics.IOReadLatencySeconds.WithLabelValues("disk_store_uring").Observe(time.Since(start).Seconds())

	n, err := req.ReturnInt()
	if err != nil {
		return 0, err
	}

	if n > 0 {
		metrics.IOReadBytesTotal.WithLabelValues("disk_store").Add(float64(n))
		metrics.IOReadOpsTotal.WithLabelValues("disk_store").Inc()
	}

	if n == 0 && len(p) > 0 {
		return 0, io.EOF
	}

	return n, nil
}

func (b *UringStorageBackend) WriteAt(p []byte, off int64) (int, error) {
	start := time.Now()

	req, err := b.ring.SubmitRequest(iouring.Pwrite(int(b.f.Fd()), p, uint64(off)), nil)
	if err != nil {
		return 0, err
	}

	<-req.Done()
	metrics.IOWriteLatencySeconds.WithLabelValues("disk_store_uring").Observe(time.Since(start).Seconds())

	n, err := req.ReturnInt()
	if err != nil {
		return 0, err
	}

	if n > 0 {
		metrics.IOWriteBytesTotal.WithLabelValues("disk_store").Add(float64(n))
		metrics.IOWriteOpsTotal.WithLabelValues("disk_store").Inc()
	}

	return n, nil
}

func (b *UringStorageBackend) Readv(iovs [][]byte, off int64) (int, error) {
	// Note: iouring-go might have a Readv/Writev specific helper or we use raw opcode.
	// Looking at common iouring wrappers, they often support multiple iovecs.
	// If not directly available as easy helper, we can use the low-level Submission Queue Entry.

	// For now, let's see if iouring.Readv exists in the library.
	// Actually, let's check iouring_linux.go in the library or assume it exists.
	// In many libraries it is iouring.Readv(fd, iovs, off).

	start := time.Now()

	// For prototyping, if Readv helper is missing, we use standard opcode logic via the library's SubmitRequest
	// In absence of certain docs, I'll use a loop of Preads if I'm unsure,
	// but the goal is "vectored I/O".

	// Actually, let's implement a loop of SubmitRequests but wait for them all at once?
	// That would be true async batching.

	reqs := make([]iouring.Request, len(iovs))
	currOff := off
	for i, buf := range iovs {
		req, err := b.ring.SubmitRequest(iouring.Pread(int(b.f.Fd()), buf, uint64(currOff)), nil)
		if err != nil {
			return 0, err
		}
		reqs[i] = req
		currOff += int64(len(buf))
	}

	total := 0
	for _, req := range reqs {
		<-req.Done()
		n, err := req.ReturnInt()
		if err != nil {
			return total, err
		}
		total += n
	}

	metrics.IOReadLatencySeconds.WithLabelValues("disk_store_uring_vectored").Observe(time.Since(start).Seconds())
	return total, nil
}

func (b *UringStorageBackend) Writev(iovs [][]byte, off int64) (int, error) {
	start := time.Now()
	reqs := make([]iouring.Request, len(iovs))
	currOff := off
	for i, buf := range iovs {
		req, err := b.ring.SubmitRequest(iouring.Pwrite(int(b.f.Fd()), buf, uint64(currOff)), nil)
		if err != nil {
			return 0, err
		}
		reqs[i] = req
		currOff += int64(len(buf))
	}

	total := 0
	for _, req := range reqs {
		<-req.Done()
		n, err := req.ReturnInt()
		if err != nil {
			return total, err
		}
		total += n
	}

	metrics.IOWriteLatencySeconds.WithLabelValues("disk_store_uring_vectored").Observe(time.Since(start).Seconds())
	return total, nil
}

func (b *UringStorageBackend) Sync() error {
	req, err := b.ring.SubmitRequest(iouring.Fsync(int(b.f.Fd())), nil)
	if err != nil {
		return err
	}
	<-req.Done()
	return req.Err()
}

func (b *UringStorageBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ring.Close()
	return b.f.Close()
}

func (b *UringStorageBackend) Size() (int64, error) {
	fi, err := b.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (b *UringStorageBackend) Name() string {
	return b.path
}
