//go:build linux && iouring

package storage

import (
	"os"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/iceber/iouring-go"
)

// UringBackend implements WALBackend using io_uring.
type UringBackend struct {
	f      *os.File
	ring   *iouring.IOURing
	offset int64
	mu     sync.Mutex
	path   string
}

func NewUringBackend(path string) (WALBackend, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	// Initialize io_uring with reasonable queue depth
	ring, err := iouring.New(2048)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &UringBackend{
		f:      f,
		ring:   ring,
		offset: stat.Size(),
		path:   path,
	}, nil
}

func (b *UringBackend) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Metrics
	start := time.Now()

	// Prepare Pwrite request
	// Note: We use Pwrite to be explicit about offset.
	req, err := b.ring.SubmitRequest(iouring.Pwrite(int(b.f.Fd()), p, uint64(b.offset)), nil)
	if err != nil {
		metrics.WalUringSubmitLatencySeconds.Observe(time.Since(start).Seconds())
		return 0, err
	}

	// Wait for completion
	<-req.Done()

	duration := time.Since(start).Seconds()
	metrics.WalUringSubmitLatencySeconds.Observe(duration)

	n, err := req.ReturnInt()
	if err != nil {
		return 0, err
	}

	if n > 0 {
		b.offset += int64(n)
		metrics.WalWritesTotal.WithLabelValues("uring_ok").Inc()
	} else {
		metrics.WalWritesTotal.WithLabelValues("uring_zero").Inc()
	}

	return n, nil
}

func (b *UringBackend) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	start := time.Now()
	req, err := b.ring.SubmitRequest(iouring.Fsync(int(b.f.Fd())), nil)
	if err != nil {
		metrics.WalFsyncDurationSeconds.WithLabelValues("uring_error").Observe(time.Since(start).Seconds())
		return err
	}
	<-req.Done()
	metrics.WalFsyncDurationSeconds.WithLabelValues("uring_ok").Observe(time.Since(start).Seconds())

	if err := req.Err(); err != nil {
		return err
	}
	return nil
}

func (b *UringBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var err error
	if b.ring != nil {
		if e := b.ring.Close(); e != nil {
			err = e
		}
	}
	if e := b.f.Close(); e != nil && err == nil {
		err = e
	}
	return err
}

func (b *UringBackend) Name() string {
	return b.path
}

func (b *UringBackend) File() *os.File {
	return b.f
}
