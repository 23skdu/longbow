//go:build linux

package store

import (
	"fmt"
	"os"
	"sync"
	"github.com/23skdu/longbow/internal/iouring"
)

// DiskWriterUring utilizes io_uring for high-performance, non-blocking snapshots.
type DiskWriterUring struct {
	f      *os.File
	ring   *iouring.Ring
	mu     sync.RWMutex
	active bool
}



func NewDiskWriterUring(path string) (*DiskWriterUring, error) {
	// 1. Open file with Direct I/O
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// 2. Initialize io_uring with 1024 depth
	ring, err := iouring.NewRing(1024, 0)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to init io_uring: %w", err)
	}

	return &DiskWriterUring{
		f:      f,
		ring:   ring,
		active: true,
	}, nil
}



// SubmitWrite handles an asynchronous write using io_uring SQE.
func (d *DiskWriterUring) SubmitWrite(data []byte, offset int64) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.active {
		return fmt.Errorf("disk writer inactive")
	}

	// 1. Submit write operation (non-blocking)
	err := d.ring.SubmitWrite(int(d.f.Fd()), data, uint64(offset), 0)
	if err != nil {
		return fmt.Errorf("iouring submit error: %w", err)
	}

	// 2. Flush and wait for completion
	_, err = d.ring.FlushAndWait(1, 0)
	if err != nil {
		return fmt.Errorf("iouring flush error: %w", err)
	}

	// 3. Peek and check completion
	cqe := d.ring.Peek()
	if cqe == nil {
		return fmt.Errorf("no completion available")
	}
	defer d.ring.Advance(1)

	if cqe.Res < 0 {
		return fmt.Errorf("async write failed: %d", cqe.Res)
	}

	return nil
}



func (d *DiskWriterUring) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active = false
	if d.ring != nil {
		d.ring.Close()
	}
	return d.f.Close()
}

