package pool

import (
	"bytes"
	"sync"

	"github.com/23skdu/longbow/internal/metrics"
)

// BytePool pools bytes.Buffer instances to reduce allocation
// pressure in hot paths (WAL, serialization).
type BytePool struct {
	pool sync.Pool
}

// NewBytePool creates a new buffer pool.
func NewBytePool() *BytePool {
	return &BytePool{
		pool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get retrieves a buffer from the pool.
// The buffer is guaranteed to be empty (Reset called).
func (p *BytePool) Get() *bytes.Buffer {
	metrics.WalBufferPoolOperations.WithLabelValues("get").Inc()
	return p.pool.Get().(*bytes.Buffer)
}

// Put returns a buffer to the pool after resetting it.
func (p *BytePool) Put(buf *bytes.Buffer) {
	metrics.WalBufferPoolOperations.WithLabelValues("put").Inc()
	buf.Reset()
	p.pool.Put(buf)
}
