package store

import (
"bytes"
"sync"
)

// walBufferPool pools bytes.Buffer instances to reduce allocation
// pressure in WAL writeEntry() hot path.
type walBufferPool struct {
pool sync.Pool
}

// newWALBufferPool creates a new buffer pool.
func newWALBufferPool() *walBufferPool {
return &walBufferPool{
pool: sync.Pool{
New: func() any {
return new(bytes.Buffer)
},
},
}
}

// Get retrieves a buffer from the pool.
// The buffer is guaranteed to be empty (Reset called).
func (p *walBufferPool) Get() *bytes.Buffer {
return p.pool.Get().(*bytes.Buffer)
}

// Put returns a buffer to the pool after resetting it.
func (p *walBufferPool) Put(buf *bytes.Buffer) {
buf.Reset()
p.pool.Put(buf)
}
