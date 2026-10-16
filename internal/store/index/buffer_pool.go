package index

import "sync"

// Float32RowPool provides pooled []float32 slices of a fixed dimension
// to eliminate heap allocations during vector scans and deserializations.
type Float32RowPool struct {
	dim  int
	pool sync.Pool
}

// NewFloat32RowPool creates a new Float32RowPool for vectors of dimensionality dim.
func NewFloat32RowPool(dim int) *Float32RowPool {
	if dim <= 0 {
		dim = 1
	}
	return &Float32RowPool{
		dim: dim,
		pool: sync.Pool{
			New: func() any {
				s := make([]float32, dim)
				return &s
			},
		},
	}
}

// Get retrieves a float32 slice with length and capacity equal to dim.
func (p *Float32RowPool) Get() []float32 {
	sp := p.pool.Get().(*[]float32)
	s := *sp
	if cap(s) < p.dim {
		s = make([]float32, p.dim)
	}
	return s[:p.dim]
}

// Put returns a float32 slice to the pool.
func (p *Float32RowPool) Put(s []float32) {
	if cap(s) < p.dim {
		return
	}
	s = s[:p.dim]
	p.pool.Put(&s)
}

// ByteRowPool provides pooled []byte slices of a fixed size.
type ByteRowPool struct {
	size int
	pool sync.Pool
}

// NewByteRowPool creates a new ByteRowPool for buffers of length size.
func NewByteRowPool(size int) *ByteRowPool {
	if size <= 0 {
		size = 1
	}
	return &ByteRowPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, size)
				return &b
			},
		},
	}
}

// Get retrieves a byte slice with length and capacity equal to size.
func (p *ByteRowPool) Get() []byte {
	bp := p.pool.Get().(*[]byte)
	b := *bp
	if cap(b) < p.size {
		b = make([]byte, p.size)
	}
	return b[:p.size]
}

// Put returns a byte slice to the pool.
func (p *ByteRowPool) Put(b []byte) {
	if cap(b) < p.size {
		return
	}
	b = b[:p.size]
	p.pool.Put(&b)
}
