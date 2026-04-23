package types

import (
	"sync"
	"github.com/23skdu/longbow/internal/core"
)

// LazyMetadata decodes binary metadata on first access and caches the result.
// This prevents multiple decodings during complex filter evaluation.
type LazyMetadata struct {
	data []byte
	once sync.Once
	// Deprecated: use ArrowMetadata for zero-alloc access
	decoded map[string]interface{}
	err     error
}

func NewLazyMetadata(data []byte) *LazyMetadata {
	return &LazyMetadata{data: data}
}

func (l *LazyMetadata) Get() (map[string]interface{}, error) {
	l.once.Do(func() {
		l.decoded, l.err = core.DecodeMetadata(l.data)
	})
	return l.decoded, l.err
}

func (l *LazyMetadata) GetField(field string) (interface{}, bool) {
	// Optimization: Use zero-alloc decoder first
	meta := core.NewArrowMetadata(l.data)
	if val, ok := meta.GetField(field); ok {
		return val, true
	}

	// Fallback to full decode if needed (unlikely with ArrowMetadata)
	m, err := l.Get()
	if err != nil {
		return nil, false
	}
	val, ok := m[field]
	return val, ok
}
