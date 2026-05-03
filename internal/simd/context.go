package simd

import (
	"sync/atomic"
)

// SimdContext tracks SIMD operations within a logical task (e.g. indexing a batch)
// to avoid frequent global metric increments.
type SimdContext struct {
	Implementation string
	Calls          int64
}

// RecordCall increments the local call counter
func (c *SimdContext) RecordCall() {
	if c == nil {
		return
	}
	c.Calls++
}

// GlobalSimdContext can be used for tasks that span multiple goroutines
type GlobalSimdContext struct {
	Implementation string
	Calls          atomic.Int64
}

func (c *GlobalSimdContext) RecordCall() {
	if c == nil {
		return
	}
	c.Calls.Add(1)
}
