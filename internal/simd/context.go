package simd

import (
	"sync/atomic"
)

// Context tracks SIMD operations within a logical task (e.g. indexing a batch)
// to avoid frequent global metric increments.
type Context struct {
	Implementation string
	Calls          int64
}

// RecordCall increments the local call counter
func (c *Context) RecordCall() {
	if c == nil {
		return
	}
	c.Calls++
}

// GlobalSimdContext can be used for tasks that span multiple goroutines
// where atomic updates are necessary for tracking call counts.
type GlobalSimdContext struct {
	Implementation string
	Calls          atomic.Int64
}

// RecordCall increments the global call counter in a thread-safe manner.
func (c *GlobalSimdContext) RecordCall() {
	if c == nil {
		return
	}
	c.Calls.Add(1)
}
