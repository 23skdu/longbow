package simd

import (
	"sync/atomic"
)

// Context tracks SIMD operations within a logical task (e.g., indexing a batch)
// to avoid frequent global metric increments and provides context for kernel selection.
type Context struct {
	// Implementation is the name of the SIMD instruction set being used (e.g., "avx512", "neon").
	Implementation string
	// Calls tracks the number of SIMD operations performed within this context.
	Calls int64
}

// RecordCall increments the local call counter for the context. This is not thread-safe.
func (c *Context) RecordCall() {
	if c == nil {
		return
	}
	c.Calls++
}

// GlobalContext can be used for tasks that span multiple goroutines
// where atomic updates are necessary for tracking call counts across the entire system.
type GlobalContext struct {
	// Implementation is the name of the SIMD instruction set being used.
	Implementation string
	// Calls tracks the total number of SIMD operations performed globally.
	Calls atomic.Int64
}

// RecordCall increments the global call counter in a thread-safe manner using atomic operations.
func (c *GlobalContext) RecordCall() {
	if c == nil {
		return
	}
	c.Calls.Add(1)
}
