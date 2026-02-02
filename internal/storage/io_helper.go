package storage

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

// RateLimitedWriter wraps an io.Writer and limits the write throughput.
type RateLimitedWriter struct {
	w       io.Writer
	limiter *rate.Limiter
	ctx     context.Context
}

// NewRateLimitedWriter creates a new rate limited writer.
// If limitBytesPerSec is <= 0, valid writes are passed through without limiting.
func NewRateLimitedWriter(w io.Writer, limitBytesPerSec int, ctx context.Context) io.Writer {
	if limitBytesPerSec <= 0 {
		return w
	}
	// Burst equals the limit (1 second worth of data) allows averaging out.
	limiter := rate.NewLimiter(rate.Limit(limitBytesPerSec), limitBytesPerSec)
	return &RateLimitedWriter{w: w, limiter: limiter, ctx: ctx}
}

func (rw *RateLimitedWriter) Write(p []byte) (n int, err error) {
	if rw.limiter == nil {
		return rw.w.Write(p)
	}

	// WaitN blocks until the limiter allows 'n' events (bytes).
	// If p is larger than burst, WaitN will error or block appropriately?
	// rate.WaitN will block multiple intervals if needed.
	// But standard rate.Limiter errors if n > burst.
	// We need to chunk the write if it exceeds burst?
	// Or set a large burst?
	// With Limit == Burst, we support writing up to 1s of data at once.
	// Typically snapshots writes are buffered (e.g. 4KB-1MB).
	// If write is huge (e.g. 100MB), we should loop.

	written := 0
	for written < len(p) {
		remaining := len(p) - written
		toWait := remaining
		burst := rw.limiter.Burst()
		if toWait > burst {
			toWait = burst
		}

		if err := rw.limiter.WaitN(rw.ctx, toWait); err != nil {
			return written, err
		}

		n, err := rw.w.Write(p[written : written+toWait])
		if n > 0 {
			written += n
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}
