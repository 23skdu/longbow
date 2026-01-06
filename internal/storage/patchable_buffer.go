package storage

import (
	"errors"
)

// PatchableBuffer is a buffer that supports appending and random-access writes (patching).
// It is used to write data sequentially (like a standard buffer) but allows
// going back to fill in reserved headers once the payload size/checksum is known.
type PatchableBuffer struct {
	buf []byte
}

// NewPatchableBuffer creates a new PatchableBuffer with initial capacity.
func NewPatchableBuffer(capacity int) *PatchableBuffer {
	return &PatchableBuffer{
		buf: make([]byte, 0, capacity),
	}
}

// Write appends p to the buffer, implementing io.Writer.
func (b *PatchableBuffer) Write(p []byte) (n int, err error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// WriteAt writes p starting at offset off, overwriting existing data.
// It implements io.WriterAt but limits writes to the current length of the buffer.
// It returns an error if the write extends beyond the current buffer length.
func (b *PatchableBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	if int(off) >= len(b.buf) {
		return 0, errors.New("offset out of bounds")
	}
	if int(off)+len(p) > len(b.buf) {
		return 0, errors.New("write out of bounds")
	}

	copy(b.buf[off:], p)
	return len(p), nil
}

// Bytes returns the slice holding the unread portion of the buffer.
// The slice is valid for use only until the next buffer modification.
func (b *PatchableBuffer) Bytes() []byte {
	return b.buf
}

// Len returns the number of bytes of the unread portion of the buffer.
func (b *PatchableBuffer) Len() int {
	return len(b.buf)
}

// Reset resets the buffer to be empty, but it retains the underlying storage for use by future writes.
func (b *PatchableBuffer) Reset() {
	b.buf = b.buf[:0]
}

// Grow guarantees that n bytes can be written to the buffer without another allocation.
func (b *PatchableBuffer) Grow(n int) {
	if cap(b.buf)-len(b.buf) < n {
		newBuf := make([]byte, len(b.buf), 2*cap(b.buf)+n)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
}
