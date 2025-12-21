//go:build !linux

package store

// NewWAL creates the best available WAL implementation for the platform.
// For non-Linux, it defaults to StdWAL.
func NewWAL(dir string, v *VectorStore) WAL {
	return NewStdWAL(dir, v)
}
