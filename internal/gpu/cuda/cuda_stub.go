//go:build !gpu || !linux || !cgo

package cuda

import (
	"fmt"

	"github.com/23skdu/longbow/internal/gpu/types"
)

// NewCUDAIndexImpl constructs a new CUDAIndexImpl.
func NewCUDAIndexImpl(cfg types.GPUConfig) (types.Index, error) {
	return nil, fmt.Errorf("CUDA index not supported on this platform: build with -tags gpu,linux")
}

// MemcpyKind defines direction of CUDA memory copy
type MemcpyKind int

const (
	// MemcpyHostToHost is a memcpy host to host constant.
	MemcpyHostToHost MemcpyKind = 0
	// MemcpyHostToDevice is a memcpy host to device constant.
	MemcpyHostToDevice MemcpyKind = 1
	// MemcpyDeviceToHost is a memcpy device to host constant.
	MemcpyDeviceToHost MemcpyKind = 2
	// MemcpyDeviceToDevice is a memcpy device to device constant.
	MemcpyDeviceToDevice MemcpyKind = 3
)

// HostAlloc runs the host alloc operation.
func HostAlloc(size int64) (any, error) {
	return nil, fmt.Errorf("CUDA not supported on this platform")
}

// FreeHost runs the free host operation.
func FreeHost(ptr any) error {
	return nil
}

// MemcpyAsync runs the memcpy async operation.
func MemcpyAsync(dst, src any, size int64, kind MemcpyKind, stream any) error {
	return fmt.Errorf("CUDA not supported on this platform")
}

// StreamSynchronize runs the stream synchronize operation.
func StreamSynchronize(stream any) error {
	return fmt.Errorf("CUDA not supported on this platform")
}

// PinnedHostPool represents a pinned host pool value.
type PinnedHostPool struct{}

// NewPinnedHostPool constructs a new PinnedHostPool.
func NewPinnedHostPool() *PinnedHostPool {
	return &PinnedHostPool{}
}

// Get returns the .
func (p *PinnedHostPool) Get(size int64) (any, error) {
	return nil, fmt.Errorf("CUDA not supported on this platform")
}

// Put runs the put operation.
func (p *PinnedHostPool) Put(ptr any, size int64) {}

// Close runs the close operation.
func (p *PinnedHostPool) Close() error {
	return nil
}
