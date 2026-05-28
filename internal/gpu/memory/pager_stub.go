//go:build !gpu || !linux

package memory

// Stub for non-GPU builds. GPU paging is only available on CUDA Linux.
type GPUPager struct{}

func NewGPUPager(pool *GPUMemPool, maxVRAM, pageSize int64) *GPUPager {
	return &GPUPager{}
}
