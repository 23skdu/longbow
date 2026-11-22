//go:build !gpu

package memory

// GPUPager for non-GPU builds. GPU paging is only available on CUDA Linux.
type GPUPager struct{}

// NewGPUPager constructs a new GPUPager.
func NewGPUPager(pool *GPUMemPool, maxVRAM, pageSize int64) *GPUPager {
	return &GPUPager{}
}

// PageInfo runs the page info operation.
func (p *GPUPager) PageInfo(id PageID) *PageInfo { return nil }

// GetCPUBuf returns the cpu buf.
func (p *GPUPager) GetCPUBuf(pi *PageInfo) []byte { return nil }

// Alloc runs the alloc operation.
func (p *GPUPager) Alloc(id PageID) (*PageInfo, error) { return nil, nil }

// Promote runs the promote operation.
func (p *GPUPager) Promote(pi *PageInfo) error { return nil }

// Close runs the close operation.
func (p *GPUPager) Close() error { return nil }

// PageID represents a page id value.
type PageID uint64

// PageInfo represents a page info value.
type PageInfo struct{}
