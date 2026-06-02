//go:build !gpu

package memory

// Stub for non-GPU builds. GPU paging is only available on CUDA Linux.
type GPUPager struct{}

func NewGPUPager(pool *GPUMemPool, maxVRAM, pageSize int64) *GPUPager {
	return &GPUPager{}
}

func (p *GPUPager) PageInfo(id PageID) *PageInfo       { return nil }
func (p *GPUPager) GetCPUBuf(pi *PageInfo) []byte      { return nil }
func (p *GPUPager) Alloc(id PageID) (*PageInfo, error) { return nil, nil }
func (p *GPUPager) Promote(pi *PageInfo) error         { return nil }
func (p *GPUPager) Close() error                       { return nil }

type PageID uint64
type PageInfo struct{}
