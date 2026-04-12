//go:build linux && cgo && gpu

package mesh

/*
#cgo LDFLAGS: -libverbs
#include <infiniband/verbs.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/23skdu/longbow/internal/metrics"
)

// RDMAStat holds metrics for RDMA performance.
type RDMAStat struct {
	BytesProcessed atomic.Int64
	ErrorCount     atomic.Int64
	ActivePeers    atomic.Int32
}

// RDMAContext manages the RDMA lifecycle securely under ibverbs.
type RDMAContext struct {
	enabled bool
	stats   RDMAStat
	mu      sync.RWMutex
	peers   map[string]*RDMAConnection

	ibvCtx *C.struct_ibv_context
	pd     *C.struct_ibv_pd
}

// RDMAConnection represents a high-throughput RDMA link.
type RDMAConnection struct {
	PeerAddr string
	RKey     uint32
	BaseAddr uint64
}

// MemoryRegion represents a registered memory area mapped via ibverbs.
type MemoryRegion struct {
	Addr       uintptr
	Length     uint64
	RKey       uint32
	Handle     uint64 // Maps to MR handle
	ibvMR      *C.struct_ibv_mr
}

func NewRDMAContext(enabled bool) *RDMAContext {
	c := &RDMAContext{
		enabled: enabled,
		peers:   make(map[string]*RDMAConnection),
	}

	if enabled {
		var numDevices C.int
		devList := C.ibv_get_device_list(&numDevices)
		if devList == nil || numDevices == 0 {
			metrics.RDMAErrorsTotal.Inc()
			fmt.Println("WARN: No RDMA devices found, falling back.")
			c.enabled = false
			return c
		}

		// Pick the first device for simplicity
		c.ibvCtx = C.ibv_open_device(*devList)
		C.ibv_free_device_list(devList)

		if c.ibvCtx == nil {
			c.enabled = false
			return c
		}

		c.pd = C.ibv_alloc_pd(c.ibvCtx)
		if c.pd == nil {
			C.ibv_close_device(c.ibvCtx)
			c.enabled = false
			return c
		}
	}

	return c
}

// RegisterMemoryRegion registers a host memory area for RDMA access.
func (c *RDMAContext) RegisterMemoryRegion(data []byte) (*MemoryRegion, error) {
	if !c.enabled || c.pd == nil {
		return nil, fmt.Errorf("rdma not enabled or active")
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("empty buffer")
	}

	ptr := unsafe.Pointer(&data[0])
	length := C.size_t(len(data))
	
	// IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
	accessFlags := C.int(C.IBV_ACCESS_LOCAL_WRITE | C.IBV_ACCESS_REMOTE_WRITE | C.IBV_ACCESS_REMOTE_READ)

	mr := C.ibv_reg_mr(c.pd, ptr, length, accessFlags)
	if mr == nil {
		return nil, fmt.Errorf("libibverbs: failed to register memory region")
	}

	return &MemoryRegion{
		Addr:   uintptr(ptr),
		Length: uint64(length),
		RKey:   uint32(mr.rkey),
		ibvMR:  mr,
	}, nil
}

// Unregister releases the memory region.
func (m *MemoryRegion) Unregister() error {
	if m.ibvMR != nil {
		res := C.ibv_dereg_mr(m.ibvMR)
		if res != 0 {
			return fmt.Errorf("failed to deregister MR: %d", res)
		}
		m.ibvMR = nil
	}
	return nil
}

// RegisterGPUMemory registers GPU VRAM for remote direct access.
func (c *RDMAContext) RegisterGPUMemory(ctx context.Context, ptr uintptr, size uint64) (*MemoryRegion, error) {
	if !c.enabled || c.pd == nil {
		return nil, fmt.Errorf("rdma not enabled or active")
	}

	// This utilizes NVIDIA PeerDirect functionality implicitly if the memory ptr
	// points to a GPU mapping and peer memory modules are active.
	accessFlags := C.int(C.IBV_ACCESS_LOCAL_WRITE | C.IBV_ACCESS_REMOTE_WRITE | C.IBV_ACCESS_REMOTE_READ)
	mr := C.ibv_reg_mr(c.pd, unsafe.Pointer(ptr), C.size_t(size), accessFlags)
	if mr == nil {
		return nil, fmt.Errorf("libibverbs: failed to register GPU memory region")
	}

	return &MemoryRegion{
		Addr:   ptr,
		Length: size,
		RKey:   uint32(mr.rkey),
		ibvMR:  mr,
	}, nil
}

// ProcessBytes records RDMA ingestion telemetry.
func (c *RDMAContext) ProcessBytes(n int64) {
	c.stats.BytesProcessed.Add(n)
	metrics.RDMABytesProcessedTotal.Add(float64(n))
}

// RecordError increments the RDMA error counter.
func (c *RDMAContext) RecordError() {
	c.stats.ErrorCount.Add(1)
	metrics.RDMAErrorsTotal.Inc()
}
