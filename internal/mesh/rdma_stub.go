//go:build !linux || !cgo || !gpu

package mesh

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/23skdu/longbow/internal/metrics"
)

// RDMAStat holds metrics for RDMA performance.
type RDMAStat struct {
	BytesProcessed atomic.Int64
	ErrorCount     atomic.Int64
	ActivePeers    atomic.Int32
}

// RDMAContext manages the RDMA lifecycle.
type RDMAContext struct {
	enabled bool
	stats   RDMAStat
	mu      sync.RWMutex
	peers   map[string]*RDMAConnection
}

// RDMAConnection represents a high-throughput RDMA link.
type RDMAConnection struct {
	PeerAddr string
	RKey     uint32
	BaseAddr uint64
}

// MemoryRegion represents a registered memory area for RDMA.
type MemoryRegion struct {
	Addr       uintptr
	Length     uint64
	RKey       uint32
	Handle     uint64 // internal handle
}

func NewRDMAContext(enabled bool) *RDMAContext {
	return &RDMAContext{
		enabled: enabled,
		peers:   make(map[string]*RDMAConnection),
	}
}

// RegisterMemoryRegion registers a host memory area for RDMA access.
func (c *RDMAContext) RegisterMemoryRegion(data []byte) (*MemoryRegion, error) {
	if !c.enabled {
		return nil, fmt.Errorf("rdma not enabled")
	}
	
	// Simulation of ibv_reg_mr
	mr := &MemoryRegion{
		Addr:   uintptr(cap(data)), // simplified representation
		Length: uint64(len(data)),
		RKey:   54321,
		Handle: 2,
	}
	return mr, nil
}

// Unregister releases the memory region.
func (m *MemoryRegion) Unregister() error {
	// Simulation of ibv_dereg_mr
	return nil
}

// RegisterGPUMemory registers GPU VRAM for remote direct access.
func (c *RDMAContext) RegisterGPUMemory(ctx context.Context, ptr uintptr, size uint64) (*MemoryRegion, error) {

	if !c.enabled {
		return nil, fmt.Errorf("rdma not enabled")
	}

	// In a real implementation, this would call ibv_reg_mr
	// For now, we simulate the registration and return a handle.
	mr := &MemoryRegion{
		Addr:   ptr,
		Length: size,
		RKey:   12345, // Dummy RKey
		Handle: 1,
	}

	return mr, nil
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
