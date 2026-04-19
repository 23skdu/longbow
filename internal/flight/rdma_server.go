package flight

import (
	"fmt"
	"net"
	"strconv"

	"github.com/23skdu/longbow/internal/mesh"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RDMAServer handles zero-copy Arrow Flight transfers over RoCEv2.
type RDMAServer struct {
	flight.BaseFlightServer
	ctx     *mesh.RDMAContext
	tensors *TensorStreamHandler
}

func NewRDMAServer(enabled bool) *RDMAServer {
	s := &RDMAServer{
		ctx: mesh.NewRDMAContext(enabled),
	}
	s.tensors = NewTensorStreamHandler(s)
	return s
}

// DoPut implements the RDMA-accelerated path for data ingestion.
func (s *RDMAServer) DoPut(stream flight.FlightService_DoPutServer) error {
	// 1. Detect if client supports RDMA via metadata
	
	// 2. Perform RDMA Handshake
	// Register a buffer for this specific ingestion stream
	bufSize := 1024 * 1024 * 64 // 64MB buffer for Arrow batches
	mr, err := s.ctx.RegisterMemoryRegion(make([]byte, bufSize))
	if err != nil {
		return fmt.Errorf("rdma: failed to register memory region: %w", err)
	}
	defer mr.Unregister()

	// 3. Inform client of our RKey and Address (RKey is returned in MR)
	md := metadata.Pairs(
		"x-longbow-rdma-rkey", strconv.FormatUint(uint64(mr.RKey), 10),
		"x-longbow-rdma-addr", strconv.FormatUint(uint64(mr.Addr), 10),
		"x-longbow-rdma-len", strconv.FormatUint(mr.Length, 10),
	)
	if err := grpc.SetHeader(stream.Context(), md); err != nil {
		return fmt.Errorf("failed to send RDMA metadata headers: %w", err)
	}

	// 4. Await Completion or fallback
	// For now, we simulate the zero-copy path by incrementing metrics
	for {
		batch, err := stream.Recv()
		if err != nil {
			break
		}
		
		if batch != nil {
			// In zero-copy mode, the client writes directly to the MR.
			// The Recv() call might only contain metadata about the write.
			s.ctx.ProcessBytes(int64(len(batch.DataBody)))
		}
	}
	
	return nil
}

// DoGet implements the RDMA-accelerated path for data retrieval (e.g., tensors).
func (s *RDMAServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// 1. Detect if this is a tensor request (simplified for POC)
	// In a real implementation, the ticket would contain the tensor ID
	if string(tkt.Ticket) == "tensor_request" {
		// Mock GPU pointer and size for demonstration
		mockGPUPtr := uintptr(0xDEADBEEF)
		mockSize := uint64(1024 * 1024 * 128) // 128MB
		return s.tensors.StreamTensorRDMA(stream.Context(), stream, mockGPUPtr, mockSize)
	}

	return s.BaseFlightServer.DoGet(tkt, stream)
}

// StartRDMAListener starts the RDMA listener on the specified port.
func (s *RDMAServer) StartRDMAListener(addr string) error {
	// In RoCEv2, this is often a standard TCP listener for the handshake, 
	// followed by RDMA for the data path.
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Printf("RDMA Listener started on %s\n", addr)
	_ = l
	return nil
}
