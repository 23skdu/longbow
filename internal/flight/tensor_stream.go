package flight

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TensorStreamHandler handles direct GPU-to-GPU transfers for tensors
type TensorStreamHandler struct {
	server *RDMAServer
}

func NewTensorStreamHandler(server *RDMAServer) *TensorStreamHandler {
	return &TensorStreamHandler{server: server}
}

// StreamTensorRDMA initiates a zero-copy RDMA transfer of a GPU-backed tensor
func (h *TensorStreamHandler) StreamTensorRDMA(ctx context.Context, stream flight.FlightService_DoGetServer, ptr uintptr, size uint64) error {
	if h.server == nil || h.server.ctx == nil {
		return fmt.Errorf("rdma: server not initialized or rdma disabled")
	}

	// 1. Register GPU memory for remote access
	mr, err := h.server.ctx.RegisterGPUMemory(ctx, ptr, size)
	if err != nil {
		return fmt.Errorf("rdma: failed to register gpu memory: %w", err)
	}
	defer mr.Unregister()

	// 2. Prepare handshake metadata
	md := metadata.Pairs(
		"x-longbow-tensor-rdma-rkey", strconv.FormatUint(uint64(mr.RKey), 10),
		"x-longbow-tensor-rdma-addr", strconv.FormatUint(uint64(mr.Addr), 10),
		"x-longbow-tensor-rdma-len", strconv.FormatUint(mr.Length, 10),
		"x-longbow-transfer-mode", "zero-copy-gpu",
	)

	// 3. Send headers to client
	if err := grpc.SetHeader(stream.Context(), md); err != nil {
		return fmt.Errorf("rdma: failed to set grpc headers: %w", err)
	}

	// 4. Send an empty FlightData to signal the client can start RDMA Read
	// The client will use the RKey and Addr to pull data directly from our GPU.
	msg := &flight.FlightData{
		DataHeader: []byte("RDMA_READY"),
	}
	if err := stream.Send(msg); err != nil {
		return fmt.Errorf("rdma: failed to signal ready: %w", err)
	}

	// 5. Wait for client to finish or timeout
	// In a real implementation, we might wait for an RDMA completion event 
	// or a final gRPC signal.
	
	return nil
}

// GetTensorSchema returns the schema for a tensor transfer
func GetTensorSchema(dtype arrow.DataType, shape []int64) *arrow.Schema {
	md := arrow.NewMetadata([]string{"longbow.tensor.shape"}, []string{fmt.Sprintf("%v", shape)})
	return arrow.NewSchema([]arrow.Field{
		{Name: "tensor_data", Type: dtype},
	}, &md)
}
