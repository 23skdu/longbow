package sharding

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// frame is a proxy frame that carries raw bytes.
type frame struct {
	payload []byte
}

func (f *frame) Marshal() ([]byte, error) { return f.payload, nil }
func (f *frame) Unmarshal(data []byte) error {
	f.payload = data
	return nil
}

// ForwarderConfig holds configuration for the forwarder
type ForwarderConfig struct {
	DialTimeout time.Duration
	Logger      *zap.Logger
}

// DefaultForwarderConfig returns default config
func DefaultForwarderConfig() ForwarderConfig {
	return ForwarderConfig{
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	}
}

// NodeResolver resolves node IDs to addresses
type NodeResolver interface {
	GetNodeAddr(nodeID string) string
}

// RequestForwarder handles forwarding gRPC requests to remote nodes
type RequestForwarder struct {
	config   ForwarderConfig
	resolver NodeResolver
	mu       sync.RWMutex
	conns    map[string]*grpc.ClientConn // target (host:port) -> conn
}

// NewRequestForwarder creates a new forwarder
func NewRequestForwarder(cfg ForwarderConfig, resolver NodeResolver) *RequestForwarder {
	return &RequestForwarder{
		config:   cfg,
		resolver: resolver,
		conns:    make(map[string]*grpc.ClientConn),
	}
}

// GetConn returns or creates a connection to the target
func (f *RequestForwarder) GetConn(ctx context.Context, target string) (*grpc.ClientConn, error) {
	f.mu.RLock()
	conn, ok := f.conns[target]
	f.mu.RUnlock()
	if ok {
		return conn, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Double check
	if conn, ok := f.conns[target]; ok {
		return conn, nil
	}

	f.config.Logger.Info("Creating new gRPC connection", zap.String("target", target))
	f.config.Logger.Info("Creating new gRPC connection", zap.String("target", target))

	// We assume insecure internal traffic for now
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial target %s: %w", target, err)
	}

	f.conns[target] = conn
	return conn, nil
}

// Forward forwards a unary request to the target node transparently.
func (f *RequestForwarder) Forward(ctx context.Context, targetNodeID string, req interface{}, method string) (interface{}, error) {
	addr := f.resolver.GetNodeAddr(targetNodeID)
	if addr == "" {
		return nil, fmt.Errorf("forwarder: unknown node ID %s", targetNodeID)
	}

	conn, err := f.GetConn(ctx, addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "forwarder: get conn: %v", err)
	}

	// Since we don't know the response type here, we need to handle it carefully.
	// For "fully transparent" proxying in an interceptor, we ideally want to return
	// the same type the server expects.
	// However, we can use the original req's type to help with discovery if needed.

	// Propagate metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	// We use the rawCodec to handle the request/response as bytes or proto.Message.
	// If it's already a proto.Message, grpc will use the default codec unless we override.
	// But in an interceptor, we are already unmarshaled.

	// Since we are in Longbow, we know most methods are FlightService.
	// For now, let's implement a typed forward for known methods if we can,
	// or fallback to a smarter generic approach.

	var reply interface{}
	switch method {
	case "/arrow.flight.protocol.FlightService/GetFlightInfo":
		reply = &flight.FlightInfo{}
	case "/arrow.flight.protocol.FlightService/GetSchema":
		reply = &flight.SchemaResult{}
	default:
		// Fallback for unknown unary methods: attempt to use raw bytes
		// Note: This requires the server to accept the raw bytes back.
		return nil, status.Errorf(codes.Unimplemented, "forwarding for method %s not yet implemented", method)
	}

	err = conn.Invoke(ctx, method, req, reply)
	if err != nil {
		return nil, err
	}

	return reply, nil
}

// ForwardStream handles transparent proxying for streaming gRPC calls.
func (f *RequestForwarder) ForwardStream(ctx context.Context, targetNodeID string, serverStream grpc.ServerStream, method string) error {
	addr := f.resolver.GetNodeAddr(targetNodeID)
	if addr == "" {
		return status.Errorf(codes.Unavailable, "forwarder: unknown node ID %s", targetNodeID)
	}

	conn, err := f.GetConn(ctx, addr)
	if err != nil {
		return status.Errorf(codes.Unavailable, "forwarder: get conn: %v", err)
	}

	// Propagate metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	// Create client stream
	// We use a custom Desc to handle arbitrary streams
	desc := &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}

	clientStream, err := conn.NewStream(ctx, desc, method)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create client stream: %v", err)
	}

	// Bi-directional piping of messages
	errChan := make(chan error, 2)

	// Server -> Client (Forwarding request/data)
	go func() {
		for {
			var msg interface{}
			switch method {
			case "/arrow.flight.protocol.FlightService/DoGet":
				msg = &flight.Ticket{}
			case "/arrow.flight.protocol.FlightService/DoAction":
				msg = &flight.Action{}
			default:
				msg = &flight.FlightData{}
			}

			if err := serverStream.RecvMsg(msg); err != nil {
				clientStream.CloseSend()
				if err == io.EOF {
					errChan <- nil
				} else {
					errChan <- err
				}
				return
			}
			if err := clientStream.SendMsg(msg); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Client -> Server (Returning data/response)
	go func() {
		for {
			var msg interface{}
			if method == "/arrow.flight.protocol.FlightService/DoAction" {
				msg = &flight.Result{}
			} else {
				msg = &flight.FlightData{}
			}

			if err := clientStream.RecvMsg(msg); err != nil {
				if err == io.EOF {
					errChan <- nil
				} else {
					errChan <- err
				}
				return
			}
			if err := serverStream.SendMsg(msg); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Wait for completion or error
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

// Close closes all connections
func (f *RequestForwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, conn := range f.conns {
		conn.Close()
	}
	f.conns = make(map[string]*grpc.ClientConn)
	return nil
}
