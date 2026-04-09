package sharding

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// HealthCheckFunc is a function that checks if a connection is healthy
type HealthCheckFunc func(conn *grpc.ClientConn) bool

// DefaultHealthCheck uses gRPC connectivity state
func DefaultHealthCheck(conn *grpc.ClientConn) bool {
	state := conn.GetState()
	// Healthy if READY or CONNECTING (not TransientFailure or Shutdown)
	return state == connectivity.Ready || state == connectivity.Connecting
}

// ForwarderConfig holds configuration for the forwarder
type ForwarderConfig struct {
	DialTimeout         time.Duration
	Logger              zerolog.Logger
	HealthCheckInterval time.Duration
	HealthCheckFunc     HealthCheckFunc
	MaxConnAge          time.Duration
}

// DefaultForwarderConfig returns default config
func DefaultForwarderConfig() ForwarderConfig {
	return ForwarderConfig{
		DialTimeout:         5 * time.Second,
		Logger:              zerolog.Nop(),
		HealthCheckInterval: 30 * time.Second,
		HealthCheckFunc:     DefaultHealthCheck,
		MaxConnAge:          5 * time.Minute,
	}
}

// byteCodec is a generic gRPC codec that just passes raw bytes through.
type byteCodec struct{}

func (byteCodec) Marshal(v any) ([]byte, error) {
	switch b := v.(type) {
	case []byte:
		return b, nil
	default:
		return nil, fmt.Errorf("byteCodec: unexpected type %T", v)
	}
}

func (byteCodec) Unmarshal(data []byte, v any) error {
	switch b := v.(type) {
	case *[]byte:
		*b = data
		return nil
	default:
		return fmt.Errorf("byteCodec: unexpected type %T", v)
	}
}

func (byteCodec) Name() string {
	return "bytecodec"
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
	connAge  map[string]time.Time        // target -> creation time
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewRequestForwarder creates a new forwarder
func NewRequestForwarder(cfg *ForwarderConfig, resolver NodeResolver) *RequestForwarder {
	f := &RequestForwarder{
		config:   *cfg,
		resolver: resolver,
		conns:    make(map[string]*grpc.ClientConn),
		connAge:  make(map[string]time.Time),
		stopChan: make(chan struct{}),
	}

	// Start health check goroutine if health check is configured
	if cfg.HealthCheckInterval > 0 && cfg.HealthCheckFunc != nil {
		f.wg.Add(1)
		go f.healthCheckLoop()
	}

	return f
}

// healthCheckLoop periodically checks connection health
func (f *RequestForwarder) healthCheckLoop() {
	defer f.wg.Done()

	ticker := time.NewTicker(f.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.stopChan:
			return
		case <-ticker.C:
			f.checkAllConnections()
		}
	}
}

// checkAllConnections checks health of all connections and refreshes stale ones
func (f *RequestForwarder) checkAllConnections() {
	f.mu.Lock()
	defer f.mu.Unlock()

	now := time.Now()
	for target, conn := range f.conns {
		age := now.Sub(f.connAge[target])

		// Check max age
		if f.config.MaxConnAge > 0 && age > f.config.MaxConnAge {
			f.config.Logger.Debug().Str("target", target).Dur("age", age).Msg("Connection max age exceeded, refreshing")
			if err := f.refreshConnectionLocked(target, conn); err != nil {
				metrics.ConnectionPoolRefreshTotal.Inc()
				f.config.Logger.Error().Err(err).Str("target", target).Msg("Failed to refresh connection")
			}
			continue
		}

		// Run health check
		healthy := f.config.HealthCheckFunc(conn)
		if !healthy {
			metrics.ConnectionPoolHealthCheckTotal.WithLabelValues("unhealthy").Inc()
			f.config.Logger.Debug().Str("target", target).Msg("Connection unhealthy, refreshing")
			if err := f.refreshConnectionLocked(target, conn); err != nil {
				metrics.ConnectionPoolRefreshTotal.Inc()
				f.config.Logger.Error().Err(err).Str("target", target).Msg("Failed to refresh unhealthy connection")
			}
		} else {
			metrics.ConnectionPoolHealthCheckTotal.WithLabelValues("healthy").Inc()
		}
	}
}

// refreshConnectionLocked refreshes a connection (must hold lock)
func (f *RequestForwarder) refreshConnectionLocked(target string, oldConn *grpc.ClientConn) error {
	// Close old connection
	if err := oldConn.Close(); err != nil {
		f.config.Logger.Warn().Err(err).Str("target", target).Msg("Error closing old connection")
	}
	metrics.ConnectionPoolCloseTotal.Inc()

	// Create new connection
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	newConn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return fmt.Errorf("failed to create new connection for %s: %w", target, err)
	}

	f.conns[target] = newConn
	f.connAge[target] = time.Now()
	metrics.ConnectionPoolCreateTotal.Inc()

	return nil
}

// GetConn returns or creates a connection to the target
func (f *RequestForwarder) GetConn(ctx context.Context, target string) (*grpc.ClientConn, error) {
	start := time.Now()
	f.mu.RLock()
	conn, ok := f.conns[target]

	if ok {
		metrics.ConnectionPoolGetTotal.WithLabelValues("hit").Inc()
		metrics.ConnectionPoolGetDurationSeconds.WithLabelValues("hit").Observe(time.Since(start).Seconds())
		f.mu.RUnlock()
		return conn, nil
	}
	f.mu.RUnlock()

	// Cache miss - need to create connection
	f.mu.Lock()
	defer f.mu.Unlock()

	// Double check after acquiring write lock
	if conn, ok := f.conns[target]; ok {
		metrics.ConnectionPoolGetTotal.WithLabelValues("hit").Inc()
		metrics.ConnectionPoolGetDurationSeconds.WithLabelValues("hit").Observe(time.Since(start).Seconds())
		return conn, nil
	}

	metrics.ConnectionPoolGetTotal.WithLabelValues("miss").Inc()
	f.config.Logger.Info().Str("target", target).Msg("Creating new gRPC connection")

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		metrics.ConnectionPoolGetTotal.WithLabelValues("error").Inc()
		metrics.ConnectionPoolGetDurationSeconds.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return nil, fmt.Errorf("failed to dial target %s: %w", target, err)
	}

	f.conns[target] = conn
	f.connAge[target] = time.Now()
	metrics.ConnectionPoolCreateTotal.Inc()
	metrics.ConnectionPoolActiveConnections.WithLabelValues(target).Inc()

	metrics.ConnectionPoolGetDurationSeconds.WithLabelValues("miss").Observe(time.Since(start).Seconds())

	return conn, nil
}

// Forward forwards a unary request to the target node transparently.
// It uses byteCodec to handle any request/response type as raw bytes.
func (f *RequestForwarder) Forward(ctx context.Context, targetNodeID string, req any, method string) (any, error) {
	addr := f.resolver.GetNodeAddr(targetNodeID)
	if addr == "" {
		return nil, fmt.Errorf("forwarder: unknown node ID %s", targetNodeID)
	}

	conn, err := f.GetConn(ctx, addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "forwarder: get conn: %v", err)
	}

	// Propagate metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	// Marshal request if it's a proto.Message
	var requestBytes []byte
	switch r := req.(type) {
	case []byte:
		requestBytes = r
	case proto.Message:
		var err error
		requestBytes, err = proto.Marshal(r)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "forwarder: failed to marshal request: %v", err)
		}
	default:
		return nil, status.Errorf(codes.Internal, "forwarder: unexpected request type %T", req)
	}

	var responseBytes []byte
	err = conn.Invoke(ctx, method, requestBytes, &responseBytes, grpc.ForceCodec(byteCodec{}))
	if err != nil {
		return nil, err
	}

	return responseBytes, nil
}

// ForwardStream handles transparent proxying for streaming gRPC calls.
// initialRequest is optional and should be provided if the request has already been consumed by the handler (e.g., in Server-Streaming RPCs like DoGet).
func (f *RequestForwarder) ForwardStream(ctx context.Context, targetNodeID string, serverStream grpc.ServerStream, method string, initialRequest any) error {
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

	// Create client stream using byteCodec for generic proxying
	desc := &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}

	clientStream, err := conn.NewStream(ctx, desc, method, grpc.ForceCodec(byteCodec{}))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create client stream: %v", err)
	}

	// If there's an initial request, send it first
	if initialRequest != nil {
		var requestBytes []byte
		switch r := initialRequest.(type) {
		case []byte:
			requestBytes = r
		case proto.Message:
			var err error
			requestBytes, err = proto.Marshal(r)
			if err != nil {
				return status.Errorf(codes.Internal, "forwarder: failed to marshal initial request: %v", err)
			}
		default:
			return status.Errorf(codes.Internal, "forwarder: unexpected initial request type %T", initialRequest)
		}
		if err := clientStream.SendMsg(requestBytes); err != nil {
			return status.Errorf(codes.Internal, "forwarder: failed to send initial request: %v", err)
		}
	}

	// Bi-directional piping of messages
	errChan := make(chan error, 2)

	// Server -> Client (Forwarding request/data from the client of the proxy to the target server)
	go func() {
		for {
			var msg []byte
			if err := serverStream.RecvMsg(&msg); err != nil {
				_ = clientStream.CloseSend()
				if err == io.EOF {
					errChan <- nil
				} else {
					// For server-streaming RPCs where the request is already consumed, 
					// RecvMsg will fail. We treat this as "nothing more to receive" 
					// if we already sent the initial request.
					if initialRequest != nil {
						errChan <- nil
					} else {
						errChan <- err
					}
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
			var msg []byte
			if err := clientStream.RecvMsg(&msg); err != nil {
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

// Close closes all connections and stops the health check goroutine
func (f *RequestForwarder) Close() error {
	// Signal health check goroutine to stop
	select {
	case <-f.stopChan:
		// Already closed
	default:
		close(f.stopChan)
	}
	f.wg.Wait()

	f.mu.Lock()
	defer f.mu.Unlock()

	for target, conn := range f.conns {
		_ = conn.Close()
		metrics.ConnectionPoolCloseTotal.Inc()
		metrics.ConnectionPoolActiveConnections.DeleteLabelValues(target)
	}

	f.conns = make(map[string]*grpc.ClientConn)
	f.connAge = make(map[string]time.Time)
	return nil
}
