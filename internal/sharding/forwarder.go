package sharding

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/23skdu/longbow/internal/metrics"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
func (f *RequestForwarder) Forward(ctx context.Context, targetNodeID string, req any, method string) (any, error) {
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

	var reply any
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
			var msg any
			switch method {
			case "/arrow.flight.protocol.FlightService/DoGet":
				msg = &flight.Ticket{}
			case "/arrow.flight.protocol.FlightService/DoAction":
				msg = &flight.Action{}
			default:
				msg = &flight.FlightData{}
			}

			if err := serverStream.RecvMsg(msg); err != nil {
				_ = clientStream.CloseSend()
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
			var msg any
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
