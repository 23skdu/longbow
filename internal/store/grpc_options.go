package store

import (
"errors"
"fmt"
"time"

"google.golang.org/grpc"
"google.golang.org/grpc/keepalive"
)

// GRPCConfig holds all gRPC server/client tuning parameters for optimal throughput.
// Configuring these parameters can yield 5-10% improvement in sustained throughput.
type GRPCConfig struct {
// Keepalive parameters
KeepAliveTime                time.Duration // Time between keepalive pings (server sends to client)
KeepAliveTimeout             time.Duration // Timeout for keepalive ping acknowledgement
KeepAliveMinTime             time.Duration // Minimum time client should wait between pings
KeepAlivePermitWithoutStream bool          // Allow keepalive pings when no active streams

// Concurrent streams limit
MaxConcurrentStreams uint32 // Maximum concurrent streams per connection

// HTTP/2 flow control window sizes
InitialWindowSize     int32 // Initial window size for a stream
InitialConnWindowSize int32 // Initial window size for a connection

// Message size limits
MaxRecvMsgSize int // Maximum message size the server can receive
MaxSendMsgSize int // Maximum message size the server can send
}

// DefaultGRPCConfig returns a GRPCConfig with sensible defaults optimized for throughput.
// These defaults are tuned for vector database workloads with large message payloads.
func DefaultGRPCConfig() GRPCConfig {
return GRPCConfig{
// Keepalive: 2h default, conservative for long-lived connections
KeepAliveTime:                2 * time.Hour,
KeepAliveTimeout:             20 * time.Second,
KeepAliveMinTime:             5 * time.Minute,
KeepAlivePermitWithoutStream: false,

// Allow 250 concurrent streams per connection (up from default 100)
MaxConcurrentStreams: 250,

// 1MB window sizes (up from default 64KB) for better throughput
InitialWindowSize:     1 << 20, // 1MB
InitialConnWindowSize: 1 << 20, // 1MB

// 64MB message limits for large vector batches
MaxRecvMsgSize: 64 * 1024 * 1024, // 64MB
MaxSendMsgSize: 64 * 1024 * 1024, // 64MB
}
}

// Validate checks if the configuration is valid.
func (c GRPCConfig) Validate() error {
if c.MaxConcurrentStreams == 0 {
return errors.New("max_concurrent_streams must be > 0")
}
if c.InitialWindowSize < 0 {
return errors.New("initial_window_size must be >= 0")
}
if c.InitialConnWindowSize < 0 {
return errors.New("initial_conn_window_size must be >= 0")
}
if c.MaxRecvMsgSize < 0 {
return errors.New("max_recv_msg_size must be >= 0")
}
if c.MaxSendMsgSize < 0 {
return errors.New("max_send_msg_size must be >= 0")
}
return nil
}

// ServerKeepaliveParams returns keepalive.ServerParameters for server configuration.
func (c GRPCConfig) ServerKeepaliveParams() keepalive.ServerParameters {
return keepalive.ServerParameters{
Time:    c.KeepAliveTime,
Timeout: c.KeepAliveTimeout,
}
}

// ServerEnforcementPolicy returns keepalive.EnforcementPolicy for server configuration.
func (c GRPCConfig) ServerEnforcementPolicy() keepalive.EnforcementPolicy {
return keepalive.EnforcementPolicy{
MinTime:             c.KeepAliveMinTime,
PermitWithoutStream: c.KeepAlivePermitWithoutStream,
}
}

// ClientKeepaliveParams returns keepalive.ClientParameters for client configuration.
func (c GRPCConfig) ClientKeepaliveParams() keepalive.ClientParameters {
return keepalive.ClientParameters{
Time:                c.KeepAliveTime,
Timeout:             c.KeepAliveTimeout,
PermitWithoutStream: c.KeepAlivePermitWithoutStream,
}
}

// BuildServerOptions returns a slice of grpc.ServerOption configured from this GRPCConfig.
// These options optimize gRPC server performance for sustained throughput.
func (c GRPCConfig) BuildServerOptions() []grpc.ServerOption {
return []grpc.ServerOption{
// Keepalive configuration
grpc.KeepaliveParams(c.ServerKeepaliveParams()),
grpc.KeepaliveEnforcementPolicy(c.ServerEnforcementPolicy()),

// Concurrency limit
grpc.MaxConcurrentStreams(c.MaxConcurrentStreams),

// HTTP/2 flow control windows
grpc.InitialWindowSize(c.InitialWindowSize),
grpc.InitialConnWindowSize(c.InitialConnWindowSize),

// Message size limits
grpc.MaxRecvMsgSize(c.MaxRecvMsgSize),
grpc.MaxSendMsgSize(c.MaxSendMsgSize),
}
}

// BuildClientOptions returns a slice of grpc.DialOption configured from this GRPCConfig.
// These options optimize gRPC client performance to match server settings.
func (c GRPCConfig) BuildClientOptions() []grpc.DialOption {
return []grpc.DialOption{
// Keepalive configuration
grpc.WithKeepaliveParams(c.ClientKeepaliveParams()),

// HTTP/2 flow control windows
grpc.WithInitialWindowSize(c.InitialWindowSize),
grpc.WithInitialConnWindowSize(c.InitialConnWindowSize),

// Message size limits
grpc.WithDefaultCallOptions(
grpc.MaxCallRecvMsgSize(c.MaxRecvMsgSize),
grpc.MaxCallSendMsgSize(c.MaxSendMsgSize),
),
}
}

// String returns a human-readable representation of the config for logging.
func (c GRPCConfig) String() string {
return fmt.Sprintf(
"GRPCConfig{keepalive_time=%v, keepalive_timeout=%v, keepalive_min_time=%v, "+
"permit_without_stream=%v, max_concurrent_streams=%d, "+
"initial_window_size=%d, initial_conn_window_size=%d, "+
"max_recv_msg_size=%d, max_send_msg_size=%d}",
c.KeepAliveTime,
c.KeepAliveTimeout,
c.KeepAliveMinTime,
c.KeepAlivePermitWithoutStream,
c.MaxConcurrentStreams,
c.InitialWindowSize,
c.InitialConnWindowSize,
c.MaxRecvMsgSize,
c.MaxSendMsgSize,
)
}
