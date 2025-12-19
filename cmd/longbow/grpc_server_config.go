package main

import (
"errors"

"google.golang.org/grpc"
"google.golang.org/grpc/keepalive"
)

// BuildGRPCServerOptions returns grpc.ServerOption slice for server configuration.
// Combines keepalive settings with message size and flow control options.
func (c *Config) BuildGRPCServerOptions() []grpc.ServerOption {
kaParams := keepalive.ServerParameters{
Time:    c.KeepAliveTime,
Timeout: c.KeepAliveTimeout,
}
kaPolicy := keepalive.EnforcementPolicy{
MinTime:             c.KeepAliveMinTime,
PermitWithoutStream: c.KeepAlivePermitWithoutStream,
}

return []grpc.ServerOption{
// Keepalive configuration
grpc.KeepaliveParams(kaParams),
grpc.KeepaliveEnforcementPolicy(kaPolicy),

// Concurrency limit
grpc.MaxConcurrentStreams(c.GRPCMaxConcurrentStreams),

// HTTP/2 flow control windows
grpc.InitialWindowSize(c.GRPCInitialWindowSize),
grpc.InitialConnWindowSize(c.GRPCInitialConnWindowSize),

// Message size limits
grpc.MaxRecvMsgSize(c.GRPCMaxRecvMsgSize),
grpc.MaxSendMsgSize(c.GRPCMaxSendMsgSize),
}
}

// ValidateGRPCConfig checks if the gRPC configuration is valid.
func (c *Config) ValidateGRPCConfig() error {
if c.GRPCMaxConcurrentStreams == 0 {
return errors.New("grpc_max_concurrent_streams must be > 0")
}
if c.GRPCInitialWindowSize < 0 {
return errors.New("grpc_initial_window_size must be >= 0")
}
if c.GRPCInitialConnWindowSize < 0 {
return errors.New("grpc_initial_conn_window_size must be >= 0")
}
if c.GRPCMaxRecvMsgSize < 0 {
return errors.New("grpc_max_recv_msg_size must be >= 0")
}
if c.GRPCMaxSendMsgSize < 0 {
return errors.New("grpc_max_send_msg_size must be >= 0")
}
return nil
}
