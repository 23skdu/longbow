package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestGRPCConfigDefaults verifies default values for gRPC tuning configuration
func TestGRPCConfigDefaults(t *testing.T) {
	cfg := DefaultGRPCConfig()

	// Keepalive defaults
	assert.Equal(t, 2*time.Hour, cfg.KeepAliveTime, "default keepalive time should be 2h")
	assert.Equal(t, 20*time.Second, cfg.KeepAliveTimeout, "default keepalive timeout should be 20s")
	assert.Equal(t, 5*time.Minute, cfg.KeepAliveMinTime, "default keepalive min time should be 5m")
	assert.False(t, cfg.KeepAlivePermitWithoutStream, "default permit without stream should be false")

	// Concurrent streams
	assert.Equal(t, uint32(250), cfg.MaxConcurrentStreams, "default max concurrent streams should be 250")

	// Window sizes (HTTP/2 flow control)
	assert.Equal(t, int32(4<<20), cfg.InitialWindowSize, "default initial window size should be 4MB")
	assert.Equal(t, int32(4<<20), cfg.InitialConnWindowSize, "default initial conn window size should be 4MB")

	// Message sizes
	assert.Equal(t, 64*1024*1024, cfg.MaxRecvMsgSize, "default max recv msg size should be 64MB")
	assert.Equal(t, 64*1024*1024, cfg.MaxSendMsgSize, "default max send msg size should be 64MB")
}

// TestGRPCConfigCustomValues verifies custom configuration values are respected
func TestGRPCConfigCustomValues(t *testing.T) {
	cfg := GRPCConfig{
		KeepAliveTime:                30 * time.Second,
		KeepAliveTimeout:             10 * time.Second,
		KeepAliveMinTime:             15 * time.Second,
		KeepAlivePermitWithoutStream: true,
		MaxConcurrentStreams:         500,
		InitialWindowSize:            2 << 20,           // 2MB
		InitialConnWindowSize:        4 << 20,           // 4MB
		MaxRecvMsgSize:               128 * 1024 * 1024, // 128MB
		MaxSendMsgSize:               128 * 1024 * 1024, // 128MB
	}

	assert.Equal(t, 30*time.Second, cfg.KeepAliveTime)
	assert.Equal(t, 10*time.Second, cfg.KeepAliveTimeout)
	assert.Equal(t, 15*time.Second, cfg.KeepAliveMinTime)
	assert.True(t, cfg.KeepAlivePermitWithoutStream)
	assert.Equal(t, uint32(500), cfg.MaxConcurrentStreams)
	assert.Equal(t, int32(2<<20), cfg.InitialWindowSize)
	assert.Equal(t, int32(4<<20), cfg.InitialConnWindowSize)
	assert.Equal(t, 128*1024*1024, cfg.MaxRecvMsgSize)
	assert.Equal(t, 128*1024*1024, cfg.MaxSendMsgSize)
}

// TestBuildServerOptions verifies gRPC server options are correctly built
func TestBuildServerOptions(t *testing.T) {
	cfg := DefaultGRPCConfig()
	opts := cfg.BuildServerOptions()

	// Should return at least 7 options:
	// - KeepaliveParams
	// - KeepaliveEnforcementPolicy
	// - MaxConcurrentStreams
	// - InitialWindowSize
	// - InitialConnWindowSize
	// - MaxRecvMsgSize
	// - MaxSendMsgSize
	require.GreaterOrEqual(t, len(opts), 7, "should have at least 7 server options")
}

// TestBuildServerOptionsCreatesValidServer verifies options create a valid gRPC server
func TestBuildServerOptionsCreatesValidServer(t *testing.T) {
	cfg := DefaultGRPCConfig()
	opts := cfg.BuildServerOptions()

	// This should not panic - if it does, the options are invalid
	server := grpc.NewServer(opts...)
	require.NotNil(t, server, "should create valid gRPC server")
	server.Stop()
}

// TestBuildServerOptionsWithAggressiveKeepalive tests high-frequency keepalive settings
func TestBuildServerOptionsWithAggressiveKeepalive(t *testing.T) {
	cfg := GRPCConfig{
		KeepAliveTime:                10 * time.Second,
		KeepAliveTimeout:             5 * time.Second,
		KeepAliveMinTime:             5 * time.Second,
		KeepAlivePermitWithoutStream: true,
		MaxConcurrentStreams:         1000,
		InitialWindowSize:            4 << 20,
		InitialConnWindowSize:        8 << 20,
		MaxRecvMsgSize:               256 * 1024 * 1024,
		MaxSendMsgSize:               256 * 1024 * 1024,
	}

	opts := cfg.BuildServerOptions()
	server := grpc.NewServer(opts...)
	require.NotNil(t, server)
	server.Stop()
}

// TestBuildClientOptions verifies gRPC client options are correctly built
func TestBuildClientOptions(t *testing.T) {
	cfg := DefaultGRPCConfig()
	opts := cfg.BuildClientOptions()

	// Should return dial options for keepalive, window sizes, message sizes
	require.GreaterOrEqual(t, len(opts), 4, "should have at least 4 client options")
}

// TestKeepaliveServerParams verifies keepalive.ServerParameters are built correctly
func TestKeepaliveServerParams(t *testing.T) {
	cfg := GRPCConfig{
		KeepAliveTime:    1 * time.Minute,
		KeepAliveTimeout: 30 * time.Second,
	}

	params := cfg.ServerKeepaliveParams()

	assert.Equal(t, 1*time.Minute, params.Time)
	assert.Equal(t, 30*time.Second, params.Timeout)
}

// TestKeepaliveEnforcementPolicy verifies keepalive.EnforcementPolicy is built correctly
func TestKeepaliveEnforcementPolicy(t *testing.T) {
	cfg := GRPCConfig{
		KeepAliveMinTime:             2 * time.Minute,
		KeepAlivePermitWithoutStream: true,
	}

	policy := cfg.ServerEnforcementPolicy()

	assert.Equal(t, 2*time.Minute, policy.MinTime)
	assert.True(t, policy.PermitWithoutStream)
}

// TestClientKeepaliveParams verifies client keepalive parameters
func TestClientKeepaliveParams(t *testing.T) {
	cfg := GRPCConfig{
		KeepAliveTime:                60 * time.Second,
		KeepAliveTimeout:             20 * time.Second,
		KeepAlivePermitWithoutStream: true,
	}

	params := cfg.ClientKeepaliveParams()

	assert.Equal(t, 60*time.Second, params.Time)
	assert.Equal(t, 20*time.Second, params.Timeout)
	assert.True(t, params.PermitWithoutStream)
}

// TestGRPCConfigValidation verifies configuration validation
func TestGRPCConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     GRPCConfig
		wantErr bool
	}{
		{
			name:    "default config is valid",
			cfg:     DefaultGRPCConfig(),
			wantErr: false,
		},
		{
			name: "zero max concurrent streams is invalid",
			cfg: GRPCConfig{
				MaxConcurrentStreams: 0,
			},
			wantErr: true,
		},
		{
			name: "negative window size is invalid",
			cfg: GRPCConfig{
				MaxConcurrentStreams:  100,
				InitialWindowSize:     -1,
				InitialConnWindowSize: 1 << 20,
			},
			wantErr: true,
		},
		{
			name: "zero keepalive time uses system default (valid)",
			cfg: GRPCConfig{
				KeepAliveTime:         0,
				MaxConcurrentStreams:  100,
				InitialWindowSize:     1 << 20,
				InitialConnWindowSize: 1 << 20,
				MaxRecvMsgSize:        64 * 1024 * 1024,
				MaxSendMsgSize:        64 * 1024 * 1024,
			},
			wantErr: false,
		},
		{
			name: "very large window sizes are valid",
			cfg: GRPCConfig{
				MaxConcurrentStreams:  100,
				InitialWindowSize:     16 << 20, // 16MB
				InitialConnWindowSize: 16 << 20,
				MaxRecvMsgSize:        256 * 1024 * 1024,
				MaxSendMsgSize:        256 * 1024 * 1024,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestGRPCConfigString verifies string representation for logging
func TestGRPCConfigString(t *testing.T) {
	cfg := DefaultGRPCConfig()
	str := cfg.String()

	// Should contain key configuration values for debugging
	assert.Contains(t, str, "keepalive_time")
	assert.Contains(t, str, "max_concurrent_streams")
	assert.Contains(t, str, "initial_window_size")
}

// Benchmark tests for option building
func BenchmarkBuildServerOptions(b *testing.B) {
	cfg := DefaultGRPCConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.BuildServerOptions()
	}
}

func BenchmarkBuildClientOptions(b *testing.B) {
	cfg := DefaultGRPCConfig()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.BuildClientOptions()
	}
}
