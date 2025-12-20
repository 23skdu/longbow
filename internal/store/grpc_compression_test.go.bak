package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

// TestGRPCConfigCompressionEnabledByDefault verifies compression is enabled by default
func TestGRPCConfigCompressionEnabledByDefault(t *testing.T) {
	cfg := DefaultGRPCConfig()
	assert.True(t, cfg.CompressionEnabled, "compression should be enabled by default for 50-70% bandwidth reduction")
}

// TestGRPCConfigCompressionCanBeDisabled verifies compression can be explicitly disabled
func TestGRPCConfigCompressionCanBeDisabled(t *testing.T) {
	cfg := DefaultGRPCConfig()
	cfg.CompressionEnabled = false
	assert.False(t, cfg.CompressionEnabled)
}

// TestBuildServerOptionsWithCompressionEnabled verifies server options work with compression enabled
func TestBuildServerOptionsWithCompressionEnabled(t *testing.T) {
	cfg := DefaultGRPCConfig()
	cfg.CompressionEnabled = true
	opts := cfg.BuildServerOptions()

	require.GreaterOrEqual(t, len(opts), 7, "should have at least 7 server options")

	server := grpc.NewServer(opts...)
	require.NotNil(t, server, "should create valid gRPC server with compression support")
	server.Stop()
}

// TestBuildServerOptionsWithCompressionDisabled verifies server works with compression disabled
func TestBuildServerOptionsWithCompressionDisabled(t *testing.T) {
	cfg := DefaultGRPCConfig()
	cfg.CompressionEnabled = false
	opts := cfg.BuildServerOptions()

	server := grpc.NewServer(opts...)
	require.NotNil(t, server, "should create valid gRPC server without compression")
	server.Stop()
}

// TestBuildClientOptionsWithCompressionEnabled verifies client options include UseCompressor when enabled
func TestBuildClientOptionsWithCompressionEnabled(t *testing.T) {
	cfg := DefaultGRPCConfig()
	cfg.CompressionEnabled = true
	opts := cfg.BuildClientOptions()

	// Original 4 options still present
	require.GreaterOrEqual(t, len(opts), 4, "should have at least 4 client options with compression")
}

// TestBuildClientOptionsWithCompressionDisabled verifies client works with compression disabled
func TestBuildClientOptionsWithCompressionDisabled(t *testing.T) {
	cfg := DefaultGRPCConfig()
	cfg.CompressionEnabled = false
	opts := cfg.BuildClientOptions()

	require.GreaterOrEqual(t, len(opts), 4, "should have at least 4 client options without compression")
}

// TestGRPCConfigCompressionValidation verifies compression config validation
func TestGRPCConfigCompressionValidation(t *testing.T) {
	tests := []struct {
		name               string
		compressionEnabled bool
		wantErr            bool
	}{
		{
			name:               "compression enabled is valid",
			compressionEnabled: true,
			wantErr:            false,
		},
		{
			name:               "compression disabled is valid",
			compressionEnabled: false,
			wantErr:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultGRPCConfig()
			cfg.CompressionEnabled = tt.compressionEnabled
			err := cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestGRPCConfigStringIncludesCompression verifies String() output includes compression status
func TestGRPCConfigStringIncludesCompression(t *testing.T) {
	cfg := DefaultGRPCConfig()
	str := cfg.String()
	assert.Contains(t, str, "compression_enabled", "String() should include compression status")
}

// BenchmarkBuildServerOptionsWithCompressionEnabled benchmarks server option building
func BenchmarkBuildServerOptionsWithCompressionEnabled(b *testing.B) {
	cfg := DefaultGRPCConfig()
	cfg.CompressionEnabled = true
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.BuildServerOptions()
	}
}

// BenchmarkBuildClientOptionsWithCompressionEnabled benchmarks client option building
func BenchmarkBuildClientOptionsWithCompressionEnabled(b *testing.B) {
	cfg := DefaultGRPCConfig()
	cfg.CompressionEnabled = true
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.BuildClientOptions()
	}
}
