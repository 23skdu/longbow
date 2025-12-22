package main

import (
	"os"
	"testing"

	"github.com/kelseyhightower/envconfig"
)

// TestGRPCServerConfigEnvVars verifies environment variable parsing for gRPC server options
func TestGRPCServerConfigEnvVars(t *testing.T) {
	// Set env vars
	os.Setenv("LONGBOW_GRPC_MAX_RECV_MSG_SIZE", "33554432")       //nolint:errcheck // test helper  // 32MB
	os.Setenv("LONGBOW_GRPC_MAX_SEND_MSG_SIZE", "16777216")       //nolint:errcheck // test helper  // 16MB
	os.Setenv("LONGBOW_GRPC_INITIAL_WINDOW_SIZE", "2097152")      //nolint:errcheck // test helper // 2MB
	os.Setenv("LONGBOW_GRPC_INITIAL_CONN_WINDOW_SIZE", "4194304") //nolint:errcheck // test helper // 4MB
	os.Setenv("LONGBOW_GRPC_MAX_CONCURRENT_STREAMS", "500")       //nolint:errcheck // test helper
	defer func() {                                                //nolint:errcheck // test cleanup
		_ = os.Unsetenv("LONGBOW_GRPC_MAX_RECV_MSG_SIZE")
		_ = os.Unsetenv("LONGBOW_GRPC_MAX_SEND_MSG_SIZE")
		_ = os.Unsetenv("LONGBOW_GRPC_INITIAL_WINDOW_SIZE")
		_ = os.Unsetenv("LONGBOW_GRPC_INITIAL_CONN_WINDOW_SIZE")
		_ = os.Unsetenv("LONGBOW_GRPC_MAX_CONCURRENT_STREAMS")
	}()

	var cfg Config
	if err := envconfig.Process("LONGBOW", &cfg); err != nil {
		t.Fatalf("Failed to process config: %v", err)
	}

	// Verify parsed values
	if cfg.GRPCMaxRecvMsgSize != 33554432 {
		t.Errorf("GRPCMaxRecvMsgSize = %d, want 33554432", cfg.GRPCMaxRecvMsgSize)
	}
	if cfg.GRPCMaxSendMsgSize != 16777216 {
		t.Errorf("GRPCMaxSendMsgSize = %d, want 16777216", cfg.GRPCMaxSendMsgSize)
	}
	if cfg.GRPCInitialWindowSize != 2097152 {
		t.Errorf("GRPCInitialWindowSize = %d, want 2097152", cfg.GRPCInitialWindowSize)
	}
	if cfg.GRPCInitialConnWindowSize != 4194304 {
		t.Errorf("GRPCInitialConnWindowSize = %d, want 4194304", cfg.GRPCInitialConnWindowSize)
	}
	if cfg.GRPCMaxConcurrentStreams != 500 {
		t.Errorf("GRPCMaxConcurrentStreams = %d, want 500", cfg.GRPCMaxConcurrentStreams)
	}
}

// TestGRPCServerConfigDefaults verifies default values
func TestGRPCServerConfigDefaults(t *testing.T) {
	// Clear any env vars that might interfere
	_ = os.Unsetenv("LONGBOW_GRPC_MAX_RECV_MSG_SIZE")        //nolint:errcheck
	_ = os.Unsetenv("LONGBOW_GRPC_MAX_SEND_MSG_SIZE")        //nolint:errcheck
	_ = os.Unsetenv("LONGBOW_GRPC_INITIAL_WINDOW_SIZE")      //nolint:errcheck
	_ = os.Unsetenv("LONGBOW_GRPC_INITIAL_CONN_WINDOW_SIZE") //nolint:errcheck
	_ = os.Unsetenv("LONGBOW_GRPC_MAX_CONCURRENT_STREAMS")   //nolint:errcheck

	var cfg Config
	if err := envconfig.Process("LONGBOW", &cfg); err != nil {
		t.Fatalf("Failed to process config: %v", err)
	}

	// Default: 512MB
	expectedMaxRecv := 512 * 1024 * 1024
	if cfg.GRPCMaxRecvMsgSize != expectedMaxRecv {
		t.Errorf("GRPCMaxRecvMsgSize default = %d, want %d", cfg.GRPCMaxRecvMsgSize, expectedMaxRecv)
	}
	if cfg.GRPCMaxSendMsgSize != expectedMaxRecv {
		t.Errorf("GRPCMaxSendMsgSize default = %d, want %d", cfg.GRPCMaxSendMsgSize, expectedMaxRecv)
	}
	// Default: 1MB
	expectedWindow := int32(1 << 20)
	if cfg.GRPCInitialWindowSize != expectedWindow {
		t.Errorf("GRPCInitialWindowSize default = %d, want %d", cfg.GRPCInitialWindowSize, expectedWindow)
	}
	if cfg.GRPCInitialConnWindowSize != expectedWindow {
		t.Errorf("GRPCInitialConnWindowSize default = %d, want %d", cfg.GRPCInitialConnWindowSize, expectedWindow)
	}
	// Default: 250
	if cfg.GRPCMaxConcurrentStreams != 250 {
		t.Errorf("GRPCMaxConcurrentStreams default = %d, want 250", cfg.GRPCMaxConcurrentStreams)
	}
}

// TestBuildGRPCServerOptions verifies BuildGRPCServerOptions returns correct options
func TestBuildGRPCServerOptions(t *testing.T) {
	cfg := Config{
		GRPCMaxRecvMsgSize:        32 * 1024 * 1024,
		GRPCMaxSendMsgSize:        32 * 1024 * 1024,
		GRPCInitialWindowSize:     2 * 1024 * 1024,
		GRPCInitialConnWindowSize: 4 * 1024 * 1024,
		GRPCMaxConcurrentStreams:  500,
	}

	opts := cfg.BuildGRPCServerOptions()
	if len(opts) == 0 {
		t.Error("BuildGRPCServerOptions returned empty slice")
	}
	// Should have at least 7 options: keepalive(2) + window(2) + msg size(2) + streams(1)
	if len(opts) < 7 {
		t.Errorf("BuildGRPCServerOptions returned %d options, want at least 7", len(opts))
	}
}

// TestGRPCServerConfigValidation verifies config validation
func TestGRPCServerConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				GRPCMaxRecvMsgSize:        64 * 1024 * 1024,
				GRPCMaxSendMsgSize:        64 * 1024 * 1024,
				GRPCInitialWindowSize:     1 << 20,
				GRPCInitialConnWindowSize: 1 << 20,
				GRPCMaxConcurrentStreams:  250,
			},
			wantErr: false,
		},
		{
			name: "zero max concurrent streams is invalid",
			cfg: Config{
				GRPCMaxRecvMsgSize:       64 * 1024 * 1024,
				GRPCMaxConcurrentStreams: 0,
			},
			wantErr: true,
		},
		{
			name: "negative window size is invalid",
			cfg: Config{
				GRPCMaxConcurrentStreams: 250,
				GRPCInitialWindowSize:    -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.ValidateGRPCConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateGRPCConfig() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
