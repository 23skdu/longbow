package main

import (
	"testing"
	"time"
)

// Unit tests for config.go - covers extracted helper functions

func TestValidateConfig_Valid(t *testing.T) {
	cfg := DefaultConfig()
	if err := ValidateConfig(&cfg); err != nil {
		t.Errorf("ValidateConfig() error = %v, want nil", err)
	}
}

func TestValidateConfig_EmptyListenAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = ""
	if err := ValidateConfig(&cfg); err != ErrInvalidListenAddr {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidListenAddr)
	}
}

func TestValidateConfig_EmptyMetaAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MetaAddr = ""
	if err := ValidateConfig(&cfg); err != ErrInvalidMetaAddr {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidMetaAddr)
	}
}

func TestValidateConfig_EmptyMetricsAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MetricsAddr = ""
	if err := ValidateConfig(&cfg); err != ErrInvalidMetricsAddr {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidMetricsAddr)
	}
}

func TestValidateConfig_InvalidMaxMemory(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxMemory = 0
	if err := ValidateConfig(&cfg); err != ErrInvalidMaxMemory {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidMaxMemory)
	}

	cfg.MaxMemory = -100
	if err := ValidateConfig(&cfg); err != ErrInvalidMaxMemory {
		t.Errorf("ValidateConfig() with negative error = %v, want %v", err, ErrInvalidMaxMemory)
	}
}

func TestValidateConfig_EmptyDataPath(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataPath = ""
	if err := ValidateConfig(&cfg); err != ErrInvalidDataPath {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidDataPath)
	}
}

func TestValidateConfig_InvalidMaxWALSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxWALSize = 0
	if err := ValidateConfig(&cfg); err != ErrInvalidMaxWALSize {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidMaxWALSize)
	}
}

func TestValidateConfig_InvalidLogFormat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LogFormat = "xml"
	if err := ValidateConfig(&cfg); err != ErrInvalidLogFormat {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidLogFormat)
	}
}

func TestValidateConfig_ValidLogFormats(t *testing.T) {
	for _, format := range []string{"json", "console"} {
		cfg := DefaultConfig()
		cfg.LogFormat = format
		if err := ValidateConfig(&cfg); err != nil {
			t.Errorf("ValidateConfig() with LogFormat=%q error = %v, want nil", format, err)
		}
	}
}

func TestValidateConfig_InvalidLogLevel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LogLevel = "trace"
	if err := ValidateConfig(&cfg); err != ErrInvalidLogLevel {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidLogLevel)
	}
}

func TestValidateConfig_ValidLogLevels(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error"} {
		cfg := DefaultConfig()
		cfg.LogLevel = level
		if err := ValidateConfig(&cfg); err != nil {
			t.Errorf("ValidateConfig() with LogLevel=%q error = %v, want nil", level, err)
		}
	}
}

func TestValidateConfig_InvalidKeepAliveTime(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepAliveTime = 0
	if err := ValidateConfig(&cfg); err != ErrInvalidKeepAliveTime {
		t.Errorf("ValidateConfig() error = %v, want %v", err, ErrInvalidKeepAliveTime)
	}
}

// CalculateEvictionInterval tests

func TestCalculateEvictionInterval_ZeroTTL(t *testing.T) {
	result := CalculateEvictionInterval(0)
	if result != 0 {
		t.Errorf("CalculateEvictionInterval(0) = %v, want 0", result)
	}
}

func TestCalculateEvictionInterval_NegativeTTL(t *testing.T) {
	result := CalculateEvictionInterval(-time.Hour)
	if result != 0 {
		t.Errorf("CalculateEvictionInterval(-1h) = %v, want 0", result)
	}
}

func TestCalculateEvictionInterval_LargeTTL(t *testing.T) {
	// 1 hour TTL -> 1/10 is 6min, but capped at 1min
	result := CalculateEvictionInterval(time.Hour)
	if result != time.Minute {
		t.Errorf("CalculateEvictionInterval(1h) = %v, want %v", result, time.Minute)
	}
}

func TestCalculateEvictionInterval_SmallTTL(t *testing.T) {
	// 30 second TTL -> 1/10 is 3s
	result := CalculateEvictionInterval(30 * time.Second)
	if result != 3*time.Second {
		t.Errorf("CalculateEvictionInterval(30s) = %v, want %v", result, 3*time.Second)
	}
}

func TestCalculateEvictionInterval_VerySmallTTL(t *testing.T) {
	// 5 second TTL -> 1/10 is 500ms, capped at 1s minimum
	result := CalculateEvictionInterval(5 * time.Second)
	if result != time.Second {
		t.Errorf("CalculateEvictionInterval(5s) = %v, want %v", result, time.Second)
	}
}

func TestCalculateEvictionInterval_TinyTTL(t *testing.T) {
	// 100ms TTL -> 1/10 is 10ms, capped at 1s minimum
	result := CalculateEvictionInterval(100 * time.Millisecond)
	if result != time.Second {
		t.Errorf("CalculateEvictionInterval(100ms) = %v, want %v", result, time.Second)
	}
}

// BuildKeepaliveParams tests

func TestBuildKeepaliveParams(t *testing.T) {
	cfg := DefaultConfig()
	params := BuildKeepaliveParams(&cfg)

	if params.Time != cfg.KeepAliveTime {
		t.Errorf("BuildKeepaliveParams().Time = %v, want %v", params.Time, cfg.KeepAliveTime)
	}
	if params.Timeout != cfg.KeepAliveTimeout {
		t.Errorf("BuildKeepaliveParams().Timeout = %v, want %v", params.Timeout, cfg.KeepAliveTimeout)
	}
}

func TestBuildKeepaliveParams_CustomValues(t *testing.T) {
	cfg := Config{
		KeepAliveTime:    30 * time.Second,
		KeepAliveTimeout: 10 * time.Second,
	}
	params := BuildKeepaliveParams(&cfg)

	if params.Time != 30*time.Second {
		t.Errorf("BuildKeepaliveParams().Time = %v, want 30s", params.Time)
	}
	if params.Timeout != 10*time.Second {
		t.Errorf("BuildKeepaliveParams().Timeout = %v, want 10s", params.Timeout)
	}
}

// BuildKeepalivePolicy tests

func TestBuildKeepalivePolicy(t *testing.T) {
	cfg := DefaultConfig()
	policy := BuildKeepalivePolicy(&cfg)

	if policy.MinTime != cfg.KeepAliveMinTime {
		t.Errorf("BuildKeepalivePolicy().MinTime = %v, want %v", policy.MinTime, cfg.KeepAliveMinTime)
	}
	if policy.PermitWithoutStream != cfg.KeepAlivePermitWithoutStream {
		t.Errorf("BuildKeepalivePolicy().PermitWithoutStream = %v, want %v", policy.PermitWithoutStream, cfg.KeepAlivePermitWithoutStream)
	}
}

func TestBuildKeepalivePolicy_PermitWithoutStream(t *testing.T) {
	cfg := DefaultConfig()
	cfg.KeepAlivePermitWithoutStream = true
	policy := BuildKeepalivePolicy(&cfg)

	if !policy.PermitWithoutStream {
		t.Error("BuildKeepalivePolicy().PermitWithoutStream = false, want true")
	}
}

// DefaultConfig tests

func TestDefaultConfig_Values(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ListenAddr != "0.0.0.0:3000" {
		t.Errorf("DefaultConfig().ListenAddr = %q, want %q", cfg.ListenAddr, "0.0.0.0:3000")
	}
	if cfg.MetaAddr != "0.0.0.0:3001" {
		t.Errorf("DefaultConfig().MetaAddr = %q, want %q", cfg.MetaAddr, "0.0.0.0:3001")
	}
	if cfg.MetricsAddr != "0.0.0.0:9090" {
		t.Errorf("DefaultConfig().MetricsAddr = %q, want %q", cfg.MetricsAddr, "0.0.0.0:9090")
	}
	if cfg.MaxMemory != 1073741824 {
		t.Errorf("DefaultConfig().MaxMemory = %d, want 1073741824", cfg.MaxMemory)
	}
	if cfg.DataPath != "./data" {
		t.Errorf("DefaultConfig().DataPath = %q, want %q", cfg.DataPath, "./data")
	}
	if cfg.LogFormat != "json" {
		t.Errorf("DefaultConfig().LogFormat = %q, want %q", cfg.LogFormat, "json")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("DefaultConfig().LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
}

func TestDefaultConfig_IsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := ValidateConfig(&cfg); err != nil {
		t.Errorf("ValidateConfig(DefaultConfig()) = %v, want nil", err)
	}
}
