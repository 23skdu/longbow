package main

import (
	"errors"
	"time"

	"google.golang.org/grpc/keepalive"
)

// Config validation errors
var (
	ErrInvalidListenAddr    = errors.New("listen_addr cannot be empty")
	ErrInvalidMetaAddr      = errors.New("meta_addr cannot be empty")
	ErrInvalidMetricsAddr   = errors.New("metrics_addr cannot be empty")
	ErrInvalidMaxMemory     = errors.New("max_memory must be positive")
	ErrInvalidDataPath      = errors.New("data_path cannot be empty")
	ErrInvalidMaxWALSize    = errors.New("max_wal_size must be positive")
	ErrInvalidLogFormat     = errors.New("log_format must be 'json' or 'console'")
	ErrInvalidLogLevel      = errors.New("log_level must be debug, info, warn, or error")
	ErrInvalidKeepAliveTime = errors.New("keepalive_time must be positive")
)

// ValidateConfig validates the configuration and returns an error if invalid
func ValidateConfig(cfg *Config) error {
	if cfg.ListenAddr == "" {
		return ErrInvalidListenAddr
	}
	if cfg.MetaAddr == "" {
		return ErrInvalidMetaAddr
	}
	if cfg.MetricsAddr == "" {
		return ErrInvalidMetricsAddr
	}
	if cfg.MaxMemory <= 0 {
		return ErrInvalidMaxMemory
	}
	if cfg.DataPath == "" {
		return ErrInvalidDataPath
	}
	if cfg.MaxWALSize <= 0 {
		return ErrInvalidMaxWALSize
	}
	if cfg.LogFormat != "json" && cfg.LogFormat != "console" {
		return ErrInvalidLogFormat
	}
	if cfg.LogLevel != "debug" && cfg.LogLevel != "info" && cfg.LogLevel != "warn" && cfg.LogLevel != "error" {
		return ErrInvalidLogLevel
	}
	if cfg.KeepAliveTime <= 0 {
		return ErrInvalidKeepAliveTime
	}
	return nil
}

// CalculateEvictionInterval computes the eviction check interval based on TTL
// Returns the interval to use for eviction checks, or 0 if TTL is disabled
func CalculateEvictionInterval(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}

	checkInterval := time.Minute
	if ttl/10 < checkInterval {
		checkInterval = ttl / 10
	}
	if checkInterval < time.Second {
		checkInterval = time.Second
	}
	return checkInterval
}

// BuildKeepaliveParams creates gRPC keepalive server parameters from config
func BuildKeepaliveParams(cfg *Config) keepalive.ServerParameters {
	return keepalive.ServerParameters{
		Time:    cfg.KeepAliveTime,
		Timeout: cfg.KeepAliveTimeout,
	}
}

// BuildKeepalivePolicy creates gRPC keepalive enforcement policy from config
func BuildKeepalivePolicy(cfg *Config) keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             cfg.KeepAliveMinTime,
		PermitWithoutStream: cfg.KeepAlivePermitWithoutStream,
	}
}
