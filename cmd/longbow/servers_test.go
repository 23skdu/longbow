package main

import (
	"io"
	"testing"
)

func TestMetricsListenAddr(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want string
	}{
		{name: "nil config", cfg: nil, want: ":6000"},
		{name: "empty address", cfg: &Config{}, want: ":6000"},
		{name: "config default", cfg: &Config{MetricsAddr: "0.0.0.0:9090"}, want: "0.0.0.0:9090"},
		{name: "environment override", cfg: &Config{MetricsAddr: "127.0.0.1:19090"}, want: "127.0.0.1:19090"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := metricsListenAddr(tt.cfg); got != tt.want {
				t.Fatalf("metricsListenAddr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMetricsListenAddrUsesCLIFlag(t *testing.T) {
	cfg := &Config{MetricsAddr: "0.0.0.0:9090"}
	if err := parseCLIFlags(cfg, []string{"--metrics-addr", "127.0.0.1:18080"}, io.Discard); err != nil {
		t.Fatalf("parseCLIFlags() error = %v", err)
	}
	if got := metricsListenAddr(cfg); got != "127.0.0.1:18080" {
		t.Fatalf("metricsListenAddr() = %q, want CLI override", got)
	}
}
