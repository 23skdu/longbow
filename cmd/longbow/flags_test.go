package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestParseCLIFlags_VersionFlag(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--version"}, &buf)
	if err != ErrVersionRequested {
		t.Fatalf("expected ErrVersionRequested, got %v", err)
	}
	if !strings.Contains(buf.String(), "Longbow version") {
		t.Errorf("expected version output, got %q", buf.String())
	}
}

func TestParseCLIFlags_VShortFlag(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"-v"}, &buf)
	if err != ErrVersionRequested {
		t.Fatalf("expected ErrVersionRequested, got %v", err)
	}
	if !strings.Contains(buf.String(), "Longbow version") {
		t.Errorf("expected version output, got %q", buf.String())
	}
}

func TestParseCLIFlags_HelpFlag(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--help"}, &buf)
	if err != ErrHelpRequested {
		t.Fatalf("expected ErrHelpRequested, got %v", err)
	}
	if !strings.Contains(buf.String(), "Longbow – vector search server") {
		t.Errorf("expected usage output, got %q", buf.String())
	}
}

func TestParseCLIFlags_UnknownFlag(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--bogus"}, &buf)
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
	if strings.Contains(err.Error(), "help requested") {
		t.Fatalf("unexpected help error: %v", err)
	}
}

func TestParseCLIFlags_NoArgs(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{}, &buf)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestParseCLIFlags_MaxMemory(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--max-memory", "17179869184"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxMemory != 17179869184 {
		t.Errorf("expected MaxMemory=17179869184, got %d", cfg.MaxMemory)
	}
}

func TestParseCLIFlags_MaxMemoryEquals(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--max-memory=999"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxMemory != 999 {
		t.Errorf("expected MaxMemory=999, got %d", cfg.MaxMemory)
	}
}

func TestParseCLIFlags_ListenAddr(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--listen-addr", "0.0.0.0:4000"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:4000" {
		t.Errorf("expected ListenAddr=0.0.0.0:4000, got %q", cfg.ListenAddr)
	}
}

func TestParseCLIFlags_DataPath(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--data-path", "/custom/path"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DataPath != "/custom/path" {
		t.Errorf("expected DataPath=/custom/path, got %q", cfg.DataPath)
	}
}

func TestParseCLIFlags_NodeID(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--node-id", "my-node-42"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.NodeID != "my-node-42" {
		t.Errorf("expected NodeID=my-node-42, got %q", cfg.NodeID)
	}
}

func TestParseCLIFlags_LogFormat(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--log-format", "console"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LogFormat != "console" {
		t.Errorf("expected LogFormat=console, got %q", cfg.LogFormat)
	}
}

func TestParseCLIFlags_LogLevel(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--log-level", "debug"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel=debug, got %q", cfg.LogLevel)
	}
}

func TestParseCLIFlags_GPUEnabled(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--gpu-enabled"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.GPUEnabled {
		t.Error("expected GPUEnabled=true")
	}
}

func TestParseCLIFlags_GPUDeviceID(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--gpu-device-id", "2"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.GPUDeviceID != 2 {
		t.Errorf("expected GPUDeviceID=2, got %d", cfg.GPUDeviceID)
	}
}

func TestParseCLIFlags_GossipEnabled(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--gossip-enabled"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.GossipEnabled {
		t.Error("expected GossipEnabled=true")
	}
}

func TestParseCLIFlags_MultipleFlags(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	args := []string{
		"--max-memory", "1073741824",
		"--listen-addr", "0.0.0.0:5000",
		"--data-path", "/var/longbow/data",
		"--log-format", "json",
		"--log-level", "warn",
		"--gpu-enabled",
		"--gpu-device-id", "1",
		"--gossip-enabled",
	}
	err := parseCLIFlags(&cfg, args, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxMemory != 1073741824 {
		t.Errorf("MaxMemory = %d", cfg.MaxMemory)
	}
	if cfg.ListenAddr != "0.0.0.0:5000" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.DataPath != "/var/longbow/data" {
		t.Errorf("DataPath = %q", cfg.DataPath)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q", cfg.LogFormat)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
	if !cfg.GPUEnabled {
		t.Error("GPUEnabled should be true")
	}
	if cfg.GPUDeviceID != 1 {
		t.Errorf("GPUDeviceID = %d", cfg.GPUDeviceID)
	}
	if !cfg.GossipEnabled {
		t.Error("GossipEnabled should be true")
	}
}

func TestParseCLIFlags_OverridesDefaults(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{
		ListenAddr: "0.0.0.0:3000",
		MaxMemory:  1073741824,
	}
	err := parseCLIFlags(&cfg, []string{"--listen-addr", "0.0.0.0:9999", "--max-memory", "888"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:9999" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.MaxMemory != 888 {
		t.Errorf("MaxMemory = %d", cfg.MaxMemory)
	}
}

func TestParseCLIFlags_MetaAddr(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--meta-addr", "0.0.0.0:3005"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MetaAddr != "0.0.0.0:3005" {
		t.Errorf("expected MetaAddr=0.0.0.0:3005, got %q", cfg.MetaAddr)
	}
}

func TestParseCLIFlags_MetricsAddr(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{}
	err := parseCLIFlags(&cfg, []string{"--metrics-addr", "0.0.0.0:9091"}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MetricsAddr != "0.0.0.0:9091" {
		t.Errorf("expected MetricsAddr=0.0.0.0:9091, got %q", cfg.MetricsAddr)
	}
}

func TestParseCLIFlags_MetaAddrEmptyDefault(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{MetaAddr: "0.0.0.0:3001"}
	err := parseCLIFlags(&cfg, []string{}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MetaAddr != "0.0.0.0:3001" {
		t.Errorf("expected MetaAddr default preserved, got %q", cfg.MetaAddr)
	}
}

func TestParseCLIFlags_EmptyArgsPreservesLogDefaults(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{LogFormat: "json", LogLevel: "info"}
	err := parseCLIFlags(&cfg, []string{}, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q", cfg.LogFormat)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
}
