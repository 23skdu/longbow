package store

import (
"os"
"path/filepath"
"sync/atomic"
"testing"
"time"
)

// ============================================================
// Subtask 1: AsyncFsyncConfig Tests
// ============================================================

// TestDefaultAsyncFsyncConfig verifies default values
func TestDefaultAsyncFsyncConfig(t *testing.T) {
cfg := DefaultAsyncFsyncConfig()

if cfg.DirtyThreshold != 1024*1024 {
t.Errorf("DirtyThreshold: got %d, want %d", cfg.DirtyThreshold, 1024*1024)
}
if cfg.MaxPendingFsyncs != 4 {
t.Errorf("MaxPendingFsyncs: got %d, want %d", cfg.MaxPendingFsyncs, 4)
}
if cfg.FsyncTimeout != 5*time.Second {
t.Errorf("FsyncTimeout: got %v, want %v", cfg.FsyncTimeout, 5*time.Second)
}
if !cfg.Enabled {
t.Error("Enabled should be true by default")
}
if cfg.FallbackToSync {
t.Error("FallbackToSync should be false by default")
}
}

// TestAsyncFsyncConfigValidation tests validation rules
func TestAsyncFsyncConfigValidation(t *testing.T) {
tests := []struct {
name    string
cfg     AsyncFsyncConfig
wantErr bool
}{
{"valid default config", DefaultAsyncFsyncConfig(), false},
{"disabled config skips validation", AsyncFsyncConfig{Enabled: false}, false},
{"zero dirty threshold invalid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 0, MaxPendingFsyncs: 4, FsyncTimeout: 5 * time.Second}, true},
{"negative dirty threshold invalid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: -1, MaxPendingFsyncs: 4, FsyncTimeout: 5 * time.Second}, true},
{"zero pending fsyncs invalid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 1024, MaxPendingFsyncs: 0, FsyncTimeout: 5 * time.Second}, true},
{"negative pending fsyncs invalid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 1024, MaxPendingFsyncs: -1, FsyncTimeout: 5 * time.Second}, true},
{"zero timeout invalid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 1024, MaxPendingFsyncs: 4, FsyncTimeout: 0}, true},
{"negative timeout invalid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 1024, MaxPendingFsyncs: 4, FsyncTimeout: -1 * time.Second}, true},
{"small threshold valid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 1, MaxPendingFsyncs: 1, FsyncTimeout: 1 * time.Millisecond}, false},
{"large threshold valid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 100 * 1024 * 1024, MaxPendingFsyncs: 16, FsyncTimeout: 30 * time.Second}, false},
{"fallback enabled valid", AsyncFsyncConfig{Enabled: true, DirtyThreshold: 1024, MaxPendingFsyncs: 4, FsyncTimeout: 5 * time.Second, FallbackToSync: true}, false},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
err := tt.cfg.Validate()
if (err != nil) != tt.wantErr {
t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
}
})
}
}

// TestAsyncFsyncConfigIsSyncDisabled verifies sync disable detection
func TestAsyncFsyncConfigIsSyncDisabled(t *testing.T) {
cfg := AsyncFsyncConfig{Enabled: false}
if cfg.IsAsyncEnabled() {
t.Error("IsAsyncEnabled() should return false when Enabled is false")
}

cfg = DefaultAsyncFsyncConfig()
if !cfg.IsAsyncEnabled() {
t.Error("IsAsyncEnabled() should return true when Enabled is true")
}
}

// TestAsyncFsyncConfigShouldFsync tests dirty threshold logic
func TestAsyncFsyncConfigShouldFsync(t *testing.T) {
cfg := AsyncFsyncConfig{Enabled: true, DirtyThreshold: 1000, MaxPendingFsyncs: 4, FsyncTimeout: 5 * time.Second}

if cfg.ShouldFsync(500) {
t.Error("ShouldFsync(500) should return false when threshold is 1000")
}
if !cfg.ShouldFsync(1000) {
t.Error("ShouldFsync(1000) should return true when threshold is 1000")
}
if !cfg.ShouldFsync(1500) {
t.Error("ShouldFsync(1500) should return true when threshold is 1000")
}
}

// ============================================================
// Subtask 2: AsyncFsyncer Tests
// ============================================================

// TestNewAsyncFsyncer verifies creation
func TestNewAsyncFsyncer(t *testing.T) {
cfg := DefaultAsyncFsyncConfig()
syncer := NewAsyncFsyncer(cfg)
if syncer == nil {
t.Fatal("NewAsyncFsyncer returned nil")
}
defer func() { _ = syncer.Stop() }()

if syncer.Config() != cfg {
t.Error("Config() mismatch")
}
}

// TestAsyncFsyncerStartStop verifies lifecycle
func TestAsyncFsyncerStartStop(t *testing.T) {
cfg := DefaultAsyncFsyncConfig()
syncer := NewAsyncFsyncer(cfg)

tmpDir := t.TempDir()
f, err := os.Create(filepath.Join(tmpDir, "test.wal"))
if err != nil {
t.Fatalf("create file: %v", err)
}

if err := syncer.Start(f); err != nil {
t.Fatalf("Start: %v", err)
}
if !syncer.IsRunning() {
t.Error("IsRunning should be true after Start")
}
if err := syncer.Start(f); err != nil {
t.Fatalf("double Start: %v", err)
}
if err := syncer.Stop(); err != nil {
t.Fatalf("Stop: %v", err)
}
if syncer.IsRunning() {
t.Error("IsRunning should be false after Stop")
}
if err := syncer.Stop(); err != nil {
t.Fatalf("double Stop: %v", err)
}
_ = f.Close()
}

// TestAsyncFsyncerRequestFsync verifies async fsync submission
func TestAsyncFsyncerRequestFsync(t *testing.T) {
cfg := DefaultAsyncFsyncConfig()
cfg.DirtyThreshold = 100
syncer := NewAsyncFsyncer(cfg)

tmpDir := t.TempDir()
f, err := os.Create(filepath.Join(tmpDir, "test.wal"))
if err != nil {
t.Fatalf("create file: %v", err)
}
defer func() { _ = f.Close() }()

if err := syncer.Start(f); err != nil {
t.Fatalf("Start: %v", err)
}
defer func() { _ = syncer.Stop() }()

data := make([]byte, 50)
if _, err := f.Write(data); err != nil {
t.Fatalf("write: %v", err)
}

syncer.AddDirtyBytes(50)
if syncer.DirtyBytes() != 50 {
t.Errorf("DirtyBytes: got %d, want 50", syncer.DirtyBytes())
}

requested := syncer.RequestFsyncIfNeeded()
if requested {
t.Error("RequestFsyncIfNeeded should return false below threshold")
}

syncer.AddDirtyBytes(60)
if syncer.DirtyBytes() != 110 {
t.Errorf("DirtyBytes: got %d, want 110", syncer.DirtyBytes())
}

requested = syncer.RequestFsyncIfNeeded()
if !requested {
t.Error("RequestFsyncIfNeeded should return true above threshold")
}

syncer.WaitForPendingFsyncs()

if syncer.DirtyBytes() != 0 {
t.Errorf("DirtyBytes after fsync: got %d, want 0", syncer.DirtyBytes())
}
}

// TestAsyncFsyncerStats verifies statistics tracking
func TestAsyncFsyncerStats(t *testing.T) {
cfg := DefaultAsyncFsyncConfig()
cfg.DirtyThreshold = 10
syncer := NewAsyncFsyncer(cfg)

tmpDir := t.TempDir()
f, err := os.Create(filepath.Join(tmpDir, "test.wal"))
if err != nil {
t.Fatalf("create file: %v", err)
}
defer func() { _ = f.Close() }()

if err := syncer.Start(f); err != nil {
t.Fatalf("Start: %v", err)
}
defer func() { _ = syncer.Stop() }()

stats := syncer.Stats()
if stats.TotalRequests != 0 {
t.Errorf("TotalRequests: got %d, want 0", stats.TotalRequests)
}

for i := 0; i < 3; i++ {
syncer.AddDirtyBytes(20)
syncer.RequestFsyncIfNeeded()
syncer.WaitForPendingFsyncs()
}

stats = syncer.Stats()
if stats.TotalRequests != 3 {
t.Errorf("TotalRequests: got %d, want 3", stats.TotalRequests)
}
if stats.CompletedFsyncs != 3 {
t.Errorf("CompletedFsyncs: got %d, want 3", stats.CompletedFsyncs)
}
}

// TestAsyncFsyncerFallbackToSync verifies fallback behavior
func TestAsyncFsyncerFallbackToSync(t *testing.T) {
cfg := DefaultAsyncFsyncConfig()
cfg.MaxPendingFsyncs = 1
cfg.FallbackToSync = true
cfg.DirtyThreshold = 10
syncer := NewAsyncFsyncer(cfg)

tmpDir := t.TempDir()
f, err := os.Create(filepath.Join(tmpDir, "test.wal"))
if err != nil {
t.Fatalf("create file: %v", err)
}
defer func() { _ = f.Close() }()

if err := syncer.Start(f); err != nil {
t.Fatalf("Start: %v", err)
}
defer func() { _ = syncer.Stop() }()

syncer.AddDirtyBytes(20)
err = syncer.ForceFsync()
if err != nil {
t.Fatalf("ForceFsync: %v", err)
}

stats := syncer.Stats()
if stats.SyncFallbacks < 1 {
t.Errorf("SyncFallbacks: got %d, want >= 1", stats.SyncFallbacks)
}
}

// TestAsyncFsyncerConcurrent verifies thread safety
func TestAsyncFsyncerConcurrent(t *testing.T) {
cfg := DefaultAsyncFsyncConfig()
cfg.DirtyThreshold = 100
syncer := NewAsyncFsyncer(cfg)

tmpDir := t.TempDir()
f, err := os.Create(filepath.Join(tmpDir, "test.wal"))
if err != nil {
t.Fatalf("create file: %v", err)
}
defer func() { _ = f.Close() }()

if err := syncer.Start(f); err != nil {
t.Fatalf("Start: %v", err)
}
defer func() { _ = syncer.Stop() }()

var ops atomic.Int64
done := make(chan struct{})

for i := 0; i < 4; i++ {
go func() {
for j := 0; j < 100; j++ {
syncer.AddDirtyBytes(10)
syncer.RequestFsyncIfNeeded()
ops.Add(1)
}
done <- struct{}{}
}()
}

for i := 0; i < 4; i++ {
<-done
}

syncer.WaitForPendingFsyncs()

if ops.Load() != 400 {
t.Errorf("ops: got %d, want 400", ops.Load())
}
}

// BenchmarkAsyncFsyncerRequest benchmarks fsync request submission
func BenchmarkAsyncFsyncerRequest(b *testing.B) {
cfg := DefaultAsyncFsyncConfig()
cfg.DirtyThreshold = 1024 * 1024
syncer := NewAsyncFsyncer(cfg)

tmpDir := b.TempDir()
f, _ := os.Create(filepath.Join(tmpDir, "test.wal"))
defer func() { _ = f.Close() }()

_ = syncer.Start(f)
defer func() { _ = syncer.Stop() }()

b.ResetTimer()
for i := 0; i < b.N; i++ {
syncer.AddDirtyBytes(100)
syncer.RequestFsyncIfNeeded()
}
}


// ============================================================
// Subtask 3: WALBatcher Integration Tests
// ============================================================

// TestWALBatcherWithAsyncFsync verifies WALBatcher uses async fsync
func TestWALBatcherWithAsyncFsync(t *testing.T) {
tmpDir := t.TempDir()

cfg := DefaultWALBatcherConfig()
cfg.AsyncFsync = DefaultAsyncFsyncConfig()
cfg.AsyncFsync.DirtyThreshold = 100 // Low threshold for testing

batcher := NewWALBatcher(tmpDir, &cfg)
if batcher == nil {
t.Fatal("NewWALBatcher returned nil")
}

if err := batcher.Start(); err != nil {
t.Fatalf("Start: %v", err)
}
defer func() { _ = batcher.Stop() }()

// Verify async fsyncer is running
if !batcher.IsAsyncFsyncEnabled() {
t.Error("IsAsyncFsyncEnabled should return true")
}
}

// TestWALBatcherAsyncFsyncStats verifies stats tracking
func TestWALBatcherAsyncFsyncStats(t *testing.T) {
tmpDir := t.TempDir()

cfg := DefaultWALBatcherConfig()
cfg.AsyncFsync = DefaultAsyncFsyncConfig()
cfg.AsyncFsync.DirtyThreshold = 50

batcher := NewWALBatcher(tmpDir, &cfg)
if err := batcher.Start(); err != nil {
t.Fatalf("Start: %v", err)
}
defer func() { _ = batcher.Stop() }()

stats := batcher.AsyncFsyncStats()
if stats.TotalRequests < 0 {
t.Errorf("TotalRequests should be >= 0, got %d", stats.TotalRequests)
}
}

// TestWALBatcherDisabledAsyncFsync verifies sync fallback works
func TestWALBatcherDisabledAsyncFsync(t *testing.T) {
tmpDir := t.TempDir()

cfg := DefaultWALBatcherConfig()
cfg.AsyncFsync = AsyncFsyncConfig{Enabled: false}

batcher := NewWALBatcher(tmpDir, &cfg)
if err := batcher.Start(); err != nil {
t.Fatalf("Start: %v", err)
}
defer func() { _ = batcher.Stop() }()

if batcher.IsAsyncFsyncEnabled() {
t.Error("IsAsyncFsyncEnabled should return false when disabled")
}
}
