package profiling

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type Profiler struct {
	logger *zerolog.Logger

	cpuProfileFile string
	memProfileFile string

	enabled atomic.Bool
	mu      sync.RWMutex

	profileDir string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type ProfilerConfig struct {
	ProfileDir      string
	CPUProfile      bool
	MemProfile      bool
	BlockProfile    bool
	MutexProfile    bool
	ProfileInterval time.Duration
}

func NewProfiler(config ProfilerConfig, logger *zerolog.Logger) *Profiler {
	if config.ProfileInterval == 0 {
		config.ProfileInterval = 30 * time.Second
	}

	if config.ProfileDir == "" {
		config.ProfileDir = "/tmp/longbow-profiles"
	}

	if err := os.MkdirAll(config.ProfileDir, 0755); err != nil {
		logger.Warn().Err(err).Msg("Failed to create profile directory")
	}

	return &Profiler{
		logger:     logger,
		profileDir: config.ProfileDir,
		stopCh:     make(chan struct{}),
	}
}

func (p *Profiler) Start(ctx context.Context) error {
	if !p.enabled.CompareAndSwap(false, true) {
		return fmt.Errorf("profiler already running")
	}

	p.wg.Add(1)
	go p.profileLoop(ctx)

	p.logger.Info().Msg("Profiler started")
	return nil
}

func (p *Profiler) Stop() error {
	if !p.enabled.CompareAndSwap(true, false) {
		return fmt.Errorf("profiler not running")
	}

	close(p.stopCh)
	p.wg.Wait()

	p.logger.Info().Msg("Profiler stopped")
	return nil
}

func (p *Profiler) profileLoop(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.collectProfiles()
		}
	}
}

func (p *Profiler) collectProfiles() {
	if !p.enabled.Load() {
		return
	}

	timestamp := time.Now().Format("20060102_150405")

	cpuProfile := filepath.Join(p.profileDir, fmt.Sprintf("cpu_%s.prof", timestamp))
	if err := p.writeCPUProfile(cpuProfile); err != nil {
		p.logger.Warn().Err(err).Msg("Failed to write CPU profile")
	}

	memProfile := filepath.Join(p.profileDir, fmt.Sprintf("mem_%s.prof", timestamp))
	if err := p.writeMemProfile(memProfile); err != nil {
		p.logger.Warn().Err(err).Msg("Failed to write memory profile")
	}

	p.logger.Debug().
		Str("cpu", cpuProfile).
		Str("mem", memProfile).
		Msg("Profiles collected")
}

func (p *Profiler) writeCPUProfile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	return pprof.StartCPUProfile(f)
}

func (p *Profiler) writeMemProfile(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	runtime.GC()
	_ = pprof.WriteHeapProfile(f) // nosec G104

	return nil
}

func (p *Profiler) WriteProfile(profileType string, w io.Writer) error {
	switch profileType {
	case "cpu":
		_ = pprof.StartCPUProfile(w) // nosec G104
		defer pprof.StopCPUProfile()
	case "heap":
		runtime.GC()
		_ = pprof.WriteHeapProfile(w) // nosec G104
	case "goroutine":
		_ = pprof.Lookup("goroutine").WriteTo(w, 2) // nosec G104
	case "threadcreate":
		_ = pprof.Lookup("threadcreate").WriteTo(w, 2) // nosec G104
	case "block":
		_ = pprof.Lookup("block").WriteTo(w, 2) // nosec G104
	case "mutex":
		_ = pprof.Lookup("mutex").WriteTo(w, 2) // nosec G104
	default:
		return fmt.Errorf("unknown profile type: %s", profileType)
	}
	return nil
}

type GPURecorder struct {
	mu         sync.RWMutex
	records    []GPURecord
	maxRecords int
}

type GPURecord struct {
	Timestamp    time.Time
	KernelName   string
	DurationUs   int64
	MemoryUsedMB int64
	Utilization  float32
}

func NewGPURecorder(maxRecords int) *GPURecorder {
	return &GPURecorder{
		maxRecords: maxRecords,
		records:    make([]GPURecord, 0, maxRecords),
	}
}

func (g *GPURecorder) Record(kernelName string, durationUs int64, memUsedMB int64, util float32) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.records = append(g.records, GPURecord{
		Timestamp:    time.Now(),
		KernelName:   kernelName,
		DurationUs:   durationUs,
		MemoryUsedMB: memUsedMB,
		Utilization:  util,
	})

	if len(g.records) > g.maxRecords {
		g.records = g.records[1:]
	}
}

func (g *GPURecorder) GetRecords() []GPURecord {
	g.mu.RLock()
	defer g.mu.RUnlock()

	records := make([]GPURecord, len(g.records))
	copy(records, g.records)
	return records
}

type FlameGraphGenerator struct{}

func NewFlameGraphGenerator() *FlameGraphGenerator {
	return &FlameGraphGenerator{}
}

func (f *FlameGraphGenerator) GenerateFlamegraph(profileData []byte) ([]byte, error) {
	tmpInput, err := os.CreateTemp("", "profile-input-*.prof")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpInput.Name())

	if _, err := tmpInput.Write(profileData); err != nil {
		return nil, err
	}
	_ = tmpInput.Close() // nosec G104

	tmpOutput, err := os.CreateTemp("", "profile-output-*.svg")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpOutput.Name())
	_ = tmpOutput.Close() // nosec G104

	cmd := exec.Command("go", "tool", "pprof", "-proto", tmpInput.Name())
	protoData, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	return protoData, nil
}
