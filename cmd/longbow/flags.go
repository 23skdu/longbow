package main

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/23skdu/longbow/pkg/version"
)

var (
	ErrVersionRequested = errors.New("version requested")
	ErrHelpRequested    = errors.New("help requested")
)

// parseCLIFlags defines and parses CLI flags, overlaying matching Config fields.
// CLI flags take precedence over environment variables.
// It returns ErrVersionRequested for --version/-v, ErrHelpRequested for --help,
// or a parse error. The caller should handle these sentinel errors.
func parseCLIFlags(cfg *Config, args []string, output io.Writer) error {
	for _, arg := range args {
		if arg == "--version" || arg == "-v" {
			fmt.Fprintln(output, version.Info())
			return ErrVersionRequested
		}
	}

	fs := flag.NewFlagSet("longbow", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.Usage = func() {
		w := fs.Output()
		fmt.Fprintln(w, "Longbow – vector search server")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Environment variables with LONGBOW_ prefix (e.g. LONGBOW_MAX_MEMORY) are")
		fmt.Fprintln(w, "also supported. CLI flags take precedence over environment variables.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Usage:")
		fmt.Fprintln(w, "  longbow [flags]")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		fs.PrintDefaults()
	}

	fs.StringVar(&cfg.ListenAddr, "listen-addr", cfg.ListenAddr, "gRPC data listen address")
	fs.StringVar(&cfg.MetaAddr, "meta-addr", cfg.MetaAddr, "gRPC meta listen address")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "Prometheus metrics listen address")
	fs.Int64Var(&cfg.MaxMemory, "max-memory", cfg.MaxMemory, "Maximum memory in bytes (e.g. 17179869184 for 16GB)")
	fs.StringVar(&cfg.DataPath, "data-path", cfg.DataPath, "Data directory path")
	fs.StringVar(&cfg.NodeID, "node-id", cfg.NodeID, "Unique node identifier (default: hostname)")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "Log format: json or console")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, error")
	fs.BoolVar(&cfg.GPUEnabled, "gpu-enabled", cfg.GPUEnabled, "Enable GPU acceleration")
	fs.IntVar(&cfg.GPUDeviceID, "gpu-device-id", cfg.GPUDeviceID, "GPU device ID")
	fs.BoolVar(&cfg.GossipEnabled, "gossip-enabled", cfg.GossipEnabled, "Enable gossip protocol for clustering")
	fs.BoolVar(&cfg.AutoSpillToDisk, "auto-spill-disk", cfg.AutoSpillToDisk, "Automatically fallback to disk backing when memory exceeds 70% of physical RAM")
	fs.Float64Var(&cfg.SpillThresholdRatio, "spill-threshold-ratio", cfg.SpillThresholdRatio, "Physical memory ratio threshold to trigger automatic spill-to-disk")
	fs.BoolVar(&cfg.AutoQuantize, "auto-quantize", cfg.AutoQuantize, "Standardize TurboQuant as default storage mode for high-scale configurations")
	fs.Int64Var(&cfg.AutoQuantizeThreshold, "auto-quantize-threshold", cfg.AutoQuantizeThreshold, "Vector count threshold to trigger automatic TurboQuant standardization")
	fs.IntVar(&cfg.AutoQuantizeBits, "auto-quantize-bits", cfg.AutoQuantizeBits, "Target bit depth for automatic TurboQuant standardization (2, 4, or 8)")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return ErrHelpRequested
		}
		return err
	}

	return nil
}
