package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/23skdu/longbow/internal/store"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

// Config holds the application configuration
type Config struct {
	ListenAddr       string        `envconfig:"LISTEN_ADDR" default:"0.0.0.0:3000"`
	MetricsAddr      string        `envconfig:"METRICS_ADDR" default:"0.0.0.0:9090"`
	MaxMemory        int64         `envconfig:"MAX_MEMORY" default:"1073741824"` // 1GB default
	DataPath         string        `envconfig:"DATA_PATH" default:"./data"`
	SnapshotInterval time.Duration `envconfig:"SNAPSHOT_INTERVAL" default:"1h"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		logger.Info("No .env file found, using environment variables")
	}

	var cfg Config
	if err := envconfig.Process("LONGBOW", &cfg); err != nil {
		logger.Error("Failed to process config", "error", err)
		os.Exit(1)
	}

	logger.Info("Starting Longbow",
		"listen_addr", cfg.ListenAddr,
		"metrics_addr", cfg.MetricsAddr,
		"max_memory", cfg.MaxMemory,
		"data_path", cfg.DataPath,
		"snapshot_interval", cfg.SnapshotInterval)

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Initialize vector store
	// Signature: NewVectorStore(mem memory.Allocator, logger *slog.Logger, maxMemory int64)
	vectorStore := store.NewVectorStore(mem, logger, cfg.MaxMemory)

	// Start metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
			logger.Error("Metrics server failed", "error", err)
		}
	}()

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register Flight service
	flight.RegisterFlightServiceServer(grpcServer, vectorStore)

	// Start listener
	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("Failed to listen", "error", err)
		os.Exit(1)
	}

	// Handle signals for graceful shutdown and hot reload
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		logger.Info("Listening for gRPC connections", "addr", cfg.ListenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server failed", "error", err)
		}
	}()

	// Event loop
	for {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP:
			logger.Info("Received SIGHUP, reloading configuration")
			// Reload .env file
			if err := godotenv.Overload(); err != nil {
				logger.Error("Failed to reload .env file", "error", err)
			}

			var newCfg Config
			if err := envconfig.Process("LONGBOW", &newCfg); err != nil {
				logger.Error("Failed to process new config", "error", err)
				continue
			}

			// Update dynamic parameters
			vectorStore.UpdateConfig(newCfg.MaxMemory, newCfg.SnapshotInterval)

			logger.Info("Configuration reloaded",
				"max_memory", newCfg.MaxMemory,
				"snapshot_interval", newCfg.SnapshotInterval)

		case os.Interrupt, syscall.SIGTERM:
			logger.Info("Shutting down...")
			grpcServer.GracefulStop()
			return
		}
	}
}
