package main

import (
"log/slog"
"net"
"net/http"
"os"
"os/signal"
"syscall"
"time"

"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/memory"
"github.com/joho/godotenv"
"github.com/kelseyhightower/envconfig"
"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/23skdu/longbow/internal/store"
"google.golang.org/grpc"
)

// Config holds the application configuration
type Config struct {
ListenAddr       string        envconfig:"LISTEN_ADDR" default:"0.0.0.0:3000"
MetricsAddr      string        envconfig:"METRICS_ADDR" default:"0.0.0.0:9090"
MaxMemory        int64         envconfig:"MAX_MEMORY" default:"1073741824" // 1GB default
DataPath         string        envconfig:"DATA_PATH" default:"./data"
SnapshotInterval time.Duration envconfig:"SNAPSHOT_INTERVAL" default:"1h"
}

func main() {
// Setup JSON Logger
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
slog.SetDefault(logger)

// Load .env file if it exists
if err := godotenv.Load(); err != nil {
logger.Info("No .env file found, relying on environment variables")
}

var cfg Config
if err := envconfig.Process("LONGBOW", &cfg); err != nil {
logger.Error("Failed to process configuration", "error", err)
os.Exit(1)
}

logger.Info("Configuration loaded",
"listen_addr", cfg.ListenAddr,
"metrics_addr", cfg.MetricsAddr,
"max_memory", cfg.MaxMemory,
"data_path", cfg.DataPath,
"snapshot_interval", cfg.SnapshotInterval,
)

// Start Metrics Server
go func() {
logger.Info("Starting metrics server", "address", cfg.MetricsAddr)
http.Handle("/metrics", promhttp.Handler())
if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
logger.Error("Failed to start metrics server", "error", err)
}
}()

mem := memory.NewGoAllocator()
vectorStore := store.NewVectorStore(mem, logger, cfg.MaxMemory)

// Initialize Persistence
if err := vectorStore.InitPersistence(cfg.DataPath, cfg.SnapshotInterval); err != nil {
logger.Error("Failed to initialize persistence", "error", err)
os.Exit(1)
}

lis, err := net.Listen("tcp", cfg.ListenAddr)
if err != nil {
logger.Error("Failed to listen", "error", err, "address", cfg.ListenAddr)
os.Exit(1)
}

// Standard gRPC server (HTTP/2)
grpcServer := grpc.NewServer()

// Register the VectorStore directly as the Flight Service
flight.RegisterFlightServiceServer(grpcServer, vectorStore)

// Signal Handling for Graceful Shutdown and Hot Reload
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

go func() {
logger.Info("Longbow Arrow Flight server starting", "address", cfg.ListenAddr)
if err := grpcServer.Serve(lis); err != nil {
logger.Error("Failed to serve", "error", err)
os.Exit(1)
}
}()

// Event Loop
for {
sig := <-sigChan
switch sig {
case syscall.SIGHUP:
logger.Info("Received SIGHUP, reloading configuration...")
// Reload .env
_ = godotenv.Overload()

// Reprocess config
var newCfg Config
if err := envconfig.Process("LONGBOW", &newCfg); err != nil {
logger.Error("Failed to reload configuration", "error", err)
continue
}

// Update dynamic parameters
vectorStore.UpdateConfig(newCfg.MaxMemory, newCfg.SnapshotInterval)

logger.Info("Configuration reloaded", 
"max_memory", newCfg.MaxMemory, 
"snapshot_interval", newCfg.SnapshotInterval)

case os.Interrupt, syscall.SIGTERM:
logger.Info("Received termination signal, shutting down gracefully", "signal", sig)
grpcServer.GracefulStop()
logger.Info("Server stopped")
return
}
}
}
