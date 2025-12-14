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
MetaAddr         string        `envconfig:"META_ADDR" default:"0.0.0.0:3001"` // New config for Metadata Server
MetricsAddr      string        `envconfig:"METRICS_ADDR" default:"0.0.0.0:9090"`
MaxMemory        int64         `envconfig:"MAX_MEMORY" default:"1073741824"` // 1GB default
DataPath         string        `envconfig:"DATA_PATH" default:"./data"`
TTL time.Duration `envconfig:"TTL" default:"0s"` // 0s means disabled
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
"meta_addr", cfg.MetaAddr,
"metrics_addr", cfg.MetricsAddr,
"max_memory", cfg.MaxMemory,
"data_path", cfg.DataPath,
"snapshot_interval", cfg.SnapshotInterval,
		"ttl", cfg.TTL)

// Create memory allocator
mem := memory.NewGoAllocator()

// Initialize vector store
vectorStore := store.NewVectorStore(mem, logger, cfg.MaxMemory, cfg.TTL)


// Start eviction ticker if TTL is enabled
if cfg.TTL > 0 {
// Check for evictions every minute or 1/10th of TTL, whichever is smaller but at least 1s
checkInterval := time.Minute
if cfg.TTL/10 < checkInterval {
checkInterval = cfg.TTL / 10
}
if checkInterval < time.Second {
checkInterval = time.Second
}
vectorStore.StartEvictionTicker(checkInterval)
logger.Info("Eviction ticker started", "interval", checkInterval)
}

	// Start metrics server
go func() {
http.Handle("/metrics", promhttp.Handler())
if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
logger.Error("Metrics server failed", "error", err)
}
}()

// --- Data Server Setup ---
dataServer := grpc.NewServer()
flight.RegisterFlightServiceServer(dataServer, store.NewDataServer(vectorStore))

dataLis, err := net.Listen("tcp", cfg.ListenAddr)
if err != nil {
logger.Error("Failed to listen for Data Server", "error", err, "addr", cfg.ListenAddr)
os.Exit(1)
}

// --- Meta Server Setup ---
metaServer := grpc.NewServer()
flight.RegisterFlightServiceServer(metaServer, store.NewMetaServer(vectorStore))

metaLis, err := net.Listen("tcp", cfg.MetaAddr)
if err != nil {
logger.Error("Failed to listen for Meta Server", "error", err, "addr", cfg.MetaAddr)
os.Exit(1)
}

// Handle signals for graceful shutdown and hot reload
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

// Start Data Server
go func() {
logger.Info("Listening for Data gRPC connections", "addr", cfg.ListenAddr)
if err := dataServer.Serve(dataLis); err != nil {
logger.Error("Data gRPC server failed", "error", err)
}
}()

// Start Meta Server
go func() {
logger.Info("Listening for Meta gRPC connections", "addr", cfg.MetaAddr)
if err := metaServer.Serve(metaLis); err != nil {
logger.Error("Meta gRPC server failed", "error", err)
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
// Stop both servers
dataServer.GracefulStop()
metaServer.GracefulStop()

// Close VectorStore
if err := vectorStore.Close(); err != nil {
logger.Error("Failed to close VectorStore", "error", err)
}
return
}
}
}
