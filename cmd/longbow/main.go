package main

import (
"net"
"net/http"
"os"
"os/signal"
"syscall"
"time"

"github.com/23skdu/longbow/internal/logging"
"github.com/23skdu/longbow/internal/store"
"github.com/apache/arrow-go/v18/arrow/flight"
"github.com/apache/arrow-go/v18/arrow/memory"
"github.com/joho/godotenv"
"github.com/kelseyhightower/envconfig"
"github.com/prometheus/client_golang/prometheus/promhttp"
"go.uber.org/zap"
"google.golang.org/grpc"
"google.golang.org/grpc/keepalive"
)

// Config holds the application configuration
type Config struct {
// gRPC KeepAlive Configuration
KeepAliveTime              time.Duration `envconfig:"GRPC_KEEPALIVE_TIME" default:"2h"`
KeepAliveTimeout           time.Duration `envconfig:"GRPC_KEEPALIVE_TIMEOUT" default:"20s"`
KeepAliveMinTime           time.Duration `envconfig:"GRPC_KEEPALIVE_MIN_TIME" default:"5m"`
KeepAlivePermitWithoutStream bool          `envconfig:"GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM" default:"false"`

ListenAddr       string        `envconfig:"LISTEN_ADDR" default:"0.0.0.0:3000"`
MetaAddr         string        `envconfig:"META_ADDR" default:"0.0.0.0:3001"`
MetricsAddr      string        `envconfig:"METRICS_ADDR" default:"0.0.0.0:9090"`
MaxMemory        int64         `envconfig:"MAX_MEMORY" default:"1073741824"`
DataPath         string        `envconfig:"DATA_PATH" default:"./data"`
TTL              time.Duration `envconfig:"TTL" default:"0s"`
SnapshotInterval time.Duration `envconfig:"SNAPSHOT_INTERVAL" default:"1h"`
MaxWALSize       int64         `envconfig:"MAX_WAL_SIZE" default:"104857600"`

// Logging configuration
LogFormat string `envconfig:"LOG_FORMAT" default:"json"`
LogLevel  string `envconfig:"LOG_LEVEL" default:"info"`
}

func main() {
// Load .env file if it exists (do this before logger init to read LOG_* vars)
_ = godotenv.Load()

var cfg Config
if err := envconfig.Process("LONGBOW", &cfg); err != nil {
// Fallback to basic logging if config fails
panic("Failed to process config: " + err.Error())
}

// Initialize zap logger
logger, err := logging.NewLogger(logging.Config{
Format: cfg.LogFormat,
Level:  cfg.LogLevel,
})
if err != nil {
panic("Failed to initialize logger: " + err.Error())
}
defer func() { _ = logger.Sync() }()

logger.Info(">>---> Starting Longbow",
zap.String("listen_addr", cfg.ListenAddr),
zap.String("meta_addr", cfg.MetaAddr),
zap.String("metrics_addr", cfg.MetricsAddr),
zap.Int64("max_memory", cfg.MaxMemory),
zap.String("data_path", cfg.DataPath),
zap.Duration("snapshot_interval", cfg.SnapshotInterval),
zap.Int64("max_wal_size", cfg.MaxWALSize),
zap.Duration("ttl", cfg.TTL),
)

// Create memory allocator
mem := memory.NewGoAllocator()

// Initialize vector store
vectorStore := store.NewVectorStore(mem, logger, cfg.MaxMemory, cfg.MaxWALSize, cfg.TTL)

// Start eviction ticker if TTL is enabled
if cfg.TTL > 0 {
checkInterval := time.Minute
if cfg.TTL/10 < checkInterval {
checkInterval = cfg.TTL / 10
}
if checkInterval < time.Second {
checkInterval = time.Second
}
vectorStore.StartEvictionTicker(checkInterval)
vectorStore.StartWALCheckTicker(10 * time.Second)
logger.Info("Eviction ticker started", zap.Duration("interval", checkInterval))
}

// Start metrics server
go func() {
http.Handle("/metrics", promhttp.Handler())
if err := http.ListenAndServe(cfg.MetricsAddr, nil); err != nil {
logger.Error("Metrics server failed", zap.Error(err))
}
}()

// gRPC Server Options
kaParams := keepalive.ServerParameters{
Time:    cfg.KeepAliveTime,
Timeout: cfg.KeepAliveTimeout,
}
kaPolicy := keepalive.EnforcementPolicy{
MinTime:             cfg.KeepAliveMinTime,
PermitWithoutStream: cfg.KeepAlivePermitWithoutStream,
}
serverOpts := []grpc.ServerOption{
grpc.KeepaliveParams(kaParams),
grpc.KeepaliveEnforcementPolicy(kaPolicy),
}

// --- Data Server Setup ---
dataServer := grpc.NewServer(serverOpts...)
flight.RegisterFlightServiceServer(dataServer, store.NewDataServer(vectorStore))

dataLisBase, err := net.Listen("tcp", cfg.ListenAddr)
if err != nil {
logger.Error("Failed to listen for Data Server", zap.Error(err), zap.String("addr", cfg.ListenAddr))
_ = logger.Sync()
	os.Exit(1) //nolint:gocritic // sync called above
}
	dataLis := store.NewTCPNoDelayListener(dataLisBase.(*net.TCPListener))

// --- Meta Server Setup ---
metaServer := grpc.NewServer(serverOpts...)
flight.RegisterFlightServiceServer(metaServer, store.NewMetaServer(vectorStore))

metaLisBase, err := net.Listen("tcp", cfg.MetaAddr)
if err != nil {
logger.Error("Failed to listen for Meta Server", zap.Error(err), zap.String("addr", cfg.MetaAddr))
_ = logger.Sync()
	os.Exit(1) //nolint:gocritic // sync called above
}

	metaLis := store.NewTCPNoDelayListener(metaLisBase.(*net.TCPListener))
// Handle signals for graceful shutdown and hot reload
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

// Start Data Server
go func() {
logger.Info("Listening for Data gRPC connections", zap.String("addr", cfg.ListenAddr))
if err := dataServer.Serve(dataLis); err != nil {
logger.Error("Data gRPC server failed", zap.Error(err))
}
}()

// Start Meta Server
go func() {
logger.Info("Listening for Meta gRPC connections", zap.String("addr", cfg.MetaAddr))
if err := metaServer.Serve(metaLis); err != nil {
logger.Error("Meta gRPC server failed", zap.Error(err))
}
}()

// Event loop
for {
sig := <-sigChan
switch sig {
case syscall.SIGHUP:
logger.Info("Received SIGHUP, reloading configuration")
if err := godotenv.Overload(); err != nil {
logger.Error("Failed to reload .env file", zap.Error(err))
}

var newCfg Config
if err := envconfig.Process("LONGBOW", &newCfg); err != nil {
logger.Error("Failed to process new config", zap.Error(err))
continue
}

vectorStore.UpdateConfig(newCfg.MaxMemory, newCfg.MaxWALSize, newCfg.SnapshotInterval)

logger.Info("Configuration reloaded",
zap.Int64("max_memory", newCfg.MaxMemory),
zap.Duration("snapshot_interval", newCfg.SnapshotInterval),
zap.Int64("max_wal_size", newCfg.MaxWALSize),
)

case os.Interrupt, syscall.SIGTERM:
logger.Info("Shutting down...")
dataServer.GracefulStop()
metaServer.GracefulStop()

if err := vectorStore.Close(); err != nil {
logger.Error("Failed to close VectorStore", zap.Error(err))
}
return
}
}
}
