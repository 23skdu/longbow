package main

import (
"flag"
"log/slog"
"net"
"net/http"
"os"
"os/signal"
"syscall"

"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/memory"
"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/23skdu/longbow/internal/store"
"google.golang.org/grpc"
)

func main() {
listenAddr := flag.String("listen", "0.0.0.0:3000", "Address to listen on for Flight service")
metricsAddr := flag.String("metrics", "0.0.0.0:9090", "Address to listen on for Prometheus metrics")
maxMemory := flag.Int64("max_memory", 1024*1024*1024, "Maximum memory usage in bytes (default 1GB)")
flag.Parse()

// Setup JSON Logger
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
slog.SetDefault(logger)

// Start Metrics Server
go func() {
logger.Info("Starting metrics server", "address", *metricsAddr)
http.Handle("/metrics", promhttp.Handler())
if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
logger.Error("Failed to start metrics server", "error", err)
}
}()

mem := memory.NewGoAllocator()
vectorStore := store.NewVectorStore(mem, logger, *maxMemory)

lis, err := net.Listen("tcp", *listenAddr)
if err != nil {
logger.Error("Failed to listen", "error", err, "address", *listenAddr)
os.Exit(1)
}

// Standard gRPC server (HTTP/2)
grpcServer := grpc.NewServer()

// Register the VectorStore directly as the Flight Service
flight.RegisterFlightServiceServer(grpcServer, vectorStore)

// Graceful Shutdown Handling
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

go func() {
logger.Info("Longbow Arrow Flight server starting", "address", *listenAddr)
if err := grpcServer.Serve(lis); err != nil {
logger.Error("Failed to serve", "error", err)
os.Exit(1)
}
}()

// Wait for termination signal
sig := <-sigChan
logger.Info("Received termination signal, shutting down gracefully", "signal", sig)
grpcServer.GracefulStop()
logger.Info("Server stopped")
}
