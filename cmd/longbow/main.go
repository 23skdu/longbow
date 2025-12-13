package main

import (
"flag"
"log/slog"
"net"
"net/http"
"os"

"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/memory"
"github.com/prometheus/client_golang/prometheus/promhttp"
"github.com/23skdu/longbow/internal/store"
"google.golang.org/grpc"
)

func main() {
listenAddr := flag.String("listen", "0.0.0.0:3000", "Address to listen on for Flight service")
metricsAddr := flag.String("metrics", "0.0.0.0:9090", "Address to listen on for Prometheus metrics")
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
vectorStore := store.NewVectorStore(mem, logger)

lis, err := net.Listen("tcp", *listenAddr)
if err != nil {
logger.Error("Failed to listen", "error", err, "address", *listenAddr)
os.Exit(1)
}

logger.Info("Longbow Arrow Flight server starting", "address", *listenAddr)

// Standard gRPC server (HTTP/2)
grpcServer := grpc.NewServer()

// Register the VectorStore directly as the Flight Service
flight.RegisterFlightServiceServer(grpcServer, vectorStore)

if err := grpcServer.Serve(lis); err != nil {
logger.Error("Failed to serve", "error", err)
os.Exit(1)
}
}
