package main

import (
"flag"
"log"
"net"

"github.com/apache/arrow/go/v18/arrow/flight"
"github.com/apache/arrow/go/v18/arrow/memory"
"github.com/23skdu/longbow/internal/store"
"google.golang.org/grpc"
)

func main() {
listenAddr := flag.String("listen", "0.0.0.0:3000", "Address to listen on")
flag.Parse()

mem := memory.NewGoAllocator()
vectorStore := store.NewVectorStore(mem)

// Create Flight Server
srv := flight.NewFlightServer(vectorStore)
srv.Init("0.0.0.0:3000")

lis, err := net.Listen("tcp", *listenAddr)
if err != nil {
log.Fatalf("Failed to listen: %v", err)
}

log.Printf("Longbow Arrow Flight server listening on %s", *listenAddr)

// Standard gRPC server (HTTP/2)
grpcServer := grpc.NewServer()
srv.RegisterFlightService(grpcServer)

if err := grpcServer.Serve(lis); err != nil {
log.Fatalf("Failed to serve: %v", err)
}
}
