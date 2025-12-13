# Components

## 1. Flight Server

The core component that listens for Arrow Flight requests. It handles:

* `DoGet`: Streaming vector data to clients.
* `DoPut`: Receiving vector data from clients.
* `ListFlights`: Listing available vector streams.
* `GetFlightInfo`: Returning metadata about streams.

## 2. In-Memory Store

A thread-safe map storage engine that holds Arrow Records in memory.
It uses `sync.RWMutex` for concurrent access control.

## 3. Metrics Server

A separate HTTP server running on port 9090 that exposes Prometheus metrics.
