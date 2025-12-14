# Components

## 1. Flight Server

The core component that listens for Arrow Flight requests. It handles:

* DoGet: Streaming vector data to clients.
* DoPut: Receiving vector data from clients.
* ListFlights: Listing available vector streams.
* GetFlightInfo: Returning metadata about streams.

## 2. In-Memory Store

A thread-safe map storage engine that holds Arrow Records in memory.
It uses sync.RWMutex for concurrent access control.

## 3. Metrics Server

A separate HTTP server running on port 9090 that exposes Prometheus metrics.

## Port Optimization

As of version 0.0.3, the Flight service is split into two ports:

* **Data Port (default 3000)**: Handles DoGet and DoPut operations.
* **Meta Port (default 3001)**: Handles ListFlights and GetFlightInfo operations.

This separation prevents heavy data transfer operations from blocking metadata lookups.
