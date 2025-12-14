# Recent Features

## gRPC Keep-Alive

To prevent load balancers from terminating long-running streams, Longbow now
supports configurable gRPC Keep-Alive settings.

### Configuration

- `GRPC_KEEPALIVE_TIME`: Time between pings (default: 2h)
- `GRPC_KEEPALIVE_TIMEOUT`: Timeout for pings (default: 20s)
- `GRPC_KEEPALIVE_MIN_TIME`: Minimum time between pings (default: 5m)
- `GRPC_KEEPALIVE_PERMIT_WITHOUT_STREAM`: Allow pings without active streams
  (default: false)

## Schema Evolution

See [Schema Evolution](schema_evolution.md) for details on how Longbow handles
changing data structures.
