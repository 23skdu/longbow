# Rate Limiting

Longbow includes a built-in Token Bucket rate limiter to protect the service from overload. This is implemented as a gRPC interceptor that monitors both Unary and Streaming requests.

## Configuration

Rate limiting is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LONGBOW_RATE_LIMIT_RPS` | `0` (Disabled) | Requests Per Second limit. |
| `LONGBOW_RATE_LIMIT_BURST` | `0` (Same as RPS) | Maximum burst size allowed. |

### Example

To limit the server to 1000 requests per second with a burst of 2000:

```bash
export LONGBOW_RATE_LIMIT_RPS=1000
export LONGBOW_RATE_LIMIT_BURST=2000
```

## Behavior

* **Allowed**: If tokens are available, the request proceeds.
* **Throttled**: If the bucket is empty, the request is rejected immediately with gRPC status code `ResourceExhausted` (8).

## Metrics

Limits are tracked via Prometheus metrics:

* `longbow_rate_limit_requests_total{status="allowed"}`
* `longbow_rate_limit_requests_total{status="throttled"}`
