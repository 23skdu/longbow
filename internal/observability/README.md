# Longbow Observability Configuration

This directory contains Grafana dashboard definitions and monitoring configurations for the Longbow vector database.

## Overview

The observability stack provides comprehensive monitoring for:
- Application metrics and performance
- Distributed tracing
- Health checks and readiness probes
- System resources and runtime metrics
- Component-specific monitoring

## Grafana Dashboards

### Application Dashboard (`application.json`)
**UID:** `longbow-application`

Monitors overall application health and performance:
- HTTP request rates and error rates
- Request duration percentiles (P50, P95, P99)
- Component health status (Database, Storage, Metrics)
- Memory usage and Go runtime metrics
- Goroutine counts and GC statistics

### Observability Dashboard (`observability.json`)
**UID:** `longbow-observability`

Focuses on the observability stack itself:
- Distributed tracing span creation rates
- Span duration percentiles
- Trace sampling configuration
- Health check response times
- Component health status overview

## Metrics Available

### HTTP Metrics
- `http_requests_total` - Total HTTP requests by method, status code, and path
- `http_request_duration_seconds` - HTTP request duration histogram

### Health Check Metrics
- `health_check_duration_seconds` - Health check execution time by component
- `health_check_status` - Health check result status by component
- `component_health_status` - Current component health status

### Distributed Tracing Metrics
- `trace_spans_created_total` - Total number of trace spans created
- `trace_span_duration_seconds` - Trace span duration histogram
- `trace_sampling_rate` - Current trace sampling rate
- `trace_correlation_context_total` - Correlation context usage

### Go Runtime Metrics
- `go_memstats_heap_alloc_bytes` - Heap memory allocation
- `go_memstats_heap_inuse_bytes` - Heap memory in use
- `go_memstats_stack_inuse_bytes` - Stack memory in use
- `go_goroutines` - Number of goroutines
- `go_gc_duration_seconds_count` - GC count

## Importing Dashboards

1. Navigate to Grafana UI
2. Go to **Dashboards** → **Import**
3. Upload the JSON files from this directory
4. Configure the Prometheus data source (`${DS_PROMETHEUS}`)

## Health Endpoints

The application exposes several health endpoints:

- `/health` - Comprehensive health check of all components
- `/health/live` - Simple liveness check
- `/health/ready` - Readiness check for critical components

Each endpoint returns JSON with component status, response times, and system information.

## Alerting

Recommended alerting rules:

### Critical Alerts
- Component health status unhealthy (status = 0)
- High error rate (>5% of total requests)
- Memory usage exceeding 80% of available memory
- Health check response time >5 seconds

### Warning Alerts
- Component health status degraded (status = 0.5)
- High request latency (P95 > 2 seconds)
- Goroutine count > 1000
- GC frequency > 10/minute

## Configuration

### Prometheus Configuration
```yaml
scrape_configs:
  - job_name: 'longbow'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

### Health Check Configuration
Health checks can be customized by implementing the `HealthChecker` interface:
```go
type HealthChecker interface {
    Name() string
    Check(ctx context.Context) *ComponentHealth
}
```

## Monitoring Best Practices

1. **Dashboard Organization**: Use tags to group related dashboards
2. **Alert Thresholds**: Set appropriate thresholds based on baseline metrics
3. **Retention**: Configure appropriate data retention periods
4. **Resource Limits**: Monitor Grafana and Prometheus resource usage
5. **Documentation**: Keep dashboard descriptions updated with context

## Troubleshooting

### Common Issues
- **Missing Metrics**: Verify Prometheus scrape configuration
- **Health Checks Failing**: Check component logs and dependencies
- **High Memory Usage**: Investigate goroutine leaks or memory bloat
- **Slow Queries**: Review distributed tracing data

### Debug Commands
```bash
# Check component health
curl http://localhost:8080/health

# View metrics
curl http://localhost:8080/metrics

# Test liveness
curl http://localhost:8080/health/live
```

## Future Enhancements

- Custom alerting rules
- SLA/SLO dashboards
- Capacity planning views
- Anomaly detection
- Automated incident response
- Multi-cluster monitoring