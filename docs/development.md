# Development Guide

This document provides guidance for contributing to and developing Longbow.

## Getting Started

### Prerequisites

- Go 1.24.x or later
- Git
- Docker (optional)
- Make

### Setup Development Environment

1. **Clone the repository**
   ```bash
   git clone https://github.com/23skdu/longbow.git
   cd longbow
   ```

2. **Install dependencies**
   ```bash
   make deps
   ```

3. **Run tests to verify setup**
   ```bash
   make test
   ```

## Development Workflow

### Code Organization

- `cmd/` - Entry points
- `internal/` - Core implementation
- `pkg/` - Public APIs
- `docs/` - Documentation
- `scripts/` - Utility scripts

### Development Commands

Use the provided development utilities in `scripts/dev/dev.sh`:

```bash
# Start in development mode with hot reload
./scripts/dev/dev.sh start --dev

# Check status
./scripts/dev/dev.sh status

# View logs
./scripts/dev/dev.sh logs

# Run benchmarks
./scripts/dev/dev.sh bench

# Profile performance
./scripts/dev/dev.sh profile
```

### Hot Reload

The development server supports hot reload for faster iteration:

- Automatic file watching
- Graceful restart on file changes
- Configuration reload via signals

### Testing

#### Running Tests

```bash
# Run all tests
make test

# Run tests with race detection
make race

# Run tests with coverage
make test-coverage

# Run specific test
go test -v ./internal/store -run TestVectorStore
```

#### Test Structure

- Unit tests: Test individual components
- Integration tests: Test component interaction
- Performance tests: Benchmark critical paths
- Fuzz tests: Test with random inputs

### Building

```bash
# Build for development
make build

# Build for production
make prod

# Build Docker image
make docker

# Push Docker image
make docker-push
```

### Debugging

#### Profiling

Longbow exposes pprof endpoints for debugging:

- CPU Profile: http://localhost:9090/debug/pprof/profile
- Heap Profile: http://localhost:9090/debug/pprof/heap
- Goroutine Profile: http://localhost:9090/debug/pprof/goroutine
- Block Profile: http://localhost:9090/debug/pprof/block

#### Debug Logging

Enable debug logging:

```bash
export LONGBOW_LOG_LEVEL=debug
./scripts/dev/dev.sh start --debug
```

#### Common Issues

1. **Build failures**: Ensure Go version compatibility
2. **Test timeouts**: Check for race conditions
3. **Memory issues**: Monitor via pprof heap profile
4. **Port conflicts**: Use different ports with `--port` flag

### Code Standards

#### Formatting

Use the provided formatting tools:

```bash
# Format code
make fmt

# Run linter
make lint
```

#### Documentation

Update godoc comments for all exported types and functions.

#### Performance Considerations

- Use batched operations where possible
- Implement lock-free data structures
- Consider memory allocation patterns
- Profile before optimizing

### Configuration

Development uses these environment variables:

- `LONGBOW_LOG_LEVEL` - Debug logging (debug/info/warn/error)
- `LONGBOW_HOT_RELOAD` - Enable hot reload (true/false)
- `LONGBOW_DEV_MODE` - Development mode optimizations

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Update documentation
7. Submit a pull request

### Development Tools

#### IDE Configuration

For VS Code, install these extensions:
- Go extension
- Docker extension
- Better Comments extension

#### Git Hooks

Configure pre-commit hooks:

```bash
# Install pre-commit
go install github.com/pre-commit/pre-commit@latest

# Configure .pre-commit-config.yaml
pre-commit install
```

### Performance Testing

Run comprehensive benchmarks:

```bash
# Run full benchmark suite
make benchmark

# Run I/O benchmarks
go test -bench=BenchmarkWALStandard -benchmem ./internal/storage/benchmark/

# Run memory benchmarks
go test -bench=BenchmarkMemory -benchmem ./internal/memory/
```

### Getting Help

For additional help:

```bash
# Development utilities help
./scripts/dev/dev.sh help

# Makefile targets
make help

# Check dependencies
./scripts/dev/dev.sh deps
```