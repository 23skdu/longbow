# Makefile for Longbow

.PHONY: help build build-cuda build-metal build-gpu build-rpi0 build-rpi0-2 test test-low-mem lint race clean docker docker-push install deps fmt vet

# Default target
help:
	@echo "Longbow Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  build     - Build the longbow binary"
	@echo "  build-cuda - Build with CUDA GPU support (Linux AMD64)"
	@echo "  build-metal - Build with Metal GPU support (macOS ARM64)"
	@echo "  build-gpu - Build with GPU support (auto-detect backend)"
	@echo "  build-rpi0  - Cross-compile for Raspberry Pi Zero (ARMv6, Linux)"
	@echo "  build-rpi0-2 - Cross-compile for Raspberry Pi Zero 2W (ARM64, Linux)"
	@echo "  test-low-mem - Run tests with LONGBOW_LOW_MEM=1"
	@echo "  test      - Run tests"
	@echo "  lint      - Run linter"
	@echo "  race      - Run race condition tests"
	@echo "  fmt       - Format Go code"
	@echo "  vet       - Run go vet"
	@echo "  clean     - Clean build artifacts"
	@echo "  docker    - Build Docker image"
	@echo "  deps      - Install dependencies"
	@echo "  install   - Install longbow binary"
	@echo "  benchmark - Run benchmarks"

# Build the longbow binary
build:
	@echo "Building longbow..."
	go build -v -o bin/longbow ./cmd/longbow

# Build with CUDA GPU support (Linux AMD64)
build-cuda:
	@echo "Building longbow with CUDA support..."
	@echo "Note: Requires CUDA toolkit and FAISS library"
	@if [ -z "$(CUDA_HOME)" ]; then \
		if [ -d "/usr/local/cuda" ]; then \
			export CUDA_HOME=/usr/local/cuda; \
		fi; \
	fi; \
	nvcc -O3 --ptxas-options=-v --compiler-options '-fPIC' -c internal/gpu/cuda/kernels.cu -o internal/gpu/cuda/kernels.o
	CGO_ENABLED=1 go build -tags "gpu onnx" -v -o bin/longbow-cuda ./cmd/longbow
	ln -sf longbow-cuda bin/longbow

# Build with Metal GPU support (macOS ARM64)
build-metal:
	@echo "Building longbow with Metal support..."
	@echo "Note: Requires macOS with Apple Silicon (M1/M2/M3)"
	CGO_ENABLED=1 go build -tags "gpu onnx" -v -o bin/longbow-metal ./cmd/longbow
	ln -sf longbow-metal bin/longbow


# Build with GPU support (auto-detect backend based on platform)
build-gpu:
	@echo "Building longbow with GPU support..."
	@ if [ "$(shell uname -s)" = "Darwin" ]; then \
		$(MAKE) build-metal; \
	else \
		$(MAKE) build-cuda; \
	fi

# Cross-compile for Raspberry Pi Zero (ARMv6, 32-bit, linux)
# Use LONGBOW_LOW_MEM=1 at runtime on the device to activate low-memory mode.
build-rpi0:
	@echo "Cross-compiling for Raspberry Pi Zero (ARMv6, Linux)..."
	GOOS=linux GOARCH=arm GOARM=6 CGO_ENABLED=0 go build -v -o bin/longbow-rpi0 ./cmd/longbow
	@echo "Binary: bin/longbow-rpi0"
	@echo "Tip: Set LONGBOW_LOW_MEM=1 on the device to enable low-memory mode."

# Cross-compile for Raspberry Pi Zero 2W (ARM64, 64-bit, linux)
build-rpi0-2:
	@echo "Cross-compiling for Raspberry Pi Zero 2W (ARM64, Linux)..."
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -v -o bin/longbow-rpi0-2 ./cmd/longbow
	@echo "Binary: bin/longbow-rpi0-2"
	@echo "Tip: Set LONGBOW_LOW_MEM=1 on the device to enable low-memory mode."

# Run tests
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.txt ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

# Run tests with low-memory mode enabled (for Raspberry Pi Zero validation)
test-low-mem:
	@echo "Running tests with LONGBOW_LOW_MEM=1..."
	LONGBOW_LOW_MEM=1 go test -v -run TestLowMemConfig ./internal/store/...

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run
	@$(MAKE) lint-metrics

lint-metrics:
	@echo "Verifying Prometheus metrics documentation..."
	python3 scripts/verify_metrics.py

# Run race condition tests
race:
	@echo "Running race condition tests..."
	go test -race -run=Race ./...

# Format Go code
fmt:
	@echo "Formatting Go code..."
	go fmt ./...

# Run go vet
vet:
	@echo "Running go vet..."
	go vet ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	rm -f coverage.txt
	rm -f coverage.html

# Build Docker image
docker:
	@echo "Building standard Docker image..."
	docker build -t longbow:latest .

# Build NVIDIA Docker image
docker-nvidia:
	@echo "Building NVIDIA Docker image..."
	docker build -f Dockerfile.nvidia -t longbow:nvidia .

# Build Metal Docker image (requires binary built on host)
docker-metal: build-metal
	@echo "Building Metal Docker image..."
	docker build -f Dockerfile.metal -t longbow:metal .

# Push Docker image
docker-push:
	@echo "Pushing Docker images..."
	docker push longbow:latest
	docker push longbow:nvidia
	docker push longbow:metal

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod verify

# Install binary
install: build
	@echo "Installing longbow..."
	sudo cp bin/longbow /usr/local/bin/

# Run benchmarks
benchmark:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Development mode with hot reload
dev:
	@echo "Starting development mode with hot reload..."
	go run ./cmd/longbow

# Production build
prod: clean test lint build
	@echo "Production build complete"

# Continuous integration target
ci: deps fmt vet lint test
	@echo "CI pipeline complete"