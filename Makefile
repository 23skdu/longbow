# Makefile for Longbow

.PHONY: help build test lint race clean docker docker-push install deps fmt vet

# Default target
help:
	@echo "Longbow Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  build     - Build the longbow binary"
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

# Run tests
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.txt ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -race -coverprofile=coverage.txt -covermode=atomic ./...

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run

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
	@echo "Building Docker image..."
	docker build -t longbow:latest .

# Push Docker image
docker-push:
	@echo "Pushing Docker image..."
	docker push longbow:latest

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