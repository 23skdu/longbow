#!/bin/bash

set -e

# CI Pipeline script for Longbow
# This script provides automated testing, linting, and security scanning

echo "Starting CI pipeline..."

# Install dependencies
echo "Installing dependencies..."
go mod download
go mod verify

# Run linter
echo "Running linter..."
golangci-lint run

# Run tests with race detection
echo "Running tests with race detection..."
go test -v -race ./...

# Run security scan
echo "Running security scan..."
if command -v gosec >/dev/null 2>&1; then
	echo "Running gosec security scan..."
	gosec ./...
else
	echo "gosec not found, skipping security scan"
fi

# Check for vulnerable dependencies
echo "Checking for vulnerable dependencies..."
if command -v govulncheck >/dev/null 2>&1; then
	echo "Running vulnerability check..."
	govulncheck ./...
else
	echo "govulncheck not found, skipping vulnerability check"
fi

# Build
echo "Building..."
go build -v ./...

echo "CI pipeline complete!"