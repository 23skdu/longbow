# Stage 1: Build
FROM golang:1.24-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with io_uring support for Linux
# Note: io_uring requires Linux kernel 5.1+
# The bookworm image includes kernel headers that support io_uring
RUN CGO_ENABLED=0 go build \
    -tags=linux,iouring \
    -ldflags="-s -w" \
    -o longbow ./cmd/longbow

# Stage 2: Minimal runtime
# Using debian:bookworm-slim instead of scratch for io_uring syscall support
FROM debian:bookworm-slim

# Install ca-certificates for TLS
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/longbow /longbow

# Default data directory
VOLUME /data

# Environment variables
ENV LONGBOW_GPU_ENABLED=false
ENV LONGBOW_STORAGE_USE_IOURING=true
ENV GOGC=75

# Expose ports
# 3000: Data Server (gRPC/Arrow Flight)
# 3001: Meta Server (gRPC)
# 9090: Metrics Server (Prometheus)
EXPOSE 3000 3001 9090

ENTRYPOINT ["/longbow"]
