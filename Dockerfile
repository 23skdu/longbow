# Stage 1: Build
FROM golang:1.24-bookworm AS builder

WORKDIR /app

# Install build dependencies for CGO and potentially FAISS
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with CGO and GPU support
# Note: In a real environment, FAISS shared libs would need to be present
RUN CGO_ENABLED=1 go build \
    -ldflags="-s -w" \
    -o longbow ./cmd/longbow

# Stage 2: Minimal runtime
FROM debian:bookworm-slim

WORKDIR /app

# Copy CA certificates for TLS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary
COPY --from=builder /app/longbow /usr/local/bin/longbow

# Default data directory
VOLUME /data

# Default environment for safe fallback
ENV LONGBOW_GPU_ENABLED=false

EXPOSE 3000 3001 9090

ENTRYPOINT ["/usr/local/bin/longbow"]
