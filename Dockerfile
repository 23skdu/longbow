# Stage 1: Build
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install CA certificates for copying to scratch
RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build static binary (no CGO required after DuckDB removal)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o longbow ./cmd/longbow

# Stage 2: Minimal runtime
# scratch = zero OS overhead, ~50MB total image
FROM scratch

WORKDIR /app

# Copy CA certificates for TLS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary
COPY --from=builder /app/longbow /usr/local/bin/longbow

# Default data directory
VOLUME /data

EXPOSE 3000 3001 9090

ENTRYPOINT ["/usr/local/bin/longbow"]
