# Stage 1: Build
FROM golang:1.24-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build without CGO for scratch compatibility
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w" \
    -o longbow ./cmd/longbow

# Stage 2: Minimal runtime (scratch)
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/longbow /longbow

# Default data directory
VOLUME /data

# Default environment
ENV LONGBOW_GPU_ENABLED=false
ENV GOGC=75
EXPOSE 3000 3001 9090
ENTRYPOINT ["/longbow"]
