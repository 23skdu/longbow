# Stage 1: Build
# Use Debian Bookworm-based Go image to match runtime glibc version
FROM golang:1.24-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with CGO enabled for DuckDB
RUN CGO_ENABLED=1 GOOS=linux go build -o longbow ./cmd/longbow

# Stage 2: Runtime
# Use debian-slim to provide the necessary glibc runtime libraries
FROM debian:bookworm-slim

WORKDIR /app

# Install ca-certificates for HTTPS connectivity and libm for math ops
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/longbow /usr/local/bin/longbow

EXPOSE 3000 3001 9090

ENTRYPOINT ["longbow"]
