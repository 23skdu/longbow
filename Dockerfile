# Stage 1: Build
# Use Debian-based Go image (glibc) to match DuckDB pre-compiled libs
FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build with CGO enabled for DuckDB
# Removed '-tags musl' and static linking flags as we are now on glibc
RUN CGO_ENABLED=1 GOOS=linux go build -o longbow ./cmd/longbow

# Stage 2: Runtime
# Use debian-slim to provide the necessary glibc runtime libraries
FROM debian:bookworm-slim

WORKDIR /app

# Install ca-certificates for HTTPS connectivity
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/longbow /usr/local/bin/longbow

EXPOSE 3000

ENTRYPOINT ["longbow"]
