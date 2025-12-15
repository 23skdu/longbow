# Stage 1: Build
FROM golang:1.25.5-alpine AS builder

WORKDIR /app

# Install git and build tools (gcc/g++ required for CGO)
# build-base is needed for compiling CGO dependencies like DuckDB
RUN apk add --no-cache git build-base build-base

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Build static binary with CGO enabled
# -tags musl: optimizes for Alpine
# -ldflags "-extldflags '-static'": ensures static linking for scratch image compatibility
RUN CGO_ENABLED=1 GOOS=linux go build -a -tags musl -ldflags "-extldflags '-static'" -o longbow ./cmd/longbow

# Stage 2: Runtime
FROM scratch

COPY --from=builder /app/longbow /longbow

# Expose port
EXPOSE 3000

ENTRYPOINT ["/longbow"]
