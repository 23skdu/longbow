# Stage 1: Build
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install git and build tools
RUN apk add --no-cache git

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o longbow ./cmd/longbow

# Stage 2: Runtime
FROM scratch

COPY --from=builder /app/longbow /longbow

# Expose port
EXPOSE 3000

ENTRYPOINT ["/longbow"]
