# Stage 1: Build
FROM golang:1.26-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BUILD_TAGS=linux
# Build with optional io_uring support for Linux
# Note: io_uring requires Linux kernel 5.1+
# The bookworm image includes kernel headers that support io_uring
# Use --build-arg BUILD_TAGS="linux,iouring" to enable io_uring
RUN CGO_ENABLED=0 go build \
    -tags=${BUILD_TAGS:-linux} \
    -ldflags="-s -w" \
    -o longbow ./cmd/longbow

# Stage 2: Minimal runtime - using scratch for smallest possible image
# Requires static build (CGO_ENABLED=0)
FROM scratch

COPY --from=builder /app/longbow /longbow

VOLUME /data

ENV LONGBOW_GPU_ENABLED=false
ENV LONGBOW_STORAGE_USE_IOURING=false
ENV GOGC=75

EXPOSE 3000 3001 9090

ENTRYPOINT ["/longbow"]
