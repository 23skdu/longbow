# Dockerfile.metal
# Apple Metal GPU build for macOS/Apple Silicon
#
# IMPORTANT: Metal can only be built on macOS with Apple Silicon
# The resulting binary will NOT run on Linux
#
# Build on macOS:
#   docker build -f Dockerfile.metal -t longbow:metal . 
#
# Or build directly with Go:
#   CGO_ENABLED=1 go build -tags gpu -o longbow ./cmd/longbow

FROM debian:bookworm-slim

WORKDIR /app

# Metal binary would be copied here if built on macOS
COPY longbow-metal /usr/local/bin/longbow

ENV LONGBOW_GPU_ENABLED=true
ENV LONGBOW_STORAGE_USE_IOURING=false
ENV GOGC=75

EXPOSE 3000 3001 9090

# Build Instructions for Metal:
# ============================
# 
# On macOS with Apple Silicon (M1, M2, M3, M4):
#
# 1. Ensure Xcode Command Line Tools:
#    xcode-select --install
#
# 2. Build with Metal GPU support:
#    CGO_ENABLED=1 go build -tags gpu -o longbow ./cmd/longbow
#
# 3. Run with GPU enabled:
#    LONGBOW_GPU_ENABLED=true ./longbow
#
# Note: Metal framework is only available on macOS with Apple Silicon.
# Intel Macs will fall back to CPU-only mode.
