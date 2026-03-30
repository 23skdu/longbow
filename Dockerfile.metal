# Dockerfile.metal
# Metal GPU build for macOS/Apple Silicon
# 
# IMPORTANT: Metal can only be built on macOS with Apple Silicon
# The resulting binary will NOT run on Linux - it's macOS only
#
# Build on macOS:
#   CGO_ENABLED=1 go build -tags gpu -o longbow-metal ./cmd/longbow
#
# Or use docker buildx (limited - won't actually run on Linux):
#   docker build -f Dockerfile.metal -t ghcr.io/23skdu/longbow:metal-test .

FROM debian:bookworm-slim

WORKDIR /app

# Metal binary would be copied here if built on macOS
# COPY longbow-metal /usr/local/bin/longbow

ENV LONGBOW_GPU_ENABLED=true
ENV LONGBOW_STORAGE_USE_IOURING=false

EXPOSE 3000 3001 9090

# Build Instructions for Metal:
# ============================
# 
# On macOS with Apple Silicon:
#
# 1. Ensure you have Xcode Command Line Tools:
#    xcode-select --install
#
# 2. Build with Metal GPU support:
#    CGO_ENABLED=1 go build -tags gpu -o longbow-metal ./cmd/longbow
#
# 3. Run with GPU enabled:
#    GPU_ENABLED=true ./longbow-metal
#
# Note: The Metal framework is only available on macOS with Apple Silicon
# (M1, M2, M3, M4 chips). Intel Macs will fall back to CPU-only mode.
