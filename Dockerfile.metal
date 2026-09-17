# Dockerfile.metal
# Apple Metal GPU build for macOS/Apple Silicon
#
# IMPORTANT: Metal can only be built on macOS with Apple Silicon
# The resulting binary will NOT run on Linux
#
# Build on macOS:
#   CGO_ENABLED=1 go build -tags gpu -o longbow-metal ./cmd/longbow
#   docker build -f Dockerfile.metal -t longbow:metal .

FROM debian:bookworm-slim

RUN groupadd -r longbow && useradd -r -g longbow -d /app -s /sbin/nologin longbow

WORKDIR /app

COPY bin/longbow-metal /usr/local/bin/longbow
COPY bin/bench-tool /usr/local/bin/bench-tool

RUN chown -R longbow:longbow /app /usr/local/bin/longbow /usr/local/bin/bench-tool

ENV LONGBOW_GPU_ENABLED=true
ENV LONGBOW_STORAGE_USE_IOURING=false
ENV GOGC=75

EXPOSE 3000 3001 9090

USER longbow:longbow
ENTRYPOINT ["/usr/local/bin/longbow"]
