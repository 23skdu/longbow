#!/bin/bash
pkill -9 -f orchestrate_benchmarks.sh
pkill -9 -f bench_tool_runner.sh
pkill -9 -x longbow
pkill -9 -x longbow-avx2
pkill -9 -x longbow-cuda
pkill -9 -x longbow-cpu
pkill -9 -x bench-tool
# Clean up any lingering background jobs
jobs -p | xargs kill -9 2>/dev/null || true
