#!/bin/bash
# Install MLX for Apple Silicon GPU support

set -e

echo "Installing MLX for Apple Silicon..."

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "Error: This script is for macOS only"
    exit 1
fi

# Check if running on Apple Silicon
if [[ $(uname -m) != "arm64" ]]; then
    echo "Error: This script requires Apple Silicon (arm64)"
    exit 1
fi

# Install MLX Python package
echo "Installing MLX Python package..."
pip3 install mlx

# Verify installation
echo "Verifying MLX installation..."
python3 -c "import mlx.core as mx; print(f'MLX version: {mx.__version__}')"

echo "MLX installation complete!"
echo ""
echo "To build Longbow with Metal GPU support:"
echo "  go build -tags=gpu -o longbow cmd/longbow/main.go"
echo ""
echo "To run with GPU enabled:"
echo "  GPU_ENABLED=true ./longbow"
