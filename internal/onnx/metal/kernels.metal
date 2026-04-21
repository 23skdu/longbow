#include <metal_stdlib>
using namespace metal;

// Matrix-Vector Multiplication: Y = A * X
kernel void matmul(
    device const float* A [[buffer(0)]],
    device const float* X [[buffer(1)]],
    device float* Y [[buffer(2)]],
    constant uint& M [[buffer(3)]],
    constant uint& N [[buffer(4)]],
    uint id [[thread_position_in_grid]])
{
    if (id >= M) return;
    
    float sum = 0.0f;
    for (uint i = 0; i < N; i++) {
        sum += A[id * N + i] * X[i];
    }
    Y[id] = sum;
}

// Vector Addition: Z = X + Y
kernel void vec_add(
    device const float* X [[buffer(0)]],
    device const float* Y [[buffer(1)]],
    device float* Z [[buffer(2)]],
    constant uint& N [[buffer(3)]],
    uint id [[thread_position_in_grid]])
{
    if (id >= N) return;
    Z[id] = X[id] + Y[id];
}

// ReLU Activation: Y = max(0, X)
kernel void relu_activation(
    device const float* X [[buffer(0)]],
    device float* Y [[buffer(1)]],
    constant uint& N [[buffer(2)]],
    uint id [[thread_position_in_grid]])
{
    if (id >= N) return;
    Y[id] = max(0.0f, X[id]);
}

// Softmax (simplified per-row)
kernel void softmax_activation(
    device const float* X [[buffer(0)]],
    device float* Y [[buffer(1)]],
    constant uint& N [[buffer(2)]],
    uint id [[thread_position_in_grid]])
{
    if (id != 0) return; // Only first thread handles for now (simplified)
    
    float max_val = -INFINITY;
    for (uint i = 0; i < N; i++) {
        max_val = max(max_val, X[i]);
    }
    
    float sum = 0.0f;
    for (uint i = 0; i < N; i++) {
        Y[i] = exp(X[i] - max_val);
        sum += Y[i];
    }
    
    for (uint i = 0; i < N; i++) {
        Y[i] /= sum;
    }
}
