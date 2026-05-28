#include <metal_stdlib>
using namespace metal;

kernel void test_kernel(
    device const float* device* pages [[buffer(1)]]
) {
    device const float* val = pages[0];
}
