#include <metal_stdlib>
using namespace metal;

kernel void test_kernel(
    constant uint64_t* page_addrs [[buffer(1)]]
) {
    device const float* val = (device const float*)(size_t)(page_addrs[0]);
}
