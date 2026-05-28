#include <metal_stdlib>
using namespace metal;

struct PageArgBuffer {
    device const float* pages[1024];
};

kernel void test_kernel(
    constant PageArgBuffer& pageArgs [[buffer(1)]]
) {
    device const float* val = pageArgs.pages[0];
}
