#include "metal_info_darwin.h"
#import <Metal/Metal.h>
#import <Foundation/Foundation.h>
#include <string.h>
#include <stdlib.h>

long get_metal_memory() {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return 0;
        if (@available(macOS 10.12, *)) {
            return (long)device.recommendedMaxWorkingSetSize;
        }
        return 0;
    }
}

char* get_metal_device_name() {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return NULL;
        return strdup([device.name UTF8String]);
    }
}
