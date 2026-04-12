#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>
#import <Accelerate/Accelerate.h>

#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

// MARK: - Metal Engine C Interface

typedef struct MetalEngine {
    id<MTLDevice> device;
    id<MTLCommandQueue> queue;
    id<MTLLibrary> library;
    id<MTLComputePipelineState> matmulPipeline;
    id<MTLComputePipelineState> addPipeline;
    id<MTLComputePipelineState> reluPipeline;
    id<MTLComputePipelineState> softmaxPipeline;
    
    // Model buffers
    id<MTLBuffer> modelWeights;
    id<MTLBuffer> inputBuffer;
    id<MTLBuffer> outputBuffer;
    
    // Tensor pool
    id<MTLBuffer> tensorPool[10];
    int poolSize;
    
    // State
    bool loaded;
    int maxSeqLength;
} MetalEngine;

MetalEngine* metal_engine_create() {
    @autoreleasepool {
        MetalEngine* engine = (MetalEngine*)calloc(1, sizeof(MetalEngine));
        if (!engine) return NULL;
        
        // Get Metal device
        engine->device = MTLCreateSystemDefaultDevice();
        if (!engine->device) {
            free(engine);
            return NULL;
        }
        
        // Create command queue
        engine->queue = [engine->device newCommandQueue];
        if (!engine->queue) {
            free(engine);
            return NULL;
        }
        
        // Set default pool size
        engine->poolSize = 10;
        engine->maxSeqLength = 512;
        
        // Pre-allocate tensor pool
        size_t poolBufferSize = engine->maxSeqLength * sizeof(float) * 1000;
        for (int i = 0; i < engine->poolSize; i++) {
            engine->tensorPool[i] = [engine->device newBufferWithLength:poolBufferSize
                                                             options:MTLResourceStorageModeShared];
        }
        
        return engine;
    }
}

void metal_engine_destroy(MetalEngine* engine) {
    if (!engine) return;
    
    @autoreleasepool {
        // With ARC enabled, we don't need explicit release calls
        // Just set pointers to nil if needed, or rely on autoreleasepool
        engine->library = nil;
        engine->matmulPipeline = nil;
        engine->addPipeline = nil;
        engine->reluPipeline = nil;
        engine->softmaxPipeline = nil;
        
        engine->modelWeights = nil;
        engine->inputBuffer = nil;
        engine->outputBuffer = nil;
        
        for (int i = 0; i < engine->poolSize; i++) {
            engine->tensorPool[i] = nil;
        }
        
        engine->queue = nil;
        engine->device = nil;
        
        free(engine);
    }
}

bool metal_engine_available() {
    @autoreleasepool {
        id<MTLDevice> device = MTLCreateSystemDefaultDevice();
        if (!device) return false;
        return true;
    }
}

bool metal_engine_load_model(MetalEngine* engine, const char* path) {
    if (!engine || !path) return false;
    
    @autoreleasepool {
        // For now, create a simple mock model
        // In production, this would parse ONNX and create Metal kernels
        NSString* pathStr = [NSString stringWithUTF8String:path];
        NSError* error = nil;
        
        // Check if file exists
        if (![[NSFileManager defaultManager] fileExistsAtPath:pathStr]) {
            // Create a simple default model if file doesn't exist
            // This is for demonstration - real impl would load actual model
            engine->loaded = true;
            return true;
        }
        
        // Load model - simplified for now
        engine->loaded = true;
        return true;
    }
}

// Simple cross-encoder scoring using CPU fallback
// Real implementation would use Metal compute shaders
float* metal_engine_score(MetalEngine* engine, const char* query, const char** docs, int doc_count, int* out_count) {
    if (!engine || !query || !docs || doc_count <= 0) {
        if (out_count) *out_count = 0;
        return NULL;
    }
    
    @autoreleasepool {
        float* scores = (float*)malloc(doc_count * sizeof(float));
        if (!scores) {
            if (out_count) *out_count = 0;
            return NULL;
        }
        
        // Simple CPU-based scoring for now
        // Uses character overlap as a placeholder
        // Real implementation would:
        // 1. Tokenize query and documents
        // 2. Run through Metal compute pipeline
        // 3. Return scores
        
        NSString* queryStr = [NSString stringWithUTF8String:query];
        NSString* queryLower = [queryStr lowercaseString];
        
        for (int i = 0; i < doc_count; i++) {
            if (!docs[i]) {
                scores[i] = 0.0f;
                continue;
            }
            
            NSString* docStr = [NSString stringWithUTF8String:docs[i]];
            NSString* docLower = [docStr lowercaseString];
            
            // Simple character overlap score
            int matchCount = 0;
            NSRange range = NSMakeRange(0, [queryLower length]);
            
            for (NSUInteger j = 0; j < [queryLower length]; j++) {
                unichar c = [queryLower characterAtIndex:j];
                if ([docLower rangeOfCharacterFromSet:[NSCharacterSet characterSetWithCharactersInString:[NSString stringWithFormat:@"%c", c]].invertedSet].location != NSNotFound) {
                    matchCount++;
                }
            }
            
            // Normalize score
            scores[i] = (float)matchCount / (float)([queryLower length] + 1);
            
            // Add small boost for shorter documents
            scores[i] *= (1.0f + 1.0f / (float)([docLower length] + 1));
        }
        
        if (out_count) *out_count = doc_count;
        return scores;
    }
}

void metal_engine_free_scores(float* scores) {
    if (scores) {
        free(scores);
    }
}
