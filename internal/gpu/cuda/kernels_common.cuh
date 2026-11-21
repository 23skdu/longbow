#pragma once

#include <cuda_runtime.h>
#include <device_launch_parameters.h>
#include <cuda_fp16.h>
#include <math.h>
#include <stdint.h>

// Top-K implementation using shared memory heap
#define MAX_K 1024
#ifndef WARP_SZ
#define WARP_SZ 32
#endif

typedef struct {
    float dist;
    int64_t id;
} ResultPair;

static __device__ inline void heap_push(ResultPair* heap, int* size, float dist, int64_t id, int k) {
    if (*size < k) {
        heap[*size] = {dist, id};
        int curr = *size;
        while (curr > 0) {
            int parent = (curr - 1) / 2;
            if (heap[curr].dist > heap[parent].dist) {
                ResultPair tmp = heap[curr];
                heap[curr] = heap[parent];
                heap[parent] = tmp;
                curr = parent;
            } else break;
        }
        (*size)++;
    } else if (dist < heap[0].dist) {
        heap[0] = {dist, id};
        int curr = 0;
        while (true) {
            int left = 2 * curr + 1;
            int right = 2 * curr + 2;
            int largest = curr;
            if (left < k && heap[left].dist > heap[largest].dist) largest = left;
            if (right < k && heap[right].dist > heap[largest].dist) largest = right;
            if (largest != curr) {
                ResultPair tmp = heap[curr];
                heap[curr] = heap[largest];
                heap[largest] = tmp;
                curr = largest;
            } else break;
        }
    }
}
