//go:build gpu && linux
// +build gpu,linux

#ifndef CUDA_PROF_H
#define CUDA_PROF_H

#include <stddef.h>
#include <stdint.h>
#ifdef USE_GPU
#include <cuda_runtime.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// GPU kernel profiling (roadmap Item 34).
//
// Every named GPU operation is bracketed by an event pair recorded on its
// stream, so the time attributed to it is device time and not the host cost of
// launching it. Event pairs come from a fixed pool and are recycled only after a
// device synchronize, so a launch in flight is never timed against events that
// have been reused.
//
// A launch that cannot be timed - profiling disabled, or the pool exhausted by
// concurrent searches - is counted in the untimed tally rather than dropped,
// so the caller can report how much of the GPU work the table does not cover.

#define LB_GPU_PROF_MAX_OPS 64

void lb_gpu_prof_init(void);
void lb_gpu_prof_set_enabled(int on);
int lb_gpu_prof_enabled(void);

// lb_gpu_prof_op_id interns a name and returns its id, or -1 if the table is
// full. Call it once per call site, at init, not per launch: interning does not
// depend on whether profiling is currently on, because init happens first.
int lb_gpu_prof_op_id(const char *name);

void lb_gpu_prof_begin(int op_id, void *stream);
void lb_gpu_prof_end(void *stream);

// lb_gpu_prof_collect synchronizes the device, folds every completed pair into
// the per-op totals and returns the number of ops seen. ms and calls must each
// hold LB_GPU_PROF_MAX_OPS entries.
int lb_gpu_prof_collect(double *ms, long long *calls, int max_ops);
int lb_gpu_prof_op_count(void);
const char *lb_gpu_prof_op_name(int op_index);
long long lb_gpu_prof_untimed(void);
long long lb_gpu_prof_pool_exhausted(void);

// lb_gpu_prof_reset clears the accumulated totals, leaving the pool and the
// interned names in place.
void lb_gpu_prof_reset(void);

#ifdef __cplusplus
}
#endif

#endif // CUDA_PROF_H