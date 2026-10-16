//go:build gpu && linux
// +build gpu,linux

// GPU kernel profiling with CUDA events. See cuda_prof_linux.h for the contract
// this implements; roadmap Item 34.

#include "cuda_prof_linux.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef USE_GPU
#include <cuda_runtime.h>

// Pool of event pairs. Sized for concurrent searches: the server runs one
// search per worker, each bracketing a handful of launches.
#define LB_PROF_POOL 4096

// Fold completed pairs once this many are waiting. Folding eagerly is what
// keeps a long run measured: recycling only at collect() exhausted the pool
// after 512 launches, and a 500-query run has ~1500 of them.
#define LB_PROF_FOLD_MARK 512

typedef struct {
    cudaEvent_t start;
    cudaEvent_t end;
    int in_use;
} lb_prof_pair;

static lb_prof_pair g_pool[LB_PROF_POOL];
static int g_free[LB_PROF_POOL];
static int g_free_top = 0;
static int g_initialized = 0;
static int g_enabled = 0;

static char g_names[LB_GPU_PROF_MAX_OPS][64];
static int g_name_len[LB_GPU_PROF_MAX_OPS];
static int g_op_count = 0;

static double g_total_ms[LB_GPU_PROF_MAX_OPS];
static long long g_calls[LB_GPU_PROF_MAX_OPS];
static long long g_untimed = 0;
static long long g_pool_exhausted = 0;

// Completed pairs waiting to be folded in by collect().
typedef struct {
    int op_id;
    int pair;
} lb_prof_pending;

static lb_prof_pending g_pending[LB_PROF_POOL * 2];
static int g_pending_count = 0;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

// One in-flight launch per thread: begin records the start event and remembers
// the slot, end records the finish. Thread-local so concurrent searches on
// different streams do not have to lock on the hot path.
#define LB_PROF_DEPTH 8
typedef struct {
    int op_id;
    int pair;
} lb_prof_inflight;

static __thread lb_prof_inflight t_inflight[LB_PROF_DEPTH];
static __thread int t_inflight_top = 0;
static __thread int t_untimed = 0;

static void lb_prof_ensure_init(void) {
    if (g_initialized) {
        return;
    }
    pthread_mutex_lock(&g_mutex);
    if (!g_initialized) {
        for (int i = 0; i < LB_PROF_POOL; i++) {
            // WithFlagDefault timing is what we want: device time, not host.
            if (cudaEventCreateWithFlags(&g_pool[i].start, cudaEventDefault) != cudaSuccess ||
                cudaEventCreateWithFlags(&g_pool[i].end, cudaEventDefault) != cudaSuccess) {
                cudaGetLastError();
                // Leave the entry unusable; the pool shrinks by one.
                g_pool[i].in_use = 1;
                g_free[g_free_top++] = i;
                g_initialized = 1;
                pthread_mutex_unlock(&g_mutex);
                return;
            }
            g_pool[i].in_use = 1;
            g_free[g_free_top++] = i;
        }
        g_initialized = 1;
    }
    pthread_mutex_unlock(&g_mutex);
}

void lb_gpu_prof_init(void) { lb_prof_ensure_init(); }

void lb_gpu_prof_set_enabled(int on) {
    lb_prof_ensure_init();
    pthread_mutex_lock(&g_mutex);
    g_enabled = on ? 1 : 0;
    pthread_mutex_unlock(&g_mutex);
}

int lb_gpu_prof_enabled(void) { return g_enabled; }

int lb_gpu_prof_op_id(const char *name) {
    // Interning is unconditional: a call site registers at package init, which
    // happens before anything can turn profiling on, and gating it on the flag
    // would hand every site -1 forever. begin/end check the flag instead.
    if (name == NULL) {
        return -1;
    }
    pthread_mutex_lock(&g_mutex);
    int id = -1;
    for (int i = 0; i < g_op_count; i++) {
        if (strcmp(g_names[i], name) == 0) {
            id = i;
            break;
        }
    }
    if (id < 0 && g_op_count < LB_GPU_PROF_MAX_OPS) {
        id = g_op_count++;
        snprintf(g_names[id], sizeof(g_names[0]), "%s", name);
        g_name_len[id] = (int)strlen(g_names[id]);
    }
    pthread_mutex_unlock(&g_mutex);
    return id;
}

void lb_gpu_prof_begin(int op_id, void *stream) {
    if (!g_enabled || op_id < 0) {
        return;
    }
    if (t_inflight_top >= LB_PROF_DEPTH) {
        t_untimed++;
        return;
    }
    int pair = -1;
    pthread_mutex_lock(&g_mutex);
    if (g_free_top > 0) {
        pair = g_free[--g_free_top];
    }
    pthread_mutex_unlock(&g_mutex);
    if (pair < 0) {
        t_untimed++;
        pthread_mutex_lock(&g_mutex);
        g_pool_exhausted++;
        pthread_mutex_unlock(&g_mutex);
        return;
    }
    cudaEvent_t start = g_pool[pair].start;
    cudaEventRecord(start, (cudaStream_t)stream);
    t_inflight[t_inflight_top].op_id = op_id;
    t_inflight[t_inflight_top].pair = pair;
    t_inflight_top++;
}

static void lb_prof_fold_completed(void);

void lb_gpu_prof_end(void *stream) {
    if (!g_enabled || t_inflight_top == 0) {
        return;
    }
    lb_prof_inflight slot = t_inflight[--t_inflight_top];
    cudaEventRecord(g_pool[slot.pair].end, (cudaStream_t)stream);

    pthread_mutex_lock(&g_mutex);
    if (g_pending_count < (int)(sizeof(g_pending) / sizeof(g_pending[0]))) {
        g_pending[g_pending_count].op_id = slot.op_id;
        g_pending[g_pending_count].pair = slot.pair;
        g_pending_count++;
        if (g_pending_count >= LB_PROF_FOLD_MARK) {
            lb_prof_fold_completed();
        }
    } else {
        // Cannot hold the pair: leak it back to the pool unused rather than
        // double-recycling it later.
        g_pool[slot.pair].in_use = 0;
        if (g_free_top < LB_PROF_POOL) {
            g_free[g_free_top++] = slot.pair;
        }
    }
    pthread_mutex_unlock(&g_mutex);
}

// lb_prof_fold_completed accumulates and recycles the pending pairs whose end
// event has already run, without waiting for the device. A pair still in
// flight stays pending and keeps its pool entry, which is the point: it must
// not be recycled while the GPU is still reading it. The caller holds g_mutex.
static void lb_prof_fold_completed(void) {
    int keep = 0;
    for (int i = 0; i < g_pending_count; i++) {
        int op = g_pending[i].op_id;
        int pair = g_pending[i].pair;
        if (cudaEventQuery(g_pool[pair].end) != cudaSuccess) {
            // Still running: keep it where it is, preserving order.
            g_pending[keep++] = g_pending[i];
            continue;
        }
        float elapsed = 0.0f;
        if (cudaEventElapsedTime(&elapsed, g_pool[pair].start, g_pool[pair].end) == cudaSuccess &&
            op >= 0 && op < LB_GPU_PROF_MAX_OPS) {
            g_total_ms[op] += (double)elapsed;
            g_calls[op] += 1;
        }
        g_pool[pair].in_use = 0;
        if (g_free_top < LB_PROF_POOL) {
            g_free[g_free_top++] = pair;
        }
    }
    g_pending_count = keep;
}

int lb_gpu_prof_collect(double *ms, long long *calls, int max_ops) {
    if (!g_enabled) {
        return 0;
    }
    // One synchronize covers every recorded pair: an event's elapsed time is
    // only meaningful once both of its events have completed.
    cudaDeviceSynchronize();

    int ops = 0;
    pthread_mutex_lock(&g_mutex);
    // Every event has completed after the synchronize, so one pass folds the
    // lot.
    lb_prof_fold_completed();
    for (int i = 0; i < g_pending_count; i++) {
        int op = g_pending[i].op_id;
        int pair = g_pending[i].pair;
        float elapsed = 0.0f;
        if (cudaEventElapsedTime(&elapsed, g_pool[pair].start, g_pool[pair].end) == cudaSuccess && op < max_ops) {
            g_total_ms[op] += (double)elapsed;
            g_calls[op] += 1;
        }
        g_pool[pair].in_use = 0;
        if (g_free_top < LB_PROF_POOL) {
            g_free[g_free_top++] = pair;
        }
    }
    g_pending_count = 0;
    for (int i = 0; i < g_op_count && i < max_ops; i++) {
        if (ms != NULL) ms[i] = g_total_ms[i];
        if (calls != NULL) calls[i] = g_calls[i];
        ops++;
    }
    pthread_mutex_unlock(&g_mutex);
    return ops;
}

int lb_gpu_prof_op_count(void) {
    pthread_mutex_lock(&g_mutex);
    int n = g_op_count;
    pthread_mutex_unlock(&g_mutex);
    return n;
}

const char *lb_gpu_prof_op_name(int op_index) {
    if (op_index < 0 || op_index >= LB_GPU_PROF_MAX_OPS) {
        return "";
    }
    return g_names[op_index];
}

long long lb_gpu_prof_untimed(void) {
    pthread_mutex_lock(&g_mutex);
    long long n = g_untimed;
    pthread_mutex_unlock(&g_mutex);
    return n;
}

long long lb_gpu_prof_pool_exhausted(void) {
    pthread_mutex_lock(&g_mutex);
    long long n = g_pool_exhausted;
    pthread_mutex_unlock(&g_mutex);
    return n;
}

void lb_gpu_prof_reset(void) {
    pthread_mutex_lock(&g_mutex);
    memset(g_total_ms, 0, sizeof(g_total_ms));
    memset(g_calls, 0, sizeof(g_calls));
    g_untimed = 0;
    g_pool_exhausted = 0;
    pthread_mutex_unlock(&g_mutex);
}

#else // !USE_GPU

// The stub build links the same API and reports nothing, so a non-CUDA binary
// with the gpu tag (or a CPU-only CUDA-less host) still compiles.

void lb_gpu_prof_init(void) {}
void lb_gpu_prof_set_enabled(int on) { (void)on; }
int lb_gpu_prof_enabled(void) { return 0; }
int lb_gpu_prof_op_id(const char *name) { (void)name; return -1; }
void lb_gpu_prof_begin(int op_id, void *stream) { (void)op_id; (void)stream; }
void lb_gpu_prof_end(void *stream) { (void)stream; }
int lb_gpu_prof_collect(double *ms, long long *calls, int max_ops) {
    (void)ms; (void)calls; (void)max_ops; return 0;
}
int lb_gpu_prof_op_count(void) { return 0; }
const char *lb_gpu_prof_op_name(int op_index) { (void)op_index; return ""; }
long long lb_gpu_prof_untimed(void) { return 0; }
long long lb_gpu_prof_pool_exhausted(void) { return 0; }
void lb_gpu_prof_reset(void) {}

#endif // USE_GPU