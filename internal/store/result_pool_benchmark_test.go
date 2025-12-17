package store

import (
"testing"
)

// BenchmarkResultPool_Get benchmarks getting slices from expanded pools
func BenchmarkResultPool_Get(b *testing.B) {
pool := newResultPool()

benchmarks := []struct {
name string
k    int
}{
{"k=10", 10},
{"k=20_reranking", 20},
{"k=50", 50},
{"k=100", 100},
{"k=256_RAG", 256},
{"k=1000_batch", 1000},
}

for _, bm := range benchmarks {
b.Run(bm.name, func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := pool.get(bm.k)
pool.put(slice)
}
})
}
}

// BenchmarkResultPool_GetPut_Reuse benchmarks pool reuse efficiency
func BenchmarkResultPool_GetPut_Reuse(b *testing.B) {
pool := newResultPool()

// Pre-warm the pools
for _, k := range []int{10, 20, 50, 100, 256, 1000} {
s := pool.get(k)
pool.put(s)
}

benchmarks := []struct {
name string
k    int
}{
{"k=20_reranking", 20},
{"k=256_RAG", 256},
{"k=1000_batch", 1000},
}

for _, bm := range benchmarks {
b.Run(bm.name, func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := pool.get(bm.k)
// Simulate work
for j := range slice {
slice[j] = VectorID(j)
}
pool.put(slice)
}
})
}
}

// BenchmarkResultPool_NoPool benchmarks allocation without pooling (k > 1000)
func BenchmarkResultPool_NoPool(b *testing.B) {
pool := newResultPool()

b.Run("k=2000_no_pool", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := pool.get(2000)
pool.put(slice) // Should be a no-op for non-pooled sizes
}
})
}

// BenchmarkResultPool_Reslice benchmarks reslicing from larger pools
func BenchmarkResultPool_Reslice(b *testing.B) {
pool := newResultPool()

benchmarks := []struct {
name string
k    int
}{
{"k=15_from_pool20", 15},
{"k=150_from_pool256", 150},
{"k=500_from_pool1000", 500},
}

for _, bm := range benchmarks {
b.Run(bm.name, func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := pool.get(bm.k)
pool.put(slice)
}
})
}
}

// BenchmarkResultPool_Parallel benchmarks concurrent pool access
func BenchmarkResultPool_Parallel(b *testing.B) {
pool := newResultPool()

benchmarks := []struct {
name string
k    int
}{
{"k=20_parallel", 20},
{"k=256_parallel", 256},
{"k=1000_parallel", 1000},
}

for _, bm := range benchmarks {
b.Run(bm.name, func(b *testing.B) {
b.ReportAllocs()
b.RunParallel(func(pb *testing.PB) {
for pb.Next() {
slice := pool.get(bm.k)
for j := range slice {
slice[j] = VectorID(j)
}
pool.put(slice)
}
})
})
}
}

// BenchmarkResultPool_PooledVsAlloc compares pooled vs direct allocation
func BenchmarkResultPool_PooledVsAlloc(b *testing.B) {
pool := newResultPool()

// Pre-warm
for _, k := range []int{256, 1000} {
s := pool.get(k)
pool.put(s)
}

b.Run("k=256_pooled", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := pool.get(256)
for j := range slice {
slice[j] = VectorID(j)
}
pool.put(slice)
}
})

b.Run("k=256_direct_alloc", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := make([]VectorID, 256)
for j := range slice {
slice[j] = VectorID(j)
}
_ = slice
}
})

b.Run("k=1000_pooled", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := pool.get(1000)
for j := range slice {
slice[j] = VectorID(j)
}
pool.put(slice)
}
})

b.Run("k=1000_direct_alloc", func(b *testing.B) {
b.ReportAllocs()
for i := 0; i < b.N; i++ {
slice := make([]VectorID, 1000)
for j := range slice {
slice[j] = VectorID(j)
}
_ = slice
}
})
}
