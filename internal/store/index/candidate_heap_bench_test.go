package index

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/23skdu/longbow/internal/store/types"
)

// binaryMinAdapter is the pre-4ary binary heap, kept as a benchmark baseline only.
type binaryMinAdapter []types.Candidate

func (h binaryMinAdapter) Len() int           { return len(h) }
func (h binaryMinAdapter) Less(i, j int) bool { return h[i].Dist < h[j].Dist }
func (h binaryMinAdapter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *binaryMinAdapter) pushCandidate(c types.Candidate) {
	*h = append(*h, c)
	h.binaryUp(len(*h) - 1)
}

func (h *binaryMinAdapter) popCandidate() types.Candidate {
	n := len(*h)
	c := (*h)[0]
	(*h)[0] = (*h)[n-1]
	*h = (*h)[:n-1]
	if len(*h) > 0 {
		h.binaryDown(0, len(*h))
	}
	return c
}

func (h *binaryMinAdapter) binaryUp(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *binaryMinAdapter) binaryDown(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

// binaryMaxAdapter is the pre-4ary binary heap, kept as a benchmark baseline only.
type binaryMaxAdapter []types.Candidate

func (h binaryMaxAdapter) Len() int           { return len(h) }
func (h binaryMaxAdapter) Less(i, j int) bool { return h[i].Dist > h[j].Dist }
func (h binaryMaxAdapter) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *binaryMaxAdapter) pushCandidate(c types.Candidate) {
	*h = append(*h, c)
	h.binaryUp(len(*h) - 1)
}

func (h *binaryMaxAdapter) popCandidate() types.Candidate {
	n := len(*h)
	c := (*h)[0]
	(*h)[0] = (*h)[n-1]
	*h = (*h)[:n-1]
	if len(*h) > 0 {
		h.binaryDown(0, len(*h))
	}
	return c
}

func (h *binaryMaxAdapter) binaryUp(j int) {
	for {
		i := (j - 1) / 2
		if i == j || !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		j = i
	}
}

func (h *binaryMaxAdapter) binaryDown(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < n && h.Less(j2, j1) {
			j = j2
		}
		if !h.Less(j, i) {
			break
		}
		h.Swap(i, j)
		i = j
	}
	return i > i0
}

var benchHeapSink types.Candidate

func reportElem(b *testing.B, elemsPerOp int) {
	nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
	b.ReportMetric(nsPerOp/float64(elemsPerOp), "ns/elem")
}

func benchPool(n int, seed int64) []types.Candidate {
	return uniqueShuffledCands(rand.New(rand.NewSource(seed)), n)
}

func BenchmarkMinCandidateHeapAdapter_PushPop(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024} {
		pool := benchPool(n, int64(n)*31+7)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.Run("4ary", func(b *testing.B) {
				h := make(MinCandidateHeapAdapter, 0, n)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for k := 0; k < n; k++ {
						h.PushCandidate(pool[k])
					}
					for k := 0; k < n; k++ {
						benchHeapSink = h.PopCandidate()
					}
				}
				reportElem(b, 2*n)
			})
			b.Run("binary", func(b *testing.B) {
				h := make(binaryMinAdapter, 0, n)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for k := 0; k < n; k++ {
						h.pushCandidate(pool[k])
					}
					for k := 0; k < n; k++ {
						benchHeapSink = h.popCandidate()
					}
				}
				reportElem(b, 2*n)
			})
		})
	}
}

func BenchmarkMaxCandidateHeapAdapter_PushPop(b *testing.B) {
	for _, n := range []int{16, 64, 256, 1024} {
		pool := benchPool(n, int64(n)*13+3)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.Run("4ary", func(b *testing.B) {
				h := make(MaxCandidateHeapAdapter, 0, n)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for k := 0; k < n; k++ {
						h.PushCandidate(pool[k])
					}
					for k := 0; k < n; k++ {
						benchHeapSink = h.PopCandidate()
					}
				}
				reportElem(b, 2*n)
			})
			b.Run("binary", func(b *testing.B) {
				h := make(binaryMaxAdapter, 0, n)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for k := 0; k < n; k++ {
						h.pushCandidate(pool[k])
					}
					for k := 0; k < n; k++ {
						benchHeapSink = h.popCandidate()
					}
				}
				reportElem(b, 2*n)
			})
		})
	}
}

func BenchmarkCandidateHeapAdapter_Drain(b *testing.B) {
	for _, ef := range []int{64, 256} {
		pool := benchPool(ef, int64(ef)*17+11)
		b.Run(fmt.Sprintf("ef=%d", ef), func(b *testing.B) {
			b.Run("min", func(b *testing.B) {
				b.Run("4ary", func(b *testing.B) {
					h := make(MinCandidateHeapAdapter, 0, ef)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for k := 0; k < ef; k++ {
							h.PushCandidate(pool[k])
						}
						for h.Len() > 0 {
							benchHeapSink = h.PopCandidate()
						}
					}
					reportElem(b, 2*ef)
				})
				b.Run("binary", func(b *testing.B) {
					h := make(binaryMinAdapter, 0, ef)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for k := 0; k < ef; k++ {
							h.pushCandidate(pool[k])
						}
						for h.Len() > 0 {
							benchHeapSink = h.popCandidate()
						}
					}
					reportElem(b, 2*ef)
				})
			})
			b.Run("max", func(b *testing.B) {
				b.Run("4ary", func(b *testing.B) {
					h := make(MaxCandidateHeapAdapter, 0, ef)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for k := 0; k < ef; k++ {
							h.PushCandidate(pool[k])
						}
						for h.Len() > 0 {
							benchHeapSink = h.PopCandidate()
						}
					}
					reportElem(b, 2*ef)
				})
				b.Run("binary", func(b *testing.B) {
					h := make(binaryMaxAdapter, 0, ef)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for k := 0; k < ef; k++ {
							h.pushCandidate(pool[k])
						}
						for h.Len() > 0 {
							benchHeapSink = h.popCandidate()
						}
					}
					reportElem(b, 2*ef)
				})
			})
		})
	}
}

func BenchmarkCandidateHeapAdapter_SearchLayerTrim(b *testing.B) {
	const poolN = 4096
	for _, ef := range []int{64, 256} {
		pool := benchPool(poolN, int64(ef)*101+5)
		b.Run(fmt.Sprintf("ef=%d", ef), func(b *testing.B) {
			b.Run("4ary", func(b *testing.B) {
				h := make(MaxCandidateHeapAdapter, 0, ef+1)
				k := 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if k == len(pool) {
						for h.Len() > 0 {
							benchHeapSink = h.PopCandidate()
						}
						k = 0
					}
					h.PushCandidate(pool[k])
					k++
					if h.Len() > ef {
						benchHeapSink = h.PopCandidate()
					}
				}
				reportElem(b, 1)
			})
			b.Run("binary", func(b *testing.B) {
				h := make(binaryMaxAdapter, 0, ef+1)
				k := 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if k == len(pool) {
						for h.Len() > 0 {
							benchHeapSink = h.popCandidate()
						}
						k = 0
					}
					h.pushCandidate(pool[k])
					k++
					if h.Len() > ef {
						benchHeapSink = h.popCandidate()
					}
				}
				reportElem(b, 1)
			})
		})
	}
}
