//go:build amd64

package tensor

import "unsafe"

const (
	tileMR = 4
	tileNR = 8
	// Cache-tiling parameter:
	//   KC — inner (contracted) dimension tile.  The A strip (4×KC) and the
	//   packed-B sub-panel (KC×8) together fit in L1 data cache (32 KB) so that
	//   the micro-kernel inner loop runs from cache rather than main memory.
	//
	//   4·KC·4 + KC·8·4 = 48·KC ≤ 32·1024  →  KC ≤ 682.  We choose KC = 128 for
	//   generous headroom.
	tileKC = 128
)

// init overrides contractSIMDMatMul with an AVX2-accelerated GEMM using
// a hand-tuned 4×8 assembly micro-kernel (gemm_amd64.s) with B-panel
// packing and optional K-dimension cache tiling.
func init() {
	contractSIMDMatMul = matMulTiledAMD64
}

// matMulTiledAMD64 is a tiled GEMM with per-tile B-panel packing and
// K-dimension cache tiling for large contractions.
//
// For each 4×8 output tile:
//   - The K dimension is processed in KC-sized chunks (tileKC = 128) when
//     k > tileKC, so that the 4×KC A strip and KC×8 packed-B sub-panel
//     fit in L1 cache.
//   - When k ≤ tileKC a single direct pass is used (no chunking overhead).
//
// Edge tiles (last rows / last columns) fall back to generic multiply.
func matMulTiledAMD64(a, b, out *Tensor, m, n, k int) bool {
	if a.Dtype() != DtypeFloat32 || b.Dtype() != DtypeFloat32 || out.Dtype() != DtypeFloat32 {
		return false
	}
	adata := a.Float32s()
	bdata := b.Float32s()
	outdata := out.Float32s()

	const tileM = tileMR
	const tileN = tileNR

	// Single packing buffer reused across all tile calls (max size = tileKC×tileN).
	bpack := make([]float32, tileKC*tileN)

	// When K fits in a single tile we use the simplified fast path:
	// pack B once per output tile and call the micro-kernel directly.
	useKCtiling := k > tileKC

	for i0 := 0; i0 < m; i0 += tileM {
		imax := i0 + tileM
		if imax > m {
			imax = m
		}
		rows := imax - i0

		for j0 := 0; j0 < n; j0 += tileN {
			jmax := j0 + tileN
			if jmax > n {
				jmax = n
			}
			cols := jmax - j0

			if rows == tileM && cols == tileN {
				if useKCtiling {
					// KC tiling: zero C, then accumulate over K in chunks
					for i := i0; i < imax; i++ {
						for j := j0; j < jmax; j++ {
							outdata[i*n+j] = 0
						}
					}
					for kc := 0; kc < k; kc += tileKC {
						kChunk := tileKC
						if kc+kChunk > k {
							kChunk = k - kc
						}
						packBPanel(bdata, bpack, n, kc, j0, kChunk)
						aPtr := uintptr(unsafe.Pointer(&adata[i0*k+kc])) // #nosec G103
						bPtr := uintptr(unsafe.Pointer(&bpack[0]))      // #nosec G103
						cPtr := uintptr(unsafe.Pointer(&outdata[i0*n+j0])) // #nosec G103
						gemm4x8KernelPacked(aPtr, bPtr, cPtr, kChunk, k, n)
					}
				} else {
					// Single pass: K ≤ tileKC — one pack, one call, overwrite C
					packBPanel(bdata, bpack, n, 0, j0, k)
					aPtr := uintptr(unsafe.Pointer(&adata[i0*k])) // #nosec G103
					bPtr := uintptr(unsafe.Pointer(&bpack[0]))    // #nosec G103
					cPtr := uintptr(unsafe.Pointer(&outdata[i0*n+j0])) // #nosec G103
					// Zero C before the call (micro-kernel accumulates)
					for i := i0; i < imax; i++ {
						for j := j0; j < jmax; j++ {
							outdata[i*n+j] = 0
						}
					}
					gemm4x8KernelPacked(aPtr, bPtr, cPtr, k, k, n)
				}
			} else {
				// Partial tile at matrix edge: generic fallback
				for i := i0; i < imax; i++ {
					for j := j0; j < jmax; j++ {
						var sum float32
						for l := 0; l < k; l++ {
							sum += adata[i*k+l] * bdata[l*n+j]
						}
						outdata[i*n+j] = sum
					}
				}
			}
		}
	}
	return true
}

// packBPanel copies kChunk rows of B starting at row kc, column j0 into a
// packed buffer of size kChunk × tileNR (row-major, stride = tileNR elements).
func packBPanel(bdata []float32, bpack []float32, n, kc, j0, kChunk int) {
	for l := 0; l < kChunk; l++ {
		src := bdata[(kc+l)*n+j0:]
		dst := bpack[l*tileNR : l*tileNR+tileNR]
		copy(dst, src[:tileNR])
	}
}
