package hnsw

import (
	"math"
	"unsafe"

	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"

	"github.com/23skdu/longbow/internal/pq"
	"github.com/23skdu/longbow/internal/store/types"
)

func (h *ArrowHNSW) resolveHNSWComputer(data *types.GraphData, searchCtx *ArrowSearchContext, queryVal any, squared bool, options any) DistanceComputer {
	searchOptions := types.SearchOptions{}
	if opt, ok := options.(types.SearchOptions); ok {
		searchOptions = opt
	}

	switch q := queryVal.(type) {
	case []float32:
		bypassQuantization := searchOptions.VectorType == types.VectorTypeFloat32

		// Fallback dynamically to full-precision Float32 on Layer 0 if primary type is Float32
		// ForceQuantized is true for upper layers (layer > 0)
		if !bypassQuantization && !searchOptions.ForceQuantized && data.Type == types.VectorTypeFloat32 {
			// Unless the user explicitly requested VectorTypeTQ, fall back to Float32
			if searchOptions.VectorType != types.VectorTypeTQ {
				bypassQuantization = true
			}
		}

		if bypassQuantization {
			// Bypass TurboQuant and Product Quantization to force exact matching fallback
		} else {
			// One load: the runtime is replaced as a unit when the dimension is
			// discovered, and two loads could straddle that replacement.
			if tq := h.tq(); tq != nil && tq.compute != nil && data.TurboQuantEnabled && searchCtx != nil {
				pow2 := tq.compute.encoder.pow2
				if len(searchCtx.rotatedQueryTQ) < pow2 {
					searchCtx.rotatedQueryTQ = make([]float32, pow2)
				}
				searchCtx.EnsureTQReconCapacity(pow2)
				_ = tq.compute.PrecomputeRotatedQuery(q, searchCtx.rotatedQueryTQ)
				slot := searchCtx.nextCompSlot()
				c := &searchCtx.compTQ[slot]
				c.data = data
				c.h = h
				c.rotatedQuery = searchCtx.rotatedQueryTQ
				c.diskGraph = searchCtx.diskGraph
				c.maxGen = searchCtx.MaxGeneration
				c.sctx = searchCtx
				c.batchOK = false
				return c
			}
			if h.config.PQEnabled && h.oopqEncoder != nil {
				var table any
				var err error
				switch enc := h.oopqEncoder.(type) {
				case *pq.PQEncoder:
					table, err = enc.BuildADCTable(q)
				case *pq.OPQEncoder:
					table, err = enc.BuildADCTable(q)
				}
				if err == nil && table != nil {
					if searchCtx != nil {
						slot := searchCtx.nextCompSlot()
						c := &searchCtx.compPQ[slot]
						c.data = data
						c.q = q
						c.table = table
						c.h = h
						c.diskGraph = searchCtx.GetDiskGraph()
						c.maxGen = searchCtx.MaxGeneration
						return c
					}
					return &pqComputer{data: data, q: q, table: table, h: h, diskGraph: h.diskGraph.Load(), maxGen: math.MaxUint64}
				}
			}
		}
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.GetDiskGraph()
		}
		if data.Type == types.VectorTypeFloat32 {
			maxGen := uint64(math.MaxUint64)
			if searchCtx != nil {
				maxGen = searchCtx.MaxGeneration
			}

			// Detect Shared Vector Space and use specialized computer
			if h.sharedVectorSpace.Load() && h.dataset != nil {
				recs := h.dataset.GetRecords()
				slices := make([][]float32, len(recs))
				vecColIdx := -1
				for i, rec := range recs {
					if rec == nil {
						continue
					}
					if vecColIdx == -1 {
						vecColIdx = h.getVectorColumnIndex(rec)
					}
					if vecColIdx != -1 {
						col := rec.Column(vecColIdx)
						if list, ok := col.(*arrowarray.FixedSizeList); ok {
							if values, ok := list.ListValues().(*arrowarray.Float32); ok {
								slices[i] = values.Float32Values()
							}
						}
					}
				}
				return &sharedFloat32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen, slices: slices}
			}

			// Force SQ8 Quantized Navigation on upper layers
			if searchOptions.ForceQuantized && h.config.SQ8Enabled && data.SQ8Enabled && h.quantizer != nil && h.sq8Ready.Load() {
				minV, maxV := h.quantizer.Params()
				scale := (maxV - minV) / 255.0
				if searchCtx != nil {
					slot := searchCtx.nextCompSlot()
					c := &searchCtx.compSQ8[slot]
					*c = float32ToSQ8Computer{data: data, q: q, dims: len(q), h: h, squared: squared, maxGen: maxGen, minV: minV, scale: scale}
					return c
				}
				return &float32ToSQ8Computer{data: data, q: q, dims: len(q), h: h, squared: squared, maxGen: maxGen, minV: minV, scale: scale}
			}

			if searchCtx != nil {
				slot := searchCtx.nextCompSlot()
				c := &searchCtx.compF32ToF32[slot]
				*c = float32ToFloat32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen, sctx: searchCtx}
				return c
			}
			return &float32ToFloat32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen, sctx: searchCtx}
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		if data.Type == types.VectorTypeInt8 {
			var qInt8 []int8
			if searchCtx != nil {
				searchCtx.queryInt8 = resizeInt8(searchCtx.queryInt8, len(q))
				narrowFloatsToInt8(q, searchCtx.queryInt8)
				qInt8 = searchCtx.queryInt8
			} else {
				qInt8 = make([]int8, len(q))
				narrowFloatsToInt8(q, qInt8)
			}
			qUint8 := *(*[]uint8)(unsafe.Pointer(&qInt8)) // #nosec G103
			if searchCtx != nil {
				slot := searchCtx.nextCompSlot()
				c := &searchCtx.compInt8[slot]
				*c = int8Computer{data: data, q: qUint8, qInt8: qInt8, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen, isUint8: false, sctx: searchCtx}
				return c
			}
			return &int8Computer{data: data, q: qUint8, qInt8: qInt8, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen, isUint8: false, sctx: searchCtx}
		}
		if data.Type == types.VectorTypeUint8 {
			var qUint8 []uint8
			if searchCtx != nil {
				searchCtx.queryUint8 = resizeUint8(searchCtx.queryUint8, len(q))
				narrowFloatsToUint8(q, searchCtx.queryUint8)
				qUint8 = searchCtx.queryUint8
			} else {
				qUint8 = make([]uint8, len(q))
				narrowFloatsToUint8(q, qUint8)
			}
			qInt8 := *(*[]int8)(unsafe.Pointer(&qUint8)) // #nosec G103
			if searchCtx != nil {
				slot := searchCtx.nextCompSlot()
				c := &searchCtx.compInt8[slot]
				*c = int8Computer{data: data, q: qUint8, qInt8: qInt8, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen, isUint8: true, sctx: searchCtx}
				return c
			}
			return &int8Computer{data: data, q: qUint8, qInt8: qInt8, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen, isUint8: true, sctx: searchCtx}
		}
		if data.Type == types.VectorTypeInt16 {
			if searchCtx == nil {
				qInt16 := make([]int16, len(q))
				narrowFloatsToInt16(q, qInt16)
				return &int16Computer{data: data, q: qInt16, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			searchCtx.queryInt16 = resizeInt16(searchCtx.queryInt16, len(q))
			narrowFloatsToInt16(q, searchCtx.queryInt16)
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compInt16[slot]
			*c = int16Computer{data: data, q: searchCtx.queryInt16, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		if data.Type == types.VectorTypeUint16 {
			if searchCtx == nil {
				qUint16 := make([]uint16, len(q))
				narrowFloatsToUint16(q, qUint16)
				return &uint16Computer{data: data, q: qUint16, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			searchCtx.queryUint16 = resizeUint16(searchCtx.queryUint16, len(q))
			narrowFloatsToUint16(q, searchCtx.queryUint16)
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compUint16[slot]
			*c = uint16Computer{data: data, q: searchCtx.queryUint16, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		if data.Type == types.VectorTypeInt32 {
			if searchCtx == nil {
				qInt32 := make([]int32, len(q))
				narrowFloatsToInt32(q, qInt32)
				return &int32Computer{data: data, q: qInt32, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			searchCtx.queryInt32 = resizeInt32(searchCtx.queryInt32, len(q))
			narrowFloatsToInt32(q, searchCtx.queryInt32)
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compInt32[slot]
			*c = int32Computer{data: data, q: searchCtx.queryInt32, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		if data.Type == types.VectorTypeUint32 {
			if searchCtx == nil {
				qUint32 := make([]uint32, len(q))
				narrowFloatsToUint32(q, qUint32)
				return &uint32Computer{data: data, q: qUint32, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			searchCtx.queryUint32 = resizeUint32(searchCtx.queryUint32, len(q))
			narrowFloatsToUint32(q, searchCtx.queryUint32)
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compUint32[slot]
			*c = uint32Computer{data: data, q: searchCtx.queryUint32, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		if data.Type == types.VectorTypeInt64 {
			if searchCtx == nil {
				qInt64 := make([]int64, len(q))
				for i, val := range q {
					qInt64[i] = int64(val) // #nosec G115 -- the corpus is int64, and float32 is exactly representable as int64 for |v| < 2^53
				}
				return &int64Computer{data: data, q: qInt64, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			searchCtx.queryInt64 = searchCtx.queryInt64[:0]
			for _, val := range q {
				searchCtx.queryInt64 = append(searchCtx.queryInt64, int64(val)) // #nosec G115 -- the corpus is int64
			}
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compInt64[slot]
			*c = int64Computer{data: data, q: searchCtx.queryInt64, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		if data.Type == types.VectorTypeUint64 {
			if searchCtx == nil {
				qUint64 := make([]uint64, len(q))
				for i, val := range q {
					if val < 0 {
						qUint64[i] = 0
					} else {
						qUint64[i] = uint64(val) // #nosec G115 -- non-negative float32
					}
				}
				return &uint64Computer{data: data, q: qUint64, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			}
			searchCtx.queryUint64 = searchCtx.queryUint64[:0]
			for _, val := range q {
				if val < 0 {
					searchCtx.queryUint64 = append(searchCtx.queryUint64, 0)
				} else {
					searchCtx.queryUint64 = append(searchCtx.queryUint64, uint64(val)) // #nosec G115 -- non-negative float32
				}
			}
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compUint64[slot]
			*c = uint64Computer{data: data, q: searchCtx.queryUint64, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		if searchCtx != nil {
			if data.Type == types.VectorTypeComplex64 {
				physDims := len(q)
				logDims := physDims / 2
				if cap(searchCtx.queryC64) < logDims {
					searchCtx.queryC64 = make([]complex64, logDims)
				}
				searchCtx.queryC64 = searchCtx.queryC64[:logDims]
				for i := 0; i < logDims; i++ {
					searchCtx.queryC64[i] = complex(q[2*i], q[2*i+1])
				}
				slot := searchCtx.nextCompSlot()
				c := &searchCtx.compC64[slot]
				*c = complex64Computer{
					data: data, q: searchCtx.queryC64, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen,
					sctx: searchCtx,
				}
				return c
			}
			if data.Type == types.VectorTypeComplex128 {
				physDims := len(q)
				logDims := physDims / 2
				if cap(searchCtx.queryC128) < logDims {
					searchCtx.queryC128 = make([]complex128, logDims)
				}
				searchCtx.queryC128 = searchCtx.queryC128[:logDims]
				for i := 0; i < logDims; i++ {
					searchCtx.queryC128[i] = complex(float64(q[2*i]), float64(q[2*i+1]))
				}
				var sum float64
				for _, v := range searchCtx.queryC128 {
					sum += real(v)*real(v) + imag(v)*imag(v)
				}
				slot := searchCtx.nextCompSlot()
				c := &searchCtx.compC128[slot]
				*c = complex128Computer{
					data: data, q: searchCtx.queryC128, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen, queryMag: math.Sqrt(sum),
					sctx: searchCtx,
				}
				return c
			}
			slot := searchCtx.nextCompSlot()
			comp := &searchCtx.compF32[slot]
			*comp = float32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen, sctx: searchCtx}
			if data.Type == types.VectorTypeFloat64 {
				searchCtx.queryF64 = searchCtx.queryF64[:0]
				for _, val := range q {
					searchCtx.queryF64 = append(searchCtx.queryF64, float64(val))
				}
				comp.qF64 = searchCtx.queryF64
			}
			if data.Type == types.VectorTypeFloat16 {
				searchCtx.queryF16 = searchCtx.queryF16[:0]
				for _, val := range q {
					searchCtx.queryF16 = append(searchCtx.queryF16, float16.New(val))
				}
				comp.qF16 = searchCtx.queryF16
			}
			return comp
		}
		return &float32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, squared: squared, maxGen: maxGen}
	case []int8, []uint8:
		var q8 []uint8
		var qInt8 []int8
		if qi8, ok := queryVal.([]int8); ok {
			q8 = *(*[]uint8)(unsafe.Pointer(&qi8)) // #nosec G103
			qInt8 = qi8
		} else {
			q8 = queryVal.([]uint8)
			qInt8 = *(*[]int8)(unsafe.Pointer(&q8)) // #nosec G103
		}
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}

		// Detect Shared Vector Space for Int8
		if h.sharedVectorSpace.Load() && h.dataset != nil && (data.Type == types.VectorTypeInt8 || data.Type == types.VectorTypeUint8) {
			recs := h.dataset.GetRecords()
			slices := make([][]int8, len(recs))
			vecColIdx := -1
			for i, rec := range recs {
				if rec == nil {
					continue
				}
				if vecColIdx == -1 {
					vecColIdx = h.getVectorColumnIndex(rec)
				}
				if vecColIdx != -1 {
					col := rec.Column(vecColIdx)
					if list, ok := col.(*arrowarray.FixedSizeList); ok {
						if values, ok := list.ListValues().(*arrowarray.Int8); ok {
							slices[i] = values.Int8Values()
						} else if values, ok := list.ListValues().(*arrowarray.Uint8); ok {
							u8s := values.Uint8Values()
							slices[i] = *(*[]int8)(unsafe.Pointer(&u8s)) // #nosec G103
						}
					}
				}
			}
			return &sharedInt8Computer{data: data, q: q8, qInt8: qInt8, dims: len(q8), h: h, diskGraph: dg, maxGen: maxGen, slices: slices, isUint8: data.Type == types.VectorTypeUint8}
		}

		if searchCtx != nil {
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compInt8[slot]
			*c = int8Computer{data: data, q: q8, qInt8: qInt8, dims: len(q8), h: h, diskGraph: dg, maxGen: maxGen, isUint8: data.Type == types.VectorTypeUint8, sctx: searchCtx}
			return c
		}
		return &int8Computer{data: data, q: q8, qInt8: qInt8, dims: len(q8), h: h, diskGraph: dg, maxGen: maxGen, isUint8: data.Type == types.VectorTypeUint8}
	case []float64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		if data.Type == types.VectorTypeComplex128 {
			physDims := len(q)
			logDims := physDims / 2
			if searchCtx != nil {
				if cap(searchCtx.queryC128) < logDims {
					searchCtx.queryC128 = make([]complex128, logDims)
				}
				searchCtx.queryC128 = searchCtx.queryC128[:logDims]
				for i := 0; i < logDims; i++ {
					searchCtx.queryC128[i] = complex(q[2*i], q[2*i+1])
				}
				var sum float64
				for _, v := range searchCtx.queryC128 {
					sum += real(v)*real(v) + imag(v)*imag(v)
				}
				slot := searchCtx.nextCompSlot()
				c := &searchCtx.compC128[slot]
				*c = complex128Computer{
					data: data, q: searchCtx.queryC128, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen, queryMag: math.Sqrt(sum),
					sctx: searchCtx,
				}
				return c
			}
			qC128 := make([]complex128, logDims)
			for i := 0; i < logDims; i++ {
				qC128[i] = complex(q[2*i], q[2*i+1])
			}
			var sum float64
			for _, v := range qC128 {
				sum += real(v)*real(v) + imag(v)*imag(v)
			}
			return &complex128Computer{data: data, q: qC128, dims: logDims, h: h, diskGraph: dg, maxGen: maxGen, queryMag: math.Sqrt(sum)}
		}
		if searchCtx != nil {
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compF64[slot]
			*c = float64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &float64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []float16.Num:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
		}
		if searchCtx != nil {
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compF16[slot]
			*c = float16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &float16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []complex64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
			if cap(searchCtx.queryC64) < len(q) {
				searchCtx.queryC64 = make([]complex64, len(q))
			}
			searchCtx.queryC64 = searchCtx.queryC64[:len(q)]
			copy(searchCtx.queryC64, q)
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compC64[slot]
			*c = complex64Computer{data: data, q: searchCtx.queryC64, dims: len(q), h: h, diskGraph: dg, maxGen: searchCtx.MaxGeneration, sctx: searchCtx}
			return c
		}
		dg = h.diskGraph.Load()
		return &complex64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: math.MaxUint64}
	case []complex128:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
			if cap(searchCtx.queryC128) < len(q) {
				searchCtx.queryC128 = make([]complex128, len(q))
			}
			searchCtx.queryC128 = searchCtx.queryC128[:len(q)]
			copy(searchCtx.queryC128, q)
			var sum float64
			for _, v := range searchCtx.queryC128 {
				sum += real(v)*real(v) + imag(v)*imag(v)
			}
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compC128[slot]
			*c = complex128Computer{data: data, q: searchCtx.queryC128, dims: len(q), h: h, diskGraph: dg, maxGen: searchCtx.MaxGeneration, queryMag: math.Sqrt(sum), sctx: searchCtx}
			return c
		}
		dg = h.diskGraph.Load()
		var sum float64
		for _, v := range q {
			sum += real(v)*real(v) + imag(v)*imag(v)
		}
		return &complex128Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: math.MaxUint64, queryMag: math.Sqrt(sum)}
	case []int16:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compInt16[slot]
			*c = int16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &int16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []uint16:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compUint16[slot]
			*c = uint16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &uint16Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []int32:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compInt32[slot]
			*c = int32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &int32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []uint32:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compUint32[slot]
			*c = uint32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &uint32Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []int64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compInt64[slot]
			*c = int64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &int64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	case []uint64:
		var dg *DiskGraph
		if searchCtx != nil {
			dg = searchCtx.diskGraph
		} else {
			dg = h.diskGraph.Load()
		}
		maxGen := uint64(math.MaxUint64)
		if searchCtx != nil {
			maxGen = searchCtx.MaxGeneration
			slot := searchCtx.nextCompSlot()
			c := &searchCtx.compUint64[slot]
			*c = uint64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
			return c
		}
		return &uint64Computer{data: data, q: q, dims: len(q), h: h, diskGraph: dg, maxGen: maxGen}
	}
	return nil
}
