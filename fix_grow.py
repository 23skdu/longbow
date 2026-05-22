import re

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'r') as f:
    content = f.read()

# Replace block in GrowMetadataSlices
old_block = """	if !g.SharedVectorSpace {
		g.VectorsF32 = growOffsetSlice(g.VectorsF32)
		g.VectorsSQ8 = growOffsetSlice(g.VectorsSQ8)
		g.VectorsPQ = growOffsetSlice(g.VectorsPQ)
		g.VectorsBQ = growOffsetSlice(g.VectorsBQ)
		g.VectorsTQ = growOffsetSlice(g.VectorsTQ)
		g.VectorsF16 = growOffsetSlice(g.VectorsF16)
		g.VectorsInt8 = growOffsetSlice(g.VectorsInt8)
		g.VectorsInt16 = growOffsetSlice(g.VectorsInt16)
		g.VectorsUint16 = growOffsetSlice(g.VectorsUint16)
		g.VectorsInt32 = growOffsetSlice(g.VectorsInt32)
		g.VectorsUint32 = growOffsetSlice(g.VectorsUint32)
		g.VectorsInt64 = growOffsetSlice(g.VectorsInt64)
		g.VectorsUint64 = growOffsetSlice(g.VectorsUint64)
		g.VectorsFloat64Offsets = growOffsetSlice(g.VectorsFloat64Offsets)
		g.VectorsComplex64Offsets = growOffsetSlice(g.VectorsComplex64Offsets)
		g.VectorsComplex128Offsets = growOffsetSlice(g.VectorsComplex128Offsets)
	}"""

new_block = """	if !g.SharedVectorSpace {
		if g.Type == VectorTypeFloat32 || g.Type == VectorTypeUnknown {
			g.VectorsF32 = growOffsetSlice(g.VectorsF32)
		}
		if g.SQ8Enabled {
			g.VectorsSQ8 = growOffsetSlice(g.VectorsSQ8)
		}
		if g.PQEnabled && g.PQM > 0 {
			g.VectorsPQ = growOffsetSlice(g.VectorsPQ)
		}
		if g.BQEnabled {
			g.VectorsBQ = growOffsetSlice(g.VectorsBQ)
		}
		if g.TurboQuantEnabled {
			g.VectorsTQ = growOffsetSlice(g.VectorsTQ)
		}
		if g.Type == VectorTypeFloat16 {
			g.VectorsF16 = growOffsetSlice(g.VectorsF16)
		}
		if g.Type == VectorTypeInt8 || g.Type == VectorTypeUint8 {
			g.VectorsInt8 = growOffsetSlice(g.VectorsInt8)
		}
		if g.Type == VectorTypeInt16 {
			g.VectorsInt16 = growOffsetSlice(g.VectorsInt16)
		}
		if g.Type == VectorTypeUint16 {
			g.VectorsUint16 = growOffsetSlice(g.VectorsUint16)
		}
		if g.Type == VectorTypeInt32 {
			g.VectorsInt32 = growOffsetSlice(g.VectorsInt32)
		}
		if g.Type == VectorTypeUint32 {
			g.VectorsUint32 = growOffsetSlice(g.VectorsUint32)
		}
		if g.Type == VectorTypeInt64 {
			g.VectorsInt64 = growOffsetSlice(g.VectorsInt64)
		}
		if g.Type == VectorTypeUint64 {
			g.VectorsUint64 = growOffsetSlice(g.VectorsUint64)
		}
		if g.Type == VectorTypeFloat64 {
			g.VectorsFloat64Offsets = growOffsetSlice(g.VectorsFloat64Offsets)
		}
		if g.Type == VectorTypeComplex64 {
			g.VectorsComplex64Offsets = growOffsetSlice(g.VectorsComplex64Offsets)
		}
		if g.Type == VectorTypeComplex128 {
			g.VectorsComplex128Offsets = growOffsetSlice(g.VectorsComplex128Offsets)
		}
	}"""

content = content.replace(old_block, new_block)

with open('/Users/rsd/REPOS/longbow/internal/store/types/graph_data.go', 'w') as f:
    f.write(content)

