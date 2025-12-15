import sys

with open('internal/store/store.go', 'r') as f:
    lines = f.readlines()

# Find the start of calculateBatchNorm
start_idx = -1
for i, line in enumerate(lines):
    if 'func calculateBatchNorm(arr arrow.Array) float64 {' in line:
        start_idx = i
        break

if start_idx != -1:
    # Keep everything before the function
    lines = lines[:start_idx]
    
    # Append the fixed function
    fixed_func = """
func calculateBatchNorm(arr arrow.Array) float64 {
\tlistArr, ok := arr.(*array.FixedSizeList)
\tif !ok {
\t\treturn 0
\t}

\t// Get list size from type
\twidth := int(listArr.DataType().(*arrow.FixedSizeListType).Len())

\t// Access values via child data
\tif len(listArr.Data().Children()) == 0 {
\t\treturn 0
\t}
\tvalsData := listArr.Data().Children()[0]
\t
\t// Create a Float32 array wrapper to access values
\tfloatArr := array.NewFloat32Data(valsData)
\tdefer floatArr.Release()
\t
\tvar totalNorm float64
\tcount := 0
\t
\tfor i := 0; i < listArr.Len(); i++ {
\t\tstart := i * width
\t\tend := start + width
\t\t
\t\tif end > floatArr.Len() {
\t\t\tbreak
\t\t}
\t\t
\t\tvar sumSq float64
\t\tfor j := start; j < end; j++ {
\t\t\tval := floatArr.Value(j)
\t\t\tsumSq += float64(val * val)
\t\t}
\t\ttotalNorm += math.Sqrt(sumSq)
\t\tcount++
\t}
\t
\tif count == 0 {
\t\treturn 0
\t}
\treturn totalNorm / float64(count)
}
"""
    lines.append(fixed_func)

    with open('internal/store/store.go', 'w') as f:
        f.writelines(lines)
