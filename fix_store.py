import sys

# Read the file
with open('internal/store/store.go', 'r') as f:
    lines = f.readlines()

# Find where the broken code starts (the first occurrence of the new functions)
cutoff_index = -1
for i, line in enumerate(lines):
    if 'func (s *VectorStore) StartMetricsTicker' in line:
        cutoff_index = i
        break

# If found, truncate. If not found (maybe previous write failed?), we append.
if cutoff_index != -1:
    # Go back a bit to remove the comment if present
    if i > 0 and '// StartMetricsTicker' in lines[i-1]:
        cutoff_index = i - 1
    lines = lines[:cutoff_index]

# Prepare the correct code
new_code = """
// StartMetricsTicker starts background metrics collection
func (s *VectorStore) StartMetricsTicker(interval time.Duration) {
\tticker := time.NewTicker(interval)
\tgo func() {
\t\tfor range ticker.C {
\t\t\ts.updateMemoryMetrics()
\t\t}
\t}()
}

func (s *VectorStore) updateMemoryMetrics() {
\tvar m runtime.MemStats
\truntime.ReadMemStats(&m)

\tif m.Sys > 0 {
\t\tratio := float64(m.HeapAlloc) / float64(m.Sys)
\t\tmetrics.MemoryFragmentationRatio.Set(ratio)
\t}
}

func (s *VectorStore) updateVectorMetrics(rec arrow.Record) {
\tmetrics.VectorIndexSize.Add(float64(rec.NumRows()))
\tfor i, field := range rec.Schema().Fields() {
\t\tif field.Name == "vector" {
\t\t\tcol := rec.Column(i)
\t\t\tavgNorm := calculateBatchNorm(col)
\t\t\tmetrics.AverageVectorNorm.Set(avgNorm)
\t\t\tbreak
\t\t}
\t}
}

func calculateBatchNorm(arr arrow.Array) float64 {
\tlistArr, ok := arr.(*array.FixedSizeList)
\tif !ok {
\t\treturn 0
\t}
\n\t// Get list size from type
\twidth := int(listArr.DataType().(*arrow.FixedSizeListType).Len())
\n\t// Access values via child data
\tif len(listArr.Data().Children()) == 0 {
\t\treturn 0
\t}
\tvalsData := listArr.Data().Children()[0]
\t
\t// Create a Float32 array wrapper to access values
\tfloatArr := array.NewFloat32Data(valsData)
\tdefer floatArr.Release()
\t
\tvalues := floatArr.Values()
\t
\tvar totalNorm float64
\tcount := 0
\t
\tfor i := 0; i < listArr.Len(); i++ {
\t\tstart := i * width
\t\tend := start + width
\t\t
\t\tif end > len(values) {
\t\t\tbreak
\t\t}
\t\t
\t\tvar sumSq float64
\t\tfor j := start; j < end; j++ {
\t\t\tval := values[j]
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

lines.append(new_code)

with open('internal/store/store.go', 'w') as f:
    f.writelines(lines)
