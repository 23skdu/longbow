import sys

with open('internal/store/store.go', 'r') as f:
    lines = f.readlines()

new_lines = []
imports_added = False
in_new_vector_store = False

for i, line in enumerate(lines):
    # 1. Add imports
    if '"time"' in line and not imports_added:
        new_lines.append(line)
        new_lines.append('\t"math"\n')
        new_lines.append('\t"runtime"\n')
        imports_added = True
        continue

    # 2. Refactor NewVectorStore
    if 'func NewVectorStore' in line:
        in_new_vector_store = True
        new_lines.append(line)
        continue
    
    if in_new_vector_store and 'return &VectorStore{' in line:
        new_lines.append(line.replace('return &VectorStore{', 's := &VectorStore{'))
        continue

    if in_new_vector_store and line.strip() == '}':
        # This is the closing brace of the struct init
        new_lines.append(line)
        new_lines.append('\ts.StartMetricsTicker(10 * time.Second)\n')
        new_lines.append('\treturn s\n')
        in_new_vector_store = False
        continue

    # 3. Instrument DoPut
    if '// Write to WAL' in line:
        new_lines.append('\t\tbuildStart := time.Now()\n')
        new_lines.append(line)
        continue

    if 'rowsWritten += int(rec.NumRows())' in line:
        new_lines.append(line)
        new_lines.append('\t\tmetrics.IndexBuildLatency.Observe(time.Since(buildStart).Seconds())\n')
        new_lines.append('\t\ts.updateVectorMetrics(rec)\n')
        continue

    new_lines.append(line)

# 4. Append new methods
new_methods = """
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
\tvalues := listArr.Values().(*array.Float32)
\tvar totalNorm float64
\tcount := 0
\tfor i := 0; i < listArr.Len(); i++ {
\t\tstart := int64(i) * int64(listArr.ListSize())
\t\tend := start + int64(listArr.ListSize())
\t\tvar sumSq float64
\t\tfor j := start; j < end; j++ {
\t\t\tval := values.Value(int(j))
\t\t\tsumSq += float64(val * val)
\t\t}
\t\ttotalNorm += math.Sqrt(sumSq)
\t\tcount++
\t}
\tif count == 0 {
\t\treturn 0
\t}
\treturn totalNorm / float64(count)
}
"""

new_lines.append(new_methods)

with open('internal/store/store.go', 'w') as f:
    f.writelines(new_lines)
