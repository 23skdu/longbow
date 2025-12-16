package store

import (
"bytes"
"encoding/binary"
"encoding/json"
"testing"

"github.com/apache/arrow-go/v18/arrow"
"github.com/apache/arrow-go/v18/arrow/array"
"github.com/apache/arrow-go/v18/arrow/ipc"
"github.com/apache/arrow-go/v18/arrow/memory"
)

// FuzzIPCReader tests that ipc.NewReader does not panic on malformed input.
// This targets the WAL replay path in persistence.go.
func FuzzIPCReader(f *testing.F) {
mem := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "vec", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
}, nil)
bldr := array.NewRecordBuilder(mem, schema)
defer bldr.Release()

lb := bldr.Field(0).(*array.ListBuilder)
vb := lb.ValueBuilder().(*array.Float32Builder)
lb.Append(true)
vb.AppendValues([]float32{1.0, 2.0, 3.0}, nil)
rec := bldr.NewRecordBatch()
defer rec.Release()

var buf bytes.Buffer
w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
_ = w.Write(rec)
_ = w.Close()

f.Add(buf.Bytes())
f.Add([]byte{})
f.Add([]byte{0x00})
f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
f.Add([]byte("ARROW1"))

f.Fuzz(func(t *testing.T, data []byte) {
r, err := ipc.NewReader(bytes.NewReader(data))
if err != nil {
return
}
defer r.Release()
for r.Next() {
rec := r.RecordBatch()
if rec != nil {
_ = rec.NumRows()
_ = rec.NumCols()
}
}
_ = r.Err()
})
}

// FuzzWALEntryParsing tests WAL binary format parsing for corrupted WAL files.
func FuzzWALEntryParsing(f *testing.F) {
validEntry := func() []byte {
var buf bytes.Buffer
name := "test_dataset"
_ = binary.Write(&buf, binary.LittleEndian, uint64(len(name)))
buf.WriteString(name)
ipcData := []byte{0x41, 0x52, 0x52, 0x4F, 0x57, 0x31}
_ = binary.Write(&buf, binary.LittleEndian, uint64(len(ipcData)))
buf.Write(ipcData)
return buf.Bytes()
}

f.Add(validEntry())
f.Add([]byte{})
f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
f.Add([]byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x41})

f.Fuzz(func(t *testing.T, data []byte) {
r := bytes.NewReader(data)

var nameLen uint64
if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
return
}
if nameLen > 1<<20 {
return
}

nameBytes := make([]byte, nameLen)
if _, err := r.Read(nameBytes); err != nil {
return
}

var recLen uint64
if err := binary.Read(r, binary.LittleEndian, &recLen); err != nil {
return
}
if recLen > 1<<24 {
return
}

recBytes := make([]byte, recLen)
if _, err := r.Read(recBytes); err != nil {
return
}

ipcReader, err := ipc.NewReader(bytes.NewReader(recBytes))
if err != nil {
return
}
defer ipcReader.Release()

for ipcReader.Next() {
rec := ipcReader.RecordBatch()
if rec != nil {
_ = rec.NumRows()
}
}
})
}

// FuzzFlightTicket tests Flight ticket JSON parsing.
func FuzzFlightTicket(f *testing.F) {
f.Add([]byte(`{"dataset":"test","k":10}`))
f.Add([]byte(`{}`))
f.Add([]byte(`{"dataset":""}`))
f.Add([]byte(`{"k":-1}`))
f.Add([]byte(`null`))
f.Add([]byte(`[]`))
f.Add([]byte{})

f.Fuzz(func(t *testing.T, data []byte) {
var ticket struct {
Dataset string    `json:"dataset"`
K       int       `json:"k"`
Vector  []float32 `json:"vector"`
}
_ = json.Unmarshal(data, &ticket)
})
}

// FuzzFlightActionBody tests Flight action body parsing for analytics queries.
func FuzzFlightActionBody(f *testing.F) {
f.Add([]byte(`{"dataset":"test","query":"SELECT * FROM data"}`))
f.Add([]byte(`{"dataset":"","query":""}`))
f.Add([]byte(`{}`))
f.Add([]byte{})

f.Fuzz(func(t *testing.T, data []byte) {
var req struct {
Dataset string `json:"dataset"`
Query   string `json:"query"`
}
_ = json.Unmarshal(data, &req)
})
}

// FuzzArrowSchemaFields tests Arrow schema construction with random field types.
func FuzzArrowSchemaFields(f *testing.F) {
f.Add("field1", uint8(0))
f.Add("vec", uint8(1))
f.Add("", uint8(255))

f.Fuzz(func(t *testing.T, fieldName string, typeCode uint8) {
var dtype arrow.DataType
switch typeCode % 8 {
case 0:
dtype = arrow.PrimitiveTypes.Float32
case 1:
dtype = arrow.PrimitiveTypes.Float64
case 2:
dtype = arrow.PrimitiveTypes.Int32
case 3:
dtype = arrow.PrimitiveTypes.Int64
case 4:
dtype = arrow.BinaryTypes.String
case 5:
dtype = arrow.ListOf(arrow.PrimitiveTypes.Float32)
case 6:
dtype = arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)
case 7:
dtype = arrow.BinaryTypes.Binary
}

field := arrow.Field{Name: fieldName, Type: dtype}
schema := arrow.NewSchema([]arrow.Field{field}, nil)
_ = schema.String()
})
}

// FuzzRecordBuilderWithRandomData tests building records with random dimensions.
func FuzzRecordBuilderWithRandomData(f *testing.F) {
f.Add(uint16(1), uint16(1))
f.Add(uint16(128), uint16(10))
f.Add(uint16(0), uint16(0))

f.Fuzz(func(t *testing.T, dims, rows uint16) {
if dims > 2048 || rows > 1000 || dims == 0 || rows == 0 {
return
}

mem := memory.NewGoAllocator()
schema := arrow.NewSchema([]arrow.Field{
{Name: "vec", Type: arrow.FixedSizeListOf(int32(dims), arrow.PrimitiveTypes.Float32)},
}, nil)

bldr := array.NewRecordBuilder(mem, schema)
defer bldr.Release()

lb := bldr.Field(0).(*array.FixedSizeListBuilder)
vb := lb.ValueBuilder().(*array.Float32Builder)

for i := uint16(0); i < rows; i++ {
lb.Append(true)
for j := uint16(0); j < dims; j++ {
vb.Append(float32(i) * float32(j))
}
}

rec := bldr.NewRecordBatch()
defer rec.Release()

if rec.NumRows() != int64(rows) {
t.Errorf("expected %d rows, got %d", rows, rec.NumRows())
}

var buf bytes.Buffer
w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
if err := w.Write(rec); err != nil {
t.Fatalf("write failed: %v", err)
}
if err := w.Close(); err != nil {
t.Fatalf("close failed: %v", err)
}

r, err := ipc.NewReader(bytes.NewReader(buf.Bytes()), ipc.WithAllocator(mem))
if err != nil {
t.Fatalf("reader failed: %v", err)
}
defer r.Release()

if !r.Next() {
t.Fatal("no records read")
}
})
}
