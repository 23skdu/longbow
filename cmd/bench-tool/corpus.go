package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"log"
	"math/rand"
	"os"
	"strconv"

	"github.com/23skdu/longbow/pkg/safe"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// generateRecord is a multi-type arrow table builder (backward compatible wrapper).
func generateRecord(count int, dim int, dtype string, tqBits int) (arrow.RecordBatch, *arrow.Schema, error) {
	// R16: deterministic, unlike the time.Now() seed it replaces.
	rng := rand.New(rand.NewSource(RunSeed)) // #nosec G404 -- benchmark data, not crypto
	return generateRecordBatch(rng, 0, count, dim, dtype, tqBits)
}

// hashRecord appends an Arrow IPC serialisation of rec to h. The serialisation
// is a function of the schema and the values alone, so two runs that generated
// the same corpus hash the same, and a corpus that moved with the scheduler
// shows up as a different digest instead of as a mysterious search difference
// (roadmap Item 31).
func hashRecord(h hash.Hash, rec arrow.RecordBatch) error {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()))
	if err := w.Write(rec); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	_, err := h.Write(buf.Bytes())
	return err
}

// generateRecordBatch builds an Arrow record batch using an isolated PRNG and base offset for lock-free parallel generation.
func generateRecordBatch(rng *rand.Rand, offset int, count int, dim int, dtype string, tqBits int) (arrow.RecordBatch, *arrow.Schema, error) {
	pool := memory.NewGoAllocator()
	var dt arrow.DataType

	switch dtype {
	case "float32", "turboquant":
		dt = arrow.PrimitiveTypes.Float32
	case "float64":
		dt = arrow.PrimitiveTypes.Float64
	case "float16":
		dt = arrow.FixedWidthTypes.Float16
	case "int32":
		dt = arrow.PrimitiveTypes.Int32
	case "int16":
		dt = arrow.PrimitiveTypes.Int16
	case "int8":
		dt = arrow.PrimitiveTypes.Int8
	case "uint32":
		dt = arrow.PrimitiveTypes.Uint32
	case "uint16":
		dt = arrow.PrimitiveTypes.Uint16
	case "uint8":
		dt = arrow.PrimitiveTypes.Uint8
	case "int64":
		dt = arrow.PrimitiveTypes.Int64
	case "uint64":
		dt = arrow.PrimitiveTypes.Uint64
	case "complex64":
		dt = arrow.PrimitiveTypes.Float32
	case "complex128":
		dt = arrow.PrimitiveTypes.Float64
	default:
		return nil, nil, fmt.Errorf("unsupported dtype: %s", dtype)
	}

	listLen := int32(dim) // #nosec G115
	if dtype == "complex64" || dtype == "complex128" {
		listLen = int32(2 * dim) // #nosec G115
	}
	var meta arrow.Metadata
	if dtype == "turboquant" {
		meta = arrow.NewMetadata([]string{"longbow.vector_type", "longbow.turboquant_bits"}, []string{dtype, strconv.Itoa(tqBits)})
	} else {
		meta = arrow.NewMetadata([]string{"longbow.vector_type"}, []string{dtype})
	}

	var vecField arrow.Field
	if meta.Len() > 0 {
		vecField = arrow.Field{Name: "vector", Type: arrow.FixedSizeListOf(listLen, dt), Metadata: meta}
	} else {
		vecField = arrow.Field{Name: "vector", Type: arrow.FixedSizeListOf(listLen, dt)}
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			vecField,
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ns},
			{Name: "geo_point", Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64)},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "category", Type: arrow.BinaryTypes.String},
			{Name: "text", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	// 1. Build IDs using strconv.Itoa (fast zero-reflection string formatting)
	idBldr := array.NewStringBuilder(pool)
	defer idBldr.Release()
	idBldr.Reserve(count)
	for i := 0; i < count; i++ {
		idBldr.Append(strconv.Itoa(offset + i))
	}
	idArr := idBldr.NewArray()
	defer idArr.Release()

	// 2. Build Vectors
	listBldr := array.NewFixedSizeListBuilder(pool, listLen, dt)
	defer listBldr.Release()
	listBldr.Reserve(count)

	dimensionMultiplier := 1
	if dtype == "complex64" || dtype == "complex128" {
		dimensionMultiplier = 2
	}

	switch dtype {
	case "float32", "complex64", "turboquant":
		vb := listBldr.ValueBuilder().(*array.Float32Builder)
		stride := dim * dimensionMultiplier
		vb.Reserve(count * stride)
		vals := make([]float32, count*stride)
		for i := range vals {
			vals[i] = rng.Float32() // #nosec G404
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*stride:(i+1)*stride], nil)
		}
	case "float64", "complex128":
		vb := listBldr.ValueBuilder().(*array.Float64Builder)
		stride := dim * dimensionMultiplier
		vb.Reserve(count * stride)
		vals := make([]float64, count*stride)
		for i := range vals {
			vals[i] = rng.Float64() // #nosec G404
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*stride:(i+1)*stride], nil)
		}
	case "float16":
		vb := listBldr.ValueBuilder().(*array.Float16Builder)
		vb.Reserve(count * dim)
		vals := make([]float16.Num, count*dim)
		for i := range vals {
			vals[i] = float16.New(rng.Float32()) // #nosec G404
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int32":
		vb := listBldr.ValueBuilder().(*array.Int32Builder)
		vb.Reserve(count * dim)
		vals := make([]int32, count*dim)
		for i := range vals {
			vals[i] = int32(rng.Intn(corpusValueBound("int32"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int16":
		vb := listBldr.ValueBuilder().(*array.Int16Builder)
		vb.Reserve(count * dim)
		vals := make([]int16, count*dim)
		for i := range vals {
			vals[i] = int16(rng.Intn(corpusValueBound("int16"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int8":
		vb := listBldr.ValueBuilder().(*array.Int8Builder)
		vb.Reserve(count * dim)
		vals := make([]int8, count*dim)
		for i := range vals {
			vals[i] = int8(rng.Intn(corpusValueBound("int8"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint32":
		vb := listBldr.ValueBuilder().(*array.Uint32Builder)
		vb.Reserve(count * dim)
		vals := make([]uint32, count*dim)
		for i := range vals {
			vals[i] = uint32(rng.Intn(corpusValueBound("uint32"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint16":
		vb := listBldr.ValueBuilder().(*array.Uint16Builder)
		vb.Reserve(count * dim)
		vals := make([]uint16, count*dim)
		for i := range vals {
			vals[i] = uint16(rng.Intn(corpusValueBound("uint16"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint8":
		vb := listBldr.ValueBuilder().(*array.Uint8Builder)
		vb.Reserve(count * dim)
		vals := make([]uint8, count*dim)
		for i := range vals {
			vals[i] = uint8(rng.Intn(corpusValueBound("uint8"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "int64":
		vb := listBldr.ValueBuilder().(*array.Int64Builder)
		vb.Reserve(count * dim)
		vals := make([]int64, count*dim)
		for i := range vals {
			vals[i] = int64(rng.Intn(corpusValueBound("int64"))) // #nosec G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	case "uint64":
		vb := listBldr.ValueBuilder().(*array.Uint64Builder)
		vb.Reserve(count * dim)
		vals := make([]uint64, count*dim)
		for i := range vals {
			vals[i] = uint64(rng.Intn(corpusValueBound("uint64"))) // #nosec G115,G404 -- bound comes from corpusValueRange
		}
		for i := 0; i < count; i++ {
			listBldr.Append(true)
			vb.AppendValues(vals[i*dim:(i+1)*dim], nil)
		}
	}

	vecArr := listBldr.NewArray()
	defer vecArr.Release()

	// 3. Build Timestamp
	//
	// Item 31: derived from the run's as-of timestamp and the row's position,
	// not from time.Now(). The wall-clock column was the one part of the corpus
	// no seed could reproduce - every row differed between two runs before a
	// single vector was drawn, so "the same seed builds the same corpus" was
	// false at the schema level - and it made Temporal answers depend on when
	// the run happened rather than on what was in it. Walking back from as_of
	// keeps the rows in strictly increasing order of age and every one of them
	// inside the window the Temporal mode queries (default as_of is
	// 2100-01-01, which is after all of them, exactly as "now" was).
	tsBldr := array.NewTimestampBuilder(pool, arrow.FixedWidthTypes.Timestamp_ns.(*arrow.TimestampType))
	defer tsBldr.Release()
	tsBldr.Reserve(count)
	for i := 0; i < count; i++ {
		// -1 keeps every row strictly below as_of, so an as_of <= query still
		// includes the first row whatever comparison the server applies.
		tsBldr.Append(arrow.Timestamp(TemporalAsOfNanos) - arrow.Timestamp(int64(offset+i)) - 1) // #nosec G115 -- row counts are int
	}
	tsArr := tsBldr.NewArray()
	defer tsArr.Release()

	// 4. Build Geo Point
	geoBldr := array.NewFixedSizeListBuilder(pool, 2, arrow.PrimitiveTypes.Float64)
	defer geoBldr.Release()
	geoBldr.Reserve(count)
	geoValBldr := geoBldr.ValueBuilder().(*array.Float64Builder)
	geoValBldr.Reserve(count * 2)
	for i := 0; i < count; i++ {
		geoBldr.Append(true)
		geoValBldr.Append(40.7128 + rng.Float64()*0.1)  // #nosec G404 -- non-cryptographic use for benchmark data
		geoValBldr.Append(-74.0060 + rng.Float64()*0.1) // #nosec G404 -- non-cryptographic use for benchmark data
	}
	geoArr := geoBldr.NewArray()
	defer geoArr.Release()

	// 5. Build Active (Boolean)
	boolBldr := array.NewBooleanBuilder(pool)
	defer boolBldr.Release()
	boolBldr.Reserve(count)
	for i := 0; i < count; i++ {
		boolBldr.Append(rng.Float32() > 0.5) // #nosec G404
	}
	activeArr := boolBldr.NewArray()
	defer activeArr.Release()

	// 6. Build Category (String)
	strBldr := array.NewStringBuilder(pool)
	defer strBldr.Release()
	strBldr.Reserve(count)
	categories := []string{"electronics", "clothing", "home", "books"}
	for i := 0; i < count; i++ {
		strBldr.Append(categories[rng.Intn(len(categories))]) // #nosec G404
	}
	catArr := strBldr.NewArray()
	defer catArr.Release()

	// Build text content for AdvancedTextSearch: a category-derived sentence with
	// a selective SKU token, so the query below exercises the text index.
	textBldr := array.NewStringBuilder(pool)
	defer textBldr.Release()
	textBldr.Reserve(count)
	catStrs := catArr.(*array.String)
	for i := 0; i < count; i++ {
		cat := catStrs.Value(i)
		textBldr.Append(cat + " sku" + strconv.Itoa((offset+i)/2%1000) + " red widget")
	}
	textArr := textBldr.NewArray()
	defer textArr.Release()

	return array.NewRecordBatch(schema, []arrow.Array{idArr, vecArr, tsArr, geoArr, activeArr, catArr, textArr}, int64(count)), schema, nil
}

type generatedChunkOutput struct {
	index  int
	rec    arrow.RecordBatch
	schema *arrow.Schema
	err    error
}

func saveGeneratedCorpusToFiles(scale, dim int, outputArrow, outputFbin string, numChunks int, chunkChans []chan generatedChunkOutput, sem chan struct{}) {
	var genSchema *arrow.Schema
	preGenerated := make([]arrow.RecordBatch, 0, numChunks)

	for idx := 0; idx < numChunks; idx++ {
		out := <-chunkChans[idx]
		<-sem
		if out.err != nil {
			log.Fatalf("Pre-generation failed at chunk %d: %v", idx, out.err)
		}
		preGenerated = append(preGenerated, out.rec)
		if genSchema == nil {
			genSchema = out.schema
		}
	}

	if outputArrow != "" {
		log.Printf("[GEN] Saving %d vectors to Arrow IPC: %s\n", scale, outputArrow)
		f, err := os.Create(outputArrow) // #nosec G304 -- CLI operator flag
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		writer, err := ipc.NewFileWriter(f, ipc.WithSchema(genSchema))
		if err != nil {
			log.Fatalf("Failed to create IPC writer: %v", err)
		}
		for _, rec := range preGenerated {
			if err := writer.Write(rec); err != nil {
				log.Fatalf("Failed to write record: %v", err)
			}
		}
		if err := writer.Close(); err != nil {
			log.Printf("Warning: failed to close IPC writer: %v", err)
		}
		if err := f.Close(); err != nil {
			log.Printf("Warning: failed to close output file: %v", err)
		}
	} else {
		log.Printf("[GEN] Saving %d vectors to .fbin: %s\n", scale, outputFbin)
		f, err := os.Create(outputFbin) // #nosec G304 -- CLI operator flag
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		if _, err := safe.Int64ToUint32(int64(scale)); err != nil {
			log.Fatalf("scale %d out of uint32 range", scale)
		}
		if _, err := safe.Int64ToUint32(int64(dim)); err != nil {
			log.Fatalf("dim %d out of uint32 range", dim)
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(scale)); err != nil { // #nosec G115
			log.Fatalf("Failed to write .fbin header (count): %v", err)
		}
		if err := binary.Write(f, binary.LittleEndian, uint32(dim)); err != nil { // #nosec G115
			log.Fatalf("Failed to write .fbin header (dim): %v", err)
		}
		for _, rec := range preGenerated {
			vecArr := rec.Column(1).(*array.FixedSizeList)
			floatArr := vecArr.ListValues().(*array.Float32)
			if err := binary.Write(f, binary.LittleEndian, floatArr.Float32Values()); err != nil {
				log.Fatalf("Failed to write .fbin vector data: %v", err)
			}
		}
		if err := f.Close(); err != nil {
			log.Printf("Warning: failed to close output file: %v", err)
		}
	}

	for _, rec := range preGenerated {
		rec.Release()
	}
	log.Printf("[GEN] Saved %d vectors. Exiting.\n", scale)
}
