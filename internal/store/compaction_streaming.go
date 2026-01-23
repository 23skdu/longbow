package store

import (
	qry "github.com/23skdu/longbow/internal/query"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// StreamingCompactor performs compaction by pre-calculating sizes and writing directly
// to allocated buffers, avoiding RecordBuilder resize churn.
type StreamingCompactor struct {
	pool memory.Allocator
}

func NewStreamingCompactor(pool memory.Allocator) *StreamingCompactor {
	return &StreamingCompactor{pool: pool}
}

// Compact merges batches while filtering deleted rows (tombstones).
func (sc *StreamingCompactor) Compact(schema *arrow.Schema, batches []arrow.RecordBatch, tombstones []*qry.Bitset) (batch arrow.RecordBatch, mapping [][]int, removed int64, err error) {
	// 1. Calculate total output rows and pre-validate
	totalOutRows := int64(0)
	mapping = make([][]int, len(batches))

	// Pre-calculate mappings to avoid doing it during copy
	// This gives us exact output size
	currentOffset := 0
	removedCount := int64(0)

	for i, b := range batches {
		rows := int(b.NumRows())
		mapping[i] = make([]int, rows)
		tomb := tombstones[i] // can be nil

		for r := 0; r < rows; r++ {
			if tomb != nil && tomb.Contains(r) {
				mapping[i][r] = -1
				removedCount++
			} else {
				mapping[i][r] = currentOffset
				currentOffset++
				totalOutRows++
			}
		}
	}

	if totalOutRows == 0 {
		// Return empty batch
		cols := make([]arrow.Array, len(schema.Fields()))
		for i := range cols {
			// standard empty array creation
			b := array.NewBuilder(sc.pool, schema.Field(i).Type)
			cols[i] = b.NewArray()
			b.Release()
		}
		return array.NewRecordBatch(schema, cols, 0), mapping, removedCount, nil
	}

	// 2. Allocate and Fill Columns
	outCols := make([]arrow.Array, len(schema.Fields()))

	for colIdx, field := range schema.Fields() {
		// Gather input arrays for this column
		inArrays := make([]arrow.Array, len(batches))
		for i, b := range batches {
			if colIdx < int(b.NumCols()) {
				inArrays[i] = b.Column(colIdx)
			} else {
				// Handle missing column (schema evolution)?
				// For now assume nil/null if missing, but we need an array.
				// This simplifies if we assume schema match.
				// If missing, we'd need to structurally fill nulls.
				// Let's defer complex evolution handling and assume mostly matching or compatible.
				// If not found, we treat as all-null.
				inArrays[i] = nil
			}
		}

		var err error
		outCols[colIdx], err = sc.compactColumn(field.Type, inArrays, mapping, int(totalOutRows))
		if err != nil {
			// Cleanup
			for j := 0; j < colIdx; j++ {
				outCols[j].Release()
			}
			return nil, nil, 0, err
		}
	}

	return array.NewRecordBatch(schema, outCols, totalOutRows), mapping, removedCount, nil
}

func (sc *StreamingCompactor) compactColumn(dt arrow.DataType, inputs []arrow.Array, mapping [][]int, totalRows int) (arrow.Array, error) {
	// Handle types
	switch dt := dt.(type) {
	case *arrow.Float32Type:
		return sc.compactFixedSize(dt, inputs, mapping, totalRows, 4)
	case *arrow.Int64Type:
		return sc.compactFixedSize(dt, inputs, mapping, totalRows, 8)
	case *arrow.Int32Type:
		return sc.compactFixedSize(dt, inputs, mapping, totalRows, 4)
	case *arrow.FixedSizeListType: // e.g. Vectors
		// Flatten inputs? No, FixedSizeList physical layout is one big data buffer.
		// We can treat it as one "Value" buffer of size (totalRows * stride).
		// But we need to handle the validity bitmap correctly.
		// Note: recurse? Or custom handler?
		// FixedSizeList is essentially a container. We need to compact the CHILD array.
		// But the child array has (rows * listSize) elements.
		// We can just construct the child array and wrap it.
		return sc.compactFixedSizeList(dt, inputs, mapping, totalRows)

	default:
		// Fallback to builder for unsupported types to be safe
		return sc.fallbackCompact(dt, inputs, mapping)
	}
}

func (sc *StreamingCompactor) compactFixedSize(dt arrow.DataType, inputs []arrow.Array, mapping [][]int, totalRows, itemSize int) (arrow.Array, error) {
	// 1. Allocate Buffers
	// Validity Bitmap + Values Buffer
	validityBytes := int(bitutil.BytesForBits(int64(totalRows)))
	dataBytes := totalRows * itemSize

	validityBuf := memory.NewResizableBuffer(sc.pool)
	validityBuf.Resize(validityBytes)
	memory.Set(validityBuf.Bytes(), 0) // Initialize to 0 (all null)? Or 0xFF?
	// We will bit-set valid rows.

	dataBuf := memory.NewResizableBuffer(sc.pool)
	dataBuf.Resize(dataBytes)

	// 2. Copy Loop
	outData := dataBuf.Bytes()
	outValid := validityBuf.Bytes()

	// To avoid lots of tight loop type assertions, we might want to get raw bytes from inputs
	// But inputs are arrow.Array.
	// For FixedWidth types, we can access the underlying buffers.

	nullCount := 0

	for i, arr := range inputs {
		if arr == nil {
			// All nulls for this batch (missing col)
			// Validity map is already 0 initialized (assuming null) so just skip
			// Wait, is 0 null? Yes, 0 bit means null.
			// Just ensure we advance stats if needed?
			// The mapping handles the index.
			// Actually, if we initialized to 0, these are nulls.
			// We just need to ensure we count them properly if we track nullCount.
			count := 0
			for _, destIdx := range mapping[i] {
				if destIdx != -1 {
					count++
				}
			}
			nullCount += count
			continue
		}

		// Get raw input buffers
		// Data buffer is usually at index 1 for simple types

		// Fast path: Copy bytes
		inData := arr.Data().Buffers()[1].Bytes()
		// Validity buffer might be nil if no nulls
		var inValid []byte
		if arr.Data().Buffers()[0] != nil {
			inValid = arr.Data().Buffers()[0].Bytes()
		}

		// We iterate mapping
		// Optimization: if mapping is contiguous (no tombstones), we can `copy` a chunk
		// For verification, let's do row-by-row but optimize access

		// TODO: Optimized block copy
		rows := arr.Len()
		offset := arr.Data().Offset() // Start offset in input buffer

		for r := 0; r < rows; r++ {
			destIdx := mapping[i][r]
			if destIdx == -1 {
				continue
			}

			// Check null
			isNull := false
			if inValid != nil {
				// bitutil check
				if (inValid[(offset+r)/8] & (1 << uint((offset+r)%8))) == 0 {
					isNull = true
				}
			}
			// If inValid is nil, it's not null (valid)

			if isNull {
				nullCount++
				// bit is already 0
			} else {
				// Set valid bit
				outValid[destIdx/8] |= (1 << uint(destIdx%8))

				// Copy data
				srcPos := (offset + r) * itemSize
				dstPos := destIdx * itemSize

				// Perform copy
				copy(outData[dstPos:dstPos+itemSize], inData[srcPos:srcPos+itemSize])
			}
		}
	}

	// 3. Construct Array
	data := array.NewData(dt, totalRows, []*memory.Buffer{validityBuf, dataBuf}, nil, nullCount, 0)
	defer data.Release()

	// Create specific array type
	return array.MakeFromData(data), nil
}

func (sc *StreamingCompactor) compactFixedSizeList(dt *arrow.FixedSizeListType, inputs []arrow.Array, mapping [][]int, totalRows int) (arrow.Array, error) {
	// Similar to above, but we delegate value copying to the inner type
	// But FixedSizeList structure is: ValidityBitmap (Buffer 0) -> [Values (Child Array)]
	// It does NOT have a data buffer itself.

	// We need to construct the VALIDITY bitmap for the list itself (validity of the vectors).
	// And we need to construct the compacted CHILD array.

	// 1. Compact Validity Bitmap
	validityBytes := int(bitutil.BytesForBits(int64(totalRows)))
	validityBuf := memory.NewResizableBuffer(sc.pool)
	validityBuf.Resize(validityBytes)
	memory.Set(validityBuf.Bytes(), 0)

	outValid := validityBuf.Bytes()
	nullCount := 0

	// 2. Prepare Child Compaction
	// We need to transform the row mapping: row R in batch I corresponds to range [R*N, (R+1)*N] in child array
	// Actually, we can just treat the child array as a big flat array and generate a new mapping for IT.

	listSize := int(dt.Len())
	childMapping := make([][]int, len(mapping))

	childInputs := make([]arrow.Array, len(inputs))

	for i, arr := range inputs {
		if arr == nil {
			childInputs[i] = nil // Handling nil batch?
			// If parent is nil, child rows don't matter?
			// But we need to conform to layout.
			// We can generate FAKE nulls for the child if needed, or just let them be garbage/zero since parent is null.
			// Safest is to have valid child rows (e.g. zeros) for the null slots.
			continue
		}

		fsl := arr.(*array.FixedSizeList)
		childInputs[i] = fsl.ListValues()

		// Map metadata for parent validity
		// Also build child mapping
		rows := arr.Len()
		offset := arr.Data().Offset() // List offset

		childMap := make([]int, rows*listSize)

		// Parent validity check
		var inValid []byte
		if arr.Data().Buffers()[0] != nil {
			inValid = arr.Data().Buffers()[0].Bytes()
		}

		for r := 0; r < rows; r++ {
			destIdx := mapping[i][r]
			if destIdx == -1 {
				// Mark all child elements as dropped
				for k := 0; k < listSize; k++ {
					childMap[r*listSize+k] = -1
				}
				continue
			}

			// Handle parent validity
			isNull := false
			if inValid != nil {
				if (inValid[(offset+r)/8] & (1 << uint((offset+r)%8))) == 0 {
					isNull = true
				}
			}

			if isNull {
				nullCount++
			} else {
				bitutil.SetBit(outValid, destIdx)
			}

			// Map child elements
			// Destination start for this vector: destIdx * listSize
			dstBase := destIdx * listSize
			for k := 0; k < listSize; k++ {
				// Source logical index implied by iteration?
				// No, childInputs are flattened.
				// Child input I corresponds to Parent I.
				// Parent I row R maps to Child I row (ParentOffset + R) * listSize + k?
				// Wait, FSL `ListValues()` returns a slice of the child array that corresponds to the FSL slice.
				// The FSL itself might have an offset.
				// `fsl.ListValues()` usually returns the full underlying array window?
				// Using `ListValues()` returns a slice spanning the range.
				// So if we iterate 0..rows of the FSL, we are iterating 0..rows*listSize of the child slice.
				// So the child mapping is simply local:
				childMap[r*listSize+k] = dstBase + k
			}
		}
		childMapping[i] = childMap
	}

	// 3. Recurse for Child
	// Total child rows = totalRows * listSize
	childArr, err := sc.compactColumn(dt.Elem(), childInputs, childMapping, totalRows*listSize)
	if err != nil {
		validityBuf.Release()
		return nil, err
	}

	// 4. Build FSL
	// MakeFromData handles the wrapper creation
	data := array.NewData(dt, totalRows, []*memory.Buffer{validityBuf}, []arrow.ArrayData{childArr.Data()}, nullCount, 0)
	defer data.Release()

	return array.MakeFromData(data), nil
}

func (sc *StreamingCompactor) fallbackCompact(dt arrow.DataType, inputs []arrow.Array, mapping [][]int) (arrow.Array, error) {
	// Use builder
	b := array.NewBuilder(sc.pool, dt)
	defer b.Release()

	for i, arr := range inputs {
		if arr == nil {
			// append nulls
			count := 0
			for _, m := range mapping[i] {
				if m != -1 {
					count++
				}
			}
			for k := 0; k < count; k++ {
				b.AppendNull()
			}
			continue
		}

		for r := 0; r < arr.Len(); r++ {
			if mapping[i][r] != -1 {
				// Low efficiency but generic
				appendValue(b, arr, r)
			}
		}
	}
	return b.NewArray(), nil
}
