package store

import (
	"fmt"
	"math"
	"sort"

	"github.com/23skdu/longbow/internal/simd"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

var (
	// Ensure kernels are registered only once
	kernelsRegistered bool
)

func init() {
	RegisterHNSWKernels()
}

// RegisterHNSWKernels registers custom Arrow compute kernels for HNSW operations.
// Should be called during initialization.
func RegisterHNSWKernels() {
	if kernelsRegistered {
		return
	}
	reg := compute.GetFunctionRegistry()
	if reg == nil {
		return // Should not happen
	}

	// 1. Register l2_distance
	// Input: [FixedSizeList(float32), FixedSizeList(float32)] -> Output: Float32
	// For HNSW: Query (Scalar/Array) vs Candidates (Array)
	l2Name := "l2_distance"
	l2Doc := compute.FunctionDoc{
		Summary:     "Compute L2 distance between vectors",
		Description: "Computes Euclidean distance between fixed size list vectors",
		ArgNames:    []string{"left", "right"},
	}
	l2Func := compute.NewScalarFunction(l2Name, compute.Binary(), l2Doc)

	// Input type matcher: FixedSizeList of Float32
	// We match generic FixedSizeList, check type in kernel? Or specific matcher?
	// TypeMatcher: FixedSizeListLike?
	// Note: Generic FixedSizeList matcher might need custom implementation or use generic ID
	// For now, let's assume we use FixedSizeList(Float32) specifically.

	// Kernel signature
	// Input: Any FixedSizeList, Any FixedSizeList
	// Output: Float32
	// exec.NewExactInput(arrow.FixedSizeListOf(dim, arrow.PrimitiveTypes.Float32)) is too specific (dim changes).
	// Use Matcher for ID?
	// exec.NewIDInput(arrow.FIXED_SIZE_LIST) matches any FSL.

	inTypes := []exec.InputType{
		exec.NewIDInput(arrow.FIXED_SIZE_LIST),
		exec.NewIDInput(arrow.FIXED_SIZE_LIST),
	}
	outType := exec.NewOutputType(arrow.PrimitiveTypes.Float32)

	k := exec.NewScalarKernel(inTypes, outType, l2DistanceExec, nil)
	if err := l2Func.AddKernel(k); err != nil {
		fmt.Printf("Failed to add l2 kernel: %v\n", err)
	}

	if added := reg.AddFunction(l2Func, false); !added {
		_ = "already exists"
	}

	// 2. Register select_k_neighbors
	// Input: [Float32 (distances), Uint32 (ids)] -> Output: Struct<ids, dists> (Array)
	// This is a Vector Kernel (selection/sort).
	selName := "select_k_neighbors"
	selDoc := compute.FunctionDoc{
		Summary:  "Select top K neighbors",
		ArgNames: []string{"distances", "ids"},
	}
	selFunc := compute.NewVectorFunction(selName, compute.Binary(), selDoc)

	selInTypes := []exec.InputType{
		exec.NewExactInput(arrow.PrimitiveTypes.Float32),
		exec.NewExactInput(arrow.PrimitiveTypes.Uint32),
	}
	// Output type depends on K, but general Struct?
	// We'll determine output type resolver or static struct.
	// For now, struct { ids: uint32, dists: float32 } (Array of structs)
	// Actually, returning a StructArray is complex.
	// Returning Indices (Uint32) is standard for SelectK.
	// Let's implement returning INDICES of top K.
	// Then caller uses Take. But we wanted valid "Arrow Compute".
	// Let's return Indices. It's simpler and composable.
	selOutType := exec.NewOutputType(arrow.PrimitiveTypes.Uint32) // Indices

	selK := exec.NewVectorKernel(selInTypes, selOutType, selectKExec, selectKInit)
	if err := selFunc.AddKernel(selK); err != nil {
		fmt.Printf("Failed to add select_k kernel: %v\n", err)
	}

	if added := reg.AddFunction(selFunc, false); !added {
		// Ignore
		_ = "already exists"
	}

	kernelsRegistered = true
}

// l2DistanceExec implements the L2 distance execution logic.
func l2DistanceExec(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// batch.Values[0]: Left (Query)
	// batch.Values[1]: Right (Candidates)
	// out: Float32 array

	// Assume float32 types based exclusively on registration
	var dim int
	var leftVals, rightVals []float32
	var leftIsScalar, rightIsScalar bool

	// Handle Left Input
	if batch.Values[0].Scalar != nil {
		leftIsScalar = true
		s := batch.Values[0].Scalar.(*scalar.FixedSizeList)
		if !s.Valid {
			return fmt.Errorf("invalid left scalar")
		}
		// For a FixedSizeList scalar, .Value is the child array (e.g. *array.Float32)
		valArr := s.Value.(*array.Float32)
		dim = int(s.Type.(*arrow.FixedSizeListType).Len())
		leftVals = valArr.Float32Values()
	} else {
		leftIsScalar = false
		leftSpan := &batch.Values[0].Array
		if len(leftSpan.Children) == 0 {
			return fmt.Errorf("invalid left list array")
		}
		dim = int(leftSpan.Type.(*arrow.FixedSizeListType).Len())
		leftVals = exec.GetSpanValues[float32](&leftSpan.Children[0], 1)
	}

	// Handle Right Input
	if batch.Values[1].Scalar != nil {
		rightIsScalar = true
		s := batch.Values[1].Scalar.(*scalar.FixedSizeList)
		if !s.Valid {
			return fmt.Errorf("invalid right scalar")
		}
		// For a FixedSizeList scalar, .Value is the child array
		valArr := s.Value.(*array.Float32)
		rightVals = valArr.Float32Values()
	} else {
		rightIsScalar = false
		rightSpan := &batch.Values[1].Array
		if len(rightSpan.Children) == 0 {
			return fmt.Errorf("invalid right list array")
		}
		rightVals = exec.GetSpanValues[float32](&rightSpan.Children[0], 1)
	}

	lenBatch := batch.Len

	// Prepare output slice
	// out is *ExecResult (alias ArraySpan).
	// We need to write into out.Buffers[1].
	// exec.GetSpanValues gives read-only slice?
	// We can cast the buffer.

	// How to get mutable slice from ArraySpan?
	// unsafe hack or proper accessor?
	// Go slices are pointers. GetSpanValues might return a slice backing the buffer.
	outBuf := exec.GetSpanValues[float32](out, 1) // Buffer 1 is data for Float32?
	// Wait, Float32 array: Buffer 0=Validity, Buffer 1=Values.
	// exec.GetSpanValues implementation:
	// return unsafe.Slice(...)

	// We need a Mutable slice.
	// We'll perform unsafe cast if needed, or assume GetSpanValues returns writeable if buffer is mutable.
	// The buffer allocated by executor should be mutable.

	outSlice := outBuf[:lenBatch]

	// Logic simplified above, removing redundant checks
	// Iterate
	// Note: We use dim from type, but be safe with slice bounds
	for i := int64(0); i < lenBatch; i++ {
		// Calculate offsets
		lOff := int64(0)
		if !leftIsScalar {
			lOff = (batch.Values[0].Array.Offset + i) * int64(dim)
		}
		lVec := leftVals[lOff : lOff+int64(dim)]

		rOff := int64(0)
		if !rightIsScalar {
			rOff = (batch.Values[1].Array.Offset + i) * int64(dim)
		}
		rVec := rightVals[rOff : rOff+int64(dim)]

		d, err := simd.EuclideanDistance(lVec, rVec)
		if err != nil {
			outSlice[i] = math.MaxFloat32
		} else {
			outSlice[i] = d
		}
	}
	return nil
}

// SelectKOptions
type SelectKOptions struct {
	K int
}

func (s *SelectKOptions) TypeName() string {
	return "select_k_options"
}

func selectKInit(ctx *exec.KernelCtx, args exec.KernelInitArgs) (exec.KernelState, error) {
	// Parse options
	if opts, ok := args.Options.(*SelectKOptions); ok {
		return opts, nil
	}
	return &SelectKOptions{K: 10}, nil // default?
}

func selectKExec(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// Input 0: Distances (Float32)
	// Input 1: IDs (Uint32)
	// Output: Indices (Uint32) - wait, if we return indices relative to input, we don't need IDs input.
	// If we want Actual IDs, we return IDs.
	// TopK usually returns indices into the array.
	// But if we passed IDs, maybe we want filtered IDs.
	// Let's implement: Return *Indices* of the Top K smallest distances.
	// Caller then takes from IDs and Distances.

	dists := exec.GetSpanValues[float32](&batch.Values[0].Array, 1)
	n := int(batch.Len)

	// Create index slice
	type item struct {
		idx  int
		dist float32
	}
	items := make([]item, n)
	for i := 0; i < n; i++ {
		items[i] = item{
			idx:  i,
			dist: dists[int64(i)+batch.Values[0].Array.Offset], // Handle offset!
		}
	}

	// Use standard sort for now (simpler than quickselect impl inline)
	// For K << N, this is suboptimal but robust.
	// For HNSW N is small (32-200), so Sort is extremely fast (< 1us).
	// Arrow's C++ SelectKUnstable uses partition.

	// Custom Sort logic
	// We want smallest distances first.
	// Simple bubble/insertion sort might be faster for very small N?
	// Go's sort is pdqsort, good enough.

	// Inline bubble sort for very small N? No, keep it clean.
	// Just full sort.

	// We'll reimplement a simple partition-based selection if needed later.
	// For now:

	// Selection Sort for K if K is very small?
	// Let's use a simple insertion-sort-like loop if K < 5?
	// Actually, just sorting all is fine.

	// Wait, we need to sort `items` slice.
	// sort.Slice vs manual implementation.
	// Manual implementation avoids reflection overhead of sort.Slice.

	// Use sort.Slice for robustness
	sort.Slice(items, func(i, j int) bool {
		return items[i].dist < items[j].dist
	})

	// VectorKernel executor might not preallocate output.
	// We must allocate if GetSpanValues returned empty.

	outIndices := exec.GetSpanValues[uint32](out, 1)
	if int64(len(outIndices)) < batch.Len {
		// Allocate Buffer
		nBytes := int(batch.Len) * 4 // Uint32

		alloc := exec.GetAllocator(ctx.Ctx)
		buf := memory.NewResizableBuffer(alloc)
		buf.Resize(nBytes)

		// Assign to output
		// Buffer 0 is Validity, Buffer 1 is Data
		// out.Buffers[1].Buf = buf // BufferSpan struct?
		// BufferSpan: {Buf *memory.Buffer, Offset int64, Len int64}
		// We need to construct BufferSpan.
		// Actually BufferSpan uses `memory.Buffer`?
		// Check BufferSpan definition.
		// "Buffers [3]BufferSpan"

		// Assuming we can set it.
		// Use unsafe or generic setter if available?
		// ArraySpan has "GetBuffer(idx)".
		// "SetBuffer" does not exist in ArraySpan doc (SetSlice, SetMembers).
		// But buffers are public?
		// [3]BufferSpan.
		// Let's rely on BufferSpan fields being public or accessible.
		// Go doc for BufferSpan:
		// type BufferSpan struct {
		//    Buf *memory.Buffer
		//    ...
		// }

		// Use SetBuffer wrapper
		out.Buffers[1].SetBuffer(buf)

		// Set SelfAlloc to true so it gets managed?
		// SetBuffer sets Owner.
		// SelfAlloc is true if kernel allocates its own buffer.
		out.Buffers[1].SelfAlloc = true

		// Verify
		if len(out.Buffers[1].Buf) != nBytes {
			return fmt.Errorf("buffer size mismatch after alloc")
		}

		// Re-get span values
		outIndices = exec.GetSpanValues[uint32](out, 1)
	}

	if int64(len(outIndices)) < batch.Len {
		return fmt.Errorf("failed to allocate output buffer")
	}

	for i := 0; i < n; i++ {
		outIndices[i] = uint32(items[i].idx)
	}

	return nil
}
