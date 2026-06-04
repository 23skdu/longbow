package tensor

import (
	"fmt"
	"unsafe"
)

// Shape describes the size of each axis in a tensor.
type Shape []int

// Strides describes the byte offset between consecutive elements along each axis.
type Strides []int

// Tensor is a multi-dimensional array with typed elements.
type Tensor struct {
	dtype  Dtype
	shape  Shape
	data   []byte
	labels []string // optional axis labels for Einstein notation
	offset int      // byte offset for zero-copy views
}

// New creates a new tensor with the given dtype and shape, backed by a freshly allocated buffer.
func New(dtype Dtype, shape Shape) *Tensor {
	if len(shape) == 0 {
		shape = Shape{1}
	}
	total := numElements(shape)
	if total < 0 {
		panic("tensor: shape overflow")
	}
	sz := total * dtype.Size()
	return &Tensor{
		dtype: dtype,
		shape: cloneShape(shape),
		data:  make([]byte, sz),
	}
}

// NewFromData creates a tensor wrapping existing data. The caller must ensure data's length
// matches the expected size (product(shape) * dtype.Size()).
func NewFromData(dtype Dtype, shape Shape, data []byte) *Tensor {
	if len(shape) == 0 {
		shape = Shape{1}
	}
	return &Tensor{
		dtype: dtype,
		shape: cloneShape(shape),
		data:  data,
	}
}

// Dtype returns the element data type.
func (t *Tensor) Dtype() Dtype { return t.dtype }

// Shape returns the shape of the tensor (aliases the internal slice; do not mutate).
func (t *Tensor) Shape() Shape { return t.shape }

// Rank returns the number of axes.
func (t *Tensor) Rank() int { return len(t.shape) }

// NumElements returns the total number of scalar elements.
func (t *Tensor) NumElements() int { return numElements(t.shape) }

// Data returns the backing byte slice.
func (t *Tensor) Data() []byte { return t.data }

// Labels returns optional axis labels. May be nil.
func (t *Tensor) Labels() []string { return t.labels }

// SetLabels sets axis labels for Einstein notation.
func (t *Tensor) SetLabels(labels []string) {
	if len(labels) != 0 && len(labels) != t.Rank() {
		panic(fmt.Sprintf("tensor: got %d labels for rank-%d tensor", len(labels), t.Rank()))
	}
	t.labels = labels
}

// Strides computes the byte strides for a contiguous row-major tensor.
func (t *Tensor) Strides() Strides {
	return computeStrides(t.shape, t.dtype.Size())
}

// IsContiguous returns true if the tensor is stored contiguously.
func (t *Tensor) IsContiguous() bool {
	return true
}

// Slice creates a zero-copy view selecting a single index along each specified axis.
// axes maps axis index -> index to select. Axes not present keep their full range.
// The returned tensor shares the backing data with the original when the resulting
// slice is contiguous; otherwise it copies.
// Example: Slice(map[int]int{0: 1}) on a 3×4 tensor selects row 1 → shape [4].
func (t *Tensor) Slice(axes map[int]int) *Tensor {
	if len(axes) == 0 {
		return t
	}
	for axis, idx := range axes {
		if axis < 0 || axis >= t.Rank() {
			panic(fmt.Sprintf("tensor: Slice: axis %d out of range [0,%d)", axis, t.Rank()))
		}
		if idx < 0 || idx >= t.shape[axis] {
			panic(fmt.Sprintf("tensor: Slice: axis %d index %d out of range [0,%d)", axis, idx, t.shape[axis]))
		}
	}
	// Compute byte offset and new shape
	stride := t.dtype.Size()
	byteOff := 0
	newShape := make(Shape, 0, t.Rank())
	for i, d := range t.shape {
		if idx, ok := axes[i]; ok {
			byteOff += idx * stride
			stride *= d
		} else {
			newShape = append(newShape, d)
		}
	}
	if len(newShape) == 0 {
		newShape = Shape{1}
	}
	// Determine if the result is contiguous: the kept axes must be the last n axes
	// (i.e., the contiguous suffix in row-major layout).
	// In our layout, the contiguous suffix is the set of axes that share the smallest
	// byte stride and are adjacent at the end. We compute this by checking that all
	// kept axes are after all removed axes.
	keptCount := len(newShape)
	isContiguousSuffix := true
	keptIdx := 0
	for i := 0; i < t.Rank(); i++ {
		if _, removing := axes[i]; removing {
			if keptIdx < keptCount {
				isContiguousSuffix = false
				break
			}
		} else {
			keptIdx++
		}
	}
	if isContiguousSuffix {
		sliceData := t.data[byteOff : byteOff+numElements(newShape)*t.dtype.Size()]
		out := &Tensor{
			dtype:  t.dtype,
			shape:  newShape,
			data:   sliceData,
			offset: 0,
		}
		if t.labels != nil {
			out.labels = make([]string, 0, keptCount)
			for i := range t.shape {
				if _, ok := axes[i]; !ok {
					out.labels = append(out.labels, t.labels[i])
				}
			}
		}
		return out
	}
	// Non-contiguous slice: copy data
	out := New(t.dtype, newShape)
	oStrides := computeStrides(newShape, t.dtype.Size())
	n := numElements(newShape)
	for i := 0; i < n; i++ {
		rem := i
		tIdx := make([]int, t.Rank())
		outIdx := make([]int, keptCount)
		for d := keptCount - 1; d >= 0; d-- {
			outIdx[d] = rem % newShape[d]
			rem /= newShape[d]
		}
		oi := 0
		for d := 0; d < t.Rank(); d++ {
			if idx, ok := axes[d]; ok {
				tIdx[d] = idx
			} else {
				tIdx[d] = outIdx[oi]
				oi++
			}
		}
		srcOff := t.byteOffset(tIdx)
		dstOff := offsetFromIndices(outIdx, oStrides, 4)
		switch t.dtype {
		case DtypeFloat32:
			*(*float32)(unsafe.Pointer(&out.data[dstOff])) = *(*float32)(unsafe.Pointer(&t.data[srcOff])) // #nosec G103
		case DtypeFloat64:
			*(*float64)(unsafe.Pointer(&out.data[dstOff])) = *(*float64)(unsafe.Pointer(&t.data[srcOff])) // #nosec G103
		}
	}
	return out
}

// byteOffset returns the byte offset into the data slice for the given indices.
func (t *Tensor) byteOffset(indices []int) int {
	strides := computeStrides(t.shape, t.dtype.Size())
	offset := 0
	for i, idx := range indices {
		offset += idx * strides[i]
	}
	return offset
}

func numElements(shape Shape) int {
	n := 1
	for _, d := range shape {
		n *= d
	}
	return n
}

func computeStrides(shape Shape, elemSize int) Strides {
	s := make(Strides, len(shape))
	if len(shape) == 0 {
		return s
	}
	s[len(shape)-1] = elemSize
	for i := len(shape) - 2; i >= 0; i-- {
		s[i] = s[i+1] * shape[i+1]
	}
	return s
}

func cloneShape(s Shape) Shape {
	out := make(Shape, len(s))
	copy(out, s)
	return out
}

// Clone creates a deep copy of the tensor.
func (t *Tensor) Clone() *Tensor {
	out := New(t.dtype, t.shape)
	copy(out.data, t.data)
	if t.labels != nil {
		out.labels = make([]string, len(t.labels))
		copy(out.labels, t.labels)
	}
	return out
}

// At returns a reference to the element at the given indices.
// The returned pointer is valid only as long as the tensor is alive.
func (t *Tensor) At(indices ...int) unsafe.Pointer {
	if len(indices) != t.Rank() {
		panic(fmt.Sprintf("tensor: At: got %d indices for rank-%d tensor", len(indices), t.Rank()))
	}
	byteOff := t.byteOffset(indices)
	return unsafe.Pointer(&t.data[byteOff]) // #nosec G103
}

// Float32s returns the data as a float32 slice. Panics if dtype is not Float32.
func (t *Tensor) Float32s() []float32 {
	if t.dtype != DtypeFloat32 {
		panic("tensor: Float32s called on non-float32 tensor")
	}
	if len(t.data) == 0 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&t.data[0])), len(t.data)/4) // #nosec G103
}

// Float64s returns the data as a float64 slice. Panics if dtype is not Float64.
func (t *Tensor) Float64s() []float64 {
	if t.dtype != DtypeFloat64 {
		panic("tensor: Float64s called on non-float64 tensor")
	}
	if len(t.data) == 0 {
		return nil
	}
	return unsafe.Slice((*float64)(unsafe.Pointer(&t.data[0])), len(t.data)/8) // #nosec G103
}

// Reshape returns a new view with the given shape if the total number of elements matches.
func (t *Tensor) Reshape(shape Shape) *Tensor {
	if numElements(shape) != t.NumElements() {
		panic("tensor: Reshape: element count mismatch")
	}
	out := &Tensor{
		dtype:  t.dtype,
		shape:  cloneShape(shape),
		data:   t.data,
		labels: t.labels,
	}
	return out
}
