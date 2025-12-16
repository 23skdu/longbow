package store

import "sync"

// resultPool manages pools of []VectorID slices for common k values.
// This reduces allocations during search operations.
type resultPool struct {
pool10  sync.Pool // for k=10
pool50  sync.Pool // for k=50
pool100 sync.Pool // for k=100
}

// newResultPool creates a new result pool instance.
func newResultPool() *resultPool {
return &resultPool{
pool10: sync.Pool{
New: func() any {
s := make([]VectorID, 10)
return &s
},
},
pool50: sync.Pool{
New: func() any {
s := make([]VectorID, 50)
return &s
},
},
pool100: sync.Pool{
New: func() any {
s := make([]VectorID, 100)
return &s
},
},
}
}

// get retrieves a []VectorID slice of the specified length.
// For common k values (10, 50, 100), slices are pooled.
// For other values, a new slice is allocated.
func (p *resultPool) get(k int) []VectorID {
var slice []VectorID

switch {
case k == 10:
ptr := p.pool10.Get().(*[]VectorID)
slice = *ptr
case k == 50:
ptr := p.pool50.Get().(*[]VectorID)
slice = *ptr
case k == 100:
ptr := p.pool100.Get().(*[]VectorID)
slice = *ptr
case k <= 10:
// Use pool10 for smaller k, then reslice
ptr := p.pool10.Get().(*[]VectorID)
slice = (*ptr)[:k]
case k <= 50:
// Use pool50 for medium k, then reslice
ptr := p.pool50.Get().(*[]VectorID)
slice = (*ptr)[:k]
case k <= 100:
// Use pool100 for larger k, then reslice
ptr := p.pool100.Get().(*[]VectorID)
slice = (*ptr)[:k]
default:
// Allocate for k > 100
slice = make([]VectorID, k)
return slice
}

// Zero the slice to prevent data leaks
for i := range slice {
slice[i] = 0
}
return slice
}

// put returns a []VectorID slice to the appropriate pool.
func (p *resultPool) put(slice []VectorID) {
if slice == nil {
return
}

// Restore full capacity before returning
capacity := cap(slice)
slice = slice[:capacity]

switch capacity {
case 10:
p.pool10.Put(&slice)
case 50:
p.pool50.Put(&slice)
case 100:
p.pool100.Put(&slice)
// Non-standard capacities are not pooled, let GC handle them
}
}
