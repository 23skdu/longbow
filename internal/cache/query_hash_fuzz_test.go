package cache

import (
	"math"
	"testing"

	qry "github.com/23skdu/longbow/internal/query"
)

// hashQueryInput is a deterministic byte consumer used to build a
// VectorSearchRequest entirely from fuzz input.
type hashQueryInput struct {
	rest []byte
}

func (in *hashQueryInput) need(n int) []byte {
	if len(in.rest) < n {
		b := in.rest
		in.rest = nil
		return b
	}
	b := in.rest[:n]
	in.rest = in.rest[n:]
	return b
}

func (in *hashQueryInput) u32() uint32 {
	b := in.need(4)
	var v uint32
	for i := 0; i < len(b); i++ {
		v |= uint32(b[i]) << (8 * i)
	}
	return v
}

// filters builds a bounded nested filter tree (depth <= 3, <= 3 filters per
// level), consuming bytes from the input and terminating when input runs out.
func (in *hashQueryInput) filters(depth int) []qry.Filter {
	if depth > 2 {
		return nil
	}
	var fs []qry.Filter
	for len(fs) < 3 {
		ctrl := in.need(1)
		if len(ctrl) == 0 || ctrl[0]%3 == 0 {
			break
		}
		flt := qry.Filter{
			Field:    string(in.need(8)),
			Operator: string(in.need(4)),
			Value:    string(in.need(12)),
			Logic:    string(in.need(4)),
		}
		if ctrl[0]%2 == 1 {
			flt.Filters = in.filters(depth + 1)
		}
		fs = append(fs, flt)
	}
	return fs
}

func buildHashQueryRequest(data []byte) *qry.VectorSearchRequest {
	in := &hashQueryInput{rest: data}

	req := &qry.VectorSearchRequest{}
	req.Dataset = string(in.need(16))
	req.TextQuery = string(in.need(24))
	req.K = int(int32(in.u32()))
	req.Alpha = math.Float32frombits(in.u32())
	req.GraphAlpha = math.Float32frombits(in.u32())
	if local := in.need(1); len(local) > 0 {
		req.LocalOnly = local[0]&1 == 1
	}
	for i := 0; i < 32 && len(in.rest) >= 4; i++ {
		req.Vector = append(req.Vector, math.Float32frombits(in.u32()))
	}
	req.Filters = in.filters(0)
	return req
}

// FuzzHashQuery checks that HashQuery is a deterministic cache key: the same
// request always hashes identically, and every field that affects search
// results changes the hash when mutated.
func FuzzHashQuery(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("seed-one"))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add(make([]byte, 256))
	f.Add([]byte("nested-filter-seed-data-for-parsing"))
	f.Add(make([]byte, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}

		req := buildHashQueryRequest(data)
		h1 := HashQuery(req)
		h2 := HashQuery(req)
		if h1 != h2 {
			t.Fatalf("HashQuery not deterministic: %d != %d", h1, h2)
		}

		// Sensitivity: each result-affecting field must change the key.
		mut := *req
		mut.Dataset = req.Dataset + "!"
		if got := HashQuery(&mut); got == h1 {
			t.Fatalf("mutating Dataset did not change the hash (base %d)", h1)
		}

		mut = *req
		mut.K = req.K + 1
		if got := HashQuery(&mut); got == h1 {
			t.Fatalf("mutating K did not change the hash (base %d)", h1)
		}

		mut = *req
		mut.LocalOnly = !req.LocalOnly
		if got := HashQuery(&mut); got == h1 {
			t.Fatalf("mutating LocalOnly did not change the hash (base %d)", h1)
		}

		mut = *req
		mut.TextQuery = req.TextQuery + "!"
		if got := HashQuery(&mut); got == h1 {
			t.Fatalf("mutating TextQuery did not change the hash (base %d)", h1)
		}
	})
}
