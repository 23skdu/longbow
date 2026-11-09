package cluster

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// FuzzConsistencyLevelRoundTrip verifies ParseConsistencyLevel/ConsistencyLevel.String:
// parsing succeeds exactly for the documented names (case-insensitive), a
// successfully parsed level always round-trips through String(), and String()
// of an arbitrary level value only ever renders the documented names or
// "UNKNOWN".
func FuzzConsistencyLevelRoundTrip(f *testing.F) {
	f.Add("one", 0)
	f.Add("QUORUM", 1)
	f.Add("all", 2)
	f.Add("One", 3)
	f.Add("bogus", -1)
	f.Add("", 7)
	f.Add("ONE", 1000)

	f.Fuzz(func(t *testing.T, s string, raw int) {
		if len(s) > 256 {
			return
		}

		level, err := ParseConsistencyLevel(s)
		if err == nil {
			switch level {
			case ConsistencyOne, ConsistencyQuorum, ConsistencyAll:
			default:
				t.Fatalf("ParseConsistencyLevel(%q) = %d, want one of the three levels", s, level)
			}
			back, err2 := ParseConsistencyLevel(level.String())
			if err2 != nil || back != level {
				t.Fatalf("round trip of %d via String()=%q failed: (%d, %v)",
					level, level.String(), back, err2)
			}
		} else {
			upper := strings.ToUpper(s)
			if upper == "ONE" || upper == "QUORUM" || upper == "ALL" {
				t.Fatalf("ParseConsistencyLevel(%q) rejected a valid name: %v", s, err)
			}
		}

		cl := ConsistencyLevel(raw)
		str := cl.String()
		switch str {
		case "ONE", "QUORUM", "ALL":
			back, err3 := ParseConsistencyLevel(str)
			if err3 != nil || back != cl {
				t.Fatalf("String/Parse round trip failed for %d: %q -> (%d, %v)", raw, str, back, err3)
			}
		case "UNKNOWN":
			if raw == 0 || raw == 1 || raw == 2 {
				t.Fatalf("valid level %d rendered as UNKNOWN", raw)
			}
		default:
			t.Fatalf("String() = %q for level %d, want documented name or UNKNOWN", str, raw)
		}
	})
}

// FuzzQuorumCalculatorBounds verifies quorum arithmetic: the required node
// count always lies in [0, totalNodes], matches the documented formula for
// each consistency level, and IsSatisfied flips exactly at the boundary.
func FuzzQuorumCalculatorBounds(f *testing.F) {
	f.Add(0, 0)
	f.Add(1, 2)
	f.Add(5, 1)
	f.Add(4, 1)
	f.Add(10, 2)
	f.Add(3, 7)
	f.Add(-5, 1)
	f.Add(1000001, 1)

	f.Fuzz(func(t *testing.T, totalNodes, levelRaw int) {
		if totalNodes < 0 || totalNodes > 1_000_000 {
			return
		}

		qc := NewQuorumCalculator()
		level := ConsistencyLevel(levelRaw)
		required := qc.RequiredNodes(totalNodes, level)

		if required < 0 || required > totalNodes {
			t.Fatalf("RequiredNodes(%d, %d) = %d, outside [0, %d]",
				totalNodes, levelRaw, required, totalNodes)
		}

		var want int
		switch {
		case totalNodes <= 0:
			want = 0
		case level == ConsistencyOne:
			want = 1
		case level == ConsistencyQuorum:
			want = totalNodes/2 + 1
		case level == ConsistencyAll:
			want = totalNodes
		default:
			want = 1 // unknown levels fall back to "one acknowledgement"
		}
		if required != want {
			t.Fatalf("RequiredNodes(%d, %d) = %d, want %d", totalNodes, levelRaw, required, want)
		}

		if !qc.IsSatisfied(required, totalNodes, level) {
			t.Fatalf("IsSatisfied(%d, %d, %d) = false, want true at the boundary",
				required, totalNodes, levelRaw)
		}
		if qc.IsSatisfied(required-1, totalNodes, level) {
			t.Fatalf("IsSatisfied(%d, %d, %d) = true, want false one below the boundary",
				required-1, totalNodes, levelRaw)
		}
	})
}

// FuzzZeroCopyRoundTrip verifies the Arrow replication codec: a RecordBatch
// built from fuzzed values must survive MarshalZeroCopy/UnmarshalZeroCopy
// with schema, row count, null positions and every cell value preserved.
func FuzzZeroCopyRoundTrip(f *testing.F) {
	f.Add([]byte("payload"), int64(3), byte(0))
	f.Add([]byte(""), int64(1), byte(1))
	f.Add([]byte{0x00, 0xff, 0xaa}, int64(2), byte(1))
	f.Add([]byte("abcdefgh"), int64(0), byte(0))
	f.Add(make([]byte, 64), int64(8), byte(1))

	f.Fuzz(func(t *testing.T, payload []byte, rows int64, flags byte) {
		if len(payload) > 256 {
			return
		}
		if rows < 0 || rows > 64 {
			return
		}
		nulls := flags&1 != 0
		// An all-null column set is out of scope (exercises no data buffers).
		if nulls && rows == 1 {
			return
		}

		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "data", Type: arrow.BinaryTypes.Binary},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)

		b := array.NewRecordBuilder(mem, schema)
		defer b.Release()

		idb := b.Field(0).(*array.Int64Builder)
		binb := b.Field(1).(*array.BinaryBuilder)
		strb := b.Field(2).(*array.StringBuilder)

		for i := int64(0); i < rows; i++ {
			if nulls && i%2 == 0 {
				idb.AppendNull()
				binb.AppendNull()
				strb.AppendNull()
				continue
			}
			idb.Append(i*3 + 1)
			binb.Append(payload)
			strb.Append(string(payload))
		}

		rec := b.NewRecordBatch()
		defer rec.Release()

		marshaled, err := MarshalZeroCopy("fuzz-dataset", rec)
		if err != nil {
			t.Fatalf("MarshalZeroCopy: %v", err)
		}
		if marshaled.RowCount != rec.NumRows() {
			t.Fatalf("payload RowCount = %d, want %d", marshaled.RowCount, rec.NumRows())
		}
		if marshaled.DatasetName != "fuzz-dataset" {
			t.Fatalf("payload DatasetName = %q", marshaled.DatasetName)
		}

		rec2, err := UnmarshalZeroCopy(marshaled, mem)
		if err != nil {
			t.Fatalf("UnmarshalZeroCopy of a valid payload failed: %v", err)
		}
		defer rec2.Release()

		if rec2.NumRows() != rec.NumRows() {
			t.Fatalf("row count %d, want %d", rec2.NumRows(), rec.NumRows())
		}
		if !rec.Schema().Equal(rec2.Schema()) {
			t.Fatalf("schema mismatch after round trip:\n%v\nvs\n%v", rec.Schema(), rec2.Schema())
		}

		ids1 := rec.Column(0).(*array.Int64)
		ids2 := rec2.Column(0).(*array.Int64)
		bins1 := rec.Column(1).(*array.Binary)
		bins2 := rec2.Column(1).(*array.Binary)
		strs1 := rec.Column(2).(*array.String)
		strs2 := rec2.Column(2).(*array.String)

		for i := int64(0); i < rows; i++ {
			if ids1.IsNull(int(i)) != ids2.IsNull(int(i)) {
				t.Fatalf("row %d: null position changed", i)
			}
			if ids1.IsNull(int(i)) {
				continue
			}
			if ids1.Value(int(i)) != ids2.Value(int(i)) {
				t.Fatalf("row %d: id %d, want %d", i, ids2.Value(int(i)), ids1.Value(int(i)))
			}
			if got, want := bins2.Value(int(i)), bins1.Value(int(i)); string(got) != string(want) {
				t.Fatalf("row %d: data %q, want %q", i, got, want)
			}
			if got, want := strs2.Value(int(i)), strs1.Value(int(i)); got != want {
				t.Fatalf("row %d: name %q, want %q", i, got, want)
			}
		}
	})
}
