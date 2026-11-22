package safe

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUint32ToInt32 runs the test uint32 to int32 operation.
func TestUint32ToInt32(t *testing.T) {
	tests := []struct {
		name    string
		input   uint32
		want    int32
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{"one", 1, 1, false},
		{"max int32", math.MaxInt32, math.MaxInt32, false},
		{"overflow", math.MaxInt32 + 1, 0, true},
		{"max uint32", math.MaxUint32, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Uint32ToInt32(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestIntToInt32 runs the test int to int32 operation.
func TestIntToInt32(t *testing.T) {
	tests := []struct {
		name    string
		input   int
		want    int32
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{"one", 1, 1, false},
		{"max int32", math.MaxInt32, math.MaxInt32, false},
		{"min int32", math.MinInt32, math.MinInt32, false},
	}
	if math.MaxInt > math.MaxInt32 {
		max32 := int64(math.MaxInt32)
		min32 := int64(math.MinInt32)
		tests = append(tests,
			struct {
				name    string
				input   int
				want    int32
				wantErr bool
			}{"overflow positive", int(max32 + 1), 0, true},
			struct {
				name    string
				input   int
				want    int32
				wantErr bool
			}{"overflow negative", int(min32 - 1), 0, true},
		)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IntToInt32(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestUint32ToInt runs the test uint32 to int operation.
func TestUint32ToInt(t *testing.T) {
	assert.Equal(t, 0, Uint32ToInt(0))
	assert.Equal(t, 42, Uint32ToInt(42))
	maxU32 := uint32(math.MaxUint32)
	assert.Equal(t, int(maxU32), Uint32ToInt(maxU32))
}

// TestInt64ToUint32 runs the test int64 to uint32 operation.
func TestInt64ToUint32(t *testing.T) {
	tests := []struct {
		name    string
		input   int64
		want    uint32
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{"one", 1, 1, false},
		{"max uint32", math.MaxUint32, math.MaxUint32, false},
		{"negative", -1, 0, true},
		{"overflow", math.MaxUint32 + 1, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Int64ToUint32(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
