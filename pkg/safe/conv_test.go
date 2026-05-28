package safe

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		{"overflow positive", math.MaxInt32 + 1, 0, true},
		{"overflow negative", math.MinInt32 - 1, 0, true},
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

func TestUint32ToInt(t *testing.T) {
	assert.Equal(t, 0, Uint32ToInt(0))
	assert.Equal(t, 42, Uint32ToInt(42))
	assert.Equal(t, int(math.MaxUint32), Uint32ToInt(math.MaxUint32))
}

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
