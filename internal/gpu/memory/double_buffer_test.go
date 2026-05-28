package memory

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDoubleBuffer(t *testing.T) {
	db := NewDoubleBuffer(1024)
	assert.NotNil(t, db)
	assert.Equal(t, 1024, db.Capacity())
	assert.Equal(t, 0, db.ActiveSize())
}

func TestDoubleBufferActiveInactive(t *testing.T) {
	db := NewDoubleBuffer(64)
	active := db.GetActive()
	assert.Len(t, active, 64)

	inactive := db.GetInactive()
	assert.Len(t, inactive, 64)

	assert.False(t, &active[0] == &inactive[0])
}

func TestDoubleBufferSwap(t *testing.T) {
	db := NewDoubleBuffer(64)
	before := db.GetActive()
	db.Swap()
	after := db.GetActive()
	assert.False(t, &before[0] == &after[0])
	assert.Equal(t, 0, db.ActiveSize())
}

func TestDoubleBufferWrite(t *testing.T) {
	db := NewDoubleBuffer(64)
	n, err := db.Write([]byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, 5, db.ActiveSize())
}

func TestDoubleBufferWriteCapacityExceeded(t *testing.T) {
	db := NewDoubleBuffer(10)
	_, err := db.Write(make([]byte, 11))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "capacity exceeded")
}

func TestDoubleBufferWriteAcrossSwap(t *testing.T) {
	db := NewDoubleBuffer(64)
	db.Write([]byte("first"))
	db.Swap()
	n, err := db.Write([]byte("second"))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, 6, db.ActiveSize())

	db.Swap()
	ok := db.GetActive()
	assert.NotNil(t, ok)
}

func TestDoubleBufferReset(t *testing.T) {
	db := NewDoubleBuffer(64)
	db.Write([]byte("data"))
	db.Reset()
	assert.Equal(t, 0, db.ActiveSize())
}

func TestDoubleBufferCapacity(t *testing.T) {
	db := NewDoubleBuffer(256)
	assert.Equal(t, 256, db.Capacity())
}

func TestDoubleBufferRoundTrip(t *testing.T) {
	db := NewDoubleBuffer(32)
	data := []byte("test-data-here")
	n, err := db.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	buf := db.GetActive()
	assert.Equal(t, data, buf[:len(data)])
}

func TestDoubleBufferZeroCapacity(t *testing.T) {
	db := NewDoubleBuffer(0)
	_, err := db.Write([]byte{1})
	assert.Error(t, err)
}
