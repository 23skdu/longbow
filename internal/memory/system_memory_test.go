package memory

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPhysicalMemory_Detection(t *testing.T) {
	mem := GetPhysicalMemory()
	assert.Greater(t, mem, int64(0), "Physical memory must be greater than 0")
}

func TestGetPhysicalMemory_Override(t *testing.T) {
	defer os.Unsetenv("LONGBOW_PHYSICAL_RAM")

	os.Setenv("LONGBOW_PHYSICAL_RAM", "24GiB")
	mem := GetPhysicalMemory()
	assert.Equal(t, int64(24)*1024*1024*1024, mem)

	os.Setenv("LONGBOW_PHYSICAL_RAM", "500MB")
	mem = GetPhysicalMemory()
	assert.Equal(t, int64(500)*1000*1000, mem)
}

func TestParseMemorySize(t *testing.T) {
	assert.Equal(t, int64(1024), parseMemorySize("1KiB"))
	assert.Equal(t, int64(1000), parseMemorySize("1KB"))
	assert.Equal(t, int64(4*1024*1024), parseMemorySize("4MiB"))
	assert.Equal(t, int64(16*1024*1024*1024), parseMemorySize("16GiB"))
	assert.Equal(t, int64(123456), parseMemorySize("123456"))
	assert.Equal(t, int64(0), parseMemorySize("invalid"))
	assert.Equal(t, int64(0), parseMemorySize("-100GB"))
}
