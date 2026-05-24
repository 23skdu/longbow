package store

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestUringReaderStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	// Only run on Linux if iouring is available, or use stub on other platforms
	// But we want to test the hardened implementation.

	tempDir, err := os.MkdirTemp("", "uring_stress")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	testFile := filepath.Join(tempDir, "testdata.bin")
	dataSize := 1024 * 1024 * 10 // 10MB
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatal(err)
	}

	reader, err := NewUringReader(testFile)
	if err != nil {
		t.Skipf("Skipping uring stress test: %v", err)
		return
	}
	defer reader.Close()

	numGoroutines := 100
	numReadsPerGoroutine := 50
	readSize := 4096

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numReadsPerGoroutine; j++ {
				offset := int64((id*numReadsPerGoroutine + j) * readSize % (dataSize - readSize))
				buf := make([]byte, readSize)
				n, err := reader.ReadAt(buf, offset)
				if err != nil {
					t.Errorf("ReadAt failed at offset %d: %v", offset, err)
					return
				}
				if n != readSize {
					t.Errorf("ReadAt returned short read: %d < %d", n, readSize)
					return
				}
				if !bytes.Equal(buf, data[offset:offset+int64(readSize)]) {
					t.Errorf("Data corruption at offset %d", offset)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}
