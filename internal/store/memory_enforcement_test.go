package store

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestMemoryEnforcementLoadAndReject(t *testing.T) {
	const memLimit = 1024
	s := NewVectorStore(memory.NewGoAllocator(), zerolog.Nop(), memLimit, 0, 0)
	defer func() { _ = s.Close() }()

	s.memoryConfig.RejectWrites = true
	s.currentMemory.Store(900)

	const workers = 16
	const iters = 25
	var rejected atomic.Int64
	var allowed atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				if err := s.checkMemoryBeforeWrite(256, "active"); err != nil {
					rejected.Add(1)
				} else {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(workers*iters), rejected.Load())
	require.Equal(t, int64(0), allowed.Load())

	// Under limit succeeds
	s.currentMemory.Store(100)
	require.NoError(t, s.checkMemoryBeforeWrite(256, "active"))
}
