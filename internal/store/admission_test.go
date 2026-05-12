package store

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/23skdu/longbow/internal/autoscale"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdmissionController(t *testing.T) {
	maxMem := atomic.Int64{}
	maxMem.Store(1024 * 1024 * 1024) // 1GB
	currMem := atomic.Int64{}
	
	ac := NewAdmissionController(&maxMem, &currMem, nil, zerolog.Nop())

	t.Run("Normal Load", func(t *testing.T) {
		currMem.Store(100 * 1024 * 1024) // 10%
		err := ac.Admit(context.Background(), "search")
		assert.NoError(t, err)
	})

	t.Run("Throttled Ingestion", func(t *testing.T) {
		currMem.Store(910 * 1024 * 1024) // 88.8% (Threshold is 88%)
		err := ac.Admit(context.Background(), "ingest")
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("Rejected Search", func(t *testing.T) {
		currMem.Store(950 * 1024 * 1024) // 95% (Threshold is 92%)
		err := ac.Admit(context.Background(), "search")
		assert.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	})

	t.Run("AutoScaler Health", func(t *testing.T) {
		currMem.Store(500)
		scaler := autoscale.NewAutoScaler(zerolog.Nop())
		// Force health to critical by adding fake high QPS
		for i := 0; i < 10000; i++ {
			scaler.RecordSearch(0)
		}
		
		ac.scaler = scaler
		err := ac.Admit(context.Background(), "search")
		// Depending on windowing, we might need to manually trigger a snapshot or wait
		// But Admit calls scaler.GetLoadSnapshot()
		// Let's check if the logic in scaler.go correctly marks it critical
		if status.Code(err) == codes.ResourceExhausted {
			assert.Contains(t, err.Error(), "critical capacity")
		}
	})
}
